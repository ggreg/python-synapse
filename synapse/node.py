"""
Provides Node and Actor.

The value of the key *type* in the configuration is used to define the
underlying protocol. Currently there are some global variables specific
to zmq like _context and poller. _context holds a single :class:`zmq.Context`
that is shared among all node sockets. The poller is a :class:`zmq.Poller`.
However a :class:`zmq.Poller` can poll a posix socket as well as a zmq socket.

The module relies on gevent for its execution. The poller runs in a dedicated
greenlet. When it registers a node, if the node provides a loop, it spawns it
in a new greenlet. Blocking calls like :meth:`Node.recv` must be protected by
an event or another way to put the greenlet into sleep and wake it up once it
will not block.

For this purpose, we use a :class:`gevent.event.Event` to wait.

Keep in mind greenlets are scheduled in a round-robin fashion. It means if we
start two actors A1 and A2, each one has two nodes, its mailbox and the
announce service. We call A1's mailbox A1.mbox and A1's announce subscriber
A1.as and use the same convention for A2.

Greenlets are scheduled in the order they were spawned: ::

    | poller | A1.mbox | A1.as | A2.mbox | A2.as |

Execution starts in the poller: ::

    |*poller*| A1.mbox | A1.as | A2.mbox | A2.as |

The poller blocks in :meth:`Poller.poll` until a incoming message is available
in a socket. When it happens the poller sets the node's event corresponding to
the socket and sleeps to let other greenlets run. It wakes all waiting events
before sleeping. Let consider A2 receives a message in its mailbox: ::


    | poller | A1.mbox | A1.as | A2.mbox | A2.as |
                  ^        ^
                 still waiting

    | poller | A1.mbox | A1.as |*A2.mbox*| A2.as |
                                    ^
                       activates and calls its handler

.. note:: every function that may block MUST be protected by a event or another
   data structure that will be wake up either directly by the poller or
   indirectly by a chain of events. The poller is always the source that wakes
   the first event in the chain.

"""
import logging
import math
import time

from ordereddict import OrderedDict
import gevent
import gevent.event
import zmq

from message import makeMessage, makeCodec, \
                    HelloMessage, ByeMessage, \
                    WhereIsMessage, IsAtMessage, AckMessage



_context = zmq.Context()



class Node(object):
    """Node abstract interface.
    A node is part of a network e.g. a graph that connects objects. It can
    receive messages from or send messages to other nodes connected by an edge.

    """
    def send(self, dst, msg):
        raise NotImplementedError()


    def recv(self, src):
        raise NotImplementedError()



class NodeDirectory(object):
    """Stores a node name -> Node object mapping

    A node name is mapped to a single Node object. The Node object is
    instanciated from an URI by :meth:`add`.

    Take an optional :class:`AnnounceClient` to lookup for nodes.

    """
    def __init__(self, config, announce=None):
        self._config = {
            'type': config['type'],
            'role': 'client'
            }
        self._announce = announce
        self._nodes = {}


    def __contains__(self, name):
        return self._nodes.__contains__(name)


    def __getitem__(self, name):
        """
        If *announce* was defined in the constructor and the name is not in the
        mapping, the announcer queried to resolve the name to a node URI.

        """
        try:
            return self._nodes[name]
        except KeyError:
            if self._announce:
                rep = self._announce.where_is(name)
                return self.add(name, rep.uri)
            raise ValueError("Node %s is unknown" % name)


    def add(self, name, uri):
        """Add a new node to the directory, and if the node is not already
        connected, connect to it.

        """
        self._nodes[name] = makeNode({
                'type': self._config['type'],
                'role': self._config['role'],
                'uri':  uri
                })
        s = self._nodes[name].socket
        if not s or s.closed:
            self._nodes[name].connect()
        return self._nodes[name]


    def remove(self, name, uri):
        del self._nodes[name]



class Actor(object):
    """
    An actor receives messages in its mailbox and handles them.

    In response to a message it receives, an actor can make local decisions,
    create more actors, send more messages, and determine how to respond to the
    next message received.

    :IVariables:
    - `name`: the name that identifies the current node
    - `uri`: defines the protocol address of the current node
    - `mailbox`: object that provide access to messages
    - `nodes`: list of other nodes
    - `announce`: subscribed queue of announces where new nodes introduce
      themselves
    - `handler`: callable called when a message is received

    """
    def __init__(self, config, handler):
        self._uri = config['uri']
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._mailbox = makeNode({
                'name': config['name'],
                'type': config['type'],
                'uri':  self._uri,
                'role': 'server'
                },
                self.on_message)
        self._announce = AnnounceClient(config, self.on_announce)
        self._nodes = NodeDirectory(config, self._announce)
        self._handler = handler


    def __del__(self):
        """
        Send a *bye* message to the :class:`AnnounceServer` when the
        :class:`Actor` object is destroyed.

        """
        self._announce.bye(self)


    @property
    def name(self):
        return self._mailbox.name


    def connect(self):
        """
        When an actor connects to network, it initializes its nodes and
        registers them to the poller. It ends by introducing itself to other
        actors with a *hello* message.

        """
        self._mailbox.start()
        poller.register(self._mailbox)

        self._announce.connect()
        poller.register(self._announce._client)
        poller.register(self._announce._subscriber)
        self._announce.hello(self._mailbox)

        logging.debug('%s connected' % self.name)


    def sendrecv(self, node_name, msg):
        remote = self._nodes[node_name]
        msg = self._codec.dumps(msg)
        remote.send(msg)
        reply = remote.recv()
        return self._codec.loads(reply)


    def on_message(self, msgstring):
        """
        Called when the socket is ready to receive data. The request is
        decoded, passed to the handler, and sent back to caller.

        If the handler returns None, an :class:`AckMessage` is used. With a
        REQ/REP socket, when a request is received, you **have** to send back
        a response.

        """
        request = self._codec.loads(msgstring)
        logging.debug('handling message in %s' % self.name)
        reply = self._handler(self, request)
        if reply:
            replystring = self._codec.dumps(reply)
        else:
            replystring = self._codec.dumps(AckMessage(self._mailbox.name))
        return replystring


    def on_announce(self, msg):
        """
        Handler called by the :class:`AnnounceClient` when it receives an
        announce for a :class:`Node`.

        """
        if msg.type == 'hello':
            self._nodes.add(msg.src, msg.uri)
        if msg.type == 'is_at':
            self._nodes.add(msg.src, msg.uri)
        if msg.type == 'bye':
            self._nodes.remove(msg.src, msg.uri)



class AnnounceServer(object):
    """
    The announce server listens to messages and publishes them to all connected
    nodes.

    :IVariables:
    - `server`
    - `publisher`
    - `codec`
    - `nodes`

    """
    def __init__(self, config):
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._server = makeNode({
                'name': 'announce:server',
                'type': config['type'],
                'uri':  config['announce']['server_uri'],
                'role': 'server'
                },
                self.handle_message)
        self._publisher = makeNode({
                'name': 'announce:publisher',
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'publish'
                })
        self._nodes = NodeDirectory(config)


    def start(self):
        self._publisher.start()
        self._server.start()
        poller.register(self._server)


    def handle_message(self, msgstring):
        msg = self._codec.loads(msgstring)
        if msg.type == 'hello':
            logging.debug('hello from %s' % msg.src)
            self._nodes.add(msg.src, msg.uri)
            reply = AckMessage(self._server.name)
        if msg.type == 'bye':
            self._nodes.remove(msg.src, msg.uri)
            reply = AckMessage(self._server.name)
        if msg.type == 'where_is':
            node = self._nodes[msg.name]
            reply = IsAtMessage(msg.name, node.uri)
        return self._codec.dumps(reply)



class AnnounceClient(object):
    """
    The announce service localizes the nodes in the network. When a node joins
    the network it sends a 'hello' message. The 'hello' message is published to
    all other nodes through the announce queue.

    When a node wants to know the URI of another node it sends a 'where_is'
    message.

    If the *handler* parameter is ommitted, the :class:`AnnounceClient` is used
    only as a resolver. It won't be notified of new nodes, it will only be able
    to lookup for new nodes.

    :IVariables:
    - `subscriber`
    - `client`
    - `codec`

    """
    def __init__(self, config, handler=None):
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._nodes = []
        self._client = makeNode({
                'name': 'announce:client',
                'type': config['type'],
                'uri':  config['announce']['server_uri'],
                'role': 'client'
                })
        self._nodes.append(self._client)
        self._subscriber = makeNode({
                'name': 'announce:subscribe',
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'subscribe',
                },
                self.handle_announce) if handler else None
        if self._subscriber:
            self._nodes.append(self._client)
        self._handler = handler


    @property
    def nodes(self):
        return self._nodes


    def connect(self):
        if self._subscriber:
            self._subscriber.connect()
        self._client.connect()


    def send_to(self, dst, msg):
        logging.debug('message %s sent to %s' % (msg.type, dst.name))
        return dst.send(self._codec.dumps(msg))


    def recv_from(self, src):
        msg = src.recv()
        logging.debug('got %s from %s' % (msg, src.name))
        return self._codec.loads(msg)


    def hello(self, node):
        msg = HelloMessage(node.name, node.uri)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def bye(self, node):
        msg = ByeMessage(node.name)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def where_is(self, other_node_name):
        msg = WhereIsMessage(other_node_name)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def handle_announce(self, socket, events):
        request = self._codec.loads(msgstring)
        self._handler(request)



class Poller(object):
    """
    Monitors nodes. Registers and unregisters nodes with respectively
    :meth:`register` and :meth:`unregister`.

    """
    def loop(self):
        raise NotImplementedError()


    def register(self, node):
        raise NotImplementedError()


    def unregister(self, node):
        raise NotImplementedError()


    def poll(self):
        """Warning blocks until a event happens on a monitored socket. Can
        handle several events in a loop.

        """
        raise NotImplementedError()



class PollerException(Exception):
    pass



class ZMQPoller(Poller):
    def __init__(self, config, periodic_handler=None):
        self._poller = zmq.Poller()
        self._nodes_by_socket = OrderedDict()
        self._task = gevent.spawn(self.loop)
        self._processes = []
        self._timeout = config.get('timeout')
        self._last_time = None

        self._periodic_task = None
        self._periodic_event = None
        if periodic_handler:
            self.periodic_handler = periodic_handler
        else:
            self._periodic_handler = None


    def get_timeout(self):
        return self._timeout
    def set_timeout(self, timeout):
        self._timeout = timeout
    timeout = property(get_timeout, set_timeout)


    def get_periodic_handler(self):
        return self._periodic_handler
    def set_periodic_handler(self, handler):
        if self._periodic_handler:
            raise PollerException('periodic handler already defined')
        self._periodic_handler = handler
        self._periodic_task = gevent.spawn(self.periodic_loop)
        self._periodic_event = gevent.event.Event()
    periodic_handler = property(get_periodic_handler, set_periodic_handler)


    def unset_periodic_handler(self, handler):
        self._periodic_handler = None
        gevent.kill(self._periodic_task)
        self._periodic_event = None


    def wait(self):
        import gc
        collected = gc.collect()
        logging.debug('[poller] GC collected: %d; garbage: %s' % \
                      (collected, gc.garbage))
        return self._task.join()


    def loop(self):
        cont = True
        while cont:
            logging.debug('polling...')
            cont = self.poll()


    def periodic_loop(self):
        while True:
            self._periodic_event.wait()
            self._periodic_handler()
            self._periodic_event.clear()


    def register(self, node):
        """
        Maps the socket to its node. If the node contains a *loop*, spawns a
        loop in a greenlet.

        """
        self._nodes_by_socket[node._socket] = node
        if getattr(node, 'loop', None):
            self._processes.append(gevent.spawn(node.loop))
        self._poller.register(node._socket, zmq.POLLIN)


    def unregister(self, node):
        self._poller.unregister(node._socket)
        del self._nodes_by_socket[node._socket]


    def poll(self):
        """
        Blocks until a event happens on a socket. Then gets all the events and
        wakes all the corresponding nodes. Finally sleeps to let node greenlets
        run. If the poller has a *timeout* set, interrupt the polling after
        *timeout* seconds. When not specified, time values are represented in
        secondes.

        """
        polling_timeout_ms = None
        if self._last_time and self._timeout:
            timeout = self._timeout
            last = self._last_time
            elapsed = time.time() - last
            if elapsed >= timeout and self._periodic_handler and \
                    not self._periodic_event.is_set():
                self._periodic_event.set()
                gevent.sleep()
            elapsed = time.time() - last
            missed = elapsed / timeout
            if int(missed) > 1:
                logging.info('[poller] missed %d polling timeouts of %d' % \
                             (int(missed)-1, timeout))
            self._last_time = last + math.floor(missed) * timeout
            polling_timeout_ms = (timeout-missed) * 1000
            logging.debug('[poller] reset with %f (last timestamp: %f)' % \
                          (polling_timeout_ms, self._last_time))
        elif self._timeout:
            self._last_time = time.time()
            polling_timeout_ms = self._timeout*1000
            logging.debug('[poller] last timestamp: %f' % self._last_time)

        actives = self._poller.poll(polling_timeout_ms)
        logging.debug('[poller] %d active sockets' % len(actives))
        for active_socket, poll_event in actives:
            logging.debug('[poller] active socket: %s' % active_socket)
            node = self._nodes_by_socket[active_socket]
            logging.debug('[poller] wake node %s' % node.name)
            node.event.set()
        if actives:
            gevent.sleep()

        return True



class ZMQNode(Node):
    """Node built on top of ZeroMQ.

    ZeroMQ provides a socket API to build several kinds of topology.

    """
    type = 'zmq'

    def __init__(self, config):
        self._name = config.get('name', 'ANONYMOUS')
        self._uri = config['uri']
        self._socket = None
        self._event = gevent.event.Event()


    @property
    def name(self):
        return self._name


    @property
    def uri(self):
        return self._uri


    @property
    def socket(self):
        return self._socket


    @property
    def event(self):
        return self._event


    def send(self, msg):
        """Send a message
        :param  dst: object that contains a send() socket interface
        :param  msg: serializable string

        """
        return self._socket.send(msg)


    def recv(self):
        """
        Return a message as a string from the receiving queue.

        Blocks on the underlying ``self._socket.recv()``, that's why it waits
        on a event that will be woke up by the poller.

        """
        logging.debug('%s waiting in recv()' % self.name)
        self._event.wait()
        msgstring = self._socket.recv()
        self._event.clear()
        return msgstring



def mixIn(target, mixin_class):
    if mixin_class not in target.__bases__:
        target.__bases__ = (mixin_class,) + target.__bases__
    return target



def makeNode(config, handler=None):
    dispatch = {'zmq': {
                'class': ZMQNode,
                'roles': {
                    'client':   ZMQClient,
                    'server':   ZMQServer,
                    'publish':  ZMQPublish,
                    'subscribe':ZMQSubscribe}}}

    cls = dispatch[config['type']]['class']
    if 'role' in config:
        cls = dispatch[config['type']]['roles'][config['role']]

    return cls(config, handler) if handler else cls(config)



def makePoller(config):
    dispatch = {'zmq': ZMQPoller}
    return dispatch[config['type']](config)



class ZMQServer(ZMQNode):
    def __init__(self, config, handler):
        ZMQNode.__init__(self, config)
        self._handler = handler


    def start(self):
        self._socket = _context.socket(zmq.REP)
        self._socket.bind(self._uri)


    def loop(self):
        while True:
            logging.debug('%s in server loop' % self.name or 'ANONYMOUS')
            raw_request = self.recv()
            raw_reply = self._handler(raw_request)
            self._socket.send(raw_reply)
            logging.debug('%s replied' % self.name)


    def recv(self):
        """Receive a message
        @param  src: object that contains a recv() socket interface
        @rtype: str

        """
        logging.debug('[%s] waiting on event %s' % (self.name, self._event))
        self._event.wait()
        msgstring = self._socket.recv()
        logging.debug('[%s] socket: %s' % (self.name, self._socket))
        logging.debug('[%s] recv -> %s' % (self.name, msgstring))
        self._event.clear()
        return msgstring


    def send(self, msg):
        raise NotImplementedError()



class ZMQClient(ZMQNode):
    def connect(self):
        self._socket = _context.socket(zmq.REQ)
        self._socket.connect(self._uri)
        poller.register(self)


    def __repr__(self):
        return '<%s: %s>' % (type(self), self.uri)



class ZMQPublish(ZMQNode):
    """Prove the publish side of a PUB/SUB topology.

    Behave as a server. Support only :meth:`send`. :meth:`start` do not take
    any handler as the publisher produces messages.

    """
    def start(self):
        self._socket = _context.socket(zmq.PUB)


    def recv(self):
        raise NotImplementedError()



class ZMQSubscribe(ZMQNode):
    """Provide the subscribe side of a PUB/SUB topology.

    Behave as a client. It connects to the remote publish side and listens to
    incoming messages in the queue. :meth:`connect` takes the handler that will
    be called when a message arrives. Support only :meth:`recv`.

    """
    def __init__(self, config, handler):
        ZMQNode.__init__(self, config)
        self._handler = handler


    def connect(self):
        self._socket = _context.socket(zmq.SUB)
        self._socket.bind(self._uri)
        self._socket.connect(self._uri)
        self._socket.setsockopt(zmq.SUBSCRIBE, '')


    def loop(self):
        while True:
            logging.debug('%s in subscriber loop' % self.name or 'ANONYMOUS')
            raw_request = self.recv()
            self._handler(raw_request)


    def recv(self):
        """Receive a message
        @param  src: object that contains a recv() socket interface
        @rtype: str

        """
        self._event.wait()
        msgstring = self._socket.recv()
        logging.debug('[%s] recv -> %s' % (self.name, msgstring))
        self._event.clear()
        return msgstring


    def send(self, msg):
        raise NotImplementedError()


poller = ZMQPoller({})
