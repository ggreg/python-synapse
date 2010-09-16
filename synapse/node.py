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
import os

from ordereddict import OrderedDict
import gevent
import gevent.event
import gevent.queue
import zmq

from message import makeMessage, makeCodec, \
                    HelloMessage, ByeMessage, \
                    WhereIsMessage, IsAtMessage, \
                    AckMessage, NackMessage, \
                    MessageException, MessageInvalidException, \
                    CodecException



_context = zmq.Context()



def log_on_exit(greenlet):
    logging.debug('greenlet %s exited' % greenlet)



def spawn(*args, **kwargs):
    """Spawn a greenlet and assert it is not None

    This function wraps :func:`gevent.spawn` and check the callback is not
    None, as well as the greenlet. It also logs when a greenlet is spawned.

    """
    import types
    handler = args[0]
    assert handler is not None
    greenlet = gevent_spawn(*args, **kwargs)
    greenlet.link(log_on_exit)
    if isinstance(handler, types.FunctionType) or \
            isinstance(handler, types.MethodType):
        name = handler.__name__
    else:
        name = handler.__class__
    logging.debug('spawn function %s in greenlet %s' % (name, str(greenlet)))
    assert greenlet is not None
    return greenlet

gevent_spawn = gevent.spawn
gevent.spawn = spawn



class Node(object):
    """Node abstract interface.
    A node is part of a network e.g. a graph that connects objects. It can
    receive messages from or send messages to other nodes connected by an edge.

    """
    name = 'ANONYMOUS'
    def send(self, dst, msg):
        raise NotImplementedError()


    def recv(self, src):
        raise NotImplementedError()



def async(func):
    """Use this simple decorator to tell when a callbackk is asynchronous"""
    func.async = True
    return func



class NodeDirectory(object):
    """Provides a mapping interface to resolve a name to a node connection.

    A node name is mapped to a single Node object. The Node object is
    instanciated from an URI by :meth:`add`.

    Take an optional :class:`AnnounceClient` to lookup for nodes.

    """
    def __init__(self, config, announce=None):
        """
        :Parameters:
          config : mapping
            defines the transport *type*
          announce : AnnounceClient
            used to resolved names

        """
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
        Return a connected node.

        If there is *name* key, it tries to resolve the name, to find the node.

        :Parameters:
          name : str

        """
        try:
            return self._nodes[name]
        except KeyError:
            if self._announce:
                rep = self._announce.where_is(name)
                return self.add(name, rep.uri)
            raise ValueError("Node %s is unknown" % name)


    def add(self, name, uri):
        """Add a new node to the directory. Ff the node is not already
        connected, connect it.

        :Parameters:
          name : str
          uri : str

        """
        self._nodes[name] = makeNode({
                'name': 'client:%s' % name,
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



def catch_exceptions(*exceptions):
    def wrapper(method):
        def wrapped_method(actor, *args, **kwargs):
            try:
                return method(actor, *args, **kwargs)
            except Exception, err:
                raise NodeException(str(err),
                        actor._codec.dumps(NackMessage(actor.name, str(err))))
        return wrapped_method
    return wrapper



class Actor(object):
    """
    An actor receives messages in its mailbox and handles them.

    In response to a message it receives, an actor can make local decisions,
    create more actors, send more messages, and determine how to respond to the
    next message received.

    A message is dispatched by :meth:`on_message` with respect to its *type*,
    and then handle either by an ad hoc method
    :meth:`on_message_<message.type>` or a handler passed as the second
    argument of the constructor. If no ad hoc method matches and no handler was
    defined, it cannot handle the message and returns an error message to the
    sender.

    An actor runs in two greenlets:
    - one for the announce client node
    - one for the mailbox node

    The announce client node is scheduled first.

    :IVariables:
    - `name`: the name that identifies the current node
    - `uri`: defines the protocol address of the current node
    - `mailbox`: object that provide access to messages
    - `nodes`: list of other nodes
    - `announce`: subscribed queue of announces where new nodes introduce
      themselves
    - `handler`: Optional. callable called when a message is received

    """
    def __init__(self, config, handler=None):
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
        self._announce = AnnounceClient(config, self.on_message)
        self._nodes = NodeDirectory(config, self._announce)
        self._handler = handler if handler else getattr(self, 'handle_message', None)
        if hasattr(handler, 'async'):
            self._handler.async = True
        self._tasks = []
        self._pendings = {}


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

        logging.debug('[%s] connected' % self.name)


    def will_handle(self, msgid, incoming_msg, func):
        """Wait on the *incoming_msg* queue. Defers the message handling.

        When the message is available, it is passed to *func*.

        :Parameters:
          msgid : message.Message.id
            message id registered in pendings messages
          incoming_msg : gevent.queue.Queue
            queue that make the greenlet sleeping until a message arrives
          func : callable
            callback that takes a message.Message subclass object

        """
        msgstring = incoming_msg.get()
        replystring = func(msgstring)
        del self._pendings[msgid]


    def sendrecv(self, node_name, msg, on_recv=None):
        """Send a message to a node and receive the reply.

        If the caller defines *on_recv*, :meth:`sendrecv` spawns the callback
        in a greenlet. The callback will be called on :meth:`on_message` when
        the actor receives the reply. The reply should commonly be a AckMessage
        to confirm the recipient is alive.

        Otherwise, it performs a synchronous calls and returns the reply.

        Warning: it does not return the reply message from the recipient
        *node_name*.

        :Parameters:
          node_name : str
          msg : message.Message subclass object
          on_recv : callable
            callback that takes a message.Message subclass object

        """
        remote = self._nodes[node_name]
        msgstring = self._codec.dumps(msg)

        remote.send(msgstring)

        if on_recv:
            logging.debug('[%s] async sendrecv' % self.name)
            incoming_msg = gevent.queue.Queue()
            self._pendings[msg.id] = incoming_msg
            gevent.spawn(self.will_handle, msg.id, incoming_msg, on_recv)

        logging.debug('[%s] handshake reply from %s in sendrecv()' % \
                      (self.name, node_name))

        reply = remote.recv()
        return self._codec.loads(reply)


    def wake_message_worker(self, msg):
        """Wake a greenlet to handle message with a specific id.

        It is called by :meth:`on_message` to resume a :meth:`sendrecv`.

        """
        self._pendings[msg.id].put(msg)


    @catch_exceptions(MessageException)
    def on_message(self, msgstring):
        """
        Called when the socket is ready to receive data. The request is
        decoded, passed to the handler, and sent back to caller.

        The method tries to dispatch the message with respect to its type. For
        example, a message 'test_msg' will be handled by the method
        *on_message_test_msg*. If it cannot find a method with this name, it
        will call :attr:`_handler`. Then if there is no :attr:`_handler`, it
        cannot handle the message and logs an error.

        :attr:`_handler` may be considered as a dispatcher provided by a layer
        on top of the actor. It allows to build custom dispatch and protocol.

        If the handler returns None, an :class:`AckMessage` is used. With a
        REQ/REP socket, when a request is received, you **have** to send back
        a response.

        """

        try:
            msg = self._codec.loads(msgstring)
            logging.debug('[%s] handling message #%d' % (self.name, msg.id))
        except CodecException, err:
            raise MessageInvalidException(str(err))

        handler = getattr(self, 'on_message_%s' % msg.type, self._handler)
        if not handler:
            errmsg = 'cannot handle message %s #%d' % (msg.type, msg.id)
            raise MessageInvalidException(msg)

        if msg.id in self._pendings:
            logging.debug('[%s] resume pending worker for message #%d' % \
                    (self.name, msg.id))
            replystring = self._codec.dumps(AckMessage(self.name))
            self._mailbox._socket.send(replystring)
            self.wake_message_worker(msg)
            return

        if hasattr(handler, 'async'):
            logging.debug('[%s] handling async call for message #%d' % \
                          (self.name, msg.id))
            replystring = self._codec.dumps(AckMessage(self.name))
            self._mailbox._socket.send(replystring)
            gevent.spawn(handler, self, msg)
            return

        logging.debug('[%s] handle synchronous message #%d' % \
                      (self.name, msg.id))

        try:
            reply = handler(self, msg)
        except Exception, err:
            errmsg = str(err)
            logging.error('[%s] error in handler: %s' % (self.name, errmsg))
            reply = NackMessage(self.name, errmsg)
        finally:
            if reply is None:
                reply = AckMessage(self.name)
            return self._codec.dumps(reply)


    def on_message_hello(self, msg):
        if msg.uri == self._uri or not self._uri:
            return
        try:
            self._nodes.add(msg.src, msg.uri)
        except Exception, err:
            logging.error('[%s] cannot add node %s with uri "%s": "%s"' % \
                          (self.name, msg.src, msg.uri, str(err)))


    def on_message_is_at(self, msg):
        self._nodes.add(msg.src, msg.uri)


    def on_message_bye(self, msg):
        if msg.uri == self._uri:
            return
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
        self.name = 'announce:server'
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
            logging.debug('[%s] hello from %s' % (self.name, msg.src))
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
        self.name = 'announce:client'
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
        logging.debug('[%s] message %s#%d sent to %s' % \
                      (self.name, msg.type, msg.id, dst.name))
        return dst.send(self._codec.dumps(msg))


    def recv_from(self, src):
        msg = src.recv()
        logging.debug('[%s] got message from %s' % \
                      (self.name, src.name))
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
        self._name = 'poller@%d' % os.getpid()
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
        self._loop_again = True


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
        logging.debug('[%s] GC collected: %d; garbage: %s' % \
                      (self._name, collected, gc.garbage))
        return self._task.join()


    def loop(self):
        while self._loop_again:
            logging.debug('[%s] polling...' % self._name)
            cont = self.poll()


    def periodic_loop(self):
        while True:
            self._periodic_event.wait()
            self._periodic_handler()
            self._periodic_event.clear()


    def register(self, waiting_event):
        """
        Maps the socket to its node. If the node contains a *loop*, spawns a
        loop in a greenlet.

        """
        self._nodes_by_socket[waiting_event._socket] = waiting_event
        if getattr(waiting_event, 'loop', None):
            greenlet = gevent.spawn(waiting_event.loop)
            greenlet.link_exception(lambda x: self.unregister(waiting_event))
            self._processes.append(greenlet)
        self._poller.register(waiting_event.socket, zmq.POLLIN)


    def unregister(self, node):
        self._poller.unregister(node._socket)
        del self._nodes_by_socket[node._socket]
        if len(self._nodes_by_socket) == 0 and \
                self._timeout and self._periodic_handler:
            self._loop_again = False


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
                logging.info('[%s] missed %d polling timeouts of %d' % \
                             (self._name, int(missed)-1, timeout))
            self._last_time = last + math.floor(missed) * timeout
            polling_timeout_ms = (timeout-missed) * 1000
            logging.debug('[%s] reset with %f (last timestamp: %f)' % \
                          (self._name, polling_timeout_ms, self._last_time))
        elif self._timeout:
            self._last_time = time.time()
            polling_timeout_ms = self._timeout*1000
            logging.debug('[%s] last timestamp: %f' % \
                          (self._name, self._last_time))

        actives = self._poller.poll(polling_timeout_ms)
        logging.debug('[%s] %d active sockets' % (self._name, len(actives)))
        for active_socket, poll_event in actives:
            logging.debug('[%s] active socket: %s' % (self._name, active_socket))
            waiting_event = self._nodes_by_socket[active_socket]
            logging.debug('[%s] wake %s' % (self._name, waiting_event.name))
            waiting_event.wake()
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


    def wait(self):
        self._event.wait()


    def wake(self):
        self._event.set()


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
        logging.debug('[%s] waiting in recv()' % self.name)
        self.wait()
        msgstring = self._socket.recv()
        logging.debug('[%s] socket: %s' % (self.name, self._socket))
        logging.debug('[%s] recv -> %s' % (self.name, msgstring))
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



class NodeException(Exception):
    def __init__(self, errmsg, reply):
        self.errmsg = errmsg
        self.reply = reply


    def __str__(self):
        return self.errmsg



class ZMQServer(ZMQNode):
    def __init__(self, config, handler):
        ZMQNode.__init__(self, config)
        self._handler = handler


    def start(self):
        self._socket = _context.socket(zmq.REP)
        self._socket.bind(self._uri)


    def loop(self):
        while True:
            try:
                logging.debug('[%s] in server loop' % self.name)
                raw_request = self.recv()
                raw_reply = self._handler(raw_request)
            except NodeException, err:
                errmsg = str(err.errmsg)
                logging.debug(errmsg)
                raw_reply = err.reply

            if raw_reply:
                self._socket.send(raw_reply)



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
            logging.debug('[%s] in subscriber loop' % self.name)
            raw_request = self.recv()
            self._handler(raw_request)


    def send(self, msg):
        raise NotImplementedError()



poller = ZMQPoller({})
