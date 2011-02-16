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

import gevent
import gevent.event
import gevent.queue
import gevent.coros
from gevent_zeromq import zmq

from synapse.message import makeMessage, makeCodec, \
                            HelloMessage, ByeMessage, \
                            WhereIsMessage, IsAtMessage, \
                            UnknownNodeMessage, AckMessage, \
                            NackMessage, MessageException, \
                            MessageInvalidException, \
                            CodecException



_context = zmq.Context()



def log_on_exit(greenlet):
    logger = logging.getLogger('greenlet@%d' % os.getpid())
    logger.debug('greenlet %s exited' % greenlet)
    if greenlet.exception:
        logger.error('greenlet exited with exception %s:%s' %
                     (greenlet.exception.__class__, greenlet.exception))


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
    logging.getLogger('greenlet@%d' % os.getpid()).debug(
        'spawn function %s in greenlet %s' % (name, str(greenlet)))
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
                if rep.type == 'is_at':
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
                'name': '%s.client' % name,
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
        self._name = config['name']
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
        self._log = logging.getLogger(self.name)


    def __del__(self):
        """
        Send a *bye* message to the :class:`AnnounceServer` when the
        :class:`Actor` object is destroyed.

        """
        self._announce.bye(self)


    @property
    def name(self):
        return self._name


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

        self._log.debug('connected')


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
            self._log.debug('async sendrecv')
            incoming_msg = gevent.queue.Queue()
            self._pendings[msg.id] = incoming_msg
            gevent.spawn(self.will_handle, msg.id, incoming_msg, on_recv)

        self._log.debug('handshake reply from %s in sendrecv()' % \
                        node_name)

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
            self._log.debug('handling message #%d' % msg.id)
        except CodecException, err:
            raise MessageInvalidException(str(err))

        handler = getattr(self, 'on_message_%s' % msg.type, self._handler)
        if not handler:
            errmsg = 'cannot handle message %s #%d' % (msg.type, msg.id)
            raise MessageInvalidException(msg)

        if msg.id in self._pendings:
            self._log.debug('resume pending worker for message #%d' % msg.id)
            replystring = self._codec.dumps(AckMessage(self.name))
            self._mailbox._socket.send(replystring)
            self.wake_message_worker(msg)
            return

        if hasattr(handler, 'async'):
            self._log.debug('handling async call for message #%d' % msg.id)
            replystring = self._codec.dumps(AckMessage(self.name))
            self._mailbox._socket.send(replystring)
            gevent.spawn(handler, self, msg)
            return

        self._log.debug('handle synchronous message #%d' % msg.id)

        try:
            reply = handler(self, msg)
        except Exception, err:
            errmsg = str(err)
            self._log.error('error in handler: %s' % errmsg, exc_info=True)
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
            self._log.error('cannot add node %s with uri "%s": "%s"' % \
                          (msg.src, msg.uri, str(err)))


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
        self.name = 'announce.server'
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._server = makeNode({
                'name': self.name,
                'type': config['type'],
                'uri':  config['announce']['server_uri'],
                'role': 'server'
                },
                self.handle_message)
        self._publisher = makeNode({
                'name': 'announce.publisher',
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'publish'
                })
        self._nodes = NodeDirectory(config)
        self._log = logging.getLogger(self.name)


    def start(self):
        self._publisher.start()
        self._server.start()
        poller.register(self._server)


    def handle_message(self, msgstring):
        msg = self._codec.loads(msgstring)
        if msg.type == 'hello':
            self._log.debug('hello from %s' % msg.src)
            self._nodes.add(msg.src, msg.uri)
            reply = AckMessage(self._server.name)
        if msg.type == 'bye':
            self._nodes.remove(msg.src, msg.uri)
            reply = AckMessage(self._server.name)
        if msg.type == 'where_is':
            if msg.name not in self._nodes:
                reply = UnknownNodeMessage(msg.name)
            else:
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
        self.name = '%s.announce' % config['name']
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._nodes = []
        self._client = makeNode({
                'name': self.name,
                'type': config['type'],
                'uri':  config['announce']['server_uri'],
                'role': 'client'
                })
        self._nodes.append(self._client)
        self._subscriber = makeNode({
                'name': '%s.subscribe' % config['name'],
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'subscribe',
                },
                self.handle_announce) if handler else None
        if self._subscriber:
            self._nodes.append(self._client)
        self._handler = handler
        self._log = logging.getLogger(self.name)


    @property
    def nodes(self):
        return self._nodes


    def connect(self):
        if self._subscriber:
            self._subscriber.connect()
        self._client.connect()


    def send_to(self, dst, msg):
        self._log.debug('message %s#%d' % (msg.type, msg.id))
        return dst.send(self._codec.dumps(msg))


    def recv_from(self, src):
        msg = src.recv()
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


class EventPoller(Poller):
    def __init__(self, config, periodic_handler=None):
        self._pid = os.getpid()
        self._name = 'event.poller@%d' % self._pid
        self._task = gevent.spawn(self.loop)
        self._loop_again = True
        self._greenlets = []
        self._periodical_handlers = []
        self._log = logging.getLogger(self._name)

    def add_periodical_handler(self, handler, timeout):
        """Install a new periodical handler.
        If the :meth:`periodical_loop` exits, it can mean that the
        :meth:`handler` decided to stop, or it exited with an exception.
        In both case, we need to know when it stopped.

        :Parameters:
          handler : callable
            the callable to call each period
          timeout : integer
            time between each poll
        """
        assert(callable(handler))
        assert(timeout > 0)
        period = gevent.spawn(self.periodical_loop, handler, timeout)
        period.link(lambda x: self.remove_periodical_handler(period))
        self._periodical_handlers.append(period)

    def remove_periodical_handler(self, periodical):
        """Remove a periodical handler from the list of running handlers.
        If no more handlers are running, #@!#

        """
        self._periodical_handlers.remove(periodical)

    def loop(self):
        while self._loop_again:
            gevent.core.loop()

    def periodical_loop(self, handler, timeout):
        """Call the `handler` each `timeout` seconds.
        This method is called in a dedicated greenlet.

        :Parameters:
          handler : callable
            the callable to call each period
          timeout : integer
            time between each poll
        """
        while handler():
            gevent.sleep(timeout)

    def wait(self):
        """Simply waits until the greenlet of the poller stops"""
        import gc
        collected = gc.collect()
        self._log.debug('GC collected: %d; garbage: %s' % \
                      (collected, gc.garbage))
        return self._task.join()

    def register(self, node):
        """Register a new node in the poller.
        If the given node is a `server`, we need to spawn it's :meth:`loop`
        method in a dedicated greenlet, and watch for its completion.

        :Parameters:
          node : Node-inherited class
            the node to register
        """
        if getattr(node, 'loop', None):
            greenlet = gevent.spawn(node.loop)
            greenlet.link(lambda x: self.unregister(greenlet))
            self._greenlets.append(greenlet)

    def unregister(self, greenlet):
        """Unregister a :meth:`poll` method for a `server` node.

        :Parameters:
          greenlet : greenlet
            the greenlet instance that stopped
        """
        self._greenlets.remove(greenlet)

    def __repr__(self):
        """Simply returns the class with the current pid"""
        return '<%s pid:%d>' % (self.__class__.__name__, self._pid)


class ZMQNode(Node):
    """Node built on top of ZeroMQ.

    ZeroMQ provides a socket API to build several kinds of topology.

    """
    type = 'zmq'

    def __init__(self, config):
        self._name = config.get('name', 'ANONYMOUS')
        self._uri = config['uri']
        self._socket = None
        self._lock = gevent.coros.Semaphore()
        self._log = logging.getLogger(self.name)


    @property
    def name(self):
        return self._name


    @property
    def uri(self):
        return self._uri


    @property
    def socket(self):
        return self._socket


    def send(self, msg):
        """Send a message
        :param  dst: object that contains a send() socket interface
        :param  msg: serializable string

        """
        self._lock.acquire()
        ret = self._socket.send(msg)
        self._lock.release()
        return ret


    def recv(self):
        """
        Return a message as a string from the receiving queue.

        Blocks on the underlying ``self._socket.recv()``, that's why it waits
        on a event that will be woke up by the poller.

        """
        self._log.debug('waiting in recv()')
        self._lock.acquire()
        msgstring = self._socket.recv()
        self._log.debug('socket: %s' % self._socket)
        self._log.debug('recv -> %s' % msgstring)
        self._lock.release()
        return msgstring

    def __repr__(self):
        return "<%s %s (%s)>" % (self.__class__.__name__,
                                 self._name,
                                 self._uri)


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
    dispatch = {'zmq': EventPoller}
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
                self._log.debug('in server loop')
                raw_request = self.recv()
                raw_reply = self._handler(raw_request)
            except NodeException, err:
                errmsg = str(err.errmsg)
                self._log.debug(errmsg)
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
            self._log.debug('in subscriber loop')
            raw_request = self.recv()
            self._handler(raw_request)


    def send(self, msg):
        raise NotImplementedError()



poller = EventPoller({})
