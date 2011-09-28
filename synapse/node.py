#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
Provides Node and Actor.

The value of the key *type* in the configuration is used to define the
underlying protocol. Currently there are some global variables specific
to zmq like _context and poller. _context holds a single :class:`zmq.Context`
that is shared among all node sockets.

The module relies on gevent for its execution. The poller runs in a dedicated
greenlet. When it registers a node, if the node provides a loop, it spawns it
in a new greenlet. Synapse uses the `gevent-zeromq` library to modify
:class:`zmq.Socket` in order to protect blocking read, write using a
:class:`gevent.event.Event` object.

Keep in mind greenlets are scheduled in a round-robin fashion. It means if we
start two actors A1 and A2, each one has two nodes, its mailbox and the
announce service. We call A1's mailbox A1.mbox and A1's announce subscriber
A1.as and use the same convention for A2.

Greenlets are scheduled in the order they were spawned: ::

    | poller | A1.mbox | A1.as | A2.mbox | A2.as |

Execution starts in the poller: ::

    |*poller*| A1.mbox | A1.as | A2.mbox | A2.as |

As the :class:`EventPoller` is always the one giving hand to others greenlets
(using :meth:`gevent.sleep`), any greenlet can run concurrently among greenlet
spawned by synapse.

For example, a code using :class:`gevent.socket` can be safely used.

    | poller | A1.mbox | A1.as | *gevent.socket* | A2.mbox | A2.as |
                                      ^
                    will be called when the fd is ready

More generally, any custom code using gevent can be run along with synapse, as
long as it does not forget to give the hand back before any blocking call.


Synapse also provides periodical handlers, which define a callable that will be
called indefinitely, waiting for the given period between each call, and until
the handle call returns explicitely "False" or raise an exception.

It works as following:

    while handler() is not False:
        wait `period` seconds

Because we wait the given period after the call, if the handler call takes
time, this time will not be consumed on the waiting time. For example, if the
period is 10s, and the handler call takes 5s, the total time for a loop will be
15s (5s for the call, 10s for the waiting part).

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

from synapse.message import ( makeMessage, makeCodec,
                            HelloMessage, ByeMessage,
                            WhereIsMessage, IsAtMessage,
                            UnknownNodeMessage, AckMessage,
                            NackMessage, MessageException,
                            MessageInvalidException,
                            CodecException )



_context = zmq.Context()

# FIXME: unused
# remove it ?
def mixIn(target, mixin_class):
    if mixin_class not in target.__bases__:
        target.__bases__ = (mixin_class,) + target.__bases__
    return target


# decorators

def async(func):
    """Use this simple decorator to tell when a callback is asynchronous"""
    func.async = True
    return func


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

# exceptions

class NodeException(Exception):
    def __init__(self, errmsg, reply):
        self.errmsg = errmsg
        self.reply = reply


    def __str__(self):
        return self.errmsg


class PollerException(Exception):
    pass


# base types

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
        """Add a new node to the directory. If the node is not already
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


    def remove(self, name):
        """
        remove a node from the direcory
        Arguments:
            :name name: name of the node to remove
            :name type: str
            :return: None
            :raises KeyError: if the node is not register is the directory
        """
        del self._nodes[name]


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
        config['type'] = 'zmq'
        self._announce = AnnounceClient(config, self.on_message)
        self._nodes = NodeDirectory(config, self._announce)
        self._handler = handler if handler else getattr(self, 'handle_message', None)
        self._tasks = []
        self._pendings = {}
        self._log = logging.getLogger(self.name)


    def __del__(self):
        """
        Send a *bye* message to the :class:`AnnounceServer` when the
        :class:`Actor` object is destroyed.

        """
        self.close()


    def __enter__(self):
        self.connect()
        return self
    
    
    def __exit__(self, type, value, traceback):
        self.close()


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
        self._greenlet = poller.register(self._mailbox)

        self._announce.connect()
        # client do not need to be registred
        #poller.register(self._announce._client)
        self._greenlet_ann_sub = poller.register(self._announce._subscriber)
        self._announce.hello(self._mailbox)

        self._log.debug('connected')

    def close(self):
        self._announce.bye(self)
        self._announce.close()
        self._mailbox.stop()
        [gevent.kill(g) for g in ( self._greenlet, self._greenlet_ann_sub) if g]
                      

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
        msg = incoming_msg.get()
        #assert msg.id == msgid
        replystring = func(msg)
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
            poller.spawn(self.will_handle, msg.id, incoming_msg, on_recv)

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
            poller.spawn(handler, self, msg)
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


    def on_message_hello(self,actor, msg):
        if msg.uri == self._uri or not self._uri:
            return
        self._nodes.add(msg.src, msg.uri)


    def on_message_is_at(self,actor, msg):
        self._nodes.add(msg.name, msg.uri)

    def on_message_bye(self, actor, msg):
        if msg.src == self.name:
            return
        self._nodes.remove(msg.src)



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
    
    def stop(self):
        self._publisher.stop()
        self._server.stop()
        
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, type, value, traceback):
        self.stop()

    def handle_message(self, msgstring):
        msg = self._codec.loads(msgstring)
        if msg.type == 'hello':
            self._log.debug('hello from %s' % msg.src)
            self._nodes.add(msg.src, msg.uri)
            reply = AckMessage(self._server.name)
        if msg.type == 'bye':
            self._nodes.remove(msg.src)
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


    def close(self):
        if self._subscriber:
            self._subscriber.close()
        self._client.close()


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
        """
        FIXME: 
            - undocumented funtion 
            - neither socket nor events is used
            - msgstring is undefined
        
        request = self._codec.loads(msgstring)
        self._handler(request)
        """


class Poller(object):
    """
    Monitors nodes. Registers nodes with :meth:`register`.

    """
    def loop(self):
        raise NotImplementedError()


    def register(self, node):
        raise NotImplementedError()


    def poll(self):
        """Warning blocks until a event happens on a monitored socket. Can
        handle several events in a loop.

        """
        raise NotImplementedError()



class EventPoller(Poller):
    def __init__(self, config, periodic_handler=None):
        self._pid = os.getpid()
        self._name = 'event.poller@%d' % self._pid
        self._log = logging.getLogger(self._name)
        self._task = gevent.spawn(self.loop)
        self._loop_again = True
        self._greenlets = []
        self._periodical_handlers = []
### kept for backward compatiblity
        self._old_timeout = None
        self._old_periodic_handler = None

    def get_timeout(self):
        return self._old_timeout
    def set_timeout(self, timeout):
        self._old_timeout = timeout
    timeout = property(get_timeout, set_timeout)


    def get_periodic_handler(self):
        return self._old_periodic_handler
    def set_periodic_handler(self, handler):
        if self._old_periodic_handler:
            raise PollerException('periodic handler already defined')
        if self._old_timeout is None:
            raise PollerException('missing timeout')
        self._old_periodic_handler = handler
        self.add_periodical_handler(handler, self._old_timeout)
    periodic_handler = property(get_periodic_handler, set_periodic_handler)
### END

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
        period = self.spawn(self.periodical_loop, handler, timeout)
        self._periodical_handlers.append(period)

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
        while handler() is not False:
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
            greenlet = self.spawn(node.loop)
            return greenlet
        
    def spawn(self, handler, *args, **kwargs):
        """Spawn a new greenlet, and link it to the current greenlet when an
        exception is raise. This will cause to make the current process to stop
        if any of the spawned greenlet fail with an unhandled exception.
        The greenlet successful completion is also linked to the
        :meth:`remove` method to remove it from the list of greenlets.

        :Parameters:
          args : args
            positional arguments
          kwargs : keyword args
            keyword arguments
        """
        import types
        assert handler is not None
        greenlet = gevent.spawn_link_exception(self.__spawn_handler__(handler),
                                               *args, **kwargs)
        greenlet.link(lambda x: self.remove(greenlet))
        self._greenlets.append(greenlet)
        if isinstance(handler, types.FunctionType) or \
                isinstance(handler, types.MethodType):
            name = handler.__name__
        else:
            name = handler.__class__
        self._log.debug('spawn function %s in greenlet %s' % (name, str(greenlet)))
        assert greenlet is not None
        return greenlet

    def remove(self, greenlet):
        """Remove a greenlet from the list of currently running greenlets

        :Parameters:
          greenlet : greenlet
            the greenlet instance that stopped
        """
        self._log.debug("greenlet %s exited" % str(greenlet))
        self._greenlets.remove(greenlet)

    def __spawn_handler__(self, handler):
        """Just a simple decorator that will be used when spawning greenlets,
        to catch exceptions to log them, and re-raise the exception

        """
        # You can's use wraps for callable class (__call__)
        # furthermore keep signature of the method looks not usefull
        #from functools import wraps
        #@wraps(handler)
        def method(*args, **kwargs):
            """Catch exceptions and log them"""
            try:
                return handler(*args, **kwargs)
            except Exception, err:
                import traceback
                map(self._log.error, traceback.format_exc().splitlines())
                raise
        return method

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
        """
        Send a message
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

    def stop(self):
        self._socket.close()


class ZMQClient(ZMQNode):
    
    def connect(self):
        self._socket = _context.socket(zmq.REQ)
        self._socket.connect(self._uri)
        # client side do not need to be registred, they are not looping
        #poller.register(self)
        
    def close(self):
        self._socket.close()

class ZMQPublish(ZMQNode):
    """Prove the publish side of a PUB/SUB topology.

    Behave as a server. Support only :meth:`send`. :meth:`start` do not take
    any handler as the publisher produces messages.

    """
    def start(self):
        self._socket = _context.socket(zmq.PUB)
        self._socket.bind(self._uri)


    def recv(self):
        raise NotImplementedError()

    def stop(self):
        self._socket.close()


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
        #self._socket.bind(self._uri)
        self._socket.connect(self._uri)
        self._socket.setsockopt(zmq.SUBSCRIBE, '')


    def loop(self):
        while True:
            self._log.debug('in subscriber loop')
            raw_request = self.recv()
            self._handler(raw_request)


    def send(self, msg):
        raise NotImplementedError()

    def close(self):
        self._socket.close()

# factories function

node_registry = {
                'zmq': {
                    'roles': {
                        'client':   ZMQClient,
                        'server':   ZMQServer,
                        'publish':  ZMQPublish,
                        'subscribe':ZMQSubscribe
                    }
                },
            }

def registerNode(name, config):
    node_registry[name] = config

def makeNode(config, handler=None):
    cls = node_registry[config['type']]['roles'][config['role']]
    return cls(config, handler) if handler else cls(config)

def makePoller(config):
    dispatch = {'zmq': EventPoller}
    return dispatch[config['type']](config)

# create the poller
poller = EventPoller({})
