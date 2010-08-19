"""
Provides Node and Actor.

All objects share the same global _context and _loop. Non-blocking and
asynchronous events handler are registered in the global _loop. The event loop
allows to handle multiple requests in a single process. However internally
EventLoop.start() blocks on a poll().

You may start the loop in a dedicated thread if you need to compute in
parallel. In this case you must handle concurrent access to data. This module
does not protect data against concurrent access. Hence it will be hard to
implement fine-grained locking. With no thread (but the main process one), you
must start the loop after having setup every nodes or actor.

Instead of Python threads, greenlets of other cooperative light-processes can
help you to avoid concurrent accesses. In this case you need to manage the
scheduling between light-processes and to check the polling status sometimes.

With a single process, everything is sequential. Consequently an actor
handles one request at a time. Let consider you have the light-processes. The
first runs computations and the second runs the polling loop. Computations need
data to run. Then it switches execution to the polling loop. The polling loop
waits until a new incoming request arrives. This is not an issue since no
request implies no computation. Now imagine a request arrives. It unblocks the
polling loop. Then the pooling loop sets a flag and switch the execution to the
computation loop. The computation runs until its end while requests accumulate
into the socket buffer. When the computation is finished, it switches execution
to the polling loop.

It's simple cooperative multitasking. What happens if the computation blocks on
I/O? CPU time is wasted. It is the issue that brought multitasking to operation
systems. If you have several tasks to run and some tasks block, you waste CPU
time. Then you need to switch execution between tasks when they block.

"""
import zmq

from message import makeMessage, makeCodec, \
                    HelloMessage, ByeMessage, WhereIsMessage, AckMessage



class EventLoop(object):
    """Loop that checks events on file descriptors.

    Use :meth:`add_recv_handler` to add on handler on a file descriptor
    incoming events e.g. when it is ready to receive data. Then call
    :methd:`start` to start handling events. It will run a polling loop that
    blocks until a event is triggered.

    """
    def __init__(self, run_in_thread=False):
        from zmq.eventloop import ioloop
        self._loop = ioloop.IOLoop.instance()
        self._is_started = False
        self._thread = None
        if run_in_thread:
            import threading
            self._thread = threading.Thread(target=self._loop.start)


    def add_recv_handler(self, socket, handler):
        """add a handler that will be triggered when the file descriptor is
        ready to receive data.

        """
        self._loop.add_handler(socket, handler, zmq.POLLIN)


    def del_handler(self, handler):
        self._loop.remove_handler(handler)


    def start(self):
        """start the polling loop to handler events. Blocks the process until a
        event is triggered.

        """
        if not self._is_started:
            if self._thread:
                self._thread.start()
            else:
                self._loop.start()
            self._is_started = True



_context = zmq.Context()
_loop = EventLoop()



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

    A node name is mappend to a single Node object. The Node object is
    instanciated from an URI by :meth:`add`.

    """
    def __init__(self, config):
        self._config = {
            'type': config['type'],
            'role': 'client'
            }
        self._nodes = {}


    def __contains__(self, name):
        return self._nodes.__contains__(name)


    def __getitem__(self, name):
        return self._nodes[name]


    def add(self, name, uri):
        self._nodes[name] = makeNode({
                'type': self._config['type'],
                'role': self._config['role'],
                'uri':  uri
                })


    def remove(self, name, uri):
        del self._nodes[name]



class Actor(object):
    """
    An actor receives messages in its mailbox.

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
        self._handler = handler
        self._uri = config['uri']
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._mailbox = makeNode({
                'name': config['name'],
                'type': config['type'],
                'uri':  self._uri,
                'role': 'server'
                })
        self._announce = AnnounceClient(config, self.on_announce)
        self._nodes = NodeDirectory(config)


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
        self._mailbox.start(self.on_message)
        self._announce.connect()
        self._announce.hello(self._mailbox)


    def sendrecv(self, node_name, msg):
        remote = self.nodes[node_name]
        remote.send(msg)
        reply = remote.recv(msg)


    def on_message(self, socket, events):
        """
        :meth:`on_message` is called by the :class:`EventLoop` when the socket
        is ready to receive data. The request is loaded by the codec, call the
        handler, and send back the handler's return. If the handler returns None,
        a :class:`AckMessage` is used. With a REQ/REP socket, when a request is
        received, you **have** to send back a response.

        """
        msgstring = socket.recv()
        request = self._codec.loads(msgstring)
        reply = self._handler(request)
        if reply:
            replystring = self._codec.dumps(reply)
        else:
            replystring = self._codec.dumps(AckMessage(self._mailbox.name))
        socket.send(replystring)


    def on_announce(self, msg):
        """
        Handler called by the :class:`AnnounceClient` when it receives an
        announce for a :class:`Node`.

        """
        if msg.type == 'hello':
            self._nodes.add(msg.src, msg.uri)
        if msg.type == 'bye':
            self._nodes.remove(msg.src, msg.uri)



class AnnounceServer(object):
    """
    The announce server listens to messages and publish them to all connected
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
                })
        self._publisher = makeNode({
                'name': 'announce:publisher',
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'publish'
                })
        self._nodes = NodeDirectory(config)


    def start(self):
        self._publisher.start()
        self._server.start(self.handle_message)


    def handle_message(self, socket, events):
        msgstring = socket.recv()
        msg = self._codec.loads(msgstring)
        if msg.type == 'hello':
            print 'hello from %s' % msg.src
            self._nodes.add(msg.src, msg.uri)
        if msg.type == 'bye':
            self._nodes.remove(msg.src, msg.uri)
        socket.send(self._codec.dumps(AckMessage(self._server.name)))



class AnnounceClient(object):
    """
    The announce service localizes the nodes in the network. When a node joins
    the network it sends a 'hello' message. The 'hello' message is published to
    all other nodes through the announce queue.

    When a node wants to know the URI of another node it sends a 'where_is'
    message.

    :IVariables:
    - `subscriber`
    - `client`
    - `codec`

    """
    def __init__(self, config, handler):
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._subscriber = makeNode({
                'type': config['type'],
                'uri':  config['announce']['pubsub_uri'],
                'role': 'subscribe',
                })
        self._client = makeNode({
                'type': config['type'],
                'uri':  config['announce']['server_uri'],
                'role': 'client'
                })
        self._handler = handler


    def connect(self):
        self._subscriber.connect(self.handle_announce)
        self._client.connect()


    def send_to(self, dst, msg):
        return dst.send(self._codec.dumps(msg))


    def recv_from(self, src):
        return self._codec.loads(src.recv())


    def hello(self, node):
        msg = HelloMessage(node.name, node.uri)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def bye(self, node):
        msg = ByeMessage(node.name)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def where_is(node_name):
        msg = WhereIsMessage(node.name, params={'who': other_node_name})
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def handle_announce(self, socket, events):
        msgstring = socket.recv()
        request = self._codec.loads(msgstring)
        self._handler(request)



class ZMQNode(Node):
    """Node built on top of ZeroMQ.

    ZeroMQ provides a socket API to build several kinds of topology.

    """
    type = 'zmq'

    def __init__(self, config):
        self._name = config.get('name', '')
        self._uri = config['uri']
        self._socket = None


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
        @param  dst: object that contains a send() socket interface
        @param  msg: serializable string

        """
        self._socket.send(msg)


    def recv(self):
        """Receive a message
        @param  src: object that contains a recv() socket interface
        @rtype: str

        """
        return self._socket.recv()



def mixIn(target, mixin_class):
    if mixin_class not in target.__bases__:
        target.__bases__ = (mixin_class,) + target.__bases__
    return target



def makeNode(config):
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

    return cls(config)



class ZMQServer(ZMQNode):
    def start(self, handler):
        self._socket = _context.socket(zmq.REP)
        self._socket.bind(self._uri)
        self._handler = handler
        _loop.add_recv_handler(self._socket, self._handler)


    def __del__(self):
        if self._socket:
            _loop.del_handler(self._socket)


class ZMQClient(ZMQNode):
    def connect(self):
        self._socket = _context.socket(zmq.REQ)
        self._socket.connect(self._uri)


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
    def connect(self, handler):
        self._socket = _context.socket(zmq.SUB)
        self._socket.bind(self._uri)
        self._socket.connect(self._uri)
        self._socket.setsockopt(zmq.SUBSCRIBE, '')
        self._handler = handler
        _loop.add_recv_handler(self._socket, self._handler)


    def send(self, msg):
        raise NotImplementedError()


    def __del__(self):
        if self._socket:
            _loop.del_handler(self._socket)
