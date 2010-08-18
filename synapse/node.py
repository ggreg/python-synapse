"""
Provides Node and Actor.

All objects share the same global _context and _loop. Non-blocking and
asynchronous events handler are registered in the global _loop. The event loop
allows to handle multiple requests in a single requests.

"""
import zmq

from message import makeMessage, makeCodec, \
                    HelloMessage, ByeMessage, WhereIsMessage



class EventLoop(object):
    def __init__(self, run_in_thread=False):
        from zmq.eventloop import ioloop
        self._loop = ioloop.IOLoop.instance()
        self._is_started = False
        self._thread = None
        if run_in_thread:
            import threading
            self._thread = threading.Thread(target=self._loop.start)


    def add_recv_handler(self, socket, handler):
        self._loop.add_handler(socket, handler, zmq.POLLIN)


    def del_handler(self, handler):
        self._loop.remove_handler(handler)


    def start(self):
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

    """
    def __init__(self, config):
        self._uri = config['uri']
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._mailbox = makeNode({
                'type': config['type'],
                'uri':  self._uri,
                'role': 'server'
                })
        self._announce = AnnounceClient(config)
        self.nodes = NodeDirectory()


    def connect(self):
        self._mailbox.connect(on_message)
        self._announce.connect()


    def sendrecv(self, node_name, msg):
        remote = self.nodes[node_name]
        remote.send(msg)
        reply = remote.recv(msg)


    def on_message(self, socket, events):
        msgstring = socket.recv()
        request = self._codec(msgstring)
        raise NotImplementedError()


    def on_announce(self, msg):
        if msg['type'] == 'hello':
            self.nodes.add(msg['src'], msg['uri'])
        if msg['type'] == 'bye':
            self.nodes.remove(msg['src'], msg['uri'])



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
                'type': config['type'],
                'uri':  config['uri'],
                'role': 'server'
                })
        self._publisher = makeNode({
                'type': config['type'],
                'uri':  config['announce']['uri'],
                'role': 'publish'
                })
        self._nodes = {}


    def start(self):
        self._publisher.start()
        self._server.start(self.handle_message)


    def handle_message(self, socket, events):
        msgstring = socket.recv()
        msg = self._codec.loads(msgstring)
        print msg.type
        socket.send('ack')



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
                'uri':  config['announce']['uri'],
                'role': 'subscribe',
                })
        self._client = makeNode({
                'type': config['type'],
                'uri':  config['uri'],
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
        self._uri = config['uri']
        self._socket = None


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
