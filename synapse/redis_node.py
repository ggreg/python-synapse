import re
import logging


import gevent

from redis import Redis

from .node import Node, registerNode


"""
pip install git+https://github.com/mardiros/redis-py.git

"""

parse_uri_re = re.compile(
    '''redis://
    (?:(?:[^:]+:)?(?P<passwd>:[^@]+)@)?  # password
    (?P<host>[^/:\\n]+)     # server
    (?::(?P<port>[0-9]+))?  # port
    (?:/(?P<db>[^\\n\\?]+))? # database
    (?:\\?(?P<params>[^\\n]+))?''',
    re.VERBOSE)


def parse_uri(uri):
    g = parse_uri_re.match(uri)
    password = g.group('passwd')
    host = g.group('host')
    port = int(g.group('port') or 6379)
    db = int(g.group('db') or 0)
    params = g.group('params' or '')
    
    rv = { 'host': host, 'port': port, 'db': db, 
            'password': password, 'gevent_socket': True }

    if params:
        rv.update(dict([ p.split("=",1) for p in params.split('&') ]))
    
    return rv

class RedisNode(Node):
    """Node built on top of Redis.
    """
    type = 'redis'

    def __init__(self, config):
        self._name = config.get('name', 'ANONYMOUS')
        self._uri = config['uri']
        self._client = None
        self._lock = gevent.coros.Semaphore()
        self._log = logging.getLogger(self.name)

    @property
    def name(self):
        return self._name

    @property
    def uri(self):
        return self._uri

    def __repr__(self):
        return "<%s %s (%s)>" % (self.__class__.__name__,
                                 self._name,
                                 self._uri)

    def send(self, msg):
        raise NotImplementedError()

    def recv(self):
        raise NotImplementedError()

class RedisPublisher(RedisNode):
    
    def start(self):
        conf = parse_uri(self._uri)
        self._channel = conf.pop('channel',self._name)
        self._method = conf.pop('method', 'pubsub')
        self._client = Redis(**conf)

    def stop(self):
        self._client.connection_pool.disconnect()

    def send(self, msg):
        """
        Send a message
        :param  dst: object that contains a send() socket interface
        :param  msg: serializable string

        """
        self._lock.acquire()
        if self._method == 'pubsub':
            ret = self._client.publish(self._channel,msg)
            if ret < 1:
                self._log.error('No subscriber receive this message ')
        else:
            ret = self._client.rpush(self._channel,msg)
        self._lock.release()
        return ret



class RedisSubscriber(RedisNode):
    def __init__(self, config, handler):
        RedisNode.__init__(self, config)
        self._handler = handler


    def connect(self):
        conf = parse_uri(self._uri)
        self._channel = conf.pop('channel',self._name)
        self._method = conf.pop('method', 'pubsub')
        self._client = Redis(**conf)

        if self._method == 'pubsub':
            self._pubsub = self._client.pubsub()
            self._pubsub.subscribe(self._channel)

    def loop(self):
        while True:
            self._log.debug('in subscriber loop')
            raw_request = self.recv()
            self._handler(raw_request)

    def recv(self):
        """
        Return a message as a string from the receiving queue.

        """
        self._log.debug('waiting in recv()')
        self._lock.acquire()
        if self._method == 'pubsub':
            msg = self._pubsub.listen().next()['data']
        else:
            msg = self._client.blpop(self._channel)[1]
            
            
        self._log.debug('redisclient: %s' % self._name)
        self._log.debug('recv -> %s' % msg)
        self._lock.release()
        return msg

    def close(self):
        if self._method == 'pubsub':
            self._pubsub.unsubscribe(self._channel)
        self._client.connection_pool.disconnect()



def regiserNode():
    registerNode('redis', {
                    'roles': {
                        'publish':  RedisPublisher,
                        'subscribe':RedisSubscriber
                    }
                })

