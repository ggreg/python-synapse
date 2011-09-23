#!/usr/bin/env python
# -*- coding: utf8 -*-

from unittest import TestCase

from synapse.node import poller

class TestDecorators(TestCase):

    def test_async(self):
        from synapse.node import async
        @async
        def asyncfn():
            return 'asyncfn_result'

        self.assertTrue( hasattr(asyncfn, 'async') )
        self.assertTrue( asyncfn.async )
        self.assertEquals(asyncfn(), 'asyncfn_result')
        
    def test_catch_exceptions(self):
        """
        catch_exceptions catch every exceptions,
        and convert it as a NodeException if raised.
        the decorated function first parameters must be an Actor.
        """
        from synapse.message import makeCodec
        from synapse.node import catch_exceptions, NodeException
        
        class DummyActor(object):
            name = 'dummy'
            _codec = makeCodec({ 'type': 'jsonrpc' })

        class DummyException(Exception): pass

        @catch_exceptions()
        def raiser(DummyActor, raise_):
            if raise_:
                raise DummyException("dummy message")
            return 'not_raised'
        
        
        self.assertEquals(raiser(DummyActor(), False), 'not_raised')
        
        self.assertRaises(NodeException, raiser, DummyActor(), True)
        try:
            raiser(DummyActor(),True)
        except NodeException, e:
            self.assertEquals(str(e), "dummy message")


class TestNode(TestCase):

    def test_node(self):
        from synapse.node import Node
        n = Node()
        self.assertEquals(n.name,'ANONYMOUS')
        self.assertRaises(NotImplementedError, n.send, 'dst','msg')
        self.assertRaises(NotImplementedError,n.recv,'src')
        
    def test_nodedirectory(self):
        from synapse.node import NodeDirectory, ZMQClient
        
        config = {
            'type': 'zmq',
            'codec': 'jsonrpc'
            }
        nd = NodeDirectory(config)
        self.assertFalse( 'whatever' in nd)
        
        nd.add('test','ipc://./test.unix')
        self.assertTrue( 'test' in nd)
        
        node = nd['test']
        self.assertTrue(isinstance(node, ZMQClient))

        nd.remove('test','FIXME: unused_param')
        self.assertFalse( 'test' in nd)

        self.assertRaises(KeyError,nd.remove,'test')


        self.assertRaises(ValueError,nd.__getitem__,'not_found')

    def test_nodedirectory_anounced(self):
        from synapse.node import NodeDirectory, ZMQClient
        
        config = {
            'type': 'zmq',
            'codec': 'jsonrpc'
            }
        class DummyMessage(object):
            type = 'is_at'
            uri = 'ipc://./test2.unix' 

        class DummyAnnoucer(object):
            def where_is(self,name):
                return DummyMessage()

        announcer = DummyAnnoucer()

        nd = NodeDirectory(config,announcer)
        node = nd['not_found']
        self.assertTrue(isinstance(node, ZMQClient))

        


    def test_poller(self):
        from synapse.node import Poller, poller, PollerException
        p = Poller()
        self.assertRaises( NotImplementedError, p.loop )
        self.assertRaises( NotImplementedError, p.register, None )
        self.assertRaises( NotImplementedError, p.poll )
        self.assertTrue( isinstance(poller,Poller) )
        
        # backward compatibility
        def handler(): pass
        self.assertTrue( poller.timeout is None )
        self.assertTrue( poller.periodic_handler is None )
        self.assertRaises( PollerException, poller.set_periodic_handler, handler )
        
        poller.timeout = 10
        self.assertEquals( poller.timeout, 10 )
        poller.periodic_handler = handler
        self.assertEquals( poller.periodic_handler, handler )
        # end test backward
    
        
    def test_actor(self):
        config1 = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test1',
            'uri': 'ipc://./test1.unix'
            }
        config2 = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test2',
            'uri': 'ipc://./test2.unix'
            }
    
    
    def test_mixin(self):
        """
        FIXME: unused function: remove it ?
        """
        from synapse.node import mixIn
        
        class A(object):pass
        class B: pass
        
        mixIn(A,B)
        self.assertTrue(isinstance(A(),B))


class TestZMQ(TestCase):
    
    def test_zmqnode(self):
        from synapse.node import ZMQNode
        
        conf = {
                'uri':'icp://./test.unix',
                'name':'zmcnode',
                }
        
        class DummySocket(object):
            def send(self,msg):
                return "dummy_socket_send"
            def recv(self):
                return "dummy_socket_recv"
            
        n = ZMQNode(conf)
        self.assertEquals(n.name,conf['name'])
        self.assertEquals(n.uri,conf['uri'])
        self.assertEquals(repr(n),"<ZMQNode zmcnode (icp://./test.unix)>")

        dummysocket = DummySocket()
        n._socket = dummysocket
        self.assertEquals(n.socket,dummysocket)
        self.assertEquals(n.send(""),"dummy_socket_send")
        self.assertEquals(n.recv(),"dummy_socket_recv")

    def test_zmqserver_sync(self):
        import time
        import gevent
        from synapse.node import ZMQServer, ZMQClient, async, poller
        conf_srv = {
                'uri':'tcp://*:5555',
                'name':'zmq_srv_sync',
                }

        conf_cli = {
                'uri':'tcp://localhost:5555',
                'name':'zmq_cli_sync',
                }
        
        def sync_handler(msg):
            return "sync_response"
            
        
        server = ZMQServer(conf_srv,sync_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        server = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")
        resonse = client.recv()
        self.assertEquals(resonse,"sync_response")
        gevent.kill(server)

    def test_zmqserver_async(self):
        import time
        import gevent
        from synapse.node import ZMQServer, ZMQClient, async, poller
        conf_srv = {
                'uri':'tcp://*:5557',
                'name':'zmq_srv_async',
                }

        conf_cli = {
                'uri':'tcp://localhost:5557',
                'name':'zmq_cli_async',
                }
        @async
        def sync_handler(msg):
            return "async_response"
            
        
        server = ZMQServer(conf_srv,sync_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        server = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")
        resonse = client.recv()
        self.assertEquals(resonse,"async_response")
        gevent.kill(server)


    def test_zmqserver_exc(self):
        import time
        import gevent
        from synapse.node import ( ZMQServer, ZMQClient, NodeException,
                                   async, poller )
        conf_srv = {
                'uri':'tcp://*:5556',
                'name':'zmqsrv',
                }

        conf_cli = {
                'uri':'tcp://localhost:5556',
                'name':'zmqcli',
                }
        
        def sync_handler(msg):
            raise NodeException("buggy","exc_result")
            #return "sync_response"
            
        
        server = ZMQServer(conf_srv,sync_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        server = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("sync_message")
        resonse = client.recv()
        self.assertEquals(resonse,"exc_result")
        gevent.kill(server)


class TestFactory(TestCase):
    
    def test_makeNode(self):
        from synapse.node import makeNode
        

    def test_makePoller(self):
        from synapse.node import makePoller, EventPoller
        
        self.assertRaises(KeyError,makePoller,{"type":"not_implemented"})
        poller = makePoller({"type": "zmq"})
        self.assertTrue(isinstance(poller,EventPoller))
        
