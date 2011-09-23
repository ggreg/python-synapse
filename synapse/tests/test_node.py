#!/usr/bin/env python
# -*- coding: utf8 -*-

from unittest import TestCase



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
        from synapse.node import NodeDirectory
        
        config = {
            'type': 'zmq',
            'codec': 'jsonrpc'
            }
        nd = NodeDirectory(config)
        self.assertFalse( 'whatever' in nd)
        
        nd.add('test','ipc://./test.unix')
        self.assertTrue( 'test' in nd)
        nd.remove('test','FIXME: unused_param')
        self.assertFalse( 'test' in nd)

        self.assertRaises(KeyError,nd.remove,'test')

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
    