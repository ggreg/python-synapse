#!/usr/bin/env python
# -*- coding: utf8 -*-

from unittest import TestCase

from synapse.node import poller
# default poller fail a few test
#poller._loop_again = False

class DecoratorTestCase(TestCase):

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

    
    def test_mixin(self):
        """
        FIXME: unused function: remove it ?
        """
        from synapse.node import mixIn
        
        class A(object):pass
        class B: pass
        
        mixIn(A,B)
        self.assertTrue(isinstance(A(),B))


class NodeTestCase(TestCase):

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

        nd.remove('test')
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

        

class ActorTestCase(TestCase):
    
    def test_actor(self):
        import gevent
        from synapse.message import Message
        from synapse.node import AnnounceServer, Actor 
        
        class DummyMessage(Message):
            type = 'dummy'
            def __init__(self,msg,id=None):
                Message.__init__(self, id)
                self.msg = msg
                
            @property
            def attrs(self):
                return {'msg':self.msg}

        srv_config = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test_actor_announcer',
            'announce' : {
                      'server_uri':'ipc://./test_actor_srv.unix',
                      'pubsub_uri':'ipc://./test_actor_pub.unix'
                      }
            }
        srv = AnnounceServer(srv_config)
        srv.start()

        actor_config1 = srv_config.copy()
        actor_config1.update({
                        'name': 'actor1',
                        'uri': 'ipc://./test_actor_1.unix'
                      })
        actor_config2 = srv_config.copy()
        actor_config2.update({
                        'name': 'actor2',
                        'uri': 'ipc://./test_actor_2.unix'
                      })
        
        class Handler(object):

            def __init__(self):
                self.withresponse_actor = None
                self.withresponse_msg = None
                self.noresponse_actor = None
                self.noresponse_msg = None
                
            def handler_withresponse(self,actor,msg):
                self.withresponse_actor = actor
                self.withresponse_msg = msg
                return DummyMessage("actor1 to actor2 response")
            
            def handler_noresponse(self,actor,msg):
                self.noresponse_actor = actor
                self.noresponse_msg = msg
                #will return an ack if None return here
            
        hdl = Handler()
        
        actor1 = Actor(actor_config1, hdl.handler_withresponse )
        self.assertEquals(actor1.name, 'actor1')
        actor1.connect()
        
        actor2 = Actor(actor_config2, hdl.handler_noresponse)
        actor2.connect()
        
        dummy = DummyMessage('actor1 to actor2 request')
        response = actor1.sendrecv('actor2', dummy)
        self.assertEquals(response.type,'ack')
        self.assertEquals(hdl.noresponse_msg.msg,'actor1 to actor2 request')
        self.assertEquals(hdl.noresponse_actor.name,'actor2')
        

        dummy = DummyMessage('actor2 to actor1 request')
        response = actor2.sendrecv('actor1', dummy)
        self.assertEquals(response.msg,"actor1 to actor2 response")
        self.assertEquals(response.type,'dummy')

        self.assertEquals(hdl.withresponse_actor.name,'actor1')
        self.assertEquals(hdl.withresponse_msg.msg,'actor2 to actor1 request')
        
        self.assertEquals(len(srv._nodes._nodes),2)
        actor1.__del__()
        actor2.close()
        self.assertEquals(len(srv._nodes._nodes),0)
        srv.stop()

    def test_actor_async(self):
        import gevent
        from synapse.message import Message
        from synapse.node import AnnounceServer, Actor , async
        
        class DummyMessage(Message):
            type = 'dummy'
            def __init__(self,msg,id=None):
                Message.__init__(self, id)
                self.msg = msg
                
            @property
            def attrs(self):
                return {'msg':self.msg}

        srv_config = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test_actor_announcer',
            'announce' : {
                      'server_uri':'ipc://./test_asyncactor_srv.unix',
                      'pubsub_uri':'ipc://./test_asyncactor_pub.unix'
                      }
            }
        srv = AnnounceServer(srv_config)
        srv.start()

        actor_config1 = srv_config.copy()
        actor_config1.update({
                        'name': 'actor1',
                        'uri': 'ipc://./test_asyncactor_1.unix'
                      })
        actor_config2 = srv_config.copy()
        actor_config2.update({
                        'name': 'actor2',
                        'uri': 'ipc://./test_asyncactor_2.unix'
                      })

        
        class Handler(object):

            def __init__(self):
                self.withresponse_actor = None
                self.withresponse_msg = None
                self.async_response = None
            
        hdl = Handler()

        @async
        def handler_withresponse(actor,msg):
            hdl.withresponse_actor = actor
            hdl.withresponse_msg = msg
            return DummyMessage("actor1 to actor2 response")


        def on_recv(msg):
            hdl.async_response = msg.msg
       
        
        actor1 = Actor(actor_config1, handler_withresponse )
        actor1.connect()
        
        actor2 = Actor(actor_config2, handler_withresponse)
        actor2.connect()

        # asyn method always return an ack
        dummy = DummyMessage('actor1 to actor2 request')
        response = actor1.sendrecv('actor2', dummy, on_recv)
        self.assertEquals(response.type,'ack')
        self.assertEquals(hdl.withresponse_actor.name,'actor2')

        actor2.sendrecv('actor1',DummyMessage('actor2 async response',dummy.id))
        #gevent.sleep(0.) # force run in the next eventloop cycle
        gevent.sleep(1)

        self.assertEquals(hdl.async_response,'actor2 async response')
        self.assertEquals(len(actor1._pendings), 0)
        
        self.assertEquals(len(srv._nodes._nodes),2)
        actor1.close()
        actor2.close()
        self.assertEquals(len(srv._nodes._nodes),0)
        srv.stop()


    def test_actor_classstyle(self):
        import gevent
        from synapse.message import Message, MessageCodecJSONRPC, CodecException
        from synapse.node import ( AnnounceServer, Actor, 
                                  HelloMessage, IsAtMessage, ByeMessage )
        
        class DummyMessage(Message):
            type = 'dummy'
            def __init__(self,msg,id=None):
                Message.__init__(self, id)
                self.msg = msg
                
            @property
            def attrs(self):
                return {'msg':self.msg}

        class BuggyMessage(Message):
            type = 'buggy'
            @property
            def attrs(self):
                return {}

        class UnmanagedMessage(Message):
            type = 'unmanaged'
            @property
            def attrs(self):
                return {}

        class BuggyMessageCodec(MessageCodecJSONRPC):

            def loads(self, msgstring):
                raise CodecException("loads_bug")



        srv_config = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test_actor_announcer',
            'announce' : {
                      'server_uri':'ipc://./test_actor_srv.unix',
                      'pubsub_uri':'ipc://./test_actor_pub.unix'
                      }
            }
        srv = AnnounceServer(srv_config)
        srv.start()

        actor_config1 = srv_config.copy()
        actor_config1.update({
                        'name': 'actor1',
                        'uri': 'ipc://./test_actor_1.unix'
                      })
        
        actor_config2 = srv_config.copy()
        actor_config2.update({
                        'name': 'actor2',
                        'uri': 'ipc://./test_actor_2.unix'
                      })
        
        class DummyActor(Actor):

            def __init__(self,config):
                Actor.__init__(self,config)
                self.withresponse_actor = None
                self.withresponse_msg = None
                
            def on_message_dummy(self,actor,msg):
                self.withresponse_actor = actor
                self.withresponse_msg = msg
                return DummyMessage("actor1 to actor2 response")

            def on_message_buggy(self,actor,msg):
                raise Exception("raised")

        
        actor1 = DummyActor(actor_config1)
        self.assertEquals(actor1.name, 'actor1')
        actor1.connect()
        
        actor2 = DummyActor(actor_config2)
        actor2.connect()

        dummy = DummyMessage('actor1 to actor2 request')
        response = actor1.sendrecv('actor2', dummy)
        self.assertEquals(response.type,'dummy')
        self.assertEquals(response.msg,'actor1 to actor2 response')
        self.assertEquals(actor2.withresponse_msg.msg,'actor1 to actor2 request')
        self.assertEquals(actor2.withresponse_actor.name,'actor2')

        self.assertRaises(ValueError, actor1.sendrecv,'actorundefined', dummy)

        response = actor1.sendrecv('actor1', 
                                   BuggyMessage())
        self.assertEquals(response.type,'nack')
        self.assertEquals(response.msg,'raised')
        

        response = actor1.sendrecv('actor1', 
                                   UnmanagedMessage())
        self.assertEquals(response.type,'nack')
        
        
        response = actor1.sendrecv('actor1', 
                                   HelloMessage(actor1.name,actor1._uri))
        self.assertEquals(response.type,'ack')
        response = actor1.sendrecv('actor2', 
                                   HelloMessage(actor1.name,actor1._uri))
        self.assertEquals(response.type,'ack')

        response = actor1.sendrecv('actor2', 
                                   IsAtMessage(actor1.name,actor1._uri))
        self.assertEquals(response.type,'ack')
        

        response = actor1.sendrecv('actor1', ByeMessage("actor1"))
        self.assertEquals(response.type,'ack')

        response = actor1.sendrecv('actor1', ByeMessage("actor2"))
        self.assertEquals(response.type,'ack')

        oldcodec = actor2._codec 
        actor2._codec = BuggyMessageCodec(None)
        response = actor1.sendrecv('actor2', dummy)
        self.assertEquals(response.type,'nack')
        actor2._codec = oldcodec 
        
        self.assertEquals(len(srv._nodes._nodes),2)
        actor1.close()
        actor2.close()
        self.assertEquals(len(srv._nodes._nodes),0)
        srv.stop()


class AnnouncerTestCase(TestCase):

    def test_announce(self):
        from synapse.node import AnnounceServer, AnnounceClient
            
        srv_config = {
            'type': 'zmq',
            'codec': 'jsonrpc',
            'name': 'test_announcer',
            'announce' : {
                      'server_uri':'ipc://./test_announcer_srv.unix',
                      'pubsub_uri':'ipc://./test_announcer_pub.unix'
                      }
            }
        srv = AnnounceServer(srv_config)
        srv.start()
        
        cli_config1 = srv_config.copy()
        cli_config1.update({
                        'name': 'cli1',
                        'uri': 'ipc://./test_announcer_cli1.unix'
                      })
        cli_config2 = srv_config.copy()
        cli_config2.update({
                        'name': 'cli2',
                        'uri': 'ipc://./test_announcer_cli2.unix'
                      })
        
        def announce_handler(msg): pass
        
        cli1 = AnnounceClient(cli_config1, announce_handler)
        self.assertEquals(len(cli1.nodes),2) # client + subscriber
        cli1.connect()

        cli2 = AnnounceClient(cli_config2, announce_handler)
        cli2.connect()
        
        ack = cli1.hello(cli2._client)
        self.assertEquals(ack.type,'ack')
        self.assertEquals(ack.src,'announce.server')
        
        ack = cli1.where_is('cli2.announce')
        self.assertEquals(ack.type,'is_at')
        self.assertEquals(ack.uri,srv_config['announce']['server_uri'])

        ack = cli1.where_is('unknown_node.announce')
        self.assertEquals(ack.type,'unknown_node')

        ack = cli1.bye(cli2._client)
        self.assertEquals(ack.type,'ack')
        self.assertEquals(ack.src,'announce.server')
        
        srv.stop()


class PollerTestCase(TestCase):

    def test_poller(self):
        import gevent
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
        self.assertRaises( PollerException, poller.set_periodic_handler, handler )
        # end test backward

        self.assertEquals( repr(poller), '<EventPoller pid:%d>' % poller._pid )
        
        class TestPeriod(object):
            
            def __init__(self):
                self.nb_run = 0
            
            def handle(self):
                if self.nb_run >= 2:
                    return False
                self.nb_run+=1
                return True
        tp = TestPeriod()
        poller.add_periodical_handler(tp.handle,0.1)
        gevent.sleep(0.5)
        self.assertEquals( tp.nb_run, 2)
        
        class DummyNode(object):
            
            def __init__(self):
                self.nb_run = 0

            def loop(self):
                self.nb_run += 1
        
        node = DummyNode()
        poller.register(node)
        gevent.sleep(0.5)
        self.assertEquals( node.nb_run, 1 )
        
        class Spawned(object):
            def __init__(self):
                self.nb_run = 0

            def __call__(self):
                self.nb_run += 1
                
        spawned = Spawned()
        poller.spawn(spawned)
        gevent.sleep(0.5)
        self.assertEquals( spawned .nb_run, 1 )

        
        
        @poller.__spawn_handler__
        def buggy():
            raise TypeError()
        self.assertRaises(TypeError,buggy)

    def test_waiteventpoller(self):
        import gevent
        from synapse.node import EventPoller
            
        class DummyPoller(EventPoller):
            
            def __init__(self,conf):
                EventPoller.__init__(self,conf)
                self.nb_run = 0
                
            def loop(self):
                while self._loop_again:
                    self.nb_run += 1
                    gevent.sleep(1)
            
        dp = DummyPoller({})
        gevent.sleep(0.5)
        dp._loop_again = 0
        dp.wait()
        self.assertEquals(dp.nb_run,1)
        
        


class ZMQTestCase(TestCase):
    
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
        import gevent
        from synapse.node import ZMQServer, ZMQClient
        conf_srv = {
                'uri':'tcp://*:5555',
                'name':'zmq_srv_sync',
                }

        conf_cli = {
                'uri':'tcp://localhost:5555',
                'name':'zmq_cli_sync',
                }
        
        def srv_handler(msg):
            return "sync_response"
            
        
        server = ZMQServer(conf_srv,srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        serverlet = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")
        
        response = client.recv()
        self.assertEquals(response,"sync_response")
        gevent.kill(serverlet)
        client.close()
        server.stop()


    def test_zmqserver_async(self):
        import gevent
        from synapse.node import ZMQServer, ZMQClient, async
        conf_srv = {
                'uri':'tcp://*:5556',
                'name':'zmq_srv_async',
                }

        conf_cli = {
                'uri':'tcp://localhost:5556',
                'name':'zmq_cli_async',
                }
        @async
        def srv_handler(msg):
            return "async_response"
            
        
        server = ZMQServer(conf_srv,srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        serverlet = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")
        response = client.recv()
        self.assertEquals(response,"async_response")
        gevent.sleep(1)
        gevent.kill(serverlet)
        client.close()
        server.stop()


    def test_zmqserver_exc(self):
        import gevent
        from synapse.node import ZMQServer, ZMQClient, NodeException
        conf_srv = {
                'uri':'tcp://*:5557',
                'name':'zmqsrv',
                }

        conf_cli = {
                'uri':'tcp://localhost:5557',
                'name':'zmqcli',
                }
        
        def srv_handler(msg):
            raise NodeException("buggy","exc_result")
            
        
        server = ZMQServer(conf_srv,srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError,server.send,"unimplemented")

        serverlet = gevent.spawn(server.loop)
        
        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("sync_message")
        response = client.recv()
        self.assertEquals(response,"exc_result")
        gevent.kill(serverlet)
        client.close()
        server.stop()
        
    def test_zmq_publish(self):
        import gevent
        from synapse.node import ZMQPublish, ZMQSubscribe
        conf_pub = {
                'uri':'tcp://127.0.0.1:5560',
                'name':'zmq_pub',
                }

        conf_sub = {
                'uri':'tcp://127.0.0.1:5560',
                'name':'zmq_sub',
                }
        
        class Hdl:
            def __init__(self):
                self.msg = None
                
            def sub_handler(self,msg):
                hdl.msg = msg
        
        hdl = Hdl()
        
        pub = ZMQPublish(conf_pub)
        self.assertTrue(pub.socket is None)
        pub.start()
        pub.send("puslished_message")
        
        self.assertTrue(pub.socket is not None)
        self.assertRaises(NotImplementedError,pub.recv)



        sub = ZMQSubscribe(conf_sub,hdl.sub_handler)
        self.assertTrue(sub.socket is None)
        sub.connect()
        self.assertTrue(sub.socket is not None)
        
        self.assertRaises(NotImplementedError,sub.send,"")

        subglet = gevent.spawn(sub.loop)
        gevent.sleep(1)
        pub.send("puslished_message")
        gevent.sleep(1)
        self.assertEquals(hdl.msg,"puslished_message")
        subglet.kill()

        pub.stop()
        sub.close()
        


class FactoryTestCase(TestCase):
    
    def test_makeNode(self):
        from synapse.node import makeNode
        

    def test_makePoller(self):
        from synapse.node import makePoller, EventPoller
        
        self.assertRaises(KeyError,makePoller,{"type":"not_implemented"})
        poller = makePoller({"type": "zmq"})
        self.assertTrue(isinstance(poller,EventPoller))
        
