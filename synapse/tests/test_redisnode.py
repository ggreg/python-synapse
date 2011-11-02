#!/usr/bin/env python
# -*- coding: utf8 -*-

from unittest import TestCase


class RedisTestCase(TestCase):

    def test_registerNode(self):
        from synapse.redis_node import registerNode
        from synapse import node
        registerNode()
        self.assertTrue('redis' in node.node_registry)

    def test_redis_queue(self):
        import gevent
        from synapse.redis_node import RedisPublisher, RedisSubscriber
        conf_pub = {'uri': 'redis://127.0.0.1/0?method=queue&channel=test',
                    'name': 'redis_pub'}

        conf_sub = {'uri': 'redis://127.0.0.1/0?method=queue&channel=test',
                    'name': 'redis_sub'}

        class Hdl(object):

            def __init__(self):
                self.msg = []

            def sub_handler(self, msg):
                hdl.msg.append(msg)

        hdl = Hdl()

        pub = RedisPublisher(conf_pub)
        pub.start()
        pub.send("puslished_message 0")

        sub = RedisSubscriber(conf_sub, hdl.sub_handler)
        sub.connect()

        self.assertRaises(NotImplementedError, pub.recv)

        self.assertEquals(sub.uri, pub.uri)
        self.assertEquals(repr(sub), '<RedisSubscriber redis_sub '\
                          '(redis://127.0.0.1/0?method=queue&channel=test)>')
        self.assertEquals(repr(pub), '<RedisPublisher redis_pub '\
                          '(redis://127.0.0.1/0?method=queue&channel=test)>')

        self.assertRaises(NotImplementedError, sub.send, "")

        subglet = gevent.spawn(sub.loop)
        pub.send("puslished_message 1")
        gevent.sleep(1)
        self.assertEquals(len(hdl.msg), 2)
        self.assertEquals(hdl.msg.pop(), "puslished_message 1")
        self.assertEquals(hdl.msg.pop(), "puslished_message 0")
        subglet.kill()

        pub.stop()
        sub.close()

    def test_redis_pubsub(self):
        import gevent
        from synapse.redis_node import RedisPublisher, RedisSubscriber
        conf_pub = {'uri': 'redis://127.0.0.1/0?method=pubsub&channel=test',
                    'name': 'redis_pub'}

        conf_sub = {'uri': 'redis://127.0.0.1/0?method=pubsub&channel=test',
                    'name': 'redis_sub'}

        class Hdl(object):
            def __init__(self):
                self.msg = None

            def sub_handler(self, msg):
                hdl.msg = msg

        hdl = Hdl()

        pub = RedisPublisher(conf_pub)
        pub.start()
        pub.send("puslished_message 0")
        #pub.send("puslished_message 1")

        sub = RedisSubscriber(conf_sub, hdl.sub_handler)
        sub.connect()

        self.assertRaises(NotImplementedError, pub.recv)

        self.assertEquals(sub.uri, pub.uri)
        self.assertEquals(repr(sub), '<RedisSubscriber redis_sub '\
                          '(redis://127.0.0.1/0?method=pubsub&channel=test)>')
        self.assertEquals(repr(pub), '<RedisPublisher redis_pub '\
                          '(redis://127.0.0.1/0?method=pubsub&channel=test)>')

        self.assertRaises(NotImplementedError, sub.send, "")

        subglet = gevent.spawn(sub.loop)
        pub.send("puslished_message 1")
        self.assertEquals(hdl.msg, "puslished_message 1")
        subglet.kill()

        pub.stop()
        sub.close()
