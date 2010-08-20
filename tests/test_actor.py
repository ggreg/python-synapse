#!/usr/bin/env python

import sys
import subprocess
import logging
import yaml
sys.path = ['..'] + sys.path
from synapse import node, message



actor_config1 = {
    'name': 'test1',
    'uri': 'ipc://./test1.unix'
    }


actor_config2 = {
    'name': 'test2',
    'uri': 'ipc://./test2.unix'
    }



def echo_reply_handler(self, msg):
    logging.debug('echo reply: %s' % msg)
    return msg



def forwarder_handler(self, msg):
    dstname = actor_config2['name']
    try:
        dst = self._nodes[dstname]
    except KeyError:
        uri = self._announce.where_is(dstname).uri
        logging.debug('forwarder_handler: %s is at %s' % (dstname, uri))
        self._nodes.add(dstname, uri)
        dst = self._nodes[dstname]
    finally:
        msg.src = self.name
        dst.connect()
        dst.send(self._codec.dumps(msg))
        logging.debug('forwarder_handler: message forwarded')
        reply = dst.recv()
        return self._codec.loads(reply)



def seed():
    common_config = yaml.load(file('config.yaml'))
    client = node.makeNode({'type': common_config['type'], 'uri': actor_config1['uri'], 'role': 'client'})
    codec = message.makeCodec({'type': 'jsonrpc'})
    msg = codec.dumps(message.makeMessage({'type': 'hello', 'src': 'tester', 'uri': ''}))
    client.connect()
    logging.debug('seed: connected to forwarder node')
    client.send(msg)
    logging.debug('seed: message sent')
    logging.debug('seed: message received (%s)' % client.recv())



def test_actor():
    import threading

    common_config = yaml.load(file('config.yaml'))
    actor_config1.update(common_config)
    actor_config2.update(common_config)

    announcer_process = subprocess.Popen(['./start_announcer.py'])

    a1 = node.Actor(actor_config1, forwarder_handler)
    a1.connect()
    loop1 = node._loop
    t1 = threading.Thread(target=loop1.start)

    node._loop = node.EventLoop()
    a2 = node.Actor(actor_config2, echo_reply_handler)
    a2.connect()
    loop2 = node._loop
    t2 = threading.Thread(target=loop2.start)

    if loop1 is loop2:
        print 'error same loop'
        return 1
    t1.start()
    t2.start()
