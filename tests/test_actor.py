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



class ForwarderHandler(object):
    def __init__(self):
        self._dst = None


    def __call__(self, actor, msg):
        dstname = actor_config2['name']
        if not self._dst:
            self._dst = actor._nodes[dstname]

        msg.src = actor.name
        self._dst.send(actor._codec.dumps(msg))
        logging.debug('forwarder_handler: message forwarded')

        reply = self._dst.recv()

        return actor._codec.loads(reply)



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
    common_config = yaml.load(file('config.yaml'))
    actor_config1.update(common_config)
    actor_config2.update(common_config)

    announcer_process = subprocess.Popen(['./start_announcer.py'])

    a1 = node.Actor(actor_config1, ForwarderHandler())
    a1.connect()

    a2 = node.Actor(actor_config2, echo_reply_handler)
    a2.connect()
    node.poller.wait()
