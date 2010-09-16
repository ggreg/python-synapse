#!/usr/bin/env python

import sys
sys.path = ['..'] + sys.path
import subprocess
import logging
logging.getLogger().setLevel(logging.DEBUG)
import yaml
import time

from synapse import node, message



actor_config1 = {
    'name': 'test1',
    'uri': 'ipc://./test1.unix'
    }


actor_config2 = {
    'name': 'test2',
    'uri': 'ipc://./test2.unix'
    }



def echo_reply_handler(actor, msg):
    logging.debug('echo reply: %s' % msg)
    return msg



@node.async
def echo_reply_handler_delay(actor, delay, msg):
    time.sleep(delay)
    reply = echo_reply_handler(actor, msg)

    async_msg = actor._codec.dumps(message.makeMessage(
            {'type': 'reply', 'src': actor.name, 'data': 'TEST', 'id': msg.id}))
    actor._nodes['test1'].send(async_msg)
    logging.debug('[test2] async_msg #%d sent to test1' % msg.id)
    ack = actor._nodes['test1'].recv()
    return


class ForwarderHandler(object):
    def __init__(self):
        self._dst = None


    def __call__(self, actor, msg):
        logging.debug('in forwarder handler')
        dstname = actor_config2['name']
        if not self._dst:
            self._dst = actor._nodes[dstname]

        msg.src = actor.name
        self._dst.send(actor._codec.dumps(msg))
        logging.debug('forwarder_handler: message forwarded')

        reply = self._dst.recv()

        return actor._codec.loads(reply)


@node.async
def forwarder_handler2(actor, msg):
        logging.debug('in async forwarder handler')
        dstname = actor_config2['name']
        actor.sendrecv(dstname, msg, on_recv=display_message)
        return


def display_message(msg):
    msg.src += ' (forwarded)'
    print 'MESSAGE: %s' % msg.attrs



from synapse import message


class TestMessage(message.Message):
    type = 'test'
    def __init__(self, data=None, id=None):
        message.Message.__init__(self, id)
        self.data = 'test'


    @property
    def attrs(self):
        return {
            'data': self.data
            }



def seed(msg=None):
    common_config = yaml.load(file('config.yaml'))
    client = node.makeNode({'type': common_config['type'], 'uri': actor_config1['uri'], 'role': 'client'})
    if not msg:
        codec = message.makeCodec({'type': 'jsonrpc'})
        msg = codec.dumps(message.makeMessage({'type': 'test'}))
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


def test_sendrecv():
    common_config = yaml.load(file('config.yaml'))
    actor_config1.update(common_config)
    actor_config2.update(common_config)

    announcer_process = subprocess.Popen(['./start_announcer.py'])

    a1 = node.Actor(actor_config1, forwarder_handler2)
    a1.connect()

    a2 = node.Actor(actor_config2,
            lambda a, m: echo_reply_handler_delay(a, 1, m))
    a2.connect()

    node.poller.wait()
