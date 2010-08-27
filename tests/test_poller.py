#!/usr/bin/env python

import logging
import time
import sys
sys.path = ['..'] + sys.path
import yaml

from synapse import node, message



actor_config1 = {
    'name': 'test1',
    'uri': 'ipc://./test1.unix'
    }



def echo_reply_handler(self, msg):
    logging.debug('echo reply: %s' % msg)
    time.sleep(1)
    for i in xrange(100000):
        pass
    logging.info('[echo_reply_handler] time: %s' % str(time.time()))
    return msg



def periodic_handler():
    logging.info('[periodic_handler] time: %s' % str(time.time()))



def test_poller():
    common_config = yaml.load(file('config.yaml'))
    node.poller.timeout = 5
    node.poller.periodic_handler = periodic_handler
    actor_config1.update(common_config)
    actor = node.Actor(actor_config1, echo_reply_handler)
    actor.connect()
    node.poller.wait()
