#!/usr/bin/env python

import sys
sys.path = ['..'] + sys.path
import yaml
import gevent

from synapse import node



if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    common_config = yaml.load(file('config.yaml'))
    announce_server_config = common_config
    announcer = node.AnnounceServer(announce_server_config)
    announcer.start()
    node.poller._task.join()
