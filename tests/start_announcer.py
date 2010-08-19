#!/usr/bin/env python

import sys
sys.path = ['..'] + sys.path
import yaml

from synapse import node



if __name__ == '__main__':
    common_config = yaml.load(file('config.yaml'))
    announce_server_config = common_config
    announcer = node.AnnounceServer(announce_server_config)
    announcer.start()
    node._loop.start()
