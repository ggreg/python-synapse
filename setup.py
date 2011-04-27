#!/usr/bin/env python

from distutils.core import setup

setup(name='synapse',
      version       = '0.2',
      description   = 'Distributed communication module',
      author        = 'Greg Leclercq',
      author_email  = 'greg@0x80.net',
      url           = 'http://github.org/ggreg/python-synapse',
      packages      = ['synapse'],
      requires      = ['pyzmq (>= 2.1.1)', 'gevent_zeromq']
     )
