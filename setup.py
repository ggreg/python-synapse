#!/usr/bin/env python
import os

#from distutils.core import setup
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
#CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()


setup(name='synapse',
      version       = '0.2',
      description   = 'Distributed communication module',
      long_description=README, # + '\n\n' +  CHANGES,
      author        = 'Greg Leclercq',
      author_email  = 'greg@0x80.net',
      url           = 'http://github.org/ggreg/python-synapse',
      packages      = ['synapse'],
      requires      = ['pyzmq (>= 2.1.1)', 'gevent_zeromq','simplejson'],
      zip_safe=False,
      test_suite='synapse'
     )
