README for python-synapse
*************************

Overview
========

Synapse is a simple Python module that provides a communication interface
through nodes. It abstracts the underlying protocol. The configuration stores
protocol specific data and a node is instanciated with the node.makeNode()
factory.

The node is the unit of communication. Actor is built on top of Node to provide
a distributed communication interface. When an actor joins the network it
announces itself to a announce service. The announce is stored in the remote
service and broadcast to all other nodes by a pub/sub queue.

Currently it supports Zeromq as the underlying protocol and JSON-RPC to encode
and decode messages.

Read doc/quickstart for a practical introduction. As documentation in doc/ is
in Sphinx format, type ``make`` in the directory to see how to build it in HTML
or PDF.


Tests
=====

.. highlight bash

launch all the tests::

    $ python setup.py test

Test a specific feature:

- Message

    $ python setup.py -s synapses.tests.test_message
    
- All Node features

    $ python setup.py -s synapses.tests.test_node

- Node zmq tests

    $ python setup.py -s synapses.tests.test_node.ZMQTestCase
    



| To test the code coverage
| ( you need nose, use last version from http://pypi.python.org/pypi/nose )
::

    $ python setup.py nosetests



