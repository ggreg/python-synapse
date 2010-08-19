Python-synapse Quickstart
*************************

Introduction
============

Python-synapse provides a high-level :class:`Actor` class to communicate and
handle messages. An :class:`Actor` receives message in its mailbox. A handler
processes each :class:`Message`. This processing step may involve to send
messages to other actor as well as spawning other actors.

An :class:`Actor` is identified by its *name*. It is a string used to localize
the actor. The actor maintains a :class:`NodeDirectory` that maps a name to an
:class:`Actor`'s :class:`node`. A :class:`node` is a unit of communication. It
provides :meth:`send` and :meth:`recv` methods to exhange messages with other
nodes.

Start an :class:`AnnounceServer`
================================

The first step is to start an :class:`AnnounceServer`: ::

    from synapse import node
    
    ann_config = {
        'type': 'zmq', 'codec': 'jsonrpc',
        'announce': {
            'server_uri': 'ipc:///tmp/ann_server.unix',
            'pubsub_uri': 'ipc:///tmp/ann_pubsub.unix'}}
     
    ann_server = node.AnnounceServer(ann_config)

The :class:`AnnounceServer` should be run in a dedicated and monitored. I use a
simple supervisor process that forks childs and monitors then. When it catches
a SIGCHLD, it logs the process that terminated and calls a callback. The
callback may restart the process.

.. note:: the announce service is similar to DNS. Basically, DNS maps a name to
   an IP while the announce service maps a name to an URI. Though NAPTR DNS
   records may be used in our case, we prefer to not rely on another protocol
   for now.

When you imported :mod:`node` is instanciated an :class:`EventLoop` as an
object node._loop. This object manages a event loop in the process. You use it
to add event handlers. Internally :class:`AnnounceServer` adds handlers to this
loop. You start the :class:`AnnounceServer` with :meth:`AnnounceServer.start`.
However it will not handle announce messages yet. Once everything it configured
you call node._loop.start(). Then it blocks and starts to handle events: ::

    ann_server.start()
    node._loop.start()

Now the process waits for incoming messages in the :class:`AnnounceServer`
queue.


Start an :class:`Actor`
=======================

The :class:`Actor` will run in another process. First define its configuration: ::

    from synapse import node

    actor_config = {
        'name': 'test_actor',
        'type': 'zmq', 'codec': 'jsonrpc',
        'uri': 'ipc:///tmp/actor_mailbox.unix',
        'announce': {
            'server_uri': 'ipc:///tmp/ann_server.unix',
            'pubsub_uri': 'ipc:///tmp/ann_pubsub.unix'}}

Then you need a handler that will proces each message the actor receives: ::

    def actor_handler(msg):
        print msg
        return msg

It is a simple echo that replies the message it received.

Now instanciate the :class:`Actor` from the configuration and handler: ::

    actor = node.Actor(actor_config, actor_handler)

Call :meth:`Actor.connect` to connect the :class:`Actor` to the network. It
will send a hello to the announce service: ::

    actor.connect()

Now start the event loop to make actor handle messages: ::

    node._loop.start()


