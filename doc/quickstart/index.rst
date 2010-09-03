Quickstart
**********

Introduction
============

Python-synapse provides modules to communicate between lightweight processes
with asynchronous messages.

The high-level :class:`Actor` class encapsulates computations and message
handling. An :class:`Actor` receives messages in its mailbox. A handler
processes each :class:`Message`. This processing step may involve to send
messages to other actor as well as spawning other actors.

An :class:`Actor` is identified by its *name*. It is a string used to localize
the actor. The actor maintains a :class:`NodeDirectory` that maps a name to an
:class:`Actor`'s :class:`node`. A :class:`node` is a unit of communication. It
provides :meth:`send` and :meth:`recv` methods to exchange messages with other
nodes.

The :class:`NodeDirectory` resolves a name by sending requests to an
:class:`AnnounceServer`. Though it is not mandatory, we recommend to start an
:class:`AnnounceServer` to dynamically locate nodes as they join the network.
It allows to change the topology without restarting the actors.


Start an :class:`AnnounceServer`
================================

To start an :class:`AnnounceServer`, instanciate an object and call
:meth:`AnnounceServer.start`. :class:`AnnounceServer` takes a configuration
mapping as its first argument. The configuration is commonly a dict created
from a file. The example below shows the required keys: ::

    from synapse import node

    ann_config = {
        'type': 'zmq',
        'codec': 'jsonrpc',
        'announce': {
            'server_uri': 'ipc:///tmp/ann_server.unix',
            'pubsub_uri': 'ipc:///tmp/ann_pubsub.unix'}}

- *type*: transport protocol. 'zmq' means ZeroMQ
- *codec*: codec (coder/decoder) for messages. 'jsonrpc' is self-explaining and
  message JSON-RPC
- *announce*: parameters specific to the announce service
   
  - *server_uri*: URI of the announce server. An actor queries the server to
    register itself or resolve a name.
  - *pubsub_uri*: URI of the publish and subscribe service. An actor subscribes
    to this service to receive events from other actors. It commonly updates
    its :class:`NodeDirectory` when it gets :class:`message.HelloMessage` or
    :class:`message.ByeMessage`.


Then instanciate the server and start it: ::

    ann_server = node.AnnounceServer(ann_config)
    ann_server.start()


The :class:`AnnounceServer` should be run in a dedicated and monitored process.
I use a simple supervisor process that forks children and monitors then. When it
catches a SIGCHLD, it logs the process that terminated and calls a callback.
The callback may restart the process.

.. note:: the announce service is similar to DNS. Basically, DNS maps a name to
   an IP while the announce service maps a name to an URI. Though NAPTR DNS
   records may be used in our case, we prefer to not rely on another protocol
   for now.

Now the process waits for incoming messages in the :class:`AnnounceServer`
queue.


Start an :class:`Actor`
=======================

The :class:`Actor` will run in another process. First define its configuration: ::

    from synapse import node

    actor_config = {
        'name': 'test_actor',
        'type': 'zmq',
        'codec': 'jsonrpc',
        'uri': 'ipc:///tmp/actor_mailbox.unix',
        'announce': {
            'server_uri': 'ipc:///tmp/ann_server.unix',
            'pubsub_uri': 'ipc:///tmp/ann_pubsub.unix'}}


Two keys differ from the announce server configuration: *name* and *uri*.
*name* defines the of the actor's mailbox name. Before an actor sends a
message to another actors, it connects to its mailbox. Connecting to the
mailbox requires to know the network address which is the *uri*.

You instanciate an :class:`Actor` from a configuration and an optional handler.
What is the handler's role?

When an :class:`Actor` receives a message in its mailbox, it dispatches it to a
function. By defaut, it tries to dispatch the message with respect to the
message type. Let consider we want to handle a EchoMessage.
:attr:`EchoMessage.type` is 'echo_request'. Write a :class:`EchoActor` that
inherits from :class:`Actor` and defines a method
:meth:`on_message_echo_request`: ::

    class EchoActor(node.Actor):
        def on_message_echo_request(self, msg):
            return msg

It just returns the same message to the sender. When :class:`EchoActor`
receives a 'echo_reply' message, it handles it with
:meth:`EchoActor.on_message_echo_request`. What would happen if you did not
define this method? In this case, the actor fails to dispatch the message to an
ad hoc method. Then it passes the message to its handler. If there is no
handler, it replies an error to the sender to tell it does not support this
messsage type. A handler is a callable. We can simply write a handler that does
the same as :meth:`EchoActor.on_message_echo_request`: ::

    def echo_request_handler(actor, msg):
        return msg

    echo = EchoActor(actor_config, echo_request_handler)

Take care to not define :meth:`EchoActor.on_message_echo_request`, because it
will handle the 'echo_request' message instead of the
:func:`echo_request_handler`.

Now the actor is ready to join the network! Call :meth:`Actor.connect`: ::

    actor.connect()


The actor registers itself to the announce server. The announce server publish
the hello to all other actors. Finally the actor waits for message to handle.

However it will not work yet. You need to enter in the poller's loop: ::

    node.poller.wait()

And here you wonder: what is the poller?

Lightweight process management in Python-synapse
================================================

Python-synapse relies on greenlets to manage parallel execution in the same
process. At the startup, the process owns the following greenlets:

- poller
- actor.mailbox
- actor.announce

The poller is used to wake sleeping greenlets. Let describe the basic timeline
of a process with two actors A1 and A2.

First we instanciate and connect A1, then A2. When A1 connects to the network
it:

- starts to listen to its mailbox
- registers to the poller
- connects to the announce service
- registers its announce client to the poller
- registers its announce subscriber to the poller
- sends a hello to the announce server

After sending the *hello*, it waits the reply from the announce server. At this
moment, the greenlet blocks and becomes asleep. The execution is tranfered to
the next greenlet which holds A2 code.

Finally it ends in the poller greenlet. The poller blocks until it receives a
new event. When it receives the reply from the announce server to A1 announce
client, it wakes A1.announce. Then the execution is transfered to the next
greenlet. In this case, it is A1.mailbox. A1.mailbox is still asleep. The
execution is transfered again to the next greenlet: A1.announce. As it is
awake, it handles the message.

The same flow applies to A2.

Finally all greenlets are asleep and the execution is blocking in the poller
greenlet.
