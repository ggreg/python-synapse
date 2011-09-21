"""Will test that a totally isolated greenlet performing IO can life along
synapse

Node1 and Node2 will do ping-pong calls
Node1 will send a message to Node2 each 5s, Node2 will do the same with Node1

A WSGIServer is launch on 8088, and use gevent to wait 10s before replying to
test that everything is still working

To make the test works, you have to start in another shell the
start_announcer.py.
If the test works, it will constantly print, as both node do "ping pong" with
each other
It will also be able to serve page (/) on 8088 (but waits 10s before sending
the response)

"""

import yaml
from synapse import node, message

from gevent.wsgi import WSGIServer
import gevent

node1_conf = {
    'name': 'node1',
    'uri': 'ipc://./node1.unix'
}

node2_conf = {
    'name': 'node2',
    'uri': 'ipc://./node2.unix'
}

class Node1(node.Actor):
    def __init__(self, config):
        super(Node1, self).__init__(config)
        node.poller.add_periodical_handler(self.periodical, 5)

    def periodical(self):
        try:
            logging.info('sendrecv')
            ret = self.sendrecv('node2', message.AckMessage(self.name))
        except Exception, err:
            self._log.error('%s: %s' % (err.__class__.__name__, err))

    @node.async
    def on_message_ack(self, actor, request):
        self._log.info("received a AckMessage")

class Node2(node.Actor):
    def __init__(self, config):
        super(Node2, self).__init__(config)
        node.poller.add_periodical_handler(self.periodical, 3)

    def periodical(self):
        try:
            self.sendrecv('node1', message.AckMessage(self.name))
        except Exception, err:
            self._log.error('%s: %s' % (err.__class__.__name__, err))

    @node.async
    def on_message_ack(self, actor, request):
        self._log.info("received a AckMessage, replying a NackMessage")
        return message.NackMessage(self.name, self.name)

def create_wsgiserver():
    # from https://bitbucket.org/denis/gevent/src/26f61c2d72f8/examples/wsgiserver.py
    def hello_world(env, start_response):
        if env['PATH_INFO'] == '/':
            start_response('200 OK', [('Content-Type', 'text/html')])
            gevent.sleep(10)
            return ["<b>hello world</b>"]
        else:
            start_response('404 Not Found', [('Content-Type', 'text/html')])
            return ['<h1>Not Found</h1>']
    logging.info('starting WSGIServer on 8088')
    WSGIServer(('', 8088), hello_world).serve_forever()


if __name__ == '__main__':
    import logging
    from logging import StreamHandler
    from gevent import spawn
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    stream = StreamHandler()
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    common_config = yaml.load(file('config.yaml'))
    node1_conf.update(common_config)
    node2_conf.update(common_config)
    logging.info('starting node 1')
    node1 = Node1(node1_conf)
    node1.connect()
    logging.info('starting node 2')
    node2 = Node2(node2_conf)
    node2.connect()
    create_wsgiserver()
