#!/usr/bin/env python
"""
This is an SMTP Relay server built on top of synapse.
Of course, do not use on it a production environment.
You must have a good DNS/reverse DNS configuration
to see it works
"""
import re
import sys
sys.path = ['..'] + sys.path
import subprocess
import logging
import yaml
import time

from gevent import socket
import gevent.server

import dnslib

from synapse import node, message
from synapse import redis_node


# you should always use 127.0.0.1 for this smtp server,
# in case you don't want to relay spam
config = {
    'smtp': {
        'type': 'smtp',
        'codec': 'jsonrpc',
        'name': 'smtp',
        'uri': 'smtp://127.0.0.1:8025'
        },
    'redis_pub': {
        'type': 'redis',
        'codec': 'jsonrpc',
        'name': 'mail_queue',
        'role': 'publish',
        'uri': 'redis://127.0.0.1/0?method=pubsub&channel=smtp'
        },
    'redis_sub': {
        'type': 'redis',
        'codec': 'jsonrpc',
        'name': 'smtp_relay',
        'role': 'subscribe',
        'uri': 'redis://127.0.0.1/0?method=pubsub&channel=smtp',
        'domain': '>>>WRITE YOUR DOMAIN HERE<<<'
        }
    }


node_conf = yaml.load(file('config.yaml'))
node_conf.update(config['smtp'])

redispub_conf = yaml.load(file('config.yaml'))
redispub_conf.update(config['redis_pub'])

redissub_conf = yaml.load(file('config.yaml'))
redissub_conf.update(config['redis_sub'])


class Resolver(object):
    """
    Basic DNS Resolver for MX Server
    """
    def __init__(self):
        with open('/etc/resolv.conf') as resolver:
            self.host = [line for line in resolver.readlines()
                            if line.startswith('nameserver')].pop(0) \
                                    .strip().split().pop()
            self.port = 53

    def resolveMX(self, domain):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((self.host, self.port))
        d = dnslib.DNSRecord(q=dnslib.DNSQuestion(domain, qtype=15))
        MSG = d.pack()
        if len(MSG) != s.send(MSG):
            # where to get error message "$!".
            print "cannot send to %s(%d):" % (HOSTNAME, PORTNO)
            raise SystemExit(1)
        MAXLEN = 1024
        (data, addr) = s.recvfrom(MAXLEN)
        s.close()
        smtp_srv = None
        smtp_pref = None
        data = dnslib.DNSRecord.parse(data)
        for d in data.rr:
            pref, srv = d.rdata.preference, d.rdata._mx
            if smtp_pref is None or pref < smtp_pref:
                smtp_pref = pref
                smtp_srv = str(srv)
        return smtp_srv

resolver = Resolver()


class Email(message.Message):
    type = 'email'

    def __init__(self, from_, to, data, id=None):
        message.Message.__init__(self, id)
        self.from_ = from_
        self.to = to
        self.data = data

    @property
    def attrs(self):
        return {'from_': self.from_,
                'to': self.to,
                'data': self.data}


class SMTPNode(node.Node):
    """
    Node built on top of Gevent socket
    """
    type = 'smtp'

    def __init__(self, config):
        node.Node.__init__(self, config)
        host, port = re.findall('smtp://([^:]+):([0-9]+)', config['uri'])[0]
        self._host = host if host != '*' else '0.0.0.0'
        self._port = int(port, 10)
        self._poolsize = 10000  # config['poolsize']
        self._lock = gevent.coros.Semaphore()
        self._log = logging.getLogger(self.name)


class SMTPServer(SMTPNode):

    def __init__(self, config, handler):
        SMTPNode.__init__(self, config)
        self._handler = handler

    def start(self):
        self._server = gevent.server.StreamServer((self._host, self._port),
                                                   self.handle,
                                                   self._poolsize)
        gevent.spawn(self._server.serve_forever)

    def recv(self, src):
        return self._handler(src)

    def handle(self, socket, address):
        self._log.info('New connection from %s:%s' % address)
        SMTPConnection(self, socket, address)

    def stop(self):
        self._server.kill()


class SMTPConnection(object):

    def __init__(self, server, socket_, address):

        self.name = socket.getfqdn()
        self.server = server
        self._log = server._log
        self.peer = address
        self.stream = socket_.makefile()
        self.stream.write("%s SMTP RELAY\r\n" % self.name)
        self.stream.flush()

        self.from_ = None
        self.to = []

        self.run = True
        while self.run:
            line = self.stream.readline()
            parts = line.split(None, 1)
            self._log.debug("<<<" + line)
            method = self.lookupMethod(parts[0]) or self.do_UNKNOWN
            method(parts.pop().split(':', 1).pop().rstrip('\r\n'))

        socket_.close()

    def lookupMethod(self, command):
        return getattr(self, 'do_' + command.upper(), None)

    def sendCode(self, code, message=''):
        "Send an SMTP code with a message."
        lines = message.splitlines()
        lastline = lines[-1:]
        for line in lines[:-1]:
            self._log.debug(">>> %3.3d-%s\r\n" % (code, line))
            self.stream.write('%3.3d-%s\r\n' % (code, line))

        self._log.debug('%3.3d %s\r\n' % (code,
                                    lastline and lastline[0] or ''))
        self.stream.write('%3.3d %s\r\n' % (code,
                                    lastline and lastline[0] or ''))

        self.stream.flush()

    def do_HELO(self, msg):
        self.sendCode(250, self.name)

    def do_MAIL(self, msg):
        if self.from_:
            self.sendCode(500, 'There is only sender per message')

        self.from_ = msg
        self.sendCode(250, 'Email address accepted')

    def do_RCPT(self, msg):
        self.to.append(msg)
        self.sendCode(250, 'Recipient address accepted')

    def do_RSET(self, msg):
        self.from_ = None
        self.to = []
        self.sendCode(250, 'I remember nothing')

    def do_DATA(self, msg):
        self.sendCode(354, 'Continue')

        msg = []
        line = self.stream.readline()
        while line.strip() != '.':
            msg.append(line)
            line = self.stream.readline()

        msg = ''.join(msg)
        for addr in self.to:
            m = Email(self.from_, addr, msg)
            self.server._handler(m)
        self.sendCode(220, '%d mail queued' % len(self.to))

    def do_UNKNOWN(self, msg):
        self.sendCode(500, 'Command not implemented')
        return True

    def do_QUIT(self, rest):
        self.sendCode(221, 'See you later')
        self.run = False


class IncomingActor(node.Actor):

    def __init__(self, config):
        node.Actor.__init__(self, config)
        self._queue = node.makeNode(redispub_conf)
        self._queue.start()

    def on_message(self, msg):
        self._queue.send(self._codec.dumps(msg))


class Relay(object):

    def __init__(self, domain, msg):
        self._log = logging.getLogger('relay.tcp')
        srv = resolver.resolveMX(msg.to.split('@').pop())
        socket_ = socket.create_connection((srv, 25))
        self.stream = socket_.makefile()
        try:
            handshake = self.stream.readline()
            self._log.debug('<<< %s' % handshake)
            if handshake[0] == '5':
                raise Exception('we are blacklisted')
            header_ok = True
            for h in ('helo %s' % domain,
                      'mail from: %s' % msg.from_,
                      'rcpt to: %s' % msg.to
                      ):
                ret = self.send_header(h)
                if not ret:
                    raise Exception('error in header')
            if header_ok:
                self.send_data(msg.data)
            self.write('quit')
            self.read()
        except Exception:
            import traceback
            traceback.print_exc()
        finally:
            socket_.close()

    def send_header(self, header):
        self.write(header)
        read = self.read()
        while read[3] == '-':
            read = self.read()
        return read[0] == '2'

    def send_data(self, data):
        self.write('data')
        read = self.read()
        if read[0] == '3':
            for i in data.split('\n'):
                self.write(i.rstrip('\r'))

            self.write('.')
            if read[0] == '2':
                return True
        return False

    def write(self, line):
        self._log.debug('>>> %s' % line)
        self.stream.write(line + "\r\n")
        self.stream.flush()

    def read(self):
        rv = self.stream.readline()
        self._log.debug('<<< %s' % rv)
        return rv


class RelayActor(node.Actor):

    def __init__(self, config):
        node.Actor.__init__(self, config)
        self.domain_name = config.get('domain', socket.getfqdn())

    def on_message_email(self, sender, msg):
        Relay(self.domain_name, msg)


node.registerNode('smtp', {'roles': {'server': SMTPServer}})
redis_node.registerNode()


if __name__ == '__main__':
    import logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    from gevent import spawn

    with IncomingActor(node_conf) as smtp:
        with RelayActor(redissub_conf) as sub:
            poller.wait()
