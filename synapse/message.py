import random
import simplejson as json



class Message(object):
    def __init__(self, id=None):
        self._id = id


    @property
    def id(self):
        if not self._id:
            rand_min = 0
            rand_max = 2**32

            self._id = random.randint(rand_min, rand_max)
        return self._id



class ReplyMessage(Message):
    type = 'reply'
    def __init__(self, src, data, id):
        Message.__init__(self, id)
        self.src = src
        self.data = data


    @property
    def attrs(self):
        return {
            'src': self.src,
            'data': self.data}



class HelloMessage(Message):
    type = 'hello'
    def __init__(self, src, uri, id=None):
        Message.__init__(self, id)
        self.src = src
        self.uri = uri


    @property
    def attrs(self):
        return {
            'src': self.src,
            'uri': self.uri}



class ByeMessage(Message):
    type = 'bye'
    def __init__(self, src, id=None):
        Message.__init__(self, id)
        self.src = src


    @property
    def attrs(self):
        return {
            'src': self.src}



class WhereIsMessage(Message):
    type = 'where_is'
    def __init__(self, name, id=None):
        Message.__init__(self, id)
        self.name = name


    @property
    def attrs(self):
        return {
            'name': self.name}



class IsAtMessage(Message):
    type = 'is_at'
    def __init__(self, name, uri, id=None):
        Message.__init__(self, id)
        self.name = name
        self.uri = uri


    @property
    def attrs(self):
        return {
            'name': self.name,
            'uri': self.uri}



class AckMessage(Message):
    type = 'ack'
    def __init__(self, src, id=None):
        Message.__init__(self, id)
        self.src = src


    @property
    def attrs(self):
        return {
            'src': self.src}



class NackMessage(Message):
    type = 'nack'
    def __init__(self, src, msg, id=None):
        Message.__init__(self, id)
        self.src = src
        self.msg = msg

    @property
    def attrs(self):
        return {
            'src': self.src,
            'msg': self.msg}

class DispatchMessage(Message):
    """Receiving this message says that the sender want to
    call a specific method of the :class:`Actor` object.
    """
    type = 'dispatch'
    def __init__(self, method, msg, id=None):
        Message.__init__(self, id)
        self.method = method
        self.msg = msg

    @property
    def attrs(self):
        return {
            'method': self.method,
            'msg': self.msg}

class MessageCodec(object):
    def loads(self, msgstring):
        raise NotImplementedError()


    def dumps(self, msg):
        raise NotImplementedError()

class DateTimeJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time types
    Taken from django (http://code.djangoproject.com/browser/django/trunk/django/core/serializers/json.py)
    Modified to handle xmlrpclib.DateTime objects

    """

    DATE_FORMAT = "%Y-%m-%d"
    TIME_FORMAT = "%H:%M:%S"

    def default(self, o):
        import datetime
        try:
            import xmlrpclib
            if isinstance(o, xmlrpclib.DateTime):
                converted = datetime.datetime.strptime(o.value, "%s %s.%%f" % (self.DATE_FORMAT, self.TIME_FORMAT))
                return converted.strftime("%s %s" % (self.DATE_FORMAT, self.TIME_FORMAT))
        except Exception, e:
            pass
        if isinstance(o, datetime.datetime):
            return o.strftime("%s %s" % (self.DATE_FORMAT, self.TIME_FORMAT))
        elif isinstance(o, datetime.date):
            return o.strftime(self.DATE_FORMAT)
        elif isinstance(o, datetime.time):
            return o.strftime(self.TIME_FORMAT)
        else:
            return super(self.__class__, self).default(o)

class MessageCodecJSONRPC(MessageCodec):
    def __init__(self, config):
        pass


    def loads(self, msgstring):
        jsonrpc_msg = json.loads(msgstring)
        msgtype = jsonrpc_msg['method']
        msgattrs = jsonrpc_msg['params']
        msg_dict = {'type': msgtype}
        msg_dict.update(msgattrs)
        msg_dict['id'] = jsonrpc_msg['id']
        msg = makeMessage(msg_dict)
        return msg


    def dumps(self, msg):
        jsonrpc_msg = {
            'method': msg.type,
            'params': msg.attrs,
            'id': msg.id}

        return json.dumps(jsonrpc_msg, cls=DateTimeJSONEncoder)



def makeCodec(config):
    dispatch = {
        'jsonrpc': MessageCodecJSONRPC,
    }
    return dispatch[config['type']](config)



def makeMessage(msg):
    msgtmp = msg.copy()
    subclasses = Message.__subclasses__()
    dispatch = dict((cls.type, cls)for cls in subclasses)
    msgtype = msgtmp['type']
    del msgtmp['type']
    return dispatch[msgtype](**msgtmp)
