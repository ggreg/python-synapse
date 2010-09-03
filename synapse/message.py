import random
import simplejson as json



class MessageException(Exception):
    pass



class MessageInvalidException(MessageException):
    def __init__(self, msg):
        self._msg = msg


    def __str__(self):
        return 'message invalid : "%s"' % str(self._msg)



class CodecException(Exception):
    pass



class CodecInvalidException(CodecException):
    def __init__(self, key):
        self._key = key


    def __str__(self):
        return 'codec "%s" is not supported' % self._key



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



class MessageCodec(object):
    def loads(self, msgstring):
        raise NotImplementedError()


    def dumps(self, msg):
        raise NotImplementedError()



class DateTimeJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that encodes objects:

    - datetime.datetime
    - datetime.date
    - datetime.time
    - xmlrpclib.DateTime

    """
    def default(self, obj):
        import datetime
        import xmlrpclib
        if isinstance(obj, datetime.datetime) or \
                isinstance(obj, datetime.date) or \
                isinstance(obj, datetime.time):
            return obj.isoformat()
        elif isinstance(obj, xmlrpclib.DateTime):
            return datetime.datetime.strptime(obj.value, "%Y-%m-%d %H:%M:%S.%f")
        else:
            return super(self.__class__, self).default(obj)



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
