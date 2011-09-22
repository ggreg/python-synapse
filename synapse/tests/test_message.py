#!/usr/bin/env python

from unittest import TestCase

class TestJSONEncoder(TestCase):
    def _test_json_datetime(self, date):
        import json
        from synapse import message
        msg = message.makeMessage({
                'type': 'reply',
                'src': 'tester',
                'data': date,
                'id': 0})
        codec = message.makeCodec({'type': 'jsonrpc'})
        msgstring = codec.dumps(msg)
        json_msg = json.loads(msgstring)
    
        self.assertEqual(date.isoformat(), json_msg['params']['data'])
    
    
    def test_json_datetime(self):
        from datetime import datetime
        self._test_json_datetime(datetime.now())
    
    
    def test_json_time(self):
        from datetime import datetime
        self._test_json_datetime(datetime.now().time())
    
    
    def test_json_date(self):
        from datetime import datetime
        self._test_json_datetime(datetime.now().date())


    def test_json_typeerror(self):
        from synapse.message import DateTimeJSONEncoder
        codec = DateTimeJSONEncoder()
        self.assertRaises(TypeError,codec.default,"")
        self.assertRaises(TypeError,codec.default,True)
        self.assertRaises(TypeError,codec.default,1)
        self.assertRaises(TypeError,codec.default,1L)
        self.assertRaises(TypeError,codec.default,1.)



    def test_json_xmlrpclib_datetime(self):
        import xmlrpclib
        import json
        from datetime import datetime
        from synapse import message

        xmlrpc_date = xmlrpclib.DateTime()
        msg = message.makeMessage({
                'type': 'reply',
                'src': 'tester',
                'data': xmlrpc_date,
                'id': 0})
        codec = message.makeCodec({'type': 'jsonrpc'})
        msgstring = codec.dumps(msg)

        json_msg = json.loads(msgstring)
        date = datetime.strptime(xmlrpc_date.value, "%Y%m%dT%H:%M:%S")

        self.assertEqual(date.isoformat(), json_msg['params']['data'])



class TestMessage(TestCase):
    
    
    def test_makeCodec(self):
        from synapse.message import ( makeCodec, MessageCodec, Message, 
                                     MessageCodecJSONRPC, CodecException,
                                     CodecInvalidException 
                                     )
        m = MessageCodec()
        self.assertRaises(NotImplementedError, m.loads, '')
        self.assertRaises(NotImplementedError, m.dumps, {})
        
        
        codec = makeCodec({ 'type': 'jsonrpc' })
        self.assertTrue( isinstance(codec, MessageCodecJSONRPC) )
        

        class GoodMessage(Message):
            type = 'test_serialize'
            def __init__(self, dummystr, dummyint, id=None):
                Message.__init__(self, id)
                self.dummystr = dummystr
                self.dummyint = dummyint

            @property
            def attrs(self):
                return { 'dummystr': self.dummystr,
                        'dummyint': self.dummyint,
                         }

        class BadMessage:
            type = 'not_a_msg'


        m = GoodMessage("dummy",1)
        s = codec.dumps(m)
        m2 = codec.loads(s)
        self.assertTrue( isinstance(m2,GoodMessage))
        self.assertEquals( m.dummystr, m2.dummystr )
        self.assertEquals( m.dummyint, m.dummyint )
        
        class BadMessage: pass

        m = BadMessage()
        self.assertRaises(CodecException, codec.dumps, m)

        self.assertRaises(CodecException, codec.loads, "this is not a msg")

        
        self.assertRaises(CodecInvalidException, makeCodec,{'type': 'unsupported'})
        try:
            makeCodec({ 'type': 'unsupported'})
        except CodecInvalidException, e:
            self.assertEquals(str(e), "codec 'unsupported' is not supported")
        
        
    def test_makeMessage(self):
        
        from synapse.message import ( makeMessage, MessageInvalidException,
                                     Message, ReplyMessage, HelloMessage, 
                                     ByeMessage, WhereIsMessage,IsAtMessage,
                                     UnknownNodeMessage, AckMessage, 
                                     NackMessage )
        
        class BadMessage:
            type = 'not_a_msg'
        
        self.assertRaises( MessageInvalidException, makeMessage,
                          { 'type': 'not_a_msg'})
        try:
            makeMessage({ 'type': 'not_a_msg'})
        except MessageInvalidException, e:
            self.assertTrue('not_a_msg is not a subclass of Message' in str(e))


        class GoodMessage(Message):
            type = 'custom'
            def __init__(self, dummy, id=None):
                Message.__init__(self, id)
                self.dummy = dummy

            @property
            def attrs(self):
                return { 'dummy': self.dummy }

        m = makeMessage({ 'type': 'custom', 'dummy':'msg_dummy'})
        self.assertTrue( isinstance(m, GoodMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['dummy'], 'msg_dummy' )


        m = makeMessage({ 'type': 'hello', 'src':'msg_src', 'uri':'msg_uri'})
        self.assertTrue( isinstance(m, HelloMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['src'], 'msg_src' )
        self.assertEquals( m.attrs['uri'], 'msg_uri' )

        m = makeMessage({ 'type': 'reply', 'src':'msg_src', 'data':'msg_data', 'id': 8888})
        self.assertTrue( isinstance(m, ReplyMessage ))
        self.assertEquals( m.id, 8888 )
        self.assertEquals( m.attrs['src'], 'msg_src' )
        self.assertEquals( m.attrs['data'], 'msg_data' )
            
        m = makeMessage({ 'type': 'bye', 'src':'msg_src'})
        self.assertTrue( isinstance(m, ByeMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['src'], 'msg_src' )


        m = makeMessage({ 'type': 'where_is', 'name':'msg_name'})
        self.assertTrue( isinstance(m, WhereIsMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['name'], 'msg_name' )

        m = makeMessage({ 'type': 'is_at', 'name':'msg_name', 'uri': 'msg_uri'})
        self.assertTrue( isinstance(m, IsAtMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['name'], 'msg_name' )
        self.assertEquals( m.attrs['uri'], 'msg_uri' )

        m = makeMessage({ 'type': 'unknown_node', 'name':'msg_name'})
        self.assertTrue( isinstance(m, UnknownNodeMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['name'], 'msg_name' )

        m = makeMessage({ 'type': 'ack', 'src':'msg_src'})
        self.assertTrue( isinstance(m, AckMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['src'], 'msg_src' )


        m = makeMessage({ 'type': 'nack', 'src':'msg_src', 'msg':'msg_msg' })
        self.assertTrue( isinstance(m, NackMessage ))
        self.assertTrue( hasattr(m,'id'))
        self.assertEquals( m.attrs['src'], 'msg_src' )
        self.assertEquals( m.attrs['msg'], 'msg_msg' )


