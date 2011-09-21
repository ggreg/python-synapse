#!/usr/bin/env python

from unittest import TestCase

class TestMessage(TestCase):
    
    
    def test_makeCodec(self):
        from synapse.message import ( makeCodec, MessageCodecJSONRPC, 
                                     CodecInvalidException 
                                     )
        codec = makeCodec({ 'type': 'jsonrpc' })
        self.assertTrue( isinstance(codec, MessageCodecJSONRPC) )
        
        self.assertRaises(CodecInvalidException, makeCodec,{'type': 'xmlrpc'})
        
        
    def test_makeMessage(self):
        
        from synapse.message import ( makeMessage, MessageInvalidException )
        
        class BadMessage:
            type = 'dummy'
            
        self.assertRaises( MessageInvalidException, makeMessage,
                          { 'type': 'dummy'})
