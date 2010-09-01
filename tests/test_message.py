import sys
sys.path = ['..'] + sys.path
import unittest
import datetime
import simplejson as json

from synapse import message



class TestJSONEncoder(unittest.TestCase):
    def _test_json_datetime(self, date):
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
        self._test_json_datetime(datetime.datetime.now())
    
    
    def test_json_time(self):
        self._test_json_datetime(datetime.datetime.now().time())
    
    
    def test_json_date(self):
        self._test_json_datetime(datetime.datetime.now().date())


    def test_json_xmlrpclib_datetime(self):
        import xmlrpclib

        xmlrpc_date = xmlrpclib.DateTime()
        msg = message.makeMessage({
                'type': 'reply',
                'src': 'tester',
                'data': xmlrpc_date,
                'id': 0})
        codec = message.makeCodec({'type': 'jsonrpc'})
        msgstring = codec.dumps(msg)
        json_msg = json.loads(msgstring)
        date = datetime.datetime.strptime(xmlrpc_date.value, "%Y%m%dT%H:%M:%S")

        self.assertEqual(date.isoformat(), json_msg['params']['data'])


if __name__ == '__main__':
    unittest.main()
