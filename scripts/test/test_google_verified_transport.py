import sys,io,ssl,unittest,urllib.error
from pathlib import Path
from unittest.mock import patch,Mock
from scripts import google_verified_transport as transport
import requests
class Reply(io.BytesIO):
    code=200;headers={'Content-Type':'application/json'}
    def __init__(self,body=b'{"ok":true}'):super().__init__(body)
class TransportTests(unittest.TestCase):
    def test_eof_fallback_caches_verified_route_and_preserves_authorization(self):
        s=transport.GoogleSession();s.headers['Authorization']='Bearer test'
        with patch.object(requests.Session,'request',side_effect=requests.exceptions.SSLError('UNEXPECTED_EOF')) as primary,patch.object(transport,'stdlib_request',return_value='ok') as fallback:
            self.assertEqual(s.get('https://www.googleapis.com/drive/v3/files'),'ok')
            self.assertEqual(s.get('https://www.googleapis.com/drive/v3/files'),'ok')
        self.assertEqual(primary.call_count,1);self.assertEqual(fallback.call_count,2)
        self.assertEqual(fallback.call_args.kwargs['headers']['Authorization'],'Bearer test')
    def test_certificate_error_is_not_fallback_or_silenced(self):
        with patch.object(requests.Session,'request',side_effect=requests.exceptions.SSLError('CERTIFICATE_VERIFY_FAILED')),patch.object(transport,'stdlib_request') as fallback:
            with self.assertRaises(requests.exceptions.SSLError):transport.GoogleSession().get('https://www.googleapis.com/x')
        fallback.assert_not_called()
    def test_fallback_rewinds_stream_to_original_offset(self):
        body=io.BytesIO(b'prefixDATA');body.seek(6)
        def interrupted(*a,**k):body.read();raise requests.exceptions.SSLError('EOF occurred')
        def fallback(*a,**k):return k['data'].read()
        with patch.object(requests.Session,'request',side_effect=interrupted),patch.object(transport,'stdlib_request',side_effect=fallback):
            self.assertEqual(transport.GoogleSession().put('https://www.googleapis.com/upload',data=body),b'DATA')
    def test_verified_https_and_no_automatic_response_compression(self):
        raw=Reply();opener=Mock();opener.open.return_value=raw
        with patch.object(transport.urllib.request,'build_opener',return_value=opener),patch.object(transport.ssl,'create_default_context',wraps=ssl.create_default_context) as context:
            r=transport.stdlib_request('GET','https://www.googleapis.com/x',params={'fields':'id'})
        self.assertEqual(r.json(),{'ok':True});context.assert_called_once_with()
        request=opener.open.call_args.args[0]
        self.assertIn('fields=id',request.full_url);self.assertEqual(request.get_header('Accept-encoding'),'identity')
    def test_stream_is_requests_compatible_and_closes(self):
        raw=Reply(b'abcdefgh');opener=Mock();opener.open.return_value=raw
        with patch.object(transport.urllib.request,'build_opener',return_value=opener):r=transport.stdlib_request('GET','https://www.googleapis.com/x',stream=True)
        self.assertEqual(b''.join(r.iter_content(chunk_size=3)),b'abcdefgh');r.close();self.assertTrue(raw.closed)
    def test_real_oauth_form_post_body_is_bytes(self):
        opener=Mock();opener.open.return_value=Reply(b'{"access_token":"test"}')
        with patch.object(transport.urllib.request,'build_opener',return_value=opener):
            response=transport.stdlib_request('POST','https://oauth2.googleapis.com/token',data={'grant_type':'refresh_token','refresh_token':'test value'})
        request=opener.open.call_args.args[0]
        self.assertIsInstance(request.data,bytes)
        self.assertIn(b'refresh_token=test+value',request.data)
        self.assertEqual(response.json()['access_token'],'test')
    def test_http_error_remains_http_error_for_existing_retry_policy(self):
        e=urllib.error.HTTPError('https://www.googleapis.com/x',403,'Denied',{},Reply(b'{"error":"denied"}'));opener=Mock();opener.open.side_effect=e
        with patch.object(transport.urllib.request,'build_opener',return_value=opener):r=transport.stdlib_request('GET','https://www.googleapis.com/x')
        self.assertEqual(r.status_code,403)
        with self.assertRaises(requests.HTTPError):r.raise_for_status()
    def test_non_google_or_disabled_verification_is_refused(self):
        for url,kw in [('http://www.googleapis.com/x',{}),('https://evil.test',{}),('https://www.googleapis.com/x',{'verify':False})]:
            with self.assertRaises(requests.RequestException):transport.stdlib_request('GET',url,**kw)
    def test_public_pages_not_permitted_in_google_transport(self):
        with self.assertRaises(requests.RequestException):transport.stdlib_request('GET','https://jkpark-create.github.io/kmtc-weekly-bsa-review/protected-assets.json')
