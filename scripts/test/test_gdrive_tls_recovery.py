import io
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import requests
from scripts import sync_dashboard_data_to_gdrive as sync


class NativeResponse(io.BytesIO):
    status = 200
    headers = {"Content-Type": "application/json"}


class DriveTlsRecoveryTest(unittest.TestCase):
    def tearDown(self):
        sync.USE_STDLIB_TLS = False

    def test_stdlib_preserves_method_body_and_streamed_bytes(self):
        payload = b"compressed runtime data"
        with patch.object(sync.urllib.request, "urlopen", return_value=NativeResponse(payload)) as open_url:
            response = sync.stdlib_request("patch", "https://www.googleapis.com/upload", data=payload,
                                           headers={"Authorization": "Bearer fixture"}, stream=True)
            self.assertEqual(b"".join(response.iter_content(5)), payload)
            native = open_url.call_args.args[0]
            self.assertEqual(native.get_method(), "PATCH")
            self.assertEqual(native.data, payload)
            self.assertEqual(native.get_header("Accept-encoding"), "identity")
            self.assertEqual(native.get_header("Authorization"), "Bearer fixture")
            response.close()

    def test_oauth_tls_failure_switches_authenticated_drive_transport(self):
        with TemporaryDirectory() as directory:
            credentials = Path(directory)
            (credentials / "credentials.json").write_text(json.dumps({"installed": {
                "client_id": "fixture", "client_secret": "fixture"}}))
            (credentials / "token.json").write_text(json.dumps({"refresh_token": "fixture"}))
            token_response = requests.Response()
            token_response.status_code = 200
            token_response._content = b'{"access_token":"fixture-access"}'
            with patch.object(sync, "CREDS_DIR", credentials), \
                 patch.object(sync.requests, "post", side_effect=requests.exceptions.SSLError("handshake")), \
                 patch.object(sync, "stdlib_request", return_value=token_response) as native:
                self.assertEqual(sync.refresh_access_token(retries=1), "fixture-access")
                self.assertTrue(sync.USE_STDLIB_TLS)
                sync.request("fixture-access", "GET", "https://www.googleapis.com/drive/v3/files", label="list")
                self.assertEqual(native.call_args.kwargs["headers"]["Authorization"], "Bearer fixture-access")

    def test_non_tls_connection_failure_does_not_switch_transport(self):
        with patch.object(sync, "session", return_value=Mock(request=Mock(side_effect=requests.ConnectionError("offline")))), \
             patch.object(sync, "stdlib_request") as native:
            with self.assertRaises(RuntimeError):
                sync.request("fixture", "GET", "https://www.googleapis.com/drive/v3/files", label="list", retries=1)
            native.assert_not_called()


if __name__ == "__main__":
    unittest.main()
