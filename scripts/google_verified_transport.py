"""Recover Google TLS EOF interruptions with verified stdlib HTTPS.

No credential files, CA settings, account permissions, or verification flags
are changed. HTTP responses stay compatible with the existing requests callers.
"""
import ssl
import urllib.request
import urllib.error
from urllib.parse import urlsplit
import requests


def tls_eof(error):
    return isinstance(error, requests.exceptions.SSLError) and any(
        text in str(error).upper() for text in ('UNEXPECTED_EOF', 'EOF OCCURRED'))


def google_url(url):
    parsed = urlsplit(url)
    return parsed.scheme == 'https' and bool(parsed.hostname) and (
        parsed.hostname == 'googleapis.com' or parsed.hostname.endswith('.googleapis.com')
        )


class SameGoogleRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not google_url(newurl) or urlsplit(req.full_url).hostname != urlsplit(newurl).hostname:
            raise requests.exceptions.RequestException('Google transport refused cross-host redirect')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class CompatibleRaw:
    def __init__(self, raw): self.raw = raw
    def read(self, amount=None):
        try: return self.raw.read(amount)
        except (OSError, ssl.SSLError) as exc:
            raise requests.exceptions.ConnectionError('Verified HTTPS response interrupted') from exc
    def close(self): self.raw.close()
    def release_conn(self): self.close()


def stdlib_request(method, url, *, headers=None, timeout=180, stream=False,
                   params=None, data=None, json=None, files=None, verify=True,
                   cert=None, **kwargs):
    if not google_url(url):
        raise requests.exceptions.RequestException('Verified fallback requires a Google HTTPS URL')
    if verify is False or cert or isinstance(verify, str):
        raise requests.exceptions.RequestException('Verified fallback does not override custom TLS settings')
    if any(kwargs.get(k) for k in ('proxies', 'auth', 'cookies')):
        raise requests.exceptions.RequestException('Verified fallback cannot change custom session settings')
    headers = dict(headers or {})
    if urlsplit(url).hostname == 'jkpark-create.github.io' and any(
            key.lower() == 'authorization' for key in headers):
        raise requests.exceptions.RequestException('Public Pages fallback must remain anonymous')
    headers['Accept-Encoding'] = 'identity'
    prepared = requests.Request(method, url, headers=headers, params=params,
                                data=data, json=json, files=files).prepare()
    body = prepared.body.encode('utf-8') if isinstance(prepared.body, str) else prepared.body
    request = urllib.request.Request(prepared.url, data=body,
                                     headers=dict(prepared.headers), method=method.upper())
    limit = max(timeout) if isinstance(timeout, tuple) else timeout
    opener = urllib.request.build_opener(SameGoogleRedirect(),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    try:
        try: raw = opener.open(request, timeout=limit)
        except urllib.error.HTTPError as response: raw = response
    except (OSError, urllib.error.URLError) as exc:
        raise requests.exceptions.ConnectionError('Verified Google HTTPS transport unavailable') from exc
    response = requests.Response()
    response.status_code = raw.code
    response.headers = requests.structures.CaseInsensitiveDict(raw.headers)
    response.url = prepared.url
    response.request = prepared
    response.raw = CompatibleRaw(raw)
    if not stream:
        try: response._content = response.raw.read()
        finally: response.raw.close()
        response._content_consumed = True
    return response


class GoogleSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.prefer_verified_stdlib = False

    def request(self, method, url, **kwargs):
        body = kwargs.get('data')
        position = body.tell() if hasattr(body, 'tell') else None
        if not self.prefer_verified_stdlib:
            try: return super().request(method, url, **kwargs)
            except requests.exceptions.SSLError as exc:
                if not tls_eof(exc) or not google_url(url): raise
                self.prefer_verified_stdlib = True
                self.close()
        if position is not None: body.seek(position)
        kwargs.setdefault('verify', self.verify)
        kwargs.setdefault('cert', self.cert)
        headers = dict(self.headers)
        headers.update(kwargs.pop('headers', {}) or {})
        return stdlib_request(method, url, headers=headers, **kwargs)
