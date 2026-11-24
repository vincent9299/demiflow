"""Loopback servers for the exchange tests: every request is recorded as the server received it.

Servers: A (main), B (second origin) and P1/P2 (forward proxies for plain http).
"""
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

RECORDS = []
LOCK = threading.Lock()


def answer(tag, method, raw_path, headers, body):
    """(status, header pairs, body bytes) for one request."""
    split = urlsplit(raw_path)
    path, query = split.path, parse_qs(split.query)
    if path == '/echo':
        return 200, [('Content-Type', 'text/plain')], ('echo:' + tag + ':' + method + ':' + raw_path).encode()
    if path == '/setcookie':
        return 200, [('Set-Cookie', 'units=metric; Path=/')], b'cookie-set'
    if path == '/twocookies':
        return 200, [('Set-Cookie', 'first=1; Path=/'), ('Set-Cookie', 'second=two; Path=/')], b'two-cookies'
    match = re.fullmatch(r'/redirect/(\d+)/(\d+)', path)
    if match:
        code, left = int(match.group(1)), int(match.group(2))
        if left > 0:
            return code, [('Location', f'/redirect/{code}/{left - 1}')], b'moved'
        return 200, [], ('final:' + method + ':' + str(len(body))).encode()
    if path == '/cross':
        return 302, [('Location', query['to'][0])], b'cross'
    if path == '/cookie-redirect':
        return 302, [('Set-Cookie', 'hop=1; Path=/'), ('Location', '/echo?after=cookie')], b'moved'
    match = re.fullmatch(r'/status/(\d+)', path)
    if match:
        code, kind = int(match.group(1)), (query.get('kind') or [''])[0]
        extra, text = [], 'status ' + str(code)
        if kind == 'cfchallenge':
            extra, text = [('Server', 'cloudflare')], 'x __cf_chl_jschl_tk__= y'
        elif kind == 'cfcaptcha':
            extra, text = [('Server', 'cloudflare')], 'x __cf_chl_captcha_tk__= y'
        elif kind == 'cf1020':
            extra, text = [('Server', 'cloudflare')], '<span class="cf-error-code">1020</span>'
        elif kind == 'recaptcha':
            text = 'see "https://www.google.com/recaptcha/api.js"'
        elif kind == 'retry':
            extra = [('Retry-After', '7')]
        return code, extra, text.encode()
    match = re.fullmatch(r'/big/(\d+)', path)
    if match:
        return 200, [], b'x' * int(match.group(1))
    match = re.fullmatch(r'/slow/([0-9.]+)', path)
    if match:
        time.sleep(float(match.group(1)))
        return 200, [], b'slow'
    if path == '/json':
        return 200, [('Content-Type', 'application/json')], json.dumps({'items': [1, 2], 'path': raw_path}).encode()
    return 404, [], b'not found'


def handler_class(tag, proxy=False):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def serve(self):
            length = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(length) if length else b''
            record = {'server': tag, 'method': self.command, 'path': self.path, 'version': self.request_version,
                      'headers': [[k, v] for k, v in self.headers.items()], 'body': body.decode('latin-1'),
                      'client': self.client_address[0]}
            with LOCK:
                RECORDS.append(record)
            if proxy:
                status, extra, payload = 200, [], ('via-' + tag + ':' + self.path).encode()
            else:
                status, extra, payload = answer(tag, self.command, self.path, self.headers, body)
            self.send_response(status)
            for name, value in extra:
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(payload)

        do_GET = do_POST = do_PUT = do_HEAD = do_OPTIONS = do_PATCH = do_DELETE = serve
    return Handler


class Servers:
    def __init__(self):
        self.items = {}
        for tag, proxy in (('A', False), ('B', False), ('P1', True), ('P2', True)):
            self.items[tag] = ThreadingHTTPServer(('127.0.0.1', 0), handler_class(tag, proxy))
        self.threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in self.items.values()]
        for thread in self.threads:
            thread.start()

    def port(self, tag):
        return self.items[tag].server_port

    def url(self, tag):
        return 'http://127.0.0.1:' + str(self.port(tag))

    def normalize(self, text):
        """Replace this run's port numbers by server tags."""
        for tag in self.items:
            text = text.replace('127.0.0.1:' + str(self.port(tag)), 'HOST_' + tag)
            text = text.replace('127.0.0.1%3A' + str(self.port(tag)), 'HOST_' + tag)
        return text

    def take(self):
        with LOCK:
            records = list(RECORDS)
            RECORDS.clear()
        return json.loads(self.normalize(json.dumps(records)))

    def close(self):
        for server in self.items.values():
            server.shutdown()
            server.server_close()
