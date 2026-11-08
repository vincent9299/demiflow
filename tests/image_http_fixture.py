"""Bounded synthetic pixels and an actual loopback HTTP provider; no model weights."""
import asyncio
import base64
import contextlib
import gzip
import io
import json
import random
import threading
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest
from PIL import Image


def picture(width=4, height=4, color='red', noisy=False):
    with Image.new('RGB', (width, height), color) as image:
        if noisy:
            image.frombytes(random.Random(17).randbytes(width * height * 3))
        with io.BytesIO() as out:
            image.save(out, format='PNG')
            return out.getvalue()

class FixtureServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

class SlowConsumer:
    """Events are fixture resources, not mutable semantic state or row data."""
    def __init__(self, entered, release):
        self.entered, self.release = entered, release

    def recovery_identity(self):
        return {'fixture': 'bounded-slow-consumer-v1'}

    def snapshot_state(self):
        return {}

    def restore_state(self, value):
        assert value == {}

    async def __call__(self, row):
        self.entered.set()
        for _ in range(200):
            if self.release.is_set():
                return row
            await asyncio.sleep(0.01)
        raise RuntimeError('Synthetic consumer release timed out')

@pytest.fixture
def fixture_http():
    state = {'requests': [], 'mode': 'b64', 'raw': picture(), 'response_override': None}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def reply(self, payload, content_type, *, encoding=None):
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(payload)))
            if encoding:
                self.send_header('Content-Encoding', encoding)
            self.end_headers()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(payload)

        def do_GET(self):
            state['requests'].append({'method': 'GET', 'path': self.path, 'authorization': self.headers.get('Authorization')})
            self.reply(state['raw'], 'image/png')

        def do_POST(self):
            length = int(self.headers.get('Content-Length', 0))
            if not 0 <= length <= 64 * 1024 or len(state['requests']) >= 128:
                self.send_error(413)
                return
            raw = self.rfile.read(length)
            content_type = self.headers.get('Content-Type', '')
            request = {'method': 'POST', 'path': self.path, 'content_type': content_type,
                       'raw_bytes': len(raw), 'authorization': self.headers.get('Authorization')}
            if content_type.startswith('multipart/'):
                message = BytesParser(policy=default).parsebytes(
                    ('Content-Type: ' + content_type + '\r\n\r\n').encode() + raw)
                request['multipart'] = [dict(name=p.get_param('name', header='content-disposition'),
                    content_type=p.get_content_type(), data=p.get_payload(decode=True)) for p in message.iter_parts()]
            else:
                request['body'] = json.loads(raw)
            state['requests'].append(request)
            if state['mode'] == 'gzip_large':
                expanded = json.dumps({'ignored': 'X' * (256 * 1024)}).encode()
                self.reply(gzip.compress(expanded), 'application/json', encoding='gzip')
                return
            if state['response_override'] is not None:
                body = state['response_override']
            elif self.path.startswith('/json/'):
                body = {'choices': [{'message': {'content': '{"answer":"ok"}'}}]}
            else:
                url = ('http://127.0.0.1:' + str(self.server.server_port) + '/asset.png'
                       if state['mode'] == 'url' else 'data:image/png;base64,' + base64.b64encode(state['raw']).decode())
                if self.path.endswith('/chat/completions'):
                    body = {'choices': [{'message': {'content': [{'type': 'image_url', 'image_url': {'url': url}}]}}]}
                else:
                    body = {'data': [{'url': url}]} if state['mode'] == 'url' else {
                        'data': [{'b64_json': base64.b64encode(state['raw']).decode()}]}
            self.reply(json.dumps(body).encode(), 'application/json')

    server = FixtureServer(('127.0.0.1', 0), Handler)
    state['base'] = 'http://127.0.0.1:' + str(server.server_port) + '/v1'
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

def definition(base, api='images_json', template='{{ instruction }}{{ refs | image }}'):
    return {'schema_version': 'demiflow_prompt_pack_v2', 'prompts': {'generate': {
        'version': 'synthetic-v1', 'response_format': 'image',
        'model': {'backend': 'modelhub', 'model': 'synthetic-fixture', 'api': api,
                  'base_url': base, 'api_key_env': '', 'revision': 'fixture-r1', 'timeout_s': 2},
        'template': template}}}

def json_definition(base):
    return {'schema_version': 'demiflow_prompt_pack_v2', 'prompts': {'answer': {
        'version': '1', 'model': {'name': 'json-fixture', 'transport': 'openai_compatible',
            'base_url': base.replace('/v1', '/json/v1'), 'api_key_env': 'SYNTHETIC_JSON_KEY'},
        'template': '{{ instruction }}', 'response_schema': {'type': 'object', 'additionalProperties': False,
            'required': ['answer'], 'properties': {'answer': {'type': 'string'}}}}}}
