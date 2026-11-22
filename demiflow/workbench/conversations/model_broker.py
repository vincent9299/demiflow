"""Task-bound model transport; upstream credentials stay outside the CLI.

Only the configured provider/model is reachable. This is a bounded transport,
not an agent loop, a general HTTP proxy, or a tool executor. The external
launcher connects the one Unix endpoint before entering its network namespace.
"""
from __future__ import annotations

import hmac
import http.server
import fcntl
import json
import os
from pathlib import Path
import secrets
import selectors
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

from .common import identifier, reject, wire
from .transport import Transport, decode

FRAME = 1024 * 1024
REQUESTS = 64
OPTION_FIELDS = {'provider', 'model', 'reasoningEffort', 'messages', 'system', 'tools',
                 'toolHistory', 'temperature', 'maxTokens', 'stop', 'sessionId', 'purpose'}


def capabilities(name, config):
    """No authentication reads or network access during capability discovery."""
    if name == 'deepseek':
        executable = Path(config.get('executable', '')).resolve()
        route = config.get('model_provider', 'deepseek-official')
        authenticated = bool(config.get('native_credentials')) if route == 'deepseek-account' else bool(
            config.get('credential_env') and os.environ.get(config['credential_env']))
        installed = (executable.name == 'bin.js' and executable.is_file() and
                     Path(config.get('node', '')).is_file() and authenticated)
        return dict(available=installed, reason='' if installed else '受限执行需要已核验的 Harness 安装')
    if name == 'codex':
        url = urllib.parse.urlsplit(config.get('base_url', ''))
        available = bool(config.get('credential_env') and os.environ.get(config['credential_env']) and
            url.scheme in {'https', 'http'} and url.hostname and not url.username and not url.password and
            not url.query and not url.fragment and
            (url.scheme == 'https' or url.hostname in {'localhost', '127.0.0.1', '::1'}))
        return dict(available=available, reason='' if available else '受限 Codex 尚未配置固定模型通道')
    return dict(available=False, reason='不支持此模型通道')


def validate_options(value, config, *, resolve=False):
    if type(value) is not dict or value.keys() - OPTION_FIELDS:
        reject('Unsupported model request fields')
    route = config.get('model_provider', 'deepseek-official')
    if value.get('provider') != route or value.get('model') != config['model']:
        reject('Model route cannot change within a development task')
    if resolve:
        if set(value) != {'provider', 'model'}: reject('Invalid model metadata request')
        return value
    if type(value.get('messages')) is not list or not 1 <= len(value['messages']) <= 2048:
        reject('Model message count exceeds budget')
    if 'system' in value and type(value['system']) is not str: reject('Invalid model system input')
    if 'tools' in value and (type(value['tools']) is not list or len(value['tools']) > 128):
        reject('Model tool declaration count exceeds budget')
    if 'maxTokens' in value and (type(value['maxTokens']) is not int or not 1 <= value['maxTokens'] <= 16384):
        reject('Model output token request exceeds budget')
    if value.get('reasoningEffort', config.get('effort', 'high')) != config.get('effort', 'high'):
        reject('Model reasoning effort cannot change within a development task')
    if value.get('purpose') not in (None, 'compaction', 'session-title'):
        reject('Unsupported auxiliary model request')
    # Account adapters can resolve file/image attachment references. They must
    # never become an unsandboxed host-file reader on behalf of the CLI.
    def inspect(item, depth=0):
        if depth > 48: reject('Model input nesting exceeds budget')
        if isinstance(item, dict):
            if item.get('type') in {'image', 'file', 'audio', 'video', 'image_url', 'input_image', 'input_file'}:
                reject('Restricted development model transport currently accepts text only')
            for child in item.values(): inspect(child, depth + 1)
        elif isinstance(item, list):
            for child in item: inspect(child, depth + 1)
    inspect(value)
    for message in value['messages']:
        if (type(message) is not dict or message.get('role') not in {'system','developer','user','assistant','tool'} or
                type(message.get('content')) is not list or len(message['content']) > 4096):
            reject('Invalid model message content')
        for block in message['content']:
            if type(block) is not dict or block.get('type') not in {'text','reasoning','tool-call','tool-addition','tool-removal'}:
                reject('Unsupported model content block')
    return dict(value, reasoningEffort=config.get('effort', 'high'), maxTokens=value.get('maxTokens', 16384))


class _Quota:
    """One root-turn transport allowance, retained across automatic repair epochs.

    This is only a reservation counter under the existing private session control
    directory. It contains no prompt, credentials, task evidence or acceptance.
    Corruption or overlapping owners fail closed; charges precede dispatch.
    """
    def __init__(self, path, turn_id, maximum):
        self.fd = None
        self.maximum = maximum
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > 4096:
                reject('Invalid protected model quota')
            raw = os.pread(self.fd, 4096, 0)
            self.value = decode(raw) if raw else dict(turn=turn_id, requests=0, connections=0, input=0, output=0)
            if (set(self.value) != {'turn','requests','connections','input','output'} or self.value['turn'] != turn_id or
                    any(type(self.value[k]) is not int or self.value[k] < 0 for k in ('requests','connections','input','output'))):
                reject('Protected model quota identity differs')
        except BaseException:
            self.close(); raise

    def charge(self, **amounts):
        limits = dict(requests=REQUESTS, connections=256, input=self.maximum, output=self.maximum)
        value = dict(self.value)
        for key, amount in amounts.items():
            value[key] += amount
            if value[key] > limits[key]: reject('Model root-turn ' + key + ' budget exhausted')
        raw = wire(value, 4096)
        os.pwrite(self.fd, raw, 0); os.ftruncate(self.fd, len(raw)); os.fsync(self.fd)
        self.value = value

    def close(self):
        if self.fd is not None: os.close(self.fd); self.fd = None


def validate_responses(value, config):
    fields = {'model','input','instructions','tools','tool_choice','parallel_tool_calls','reasoning',
        'text','stream','store','max_output_tokens','temperature','top_p','truncation','include',
        'metadata','prompt_cache_key','prompt_cache_retention','service_tier','stream_options','safety_identifier','user'}
    if type(value) is not dict or value.keys() - fields or value.get('model') != config['model']:
        reject('Model route or request fields differ')
    if 'max_output_tokens' in value and (type(value['max_output_tokens']) is not int or not 1 <= value['max_output_tokens'] <= 16384):
        reject('Model output token request exceeds budget')
    data = value.get('input')
    if not isinstance(data, (str,list)) or isinstance(data,list) and len(data) > 2048:
        reject('Unsupported model input')
    def content(item):
        if isinstance(item,str): return
        if type(item) is not list or len(item) > 4096: reject('Unsupported model content')
        for block in item:
            if (type(block) is not dict or block.get('type') not in {'input_text','output_text','refusal'} or
                    block.keys() - {'type','text','refusal','annotations','logprobs'}):
                reject('Model input must contain inline text only')
    if isinstance(data,list):
        for item in data:
            if type(item) is not dict: reject('Unsupported model input item')
            kind = item.get('type','message')
            if kind == 'message':
                if item.keys()-{'type','role','content','id','status'} or item.get('role') not in {'system','developer','user','assistant'}:
                    reject('Unsupported model message')
                content(item.get('content'))
            elif kind in {'function_call','custom_tool_call'}:
                if item.keys()-{'type','id','call_id','name','arguments','input','status','namespace'}:
                    reject('Unsupported local tool call')
            elif kind in {'function_call_output','custom_tool_call_output'}:
                if item.keys()-{'type','id','call_id','output','status'}: reject('Unsupported local tool result')
                content(item.get('output'))
            elif kind in {'reasoning','compaction'}:
                if item.keys()-{'type','id','summary','content','encrypted_content','status'}:
                    reject('Unsupported inline model context')
            else: reject('Remote model references and attachments are not enabled')
    declared = value.get('tools', [])
    if type(declared) is not list: reject('Invalid model tool declarations')
    count = 0
    def local_tools(items):
        nonlocal count
        for tool in items:
            count += 1
            if count > 128 or type(tool) is not dict: reject('Model tool declarations exceed budget')
            if tool.get('type') in {'function','custom'}: continue
            if tool.get('type') != 'namespace' or type(tool.get('tools')) is not list:
                reject('Hosted model tools are not enabled')
            local_tools(tool['tools'])
    local_tools(declared)
    return dict(value, store=False, max_output_tokens=value.get('max_output_tokens',16384))


class ModelBroker:
    def __init__(self, deployment, turn, session, home, *, defer_handshake=False):
        self.name = session['provider']; self.config = deployment.providers[self.name]
        if not capabilities(self.name, self.config)['available']:
            reject('Configured provider has no qualified fixed model channel')
        self.deadline = turn['created'] + deployment.limits.turn_seconds
        self.token = secrets.token_hex(32)
        self.closed = threading.Event(); self.requests = self.connections = 0
        self.input_bytes = self.output_bytes = 0; self.failure = None
        self.maximum = deployment.limits.output_bytes
        self.process = self.transport = self.listener = self.thread = self.client = self.quota = self.http_process = None
        self._init_thread = None; self._ready = threading.Event(); self._init_error = None
        self.output_reserved = 0
        self.socket_dir = Path(tempfile.mkdtemp(prefix='df-model-'))
        self.socket_path = self.socket_dir / 'channel'
        self.home = Path(home); self.home.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.child_config = {k: v for k, v in self.config.items()
                             if k not in {'native_credentials', 'credential_env', 'base_url', 'inherit_proxy'}}
        try:
            self.quota = _Quota(self.home.parent.parent / (identifier(turn['id']) + '.model-budget.json'),
                                turn['id'], self.maximum)
            if self.name == 'deepseek':
                from .deepseek import DeepSeek
                command, env = DeepSeek.launch(dict(self.config, _model_broker=True), self.home, str(self.home))
                launcher = Path(__file__).with_name('provider_exec.py')
                self.process = subprocess.Popen([sys.executable, '-I', '-B', str(launcher), str(os.getpid()), *command],
                    cwd=self.home, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    close_fds=True, start_new_session=True)
                self.transport = Transport(self.process, deployment.limits)
                self.child_config['_boundary'] = dict(token=self.token)
                if defer_handshake:
                    self._init_thread = threading.Thread(target=self._run_handshake, name='development-model-init', daemon=True)
                    self._init_thread.start()
                else:
                    self._finish_deepseek_handshake()
                    self._ready.set()
            else:
                url = urllib.parse.urlsplit(self.config['base_url'])
                if (url.username or url.password or url.query or url.fragment or not url.hostname or
                        url.scheme not in {'https', 'http'} or url.scheme == 'http' and url.hostname not in {'localhost', '127.0.0.1', '::1'}):
                    reject('Fixed model endpoint must use HTTPS or explicit local test transport')
                if not os.environ.get(self.config['credential_env']): reject('Configured model authentication is unavailable')
                self.child_config['_boundary'] = dict(token=self.token)
                self._ready.set()
            self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.listener.bind(str(self.socket_path)); self.listener.listen(4); self.listener.settimeout(.2)
            self.thread = threading.Thread(target=self._serve, name='development-model-channel', daemon=True)
            self.thread.start()
        except BaseException:
            self.close(); raise

    def _finish_deepseek_handshake(self):
        reply = self._rpc('initialize', {}, lambda _: None, timeout=30)
        expected = dict(protocol='demiflow-model-channel-v1',
            provider=self.config.get('model_provider', 'deepseek-official'), model=self.config['model'])
        if reply != expected: reject('Native model broker handshake differs')

    def _run_handshake(self):
        try:
            self._finish_deepseek_handshake()
        except BaseException as exc:
            self._init_error = exc
        finally:
            self._ready.set()

    def wait_ready(self, timeout=35):
        if not self._ready.wait(timeout):
            raise TimeoutError('Native model broker handshake timed out')
        if self._init_error is not None:
            raise self._init_error

    def _check(self):
        if self.closed.is_set() or time.time() >= self.deadline:
            raise TimeoutError('Development model channel has expired')

    def _rpc(self, method, params, on_chunk, *, timeout=180):
        self._check()
        key = self.transport.request(method, params)
        until = min(self.deadline, time.time() + timeout)
        while True:
            self._check()
            if time.time() >= until: raise TimeoutError('Model request exceeded its deadline')
            self.transport.poll(.05)
            while self.transport.events:
                event = self.transport.pop()
                if (event.get('method') != 'model/chunk' or event.get('params', {}).get('requestId') != key):
                    reject('Unexpected native model response identity')
                on_chunk(event['params']['chunk'])
            result = self.transport.take(key)
            if result is not None: return result
            if self.transport.closed: raise ConnectionError('Native model channel ended before completion')

    def _receive(self, client):
        buffer = bytearray(); until = min(self.deadline, time.time() + 5)
        while b'\n' not in buffer:
            self._check()
            if time.time() >= until: raise TimeoutError('Model request input timed out')
            try: chunk = client.recv(min(65536, FRAME + 1 - len(buffer)))
            except socket.timeout: continue
            if not chunk: raise ConnectionError('Incomplete model request')
            buffer.extend(chunk)
            if len(buffer) > FRAME: reject('Model request exceeds byte budget')
        raw, tail = bytes(buffer).split(b'\n', 1)
        if tail.strip(): reject('Only one request per model connection is supported')
        self.input_bytes += len(raw)
        self.quota.charge(input=len(raw))
        return decode(raw)

    def _output(self, count):
        self.output_bytes += count
        if self.output_bytes > self.output_reserved:
            charge = ((self.output_bytes - self.output_reserved + 65535) // 65536) * 65536
            self.quota.charge(output=charge)
            self.output_reserved += charge

    def _send(self, client, value):
        raw = wire(value, FRAME) + b'\n'; self._output(len(raw))
        self._check(); client.sendall(raw)

    def _handle_json(self, client):
        self.wait_ready()
        request = self._receive(client)
        if (set(request) != {'token', 'method', 'options'} or type(request['token']) is not str
                or not hmac.compare_digest(request['token'], self.token)):
            raise PermissionError('Model channel authorization differs')
        if request['method'] not in {'resolve', 'stream'}: reject('Unsupported model channel method')
        resolve = request['method'] == 'resolve'
        options = validate_options(request['options'], self.config, resolve=resolve)
        if not resolve:
            self.requests += 1
            self.quota.charge(requests=1)
        client.settimeout(2)
        result = self._rpc('model/resolve' if resolve else 'model/stream', options,
                           lambda chunk: self._send(client, dict(value=chunk)))
        if resolve:
            # The private CLI must plan within this channel's actual capabilities.
            result = dict(result, inputModalities=['text'], defaultMaxTokens=min(result.get('defaultMaxTokens', 16384), 16384))
            self._send(client, dict(value=result))
        elif result != {'complete': True}: reject('Model completion receipt differs')
        self._send(client, dict(done=True))

    def _handle_http(self, handler):
        self._check()
        if not hmac.compare_digest(handler.headers.get('Authorization', ''), 'Bearer ' + self.token):
            raise PermissionError('Model channel authorization differs')
        if handler.headers.get('Transfer-Encoding') or handler.path not in {'/responses', '/v1/responses', '/responses/compact', '/v1/responses/compact'}:
            reject('Unsupported fixed model request')
        count = int(handler.headers.get('Content-Length', '-1'))
        if not 0 < count <= FRAME: reject('Model HTTP request exceeds budget')
        raw = handler.rfile.read(count)
        if len(raw) != count: reject('Incomplete model HTTP request')
        value = validate_responses(decode(raw), self.config)
        self.requests += 1; self.input_bytes += len(raw)
        self.quota.charge(requests=1, input=len(raw))
        suffix = '/responses/compact' if handler.path.endswith('/compact') else '/responses'
        endpoint = self.config['base_url'].rstrip('/') + suffix
        from .policy import proxy_environment
        environment = dict(PATH='/usr/bin:/bin',LANG='C.UTF-8',
            DEMIFLOW_UPSTREAM_TOKEN=os.environ[self.config['credential_env']], **proxy_environment(self.config))
        self._check()
        launcher=Path(__file__).with_name('provider_exec.py')
        process = subprocess.Popen([sys.executable,'-I','-B',str(launcher),str(os.getpid()),sys.executable,
            '-I','-B',str(Path(__file__).with_name('model_http.py'))], cwd=self.home, env=environment,
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,close_fds=True,start_new_session=True)
        self.http_process = process
        selector = selectors.DefaultSelector()
        try:
            self._check()
            process.stdin.write(wire(dict(url=endpoint,body=value,deadline=min(self.deadline,time.time()+180),maximum=self.maximum),FRAME+8192))
            process.stdin.close()
            os.set_blocking(process.stdout.fileno(),False)
            selector.register(process.stdout,selectors.EVENT_READ)
            header = bytearray(); started = False
            while True:
                self._check()
                if not selector.select(.1): continue
                chunk = os.read(process.stdout.fileno(),65536)
                if not chunk: break
                if not started:
                    header.extend(chunk)
                    if b'\n' not in header:
                        if len(header)>256: reject('Invalid fixed HTTP worker header')
                        continue
                    row,chunk=bytes(header).split(b'\n',1); header.clear()
                    metadata=decode(row)
                    if set(metadata)!={'content_type'} or metadata['content_type'] not in {'application/json','text/event-stream'}:
                        reject('Invalid fixed HTTP worker header')
                    handler.send_response(200);handler.send_header('Content-Type',metadata['content_type'])
                    handler.send_header('Connection','close');handler.end_headers();started=True
                self._output(len(chunk))
                handler.wfile.write(chunk); handler.wfile.flush()
            if process.wait(timeout=2) != 0 or not started: reject('Configured HTTP model request failed')
        except BaseException:
            # A dispatched request with a lost result is never replayed through
            # a new connection, even if the CLI's native transport retries.
            self.closed.set()
            raise
        finally:
            selector.close()
            if process.poll() is None: process.kill();process.wait(timeout=2)
            if process.stdin and not process.stdin.closed: process.stdin.close()
            process.stdout.close()
            self.http_process = None

    def _serve(self):
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                try: owner._handle_http(self)
                except BaseException:
                    self.send_error(502, 'Configured model request failed or exceeded its budget')
        try:
            while not self.closed.is_set() and time.time() < self.deadline:
                try: client, _ = self.listener.accept()
                except socket.timeout: continue
                self.client = client; self.connections += 1
                try:
                    self.quota.charge(connections=1)
                    client.settimeout(.2 if self.name == 'deepseek' else 5)
                    if self.name == 'deepseek': self._handle_json(client)
                    else: Handler(client, ('127.0.0.1', 0), self)
                except BaseException:
                    self.failure = 'Configured model request failed or exceeded its budget'
                    try: self._send(client, dict(error=self.failure))
                    except BaseException: pass
                    # Never retry a dispatched request with an uncertain result.
                    if self.transport and self.transport.pending: self.closed.set()
                finally:
                    client.close(); self.client = None
        except OSError:
            pass

    def close(self):
        self.closed.set()
        if self.client:
            try: self.client.shutdown(socket.SHUT_RDWR)
            except OSError: pass
        if self.listener: self.listener.close()
        if self.http_process and self.http_process.poll() is None:
            try: self.http_process.kill(); self.http_process.wait(timeout=2)
            except ProcessLookupError: pass
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=3)
            except subprocess.TimeoutExpired: self.process.kill(); self.process.wait(timeout=2)
        if self.thread and self.thread is not threading.current_thread(): self.thread.join(timeout=3)
        if self._init_thread and self._init_thread is not threading.current_thread(): self._init_thread.join(timeout=3)
        if self.thread and self.thread.is_alive(): raise TimeoutError('Model channel has not drained')
        if self._init_thread and self._init_thread.is_alive(): raise TimeoutError('Model handshake has not drained')
        if self.transport: self.transport.close()
        if self.quota and (not self.thread or not self.thread.is_alive()): self.quota.close()
        if not self.thread or not self.thread.is_alive(): shutil.rmtree(self.socket_dir, ignore_errors=True)


def start_model_broker(deployment, turn, session, home, *, defer_handshake=False):
    return ModelBroker(deployment, turn, session, home, defer_handshake=defer_handshake)
