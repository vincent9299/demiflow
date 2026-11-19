"""Same-origin HTTP/WebSocket bridge to the owned native Jupyter server.

The outer workspace authenticates the browser and rechecks project grants;
the upstream's private token is never sent to the browser or accepted from it.
"""
from http.cookies import SimpleCookie
import hmac
import http.client
import re
import select
import socket
from urllib.parse import unquote, urlsplit

from demiflow.forge.schema import ForgeError

MAX_BODY = 16 * 1024**2
HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
       'te', 'trailer', 'transfer-encoding', 'upgrade', 'host', 'authorization',
       'cookie', 'x-forge-token', 'origin', 'referer'}


def route(handler):
    path = urlsplit(handler.path).path
    if not path.startswith('/jupyter/'):
        return False
    service = handler.server.notebooks
    if service is None:
        raise ForgeError('Native Notebook is not configured', code='operation_rejected')
    match = re.fullmatch(r'/jupyter/([A-Za-z][A-Za-z0-9_-]{0,63})/(.*)', path)
    if not match or len(handler.path) > 8192:
        raise ForgeError('Invalid Notebook path', code='invalid_request')
    project, relative = match.groups()
    decoded = unquote(relative)
    if '\\' in decoded or '\0' in decoded or '%' in decoded or '..' in decoded.split('/'):
        raise PermissionError('Invalid Notebook path')
    origin = handler._request_origin()
    origins = handler.headers.get_all('Origin', [])
    if len(origins) > 1 or origins and origins[0] != origin:
        raise PermissionError('Invalid Notebook Origin')
    if handler.headers.get('Sec-Fetch-Site') == 'cross-site':
        raise PermissionError('Cross-site Notebook request')
    root, permissions = service.authorize(project)
    cookies = SimpleCookie()
    try: cookies.load(handler.headers.get('Cookie', ''))
    except Exception: raise PermissionError('Invalid Notebook session') from None
    provided = cookies.get('demiflow_notebook')
    if provided is None or not hmac.compare_digest(provided.value, service.credentials(project, root)):
        raise PermissionError('Notebook session expired; reopen the data page')
    websocket = handler.headers.get('Upgrade', '').lower() == 'websocket'
    execution = decoded == 'api/kernels' or decoded.startswith('api/kernels/') or decoded == 'api/sessions' or decoded.startswith('api/sessions/')
    if (websocket or execution and handler.command not in {'GET','HEAD'}) and not permissions['analysis_execute']:
        raise PermissionError('Notebook execution is disabled for this project')
    if handler.command not in {'GET', 'HEAD'}:
        grant = 'analysis_execute' if execution else 'analysis_write'
        if not permissions[grant]:
            raise PermissionError('Notebook permission is disabled: ' + grant)
        if not origins or origins[0] != origin:
            # Native fetches use same-origin Referer when Origin is omitted on
            # same-origin methods; no cross-site form submission is accepted.
            referer = urlsplit(handler.headers.get('Referer', ''))
            if referer.scheme + '://' + referer.netloc != origin:
                raise PermissionError('Notebook write requires its same-origin page')
    # Terminal/extension installation is outside this document workspace.
    if decoded.startswith(('api/terminals', 'terminals/', 'lab/api/extensions', 'api/shutdown')):
        raise PermissionError('This endpoint is not part of the Notebook workspace')
    with service.lock:
        instance = service.get(project)
        instance.requests += 1
        if websocket or execution and handler.command not in {'GET', 'HEAD'}:
            instance.executable = True
    try:
        if websocket:
            _websocket(handler, service, instance, origin)
        else:
            _http(handler, instance)
    finally:
        with service.lock:
            instance.requests -= 1
    return True


def _http(handler, instance):
    if handler.headers.get_all('Transfer-Encoding'):
        raise ForgeError('Chunked request bodies are not supported', code='invalid_request')
    lengths = handler.headers.get_all('Content-Length', [])
    if len(lengths) > 1 or lengths and not re.fullmatch(r'[0-9]{1,10}', lengths[0]):
        raise ForgeError('Invalid Notebook body length', code='invalid_request')
    length = int(lengths[0]) if lengths else 0
    if length > MAX_BODY:
        raise ForgeError('Notebook exceeds 16 MiB', code='resource_limit')
    body = handler.rfile.read(length) if length else None
    if body is not None and len(body) != length:
        raise ForgeError('Incomplete Notebook request', code='invalid_request')
    headers = {k: v for k, v in handler.headers.items() if k.lower() not in HOP}
    headers.update(Authorization='token ' + instance.token, Connection='close')
    connection = http.client.HTTPConnection('127.0.0.1', instance.port, timeout=45)
    try:
        connection.request(handler.command, handler.path, body=body, headers=headers)
        response = connection.getresponse()
        response_size = response.getheader('Content-Length')
        if response_size and int(response_size) > 32 * 1024**2:
            raise ForgeError('Notebook response exceeds 32 MiB', code='resource_limit')
        handler.send_response(response.status)
        for name, value in response.getheaders():
            if name.lower() not in HOP | {'set-cookie', 'content-security-policy'}:
                handler.send_header(name, value)
        handler.send_header('Content-Security-Policy', "frame-ancestors 'self'")
        handler.send_header('Referrer-Policy', 'same-origin')
        handler.send_header('Connection', 'close')
        handler.end_headers()
        handler.close_connection = True
        if handler.command != 'HEAD':
            transferred = 0
            while chunk := response.read(64 * 1024):
                transferred += len(chunk)
                if transferred > 32 * 1024**2:
                    break
                handler.wfile.write(chunk)
    finally:
        connection.close()


def _websocket(handler, service, instance, origin):
    if not service.channels.acquire(blocking=False):
        raise ForgeError('Notebook channel capacity is busy', code='busy')
    upstream = None
    counted = False
    try:
        upstream = socket.create_connection(('127.0.0.1', instance.port), timeout=5)
        headers = {k: v for k, v in handler.headers.items()
                   if k.lower() not in HOP and k.lower() != 'content-length'}
        headers.update(Host='127.0.0.1:' + str(instance.port),
                       Origin='http://127.0.0.1:' + str(instance.port),
                       Authorization='token ' + instance.token, Connection='Upgrade', Upgrade='websocket')
        raw = 'GET ' + handler.path + ' HTTP/1.1\r\n' + ''.join(k+': '+v+'\r\n' for k,v in headers.items()) + '\r\n'
        upstream.sendall(raw.encode('latin-1'))
        response = bytearray()
        while not response.endswith(b'\r\n\r\n'):
            part = upstream.recv(1)
            if not part or len(response) >= 16384:
                raise OSError('Incomplete Notebook WebSocket handshake')
            response.extend(part)
        if not response.startswith(b'HTTP/1.1 101 '):
            raise OSError('Notebook WebSocket upgrade was rejected')
        handler.wfile.write(response)
        handler.wfile.flush()
        handler.close_connection = True
        handler.connection.settimeout(5)
        with service.lock:
            if service.closed.is_set():
                return
            service.sockets.update((upstream, handler.connection))
            instance.channels += 1
            counted = True
        while not service.closed.is_set():
            root, permissions = service.authorize(instance.project)
            if root != instance.root or not permissions['analysis_execute']:
                break
            ready, _, _ = select.select([upstream, handler.connection], [], [], 5)
            for source in ready:
                chunk = source.recv(64 * 1024)
                if not chunk:
                    return
                (handler.connection if source is upstream else upstream).sendall(chunk)
    finally:
        with service.lock:
            if counted:
                instance.channels -= 1
            service.sockets.discard(upstream)
            service.sockets.discard(handler.connection)
        if upstream is not None:
            upstream.close()
        service.channels.release()


def open_notebook(handler, project):
    service = handler.server.notebooks
    if service is None:
        raise ForgeError('Native Notebook is not configured', code='operation_rejected')
    instance = service.get(project)
    _, permissions = service.authorize(project)
    cookie = 'demiflow_notebook=' + service.credentials(project, instance.root)
    cookie += '; Path=' + instance.base_url + '; HttpOnly; SameSite=Strict'
    if handler._request_origin().startswith('https:'):
        cookie += '; Secure'
    handler._send(dict(url=instance.base_url+instance.initial_path, base_url=instance.base_url,
        root=str(instance.root), notebooks=str(instance.notebooks), permissions=permissions),
        headers={'Set-Cookie': cookie})
