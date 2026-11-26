"""Request a single operation over the turn-bound controller channel."""
import json
import os
import socket

from demiflow.forge.platform import bounded_json


def _decode(raw):
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise ValueError('Control frame must be valid UTF-8') from exc
    if '\x00' in text:
        raise ValueError('Control frame contains NUL byte')
    try:
        value = json.loads(text, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Invalid JSON constant')))
    except ValueError as exc:
        raise ValueError('Invalid control JSON frame') from exc
    if type(value) is not dict:
        raise ValueError('Control JSON frame must be an object')
    return value


def call(endpoint, token, turn_id, epoch, method, params):
    # Trusted relay overrides the host Unix path with loopback inside the
    # isolated network namespace. Arbitrary hosts and non-loopback URLs reject.
    endpoint = os.environ.get('DEMIFLOW_CONTROL_ENDPOINT') or endpoint
    if endpoint.startswith('127.0.0.1:'):
        port = endpoint.split(':', 1)[1]
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError('Invalid control relay port')
        connection = socket.create_connection(('127.0.0.1', int(port)), timeout=10)
    elif endpoint.startswith('/'):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(10)
        connection.connect(endpoint)
    else:
        raise ValueError('Unsupported control endpoint')
    with connection:
        payload = bounded_json(dict(token=token, turn_id=turn_id, epoch=epoch, method=method, params=params), 65535)
        connection.sendall(payload + b'\n')
        with connection.makefile('rb') as stream:
            raw = stream.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b'\n'):
            raise ValueError('Control response exceeds frame budget')
        response = _decode(raw)
        if 'error' in response:
            raise ValueError(response['error'].get('message', 'Control request rejected'))
        return response['result']
