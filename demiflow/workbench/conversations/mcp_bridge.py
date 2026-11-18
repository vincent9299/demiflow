"""Thin MCP client for the worker's one-turn authenticated control channel.

No deployment, Store, credential, task record or permission file is opened here.
The CLI sees only its fixed tool schema and bounded responses from the worker.
"""
import json
import os
from pathlib import Path
import socket
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.transport import decode


def call(endpoint, token, turn_id, epoch, method, params):
    # B1's trusted relay overrides the host Unix path with loopback inside the
    # isolated network namespace. Arbitrary hosts and non-loopback URLs reject.
    endpoint = os.environ.get('DEMIFLOW_CONTROL_ENDPOINT') or endpoint
    if endpoint.startswith('127.0.0.1:'):
        port = endpoint.split(':', 1)[1]
        if not port.isdigit() or not 1 <= int(port) <= 65535: raise ValueError('Invalid control relay port')
        connection = socket.create_connection(('127.0.0.1', int(port)), timeout=10)
    elif endpoint.startswith('/'):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(10); connection.connect(endpoint)
    else:
        raise ValueError('Unsupported control endpoint')
    with connection:
        connection.sendall(wire(dict(token=token, turn_id=turn_id, epoch=epoch, method=method, params=params), 65535) + b'\n')
        with connection.makefile('rb') as stream:
            raw = stream.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b'\n'): raise ValueError('Control response exceeds frame budget')
        response = decode(raw)
        if 'error' in response: raise ValueError(response['error'].get('message', 'Control request rejected'))
        return response['result']


def main(endpoint, token, turn_id, epoch):
    for _ in range(128):
        raw = sys.stdin.buffer.readline(65537)
        if not raw: return
        if len(raw) > 65536 or not raw.endswith(b'\n'): raise ValueError('MCP frame limit')
        message = decode(raw)
        if 'id' not in message: continue
        response = dict(jsonrpc='2.0', id=message['id'])
        method = message.get('method')
        try:
            params = message.get('params', {})
            if method == 'initialize':
                result = dict(protocolVersion=params['protocolVersion'], serverInfo=dict(name='demiflow-development', version='2'), capabilities=dict(tools={}))
            elif method in {'tools/list', 'tools/call'}:
                result = call(endpoint, token, turn_id, epoch, method, params)
            elif method == 'ping': result = {}
            else: raise ValueError('Unsupported MCP method')
            response['result'] = result
        except Exception as exc:
            response['error'] = dict(code=-32602, message=str(exc)[:2048])
        sys.stdout.buffer.write(wire(response, 65536) + b'\n'); sys.stdout.buffer.flush()
        if method == 'tools/call' and 'result' in response:
            call(endpoint, token, turn_id, epoch, 'delivery/sent', {})


if __name__ == '__main__':
    if len(sys.argv) != 6 or sys.argv[1] != '--connect': raise SystemExit('A bound control endpoint is required')
    main(*sys.argv[2:])
