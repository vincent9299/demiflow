"""Thin MCP client for the worker's one-turn authenticated control channel.

No deployment, Store, credential, task record or permission file is opened here.
The CLI sees only its fixed tool schema and bounded responses from the worker.
"""
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.transport import decode


from demiflow.forge.control_client import call


def main(endpoint, token, turn_id, epoch):
    for _ in range(4096):
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
                if isinstance(params, dict) and isinstance(params.get('_meta'), dict):
                    params = {key: value for key, value in params.items() if key != '_meta'}
                result = call(endpoint, token, turn_id, epoch, method, params)
            elif method == 'ping': result = {}
            else: raise ValueError('Unsupported MCP method')
            response['result'] = result
        except Exception as exc:
            response['error'] = dict(code=-32602, message=str(exc)[:2048])
        sys.stdout.buffer.write(wire(response, 65536) + b'\n'); sys.stdout.buffer.flush()


if __name__ == '__main__':
    if len(sys.argv) != 6 or sys.argv[1] != '--connect': raise SystemExit('A bound control endpoint is required')
    main(*sys.argv[2:])
