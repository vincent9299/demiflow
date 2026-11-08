"""Fixed ACP MCP stdio bridge to the same public development CLI.

The owning worker supplies immutable deployment, turn and task identities. This
server has no arbitrary command, path, provider or independent Review endpoint.
"""
import json
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.context import BRIDGE
from demiflow.workbench.conversations.context import ProtocolBridge
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.transport import decode


def main(config_path, turn_id):
    deployment=Deployment.load(config_path);store=Store(deployment.state,deployment.limits)
    turn=store.turn(turn_id);session=store.session(turn['session'])
    project=deployment.project(session['workspace'],json.loads(session['root'])['path'])
    scratch=deployment.state/session['id']/turn_id
    protocol_bridge = ProtocolBridge(store, turn_id, project, scratch)
    for _ in range(128):
        raw=sys.stdin.buffer.readline(65537)
        if not raw:return
        if len(raw)>65536 or not raw.endswith(b'\n'):raise ValueError('MCP frame limit')
        message=decode(raw)
        if 'id' not in message:continue
        response={'jsonrpc':'2.0','id':message['id']}
        try:
            method=message.get('method');params=message.get('params',{})
            if method=='initialize':
                result=dict(protocolVersion=params['protocolVersion'],serverInfo=dict(name='demiflow-development',version='1'),capabilities=dict(tools={}))
            elif method=='tools/list':
                result=dict(tools=[BRIDGE])
            elif method=='tools/call' and params.get('name')==BRIDGE['name']:
                current=store.turn(turn_id)
                if current['cleanup']=='complete' or current['stop_at']:raise ValueError('Turn no longer active')
                answer=protocol_bridge.invoke(params['arguments'])
                result=dict(content=[dict(type='text',text=wire(answer,60000).decode())],isError=False)
            elif method=='ping':result={}
            else:raise ValueError('Unsupported MCP method')
            response['result']=result
        except Exception as exc:
            response['error']=dict(code=-32602,message=str(exc)[:2048])
        sys.stdout.buffer.write(wire(response,65536)+b'\n');sys.stdout.buffer.flush()
        protocol_bridge.sent()


if __name__=='__main__':main(sys.argv[1],sys.argv[2])
