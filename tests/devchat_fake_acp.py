"""ACP lifecycle fault fixture; no model or credential access."""
import json
from pathlib import Path
import subprocess
import sys
import time

root = None
prompt_id = None


def send(value):
    print(json.dumps(dict(jsonrpc='2.0', **value)), flush=True)


for line in sys.stdin:
    value = json.loads(line)
    method, params = value.get('method'), value.get('params', {})
    if method == 'initialize':
        result = dict(protocolVersion=1, agentInfo=dict(name='demiflow-deepseek-compaction' if 'devchat-compact' in sys.argv else 'deepseek-harness-acp'),
                      agentCapabilities=dict(sessionCapabilities=dict(resume={}, close={})))
    elif method in {'session/new', 'session/resume'}:
        root = Path(params['cwd'])
        servers = params['mcpServers']
        result = dict(sessionId='acp-fixture', configOptions=[dict(id='model', currentValue='["deepseek-official","fixture"]')])
    elif method == 'session/compact':
        if (root/'compact-fail').exists():
            send(dict(id=value['id'],error=dict(code=-32603,message='fixture compaction failed')));continue
        result=dict(status='compacted',compaction_id='fixture-summary',shadowed_items=4,estimated_tokens_before=1200)
    elif method == 'session/prompt':
        if params['prompt'][0]['text'].endswith('request delivery review fixture'):
            (root / 'flows/fixture.py').write_text('def sample():\n    return 1\n')
            review = dict(method='tools/call', params=dict(name='request_pipeline_review', arguments={}))
            requests = [dict(method='tools/list', params={}), review, review]
            bridge = servers[0]
            response = subprocess.run([bridge['command'], *bridge['args']], check=True, capture_output=True,
                input=''.join(json.dumps(dict(jsonrpc='2.0', id=i, **r))+'\n' for i,r in enumerate(requests)),
                text=True, timeout=5)
            listed, first, retry = (json.loads(line)['result'] for line in response.stdout.splitlines())
            assert 'request_pipeline_review' in {tool['name'] for tool in listed['tools']}
            assert first == retry and not first['isError']
            send(dict(id=value['id'],result=dict(stopReason='end_turn')))
            continue
        if params['prompt'][0]['text'].rsplit('当前请求：\n', 1)[-1].startswith('public process fixture'):
            for kind,body in [('agent_thought_chunk','公开过程：先检查现有数据。'),('agent_message_chunk','正在查找数据来源。')]:
                send(dict(method='session/update',params=dict(sessionId='acp-fixture',update=dict(
                    sessionUpdate=kind,messageId='message-1',content=dict(type='text',text=body)))))
            until=time.monotonic()+20
            while not (root/'release-process').exists() and time.monotonic()<until:time.sleep(.05)
            send(dict(method='session/update',params=dict(sessionId='acp-fixture',update=dict(
                sessionUpdate='tool_call',toolCallId='read-input',title='读取输入信息',status='completed',content=[]))))
            send(dict(method='session/update',params=dict(sessionId='acp-fixture',update=dict(
                sessionUpdate='agent_message_chunk',messageId='message-2',content=dict(type='text',text='已找到输入，下一步可以开发过滤流程。')))))
            send(dict(id=value['id'],result=dict(stopReason='end_turn')))
            continue
        (Path.home() / 'received-request.txt').write_text(params['prompt'][0]['text'])
        if '## 当前版本 DemiFlow 原生 API' in params['prompt'][0]['text'] or '# DemiForge 项目开发' in params['prompt'][0]['text']:
            (Path.home() / 'received-context.txt').write_text(params['prompt'][0]['text'])
        if 'stream fixture' in params['prompt'][0]['text']:
            send(dict(method='session/update',params=dict(sessionId='acp-fixture',update=dict(
                sessionUpdate='agent_message_chunk',content=dict(type='text',text='First streamed paragraph.\n\n'+'Streaming fixture line.\n\n'*60)))))
            until=time.monotonic()+20
            while not (root/'release-stream').exists() and time.monotonic()<until:time.sleep(.05)
            send(dict(method='session/update',params=dict(sessionId='acp-fixture',update=dict(
                sessionUpdate='agent_message_chunk',content=dict(type='text',text='Second streamed paragraph.')))))
            send(dict(id=value['id'],result=dict(stopReason='end_turn')))
            continue
        if 'interactive edit fixture' in params['prompt'][0]['text']:
            prompt_id = value['id']
            send(dict(method='session/update', params=dict(sessionId='acp-fixture', update=dict(
                sessionUpdate='tool_call', toolCallId='edit', title='Update code.py', kind='edit',
                rawInput=dict(path='code.py', text='# fixture addition')))))
            send(dict(id='permission', method='session/request_permission', params=dict(sessionId='acp-fixture',
                toolCall=dict(toolCallId='edit'), options=[dict(optionId='once',kind='allow_once',name='Allow once'),
                    dict(optionId='no',kind='reject_once',name='Reject')])) )
            continue
        subprocess.Popen([sys.executable, '-c', 'import os,time;os.setsid();time.sleep(90)'])
        result = dict(stopReason='end_turn')
    elif value.get('id') == 'permission':
        if value.get('result', {}).get('outcome', {}).get('optionId') == 'once':
            with (root / 'code.py').open('a') as stream:
                stream.write('# fixture addition\n')
            send(dict(method='session/update', params=dict(sessionId='acp-fixture', update=dict(
                sessionUpdate='agent_message_chunk', content=dict(type='text', text='## Fixture result\n\nUpdated **code.py**.')))))
        send(dict(id=prompt_id, result=dict(stopReason='end_turn')))
        continue
    elif method == 'session/close':
        send(dict(id=value['id'], error=dict(code=-32603, message='injected close failure')))
        continue
    else:
        continue
    send(dict(id=value['id'], result=result))
