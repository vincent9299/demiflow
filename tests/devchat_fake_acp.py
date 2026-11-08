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
        result = dict(sessionId='acp-fixture', configOptions=[dict(id='model', currentValue='["deepseek-official","fixture"]')])
    elif method == 'session/compact':
        if (root/'compact-fail').exists():
            send(dict(id=value['id'],error=dict(code=-32603,message='fixture compaction failed')));continue
        result=dict(status='compacted',compaction_id='fixture-summary',shadowed_items=4,estimated_tokens_before=1200)
    elif method == 'session/prompt':
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
