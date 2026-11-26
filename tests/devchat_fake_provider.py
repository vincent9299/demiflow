"""Pinned-protocol fixture; never a model or production selectable provider."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

scenario = os.environ.get('DEVCHAT_FAKE_SCENARIO', 'normal')
thread = 'thread-fixture'
turn = 'turn-fixture'
project_root = None

def send(value):
    print(json.dumps(value), flush=True)

def event(method, params): send(dict(method=method, params=params))

for line in sys.stdin:
    value=json.loads(line); method=value.get('method'); params=value.get('params', {})
    if method=='initialize': result={'userAgent':'codex/0.164.0'}
    elif method=='config/read': result={'config':{'features':dict(apps=False,hooks=False,multi_agent=False,multi_agent_v2=False),
        'shell_environment_policy':{'inherit':'none'},'web_search':'disabled'}}
    elif method == 'config/batchWrite':
        new_servers = params['edits'][0]['value']
        result = {}
    elif method == 'config/mcpServer/reload':
        server = new_servers['development-protocol']
        request = json.dumps(dict(id=1, method='tools/list', params={})) + '\n'
        reply = subprocess.check_output([server['command'], *server['args']], input=request, text=True)
        tools = {tool['name']:tool for tool in json.loads(reply)['result']['tools'] if tool['name'] != 'read_input'}
        result = {}
    elif method == 'mcpServerStatus/list':
        result = dict(data=[dict(name='development-protocol', tools=tools)])
    elif method in {'thread/start','thread/resume'}:
        project_root = Path(params['cwd'])
        instructions = params.get('developerInstructions', '')
        if '## 当前版本 DemiFlow 原生 API' in instructions or '# DemiForge 项目开发' in instructions:
            (Path.home() / 'received-context.txt').write_text(params['developerInstructions'])
        result={'cwd':params['cwd'],'model':params['model'],'approvalPolicy':'on-request','sandbox':{'type':'dangerFullAccess'},'thread':{'id':thread}}
    elif method=='thread/compact/start':
        send({'id':value['id'],'result':{}})
        event('turn/started',{'threadId':thread,'turn':{'id':turn}})
        event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'compact','type':'contextCompaction'}})
        event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
        continue
    elif method=='turn/steer':
        assert params['threadId']==thread and params['expectedTurnId']==turn
        message = params['input'][0]['text']
        send({'id':value['id'],'result':{'turnId':turn}})
        if message=='继续汇总':
            event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'answer','type':'agentMessage','text':message}})
            event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
        continue
    elif method=='turn/start':
        send({'id':value['id'],'result':{'turn':{'id':turn}}})
        event('turn/started',{'threadId':thread,'turn':{'id':turn}})
        message=params['input'][0]['text']
        if '## 当前版本 DemiFlow 原生 API' in instructions or '# DemiForge 项目开发' in instructions:
            (Path.home() / 'received-request.txt').write_text(message)
        if message.startswith('generate notebook widget fixture'):
            assert 'ipywidgets' in instructions and '当前 Notebook' in instructions
            # Strict cell-only isolation (commit 2be73c09): notebook_cell runs in its
            # private provider home with neither read nor write access to the project root.
            assert project_root.resolve() == Path.home().resolve(), (project_root, Path.home())
            parts = message.split(' ', 4)
            target_note = Path(parts[4]) if len(parts) == 5 else (project_root / 'notes.py')
            try:
                target_note.read_text()
            except PermissionError:
                pass
            else:
                raise AssertionError('Notebook generation unexpectedly received project read access')
            try:
                target_note.open('ab').close()
            except PermissionError:
                pass
            else:
                raise AssertionError('Notebook generation received write access')
            answer = '已生成隔离 Notebook 卡片。\n```python\nfrom ipywidgets import IntSlider\nIntSlider()\n```'
            event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'answer','type':'agentMessage','text':answer}})
            event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
            continue
        if message.startswith('guided fixture edit'):
            assert ('Dataset.map_prompt_async' in instructions and 'DF-02' in instructions) or (
                '# DemiForge 项目开发' in instructions and 'demiflow forge check' in instructions)
            flow = project_root / 'flows' / 'guided.py'
            if message.endswith('native'):
                flow.write_text("from demiflow.data import from_items\ndef run():\n    return from_items([{'id': 1}]).materialize()\n")
            elif message.endswith('manual'):
                flow.write_text("def run():\n    return [2, 4]\n")
            elif message.endswith('unproved'):
                flow.write_text("def run():\n    return [value * 2 for value in [1, 2]]\n")
            elif message.endswith('prompt'):
                flow.write_text("from demiflow.data import from_items\ndef run():\n"
                    "    return from_items([{'payload': 'text'}]).map_prompt_async('enrich', "
                    "config='prompts/guided.yaml', inputs={'payload': 'payload'}, output='answer', "
                    "call_output='call', error_output='error', max_requests=1, "
                    "options={'trust_env':False, 'timeout_s':2, 'collect_journal_totals':False, 'max_request_bytes':1048576, 'max_response_bytes':1048576, "
                    "'model_revision':'fixture-v1'}).materialize()\n")
            elif message.endswith('crossflow'):
                flow.write_text("from flows.other import source\ndef run():\n    return source().materialize()\n")
            elif message.endswith('layout'):
                flow.write_text("from demiflow.data import from_items\nfrom lib.normalize import normalize\ndef run():\n    return from_items([{'name': 'A'}]).map(normalize).materialize()\n")
            elif message.endswith('misplaced'):
                (project_root / 'prompts' / 'misplaced.py').write_text(
                    "from demiflow.data import from_items\ndef run():\n    return from_items([{'id': 1}]).count()\n")
        if message.startswith('disconnect'): sys.exit(3)
        if message.startswith('hang'):
            subprocess.Popen([sys.executable,'-c','import os,time;os.setsid();time.sleep(90)'])
            continue
        if message.startswith('input'):
            send({'id':'server-question','method':'item/tool/requestUserInput','params':{'threadId':thread,'turnId':turn,'itemId':'question',
                'isBlocking':True,'questions':[{'id':'q1','header':'Choice','question':'Which value?','options':None,'isOther':True,'isSecret':False}]}})
            continue
        if message.startswith('public summary fixture'):
            summary='Public summary start.\n\n'+'Summary detail line.\n'*1800+'\nSummary end.'
            event('item/reasoning/summaryTextDelta',dict(threadId=thread,turnId=turn,itemId='reason',summaryIndex=0,delta='Public summary start.'))
            event('item/completed',dict(threadId=thread,turnId=turn,item=dict(id='reason',type='reasoning',summary=[summary],content=['PRIVATE REASONING MUST NOT APPEAR'])))
            event('item/started',dict(threadId=thread,turnId=turn,item=dict(id='tool',type='commandExecution',command='fixture inspection')))
            event('item/commandExecution/outputDelta',dict(threadId=thread,turnId=turn,itemId='tool',delta='Actual tool output.'))
            answer='Long final answer start.\n\n'+'Final detail line.\n'*2000+'\nFinal answer end.'
            event('item/completed',dict(threadId=thread,turnId=turn,item=dict(id='answer',type='agentMessage',text=answer)))
            event('turn/completed',dict(threadId=thread,turn=dict(id=turn,status='completed')))
            continue
        if message.startswith('flood'):
            for _ in range(100): event('item/agentMessage/delta',{'threadId':thread,'turnId':turn,'itemId':'answer','delta':'x'*16000})
        event('item/agentMessage/delta',{'threadId':thread,'turnId':turn,'itemId':'answer','delta':'fixture answer <script>unsafe</script>'})
        event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'answer','type':'agentMessage','text':'fixture answer <script>unsafe</script>'}})
        event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
        continue
    elif method=='turn/interrupt':
        event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'interrupted'}})
        result={}
    elif method is None:
        if value.get('id')=='server-question':
            event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'answer','type':'agentMessage','text':str(value.get('result'))}})
            event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
        continue
    else: continue
    send({'id':value['id'],'result':result})
