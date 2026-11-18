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
    elif method in {'thread/start','thread/resume'}:
        project_root = Path(params['cwd'])
        if '受信任务上下文' in params.get('developerInstructions', ''):
            (Path.home() / 'received-context.txt').write_text(params['developerInstructions'])
        result={'cwd':params['cwd'],'model':params['model'],'approvalPolicy':'on-request','sandbox':{'type':'dangerFullAccess'},'thread':{'id':thread}}
    elif method=='thread/compact/start':
        send({'id':value['id'],'result':{}})
        event('turn/started',{'threadId':thread,'turn':{'id':turn}})
        event('item/completed',{'threadId':thread,'turnId':turn,'item':{'id':'compact','type':'contextCompaction'}})
        event('turn/completed',{'threadId':thread,'turn':{'id':turn,'status':'completed'}})
        continue
    elif method=='turn/start':
        send({'id':value['id'],'result':{'turn':{'id':turn}}})
        event('turn/started',{'threadId':thread,'turn':{'id':turn}})
        message=params['input'][0]['text']
        if message.startswith('managed-edit'):
            with (project_root / 'orders/operators/map.py').open('a') as stream:
                stream.write('# managed fixture change\n')
        if message.startswith('managed-entry'):
            with (project_root / 'orders/orders_pipeline.py').open('a') as stream:
                stream.write('# managed entry fixture change\n')
        if message.startswith('managed-norm-alter'):
            with (project_root.parent / 'AGENTS.md').open('a') as stream:
                stream.write('Changed ancestor fixture requirement.\n')
        if message.startswith('disconnect'): sys.exit(3)
        if message.startswith('hang'):
            subprocess.Popen([sys.executable,'-c','import os,time;os.setsid();time.sleep(90)'])
            continue
        if message.startswith('input'):
            send({'id':'server-question','method':'item/tool/requestUserInput','params':{'threadId':thread,'turnId':turn,'itemId':'question',
                'isBlocking':True,'questions':[{'id':'q1','header':'Choice','question':'Which value?','options':None,'isOther':True,'isSecret':False}]}})
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
