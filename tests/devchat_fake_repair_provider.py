"""Deterministic coding-harness fixture for bounded controller repairs."""
import json
import os
from pathlib import Path
import sys

root = None
thread = 'repair-thread'
turn = None


def send(value):
    print(json.dumps(value), flush=True)


def event(method, params):
    send(dict(method=method, params=params))


for line in sys.stdin:
    value = json.loads(line)
    method, params = value.get('method'), value.get('params', {})
    if method == 'initialize':
        result = dict(userAgent='codex/0.164.0')
    elif method == 'config/read':
        result = dict(config=dict(features=dict(apps=False, hooks=False, multi_agent=False, multi_agent_v2=False),
                                  shell_environment_policy=dict(inherit='none'), web_search='disabled'))
    elif method in {'thread/start', 'thread/resume'}:
        root = Path(params['cwd'])
        (Path.home() / 'received-context.txt').write_text(params.get('developerInstructions', ''))
        result = dict(cwd=str(root), model=params['model'], approvalPolicy=params['approvalPolicy'],
                      sandbox=dict(type='dangerFullAccess'), thread=dict(id=thread))
    elif method == 'turn/start':
        message = params['input'][0]['text']
        submission = params['clientUserMessageId']
        turn = 'provider-' + submission
        if os.environ.get('DEMIFLOW_MODEL_ENDPOINT'):
            from urllib.request import Request, urlopen
            request = Request('http://' + os.environ['DEMIFLOW_MODEL_ENDPOINT'] + '/v1/responses',
                data=json.dumps(dict(model='fixture', input='fixture development transport', tools=[], store=False)).encode(),
                headers={'Content-Type':'application/json', 'Authorization':'Bearer '+os.environ['DEMIFLOW_MODEL_TOKEN']})
            with urlopen(request, timeout=5) as response:
                assert json.load(response)['status'] == 'completed'
            for forbidden, edit in [(Path.home().parent.parent/'chat.sqlite3', False), (root/'AGENTS.md', True)]:
                try:
                    if edit: forbidden.write_text('forbidden')
                    else: forbidden.read_bytes()
                except PermissionError: pass
                else: raise AssertionError('Restricted fixture reached protected control or authority')
        with (Path.home() / 'provider-calls.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(submission=submission, message=message)) + '\n')
        source = root / 'orders/operators/map.py'
        mode = Path.home() / 'repair-mode'
        if message.startswith('repair-fixture:'):
            mode.write_text(message.split(':', 1)[1])
            source.write_text('# repaired\n' if mode.read_text().startswith('review') else '# pending repair\n')
        elif mode.read_text() == 'success':
            source.write_text('# repaired\n')
        elif mode.read_text() == 'progress':
            with source.open('a') as stream: stream.write('# another unsuccessful attempt\n')
        elif mode.read_text() == 'review':
            source.write_text('# repaired\n# reviewed\n')
        elif mode.read_text() == 'review-exhaust':
            with source.open('a') as stream: stream.write('# inconclusive review repair\n')
        elif mode.read_text() == 'disconnect':
            sys.exit(3)
        elif mode.read_text() == 'hold':
            import time
            time.sleep(60)
        send(dict(id=value['id'], result=dict(turn=dict(id=turn))))
        event('turn/started', dict(threadId=thread, turn=dict(id=turn)))
        if mode.read_text() == 'cancel-approval': continue
        event('item/completed', dict(threadId=thread, turnId=turn, item=dict(id='answer', type='agentMessage', text='Fixture provider segment ended.')))
        event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='completed')))
        continue
    elif method == 'turn/interrupt':
        if (Path.home()/'repair-mode').read_text() == 'cancel-approval':
            (Path.home()/'late-approval-issued').write_text('request emitted after interrupt')
            event('item/started', dict(threadId=thread, turnId=turn,
                item=dict(id='late-change', type='fileChange', changes=[dict(path=str(root/'orders/operators/map.py'))])))
            send(dict(id='late-approval', method='item/fileChange/requestApproval',
                params=dict(threadId=thread, turnId=turn, itemId='late-change')))
            send(dict(id=value['id'], result={}))
            continue
        event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='interrupted')))
        result = {}
    elif method is None and value.get('id') == 'late-approval':
        (Path.home()/'late-approval-reply.json').write_text(json.dumps(value))
        if value.get('result', {}).get('decision') == 'accept':
            (root/'orders/operators/map.py').write_text('# forbidden after cancellation\n')
        event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='interrupted')))
        continue
    else:
        continue
    send(dict(id=value['id'], result=result))
