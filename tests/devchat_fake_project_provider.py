"""Offline project conversation fixture; exercises the real native bridge."""
import json
from pathlib import Path
import sys

root = None
turn = None
thread = 'project-conversation-fixture'


def send(value):
    print(json.dumps(value), flush=True)


def event(method, params):
    send(dict(method=method, params=params))


def complete(text='The project request is complete.'):
    event('item/completed', dict(threadId=thread, turnId=turn,
        item=dict(id='answer', type='agentMessage', text=text)))
    event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='completed')))


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
        (Path.home()/'received-context.txt').write_text(params.get('developerInstructions', ''))
        result = dict(cwd=str(root), model=params['model'], approvalPolicy=params['approvalPolicy'],
            sandbox=dict(type='dangerFullAccess'), thread=dict(id=thread))
    elif method == 'turn/start':
        message = params['input'][0]['text']
        turn = 'provider-' + params['clientUserMessageId']
        send(dict(id=value['id'], result=dict(turn=dict(id=turn))))
        event('turn/started', dict(threadId=thread, turn=dict(id=turn)))
        if message == 'repair-fixture:project-initial':
            with (root/'orders/operators/map.py').open('a') as stream:
                stream.write('\n# Initial project requirement implemented.\n')
            complete()
        elif message in {'Create an image quality operator','repair-fixture:native-question'}:
            send(dict(id='question', method='item/tool/call', params=dict(threadId=thread, turnId=turn,
                tool='development_protocol', arguments=dict(operation='note', arguments=dict(kind='question',
                    text='What minimum image width should the local quality filter retain?')))))
        elif message == 'Keep images whose width is at least 64 pixels':
            send(dict(id='resolve', method='item/tool/call', params=dict(threadId=thread, turnId=turn,
                tool='development_protocol', arguments=dict(operation='resolve_question',
                    arguments=dict(note_index=int((Path.home()/'question-index').read_text()))))))
        elif message == 'Add an image quality flow and its reusable operator':
            for name in ('flows','operators','tests'):
                (root/'orders'/name).mkdir(exist_ok=True)
            (root/'orders/operators/image_quality.py').write_text('def quality(width):\n    return width >= 64\n')
            (root/'orders/flows/image_quality.py').write_text('from ..operators.image_quality import quality\ndef run(*, width: int = 64):\n    return quality(width)\n')
            (root/'orders/flows/image_quality.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments=dict(width=64))))
            (root/'orders/tests/test_image_quality.py').write_text('import unittest\nfrom orders.operators.image_quality import quality\nclass Quality(unittest.TestCase):\n    def test_boundary(self):\n        self.assertFalse(quality(63))\n        self.assertTrue(quality(64))\n')
            for protected in (root/'workspace.json',root/'.demiforge/project.json'):
                try: protected.open('a').close()
                except PermissionError: pass
                else: raise AssertionError('Provider can write a protected project control')
            send(dict(id='register', method='item/tool/call', params=dict(threadId=thread, turnId=turn,
                tool='development_protocol', arguments=dict(operation='register_flow',arguments=dict(
                    flow_id='image_quality',path='orders/flows/image_quality.py',symbol='run',params='orders/flows/image_quality.json')))))
        else:
            complete()
        continue
    elif method is None and value.get('id') in {'question', 'resolve', 'register'}:
        result = value['result']
        assert result['success'], result
        content = json.loads(result['contentItems'][0]['text'])
        if value['id'] == 'question':
            (Path.home()/'question-index').write_text(str(content['note_index']))
            if message=='repair-fixture:native-question':
                send(dict(id='native-answer',method='item/tool/requestUserInput',params=dict(threadId=thread,turnId=turn,
                    itemId='threshold',isBlocking=True,questions=[dict(id='width',header='Width',
                        question='What minimum width?',options=None,isOther=True,isSecret=False)])))
            else:
                complete('What minimum image width should the local quality filter retain?')
        elif value['id']=='resolve':
            (root/'orders/operators/image_quality.py').write_text('def quality(width):\n    return width >= 64\n')
            complete('The local image quality operator now retains widths of at least 64 pixels.')
        else:
            assert content['status']=='queued_after_provider_drain'
            assert 'image_quality' not in json.loads((root/'.demiforge/project.json').read_text())['projects']['orders']['flows']
            assert 'orders/flows/image_quality.py:run' not in json.loads((root/'workspace.json').read_text())['entries']
            complete('The image quality flow implementation and registration request are ready.')
        continue
    elif method is None and value.get('id')=='native-answer':
        assert value['result']['answers']['width']['answers']==['64']
        send(dict(id='resolve', method='item/tool/call', params=dict(threadId=thread, turnId=turn,
            tool='development_protocol',arguments=dict(operation='resolve_question',
                arguments=dict(note_index=int((Path.home()/'question-index').read_text()))))))
        continue
    elif method == 'turn/interrupt':
        event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='interrupted')))
        result = {}
    else:
        continue
    send(dict(id=value['id'], result=result))
