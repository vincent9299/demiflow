"""Offline Codex protocol fixture: native scope stays narrow, input reads do not."""
import json
import os
from pathlib import Path
import sys

root = None
turn = None
thread = 'input-discovery-fixture'


def send(value):
    print(json.dumps(value), flush=True)


def event(method, params):
    send(dict(method=method, params=params))


def call(identifier, arguments):
    send(dict(id=identifier, method='item/tool/call', params=dict(threadId=thread,
        turnId=turn, tool='development_protocol', arguments=dict(operation='input', arguments=arguments))))


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
        assert 'input' in params['dynamicTools'][0]['inputSchema']['properties']['operation']['enum']
        result = dict(cwd=str(root), model=params['model'], approvalPolicy=params['approvalPolicy'],
            sandbox=dict(type='dangerFullAccess'), thread=dict(id=thread))
    elif method == 'turn/start':
        turn = 'provider-' + params['clientUserMessageId']
        send(dict(id=value['id'], result=dict(turn=dict(id=turn))))
        event('turn/started', dict(threadId=thread, turn=dict(id=turn)))
        event('item/completed', dict(threadId=thread, turnId=turn,
            item=dict(id='progress', type='agentMessage', text='我先检查已有项目附近的数据来源。')))
        external = root.with_name(root.name + '-inputs')
        try:
            (external / 'input.txt').read_text()
        except PermissionError:
            pass
        else:
            raise AssertionError('Native CLI unexpectedly gained host-wide reading')
        assert 'DEMIFLOW_INPUT_DENIED_PATHS' not in os.environ
        call('discover', {})
        continue
    elif method is None and value.get('id') in {'discover', 'read', 'inspect', 'private', 'ready'}:
        result = value['result']
        if value['id'] in {'private', 'ready'}:
            assert result['success'] is False, result
            if value['id'] == 'private':
                call('ready', dict(action='read', path=str(external / 'ready.json')))
            else:
                event('item/completed', dict(threadId=thread, turnId=turn,
                    item=dict(id='result', type='agentMessage', text='已读取项目外输入并检查 score 字段；没有修改输入或输出。')))
                event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='completed')))
            continue
        assert result['success'], result
        content = json.loads(result['contentItems'][0]['text'])
        if value['id'] == 'discover':
            assert external.name in json.dumps(content)
            call('read', dict(action='read', path=str(external / 'input.txt')))
        elif value['id'] == 'read':
            assert 'external-input-fixture' in json.dumps(content)
            call('inspect', dict(action='inspect', path=str(external / 'input.parquet')))
        else:
            assert 'score' in json.dumps(content)
            call('private', dict(action='read', path=str(external / 'session-key.opaque')))
        continue
    elif method == 'turn/interrupt':
        event('turn/completed', dict(threadId=thread, turn=dict(id=turn, status='interrupted')))
        result = {}
    else:
        continue
    send(dict(id=value['id'], result=result))
