"""Small authenticated project routes. Events use bounded cursor long polling."""
import json
import re
import threading
import time

from demiflow.workbench.conversations.common import integer
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import wire

GET_FIELDS = {
    'dev-capabilities': (set(), set()),
    'dev-conversation': (set(),set()),
    'dev-project-turns': (set(),{'offset','limit','kind'}),
    'dev-sessions': (set(), {'offset', 'limit'}),
    'dev-session': ({'session_id'}, set()),
    'dev-context': ({'session_id', 'turn_id'}, set()),
    'dev-events': ({'session_id'}, {'after_sequence', 'limit', 'wait_ms'}),
    'dev-output': ({'session_id', 'item_id'}, {'after_sequence'}),
}
POST_FIELDS = {
    'dev-compact': ({'session_id','submission_id','expected_revision'},set()),
    'dev-session-create': ({'request_id', 'provider_id'}, set()),
    'dev-turn-start': ({'session_id', 'submission_id', 'expected_revision', 'text'}, set()),
    'dev-input': ({'session_id', 'turn_id', 'pending_request_id', 'expected_digest', 'answer'}, set()),
    'dev-cancel': ({'session_id', 'turn_id', 'request_id'}, set()),
    'dev-recover': ({'session_id', 'expected_revision'}, set()),
}


def disabled():
    return dict(format='demiflow_development_chat_v1', enabled=False, start=False, mode=None,
                service_configured=False, project_authorized=False,
                reason='当前服务尚未启用项目开发。已有运行许可不会自动授予开发权限。', providers=[])


def get(server, workspace, root, name, query, *, consumer_closed=None):
    for key in ('offset', 'limit', 'after_sequence', 'wait_ms'):
        if key in query and (type(query[key]) is not str or not re.fullmatch(r'[0-9]{1,16}', query[key])):
            reject('Invalid integer query field: ' + key, 'invalid_request')
    service = server.development
    if name == 'dev-capabilities':
        if service is not None and getattr(service, 'remote', False):
            return service.capabilities(workspace, root, consumer_closed=consumer_closed)
        return service.capabilities(workspace, root) if service else disabled()
    if service is None: reject('Project development is not enabled')
    service.project(workspace, root)
    if getattr(service, 'remote', False):
        return service.forward(workspace, root, name, query, consumer_closed=consumer_closed)
    if name=='dev-conversation':return service.conversation(workspace,root)
    if name=='dev-project-turns':return service.store.project_turns(workspace,int(query.get('offset',0)),int(query.get('limit',20)),query.get('kind','all'))
    sid = query.get('session_id')
    if name == 'dev-sessions':
        return service.store.list(workspace, int(query.get('offset', 0)), int(query.get('limit', 20)))
    if name == 'dev-session': return service.show(workspace, sid)
    if name == 'dev-context':
        service.store.show(workspace, sid)
        turn = service.store.turn(query['turn_id'])
        if turn['session'] != sid: reject('Turn does not belong to session', 'not_found')
        value = json.loads(turn['context']) if turn['context'] else None
        if value is not None:
            # The context provenance is bounded; full source text lives at its
            # canonical source, never an unbounded browser transcript field.
            value.pop('binding', None)
            value['delivery'] = turn['delivery']
        return dict(context=value)
    after = int(query.get('after_sequence', 0))
    if name == 'dev-output':
        page = service.store.events(workspace, sid, after, 50)
        chunks = []
        item_id = query['item_id']
        for e in page['events']:
            data, kind = e['data'], e['type']
            matches = (data.get('item_id') == item_id or data.get('id') == item_id or
                       (kind == 'message.user' and item_id == e['turn_id'] + ':user') or
                       (kind == 'change.observed' and item_id == 'change:' + str(e['sequence'])) or
                       (kind == 'check.reference' and item_id == 'check:' + str(e['sequence'])))
            if matches:
                chunks.append(dict(sequence=e['sequence'], type=kind,
                                   text=data.get('text', wire(data, 65536).decode())))
        return dict(chunks=chunks, next_sequence=page['next_sequence'], last_sequence=page['last_sequence'])
    wait_ms = integer(int(query.get('wait_ms', 0)), 0, 1500)
    if not server.development_readers.acquire(blocking=False): reject('Development subscriptions are full', 'busy')
    try:
        until = time.monotonic() + wait_ms / 1000
        while True:
            result = service.store.events(workspace, sid, after, int(query.get('limit', 50)))
            if result['events'] or time.monotonic() >= until: return result
            time.sleep(.1)
    finally:
        server.development_readers.release()


def post(server, workspace, root, name, request, *, consumer_closed=None):
    service = server.development
    if service is None: reject('Project development is not enabled')
    if getattr(service, 'remote', False):
        return service.forward(workspace, root, name, request, mutation=True, consumer_closed=consumer_closed)
    actions = {'dev-session-create': service.create_project, 'dev-turn-start': service.start_project, 'dev-compact': service.compact, 'dev-input': service.input,
               'dev-cancel': service.cancel, 'dev-recover': service.recover}
    return actions[name](workspace, root, **request)
