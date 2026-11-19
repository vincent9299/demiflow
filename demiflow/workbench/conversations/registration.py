"""Deferred, additive project registration inside the existing request lifecycle.

The untrusted harness can enqueue bounded typed intents. Only the drained gate
publishes controller-signed metadata transitions under the workspace/Store locks.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time

from demiflow.forge.io import no_symlinks
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.workbench.conversations.common import digest, reject, root_identity, text, wire
from demiflow.workbench.projects.io import control_lock, write_control
from demiflow.workbench.projects.workspace import plan_flow_registration


def _raw(path):
    path = no_symlinks(Path(path))
    if path.stat().st_nlink != 1: reject('Registration controls cannot be hard linked', 'conflict')
    return read_bounded(path, 1024**2).decode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw.encode()).hexdigest()


def _policy(deployment, session):
    value=json.loads(wire(deployment.value))
    for project in value['projects'].values(): project.pop('identity',None)
    if digest(value)!=session['policy_hash']:
        reject('Project registration deployment changed','conflict')


def _request_id(turn, intents):
    return 'registration:'+turn['id']+':'+digest([item['intent_id'] for item in intents])[:16]


def _pinned(db, store, turn, session, project, *, active):
    current = dict(db.execute('SELECT * FROM turns WHERE id=?', (turn['id'],)).fetchone())
    actual = dict(db.execute('SELECT * FROM sessions WHERE id=?', (session['id'],)).fetchone())
    primary = db.execute('SELECT session FROM project_conversations WHERE workspace=?', (session['workspace'],)).fetchone()
    latest = db.execute('SELECT id FROM turns WHERE session=? ORDER BY created DESC,id DESC LIMIT 1', (session['id'],)).fetchone()
    if (current['epoch'] != turn['epoch'] or current['session'] != session['id'] or
            actual['policy_hash'] != session['policy_hash'] or actual['root'] != session['root'] or
            root_identity(project['root']) != json.loads(session['root']) or
            primary is None or primary['session'] != session['id'] or latest is None or latest['id'] != turn['id']):
        reject('Project registration binding changed', 'conflict')
    control = json.loads(current['development_control'])
    if active and (current['stop_at'] or time.time() >= control['deadline'] or
            current['dispatch'] != 'acknowledged' or current['delivery'] != 'acknowledged'):
        reject('Project registration request is no longer active', 'conflict')
    return current, control


def enqueue_registration(bridge, arguments):
    """No control-file writes or verification run in this RPC handler."""
    if type(arguments) is not dict or set(arguments) != {'flow_id','path','symbol','params'}:
        reject('register_flow requires only flow_id, path, symbol and params')
    flow = {key: text(value, 256) for key,value in arguments.items()}
    from demiflow.forge.development.project import safe_id
    safe_id(flow['flow_id'])
    if not flow['symbol'].isidentifier(): reject('Flow symbol must be an identifier')
    turn = bridge.store.turn(bridge.tid)
    session = bridge.store.session(turn['session'])
    current = bridge.managed_tasks.status(bridge.task_id)
    if not current.get('source_fingerprint') or current.get('development_state') == 'needs_input':
        reject('Resolve project requirements and authority before registering a flow', 'conflict')
    root = Path(bridge.project['root'])
    controls = dict(project_sha256=_sha(_raw(root/'.demiforge/project.json')),
                    workspace_sha256=_sha(_raw(root/'workspace.json')))
    # Cheap typed admission is repeated by both owners after provider drain.
    from demiflow.forge.development.project import Project
    declared = Project(root); owner = declared.pipeline(bridge.project['pipeline'])['path']
    for key in ('path','params'):
        value = flow[key]
        if (Path(value).is_absolute() or str(Path(value)) != value or '..' in Path(value).parts or
                not value.startswith(owner+'/') or
                not all(any(value == p or value.startswith(p+'/') for p in bridge.project[group]) for group in ('writes','watch'))):
            reject('Flow registration source and parameters must remain in the authorized owner scope')
        target = no_symlinks(root/value)
        if (target.stat().st_nlink != 1 or
                any(target == (root/p).resolve() or (root/p).resolve() in target.parents for p in declared.config['readonly'])):
            reject('Flow registration source and parameters must remain in the authorized owner scope')
    if not flow['path'].endswith('.py'): reject('Flow entry must be Python source')
    intent_id = digest(flow)
    with bridge.store.connect(write=True) as db:
        current_turn, control = _pinned(db, bridge.store, turn, session, bridge.project, active=True)
        if current_turn['cleanup'] == 'complete': reject('Provider already drained', 'conflict')
        intents = control.setdefault('registration_intents', [])
        previous = next((item for item in intents if item['intent_id'] == intent_id), None)
        if previous:
            return dict(status=previous['status'], intent_id=intent_id, duplicate=True)
        if len(intents) >= 8: reject('A request can add at most eight flows', 'resource_limit')
        if any(item['flow']['flow_id'] == flow['flow_id'] or item['flow']['path'] == flow['path'] for item in intents):
            reject('Flow registration intent conflicts with an earlier request', 'conflict')
        intents.append(dict(intent_id=intent_id, flow=flow, epoch=turn['epoch'], status='queued',
            queued_fingerprint=current['source_fingerprint'], **controls))
        bridge.store._update(db, turn['id'], dict(development_control=wire(control,32768).decode()))
        bridge.store._event(db,session['id'],turn['id'],turn['epoch'],'project.registration.queued',
            dict(intent_id=intent_id,flow=flow))
    return dict(status='queued_after_provider_drain', intent_id=intent_id, duplicate=False)


def publish_control(record):
    """Exact raw-byte CAS, with the existing owner writer and serialization."""
    path = Path(record['path']); raw = _raw(path)
    if _sha(raw) == record['after_sha256']: return
    if _sha(raw) != record['before_sha256']:
        reject('Registration control contains an unrecognized concurrent change', 'conflict')
    after = json.loads(record['after_raw'])
    if bounded_json(after,1024**2).decode() != record['after_raw']:
        reject('Registration plan uses an unsupported control serialization', 'conflict')
    write_control(path,after,expected=content_digest(json.loads(raw)))
    if _sha(_raw(path)) != record['after_sha256']:
        reject('Registration publication changed unexpectedly', 'conflict')


def _publish(plan):
    # Validate both first; never overwrite a foreign update to the second file.
    for key in ('project','workspace'):
        record=plan[key]
        if _sha(_raw(record['path'])) not in {record['before_sha256'],record['after_sha256']}:
            reject('Registration recovery found unknown control bytes', 'conflict')
    for key in ('project','workspace'): publish_control(plan[key])


def apply_registrations(deployment, store, turn, session, project, tasks):
    """Publish one drained batch. Caller schedules the ordinary bounded continuation."""
    control=json.loads(turn.get('development_control') or '{}')
    intents=[item for item in control.get('registration_intents',[]) if item['status']=='queued']
    if not intents: return None
    _policy(deployment,session)
    flows=[item['flow'] for item in intents]
    workspace=plan_flow_registration(project['root'],flows)
    candidate=tasks.status(session['id'])
    if candidate.get('development_state')=='needs_input' or not candidate.get('source_fingerprint'):
        reject('Project registration requires resolved input and current authority', 'conflict')
    request_id=_request_id(turn,intents)
    from demiflow.workbench.conversations.development import human_decisions
    with control_lock(project['root']):
        with store.connect(write=True) as db:
            current, control=_pinned(db,store,turn,session,project,active=True)
            if current['cleanup']!='complete' or current['state']!='completed':
                reject('Project registration requires a completely drained provider', 'conflict')
            root=Path(project['root'])
            project_sha=_sha(_raw(root/'.demiforge/project.json'))
            workspace_sha=_sha(_raw(root/'workspace.json'))
            if (any(item['project_sha256']!=project_sha or item['workspace_sha256']!=workspace_sha for item in intents)
                    or workspace['before_sha256']!=workspace_sha):
                reject('Project controls changed after the registration request', 'conflict')
            plan=tasks.prepare_registration(session['id'],request_id=request_id,
                event_ids=[part['event_id'] for part in human_decisions(turn)],
                expected_fingerprint=candidate['source_fingerprint'], flows=flows,
                workspace_before=workspace['before_raw'],workspace_after=workspace['after_raw'])
            _publish(plan)
            result=tasks.commit_registration(session['id'],plan['revision_id'])
            for item in control['registration_intents']:
                if item['intent_id'] in {i['intent_id'] for i in intents}: item.update(status='applied',revision_id=plan['revision_id'])
            store._update(db,turn['id'],dict(development_control=wire(control,32768).decode(),delivery='stale'))
            store._event(db,session['id'],turn['id'],turn['epoch'],'project.registration.applied',
                dict(revision_id=plan['revision_id'],flows=flows))
            return result


def recover_registrations(deployment, store, turn, session, project, tasks):
    """Reconcile only a signed metadata transaction; never resend model/test work."""
    control=json.loads(turn.get('development_control') or '{}')
    intents=[item for item in control.get('registration_intents',[]) if item['status']=='queued']
    if not intents: return None
    _policy(deployment,session)
    plan=tasks.registration_plan(session['id'],request_id=_request_id(turn,intents))
    if not plan: return None
    with control_lock(project['root']):
        with store.connect(write=True) as db:
            current,control=_pinned(db,store,turn,session,project,active=False)
            if current['cleanup']!='complete': reject('Registration recovery requires drained developer cleanup','conflict')
            states=[_sha(_raw(plan[k]['path'])) for k in ('project','workspace')]
            if plan['status']=='committed':
                if any(sha!=plan[k]['after_sha256'] for sha,k in zip(states,('project','workspace'))):
                    reject('Committed registration declarations changed','conflict')
                result=tasks.commit_registration(session['id'],plan['revision_id'])
                action='reconciled'
            elif all(sha==plan[k]['before_sha256'] for sha,k in zip(states,('project','workspace'))):
                result=tasks.abort_registration(session['id'],plan['revision_id'])
                action='aborted'
            else:
                _publish(plan)
                result=tasks.commit_registration(session['id'],plan['revision_id'])
                action='reconciled'
            for item in control.get('registration_intents',[]):
                if item['status']=='queued': item.update(status=action,revision_id=plan['revision_id'])
            store._update(db,turn['id'],dict(development_control=wire(control,32768).decode(),delivery='stale'))
            store._event(db,session['id'],turn['id'],turn['epoch'],'project.registration.'+action,
                dict(revision_id=plan['revision_id'],acceptance=False))
            return result
