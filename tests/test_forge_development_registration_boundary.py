"""Independent adversarial checks for controller-owned flow registration.

All projects, control files and receipts are isolated fixtures. No model or
provider process is involved, and candidate entry modules must never be imported.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.development.managed import ManagedTasks, ReviewAuthority
from demiflow.forge.development.project import ForgeError
from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError as WorkbenchError
from demiflow.workbench.conversations import registration as controller
from demiflow.workbench.conversations.common import digest, root_identity, wire
from demiflow.workbench.conversations.context import BridgeServer, ProtocolBridge
from demiflow.workbench.conversations.development import human_decisions, initial
from demiflow.workbench.conversations.mcp_bridge import call
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.projects.io import control_lock
from test_forge_development_chat import configuration
from test_forge_development_managed import pass_gates
from test_forge_development_multiflow import workspace as multiflow_workspace
from test_forge_project_management import snapshot, cached_snapshot


def _raw(value):
    return bounded_json(value, 1024 * 1024).decode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


@pytest.fixture
def registered_project(multiflow_workspace, tmp_path_factory):
    root, config, write, save = multiflow_workspace
    ws.init_workspace(root, existing=True)
    for name in ('select', 'evaluate'):
        params = f'orders/flows/{name}.json'
        write(params, _raw(dict(format='demiflow_forge_flow_parameters_v1',
                                arguments=dict(value=6))))
        ws.register_flow(root, f'orders/flows/{name}.py:run', params=params)
    write('protected-history.txt', 'Immutable business history.\n')
    config['readonly'] = ['workspace.json', 'protected-history.txt']
    project = save()
    authority = ReviewAuthority()
    state_root = tmp_path_factory.mktemp('registration-controller')
    api = ManagedTasks(state_root, 'fixture', root, review_authority=authority)
    api.prepare_task('session', 'orders', 'Develop flows in the existing project',
                     ['orders'], ['Do not alter history, permissions or verifier'],
                     contract=dict(allow_tests=True, max_verification_seconds=5,
                                   developer_session_id='developer'))
    return SimpleNamespace(root=root, project=project, api=api, authority=authority,
                           state_root=state_root, write=write,
                           task_file=api.store.root / 'session' / 'task.json')


def _state(case):
    return json.loads(case.task_file.read_text())


def _controls(case):
    return {name: (case.root / name).read_bytes()
            for name in ('.demiforge/project.json', 'workspace.json')}


def _new_flow(case, name='added'):
    path = f'orders/flows/{name}.py'
    params = f'orders/flows/{name}.json'
    case.write(path, 'raise AssertionError("registration must not import code")\n'
                    'def run(*, value: int = 7):\n    return value\n')
    case.write(params, _raw(dict(format='demiflow_forge_flow_parameters_v1',
                                arguments=dict(value=7))))
    return dict(flow_id=name, path=path, symbol='run', params=params)


def _accepted(case):
    result = pass_gates(case.api, case.authority)
    result = case.api.evaluate_acceptance('session',
        expected_fingerprint=result['source_fingerprint'])
    assert result['development_state'] == 'accepted'
    return copy.deepcopy(_state(case))


def _workspace_proposal(case, flows):
    before = (case.root / 'workspace.json').read_text()
    value = json.loads(before)
    for flow in flows:
        value['entries'][flow['path'] + ':' + flow['symbol']] = [flow['params']]
    return before, _raw(value)


def _prepare(case, flows, *, request_id='add-flow', workspace_after=None):
    case.api.note('session', 'decision', 'Add the requested flow in this project',
                  event_id='human-add-flow')
    before, after = _workspace_proposal(case, flows)
    return case.api.prepare_registration('session', request_id=request_id,
        event_ids=['human-add-flow'],
        expected_fingerprint=case.api.context('session')['fingerprint'], flows=flows,
        workspace_before=before, workspace_after=after if workspace_after is None else workspace_after)


def _publish_exact(case, plan, key):
    record = plan[key]
    path = Path(record['path'])
    assert path in {case.root / '.demiforge/project.json', case.root / 'workspace.json'}
    assert _sha(path.read_text()) == record['before_sha256']
    assert _sha(record['after_raw']) == record['after_sha256']
    path.write_text(record['after_raw'])


def _assert_original_history(case, before):
    after = _state(case)
    for key in ('binding', 'baseline', 'readonly_baseline', 'baseline_coverage',
                'goal', 'writes', 'non_goals'):
        assert after.get(key) == before.get(key), key
    assert after['managed']['acceptance'] == before['managed']['acceptance']
    assert after['review'] == before['review']


def test_domain_pending_requires_both_exact_controls_and_preserves_old_receipt(registered_project):
    case = registered_project
    original = _accepted(case)
    controls = _controls(case)
    plan = _prepare(case, [_new_flow(case)])
    assert plan['status'] == 'prepared'
    assert _controls(case) == controls  # Domain journal has no public write authority.
    assert not case.api.status('session')['ready']
    for operation in (case.api.context, case.api.check, case.api.verify,
                      case.api.evaluate_acceptance):
        with pytest.raises(ForgeError, match='pending'):
            operation('session')
    with pytest.raises(ForgeError):
        case.api.commit_registration('session', plan['revision_id'])
    _publish_exact(case, plan, 'project')
    with pytest.raises(ForgeError):
        case.api.commit_registration('session', plan['revision_id'])
    with pytest.raises(ForgeError):
        case.api.abort_registration('session', plan['revision_id'])
    _publish_exact(case, plan, 'workspace')
    result = case.api.commit_registration('session', plan['revision_id'])
    assert not result['ready'] and result['development_state'] != 'accepted'
    assert result['source_fingerprint'] != original['managed']['acceptance']['fingerprint']
    _assert_original_history(case, original)
    assert case.api.context('session')['read_pages'].get('flow-entry-added') is None


@pytest.mark.parametrize('change', ['locations', 'platform', 'audit', 'existing-params'])
def test_domain_rejects_any_workspace_delta_beyond_new_entries(registered_project, change):
    case = registered_project
    flow = _new_flow(case)
    _, proposed = _workspace_proposal(case, [flow])
    value = json.loads(proposed)
    if change == 'locations':
        value['locations']['elsewhere'] = dict(root='/tmp', writable=True)
    elif change == 'platform':
        value['platform']['digest'] = '0' * 64
    elif change == 'audit':
        value['audit']['change_scope'] = ['orders']
    else:
        value['entries']['orders/flows/select.py:run'] = [flow['params']]
    controls = _controls(case)
    with pytest.raises(ForgeError, match='Workspace patch'):
        _prepare(case, [flow], workspace_after=_raw(value))
    assert _controls(case) == controls
    assert case.api.registration_plan('session') is None


@pytest.mark.parametrize('change', ['outside', 'traversal', 'readonly', 'symlink',
                                   'old-id', 'old-entry', 'verifier'])
def test_domain_does_not_authorize_foreign_paths_or_old_registration(registered_project, change):
    case = registered_project
    flow = _new_flow(case)
    case.write('outside.json', '{}')
    if change == 'outside':
        flow['params'] = 'outside.json'
    elif change == 'traversal':
        flow['params'] = 'orders/../outside.json'
    elif change == 'readonly':
        flow['params'] = 'protected-history.txt'
    elif change == 'symlink':
        (case.root / flow['params']).unlink()
        (case.root / flow['params']).symlink_to(case.root / 'outside.json')
    elif change == 'old-id':
        flow['flow_id'] = 'select'
    elif change == 'old-entry':
        flow['path'] = 'orders/flows/select.py'
    else:
        flow['verify'] = [{'argv': ['allow-anything']}]
    controls = _controls(case)
    with pytest.raises(ForgeError):
        _prepare(case, [flow])
    assert _controls(case) == controls
    assert case.api.registration_plan('session') is None


def test_domain_unknown_bytes_block_commit_and_abort_after_restart(registered_project):
    case = registered_project
    original = copy.deepcopy(_state(case))
    plan = _prepare(case, [_new_flow(case)])
    _publish_exact(case, plan, 'project')
    foreign = json.loads((case.root / 'workspace.json').read_text())
    foreign['name'] = 'another trusted editor changed this'
    (case.root / 'workspace.json').write_text(_raw(foreign))
    controls = _controls(case)
    case.api = ManagedTasks(case.state_root, 'fixture', case.root, review_authority=case.authority)
    assert case.api.registration_plan('session', request_id='add-flow')['status'] == 'prepared'
    for operation in (case.api.commit_registration, case.api.abort_registration):
        with pytest.raises(ForgeError):
            operation('session', plan['revision_id'])
    assert _controls(case) == controls
    assert not case.api.status('session')['ready']
    _assert_original_history(case, original)


def test_domain_committed_request_is_discoverable_and_idempotent_after_restart(registered_project):
    case = registered_project
    plan = _prepare(case, [_new_flow(case)])
    _publish_exact(case, plan, 'project')
    _publish_exact(case, plan, 'workspace')
    case.api.commit_registration('session', plan['revision_id'])
    before = _state(case)
    controls = _controls(case)
    case.api = ManagedTasks(case.state_root, 'fixture', case.root, review_authority=case.authority)
    restored = case.api.registration_plan('session', request_id='add-flow')
    assert restored['status'] == 'committed' and restored['revision_id'] == plan['revision_id']
    case.api.commit_registration('session', restored['revision_id'])
    assert _state(case) == before and _controls(case) == controls
    foreign = json.loads((case.root / 'workspace.json').read_text())
    foreign['name'] = 'unapproved metadata'
    (case.root / 'workspace.json').write_text(_raw(foreign))
    result = case.api.status('session')
    assert not result['ready'] and result['source_fingerprint'] is None


def test_domain_rejects_sixty_fifth_flow_before_creating_a_journal(registered_project):
    case = registered_project
    manifest_path = case.root / '.demiforge/project.json'
    manifest = json.loads(manifest_path.read_text())
    for index in range(2, 64):
        flow = _new_flow(case, f'existing{index}')
        manifest['projects']['orders']['flows'][flow['flow_id']] = {
            key: flow[key] for key in ('path', 'symbol')}
    manifest_path.write_text(_raw(manifest))
    # This is fixture initialization for an already-full project. Its original
    # task remains untouched; the new fixture baseline is established before
    # the attempted registration.
    case.api.prepare_task('capacity', 'orders', 'Keep the existing 64-flow limit',
                          ['orders'], ['No expanded registration budget'])
    case.api.note('capacity', 'decision', 'Add another flow', event_id='human-capacity')
    flow = _new_flow(case, 'overflow')
    before, after = _workspace_proposal(case, [flow])
    controls = _controls(case)
    with pytest.raises(ForgeError, match='64 named flows'):
        case.api.prepare_registration('capacity', request_id='overflow', event_ids=['human-capacity'],
            expected_fingerprint=case.api.context('capacity')['fingerprint'], flows=[flow],
            workspace_before=before, workspace_after=after)
    assert _controls(case) == controls
    assert case.api.registration_plan('capacity') is None


@pytest.fixture
def registration_turn(registered_project, tmp_path_factory):
    case = registered_project
    _, _, value = configuration(tmp_path_factory.mktemp('registration-deployment'))
    value['state_root'] = str(case.state_root)
    value['limits']['turn_seconds'] = 120
    value['projects']['wfixture'] = dict(root=str(case.root), kind='demiforge_registered',
        phase='implementation', pipeline='orders', writes=['orders'], watch=['orders'],
        sources=['AGENTS.md'], non_goals=['No production or authority changes'],
        authorization='Isolated registration fixture', allow_tests=True)
    case.deployment = Deployment(value)
    normalized = json.loads(wire(case.deployment.value))
    for project in normalized['projects'].values(): project.pop('identity', None)
    policy_hash = digest(normalized)
    case.project = case.deployment.projects['wfixture']
    case.store = Store(case.state_root, case.deployment.limits, initialize=True)
    case.session = case.store.ensure_project('wfixture', root_identity(case.root),
                                            'codex', 'create', policy_hash)
    turn, _ = case.store.submit('wfixture', case.session['id'], 'add-flows', 0,
        'Add flows within the original project and preserve its history.',
        root_identity(case.root), policy_hash, development=initial(case.session['id']))
    case.tid = turn['id']
    case.store.update(case.tid, state='running', dispatch='acknowledged', delivery='acknowledged',
                      context=wire(dict(managed=True, bytes=0)).decode())
    case.bridge = ProtocolBridge(case.store, case.tid, case.project, case.state_root)
    case.api = case.bridge.managed_tasks
    case.authority = case.api.review_authority
    case.api.prepare_task(case.session['id'], 'orders', turn['text'], ['orders'],
        ['No production or authority changes'], contract=dict(allow_tests=True))
    for part in human_decisions(turn):
        case.api.note(case.session['id'], 'decision', part['text'], event_id=part['event_id'])
    case.task_file = case.api.store.root / case.session['id'] / 'task.json'
    return case


def _enqueue(case, flow):
    return case.bridge.invoke(dict(operation='register_flow', arguments=flow))


def _drain(case):
    # Trusted fixture evidence: no provider is ever launched by these tests.
    case.store.update(case.tid, state='completed', cleanup='complete',
                      gate_phase='queued', gate_cleanup='pending')


def _apply(case):
    return controller.apply_registrations(case.deployment, case.store,
        case.store.turn(case.tid), case.store.session(case.session['id']), case.project, case.api)


def _recover(case):
    return controller.recover_registrations(case.deployment, case.store,
        case.store.turn(case.tid), case.store.session(case.session['id']), case.project, case.api)


def _restart(case):
    case.store = Store(case.state_root, case.deployment.limits)
    case.api = ManagedTasks(case.state_root, 'wfixture', case.root, review_authority=case.authority)


def test_control_ipc_only_enqueues_eight_intents_and_rejects_extra_authority(registration_turn):
    case = registration_turn
    controls = _controls(case)
    turn = case.store.turn(case.tid)
    channel = BridgeServer(case.bridge, epoch=turn['epoch'], deadline=time.time() + 30)
    def invoke(flow):
        answer = call(str(channel.path), channel.token, case.tid, turn['epoch'], 'tools/call',
            dict(name='development_protocol', arguments=dict(operation='register_flow', arguments=flow)))
        return json.loads(answer['content'][0]['text'])
    try:
        first = _new_flow(case, 'new0')
        assert invoke(first)['status'] == 'queued_after_provider_drain'
        assert invoke(first)['duplicate'] is True
        with pytest.raises(ValueError):
            invoke(dict(first, verify=[dict(argv=['anything'])]))
        with pytest.raises(ValueError):
            invoke(dict(first, params='../outside.json'))
        for index in range(1, 8):
            assert invoke(_new_flow(case, f'new{index}'))['status'] == 'queued_after_provider_drain'
        with pytest.raises(ValueError, match='eight'):
            invoke(_new_flow(case, 'ninth'))
        control = json.loads(case.store.turn(case.tid)['development_control'])
        assert len(control['registration_intents']) == 8
        assert _controls(case) == controls
        assert case.api.registration_plan(case.session['id']) is None
    finally:
        channel.close()


@pytest.mark.parametrize('change', ['cancelled', 'expired', 'epoch', 'primary', 'undrained'])
def test_queued_registration_rechecks_lifetime_before_any_control_write(registration_turn, change):
    case = registration_turn
    _enqueue(case, _new_flow(case))
    if change != 'undrained': _drain(case)
    controls = _controls(case)
    turn = case.store.turn(case.tid)
    if change == 'cancelled':
        case.store.cancel('wfixture', case.session['id'], case.tid)
    elif change == 'expired':
        control = json.loads(turn['development_control']); control['deadline'] = time.time() - 1
        case.store.update(case.tid, development_control=wire(control).decode())
    elif change == 'epoch':
        # Simulate an old gate still holding its admitted turn after the Store
        # has assigned ownership to another provider epoch.
        with case.store.connect(write=True) as db:
            db.execute('UPDATE turns SET epoch=? WHERE id=?', ('different-epoch', case.tid))
    elif change == 'primary':
        with case.store.connect(write=True) as db:
            db.execute('DELETE FROM project_conversations WHERE workspace=?', ('wfixture',))
    with pytest.raises((ForgeError, WorkbenchError)):
        if change == 'epoch':
            controller.apply_registrations(case.deployment, case.store, turn,
                case.store.session(case.session['id']), case.project, case.api)
        else:
            _apply(case)
    assert _controls(case) == controls
    assert case.api.registration_plan(case.session['id']) is None


class InterruptedPublication(BaseException):
    """A process exit bypasses ordinary product Exception handlers."""


@pytest.mark.parametrize('point', ['before-first', 'after-first', 'after-commit'])
def test_restart_recovers_exact_journal_without_reviving_cancelled_turn(registration_turn, monkeypatch, point):
    case = registration_turn
    original = copy.deepcopy(_state(case))
    controls = _controls(case)
    _enqueue(case, _new_flow(case))
    _drain(case)
    real_publish, real_update = controller.publish_control, case.store._update
    calls = []
    def publish(record):
        calls.append(record['path'])
        if point == 'before-first' or point == 'after-first' and len(calls) == 2:
            raise InterruptedPublication()
        real_publish(record)
    def update(db, tid, changes):
        if point == 'after-commit' and changes.get('delivery') == 'stale':
            raise InterruptedPublication()
        return real_update(db, tid, changes)
    with monkeypatch.context() as scoped:
        scoped.setattr(controller, 'publish_control', publish)
        scoped.setattr(case.store, '_update', update)
        with pytest.raises(InterruptedPublication): _apply(case)
    case.store.cancel('wfixture', case.session['id'], case.tid)
    stopped = case.store.turn(case.tid)
    assert stopped['stop_at'] is not None
    _restart(case)
    def forbidden(*args, **kwargs):
        raise AssertionError('Metadata recovery must not schedule development or verification')
    monkeypatch.setattr(case.store, 'schedule_repair', forbidden)
    monkeypatch.setattr(case.api, 'verify', forbidden)
    _recover(case)
    after = case.store.turn(case.tid)
    assert (after['state'], after['stop_at'], after['cleanup'], after['gate_cleanup']) == (
        stopped['state'], stopped['stop_at'], stopped['cleanup'], stopped['gate_cleanup'])
    state = _state(case)
    item = state['managed']['registration_revisions'][0]
    assert item['status'] == ('aborted' if point == 'before-first' else 'committed')
    if point == 'before-first':
        assert _controls(case) == controls
    else:
        for key in ('project', 'workspace'):
            record = item['plan'][key]
            assert Path(record['path']).read_text() == record['after_raw']
    _assert_original_history(case, original)
    assert not case.api.status(case.session['id'])['ready']
    settled_controls, settled_state = _controls(case), _state(case)
    assert _recover(case) is None
    assert _controls(case) == settled_controls and _state(case) == settled_state


def test_recovery_never_overwrites_unknown_bytes(registration_turn, monkeypatch):
    case = registration_turn
    _enqueue(case, _new_flow(case))
    _drain(case)
    real_publish = controller.publish_control
    def stop_after_project(record):
        if Path(record['path']).name == 'workspace.json': raise InterruptedPublication()
        real_publish(record)
    with monkeypatch.context() as scoped:
        scoped.setattr(controller, 'publish_control', stop_after_project)
        with pytest.raises(InterruptedPublication): _apply(case)
    foreign = json.loads((case.root / 'workspace.json').read_text())
    foreign['name'] = 'concurrent human metadata'
    (case.root / 'workspace.json').write_text(_raw(foreign))
    controls = _controls(case)
    _restart(case)
    with pytest.raises((ForgeError, WorkbenchError), match='unknown'):
        _recover(case)
    assert _controls(case) == controls
    assert case.api.registration_plan(case.session['id'])['status'] == 'prepared'
    assert not case.api.status(case.session['id'])['ready']


def test_registration_uses_existing_workspace_lock_and_respects_concurrent_public_writer(registration_turn, monkeypatch):
    case = registration_turn
    _enqueue(case, _new_flow(case))
    _drain(case)
    controls = _controls(case)
    with control_lock(case.root):
        with pytest.raises(WorkbenchError, match='being updated'):
            _apply(case)
    assert _controls(case) == controls
    assert case.api.registration_plan(case.session['id']) is None
    case.write('orders/flows/public.json', _raw(dict(arguments=dict(value=10))))
    real_plan = controller.plan_flow_registration
    def concurrent_writer(root, flows):
        proposal = real_plan(root, flows)
        ws.register_flow(root, 'orders/flows/select.py:run', params='orders/flows/public.json')
        return proposal
    monkeypatch.setattr(controller, 'plan_flow_registration', concurrent_writer)
    with pytest.raises(WorkbenchError, match='changed after'):
        _apply(case)
    assert (case.root / '.demiforge/project.json').read_bytes() == controls['.demiforge/project.json']
    assert 'orders/flows/public.json' in ws.load_workspace(case.root)[1]['entries']['orders/flows/select.py:run']
    assert case.api.registration_plan(case.session['id']) is None
