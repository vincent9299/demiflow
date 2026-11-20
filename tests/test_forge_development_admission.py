"""I1 uses the existing Store, managed receipts and run seals; no model calls."""
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import admission, development
from demiflow.workbench.conversations.common import digest, root_identity, wire
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.review import review_authority
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from demiflow.execution.run_journal import RunJournal
from test_forge_development_runtime import combined_configuration
from test_forge_development_managed import read_all, claims
from test_forge_development_migration import fixture
from test_forge_runs import base, root, prepared as legacy_prepared

ENTRY = 'orders/flows/questions.py:select'
PARAMS = 'orders/flows/select_a.json'


def prepared(root):
    return runs.prepare(root, ENTRY, params=PARAMS)


def snapshot(deployment, directory, suffix=''):
    value = json.loads(wire(deployment.value))
    for project in value['projects'].values(): project.pop('identity', None)
    path = directory / (digest(value) + suffix + '.json')
    # Deliberately different encoding from policy digest; raw SHA is separate.
    path.write_text(json.dumps(value, indent=2))
    return path, digest(value)


@pytest.fixture
def admitted(root, tmp_path, monkeypatch):
    config, _, workspace, domain, _ = combined_configuration(tmp_path, root, monkeypatch)
    domain.chmod(0o700)
    fixture(root)
    # The accepted owner must actually cover the native flow being prepared.
    # Keep the original flows/questions.py as a real unregistered counterexample.
    (root/'orders/flows').mkdir()
    (root/'orders/flows/questions.py').write_text((root/'flows/questions.py').read_text().replace(
        'from operators.question_rules import', 'from orders.operators.question_rules import'))
    shutil.copyfile(root/'flows/select_a.json', root/PARAMS)
    shutil.copytree(root/'operators', root/'orders/operators')
    declaration_path = root/'.demiforge/project.json'
    declaration = json.loads(declaration_path.read_text())
    declaration['pipelines']['orders'].update(entry=dict(path='orders/flows/questions.py', symbol='select'), operators=['orders/operators'])
    declaration_path.write_text(json.dumps(declaration))
    runs.ws.register_flow(root, ENTRY, params=PARAMS)
    assert project_ops.inspect_flow(root, ENTRY, params=PARAMS)['status'] == 'checked'
    value = json.loads(config.read_text())
    value['mode'] = 'restricted'
    value['projects'][workspace].update(kind='demiforge_registered', pipeline='orders',
        writes=['orders'], watch=['orders'], allow_tests=True, run_policy='require')
    deployment = Deployment(value)
    deployment.state.mkdir(mode=0o700)
    sealed, policy_hash = snapshot(deployment, deployment.state)
    admission.install(deployment, sealed)
    store = Store(deployment.state, deployment.limits, initialize=True)
    session = store.ensure_project(workspace, root_identity(root), 'codex', 'fixture', policy_hash)
    turn, _ = store.submit(workspace, session['id'], 'message', 0, 'Develop owned fixture',
        root_identity(root), policy_hash, development=development.initial(session['id']))
    tasks = development.project_tasks(deployment, session, deployment.projects[workspace])
    tasks.prepare_task(session['id'], 'orders', 'Develop owned fixture', ['orders'], ['No production'],
        contract=dict(allow_tests=True, max_verification_seconds=5,
            developer_session_id=session['id'], require_enforcement=True))
    read_all(tasks, session['id'])
    current = tasks.check(session['id'])
    authority = review_authority(deployment.state)
    boundary = authority.execution_identity
    def launch(argv, *, cwd, env, scratch, timeout):
        # Unit-level trusted capability fixture. Actual B1 completion provenance
        # and kernel isolation are independently covered by chat_repair/B1 tests.
        process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        process.execution_assurance = 'enforced'
        process.execution_evidence = dict(**boundary, execution_id='fixture-verifier')
        return process
    current = tasks.verify(session['id'], launch=launch)
    fingerprint = current['source_fingerprint']
    tasks.record_execution(session['id'], fingerprint, receipt=authority.issue_execution(session['id'],
        fingerprint, phase='developer', **boundary, execution_id='fixture-developer'))
    value = claims()
    tasks.record_review(session['id'], fingerprint, value, receipt=authority.issue(session['id'], fingerprint,
        value, provider='fixture', model='fixture', session_id='independent-fixture', mode='no-tools'))
    accepted = tasks.evaluate_acceptance(session['id'])
    assert accepted['development_state'] == 'accepted' and accepted['assurance'] == 'enforced', accepted
    store.update(turn['id'], state='completed', dispatch='acknowledged', delivery='acknowledged', cleanup='complete',
        gate_cleanup='complete', gate_phase='finished', development=wire(development.projection(accepted)).decode())
    return SimpleNamespace(root=root, domain=domain, deployment=deployment, sealed=sealed, policy_hash=policy_hash,
        workspace=workspace, store=store, session=session, turn=turn, tasks=tasks, accepted=accepted)


def test_current_receipt_uses_store_and_domain_and_raw_configuration_sha(admitted):
    f = admitted
    identity = admission.check(f.root)
    assert identity == dict(task_id=f.session['id'], turn_id=f.turn['id'], source_fingerprint=f.accepted['source_fingerprint'],
        policy_hash=f.policy_hash, config_sha256=hashlib.sha256(f.sealed.read_bytes()).hexdigest())
    assert identity['config_sha256'] != identity['policy_hash']
    assert admission.lookup(f.root)[1]['run_policy'] == 'require'
    assert admission.lookup(f.root/'orders')[1]['root'] == str(f.root)
    assert admission.lookup(f.domain) is None
    assert admission.check(f.root, expected=identity) == identity
    assert admission.check(f.root, entry=ENTRY) == identity
    with pytest.raises(ForgeError, match='seal is missing or stale'):
        admission.check(f.root, expected=None)
    (f.root/'orders/rules.py').write_text('def keep(row): return False\n')
    with pytest.raises(ForgeError, match='candidate differs'):
        admission.check(f.root)


@pytest.mark.parametrize('state', ['failed', 'unknown', 'cancelled'])
def test_new_failed_unknown_cancelled_request_cannot_reuse_old_acceptance(admitted, state):
    f = admitted
    turn, _ = f.store.submit(f.workspace, f.session['id'], 'followup', 1, 'Actual new request', root_identity(f.root),
        f.policy_hash, development=development.initial(f.session['id']))
    f.store.update(turn['id'], state=state, dispatch='acknowledged', delivery='acknowledged', cleanup='complete',
        gate_cleanup='complete', gate_phase='finished', development=wire(development.projection(f.accepted)).decode())
    assert f.tasks.status(f.session['id'])['development_state'] == 'accepted'
    with pytest.raises(ForgeError, match='latest user request'):
        admission.check(f.root)


def test_compaction_keeps_last_user_acceptance_only_after_all_leases_drain(admitted):
    f = admitted
    expected = admission.check(f.root)
    turn, _ = f.store.submit(f.workspace, f.session['id'], 'compact', 1, 'Compact history', root_identity(f.root),
        f.policy_hash, operation='compact')
    with pytest.raises(ForgeError, match='still owns'):
        admission.check(f.root)
    f.store.update(turn['id'], state='completed', dispatch='acknowledged', delivery='acknowledged', cleanup='complete')
    assert admission.check(f.root) == expected
    f.store.update(turn['id'], gate_cleanup='unknown')
    with pytest.raises(ForgeError, match='still owns'):
        admission.check(f.root)


@pytest.mark.parametrize('initial_journal', ['DELETE', 'WAL'])
@pytest.mark.parametrize('change', ['cancel', 'new_turn'])
def test_cancel_and_new_turn_commit_serialize_after_admission_snapshot(admitted, initial_journal, change):
    f = admitted
    connection = sqlite3.connect(f.store.path)
    assert connection.execute('PRAGMA journal_mode='+initial_journal).fetchone()[0].lower() == initial_journal.lower()
    connection.close()
    wrote = threading.Event()
    connect = f.store.connect
    @contextmanager
    def observed_connect(*, write=False):
        with connect(write=write) as db:
            yield db
            if write: wrote.set()  # SQL ran; only transaction commit remains.
    f.store.connect = observed_connect
    def cancel():
        if change == 'cancel':
            with f.store.connect(write=True) as db:
                db.execute('UPDATE turns SET stop_at=? WHERE id=?', (time.time(), f.turn['id']))
        else:
            f.store.submit(f.workspace, f.session['id'], 'concurrent', 1, 'New real request', root_identity(f.root),
                f.policy_hash, development=development.initial(f.session['id']))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with admission.guard(f.root):
            connection = sqlite3.connect(f.store.path)
            assert connection.execute('PRAGMA journal_mode').fetchone()[0] == 'delete'
            connection.close()
            future = pool.submit(cancel)
            assert wrote.wait(timeout=2)
            time.sleep(.05)
            assert not future.done()
        future.result(timeout=3)
    with pytest.raises(ForgeError, match='uncancelled|still owns'):
        admission.check(f.root)


def test_omission_cannot_revoke_require_and_explicit_observe_keeps_safe_lookup(admitted):
    f = admitted
    before = (f.domain/'development-admission.json').read_bytes()
    value = json.loads(wire(f.deployment.value))
    for project in value['projects'].values(): project.pop('identity', None)
    value['projects'][f.workspace].pop('run_policy')
    omitted = Deployment(value)
    path, _ = snapshot(omitted, f.deployment.state)
    admission.install(omitted, path)
    assert (f.domain/'development-admission.json').read_bytes() == before
    assert admission.lookup(f.root)[1]['run_policy'] == 'require'
    value['projects'][f.workspace]['run_policy'] = 'observe'
    observed = Deployment(value)
    path, _ = snapshot(observed, f.deployment.state)
    admission.install(observed, path)
    assert admission.check(f.root) is None
    assert admission.lookup(f.root)[1]['run_policy'] == 'observe'
    path.write_text(path.read_text() + '\n')
    with pytest.raises(ForgeError, match='snapshot changed'):
        admission.lookup(f.root)


def test_default_legacy_policy_does_not_create_binding(root, tmp_path, monkeypatch):
    path, _, _, domain, _ = combined_configuration(tmp_path, root, monkeypatch)
    deployment = Deployment.load(path)
    deployment.state.mkdir(mode=0o700)
    sealed, _ = snapshot(deployment, deployment.state)
    admission.install(deployment, sealed)
    assert not (domain/'development-admission.json').exists()
    assert admission.check(root) is None and admission.lookup(root) is None
    run = legacy_prepared(root)
    assert 'development_acceptance' not in runs._load(root, run['run_id'])[1]['manifest']


def test_public_prepare_seals_current_acceptance_and_start_rechecks_latest_turn(admitted):
    f = admitted
    identity = admission.check(f.root)
    run = prepared(f.root)
    manifest = runs._load(f.root, run['run_id'])[1]['manifest']
    assert manifest['development_acceptance'] == identity
    f.store.update(f.turn['id'], state='failed')
    with pytest.raises(ForgeError, match='latest user request'):
        runs.start(f.root, run['run_id'], expected=run['digest'])
    assert runs.show(f.root, run['run_id'])['status'] == 'prepared'


def test_prepare_rejects_before_inspecting_business_source(admitted, monkeypatch):
    f = admitted
    f.store.update(f.turn['id'], state='unknown')
    def forbidden(*args, **kwargs):
        pytest.fail('Unaccepted prepare must not inspect business source')
    monkeypatch.setattr(runs.ws, 'load_workspace', forbidden)
    with pytest.raises(ForgeError, match='latest user request'):
        runs._prepare(f.root, ENTRY, params=PARAMS)


@pytest.mark.parametrize('entry', ['flows/questions.py:select', 'orders/flows/questions.py:select_a'])
def test_prepare_rejects_unregistered_path_or_symbol_before_source_inspection(admitted, monkeypatch, entry):
    f = admitted
    def forbidden(*args, **kwargs):
        pytest.fail('An unregistered runtime entry must not reach business inspection')
    monkeypatch.setattr(runs.ws, 'load_workspace', forbidden)
    with pytest.raises(ForgeError, match='outside the accepted registered pipeline'):
        runs._prepare(f.root, entry, params=PARAMS)
    with pytest.raises(ForgeError, match='outside the accepted registered pipeline'):
        runs.prepare(f.root, entry, params=PARAMS)


def test_required_parent_never_silently_admits_nested_workspace(admitted):
    f = admitted
    # The parent has a real accepted task, yet its receipt is not an implicit
    # certificate for an independently addressed child workspace.
    assert admission.check(f.root)
    assert admission.lookup(f.root/'orders')[1]['root'] == str(f.root)
    for state in ('completed', 'unknown'):
        f.store.update(f.turn['id'], state=state)
        with pytest.raises(ForgeError, match='no exact required development binding'):
            admission.check(f.root/'orders')
        with pytest.raises(ForgeError, match='no exact required development binding'):
            runs.prepare(f.root/'orders', 'flows/questions.py:select', params='flows/select_a.json')


def test_first_start_rejects_wrong_entry_sealed_by_previous_admission_version(admitted):
    f = admitted
    # Reproduce the previous controller's missing entry argument inside an
    # isolated preparation worker. The resulting run uses all real seals,
    # manifests and acceptance checks; no persisted evidence is rewritten.
    script = """
import json, sys
from contextlib import contextmanager
from demiflow.workbench.worker import prepare_native
prepare_native(analysis_read=False)
from demiflow.workbench.conversations import admission
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
original = admission.guard
@contextmanager
def previous_guard(root, **kwargs):
    kwargs.pop('entry', None)
    with original(root, **kwargs) as value:
        yield value
admission.guard = previous_guard
value = runs._prepare(sys.argv[1], 'flows/questions.py:select', params='flows/select_a.json')
print(json.dumps(value))
"""
    from demiflow.forge.platform_worker import inspection_environment
    environment, _ = inspection_environment()
    result = subprocess.run([sys.executable, '-B', '-c', script, str(f.root)],
        capture_output=True, text=True, timeout=60, env=environment)
    assert result.returncode == 0, result.stderr
    run = json.loads(result.stdout)
    before = runs._load(f.root, run['run_id'])[1]['manifest']
    assert before['development_acceptance'] == admission.check(f.root)
    with pytest.raises(ForgeError, match='outside the accepted registered pipeline'):
        runs.start(f.root, run['run_id'], expected=run['digest'])
    assert runs.show(f.root, run['run_id'])['status'] == 'prepared'
    assert runs._load(f.root, run['run_id'])[1]['manifest'] == before


@pytest.mark.parametrize('busy', [False, True])
def test_show_projects_bound_run_policy_without_rewriting_history(admitted, busy):
    from demiflow.workbench.conversations.service import Service
    f = admitted
    saved = dict(development.projection(f.accepted), busy=busy)
    f.store.update(f.turn['id'], development=wire(saved).decode())
    raw = f.store.turn(f.turn['id'])['development']
    service = Service(f.sealed)
    try:
        value = service.show(f.workspace, f.session['id'])
        assert value['turn']['development']['run_policy'] == 'require'
        assert value['turn']['development']['busy'] == busy
        assert f.store.turn(f.turn['id'])['development'] == raw
    finally:
        service.close()


def test_show_after_policy_replacement_is_stale_and_retains_original_history(admitted):
    from demiflow.workbench.conversations.service import Service
    f = admitted
    raw = f.store.turn(f.turn['id'])['development']
    value = json.loads(f.sealed.read_text())
    value['projects'][f.workspace]['run_policy'] = 'observe'
    changed = Deployment(value)
    path, _ = snapshot(changed, f.deployment.state)
    service = Service(path)
    try:
        value = service.show(f.workspace, f.session['id'])
        state = value['turn']['development']
        assert state['state'] == 'stale' and not state['busy']
        assert 'policy changed' in state['reason']
        assert state['run_policy'] == 'require'  # Original session grant.
        assert f.store.turn(f.turn['id'])['development'] == raw
    finally:
        service.close()


def test_source_change_between_seal_and_commit_leaves_no_run(admitted):
    f = admitted
    # Preserve the existing native-reader allocation boundary while injecting
    # a source edit at the final seal/commit window in this disposable worker.
    script = """
import sys
from demiflow.workbench.worker import prepare_native
prepare_native(analysis_read=False)
from pathlib import Path
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.schema import ForgeError
root = Path(sys.argv[1])
original = runs.verify_run_basis
def change_after_seal(*args, **kwargs):
    result = original(*args, **kwargs)
    (root/'orders/rules.py').write_text('def keep(row): return False\\n')
    return result
runs.verify_run_basis = change_after_seal
try:
    runs._prepare(root, 'orders/flows/questions.py:select', params='orders/flows/select_a.json')
except ForgeError as exc:
    assert 'candidate differs' in str(exc), exc
else:
    raise AssertionError('Changed candidate was admitted')
"""
    from demiflow.forge.platform_worker import inspection_environment
    environment, _ = inspection_environment()
    result = subprocess.run([sys.executable, '-B', '-c', script, str(f.root)],
        capture_output=True, text=True, timeout=60, env=environment)
    assert result.returncode == 0, result.stderr
    assert not any(path.is_dir() for path in (f.root/'.forge/runs').iterdir())


def test_historical_resume_entry_retains_original_seal_without_new_certificate(admitted, monkeypatch):
    f = admitted
    run = prepared(f.root)
    target, value = runs._load(f.root, run['run_id'])
    seal = copy.deepcopy(value['manifest']['development_acceptance'])
    journal = RunJournal(target, attempt=1)
    with runs._launch_unit(target, 1) as (_, unit_identity):
        journal.transition({'prepared'}, 'cancelled', execution_stopped=True, cleanup='complete', unit_identity=unit_identity)
    f.store.update(f.turn['id'], state='failed')
    # Supply the already-tested native recovery entry identity for this stopped
    # pre-action fixture. Public resume, its bounded worker and start admission
    # remain real; intercept only the final native process spawn.
    from demiflow.execution.recovery_identity import platform_identity
    monkeypatch.setattr(RunJournal, 'recovery_identity', lambda _: dict(platform_revision=platform_identity()))
    popen = subprocess.Popen
    def no_native_execution(argv, *args, **kwargs):
        if 'supervise' in argv:
            raise RuntimeError('fixture reached historical native launch')
        return popen(argv, *args, **kwargs)
    monkeypatch.setattr(subprocess, 'Popen', no_native_execution)
    with pytest.raises(RuntimeError, match='reached historical native launch'):
        runs.resume(f.root, run['run_id'], expected=run['digest'], expected_attempt=1)
    final = runs._load(f.root, run['run_id'])[1]
    assert final['state']['attempt'] == 2
    assert final['manifest']['development_acceptance'] == seal


def test_enabling_require_does_not_upgrade_an_observe_prepared_seal(admitted):
    f = admitted
    value = json.loads(wire(f.deployment.value))
    for project in value['projects'].values(): project.pop('identity', None)
    value['projects'][f.workspace]['run_policy'] = 'observe'
    observed = Deployment(value)
    path, _ = snapshot(observed, f.deployment.state)
    admission.install(observed, path)
    run = prepared(f.root)
    assert 'development_acceptance' not in runs._load(f.root, run['run_id'])[1]['manifest']
    admission.install(f.deployment, f.sealed)
    assert admission.check(f.root)['source_fingerprint'] == f.accepted['source_fingerprint']
    with pytest.raises(ForgeError, match='seal is missing or stale'):
        runs.start(f.root, run['run_id'], expected=run['digest'])


def test_direct_install_cannot_leave_managed_restricted_preview_unbound(tmp_path):
    deployment = SimpleNamespace(mode='restricted', projects={'fixture':dict(managed=True)}, value={})
    with pytest.raises(ForgeError, match='explicit runtime domain'):
        admission.install(deployment, tmp_path/'unused.json')
