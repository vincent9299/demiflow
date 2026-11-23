"""Create replay admission: real journals/sinks and a real workbench worker.

Fault injection stops after the real seal or native commit. No interpreter,
journal, writer, reconciliation result or receipt is replaced by a stand-in.
Use --basetemp=/tmp/demiflow-gpt-*; production/other-lane files are untouched.
"""
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import LanceWriteConflict
from demiflow.execution.recovery_contract import RecoveryContractError, RunRef, identity
from demiflow.execution.run_context import RunContext, create_target_absent_binding
from demiflow.execution.run_journal import RunJournal
from demiflow.lance.model import LanceWriteSpec

REFUSAL = 'This delivery-only entry needs its P3 native sink adapter before replay'


def keep_row(row):
    return row


def _dataset(fn=keep_row):
    return data.from_items([{'value': 2}, {'value': 3}]).map(fn)


def _intent(run, action_id, spec):
    return dict(kind='write_table', spec=spec.to_dict(), request_hash=spec.content_hash,
                operation_id=identity([run.ref.run_id, action_id, 1]))


def _pause_native(tmp_path, monkeypatch, *, mode='create', fn=keep_row):
    """Fault after sealing, before any commit; capture real native identity."""
    from demiflow.execution.outputs import sinks as dataflow_sinks
    spec = LanceWriteSpec(str(tmp_path / 'out.lance'), mode=mode,
                         on=('value',) if mode == 'merge' else ())
    captured = {}

    def stop(kernel, *, request, services, publication_reserved_bytes=None, admit_publication=None):
        action = services.action
        intent = _intent(action.context, action.action_id, spec)
        action.context.journal.action_event(action.action_id, 'intent', intent)
        seal = action.context.journal.source_binding(action.action_id + '-dataflow-sealed')
        captured.update(action_id=action.action_id, declaration=action.declaration,
                        intent=intent, plan_ref=seal['programs'][0])
        raise InterruptedError('before native publication')

    with monkeypatch.context() as fault:
        fault.setattr(dataflow_sinks, 'make_native_terminal_sink', stop)
        with pytest.raises(InterruptedError, match='before native publication'):
            with RunContext.start(tmp_path / 'run') as run:
                captured['ref'] = run.ref
                _dataset(fn).write_lance(spec.uri, mode=mode, on=spec.on,
                                         return_receipt=True)
    captured['spec'] = spec
    assert RunJournal(tmp_path / 'run').execution_commits(captured['action_id']) == []
    return captured


def _seed_admission(run, paused, *, change=None):
    """Guard unit setup, separate from workbench's real absence-proof producer."""
    binding = create_target_absent_binding(run.ref.run_id, paused['action_id'],
                                          paused['declaration'], paused['intent'])
    assert binding is not None
    if change:
        binding.update(change)
    run.journal.bind_source(run.fence, paused['action_id'] + '-create-target-absent', binding)
    run.journal.action_event(paused['action_id'], 'intent', paused['intent'])


def test_r_a_existing_target_no_admission_and_native_sink_refuses(tmp_path, monkeypatch):
    paused = _pause_native(tmp_path, monkeypatch)
    uri = paused['spec'].uri
    lance.write_dataset(pa.table({'value': [999]}), uri)
    with pytest.raises(RecoveryContractError) as denied:
        with RunContext.resume(paused['ref']):
            _dataset().write_lance(uri, mode='create')
    assert str(denied.value) == REFUSAL
    with pytest.raises(LanceWriteConflict):
        with RunContext.start(tmp_path / 'fresh-run'):
            _dataset().write_lance(uri, mode='create')
    assert lance.dataset(uri).to_table().to_pylist() == [{'value': 999}]
    assert lance.dataset(uri).version == 1


@pytest.mark.parametrize('mode', ['append', 'merge', 'overwrite'])
def test_r_b_other_modes_keep_original_replay_refusal(tmp_path, monkeypatch, mode):
    paused = _pause_native(tmp_path, monkeypatch, mode=mode)
    with pytest.raises(RecoveryContractError) as denied:
        with RunContext.resume(paused['ref']) as run:
            # A create-shaped fact must not authorize another mode.
            run.journal.bind_source(run.fence, paused['action_id'] + '-create-target-absent',
                                   {'kind': 'create-target-absent', 'mode': 'create'})
            run.journal.action_event(paused['action_id'], 'intent', paused['intent'])
            _dataset().write_lance(paused['spec'].uri, mode=mode, on=paused['spec'].on)
    assert str(denied.value) == REFUSAL
    assert not Path(paused['spec'].uri).exists()


def test_r_e_stale_absence_fact_never_overwrites_foreign_rows(tmp_path, monkeypatch):
    paused = _pause_native(tmp_path, monkeypatch)
    with pytest.raises(LanceWriteConflict, match='already exists'):
        with RunContext.resume(paused['ref']) as run:
            _seed_admission(run, paused)
            # The proof predates a competing publication: exercise the real
            # Dataset -> RunContext -> sealed plan -> LanceSink.prepare path.
            lance.write_dataset(pa.table({'value': [777]}), paused['spec'].uri)
            _dataset().write_lance(paused['spec'].uri, mode='create')
    assert lance.dataset(paused['spec'].uri).version == 1
    assert lance.dataset(paused['spec'].uri).to_table().to_pylist() == [{'value': 777}]
    assert all(f['status'] != 'acked' for f in
               RunJournal(tmp_path / 'run').execution_commits(paused['action_id']))


def test_resume_reuses_original_program_manifest_and_bindings(tmp_path, monkeypatch):
    import hashlib
    from demiflow.execution.seal import plan as dataflow_seal
    paused = _pause_native(tmp_path, monkeypatch)
    journal = RunJournal(tmp_path / 'run')
    aid = paused['action_id']
    before = {suffix: journal.source_binding(aid + suffix)
              for suffix in ('-dataflow-sealed',)}
    # The write seam seals a code-pin carrier, not a transient pickle: it is
    # deliberately unregistered so the post-run audit survives orphan cleanup.
    carrier = tmp_path / 'run' / '.dataflow-store' / paused['plan_ref']['store_id'] / 'objects' / paused['plan_ref']['key']
    assert carrier.exists()
    assert hashlib.sha256(carrier.read_bytes()).hexdigest() == paused['plan_ref']['digest']
    assert carrier.stat().st_size == paused['plan_ref']['size_bytes']

    def forbidden_restage(*args, **kwargs):
        pytest.fail('native resume must not pickle/rebind the existing execution plan')

    monkeypatch.setattr(dataflow_seal, 'stage_dataflow_programs', forbidden_restage)
    monkeypatch.setattr(dataflow_seal, 'seal_dataflow', forbidden_restage)
    with RunContext.resume(paused['ref']) as run:
        _seed_admission(run, paused)
        receipt = _dataset().write_lance(paused['spec'].uri, mode='create', return_receipt=True)
    assert receipt.written_rows == 2
    assert {suffix: journal.source_binding(aid + suffix) for suffix in before} == before


def test_resume_corrupt_original_binding_is_refused_without_restaging(tmp_path, monkeypatch):
    paused = _pause_native(tmp_path, monkeypatch)
    ref = RunJournal(tmp_path / 'run').source_binding(paused['action_id'] + '-dataflow-sealed')['physical_ref']
    (tmp_path / 'run' / '.dataflow-store' / ref['store_id'] / 'objects' / ref['key']).write_bytes(b'{}')
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='artifact size differs'):
        with RunContext.resume(paused['ref']) as run:
            _seed_admission(run, paused)
            _dataset().write_lance(paused['spec'].uri, mode='create')
    assert not Path(paused['spec'].uri).exists()


@pytest.mark.parametrize('field', ['kind', 'action_id', 'operation_id', 'request_hash', 'uri', 'mode'])
def test_guard_checks_every_persisted_identity_field(tmp_path, monkeypatch, field):
    paused = _pause_native(tmp_path, monkeypatch)
    with pytest.raises(RecoveryContractError) as denied:
        with RunContext.resume(paused['ref']) as run:
            _seed_admission(run, paused, change={field: 'different'})
            _dataset().write_lance(paused['spec'].uri, mode='create')
    assert str(denied.value) == REFUSAL
    assert not Path(paused['spec'].uri).exists()


def test_r_d_changed_business_source_is_refused_before_publication(tmp_path, monkeypatch):
    name = 'recovery_business_' + tmp_path.name.replace('-', '_')
    path = tmp_path / (name + '.py')
    path.write_text('def transform(row):\n    return row\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(name)
    try:
        paused = _pause_native(tmp_path, monkeypatch, fn=module.transform)
        path.write_text('def transform(row):\n    return {"value": 999}\n')
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        with pytest.raises((RecoveryContractError, DataflowError), match='identity|code|sealed|CODE_MISMATCH'):
            with RunContext.resume(paused['ref']) as run:
                _seed_admission(run, paused)
                _dataset(module.transform).write_lance(paused['spec'].uri, mode='create')
        assert not Path(paused['spec'].uri).exists()
        assert RunJournal(tmp_path / 'run').execution_commits(paused['action_id']) == []
    finally:
        sys.modules.pop(name, None)


def _commit_child(root, crash=False):
    root = Path(root)
    journal = RunJournal(root / 'run')
    old = journal.recovery_identity()
    scope = (RunContext.resume(RunRef(str(root / 'run'), old['run_id'])) if old
             else RunContext.start(root / 'run'))
    if crash:
        from demiflow.execution import commit

        def fault(name, commit_id):
            if name == 'after_sink_commit_before_receipt':
                os._exit(37)
        commit.fault_point = fault
    with scope:
        return _dataset().write_lance(str(root / 'out.lance'), mode='create', return_receipt=True)


def test_r_c_existing_native_commit_recovers_without_republication(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': str(repo) + os.pathsep + str(repo / 'tests'),
           'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MALLOC_ARENA_MAX': '2',
           'RAYON_NUM_THREADS': '2', 'TOKIO_WORKER_THREADS': '2', 'LANCE_CPU_THREADS': '2'}
    child = subprocess.run([sys.executable, '-B', '-c',
        'from test_gpt_write_recovery import _commit_child; import sys; _commit_child(sys.argv[1], True)',
        str(tmp_path)], cwd=repo, env=env, capture_output=True, text=True, timeout=45)
    assert child.returncode == 37, child.stdout + child.stderr
    journal = RunJournal(tmp_path / 'run')
    before = journal.execution_commits('action-1')
    assert len(before) == 1 and before[0]['status'] == 'prepared'
    # The workbench no longer re-exports the engine reader under a private name (P4).
    from demiflow.execution.journal.publication_facts import inspect_write_receipt as _native_execution_receipt
    projected = _native_execution_receipt(journal, 'action-1')
    assert projected['committed_version'] == 1
    assert journal.execution_commits('action-1') == before
    resumed = subprocess.run([sys.executable, '-B', '-c',
        'from test_gpt_write_recovery import _commit_child; import json,sys; '
        'print(json.dumps(_commit_child(sys.argv[1]).to_dict()))', str(tmp_path)],
        cwd=repo, env=env, capture_output=True, text=True, timeout=45)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    result = json.loads(resumed.stdout)
    after = journal.execution_commits('action-1')
    assert len(after) == 1 and after[0]['status'] == 'acked'
    assert before[0]['commit_id'] == after[0]['commit_id']
    assert result['committed_version'] == lance.dataset(str(tmp_path / 'out.lance')).version == 1
    assert lance.dataset(str(tmp_path / 'out.lance')).to_table().to_pylist() == [{'value': 2}, {'value': 3}]


@pytest.fixture
def stopped_workbench(tmp_path, monkeypatch):
    import test_upgrade_business_acceptance as acc
    from demiflow.workbench.runs import manager as runs, facts
    from demiflow.resources.process_observation import require_pidfds
    require_pidfds()
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    fixture = acc.build_owned_fixture(tmp_path, scenario='workbench')
    root = fixture['root']
    prepared = runs.prepare(root, fixture['entry'], params=fixture['params'])
    rid = prepared['run_id']
    target = runs.directory(root, rid)
    runs.start(root, rid, expected=prepared['digest'])
    deadline = time.monotonic() + 90
    try:
        while True:
            value = runs.show(root, rid)
            assert value['status'] not in runs.TERMINAL, value
            actions = value['actions']
            if any(RunJournal(target).source_binding(aid + '-dataflow-sealed') is not None
                   and RunJournal(target).source_binding(aid + '-create-target-absent') is not None
                   for aid in actions):
                break
            assert time.monotonic() < deadline, 'no execution action before cancellation'
            time.sleep(.01)
    finally:
        value = runs.show(root, rid)
        if value['status'] not in runs.TERMINAL:
            runs.cancel(root, rid, expected_attempt=value['attempt'])
    first = _wait_workbench(root, rid)
    assert first['status'] == 'cancelled', first
    sealed = {aid: facts.read_sealed_plan(target, aid) for aid in first['actions']}
    action_id = next(aid for aid, fact in sealed.items() if fact.get('available'))
    journal = RunJournal(target)
    assert journal.execution_commits(action_id) == []
    fixture.update(prepared=prepared, target=target, first=first, action_id=action_id,
                   dag_id=sealed[action_id]['dag_id'])
    return fixture


def _wait_workbench(root, run_id):
    from demiflow.workbench.runs import manager as runs
    deadline = time.monotonic() + 120
    while True:
        value = runs.show(root, run_id)
        if value['status'] in runs.TERMINAL and runs.execution_stopped(
                runs.directory(root, run_id), value['state']):
            return value
        assert time.monotonic() < deadline, value
        time.sleep(.05)


def _registration_child(root, rid):
    from demiflow.workbench.worker import prepare_native
    prepare_native(analysis_read=False)
    from demiflow.workbench.runs import manager as runs
    errors = runs.register_results(Path(root), rid, recover=True)
    target, value = runs._load(Path(root), rid)
    journal = RunJournal(target)
    aid = next(iter(runs._describe(rid, value)['actions']))
    return dict(errors=errors, admission=journal.source_binding(aid + '-create-target-absent'),
                events=journal.execution_view(detail='summary')['events'],
                commits=journal.execution_commits(aid))


def test_workbench_producer_requires_confirmed_absence(stopped_workbench):
    """Real stopped worker, native reconciliation/inspection and journal writes."""
    from demiflow.workbench.runs import manager as runs
    from demiflow.lance.write import write_lance
    f = stopped_workbench
    rid, aid = f['prepared']['run_id'], f['action_id']
    journal = RunJournal(f['target'])
    intent = next(event['value'] for event in journal.execution_view(detail='summary')['events']
                  if event['action_id'] == aid and event['kind'] == 'intent')
    uri = Path(intent['spec']['uri'])
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': str(repo) + os.pathsep + str(repo / 'tests'),
           'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MALLOC_ARENA_MAX': '2',
           'RAYON_NUM_THREADS': '2', 'TOKIO_WORKER_THREADS': '2', 'LANCE_CPU_THREADS': '2'}
    before = journal.execution_view(detail='summary')['events']
    # v3 records absence in driver preflight before starting any source/model.
    # Later inspection preserves that historical fact, never turns it into
    # authorization to overwrite a subsequently created target (R-E above).
    prior_proof = journal.source_binding(aid + '-create-target-absent')
    assert prior_proof == create_target_absent_binding(journal.recovery_identity()['run_id'],
        aid, journal.execution_action(aid)['declaration'], intent)
    assert prior_proof is not None

    def register(errors=()):
        child = subprocess.run([sys.executable, '-B', '-c',
            'from test_gpt_write_recovery import _registration_child; import json,sys; '
            'print(json.dumps(_registration_child(sys.argv[1],sys.argv[2])))', str(f['root']), rid],
            cwd=repo, env=env, capture_output=True, text=True, timeout=45)
        assert child.returncode == 0, child.stdout + child.stderr
        result = json.loads(child.stdout)
        assert result['errors'] == list(errors), result
        assert result['events'] == before, 'admission must not invent recovered/result events'
        assert result['commits'] == [], 'admission must not invent commits or receipts'
        return result['admission']

    # R-A: a foreign physical target exists; refusal preserves all its rows.
    lance.write_dataset(pa.table({'value': [999]}), str(uri))
    assert register() == prior_proof
    assert lance.dataset(str(uri)).to_table().to_pylist() == [{'value': 999}]
    shutil.rmtree(uri)  # Only this test's freshly created foreign-target fixture.

    # Unreadable/malformed storage is not an absence proof.
    # Since the workbench reads the target through the engine host (P4), a target
    # that is not a table directory is reported, not passed over in silence. It is
    # still no absence proof: the admission, events, commits and the file are unchanged.
    uri.write_text('not a Lance dataset')
    assert register(errors=['target: Native table location must be a directory']) == prior_proof
    assert uri.read_text() == 'not a Lance dataset'
    uri.unlink()

    # A real matching native publication without journal commits stays outside
    # this admission slice; it must not be turned into a recovered fake commit.
    spec = LanceWriteSpec(str(uri), mode='create')
    receipt = write_lance(spec, [pa.table({'value': [888]})],
                          operation_id=intent['operation_id'])
    assert receipt.status == 'committed'
    assert register() == prior_proof
    assert lance.dataset(str(uri)).version == 1
    shutil.rmtree(uri)  # Only the native publication created immediately above.

    # The same immutable preflight fact remains once the fixture target is gone.
    proof = register()
    assert proof == create_target_absent_binding(journal.recovery_identity()['run_id'],
        aid, journal.execution_action(aid)['declaration'], intent)
    assert proof is not None
    assert register() == proof  # Idempotent evidence, never an event/receipt.
    print(json.dumps({'case': 'producer', 'foreign_refused': True, 'corrupt_refused': True,
                      'published_refused': True, 'absence_admitted': True, 'admission': proof}))


def test_workbench_cancel_resume_golden(stopped_workbench):
    from demiflow.workbench.runs import manager as runs, facts
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench import project_operations as project_ops
    import test_upgrade_business_acceptance as acc
    f = stopped_workbench
    root, prepared, aid = f['root'], f['prepared'], f['action_id']
    rid = prepared['run_id']
    runs.resume(root, rid, expected=prepared['digest'], expected_attempt=f['first']['attempt'])
    result = _wait_workbench(root, rid)
    journal = RunJournal(f['target'])
    admission = journal.source_binding(aid + '-create-target-absent')
    assert admission is not None, 'absence proof was not persisted across attempts'
    assert result['status'] == 'succeeded', result['actions']
    rows = project_ops.sample_table(root, 'run:' + rid + ':target', limit=10)['rows']
    rows = [{k: v for k, v in row.items() if k != '_rowid'} for row in rows]
    acc.assert_semantics_equal(f['expected'], rows, ordering='unordered')
    commits = journal.execution_commits(aid)
    assert commits and all(fact['status'] == 'acked' for fact in commits)
    final = []
    for fact in commits:
        prepared = fact['prepared']['sinks']['write_lance']
        receipt = fact['receipts']['write_lance']['data']
        if prepared['manifest']['final']:
            final.append(fact)
            assert prepared['rows'] == receipt['rows'] == len(f['expected'])
            assert prepared['native'] is not None and receipt['native_receipt']['status'] == 'committed'
        else:
            # Intermediate ACKs retain staged partitions for recovery. Only the
            # final ACK publishes a native target version (checked below).
            assert 0 <= prepared['rows'] == receipt['rows'] <= len(f['expected'])
            assert prepared['bytes'] == receipt['bytes'] >= 0
            partitions = prepared['manifest']['partitions']
            assert sum(part['rows'] for part in partitions) == prepared['rows']
            assert sum(part['bytes'] for part in partitions) == prepared['bytes']
            assert prepared['native'] is None and 'native_receipt' not in receipt
    assert len(final) == 1 and final[0] == commits[-1]
    native = final[0]['receipts']['write_lance']['data']['native_receipt']['data']
    assert native['committed_version'] == 1
    assert native['input_rows'] == native['written_rows'] == len(f['expected'])
    table = lance.dataset(native['uri'])
    assert [version['version'] for version in table.versions()] == [1]
    fixed = lance.dataset(native['uri'], version=1).to_table().to_pylist()
    acc.assert_semantics_equal(f['expected'], fixed, ordering='unordered')
    sealed = facts.read_sealed_plan(f['target'], aid)
    assert sealed['available'] and sealed['dag_id'] == f['dag_id']
    report = runs.show(root, rid, include_report=True, compact=True)['report']
    bridge = facts.read_identity_bridge(f['target'], aid, dag_id=sealed['dag_id'],
                                       report_digest=report['digest'])
    assert bridge['available'], bridge
    print(json.dumps({'case': 'golden', 'run_id': rid, 'attempt': result['attempt'],
                      'rows': rows, 'acked': len(commits), 'dag_id': sealed['dag_id'],
                      'bridge_available': bridge['available']}, ensure_ascii=False))
