"""Stage 3 acceptance: real native outputs and explicit failure windows."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import time

import pytest
import lance
import pyarrow as pa

from demiflow import data
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.execution.run_journal import RunJournal
from demiflow.data.plan import CallableSpec, StandardCallable
from demiflow.data.runtime_contract import RuntimeContractError


@pytest.fixture(scope='module')
def base(tmp_path_factory):
    spec = importlib.util.spec_from_file_location('run_demo', Path(__file__).parents[1] / 'examples/forge_workspace/make_demo.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path_factory.mktemp('forge-runs') / 'workspace'
    module.create(root)
    return root


@pytest.fixture
def root(base, tmp_path, monkeypatch):
    # A temporary workspace must not join the machine's production deployment
    # domain or its fixed workspace allowlist/resource admission ledger.
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    target = tmp_path / 'workspace'
    shutil.copytree(base, target)
    return target


def prepared(root, configuration='select_a', **kwargs):
    return runs.prepare(root, 'flows/questions.py:select', params='flows/' + configuration + '.json', **kwargs)


def wait(root, run_id, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = runs.show(root, run_id)
        if value['status'] in runs.TERMINAL:
            return value
        time.sleep(0.1)
    runs.cancel(root, run_id)
    raise AssertionError('Run did not terminate: ' + str(value))


def execute(root, value):
    runs.start(root, value['run_id'], expected=value['digest'])
    result = wait(root, value['run_id'])
    return result


def rows(root, run):
    return ws.sample_table(root, 'run:' + run['run_id'] + ':target')['rows']


def custom(root, body, *, operators='', arguments=None, limits=None):
    (root / 'flows/custom.py').write_text('from demiflow import data\n' + body)
    if operators:
        (root / 'operators/custom.py').write_text(operators)
    params = {'format': 'demiflow_forge_flow_parameters_v1', 'arguments': arguments or {
        'source': {'kind': 'table', 'alias': 'candidates'},
        'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'custom.lance'}}}
    (root / 'flows/custom.json').write_text(json.dumps(params))
    ws.register_flow(root, 'flows/custom.py:run', params='flows/custom.json')
    return runs.prepare(root, 'flows/custom.py:run', params='flows/custom.json', limits=limits)


def test_real_ab_receipts_case_comparison_and_fixed_continuation(root):
    a = execute(root, prepared(root))
    b = execute(root, prepared(root, 'select_b'))
    assert a['status'] == b['status'] == 'succeeded', (a, b)
    assert {r['case_id'] for r in rows(root, a)} == {'c01', 'c02'}
    assert {r['case_id'] for r in rows(root, b)} == {'c01'}
    assert {r['normalized_question'] for r in rows(root, a)} == {'三角形的面积是多少？', '增长率是多少？'}
    assert a['outputs'][0]['uri'] != b['outputs'][0]['uri']
    assert a['outputs'][0]['receipt']['written_rows'] == 2
    assert not (root / 'data/selected_a.lance').exists()
    assert lance.dataset(str(root / 'data/candidates.lance')).version == 3
    assert runs.compare(root, a['run_id'], b['run_id'])['same_source']
    assert runs.compare(root, a['run_id'], b['run_id'])['same_inputs']
    next_run = custom(root, '''
def run(*, source, target):
    return data.read_lance(source['uri'], version=source['version']).limit(1).write_lance(target['uri'], mode='create', return_receipt=True)
''', arguments={'source': {'kind': 'table', 'alias': 'run:' + a['run_id'] + ':target'},
               'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'continued.lance'}})
    followed = execute(root, next_run)
    assert followed['status'] == 'succeeded', followed
    assert len(rows(root, followed)) == 1
    source_ref = next_run['report']['binding']['tables']['run:' + a['run_id'] + ':target']
    assert source_ref['version'] == a['outputs'][0]['receipt']['committed_version']


def test_prepare_does_not_execute_and_rejects_changed_basis(root):
    value = prepared(root)
    assert value['status'] == 'prepared' and not Path(value['outputs'][0]['uri']).exists()
    assert value['report']['coverage']['execution'] == 'not_run'
    (root / 'operators/question_rules.py').write_text((root / 'operators/question_rules.py').read_text() + '\n# changed\n')
    with pytest.raises(ValueError, match='stale|changed'):
        runs.start(root, value['run_id'], expected=value['digest'])
    assert runs.show(root, value['run_id'])['status'] == 'prepared'
    assert '# changed' not in runs.source(root, value['run_id'], 'operators/question_rules.py')['text']
    with pytest.raises(ValueError, match='outside'):
        runs.source(root, value['run_id'], '../workspace.json')


def test_run_log_rotation_preserves_tail_and_reports_expired_cursor(root):
    from demiflow.workbench.runs.worker import RunLog
    value = prepared(root)
    path = runs.attempt_directory(runs.directory(root, value['run_id']), 'logs', value['attempt'])
    path.mkdir(parents=True)
    log = RunLog(path, 16)
    log.write(b'0123456789abcdef')
    assert runs.logs(root, value['run_id'], limit=8)['text'] == '01234567'
    log.write(b'ghijklmn')
    log.close()
    result = runs.logs(root, value['run_id'])
    assert result == dict(text='89abcdefghijklmn', next_offset=24, truncated=True, base_offset=8)
    assert sum(p.stat().st_size for p in path.iterdir()) == 16
    assert runs.logs(root, value['run_id'], offset=24)['text'] == ''


def test_seal_tamper_and_exact_create_conflict(root):
    value = prepared(root, new_outputs=False)
    target = runs.directory(root, value['run_id'])
    path = target / 'source/operators/question_rules.py'
    path.chmod(0o600)
    path.write_text(path.read_text() + '\n# tamper\n')
    with pytest.raises(ValueError, match='Sealed'):
        runs.start(root, value['run_id'], expected=value['digest'])
    value = prepared(root, new_outputs=False)
    lance.write_dataset(pa.table({'existing': [1]}), value['outputs'][0]['uri'])
    with pytest.raises(ValueError, match='exists'):
        runs.start(root, value['run_id'], expected=value['digest'])
    assert lance.dataset(value['outputs'][0]['uri']).version == 1


def test_unrelated_source_change_and_start_retry(root):
    value = prepared(root)
    (root / 'flows/unrelated.py').write_text('def something_else():\n    return 1\n')
    first = runs.start(root, value['run_id'], expected=value['digest'])
    second = runs.start(root, value['run_id'], expected=value['digest'])
    assert first['run_id'] == second['run_id']
    result = wait(root, value['run_id'])
    assert result['status'] == 'succeeded', result
    repeated = runs.start(root, value['run_id'], expected=value['digest'])
    assert repeated['status'] == 'succeeded'
    assert lance.dataset(result['outputs'][0]['uri']).version == 1


def test_native_materialize_union_columns_and_independent_outputs(root):
    value = custom(root, '''
def run(*, source, target, other):
    d = data.read_lance(source['uri'], version=source['version'], columns=['case_id', 'question']).materialize()
    first = d.select_columns(['case_id']).rename_columns({'case_id': 'id'}).write_lance(target['uri'], mode='create', return_receipt=True)
    second = d.union(d).union(d).drop_columns(['question']).limit(4).write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''', arguments={'source': {'kind': 'table', 'alias': 'candidates'},
               'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'x.lance'},
               'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'y.lance'}})
    result = execute(root, value)
    assert result['status'] == 'succeeded', (result, runs.logs(root, result['run_id']))
    assert sorted(o['result']['ref']['row_count'] for o in result['outputs']) == [3, 4]
    assert {e['kind'] for e in runs.journal(root, result['run_id']).execution_view()['events']} >= {'materialized', 'receipt', 'result'}


def test_partial_commit_and_empty_output_failure(root):
    value = custom(root, '''
def run(*, source, target, other):
    d = data.read_lance(source['uri'], version=source['version'])
    first = d.write_lance(target['uri'], mode='create', return_receipt=True)
    second = d.limit(0).write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''', arguments={'source': {'kind': 'table', 'alias': 'candidates'},
               'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'x.lance'},
               'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'empty.lance'}})
    result = execute(root, value)
    assert result['status'] == 'failed'
    committed = [o for o in result['outputs'] if o['result']]
    assert len(committed) == 1 and committed[0]['result']['ref']['row_count'] == 3
    assert 'explicit schema' in result['state']['error']['message']
    assert not Path(next(o['uri'] for o in result['outputs'] if o['name'] == 'other')).exists()


def test_output_budget_is_failure_not_truncated_success(root):
    value = prepared(root, limits={'max_output_rows': 1})
    result = execute(root, value)
    assert result['status'] == 'failed'
    assert not result['outputs'][0]['result']
    assert 'budget' in runs.logs(root, result['run_id'])['text']
    assert not Path(result['outputs'][0]['uri']).exists()


def test_cancel_and_timeout_own_run_only(root):
    value = prepared(root)
    runs.start(root, value['run_id'], expected=value['digest'])
    runs.cancel(root, value['run_id'])
    result = wait(root, value['run_id'])
    assert result['status'] == 'cancelled', result
    assert not runs.process_matches(result['state'].get('worker'))
    value = prepared(root, limits={'timeout_s': 1, 'cancel_grace_s': 1})
    result = execute(root, value)
    assert result['status'] == 'failed' and result['state']['reason'] == 'timeout', result


def test_create_reconciliation_uses_marker_and_preserves_fixed_version(root, monkeypatch):
    value = prepared(root)
    output = value['outputs'][0]
    from demiflow.lance.model import LanceWriteSpec
    spec = LanceWriteSpec(uri=output['uri'], mode='create')
    run = runs.journal(root, value['run_id'])
    # This fault fixture has no live child; persist the stopped unit identity.
    (runs.directory(root,value['run_id'])/'unit-1.lock').touch()
    run.transition({'prepared'}, 'running')
    from demiflow.execution import RunContext, SourcePosition
    from demiflow.execution.commit import CommitCoordinator
    from demiflow.execution.lance_sink import LanceSink
    from demiflow.execution.recovery_identity import plan_identity
    import demiflow.execution.commit as protocol
    def fail(name, commit_id):
        if name == 'after_sink_commit_before_receipt':
            raise RuntimeError('lost receipt')
    monkeypatch.setattr(protocol, 'fault_point', fail)
    with pytest.raises(RuntimeError, match='lost receipt'):
        with RunContext.attach(run) as context:
            declaration = plan_identity('write_table', None, None, spec)
            with context.action(declaration, action_id=output['action_id']) as action:
                from demiflow.execution.recovery_contract import identity
                operation_id = identity([context.ref.run_id, action.action_id, 1])
                run.action_event(output['action_id'], 'intent', {'kind': 'write_table',
                    'spec': spec.to_dict(), 'request_hash': spec.content_hash, 'operation_id': operation_id})
                CommitCoordinator(action).commit(1, outputs={'lance': (LanceSink(action, spec),
                    [pa.table({'case_id': ['recovered']})])}, position=SourcePosition('fixed', 1))
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    # An external writer advances the head independently of native recovery.
    lance.write_dataset(pa.Table.from_pylist([{'case_id': 'later'}]), output['uri'], mode='append')
    result = runs.reconcile(root, value['run_id'])
    assert result['status'] == 'interrupted'
    assert result['reconciliation_errors'] == [], result['reconciliation_errors']
    assert result['outputs'][0]['result']['ref']['lance_version'] == 1
    assert rows(root, result)[0]['case_id'] == 'recovered'
    again = runs.reconcile(root, value['run_id'])
    assert again['outputs'] == result['outputs']
    assert lance.dataset(output['uri']).version == 2


def test_wrong_marker_never_promoted_and_registration_recovery(root):
    value = prepared(root)
    output = value['outputs'][0]
    run = runs.journal(root, value['run_id'])
    # This fault fixture has no live child; persist the stopped unit identity.
    (runs.directory(root,value['run_id'])/'unit-1.lock').touch()
    run.transition({'prepared'}, 'running')
    from demiflow.lance.model import LanceWriteSpec
    spec = LanceWriteSpec(uri=output['uri'], mode='create')
    run.action_event(output['action_id'], 'intent', {'kind': 'write_table', 'request_hash': spec.content_hash, 'operation_id': 'different-attempt'})
    # Even the exact same URI and write specification cannot prove attempt ownership.
    lance.write_dataset(pa.Table.from_pylist([{'id': 1}]), output['uri'], mode='create',
                        commit_message='demiflow-lance:unrelated-writer')
    result = runs.reconcile(root, value['run_id'])
    assert result['outputs'][0]['status'] == 'unknown'
    assert result['outputs'][0]['result'] is None
    second = execute(root, prepared(root))
    # Isolate the post-receipt/pre-registration failure window in this owned fixture.
    with sqlite3.connect(runs.journal(root, second['run_id']).path) as db:
        db.execute("DELETE FROM action_events WHERE kind='result'")
    repaired = runs.reconcile(root, second['run_id'])
    assert repaired['outputs'][0]['result']['ref']['row_count'] == 2
    assert lance.dataset(second['outputs'][0]['uri']).version == 1


@pytest.mark.parametrize('value,expected', [('bad', {'kind': 'int'}), (1, {'kind': 'bool'}),
    ({}, {'kind': 'record', 'fields': {'x': {'required': True, 'type': {'kind': 'str'}}}}),
    (None, {'kind': 'str', 'nullable': False})])
def test_actual_return_contract_cannot_rely_on_annotation(value, expected):
    def function(row):
        return value
    contract = dict(parameters=[], returns=expected, label='node:function', max_row_bytes=1024)
    fn = StandardCallable(CallableSpec(function, runtime_contract=contract))
    with pytest.raises(RuntimeContractError):
        fn({'x': 1})
    # Opt-in checks do not alter ordinary native calls.
    assert StandardCallable(CallableSpec(function))({'x': 1}) == value


def test_journal_cas_immutable_events_and_readonly(tmp_path):
    run = RunJournal(tmp_path / 'run')
    with pytest.raises(ValueError):
        run.execution_view()
    assert not run.directory.exists()
    run.initialize({'id': 'test'})
    assert run.transition({None}, 'prepared')
    assert not run.transition({None}, 'starting')
    run.action_event('a', 'intent', {'uri': 'one'})
    run.action_event('a', 'intent', {'uri': 'one'})
    with pytest.raises(ValueError, match='differs'):
        run.action_event('a', 'intent', {'uri': 'two'})
    with pytest.raises(ValueError, match='budget'):
        run.action_event('b', 'intent', {'text': 'x' * 300000})
    assert len(run.execution_view()['events']) == 1
