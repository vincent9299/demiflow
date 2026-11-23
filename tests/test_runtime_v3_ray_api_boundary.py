"""Original API cases through actual Ray Core, including late declared env."""
from pathlib import Path
from types import SimpleNamespace
import json
import os
import sqlite3

import pytest


@pytest.fixture(scope='module')
def ray_api_scope(tmp_path_factory):
    from runtime_v3_api_scenarios import _fixtures, _cases
    from runtime_v3_recovery_scenarios import _provider
    import ray
    import demiflow.execution.dataflow_hosts as hosts
    import demiflow.resources.ray as provider
    assert hosts.ray is ray and provider.ray is ray, 'real Ray must not share a process with carrier doubles'
    assert Path(ray.__file__).is_file() and ray.ObjectRef is ray._raylet.ObjectRef
    assert ray.actor.ActorHandle.__module__ == 'ray.actor'
    root = tmp_path_factory.mktemp('ray-api-boundaries')
    source = _fixtures(root / 'sources')
    cases = {case.name: case for case in _cases(source)}
    keys = ('DF_V3_NATIVE_TEST_URL', 'DF_V3_NATIVE_TEST_KEY')
    old = {key: os.environ.get(key) for key in keys}
    os.environ[keys[0]] = 'http://127.0.0.1:1/stale-cluster-endpoint'
    os.environ[keys[1]] = 'stale-cluster-fixture'
    def restore():
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    try:
        with _provider('ray_core'):
            restore()
            yield root, source, cases
    finally:
        restore()


@pytest.mark.parametrize('name', ['sql-invalid-query', 'model-prompt', 'model-async',
    'model-agent', 'wiki-native', 'wiki-character-bound', 'native-search-http'])
def test_real_ray_api_boundary(ray_api_scope, name):
    from runtime_v3_api_scenarios import _run_case
    root, source, cases = ray_api_scope
    outcome = _run_case(SimpleNamespace(root=root), cases[name], list(cases).index(name), source)
    (root / (name + '.json')).write_text(json.dumps(outcome, indent=2))
    assert outcome['status'] == 'passed', outcome
    assert not outcome['ray_data_import_attempts']
    if name == 'sql-invalid-query':
        assert outcome['error']['type'] == 'OperationalError'
        assert 'invalid_column' in outcome['error']['message']
    if name.startswith('model-'):
        assert outcome['result']['http_requests'] == 1


def test_unrelated_driver_environment_not_copied(ray_api_scope, monkeypatch):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    root, _, _ = ray_api_scope
    from _ray_native_boundary_fixtures import read_worker_environment
    monkeypatch.setenv('DEMIFLOW_RAY_BOUNDARY_UNDECLARED', 'must-not-cross')
    with RunContext.start(root / 'undeclared-run'):
        out = data.from_items([{}]).map(read_worker_environment).take_all()
        assert len(out) == 1 and out[0]['unrelated'] is None
        import sys
        assert out[0]['prefix'] == sys.prefix
        (root/'worker-environment-paths.json').write_text(json.dumps(out,indent=2))


def test_missing_declared_environment_is_not_cluster_fallback(ray_api_scope, monkeypatch):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_runtime_v3_native_execution import PACK
    root, _, _ = ray_api_scope
    monkeypatch.delenv('DF_V3_NATIVE_TEST_URL', raising=False)
    monkeypatch.delenv('DF_V3_NATIVE_TEST_KEY', raising=False)
    with pytest.raises(Exception, match='DF_V3_NATIVE_TEST_URL'):
        with RunContext.start(root / 'missing-run'):
            data.from_items([{'item': 'one'}]).map_prompt('enrich', config=parse_prompt_pack(PACK),
                inputs={'payload': 'item'}, output='answer', max_requests=1).take_all()


def test_declaration_environment_does_not_run_user_hooks():
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.hosts.environment import native_environment_names
    class User:
        @property
        def required_environment_names(self):
            raise AssertionError('user property must never run')
        def __call__(self, row):
            raise AssertionError('user callable must never run')
    assert native_environment_names(CallableSpec.create(User())) == ()
    class FakeSpec:
        @property
        def target(self):
            raise AssertionError('untrusted spec must never run')
    assert native_environment_names(FakeSpec()) == ()


def test_sql_failure_roundtrip_preserves_type_and_bounded_message():
    from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure
    error = sqlite3.OperationalError('no such column: invalid_column')
    payload = failure_payload(error)
    payload['traceback'] = 't' * 8000
    result = restore_failure(payload)
    assert type(result) is sqlite3.OperationalError
    assert str(result) == str(error)
    assert len(result.__notes__[0]) == 8000


def test_real_ray_typed_finite_borrow(ray_api_scope, tmp_path):
    from test_runtime_v3_finite_borrow import test_public_typed_sort_borrows_without_second_payload_copy
    test_public_typed_sort_borrows_without_second_payload_copy(tmp_path)


def test_real_ray_finite_join_restores_original_cut(ray_api_scope, tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts import state_codec
    from demiflow.execution.run_context import RunContext
    from test_runtime_v3_finite_finish import pipeline, Interrupted
    original_event = DataflowExecution._handle_event
    original_commit = DataflowCheckpointCoordinator.commit
    seen, boundary = [], {}
    def event(execution, message):
        value = original_event(execution, message)
        if message.kind == 'terminal':
            seen.append(message.payload['record_id'])
            if len(seen) == 5:
                execution.request_checkpoint()
        return value
    def commit(coordinator, *args, **kwargs):
        image = original_commit(coordinator, *args, **kwargs)
        for contribution in image.contributions:
            for ref in contribution.operator_states.values():
                if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                    continue
                state = state_codec.loads(b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes)),
                    max_bytes=coordinator._physical.bounds.max_state_bytes)
                cursor = state.get('finish_cursor')
                if state.get('kind') == 'join' and cursor and cursor['emitted']:
                    assert not image.cut.final
                    boundary.update(commit_id=image.commit_id, emitted=cursor['emitted'], plan_id=coordinator._physical.plan_id)
                    raise Interrupted('actual finite output ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution, '_handle_event', event)
        patch.setattr(DataflowCheckpointCoordinator, 'commit', commit)
        with pytest.raises(Interrupted, match='actual finite output ACK'):
            with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':64,
                    'checkpoint_interval_ms':60000, 'timeout_s':120}) as run:
                run_ref = run.ref
                pipeline(100,10).take_all()
    assert 0 < boundary['emitted'] < 1000
    with RunContext.resume(run_ref) as run:
        rows = pipeline(100,10).take_all()
        assert len(rows) == 1000
        assert sorted((row['i'],row['j']) for row in rows) == [(i,j) for i in range(100) for j in range(10)]
        assert any(fact['commit_id'] == boundary['commit_id'] and fact['status'] == 'acked'
                   for fact in run.journal.execution_commits('action-1'))
