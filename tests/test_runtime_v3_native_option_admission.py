"""Retired Ray Data pool options cannot silently select a different execution."""
from pathlib import Path

import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp, MapBatchesOp
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.planning.dataflow_compile import compile_kernel_spec
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server


def _record(value, *, marker):
    with Path(marker).open('a') as stream:
        stream.write('call\n')
    return value


@pytest.mark.parametrize('operation', [MapOp, MapBatchesOp])
@pytest.mark.parametrize('options', [
    NativeOptions('ray', compute_kind='actor_pool'),
    NativeOptions('ray', compute_kind='task_pool'),
    NativeOptions('ray', min_workers=2),
    NativeOptions('ray', initial_workers=2),
    NativeOptions('ray', max_workers=3),
])
def test_passive_compiler_rejects_every_retired_pool_field(operation, options):
    op = operation(CallableSpec.create(_record), native_options=options)
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    with pytest.raises(ValueError, match='Ray Data compute/worker ranges'):
        compile_kernel_spec(op, program_ref=None, bounds=bounds)


def test_unknown_kind_is_rejected_before_any_instance_attribute_read():
    reads = []
    class UnknownOperation:
        def __getattribute__(self, name):
            reads.append(name)
            raise AssertionError('passive admission read unknown user object')
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    with pytest.raises(DataflowError, match='UNSUPPORTED_CAPABILITY'):
        compile_kernel_spec(UnknownOperation(), program_ref=None, bounds=bounds)
    assert reads == []


@pytest.mark.parametrize('method', ['map', 'map_batches'])
@pytest.mark.parametrize('compute', [
    {'kind': 'task_pool', 'size': 2},
    {'kind': 'actor_pool', 'min_size': 2, 'initial_size': 2, 'max_size': 3},
])
def test_public_pool_rejected_before_business_and_allocation(
        tmp_path, monkeypatch, native_server, method, compute):
    marker = tmp_path / 'business-called'
    activated = []
    original = DataflowExecution._activate
    def observe(execution, lease, stage):
        activated.append(lease.allocation_id)
        return original(execution, lease, stage)
    monkeypatch.setattr(DataflowExecution, '_activate', observe)
    with pytest.raises(ValueError, match='Ray Data compute/worker ranges'):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            source = data.from_items([{'item': 'a'}])
            mapped = getattr(source, method)(_record, fn_kwargs={'marker': str(marker)},
                backend_options={'ray': {'compute': compute}})
            result = mapped.map_prompt_async('enrich', config=parse_prompt_pack(PACK),
                inputs={'payload': 'item'}, output='answer', concurrency=1,
                max_requests=1,
                request_policy={'initial_concurrency': 1, 'max_concurrency': 1})
            result.take_all()
    with run.journal._connection() as db:
        commits = db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0]
    assert not marker.exists()
    assert native_server == [] and activated == [] and commits == 0


@pytest.mark.parametrize('method', ['map', 'map_batches'])
def test_public_resource_only_request_reaches_original_grant(tmp_path, monkeypatch, method):
    marker = tmp_path / 'business-called'
    allocations = []
    original = DataflowExecution._activate
    def observe(execution, lease, stage):
        result = original(execution, lease, stage)
        actual = execution._scheduler._observation(lease.allocation_id)
        allocations.append((dict(stage.profile.requests), actual['effective_resources']))
        return result
    monkeypatch.setattr(DataflowExecution, '_activate', observe)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        result = getattr(data.from_items([{'item': 'a'}]), method)(_record,
            fn_kwargs={'marker': str(marker)},
            backend_options={'ray': {'worker_resources': {'cpu': 2}}})
        assert result.take_all() == [{'item': 'a'}]
        assert marker.read_text() == 'call\n'
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    requested = [(p, a) for p, a in allocations if p['cpu_millis'] >= 2000]
    assert requested and all(a['cpu_millis'] >= p['cpu_millis'] for p, a in requested)
