"""Independent review counterexamples: real public calls and historical bytes."""
import asyncio
import functools
import threading
from types import SimpleNamespace

import cloudpickle
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.recovery_identity import describe, operation_identity, plan_identity
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('size', [1, 1024, 1025, 1500])
def test_prompt_long_enum_preserves_historical_native_bytes(size):
    from test_gpt_prompt_policy_sealing import TEXT
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.execution.plan_facts import operation_facts
    import json
    text = TEXT.replace('result: {type: string}',
                        'result: {type: string, enum: ' + json.dumps([str(i) for i in range(size)]) + '}')
    ds = data.from_items([{'item': 'one'}]).map_prompt_async('enrich',
        config=parse_prompt_pack(text), inputs={'payload': 'item'}, output='answer')
    op = ds._plan.operations[-1]
    native = operation_facts(op)['native']
    # Exact projection in b4dd417/e40a6dd, independent of the new helper.
    historical = {'native': native, 'policy': {key: describe(getattr(op, key))
        for key in ('concurrency', 'queue_depth', 'catch', 'hard_timeout', 'execution') if hasattr(op, key)}}
    assert identity(operation_identity(op)) == identity(historical)
    assert identity(plan_identity('materialize', ds._source, ds._plan)['operations'][0]) == identity(historical)
    from demiflow.execution.chain import plan_code_pin, _stable_code_identity
    assert plan_code_pin(ds._plan)['operations'] == _stable_code_identity([historical])


def _encoded(callback):
    return cloudpickle.dumps(data.from_items([{'v': 1}]).map_batches(
        callback, batch_format='pyarrow')._plan.operations[0])


@pytest.mark.parametrize('before,after', [(pa.int64(), pa.float64()), (pa.int8(), pa.int64())])
def test_arrow_cast_requires_live_sum_before_allocating(tmp_path, before, after):
    from demiflow.execution.arrow_batch_sink import apply_batch
    table = pa.table({'v': pa.array([7] * 20000, type=before)})
    schema = pa.schema([('v', after)])
    one_copy = 20000 * 8 + 2500
    path = str(tmp_path/'out.arrow')
    with pytest.raises(RecoveryContractError, match='cast live input and output'):
        apply_batch(table, _encoded(lambda t: t), path, schema,
                    max_bytes=1024**2, memory_bytes=one_copy)
    assert not (tmp_path/'out.arrow').exists()
    result = apply_batch(table, _encoded(lambda t: t), path, schema,
                         max_bytes=1024**2, memory_bytes=1024**2)
    assert result.value.rows == 20000
    with pa.OSFile(path) as f:
        assert pa.ipc.open_stream(f).read_all().equals(table.cast(schema))


def test_arrow_sum_rejection_does_not_call_cast(tmp_path, monkeypatch):
    from demiflow.execution import arrow_batch_sink as sink
    table = pa.table({'v': list(range(10000))})
    real = sink._cast_allocation
    calls = []
    def reserve(table, schema):
        calls.append(1)
        return real(table, schema)
    monkeypatch.setattr(sink, '_cast_allocation', reserve)
    # The allocation estimate is consulted before any new conversion buffers.
    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    old_pool = pa.default_memory_pool()
    pa.set_memory_pool(pool)
    try:
        with pytest.raises(RecoveryContractError, match='cast live input and output'):
            sink.apply_batch(table, _encoded(lambda t: t), str(tmp_path/'out.arrow'),
                pa.schema([('v', pa.float64())]), 1024**2, memory_bytes=81250)
        assert calls == [1] and pool.max_memory() == 0
    finally:
        pa.set_memory_pool(old_pool)


def test_arrow_schema_noop_uses_shared_buffers(tmp_path):
    from demiflow.execution.arrow_batch_sink import apply_batch
    table = pa.table({'v': [1, 2, 3]})
    result = apply_batch(table, _encoded(lambda t: t), str(tmp_path/'out.arrow'),
                         table.schema, 4096, memory_bytes=table.get_total_buffer_size())
    assert result.value.rows == 3


class CountFactory:
    def __init__(self):
        self.count = 0
    def __call__(self, row):
        self.count += 1
        return {'count': self.count}


@pytest.mark.parametrize('scope', ['stage', 'worker'])
def test_constructed_factory_uses_actual_owner_concurrency(tmp_path, scope, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    called = tmp_path / 'called'
    class CountFactory:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            called.write_text('called')
            self.count += 1
            return {'count': self.count}
    ds = data.from_items([{'i': n} for n in range(6)]).map(
        CountFactory, concurrency=3, execution='thread', callable_scope=scope)
    if scope == 'stage':
        with pytest.raises(DataflowError, match='auto_json state requires concurrency 1'):
            with RunContext.start(tmp_path/'run'):
                ds.materialize()
        assert not called.exists()
    else:
        with RunContext.start(tmp_path/'run'):
            rows = ds.materialize().take_all()
            assert len(rows) == 6
            assert sum(row['count'] == 1 for row in rows) == 3
            assert all(1 <= row['count'] <= 4 for row in rows)


def runtime_lock_factory():
    class RuntimeLock:
        def __init__(self):
            import threading
            self.lock = threading.Lock()
            self.count = 0
        def __call__(self, row):
            with self.lock:
                self.count += 1
            return row
    return RuntimeLock


def test_runtime_resource_execution_is_honest_and_resume_fails_closed(tmp_path):
    RuntimeLock = runtime_lock_factory()
    from demiflow.execution.operator_state import OperatorStates
    from demiflow.workbench.runs.facts import read_sealed_plan, _sealed_entry
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.data.api import DataAPI
    # Strict worker/state consumers retain the fail-closed policy.
    strict = OperatorStates(SimpleNamespace(limits={'max_state_bytes': 65536}))
    with pytest.raises(RecoveryContractError, match='snapshot_state/restore_state'):
        asyncio.run(strict.register(RuntimeLock(), 'node-0'))
    executor = LocalDatasetExecutor(workers=1)
    api = DataAPI(executor)
    ds = api.from_items([{'i': i} for i in range(4)]).map(RuntimeLock, execution='thread')
    try:
        with pytest.raises(RuntimeError, match='interrupt'):
            with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}) as run:
                reference = run.ref
                # Interrupt the actual worker graph after a non-final native commit.
                from demiflow.execution.action_driver.dataflow import DataflowExecution
                original = DataflowExecution.pump
                def interrupt(execution, **kwargs):
                    result = original(execution, **kwargs)
                    if execution._restore_image is not None and not execution._restore_image.cut.final:
                        raise RuntimeError('interrupt')
                    return result
                from unittest.mock import patch
                with patch.object(DataflowExecution, 'pump', interrupt):
                    ds.materialize()
        fact = read_sealed_plan(tmp_path/'run', 'action-1')
        assert fact['available']
        assert _sealed_entry(fact)['recovery_restriction']['recovery'] == 'attempt_only'
        with pytest.raises(RecoveryContractError, match='Cannot resume attempt-only actor state'):
            with RunContext.resume(reference):
                ds.materialize()
    finally:
        executor.close()


def test_cleanup_cancellation_still_closes_all_owners():
    from demiflow.execution.stream_resources import StreamResources
    events = []
    class First:
        async def aclose(self):
            events.append('first')
    class Last:
        async def aclose(self):
            events.append('last')
            raise asyncio.CancelledError()
    async def close():
        with pytest.raises(BaseExceptionGroup):
            await StreamResources([First(), Last()]).close(action_cleanups=False)
    asyncio.run(close())
    assert events == ['last', 'first']


def test_partial_delegates_state_to_wrapped_actor():
    from demiflow.execution.operator_state import OperatorStates
    actor = CountFactory()
    state = OperatorStates(SimpleNamespace(limits={'max_state_bytes': 65536}))
    asyncio.run(state.register(functools.partial(actor), 'node-0'))
    assert state.owners['node-0'] is actor
    assert asyncio.run(state.snapshot())['node-0']['fields']['count'] == 0


def test_resource_restriction_does_not_block_completed_result_reuse(tmp_path):
    RuntimeLock = runtime_lock_factory()
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.data.api import DataAPI
    executor = LocalDatasetExecutor(workers=1)
    api = DataAPI(executor)
    ds = api.from_items([{'i': 1}]).map(RuntimeLock, execution='thread')
    try:
        with pytest.raises(RuntimeError, match='later'):
            with RunContext.start(tmp_path/'run') as run:
                reference = run.ref
                assert ds.materialize().take_all() == [{'i': 1}]
                raise RuntimeError('later')
        with RunContext.resume(reference):
            assert ds.materialize().take_all() == [{'i': 1}]
    finally:
        executor.close()


def test_folded_predicate_uses_actual_stage_concurrency(tmp_path):
    # The unified compiler puts this stateful predicate in its own serial stage.
    # Upstream concurrency must neither leak into its state nor disable calls.
    observed = tmp_path / 'predicate-counts'
    class Predicate:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            self.count += 1
            with observed.open('a') as file:
                file.write(str(self.count) + '\n')
            return True
    ds = data.from_items([{'i': i} for i in range(6)]).map_async(
        lambda row: row, concurrency=3).filter(Predicate())
    with RunContext.start(tmp_path/'run'):
        assert sorted(ds.materialize().take_all(), key=lambda r: r['i']) == [{'i': i} for i in range(6)]
    assert observed.read_text().splitlines() == [str(i) for i in range(1, 7)]


def test_map_cleanup_cancellation_does_not_abandon_sibling_workers():
    from demiflow.execution.stream_map import StreamMapRuntime
    runtime = StreamMapRuntime(SimpleNamespace(stream_options=None))
    events = []
    class Worker:
        def __init__(self, n):
            self.n = n
        async def aclose(self):
            events.append(self.n)
            if self.n == 1:
                raise asyncio.CancelledError()
    runtime.workers = [Worker(1), Worker(2)]
    with pytest.raises(BaseExceptionGroup):
        asyncio.run(runtime.aclose())
    assert events == [1, 2]


def test_registration_failure_closes_actor_even_if_stop_fails():
    from test_upgrade_v2_unit_S012 import Lifecycle, FailingRegister, worker
    class BrokenStop(Lifecycle):
        async def astop(self):
            self.stops += 1
            raise RuntimeError('stop failed')
    actor = BrokenStop()
    w = worker(actor, FailingRegister(RuntimeError('registration failed')))
    with pytest.raises(RuntimeError, match='registration failed'):
        asyncio.run(w.prepare())
    # CallableSpec constructs the execution-owned instance. Inspect that instance,
    # as S012 does, rather than the original declaration object copied into it.
    executed = w.callable._fn
    assert executed.stops == executed.closes == 1


def test_runtime_resource_inventory_is_bounded_before_copying_fields():
    from demiflow.execution.operator_state import runtime_resource_state
    actor = runtime_lock_factory()()
    actor.__dict__.update({str(i): actor.lock for i in range(16384)})
    assert not runtime_resource_state(actor)
