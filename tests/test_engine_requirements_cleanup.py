"""Counter-examples for the engine clean-up: per-request executor, verified source
snapshots for key exclusion, the retired Lance checkpoint entry, and the removed
queue forwarding."""
import inspect
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.compiler_support.dataflow_compile_inputs import compile_inputs
from demiflow.execution.contracts.dataflow import ActionRequest, RuntimeServices, SourceSnapshot
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext


def _request(action, executor):
    dataset = data.from_items([{'x': 1}])
    return ActionRequest(kind='take', source=dataset._source, plan=dataset._plan, arguments={'limit': 1},
                         sink=None, programs=(), provider='local', action=action, executor=executor)


def _executor(batch_rows):
    return SimpleNamespace(_local_kernel=SimpleNamespace(batch_rows=batch_rows, partitions=None), _block_size=None)


def test_two_requests_under_one_action_keep_their_own_executor():
    action = type('Action', (), {'action_id': 'shared'})()
    first = _request(action, _executor(128))
    second = _request(action, _executor(1024))
    assert compile_inputs(first).defaults.batch_rows == 128
    assert compile_inputs(second).defaults.batch_rows == 1024
    assert compile_inputs(first).defaults.batch_rows == 128
    assert not hasattr(action, '_dataflow_executor')


def test_request_issued_by_the_public_entry_does_not_mark_the_shared_action(tmp_path):
    from demiflow.execution.executors import dataflow_api
    assert '_dataflow_executor' not in inspect.getsource(dataflow_api)
    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=1, partitions=1, worker_mode='thread'):
        assert data.from_items([{'x': 1}, {'x': 2}]).take_all() == [{'x': 1}, {'x': 2}]


def test_source_snapshots_are_validated_and_do_not_rebuild_the_services():
    with pytest.raises(DataflowError):
        RuntimeServices.with_source_snapshots(SimpleNamespace(), {'a': object()})
    with pytest.raises(DataflowError):
        RuntimeServices.with_source_snapshots(SimpleNamespace(), ['not', 'a', 'mapping'])


def test_key_payload_without_a_verified_snapshot_is_refused_before_any_read():
    from demiflow.execution.sources.source_readers import PinnedKeyPayload
    spec = SimpleNamespace(configuration={'lance_key_source': 'a' * 64})
    for snapshots in (None, {}):
        with pytest.raises(DataflowError) as refused:
            PinnedKeyPayload(spec, SimpleNamespace(source_snapshots=snapshots))
        assert refused.value.code == 'INCOMPATIBLE_HISTORY'


def test_key_exclusion_reader_does_not_load_the_sealed_plan_again(tmp_path, monkeypatch):
    from demiflow.execution.seal import plan as seal_plan
    original = seal_plan.load_sealed_dataflow
    callers = []

    def counted(*args, **kwargs):
        callers.append(inspect.stack()[1].filename)
        return original(*args, **kwargs)

    monkeypatch.setattr(seal_plan, 'load_sealed_dataflow', counted)
    rows = [{'k': 'b', 'v': 3}, {'k': 'a', 'v': 2}, {'k': 'b', 'v': 3}, {'k': None, 'v': 7}]
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(pa.Table.from_pylist(rows), uri, mode='create')
    with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 1 << 30}), data.local_execution(
            workers=1, partitions=1, worker_mode='thread', memory_bytes=1 << 30):
        result = data.read_lance(uri, version=1).exclude_keys(
            data.from_items([{'k': 'a'}, {'k': None}]), on='k').take_all()
    assert result == [rows[0], rows[2], rows[3]]
    assert callers, 'the worker still loads and verifies its sealed plan'
    assert not [name for name in callers if name.endswith('source_readers.py')]


def test_retired_lance_checkpoint_is_refused_before_it_reads_or_advances_a_stage(tmp_path):
    from demiflow.lance.run import LanceRun
    run = LanceRun.__new__(LanceRun)
    run.storage_root, run.relative = tmp_path, 'runs/r1'
    run.manifest = {'implementation': 'test'}
    run.previous = run.version = 'manifest-digest'
    run.stages, run.reused, run.new = {}, [], []
    with pytest.raises(RecoveryContractError):
        run.lance_checkpoint(None, 'S')
    assert (run.stages, run.reused, run.new, run.previous) == ({}, [], [], 'manifest-digest')


def test_queue_module_no_longer_forwards_the_result_sink():
    from demiflow.execution.native_queue import queue, queue_sinks
    assert not hasattr(queue, 'QueueResultSink')
    assert queue_sinks.QueueResultSink is not None
