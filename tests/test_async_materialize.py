"""异步物化固定通用数据行：执行一次、可续接 writer、异常不发布部分缓存。"""
from demiflow.execution.contracts import record_codec

import asyncio
import os
from pathlib import Path

import pyarrow as pa
import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.outputs.dataflow_handles import StoredMaterializedHandle
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from _worker_event_fixtures import emit, events


class CountingActor:
    concurrency = 1
    label = "counting"

    def __init__(self, event_path, fail_at=None):
        self.calls = 0
        self.closed = False
        self.fail_at = fail_at
        self.event_path = str(event_path)
        emit(self.event_path, "construct")

    async def astart(self):
        emit(self.event_path, "start")

    def snapshot_state(self):
        return {"calls": self.calls}

    def restore_state(self, state):
        self.calls = state["calls"]

    async def __call__(self, row):
        self.calls += 1
        emit(self.event_path, "call", value=row["i"])
        await asyncio.sleep(0)
        if row['i'] == self.fail_at:
            raise ValueError('injected materialize failure')
        return {**row, 'value': row['i'] * 2}

    async def aclose(self):
        self.closed = True
        emit(self.event_path, "close", calls=self.calls)


def test_async_materialize_spills_reuses_and_writes(tmp_path):
    executor = LocalDatasetExecutor(workers=1, block_size=2)
    event_path = tmp_path / "actor-events"
    data, actor = DataAPI(executor), CountingActor(event_path)
    cached = (
        data.from_items([{'i': i} for i in range(5)])
        .map_async(actor, concurrency=2, queue_depth=2)
        .filter(lambda row: row['i'] != 3)
        .map(lambda row: {'i': row['i'], 'value': row['value'] + 1})
        .materialize()
    )
    observed = events(event_path)
    assert sorted(e["value"] for e in observed if e["event"] == "call") == list(range(5))
    assert any(e["event"] == "close" for e in observed)
    assert all(e["pid"] != os.getpid() for e in observed if e["event"] in ("start", "call", "close"))
    assert cached.count() == 4
    assert sorted(cached.take_all(), key=lambda row: row['i']) == [
        {'i': i, 'value': i * 2 + 1} for i in [0, 1, 2, 4]
    ]
    handle = cached._source.handle
    assert isinstance(handle, StoredMaterializedHandle) and handle.num_blocks() > 0
    assert all(handle._store.inspect(ref) for ref in handle._blocks)
    cached.write_lance(
        str(tmp_path / 'result.lance'),
        mode='overwrite',
        schema=pa.schema([('i', pa.int64()), ('value', pa.int64())]),
    )
    assert data.read_lance(str(tmp_path / 'result.lance')).count() == 4
    assert len([e for e in events(event_path) if e["event"] == "call"]) == 5
    refs = handle._blocks
    cached.release()
    with pytest.raises(RuntimeError, match="released"):
        next(handle.rows())
    # Release only this handle: historical commit references retain their bytes.
    assert all(handle._store.inspect(ref) for ref in refs)
    executor.close()


def test_failed_materialize_cleans_only_its_spills(tmp_path):
    executor = LocalDatasetExecutor(workers=1, block_size=1)
    data = DataAPI(executor)
    previous = data.from_items([{'i': -1}]).materialize()
    handle = previous._source.handle
    assert isinstance(handle, StoredMaterializedHandle)
    refs = handle._blocks
    event_path = tmp_path / 'failed-events'
    actor = CountingActor(event_path, fail_at=2)
    with pytest.raises(Exception, match='injected materialize failure'):
        data.from_items([{'i': i} for i in range(4)]).map_async(actor, concurrency=1).materialize()
    assert any(e['event'] == 'close' for e in events(event_path))
    failed = RunJournal(executor._run_ref.directory)
    assert failed.execution_status()['status'] == 'failed'
    # Native checkpoints may ACK an empty/nonfinal recovery image before the
    # callback fails; this must not expose a partial materialized result.
    assert failed.execution_action('action-1')['result'] is None
    for commit in failed.execution_commits('action-1'):
        assert commit['status'] == 'acked'
        prepared = commit['prepared']['sinks']['materialize']
        assert prepared['rows'] == 0
        assert prepared['manifest']['final'] is False
        assert prepared['manifest']['partitions'] == []
    assert failed.pending_dataflow_gc() is False
    from demiflow.execution.storage.gc import gc_paths
    with failed._connection() as db:
        collected = [dict(zip(('gc_id', 'store_id', 'digest', 'status'), row))
                     for row in db.execute('SELECT gc_id,store_id,digest,status FROM runtime_dataflow_gc')]
        allocations = db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0]
    assert allocations == 0
    assert collected, "failed attempt did not collect any of its unreferenced objects"
    for item in collected:
        assert item['status'] == 'done'
        target, quarantine = gc_paths(failed, item)
        assert not target.exists() and not quarantine.exists()
    assert failed.directory != handle._store.root.parent
    assert not list((failed.directory / '.dataflow-store').rglob('*.tmp'))
    assert all(handle._store.inspect(ref) for ref in refs)
    assert previous.take_all() == [{'i': -1}]
    previous.release()
    executor.close()


def test_empty_async_materialize_closes_actor(tmp_path):
    executor = LocalDatasetExecutor(workers=1)
    event_path = tmp_path / 'empty-events'
    actor = CountingActor(event_path)
    cached = DataAPI(executor).from_items([]).map_async(actor).materialize()
    assert cached.count() == 0 and cached.take_all() == []
    observed = events(event_path)
    assert not [e for e in observed if e['event'] == 'call']
    assert any(e['event'] == 'close' and e['pid'] != os.getpid() for e in observed)
    cached.release()
    executor.close()


def test_batch_materialize_reuses_complete_batches(tmp_path):
    executor = LocalDatasetExecutor(workers=1, block_size=2)
    data = DataAPI(executor)
    calls = tmp_path / "batch-calls"

    def summarize(rows):
        emit(calls, "call", count=len(rows))
        return [{'total': sum(row['i'] for row in rows)}]

    cached = (
        data.from_items([{'i': i} for i in range(5)])
        .batch_map(summarize, max_batch=2, concurrency=1)
        .materialize()
    )
    assert sorted(row['total'] for row in cached.take_all()) == [1, 4, 5]
    assert cached.count() == 3 and [e["count"] for e in events(calls)] == [2, 2, 1]
    assert cached.map(lambda row: {'total': row['total'] * 2}).count() == 3
    assert [e["count"] for e in events(calls)] == [2, 2, 1]
    executor.close()


def test_wide_rows_spill_before_row_limit_and_restore_individually(tmp_path, monkeypatch):
    from demiflow.execution.exchange import dataflow_bridge
    executor = LocalDatasetExecutor(workers=1, block_size=256)
    def rows():
        for i in range(6):
            yield {'i': i, 'payload': str(i) * 2048, 'nested': [i]}
    # The v3 store is the materialization authority; memory/batch budgets now
    # belong to the run, rather than the retired executor's private spill list.
    with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 8 * 1024**2,
            'max_row_bytes': 8192, 'max_batch_bytes': 8192}) as run:
        cached = DataAPI(executor).from_iter(rows).materialize()
        handle = cached._source.handle
        assert isinstance(handle, StoredMaterializedHandle)
        assert handle.num_blocks() > 0 and all(handle._store.inspect(r) for r in handle._blocks)
        restored = []
        original = record_codec._decode_record_document
        def record_decode(*args, **kwargs):
            record = original(*args, **kwargs)
            restored.append(record)
            assert record.encoded_bytes <= 8192
            return record
        with monkeypatch.context() as probe:
            probe.setattr(record_codec, '_decode_record_document', record_decode)
            iterator = handle.rows()
            assert next(iterator) == {'i': 0, 'payload': '0' * 2048, 'nested': [0]}
            iterator.close()
            assert len(restored) == 1, 'stored reader must decode only the requested row'
        assert cached.take(1)[0]['i'] == 0
        assert cached.count() == 6
        cached.release()
    executor.close()


def test_materialization_retains_snapshot_for_reused_mutable_input():
    executor = LocalDatasetExecutor(workers=1, block_size=256)
    def rows():
        row = {'value': []}
        for i in range(4):
            row['value'][:] = [i]
            yield row
    cached = DataAPI(executor).from_iter(rows).materialize()
    assert cached.take_all() == [{'value': [i]} for i in range(4)]
    cached.take_all()[0]['value'].append('consumer mutation')
    assert cached.take_all()[0] == {'value': [0]}
    executor.close()
