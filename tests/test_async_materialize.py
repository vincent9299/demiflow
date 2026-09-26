"""异步物化固定通用数据行：执行一次、可续接 writer、异常不发布部分缓存。"""

import asyncio
from pathlib import Path

import pyarrow as pa
import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor


class CountingActor:
    concurrency = 1
    label = "counting"

    def __init__(self, fail_at=None):
        self.calls = 0
        self.closed = False
        self.fail_at = fail_at

    async def __call__(self, row):
        self.calls += 1
        await asyncio.sleep(0)
        if row['i'] == self.fail_at:
            raise ValueError('injected materialize failure')
        return {**row, 'value': row['i'] * 2}

    async def aclose(self):
        self.closed = True


def test_async_materialize_spills_reuses_and_writes(tmp_path):
    executor = LocalDatasetExecutor(workers=1, block_size=2, materialize_memory_limit=1)
    data, actor = DataAPI(executor), CountingActor()
    cached = (
        data.from_items([{'i': i} for i in range(5)])
        .map_async(actor, concurrency=2, queue_depth=2)
        .filter(lambda row: row['i'] != 3)
        .map(lambda row: {'i': row['i'], 'value': row['value'] + 1})
        .materialize()
    )
    assert actor.calls == 5 and actor.closed
    assert cached.count() == 4
    assert sorted(cached.take_all(), key=lambda row: row['i']) == [
        {'i': i, 'value': i * 2 + 1} for i in [0, 1, 2, 4]
    ]
    assert executor._spill_paths
    cached.write_lance(
        str(tmp_path / 'result.lance'),
        mode='overwrite',
        schema=pa.schema([('i', pa.int64()), ('value', pa.int64())]),
    )
    assert data.read_lance(str(tmp_path / 'result.lance')).count() == 4
    assert actor.calls == 5
    paths = list(executor._spill_paths)
    executor.close()
    assert all(not Path(p).exists() for p in paths)


def test_failed_materialize_cleans_only_its_spills():
    executor = LocalDatasetExecutor(workers=1, block_size=1, materialize_memory_limit=1)
    data = DataAPI(executor)
    previous = data.from_items([{'i': -1}]).materialize()
    previous_paths = set(executor._spill_paths)
    actor = CountingActor(fail_at=2)
    with pytest.raises(Exception, match='injected materialize failure'):
        data.from_items([{'i': i} for i in range(4)]).map_async(actor, concurrency=1).materialize()
    assert actor.closed
    assert executor._spill_paths == previous_paths
    assert previous.take_all() == [{'i': -1}]
    executor.close()


def test_empty_async_materialize_closes_actor():
    executor = LocalDatasetExecutor(workers=1)
    actor = CountingActor()
    cached = DataAPI(executor).from_items([]).map_async(actor).materialize()
    assert cached.count() == 0 and cached.take_all() == []
    assert actor.calls == 0 and actor.closed
    executor.close()


def test_batch_materialize_reuses_complete_batches():
    executor = LocalDatasetExecutor(workers=1, block_size=2)
    data = DataAPI(executor)
    calls = []

    def summarize(rows):
        calls.append(len(rows))
        return [{'total': sum(row['i'] for row in rows)}]

    cached = (
        data.from_items([{'i': i} for i in range(5)])
        .batch_map(summarize, max_batch=2, concurrency=1)
        .materialize()
    )
    assert sorted(row['total'] for row in cached.take_all()) == [1, 4, 5]
    assert cached.count() == 3 and calls == [2, 2, 1]
    assert cached.map(lambda row: {'total': row['total'] * 2}).count() == 3
    assert calls == [2, 2, 1]
    executor.close()
