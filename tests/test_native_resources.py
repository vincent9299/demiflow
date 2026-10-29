"""Explicit Dataset budgets retain shared, weighted native admission."""
from dataclasses import replace
import fcntl
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import datafusion as native
from demiflow.execution import native_resources as resources
from demiflow.execution.local_kernel import LocalKernelOptions

GiB = 2**30


def session(path, rss, *, headroom=GiB):
    return native.DataFusionSession(resource_directory=path, max_concurrent=2,
        slot_capacity={'max_rss_bytes_per_slot': 8*GiB, 'threads_per_slot': 1},
        options=native.DataFusionOptions(memory_bytes=64*2**20, max_rss_bytes=rss,
            threads=1, partitions=1, cgroup_headroom_bytes=headroom,
            admission_timeout_s=.06, timeout_s=10))


@pytest.fixture
def capacity(monkeypatch):
    memory = [0, 32*GiB]
    monkeypatch.setattr(native, '_cpu_capacity', lambda: 4)
    monkeypatch.setattr(native, '_cgroup_memory', lambda: tuple(memory))
    return memory


def test_mixed_budgets_reserve_actual_rss_and_maximum_headroom(tmp_path, capacity):
    with session(tmp_path, 4*GiB, headroom=2*GiB) as larger, session(tmp_path, 2*GiB) as smaller:
        capacity[1] = 9*GiB
        with larger._admit():
            with smaller._admit():
                assert len(list(tmp_path.glob('slot-*.json'))) == 2
        assert not list(tmp_path.glob('slot-*.json'))
        # A small caller cannot account a busy larger worker at its own budget.
        capacity[1] = 7*GiB
        with larger._admit():
            with pytest.raises(TimeoutError, match='no query was launched'):
                with smaller._admit():
                    pytest.fail('Mixed reservations exceeded available memory')
        assert not list(tmp_path.glob('slot-*.json'))


@pytest.mark.parametrize('record', [None, 'invalid', '{"max_rss_bytes":1}', 'x'*4097,
    json.dumps({'max_rss_bytes': GiB, 'cgroup_headroom_bytes': GiB})])
def test_legacy_or_malformed_busy_slot_uses_capacity(tmp_path, capacity, record):
    with session(tmp_path, 2*GiB) as smaller:
        # Simulate an already-running old process without the new ledger.
        lock = (tmp_path/'slot-0.lock').open('a+b')
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if record is not None:
                (tmp_path/'slot-0.json').write_text(record)
            capacity[1] = 10*GiB
            with pytest.raises(TimeoutError):
                with smaller._admit():
                    pytest.fail('Legacy reservation was underestimated')
        finally:
            lock.close()
        # Unlocked stale records carry no reservation authority.
        capacity[1] = 4*GiB
        with smaller._admit():
            pass


def test_two_slot_reservation_and_exception_release(tmp_path, capacity):
    with session(tmp_path, 2*GiB) as first, session(tmp_path, 2*GiB) as second:
        with pytest.raises(RuntimeError, match='cancelled'):
            with first._admit(slots_required=2):
                with pytest.raises(TimeoutError):
                    with second._admit():
                        pytest.fail('No slot is free')
                raise RuntimeError('cancelled')
        with second._admit(slots_required=2):
            pass


def test_default_session_unchanged_and_budget_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(resources, '_cpu_capacity', lambda: 16)
    monkeypatch.setattr(resources, '_cgroup_memory', lambda: (0, 256*GiB))
    executor = SimpleNamespace(resource_root=tmp_path, _local_kernel=None)
    default = resources.dataset_session(executor, tmp_path)
    assert (default.options.memory_bytes, default.options.max_rss_bytes,
        default.options.threads, default.options.max_scratch_bytes) == (8*GiB, 24*GiB, 8, 128*GiB)
    supplied = dict(memory_bytes=2*GiB, max_rss_bytes=6*GiB, threads=2)
    executor._local_kernel = LocalKernelOptions(native_resources=supplied)
    supplied['max_rss_bytes'] = 99*GiB
    explicit = resources.dataset_session(executor, tmp_path)
    assert explicit.resources == default.resources
    assert explicit._slot_capacity == default._slot_capacity
    assert explicit.options.max_rss_bytes == 6*GiB
    assert explicit.options.cgroup_headroom_bytes == 2*GiB
    assert replace(executor._local_kernel).native_resources == executor._local_kernel.native_resources


@pytest.mark.parametrize('budget', [
    {'engine': 'other'}, {'threads': True}, {'memory_bytes': -1},
    {'timeout_s': float('nan')}, {'admission_timeout_s': float('inf')},
    {'max_rss_bytes': 48*GiB}, {'cgroup_headroom_bytes': 1},
    {'memory_bytes': GiB, 'max_rss_bytes': GiB},
])
def test_budget_rejects_invalid_values_expansion_and_disabled_protection(tmp_path, budget):
    with pytest.raises(ValueError):
        options = LocalKernelOptions(native_resources=budget)
        resources.dataset_session(SimpleNamespace(resource_root=tmp_path, _local_kernel=options), tmp_path)


def test_real_dataset_uses_explicit_limits_and_preserves_join_multiplicity(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.dataset_native import Engine
    original = Engine.render_batches
    observed = []
    def bounded_batches(self, *args, **kwargs):
        for batch in original(self, *args, **kwargs):
            observed.append(batch.num_rows)
            yield batch
    monkeypatch.setattr(Engine, 'render_batches', bounded_batches)
    left, right, output = (tmp_path/name for name in ('left.lance', 'right.lance', 'out.lance'))
    lance.write_dataset(pa.table({'k': [1, 1, None, 2], 'v': ['a', 'b', 'null', 'c']}), str(left))
    lance.write_dataset(pa.table({'k': [1, 1, None, 2], 'w': [3, 4, 9, 5]}), str(right))
    budget = dict(memory_bytes=64*2**20, max_rss_bytes=2*GiB, threads=1,
        partitions=1, batch_rows=2, max_scratch_bytes=GiB, timeout_s=30, admission_timeout_s=30)
    with data.local_execution(workers=1, worker_mode='thread', native_resources=budget) as handle:
        data.read_lance(str(left), version=1).join(data.read_lance(str(right), version=1), on='k').write_lance(
            str(output), schema=pa.schema([('k', pa.int64()), ('v', pa.string()), ('w', pa.int64())]))
        stats = handle.stats
    rows = lance.dataset(str(output)).to_table().to_pylist()
    assert sorted((r['v'], r['w']) for r in rows) == [('a', 3), ('a', 4), ('b', 3), ('b', 4), ('c', 5)]
    assert observed and max(observed) <= 2
    queries = [s for s in stats['stages'] if s['name'] == 'datafusion']
    assert queries
    for query in queries:
        request = json.loads((Path(query['query'])/'request.json').read_text())
        assert request['options']['max_rss_bytes'] == 2*GiB
        assert request['options']['memory_bytes'] == 64*2**20
        assert request['options']['threads'] == 1
        assert request['options']['batch_rows'] == 2
