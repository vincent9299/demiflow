"""Explicit Dataset budgets retain shared, weighted native admission."""
from dataclasses import replace
import fcntl
import json

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.hosts import datafusion as native
from demiflow.execution.pipeline.local_kernel import LocalKernelOptions

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


def test_public_native_budget_is_snapshotted():
    supplied = dict(memory_bytes=2*GiB, max_rss_bytes=6*GiB, threads=2)
    options = LocalKernelOptions(native_resources=supplied)
    supplied['max_rss_bytes'] = 99*GiB
    assert dict(options.native_resources)['max_rss_bytes'] == 6*GiB
    assert replace(options).native_resources == options.native_resources


@pytest.mark.parametrize('budget', [
    {'engine': 'other'}, {'threads': True}, {'memory_bytes': -1},
    {'timeout_s': float('nan')}, {'admission_timeout_s': float('inf')},
])
def test_public_native_resources_rejects_invalid_declarations(budget):
    with pytest.raises(ValueError):
        LocalKernelOptions(native_resources=budget)


def test_real_dataset_uses_explicit_limits_and_preserves_join_multiplicity(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.outputs.lance_sink import LanceSink
    from demiflow.execution.hosts.run_state import RunResourceState
    from demiflow.execution.pipeline.run_context import RunContext
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    original, charged = LanceSink.prepare, RunResourceState._charged
    observed, charges = [], []
    def bounded_batches(sink, blocks, context):
        def batches():
            for batch in blocks:
                observed.append(batch.num_rows)
                yield batch
        return original(sink, batches(), context)
    def bounded_grants(state, db, scope):
        result = charged(state, db, scope)
        charges.append(result['memory_bytes'])
        assert result['memory_bytes'] <= 64*2**20
        return result
    monkeypatch.setattr(LanceSink, 'prepare', bounded_batches)
    monkeypatch.setattr(RunResourceState, '_charged', bounded_grants)
    left, right, output = (tmp_path/name for name in ('left.lance', 'right.lance', 'out.lance'))
    lance.write_dataset(pa.table({'k': [1, 1, None, 2], 'v': ['a', 'b', 'null', 'c']}), str(left))
    lance.write_dataset(pa.table({'k': [1, 1, None, 2], 'w': [3, 4, 9, 5]}), str(right))
    budget = dict(memory_bytes=64*2**20, max_rss_bytes=2*GiB, threads=1,
        partitions=1, batch_rows=2, max_scratch_bytes=GiB, timeout_s=30, admission_timeout_s=30)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=1, worker_mode='thread', native_resources=budget):
        data.read_lance(str(left), version=1).join(data.read_lance(str(right), version=1), on='k').write_lance(
            str(output), schema=pa.schema([('k', pa.int64()), ('v', pa.string()), ('w', pa.int64())]))
        physical = load_sealed_plan(run.directory, 'action-1', version='v3')['physical']
        assert all(f['status']=='acked' for f in run.journal.execution_commits('action-1'))
    rows = lance.dataset(str(output)).to_table().to_pylist()
    assert sorted((r['v'], r['w']) for r in rows) == [('a', 3), ('a', 4), ('b', 3), ('b', 4), ('c', 5)]
    assert observed and max(observed) <= 2
    assert charges and max(charges)>0
    assert physical.bounds.max_resident_bytes == 64*2**20
    assert dict(physical.policy.native_limits) == budget
