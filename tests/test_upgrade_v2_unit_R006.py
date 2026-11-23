"""R006/R007 · real v3 batch remainder recovery on the original commit protocol.

The retired node-0-buffer layout is not fabricated. Read the real v3 operator
image attached to the same original ACK; preserve batch membership, all rows,
slot capacity and every original receipt across recovery.
"""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.seal.sealed_plans import load_sealed_plan
from _runtime_v3_buffer_recovery import images_at, crash_with_remainder, assert_prefix_unchanged


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


def _recover(tmp_path, monkeypatch, *, concurrency, count):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    items = [{'id': i} for i in range(count)]
    holders = []
    def execute():
        return data.from_items(items).batch_map(batch_identity,
            max_batch=3, concurrency=concurrency).materialize().take_all()
    with crash_with_remainder(monkeypatch, holders, kind='BatchMapOp',
            pending=lambda image: bool(image['batch'])) as fired:
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                holders.append(run)
                ref = run.ref
                execute()
    before = [f for f in RunJournal(tmp_path / 'run').execution_commits('action-1')
              if f['status'] == 'acked']
    with RunContext.resume(ref) as resumed:
        cut, snapshots = images_at(resumed, fired[0], 'BatchMapOp')
        assert not cut['cut']['final'] and any(snapshot['batch'] for snapshot in snapshots)
        sealed = load_sealed_plan(resumed.directory, 'action-1', version='v3')
        specs = [node.kernel for node in sealed['graph'].nodes if node.kernel.kind == 'BatchMapOp']
        assert len(specs) == 1 and specs[0].configuration['concurrency'] == concurrency
        for snapshot in snapshots:
            assert len(snapshot['batch']) < 3
            if snapshot['call_slots'] is not None:
                occupied = [job['slot'] for job in snapshot['inflight_calls']]
                assert sorted(snapshot['call_slots'] + occupied) == list(range(concurrency))
        result = execute()
        facts = resumed.journal.execution_commits('action-1')
    assert_prefix_unchanged(before, facts)
    return result


def test_concurrent_batch_c2_crash_recovers_remainder_exactly_once(tmp_path, monkeypatch):
    result = _recover(tmp_path, monkeypatch, concurrency=2, count=12)
    assert sorted(row['id'] for row in result) == list(range(12)) and len(result) == 12
    for row in result:
        assert row['id'] in row['group'] and len(row['group']) <= 3


def test_fixed_batch_c1_crash_preserves_exact_batch_membership(tmp_path, monkeypatch):
    expected = [{'id': i, 'group': list(range(i // 3 * 3, min(11, i // 3 * 3 + 3)))} for i in range(11)]
    assert _recover(tmp_path, monkeypatch, concurrency=1, count=11) == expected
