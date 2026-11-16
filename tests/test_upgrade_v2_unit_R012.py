"""R012/R015 · actual public Dataset grouped/timed/prepend crash recovery.

The original ACK and native v3 operator images carry the remainder. No legacy
standalone StreamMaterialization wraps a second executor or fabricates sinks.
"""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from _runtime_v3_buffer_recovery import images_at, crash_with_remainder, assert_prefix_unchanged


async def identity(row):
    return row


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


def _recover(tmp_path, monkeypatch, build, *, kind, pending):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    holders = []
    with crash_with_remainder(monkeypatch, holders, kind=kind, pending=pending) as fired:
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                holders.append(run)
                ref = run.ref
                build().materialize().take_all()
    before = [f for f in RunJournal(tmp_path / 'run').execution_commits('action-1')
              if f['status'] == 'acked']
    with RunContext.resume(ref) as resumed:
        cut, snapshots = images_at(resumed, fired[0], kind)
        assert not cut['cut']['final'] and any(pending(item) for item in snapshots)
        result = build().materialize().take_all()
        after = resumed.journal.execution_commits('action-1')
    assert_prefix_unchanged(before, after)
    return result, snapshots


def test_grouped_crash_recovers_pending_groups_exactly_once(tmp_path, monkeypatch):
    items = [{'key': 'a' if i % 2 == 0 else 'b', 'id': i} for i in range(12)]
    def build():
        return data.from_items(items).group_batches('key', max_rows=4,
            flush_interval=10, chunk_bytes=4096, buffer_bytes=8192)
    groups, images = _recover(tmp_path, monkeypatch, build, kind='StreamGroupBatchesOp',
        pending=lambda image: bool(image['groups']))
    assert any(sum(map(len, image['groups'].values())) > 0 for image in images)
    rows = [row for group in groups for row in group['items']]
    assert sorted(row['id'] for row in rows) == list(range(12)) and len(rows) == 12
    for group in groups:
        assert all(row['key'] == group['key'] for row in group['items'])


def test_timed_batch_crash_recovers_pending_window_exactly_once(tmp_path, monkeypatch):
    items = [{'id': i} for i in range(12)]
    def build():
        return data.from_items(items).batch_map(batch_identity, max_batch=3, flush_interval=10)
    rows, images = _recover(tmp_path, monkeypatch, build, kind='BatchMapOp',
        pending=lambda image: bool(image['batch']))
    assert any(image['batch'] and 0 <= image['batch_remaining_ms'] <= 10000 for image in images)
    assert sorted(row['id'] for row in rows) == list(range(12)) and len(rows) == 12
    for row in rows:
        assert row['id'] in row['group'] and len(row['group']) <= 3


def test_prepend_crash_does_not_republish_emitted_prefix(tmp_path, monkeypatch):
    upstream = [{'id': i} for i in range(6)]
    prefix = [{'id': 'p0'}, {'id': 'p1'}]
    def build():
        return data.from_items(upstream).prepend(data.from_items(prefix), max_rows=2).map_async(identity)
    rows, images = _recover(tmp_path, monkeypatch, build, kind='StreamPrependOp',
        pending=lambda image: image['ports']['prefix']['eoi'] and image['output_cursor'] >= 2)
    assert any(image['ports']['prefix']['eoi'] for image in images)
    assert [row['id'] for row in rows] == ['p0', 'p1', 0, 1, 2, 3, 4, 5]


def test_empty_source_prepend_materializes_prefix_under_checkpoints(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}):
        rows = data.from_items([]).prepend(data.from_items([{'id': 'p0'}, {'id': 'p1'}]),
            max_rows=2).materialize().take_all()
    assert [row['id'] for row in rows] == ['p0', 'p1']
