"""R006/R007 · StreamStageProgress buffer wiring + crash recovery (C05/C13).

End-to-end aligned-barrier counterexamples on the real run commit protocol:
concurrent batch (c=2) and fixed batch survive a crash after an acked cut with
a non-empty input remainder, restore exactly once, and reuse the acked prefix
unchanged. The buffer is never the commit authority; the driver's journal is.
"""
import contextlib

import pytest

from demiflow import data
from demiflow.execution import commit as protocol
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.state_artifacts import StateArtifacts


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


@contextlib.contextmanager
def _crash_first_output(monkeypatch, tmp_path):
    def stop(name, commit_id):
        fact = RunJournal(tmp_path / 'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['intent']['sinks'] == ['output']:
            raise RuntimeError('crash after publication')
    # Scope the fault to the crash phase: resume replays the acked cut through
    # _finish, so the patch must be reverted before RunContext.resume runs.
    with monkeypatch.context() as m:
        m.setattr(protocol, 'fault_point', stop)
        yield


def _buffer_remainder(run, fact):
    ref = fact['prepared']['state'].get('node-0-buffer')
    if ref is None:
        return 0
    snapshot = StateArtifacts(run).read(ref)
    total = 0
    for slot in snapshot['slots'].values():
        if 'data' in slot:
            total += slot['data']['rows']
        for group in slot.get('groups', []):
            total += group['data']['rows']
    return total


def test_concurrent_batch_c2_crash_recovers_remainder_exactly_once(tmp_path, monkeypatch):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    items = [{'id': i} for i in range(12)]
    ref = None
    with _crash_first_output(monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                ref = run.ref
                data.from_items(items).batch_map(batch_identity, max_batch=3, concurrency=2).materialize().take_all()
    pre = [f for f in RunJournal(tmp_path / 'run').execution_commits('action-1') if f['status'] == 'acked']
    assert pre, 'no acked prefix survived the crash'
    with RunContext.resume(ref) as run2:
        # The crashed cut froze a real per-slot remainder (2 admitted rows, batch=3).
        assert _buffer_remainder(run2, pre[0]) > 0
        snapshot = StateArtifacts(run2).read(pre[0]['prepared']['state']['node-0-buffer'])
        assert snapshot['kind'] == 'concurrent_batch' and snapshot['participants'] == 2
        assert set(snapshot['slots']) == {'0', '1'}
        result = data.from_items(items).batch_map(batch_identity, max_batch=3, concurrency=2).materialize().take_all()
        facts = run2.journal.execution_commits('action-1')
    assert sorted(r['id'] for r in result) == list(range(12))
    assert len(result) == 12
    for row in result:
        assert row['id'] in row['group'] and len(row['group']) <= 3
    # Acked prefix is reused unchanged; sequences stay contiguous and unique.
    pre_by_id = {f['commit_id']: f for f in pre}
    post_by_id = {f['commit_id']: f for f in facts if f['status'] == 'acked'}
    assert pre_by_id.keys() <= post_by_id.keys()
    for commit_id, before in pre_by_id.items():
        assert post_by_id[commit_id]['receipts'] == before['receipts']
    assert [f['sequence'] for f in facts] == list(range(1, len(facts) + 1))


def test_fixed_batch_c1_crash_recovers_and_stays_legacy_compatible(tmp_path, monkeypatch):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    items = [{'id': i} for i in range(11)]
    expected = [{'id': i, 'group': list(range(i // 3 * 3, min(11, i // 3 * 3 + 3)))} for i in range(11)]
    ref = None
    with _crash_first_output(monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                ref = run.ref
                data.from_items(items).batch_map(batch_identity, max_batch=3, concurrency=1).materialize().take_all()
    with RunContext.resume(ref) as run2:
        result = data.from_items(items).batch_map(batch_identity, max_batch=3, concurrency=1).materialize().take_all()
        fact = run2.journal.execution_commits('action-1')[0]
        snapshot = StateArtifacts(run2).read(fact['prepared']['state']['node-0-buffer'])
    assert result == expected
    # Single-worker fixed batch keeps the historical slot-0 lineage and identity.
    assert snapshot['kind'] == 'fixed_batch' and snapshot['participants'] == 1
    assert set(snapshot['slots']) == {'0'}