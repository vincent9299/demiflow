"""R012/R015 · worker buffer recovery for grouped / timed / prepend kinds.

Each kind keeps a real input remainder across an acked cut, survives a crash,
and resumes exactly once on the driver's single commit protocol. Group keys,
batch lineage and the prepend prefix position are the durable contract; the
flush timer is advisory and restarts on restore.

Timed batch reaches the public ``materialize()`` entry (BatchMapOp is admitted).
Grouped/prepend are driven through the native stream progress directly: their
public ``materialize()`` admission lives in plan_facts.operation_facts /
run_context.admit_stream (other groups), so this file exercises the R-group
buffer/worker code without depending on that cross-group gate.
"""
import contextlib

import pytest

from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.execution import commit as protocol
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.source_snapshot import freeze_source
from demiflow.execution.state_artifacts import StateArtifacts
from demiflow.execution.stream import run_stream
from demiflow.execution.stream_materialize import StreamMaterialization


async def identity(row):
    return row


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


@contextlib.contextmanager
def _crash_first_output(monkeypatch, tmp_path):
    def stop(name, commit_id):
        fact = RunJournal(tmp_path / 'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['intent']['sinks'] == ['output']:
            raise RuntimeError('crash after publication')
    with monkeypatch.context() as m:
        m.setattr(protocol, 'fault_point', stop)
        yield


def _first_acked(tmp_path):
    return [f for f in RunJournal(tmp_path / 'run').execution_commits('action-1')
            if f['status'] == 'acked'][0]


def _drive(ds, action, run):
    """Drive the native aligned-barrier progress for a plan, bypassing the
    cross-group public materialize() op-admission gate. Returns committed rows.

    Each cut detaches a delta, so the full result is the union of every acked
    commit's output blocks (the same contract _CheckpointBlocks exposes)."""
    executor = ds._executor
    selected = freeze_source(action, ds._source)
    terminal = StreamMaterialization(action, lambda: executor.iter_rows(selected, LogicalPlan()))
    if not terminal.complete:
        run_stream(terminal.source_rows(), ds._plan, consume=terminal.append, progress=terminal)
        terminal.finish()
    reader = BlockSink(action)
    rows = []
    for fact in run.journal.iter_execution_commits(action.action_id, through_sequence=terminal.sequence):
        if fact['status'] == 'pending':
            continue
        rows.extend(reader.iter_rows(fact['receipts']['output']['data']))
    return rows


def test_grouped_crash_recovers_pending_groups_exactly_once(tmp_path, monkeypatch):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    items = [{'key': 'a' if i % 2 == 0 else 'b', 'id': i} for i in range(12)]

    def build():
        return data.from_items(items).group_batches(
            'key', max_rows=4, flush_interval=10, chunk_bytes=4096, buffer_bytes=8192)
    ref = None
    with _crash_first_output(monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                ref = run.ref
                with run.action({'kind': 'materialize', 'source': {'fixture': 'grouped'}}) as action:
                    _drive(build(), action, run)
    fact = _first_acked(tmp_path)
    with RunContext.resume(ref) as run2:
        snapshot = StateArtifacts(run2).read(fact['prepared']['state']['node-0-buffer'])
        assert snapshot['kind'] == 'grouped'
        assert sum(g['data']['rows'] for g in snapshot['slots']['0']['groups']) > 0
        with run2.action({'kind': 'materialize', 'source': {'fixture': 'grouped'}}) as action:
            groups = _drive(build(), action, run2)
    rows = [r for group in groups for r in group['items']]
    assert sorted(r['id'] for r in rows) == list(range(12))
    assert len(rows) == 12
    for group in groups:
        assert all(r['key'] == group['key'] for r in group['items'])


def test_timed_batch_crash_recovers_pending_window_exactly_once(tmp_path, monkeypatch):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    items = [{'id': i} for i in range(12)]

    def build():
        return data.from_items(items).batch_map(batch_identity, max_batch=3, flush_interval=10).materialize()
    ref = None
    with _crash_first_output(monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                ref = run.ref
                build().take_all()
    fact = _first_acked(tmp_path)
    with RunContext.resume(ref) as run2:
        snapshot = StateArtifacts(run2).read(fact['prepared']['state']['node-0-buffer'])
        assert snapshot['kind'] == 'timed_batch'
        assert snapshot['slots']['0']['data']['rows'] > 0
        result = build().take_all()
    assert sorted(r['id'] for r in result) == list(range(12))
    assert len(result) == 12
    for row in result:
        assert row['id'] in row['group'] and len(row['group']) <= 3


def test_prepend_crash_does_not_republish_emitted_prefix(tmp_path, monkeypatch):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    upstream = [{'id': i} for i in range(6)]
    prefix = [{'id': 'p0'}, {'id': 'p1'}]

    def build():
        return (data.from_items(upstream).prepend(data.from_items(prefix), max_rows=2)
                .map_async(identity))
    ref = None
    with _crash_first_output(monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match='crash after publication'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run:
                ref = run.ref
                with run.action({'kind': 'materialize', 'source': {'fixture': 'prepend'}}) as action:
                    _drive(build(), action, run)
    fact = _first_acked(tmp_path)
    with RunContext.resume(ref) as run2:
        snapshot = StateArtifacts(run2).read(fact['prepared']['state']['node-0-buffer'])
        assert snapshot['kind'] == 'prepend'
        assert snapshot['prefix']['exhausted'] is True
        with run2.action({'kind': 'materialize', 'source': {'fixture': 'prepend'}}) as action:
            result = _drive(build(), action, run2)
    assert [r['id'] for r in result] == ['p0', 'p1', 0, 1, 2, 3, 4, 5]


def test_empty_source_prepend_materializes_prefix_under_checkpoints(tmp_path):
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    with RunContext.start(tmp_path / 'run', limits=limits) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'empty-prepend'}}) as action:
            ds = data.from_items([]).prepend(data.from_items([{'id': 'p0'}, {'id': 'p1'}]), max_rows=2)
            result = _drive(ds, action, run)
    assert [r['id'] for r in result] == ['p0', 'p1']