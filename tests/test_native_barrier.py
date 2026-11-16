"""Native aligned barriers: deterministic overlap and immutable local cuts."""
import asyncio
import json
import threading
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.state_artifacts import StateArtifacts

_OVERLAP = None


class NestedCounter:
    def __init__(self):
        self.state = {'seen': []}

    async def __call__(self, row):
        self.state['seen'].append(row['id'])
        if row['id'] == 8 and _OVERLAP is not None:
            _OVERLAP.set()
        await asyncio.sleep(0)
        return row

    def snapshot_state(self):
        return self.state

    def restore_state(self, value):
        self.state = value


def test_upstream_continues_before_commit_and_snapshot_is_detached(tmp_path, monkeypatch):
    global _OVERLAP
    _OVERLAP = threading.Event()
    original = CommitCoordinator._finish
    recorded = []

    def slow_commit(coordinator, fact, sinks, acknowledge):
        if fact['intent']['sinks'] == ['output'] and fact['sequence'] == 1:
            assert _OVERLAP.wait(5), 'upstream remained blocked on global publication'
            snapshot = {k: StateArtifacts(coordinator.run).read(v)
                        for k, v in fact['prepared']['state'].items()}
            assert snapshot['node-0'] == {'seen': [0, 1, 2, 3]}
            rows = list(sinks['output'].iter_rows(fact['prepared']['sinks']['output']))
            assert rows == [{'id': i} for i in range(4)]
            recorded.append(snapshot)
        return original(coordinator, fact, sinks, acknowledge)

    monkeypatch.setattr(CommitCoordinator, '_finish', slow_commit)
    try:
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000}):
            result = data.from_items([{'id': i} for i in range(12)]).map_async(NestedCounter()).materialize().take_all()
        assert result == [{'id': i} for i in range(12)]
        assert len(recorded) == 1
    finally:
        _OVERLAP = None


async def identity(row):
    await asyncio.sleep(0)
    return row


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


def test_fixed_batch_spans_barriers_without_checkpoint_flush(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}):
        result = data.from_items([{'id': i} for i in range(11)]).batch_map(
            batch_identity, max_batch=3, concurrency=1).materialize().take_all()
    assert result == [{'id': i, 'group': list(range(i//3*3, min(11, i//3*3+3)))} for i in range(11)]
    facts = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert 2 <= len(facts) <= 6
    assert facts[0]['receipts']['output']['data']['rows'] == 0
    assert facts[0]['intent']['position']['next_ordinal'] == 2


class SharedCounter:
    def __init__(self):
        self.count = 0

    async def __call__(self, row):
        self.count += 1
        await asyncio.sleep(0)
        return row

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, value):
        self.count = value['count']


def test_shared_actor_uses_one_snapshot_at_last_owner(tmp_path):
    actor = SharedCounter()
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000}) as run:
        result = data.from_items([{'id': i} for i in range(12)]).map_async(actor, concurrency=3).map_async(
            identity, concurrency=2).map_async(actor, concurrency=2).materialize().take_all()
        facts = run.journal.execution_commits('action-1')
        assert [StateArtifacts(run).read(f['prepared']['state']['node-0'])['count'] for f in facts] == [
            2 * f['intent']['position']['next_ordinal'] for f in facts]
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]


def test_one_input_1201_objects_crosses_bounded_manifest_pages(tmp_path):
    from demiflow.execution.object_sink import ObjectRowsSink
    from demiflow.execution.recovery_contract import SourcePosition
    from demiflow.execution.artifact_manifest import manifest_pages, entries
    from demiflow.objects import ObjectRef
    with RunContext.start(tmp_path/'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'one-to-1201'}}) as action:
            sink = ObjectRowsSink(action, max_object_bytes=128, max_objects=64)
            def expanded():
                for i in range(1201):
                    yield {'id': i, 'image': sink.objects.stage(str(i).encode())}
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, expanded())},
                position=SourcePosition('single-source-occurrence', 1))
            descriptor = fact['prepared']['sinks']['output']['objects']['objects']
            pages = list(manifest_pages(run.directory, descriptor))
            assert len(pages) == 19
            assert max(len(page) for _, page in pages) == 64
            assert sum(len(page) for _, page in pages) == 1201
            assert len(json.dumps(fact['prepared'])) < 4096
            result = list(sink.rows.iter_rows(fact['prepared']['sinks']['output']['rows']))
            action.complete({'rows': len(result)})
    assert len(result) == 1201
    assert ObjectRef(**result[-1]['image']).read(max_bytes=128) == b'1200'
    assert len(list(entries(run, descriptor))) == 1201  # committed pages survive cleanup


def test_block_pages_preserve_output_order_and_recovery(tmp_path):
    from demiflow.execution.block_sink import BlockSink
    from demiflow.execution.recovery_contract import SourcePosition
    with RunContext.start(tmp_path/'run', limits={'max_row_bytes': 1024, 'max_batch_bytes': 1024}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'blocks'}}) as action:
            sink = BlockSink(action)
            sink.block_bytes = 64
            rows = [{'id': i, 'payload': bytes(64)} for i in range(1201)]
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, iter(rows))},
                position=SourcePosition('blocks-fixture', 1201))
            prepared = fact['prepared']['sinks']['output']
            assert type(prepared['blocks']) is dict
            assert list(sink.iter_rows(prepared)) == rows
            action.complete({'rows': len(rows)})
    assert list(sink.iter_rows(prepared)) == rows


def test_private_temporary_registration_is_released_but_references_are_kept(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError, SourcePosition
    from demiflow.execution.block_sink import BlockSink
    with RunContext.start(tmp_path/'run') as run:
        path = run.directory/'scratch'/'private'
        path.parent.mkdir()
        with run.temporary_artifacts() as allocate:
            allocate(path)
            path.write_bytes(b'x')
            path.unlink()
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0] == 0
        with run.action({'kind': 'materialize', 'source': {'fixture': 'reference'}}) as action:
            sink = BlockSink(action)
            ref = run.directory/'scratch'/'protected'
            run.allocate_artifact(ref)
            ref.write_bytes(b'x')
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, iter([]))},
                position=SourcePosition('source', 0), states={'owner': {'path': 'scratch/protected'}})
            ref.unlink()
            with pytest.raises(RecoveryContractError, match='still references'):
                run.release_temporaries([ref])
            action.complete({'rows': 0})


def test_snapshot_failure_stops_without_confirming_output(tmp_path):
    class Broken(NestedCounter):
        def snapshot_state(self):
            if self.state['seen']:
                raise RuntimeError('snapshot failed')
            return self.state
    with pytest.raises(RuntimeError, match='snapshot failed'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}):
            data.from_items([{'id': i} for i in range(6)]).map_async(Broken()).materialize()
    assert RunJournal(tmp_path/'run').execution_commits('action-1') == []


def test_direct_lance_prepares_before_source_finishes_and_publishes_once(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.lance_sink import LanceSink
    counter = NestedCounter()
    original = LanceSink.prepare
    observed = []
    def prepare(sink, blocks, context):
        blocks = iter(blocks)
        first = next(blocks)
        observed.append(len(counter.state['seen']))
        assert observed[-1] < 1024
        def batches():
            yield first
            yield from blocks
        return original(sink, batches(), context)
    monkeypatch.setattr(LanceSink, 'prepare', prepare)
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 64, 'checkpoint_interval_ms': 60000}):
        data.from_items([{'id': i} for i in range(1024)]).map_async(counter).write_lance(
            str(tmp_path/'out.lance'), mode='overwrite')
    assert observed
    dataset = lance.dataset(str(tmp_path/'out.lance'))
    assert dataset.version == 1 and dataset.count_rows() == 1024
    facts = RunJournal(tmp_path/'run').execution_commits('action-1-stream')
    assert 2 <= len(facts) <= 16 and all(f['status'] == 'acked' for f in facts)


def test_temporary_release_rejects_live_files_and_stale_fence(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.run_journal import ExecutionConflict
    with RunContext.start(tmp_path/'run') as run:
        path = run.directory/'scratch'/'live'
        path.parent.mkdir()
        run.allocate_artifact(path)
        path.write_bytes(b'live')
        with pytest.raises(RecoveryContractError, match='not removed'):
            run.release_temporaries([path])
        with pytest.raises(ExecutionConflict, match='Stale'):
            run.journal.release_temporaries('obsolete-owner', ['scratch/live'])
        assert path.read_bytes() == b'live'
        path.unlink()
        run.release_temporaries([path])


def test_enospc_does_not_confirm_checkpoint_or_return_partial_result(tmp_path, monkeypatch):
    import errno
    import demiflow.execution.block_sink as blocks
    original = blocks.atomic_bytes
    def fail(path, raw):
        if '/action-1/output/' in str(path):
            raise OSError(errno.ENOSPC, 'controlled full disk')
        return original(path, raw)
    monkeypatch.setattr(blocks, 'atomic_bytes', fail)
    with pytest.raises(OSError, match='full disk'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}):
            data.from_items([{'id': i} for i in range(8)]).map_async(identity).materialize()
    facts = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert len(facts) == 1 and facts[0]['status'] == 'pending'
    assert not list((tmp_path/'run'/'materializations'/'action-1'/'output').rglob('*.pkl'))


def test_checkpoint_deadline_fails_and_drains_publication_thread(tmp_path, monkeypatch):
    import time
    from demiflow.execution.dataflow_sinks import _DataflowArtifactSink
    original = _DataflowArtifactSink.commit
    completed = []
    def slow(sink, prepared, context):
        if sink._sink_id == 'materialize':
            time.sleep(1.2)
        receipt = original(sink, prepared, context)
        completed.append(context.commit_id)
        return receipt
    monkeypatch.setattr(_DataflowArtifactSink, 'commit', slow)
    with pytest.raises(TimeoutError):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_timeout_s': 1}):
            data.from_items([{'id': i} for i in range(8)]).map_async(identity).materialize()
    # A started native write is drained before ownership/cleanup is released.
    journal = RunJournal(tmp_path/'run')
    assert completed, 'the actual native publication must have drained'
    assert all(journal.execution_commit(key)['status'] == 'acked' for key in completed)
    assert journal.execution_action('action-1')['result'] is None
    assert journal.execution_status()['status'] == 'failed'
    assert not [t for t in threading.enumerate() if t.name.startswith('demiflow-terminal-input')]


def test_publication_fence_does_not_hold_control_db_during_io(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    with RunContext.start(tmp_path/'run') as run:
        configuration = run.journal.recovery_identity()
        with ThreadPoolExecutor(max_workers=2) as pool:
            entered = threading.Event()
            def replacement():
                entered.set()
                run.journal.claim_execution(configuration, 'replacement')
            with run.journal.publication_guard(run.fence):
                changed = pool.submit(replacement)
                assert entered.wait(1)
                # A budget writer can proceed while a publisher owns its fence.
                assert pool.submit(run.reserve, 'probe', 1).result(timeout=1) == 1
                assert not changed.done()
            changed.result(timeout=2)
        from demiflow.execution.run_journal import ExecutionConflict
        with pytest.raises(ExecutionConflict, match='Stale'):
            with run.journal.publication_guard(run.fence):
                pytest.fail('old publisher admitted')
        run.journal.claim_execution(configuration, run.fence)


def test_unified_checkpoint_deadline_includes_alignment_before_publication(tmp_path, monkeypatch):
    published = []
    original = CommitCoordinator._finish
    def finish(coordinator, *args, **kwargs):
        published.append(True)
        return original(coordinator, *args, **kwargs)
    monkeypatch.setattr(CommitCoordinator, '_finish', finish)
    async def slow(row):
        await asyncio.sleep(1.6)
        return row
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 1,
                'checkpoint_timeout_s': 1, 'checkpoint_interval_ms': 60000}), data.local_execution(workers=2):
            data.from_items([{'id': i} for i in range(3)]).map_async(slow, concurrency=1).materialize()
    assert not published, 'timeout must include waiting for the first aligned cut'
    assert RunJournal(tmp_path/'run').execution_action('action-1')['result'] is None


class NativeSlowFixture:
    """Native-capability fixture: no shared semantic state or resource handles."""
    def __init__(self):
        self.release = None

    def native_stream_recovery(self, action, node_id):
        return self

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        assert state == {}

    async def __call__(self, row):
        if self.release is None:
            self.release = asyncio.Event()
        if row['id'] == 0:
            await asyncio.wait_for(self.release.wait(), 5)
        if row['id'] == 7:
            self.release.set()
        return row


def test_native_inflight_cut_allows_free_slots_past_slow_call(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}) as run:
        result = data.from_items([{'id': i} for i in range(12)]).map_async(
            NativeSlowFixture(), concurrency=3).materialize().take_all()
        fact = run.journal.execution_commits('action-1')[0]
        saved = StateArtifacts(run).read(fact['prepared']['state']['node-0-inflight'])
        from demiflow.execution.block_sink import BlockSink
        from demiflow.execution.run_context import RunAction
        action = RunAction(run, 'action-1', run.journal.execution_action('action-1')['declaration'])
        pending = list(BlockSink(action, 'node-0-inflight').iter_rows(saved['data']))
        assert any(item['id'] == 0 for item in pending)
        assert fact['receipts']['output']['data']['rows'] < 2
    assert sorted(row['id'] for row in result) == list(range(12))


def test_checkpoint_deadline_begins_before_blocked_insertion():
    from types import SimpleNamespace
    from demiflow.execution.stream_barrier import CheckpointControl
    async def scenario():
        stage = SimpleNamespace(concurrency=1, native_recovery=None)
        queue = asyncio.Queue(maxsize=1)
        await queue.put({'id': 0})
        progress = SimpleNamespace(inputs=0, ordinal=1, batch_rows=1,
            run=SimpleNamespace(limits={'checkpoint_timeout_s': 1, 'checkpoint_interval_ms': 60000,
                'checkpoint_unconfirmed_rows': 2, 'checkpoint_unconfirmed_bytes': 100}))
        control = CheckpointControl(progress, [stage], [queue], lambda task: None)
        control.accepted(1, 10)
        task = asyncio.create_task(control.run())
        try:
            await asyncio.sleep(1.1)
            assert control.pending.status == 'failed'
            with pytest.raises(TimeoutError, match='Checkpoint coordination'):
                await control.pending.wait()
            assert control.pending.local[0].is_set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_unconfirmed_byte_quota_requests_cut_before_row_trigger():
    from types import SimpleNamespace
    from demiflow.execution.stream_barrier import CheckpointControl
    async def scenario():
        stage = SimpleNamespace(concurrency=1, native_recovery=None)
        queue = asyncio.Queue(maxsize=2)
        progress = SimpleNamespace(inputs=0, ordinal=1, batch_rows=512,
            run=SimpleNamespace(limits={'checkpoint_timeout_s': 2, 'checkpoint_interval_ms': 60000,
                'checkpoint_unconfirmed_rows': 10, 'checkpoint_unconfirmed_bytes': 100}),
            group_ends=[0])
        async def align(*args):
            pass
        def seal(barrier):
            async def publish():
                progress.inputs = barrier.next_ordinal
            return publish
        progress.align, progress.seal = align, seal
        control = CheckpointControl(progress, [stage], [queue], lambda task: None)
        control.accepted(1, 80)
        task = asyncio.create_task(control.run())
        waiter = asyncio.create_task(control.admit(30))
        try:
            marker = await asyncio.wait_for(queue.get(), 1)
            assert not waiter.done()
            await marker.arrive(stage)
            await asyncio.wait_for(waiter, 1)
            assert control.bytes == 0
        finally:
            task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)
    asyncio.run(scenario())


class RecoveryEmitter:
    def __init__(self):
        self.pending = [{'id': 101}, {'id': 102}]

    def snapshot_state(self):
        return {'pending': self.pending}

    def restore_state(self, state):
        self.pending = state['pending']

    def recovery_rows(self):
        rows, self.pending = self.pending, []
        return rows

    async def __call__(self, row):
        return row


def test_recovery_rows_are_in_final_checkpoint_and_not_republished(tmp_path, monkeypatch):
    import demiflow.execution.commit as protocol
    def stop(name, commit_id):
        fact = RunJournal(tmp_path/'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['sequence'] == 2:
            raise RuntimeError('stop after recovered outputs')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='stop after recovered outputs'):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            data.from_items([]).map_async(RecoveryEmitter()).materialize()
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference):
        rows = data.from_items([]).map_async(RecoveryEmitter()).materialize().take_all()
    assert rows == [{'id': 101}, {'id': 102}]


class WideState:
    def __init__(self, size=600):
        self.payload = 'x' * size

    def snapshot_state(self):
        return {'payload': self.payload}

    def restore_state(self, state):
        self.payload = state['payload']

    async def __call__(self, row):
        return row


def test_aggregate_state_admission_applies_across_alignment_groups(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    # The terminal's own bounded metadata needs a legal minimum. Each actor's
    # image fits; the pair exceeds the original whole-cut state allowance.
    limits = {'max_state_bytes': 64 * 1024, 'checkpoint_rows': 1}
    with RunContext.start(tmp_path/'single', limits=limits), data.local_execution(workers=4):
        assert data.from_items([{'id': 1}]).map_async(WideState(40000)).materialize().take_all() == [{'id': 1}]
    with pytest.raises(DataflowError, match='Aggregate checkpoint state'):
        with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(workers=4):
            data.from_items([{'id': 1}]).map_async(WideState(40000)).map_async(WideState(40000)).materialize()
    assert not RunJournal(tmp_path/'run').execution_commits('action-1')


def test_checksum_proof_detects_replacement_and_same_size_corruption(tmp_path):
    import hashlib
    from demiflow.execution.artifact_proof import ArtifactProofs
    from demiflow.execution.recovery_contract import RecoveryContractError
    path = tmp_path/'blob'
    path.write_bytes(b'valid bytes')
    proof = ArtifactProofs(capacity=2)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    proof.verify(path, 11, sha)
    import time
    time.sleep(2.05)
    proof.verify(path, 11, sha)
    assert proof.hits == 1 and proof.hashed_bytes == 11
    replacement = tmp_path/'replacement'
    replacement.write_bytes(b'valid bytes')
    replacement.replace(path)
    proof.verify(path, 11, sha)
    assert proof.hashed_bytes == 22
    path.write_bytes(b'wrong bytes')
    with pytest.raises(RecoveryContractError, match='checksum differs'):
        proof.verify(path, 11, sha)


def test_late_writer_keeps_run_lease_and_handles_until_it_exits(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError
    release, entered, saw_stop = threading.Event(), threading.Event(), threading.Event()
    run = RunContext.start(tmp_path/'run')
    def writer():
        entered.set()
        assert release.wait(5)
        try:
            run.check_stop()
        except RecoveryContractError:
            saw_stop.set()
    worker = threading.Thread(target=writer, name='native-drain-fixture')
    try:
        with pytest.raises(RecoveryContractError, match='isolated while draining'):
            with run:
                reference = run.ref
                worker.start()
                assert entered.wait(1)
                run.quarantine_worker(worker)
        assert run.active and run._lease is not None
        with pytest.raises(BlockingIOError):
            with RunContext.resume(reference):
                pass
    finally:
        release.set()
        worker.join(5)
        run._drain_finalizer.join(5)
    assert saw_stop.is_set() and not run.active and run._lease is None
    assert not worker.is_alive() and not run._drain_finalizer.is_alive()
    with RunContext.resume(reference):
        pass


def test_temporary_hot_path_does_not_scan_tenfold_history(tmp_path, monkeypatch):
    from demiflow.execution.block_sink import storage_usage
    results = []
    for history in (1000, 10000):
        with RunContext.start(tmp_path/str(history)) as run:
            with run.journal.execution_guard(run.fence) as db:
                db.executemany('INSERT INTO runtime_artifacts(path,protected) VALUES (?,1)',
                    [('state/fixture-' + str(i),) for i in range(history)])
            instructions = [0]
            def tick():
                instructions[0] += 100
                return 0
            with run.journal._connection(write=True) as db:
                db.set_progress_handler(tick, 100)
            with run.journal._connection() as db:
                db.set_progress_handler(tick, 100)
            with monkeypatch.context() as patch:
                def no_scan(*args, **kwargs):
                    raise AssertionError('hot path scanned a directory')
                patch.setattr(Path, 'iterdir', no_scan)
                for i in range(64):
                    path = run.directory/'scratch'/str(i)
                    run.allocate_artifact(path, size=32)
                    assert storage_usage(run) == 64
                    run.release_temporaries([path])
                    assert storage_usage(run) == 0
            with run.journal._connection(write=True) as db:
                db.set_progress_handler(None, 0)
            with run.journal._connection() as db:
                db.set_progress_handler(None, 0)
                assert db.execute('SELECT items FROM runtime_storage').fetchone()[0] == history
            results.append(instructions[0])
    assert results[1] < results[0] * 1.5, results


def test_cancelled_local_write_drains_under_repeated_cancellation(tmp_path):
    from demiflow.execution.run_context import current_run
    from demiflow.execution.stream_materialize import StreamMaterialization
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    with RunContext.start(tmp_path/'run') as run:
        def write(*, expected):
            assert current_run() is expected
            entered.set()
            assert release.wait(5)
            finished.set()

        async def scenario():
            task = asyncio.create_task(StreamMaterialization._io(write, expected=run))
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done() and not finished.is_set()
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
        asyncio.run(scenario())
