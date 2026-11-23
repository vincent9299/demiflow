"""Public Dataset proofs: genuine managed workers and native commit journal."""
import math
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _rows():
    return [{'n': i} for i in range(6)]


def test_public_map_take_commits_actual_records(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        actual = data.from_items(_rows()).map(lambda r: {'n': r['n'] + 10}).take(3)
        assert actual == [{'n': 10}, {'n': 11}, {'n': 12}]
        commits = run.journal.execution_commits('action-1')
        assert len(commits) == 1
        assert commits[0]['status'] == 'acked'
        assert commits[0]['receipts']['take']['data']['rows'] == 3
        assert commits[0]['prepared']['state']['dataflow-cut-v3']


def test_public_filter_count(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        assert data.from_items(_rows()).filter(lambda r: r['n'] % 2 == 0).count() == 3
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_materialized_count_uses_authenticated_rows_and_records_action(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        stored = data.from_items(_rows()).map(lambda row: row).materialize()
        assert stored.count() == 6
        assert stored.execution_metadata().diagnostics['observed_worker_count'] == 0
        receipt = run.journal.execution_action('action-2')['result']
        assert receipt == {'kind': 'count', 'scalar': 6, 'source': stored._source.handle.recovery_reference}
        assert stored.filter(lambda row: row['n'] > 3).count() == 2
        assert stored.execution_metadata().diagnostics['observed_worker_count'] > 0
        stored.release()
        with pytest.raises(RuntimeError, match='release'):
            stored.count()


@pytest.mark.parametrize('damage', ['missing', 'same-size', 'manifest'])
def test_materialized_count_rejects_damaged_input(tmp_path, damage):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_segments import segment_refs
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        stored = data.from_items(_rows()).map(lambda row: row).materialize()
        handle = stored._source.handle
        ref = handle._manifest_ref if damage == 'manifest' else next(
            ref for block in handle._blocks for ref in segment_refs(handle._store, block, handle._bounds)
            if ref.size_bytes)
        path = handle._store._object_path(ref)
        original = path.read_bytes()
        try:
            if damage == 'missing':
                path.unlink()
            else:
                with path.open('r+b') as payload:
                    payload.write(bytes([original[0] ^ 1]))
            with pytest.raises(DataflowError):
                stored.count()
        finally:
            path.write_bytes(original)
            stored.release()


def test_public_union_take_all(tmp_path):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        left = data.from_items([{'n': 1}, {'n': 2}])
        right = data.from_items([{'n': 3}])
        assert sorted(left.union(right).take_all(), key=lambda r: r['n']) == [{'n': 1}, {'n': 2}, {'n': 3}]


def test_public_materialize_fidelity_readback(tmp_path):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        value = {'negative': [-0.0], 'positive': [0.0], 'tuple': (b'data', [None])}
        materialized = data.from_items([value]).map(lambda r: r).materialize()
        try:
            actual = materialized.take_all()
            assert actual == [value]
            assert type(actual[0]['tuple']) is tuple
            assert type(actual[0]['tuple'][1]) is list
            assert math.copysign(1, actual[0]['negative'][0]) == -1
            assert math.copysign(1, actual[0]['positive'][0]) == 1
        finally:
            materialized.release()


def _resume_dataset():
    return data.from_items(_rows()).map(lambda r: {'n': r['n'] + 10})


def test_replay_committed_result_reuses_seal_and_commit(tmp_path):
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            assert _resume_dataset().take(3) == [{'n': 10}, {'n': 11}, {'n': 12}]
            seal = run.journal.source_binding('action-1-dataflow-sealed')
            commits = run.journal.execution_commits('action-1')
            raise Interrupted()
    with RunContext.resume(ref) as resumed, data.local_execution():
        assert _resume_dataset().take(3) == [{'n': 10}, {'n': 11}, {'n': 12}]
        assert resumed.attempt == 2
        assert resumed.journal.source_binding('action-1-dataflow-sealed') == seal
        assert resumed.journal.execution_commits('action-1') == commits


def test_partial_cut_restores_input_kernel_and_terminal_prefix(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    call_log = tmp_path / 'calls'
    def record(row):
        import time
        with open(call_log, 'a') as stream:
            stream.write(str(row['n']) + '\n')
        time.sleep(.002)
        return row
    def dataset():
        return data.from_items([{'n': i} for i in range(150)]).map(record)
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    seen = {'polls': 0}
    def interrupt_after_cut(ex, **kwargs):
        result = pump(ex, **kwargs)
        seen['polls'] += 1
        if seen['polls'] == 15:
            ex.request_checkpoint()
        if ex._restore_image is not None and not ex._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_interval_ms': 60000}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt_after_cut)
                dataset().count()
    with RunContext.resume(ref) as run, data.local_execution():
        assert dataset().count() == 150
        assert run.attempt == 2
        commits = run.journal.execution_commits('action-1')
        assert len(commits) == 2 and all(c['status'] == 'acked' for c in commits)
    assert sorted(map(int, call_log.read_text().splitlines())) == list(range(150))


def test_public_lance_write_recovery_after_prepared(tmp_path, monkeypatch):
    import lance
    from demiflow.execution import commit
    class Interrupted(Exception):
        pass
    def fault(name, commit_id):
        if name == 'after_prepared':
            raise Interrupted()
    target = tmp_path / 'output.lance'
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(commit, 'fault_point', fault)
                _resume_dataset().write_lance(str(target), mode='create', return_receipt=True)
            raise AssertionError('fault was not reached')
    assert not target.exists()
    with RunContext.resume(ref) as run, data.local_execution():
        receipt = _resume_dataset().write_lance(str(target), mode='create', return_receipt=True)
        assert receipt.status == 'committed' and receipt.written_rows == 6
        assert receipt.committed_version == 1
        assert len(run.journal.execution_commits('action-1')) == 1
    assert lance.dataset(str(target)).to_table().to_pylist() == [{'n': i + 10} for i in range(6)]


def test_public_global_sort_complete_scope(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        rows = data.from_items([{'n': i} for i in (5, 1, 4, 2, 3)]).sort('n').take_all()
        assert rows == [{'n': i} for i in range(1, 6)]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_public_join_different_keys_from_materialized_sources(tmp_path):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        left = data.from_items([{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}]).materialize()
        right = data.from_items([{'key': 2, 'score': 8}]).materialize()
        try:
            assert left.join(right, on='id', right_on='key').take_all() == [
                {'id': 2, 'v': 'b', 'key': 2, 'score': 8}]
        finally:
            left.release()
            right.release()


def test_public_terminal_metadata_aggregate_and_zero_read(tmp_path):
    import pyarrow as pa
    from demiflow.data.aggregate import Sum, Count, Mean
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        ds = data.from_items(_rows())
        assert ds.take_batch(2, batch_format='pyarrow').to_pylist() == _rows()[:2]
        assert ds.schema() == pa.schema([('n', pa.int64())])
        assert ds.columns() == ['n']
        assert ds.size_bytes() > 0
        assert ds.aggregate(Sum('n'), Count(), Mean('n')) == {'sum(n)': 15, 'count()': 6, 'mean(n)': 2.5}
        def forbidden(row):
            raise AssertionError('take(0) must not evaluate its source or operations')
        assert ds.map(forbidden).take(0) == []
        assert run.journal.execution_commits('action-6')[-1]['status'] == 'acked'


def test_public_bound_map_inputs_outputs_and_call_arguments(tmp_path):
    def transform(text, *, suffix):
        return {'value': text.upper() + suffix}
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        rows = data.from_items([{'text': 'hello'}]).map(transform, inputs={'text': 'text'},
            outputs={'value': 'result'}, fn_kwargs={'suffix': '!'}).take_all()
        assert rows == [{'text': 'hello', 'result': 'HELLO!'}]


def test_public_lance_nonzero_fragment_keeps_first_row(tmp_path):
    import lance
    import pyarrow as pa
    target = str(tmp_path / 'source.lance')
    lance.write_dataset(pa.table({'n': [-1]}), target)
    lance.write_dataset(pa.table({'n': [3, 4, 5]}), target, mode='overwrite')
    version = lance.dataset(target).version
    assert lance.dataset(target).get_fragments()[0].fragment_id != 0
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        assert data.read_lance(target, version=version).take_all() == [{'n': 3}, {'n': 4}, {'n': 5}]


def test_public_automatic_cuts_lance_publishes_once(tmp_path):
    import lance
    target = tmp_path / 'destination.lance'
    with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run, data.local_execution():
        receipt = _resume_dataset().write_lance(str(target), mode='create', return_receipt=True)
        assert receipt.committed_version == 1
        assert lance.dataset(str(target)).to_table().to_pylist() == [{'n': i + 10} for i in range(6)]
        cuts = run.journal.execution_commits('action-1')
        # Publication overlaps graph progress (2bd6c34b): a cut in flight absorbs the rows that
        # arrive meanwhile, so six rows need at least one automatic cut and the final one.
        assert len(cuts) >= 2 and all(c['status'] == 'acked' for c in cuts)
        assert all(c['prepared']['sinks']['write_lance']['native'] is None for c in cuts[:-1])
        assert cuts[-1]['prepared']['sinks']['write_lance']['native'] is not None


def test_public_recovery_preserves_completed_branch_frontier(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    def slow(row):
        import time
        time.sleep(.01)
        return row
    def dataset():
        left = data.from_items([{'n': 2}, {'n': 1}]).sort('n')
        right = data.from_items([{'n': i} for i in range(3, 43)]).map(slow)
        return left.union(right)
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupted(ex, **kw):
        status = pump(ex, **kw)
        image = ex._restore_image
        if image is not None and not image.cut.final:
            assert any(c.end_of_input for c in image.source_vector), 'completed branch not reached'
            raise Interrupted()
        return status
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 12}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupted)
                dataset().take_all()
    with RunContext.resume(ref), data.local_execution():
        assert dataset().take_all() == [{'n': i} for i in range(1, 43)]


@pytest.mark.parametrize('batch_format', ['default', 'numpy', 'pandas', 'pyarrow'])
def test_public_json_batches_preserve_requested_boundaries(tmp_path, batch_format):
    import json
    source = tmp_path / 'input.jsonl'
    source.write_text(''.join(json.dumps({'n': i}) + '\n' for i in range(11)))
    def batch(values):
        if hasattr(values, 'to_pylist'):
            values = values.to_pylist()
            return [{'size': len(values), 'total': sum(r['n'] for r in values)}]
        column = values['n']
        return [{'size': len(column), 'total': int(sum(column))}]
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        result = data.read_json(str(source)).map_batches(batch, batch_size=4, batch_format=batch_format).limit(2).take_all()
        assert result == [{'size': 4, 'total': 6}, {'size': 4, 'total': 22}]


def test_public_multifile_json_each_split_read_once(tmp_path):
    import json
    files = [tmp_path / 'a.json', tmp_path / 'b.jsonl']
    files[0].write_text(json.dumps([{'n': 0}, {'n': 1}]))
    files[1].write_text('{"n": 2}\n{"n": 3}\n')
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        rows = data.read_json([str(p) for p in files], include_paths=True).take_all()
        assert sorted(row['n'] for row in rows) == [0, 1, 2, 3]
        assert all(row['path'] == str(files[row['n'] // 2]) for row in rows)


def test_public_async_lifecycle_same_loop_and_cleanup(tmp_path):
    import asyncio
    closed = str(tmp_path / 'closed')
    class Actor:
        async def astart(self):
            self.loop = asyncio.get_running_loop()
            self.task = asyncio.create_task(asyncio.sleep(3600))
        async def __call__(self, row):
            assert asyncio.get_running_loop() is self.loop
            assert not self.task.cancelled()
            await asyncio.sleep(.001)
            return None if row['n'] == 0 else [{'n': row['n']}, {'n': row['n'] * 10}]
        async def aclose(self):
            assert asyncio.get_running_loop() is self.loop
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            Path(closed).write_text('closed')
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        rows = data.from_items([{'n': 0}, {'n': 1}, {'n': 2}]).map_async(Actor).take_all()
        assert rows == [{'n': 1}, {'n': 10}, {'n': 2}, {'n': 20}]
    assert Path(closed).read_text() == 'closed'


def test_private_actor_state_resumes_at_committed_prefix(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    class Counter:
        def __init__(self):
            self._count = 0
        def __call__(self, row):
            self._count += 1
            return {'n': row['n'], 'ordinal': self._count}
    def pipeline():
        return data.from_items([{'n': i} for i in range(12)]).map(Counter)
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(ex, **kwargs):
        result = pump(ex, **kwargs)
        if ex._restore_image is not None and not ex._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 3}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().take_all()
    with RunContext.resume(ref), data.local_execution():
        assert pipeline().take_all() == [{'n': i, 'ordinal': i + 1} for i in range(12)]


def test_resource_actor_partial_resume_rejects_before_any_new_call(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.recovery_contract import RecoveryContractError
    log = str(tmp_path / 'calls')
    class ResourceActor:
        def __init__(self):
            import threading
            self.lock = threading.Lock()
        def __call__(self, row):
            with open(log, 'a') as stream:
                stream.write(str(row['n']) + '\n')
            return row
    def pipeline():
        return data.from_items([{'n': i} for i in range(12)]).map(ResourceActor)
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(ex, **kwargs):
        result = pump(ex, **kwargs)
        if ex._restore_image is not None and not ex._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 3}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().count()
    first_attempt = Path(log).read_bytes()
    with pytest.raises(RecoveryContractError, match='attempt-only'):
        with RunContext.resume(ref), data.local_execution():
            pipeline().count()
    assert Path(log).read_bytes() == first_attempt


def test_logical_limit_stops_unbounded_source_and_commits_native_output(tmp_path):
    import lance
    closed = str(tmp_path / 'source-closed')
    def rows():
        try:
            for i in range(10000):
                if i >= 500:
                    raise AssertionError('limit failed to stop upstream admission')
                yield {'n': i}
        finally:
            Path(closed).write_text('closed')
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}) as run, data.local_execution():
        receipt = data.from_iter(rows).map(lambda r: r).limit(3).write_lance(str(tmp_path / 'out.lance'), mode='create', return_receipt=True)
        assert receipt.written_rows == 3
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert lance.dataset(str(tmp_path / 'out.lance')).to_table().to_pylist() == [{'n': 0}, {'n': 1}, {'n': 2}]
    assert Path(closed).read_text() == 'closed'


def test_limit_branch_keeps_shared_prefix_alive_for_other_consumer(tmp_path):
    log = str(tmp_path / 'prefix-calls')
    def shared(row):
        with open(log, 'a') as stream:
            stream.write(str(row['n']) + '\n')
        return row
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        prefix = data.from_items([{'n': i} for i in range(6)]).map(shared)
        rows = prefix.limit(2).union(prefix.filter(lambda r: r['n'] % 2 == 0)).take_all()
    assert [r['n'] for r in rows] == [0, 1, 0, 2, 4]
    assert list(map(int, Path(log).read_text().splitlines())) == list(range(6))


def test_run_stream_uses_real_workers_and_commit_with_driver_hooks(tmp_path):
    import asyncio
    import os
    driver = os.getpid()
    progress, drained = [], []
    async def worker(row):
        await asyncio.sleep(.001)
        assert os.getpid() != driver
        return row
    async def report(stats):
        assert os.getpid() == driver
        progress.append(stats.emitted)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        stats = data.from_items(_rows()).map_async(worker).run_stream(on_progress=report, on_drain=drained.append)
        assert stats.emitted == 6
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        assert run.journal.source_binding('action-1-dataflow-sealed')
    assert drained == [stats] and progress
