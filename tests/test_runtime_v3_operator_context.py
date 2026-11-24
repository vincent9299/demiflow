"""Actual callback threads and lifecycle keep only the authenticated run view."""
import json
import os

import pytest

from demiflow import data
from demiflow.execution import RunContext, current_run
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.run_journal import RunJournal


def _record(path, phase):
    from demiflow.execution.pipeline.run_context import current_action
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    run = current_run()
    assert run is not None and current_action() is None
    assert not hasattr(run, 'journal') and not hasattr(run, 'reserve')
    with pytest.raises(RecoveryContractError, match='cannot create driver actions'):
        run.action({})
    with pytest.raises(TypeError):
        run.limits['max_requests'] = 999999999
    fact = {'phase': phase, 'run_id': run.ref.run_id, 'directory': run.ref.directory,
            'attempt': run.attempt, 'limits': dict(run.limits), 'pid': os.getpid()}
    raw = (json.dumps(fact) + '\n').encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        assert os.write(fd, raw) == len(raw)
    finally:
        os.close(fd)
    return fact


@pytest.mark.parametrize('mode', ['thread', 'process'])
@pytest.mark.parametrize('kind', ['sync', 'async', 'async_thread'])
def test_operator_context_is_scoped_across_real_invocation_threads(tmp_path, mode, kind):
    audit = str(tmp_path / 'calls.jsonl')
    def sync(row):
        _record(audit, 'call')
        return row
    async def asynchronous(row):
        import asyncio
        _record(audit, 'async')
        await asyncio.to_thread(_record, audit, 'to_thread')
        return row
    with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 64*1024**2,
            'max_row_bytes': 8192, 'max_batch_bytes': 16384, 'max_requests': 7}) as run:
        with data.local_execution(worker_mode=mode):
            source = data.range(4)
            dataset = (source.map(sync) if kind == 'sync' else
                       source.map_async(sync, execution='thread', concurrency=2) if kind == 'async_thread' else
                       source.map_async(asynchronous, concurrency=2))
            assert sorted(dataset.take_all(), key=lambda row: row['id']) == [{'id': i} for i in range(4)]
        assert current_run() is run
        facts = [json.loads(line) for line in open(audit)]
        assert len(facts) == (8 if kind == 'async' else 4)
        for fact in facts:
            assert fact['run_id'] == run.ref.run_id and fact['attempt'] == 1
            assert fact['directory'] == run.ref.directory
            assert fact['limits']['max_requests'] == 7
            assert all(0 < value <= run.limits[key] for key, value in fact['limits'].items())
            assert (fact['pid'] == os.getpid()) == (mode == 'thread')
    assert current_run() is None


def test_operator_constructor_hooks_snapshot_restore_and_close_have_same_view(tmp_path, monkeypatch):
    audit = str(tmp_path / 'actor.jsonl')
    class Counter:
        def __init__(self):
            self.n = 0
            _record(audit, 'construct')
        async def astart(self):
            _record(audit, 'start')
        async def __call__(self, row):
            _record(audit, 'call')
            self.n += 1
            return dict(row, n=self.n)
        def snapshot_state(self):
            _record(audit, 'snapshot')
            return {'n': self.n}
        def restore_state(self, state):
            _record(audit, 'restore')
            self.n = state['n']
        async def astop(self):
            _record(audit, 'stop')
        async def aclose(self):
            _record(audit, 'close')
    class AfterAck(Exception):
        pass
    commit = DataflowCheckpointCoordinator.commit
    acknowledged = []
    def stop(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        if not image.cut.final:
            acknowledged.append(image.commit_id)
            raise AfterAck('original nonfinal ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
        with pytest.raises(AfterAck, match='nonfinal ACK'):
            with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run:
                reference = run.ref
                data.range(12).map_async(Counter, concurrency=1).take_all()
    assert acknowledged and current_run() is None
    with RunContext.resume(reference) as run:
        rows = data.range(12).map_async(Counter, concurrency=1).take_all()
        assert rows == [dict(id=i, n=i+1) for i in range(12)]
        assert current_run() is run
    assert current_run() is None
    facts = [json.loads(line) for line in open(audit)]
    assert {'construct','start','call','snapshot','restore','stop','close'} <= {fact['phase'] for fact in facts}
    assert {fact['attempt'] for fact in facts} == {1, 2}
    assert all(fact['run_id'] == reference.run_id and fact['directory'] == reference.directory for fact in facts)
    assert RunJournal(reference.directory).execution_commit(acknowledged[0])['status'] == 'acked'
