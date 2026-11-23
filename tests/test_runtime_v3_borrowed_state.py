"""Real borrowed-thread ownership versus process-owned recovery state."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _counting_flow(state):
    def count(row):
        state[0] += 1
        return {**row, 'count': state[0]}
    return data.from_items([{'i': i} for i in range(4)]).map(
        count, execution='thread', concurrency=1)


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_real_host_mode_controls_borrowed_state_admission(tmp_path, mode):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    state = [0]
    with RunContext.start(tmp_path/'run') as run, data.local_execution(
            workers=2, worker_mode=mode):
        assert _counting_flow(state).take_all() == [
            {'i': i, 'count': i + 1} for i in range(4)]
        bundle = load_sealed_plan(run.ref.directory, 'action-1')
        node = next(n for n in bundle['graph'].nodes if n.kernel.kind == 'MapOp')
        stage = next(s for s in bundle['physical'].stages if node.occurrence_id in s.occurrences)
        assert bool(node.kernel.configuration.get('thread_attempt')) == (mode == 'thread')
        if mode == 'thread':
            assert node.kernel.state == 'nonrecoverable'
            assert stage.state_bytes == 0
            assert bundle['physical'].recovery == 'attempt_only'
        else:
            assert node.kernel.state == 'adapter'
            assert stage.state_bytes >= bundle['physical'].bounds.max_state_bytes
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    # Shared user observations remain live only in the explicit thread attempt.
    assert state == ([4] if mode == 'thread' else [0])



def test_borrowed_json_state_cannot_resume_after_original_ack(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    class Interrupted(Exception):
        pass
    commit = DataflowCheckpointCoordinator.commit
    seen = []
    def stop(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        assert not image.cut.final
        seen.append(image)
        raise Interrupted()
    state = [0]
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2,
                'checkpoint_interval_ms': 60000}) as run, data.local_execution(workers=2):
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                _counting_flow(state).take_all()
    assert seen
    before = list(state)
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    journal = RunJournal(reference.directory)
    committed = journal.execution_commits('action-1')
    assert committed and all(c['status'] == 'acked' for c in committed)
    fresh_state = [0]
    # A new attempt cannot rebind shared state to a fresh live registry token.
    with pytest.raises(DataflowError, match='attempt-only live program has no final ACKed result'):
        with RunContext.resume(reference), data.local_execution(workers=2):
            _counting_flow(fresh_state).take_all()
    assert state == before and fresh_state == [0]
    assert journal.execution_commits('action-1') == committed
