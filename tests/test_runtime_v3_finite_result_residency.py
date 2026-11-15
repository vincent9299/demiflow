"""A finite result exceeds a parked delivery window without exceeding memory."""
import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


def joined():
    return data.from_items([{'k': i, 'left': i} for i in range(30)]).join(
        data.from_items([{'k': i, 'right': j} for i in range(30) for j in range(2)]), on='k')


def test_finite_result_keeps_original_small_budget_and_resumes_original_seal(tmp_path, monkeypatch):
    class Interrupted(Exception):
        pass
    retire = DataflowExecution._retire_phase
    sealed = []
    def interrupt(execution, image):
        node = next(n for n in execution._graph.nodes if n.kernel.kind == 'join')
        stage = next(s for s in execution._physical.stages if node.occurrence_id in s.occurrences)
        if stage.stage_id in execution._phases[execution._phase_index]:
            outputs = [edge for edge in execution._physical.exchanges
                       if set(edge.producer_partitions) & set(stage.partitions)]
            assert outputs and all(edge.mode == 'finite_sealed' for edge in outputs)
            assert all(edge.channel_id in execution._finite_seals for edge in outputs)
            assert execution._bounds.max_resident_bytes == 128 * 1024
            assert execution._bounds.max_state_bytes == 16 * 1024
            sealed.append(execution._physical.plan_id)
            raise Interrupted()
        return retire(execution, image)
    with pytest.raises(Interrupted), RunContext.start(tmp_path/'run') as run:
        ref = run.ref
        with data.local_execution(workers=2, memory_bytes=128 * 1024), monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_retire_phase', interrupt)
            joined().take_all()
    assert len(sealed) == 1
    with RunContext.resume(ref) as run, data.local_execution(workers=2, memory_bytes=128 * 1024):
        before = run.journal.execution_commits('action-1')
        rows = joined().take_all()
        assert sorted((row['k'], row['left'], row['right']) for row in rows) == [
            (i, i, j) for i in range(30) for j in range(2)]
        after = run.journal.execution_commits('action-1')
        assert after[:len(before)] == before
        assert all(commit['status'] == 'acked' for commit in after)
