"""run_stream must not escape the public worker/memory limits of Dataset actions."""
import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.hosts.run_state import RunResourceState


def _increment(row):
    return {'n': row['n'] + 1}


@pytest.mark.parametrize('workers', [1, 2])
def test_stream_terminal_uses_original_allocator_and_public_envelope(tmp_path, monkeypatch, workers):
    observed, charges = [], []
    prepare, charged = DataflowExecution.prepare, RunResourceState._charged
    def capture(execution):
        sealed = prepare(execution)
        observed.append(execution._physical)
        return sealed
    def charge(state, db, scope):
        value = charged(state, db, scope)
        charges.append(dict(value))
        assert value['workers'] <= workers
        assert value['memory_bytes'] <= 128 * 1024
        return value
    monkeypatch.setattr(DataflowExecution, 'prepare', capture)
    monkeypatch.setattr(RunResourceState, '_charged', charge)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=workers, memory_bytes=128*1024):
        stats = data.from_items([{'n': n} for n in range(6)]).map_async(_increment, concurrency=1).run_stream()
        assert stats.emitted == 6 and stats.metrics['committed_output'] == {'rows': 6}
        assert all(commit['status'] == 'acked' for commit in run.journal.execution_commits('action-1'))
    assert observed[0].policy.max_active_workers == workers
    assert observed[0].bounds.max_resident_bytes == 128 * 1024
    assert charges and max(c['workers'] for c in charges) > 0


def test_stream_refuses_an_indivisible_stage_before_upstream_start(tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    started = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: started.append(args))
    with pytest.raises(DataflowError, match='resident budget'):
        # Default windows may adapt to this small run. Explicit row/batch
        # ceilings make the four concurrent call slots genuinely indivisible.
        with RunContext.start(tmp_path/'run', limits={
                'max_row_bytes': 32 * 1024, 'max_batch_bytes': 32 * 1024}), \
                data.local_execution(workers=1, memory_bytes=128 * 1024):
            data.from_items([{'n': 1}]).map_async(_increment, concurrency=4).run_stream()
    assert not started
