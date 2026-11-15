"""A user closing iteration retains a real non-final prefix receipt."""
import pytest
from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('workers', [1, 2])
def test_user_iterator_close_preserves_nonfinal_source_prefix(tmp_path, monkeypatch, workers):
    executions = []
    original = DataflowExecution.start
    def start(execution):
        executions.append(execution)
        return original(execution)
    monkeypatch.setattr(DataflowExecution, 'start', start)
    with RunContext.start(tmp_path/'run', limits={'timeout_s':20,
            'checkpoint_interval_ms':60000}) as run, data.local_execution(
            workers=workers, worker_mode='thread'):
        rows = data.range(1000).iter_rows()
        assert next(rows) == {'id':0}
        rows.close()
        execution = executions[0]
        image = execution._restore_image
        assert execution._stopping and not execution._checkpoint.inputs_complete()
        assert not image.cut.final
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert 0 < execution._terminal_result.rows < 1000
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        cursors = [cursor for part in image.contributions for cursor in part.sources]
        assert cursors and all(not cursor.end_of_input and cursor.next_sequence < 1000 for cursor in cursors)
