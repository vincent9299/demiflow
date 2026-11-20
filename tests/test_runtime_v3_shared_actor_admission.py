"""Public resume must reject changed instance relationships before execution."""
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from test_dataflow_shared_actor import Counter


@pytest.mark.parametrize('original_shared', [True, False])
def test_public_resume_rejects_changed_actor_relation_before_worker_start(tmp_path, monkeypatch, original_shared):
    def pipeline(shared):
        first = Counter(tmp_path/'events')
        second = first if shared else Counter(tmp_path/'events')
        return data.from_items([{'id': 1}]).map_async(first).map_async(second)

    with pytest.raises(RuntimeError, match='after-original-action'):
        with RunContext.start(tmp_path/'run') as run:
            assert pipeline(original_shared).materialize().take_all() == [{'id': 1}]
            raise RuntimeError('after-original-action')
    facts = run.journal.execution_commits('action-1')
    events = (tmp_path/'events').read_bytes()
    def no_start(*args, **kwargs):
        pytest.fail('changed actor relationship reached worker activation')
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', no_start)
    with pytest.raises(DataflowError, match='Actor instance sharing') as error:
        with RunContext.resume(run.ref):
            pipeline(not original_shared).materialize()
    assert error.value.code == 'INCOMPATIBLE_HISTORY'
    assert run.journal.execution_commits('action-1') == facts
    assert (tmp_path/'events').read_bytes() == events
