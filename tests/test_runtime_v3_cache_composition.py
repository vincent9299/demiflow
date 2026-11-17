"""A cached actor resumes into a materialized result on the actual provider."""
import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan
from test_runtime_v3_reducer_values import provider, value_executor


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, value):
        self.count += 1
        return self.count


class Interrupted(Exception):
    pass


def test_cached_actor_resume_materialize_shuffle_and_read(provider, tmp_path, monkeypatch):
    def flow():
        return data.from_items([{'value': n} for n in range(6)]).map_cached(
            Counter(), cache_dir=tmp_path / 'cache', version='v1',
            inputs=['value'], output='count')

    original = DataflowExecution.pump
    reached = []

    def interrupt(execution, **kwargs):
        result = original(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            reached.append(execution._restore_image.commit_id)
            raise Interrupted('after original nonfinal ACK')
        return result

    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run:
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                flow().materialize()
    assert reached

    with RunContext.resume(reference) as run:
        result = flow().materialize()
        try:
            expected = [{'value': n, 'count': n + 1} for n in range(6)]
            assert result.take_all() == expected
            one = result.random_shuffle(seed=7).take_all()
            two = result.random_shuffle(seed=7).take_all()
            assert one == two
            assert sorted(one, key=lambda row: row['value']) == expected
            sealed = load_sealed_plan(run.directory, 'action-1')
            assert sealed['physical'].policy.provider == provider
            commits = run.journal.execution_commits('action-1')
            assert commits and all(c['status'] == 'acked' for c in commits)
        finally:
            result.release()
