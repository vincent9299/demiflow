"""Explicit one-worker pipelines retain the real graph, budgets and recovery."""
import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


def pipeline():
    return data.from_items([{'n': n} for n in range(300)]).map(lambda row: {'n': row['n'] + 1})


def test_one_worker_crosses_credit_windows_without_process_restarts(tmp_path, monkeypatch):
    plans, observations, allocations = [], [], []
    prepare, finish, allocate = DataflowExecution.prepare, DataflowExecution._set_result, DataflowExecution._prepare_launch
    def preparing(execution):
        result = prepare(execution)
        plans.append((execution._graph, execution._physical))
        return result
    def finishing(execution, image):
        observations.append(dict(execution._node_metrics))
        return finish(execution, image)
    def allocating(execution, *args, **kwargs):
        result = allocate(execution, *args, **kwargs)
        allocations.append(result)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', preparing)
    monkeypatch.setattr(DataflowExecution, '_set_result', finishing)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', allocating)
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 60}) as run, data.local_execution(workers=1):
        assert pipeline().take_all() == [{'n': n + 1} for n in range(300)]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    graph, physical = plans[0]
    assert len(allocations) == len(physical.stages) == 1
    assert len(graph.nodes) == len(physical.stages[0].occurrences) == 3
    # Logical edges remain inspectable in the sealed plan, but they never
    # instantiate transport queues inside this one physical owner.
    assert all(edge.producer_partitions == edge.consumer_partitions == physical.stages[0].partitions
               for edge in physical.exchanges)
    assert physical.stages[0].profile.requests['memory_bytes'] + physical.residency.control_reserved_bytes <= 256 * 1024**2
    assert {occ for occ, _ in observations[0]} == {node.occurrence_id for node in graph.nodes}
    assert all(metric['rows_output'] == 300 for metric in observations[0].values())


def test_fused_checkpoint_restores_original_plan_and_result_prefix(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    class Interrupted(Exception):
        pass
    commit = DataflowCheckpointCoordinator.commit
    plans = []
    prepare = DataflowExecution.prepare
    def preparing(execution):
        result = prepare(execution)
        plans.append(execution._physical)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', preparing)
    def interrupt(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        if not image.cut.final:
            raise Interrupted()
        return image
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000, 'timeout_s': 60}
    def small_pipeline():
        return data.range(6).map(lambda row: {'n': row['id']})
    with pytest.raises(Interrupted), RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=1):
        ref = run.ref
        with monkeypatch.context() as patch:
            patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupt)
            small_pipeline().take_all()
    with RunContext.resume(ref) as resumed, data.local_execution(workers=1):
        before = resumed.journal.execution_commits('action-1')
        assert small_pipeline().take_all() == [{'n': n} for n in range(6)]
        after = resumed.journal.execution_commits('action-1')
        assert after[:len(before)] == before
    assert len(plans[0].stages) == 1 and plans[1].plan_id == plans[0].plan_id
