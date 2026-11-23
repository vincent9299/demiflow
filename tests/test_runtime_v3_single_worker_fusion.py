"""Explicit one-worker pipelines retain the real graph, budgets and recovery."""
import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext


def pipeline():
    return data.from_items([{'n': n} for n in range(300)]).map(lambda row: {'n': row['n'] + 1})


def test_timer_operator_cannot_share_a_blocking_source_business_thread():
    from demiflow.execution.executors.dataflow_api import _CompileContext
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.pipeline.run_context import RunAction
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_fusion import fuse_single_worker_pipeline
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    dataset = data.range(2).batch_map(lambda rows: rows, max_batch=2, flush_interval=.01)
    action = RunAction(_CompileContext(), 'timer-compile', {})
    request = ActionRequest(kind='take_all', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='local', action=action)
    policy, bounds = resolve_runtime_policy({'max_active_workers': 1}, {}, provider='local')
    graph = lower_action(request, bounds=bounds).graph
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    assert len(stages) == 3
    assert any(node.kernel.configuration.get('flush_interval') for node in graph.nodes)
    assert fuse_single_worker_pipeline(graph, stages, policy=policy, bounds=bounds) == stages


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
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
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
