"""Real callback facts and their original checkpoint history, never estimates."""
from types import SimpleNamespace
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.dataflow_operator_observations import CallbackObservations, observe_operator


def transform(row):
    if row['n'] == 0:
        raise LookupError('missing')
    return row if row['n'] % 2 else None


def flow(label='prepare'):
    return data.from_items([{'n': i} for i in range(8)]).map(
        transform, execution='inline', catch=(LookupError,), label=label)


def test_original_cut_restores_callback_counts_without_counting_prefix_twice(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    class Interrupted(Exception):
        pass
    original = DataflowCheckpointCoordinator.commit
    def stop(coordinator, *args, **kwargs):
        result = original(coordinator, *args, **kwargs)
        assert not result.cut.final
        raise Interrupted()
    with pytest.raises(Interrupted), RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2,
            'checkpoint_interval_ms': 60000}) as run:
        ref = run.ref
        with monkeypatch.context() as patch:
            patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
            flow().run_stream()
    with RunContext.resume(ref) as run:
        before = run.journal.execution_commits('action-1')
        result = flow().run_stream()
        assert result.emitted == 4
        assert result.miss == {'prepare:LookupError': 1, 'prepare:drop': 3}
        latency = result.metrics['stage_processing_latency']['prepare']
        assert latency['count'] == 8
        assert 0 <= latency['min_s'] <= latency['max_s'] <= latency['total_s']
        assert latency['mean_s'] == latency['total_s'] / 8
        assert run.journal.execution_commits('action-1')[:len(before)] == before


def test_display_labels_are_sealed_without_changing_execution_identity(tmp_path):
    plans = []
    for label in ('before', 'after'):
        with RunContext.start(tmp_path/label) as run:
            result = flow(label).run_stream()
            assert result.miss == {label+':LookupError': 1, label+':drop': 3}
            seal = run.journal.source_binding('action-1-dataflow-sealed')
            assert label in seal['observation_labels'].values()
            plans.append((seal['dag_id'], seal['plan_id']))
    assert plans[0] == plans[1]


def test_callback_fact_validation_and_new_boot_replace_committed_prefix():
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    observations = CallbackObservations()
    for i in range(100):
        observations.observe(i, 'Exception'+str(i))
    value = observations.snapshot()
    assert value['count'] == sum(value['miss'].values()) == 100
    assert len(value['miss']) == 33
    execution = SimpleNamespace()
    def emit(boot, value):
        observe_operator(execution, SimpleNamespace(boot_id=boot), {
            'occurrence_id':'o', 'partition_id':'p', 'callback_observations':value})
    emit('first', value)
    smaller = CallbackObservations(); smaller.observe(3, 'drop')
    with pytest.raises(DataflowError, match='backwards'):
        emit('first', smaller.snapshot())
    emit('new', smaller.snapshot())
    assert execution._operator_observations[('o','p')][1]['count'] == 1
    with pytest.raises(DataflowError, match='invalid'):
        emit('new', {**smaller.snapshot(), 'count':True})
