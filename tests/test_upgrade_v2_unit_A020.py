"""A020 · aggregate uses the same native terminal for ordinary and streaming maps."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.data.aggregate import Sum
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call, observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_streaming_aggregate_preserves_real_sum():
    ex = LocalDatasetExecutor()
    ds = _stream()
    assert call(ex, 'aggregate', ds._source, ds._plan, (Sum('id'),)) == {'sum(id)': 3}


def test_ray_native_options_aggregate_rejected_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        call(ex, 'aggregate', source, plan, (Sum('id'),))
    assert not marker.exists()


def test_plain_empty_source_aggregate_preserves_empty_sum():
    ex = LocalDatasetExecutor()
    ds = data.from_items([]).map(lambda r: r)
    assert call(ex, 'aggregate', ds._source, ds._plan, (Sum('id'),)) == {'sum(id)': None}
    # The public scalar helper retains its empty-input result. The old internal
    # executor's empty aggregation tuple was never a valid public aggregate().
    assert ds.sum('id') is None


def test_public_aggregate_requires_an_aggregation_before_source_read(tmp_path):
    source, marker = observed_source(tmp_path)
    from demiflow.data.dataset import Dataset
    from demiflow.data.plan import LogicalPlan
    with pytest.raises(ValueError, match='at least one aggregation'):
        Dataset(source, LogicalPlan(), LocalDatasetExecutor()).aggregate()
    assert not marker.exists()


def test_plain_empty_source_aggregate_admits_and_returns_none():
    ex = LocalDatasetExecutor()
    ds = data.from_items([]).map(lambda row: row)
    assert call(ex, 'aggregate', ds._source, ds._plan, ()) is None


def test_nonempty_source_empty_aggregation_preserves_empty_dict(tmp_path):
    from demiflow.execution.run_context import RunContext
    ex = LocalDatasetExecutor()
    ds = data.from_items([{'id': 1}, {'id': 2}]).map(lambda row: row)
    with RunContext.start(tmp_path / 'run') as run:
        assert call(ex, 'aggregate', ds._source, ds._plan, ()) == {}
        assert run.position == 1
        assert run.journal.execution_action('action-2') is None
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
