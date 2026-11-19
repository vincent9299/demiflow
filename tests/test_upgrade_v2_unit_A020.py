"""A020 · aggregate uses the same native terminal for ordinary and streaming maps."""
from demiflow.data.dataset import Dataset
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.data.aggregate import Sum
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_streaming_aggregate_preserves_real_sum():
    ex = LocalDatasetExecutor()
    ds = _stream()
    assert Dataset(ds._source, ds._plan, ex).aggregate(*(Sum('id'),)) == {'sum(id)': 3}


def test_ray_native_options_aggregate_rejected_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        Dataset(source, plan, ex).aggregate(*(Sum('id'),))
    assert not marker.exists()


def test_plain_empty_source_aggregate_preserves_empty_sum():
    ex = LocalDatasetExecutor()
    ds = data.from_items([]).map(lambda r: r)
    assert Dataset(ds._source, ds._plan, ex).aggregate(*(Sum('id'),)) == {'sum(id)': None}
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
