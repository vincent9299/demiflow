"""A022 · size_bytes has the same row semantics and resource admission for every map."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call, observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_plain_size_bytes_positive():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert call(ex, 'size_bytes', ds._source, ds._plan) > 0


def test_streaming_size_bytes_equals_real_plain_rows():
    ex = LocalDatasetExecutor()
    ds, plain = _stream(), _plain()
    expected = call(ex, 'size_bytes', plain._source, plain._plan)
    assert expected > 0
    assert call(ex, 'size_bytes', ds._source, ds._plan) == expected


def test_ray_native_options_size_bytes_rejected_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        call(ex, 'size_bytes', source, plan)
    assert not marker.exists()
