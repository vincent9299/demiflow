"""A021 · schema metadata peek is passive; fetching uses the actual native terminal."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call, observed_source


def _plain():
    return data.from_items([{"id": 0, "name": "x"}]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": 0}]).map(lambda r: r, execution="thread", concurrency=2)


def test_fetch_if_missing_false_returns_none_without_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    source, marker = observed_source(tmp_path)
    assert ex.schema(source, ds._plan, fetch_if_missing=False) is None
    assert not marker.exists()


def test_plain_schema_infers_fields():
    ex = LocalDatasetExecutor()
    ds = _plain()
    schema = call(ex, 'schema', ds._source, ds._plan)
    assert set(schema.names) == {"id", "name"}


def test_streaming_schema_infers_real_fields():
    ex = LocalDatasetExecutor()
    ds = _stream()
    assert call(ex, 'schema', ds._source, ds._plan).names == ['id']


def test_ray_native_options_schema_rejected_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        call(ex, 'schema', source, plan)
    assert not marker.exists()
