"""Write admission rejects invalid resources before reading the source."""
import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.datasink import Datasink
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import observed_source


def test_invalid_native_resources_rejected_before_read(tmp_path):
    source, marker = observed_source(tmp_path)
    plan = LogicalPlan().append(MapOp(CallableSpec.create(lambda row: row),
        native_options=NativeOptions(backend='ray', worker_resources={'cpu': 0})))
    dataset = Dataset(source, plan, LocalDatasetExecutor())
    class Sink(Datasink):
        def recovery_identity(self):
            return {'kind': 'admission-only-sink'}
        def prepare(self, blocks, context):
            raise AssertionError('sink must not start before admission')
        commit = inspect = abort = prepare
    with pytest.raises(DataflowError, match='positive CPU'):
        dataset.write_datasink(Sink())
    assert not marker.exists()
