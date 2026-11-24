"""A015 · the single compiler reports capability without choosing a second engine.

The old _execution_capabilities backend table was retired. Both providers now
compile the same declaration; source inspection and actual placement remain at
execution, never masquerading as a guaranteed grant in an explain report.
"""
import pytest

from demiflow import data
from demiflow.execution.executors.dataflow_api import explain_dataset
from demiflow.execution.pipeline.run_context import current_action, current_run
from _runtime_v3_executor_scope import observed_source


def report(dataset, provider='local', kind='take'):
    return explain_dataset(dataset._source, dataset._plan, provider=provider,
        options={'action_kind': kind})


def test_local_and_ray_have_the_same_declared_capability():
    ds = data.from_items([{'id': 0}]).map(lambda row: row)
    assert report(ds) == report(ds, provider='ray_core')
    assert report(ds).supported and report(ds).required_profiles


def test_lance_declaration_is_not_read_by_explain(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    uri = tmp_path / 'source.lance'
    lance.write_dataset(pa.table({'id': [1]}), str(uri))
    ds = data.read_lance(str(uri), version=1)
    def forbidden(*args, **kwargs):
        raise AssertionError('explain must not open the fixed Lance source')
    monkeypatch.setattr(lance, 'dataset', forbidden)
    assert report(ds, kind='write_lance').supported


def test_arrow_batch_capability_does_not_construct_actor():
    constructed = []
    class Actor:
        def __init__(self):
            constructed.append(1)
        def __call__(self, batch):
            return batch
    ds = data.from_items([{'id': 0}]).map_batches(Actor, batch_format='pyarrow')
    assert report(ds, kind='write_lance').supported
    assert constructed == []


def test_explain_keeps_the_same_declared_resources_for_read_and_write(tmp_path):
    source, marker = observed_source(tmp_path)
    from demiflow.data.dataset import Dataset
    from demiflow.data.plan import LogicalPlan
    from demiflow.execution.executors.local import LocalDatasetExecutor
    ds = Dataset(source, LogicalPlan(), LocalDatasetExecutor()).map_batches(lambda batch: batch,
        batch_format='pyarrow', backend_options={'ray': {'worker_resources': {'cpu': 2}}})
    read, write = report(ds), report(ds, kind='write_lance')
    assert read.required_profiles == write.required_profiles
    assert any(p.requests.get('cpu_millis') == 2000 for p in read.required_profiles)
    assert not marker.exists()


def test_empty_explain_creates_no_run_or_action():
    assert current_run() is None and current_action() is None
    result = report(data.from_items([]), kind='iter_rows')
    assert result.supported and result.required_profiles
    assert current_run() is None and current_action() is None
