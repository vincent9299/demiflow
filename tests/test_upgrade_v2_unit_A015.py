"""A015 · _execution_capabilities declares backends from configuration only."""
from types import SimpleNamespace

from demiflow import data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapBatchesOp, MapOp
from demiflow.data.sources import LanceSource
from demiflow.execution.executors.local import LocalDatasetExecutor


def _plain_plan():
    return data.from_items([{"id": 0}])._plan


def test_default_backends_always_present():
    ex = LocalDatasetExecutor()
    caps = ex._execution_capabilities(data.from_items([{"id": 0}])._source,
                                      _plain_plan(), action_kind="take")
    assert caps[:2] == ("local_pull", "local_stream")
    assert "datafusion" not in caps and "arrow_batch" not in caps


def test_lance_fast_path_declares_lance_scan():
    ex = LocalDatasetExecutor()
    source = LanceSource(query=None)
    caps = ex._execution_capabilities(source, LogicalPlan(), action_kind="write_lance",
                                      sink_spec=SimpleNamespace(schema=None))
    assert "lance_scan" in caps


def test_arrow_eligible_declares_arrow_batch():
    ex = LocalDatasetExecutor(local_kernel=SimpleNamespace(workers=1))
    source = LanceSource(query=None)
    plan = LogicalPlan((MapBatchesOp(CallableSpec.create(lambda b: b), batch_format="pyarrow"),))
    caps = ex._execution_capabilities(source, plan, action_kind="write_lance",
                                      sink_spec=SimpleNamespace(schema=object()))
    assert "arrow_batch" in caps
    assert "lance_scan" not in caps


def test_sink_spec_ignored_for_non_write():
    ex = LocalDatasetExecutor(local_kernel=SimpleNamespace(workers=1))
    source = LanceSource(query=None)
    plan = LogicalPlan((MapBatchesOp(CallableSpec.create(lambda b: b), batch_format="pyarrow"),))
    caps = ex._execution_capabilities(source, plan, action_kind="take",
                                      sink_spec=SimpleNamespace(schema=object()))
    assert "arrow_batch" not in caps and "lance_scan" not in caps


def test_capabilities_never_empty():
    ex = LocalDatasetExecutor()
    caps = ex._execution_capabilities(data.from_items([])._source, LogicalPlan(),
                                      action_kind="iter_rows")
    assert caps and "local_pull" in caps