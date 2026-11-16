"""Historical Local chain guarantees on the single managed v3 execution path.

Compatibility flags do not choose another engine. The original run owns the
sealed graph, committed output and admitted managed workers in either mode.
"""
import json
import os
import sys

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, FilterOp, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _plan_and_items():
    # Local closures: cloudpickle serializes them by value so the independent
    # worker process need not import this test module.
    def double(row):
        return {**row, "x": row["x"] * 2}

    def keep_even_doubled(row):
        return row["x"] % 4 == 0

    items = tuple({"x": i} for i in range(10))
    plan = LogicalPlan((
        MapOp(CallableSpec.create(double), stream_options=MapStreamOptions(concurrency=1)),
        FilterOp(CallableSpec.create(keep_even_doubled)),
    ))
    expected = [{"x": i * 2} for i in range(10) if (i * 2) % 4 == 0]
    return items, plan, expected


def test_local_chain_full_real_worker(tmp_path):
    items, plan, expected = _plan_and_items()
    executor = LocalDatasetExecutor(workers=1, unified_chain=True)
    dataset = Dataset(ItemsSource(items), plan, executor)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}) as run:
        handle = dataset.materialize()
        rows = handle.take_all()
        assert rows == expected
        assert len(rows) == len(expected)
        # The worker attached under the granted fence; ownership never moved.
        with run.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run.attempt, run.fence)
        # The original action itself owns the confirmed output, with no
        # invented chain child action or inferred input count.
        assert run.journal.execution_action('action-1')['result'] is not None
        facts = run.journal.execution_commits('action-1')
        assert facts and all(f['status'] == 'acked' for f in facts)
        assert facts[-1]['receipts']['materialize']['data']['rows'] == len(expected)
        from _native_receipt_fixtures import committed_rows
        from demiflow.execution.state_artifacts import StateArtifacts
        assert committed_rows(run, 'action-1', facts[-1]) == expected
        image = StateArtifacts(run).read(facts[-1]['prepared']['state']['dataflow-cut-v3'])
        assert image['cut']['final']
        assert [c['next_sequence'] for c in image['dataflow.sources.v3']] == [len(items)]
    # A genuine independent worker process was supervised (birth identity recorded).
    units = tmp_path / "run-driver-resources" / "units"
    records = list(units.glob("*/*/process.json"))
    assert records, "managed worker process identity was not recorded"
    pid = json.loads(records[0].read_text())["identity"]["pid"]
    assert pid != os.getpid()
    # Capacity was released after confirmed exit evidence.
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.execution.dataflow_capacity import local_capacity
    root, capacity = local_capacity()
    pool = LocalCapacityPool(root / 'capacity', capacity=capacity)
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_default_in_driver_path_unchanged(tmp_path):
    items, plan, expected = _plan_and_items()
    executor = LocalDatasetExecutor(workers=1, unified_chain=False)
    dataset = Dataset(ItemsSource(items), plan, executor)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
        rows = dataset.materialize().take_all()
    assert rows == expected
    # The old opt-in flag is inert: the default uses the same managed engine.
    records = list((tmp_path / 'run-driver-resources' / 'units').glob('*/*/process.json'))
    assert records and all(json.loads(p.read_text())['identity']['pid'] != os.getpid() for p in records)
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(tmp_path / 'run')
    assert journal.source_binding('action-1-dataflow-sealed') is not None


def test_wide_row_admitted_with_raised_limit(tmp_path):
    """A >1MB row passes the real worker when max_row_bytes is raised."""
    def widen(row):
        return {"wide": "x" * (2 * 1024 * 1024), "i": row["i"]}

    def passthrough(row):
        return row

    items = ({"i": 0}, {"i": 1})
    plan = LogicalPlan((
        MapOp(CallableSpec.create(widen), stream_options=MapStreamOptions(concurrency=1)),
        MapOp(CallableSpec.create(passthrough), stream_options=MapStreamOptions(concurrency=1)),
    ))
    executor = LocalDatasetExecutor(workers=1, unified_chain=True)
    dataset = Dataset(ItemsSource(items), plan, executor)
    with RunContext.start(tmp_path / "run",
                          limits={"max_row_bytes": 4 * 1024 ** 2,
                                  "checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
        rows = dataset.materialize().take_all()
    assert len(rows) == 2
    assert all(len(r["wide"]) == 2 * 1024 * 1024 for r in rows)


def test_wide_row_exceeding_limit_rejected(tmp_path):
    """A row exceeding max_row_bytes is honestly rejected, not truncated."""
    def widen(row):
        return {"wide": "x" * (2 * 1024 * 1024), "i": row["i"]}

    items = ({"i": 0},)
    plan = LogicalPlan((
        MapOp(CallableSpec.create(widen), stream_options=MapStreamOptions(concurrency=1)),
    ))
    executor = LocalDatasetExecutor(workers=1, unified_chain=True)
    dataset = Dataset(ItemsSource(items), plan, executor)
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError) as exc_info:
        with RunContext.start(tmp_path / "run",
                              limits={"max_row_bytes": 1 * 1024 ** 2,
                                      "checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
            dataset.materialize().take_all()
    assert exc_info.value.code == "BUDGET_EXCEEDED"
    assert "byte budget" in str(exc_info.value).lower()