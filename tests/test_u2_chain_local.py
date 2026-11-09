"""U2 Local full-chain acceptance (S4, D3/D5).

The whole operator chain runs in one independent managed worker process; the
driver owns only the control plane, the fixed source capture and the single
result-commit authority. These tests drive the real ResourceManager +
LocalResourceProvider + managed process unit + inherited socketpair, the frozen
K3 lifecycle, the CommitCut/CommitReceipt handshake and the final drain. The
platform-side selector seam (``unified_chain``) is opt-in; the default in-driver
stream path must remain byte-for-byte unchanged (D5).
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
        # The output child action completed through the single commit authority.
        with run.journal._connection() as db:
            child = db.execute("SELECT result FROM runtime_actions "
                               "WHERE action_id LIKE '%-chain'").fetchone()
        assert child is not None and child[0] is not None
        result = json.loads(child[0])
        assert result["rows"] == len(expected)
        assert result["inputs"] == len(items)
    # A genuine independent worker process was supervised (birth identity recorded).
    units = tmp_path / "run-resources" / "units"
    records = list(units.glob("*/*/process.json"))
    assert records, "managed worker process identity was not recorded"
    pid = json.loads(records[0].read_text())["identity"]["pid"]
    assert pid != os.getpid()
    # Capacity was released after confirmed exit evidence.
    from demiflow.resources.capacity import LocalCapacityPool
    pool = LocalCapacityPool(tmp_path / "run-resources" / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_default_in_driver_path_unchanged(tmp_path):
    items, plan, expected = _plan_and_items()
    executor = LocalDatasetExecutor(workers=1, unified_chain=False)
    dataset = Dataset(ItemsSource(items), plan, executor)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
        rows = dataset.materialize().take_all()
    assert rows == expected
    # The default seam never provisions a managed worker host.
    assert not (tmp_path / "run-resources").exists()