"""U2 S4 fault matrix: F2 worker crash, F3 driver restart, F4 writer stall.

These compose the frozen recovery contract through the real Local chain rather
than re-proving it at unit level (the CMT/AUT groups already do that). F2 kills a
genuine worker process after an acked prefix and proves a new allocation recovers
without duplicating that prefix. F3 faults the driver's single commit authority
at ``after_prepared``, resumes the run under a new fence/attempt and proves the
prepared cut is reconciled before any recompute (source trace) and published
exactly once; a completed chain short-circuits re-entry without a second worker.
F4 drives the worker executor's cancel grace: a stalled owned writer is isolated
(quarantine) while a converging one releases cleanly. The driver run-lease
retention during quarantine is the E1 finalizer, already covered by the barrier
trio. No GPU/multi-host claims.
"""
import json
import os
import signal
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, FilterOp, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution import commit as commit_module
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.execution import WorkerChainExecutor
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.process_unit import ManagedProcessUnit

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _plan(double, items_count=10):
    def keep_even_doubled(row):
        return row["x"] % 4 == 0

    items = tuple({"x": i} for i in range(items_count))
    plan = LogicalPlan((
        MapOp(CallableSpec.create(double), stream_options=MapStreamOptions(concurrency=1)),
        FilterOp(CallableSpec.create(keep_even_doubled)),
    ))
    expected = [{"x": i * 2} for i in range(items_count) if (i * 2) % 4 == 0]
    return items, plan, expected


def _dataset(double):
    items, plan, expected = _plan(double)
    executor = LocalDatasetExecutor(workers=1, unified_chain=True)
    return Dataset(ItemsSource(items), plan, executor), expected


def _chain_child_id(run):
    with run.journal._connection() as db:
        row = db.execute("SELECT action_id FROM runtime_actions "
                         "WHERE action_id LIKE '%-chain'").fetchone()
    assert row is not None, "chain output child action was not bound"
    return row[0]


def _release_crash_orphans(root):
    """Reconcile capacity for allocations whose driver died before close.

    A crash/fault leaves the allocation BOUND (charged); exit alone never frees
    capacity. The canonical unit-lock + birth-identity evidence confirms the dead
    host and releases it, exactly as the provider's ensure_absent path does.
    """
    root = Path(root)
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    for record in pool.snapshot()["active"]:
        allocation_id = record["allocation_id"]
        process_json = next((root / "units").glob("*/" + allocation_id + "/process.json"))
        scope_id = process_json.parent.parent.name
        evidence = unit.stop(scope_id, allocation_id, terminate_deadline_ms=2000)
        assert evidence["confirmed"] is True
        pool.release(allocation_id, evidence)
        try:
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)  # reap our own zombie child
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


# ---------------- F2: worker crash, new allocation recovers ----------------

def test_f2_worker_crash_recovers_acked_prefix_exactly_once(tmp_path):
    crash_marker = tmp_path / "crashed-once"

    def double(row):
        # Crash the whole worker once, after at least one durable acked cut.
        if row["x"] == 4 and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, "x": row["x"] * 2}

    dataset, expected = _dataset(double)
    # The worker dies mid-chain; the driver run exits failed. Recovery re-enters
    # the same action (RunContext.resume resets the action position) under a new
    # allocation, reconciling the acked prefix rather than duplicating it.
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run",
                              limits={"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}) as run:
            reference = run.ref
            dataset.materialize().take_all()  # worker SIGKILLs mid-chain
    assert crash_marker.exists()
    with RunContext.resume(reference) as run2:
        rows = dataset.materialize().take_all()
        assert rows == expected
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        sequences = [fact["sequence"] for fact in facts]
        assert sequences == list(range(1, len(facts) + 1))  # contiguous, no gap/dup
        assert all(fact["status"] == "acked" for fact in facts)
        assert len({fact["commit_id"] for fact in facts}) == len(facts)
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)  # worker crash never moved ownership

    records = list((tmp_path / "run-resources" / "units").glob("*/*/process.json"))
    assert len(records) == 2, "a new worker allocation was not provisioned for recovery"
    pids = {json.loads(path.read_text())["identity"]["pid"] for path in records}
    assert len(pids) == 2 and os.getpid() not in pids
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- F3: driver restart reconciles before recompute ----------------

def test_f3_driver_restart_recovers_prepared_before_recompute(tmp_path, monkeypatch):
    trace = tmp_path / "source-trace"

    def double(row):
        with open(trace, "a") as stream:
            stream.write(str(row["x"]) + "\n")
        return {**row, "x": row["x"] * 2}

    dataset, expected = _dataset(double)
    faulted = []

    def crash(name, commit_id):
        if name == "after_prepared" and not faulted:
            faulted.append(commit_id)
            raise RuntimeError("driver restart after prepare")

    monkeypatch.setattr(commit_module, "fault_point", crash)
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / "run",
                              limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}) as run:
            reference = run.ref
            old_fence = run.fence
            dataset.materialize().take_all()  # faults after the first cut is prepared
    assert faulted, "the after_prepared fault window did not fire"

    # Real driver restart: a new attempt/fence resumes the interrupted run.
    with RunContext.resume(reference) as run2:
        assert run2.attempt == 2 and run2.fence != old_fence
        rows = dataset.materialize().take_all()
        assert rows == expected
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        sequences = [fact["sequence"] for fact in facts]
        assert sequences == list(range(1, len(facts) + 1))
        assert all(fact["status"] == "acked" for fact in facts)
        assert len({fact["commit_id"] for fact in facts}) == len(facts)

    # The acked prefix was never recomputed: each source row ran exactly once.
    assert sorted(int(line) for line in trace.read_text().splitlines()) == list(range(10))
    _release_crash_orphans(tmp_path / "run-resources")


def test_f3_completed_chain_short_circuits_reentry(tmp_path):
    def double(row):
        return {**row, "x": row["x"] * 2}

    dataset, expected = _dataset(double)
    # The chain completes, then a later action fails so the run exits failed with
    # the chain child result durable. Resuming re-enters the same completed action.
    with pytest.raises(RuntimeError, match="later action failed"):
        with RunContext.start(tmp_path / "run",
                              limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}) as run:
            reference = run.ref
            assert dataset.materialize().take_all() == expected
            raise RuntimeError("later action failed")
    with RunContext.resume(reference) as run2:
        # terminal.complete short-circuits before any worker provisioning or
        # duplicate commit (the restart-after-completion exactly-once gap).
        assert dataset.materialize().take_all() == expected
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        assert all(fact["status"] == "acked" for fact in facts)

    records = list((tmp_path / "run-resources" / "units").glob("*/*/process.json"))
    assert len(records) == 1, "completed chain re-entry provisioned a second worker"
    pool = LocalCapacityPool(tmp_path / "run-resources" / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


# ---------------- F4: cancel grace convergence / quarantine isolation ----------------

class _FakeAuthority:
    def __init__(self, grace_s):
        self.limits = {"cancel_grace_s": grace_s}
        self._local_cancel = False
        self.quarantined = []

    def cancel_local(self):
        self._local_cancel = True

    def quarantine_worker(self, worker):
        if worker not in self.quarantined:
            self.quarantined.append(worker)


def _executor(authority):
    launch_spec = {"execution": {"source_child_id": "source", "output_child_id": "chain",
                                 "plan_ref": {}, "source_reference": {}, "channel_id": "c"}}
    executor = WorkerChainExecutor(launch_spec, send=lambda message: None)
    executor.authority = authority
    return executor


def test_f4_stalled_writer_is_quarantined_after_grace():
    authority = _FakeAuthority(grace_s=0.05)
    executor = _executor(authority)
    release = threading.Event()
    stalled = threading.Thread(target=release.wait, name="stalled-writer", daemon=True)
    stalled.start()
    executor._thread = stalled
    try:
        with pytest.raises(RecoveryContractError, match="cancel grace"):
            executor.cancel({})
        assert authority._local_cancel is True
        assert stalled in authority.quarantined  # owned writer isolated, not killed
        assert stalled.is_alive()
    finally:
        release.set()
        stalled.join(timeout=2)


def test_f4_converging_writer_releases_without_quarantine():
    authority = _FakeAuthority(grace_s=2.0)
    executor = _executor(authority)

    def run_until_cancel():
        while not authority._local_cancel:
            time.sleep(0.01)

    thread = threading.Thread(target=run_until_cancel, name="converging-writer", daemon=True)
    thread.start()
    executor._thread = thread
    executor.cancel({})  # cancel_local converges the writer within grace
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert authority.quarantined == []