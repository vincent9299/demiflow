"""U2 S4 fault matrix: worker loss and driver restart.

These compose the frozen recovery contract through the real Local chain rather
than re-proving it at unit level (the CMT/AUT groups already do that). F2 kills a
genuine worker process after an acked prefix and proves a new allocation recovers
without duplicating that prefix. F3 faults the driver's single commit authority
at ``after_prepared``, resumes the run under a new fence/attempt and proves the
prepared cut is reconciled before any recompute (source trace) and published
exactly once; a completed chain short-circuits re-entry without a second worker.
"""
import json
import os
import signal
import sys
import uuid
from pathlib import Path

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, FilterOp, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution import commit as commit_module
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext
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
    executor = LocalDatasetExecutor(workers=1)
    return Dataset(ItemsSource(items), plan, executor), expected


def _chain_child_id(run):
    from runtime_v3_owned_crash import materialize_action_id
    return materialize_action_id(run)


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
    from runtime_v3_owned_crash import (kill_owned_worker, assert_worker_killed,
                                       action_process_records, assert_released)
    driver_pid = os.getpid()
    crash_marker = tmp_path / "crashed-once"
    trace = tmp_path / 'business-calls'

    def double(row):
        with trace.open('a') as stream:
            stream.write(str(row['x']) + '\n')
        # Crash the whole worker once, after at least one durable acked cut.
        if row["x"] == 4 and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        return {**row, "x": row["x"] * 2}

    dataset, expected = _dataset(double)
    # The worker dies mid-chain; the driver run exits failed. Recovery re-enters
    # the same action (RunContext.resume resets the action position) under a new
    # allocation, reconciling the acked prefix rather than duplicating it.
    with pytest.raises((RecoveryContractError, OSError, DataflowError)) as failure:
        with RunContext.start(tmp_path / "run",
                              limits={"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}) as run:
            reference = run.ref
            dataset.materialize().take_all()  # worker SIGKILLs mid-chain
    from runtime_v3_owned_crash import assert_worker_loss
    assert_worker_loss(failure.value)
    assert_worker_killed(crash_marker, tmp_path / 'run')
    with RunContext.resume(reference) as run2:
        before = {f['commit_id']: f for f in run2.journal.execution_commits(_chain_child_id(run2))
                  if f['status'] == 'acked'}
        assert before, 'the real crash must retain its original ACK prefix'
        from demiflow.execution.storage.state_artifacts import StateArtifacts
        latest = max(before.values(), key=lambda fact: fact['sequence'])
        image = StateArtifacts(run2).read(latest['prepared']['state']['dataflow-cut-v3'])
        sources = image['dataflow.sources.v3']
        assert len(sources) == 1 and not sources[0]['pending_refs']
        acknowledged_prefix = sources[0]['next_sequence']
        assert 0 < acknowledged_prefix <= 4
        rows = dataset.materialize().take_all()
        assert rows == expected
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        sequences = [fact["sequence"] for fact in facts]
        assert sequences == list(range(1, len(facts) + 1))  # contiguous, no gap/dup
        assert all(fact["status"] == "acked" for fact in facts)
        assert len({fact["commit_id"] for fact in facts}) == len(facts)
        after = {f['commit_id']: f for f in facts}
        assert before.keys() < after.keys()
        assert all(after[key]['prepared'] == fact['prepared']
                   and after[key]['receipts'] == fact['receipts'] for key, fact in before.items())
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)  # worker crash never moved ownership

    records = action_process_records(run2, _chain_child_id(run2))
    assert len(records) == 2, "a new worker allocation was not provisioned for recovery"
    pids = {value['identity']['pid'] for _, value in records.values()}
    assert len(pids) == 2 and os.getpid() not in pids
    from collections import Counter
    calls = Counter(int(line) for line in trace.read_text().splitlines())
    # Pressure cuts can publish before checkpoint_rows. Only the original ACK
    # cursor is an exactly-once boundary; the unacknowledged suffix is replayable.
    assert set(calls) == set(range(10))
    assert [calls[i] for i in range(acknowledged_prefix)] == [1] * acknowledged_prefix
    assert all(calls[i] == 2 for i in range(acknowledged_prefix, 5))
    assert all(calls[i] == 1 for i in range(5, 10))
    assert_released(tmp_path / 'run')


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
    from runtime_v3_owned_crash import assert_released
    assert_released(tmp_path / 'run')


def test_f3_completed_chain_short_circuits_reentry(tmp_path):
    from runtime_v3_owned_crash import action_process_records, process_records, assert_released
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
            before_records = process_records(run.directory)
            raise RuntimeError("later action failed")
    with RunContext.resume(reference) as run2:
        # terminal.complete short-circuits before any worker provisioning or
        # duplicate commit (the restart-after-completion exactly-once gap).
        assert dataset.materialize().take_all() == expected
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        assert all(fact["status"] == "acked" for fact in facts)
        assert process_records(run2.directory) == before_records

    records = action_process_records(run2, _chain_child_id(run2))
    assert len(records) == 1, "completed chain re-entry provisioned a second worker"
    assert_released(tmp_path / 'run')
