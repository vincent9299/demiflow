"""Public Local actions, persisted worker outputs and confirmed recovery effects."""
import os
import sys

import pytest
from runtime_v3_owned_crash import kill_owned_worker

import demiflow.data as data
from demiflow.execution.pipeline.run_context import RunContext

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")

LIMITS = {"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _items(n=24):
    return [{"x": i} for i in range(n)]


def _run_pipeline(root):
    # Local closure: cloudpickle serializes the map_async callable by value so the
    # independent managed worker need not import this test module (a module-level
    # function would serialize by reference and hang the worker's graph cut).
    def double(row):
        return {**row, "x": row["x"] * 2}

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, partitions=1):
            handle = data.from_items(_items()).map_async(double, concurrency=1).materialize()
            rows = handle.take_all()
    return rows


def _managed_worker_pids(root):
    from runtime_v3_owned_crash import process_records
    return {record['identity']['pid'] for _, record in process_records(root).values()}


def test_public_map_materialize_runs_in_a_worker(tmp_path):
    root = tmp_path/'work'
    rows = _run_pipeline(root)
    assert sorted(rows, key=lambda row: row['x']) == [{'x': i * 2} for i in range(24)]
    pids = _managed_worker_pids(root)
    assert pids and os.getpid() not in pids


def test_workbench_unified_take_and_count_read_back_committed_handle(tmp_path):
    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with RunContext.start(tmp_path / "run", limits=LIMITS):
        with data.local_execution(workers=1, partitions=1):
            handle = data.from_items(_items(10)).map_async(double, concurrency=1).materialize()
            assert handle.count() == 10
            # take is bounded sampling: at most 3 committed rows, order not guaranteed.
            sampled = handle.take(3)
            assert len(sampled) <= 3 and all(row in expected for row in sampled)
            assert sorted(handle.take_all(), key=lambda r: r["x"]) == expected
    assert _managed_worker_pids(tmp_path / "run")


def test_unified_readback_is_single_committed_handle_authority(tmp_path):
    from demiflow.data.sources import MaterializedSource
    from demiflow.execution.outputs.dataflow_handles import StoredMaterializedHandle

    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with RunContext.start(tmp_path / "run", limits=LIMITS) as run:
        with data.local_execution(workers=1, partitions=1):
            handle = data.from_items(_items(10)).map_async(double, concurrency=1).materialize()
            source = handle._source
            assert isinstance(source, MaterializedSource)
            committed = source.handle
            assert isinstance(committed, StoredMaterializedHandle)
            assert committed.row_count == source.row_count == 10
            receipts = run.journal.execution_commits('action-1')
            assert receipts and all(fact['status'] == 'acked' for fact in receipts)
            assert sorted(committed.rows(), key=lambda row: row["x"]) == expected
            assert run.journal.execution_commits('action-1') == receipts
            first = sorted(handle.take_all(), key=lambda r: r["x"])
            second = sorted(handle.take_all(), key=lambda r: r["x"])
            assert first == second == expected
    from runtime_v3_owned_crash import process_records
    assert process_records(tmp_path / "run")


def test_workbench_unified_chain_kill_recovers_acked_prefix_exactly_once(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import (assert_worker_killed, assert_released,
                                        materialize_action_id, process_records)
    # R4 "recovery correct" + "known_completed reuse" + "state equality", bound onto
    # the actual workbench entry. Same genuine worker SIGKILL after an acked cut as
    # U2/F2, selected through public local_execution(). The acked
    # prefix cut is handed back to the resumed worker (DriverStreamTerminal
    # .restore_point) and reused unchanged: every pre-crash acked commit_id persists
    # with identical receipts. Exactly-once is the COMMIT authority's (the un-acked
    # suffix is legitimately reprocessed after a worker crash). The "unknown never
    # auto-replayed" half is bound by the recovery-gate test below.
    driver_pid = os.getpid()
    crash_marker = tmp_path / "crashed-once"

    def double(row):
        if row["x"] == 4 and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with pytest.raises(DataflowError, match="UNKNOWN_EFFECT"):
        with RunContext.start(tmp_path / "run", limits=LIMITS) as run:
            reference = run.ref
            with data.local_execution(workers=1, worker_mode="process", partitions=1):
                data.from_items(_items(10)).map_async(double, concurrency=1).materialize().take_all()
    assert_worker_killed(crash_marker, tmp_path / "run")
    before_processes = process_records(tmp_path / "run")
    assert_released(tmp_path / "run")
    with RunContext.resume(reference) as run2:
        child = materialize_action_id(run2)
        pre = [f for f in run2.journal.execution_commits(child) if f["status"] == "acked"]
        assert pre, "no acked prefix survived the worker crash"
        pre_by_id = {f["commit_id"]: f for f in pre}
        with data.local_execution(workers=1, worker_mode="process", partitions=1):
            rows = data.from_items(_items(10)).map_async(double, concurrency=1).materialize().take_all()
        assert sorted(rows, key=lambda r: r["x"]) == expected      # state equality: output
        facts = run2.journal.execution_commits(child)
        sequences = [fact["sequence"] for fact in facts]
        assert sequences == list(range(1, len(facts) + 1))         # contiguous, no gap/dup
        assert all(fact["status"] == "acked" for fact in facts)
        assert len({fact["commit_id"] for fact in facts}) == len(facts)
        post_by_id = {f["commit_id"]: f for f in facts if f["status"] == "acked"}
        assert pre_by_id.keys() <= post_by_id.keys()               # known_completed reuse
        for commit_id, before in pre_by_id.items():
            assert post_by_id[commit_id]["receipts"] == before["receipts"]  # state equality: reused cuts
        assert len(post_by_id) > len(pre_by_id)                    # the un-acked suffix was completed
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)                 # crash never moved ownership
    after_processes = process_records(tmp_path / "run")
    assert before_processes.keys() < after_processes.keys()
    old_pids = {v["identity"]["pid"] for _, v in before_processes.values()}
    new_pids = {v["identity"]["pid"] for key, (_, v) in after_processes.items()
                if key not in before_processes}
    assert new_pids and old_pids.isdisjoint(new_pids) and os.getpid() not in new_pids
    assert_released(tmp_path / "run")



def test_paid_call_with_unknown_effect_refuses_new_dispatch_after_restart(tmp_path):
    from demiflow.operator_llm.journal import UncertainPromptCall
    from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
    from demiflow.execution.journal.prompt import RunPromptJournal

    def bind_gate(run):
        # Mirrors bind_stages' native_stream_recovery rebind: bind the run-level durable
        # journal once, then run the production check_recovery gate before any dispatch.
        path = run.directory / "native-effects.sqlite"
        journal = getattr(run, "_native_prompt_journal", None)
        if journal is None:
            journal = run._native_prompt_journal = _SQLiteJournalShard(
                path, max_requests=run.limits["max_requests"])
            RunPromptJournal.check_recovery(journal)
        return journal

    with pytest.raises(RuntimeError, match="fault after unknown effect"):
        with RunContext.start(tmp_path / "run", limits=LIMITS) as run:
            reference = run.ref
            journal = bind_gate(run)
            assert journal.reserve({"kind": "mvp-effect", "x": 0})   # unknown: no response
            raise RuntimeError("fault after unknown effect")
    with RunContext.resume(reference) as run2:
        with pytest.raises(UncertainPromptCall, match="automatic regeneration is forbidden"):
            bind_gate(run2)                          # resume rebind fail-closes; never replayed


def test_public_default_runs_in_managed_workers(tmp_path):
    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(12)]
    with RunContext.start(tmp_path / "default", limits=LIMITS):
        with data.local_execution(workers=1, partitions=1):
            rows = data.from_items(_items(12)).map_async(double, concurrency=1).materialize().take_all()
    assert sorted(rows, key=lambda r: r["x"]) == expected
    default_pids = _managed_worker_pids(tmp_path / "default")
    assert default_pids and os.getpid() not in default_pids   # default -> unified worker
