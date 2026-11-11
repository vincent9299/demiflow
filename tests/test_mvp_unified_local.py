"""MVP-1 Local unified-Runtime runnable acceptance (S7/S9, fixed-source slice).

The real-person-adjudicated MVP-1 scope: a fixed/iterable source -> map_async ->
materialize -> take readback runs end-to-end through the platform-side unified
Runtime (run_local_chain, one managed worker, single native commit authority),
selected by the workbench entry ``local_execution(unified_chain=...)``. These
tests drive the genuine workbench entry (RunContext + local_execution + bound
public data API), prove unified results equal the legacy in-driver path, prove
take/count read back the unified committed handle, and bind the R4 baseline:
a real worker SIGKILL/resume recovers the acked prefix exactly once, and the
chain seam shares the legacy bind_stages recovery gate whose production
check_recovery fail-closes a persisted unknown paid effect (never auto-replayed).

The default seam stays False here (S7 byte-neutral); S9 flips the workbench
default under controlled scope after the internal-path regression adjudication.
"""
import json
import os
import signal
import sys

import pytest

import demiflow.data as data
from demiflow.execution.run_context import RunContext

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


def _run_pipeline(root, *, unified):
    # Local closure: cloudpickle serializes the map_async callable by value so the
    # independent managed worker need not import this test module (a module-level
    # function would serialize by reference and hang the worker's graph cut).
    def double(row):
        return {**row, "x": row["x"] * 2}

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=unified):
            handle = data.from_items(_items()).map_async(double, concurrency=1).materialize()
            rows = handle.take_all()
    return rows


def _managed_worker_pids(resource_root):
    units = resource_root / "units"
    if not units.exists():
        return set()
    return {json.loads(p.read_text())["identity"]["pid"]
            for p in units.glob("*/*/process.json")}


def test_workbench_unified_real_pipeline_matches_legacy(tmp_path):
    legacy = _run_pipeline(tmp_path / "legacy", unified=False)
    unified = _run_pipeline(tmp_path / "unified", unified=True)
    expected = [{"x": i * 2} for i in range(24)]
    key = lambda r: r["x"]
    assert sorted(legacy, key=key) == expected
    assert sorted(unified, key=key) == expected
    # The unified path supervised a genuine independent managed worker process;
    # the legacy in-driver path provisions no managed worker host.
    unified_pids = _managed_worker_pids(tmp_path / "unified-resources")
    assert unified_pids and os.getpid() not in unified_pids
    assert _managed_worker_pids(tmp_path / "legacy-resources") == set()


def test_workbench_unified_take_and_count_read_back_committed_handle(tmp_path):
    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with RunContext.start(tmp_path / "run", limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
            handle = data.from_items(_items(10)).map_async(double, concurrency=1).materialize()
            assert handle.count() == 10
            # take is bounded sampling: at most 3 committed rows, order not guaranteed.
            sampled = handle.take(3)
            assert len(sampled) <= 3 and all(row in expected for row in sampled)
            assert sorted(handle.take_all(), key=lambda r: r["x"]) == expected
    assert _managed_worker_pids(tmp_path / "run-resources")


def test_unified_readback_is_single_committed_handle_authority(tmp_path):
    # S7 item 1: Local take/count read back the ONE neutral committed-handle
    # authority, not a second readback set. run_local_chain returns
    # _LocalMaterializedHandle(_ChainBlocks(terminal), terminal.rows) where the
    # blocks iterate the driver's committed chain receipts (DriverStreamTerminal
    # .replay_descriptors -> sink.block_entries -> durable _SpilledBlock). This is
    # the exact handle class Ray's _chain_readback delegates to a neutral
    # LocalDatasetExecutor for (ray.py:537-549), so both backends share one
    # readback authority. Assert the structure and that readback is repeatable
    # over committed receipts (no re-execution, no divergent second path).
    from demiflow.data.sources import MaterializedSource
    from demiflow.execution.chain import DriverStreamTerminal, _ChainBlocks
    from demiflow.execution.executors.local import (
        _CheckpointBlocks, _LocalMaterializedHandle, _SpilledBlock,
    )

    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with RunContext.start(tmp_path / "run", limits=LIMITS) as run:
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
            handle = data.from_items(_items(10)).map_async(double, concurrency=1).materialize()
            source = handle._source
            assert isinstance(source, MaterializedSource)
            committed = source.handle
            assert isinstance(committed, _LocalMaterializedHandle)
            # Unified chain reads committed chain receipts, NOT the legacy
            # in-driver checkpoint pages: one authority, distinct block reader.
            assert isinstance(committed.blocks, _ChainBlocks)
            assert not isinstance(committed.blocks, _CheckpointBlocks)
            assert isinstance(committed.blocks.terminal, DriverStreamTerminal)
            assert committed.row_count == source.row_count == 10
            assert committed.recovery_reference["run_id"] == run.ref.run_id
            # count() takes the committed-handle fast path (no re-execution).
            assert handle.count() == committed.row_count == 10
            blocks = list(committed.blocks)
            assert blocks and all(isinstance(b, _SpilledBlock) and b.durable for b in blocks)
            assert all(str(run.directory) in b.path for b in blocks)
            # Repeatable lazy readback over the same committed receipts: two
            # take_all passes are identical, proving no second divergent authority.
            first = sorted(handle.take_all(), key=lambda r: r["x"])
            second = sorted(handle.take_all(), key=lambda r: r["x"])
            assert first == second == expected
    assert _managed_worker_pids(tmp_path / "run-resources")


def test_workbench_unified_chain_kill_recovers_acked_prefix_exactly_once(tmp_path):
    # R4 "recovery correct" + "known_completed reuse" + "state equality", bound onto
    # the actual workbench entry. Same genuine worker SIGKILL after an acked cut as
    # U2/F2, but selected through local_execution(unified_chain=True). The acked
    # prefix cut is handed back to the resumed worker (DriverStreamTerminal
    # .restore_point) and reused unchanged: every pre-crash acked commit_id persists
    # with identical receipts. Exactly-once is the COMMIT authority's (the un-acked
    # suffix is legitimately reprocessed after a worker crash). The "unknown never
    # auto-replayed" half is bound by the recovery-gate test below.
    from demiflow.execution.recovery_contract import RecoveryContractError
    from test_u2_chain_faults import _chain_child_id, _release_crash_orphans
    crash_marker = tmp_path / "crashed-once"

    def double(row):
        if row["x"] == 4 and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(10)]
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=LIMITS) as run:
            reference = run.ref
            with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                      unified_chain=True):
                data.from_items(_items(10)).map_async(double, concurrency=1).materialize().take_all()
    assert crash_marker.exists()
    with RunContext.resume(reference) as run2:
        child = _chain_child_id(run2)
        pre = [f for f in run2.journal.execution_commits(child) if f["status"] == "acked"]
        assert pre, "no acked prefix survived the worker crash"
        pre_by_id = {f["commit_id"]: f for f in pre}
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
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
    records = list((tmp_path / "run-resources" / "units").glob("*/*/process.json"))
    assert len(records) == 2, "a new worker allocation was not provisioned for recovery"
    pids = {json.loads(path.read_text())["identity"]["pid"] for path in records}
    assert len(pids) == 2 and os.getpid() not in pids
    _release_crash_orphans(tmp_path / "run-resources")


def test_unified_chain_shares_legacy_recovery_gate_and_fail_closes_unknown(tmp_path):
    # R4 "unknown paid call never auto-replayed" half, bound onto the unified chain
    # seam without re-proving the frozen journal units. Two facts secure no-regression:
    #   (1) the chain worker (WorkerStreamProgress) and the legacy in-driver terminal
    #       (StreamMaterialization) bind stages through the IDENTICAL StreamStageProgress
    #       method, so flipping unified_chain cannot regress the native_stream_recovery
    #       -> NativePromptRecovery -> check_recovery gate (stream_materialize.py:235);
    #   (2) that production gate (RunPromptJournal.check_recovery, stream_recovery.py:57)
    #       fail-closes a persisted unknown effect on resume rebind, before any dispatch.
    # The journal write uses a synchronous run-level shard for deterministic durability;
    # the async transport/S2 reservation budget is orthogonal to the fail-closed gate,
    # which is invoked here as the exact production method over the durable journal.
    from demiflow.execution.stream_materialize import StreamStageProgress, StreamMaterialization
    from demiflow.execution.worker.progress import WorkerStreamProgress
    from demiflow.operator_llm.journal import UncertainPromptCall
    from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
    from demiflow.operator_llm.stream_recovery import RunPromptJournal

    assert WorkerStreamProgress.bind_stages is StreamStageProgress.bind_stages
    assert StreamMaterialization.bind_stages is StreamStageProgress.bind_stages

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


def test_workbench_default_is_unified_with_explicit_legacy_fallback(tmp_path):
    # S9 controlled default switch: local_execution() now defaults to the unified
    # chain for the workbench Local stream-materialize slice (a genuine managed
    # worker is provisioned). Explicit unified_chain=False remains the byte-unchanged
    # legacy in-driver fallback (no managed worker). Both produce equal rows, so the
    # seam can be rolled back without a code change.
    def double(row):
        return {**row, "x": row["x"] * 2}

    expected = [{"x": i * 2} for i in range(12)]
    with RunContext.start(tmp_path / "default", limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            rows = data.from_items(_items(12)).map_async(double, concurrency=1).materialize().take_all()
    assert sorted(rows, key=lambda r: r["x"]) == expected
    default_pids = _managed_worker_pids(tmp_path / "default-resources")
    assert default_pids and os.getpid() not in default_pids   # default -> unified worker

    with RunContext.start(tmp_path / "fallback", limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=False):
            rows = data.from_items(_items(12)).map_async(double, concurrency=1).materialize().take_all()
    assert sorted(rows, key=lambda r: r["x"]) == expected
    assert _managed_worker_pids(tmp_path / "fallback-resources") == set()  # fallback -> in-driver