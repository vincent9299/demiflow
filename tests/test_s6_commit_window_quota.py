"""§6 commit-crash-window quota fix: stable commit-keyed output attribution.

A commit that crashes in the "after prepare / before prepared record" window
(commit.py:71, window-3) leaves a *pending* fact. On resume the driver re-runs
``sink.prepare`` (commit.py:62), which re-reserves the output staging quota. The
three output sinks (ChainResultSink / LanceSink / BlockSink) keyed those
reservations by unstable ids -- a random per-generation staging path for
``row_bytes`` and ``reservation_id=None`` (-> fresh uuid) for ``output_rows`` --
so every recovery charged the SAME logical output again. ``clean_orphans``
unlinks the orphan staging files but never releases ``runtime_reservations`` /
``runtime_budgets``, so repeated crashes accumulate quota until a legitimate
output is permanently uncommittable (real-person priority #3「崩溃恢复不能改变
计算语义或留下无法继续的资源状态」, original intent D-B).

The fix (candidate A, coordinator-approved) keys every output reservation by the
stable ``commit_id`` (begin_commit derives it from run/action/sequence) plus sink
and component, reusing the journal's EXISTING idempotent branch
(run_journal.py:1003-1007): same (name, id) + same amount -> reuse, no re-charge;
a differing amount -> ``Budget reservation changed`` (fail-closed). Zero
journal/commit/schema change. ``row_bytes`` is keyed per block *ordinal* (not
content sha) so two identical blocks in one commit still each count once, while
the commit component keeps cross-commit accumulation intact.

Gates pinned here (coordinator adjudication §3):
  1  window-3 crash -> recovery: identical budgets + acked + identical bytes
     (DriverStreamTerminal/ChainResultSink, and BlockSink/LanceSink direct)
  2  same logical cut crashed >=24 rounds: quota constant, never exhausted
  3  window-5 (after_prepared) recovery: prepare skipped, no re-charge (regression)
  4  model-request reservation + unknown paid effect preserved (not refunded/replayed)
  5  orphan staging blocks still recycled by clean_orphans; committed blocks survive
  7  BlockSink row_bytes deferral: streaming per-row/output_rows/scratch limits still
     reject; aggregate output_BYTES rejects at prepare, pre-publish (fail-closed)
  8  ordinal: identical blocks in one commit charged twice; cross-commit same bytes
     accumulate; non-deterministic re-prepare fail-closed (Budget reservation changed)
  9  real managed-worker SIGKILL -> resume: same quota + acked + per-record equality

Gates 6 (full regression / exact files / main / load-bearing bytes) is a delivery
activity. In-process tests inject the real commit.py fault seam and re-stage with a
fresh sink generation to simulate worker re-production; only the captured logical
output is repeated. No paid model, no network. Tests write only under pytest tmp_path.
"""
from contextlib import contextmanager
import sys

import pytest

import demiflow.data as data
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution import commit as commit_module
from demiflow.execution.outputs.block_sink import BlockSink
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.outputs.lance_sink import LanceSink
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, SourcePosition, identity
from demiflow.execution.pipeline.run_context import RunContext, RunAction
from demiflow.execution.run_journal import RunJournal
from demiflow.lance.model import LanceWriteSpec

from test_stage2_unified_plan import LIMITS, _crash_hook

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _budgets(run):
    with run.journal._connection() as db:
        return {name: (consumed, ceiling) for name, consumed, ceiling in
                db.execute("SELECT name, consumed, ceiling FROM runtime_budgets")}


def _reservations(run, name):
    with run.journal._connection() as db:
        return db.execute("SELECT reservation_id, amount FROM runtime_reservations "
                          "WHERE name=? ORDER BY reservation_id", (name,)).fetchall()


@contextmanager
def _coord_env(tmp_path, limits=None):
    """A bare action + CommitCoordinator for direct BlockSink/LanceSink commits."""
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        declaration = {"kind": "s6-commit", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        try:
            yield run, action
        finally:
            record = run.journal.execution_action("main-action")
            if record is not None and record["result"] is None:
                run.journal.complete_execution_action(
                    run.fence, "main-action", {"kind": "test-complete"})


def _make_sink(kind, action, tmp_path, rows):
    if kind == "block":
        return BlockSink(action, "output"), rows
    if kind == "lance":
        import pyarrow as pa
        table = pa.table({"x": pa.array([r["x"] for r in rows], type=pa.int64())})
        spec = LanceWriteSpec(uri=str(tmp_path / "t.lance"), mode="create", schema=table.schema)
        return LanceSink(action, spec), [table]
    raise ValueError(kind)


def _window3(name, commit_id):
    if name == "after_prepare_before_record":
        raise RuntimeError("window-3 crash before prepared record")


# --- Gate 1: window-3 recovery identical quota (BlockSink / LanceSink direct) ---

@pytest.mark.parametrize("kind", ["block", "lance"])
def test_s6_gate1_window3_sink_recovery_same_quota(tmp_path, monkeypatch, kind):
    rows = [{"x": i} for i in range(4)]
    with _coord_env(tmp_path) as (run, action):
        coord = CommitCoordinator(action)
        position = SourcePosition(identity(["src"]), 4)
        sink, blocks = _make_sink(kind, action, tmp_path, rows)
        monkeypatch.setattr(commit_module, "fault_point", _window3)
        with pytest.raises(RuntimeError, match="window-3"):
            coord.commit(1, outputs={sink.sink_id: (sink, blocks)}, position=position)
        before = _budgets(run)
        assert run.journal.execution_commits(action.action_id)[0]["status"] == "pending"
        monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
        sink2, blocks2 = _make_sink(kind, action, tmp_path, rows)
        fact = coord.commit(1, outputs={sink2.sink_id: (sink2, blocks2)}, position=position)
        assert _budgets(run) == before, "window-3 recovery doubled the output quota"
        assert fact["status"] == "acked"


def test_s6_gate2_repeat_crash_quota_constant(tmp_path, monkeypatch):
    rows = [{"x": i} for i in range(4)]
    limits = {"max_output_rows": 64, "max_output_bytes": 4096,
              "max_row_bytes": 512, "max_batch_bytes": 1024}
    with _coord_env(tmp_path, limits) as (run, action):
        position = SourcePosition(identity(["s6-source"]), len(rows))
        monkeypatch.setattr(commit_module, "fault_point", _window3)
        previous = None
        for _ in range(24):
            sink = BlockSink(action, "output")
            with pytest.raises(RuntimeError, match="window-3"):
                CommitCoordinator(action).commit(1, outputs={"output": (sink, rows)}, position=position)
            current = _budgets(run)
            if previous is not None:
                assert current == previous
            previous = current
        monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
        sink = BlockSink(action, "output")
        fact = CommitCoordinator(action).commit(1, outputs={"output": (sink, rows)}, position=position)
        assert _budgets(run) == previous
        assert fact["status"] == "acked"
        assert list(sink.iter_rows(fact["prepared"]["sinks"]["output"])) == rows


def test_s6_gate3_window5_recovery_no_recharge(tmp_path, monkeypatch):
    rows = [{"x": i} for i in range(4)]
    with _coord_env(tmp_path) as (run, action):
        def crash(name, commit_id):
            if name == "after_prepared":
                raise RuntimeError("window-5 crash after prepared")
        monkeypatch.setattr(commit_module, "fault_point", crash)
        sink = BlockSink(action, "output")
        with pytest.raises(RuntimeError, match="window-5"):
            CommitCoordinator(action).commit(1, outputs={"output": (sink, rows)},
                position=SourcePosition(identity(["s6-source"]), len(rows)))
        before = _budgets(run)
        original, = run.journal.execution_commits(action.action_id)
        assert original["status"] == "prepared"
        monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
        sink = BlockSink(action, "output")
        recovered, = CommitCoordinator(action).recover({"output": sink})
        assert _budgets(run) == before
        assert recovered["status"] == "acked"
        assert recovered["prepared"] == original["prepared"]
        assert list(sink.iter_rows(recovered["prepared"]["sinks"]["output"])) == rows


def test_s6_gate4_paid_effect_budget_preserved_during_reprepare(tmp_path, monkeypatch):
    with _coord_env(tmp_path) as (run, action):
        run.reserve("model_requests", 1, reservation_id=identity(["model-request", "paid-1"]), ceiling=100)
        position = SourcePosition(identity(["s6-source"]), 1)
        rows = [{"id": "paid-1"}]
        monkeypatch.setattr(commit_module, "fault_point", _window3)
        with pytest.raises(RuntimeError, match="window-3"):
            CommitCoordinator(action).commit(1, outputs={"output": (BlockSink(action, "output"), rows)},
                                             position=position)
        before = _budgets(run)["model_requests"]
        monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
        fact = CommitCoordinator(action).commit(1, outputs={"output": (BlockSink(action, "output"), rows)},
                                               position=position)
        assert fact["status"] == "acked"
        assert _budgets(run)["model_requests"] == before == (1, 100)


def test_s6_gate5_orphan_blocks_recycled(tmp_path, monkeypatch):
    rows = [{"x": i} for i in range(4)]
    with _coord_env(tmp_path) as (run, action):
        sink = BlockSink(action, "output")
        position = SourcePosition(identity(["s6-source"]), len(rows))
        monkeypatch.setattr(commit_module, "fault_point", _window3)
        with pytest.raises(RuntimeError, match="window-3"):
            CommitCoordinator(action).commit(1, outputs={"output": (sink, rows)}, position=position)
        orphan_paths = [run.directory / block["path"] for block in sink.block_entries(sink.descriptor())]
        monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
        sink = BlockSink(action, "output")
        fact = CommitCoordinator(action).commit(1, outputs={"output": (sink, rows)}, position=position)
        durable_paths = [run.directory / block["path"] for block in sink.block_entries(fact["prepared"]["sinks"]["output"])]
        assert orphan_paths and durable_paths and set(orphan_paths).isdisjoint(durable_paths)
        assert run.journal.clean_orphans(run.fence) >= len(orphan_paths)
        assert not any(path.exists() for path in orphan_paths)
        assert all(path.is_file() for path in durable_paths)


# --- Gate 7: BlockSink streaming limits still reject after row_bytes deferral ---

@pytest.mark.parametrize("limit", ["max_row_bytes", "max_output_rows", "max_scratch_bytes"])
def test_s6_gate7_streaming_limits_still_reject(tmp_path, limit):
    limits = {"max_row_bytes": 512, "max_batch_bytes": 1024, "max_output_rows": 8,
              "max_output_bytes": 1 << 30, "max_scratch_bytes": 1 << 30}
    if limit == "max_row_bytes":
        limits.update(max_row_bytes=64, max_batch_bytes=128)
    elif limit == "max_scratch_bytes":
        limits.update(max_scratch_bytes=256)
    with _coord_env(tmp_path, limits) as (run, action):
        sink = BlockSink(action, "output")
        with pytest.raises((RecoveryContractError, RuntimeContractError)):
            if limit == "max_row_bytes":
                sink.append({"x": "y" * 4096})
            elif limit == "max_output_rows":
                for i in range(100):
                    sink.append({"x": i})
            else:
                for i in range(100):
                    sink.append({"x": "z" * 200})


def test_s6_gate7_output_bytes_deferred_to_prepare(tmp_path):
    # Aggregate output_BYTES admission moves from streaming _flush to prepare, but
    # stays fail-closed and pre-publish. Streaming append/flush must SUCCEED here
    # (total bytes > max_output_bytes, each row < max_row_bytes, scratch large).
    limits = {"max_row_bytes": 512, "max_batch_bytes": 1024, "max_output_rows": 1000,
              "max_output_bytes": 256, "max_scratch_bytes": 1 << 30}
    with _coord_env(tmp_path, limits) as (run, action):
        sink = BlockSink(action, "output")
        for i in range(20):
            sink.append({"x": "z" * 100})
        sink._flush()
        context = CommitCoordinator(action)._context(identity(["s6-defer"]))
        with pytest.raises(RecoveryContractError, match="exhausted"):
            sink.prepare(None, context)
        assert not sink._marker(context).exists(), "over-budget output must not publish"


# --- Gate 8: ordinal semantics + cross-commit accumulation + fail-closed ---

def test_s6_gate8_ordinal_identical_blocks_charged_twice(tmp_path):
    with _coord_env(tmp_path) as (run, action):
        commit_id = identity(["s6-ordinal"])
        context = CommitCoordinator(action)._context(commit_id)
        sink = BlockSink(action, "output")
        # Two identical-content blocks (explicit flush each), distinguished only by
        # ordinal -- a content-sha key would collapse them and under-count.
        sink.append({"x": "a" * 40})
        sink._flush()
        sink.append({"x": "a" * 40})
        sink._flush()
        sink.prepare(None, context)
        rows_res = _reservations(run, "row_bytes")
        assert len(rows_res) == 2, "identical blocks in one commit must each count once"
        assert len({rid for rid, _ in rows_res}) == 2, "ordinal must distinguish identical blocks"
        assert sum(amount for _, amount in rows_res) == _budgets(run)["row_bytes"][0]
        # A fresh sink re-preparing the SAME commit reuses the same ordinal keys
        # (commit-keyed, not path-keyed): no new reservation and no re-charge.
        before = _budgets(run)
        sink2 = BlockSink(action, "output")
        sink2.append({"x": "a" * 40})
        sink2._flush()
        sink2.append({"x": "a" * 40})
        sink2._flush()
        sink2.prepare(None, context)
        assert _reservations(run, "row_bytes") == rows_res
        assert _budgets(run) == before


def test_s6_gate8_cross_commit_same_bytes_accumulate(tmp_path):
    rows = [{"x": 1}, {"x": 2}]
    with _coord_env(tmp_path) as (run, action):
        coord = CommitCoordinator(action)
        src = identity(["src"])
        sink = BlockSink(action, "output")
        coord.commit(1, outputs={"output": (sink, rows)}, position=SourcePosition(src, 2))
        single = _budgets(run)["row_bytes"][0]
        sink2 = BlockSink(action, "output")
        coord.commit(2, outputs={"output": (sink2, rows)}, position=SourcePosition(src, 4))
        # The commit component keeps distinct commits accumulating (no cross-commit collision).
        assert _budgets(run)["row_bytes"][0] == 2 * single
        assert _budgets(run)["output_rows"][0] == 4


def test_s6_gate8_nondeterministic_reprepare_fail_closed(tmp_path):
    with _coord_env(tmp_path) as (run, action):
        context = CommitCoordinator(action)._context(identity(["s6-nondet"]))
        sink = BlockSink(action, "output")
        sink.prepare([{"x": 1}, {"x": 2}], context)
        # Same commit, a DIFFERENT amount (non-deterministic re-production) must
        # fail-closed against the stable key, never silently double-charge.
        sink2 = BlockSink(action, "output")
        with pytest.raises(RecoveryContractError, match="Budget reservation changed"):
            sink2.prepare([{"x": 1}, {"x": 2}, {"x": 3}], context)


# --- Gate 10 (#42): value-canonical row encoding -> re-prepare byte identity ---

def test_s6_gate10_equal_value_diff_identity_same_bytes_sha(tmp_path):
    import pickle
    from demiflow.execution.outputs.block_sink import _canonical_row
    rid = (b"r" * 64).decode()
    live = {"request_id": rid, "ref": {"request_id": rid}}
    rid_a, rid_b = (b"r" * 64).decode(), (b"r" * 64).decode()
    reuse = {"request_id": rid_a, "ref": {"request_id": rid_b}}
    assert live == reuse and rid_a is not rid_b
    # Live shares one request_id object; reuse has distinct equal objects. Raw
    # pickle drifts (the #42 bug); value-canonical encoding does not.
    assert pickle.dumps(live, protocol=4) != pickle.dumps(reuse, protocol=4)
    assert pickle.dumps(_canonical_row(live), protocol=4) == pickle.dumps(_canonical_row(reuse), protocol=4)
    with _coord_env(tmp_path) as (run, action):
        s1, s2 = BlockSink(action, "output"), BlockSink(action, "output")
        s1.append(live); s1._flush()
        s2.append(reuse); s2._flush()
        d1, d2 = s1.descriptor(), s2.descriptor()
        b1, b2 = list(s1.block_entries(d1))[0], list(s2.block_entries(d2))[0]
        assert b1["bytes"] == b2["bytes"]
        assert b1["sha256"] == b2["sha256"]
        assert list(s1.iter_rows(d1)) == [live]
        assert list(s2.iter_rows(d2)) == [reuse]


def test_s6_gate10_canonical_reprepare_control_no_recharge(tmp_path):
    # Same commit, equal-value row with an independent request_id object and reused
    # flipped (mirrors live->reuse): must reuse the reservation, never raise.
    with _coord_env(tmp_path) as (run, action):
        context = CommitCoordinator(action)._context(identity(["s6-canon-control"]))
        rid = (b"r" * 64).decode()
        live = {"request_id": rid, "ref": {"request_id": rid}, "reused": False}
        BlockSink(action, "output").prepare([live], context)
        before = _reservations(run, "row_bytes")
        rid_a, rid_b = (b"r" * 64).decode(), (b"r" * 64).decode()
        reuse = {"request_id": rid_a, "ref": {"request_id": rid_b}, "reused": True}
        BlockSink(action, "output").prepare([reuse], context)
        assert _reservations(run, "row_bytes") == before


def test_s6_gate10_canonical_changed_value_fail_closed(tmp_path):
    with _coord_env(tmp_path) as (run, action):
        context = CommitCoordinator(action)._context(identity(["s6-canon-change"]))
        BlockSink(action, "output").prepare(
            [{"instruction": "draw a cat", "request_id": "x" * 64}], context)
        with pytest.raises(RecoveryContractError, match="Budget reservation changed"):
            BlockSink(action, "output").prepare(
                [{"instruction": "draw a longer cat description", "request_id": "x" * 64}], context)


def test_s6_gate10_max_row_bytes_boundary(tmp_path):
    import pickle
    from demiflow.execution.outputs.block_sink import _canonical_row
    from demiflow.data.runtime_contract import check_size
    fit = {"x": "a" * 40}
    over = {"x": "a" * 41}
    # canonical never inflates vs raw for the same value.
    for row in (fit, over):
        assert len(pickle.dumps(_canonical_row(row), protocol=4)) <= len(pickle.dumps(row, protocol=4))
    budget = check_size(fit, 1 << 30)
    limits = {"max_row_bytes": budget, "max_batch_bytes": 1 << 20, "max_output_rows": 100,
              "max_output_bytes": 1 << 30, "max_scratch_bytes": 1 << 30}
    with _coord_env(tmp_path, limits) as (run, action):
        sink = BlockSink(action, "output")
        sink.append(fit); sink._flush()
        assert sink.descriptor()["rows"] == 1
    # For check_size-valid rows the getsizeof value >= pickle length, so :53 is the
    # binding budget; a slightly-over row is still refused fail-closed.
    with _coord_env(tmp_path / "over", limits) as (run, action):
        with pytest.raises((RecoveryContractError, RuntimeContractError)):
            BlockSink(action, "output").append(over)


def test_s6_gate10_encoded_backstop_rejects(tmp_path, monkeypatch):
    import pickle
    import demiflow.execution.outputs.block_sink as bs
    from demiflow.execution.outputs.block_sink import _canonical_row
    # Isolate the :55 encoded-length backstop (check_size :53 normally binds first)
    # to prove canonical encoding keeps it fail-closed. Production check_size unchanged.
    monkeypatch.setattr(bs, "check_size", lambda *a, **k: 0)
    fit, over = {"x": "a" * 10}, {"x": "a" * 4096}
    budget = len(pickle.dumps(_canonical_row(fit), protocol=4))
    assert len(pickle.dumps(_canonical_row(over), protocol=4)) > budget
    limits = {"max_row_bytes": budget, "max_batch_bytes": 1 << 20, "max_output_rows": 100,
              "max_output_bytes": 1 << 30, "max_scratch_bytes": 1 << 30}
    with _coord_env(tmp_path, limits) as (run, action):
        sink = BlockSink(action, "output")
        sink.append(fit); sink._flush()
        assert sink.descriptor()["rows"] == 1
    with _coord_env(tmp_path / "over", limits) as (run, action):
        with pytest.raises(RecoveryContractError, match="Encoded row exceeds byte budget"):
            BlockSink(action, "output").append(over)


def test_s6_gate10_depth_fallback_raw_writes_reads(tmp_path, monkeypatch):
    import demiflow.execution.outputs.block_sink as bs
    # check_size caps valid rows at depth 16; lower the canon bound so a valid-depth
    # row exercises the value-determined depth fallback end-to-end through the sink.
    monkeypatch.setattr(bs, "_CANON_MAX_DEPTH", 3)
    row = {"a": {"b": {"c": {"d": {"e": 1}}}}}
    assert bs._canonical_row(row) is row
    with _coord_env(tmp_path) as (run, action):
        sink = BlockSink(action, "output")
        sink.append(row); sink._flush()
        assert list(sink.iter_rows(sink.descriptor())) == [row]


def test_s6_gate10_deep_fallback_raw_roundtrip():
    import pickle
    from demiflow.execution.outputs.block_sink import _canonical_row, _CANON_MAX_DEPTH
    deep = cur = {}
    for _ in range(_CANON_MAX_DEPTH + 5):
        cur["n"] = {}
        cur = cur["n"]
    assert _canonical_row(deep) is deep
    assert pickle.loads(pickle.dumps(_canonical_row(deep), protocol=4)) == deep


def test_s6_gate10_cycle_fallback_raw_roundtrip():
    import pickle
    from demiflow.execution.outputs.block_sink import _canonical_row
    cyc = {}
    cyc["self"] = cyc
    assert _canonical_row(cyc) is cyc
    loaded = pickle.loads(pickle.dumps(_canonical_row(cyc), protocol=4))
    assert loaded["self"] is loaded


# --- Gate 9: real managed-worker recovery preserves output quota + bytes ---

def _output_budgets(directory):
    journal = RunJournal(directory)
    with journal._connection() as db:
        return {name: consumed for name, consumed in db.execute(
            "SELECT name, consumed FROM runtime_budgets WHERE name IN "
            "('output_rows','row_bytes','output_bytes','output_admission_bytes')")}


def _clean_baseline(tmp_path, rows_in, transform):
    clean_root = tmp_path / "clean"
    with RunContext.start(clean_root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            rows = (data.from_items(rows_in).map_async(transform, concurrency=1)
                    .materialize().take_all())
    return clean_root, {r["input"]: r["value"] for r in rows}


def test_s6_gate9_real_worker_sigkill_recovery_same_quota(tmp_path, monkeypatch):
    from test_u2_chain_faults import _release_crash_orphans
    rows_in = [{"x": i} for i in range(8)]

    def transform(row):
        return {"input": row["x"], "value": row["x"] + 1}

    clean_root, clean_values = _clean_baseline(tmp_path, rows_in, transform)
    clean_budget = _output_budgets(clean_root)
    hook, marker = _crash_hook(tmp_path)

    def pipeline():
        return (data.from_items(rows_in).map_async(transform, concurrency=1)
                .map_async(hook, concurrency=1))

    root = tmp_path / "run"
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(root, limits=LIMITS) as run:
            reference = run.ref
            with data.local_execution(workers=1, worker_mode="thread", partitions=1):
                pipeline().materialize().take_all()
    assert marker.exists(), "first run must SIGKILL the worker after acked cuts"
    with RunContext.resume(reference):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            resumed = pipeline().materialize().take_all()
    _release_crash_orphans(tmp_path / "run-resources")
    assert {r["input"]: r["value"] for r in resumed} == clean_values
    assert _output_budgets(root) == clean_budget, "real-worker SIGKILL recovery doubled output quota"
    assert RunJournal(root).execution_status()["status"] == "succeeded"


def test_s6_gate9_real_worker_window3_recovery_same_quota(tmp_path, monkeypatch):
    # A real managed worker stages and sends the first cut; the DRIVER crashes in
    # window-3 (after prepare, before the prepared record), leaving a pending commit.
    # Resume re-produces the same logical cut and re-prepares the pending commit:
    # commit-keyed attribution must reuse the charge, not double it.
    from test_u2_chain_faults import _release_crash_orphans
    rows_in = [{"x": i} for i in range(8)]

    def transform(row):
        return {"input": row["x"], "value": row["x"] + 1}

    clean_root, clean_values = _clean_baseline(tmp_path, rows_in, transform)
    clean_budget = _output_budgets(clean_root)
    crashed = [False]
    prepare_calls = [0]

    def fault(name, commit_id):
        if name == "after_prepare_before_record":
            prepare_calls[0] += 1
            # The first window-3 is the fixed-source capture (input_capture, no
            # output quota); the second is the first real chain output cut, which
            # is the commit whose staging quota §6 must keep idempotent.
            if prepare_calls[0] == 2 and not crashed[0]:
                crashed[0] = True
                raise OSError("driver window-3 crash before prepared record")

    monkeypatch.setattr(commit_module, "fault_point", fault)
    root = tmp_path / "run"
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(root, limits=LIMITS) as run:
            reference = run.ref
            with data.local_execution(workers=1, worker_mode="thread", partitions=1):
                (data.from_items(rows_in).map_async(transform, concurrency=1)
                 .materialize().take_all())
    assert crashed[0], "driver must crash in window-3 with a real worker running"
    assert any(c["status"] == "pending"
               for c in RunJournal(root).execution_commits("action-1-chain")), \
        "window-3 must leave a pending commit to re-prepare on resume"
    monkeypatch.setattr(commit_module, "fault_point", lambda *_: None)
    with RunContext.resume(reference):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            resumed = (data.from_items(rows_in).map_async(transform, concurrency=1)
                       .materialize().take_all())
    _release_crash_orphans(tmp_path / "run-resources")
    assert {r["input"]: r["value"] for r in resumed} == clean_values
    assert _output_budgets(root) == clean_budget, "window-3 re-prepare doubled output quota"
    assert RunJournal(root).execution_status()["status"] == "succeeded"