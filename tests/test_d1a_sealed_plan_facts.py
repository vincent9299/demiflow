"""D1a: sealed-plan facts persistence (gap⑤ closure).

Additive chain.py compile-seam persistence of the compiled logical_dag/physical
as durable content-addressed artifacts + an immutable, attempt-independent F1
reference via the existing runtime_sources binding (no journal schema change).

Gates V1-V7 + counterexamples N1-N6 per d1_sealed_plan_facts_verdict.md §5.
Test data only under tmp_path (/tmp/demiflow-*); only test-self-built workers.
"""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.execution.recovery_contract import RecoveryContractError, identity, document
from demiflow.execution.run_context import RunContext, RunAction

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")

# Reuse the exact stage2 compile-seam harness (same seam D1a persists inside).
from test_stage2_unified_plan import _map_plan, _topology, LIMITS


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        try:
            yield SimpleNamespace(run=run, action=action)
        finally:
            record = run.journal.execution_action("main-action")
            if record is not None and record["result"] is None:
                run.journal.complete_execution_action(
                    run.fence, "main-action", {"kind": "test-complete"})


def _f1(sealed):
    return sealed.run.journal.source_binding("main-action-sealed-plan")


def _read_ref(run, ref):
    """Post-run read via the run root (=target): sha256 gate + no symlink (§3-A)."""
    assert set(ref) == {"sha256", "path", "bytes"}, "F1 ref must be the 3-key shape"
    path = run.directory / ref["path"]
    assert not any(p.is_symlink() for p in (path, *path.parents))
    raw = path.read_bytes()
    assert len(raw) == ref["bytes"]
    assert hashlib.sha256(raw).hexdigest() == ref["sha256"]
    return json.loads(raw)


def _compile(sealed, plan=None):
    from demiflow.execution.chain import stage_plan_artifact
    if plan is None:
        def f(row):
            return row

        def g(row):
            return {**row, "y": row["x"] + 1}
        plan = _map_plan(f, g)
    plan_ref = stage_plan_artifact(sealed.action, plan)
    return _topology(sealed, plan, plan_ref), plan_ref


def _find_sealed_plan(root):
    """Locate the F1 binding by its attempt-independent key suffix (any caller)."""
    import sqlite3
    with sqlite3.connect(root / "run.sqlite") as db:
        rows = db.execute(
            "SELECT document FROM runtime_sources WHERE action_id LIKE '%-sealed-plan'"
        ).fetchall()
    assert len(rows) == 1, f"expected one sealed-plan binding, got {len(rows)}"
    return json.loads(rows[0][0])


def _assert_carriers_durable(root, f1):
    """Post-run read via the run root (=target): survive + sha + validate (§7.6)."""
    from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan
    for ref, validate in ((f1["logical_dag_ref"], validate_logical_dag),
                          (f1["physical_plan_ref"], validate_physical_execution_plan)):
        path = root / ref["path"]
        assert path.is_file(), "sealed-plan carrier did not survive post-run (gap⑤ broken)"
        assert not any(p.is_symlink() for p in (path, *path.parents))
        raw = path.read_bytes()
        assert len(raw) == ref["bytes"]
        assert hashlib.sha256(raw).hexdigest() == ref["sha256"]
        validate(json.loads(raw))


# --- V1: F1 persisted, binds the execution sealed plan (gap⑤ closure) ---

def test_v1_sealed_plan_facts_persisted_and_binds_execution_plan(sealed):
    from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan
    (topology, logical_dag, physical), _ = _compile(sealed)
    f1 = _f1(sealed)
    assert f1 is not None, "F1 sealed-plan binding absent (gap⑤ not closed)"
    assert f1["schema"] == "demiflow.sealed_plan_facts.v1"
    assert f1["run_id"] == sealed.run.ref.run_id
    assert f1["action_id"] == "main-action"
    assert f1["dag_id"] == logical_dag["dag_id"]
    assert f1["plan_id"] == physical["plan_id"]
    assert f1["artifact_manifest_ref"] == topology["artifact_manifest_ref"]
    # verdict §2 refinements: attempt-independent, bridge is a separate side-car.
    assert "attempt" not in f1
    assert "identity_bridge_ref" not in f1
    assert f1["sealed_at"] == int(sealed.run.journal.recovery_identity()["started_at"] * 1000)
    ld = _read_ref(sealed.run, f1["logical_dag_ref"])
    pp = _read_ref(sealed.run, f1["physical_plan_ref"])
    validate_logical_dag(ld)
    validate_physical_execution_plan(pp)
    assert ld == logical_dag
    assert pp == physical
    assert f1["logical_dag_ref"]["path"].startswith("scratch/chain-sealed-plans/")
    assert f1["physical_plan_ref"]["path"].startswith("scratch/chain-sealed-plans/")


# --- V5: carrier sha == identity(logical_dag) == physical.logical_dag_ref.sha256 ---

def test_v5_carrier_sha_cross_check(sealed):
    (_, logical_dag, physical), _ = _compile(sealed)
    f1 = _f1(sealed)
    assert f1["logical_dag_ref"]["sha256"] == identity(logical_dag)
    assert f1["logical_dag_ref"]["sha256"] == physical["logical_dag_ref"]["sha256"]


# --- V2 (unit): re-compile is idempotent — F1 bytes unchanged, carrier not rewritten ---

def test_v2_recompile_f1_idempotent_no_rewrite(sealed):
    (_, logical_dag, physical), _ = _compile(sealed)
    first = _f1(sealed)
    first_raw = document(first)
    carrier = sealed.run.directory / first["logical_dag_ref"]["path"]
    mtime = carrier.stat().st_mtime_ns
    # Re-compile the same plan (resume re-compiles the same seam).
    _compile(sealed)
    again = _f1(sealed)
    assert document(again) == first_raw, "F1 binding bytes changed on re-compile"
    assert carrier.stat().st_mtime_ns == mtime, "content-addressed carrier was rewritten"


# --- V2 (core F5): cross-attempt kill->resume, F1 binding byte-stable, no false refuse ---

def test_v2_cross_attempt_resume_f1_binding_byte_stable(tmp_path):
    from demiflow.execution.chain import stage_plan_artifact
    root = tmp_path / "run"

    def f(row):
        return row

    def g(row):
        return {**row, "y": row["x"] + 1}
    plan = _map_plan(f, g)
    decl = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
    # Attempt 1: compile (seals F1) then crash -> run finishes failed, resumable.
    with pytest.raises(RuntimeError):
        with RunContext.start(root, limits=LIMITS) as run:
            reference = run.ref
            run.journal.bind_execution_action(run.fence, "main-action", decl)
            action = RunAction(run, "main-action", decl)
            ns = SimpleNamespace(run=run, action=action)
            pr = stage_plan_artifact(action, plan)
            _topology(ns, plan, pr)
            f1_first = run.journal.source_binding("main-action-sealed-plan")
            assert f1_first is not None
            raise RuntimeError("simulated driver crash")
    # Attempt 2: resume re-compiles the SAME plan. bind_source must be idempotent
    # (no 'Fixed source binding differs') and F1 bytes identical -- the core F5
    # invariant: dag_id/plan_id/mref are attempt-independent (verdict §2.6).
    with RunContext.resume(reference) as run2:
        assert run2.attempt > 1
        run2.journal.bind_execution_action(run2.fence, "main-action", decl)
        action2 = RunAction(run2, "main-action", decl)
        ns2 = SimpleNamespace(run=run2, action=action2)
        pr2 = stage_plan_artifact(action2, plan)
        _topology(ns2, plan, pr2)
        f1_again = run2.journal.source_binding("main-action-sealed-plan")
        assert document(f1_again) == document(f1_first), \
            "F1 binding bytes changed across attempt (plan_id stability bug, surface)"
        run2.journal.complete_execution_action(
            run2.fence, "main-action", {"kind": "test-complete"})


# --- N1: tampered carrier -> corrupt refuse on re-compile ---

def test_n1_tampered_carrier_refused(sealed):
    _compile(sealed)
    f1 = _f1(sealed)
    carrier = sealed.run.directory / f1["logical_dag_ref"]["path"]
    carrier.write_bytes(carrier.read_bytes() + b" ")
    with pytest.raises(RecoveryContractError, match="sealed-plan artifact is corrupt"):
        _compile(sealed)


# --- N2: over-budget document -> compile-time refuse (no truncation) ---

def test_n2_over_budget_refused(sealed, monkeypatch):
    import demiflow.execution.chain as chain
    monkeypatch.setattr(chain, "MAX_SEALED_PLAN_BYTES", 16)
    with pytest.raises(RecoveryContractError):
        _compile(sealed)


# --- N3: symlinked carrier namespace -> refuse ---

def test_n3_symlink_carrier_refused(sealed, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    namespace = sealed.run.directory / "scratch" / "chain-sealed-plans"
    namespace.parent.mkdir(parents=True, exist_ok=True)
    namespace.symlink_to(outside, target_is_directory=True)
    try:
        with pytest.raises(RecoveryContractError, match="symlink"):
            _compile(sealed)
    finally:
        # Remove the symlink so fixture teardown's orphan audit does not trip on it
        # (allocate_artifact reserves the path before atomic_bytes refuses the write).
        namespace.unlink()


# --- N5: externally rewritten F1 binding -> fail-closed on re-compile ---

def test_n5_rewritten_binding_refused(sealed):
    import sqlite3
    _compile(sealed)
    # External tamper via a writable connection (the journal reader is readonly),
    # mirroring the stage2 carrier test's direct run.sqlite access.
    with sqlite3.connect(sealed.run.journal.path) as db:
        db.execute("UPDATE runtime_sources SET document=? WHERE action_id=?",
                   (json.dumps({"schema": "bogus"}), "main-action-sealed-plan"))
    with pytest.raises(RecoveryContractError, match="Fixed source binding differs"):
        _compile(sealed)


# --- N6: F1 binds the C execution plan, never a B declared projection ---

def test_n6_binds_execution_plan_not_declared_projection(sealed):
    from demiflow.planning.dag import compile_logical_dag
    (_, logical_dag, _), _ = _compile(sealed)
    f1 = _f1(sealed)
    ld = _read_ref(sealed.run, f1["logical_dag_ref"])
    # The persisted graph is the engine compile_logical_dag product (C space),
    # bound by dag_id; it is not a workbench re-derived declared projection (B).
    assert ld["dag_id"] == logical_dag["dag_id"]
    assert ld["schema_version"] == "demiflow.logical_dag.v1"
    assert f1["dag_id"] == identity({k: v for k, v in ld.items() if k != "dag_id"})


# --- V6: post-run read-only consumption via the run root (=target), gap⑤ audit ---

def test_v6_post_run_readonly_consumption_via_target(tmp_path):
    from demiflow.execution.chain import stage_plan_artifact
    from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan
    root = tmp_path / "run"

    def f(row):
        return row

    def g(row):
        return {**row, "y": row["x"] + 1}
    plan = _map_plan(f, g)
    decl = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
    with RunContext.start(root, limits=LIMITS) as run:
        run.journal.bind_execution_action(run.fence, "main-action", decl)
        action = RunAction(run, "main-action", decl)
        ns = SimpleNamespace(run=run, action=action)
        pr = stage_plan_artifact(action, plan)
        _topology(ns, plan, pr)
        f1 = run.journal.source_binding("main-action-sealed-plan")
        run.journal.complete_execution_action(
            run.fence, "main-action", {"kind": "test-complete"})
    # POST-RUN: the workbench audit path reads via the run root (=target), NOT a
    # live driver context (verdict §3-A). The carriers MUST survive run teardown.
    journal = root / "run.sqlite"
    before = journal.stat().st_mtime_ns
    for ref, validate in ((f1["logical_dag_ref"], validate_logical_dag),
                          (f1["physical_plan_ref"], validate_physical_execution_plan)):
        path = root / ref["path"]
        assert path.is_file(), "sealed-plan carrier did not survive post-run (gap⑤ audit read broken)"
        assert not any(p.is_symlink() for p in (path, *path.parents))
        raw = path.read_bytes()
        assert len(raw) == ref["bytes"]
        assert hashlib.sha256(raw).hexdigest() == ref["sha256"]
        validate(json.loads(raw))
    # Read-side zero write: consuming the carriers + F1 leaves the journal untouched.
    from demiflow.execution.run_journal import RunJournal
    reader = RunJournal(root)
    assert reader.source_binding("main-action-sealed-plan") == f1
    assert journal.stat().st_mtime_ns == before, "post-run read wrote to the journal"


# --- V4 (revised §7.4): workbench scratch_bytes charges the unregistered carrier ---

def test_v4_workbench_scratch_bytes_counts_carrier_and_staging_released(sealed, tmp_path):
    from demiflow.workbench.runs.storage import scratch_bytes
    _compile(sealed)
    f1 = _f1(sealed)
    target = sealed.run.directory
    carriers = [target / f1["logical_dag_ref"]["path"],
                target / f1["physical_plan_ref"]["path"]]
    for path in carriers:
        assert path.is_file()
        # atomic_bytes finally-unlinks its staging on every path (success/failure/
        # cancel/parent-exit, state_artifacts.py:30); none may leak.
        assert not path.with_name(path.name + ".staging").exists(), "staging not released"
    # Carriers are unregistered (mirror manifest), so the workbench filesystem scan
    # -- not journal aggregate admission -- is what charges them (verdict §7.4).
    work = tmp_path / "work"
    work.mkdir()
    total_with = scratch_bytes(target, work, [])
    sizes = [p.stat().st_size for p in carriers]
    for path in carriers:
        path.unlink()
    total_without = scratch_bytes(target, work, [])
    assert total_with - total_without == sum(sizes), \
        "workbench scratch_bytes did not charge the sealed-plan carriers"


# --- V6(b): workbench-attach (external_control) post-run durability (§7.6) ---

def test_v6_workbench_attach_post_run_durability(tmp_path):
    from test_forge_runtime_review_fixes import control_fixture
    from demiflow.execution.chain import stage_plan_artifact
    from demiflow.execution.run_journal import RunJournal
    _, target, journal = control_fixture(tmp_path)
    assert journal.transition({"prepared"}, "starting")
    assert journal.transition({"starting"}, "running")

    def f(row):
        return row

    def g(row):
        return {**row, "y": row["x"] + 1}
    plan = _map_plan(f, g)
    decl = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
    # The workbench adopts the run via attach (external_control). clean_orphans
    # still runs unconditionally at __exit__ (run_context.py:200), so this proves
    # the unregistered carriers survive the P4 actual-consumer lifecycle path too.
    with RunContext.attach(journal) as run:
        run.journal.bind_execution_action(run.fence, "main-action", decl)
        action = RunAction(run, "main-action", decl)
        ns = SimpleNamespace(run=run, action=action)
        pr = stage_plan_artifact(action, plan)
        _topology(ns, plan, pr)
        f1 = run.journal.source_binding("main-action-sealed-plan")
        run.journal.complete_execution_action(
            run.fence, "main-action", {"kind": "test-complete"})
    before = (target / "run.sqlite").stat().st_mtime_ns
    _assert_carriers_durable(target, f1)
    reader = RunJournal(target)
    assert reader.source_binding("main-action-sealed-plan") == f1
    assert (target / "run.sqlite").stat().st_mtime_ns == before, "post-run read wrote to journal"


# --- V3: single seam covers all three callers (local / ray / queue, §7.6) ---

def test_v3_local_caller_seals_and_persists(tmp_path):
    import demiflow.data as data
    root = tmp_path / "run"

    def double(row):
        return {**row, "x": row["x"] * 2}
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
            data.from_items([{"x": i} for i in range(6)]).map_async(
                double, concurrency=1).materialize().take_all()
    f1 = _find_sealed_plan(root)
    assert f1["schema"] == "demiflow.sealed_plan_facts.v1"
    _assert_carriers_durable(root, f1)


def test_v3_queue_caller_seals_and_persists(tmp_path):
    import demiflow.data as data
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.execution.queue import QueueChannel, QueueConfig
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.objects import LocalObjectStore
    from test_queue_v2_boundaries import seed
    from test_raw_fetch import origin_server
    cfg = QueueConfig(backend='sqlite', queue_id='q', path=str(tmp_path / 'q.sqlite'))
    root = tmp_path / "run"

    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    def annotate(status):
        return 'downloaded' if status == 'ok' else 'failed'
    # The default queue path: run_local_queue_chain -> compile_chain_topology
    # (queue_chain.py:209) -> the same seal_plan_facts seam.
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        with RunContext.start(root, limits=LIMITS):
            with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                      unified_queue_chain=True):
                ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1,
                                     idle_timeout_s=.2).fetch(
                    store=LocalObjectStore(root / 'objects'), concurrency=1,
                    policy=FetchPolicy(max_object_bytes=131072,
                                       http=HttpSource(allow_private=True)))
                ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                      .rename_columns({'accepted': 'business_accepted'})
                      .select_columns(['result', 'business_accepted', '_queue_delivery'])
                      .map(receipt_status, inputs={'result': 'result'}, output='status')
                      .map(annotate, inputs={'status': 'status'}, output='label'))
                assert ds.write_datasink(QueueResultSink(cfg, pool='p')) is None
    f1 = _find_sealed_plan(root)
    assert f1["schema"] == "demiflow.sealed_plan_facts.v1"
    _assert_carriers_durable(root, f1)


def test_v3_ray_caller_routes_through_same_seam():
    # ray is not installed in this environment, so end-to-end ray execution is
    # unavailable (honest limitation, not a code defect). The single-seam guarantee
    # is structural: run_ray_chain calls the SAME compile_chain_topology that seals
    # F1 (chain.py:1044), exactly as the executed local/queue callers do.
    import inspect
    import demiflow.execution.chain as chain
    ray_src = inspect.getsource(chain.run_ray_chain)
    local_src = inspect.getsource(chain.run_local_chain)
    assert "compile_chain_topology(" in ray_src
    assert "compile_chain_topology(" in local_src