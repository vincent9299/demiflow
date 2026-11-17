"""Graph code-pin regression lock (gap④, real-person priority #2 on the graph path).

The chain/queue carriers seal a code-sensitive code-pin and gate the worker load
(``verify_loaded_plan``) plus the driver resume (``stage_plan_artifact`` bind_source)
so a same-Run resume can never mix business-code versions. The graph carrier rode
``stage_plan_artifact`` (code-pin-aware) on the seal side but its worker
(``GraphWorkerExecutor._load_plan``) only checked the pickle sha256 and never called
``verify_loaded_plan``, and the graph spec carried no ``sealed_operators`` — so an
edited by-reference business UDF could load on a graph worker, and a graph resume
had no structural operator binding. Option A wires the graph through the SAME shared
helpers: the driver seals ``_sealed_operators(plan)`` per worker (mirroring
``verify_loaded_plan``'s recompute exactly) and the worker load calls
``verify_loaded_plan(plan, plan_ref, sealed_operators)``.

These tests lock that wiring with GENUINE counterexamples, not self-assertion:
  * worker load gate: unedited graph plan passes (no false reject — the seal formula
    mirrors the recompute), edited captured business helper -> "code-pin differs";
  * driver resume seam: re-staging the same graph action with edited business code
    fail-closes against the sealed binding before any worker launches;
  * end-to-end real Local graph workers, default memory_incremental, by-reference
    helper re-imported on the worker: unedited run preserves rows (no false reject);
  * end-to-end crash -> edit helper -> resume: the graph resume refuses with an
    identifiable RecoveryContractError (never a TimeoutError, never mixed rows).

Env: standard suite invocation (cwd=repo, PYTHONPATH=repo:repo/tests) so spawned
worker hosts inherit the engine code AND the business-helper dir (``_load_modules``
prepends it to PYTHONPATH). The by-value closures cross to the worker by value; the
captured business helper crosses BY NAME (re-imported). No paid model, no network.
Tests write only under pytest tmp_path and reclaim only their own graph workers.
"""
import importlib
import os
import signal
import sys
import uuid
from types import SimpleNamespace

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.graph import run_local_graph
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunAction, RunContext

from test_stage2_unified_plan import _load_modules
from test_u3_graph_faults import _committed_rows, _release_crash_orphans

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


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
        declaration = {"kind": "graph-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        try:
            yield SimpleNamespace(run=run, action=action)
        finally:
            record = run.journal.execution_action("main-action")
            if record is not None and record["result"] is None:
                run.journal.complete_execution_action(
                    run.fence, "main-action", {"kind": "test-complete"})


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                              stream_options=MapStreamOptions(concurrency=1)),))


def _helper(tmp_path, monkeypatch, prefix):
    """One importable business helper module: ``delta() -> 1`` (by name on the worker)."""
    helper = prefix + "_helper_" + uuid.uuid4().hex[:8]
    sources = {helper: "def delta():\n    return 1\n"}
    module, paths = _load_modules(tmp_path, monkeypatch, sources, helper)
    return module, paths, helper


def _graph_spec(ancestor_fn, target_fns, *, n, cut_rows, mode, source_id, fixture):
    items = tuple({"x": i} for i in range(n))
    readers = list(target_fns)
    targets = [{"plan": _stream_map(target_fns[rid]), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    return {"source": ItemsSource(items), "source_id": source_id,
            "ancestor": {"plan": _stream_map(ancestor_fn),
                         "logical_node_id": "shared", "occurrence_id": "shared-p0"},
            "targets": targets, "routing": "broadcast", "exchange_mode": mode,
            "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
            "queue_depth": 4096, "cut_rows": cut_rows}, readers


# --- worker load gate: graph seal mirrors recompute; edited business code refuses ---

def test_graph_worker_load_gate_refuses_edited_code(sealed, tmp_path, monkeypatch):
    from demiflow.execution.chain import (
        _business_dependency_manifest, stage_plan_artifact, verify_loaded_plan)
    from demiflow.execution.graph import _sealed_operators
    from demiflow.planning.dag import _operation_semantics
    helper_mod, paths, helper = _helper(tmp_path, monkeypatch, "gcpload")

    def transform(row):
        return {**row, "x": row["x"] + helper_mod.delta()}

    plan = _stream_map(transform)
    assert helper in _business_dependency_manifest(plan), "captured helper must be pinned"
    plan_ref = stage_plan_artifact(sealed.action, plan)
    sealed_ops = _sealed_operators(plan)
    # The graph seal formula must equal verify_loaded_plan's recompute, else every
    # graph worker would falsely reject its own sealed plan. The unedited pass below
    # is the genuine proof (it runs the actual recompute inside verify_loaded_plan).
    recomputed = [{"logical_node_id": identity([_operation_semantics(op)[0], i])}
                  for i, op in enumerate(plan.operations)]
    assert sealed_ops == recomputed
    verify_loaded_plan(plan, plan_ref, sealed_ops)        # unedited: no false reject
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_stream_map(transform), plan_ref, sealed_ops)


# --- driver resume seam: re-staging edited graph code fail-closes before launch ---

def test_graph_driver_resume_seam_refuses_edited_code(sealed, tmp_path, monkeypatch):
    import cloudpickle
    import hashlib
    from demiflow.execution.chain import stage_plan_artifact
    helper_mod, paths, helper = _helper(tmp_path, monkeypatch, "gcpresume")

    def transform(row):
        return {**row, "x": row["x"] + helper_mod.delta()}

    plan = _stream_map(transform)
    first = stage_plan_artifact(sealed.action, plan)
    again = stage_plan_artifact(sealed.action, plan)
    assert again["code_pin_digest"] == first["code_pin_digest"]
    # Edit ONLY the captured helper; the by-reference closure pickle bytes are unchanged,
    # so only the code-sensitive code-pin (not the sha256) can catch it.
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    edited = _stream_map(transform)
    assert hashlib.sha256(cloudpickle.dumps(edited)).hexdigest() == first["sha256"]
    with pytest.raises(RecoveryContractError,
                       match="Business code changed since the plan was sealed"):
        stage_plan_artifact(sealed.action, edited)


# --- end-to-end: real Local graph workers, by-reference helper, unedited succeeds ---

def test_graph_end_to_end_by_reference_udf_succeeds(tmp_path, monkeypatch):
    helper_mod, paths, helper = _helper(tmp_path, monkeypatch, "gcpclean")

    def emit(row):
        return {**row, "x": row["x"] + helper_mod.delta()}

    def passthrough(row):
        return row

    n = 8
    spec, readers = _graph_spec(emit, {"c0": passthrough, "c1": passthrough},
                                n=n, cut_rows=4, mode="memory_incremental",
                                source_id="graph-codepin-source", fixture="graph-codepin-clean")
    limits = {"checkpoint_rows": 4, "checkpoint_unconfirmed_rows": 4,
              "checkpoint_interval_ms": 60000}
    run_dir = tmp_path / "run"
    try:
        with RunContext.start(run_dir, limits=limits) as run:
            with run.action({"kind": "materialize",
                             "source": {"fixture": "graph-codepin-clean"}}) as execution:
                action_id = execution.action_id
                result = run_local_graph(spec, control_timeout_s=15)
                execution.complete(dict(kind="graph-materialized"))
            facts = run.journal.execution_commits(action_id)
            for rid in readers:
                rows = _committed_rows(run, action_id, rid, facts)
                assert [r["x"] for r in rows] == [i + 1 for i in range(n)], rid
        assert result["egress_rows"] == n
    finally:
        _release_crash_orphans(run_dir.with_name(run_dir.name + "-resources"))


# --- end-to-end: crash -> edit helper -> resume refuses (same-Run code mixing) ---

def test_graph_crash_edit_resume_refuses(tmp_path, monkeypatch):
    from runtime_v3_owned_crash import kill_owned_worker, assert_worker_killed, process_records
    from runtime_v3_graph_history import capture_acks
    driver_pid = os.getpid()
    helper_mod, paths, helper = _helper(tmp_path, monkeypatch, "gcpcrash")
    crash_marker = tmp_path / "target-crashed-once"

    def emit(row):
        return {**row, "x": row["x"] + helper_mod.delta()}

    def crashing_target(row):
        # Crash exactly one target once, on the first POST-cut row (x==9 after cut_rows=8).
        if row["x"] == 9:
            try:
                kill_owned_worker(crash_marker, driver_pid)
            except FileExistsError:
                return row
        return row

    def passthrough(row):
        return row

    n = 24
    spec, readers = _graph_spec(emit, {"c0": crashing_target, "c1": passthrough},
                                n=n, cut_rows=8, mode="durable_incremental",
                                source_id="graph-codepin-source", fixture="graph-codepin-crash")
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    root = tmp_path / "run"
    try:
        with pytest.raises((RecoveryContractError, OSError, DataflowError)) as failure:
            with RunContext.start(root, limits=limits) as run:
                reference = run.ref
                with run.action({"kind": "materialize",
                                 "source": {"fixture": "graph-codepin-crash"}}) as execution:
                    executor.materialize_graph(spec)
                    execution.complete(dict(kind="graph-materialized"))
        from runtime_v3_owned_crash import assert_worker_loss
        assert_worker_loss(failure.value)
        assert_worker_killed(crash_marker, root)
        original_records = process_records(root)
        original_acks = capture_acks(run, 'action-1')
        assert original_acks
        # Edit ONLY the captured helper; the closure's own source + pickle bytes unchanged.
        paths[helper].write_text("def delta():\n    return 9999\n")
        importlib.reload(sys.modules[helper])
        with pytest.raises(DataflowError) as excinfo:
            with RunContext.resume(reference, limits=limits) as run2:
                with run2.action({"kind": "materialize",
                                  "source": {"fixture": "graph-codepin-crash"}}) as execution:
                    executor.materialize_graph(spec)
                    execution.complete(dict(kind="graph-materialized"))
        assert excinfo.value.code == 'INVALID_CONTRACT'
        assert str(excinfo.value) == ('INVALID_CONTRACT: Loaded program code differs from the sealed pin '
                                     '(edited code or wrong module)')
        assert process_records(root) == original_records, 'code drift must refuse before a new worker launches'
        assert capture_acks(run2, 'action-1') == original_acks, 'code drift must not rewrite or extend original ACKs'
        assert run2.journal.execution_action('action-1')['result'] is None
    finally:
        _release_crash_orphans(tmp_path / "run-resources")
