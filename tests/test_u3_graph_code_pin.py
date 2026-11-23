"""A routed graph uses the same authenticated program pin at driver and worker load.

An unchanged business helper must run. Editing only its captured module must refuse
both a worker load and replay of the original acknowledged run before a new launch.
"""
import importlib
import os
import sys
import uuid

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext

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


@pytest.mark.parametrize("owner", ["worker", "driver"])
def test_graph_program_pin_refuses_edited_code(tmp_path, monkeypatch, owner):
    import json
    from demiflow import data
    from demiflow.execution.seal.plan import verify_action_programs, verify_dataflow_program
    from test_runtime_v3_real_seams import prepared

    helper_mod, paths, helper = _helper(tmp_path, monkeypatch, "graph_pin")

    def transform(row):
        return {**row, "x": row["x"] + helper_mod.delta()}

    ds = data.from_items([{"x": 1}]).map(transform)
    with prepared(tmp_path, ds) as (execution, services):
        graph, physical, sealed = execution._graph, execution._physical, execution._sealed
        pins = json.loads(b"".join(services.store.read(
            sealed.code_pin_ref, max_bytes=services.bounds.max_control_bytes)))
        node = next(n for n in graph.nodes if n.kind == 'transform')
        expected = pins["programs"][node.occurrence_id]
        assert helper in expected["imports"]
        ports = {"store": services.store, "bounds": services.bounds}

        def verify():
            if owner == "driver":
                return verify_action_programs(services.action, graph, physical, sealed, **ports)
            return verify_dataflow_program(node.program_ref, expected_pin=expected, **ports)

        verified = verify()
        if owner == "worker":
            assert verified.value.instantiate()({"x": 2}) == {"x": 3}
        paths[helper].write_text("def delta():\n    return 9999\n")
        importlib.reload(sys.modules[helper])
        with pytest.raises(DataflowError, match="Loaded program code differs from the sealed pin"):
            verify()


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
                result = LocalDatasetExecutor().materialize_graph(spec, control_timeout_s=15)
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
    executor = LocalDatasetExecutor(workers=1)
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
