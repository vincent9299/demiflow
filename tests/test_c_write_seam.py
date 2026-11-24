"""C package write_lance seal seam: relational counterexamples + branch coverage.

Route (b): native execution and the single LanceSink publication are unchanged;
compile + seal + a forced pre-publication binding gate sit above them. These
tests prove, on the relational branch, that the gate fail-closes on structure
drift (R1), code-pin drift (R2) and end-to-end before any publication (R3); that
the three covered branches seal a plan the workbench reader can read back; and
that the streamed branch really consumes the sealed checkpoint policy and stage
limits. Public writes now use the same v3 action; explicit historical v2 helpers
remain separately tested without restoring their retired execution routes. All scratch output lives under the pytest tmp_path (/tmp/demiflow-*).
"""
import hashlib
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunAction, RunContext
from demiflow.execution.seal.sealed_plans import load_execution_bindings, load_sealed_plan
from demiflow.workbench.runs.facts import read_sealed_plan


def enrich(id):
    return id * 2


def enrich_drift(id):
    return id * 3


def keep(row):
    return row["v"] % 4 == 0


def relational_dataset():
    left = data.from_items([{"id": i, "a": i} for i in range(8)])
    right = data.from_items([{"id": i, "b": i * 10} for i in range(8)])
    keyed = left.join(right, on=("id",))
    return keyed.map(enrich, inputs={"id": "id"}, output="v").filter(keep)


def drifted_relational_dataset():
    left = data.from_items([{"id": i, "a": i} for i in range(8)])
    right = data.from_items([{"id": i, "b": i * 10} for i in range(8)])
    keyed = left.join(right, on=("id",))
    return keyed.map(enrich_drift, inputs={"id": "id"}, output="v").filter(keep)


def sealed_action_id(journal):
    with journal._connection() as db:
        actions = [r[0] for r in db.execute("SELECT action_id FROM runtime_actions")]
    sealed = [a for a in actions
              if journal.source_binding(a + "-dataflow-sealed") is not None]
    assert len(sealed) == 1, "write must carry one original native sealed plan"
    return sealed[0]


def assert_sealed_readable(run_dir, run, expected_kinds):
    action_id = sealed_action_id(run.journal)
    facts = read_sealed_plan(run_dir, action_id)
    assert facts["available"] is True and facts["version"] == "v3", facts
    sealed = load_sealed_plan(run_dir, action_id, version="v3")
    graph, physical, binding = sealed["graph"], sealed["physical"], sealed["facts"]
    assert [node.kernel.kind for node in graph.nodes] == expected_kinds
    assert graph.action_id == binding["action_id"] == action_id
    assert binding["run_id"] == run.ref.run_id
    assert graph.dag_id == physical.dag_id == binding["dag_id"] == facts["dag_id"]
    assert physical.plan_id == binding["plan_id"] == facts["plan_id"]
    view = facts["plan_view"]
    assert [n["occurrence_id"] for n in view["dag"]["nodes"]] == [n.occurrence_id for n in graph.nodes]
    bindings = load_execution_bindings(run_dir, action_id, sealed_plan=sealed)
    assert bindings["version"] == "v3"
    assert bindings["document"]["action_id"] == action_id
    assert bindings["document"]["dag_id"] == graph.dag_id
    assert run.journal.execution_commits(action_id)
    assert all(c["status"] == "acked" for c in run.journal.execution_commits(action_id))
    return action_id


def read_program_evidence(run_dir, action_id):
    from demiflow.execution.seal.sealed_plans import sealed_read_bounds
    from demiflow.execution.storage.objects import readonly_run_store
    import json
    bundle = load_sealed_plan(run_dir, action_id, version="v3")
    sealed = bundle["sealed"]
    bounds = sealed_read_bounds()
    store = readonly_run_store(run_dir, store_id=sealed.code_pin_ref.store_id, bounds=bounds)
    pin_bytes = b"".join(store.read(sealed.code_pin_ref, max_bytes=bounds.max_control_bytes))
    pin = json.loads(pin_bytes)
    manifests = []
    for node in bundle["graph"].nodes:
        if node.program_ref is not None:
            raw = b"".join(store.read(node.program_ref, max_bytes=bounds.max_control_bytes))
            manifest = json.loads(raw)
            assert manifest["occurrence_id"] == node.occurrence_id
            assert manifest["code_identity"] == pin["programs"][node.occurrence_id]
            manifests.append(manifest)
    assert manifests and set(pin["programs"]) == {m["occurrence_id"] for m in manifests}
    return bundle, store, pin_bytes, pin, manifests


# --- Golden paths: the three covered branches seal the executed plan ----------

def test_relational_write_seals_executed_plan(tmp_path):
    ds = relational_dataset()
    assert type(ds._source).__name__ == "KeyedSource"
    target = tmp_path / "out.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["materialized", "materialized", "join", "BoundMapOp", "FilterOp", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    expected = [{"id": i, "a": i, "b": i * 10, "v": i * 2}
                for i in range(8) if (i * 2) % 4 == 0]
    assert sorted(rows, key=lambda r: r["id"]) == sorted(expected, key=lambda r: r["id"])


def test_fallback_write_seals_executed_plan(tmp_path):
    def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map(double).filter(keep)
    target = tmp_path / "fallback.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["materialized", "MapOp", "FilterOp", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 4, 8]


def test_streamed_write_seals_executed_plan(tmp_path):
    async def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map_async(double)
    target = tmp_path / "stream.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["materialized", "AsyncMapOp", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 2, 4, 6, 8, 10]


# --- Relational counterexamples: the binding gate fail-closes -----------------

def test_relational_structure_drift_fail_closed(tmp_path):
    from dataclasses import replace
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.seal.plan import verify_action_programs
    from test_runtime_v3_real_seams import prepared

    with prepared(tmp_path, relational_dataset()) as (execution, services):
        sealed = execution._sealed
        assert len(sealed.programs) > 1
        verify_action_programs(services.action, execution._graph, execution._physical, sealed,
                               store=services.store, bounds=services.bounds)
        changed = replace(sealed, programs=tuple(reversed(sealed.programs)))
        with pytest.raises(DataflowError, match='membership differs'):
            verify_action_programs(services.action, execution._graph, execution._physical, changed,
                                   store=services.store, bounds=services.bounds)
        assert not services.action.context.journal.execution_commits(services.action.action_id)


def test_relational_drift_fail_closed_before_publication(tmp_path, monkeypatch):
    """A hash-valid program carrying changed code cannot publish a result.

    The original declaration and seal retain enrich's pin. Only its staged
    payload is replaced with enrich_drift before the real managed worker loads
    it, so this exercises the actual loaded-code gate, not a retired caller.
    """
    from dataclasses import asdict
    import cloudpickle
    import json
    from demiflow.execution.seal import plan as sealing
    from demiflow.execution.contracts.recovery_contract import document
    original = sealing._stage_one_program
    altered = []

    def drift_payload(declaration, *, store, bounds, **options):
        ref = original(declaration, store=store, bounds=bounds, **options)
        if declaration.callable_spec is None or declaration.callable_spec.target is not enrich:
            return ref
        manifest = json.loads(b"".join(store.read(ref, max_bytes=bounds.max_control_bytes)))
        payload = cloudpickle.dumps(drifted_relational_dataset()._plan.operations[0].callable)
        payload_ref = store.put(iter((payload,)), max_bytes=bounds.max_state_bytes,
                                media_type=sealing.PAYLOAD_MEDIA)
        manifest["payload_ref"] = asdict(payload_ref)
        altered.append(declaration.occurrence_id)
        return store.put(iter((document(manifest, bounds.max_control_bytes),)),
                         max_bytes=bounds.max_control_bytes, media_type=sealing.PROGRAM_MEDIA)

    monkeypatch.setattr(sealing, "_stage_one_program", drift_payload)
    target, run_dir = tmp_path / "out.lance", tmp_path / "run"
    with pytest.raises(Exception, match="Loaded program code differs from the sealed pin"):
        with RunContext.start(run_dir):
            relational_dataset().write_lance(str(target), mode="create", return_receipt=True)
    assert len(altered) == 1
    assert not target.exists(), "publication happened despite a failed binding gate"
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(run_dir)
    action_id = sealed_action_id(journal)
    assert journal.execution_action(action_id)['result'] is None
    # Earlier source stages may checkpoint before the consumer's load fails.
    # Such ACKs must not publish rows, a Lance version, or a final result.
    for commit in journal.execution_commits(action_id):
        assert commit['status'] == 'acked'
        sink = commit['prepared']['sinks']['write_lance']
        assert sink['rows'] == sink['bytes'] == 0
        assert sink['native'] is None
        assert sink['manifest']['final'] is False
        assert sink['manifest']['partitions'] == []


# --- Streamed layer-3: the sealed layout is really consumed -------------------

def test_streamed_consumes_sealed_checkpoint_and_stage_limits(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.hosts.endpoints import _EndpointBase
    captured, assignments = [], []
    original = DataflowExecution.start
    original_prepare = _EndpointBase.prepare

    def prepare(endpoint, assignment, *, recovery_image=None):
        result = original_prepare(endpoint, assignment, recovery_image=recovery_image)
        assignments.append(assignment)
        return result

    monkeypatch.setattr(_EndpointBase, "prepare", prepare)

    def start(execution):
        result = original(execution)
        captured.append(execution)
        return result

    monkeypatch.setattr(DataflowExecution, "start", start)

    async def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map_async(double)
    target = tmp_path / "stream.lance"
    limits = {"max_row_bytes": 1024 ** 2, "max_batch_bytes": 4 * 1024 ** 2,
              "batch_rows": 16, "checkpoint_rows": 4, "checkpoint_interval_ms": 60000,
              "checkpoint_unconfirmed_rows": 1024,
              "checkpoint_unconfirmed_bytes": 8 * 1024 ** 2}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert len(captured) == 1
        execution = captured[0]
        bundle = load_sealed_plan(run.directory, sealed_action_id(run.journal), version="v3")
        physical = bundle["physical"]
        assert execution._physical == physical
        assert execution._policy == physical.policy
        assert execution._bounds == physical.bounds
        assert physical.policy.checkpoint_rows == limits["checkpoint_rows"]
        assert physical.policy.checkpoint_interval_ms == limits["checkpoint_interval_ms"]
        assert physical.bounds.max_row_bytes == limits["max_row_bytes"]
        assert physical.bounds.max_batch_bytes == limits["max_batch_bytes"]
        assert physical.stages and physical.exchanges
        assert execution._observed_worker_count > 0
        assert len(assignments) == execution._observed_worker_count
        stage_map = {s.stage_id: s for s in physical.stages}
        assert {s.stage_id for a in assignments for s in a.stages} == set(stage_map)
        for assignment in assignments:
            assert assignment.bounds == physical.bounds
            assert assignment.sealed == bundle["sealed"]
            assert assignment.plan_id == physical.plan_id
            assert all(s == stage_map[s.stage_id] for s in assignment.stages)
            assert all(e in physical.exchanges for e in (*assignment.inputs, *assignment.outputs))
        # The native assignments are checked against the same sealed plan by
        # each worker; observed allocator grants must cover its sealed stages.
        with run.journal._connection() as db:
            allocated = db.execute("SELECT COUNT(*) FROM resource_allocations").fetchone()[0]
        assert allocated >= execution._observed_worker_count
        assert all(e.credit_bytes <= physical.bounds.max_resident_bytes for e in physical.exchanges)
        assert all(s.output_bytes <= physical.bounds.max_batch_bytes for s in physical.stages)
        assert all(c["status"] == "acked" for c in run.journal.execution_commits(execution._action.action_id))
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 2, 4, 6, 8, 10]


# --- Bridge carrier durability: survives run-exit orphan cleanup --------------

def test_identity_bridge_carrier_survives_run_exit(tmp_path):
    """The bridge is a post-run-audited side-car fact, like the sealed plan.

    Registering it as a protected=0 artifact made clean_orphans unlink it at run
    exit, leaving the journal ref pointing at a missing carrier (workbench reason
    identity_bridge_corrupt). It must be written unregistered so the workbench
    reads it back after the run, with every entry four-way identity linked.
    """
    from demiflow.execution.seal.identity_bridge import build_identity_bridge, seal_identity_bridge
    from demiflow.execution.storage.reading import read_plan_artifact
    from demiflow.planning.dag import compile_logical_dag
    from demiflow.workbench.runs import facts

    ds = relational_dataset()
    report_digest = "a" * 64
    run_dir = tmp_path / "run"
    with RunContext.start(run_dir) as run:
        action = RunAction(run, "bridge-action", {"kind": "write_table"})
        logical_dag = compile_logical_dag(
            action_id=action.action_id, source=ds._source,
            operations=ds._plan.operations, terminal_kind="write_lance",
            sink_ids=("lance",))
        nodes = logical_dag["nodes"]
        bindings = [{"report_node_id": "n%d" % i, "report_occurrence": 0,
                     "logical_node_id": n["logical_node_id"],
                     "occurrence_id": n["occurrence_id"], "mode": "execute"}
                    for i, n in enumerate(nodes)]
        bridge = build_identity_bridge(logical_dag, action_id=action.action_id,
                                       report_digest=report_digest, bindings=bindings)
        ref = seal_identity_bridge(run, action, bridge)
        carrier = run_dir / ref["path"]
        assert carrier.exists()
    # clean_orphans has run at exit; the audited carrier must still be present.
    assert carrier.exists(), "clean_orphans pruned the post-run-audited bridge carrier"
    doc = read_plan_artifact(run_dir, ref, max_bytes=facts.MAX_IDENTITY_BRIDGE_BYTES)
    assert doc["schema"] == "demiflow.identity_bridge.v1"
    result = facts.read_identity_bridge(run_dir, action.action_id,
                                        dag_id=logical_dag["dag_id"],
                                        report_digest=report_digest)
    assert result["available"] is True, result
    entries = result["bridge"]["entries"]
    assert len(entries) == len(nodes)
    for entry in entries:
        assert entry["logical_node_id"] and entry["occurrence_id"]
        assert entry["report_node_id"] and entry["report_occurrence"] == 0


# --- Deferred branch: honest absence of a sealed plan -------------------------

def test_pure_copy_consumes_sealed_native_plan(tmp_path):
    source = tmp_path / "src.lance"
    lance.write_dataset(pa.table({"id": [1, 2, 3]}), str(source))
    ds = data.read_lance(str(source))
    target = tmp_path / "copy.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(run.directory, run, ["lance", "write_lance"])
    assert lance.dataset(str(target)).to_table().to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]


# --- Code-pin source archive durability: survives run-exit orphan cleanup ------

def test_code_pin_source_archive_survives_run_exit(tmp_path, monkeypatch):
    """Every pinned source remains readable after actual run-exit collection."""
    from demiflow.execution.seal.plan import _artifact_from_json
    ds = relational_dataset()
    run_dir = tmp_path / "run"
    with RunContext.start(run_dir) as run:
        ds.write_lance(str(tmp_path / "out.lance"), mode="create", return_receipt=True)
        action_id = sealed_action_id(run.journal)
        _, store, _, _, manifests = read_program_evidence(run_dir, action_id)
        sources = [(name, _artifact_from_json(ref), m["code_identity"]["imports"][name])
                   for m in manifests for name, ref in m["business_sources"].items()]
        assert sources, "no business source was archived"
        before = {(name, ref.digest): b"".join(store.read(ref, max_bytes=ref.size_bytes))
                  for name, ref, _ in sources}
    _, store, _, _, _ = read_program_evidence(run_dir, action_id)
    for name, ref, fact in sources:
        raw = b"".join(store.read(ref, max_bytes=ref.size_bytes))
        assert raw == before[name, ref.digest]
        assert hashlib.sha256(raw).hexdigest() == ref.digest == fact["sha256"]
        assert len(raw) == ref.size_bytes == fact["bytes"]


def test_write_seam_bundle_identity_is_code_pin_carrier_survives(tmp_path, monkeypatch):
    """The actual sealed pin carrier binds loaded-code identities independently
    of pickle payload digests and survives run-exit orphan collection.
    """
    from demiflow.execution.contracts.recovery_contract import document, identity
    run_dir = tmp_path / "run"
    with RunContext.start(run_dir) as run:
        relational_dataset().write_lance(str(tmp_path / "out.lance"), mode="create", return_receipt=True)
        action_id = sealed_action_id(run.journal)
        bundle, store, raw, pin, manifests = read_program_evidence(run_dir, action_id)
        ref = bundle["sealed"].code_pin_ref
        assert pin["action_id"] == action_id
        assert pin["platform_revision"] == run.journal.recovery_identity()["platform_revision"]
        assert raw == document(pin, bundle["physical"].bounds.max_control_bytes)
        assert ref.digest == hashlib.sha256(raw).hexdigest() == identity(pin)
        assert ref.size_bytes == len(raw)
        assert bundle["facts"]["code_pin_ref"]["digest"] == ref.digest
        for manifest in manifests:
            assert manifest["code_identity"] == pin["programs"][manifest["occurrence_id"]]
            if "payload_ref" in manifest:
                assert identity(manifest["code_identity"]) != manifest["payload_ref"]["digest"]
        original = (raw, bundle["facts"])
    after, _, after_raw, _, _ = read_program_evidence(run_dir, action_id)
    assert (after_raw, after["facts"]) == original


def test_write_seam_rejects_drift_on_same_run_before_lance_publication(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    root = tmp_path / 'run'
    target = tmp_path / 'original.lance'

    def stop_after_prepare(execution):
        assert execution._sealed is not None
        raise InterruptedError('stop after genuine original seal')

    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution, 'start', stop_after_prepare)
        with pytest.raises(InterruptedError, match='after genuine original seal'):
            with RunContext.start(root) as run:
                original = run.ref
                relational_dataset().write_lance(str(target), mode='create')
    assert not target.exists()
    action_id = sealed_action_id(run.journal)
    before = run.journal.source_binding(action_id + '-dataflow-sealed')
    with pytest.raises((RecoveryContractError, DataflowError), match='[Dd]iffer|changed|mismatch'):
        with RunContext.resume(original):
            drifted_relational_dataset().write_lance(str(target), mode='create')
    assert not target.exists()
    assert run.journal.source_binding(action_id + '-dataflow-sealed') == before


# --- Streamed layer-3 counterexample: wide row rejected by sealed byte budget --

def test_streamed_wide_row_rejected_by_sealed_byte_budget(tmp_path):
    """A row wider than the sealed output_bytes is honestly rejected, not published.

    The streamed write path consumes the sealed stage budget through ObservedQueue
    (max_item_bytes = output_bytes = max_row_bytes + 256). A 2MB row under a 1MB
    max_row_bytes must fail with a byte-budget error before any Lance publication.
    """
    async def widen(row):
        return {"wide": "x" * (2 * 1024 * 1024), "i": row["i"]}

    ds = data.from_items([{"i": 0}]).map_async(widen)
    target = tmp_path / "wide.lance"
    limits = {"max_row_bytes": 1 * 1024 ** 2, "max_batch_bytes": 4 * 1024 ** 2,
              "batch_rows": 16, "checkpoint_rows": 4, "checkpoint_interval_ms": 60000,
              "checkpoint_unconfirmed_rows": 1024,
              "checkpoint_unconfirmed_bytes": 8 * 1024 ** 2}
    with pytest.raises(Exception, match="byte budget") as exc_info:
        with RunContext.start(tmp_path / "run", limits=limits):
            ds.write_lance(str(target), mode="create", return_receipt=True)
    assert "byte budget" in str(exc_info.value).lower()
    assert not target.exists(), "wide row was published despite the sealed byte budget"


# --- Acceptance gate: workbench reads the real bridge after the run exits ------

@pytest.fixture
def pidfds():
    from demiflow.resources.process_observation import require_pidfds
    if not sys.platform.startswith("linux"):
        pytest.skip("Local managed units require Linux")
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def test_workbench_bridge_readable_after_run_exit(tmp_path, pidfds, monkeypatch):
    """Post-teardown user-visible contract: sealed plan AND bridge both read back.

    Drives the genuine workbench entry (flows/questions.py:select) in a real
    independent worker. After the run terminates (clean_orphans has run), the
    workbench reader must return the sealed plan available and the identity bridge
    available, with the bridge report_digest equal to the inspection digest the
    workbench binds. An in-run-only assertion cannot catch carrier pruning.
    """
    import test_upgrade_business_acceptance as acc
    from demiflow.workbench.runs import facts, manager as runs

    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "runtime-domain"))
    fixture = acc.build_owned_fixture(tmp_path, scenario="workbench")
    observed = acc.run_workbench_scenario(fixture["root"], entry=fixture["entry"],
                                          params=fixture["params"])
    assert observed["status"] == "succeeded", observed["status"]
    target = observed["target"]

    report = runs.show(fixture["root"], observed["run_id"], include_report=True,
                       attempt=observed["attempt"], compact=True)["report"]
    inspection_digest = report["digest"]

    sealed_actions = [aid for aid, s in observed["sealed"].items() if s.get("available")]
    assert sealed_actions, "no sealed plan readable after run exit"
    action_id = sealed_actions[0]

    sealed = facts.read_sealed_plan(target, action_id)
    assert sealed["available"] is True, sealed
    assert sealed["version"] == "v3"
    assert sealed["plan_view"]["dag"]["nodes"], "sealed graph has no nodes"

    bridge = facts.read_identity_bridge(target, action_id, dag_id=sealed["dag_id"],
                                        report_digest=inspection_digest)
    assert bridge["available"] is True, bridge
    assert bridge["bridge"]["report_digest"] == inspection_digest
    entries = bridge["bridge"]["entries"]
    assert entries, "bridge has no entries"
    # gap⑤: the page displays exactly the executed plan — every bridged report
    # node must be present in the workbench display-graph node set, not merely
    # carry a non-empty id.
    display_node_ids = {n["id"] for n in observed["graph"]["nodes"]}
    for entry in entries:
        assert entry["logical_node_id"] and entry["occurrence_id"]
        assert entry["report_node_id"] and type(entry["report_occurrence"]) is int
        assert entry["report_node_id"] in display_node_ids, (
            "bridged report node missing from workbench display graph",
            entry["report_node_id"], sorted(display_node_ids))


# --- Family 1: catch whitelist must be strict-JSON; empty/None identity frozen ---

def test_catch_nonempty_canonical_strict_json():
    """A non-empty exception whitelist serializes to sorted qualified names so the
    sealed plan identity is strict-JSON (the seam compiles every write_lance plan)."""
    from demiflow.execution.seal.plan_facts import operation_facts
    from demiflow.execution.contracts.recovery_contract import identity

    async def callback(row):
        return row

    ds = data.from_items([{"id": 1}]).map_async(callback, catch=(TimeoutError,), label="probe")
    facts = operation_facts(ds._plan.operations[0])
    assert facts["arguments"]["catch"] == ["builtins.TimeoutError"]
    identity(facts)  # must not raise "Execution metadata must be strict JSON"


def test_catch_canonical_preserves_empty_identity():
    """The shared canonicalizer keeps the exact prior JSON form for empty/None so
    neither catch copy site (stream_options fields nor the async/batch/save names
    loop) can drift a sealed identity."""
    from demiflow.execution.seal.plan_facts import _catch_facts
    from demiflow.execution.contracts.recovery_contract import identity

    for raw in ((), None, []):
        assert identity({"catch": _catch_facts(raw)}) == identity({"catch": raw})


def test_catch_empty_dag_id_unchanged(monkeypatch):
    """Hard constraint: canonicalizing catch must not drift the sealed dag_id for an
    empty whitelist. Compare the real compile_logical_dag dag_id before (the exact
    pre-fix raw operation.catch copy) and after (canonical) — byte-identical. The
    None defensive path is covered by test_catch_canonical_preserves_empty_identity
    (the public map_async API normalizes catch=None to ())."""
    import demiflow.execution.seal.plan_facts as pf
    from demiflow.planning.dag import compile_logical_dag

    real_facts = pf.operation_facts

    async def callback(row):
        return row

    ds = data.from_items([{"id": 1}]).map_async(callback, catch=(), label="probe")
    source, operations = ds._source, ds._plan.operations

    def dag_id():
        return compile_logical_dag(action_id="action-1", source=source,
                                   operations=operations, terminal_kind="write_lance",
                                   sink_ids=("lance",))["dag_id"]

    after = dag_id()

    def raw_facts(operation, _real=real_facts):
        fact = _real(operation)
        if "catch" in fact.get("arguments", {}):
            fact["arguments"]["catch"] = operation.catch  # exact pre-fix raw copy
        return fact

    monkeypatch.setattr(pf, "operation_facts", raw_facts)
    before = dag_id()
    monkeypatch.setattr(pf, "operation_facts", real_facts)
    assert after == before, (after, before)
