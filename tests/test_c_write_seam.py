"""C package write_lance seal seam: relational counterexamples + branch coverage.

Route (b): native execution and the single LanceSink publication are unchanged;
compile + seal + a forced pre-publication binding gate sit above them. These
tests prove, on the relational branch, that the gate fail-closes on structure
drift (R1), code-pin drift (R2) and end-to-end before any publication (R3); that
the three covered branches seal a plan the workbench reader can read back; and
that the streamed branch really consumes the sealed checkpoint policy and stage
limits. Deferred branches (pure Lance copy, arrow_batch_sink) honestly seal
nothing. All scratch output lives under the pytest tmp_path (/tmp/demiflow-*).
"""
import hashlib
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.chain import seal_write_topology
from demiflow.execution.local_kernel import keyed_dataset
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.sealed_plans import load_execution_bindings, load_sealed_plan
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
    keyed = keyed_dataset(left, "join", on=("id",), right=right)
    return keyed.map(enrich, inputs={"id": "id"}, output="v").filter(keep)


def drifted_relational_dataset():
    left = data.from_items([{"id": i, "a": i} for i in range(8)])
    right = data.from_items([{"id": i, "b": i * 10} for i in range(8)])
    keyed = keyed_dataset(left, "join", on=("id",), right=right)
    return keyed.map(enrich_drift, inputs={"id": "id"}, output="v").filter(keep)


def sealed_action_id(journal):
    with journal._connection() as db:
        actions = [r[0] for r in db.execute("SELECT action_id FROM runtime_actions")]
    sealed = [a for a in actions
              if journal.source_binding(a + "-sealed-plan") is not None]
    assert sealed, "no action carries a sealed-plan binding"
    return sealed[0]


def assert_sealed_readable(run_dir, run, expected_kinds):
    action_id = sealed_action_id(run.journal)
    facts = read_sealed_plan(run_dir, action_id)
    assert facts["available"] is True, facts
    sealed = load_sealed_plan(run_dir, action_id)
    nodes = sealed["logical_dag"]["nodes"]
    assert [n["op_kind"] for n in nodes] == expected_kinds
    assert sealed["logical_dag"]["action_id"] == action_id
    # Four-way identity: F1 facts and the bindings bind the same run/action/dag/plan.
    f1 = sealed["facts"]
    bindings = load_execution_bindings(run_dir, action_id, sealed_plan=sealed)
    doc = bindings["document"]
    assert doc["run_id"] == f1["run_id"] == run.ref.run_id
    assert doc["action_id"] == f1["action_id"] == action_id
    assert doc["dag_id"] == f1["dag_id"]
    assert doc["plan_id"] == f1["plan_id"]
    return action_id


# --- Golden paths: the three covered branches seal the executed plan ----------

def test_relational_write_seals_executed_plan(tmp_path):
    ds = relational_dataset()
    assert type(ds._source).__name__ == "KeyedSource"
    target = tmp_path / "out.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["KeyedSource", "map", "filter", "write_lance"])
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
                               ["MaterializedSource", "map", "filter", "write_lance"])
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
                               ["MaterializedSource", "async_map", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 2, 4, 6, 8, 10]


# --- Relational counterexamples: the binding gate fail-closes -----------------

def test_relational_structure_drift_fail_closed(tmp_path):
    """R1 (layer 2): identical executed code-pin, divergent sealed operator order.

    The executed plan is unchanged so its code-pin still matches the seal; only
    the sealed logical_dag operator sequence is perturbed. The structure recheck
    must fail-close independently of the code-pin gate.
    """
    ds = relational_dataset()
    with RunContext.start(tmp_path / "run") as run:
        action = RunAction(run, "rel-action", {"kind": "write_table"})
        handle = seal_write_topology(action, ds._source, ds._plan)
        handle.verify(ds._plan)  # the honest plan passes both bindings
        handle.topology["operators"] = list(reversed(handle.topology["operators"]))
        with pytest.raises(RecoveryContractError, match="operator identity differs"):
            handle.verify(ds._plan)


def test_relational_code_pin_drift_fail_closed(tmp_path):
    """R2 (layer 1): business code edited since seal changes the code-pin."""
    ds = relational_dataset()
    drifted = drifted_relational_dataset()
    with RunContext.start(tmp_path / "run") as run:
        action = RunAction(run, "rel-action", {"kind": "write_table"})
        handle = seal_write_topology(action, ds._source, ds._plan)
        handle.verify(ds._plan)
        with pytest.raises(RecoveryContractError, match="code-pin differs"):
            handle.verify(drifted._plan)


def test_relational_drift_fail_closed_before_publication(tmp_path, monkeypatch):
    """R3: on the real relational write path, drift fail-closes before commit.

    The seal is perturbed to bind a different business UDF than the plan that
    actually executes. The pre-publication gate must raise before the LanceSink
    commits: no target dataset is created and no commit is acked.
    """
    import demiflow.execution.chain as chain_mod

    real_seal = chain_mod.seal_write_topology

    def drift_seal(action, source, plan, *, host_id="local"):
        return real_seal(action, source, drifted_relational_dataset()._plan,
                         host_id=host_id)

    monkeypatch.setattr(chain_mod, "seal_write_topology", drift_seal)
    ds = relational_dataset()
    assert type(ds._source).__name__ == "KeyedSource"
    target = tmp_path / "out.lance"
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        with RunContext.start(tmp_path / "run") as run:
            ds.write_lance(str(target), mode="create", return_receipt=True)
    assert not target.exists(), "publication happened despite a failed binding gate"
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(tmp_path / "run")
    action_id = sealed_action_id(journal)
    assert not journal.execution_commits(action_id), "a commit was acked before the gate"


# --- Streamed layer-3: the sealed layout is really consumed -------------------

def test_streamed_consumes_sealed_checkpoint_and_stage_limits(tmp_path, monkeypatch):
    import demiflow.execution.chain as chain_mod
    import demiflow.execution.stream_resources as stream_resources
    import demiflow.execution.stream_materialize as stream_materialize

    captured = {"terminals": [], "queues": []}
    real_seal = chain_mod.seal_write_topology

    def capture_seal(action, source, plan, *, host_id="local"):
        handle = real_seal(action, source, plan, host_id=host_id)
        captured["handle"] = handle
        return handle

    real_init = stream_materialize.StreamMaterialization.__init__

    def spy_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        captured["terminals"].append(self)

    real_queue = stream_resources.ObservedQueue

    def spy_queue(*args, **kwargs):
        captured["queues"].append(kwargs)
        return real_queue(*args, **kwargs)

    monkeypatch.setattr(chain_mod, "seal_write_topology", capture_seal)
    monkeypatch.setattr(stream_materialize.StreamMaterialization, "__init__", spy_init)
    monkeypatch.setattr(stream_resources, "ObservedQueue", spy_queue)

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
        policy, stage_limits = captured["handle"].layout()
        # The layout reader round-trips the run.limits-derived sealed budgets.
        assert policy.interval_ms == run.limits["checkpoint_interval_ms"]
        assert policy.unconfirmed_rows == run.limits["checkpoint_unconfirmed_rows"]
        assert policy.unconfirmed_bytes == run.limits["checkpoint_unconfirmed_bytes"]
        output_bytes = run.limits["max_row_bytes"] + 256
        inflight_bytes = max(output_bytes, run.limits["max_batch_bytes"])
        assert stage_limits[0] == {"inflight_rows": run.limits["batch_rows"],
                                   "inflight_bytes": inflight_bytes,
                                   "output_bytes": output_bytes}
        # checkpoint_policy really lands on the terminal progress object.
        terminal = captured["terminals"][-1]
        assert terminal.checkpoint_policy == policy
        # The sealed stage budget really reaches a bounded queue constructor.
        assert any(q.get("max_item_bytes") == output_bytes
                   and q.get("max_bytes") == inflight_bytes
                   for q in captured["queues"]), captured["queues"]
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
    from demiflow.execution.identity_bridge import build_identity_bridge, seal_identity_bridge
    from demiflow.execution.sealed_plans import read_plan_artifact
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

def test_deferred_pure_copy_seals_nothing(tmp_path):
    source = tmp_path / "src.lance"
    lance.write_dataset(pa.table({"id": [1, 2, 3]}), str(source))
    ds = data.read_lance(str(source))
    target = tmp_path / "copy.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        with run.journal._connection() as db:
            actions = [r[0] for r in db.execute("SELECT action_id FROM runtime_actions")]
        sealed = [a for a in actions
                  if run.journal.source_binding(a + "-sealed-plan") is not None]
        assert sealed == [], "a deferred branch must not seal a plan it does not bind"
    assert lance.dataset(str(target)).count_rows() == 3


# --- Code-pin source archive durability: survives run-exit orphan cleanup ------

def test_code_pin_source_archive_survives_run_exit(tmp_path, monkeypatch):
    """The archived business source (manifest definition_ref.location) must survive.

    snapshot_code_sources registered the archive as protected=0, so clean_orphans
    unlinked it at run exit and the K1 manifest definition_ref.location dangled
    (gap④ audit could not read the pinned source). It must be written unregistered.
    """
    import demiflow.execution.chain as chain_mod

    captured = {}
    real_seal = chain_mod.seal_write_topology

    def capture_seal(action, source, plan, *, host_id="local"):
        handle = real_seal(action, source, plan, host_id=host_id)
        captured["plan_ref"] = handle._plan_ref
        return handle

    monkeypatch.setattr(chain_mod, "seal_write_topology", capture_seal)
    ds = relational_dataset()
    target = tmp_path / "out.lance"
    run_dir = tmp_path / "run"
    with RunContext.start(run_dir):
        ds.write_lance(str(target), mode="create", return_receipt=True)
        sources = captured["plan_ref"]["code_sources"]
        assert sources, "no business source was archived"
        for ref in sources:
            assert (run_dir / ref["path"]).exists()
    # clean_orphans has run at exit; the audited source archive must still be present.
    for ref in sources:
        carrier = run_dir / ref["path"]
        assert carrier.exists(), "clean_orphans pruned the code-pin source archive"
        assert hashlib.sha256(carrier.read_bytes()).hexdigest() == ref["sha256"]


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
    assert sealed["logical_dag"]["nodes"], "sealed logical_dag has no nodes"

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
    from demiflow.execution.plan_facts import operation_facts
    from demiflow.execution.recovery_contract import identity

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
    from demiflow.execution.plan_facts import _catch_facts
    from demiflow.execution.recovery_contract import identity

    for raw in ((), None, []):
        assert identity({"catch": _catch_facts(raw)}) == identity({"catch": raw})


def test_catch_empty_dag_id_unchanged(monkeypatch):
    """Hard constraint: canonicalizing catch must not drift the sealed dag_id for an
    empty whitelist. Compare the real compile_logical_dag dag_id before (the exact
    pre-fix raw operation.catch copy) and after (canonical) — byte-identical. The
    None defensive path is covered by test_catch_canonical_preserves_empty_identity
    (the public map_async API normalizes catch=None to ())."""
    import demiflow.execution.plan_facts as pf
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