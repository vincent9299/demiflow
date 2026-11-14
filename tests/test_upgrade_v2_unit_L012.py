"""L012 · seal_write_topology — additive decision, sealed bindings stay authoritative.

Binds: ``decision=None`` and every non-native route keep the existing v1 chain seal
byte-for-byte; a native driver route (native_relational / arrow_batch / lance_scan)
seals through compile_native_topology and binds the resulting driver execution by
``identity([action_id, "driver", route])``; the returned seal still serves verify and
layout. On recovery the sealed bindings select the schema and the driver execution —
a new decision only has to agree with the historical backend/route and can never
replace it. The v1 logical-plan drift refusal and the identity bridge are preserved.

v1 cases use the real sealer, journal and carrier bytes. v2 cases use C08 contract
samples for the absent native_plan / dataset_dag modules; the v2 recovery cases also
sample the bindings document because baseline load_execution_bindings still rejects
schema v2 (L005). Those are interface-level evidence, not an integration pass.
"""
import sys
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution import chain, sealed_plans
from demiflow.execution.chain import seal_write_topology
from demiflow.execution.plan_interpreter import CheckpointPolicy
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction

from _v2_write_seal_fixtures import (
    NATIVE_ROUTES, V2_SCHEMA, NativePlanRecorder, SampleExecution, SampleLayout,
    decision, freeze_programs, install_dataset_dag, program_binding,
)

CHECKPOINT = CheckpointPolicy(checkpoint_rows=8, interval_ms=1000,
                              unconfirmed_rows=4, unconfirmed_bytes=4096)
V1_KEYS = {"run_id", "action_id", "epoch", "topology_revision", "route_epoch",
           "plan_revision", "artifact_manifest_ref", "source_id", "channel_id",
           "operators", "sink_id", "bindings_ref"}
MANIFEST_REF = {"manifest_id": "manifest-1", "sha256": "m-sha"}
BINDINGS_REF = {"path": "bindings.json", "sha256": "b-sha"}


def _double(row):
    return {"k": row["k"], "v": row["v"] * 2}


def _triple(row):
    return {"k": row["k"], "v": row["v"] * 3}


def _plan(*functions):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(f)) for f in functions))


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        action = RunAction(run, "write-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("write-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "write-action", {"kind": "test-complete"})


@pytest.fixture
def source():
    return ItemsSource(items=({"k": "a", "v": 1},))


def _v2_setup(monkeypatch, action_id, *, programs=None, logical_dag=None,
              backend="datafusion", route="native_relational", verify_error=None):
    execution_id = identity([action_id, "driver", route])
    if programs is None:
        programs = [program_binding(
            program_id=identity([action_id, []]), input_path=[],
            sealed_operators=[{"logical_node_id": "n0", "occurrence_id": "o0"}],
            operator_bindings=[{"program_index": 0, "stage_id": "stage-0"}])]
    placement = SampleExecution(execution_id=execution_id, placement="driver",
                                backend=backend, programs=freeze_programs(programs),
                                stage_limits={"stage-0": {"inflight_rows": 3}})
    layout = SampleLayout(checkpoint=CHECKPOINT, executions=(placement,),
                          artifact_manifest_ref=MANIFEST_REF, bindings_ref=BINDINGS_REF)
    recorder = NativePlanRecorder(layout=layout, placement=placement,
                                  logical_dag=logical_dag, bindings_ref=BINDINGS_REF,
                                  verify_error=verify_error)
    recorder.install(monkeypatch)
    return recorder, execution_id, layout, placement


def _seal_v2_document(action_id, execution_id, *, backend="datafusion",
                      logical_dag=None, drivers=1):
    dag = logical_dag if logical_dag is not None else {
        "dag_id": "dag", "sources": [{"source_id": "fixed"}]}
    executions = [{"execution_id": execution_id, "placement": "driver",
                   "group_id": None, "backend": backend, "stage_ids": ["stage-0"],
                   "profile_ref": "profile", "programs": [], "recovery": "native_checkpoint"}]
    if drivers == 2:
        executions.append(dict(executions[0], execution_id="second-driver"))
    return {"schema": V2_SCHEMA, "run_id": "r", "action_id": action_id,
            "dag_id": dag["dag_id"], "plan_id": "plan", "artifact_manifest_ref": MANIFEST_REF,
            "supported_slice": "native_driver_v1", "profiles": {}, "executions": executions,
            "driver_stage_ids": ["stage-0"], "source_bindings": [], "channel_options": [],
            "checkpoint_rows": 8}, dag


def _install_v2_recovery(monkeypatch, sealed, document, logical_dag):
    """Real durable recovery precondition; the v2 document itself is a C08 sample."""
    physical = {"plan_revision": 0, "channels": [{"channel_id": "chan"}]}
    bundle = {"facts": {"run_id": sealed.run.ref.run_id, "action_id": "write-action",
                        "dag_id": logical_dag["dag_id"], "plan_id": "plan",
                        "artifact_manifest_ref": MANIFEST_REF},
              "logical_dag": logical_dag, "physical_plan": physical}
    monkeypatch.setattr(sealed_plans, "load_sealed_plan", lambda *a, **k: bundle)
    monkeypatch.setattr(sealed_plans, "load_execution_bindings",
                        lambda *a, **k: {"ref": BINDINGS_REF, "document": document})
    return bundle


# --- v1 is preserved ----------------------------------------------------------

def test_no_decision_keeps_the_v1_chain_seal(sealed, source):
    plan = _plan(_double)
    seal = seal_write_topology(sealed.action, source, plan)
    assert seal._native_placement is None
    assert seal._native_layout is None
    assert set(seal.topology) == V1_KEYS
    assert seal.topology["sink_id"] == "lance"
    assert seal.verify(plan) is None
    document = sealed_plans.load_execution_bindings(
        sealed.run.directory, "write-action",
        sealed_plan=sealed_plans.load_sealed_plan(sealed.run.directory, "write-action"))["document"]
    assert document["schema"] == "demiflow.execution_bindings.v1"
    assert document["supported_slice"] == "chain_v1"


@pytest.mark.parametrize("route", ["pull", "stream_delivery", "stream_materialize",
                                   "queue_stream", "static_graph"])
def test_non_native_routes_keep_v1(sealed, source, route, monkeypatch):
    monkeypatch.delitem(sys.modules, "demiflow.execution.native_plan", raising=False)
    plan = _plan(_double)
    seal = seal_write_topology(sealed.action, source, plan,
                               decision=decision(route, backend="local_pull"))
    assert seal._native_placement is None
    assert set(seal.topology) == V1_KEYS
    assert "demiflow.execution.native_plan" not in sys.modules
    assert seal.verify(plan) is None


def test_v1_recovery_reuses_the_sealed_bindings(sealed, source):
    plan = _plan(_double)
    first = seal_write_topology(sealed.action, source, plan)
    journal = sealed.run.journal.path
    before = journal.read_bytes()
    second = seal_write_topology(sealed.action, source, plan)
    assert second.bindings_ref == first.bindings_ref
    assert second.topology["artifact_manifest_ref"] == first.topology["artifact_manifest_ref"]
    assert second.topology["operators"] == first.topology["operators"]
    assert second._native_placement is None
    assert second.verify(plan) is None
    assert journal.read_bytes() == before


def test_v1_recovery_refuses_an_edited_plan(sealed, source):
    seal_write_topology(sealed.action, source, _plan(_double))
    with pytest.raises(RecoveryContractError):
        seal_write_topology(sealed.action, source, _plan(_triple))


def test_v1_recovery_ignores_a_non_native_decision(sealed, source):
    plan = _plan(_double)
    first = seal_write_topology(sealed.action, source, plan)
    second = seal_write_topology(sealed.action, source, plan,
                                 decision=decision("pull", backend="local_pull"))
    assert second.bindings_ref == first.bindings_ref
    assert second._native_placement is None


# --- v2 fresh seal ------------------------------------------------------------

@pytest.mark.parametrize("route", NATIVE_ROUTES)
def test_native_route_seals_through_the_native_authority(sealed, source, route, monkeypatch):
    plan = _plan(_double)
    recorder, execution_id, layout, placement = _v2_setup(
        monkeypatch, sealed.action.action_id, route=route)
    chosen = decision(route)
    seal = seal_write_topology(sealed.action, source, plan, decision=chosen)
    assert recorder.compile_calls == [{"action_id": "write-action", "source": source,
                                       "plan": plan, "decision": chosen}]
    assert recorder.load_calls == [{"run_id": sealed.run.ref.run_id,
                                    "action_id": "write-action",
                                    "execution_id": execution_id}]
    assert seal._native_layout is layout
    assert seal._native_placement is placement
    assert seal._native_execution_id == execution_id
    assert seal._source is source


def test_v2_topology_mirrors_the_v1_key_set(sealed, source, monkeypatch):
    plan = _plan(_double)
    recorder, execution_id, layout, _ = _v2_setup(monkeypatch, sealed.action.action_id)
    seal = seal_write_topology(sealed.action, source, plan,
                               decision=decision("native_relational"))
    assert set(seal.topology) == V1_KEYS
    assert seal.topology["run_id"] == sealed.run.ref.run_id
    assert seal.topology["action_id"] == "write-action"
    assert seal.topology["epoch"] == sealed.run.attempt
    assert seal.topology["sink_id"] == "lance"
    assert seal.topology["source_id"] == "fixed"
    assert seal.topology["channel_id"] == "chan"
    assert seal.topology["plan_revision"] == 0
    assert seal.topology["artifact_manifest_ref"] == MANIFEST_REF
    assert seal.topology["bindings_ref"] == BINDINGS_REF
    assert seal.bindings_ref == BINDINGS_REF
    assert recorder.verify_calls == []


def test_v2_plan_ref_and_operators_come_from_the_root_program(sealed, source, monkeypatch):
    action_id = sealed.action.action_id
    root_ref = {"path": "root.json", "sha256": "root-sha", "code_pin_digest": "root-pin",
                "code_sources": [], "format": "code-pin-v1"}
    operators = [{"logical_node_id": "n0", "occurrence_id": "o0"},
                 {"logical_node_id": "n1", "occurrence_id": "o1"}]
    programs = [program_binding(program_id=identity([action_id, ["source", "left"]]),
                                input_path=["source", "left"]),
                program_binding(program_id=identity([action_id, []]), input_path=[],
                                plan_ref=root_ref, sealed_operators=operators)]
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id, programs=programs)
    seal = seal_write_topology(sealed.action, source, _plan(_double),
                               decision=decision("native_relational"))
    assert seal._plan_ref == root_ref
    assert seal.topology["operators"] == operators


def test_v2_empty_programs_bind_no_plan_ref(sealed, source, monkeypatch):
    recorder, _, _, _ = _v2_setup(monkeypatch, sealed.action.action_id, programs=[])
    seal = seal_write_topology(sealed.action, source, LogicalPlan(),
                               decision=decision("native_relational"))
    assert seal._plan_ref is None
    assert seal.topology["operators"] == []
    assert seal.layout() == (CHECKPOINT, {})


def test_v2_fresh_seal_does_not_use_the_v1_sealers(sealed, source, monkeypatch):
    _v2_setup(monkeypatch, sealed.action.action_id)

    def forbidden(*args, **kwargs):
        raise AssertionError("the native route must not seal a second v1 authority")

    monkeypatch.setattr(chain, "seal_write_plan_ref", forbidden)
    monkeypatch.setattr(chain, "compile_chain_topology", forbidden)
    seal = seal_write_topology(sealed.action, source, _plan(_double),
                               decision=decision("native_relational"))
    assert seal._native_placement is not None


def test_v2_fresh_seal_still_seals_the_identity_bridge(sealed, source, monkeypatch):
    logical_dag = {"dag_id": "native-dag", "sources": [{"source_id": "fixed"}]}
    _v2_setup(monkeypatch, sealed.action.action_id, logical_dag=logical_dag)
    calls = []
    original = chain._seal_chain_identity_bridge

    def spy(run, action, dag, src, plan):
        calls.append(dag)
        return original(run, action, dag, src, plan)

    monkeypatch.setattr(chain, "_seal_chain_identity_bridge", spy)
    seal_write_topology(sealed.action, source, _plan(_double),
                        decision=decision("native_relational"))
    assert calls == [logical_dag]


def test_v2_fresh_seal_propagates_a_compile_refusal(sealed, source, monkeypatch):
    recorder, _, _, _ = _v2_setup(monkeypatch, sealed.action.action_id)
    refusal = RecoveryContractError("native compile refused")

    def refuse(*args, **kwargs):
        raise refusal

    monkeypatch.setitem(sys.modules, "demiflow.execution.native_plan",
                        SimpleNamespace(compile_native_topology=refuse,
                                        load_native_execution=recorder.load_native_execution,
                                        verify_native_programs=recorder.verify_native_programs))
    with pytest.raises(RecoveryContractError) as excinfo:
        seal_write_topology(sealed.action, source, _plan(_double),
                            decision=decision("native_relational"))
    assert excinfo.value is refusal


# --- v2 recovery: the sealed binding is the authority -------------------------

def test_v2_recovery_loads_the_sealed_execution(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, layout, placement = _v2_setup(monkeypatch, action_id)
    seal = seal_write_topology(sealed.action, source, plan)
    assert recorder.load_calls == [{"run_id": sealed.run.ref.run_id,
                                    "action_id": action_id, "execution_id": execution_id}]
    assert recorder.compile_calls == []
    assert len(recorder.verify_calls) == 1
    assert recorder.verify_calls[0]["programs"] is placement.programs
    assert seal._native_layout is layout
    assert seal._native_execution_id == execution_id
    assert set(seal.topology) == V1_KEYS
    assert seal.topology["bindings_ref"] == BINDINGS_REF


def test_v2_recovery_accepts_an_agreeing_decision(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "arrow_batch"])
    document, logical_dag = _seal_v2_document(action_id, execution_id, backend="arrow_batch")
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id, route="arrow_batch",
                                  backend="arrow_batch")
    seal = seal_write_topology(sealed.action, source, plan,
                               decision=decision("arrow_batch", backend="arrow_batch"))
    assert seal._native_execution_id == execution_id
    assert recorder.load_calls[0]["execution_id"] == execution_id


def test_v2_recovery_refuses_a_different_backend(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id, backend="datafusion")
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id)
    with pytest.raises(RecoveryContractError, match="backend differs from the sealed backend"):
        seal_write_topology(sealed.action, source, plan,
                            decision=decision("native_relational", backend="arrow_batch"))
    assert recorder.load_calls == []


def test_v2_recovery_refuses_a_different_route(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id)
    with pytest.raises(RecoveryContractError, match="route differs from the sealed execution"):
        seal_write_topology(sealed.action, source, plan,
                            decision=decision("lance_scan", backend="datafusion"))
    assert recorder.load_calls == []


def test_v2_recovery_never_reselects_from_the_new_decision(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    historical = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, historical)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id)
    seal = seal_write_topology(sealed.action, source, plan,
                               decision=decision("native_relational"))
    assert seal._native_execution_id == historical
    assert recorder.load_calls[0]["execution_id"] == historical


def test_v2_recovery_refuses_two_driver_executions(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id, drivers=2)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id)
    with pytest.raises(RecoveryContractError, match="requires one driver execution"):
        seal_write_topology(sealed.action, source, plan)
    assert recorder.load_calls == []


def test_v2_recovery_refuses_logical_plan_drift(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    calls = install_dataset_dag(monkeypatch, {"dag_id": "different-dag",
                                              "sources": [{"source_id": "fixed"}]})
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id)
    with pytest.raises(RecoveryContractError, match="logical plan differs from the sealed plan"):
        seal_write_topology(sealed.action, source, plan)
    assert calls[0]["terminal_kind"] == "write_lance"
    assert calls[0]["sink_ids"] == ("lance",)
    assert calls[0]["plan"] is plan


def test_v2_recovery_propagates_a_native_verify_refusal(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    refusal = RecoveryContractError("Native program code-pin differs")
    recorder, _, _, _ = _v2_setup(monkeypatch, action_id, verify_error=refusal)
    with pytest.raises(RecoveryContractError) as excinfo:
        seal_write_topology(sealed.action, source, plan)
    assert excinfo.value is refusal


def test_v2_recovery_seals_the_identity_bridge(sealed, source, monkeypatch):
    plan = _plan(_double)
    seal_write_topology(sealed.action, source, plan)
    action_id = sealed.action.action_id
    execution_id = identity([action_id, "driver", "native_relational"])
    document, logical_dag = _seal_v2_document(action_id, execution_id)
    _install_v2_recovery(monkeypatch, sealed, document, logical_dag)
    install_dataset_dag(monkeypatch, logical_dag)
    _v2_setup(monkeypatch, action_id)
    calls = []
    original = chain._seal_chain_identity_bridge

    def spy(run, action, dag, src, plan):
        calls.append(dag)
        return original(run, action, dag, src, plan)

    monkeypatch.setattr(chain, "_seal_chain_identity_bridge", spy)
    seal_write_topology(sealed.action, source, plan)
    assert calls == [logical_dag]