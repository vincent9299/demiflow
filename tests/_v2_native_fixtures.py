"""Shared test-domain fixtures for L-contract v2 native unit tests.

Not collected by pytest (no ``test_`` prefix). Builds a real linear Dataset DAG
with the frozen planning compilers and a matching execution_bindings.v2 document
so the seal -> interpret -> load roundtrip runs against the real authorities. The
program_ref / manifest_ref / profile are fixed-schema doubles (the interpreter
never reads the pickle or manifest); cross-group callees that are not yet merged
(F004 compiler, L042/L044 source-aware code-pin) are injected at the boundary by
the individual tests, never stubbed in production code.
"""
import copy
import hashlib
import json
import sqlite3
from types import SimpleNamespace

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.seal.plan_interpreter import EXECUTION_BINDINGS_SCHEMA_V2
from demiflow.execution.contracts.recovery_contract import document, identity
from demiflow.execution.seal.program_identity import describe, source_identity
from demiflow.planning.dag import (
    _operation_semantics, channel_id_for, compile_logical_dag,
    compile_physical_execution_plan,
)
from test_execution_layout import (
    MANIFEST_REF, PROFILE, PROFILE_REF, PROGRAM_REF, RECOVERY, bundle_for, stage_id,
)


def first(row):
    return {**row, "a": row["x"] + 1}


def second(row):
    return {**row, "b": row["a"] * 2}


def native_source_plan():
    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    plan = LogicalPlan(tuple(MapOp(CallableSpec.create(fn)) for fn in (first, second)))
    return source, plan


def native_dag(action_id="native-action"):
    source, plan = native_source_plan()
    ld = compile_logical_dag(action_id=action_id, source=source,
                             operations=plan.operations, terminal_kind="materialize",
                             sink_ids=("output",))
    return source, plan, ld


def native_physical(ld):
    return compile_physical_execution_plan(
        logical_dag=ld, plan_revision=0, artifact_manifest_ref=MANIFEST_REF,
        worker_groups=[], recovery=dict(RECOVERY),
        feasibility_snapshot_ref=identity(["test-feasibility"]),
        read_domain="test-domain", durability_declared="durable",
        partition_count=1, scheme_kind="single")


def local_operators(plan):
    return [{"logical_node_id": identity([_operation_semantics(op)[0], index]),
             "occurrence_id": identity(["occ", index])}
            for index, op in enumerate(plan.operations)]


def root_program_binding(action_id, ld, source, plan, *, program_ref=None):
    op_nodes = ld["nodes"][1:-1]
    op_stages = [stage_id(ld, node) for node in op_nodes]
    return {
        "program_id": identity([action_id, []]),
        "input_path": [],
        "plan_ref": copy.deepcopy(PROGRAM_REF if program_ref is None else program_ref),
        "source_identity_ref": identity(source_identity(source)),
        "sealed_operators": local_operators(plan),
        "operator_bindings": [{"program_index": i, "stage_id": s}
                              for i, s in enumerate(op_stages)],
    }


def native_v2_doc(action_id="native-action", *, backend="local_stream",
                  recovery="native_checkpoint", route="native_relational",
                  program_ref=None):
    source, plan, ld = native_dag(action_id)
    pp = native_physical(ld)
    op_nodes = ld["nodes"][1:-1]
    op_stages = [stage_id(ld, node) for node in op_nodes]
    driver_stage_ids = ([stage_id(ld, ld["nodes"][0])] + op_stages
                        + [stage_id(ld, ld["nodes"][-1])])
    binding = root_program_binding(action_id, ld, source, plan, program_ref=program_ref)
    execution_id = identity([action_id, "driver", route])
    executions = [{
        "execution_id": execution_id, "placement": "driver", "group_id": None,
        "backend": backend, "stage_ids": op_stages, "profile_ref": PROFILE_REF,
        "programs": [binding], "recovery": recovery,
    }]
    scheme_version = ld["sources"][0]["partition_scheme"]["scheme_version"]
    src = ld["sources"][0]
    doc = {
        "schema": EXECUTION_BINDINGS_SCHEMA_V2, "run_id": "run-test",
        "action_id": action_id, "dag_id": ld["dag_id"], "plan_id": pp["plan_id"],
        "artifact_manifest_ref": MANIFEST_REF, "supported_slice": "native_driver_v1",
        "profiles": {PROFILE_REF: copy.deepcopy(PROFILE)}, "executions": executions,
        "driver_stage_ids": driver_stage_ids,
        "source_bindings": [{"stage_id": driver_stage_ids[0], "kind": "fixed_source",
                             "ref": {"source_id": src["source_id"],
                                     "source_identity_ref": src["source_identity_ref"]}}],
        "channel_options": [{"channel_id": channel_id_for(e["edge_id"], scheme_version),
                             "logical_edge_ids": [e["edge_id"]]} for e in ld["edges"]],
        "checkpoint_rows": 2,
    }
    return SimpleNamespace(source=source, plan=plan, ld=ld, pp=pp, doc=doc,
                           bundle=bundle_for(doc), action_id=action_id,
                           execution_id=execution_id, op_stages=op_stages,
                           driver_stage_ids=driver_stage_ids, binding=binding,
                           mref=MANIFEST_REF)


def make_workdir():
    import tempfile
    from pathlib import Path
    return Path(tempfile.mkdtemp(prefix="demiflow-lcontract-", dir="/tmp"))


def seal_existing_format(writer, root, plan, *, native, with_bindings=True, executions=None):
    """Return the references written by a separately imported published writer."""
    from runtime_v3_history_scenarios import write_legacy_seal

    bindings = None
    if with_bindings:
        document = plan.doc
        bindings = {name: document[name] for name in (
            "profiles", "driver_stage_ids", "source_bindings", "channel_options",
            "checkpoint_rows", "supported_slice",
        )}
        bindings["programs"] = [] if native else document["programs"]
        if native:
            bindings["executions"] = document["executions"] if executions is None else executions
    result = write_legacy_seal(writer, root, action_id=plan.action_id, logical_dag=plan.ld,
                               physical_plan=plan.pp, manifest_ref=plan.mref, bindings=bindings)
    return SimpleNamespace(root=root, run_id=result["facts"]["run_id"], action_id=plan.action_id,
                           ref=result["ref"], n=plan)


def retarget_history_bindings(root, action_id, new_doc):
    raw = document(new_doc)
    digest = hashlib.sha256(raw).hexdigest()
    rel = "scratch/chain-execution-bindings/" + digest + ".json"
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    ref = {"path": rel, "sha256": digest, "bytes": len(raw)}
    key = action_id + "-execution-bindings"
    with sqlite3.connect(root / "run.sqlite") as db:
        db.execute("UPDATE runtime_sources SET document=? WHERE action_id=?", (json.dumps(ref), key))
    return ref
