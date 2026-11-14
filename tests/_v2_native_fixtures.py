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
from types import SimpleNamespace

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.plan_interpreter import EXECUTION_BINDINGS_SCHEMA_V2
from demiflow.execution.recovery_contract import identity
from demiflow.execution.recovery_identity import describe, source_identity
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


def active_native_run(root, n):
    """Yield (run, action) with F1 sealed and the action bound, run still open."""
    import contextlib
    from demiflow.execution import chain
    from demiflow.execution.run_context import RunAction, RunContext

    @contextlib.contextmanager
    def _cm():
        with RunContext.start(root) as run:
            declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
            run.journal.bind_execution_action(run.fence, n.action_id, declaration)
            action = RunAction(run, n.action_id, declaration)
            sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
            chain.seal_plan_facts(run, action, n.ld, n.pp, n.mref, sealed_at)
            yield run, action
            run.journal.complete_execution_action(run.fence, n.action_id, {"kind": "test-complete"})
    return _cm()


def seal_bindings(run, action, n, *, executions=None, programs=None):
    from demiflow.execution.sealed_plans import seal_execution_bindings
    doc = n.doc
    return seal_execution_bindings(
        run, action, logical_dag=n.ld, physical_plan=n.pp,
        profiles=doc["profiles"], programs=[] if programs is None else programs,
        driver_stage_ids=doc["driver_stage_ids"], source_bindings=doc["source_bindings"],
        channel_options=doc["channel_options"], checkpoint_rows=doc["checkpoint_rows"],
        supported_slice=doc["supported_slice"],
        executions=doc["executions"] if executions is None else executions)


def seal_native_run(root, n, *, with_bindings=True, executions=None):
    """Seal a real v2 run (F1 + execution-bindings) through the original authorities.

    Uses the fixture's compiled ld/pp/doc directly, so no F004 compiler is needed;
    the carrier, journal slot and idempotent re-seal are the real durable path.
    """
    from demiflow.execution import chain
    from demiflow.execution.run_context import RunAction, RunContext
    from demiflow.execution.sealed_plans import seal_execution_bindings
    with RunContext.start(root) as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, n.action_id, declaration)
        action = RunAction(run, n.action_id, declaration)
        sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
        chain.seal_plan_facts(run, action, n.ld, n.pp, n.mref, sealed_at)
        ref = None
        if with_bindings:
            doc = n.doc
            ref = seal_execution_bindings(
                run, action, logical_dag=n.ld, physical_plan=n.pp,
                profiles=doc["profiles"], programs=[],
                driver_stage_ids=doc["driver_stage_ids"],
                source_bindings=doc["source_bindings"],
                channel_options=doc["channel_options"],
                checkpoint_rows=doc["checkpoint_rows"],
                supported_slice=doc["supported_slice"],
                executions=doc["executions"] if executions is None else executions)
        run_id = run.ref.run_id
        run.journal.complete_execution_action(run.fence, n.action_id, {"kind": "test-complete"})
    return SimpleNamespace(root=root, run_id=run_id, action_id=n.action_id, ref=ref, n=n)


def seal_v1_run(root, v1):
    """Seal a real v1 run (chain_fixture) for schema-dispatch comparison."""
    from demiflow.execution import chain
    from demiflow.execution.run_context import RunAction, RunContext
    from demiflow.execution.sealed_plans import seal_execution_bindings
    with RunContext.start(root) as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, v1.action_id, declaration)
        action = RunAction(run, v1.action_id, declaration)
        sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
        chain.seal_plan_facts(run, action, v1.ld, v1.pp, v1.mref, sealed_at)
        doc = v1.doc
        ref = seal_execution_bindings(
            run, action, logical_dag=v1.ld, physical_plan=v1.pp,
            profiles=doc["profiles"], programs=doc["programs"],
            driver_stage_ids=doc["driver_stage_ids"], source_bindings=doc["source_bindings"],
            channel_options=doc["channel_options"], checkpoint_rows=doc["checkpoint_rows"],
            supported_slice=doc["supported_slice"])
        run_id = run.ref.run_id
        run.journal.complete_execution_action(run.fence, v1.action_id, {"kind": "test-complete"})
    return SimpleNamespace(root=root, run_id=run_id, action_id=v1.action_id, ref=ref, n=v1)