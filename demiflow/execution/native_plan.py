"""Native driver compile/load/execute entrypoints for execution_bindings.v2.

First batch serves ``native_driver_v1`` only: a single driver execution owns the
source, terminal and every operation stage; v2 workers are refused upstream by
the bindings validator until implemented. The durable side reuses the existing
sealing authorities (``seal_write_plan_ref`` code-pin carrier, ``seal_plan_facts``
F1, ``seal_execution_bindings``); this module never adds a second journal, commit
ledger or pickle authority.

Dependencies on ``chain`` are imported inside functions so the worker write-seam
(``_WriteSeal.verify``) can import this module one-way without an import cycle.
"""
from __future__ import annotations

import dataclasses

from .recovery_contract import RecoveryContractError, identifier, identity
from .recovery_identity import describe
from .plan_interpreter import PlanContractError, interpret_physical_plan
from .sealed_plans import (
    load_sealed_plan, load_execution_bindings, seal_execution_bindings,
)
from .local_kernel import PlanInput


def _locate_subprogram(source, plan, input_path):
    """Walk the stable structural path from the root PlanInput to a sub-program.

    ``[]`` is the root; string segments select real fields (``source``, ``left``,
    ``inputs``) and integer segments index real sequences. A missing field, an
    out-of-range/negative index, or a leaf that is not a PlanInput is refused —
    the path must locate a genuine (source, plan) pair, never a guessed object.
    """
    node = PlanInput(source, plan)
    for segment in input_path:
        if type(segment) is str:
            if not hasattr(node, segment):
                raise PlanContractError("invalid_contract", "input_path field segment is missing")
            node = getattr(node, segment)
        elif type(segment) is int:
            if segment < 0:
                raise PlanContractError("invalid_contract", "input_path index must be nonnegative")
            try:
                node = node[segment]
            except (IndexError, KeyError, TypeError):
                raise PlanContractError("invalid_contract", "input_path index does not locate a segment")
        else:
            raise PlanContractError("invalid_contract", "input_path segments must be fields or indices")
    if not isinstance(node, PlanInput):
        raise PlanContractError("invalid_contract", "input_path does not locate a program input")
    return node.source, node.plan


def _locate_execution(layout, execution_id):
    matches = [entry for entry in layout.executions if entry.execution_id == execution_id]
    if len(matches) != 1:
        raise PlanContractError("invalid_contract",
                                "execution_id is absent or ambiguous in the sealed layout")
    return matches[0]


def stage_native_programs(action, program_slices):
    """Seal each real sub-program as a code-pin-only ProgramBinding (durable_write).

    Every slice goes through the original ``seal_write_plan_ref`` with its own
    source and program_id; no materialized actor is cloudpickled here. The
    independent worker delivery path keeps ``stage_plan_artifact`` and is not
    silently rerouted through this function.
    """
    from .chain import seal_write_plan_ref
    bindings = []
    for program_slice in program_slices:
        plan_ref = seal_write_plan_ref(action, program_slice.plan,
                                       source=program_slice.source,
                                       program_id=program_slice.program_id)
        bindings.append({
            "program_id": program_slice.program_id,
            "input_path": list(program_slice.input_path),
            "plan_ref": plan_ref,
            "source_identity_ref": identity(describe(program_slice.source)),
            "sealed_operators": [dict(op) for op in program_slice.sealed_operators],
            "operator_bindings": [dict(ob) for ob in program_slice.operator_bindings],
        })
    return tuple(bindings)


def verify_native_programs(source, plan, *, programs, action_id):
    """Verify every sealed sub-program against the live declarations (read_only).

    The native driver carrier is code-pin-only with a source-inclusive pin, so the
    pin is recomputed with ``source=``; a worker pickle carrier (source-exclusive
    pin) fails that recomputation rather than being guessed from the same five
    keys. ``sealed_operators`` is the sub-program's linear local order, checked
    exactly as ``verify_loaded_plan`` does — never the renumbered full-graph ids.
    """
    from .chain import plan_code_pin
    from ..planning.dag import _operation_semantics
    if source is None:
        raise PlanContractError("invalid_contract", "native verification requires a fixed source")
    try:
        identifier(action_id)
    except (RecoveryContractError, ValueError) as exc:
        raise PlanContractError("invalid_contract", "action_id invalid: " + str(exc))
    for binding in programs:
        input_path = binding["input_path"]
        sub_source, sub_plan = _locate_subprogram(source, plan, input_path)
        if binding["program_id"] != identity([action_id, input_path]):
            raise PlanContractError("identity_mismatch",
                                    "program_id does not bind action_id and input_path")
        if binding["source_identity_ref"] != identity(describe(sub_source)):
            raise PlanContractError("identity_mismatch",
                                    "source_identity_ref does not bind the located source")
        plan_ref = binding["plan_ref"]
        if identity(plan_code_pin(sub_plan, source=sub_source)) != plan_ref["code_pin_digest"]:
            raise RecoveryContractError(
                "Native program code-pin differs; business/source code edited since seal (fail-closed)")
        recomputed = [identity([_operation_semantics(op)[0], index])
                      for index, op in enumerate(sub_plan.operations)]
        sealed = [node["logical_node_id"] for node in binding["sealed_operators"]]
        if recomputed != sealed:
            raise RecoveryContractError(
                "Native program operator identity differs from the sealed local order")
    return None


def compile_native_topology(action, source, plan, *, decision):
    """Compile the real Dataset DAG and seal a driver-only native layout (durable_write).

    The driver placement binds the decision's actual backend/recovery; source,
    terminal and every operation stage belong to the driver. Budgets come from the
    same run limits authority as the linear seam. Recovery is the descriptive
    RecoveryGuarantee declared by the action, never re-selected from budget failure.
    """
    from .chain import seal_write_plan_ref, _build_sealed_manifest, seal_plan_facts
    from .exchange import read_domain
    from ..planning.dag import compile_physical_execution_plan, channel_id_for
    from ..planning.dataset_dag import compile_dataset_logical_dag
    run = action.context
    terminal_kind = decision.action_kind
    sink_ids = ("lance",) if terminal_kind == "write_lance" else ("output",)
    logical_dag, program_slices = compile_dataset_logical_dag(
        action_id=action.action_id, source=source, plan=plan,
        terminal_kind=terminal_kind, sink_ids=sink_ids)
    # F004 binds each local program_index to its full-graph occurrence_id; the
    # sealed ProgramBinding requires the physical stage_id. Only this compile step
    # holds the dag_id, so resolve here (stage_native_programs has no dag authority).
    program_slices = tuple(
        dataclasses.replace(program_slice, operator_bindings=tuple(
            {"program_index": binding["program_index"],
             "stage_id": identity([logical_dag["dag_id"], binding["occurrence_id"]])}
            for binding in program_slice.operator_bindings))
        for program_slice in program_slices)
    programs = stage_native_programs(action, program_slices)

    root_ref = seal_write_plan_ref(action, plan, source=source,
                                   program_id=identity([action.action_id, []]))
    manifest, mref = _build_sealed_manifest(run, root_ref, entrypoint="native")

    nodes = logical_dag["nodes"]
    dag_id = logical_dag["dag_id"]
    op_nodes = nodes[1:-1]
    op_stage_ids = [identity([dag_id, node["occurrence_id"]]) for node in op_nodes]
    driver_stage_ids = ([identity([dag_id, nodes[0]["occurrence_id"]])] + op_stage_ids
                        + [identity([dag_id, nodes[-1]["occurrence_id"]])])
    profile_doc = {"requests": {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}}
    profile_ref = identity(profile_doc)
    row_bytes = run.limits["max_row_bytes"] + 256
    queue_bytes = max(row_bytes, run.limits["max_batch_bytes"])
    stage_budgets = {node["occurrence_id"]: {
        "inflight_rows": run.limits["batch_rows"], "inflight_bytes": queue_bytes,
        "output_bytes": row_bytes} for node in op_nodes}
    physical = compile_physical_execution_plan(
        logical_dag=logical_dag, plan_revision=0, artifact_manifest_ref=mref,
        worker_groups=[],
        recovery={"checkpoint_interval_ms": run.limits["checkpoint_interval_ms"],
                  "unconfirmed_rows": run.limits["checkpoint_unconfirmed_rows"],
                  "unconfirmed_bytes": run.limits["checkpoint_unconfirmed_bytes"],
                  "terminals": []},
        feasibility_snapshot_ref=identity([run.ref.run_id, "feasibility"]),
        read_domain=read_domain(run.directory, "local"), durability_declared="durable",
        partition_count=1, scheme_kind="single", stage_budgets=stage_budgets)

    src = logical_dag["sources"][0]
    source_bindings = [{"stage_id": driver_stage_ids[0], "kind": "fixed_source",
                        "ref": {"source_id": src["source_id"],
                                "source_identity_ref": src["source_identity_ref"]}}]
    scheme_version = src["partition_scheme"]["scheme_version"]
    channel_options = [{"channel_id": channel_id_for(edge["edge_id"], scheme_version),
                        "logical_edge_ids": [edge["edge_id"]]} for edge in logical_dag["edges"]]
    executions = [{
        "execution_id": identity([action.action_id, "driver", decision.route]),
        "placement": "driver", "group_id": None, "backend": decision.backend,
        "stage_ids": op_stage_ids, "profile_ref": profile_ref,
        "programs": list(programs), "recovery": decision.recovery,
    }]
    seal_plan_facts(run, action, logical_dag, physical, mref, manifest["sealed_at"])
    bindings_ref = seal_execution_bindings(
        run, action, logical_dag=logical_dag, physical_plan=physical,
        profiles={profile_ref: profile_doc}, programs=[],
        driver_stage_ids=driver_stage_ids, source_bindings=source_bindings,
        channel_options=channel_options, checkpoint_rows=run.limits["checkpoint_rows"],
        supported_slice="native_driver_v1", executions=executions)
    return logical_dag, physical, bindings_ref


def load_native_execution(run, action_id, *, execution_id):
    """Read the same sealed inputs and locate one execution exactly (read_only).

    No re-selection from current defaults/eligibility: the sealed layout is the
    authority and ``execution_id`` must match a single placement.
    """
    sealed_plan = load_sealed_plan(run.directory, action_id)
    bindings = load_execution_bindings(run.directory, action_id, sealed_plan=sealed_plan)
    layout = interpret_physical_plan(sealed_plan["logical_dag"], sealed_plan["physical_plan"],
                                     bindings=bindings,
                                     manifest_ref=sealed_plan["facts"]["artifact_manifest_ref"])
    placement = _locate_execution(layout, execution_id)
    return layout, placement


def execute_native_plan(executor, source, plan, *, layout, execution_id,
                        arrow=False, sink_schema=None):
    """Verify then dispatch the sealed backend/profile/program (runtime_state).

    The placement is located by id, its programs are verified against the live
    source/plan under the current action authority, and only the sealed backend
    entrypoint runs — no free ``eligible()`` re-selection. The generator finally
    closes the borrowed backend iterator so this execution's resources release on
    normal completion, error and early close.
    """
    from .run_context import current_action
    placement = _locate_execution(layout, execution_id)
    if placement.placement != "driver":
        raise PlanContractError("unsupported_layout", "native execute supports driver placements only")
    action = current_action()
    if action is None:
        raise PlanContractError("invalid_contract", "execute_native_plan requires an active action authority")
    verify_native_programs(source, plan, programs=placement.programs, action_id=action.action_id)
    if placement.backend == "arrow_batch":
        from .arrow_batch_sink import execute as backend_execute
        iterator = backend_execute(executor, source, plan, sink_schema, execution=placement)
    else:
        from .dataset_native import execute as backend_execute
        iterator = backend_execute(executor, source, plan, arrow=arrow,
                                   sink_schema=sink_schema, execution=placement)
    try:
        yield from iterator
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()