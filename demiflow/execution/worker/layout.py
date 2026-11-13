"""Worker-side sealed execution layout loading (C package).

Reads the additive ``execution_plan`` block from launch_spec, loads the sealed
F1 plan and execution bindings through B's reader, interprets the layout, and
returns the worker's placement with checkpoint policy and stage limits.  When
no ``execution_plan`` block is present the legacy path runs unchanged (None).
"""
from ..plan_interpreter import PlanContractError


def load_worker_layout(launch_spec, authority, assignment):
    """Return ``(placement, checkpoint_policy, stage_limits)`` or None.

    ``stage_limits`` is keyed by program operation index with values
    ``{inflight_rows, inflight_bytes, output_bytes}`` taken from the sealed
    physical stage budgets via operator_bindings.  ``checkpoint_policy`` is
    B's readonly CheckpointPolicy at the layout level.
    """
    execution_plan = launch_spec.get("execution_plan")
    if execution_plan is None:
        return None

    from ..plan_interpreter import interpret_physical_plan, validate_worker_assignment
    from ..sealed_plans import load_execution_bindings, load_sealed_plan

    action_id = execution_plan["action_id"]
    group_id = execution_plan["group_id"]
    bindings_ref = execution_plan["bindings_ref"]

    sealed_plan = load_sealed_plan(authority.directory, action_id)
    bindings = load_execution_bindings(authority.directory, action_id,
                                       sealed_plan=sealed_plan)
    if bindings["ref"] != bindings_ref:
        raise PlanContractError("identity_mismatch",
                                "launch_spec bindings_ref differs from the sealed journal")

    manifest_ref = sealed_plan["facts"]["artifact_manifest_ref"]
    layout = interpret_physical_plan(sealed_plan["logical_dag"],
                                     sealed_plan["physical_plan"],
                                     bindings=bindings, manifest_ref=manifest_ref)

    placement = next((w for w in layout.workers if w.group_id == group_id), None)
    if placement is None:
        raise PlanContractError("invalid_assignment",
                                "unknown worker group: " + str(group_id))

    if assignment and "schema_version" in assignment:
        validate_worker_assignment(layout, assignment, group_id=group_id)
    elif assignment:
        assigned = set(assignment.get("stages") or ())
        if assigned and assigned != set(placement.stage_ids):
            raise PlanContractError("invalid_assignment",
                                    "assignment stages differ from the sealed placement")

    stage_by_id = {s["stage_id"]: s for s in layout.stages}
    stage_limits = {}
    for binding in placement.operator_bindings:
        stage = stage_by_id[binding["stage_id"]]
        stage_limits[binding["program_index"]] = dict(stage["budgets"])

    return placement, layout.checkpoint, stage_limits