"""Worker-side sealed execution layout loading (C package).

Reads the additive ``execution_plan`` block from launch_spec, loads and verifies
the execution bindings artifact, resolves the sealed F1 plan facts, interprets
the physical plan through B's interpreter, and returns the worker's placement
with checkpoint policy and stage limits.  When no ``execution_plan`` block is
present the legacy path runs unchanged (returns None).

The worker never constructs a CommitCoordinator or owns commit authority.
"""
import hashlib
import json

from ..recovery_contract import RecoveryContractError

_MAX_BINDINGS_BYTES = 4 * 1024 * 1024


def load_worker_layout(launch_spec, authority, assignment):
    """Return ``(placement, checkpoint_policy, stage_limits)`` or None.

    ``stage_limits`` is a dict keyed by program operation index with values
    ``{inflight_rows, inflight_bytes, output_bytes}``; it is passed to
    ``run_stream`` so the sealed per-operation budgets reach the actual queues.
    ``checkpoint_policy`` is B's readonly CheckpointPolicy dataclass (or None).
    """
    execution_plan = launch_spec.get("execution_plan")
    if execution_plan is None:
        return None

    from ...planning.plan_interpreter import interpret_physical_plan

    bindings_ref = execution_plan["bindings_ref"]
    group_id = execution_plan["group_id"]
    action_id = execution_plan["action_id"]

    path = authority.directory / bindings_ref["path"]
    raw = path.read_bytes()
    declared_bytes = bindings_ref.get("bytes")
    if declared_bytes is not None and len(raw) > declared_bytes:
        raise RecoveryContractError("Execution bindings artifact exceeds declared size")
    if len(raw) > _MAX_BINDINGS_BYTES:
        raise RecoveryContractError("Execution bindings artifact exceeds admission budget")
    if hashlib.sha256(raw).hexdigest() != bindings_ref["sha256"]:
        raise RecoveryContractError("Execution bindings artifact digest differs")
    bindings = json.loads(raw)

    facts = authority.journal.source_binding(action_id + "-sealed-plan")
    if facts is None:
        raise RecoveryContractError("Sealed plan facts are missing for action " + action_id)
    logical_dag = _load_ref(authority, facts["logical_dag_ref"])
    physical = _load_ref(authority, facts["physical_plan_ref"])

    layout = interpret_physical_plan(logical_dag, physical, bindings)

    placement = None
    for candidate in layout.placements:
        if candidate.group_id == group_id:
            placement = candidate
            break
    if placement is None:
        raise RecoveryContractError(
            "Worker group not found in execution layout: " + str(group_id))

    assigned = set(assignment.get("stages") or ())
    program_occurrences = {op.occurrence_id for op in placement.program}
    if assigned and assigned != program_occurrences:
        raise RecoveryContractError(
            "Assignment stages do not match the sealed program occurrences")

    stage_limits = {}
    for op in placement.program:
        limits = getattr(op, "stage_limits", None)
        if limits is not None:
            stage_limits[op.program_index] = dict(limits)

    return placement, placement.checkpoint_policy, stage_limits


def _load_ref(authority, ref):
    path = authority.directory / ref["path"]
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
        raise RecoveryContractError("Sealed plan artifact digest differs: " + ref["path"])
    return json.loads(raw)