"""Common registered-input admission; preparation remains the execution gate."""

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs.limits import limits_with
from demiflow.forge.schema import ForgeError


def inputs(root, workspace, entry, params, limits):
    admitted = limits_with(limits, policy=workspace.get("budgets"))
    path = ws.validate_parameter_path(root, params) if isinstance(params, str) else None
    if workspace["format"] in ws.MODERN_FORMATS:
        if entry not in workspace["entries"]:
            raise ForgeError(
                "Managed execution requires an explicitly registered Flow entry"
            )
        if path and params not in workspace["entries"][entry]:
            raise ForgeError("Parameter file is not registered for this Flow entry")
    return admitted, path


def describe(root, entry, *, params, limits=None):
    from demiflow.workbench.worker import operation
    from demiflow.workbench.runs.domain import preparation_lock

    with preparation_lock() as lease:
        return operation(
            "run_admission",
            root,
            entry=entry,
            params=params,
            limits=limits,
            _pass_fds=(lease,),
        )


def _describe(root, entry, *, params, limits=None):
    root, workspace, _ = ws.load_workspace(root)
    admitted, path = inputs(root, workspace, entry, params, limits)
    parameters = ws.read_control(path) if path else params
    arguments = ws.resolve_arguments(root, workspace, parameters)
    pending = [parameters.get('arguments', {})]
    outputs = 0
    while pending:
        value = pending.pop()
        if type(value) is dict:
            if value.get('kind') == 'output':
                outputs += 1
            else:
                pending.extend(value.values())
        elif type(value) is list:
            pending.extend(value)
    # Never claim that discovery or this inexpensive prerequisite projection
    # replaces the checker on effective output bindings and sealed sources.
    return dict(
        format="demiflow_forge_runtime_admission_v1",
        status="requires_prepare",
        entry=entry,
        parameter_path=params if path else None,
        limits=admitted,
        execution="native_local_only",
        output_binding='explicit' if outputs else 'compiled_targets',
        queue=False,
        checks_pending=[
            "effective_native_check",
            "source_and_environment_seal",
            "fixed_inputs_and_outputs",
            "deployment_capacity",
            "write_ahead_reservations",
        ],
        resources=dict(
            scratch="active_private",
            evidence="retained_recovery",
            history="seals_journal_logs_metrics_diagnostics",
            outputs="native_committed_versions",
        ),
    )
