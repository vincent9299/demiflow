"""Public v3 action facade: declaration, native ops, explain and run (E015-E019).

Lane E. These are the only public entry points the Local/Ray Dataset executors
consume. ``execute_action`` (E001) remains the single orchestration entry; this
module never re-allocates physical resources, never starts a second run, and
never imports Ray Data. Local and Ray differ only by ``provider``. Declarations
are pure and bounded; live clients/models/journals are assembled later in the
worker, never here.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from demiflow.execution.contracts.dataflow import ActionProgram, ActionRequest, ArtifactRef, Bounds, CapabilityReport, NativeOperation, RuntimePolicy
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import execute_action
from demiflow.execution.seal.program_identity import describe, operation_identity, plan_identity, source_identity
from demiflow.execution.pipeline.run_context import RunAction, current_action, current_run
from demiflow.data.plan import CallableSpec, LogicalOp, LogicalPlan
from demiflow.data.sources import SourcePlan
from demiflow.planning.dataflow_declaration import normalize_json_mapping as _json_mapping, normalize_native_fields, validate_native_kind

if TYPE_CHECKING:
    from demiflow.data.datasink import Datasink
    from demiflow.execution.executors.base import DatasetExecutor

_ACTION_KINDS = frozenset({
    "take", "take_all", "take_batch", "iter_rows", "iter_batches", "count",
    "aggregate", "schema", "columns", "size_bytes", "materialize",
    "write_datasink", "write_lance", "write_file", "run_stream", "collect_summary",
})
_PROVIDER = {"local": "local", "ray": "ray_core"}



def _native_operation_factory(*args: object, **kwargs: object) -> object:
    # Lazy declaration marker: the worker runtime (M) assembles the real kernel.
    # Invoking it in the driver would mean a live client/model escaped sealing.
    raise DataflowError("UNSUPPORTED_CAPABILITY",
                        "native operation factory must be assembled by the worker runtime")


def _native_operation_identity(operation: NativeOperation) -> dict:
    target = operation.factory.target
    name = target.__module__ + "." + getattr(
        target, "__qualname__", type(target).__name__)
    return {"native": {
        "contract": "demiflow.native.operation.v3", "kind": operation.kind,
        "configuration": describe(dict(operation.configuration)),
        "bindings": describe(dict(operation.bindings)),
        "code_identity": describe(name)}, "policy": {}}


# -- E017 make_native_operation -------------------------------------------
def make_native_operation(kind: str, configuration: Mapping[str, object], *,
                          bindings: Mapping[str, object]) -> LogicalOp:
    config, binds = normalize_native_fields(kind, configuration, bindings=bindings)
    factory = CallableSpec.create(target=_native_operation_factory)
    operation = NativeOperation(kind=kind, configuration=config, bindings=binds, factory=factory)
    validate_native_kind(kind)
    return operation


# -- E019 declare_action_v3 -----------------------------------------------
def declare_action_v3(source: SourcePlan, plan: LogicalPlan, *, kind: str,
                      arguments: Mapping[str, object],
                      sink_declaration: Mapping[str, object] | None) -> Mapping[str, object]:
    if type(kind) is not str or kind not in _ACTION_KINDS:
        raise DataflowError("INVALID_CONTRACT", "unknown action kind: " + str(kind))
    args = _json_mapping(arguments, "arguments")
    operations = tuple(plan.operations) if plan is not None else ()
    if not any(isinstance(op, NativeOperation) for op in operations):
        # Historical plans keep the exact plan_identity projection.
        return plan_identity(kind, source, plan, spec=sink_declaration, arguments=args)
    projected = [_native_operation_identity(op) if isinstance(op, NativeOperation)
                 else operation_identity(op) for op in operations]
    return dict(kind=kind,
                source=source_identity(source) if source is not None else None,
                operations=projected, sink=describe(sink_declaration),
                arguments=describe(args))


# -- E015 make_action_request ---------------------------------------------
def make_action_request(executor: "DatasetExecutor", source: SourcePlan, plan: LogicalPlan, *,
                        kind: str, arguments: Mapping[str, object],
                        sink: "Datasink | None" = None,
                        programs: tuple[ActionProgram, ...] = ()) -> ActionRequest:
    provider = _PROVIDER.get(getattr(executor, "NAME", ""))
    if provider is None:
        raise DataflowError("INVALID_CONTRACT", "unsupported executor provider")
    if getattr(executor, '_local_kernel_closed', False) or getattr(executor, '_closed', False):
        from demiflow.data.sources import MaterializedSource
        from demiflow.execution.outputs.dataflow_handles import StoredMaterializedHandle
        # Published immutable materializations own their original pins and may
        # outlive the context which computed them. Lazy source declarations do
        # not acquire that independent lifetime by retaining an old executor.
        if type(source) is not MaterializedSource or type(source.handle) is not StoredMaterializedHandle:
            raise RuntimeError('Dataset execution context is closed')
        source.handle._check_open()
    if type(kind) is not str or kind not in _ACTION_KINDS:
        raise DataflowError("INVALID_CONTRACT", "unknown action kind: " + str(kind))
    args = _json_mapping(arguments, "arguments")
    for op in (tuple(plan.operations) if plan is not None else ()):
        if isinstance(op, NativeOperation):
            make_native_operation(op.kind, op.configuration, bindings=op.bindings)
    declaration = declare_action_v3(source, plan, kind=kind, arguments=args,
                                    sink_declaration=None)
    action = current_action()
    from demiflow.execution.executors.plan_capture import current_capture
    capture = current_capture()
    if capture is not None:
        action = RunAction(capture.context, capture.action_id, declaration)
    if action is None:
        run = current_run()
        if run is None:
            raise DataflowError("INVALID_CONTRACT",
                                "make_action_request requires an execution scope")
        # Reuse the caller's run; bind admission exactly once, never nest a run.
        with run.action(declaration) as created:
            action = created
    return ActionRequest(kind=kind, source=source, plan=plan, arguments=args, sink=sink,
                         programs=tuple(programs), provider=provider, action=action,
                         executor=executor)








# -- E016 run_dataset_action ----------------------------------------------
def run_dataset_action(request: ActionRequest) -> object:
    from demiflow.execution.action_driver.services import resolve_action_runtime
    from demiflow.execution.executors.plan_capture import current_capture
    capture = current_capture()
    if capture is not None:
        # Sink lifecycle hooks are execution, even before driver services exist.
        policy, bounds = resolve_action_runtime(request)
        capture.intercept(request, policy=policy, bounds=bounds)
    if request.kind != 'write_datasink':
        return _run_dataset_action(request)
    sink = request.sink
    started = False

    def start_sink():
        nonlocal started
        started = True
        sink.on_write_start()

    try:
        if request.action.replaying:
            # Authenticating the original sealed program may reject a resume.
            # Until it succeeds even the failure hook is a business side effect.
            result = _run_dataset_action(request, after_prepare=start_sink)
        else:
            start_sink()
            result = _run_dataset_action(request)
    except BaseException as failure:
        if not started:
            raise
        try:
            sink.on_write_failed(failure)
        except BaseException as callback:
            failure.add_note('Datasink failure callback also failed: ' + str(callback))
        raise
    # Publication and the original ACK have completed. A callback failure must
    # not invoke on_write_failed or suggest that the sink was not committed.
    try:
        sink.on_write_complete(result)
    except BaseException as callback:
        callback.add_note('Datasink completion callback failed after confirmed publication; the original commit remains valid')
        raise
    return result


def _run_dataset_action(request: ActionRequest, *, after_prepare=None) -> object:
    from demiflow.execution.action_driver.services import resolve_action_runtime
    from demiflow.execution.journal.run_telemetry import record_execution_metadata
    policy, bounds = resolve_action_runtime(request)
    if after_prepare is None:
        handle = execute_action(request, policy=policy, bounds=bounds)
    else:
        handle = execute_action(request, policy=policy, bounds=bounds, after_prepare=after_prepare)
    from demiflow.execution.outputs.terminals import deliver_action_result
    from demiflow.execution.contracts.dataflow import DELIVERY_LIMIT
    def observed(iterator):
        try:
            yield from iterator
        finally:
            record_execution_metadata(request, handle._execution)
    # P1 retains the existing action until its lazy public result is exhausted or closed.
    native_delivery = request.kind == "take" and DELIVERY_LIMIT in request.arguments
    if request.kind in ("iter_rows", "iter_batches") or native_delivery:
        return observed(deliver_action_result(request, handle, bounds=bounds))
    primary_failure = None
    try:
        return deliver_action_result(request, handle, bounds=bounds)
    except BaseException as exc:
        primary_failure = exc
        raise
    finally:
        # Drain carries final failed/cancelled counters. Always publish after
        # close, retaining the original action/cleanup error if publication
        # itself fails (as in the streaming terminal's cleanup sequence).
        failure = primary_failure
        for label, cleanup in (
                ('Action cleanup also failed: ', handle.close),
                ('Execution metadata also failed: ', lambda: record_execution_metadata(request, handle._execution))):
            try:
                cleanup()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                elif exc is not failure:
                    failure.add_note(label + str(exc))
        if failure is not None and primary_failure is None:
            raise failure


class _CompileContext:
    """Pure compile-only context for explain: no journal, admission or worker."""
    fence = None
    attempt = 0
    limits: dict = {}


# -- E018 explain_dataset -------------------------------------------------
def explain_dataset(source: SourcePlan, plan: LogicalPlan, *, provider: str,
                    options: Mapping[str, object]) -> CapabilityReport:
    from demiflow.planning.dataflow_policy import explain_policy
    policy, bounds = explain_policy(provider, options, action_kinds=_ACTION_KINDS)
    # A capability report does not place source tasks/sink writers. Their caps
    # are validated declarations; actual feasibility remains a run-time check.
    declaration = declare_action_v3(source, plan, kind="collect_summary",
                                    arguments={}, sink_declaration=None)
    action = RunAction(_CompileContext(), "explain", declaration)
    request = ActionRequest(kind="collect_summary", source=source, plan=plan,
                            arguments={}, sink=None, programs=(), provider=provider,
                            action=action)
    from demiflow.execution.compiler_support.dataflow_compile_inputs import compile_inputs
    from demiflow.planning.dataflow_compile import explain_action
    inputs = compile_inputs(request)
    # No snapshots are taken here: read domains remain pending run-time
    # resolution rather than being claimed verified.
    return explain_action(request, inputs=inputs, bounds=bounds, policy=policy)
