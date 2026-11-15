"""Public v3 action facade: declaration, native ops, explain and run (E015-E019).

Lane E. These are the only public entry points the Local/Ray Dataset executors
consume. ``execute_action`` (E001) remains the single orchestration entry; this
module never re-allocates physical resources, never starts a second run, and
never imports Ray Data. Local and Ray differ only by ``provider``. Declarations
are pure and bounded; live clients/models/journals are assembled later in the
worker, never here.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from .contracts.dataflow import (
    ActionProgram, ActionRequest, ArtifactRef, Bounds, CapabilityReport,
    NativeOperation, RuntimePolicy,
)
from .contracts.dataflow_errors import DataflowError
from .dataflow import execute_action
from .dataflow_models import build_model_factory
from .recovery_identity import (
    describe, operation_identity, plan_identity, source_identity,
)
from .run_context import RunAction, current_action, current_run
from ..data.plan import CallableSpec, LogicalOp, LogicalPlan
from ..data.sources import SourcePlan
from ..planning.dataflow_compile import lower_action
from ..planning.dataflow_place import infer_recovery
from ..planning.dataflow_policy import resolve_runtime_policy

if TYPE_CHECKING:
    from ..data.datasink import Datasink
    from .executors.base import DatasetExecutor

_ACTION_KINDS = frozenset({
    "take", "take_all", "take_batch", "iter_rows", "iter_batches", "count",
    "aggregate", "schema", "columns", "size_bytes", "materialize",
    "write_datasink", "write_lance", "write_file", "run_stream", "collect_summary",
})
_PROVIDER = {"local": "local", "ray": "ray_core"}
_MAX_DECLARATION_BYTES = 1024 * 1024
# Preflight-only bounds for native config validation (C02 default ceilings);
# the real run resolves its own frozen policy/bounds via P008.
_PREFLIGHT_BOUNDS = Bounds(
    max_nodes=4096, max_edges=16384, max_partitions=4096, max_depth=64,
    max_control_bytes=65536, max_row_bytes=16 * 1024 * 1024,
    max_batch_bytes=64 * 1024 * 1024, max_resident_bytes=1024 * 1024 * 1024,
    max_spill_bytes=1024 * 1024 * 1024, max_state_bytes=256 * 1024 * 1024,
    max_pending_events=1024, max_open_files=256, max_output_rows=1000000,
    timeout_ms=60000)


def _json_mapping(value: object, label: str) -> dict:
    if not isinstance(value, Mapping):
        raise DataflowError("INVALID_CONTRACT", label + " must be a mapping")
    try:
        raw = json.dumps(dict(value), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise DataflowError("INVALID_CONTRACT", label + " must be bounded Json") from exc
    if len(raw.encode("utf-8")) > _MAX_DECLARATION_BYTES:
        raise DataflowError("BUDGET_EXCEEDED", label + " exceeds declaration budget")
    return dict(value)


def _native_operation_factory(*args: object, **kwargs: object) -> object:
    # Lazy declaration marker: the worker runtime (M) assembles the real kernel.
    # Invoking it in the driver would mean a live client/model escaped sealing.
    raise DataflowError("UNSUPPORTED_CAPABILITY",
                        "native operation factory must be assembled by the worker runtime")


def _native_operation_identity(operation: NativeOperation) -> dict:
    target = operation.factory.target
    name = getattr(target, "__module__", "") + "." + getattr(
        target, "__qualname__", type(target).__name__)
    return {"native": {
        "contract": "demiflow.native.operation.v3", "kind": operation.kind,
        "configuration": describe(dict(operation.configuration)),
        "bindings": describe(dict(operation.bindings)),
        "code_identity": describe(name)}, "policy": {}}


# -- E017 make_native_operation -------------------------------------------
def make_native_operation(kind: str, configuration: Mapping[str, object], *,
                          bindings: Mapping[str, object]) -> LogicalOp:
    if type(kind) is not str or not kind:
        raise DataflowError("INVALID_CONTRACT", "native operation kind must be a non-empty str")
    config = _json_mapping(configuration, "configuration")
    binds = _json_mapping(bindings, "bindings")
    factory = CallableSpec.create(target=_native_operation_factory)
    operation = NativeOperation(kind=kind, configuration=config, bindings=binds, factory=factory)
    # Pure config/identity preflight: reports configuration errors before any
    # run, without constructing a journal, network client or model instance.
    build_model_factory(operation, program_ref=None, bounds=_PREFLIGHT_BOUNDS)
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
    if type(kind) is not str or kind not in _ACTION_KINDS:
        raise DataflowError("INVALID_CONTRACT", "unknown action kind: " + str(kind))
    args = _json_mapping(arguments, "arguments")
    for op in (tuple(plan.operations) if plan is not None else ()):
        if isinstance(op, NativeOperation):
            make_native_operation(op.kind, op.configuration, bindings=op.bindings)
    declaration = declare_action_v3(source, plan, kind=kind, arguments=args,
                                    sink_declaration=None)
    action = current_action()
    if action is None:
        run = current_run()
        if run is None:
            raise DataflowError("INVALID_CONTRACT",
                                "make_action_request requires an execution scope")
        # Reuse the caller's run; bind admission exactly once, never nest a run.
        with run.action(declaration) as created:
            action = created
    return ActionRequest(kind=kind, source=source, plan=plan, arguments=args, sink=sink,
                         programs=tuple(programs), provider=provider, action=action)


# -- E016 run_dataset_action ----------------------------------------------
def run_dataset_action(request: ActionRequest) -> object:
    policy, bounds = resolve_runtime_policy({}, request.action.context.limits, provider=request.provider)
    handle = execute_action(request, policy=policy, bounds=bounds)
    from .dataflow_terminals import (collect_rows, iterate_rows, iterate_batches,
                                     materialized_handle, write_result, inspect_result,
                                     _convert_batch)
    arguments = request.arguments
    # Streaming adapters retain the action handle until exhaustion or close.
    if request.kind == "iter_rows":
        return iterate_rows(handle)
    if request.kind == "iter_batches":
        return iterate_batches(handle, batch_size=arguments.get("batch_size"),
                               batch_format=arguments.get("batch_format", "default"),
                               drop_last=arguments.get("drop_last", False))
    try:
        if request.kind == "take":
            return collect_rows(handle, limit=arguments.get("limit", 20), bounds=bounds)
        if request.kind == "take_all":
            limit = arguments.get("limit")
            rows = collect_rows(handle, limit=None if limit is None else limit + 1, bounds=bounds)
            if limit is not None and len(rows) > limit:
                raise ValueError("Dataset contains more than the take_all limit")
            return rows
        if request.kind == "take_batch":
            rows = collect_rows(handle, limit=arguments.get("batch_size", 20), bounds=bounds)
            return _convert_batch(rows, arguments.get("batch_format", "default"), handle)
        result = handle.result()
        if request.kind in {"count", "aggregate"}:
            return result.scalar
        if request.kind == "materialize":
            return materialized_handle(result, execution=handle._execution)
        if request.kind in {"write_datasink", "write_lance", "write_file"}:
            return write_result(result, kind=request.kind)
        if request.kind in {"schema", "columns", "size_bytes", "collect_summary"}:
            return inspect_result(handle, kind=request.kind)
        return result
    finally:
        handle.close()


class _CompileContext:
    """Pure compile-only context for explain: no journal, admission or worker."""
    fence = None
    attempt = 0
    limits: dict = {}


# -- E018 explain_dataset -------------------------------------------------
def explain_dataset(source: SourcePlan, plan: LogicalPlan, *, provider: str,
                    options: Mapping[str, object]) -> CapabilityReport:
    if provider not in ("local", "ray_core"):
        raise DataflowError("INVALID_CONTRACT", "unknown provider: " + str(provider))
    policy, bounds = resolve_runtime_policy(options, {}, provider=provider)
    declaration = declare_action_v3(source, plan, kind="collect_summary",
                                    arguments={}, sink_declaration=None)
    action = RunAction(_CompileContext(), "explain", declaration)
    request = ActionRequest(kind="collect_summary", source=source, plan=plan,
                            arguments={}, sink=None, programs=(), provider=provider,
                            action=action)
    compiled = lower_action(request, bounds=bounds)
    # No snapshots are taken here: read domains remain pending run-time
    # resolution rather than being claimed verified.
    return infer_recovery(compiled.graph, (), policy=policy)
