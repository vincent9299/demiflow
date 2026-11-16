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
    from ..planning.dataflow_registry import operator_registry
    # Public declarations may lower to an existing typed op. Validate their
    # names here; only the compiler with the full typed declaration builds the
    # model spec (a prompt name alone cannot reconstruct a PromptPack).
    aliases = {"embeddings": "embedding", "fetch": "raw_fetch", "enqueue": "queue_produce",
               "ack_queue": "queue_ack", "lookup": "lance_lookup", "prepend": "stream_prepend",
               "read_object": "read_local_object", "admit": "deduplicate"}
    if aliases.get(kind, kind) not in operator_registry() and kind not in {"cache", "run_stream"}:
        raise DataflowError("UNSUPPORTED_CAPABILITY", "unknown native operation: " + kind)
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
        from ..data.sources import MaterializedSource
        from .dataflow_handles import StoredMaterializedHandle
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
    if action is None:
        run = current_run()
        if run is None:
            raise DataflowError("INVALID_CONTRACT",
                                "make_action_request requires an execution scope")
        # Reuse the caller's run; bind admission exactly once, never nest a run.
        with run.action(declaration) as created:
            action = created
    action._dataflow_executor = executor
    return ActionRequest(kind=kind, source=source, plan=plan, arguments=args, sink=sink,
                         programs=tuple(programs), provider=provider, action=action)


def record_execution_metadata(request, execution):
    """Project only observed workers/counters and confirmed completion facts."""
    executor = getattr(request.action, '_dataflow_executor', None)
    if executor is None:
        return
    from .dataflow_usage import publish_prompt_usage
    publish_prompt_usage(executor, execution)
    from ..data.stats import ExecutionMetadata, StageStats
    graph, physical = execution._graph, execution._physical
    kinds = {node.occurrence_id: node.kernel.kind for node in graph.nodes} if graph is not None else {}
    observations = [{'name': kinds[occ], 'occurrence_id': occ, 'partition_id': partition,
                     'rows_input': values['rows_input'], 'rows_output': values['rows_output']}
                    for (occ, partition), values in execution._node_metrics.items() if occ in kinds]
    stages = tuple(StageStats(name=item['occurrence_id'] + ':' + item['partition_id'],
        rows_input=item['rows_input'], rows_output=item['rows_output']) for item in observations)
    workers = list(getattr(execution, '_observed_workers', {}).values())
    failure = getattr(execution, '_failure', None)
    if failure is not None:
        import asyncio
        status = 'cancelled' if isinstance(failure, asyncio.CancelledError) else 'failed'
    elif execution._state == 'succeeded':
        result = execution._terminal_result
        status = 'closed_early' if result is not None and result.completion == 'terminal_satisfied' else 'complete'
    else:
        status = execution._state
    diagnostics = {'state': execution._state, 'status': status, 'engine': 'dataflow_v3',
        'dag_id': graph.dag_id if graph is not None else None,
        'plan_id': physical.plan_id if physical is not None else None,
        'stages': observations, 'workers': workers,
        'workers_used': [(item['worker_process_id'], item['runner_thread_id']) for item in workers],
        'observed_worker_count': getattr(execution, '_observed_worker_count', len(workers)),
        'worker_details_truncated': getattr(execution, '_observed_worker_count', len(workers)) > len(workers),
        'peak_active_workers': getattr(execution, '_peak_active_workers', 0),
        'action_id': request.action.action_id}
    if failure is not None:
        diagnostics['failure_type'] = type(failure).__module__ + '.' + type(failure).__qualname__
    if physical is not None:
        live_bounds = getattr(execution, '_bounds', physical.bounds)
        diagnostics['admitted_bounds'] = {'max_row_bytes': live_bounds.max_row_bytes,
            'max_batch_bytes': live_bounds.max_batch_bytes,
            'max_resident_bytes': live_bounds.max_resident_bytes,
            'max_spill_bytes': live_bounds.max_spill_bytes,
            'max_output_rows': live_bounds.max_output_rows,
            'timeout_ms': live_bounds.timeout_ms,
            'max_active_workers': getattr(execution, '_active_worker_limit', physical.policy.max_active_workers)}
    native = getattr(execution, '_native_summary', None)
    if native is not None:
        if execution._acquisition.binding.mode == 'native_search':
            diagnostics.update(native)
        else:
            diagnostics['queue'] = dict(native)
    executor._run_ref = request.action.context.ref
    executor._last_metadata = ExecutionMetadata(source_name=type(request.source).__name__,
        stages=stages, rows_output=execution._confirmed_rows, diagnostics=diagnostics,
        run_ref=request.action.context.ref)
    if hasattr(executor, '_local_kernel_stats'):
        executor._local_kernel_stats = {**diagnostics, 'rows_output': execution._confirmed_rows}


def resolve_action_runtime(request: ActionRequest):
    """Use the same public execution envelope for every terminal, including streams."""
    executor = getattr(request.action, '_dataflow_executor', None)
    local = getattr(executor, '_local_kernel', None)
    options = {}
    workers = getattr(executor, '_workers', None)
    if workers is not None:
        options['max_active_workers'] = workers
    if local is not None:
        options['memory_bytes'] = local.memory_bytes
    from .dataflow_thread_programs import select_worker_mode
    mode = select_worker_mode(request, local)
    if mode is not None:
        options['worker_mode'] = mode
    run = request.action.context
    limits = dict(run.limits)
    explicit = set(run.explicit_limits)
    from .dataflow_native_limits import EXECUTION_LIMITS, execution_ceilings, intersect_native_limits
    ceilings = execution_ceilings(request.arguments.get(EXECUTION_LIMITS))
    if ceilings:
        for key, source_key in (('memory_bytes', 'memory_bytes'), ('max_scratch_bytes', 'scratch_bytes')):
            if source_key in ceilings:
                limits[key] = min(limits[key], ceilings[source_key])
                explicit.add(key)
        if 'memory_bytes' in ceilings:
            options['memory_bytes'] = min(options.get('memory_bytes', limits['memory_bytes']), ceilings['memory_bytes'])
        for key in ('inflight_bytes', 'output_bytes'):
            if key in ceilings:
                for name in ('max_row_bytes', 'max_batch_bytes'):
                    limits[name] = min(limits[name], ceilings[key])
                    explicit.add(name)
        if 'cpu_millis' in ceilings:
            count = ceilings['cpu_millis'] // 1000
            options['max_active_workers'] = min(options.get('max_active_workers', count), count)
    declared = getattr(executor, '_native_resources', None)
    if declared is None:
        declared = getattr(local, 'native_resources', None)
    native = ({'batch_rows': min(ceilings['inflight_rows'], limits['batch_rows'])}
              if 'inflight_rows' in ceilings else None)
    if declared is not None:
        from .dataflow_native_limits import effective_native_limits
        native = intersect_native_limits(native, effective_native_limits(declared, limits))
    if native is not None:
        for name in ('memory_bytes', 'max_rss_bytes', 'max_scratch_bytes', 'max_row_bytes', 'batch_rows'):
            if name in native:
                limits[name] = native[name]
                explicit.add(name)
        if 'memory_bytes' in native:
            options['memory_bytes'] = min(options.get('memory_bytes', limits['memory_bytes']), native['memory_bytes'])
    policy, bounds = resolve_runtime_policy(options, limits, provider=request.provider,
                                           explicit_limits=tuple(explicit))
    if local is None and not request.action.replaying:
        from ..planning.dataflow_policy import fit_default_stream_windows
        bounds = fit_default_stream_windows(request, policy=policy, bounds=bounds,
                                            explicit_limits=tuple(explicit))
    if native is not None:
        from dataclasses import replace
        policy = replace(policy, native_limits=native)
        if 'timeout_s' in native:
            bounds = replace(bounds, timeout_ms=min(bounds.timeout_ms, max(1, int(native['timeout_s'] * 1000))))
    return policy, bounds


# -- E016 run_dataset_action ----------------------------------------------
def run_dataset_action(request: ActionRequest) -> object:
    if request.kind != 'write_datasink':
        return _run_dataset_action(request)
    sink = request.sink
    try:
        sink.on_write_start()
        result = _run_dataset_action(request)
    except BaseException as failure:
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


def _run_dataset_action(request: ActionRequest) -> object:
    policy, bounds = resolve_action_runtime(request)
    handle = execute_action(request, policy=policy, bounds=bounds)
    from .dataflow_terminals import (collect_rows, iterate_rows, iterate_batches,
                                     materialized_handle, write_result, inspect_result,
                                     _convert_batch)
    arguments = request.arguments
    def observed(iterator):
        try:
            yield from iterator
        finally:
            record_execution_metadata(request, handle._execution)
    # Streaming adapters retain the action handle until exhaustion or close.
    from .dataflow_native_limits import DELIVERY_LIMIT
    native_delivery = request.kind == "take" and DELIVERY_LIMIT in arguments
    if request.kind == "iter_rows" or (native_delivery and arguments.get('batch_format') != 'pyarrow'):
        return observed(iterate_rows(handle))
    if request.kind == "iter_batches" or native_delivery:
        batch_size = arguments.get('batch_size')
        if arguments.get('native_execution_limits', {}).get('inflight_rows'):
            ceiling = (handle._execution._native_limits or {}).get('batch_rows', batch_size)
            batch_size = min(batch_size, ceiling)
        return observed(iterate_batches(handle, batch_size=batch_size,
                               batch_format=arguments.get("batch_format", "default"),
                               drop_last=arguments.get("drop_last", False)))
    try:
        if request.kind == "take":
            return collect_rows(handle, limit=arguments.get("limit", 20), bounds=bounds)
        if request.kind == "take_all":
            return collect_rows(handle, limit=arguments.get("limit"), bounds=bounds,
                                verify_complete=True)
        if request.kind == "take_batch":
            from .record_schema import RecordSchemas
            schemas, seen = RecordSchemas(handle.services), []
            def receive_schema(ref):
                schema = schemas.read(ref)
                if not seen:
                    seen.append(schema)
                elif schema != seen[0]:
                    raise DataflowError('INVALID_CONTRACT', 'take_batch cannot mix declared schemas')
            rows = collect_rows(handle, limit=arguments.get("batch_size", 20), bounds=bounds,
                                schema_receiver=receive_schema)
            return _convert_batch(rows, arguments.get("batch_format", "default"), handle,
                                  schema=seen[0] if seen else None)
        result = handle.result()
        if request.kind == "count":
            return result.scalar
        if request.kind == "aggregate":
            import base64
            from .kernels import state_codec
            return state_codec.loads(base64.b64decode(result.scalar["typed_result"]),
                                     max_bytes=bounds.max_state_bytes)
        if request.kind == "materialize":
            return materialized_handle(result, execution=handle._execution)
        if request.kind in {"write_datasink", "write_lance", "write_file"}:
            return write_result(result, kind=request.kind)
        if request.kind in {"schema", "columns", "size_bytes", "collect_summary"}:
            return inspect_result(handle, kind=request.kind)
        return result
    finally:
        try:
            record_execution_metadata(request, handle._execution)
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
    metadata_fields = {'action_kind', 'terminal_category', 'terminal_native_options',
                       'source_parallelism_cap', 'terminal_parallelism_cap'}
    if not isinstance(options, Mapping):
        raise DataflowError('INVALID_CONTRACT', 'explain options must be a mapping')
    # Executor plan() carries declaration metadata alongside runtime policy.
    # Only these closed metadata fields are separated; unknown policy keys still
    # pass to the canonical validator and fail rather than being swallowed.
    runtime_options = {key: value for key, value in options.items() if key not in metadata_fields}
    policy, bounds = resolve_runtime_policy(runtime_options, {}, provider=provider)
    action_kind = options.get('action_kind', 'collect_summary')
    if type(action_kind) is not str or action_kind not in _ACTION_KINDS:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'unknown explain action kind')
    category = options.get('terminal_category', 'action')
    if type(category) is not str or category not in ('action', 'sink'):
        raise DataflowError('INVALID_CONTRACT', 'unknown explain terminal category')
    for name in ('source_parallelism_cap', 'terminal_parallelism_cap'):
        value = options.get(name)
        if value is not None and (type(value) is not int or not 1 <= value <= bounds.max_partitions):
            raise DataflowError('INVALID_CONTRACT', name + ' must be within the partition budget')
    if options.get('terminal_native_options') is not None:
        from ..planning.dataflow_policy import normalize_native_options
        normalize_native_options(options['terminal_native_options'], operation_kind=action_kind)
    # A capability report does not place source tasks/sink writers. Their caps
    # are validated declarations; actual feasibility remains a run-time check.
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
