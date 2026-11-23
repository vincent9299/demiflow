"""Pure backend/composition rules, shared with the native lowering boundary.

This module neither schedules work nor grants workspace effect permissions.
Facts describe native semantics, never public API spellings.
"""
VERSION = "demiflow_execution_capabilities_v1"


def check_action(kind, arguments=None):
    """Shared declaration/runtime gate; no alternate checkpoint execution path."""
    arguments = arguments or {}
    retired = (kind in {'open_checkpoint', 'read_checkpoint', 'checkpoint',
                       'checkpoint_async', 'checkpoint_lance', 'checkpoint_lance_async',
                       'stream_checkpoint', 'operator_checkpoint'} or
               kind in {'run_stream', 'async_map', 'batch_map'}
               and (arguments.get('checkpoint') is not None
                    or arguments.get('checkpoint_operator') is not None
                    or arguments.get('checkpoint_lease_s', 300.0) != 300.0))
    return {'version': VERSION, 'supported': not retired,
            'reason': (f'Explicit checkpoint orchestration is retired: {kind}; native run recovery has one protocol; '
                       'stage outputs require their P3 adapters') if retired else None}


def require_action(kind, arguments=None):
    """Apply the same capability gate to native entry points before any work."""
    capability = check_action(kind, arguments)
    if not capability['supported']:
        from .recovery_contract import RecoveryContractError
        raise RecoveryContractError(capability['reason'])


def check_call(fact, *, backend="local", driver="pull"):
    """Project checked vocabulary onto the sole unified operator registry.

    ``driver`` remains a historical report label, never a second executor gate.
    Workspace effect permissions and actual native parameter checks stay with
    their existing semantic/resource contracts.
    """
    from ..planning.dataflow_registry import operator_registry
    aliases = {
        'map': 'MapOp', 'async_map': 'AsyncMapOp', 'batch_map': 'BatchMapOp',
        'flat_map': 'FlatMapOp', 'filter': 'FilterOp', 'project': 'SelectColumnsOp',
        'drop': 'DropColumnsOp', 'rename': 'RenameColumnsOp', 'limit': 'LimitOp',
        'map_batches': 'MapBatchesOp', 'predict': 'PredictOp', 'add_column': 'AddColumnOp',
        'sample': 'RandomSampleOp', 'model_map': 'OperatorLLMMapOp',
        'sort': 'SortOp', 'repartition': 'RepartitionOp',
        'shuffle': 'RandomShuffleOp', 'block_order': 'RandomizeBlockOrderOp',
    }
    kind = fact['kind']
    registry = operator_registry()
    native_kind = aliases.get(kind, kind)
    reason = None
    if backend != 'local':
        reason = 'Backend has no checked capability contract'
    elif driver not in {'pull', 'stream'}:
        reason = 'Unknown native driver'
    elif native_kind not in registry:
        reason = 'Operation has no unified compiler capability contract'
    elif fact.get('native_options'):
        from ..data.native_options import resource_only_options
        declaration = fact.get('resource_declaration')
        try:
            if declaration is None or resource_only_options(
                    {'ray': {'worker_resources': declaration}}) != declaration:
                raise ValueError('resource declaration is missing or not canonical')
        except (ValueError, TypeError):
            reason = 'Managed backend_options require an explicit resource declaration'
    return {'version': VERSION, 'backend': backend, 'driver': driver,
            'supported': reason is None, 'reason': reason}


def check_composition(facts, *, backend="local", driver=None):
    facts = list(facts)
    if len(facts) > 1024:
        return {"version": VERSION, "backend": backend, "driver": driver,
                "supported": False, "reason": "Composition exceeds 1024 operations"}
    driver = driver or ("stream" if any(f.get("driver") == "stream" for f in facts) else "pull")
    for index, fact in enumerate(facts):
        result = check_call(fact, backend=backend, driver=driver)
        if not result["supported"]:
            return {**result, "operation": index}
    return {"version": VERSION, "backend": backend, "driver": driver,
            "supported": True, "reason": None}


def validate_stream_plan(plan, *, source=None):
    """Validate all declared inputs through the same v3 compiler, without I/O.

    A relational Dataset may have no top-level operations while its PlanInputs
    contain the stream declaration. Traverse the compiler's actual input ports;
    neither a second capability table nor an obsolete pull/stream split decides
    which registered kernels can be composed.
    """
    from ..data import plan as ops
    from ..planning.dataflow_compile import (compile_kernel_spec,
        expand_relational_inputs, expand_operation_inputs)
    from ..planning.dataflow_policy import resolve_runtime_policy
    from .contracts.dataflow_errors import DataflowError
    from ..data.native_options import validate_stream_native_options

    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    pending, seen = [(source, plan, ())], set()
    count, edges, streaming = 0, 0, False
    while pending:
        current_source, current_plan, path = pending.pop()
        key = (id(current_source), id(current_plan))
        if key in seen:
            continue
        seen.add(key)
        if not isinstance(current_plan, ops.LogicalPlan):
            raise TypeError('stream input requires a LogicalPlan')
        count += 1 + len(current_plan.operations)
        if count > bounds.max_nodes:
            raise DataflowError('BUDGET_EXCEEDED', 'stream preflight exceeds node budget')
        children = list(expand_relational_inputs(current_source, path=path, bounds=bounds))
        for operation in current_plan.operations:
            streaming |= ops.is_stream_operation(operation)
            validate_stream_native_options(getattr(operation, 'native_options', None))
            if isinstance(operation, (ops.MapOp, ops.BoundMapOp)) and operation.stream_options is not None:
                operation.stream_options.validate_callable(operation.callable)
            compile_kernel_spec(operation, program_ref=None, bounds=bounds)
            children.extend(expand_operation_inputs(operation, path=path, bounds=bounds))
            if edges + len(children) > bounds.max_edges:
                raise DataflowError('BUDGET_EXCEEDED', 'stream preflight exceeds edge budget')
        edges += len(children)
        if edges > bounds.max_edges:
            raise DataflowError('BUDGET_EXCEEDED', 'stream preflight exceeds edge budget')
        pending.extend((item.source, item.plan, item.path) for item in children)
    if not streaming:
        raise ValueError('run_stream requires map_async/batch_map or explicit map streaming options')


def stream_residency(facts, limits):
    """Conservative platform payload admission, not a promise about process RSS.

    Each worker reserves input/batch and a completed result while downstream is
    blocked. Queues have separate reservations; dequeuing never erases ownership.
    Candidate internal temporaries remain protected by the enclosing process.
    """
    row_bytes = limits['max_row_bytes']
    batch_bytes = limits['max_batch_bytes']
    # Materialization cache plus bounded control state, and reader/serialization
    # buffers. Native libraries and candidate temporaries still need the process
    # guard; this reservation describes platform-owned Python payload.
    fixed = max(80 * 1024**2, 32 * 1024**2 + 3 * limits.get("max_state_bytes", 16 * 1024**2)) + 3 * batch_bytes
    facts = list(facts)
    if any(f.get("native", {}).get("contract") == "demiflow.prompt.http" for f in facts):
        # Recovery index, output encoding and atomic batch publication buffers.
        fixed += min(512 * 1024**2, limits["memory_bytes"] // 8) + 8 * 1024**2 + 2 * row_bytes
    total = fixed
    stages = []
    for fact in facts:
        args = fact['arguments']
        concurrency = args.get('concurrency') or 1
        depth = args.get('queue_depth') or concurrency
        batch_rows = args.get('max_batch') or 1
        if any(type(value) is not int or value < 1 for value in (concurrency, depth, batch_rows)):
            raise ValueError('Stream concurrency, queue depth and batch rows must be positive integers')
        if concurrency > 64 or depth > 256 or batch_rows > limits['batch_rows']:
            raise ValueError('Stream worker/queue/batch admission limit exceeded')
        # A batch may be small enough by row count yet exceed its byte budget;
        # workers check that budget before appending each incoming row.
        inputs = min(batch_rows * row_bytes, batch_bytes) if fact['kind'] == 'batch_map' else row_bytes
        # Retain the original expansion while a post-filter list/serialized
        # child is awaiting downstream capacity.
        output = 2 * batch_bytes if fact['kind'] in {'async_map', 'batch_map', 'flat_map'} else row_bytes
        reserved = (depth * (row_bytes + 1024) + concurrency * (inputs + output + 4096)
                    + fact.get('reserved_bytes', 0))
        stages.append({'kind': fact['kind'], 'reserved_bytes': reserved})
        total += reserved
    if total > limits['memory_bytes']:
        raise ValueError(f'Stream payload reservation {total} exceeds memory budget {limits["memory_bytes"]}')
    return {'reserved_bytes': total, 'fixed_bytes': fixed, 'stages': stages,
            'assurance': 'platform_payload_admission; third_party_temporaries_require_process_guard'}
