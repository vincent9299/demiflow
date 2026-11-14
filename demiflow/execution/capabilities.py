"""Pure backend/composition rules, shared with the native lowering boundary.

This module neither schedules work nor grants workspace effect permissions.
Facts describe native semantics, never public API spellings.
"""
from dataclasses import dataclass
from typing import Literal

VERSION = "demiflow_execution_capabilities_v1"


@dataclass(frozen=True)
class ActionDecision:
    """Static admission outcome; descriptive metadata, not a user mode switch."""
    action_kind: str
    backend: Literal['local_pull', 'local_stream', 'datafusion', 'arrow_batch', 'lance_scan']
    route: Literal['pull', 'stream_delivery', 'stream_materialize', 'native_relational',
                   'arrow_batch', 'lance_scan', 'queue_stream', 'static_graph']
    recovery: Literal['native_checkpoint', 'final_commit', 'delivery_only', 'attempt_only']


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
    reason = None
    kind = fact["kind"]
    if backend != "local":
        reason = "Backend has no checked capability contract"
    elif driver == "stream":
        if kind not in {"map", "async_map", "batch_map", "flat_map", "filter", "project", "drop", "rename"}:
            reason = "streaming does not support this operation; materialize the synchronous prefix first"
        elif fact.get("native_options"):
            reason = "streaming does not support backend_options"
    elif driver != "pull":
        reason = "Unknown native driver"
    elif kind in {"async_map", "batch_map"} or fact.get("stream_options"):
        reason = "Stream operations require a streaming action or materialize"
    elif kind not in {"map", "filter", "flat_map", "map_batches", "limit", "project", "drop", "rename", "add_column", "sample", "model_map"}:
        reason = "Operation has no pull backend capability contract"
    return {"version": VERSION, "backend": backend, "driver": driver,
            "supported": reason is None, "reason": reason}


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


_ACTION_KINDS = frozenset({
    'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'count',
    'aggregate', 'schema', 'size_bytes', 'materialize', 'write_datasink',
    'write_lance', 'write_file', 'run_stream',
})
_STREAM_TERMINALS = frozenset({
    'run_stream', 'materialize', 'write_lance', 'write_datasink',
    'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'count',
})
_MATERIALIZE_TERMINALS = frozenset({'materialize', 'write_lance', 'write_datasink'})


def _is_queue_source(source):
    from ..data.sources import DatasourceSource
    from .queue_source import QueueDatasource
    return type(source) is DatasourceSource and type(source.datasource) is QueueDatasource


def admit_action_plan(source, plan, *, action_kind, backend='local', sink_spec=None,
                      available_backends=()):
    """Pure static admission and route decision (C03).

    Reuses the shared capability gates; never reads the source, constructs an
    actor, opens a writer or claims a queue. ``available_backends`` is the
    executor's declared capability, not a runtime probe; absent declarations
    select only plain local routes.
    """
    from ..data import plan as ops
    from ..errors import UnsupportedExecutionOptionError
    from .plan_facts import operation_capability

    if action_kind not in _ACTION_KINDS:
        raise ValueError(f'Unknown public action kind: {action_kind!r}')
    if backend != 'local':
        raise UnsupportedExecutionOptionError(
            f'Backend {backend!r} has no checked local admission contract')
    if sink_spec is not None and action_kind != 'write_lance':
        raise ValueError('sink_spec is only accepted for write_lance')
    require_action(action_kind)

    capabilities = [operation_capability(op) for op in plan.operations]
    if any(fact.get('native_options') for fact in capabilities):
        raise UnsupportedExecutionOptionError(
            'Local execution does not accept backend_options; use the Ray executor')
    has_stream = any(ops.is_stream_operation(op) for op in plan.operations)
    available = frozenset(available_backends)

    if has_stream:
        if action_kind not in _STREAM_TERMINALS:
            raise UnsupportedExecutionOptionError(
                f'{action_kind} does not support streaming map options; '
                'materialize the streaming prefix first')
        composition = check_composition(capabilities, backend='local', driver='stream')
        if not composition['supported']:
            raise ValueError(composition['reason'])
        if action_kind in _MATERIALIZE_TERMINALS:
            return ActionDecision(action_kind, 'local_stream', 'stream_materialize', 'native_checkpoint')
        if action_kind == 'run_stream':
            route = 'queue_stream' if _is_queue_source(source) else 'stream_delivery'
            return ActionDecision(action_kind, 'local_stream', route, 'native_checkpoint')
        return ActionDecision(action_kind, 'local_stream', 'stream_delivery', 'delivery_only')

    composition = check_composition(capabilities, backend='local', driver='pull')
    if not composition['supported']:
        raise ValueError(composition['reason'])
    if action_kind == 'write_lance':
        if 'lance_scan' in available:
            return ActionDecision(action_kind, 'lance_scan', 'lance_scan', 'final_commit')
        if 'arrow_batch' in available:
            if sink_spec is None or getattr(sink_spec, 'schema', None) is None:
                raise ValueError('Arrow batch write requires a sink schema')
            return ActionDecision(action_kind, 'arrow_batch', 'arrow_batch', 'final_commit')
        if 'datafusion' in available:
            return ActionDecision(action_kind, 'datafusion', 'native_relational', 'final_commit')
        return ActionDecision(action_kind, 'local_pull', 'pull', 'final_commit')
    if 'datafusion' in available:
        return ActionDecision(action_kind, 'datafusion', 'native_relational', 'attempt_only')
    if action_kind == 'write_datasink':
        recovery = 'final_commit'
    elif action_kind == 'write_file':
        recovery = 'delivery_only'
    else:
        recovery = 'attempt_only'
    return ActionDecision(action_kind, 'local_pull', 'pull', recovery)


def validate_stream_plan(plan):
    """Preflight the complete native plan before constructing any callable."""
    from ..data import plan as ops
    from .plan_facts import operation_capability

    if not any(ops.is_stream_operation(op) for op in plan.operations):
        raise ValueError("run_stream requires map_async/batch_map or explicit map streaming options")
    facts = [operation_capability(op) for op in plan.operations]
    composition = check_composition(facts, backend="local", driver="stream")
    if not composition["supported"]:
        raise ValueError(composition["reason"])
    for op in plan.operations:
        if isinstance(op, (ops.MapOp, ops.BoundMapOp)) and op.stream_options is not None:
            op.stream_options.validate_callable(op.callable)


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
