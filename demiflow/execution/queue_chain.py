"""Driver-side unified queue settlement carrier (MVP-2 A1).

The legacy queue lifecycle stays byte-unchanged on the driver: QueueDatasource
claim/lease/heartbeat, the single RawFetch acquisition and finalize_task all run
in the driver journal exactly as ``write_queue_datasink`` does. Only the pure
stateless maps suffix that follows RawFetch is carried by an independent managed
worker. The driver runs the RawFetch prefix plus a per-task transform carrier
stage; the carrier ships each acquired row to the worker as a K4 DataRef over the
shared pickle-4 segment data plane (the control frame carries only identity,
status and reference, so intermediate tuple/bytes values keep their type), the
worker applies the pure maps (no progress, no commit) and returns the transformed
row the same way, and the driver's unchanged ``consume`` settles the task through
finalize_task. A business failure returns as an explicit request_id-correlated
response carrying the original exception, which the driver raises immediately
rather than waiting out a control timeout. Both commit scopes (acquisition +
result) remain on the driver's single journal; the worker constructs zero
CommitCoordinator. Success, failure and cancel all release this-task capacity
through one ResourceManager close+reconcile terminal.

This reuses the frozen K3 lifecycle, ResourceManager launch and K1 manifest
compiler unchanged. It is selected by the platform-side write_datasink queue
seam (controlled default ON); the legacy in-driver path is the byte-unchanged
fallback for ineligible suffixes.
"""
import logging
import threading

from .recovery_contract import RecoveryContractError, identity

logger = logging.getLogger(__name__)


def unified_queue_chain_eligible(plan):
    """The pure-maps suffix must be non-empty, cloudpickle-serializable, and
    carry every UDF by value.

    Mirrors the MVP-1 narrow serializability probe: a pure ``dumps`` only, so a
    real error inside a serializable suffix still raises along the unified path
    and is never masked here. A fetch-only graph (no suffix) or a suffix holding
    a live process-bound actor stays on the byte-unchanged legacy in-driver path.
    Admission itself remains ``validate_topology`` on the full plan, unchanged.

    S8.5 artifact immutability (narrow gate): a cloudpickle ``dumps`` also
    succeeds for an importable module-level UDF, which it carries by reference;
    the managed worker cannot import the producer's module and would hang on the
    graph cut. Such by-reference UDFs are rejected to the legacy path so the
    unified carrier only admits by-value closures whose code bytes are hash-bound.
    """
    import cloudpickle
    import pickle
    from ..data.plan import AsyncMapOp, LogicalPlan
    from .raw_fetch import RawFetch
    index = None
    for position, op in enumerate(plan.operations):
        target = getattr(getattr(op, 'callable', None), 'target', None)
        actor = getattr(target, '__self__', target)
        if type(op) is AsyncMapOp and type(actor) is RawFetch:
            index = position
            break
    if index is None:
        return False
    suffix = plan.operations[index + 1:]
    if not suffix:
        return False
    try:
        cloudpickle.dumps(LogicalPlan(suffix))
    except (pickle.PicklingError, TypeError, AttributeError) as exc:
        logger.debug("unified queue chain ineligible: maps suffix not cloudpickle-serializable "
                     "(%s: %s); routing to legacy in-driver path", type(exc).__name__, exc)
        return False
    for op in suffix:
        target = getattr(getattr(op, 'callable', None), 'target', None)
        if target is None:
            continue
        try:
            pickle.dumps(target)
        except (pickle.PicklingError, TypeError, AttributeError):
            # Standard pickle fails for closures/lambdas/local classes: cloudpickle
            # carries these by value, so the worker needs no producer module import.
            continue
        # Standard pickle succeeded: by-reference unless cloudpickle special-cases
        # __main__ by value. The __module__ guard avoids over-rejecting __main__.
        if getattr(target, '__module__', None) != '__main__':
            logger.debug("unified queue chain ineligible: by-reference UDF %r (module %s); "
                         "routing to legacy in-driver path", target, getattr(target, '__module__', None))
            return False
    return True


class _TransformClient:
    """Synchronous per-task transform shuttle over the worker control socket.

    Called from the carrier stage's thread-pool worker (concurrency=1), so one
    request is in flight at a time. Each acquired row is correlated by its
    preserved native delivery task_id; the worker's pure maps cannot change it
    (validate_topology guarantees delivery preservation). The business row rides
    the shared pickle-4 segment data plane as a K4 DataRef (type-preserving,
    bounded); the control frame carries only identity/status/reference.
    """

    def __init__(self, sock, model, *, now_ms, store, scope_ref):
        self._sock = sock
        self._model = model
        self._now_ms = now_ms
        self._store = store
        self._scope_ref = scope_ref
        self._lock = threading.Lock()

    def transform(self, row):
        from .chain import WORKER_FAILURE_SCHEMA, _worker_failure
        from .worker.bootstrap import recv_message, send_message
        from .worker.queue_transform import TRANSFORM_REQUEST_SCHEMA, TRANSFORM_RESPONSE_SCHEMA
        delivery = row.get('_queue_delivery')
        if type(delivery) is not dict or 'task_id' not in delivery:
            raise RecoveryContractError('Queue carrier row lost its native delivery display')
        request_id = delivery['task_id']
        _, refs = self._store.write([row], scope_ref=self._scope_ref, durability="ephemeral")
        with self._lock:
            send_message(self._sock, {"schema_version": TRANSFORM_REQUEST_SCHEMA,
                                      "request_id": request_id, "row_ref": refs[0]})
            while True:
                message = recv_message(self._sock)
                if message is None:
                    raise RecoveryContractError("Worker closed before the transform response")
                if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                    raise _worker_failure(message)
                if message.get("schema_version") == TRANSFORM_RESPONSE_SCHEMA:
                    if message.get("request_id") != request_id:
                        raise RecoveryContractError("Transform response does not match the outstanding task")
                    if "error_ref" in message or "error" in message:
                        raise self._read_error(message)
                    ref = message["row_ref"]
                    result = self._store.read([ref])[0]
                    self._store.discard([ref])
                    return result
                if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    self._model.handle(message, now_ms=self._now_ms())
                    continue
                raise RecoveryContractError("Unexpected worker frame during queue transform: "
                                            + str(message.get("kind") or message.get("schema_version")))

    def _read_error(self, message):
        detail = message.get("traceback")
        if detail:
            logger.error("Worker queue transform failed:\n%s", detail)
        if "error_ref" in message:
            ref = message["error_ref"]
            exc = self._store.read([ref])[0]
            self._store.discard([ref])
            if isinstance(exc, BaseException):
                return exc
            return RecoveryContractError("Worker queue transform failed: " + str(exc))
        error = message.get("error") or {}
        return RecoveryContractError("Worker queue transform failed: %s: %s"
                                     % (error.get("type"), error.get("message")))


def run_local_queue_chain(executor, source_plan, plan, sink, action, *,
                          resource_root=None, control_timeout_s=120):
    """Compat entry: translate legacy queue settlement into one v3 write_datasink action.

    The independent driver resource/transform-carrier/commit loop is retired; queue
    acquisition, the pure-maps suffix and ``finalize_task`` settlement now run
    through the unified ``execute_action`` runtime (E001). The confirmed sink
    ``WriteResult`` is returned unchanged. ``resource_root``/``control_timeout_s``
    are kept for signature compatibility and no longer select a separate worker.
    """
    from .dataflow import execute_action
    from .dataflow_api import make_action_request
    from ..planning.dataflow_policy import resolve_runtime_policy

    request = make_action_request(executor, source_plan, plan, kind="write_datasink",
                                  arguments={}, sink=sink)
    policy, bounds = resolve_runtime_policy({}, {}, provider="local")
    return execute_action(request, policy=policy, bounds=bounds).result()
