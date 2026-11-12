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
    """Drive queue settlement with the pure-maps suffix on a managed worker (A1).

    The driver setup, ``consume`` (finalize_task) and terminal accounting are
    identical to ``write_queue_datasink``; only the executed plan differs (RawFetch
    prefix + transform carrier instead of the full plan). The worker receives the
    maps suffix as a sha256-bound K1 plan artifact and never commits.
    """
    import shutil
    import sys
    import time
    import uuid

    from ..data.datasink import WriteResult
    from ..data.plan import AsyncMapOp, CallableSpec, LogicalPlan
    from ..data.stats import ExecutionMetadata
    from ..resources.capacity import LocalCapacityPool
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.local import LocalResourceProvider
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.process_unit import ManagedProcessUnit
    from .chain import WORKER_FAILURE_SCHEMA, _worker_failure, compile_chain_topology, stage_plan_artifact
    from .contracts.envelope import WORKER_PROTOCOL_VERSION
    from .contracts.worker import WorkerProtocolModel
    from .exchange.carriage_local import LocalSegmentStore
    from .queue_execution import finalize_task, queue_source, validate_topology, write_queue_datasink
    from .recovery_identity import source_identity
    from .resource_state import RunResourceState
    from .row_scope import current_row_scope
    from .stream_materialize import stream_view
    from .worker.bootstrap import LocalControlConnector, recv_message, send_message

    run = action.context
    source = queue_source(source_plan)
    (index, fetch), contract = validate_topology(source, plan, sink)
    suffix = plan.operations[index + 1:]
    if not suffix:
        return write_queue_datasink(executor, source_plan, plan, sink, action)
    prefix = plan.operations[:index + 1]
    suffix_plan = LogicalPlan(suffix)

    # K1 manifest + content-addressed plan artifact for the pure-maps worker.
    # No source capture: the queue stays incremental on the driver; the logical
    # source identity is the queue declaration (source_identity), not rows.
    plan_ref = stage_plan_artifact(action, suffix_plan)
    source_reference = {"source_id": identity(source_identity(source_plan))}
    output_child_id = action.action_id + "-queue-transform"
    topology, logical_dag, physical = compile_chain_topology(
        action, source_plan, suffix_plan, source_reference,
        output_child_id=output_child_id, host_id="local", plan_ref=plan_ref)

    source.configure(action, fetch, sink, contract, 'node-' + str(index))

    async def consume(row):
        scope = current_row_scope()
        if scope is None or scope.delivery is None:
            raise RecoveryContractError('Final task row lost its native delivery sidecar')
        await source.io.run(finalize_task, source, scope.delivery, row)

    ids = {k: uuid.uuid4().hex for k in
           ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
    scope_id, allocation_id, boot_id = ids["scope_id"], ids["allocation_id"], ids["boot_id"]
    grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
             "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
             "boot_id": boot_id, "controller_epoch": run.attempt,
             "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
    capabilities = ["cpu-map", "stream"]
    launch_spec = {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": capabilities, "grant": grant,
        "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"],
        "queue_transform": {"plan_ref": plan_ref, "sealed_operators": topology["operators"]},
    }

    root = run.directory.with_name(run.directory.name + "-resources") if resource_root is None \
        else __import__("pathlib").Path(resource_root)
    segment_dir = run.directory / "queue-transform-segments"
    store = LocalSegmentStore(segment_dir, host_id="local")
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    connector = LocalControlConnector()
    provider = LocalResourceProvider(pool, unit, control=connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(allocation_id, launch_spec)
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "local"}, run.fence,
        {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": plan_ref["sha256"],
                                "location": "file://" + str(run.directory / plan_ref["path"])}},
        entrypoint="demiflow-worker",
        argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap"),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(ids["worker_id"], allocation_id,
                            ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3}),
                            launch, provision_deadline_ms=60000)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                        uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, (request,), ()))
    sock = None
    drained = False
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise RecoveryContractError("Local queue worker did not reach RUNNING: " + str(phase))
        sock = connector.driver_socks[allocation_id]
        sock.settimeout(control_timeout_s)
        model = WorkerProtocolModel(
            scope_id=scope_id, run_id=run.ref.run_id, allocation_id=allocation_id,
            boot_id=boot_id, controller_epoch=run.attempt, grant=grant,
            plan_revision=0, capabilities=capabilities)

        def now_ms():
            return int(time.time() * 1000)

        def command(kind, payload):
            op = uuid.uuid4().hex
            send_message(sock, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": allocation_id, "boot_id": boot_id, "payload": payload})
            return op

        def read_response(op):
            while True:
                message = recv_message(sock)
                if message is None:
                    raise RecoveryContractError("Worker closed before responding")
                if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                    raise _worker_failure(message)
                if message.get("response_to") == op:
                    if "error" in message:
                        raise RecoveryContractError("Worker command failed: " + str(message["error"]))
                    return message["receipt"]
                if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    model.handle(message, now_ms=now_ms())

        register = recv_message(sock)
        if register is None or register.get("kind") != "Register":
            raise RecoveryContractError("Worker did not register")
        model.handle(register, now_ms=now_ms())
        read_response(command("VerifyArtifactAndCapabilities",
                              {"artifact_manifest_ref": topology["artifact_manifest_ref"],
                               "required_capabilities": capabilities}))
        assignment_id = uuid.uuid4().hex
        read_response(command("Restore", {"assignment_ref": assignment_id,
                                          "restore_point": None, "state_refs": []}))
        read_response(command("Activate", {"grant_ref": ids["grant_id"],
                                           "controller_epoch": run.attempt,
                                           "operation_id": uuid.uuid4().hex}))
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
            "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "worker_id": ids["worker_id"], "controller_epoch": run.attempt,
            "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": topology["artifact_manifest_ref"],
            "stages": [node["occurrence_id"] for node in logical_dag["nodes"]],
            "partitions": ["p0"], "channels": {"inputs": [], "outputs": [topology["channel_id"]]},
            "restore_point": None, "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
        read_response(command("Assign", {"assignment": assignment}))

        client = _TransformClient(sock, model, now_ms=now_ms, store=store, scope_ref=allocation_id)
        carrier_op = AsyncMapOp(CallableSpec.create(client.transform), 1, 1, (),
                                'queue_transform', None, 'thread')
        carrier_plan = LogicalPlan(prefix + (carrier_op,))
        stream_view(executor, source_plan, carrier_plan)._run_stream(
            source_iter=source.rows(), consume=consume)

        read_response(command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 5000,
                                        "reason": "queue transform drained"}))
        drained = True
    finally:
        # Unified resource terminal: success/failure/cancel all convert confirmed
        # exit to capacity release through the same ResourceManager close+reconcile
        # protocol. Only an unconfirmed exit keeps its charge; the kill below is a
        # backstop for survivors the protocol could not confirm.
        try:
            manager.close(
                MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                                uuid.uuid4().hex, accepted["desired_revision"]),
                {"mode": "DRAINED" if drained else "ABANDON",
                 "engine_receipt_ref": allocation_id + ("-drained" if drained else "-abandoned"),
                 "terminate_deadline_ms": 5000,
                 "reason": "queue transform drained" if drained else "queue transform terminal cleanup"})
            manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
        except Exception:
            logger.exception("Queue carrier resource terminal failed; falling back to process kill")
        if sock is not None:
            sock.close()
        connector.close(allocation_id)
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
        shutil.rmtree(segment_dir, ignore_errors=True)

    source.assert_settled()
    metadata = source.snapshot_metrics()
    action.complete(dict(kind='write_datasink', queue=metadata))
    executor._run_ref = action.context.ref
    executor._last_metadata = ExecutionMetadata(source_name='QueueDatasource', rows_read=metadata['claimed'],
        rows_output=metadata['settled'], diagnostics={'queue': metadata})
    return WriteResult(written_rows=metadata['settled'], failed_rows=0, blocks_written=metadata['settled'],
                       target='queue:' + source.config.queue_id + ':' + source.pool, metadata=metadata)