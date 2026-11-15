"""DataflowExecution: the sole driver of one native v3 action.

Lane E (E001-E008). ``execute_action`` is the only entry for Local and Ray Core;
it wires compile -> snapshot -> admission -> seal -> resource -> runtime ->
terminal -> cleanup into one closed loop over a single RunAction/RunJournal.
The driver owns the lifecycle prepare -> start -> pump -> finish/cancel -> close;
workers never open the journal and no second physical ledger is created. All
queues, leases and resident reservations are bounded, and physical capacity is
released only on confirmed exit evidence.
"""
from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING

from .contracts.dataflow import (
    ActionHandle, ActionRequest, ArtifactRef, Bounds, ControlReply, CutContribution,
    CutRequest, DriverServices, ExecutionStatus, PhysicalDataflow, ReadyWork,
    RecoveryImage, RuntimePolicy, SealedExecution, TerminalResult, WorkerAssignment,
    WorkerEvent, WorkerLease,
)
from .contracts.dataflow_errors import DataflowError
from .dataflow_scheduler import DataflowScheduler
from .dataflow_services import make_driver_services
from .dataflow_terminals import open_terminal
from .dataflow_checkpoint import DataflowCheckpointCoordinator
from .dataflow_seal import (
    seal_dataflow, stage_dataflow_programs, resolve_execution_constants,
)
from .dataflow_sources import snapshot_source, plan_source_splits
from .dataflow_queue import NativeAcquisitionService
from .dataflow_artifacts import stage_runtime_bundle
from .dataflow_hosts import make_host_endpoint
from .exchange.dataflow_channels import close_channels
from ..planning.dataflow_compile import lower_action
from ..planning.dataflow_place import compile_physical_dataflow
from ..planning.dataflow_policy import normalize_native_options
from ..resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ReleaseSpec, ResourceScope,
)

if TYPE_CHECKING:
    from .run_context import RunAction
    from ..resources.manager import ResourceManager


def _now_ms() -> int:
    return int(time.time() * 1000)


def execute_action(request: ActionRequest, *, policy: RuntimePolicy, bounds: Bounds) -> ActionHandle:
    services = make_driver_services(request.action, policy=policy, bounds=bounds)
    execution = DataflowExecution(request, services=services)
    try:
        execution.prepare()
        execution.start()
        return open_terminal(request, execution=execution)
    except BaseException as failure:
        try:
            execution.close()
        except Exception as cleanup:
            failure.add_note("Execution cleanup also failed: " + str(cleanup))
        raise


class DataflowExecution:
    def __init__(self, request: ActionRequest, *, services: DriverServices) -> None:
        if not isinstance(request, ActionRequest):
            raise DataflowError("INVALID_CONTRACT", "execution requires an ActionRequest")
        if services is None:
            raise DataflowError("INVALID_CONTRACT", "execution requires DriverServices")
        self._request = request
        self._services = services
        self._action = request.action
        self._policy = services.policy
        self._bounds = services.bounds
        self._store = services.store
        self._state = "preparing"
        self._sealed: SealedExecution | None = None
        self._physical: PhysicalDataflow | None = None
        self._graph = None
        self._scheduler: DataflowScheduler | None = None
        self._checkpoint: DataflowCheckpointCoordinator | None = None
        self._acquisition: NativeAcquisitionService | None = None
        self._endpoints: dict[str, object] = {}
        self._lease_by_operation: dict[str, WorkerLease] = {}
        self._sinks = {}
        if request.sink is not None:
            self._sinks["sink"] = request.sink
        self._restore_image: RecoveryImage | None = None
        self._channels = None
        self._cut: CutRequest | None = None
        self._contributions: dict[str, CutContribution] = {}
        self._confirmed_rows = 0
        self._pending_events = 0
        self._terminal_result: TerminalResult | None = None
        self._source_exhausted = False
        self._prepared = False
        self._started = False
        self._cancelled = False
        self._closed = False
        self._scope_open = False

    # -- helpers ----------------------------------------------------------
    def _fence(self) -> str:
        return self._action.context.fence

    def _attempt(self) -> int:
        return self._action.context.attempt

    def _run_id(self) -> str:
        return self._action.context.ref.run_id

    def _check_stop(self) -> None:
        self._action.context.check_stop()

    def _stage_order(self) -> list:
        stages = list(self._physical.stages)
        terminals = set(self._graph.terminals) if self._graph is not None else set()

        def consumer_first(stage):
            return 0 if any(occ in terminals for occ in stage.occurrences) else 1

        return sorted(stages, key=consumer_first)

    def _initial_work(self, stage, partition_id: str) -> ReadyWork:
        operation_id = hashlib.sha256(
            ("start:" + self._physical.plan_id + ":" + stage.stage_id + ":" + partition_id).encode()
        ).hexdigest()
        resident = max(1, stage.resident_bytes)
        return ReadyWork(stage_id=stage.stage_id, partition_id=partition_id,
                         input_refs=(), resident_bytes=resident, operation_id=operation_id)

    def _assignment(self, lease: WorkerLease, stage) -> WorkerAssignment:
        partitions = set(stage.partitions)
        inputs = tuple(x for x in self._physical.exchanges if partitions.intersection(x.consumer_partitions))
        outputs = tuple(x for x in self._physical.exchanges if partitions.intersection(x.producer_partitions))
        restore_refs = ()
        if self._restore_image is not None:
            restore_refs = tuple(ref for contribution in self._restore_image.contributions
                                 for ref in contribution.buffers)
        source_ids = set(stage.occurrences) & set(self._graph.sources)
        splits = tuple(split for source_id in source_ids
                       for index, split in enumerate(s for s in self._physical.splits
                                                     if s.source_id == source_id)
                       if stage.partitions[index % len(stage.partitions)] in lease.partition_ids)
        return WorkerAssignment(
            schema="demiflow.dataflow_assignment.v3", run_id=self._run_id(),
            action_id=self._action.action_id, attempt=self._attempt(), fence=self._fence(),
            plan_id=self._physical.plan_id, plan_revision=self._physical.plan_revision,
            lease=lease, sealed=self._sealed, stages=(stage,), inputs=inputs, outputs=outputs,
            source_splits=splits, restore_refs=restore_refs, bounds=self._bounds)

    def _prepare_launch(self, lease: WorkerLease, stage) -> WorkerLease:
        grant_bytes = self._scheduler.grants.get(lease.allocation_id)
        if grant_bytes is not None:
            staged = self._store.put(iter((grant_bytes,)), max_bytes=self._bounds.max_control_bytes,
                                     media_type="application/json")
            from dataclasses import replace
            lease = replace(lease, grant_ref=staged)
        endpoint = make_host_endpoint(lease, services=self._services)
        endpoint.prepare(self._assignment(lease, stage))
        self._endpoints[lease.allocation_id] = endpoint
        return lease

    def _activate(self, lease: WorkerLease, stage) -> None:
        endpoint = self._endpoints[lease.allocation_id]
        endpoint.activate(self._assignment(lease, stage))
        self._lease_by_operation[self._operation_for_lease(lease)] = lease

    def _operation_for_lease(self, lease: WorkerLease) -> str:
        for operation_id, work in self._scheduler._inflight.items():
            if work.stage_id == lease.stage_id and work.partition_id in lease.partition_ids:
                return operation_id
        return lease.allocation_id

    # -- E003 prepare -----------------------------------------------------
    def prepare(self) -> SealedExecution:
        if self._prepared:
            return self._sealed
        if self._closed or self._cancelled:
            raise DataflowError("CANCELLED", "execution is closed")
        self._check_stop()
        normalize_native_options(self._request.arguments.get("native_options"),
                                 operation_kind=self._request.kind)
        compiled = lower_action(self._request, bounds=self._bounds)
        graph = stage_dataflow_programs(compiled, action=self._action, store=self._store,
                                        bounds=self._bounds)
        snapshots, splits = [], []
        for declaration in compiled.declarations:
            if declaration.source_spec is None:
                continue
            snapshot = snapshot_source(declaration.source_spec, action=self._action,
                                       store=self._store, bounds=self._bounds,
                                       source_id=declaration.occurrence_id)
            snapshots.append(snapshot)
            splits.extend(plan_source_splits(snapshot, declaration.source_spec,
                          target_partitions=self._policy.default_partitions, bounds=self._bounds))
        constants_ref = resolve_execution_constants(graph, action=self._action, store=self._store)
        physical = compile_physical_dataflow(
            graph, tuple(snapshots), tuple(splits), policy=self._policy, hosts=self._services.hosts,
            bounds=self._bounds, constants_ref=constants_ref)
        sealed = seal_dataflow(self._action, graph, physical, store=self._store)
        stage_runtime_bundle(sealed, store=self._store, bounds=self._bounds)
        self._graph = graph
        self._physical = physical
        self._sealed = sealed
        self._scheduler = DataflowScheduler(physical, resources=self._services.resources)
        self._checkpoint = DataflowCheckpointCoordinator(self._action, physical, store=self._store)
        from .queue_execution import queue_source
        from .raw_fetch import RawFetch
        from ..data.plan import AsyncMapOp
        requires_acquisition = queue_source(self._request.source) is not None or any(
            isinstance(op, AsyncMapOp) and isinstance(
                getattr(op.callable.target, "__self__", op.callable.target), RawFetch)
            for op in self._request.plan.operations)
        if requires_acquisition:
            self._acquisition = NativeAcquisitionService(self._action, services=self._services)
            self._acquisition.configure(self._request.source, self._request.plan, self._request.sink)
            self._services.journal_service.bind_queue(self._acquisition)
        self._prepared = True
        return sealed

    # -- E004 start -------------------------------------------------------
    def start(self) -> None:
        if self._started:
            return
        if not self._prepared:
            raise DataflowError("INVALID_CONTRACT", "start requires prepare")
        if self._closed or self._cancelled:
            raise DataflowError("CANCELLED", "execution is closed")
        self._check_stop()
        budget = {"cpu_millis": self._bounds.max_nodes * 1000,
                  "memory_bytes": self._bounds.max_resident_bytes,
                  "max_workers": self._bounds.max_partitions}
        self._services.resources.state.open_scope(self._fence(), ResourceScope(
            self._scheduler.scope_id, self._run_id(), {"provider": self._policy.provider},
            self._fence(), budget))
        self._scope_open = True
        self._restore_image = (self._checkpoint.recover(sinks=self._sinks)
                               if self._action.context.resuming else None)
        if self._acquisition is not None:
            self._acquisition.start()
        # Consumers and control reserve capacity before producers; a partial
        # allocation still progresses, never all-to-producers (deadlock gate).
        for stage in self._stage_order():
            for partition_id in stage.partitions:
                work = self._initial_work(stage, partition_id)
                try:
                    lease = self._scheduler.place(work, prepare=self._prepare_launch, host_id=self._services.hosts[0].host_id)
                except DataflowError as exc:
                    if exc.retryable:
                        self._scheduler.enqueue(work)
                        continue
                    raise
                self._activate(lease, stage)
        self._started = True
        self._state = "running"

    # -- E005 pump --------------------------------------------------------
    def _validate_event(self, event: WorkerEvent) -> None:
        if event.attempt != self._attempt() or event.fence != self._fence():
            raise DataflowError("FENCED", "late event from a stale attempt",
                                context={"operation_id": event.operation_id})
        if event.plan_revision != self._physical.plan_revision:
            raise DataflowError("FENCED", "event plan_revision differs",
                                context={"operation_id": event.operation_id})
        lease = self._scheduler.leases.get(event.allocation_id)
        if lease is not None and event.boot_id != lease.boot_id:
            raise DataflowError("FENCED", "event boot_id differs from lease",
                                context={"allocation_id": event.allocation_id})

    def pump(self, *, deadline_ms: int) -> ExecutionStatus:
        if self._closed:
            raise DataflowError("CANCELLED", "execution is closed")
        if not self._started:
            raise DataflowError("INVALID_CONTRACT", "pump requires start")
        if type(deadline_ms) is not int or deadline_ms <= 0:
            raise DataflowError("INVALID_CONTRACT", "pump deadline_ms must be a positive int")
        self._check_stop()
        deadline = time.monotonic() + deadline_ms / 1000.0
        max_events = self._bounds.max_pending_events
        replies = self._services.journal_service.poll_replies(max_replies=max_events)
        for completed in replies:
            endpoint = self._endpoints.get(completed.allocation_id)
            if endpoint is not None:
                endpoint.deliver_reply(completed.reply)
        events: list[WorkerEvent] = []
        for allocation_id, endpoint in self._endpoints.items():
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            batch = endpoint.poll(max_events=max_events, deadline_ms=remaining_ms)
            events.extend(batch)
            if time.monotonic() >= deadline:
                break
        self._pending_events = len(events)
        for event in events:
            self._validate_event(event)
            self._services.journal_service.submit(event)
            self._handle_event(event)
        return ExecutionStatus(state=self._state, pending_events=self._pending_events,
                               confirmed_rows=self._confirmed_rows)

    def _handle_event(self, event: WorkerEvent) -> None:
        kind = event.kind
        if kind == "failure":
            code = (event.payload or {}).get("code", "INTERNAL_PROTOCOL")
            raise DataflowError(code, "worker reported failure",
                                context={"operation_id": event.operation_id})
        if kind == "progress":
            self._confirmed_rows += int((event.payload or {}).get("rows", 0))
            return
        if kind == "ready":
            payload = event.payload or {}
            work = ReadyWork(
                stage_id=payload["stage_id"], partition_id=payload["partition_id"],
                input_refs=tuple(payload.get("input_refs", ())),
                resident_bytes=int(payload.get("resident_bytes", 1)),
                operation_id=event.operation_id)
            self._scheduler.enqueue(work)
            choice = self._scheduler.choose(now_ms=_now_ms())
            if choice.work is not None:
                stage = next(s for s in self._physical.stages if s.stage_id == choice.work.stage_id)
                lease = self._scheduler.place(choice.work, prepare=self._prepare_launch, host_id=self._services.hosts[0].host_id)
                self._activate(lease, stage)
            return
        if kind == "cut":
            contribution = CutContribution(
                cut_id=(event.payload or {})["cut_id"], participant_id=event.operation_id,
                boot_id=event.boot_id, sources=(), operator_states={}, channels={},
                buffers=(), effects=(), sinks={}, final=bool((event.payload or {}).get("final", False)))
            self._checkpoint.contribute(contribution)
            self._contributions[contribution.participant_id] = contribution
            return
        if kind == "eoi":
            self._source_exhausted = True
            return
        if kind == "closed":
            return
        # effect/artifact/reserve/queue/native_request/read_* are served by the
        # DriverJournalService through submit/poll_replies; the driver does not
        # move payload here.

    # -- E006 finish ------------------------------------------------------
    def finish(self) -> TerminalResult:
        if self._terminal_result is not None:
            return self._terminal_result
        if not self._started:
            raise DataflowError("INVALID_CONTRACT", "finish requires start")
        if self._cancelled:
            raise DataflowError("CANCELLED", "execution was cancelled")
        self._check_stop()
        self._cut = self._checkpoint.begin(final=True)
        self._checkpoint.commit(self._cut.cut_id, sinks=self._sinks)
        for stage in self._physical.stages:
            self._scheduler.drain(stage.stage_id, deadline_ms=self._policy.control_timeout_ms)
        if self._acquisition is not None:
            self._acquisition.finish()
        completion = "source_exhausted" if self._source_exhausted else "terminal_satisfied"
        self._terminal_result = TerminalResult(
            kind=self._request.kind, rows=self._confirmed_rows, scalar=None,
            result_ref=None, write_result=None, complete=True, completion=completion)
        self._state = "succeeded"
        return self._terminal_result

    # -- E007 cancel ------------------------------------------------------
    def cancel(self, reason: str) -> None:
        if self._cancelled or self._closed:
            return
        self._cancelled = True
        self._state = "cancelled"
        if self._cut is not None:
            self._checkpoint.abort(self._cut.cut_id, reason=reason)
        for endpoint in self._endpoints.values():
            endpoint.cancel(reason, deadline_ms=self._policy.control_timeout_ms)
        # Unknown model requests are never replayed and committed results are
        # never deleted; physical owners are released only on confirmed exit.

    # -- E008 close -------------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        deadline_ms = self._policy.control_timeout_ms
        cleanup_error = None
        if self._scope_open and self._scheduler is not None:
            try:
                self._scheduler.close(deadline_ms=deadline_ms)
            except Exception as exc:
                cleanup_error = exc
        if self._channels is not None:
            close_channels(self._channels, reason="execution_close", deadline_ms=deadline_ms)
            self._channels = None
        if self._acquisition is not None:
            self._acquisition.close()
        if self._checkpoint is not None:
            self._checkpoint.close()
        try:
            self._services.journal_service.close(deadline_ms=deadline_ms)
        except DataflowError:
            pass
        self._endpoints.clear()
        if cleanup_error is not None:
            raise cleanup_error
