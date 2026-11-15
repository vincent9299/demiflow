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
import json
from collections import deque
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
        self._by_partition = {}
        self._deliveries = {}
        self._finite_completions = {}
        self._satisfied_consumers = {}
        self._terminal_queue = deque()
        self._terminal_seen = 0
        self._idle = set()
        self._done = set()
        self._stopping = False
        self._limit = (request.arguments.get("limit") if request.kind == "take" else
                       request.arguments.get("batch_size", 20) if request.kind == "take_batch" else None)
        self._failure = None
        self._checkpoint_requested = False
        self._replay = None
        self._node_metrics = {}
        self._report_keys = {}
        self._last_checkpoint_rows = 0
        self._checkpoint_at = time.monotonic()

    @property
    def services(self):
        return self._services

    @property
    def request(self):
        return self._request

    def subscribe_terminal(self):
        return _OutputSubscription(self)

    def satisfy_terminal(self):
        if self._stopping or self._state == "succeeded":
            return
        self._stopping = True
        self._idle.clear()
        for endpoint in self._endpoints.values():
            endpoint.send_business({"kind": "pause"})

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
        endpoint._recovery_image = self._restore_image
        endpoint.prepare(self._assignment(lease, stage))
        self._endpoints[lease.allocation_id] = endpoint
        return lease

    def _activate(self, lease: WorkerLease, stage) -> None:
        endpoint = self._endpoints[lease.allocation_id]
        endpoint.activate(self._assignment(lease, stage))
        self._lease_by_operation[self._operation_for_lease(lease)] = lease
        for partition in lease.partition_ids:
            self._by_partition[partition] = endpoint

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
        existing = self._action.context.journal.source_binding(self._action.action_id + "-dataflow-sealed")
        if self._action.context.resuming and existing is not None:
            from .dataflow_seal import load_dataflow, load_sealed_dataflow, verify_dataflow_program
            from .recovery_identity import platform_identity
            graph, physical, sealed = load_dataflow(self._action.context.directory,
                self._action.action_id, store=self._store, bounds=self._bounds)
            _, _, pin = load_sealed_dataflow(sealed, action_id=self._action.action_id,
                                             store=self._store, bounds=self._bounds)
            if pin.get("platform_revision") != platform_identity():
                raise DataflowError("CODE_MISMATCH", "recovery requires the sealed runtime revision")
            for node in graph.nodes:
                if node.program_ref is not None:
                    verify_dataflow_program(node.program_ref, expected_pin=pin["programs"][node.occurrence_id],
                                            store=self._store, bounds=self._bounds)
        else:
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
            from .checked import current
            from .identity_bridge import build_identity_bridge
            from .contracts.dataflow import thaw_record_value
            checked = current()
            bridge = None
            if checked is not None and checked.report_digest and compiled.report_bindings:
                covered = {b["occurrence_id"] for b in compiled.report_bindings}
                if covered == {n.occurrence_id for n in graph.nodes}:
                    bridge = build_identity_bridge({"dag_id": graph.dag_id,
                        "nodes": [{"occurrence_id": n.occurrence_id, "logical_node_id": n.semantic_id}
                                  for n in graph.nodes]}, action_id=self._action.action_id,
                        report_digest=checked.report_digest,
                        bindings=[thaw_record_value(b) for b in compiled.report_bindings])
            sealed = seal_dataflow(self._action, graph, physical, store=self._store, identity_bridge=bridge)
            stage_runtime_bundle(sealed, store=self._store, bounds=self._bounds)
        from .checked import current
        self._checked = current()
        if self._checked is not None:
            bridge_doc = json.loads(b"".join(self._store.read(sealed.identity_bridge_ref,
                max_bytes=self._bounds.max_control_bytes)))
            self._report_keys = {b['occurrence_id']: (self._action.action_id, b['report_node_id'], b['report_occurrence'])
                                 for b in bridge_doc.get('entries', ())}
        from .dataflow_sinks import make_sink_adapter
        for node in graph.nodes:
            if node.kind == "terminal":
                if self._request.kind in ("write_lance", "write_datasink"):
                    from .dataflow_sinks import make_native_terminal_sink
                    self._sinks[node.kernel.kind] = make_native_terminal_sink(node.kernel,
                        request=self._request, services=self._services)
                else:
                    self._sinks[node.kernel.kind] = make_sink_adapter(node.kernel, services=self._services)
        self._graph = graph
        self._physical = physical
        self._policy = physical.policy
        self._sealed = sealed
        self._scheduler = DataflowScheduler(physical, resources=self._services.resources)
        from .contracts.dataflow_codec import runtime_scope_id
        self._scheduler.scope_id = runtime_scope_id(physical.plan_id, self._attempt())
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
        if self._restore_image is not None:
            pub = self._publication(self._restore_image)
            self._terminal_seen = pub["manifest"]["total_rows"]
            self._replay = self._published_records(pub)
            if (self._restore_image.cut.final or
                    self._limit is not None and pub["manifest"]["total_rows"] >= self._limit):
                self._started = True
                self._set_result(self._restore_image)
                self._replay = self._published_records(pub)
                return
            # Validate all actor images before ANY participant executes again.
            # A completed final result is safe to read without recreating actors.
            from .kernels import state_codec
            transforms = {n.occurrence_id for n in self._graph.nodes if n.kind == "transform"}
            for contribution in self._restore_image.contributions:
                for occ, ref in contribution.operator_states.items():
                    if occ not in transforms or ref is None:
                        continue
                    state = state_codec.loads(b"".join(self._store.read(ref,
                        max_bytes=self._bounds.max_state_bytes)), max_bytes=self._bounds.max_state_bytes).get("state")
                    if state and state.get("mode") == "nonrecoverable":
                        raise DataflowError("INCOMPATIBLE_HISTORY", "actor state cannot resume: " + state["reason"],
                                            context={"occurrence_id": occ})
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
        self._checkpoint_at = time.monotonic()

    # -- E005 pump --------------------------------------------------------
    def _validate_event(self, event: WorkerEvent) -> None:
        if event.attempt != self._attempt() or event.fence != self._fence():
            raise DataflowError("FENCED", "late event from a stale attempt",
                                context={"operation_id": event.operation_id})
        if event.plan_revision != self._physical.plan_revision:
            raise DataflowError("FENCED", "event plan_revision differs",
                                context={"operation_id": event.operation_id})
        lease = self._scheduler.leases.get(event.allocation_id)
        if (lease is None or event.run_id != self._run_id()
                or event.scope_id != self._scheduler.scope_id or event.controller_epoch != self._attempt()):
            raise DataflowError("FENCED", "event is outside the active run/scope/allocation")
        if event.boot_id != lease.boot_id:
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
            if event.kind in ("effect", "artifact", "reserve", "read_action", "read_configuration",
                              "check_grant", "queue", "native_request"):
                self._services.journal_service.submit(event)
            else:
                self._handle_event(event)
        source_rows = sum(m['rows_output'] for (occ, _), m in self._node_metrics.items()
                          if occ in self._graph.sources)
        if (source_rows > self._last_checkpoint_rows and set(self._endpoints) - self._done
                and (source_rows - self._last_checkpoint_rows >= self._policy.checkpoint_rows
                     or (time.monotonic() - self._checkpoint_at) * 1000 >= self._policy.checkpoint_interval_ms)):
            self.request_checkpoint()
        self._advance_cut()
        if not events:
            time.sleep(min(.002, deadline_ms / 1000))
        return ExecutionStatus(state=self._state, pending_events=self._pending_events,
                               confirmed_rows=self._confirmed_rows)

    def _handle_event(self, event):
        from .contracts.dataflow import thaw_record_value
        payload = thaw_record_value(event.payload) if event.payload is not None else {}
        endpoint = self._endpoints[event.allocation_id]
        lease = self._scheduler.leases[event.allocation_id]
        if event.kind == "failure":
            from .contracts.dataflow_errors import DATAFLOW_ERROR_CODES
            code = payload.get("code", "INTERNAL_PROTOCOL")
            if code not in DATAFLOW_ERROR_CODES:
                code = "INTERNAL_PROTOCOL"
            message = payload.get("message") or "worker failed"
            self._failure = DataflowError(code, message[:1024])
            if payload.get("traceback"):
                self._failure.add_note(payload["traceback"][-8000:])
            self._state = "failed"
            raise self._failure
        if event.kind == "progress":
            occ, partition = payload['occurrence_id'], payload['partition_id']
            assigned = {n for stage in endpoint._assignment.stages for n in stage.occurrences}
            if occ not in assigned or partition not in lease.partition_ids:
                raise DataflowError("FENCED", "node observation is outside assigned stage")
            metrics = payload['metrics']
            if set(metrics) != {'rows_input', 'rows_output'} or any(type(v) is not int or v < 0 for v in metrics.values()):
                raise DataflowError("INVALID_CONTRACT", "node counters must be nonnegative integers")
            old = self._node_metrics.get((occ, partition), {'rows_input': 0, 'rows_output': 0})
            if any(metrics[k] < old[k] for k in metrics):
                raise DataflowError("FENCED", "node counters moved backwards within a worker boot")
            self._node_metrics[(occ, partition)] = metrics
            key = self._report_keys.get(occ)
            if key is not None:
                self._checked.progress(key, **{k: metrics[k] - old[k] for k in metrics})
                self._checked.flush_observations()
            return
        if event.kind in ("data", "terminal"):
            if payload["producer_partition"] not in lease.partition_ids:
                raise DataflowError("FENCED", "data producer differs from allocation")
            delivery = payload["delivery_id"]
            if delivery in self._deliveries:
                raise DataflowError("INVALID_CONTRACT", "duplicate in-flight delivery")
            if event.kind == "terminal":
                if not any(n in self._graph.terminals for st in self._physical.stages
                           if st.stage_id == lease.stage_id for n in st.occurrences):
                    raise DataflowError("FENCED", "non-terminal allocation emitted a terminal record")
                if len(self._terminal_queue) >= self._bounds.max_pending_events:
                    raise DataflowError("BUDGET_EXCEEDED", "terminal subscription credit exceeded")
                self._deliveries[delivery] = (endpoint, None)
                self._terminal_queue.append(payload)
                self._terminal_seen += 1
                if self._limit is not None and self._terminal_seen >= self._limit:
                    self.satisfy_terminal()
            else:
                spec = next((x for x in self._physical.exchanges if x.edge_id == payload["edge_id"]), None)
                if (spec is None or payload["producer_partition"] not in spec.producer_partitions
                        or payload["target_partition"] not in spec.consumer_partitions):
                    raise DataflowError("FENCED", "delivery is outside physical channel membership")
                target = self._by_partition[payload["target_partition"]]
                self._deliveries[delivery] = (endpoint, target)
                self._idle.discard(target._allocation_id)
                target.send_business(dict(payload, kind="data"))
            self._idle.discard(event.allocation_id)
        elif event.kind == "ack":
            prior = self._deliveries.get(payload["delivery_id"])
            if prior is None or prior[1] is not endpoint:
                raise DataflowError("FENCED", "ack is not from the expected consumer")
            self._ack(payload["delivery_id"])
        elif event.kind == "eoi":
            if payload["scope"] == "input_satisfied":
                spec = next((x for x in self._physical.exchanges if x.edge_id == payload["edge_id"]), None)
                consumer = payload["consumer_partition"]
                if spec is None or consumer not in lease.partition_ids or consumer not in spec.consumer_partitions:
                    raise DataflowError("FENCED", "input completion differs from physical consumer membership")
                done = self._satisfied_consumers.setdefault(spec.edge_id, set())
                done.add(consumer)
                if set(spec.consumer_partitions) <= done:
                    for producer in spec.producer_partitions:
                        target = self._by_partition[producer]
                        self._idle.discard(target._allocation_id)
                        target.send_business({"kind": "close_output", "edge_id": spec.edge_id})
            elif payload["scope"] == "finite":
                from .contracts.dataflow import ProducerCompletion
                from .exchange.dataflow_channels import seal_scope
                from .dataflow_sinks import _artifact_json
                from .recovery_contract import identity
                spec = next(x for x in self._physical.exchanges if x.edge_id == payload["edge_id"])
                producer = payload["producer_partition"]
                if (spec.mode != "finite_sealed" or producer not in lease.partition_ids
                        or producer not in spec.producer_partitions):
                    raise DataflowError("FENCED", "finite completion differs from physical membership")
                completion = ProducerCompletion(partition_id=producer, generation=spec.route_epoch,
                    status="succeeded", manifest_ref=ArtifactRef(**payload["manifest_ref"]),
                    last_sequence=payload["last_sequence"])
                members = self._finite_completions.setdefault(spec.channel_id, {})
                if producer in members and members[producer] != completion:
                    raise DataflowError("INVALID_CONTRACT", "conflicting finite producer completion")
                members[producer] = completion
                if set(members) == set(spec.producer_partitions):
                    seal = seal_scope(spec.scope_id, tuple(members.values()), store=self._store)
                    for partition in spec.consumer_partitions:
                        target = self._by_partition[partition]
                        delivery = identity([seal.digest, partition, "scope-delivery"])
                        self._deliveries[delivery] = (None, target)
                        self._idle.discard(target._allocation_id)
                        target.send_business({"kind": "finite_input", "edge_id": spec.edge_id,
                                              "seal_ref": _artifact_json(seal), "delivery_id": delivery})
            elif payload["scope"] == "edge":
                spec = next(x for x in self._physical.exchanges if x.edge_id == payload["edge_id"])
                if payload["producer_partition"] not in lease.partition_ids:
                    raise DataflowError("FENCED", "EOI producer differs from allocation")
                for target_id in spec.consumer_partitions:
                    target = self._by_partition[target_id]
                    self._idle.discard(target._allocation_id)
                    target.send_business(dict(payload, kind="edge_eoi"))
            else:
                from .dataflow_checkpoint import _cursor_from_json
                assignment = endpoint._assignment
                allowed = {s.split_id for s in assignment.source_splits}
                cursors = tuple(_cursor_from_json(c) for c in payload["cursors"])
                if {c.split_id for c in cursors} != allowed:
                    raise DataflowError("FENCED", "EOI cursors differ from source assignment")
                pruned = set(payload.get("pruned_splits", ()))
                if not pruned <= allowed:
                    raise DataflowError("FENCED", "pruned input differs from source assignment")
                for split in assignment.source_splits:
                    if split.split_id in pruned and not self._source_demand_satisfied(split.source_id):
                        raise DataflowError("FENCED", "source stop lacks downstream completion proof")
                self._checkpoint.observe_input_satisfied(pruned)
                self._checkpoint.observe_source_eoi(cursors)
                self._done.add(event.allocation_id)
        elif event.kind == "idle":
            self._idle.add(event.allocation_id)
        elif event.kind == "cut":
            from .contracts.dataflow_codec import decode_wire
            raw = b"".join(self._store.read(event.payload_ref, max_bytes=self._bounds.max_control_bytes))
            contribution = decode_wire(raw, type_name="CutContribution", bounds=self._bounds)
            from .contracts.dataflow_codec import compute_participant_id
            expected = {compute_participant_id(lease.stage_id, p) for p in lease.partition_ids}
            if contribution.boot_id != lease.boot_id or contribution.participant_id not in expected:
                raise DataflowError("FENCED", "cut contribution differs from worker assignment")
            self._checkpoint.contribute(contribution)
            self._contributions[contribution.participant_id] = contribution

    def _source_demand_satisfied(self, occurrence):
        outgoing = [edge for edge in self._graph.edges if edge.producer == occurrence]
        if not outgoing:
            return False
        for edge in outgoing:
            spec = next((x for x in self._physical.exchanges if x.edge_id == edge.edge_id), None)
            if spec is not None and set(spec.consumer_partitions) <= self._satisfied_consumers.get(edge.edge_id, set()):
                continue
            if not self._source_demand_satisfied(edge.consumer):
                return False
        return True

    def _ack(self, delivery):
        producer, _ = self._deliveries.pop(delivery)
        if producer is not None:
            producer.send_business({"kind": "ack", "delivery_id": delivery})

    def _advance_cut(self):
        if self._state in ("succeeded", "failed", "cancelled"):
            return
        all_workers = set(self._endpoints)
        if (self._cut is None and all_workers and not self._deliveries
                and all_workers <= self._idle and (self._stopping or self._checkpoint_requested or all_workers <= self._done)):
            self._source_exhausted = self._checkpoint._sources_exhausted()
            self._cut = self._checkpoint.begin(final=self._checkpoint.inputs_complete())
            for endpoint in self._endpoints.values():
                endpoint.request_cut(self._cut)
        if self._cut is not None and set(self._cut.participants) <= self._contributions.keys():
            image = self._checkpoint.commit(self._cut.cut_id, sinks=self._sinks)
            self._restore_image = image
            self._last_checkpoint_rows = sum(m['rows_output'] for (occ, _), m in self._node_metrics.items()
                                             if occ in self._graph.sources)
            self._checkpoint_at = time.monotonic()
            for endpoint in self._endpoints.values():
                endpoint.send_business({"kind": "receipt", "cut_id": self._cut.cut_id,
                                        "commit_id": image.commit_id})
            if self._stopping or all_workers <= self._done:
                self._set_result(image)
            else:
                self._cut = None
                self._contributions.clear()
                self._idle.clear()
                self._checkpoint_requested = False
                for endpoint in self._endpoints.values():
                    endpoint.send_business({"kind": "resume"})

    def request_checkpoint(self):
        if self._cut is not None or self._checkpoint_requested or self._stopping:
            return
        self._checkpoint_requested = True
        self._idle.clear()
        for endpoint in self._endpoints.values():
            endpoint.send_business({"kind": "pause"})

    def _publication(self, image):
        result_ref = image.confirmed_sinks[self._request.kind]
        return json.loads(b"".join(self._store.read(result_ref, max_bytes=self._bounds.max_control_bytes)))

    def _set_result(self, image):
        publication = self._publication(image)
        rows = publication["manifest"]["total_rows"]
        self._confirmed_rows = rows
        self._source_exhausted = image.cut.final and all(c.end_of_input
            for contribution in image.contributions for c in contribution.sources)
        write = None
        if self._request.kind in ("write_lance", "write_datasink"):
            from ..data.datasink import WriteResult
            fact = self._action.context.journal.execution_commit(image.commit_id)
            receipt = fact["receipts"][self._request.kind]["data"]["native_receipt"]
            write = WriteResult(written_rows=rows, blocks_written=len(publication["manifest"]["partitions"]),
                                metadata={"native_receipt": receipt})
        self._terminal_result = TerminalResult(kind=self._request.kind, rows=rows,
            scalar=rows if self._request.kind == "count" else publication["manifest"]["partitions"][0].get("scalar"),
            result_ref=image.confirmed_sinks[self._request.kind], write_result=write, complete=True,
            completion="source_exhausted" if self._source_exhausted else "terminal_satisfied")
        self._state = "succeeded"

    def _published_records(self, publication):
        from .dataflow_handles import _ChunkStream
        from .exchange.dataflow_bridge import _decode_record_document
        for partition in publication["manifest"]["partitions"]:
            ref = ArtifactRef(**partition["data_ref"])
            stream = _ChunkStream(self._store.read(ref, max_bytes=max(1, ref.size_bytes)))
            while True:
                line = stream.readline(self._bounds.max_row_bytes + 1)
                if not line:
                    break
                item = json.loads(line)
                yield _decode_record_document(item["document"].encode(), record_id=item["record_id"],
                    sequence=item["sequence"], max_bytes=self._bounds.max_row_bytes)

    # -- E006 finish ------------------------------------------------------
    def finish(self):
        if self._failure is not None:
            raise self._failure
        if self._terminal_result is None:
            raise DataflowError("INVALID_CONTRACT", "finish requires a confirmed terminal cut")
        return self._terminal_result

    # -- E007 cancel ------------------------------------------------------
    def cancel(self, reason: str) -> None:
        if self._cancelled or self._closed:
            return
        self._cancelled = True
        self._state = "cancelled"
        if self._cut is not None and self._cut.cut_id in self._checkpoint._pending:
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
        # Finish actor aclose and reader teardown while the driver journal is
        # still reachable. Physical termination remains the final fallback.
        for endpoint in self._endpoints.values():
            try:
                if not endpoint.closed():
                    endpoint.drain(deadline_ms=deadline_ms)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
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


class _OutputSubscription:
    def __init__(self, execution):
        self.execution = execution
        self.closed = False

    def read(self, *, max_records, deadline_ms):
        from .exchange.dataflow_transport import read_peer
        from .exchange.dataflow_bridge import _decode_record_document
        e = self.execution
        records = []
        while not self.closed and e._replay is not None and len(records) < max_records:
            try:
                records.append(next(e._replay))
            except StopIteration:
                e._replay = None
        while not self.closed and e._terminal_queue and len(records) < max_records:
            payload = e._terminal_queue.popleft()
            raw = read_peer(payload["peer"], bounds=e._bounds)
            records.append(_decode_record_document(raw, record_id=payload["record_id"],
                sequence=payload["sequence"], max_bytes=e._bounds.max_row_bytes))
            e._ack(payload["delivery_id"])
        return tuple(records)

    def close(self):
        if self.closed:
            return
        self.closed = True
        while self.execution._terminal_queue:
            payload = self.execution._terminal_queue.popleft()
            self.execution._ack(payload["delivery_id"])
