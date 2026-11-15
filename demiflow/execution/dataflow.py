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
        execution._failure = failure
        execution._state = 'failed'
        try:
            execution.close()
        except Exception as cleanup:
            failure.add_note("Execution cleanup also failed: " + str(cleanup))
        try:
            from .dataflow_api import record_execution_metadata
            record_execution_metadata(request, execution)
        except Exception as observation:
            failure.add_note('Execution metadata also failed: ' + str(observation))
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
        self._active_worker_limit = self._policy.max_active_workers
        self._bounds = services.bounds
        from .dataflow_native_limits import NativeBudgetGuard
        self._native_limits = self._policy.native_limits
        self._native_guard = NativeBudgetGuard(self._native_limits)
        self._began_at = time.monotonic()
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
        self._verified_completed_image: RecoveryImage | None = None
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
        self._finite_seals = {}
        self._phases = ()
        self._phase_index = 0
        self._completed_stages = set()
        self._durable_residency = None
        self._satisfied_consumers = {}
        self._terminal_stop_sources = {}
        self._terminal_queue = deque()
        self._terminal_subscription_closed = False
        self._terminal_seen = 0
        self._idle = set()
        self._done = set()
        self._stopping = False
        self._limit = (request.arguments.get("limit") if request.kind == "take" else
                       request.arguments.get("batch_size", 20) if request.kind == "take_batch" else None)
        self._failure = None
        self._checkpoint_requested = False
        self._checkpoint_deferred = set()
        self._replay = None
        self._node_metrics = {}
        self._observed_workers = {}
        self._current_worker_observations = {}
        self._observed_worker_count = 0
        self._worker_detail_bytes = 0
        self._active_worker_allocations = set()
        self._peak_active_workers = 0
        self._report_keys = {}
        self._last_checkpoint_rows = 0
        self._last_checkpoint_outputs: dict[str, int] = {}
        self._checkpoint_deliveries = 0
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
        self._checkpoint_deferred.clear()
        self._idle.clear()
        for allocation_id, endpoint in self._endpoints.items():
            endpoint.send_business({"kind": "pause"})
            endpoint.send_business({"kind": "satisfy"})
            # This is permission to stop this live source owner, not proof that
            # its input was exhausted. A prefix cut must remain non-final.
            lease = self._scheduler.leases[allocation_id]
            self._terminal_stop_sources[allocation_id] = (
                lease.boot_id, self._attempt(), self._fence(), self._physical.plan_revision,
                frozenset(split.split_id for split in endpoint._assignment.source_splits))

    # -- helpers ----------------------------------------------------------
    def _fence(self) -> str:
        return self._action.context.fence

    def _attempt(self) -> int:
        return self._action.context.attempt

    def _run_id(self) -> str:
        return self._action.context.ref.run_id

    def _check_stop(self) -> None:
        self._action.context.check_stop()
        self._native_guard.check()
        if time.monotonic() - self._began_at >= self._bounds.timeout_ms / 1000:
            raise DataflowError('DEADLINE_EXCEEDED', 'action exceeded its admitted execution timeout')

    def _stage_order(self) -> list:
        stages = list(self._physical.stages)
        terminals = set(self._graph.terminals) if self._graph is not None else set()

        def consumer_first(stage):
            return 0 if any(occ in terminals for occ in stage.occurrences) else 1

        return sorted(stages, key=consumer_first)

    def _initial_work(self, stage, partition_id: str) -> ReadyWork:
        generation = (":" + str(self._durable_residency.epoch)) if self._durable_residency is not None else ""
        operation_id = hashlib.sha256(
            ("start:" + self._physical.plan_id + ":" + stage.stage_id + ":" + partition_id + generation).encode()
        ).hexdigest()
        resident = max(1, stage.resident_bytes, stage.profile.requests['memory_bytes'])
        return ReadyWork(stage_id=stage.stage_id, partition_id=partition_id,
                         input_refs=(), resident_bytes=resident, operation_id=operation_id)

    def _place_hosts(self, pending):
        """Plan bounded per-host admission; the ResourceManager still allocates.

        Summing cluster capacity cannot prove that any individual worker fits.
        Bind each partition to a host whose runtime/store were actually probed.
        """
        if self._active_worker_limit is not None and len(pending) > self._active_worker_limit:
            raise DataflowError('RESOURCE_INFEASIBLE', 'resident group exceeds active worker budget')
        if sum(stage.profile.requests['memory_bytes'] for stage, _ in pending) > self._bounds.max_resident_bytes:
            raise DataflowError('RESOURCE_INFEASIBLE', 'resident group exceeds admitted memory budget')
        remaining = {h.host_id: dict(h.logical_resources) for h in self._services.hosts}
        assigned = {}
        for stage, work in sorted(pending, key=lambda pair:
                (pair[0].profile.requests['memory_bytes'], pair[0].profile.requests['cpu_millis']), reverse=True):
            need = {key: stage.profile.requests[key] for key in ('cpu_millis', 'memory_bytes')}
            from ..planning.dataflow_place import _gpu_count
            need['gpu'] = _gpu_count(stage.profile)
            feasible = [host for host, capacity in remaining.items()
                        if all(need[key] <= capacity.get(key, 0) for key in need)]
            if not feasible:
                raise DataflowError('RESOURCE_INFEASIBLE',
                    'No verified host has capacity for this worker placement')
            host = max(feasible, key=lambda key: remaining[key]['cpu_millis'])
            assigned[work.operation_id] = host
            for key, amount in need.items():
                remaining[host][key] = remaining[host].get(key, 0) - amount
        return assigned

    def _assignment(self, lease: WorkerLease, stage) -> WorkerAssignment:
        partitions = set(stage.partitions)
        # A fused stage calls its internal logical edges directly. Transport
        # ports are only the stage boundary; registering an internal edge as
        # both input and output otherwise opens its channel twice.
        inputs = tuple(x for x in self._physical.exchanges
                       if partitions.intersection(x.consumer_partitions)
                       and not partitions.intersection(x.producer_partitions))
        outputs = tuple(x for x in self._physical.exchanges
                        if partitions.intersection(x.producer_partitions)
                        and not partitions.intersection(x.consumer_partitions))
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
            source_splits=splits, restore_refs=restore_refs, bounds=self._bounds,
            native_limits=self._native_limits)

    def _prepare_launch(self, lease: WorkerLease, stage) -> WorkerLease:
        grant_bytes = self._scheduler.grants.get(lease.allocation_id)
        if grant_bytes is not None:
            staged = self._store.put(iter((grant_bytes,)), max_bytes=self._bounds.max_control_bytes,
                                     media_type="application/json")
            from dataclasses import replace
            lease = replace(lease, grant_ref=staged)
        self._services.journal_service.register_worker(lease, stage.occurrences)
        endpoint = make_host_endpoint(lease, services=self._services)
        from .dataflow_residency import participants_for
        owns_image = self._restore_image is not None and any(
            c.participant_id in participants_for(self._physical, (stage.stage_id,))
            for c in self._restore_image.contributions)
        endpoint._recovery_image = self._restore_image if owns_image else None
        endpoint.prepare(self._assignment(lease, stage))
        self._endpoints[lease.allocation_id] = endpoint
        return lease

    def _activate(self, lease: WorkerLease, stage) -> None:
        endpoint = self._endpoints[lease.allocation_id]
        endpoint.activate(self._assignment(lease, stage))
        # A successful activation receipt means the original leased worker is
        # resident. EOI is not an exit; only confirmed retirement removes it.
        self._active_worker_allocations.add(lease.allocation_id)
        self._peak_active_workers = max(self._peak_active_workers,
                                        len(self._active_worker_allocations))
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
        from .queue_producer import producer_actor
        from .dataflow_producer import NativeProducerService
        producer = producer_actor(self._request.plan)
        if producer is not None:
            if self._request.kind != 'run_stream':
                raise DataflowError('INVALID_CONTRACT', 'enqueue requires its source-unit run_stream terminal')
            if self._policy.default_partitions != 1:
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'Native producer has one ordered source owner')
            from dataclasses import replace
            self._policy = replace(self._policy, checkpoint_rows=1)
            self._acquisition = NativeProducerService(self._action, services=self._services)
            # Resolve native source/search ownership before staging passive
            # worker declarations. No actor is started by configure.
            producer[1].configure(self._action, self._request.source, self._request.plan)
        existing = self._action.context.journal.source_binding(self._action.action_id + "-dataflow-sealed")
        completed_live_nodes = []
        if self._action.replaying and existing is not None:
            from .dataflow_seal import load_dataflow, load_sealed_dataflow, verify_dataflow_program
            from .recovery_identity import platform_identity
            graph, physical, sealed = load_dataflow(self._action.context.directory,
                self._action.action_id, store=self._store, bounds=self._bounds)
            from .dataflow_schema import validate_literal_schema_replay
            validate_literal_schema_replay(self._request, graph, bounds=self._bounds)
            _, _, pin = load_sealed_dataflow(sealed, action_id=self._action.action_id,
                                             store=self._store, bounds=self._bounds)
            if pin.get("platform_revision") != platform_identity():
                raise DataflowError("CODE_MISMATCH", "recovery requires the sealed runtime revision")
            for node in graph.nodes:
                if node.program_ref is not None:
                    if node.kernel.configuration.get('thread_attempt'):
                        completed_live_nodes.append(node)
                        continue
                    verify_dataflow_program(node.program_ref, expected_pin=pin["programs"][node.occurrence_id],
                                            store=self._store, bounds=self._bounds)
        else:
            compiled = lower_action(self._request, bounds=self._bounds)
            graph = stage_dataflow_programs(compiled, action=self._action, store=self._store,
                                            bounds=self._bounds, worker_mode=self._policy.worker_mode)
            if producer is not None:
                self._acquisition.configure(self._request.source, self._request.plan, None, graph=graph)
            snapshots, splits = [], []
            for declaration in compiled.declarations:
                if declaration.source_spec is None:
                    continue
                snapshot = snapshot_source(declaration.source_spec, action=self._action,
                                           store=self._store, bounds=self._bounds,
                                           source_id=declaration.occurrence_id)
                if producer is not None:
                    snapshot, source_splits = self._acquisition.source_layout(snapshot)
                else:
                    source_splits = plan_source_splits(snapshot, declaration.source_spec,
                        target_partitions=self._policy.default_partitions, bounds=self._bounds)
                snapshots.append(snapshot)
                splits.extend(source_splits)
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
            sealed = seal_dataflow(self._action, graph, physical, store=self._store, identity_bridge=bridge,
                                   observation_labels=compiled.observation_labels)
            stage_runtime_bundle(sealed, store=self._store, bounds=self._bounds)
        # Preserve the sealed plan; current execution budgets only intersect
        # its original ceilings. The same store/journal/allocator retain ownership.
        from dataclasses import replace
        from .dataflow_native_limits import NativeBudgetGuard, intersect_bounds, intersect_native_limits
        self._bounds = intersect_bounds(physical.bounds, self._bounds)
        self._native_limits = intersect_native_limits(physical.policy.native_limits, self._native_limits)
        self._native_guard = NativeBudgetGuard(self._native_limits)
        live_policy = replace(physical.policy, native_limits=self._native_limits)
        self._services = replace(self._services, bounds=self._bounds, policy=live_policy)
        self._store._bounds = self._bounds
        self._store._journal._bounds = self._bounds
        self._services.journal_service._bounds = self._bounds
        self._check_stop()
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
                if self._request.kind in ("write_lance", "write_datasink", "write_file"):
                    from .dataflow_sinks import make_native_terminal_sink
                    self._sinks[node.kernel.kind] = make_native_terminal_sink(node.kernel,
                        request=self._request, services=self._services)
                else:
                    self._sinks[node.kernel.kind] = make_sink_adapter(node.kernel, services=self._services)
        from .dataflow_images import bind_image_sinks
        bind_image_sinks(graph, services=self._services, sinks=self._sinks)
        self._graph = graph
        self._physical = physical
        self._policy = live_policy
        # Historical physical plans stay byte-identical. Current caller limits
        # may only tighten their live admission envelope, never enlarge it.
        limits = [v for v in (self._active_worker_limit, physical.policy.max_active_workers) if v is not None]
        self._active_worker_limit = min(limits) if limits else None
        self._sealed = sealed
        self._scheduler = DataflowScheduler(physical, resources=self._services.resources)
        self._scheduler._policy = live_policy
        from .contracts.dataflow_codec import runtime_scope_id
        self._scheduler.scope_id = runtime_scope_id(physical.plan_id, self._attempt())
        self._checkpoint = DataflowCheckpointCoordinator(self._action, physical, store=self._store)
        if completed_live_nodes:
            # A final result belongs to the original journal, not the released
            # live registry. Authenticate that result through the SAME native
            # coordinator before admitting a code-only check. No non-final
            # live program is resumed and no replacement actor is constructed.
            image = self._checkpoint.recover(sinks=self._sinks)
            if image is None or not image.cut.final:
                raise DataflowError('INCOMPATIBLE_HISTORY',
                                    'attempt-only live program has no final ACKed result')
            from .dataflow_seal import verify_completed_live_program
            current_declarations = lower_action(self._request, bounds=self._bounds).declarations
            declarations = {d.occurrence_id: d for d in current_declarations}
            for node in completed_live_nodes:
                verify_completed_live_program(node.program_ref,
                    expected_pin=pin['programs'][node.occurrence_id],
                    declaration=declarations.get(node.occurrence_id), store=self._store, bounds=self._bounds)
            self._verified_completed_image = image
        if producer is not None:
            if self._acquisition._state == 'constructed':
                self._acquisition.configure(self._request.source, self._request.plan, None, graph=graph)
            self._services.journal_service.bind_queue(self._acquisition)
        from .queue_execution import queue_source
        requires_acquisition = queue_source(self._request.source) is not None or any(
            node.kernel.configuration.get('native_operation') == 'acquire' for node in graph.nodes)
        if requires_acquisition:
            self._acquisition = NativeAcquisitionService(self._action, services=self._services)
            self._acquisition.configure(self._request.source, self._request.plan, self._request.sink, graph=graph)
            self._services.journal_service.bind_queue(self._acquisition)
        if any(node.kernel.configuration.get('native_operation') == 'search' for node in graph.nodes):
            if self._acquisition is None:
                self._acquisition = NativeAcquisitionService(self._action, services=self._services)
            self._acquisition.configure_searches(self._request.source, self._request.plan, graph)
            self._services.journal_service.bind_queue(self._acquisition)
        from .dataflow_batch_membership import bind_contracts
        bind_contracts(self._services.journal_service, graph, physical)
        self._stream_lance = None
        if any(node.kernel.kind == 'SaveLanceOp' for node in graph.nodes):
            from .dataflow_stream_lance import NativeStreamLanceService
            self._stream_lance = NativeStreamLanceService(self)
            self._services.journal_service._stream_lance = self._stream_lance
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
        budget = {"cpu_millis": max(1, int(sum(h.logical_resources.get('cpu_millis', 0)
                                             for h in self._services.hosts))),
                  "memory_bytes": self._bounds.max_resident_bytes,
                  "max_workers": self._active_worker_limit or self._bounds.max_partitions}
        gpu_budget = int(sum(host.logical_resources.get('gpu', 0) for host in self._services.hosts))
        if gpu_budget:
            budget['accelerator_gpu'] = gpu_budget
        self._services.resources.state.open_scope(self._fence(), ResourceScope(
            self._scheduler.scope_id, self._run_id(), {"provider": self._policy.provider},
            self._fence(), budget))
        self._scope_open = True
        if self._action.replaying and self._action.context.journal.source_binding(
                self._action.action_id + '-native-model-journal') is not None:
            self._services.journal_service._native_journal().check_recovery()
        queue_owned_recovery = (self._acquisition is not None and
            self._acquisition.can_recover_without_dataflow_cut())
        if self._stream_lance is not None:
            self._stream_lance.start()
            queue_owned_recovery = queue_owned_recovery or self._stream_lance.can_recover_without_cut()
        self._restore_image = self._verified_completed_image or (self._checkpoint.recover(sinks=self._sinks)
            if self._action.replaying and not queue_owned_recovery else None)
        if self._restore_image is not None:
            from .dataflow_operator_observations import restore_operator_observations
            restore_operator_observations(self, self._restore_image)
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
            from .dataflow_residency import participants_for
            residency = self._checkpoint.residency or {}
            retired = set(residency.get('completed_stages', ()))
            if residency.get('phase_complete'):
                retired.update(residency['active_stages'])
            retired_ids = set(participants_for(self._physical, retired))
            for contribution in self._restore_image.contributions:
                if contribution.participant_id in retired_ids:
                    continue  # Its original ACKed output is restored, not its actor.
                for occ, ref in contribution.operator_states.items():
                    if occ not in transforms or ref is None:
                        continue
                    image = state_codec.loads(b"".join(self._store.read(ref,
                        max_bytes=self._bounds.max_state_bytes)), max_bytes=self._bounds.max_state_bytes)
                    for state in [image.get('state'), *(image.get('worker_states') or [])]:
                        if state and state.get("mode") == "nonrecoverable":
                            raise DataflowError("INCOMPATIBLE_HISTORY", "actor state cannot resume: " + state["reason"],
                                                context={"occurrence_id": occ})
        # This runner keeps its participants alive through the same cut. Never
        # promise phased residency then leave an unstarted participant forever
        # pending. Check before native acquisition or any business activation.
        self._configure_phases()
        if self._acquisition is not None:
            self._acquisition._restore_image = self._restore_image
            self._acquisition.start()
        self._launch_phase()
        self._started = True
        self._state = "running"
        self._checkpoint_at = time.monotonic()

    def _configure_phases(self):
        from .dataflow_residency import pack_finite_phases
        stages = self._stage_order()
        def phase_fits(phase):
            group = [s for s in stages if s.stage_id in phase]
            memory = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) * len(s.partitions) for s in group)
            if memory + self._physical.residency.control_reserved_bytes > self._bounds.max_resident_bytes:
                return False
            try:
                self._place_hosts([(s, self._initial_work(s, p)) for s in group for p in s.partitions])
            except DataflowError as exc:
                if exc.code != 'RESOURCE_INFEASIBLE':
                    raise
                return False
            return True
        pending = [(stage, self._initial_work(stage, partition))
                   for stage in stages for partition in stage.partitions]
        resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) * len(s.partitions) for s in stages)
        fits = resident + self._physical.residency.control_reserved_bytes <= self._bounds.max_resident_bytes
        if fits:
            try:
                self._place_hosts(pending)
            except DataflowError as exc:
                if exc.code != 'RESOURCE_INFEASIBLE':
                    raise
                fits = False
        prior = self._checkpoint.residency
        durable = any(edge.mode == 'durable_incremental' for edge in self._physical.exchanges)
        if durable and (not fits or prior is not None and prior.get('schema') == 'demiflow.durable_residency.v1'):
            from .dataflow_durable_residency import DurableResidency
            if any(source.replay == 'attempt_only' for source in self._physical.sources):
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'attempt-only source cannot park and restore within an action')
            if prior is not None:
                self._finite_seals = {key: ArtifactRef(**ref) for key, ref in prior['finite_seals'].items()}
            self._durable_residency = DurableResidency(self)
            self._phases = self._durable_residency.groups
            self._phase_index = self._durable_residency.active_index
        else:
            # A prepend cannot consume its live input until its finite prefix
            # has reached EOI. Admit that prefix's existing finite phase first.
            needs_prefix = any(n.kernel.kind == 'StreamPrependOp' for n in self._graph.nodes)
            self._phases = ((tuple(s.stage_id for s in stages),) if fits and prior is None and not needs_prefix
                            else pack_finite_phases(self._physical, fits=phase_fits, prior=prior))
        # Preflight EVERY phase before business side effects.
        for phase in self._phases:
            group = [s for s in stages if s.stage_id in phase]
            resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) * len(s.partitions) for s in group)
            if resident + self._physical.residency.control_reserved_bytes > self._bounds.max_resident_bytes:
                raise DataflowError('RESOURCE_INFEASIBLE', 'streaming-connected stage group exceeds resident capacity')
            self._place_hosts([(s, self._initial_work(s, p)) for s in group for p in s.partitions])
        if prior is not None and self._durable_residency is None:
            self._completed_stages = set(prior['completed_stages'])
            if prior['phase_complete']:
                self._completed_stages.update(prior['active_stages'])
            self._finite_seals = {key: ArtifactRef(**ref) for key, ref in prior['finite_seals'].items()}
            self._phase_index = next((i for i, group in enumerate(self._phases)
                if not set(group) <= self._completed_stages), len(self._phases))
            if self._phase_index == len(self._phases):
                raise DataflowError('CORRUPT_ARTIFACT', 'completed residency lacks final publication')

    def _launch_phase(self):
        phase = self._phases[self._phase_index]
        stages = [s for s in self._stage_order() if s.stage_id in phase]
        pending = [(stage, self._initial_work(stage, partition))
                   for stage in stages for partition in stage.partitions]
        hosts = self._place_hosts(pending)
        ready = []
        from .dataflow_native_limits import admission_timeout_ms
        deadline = time.monotonic() + admission_timeout_ms(self._policy) / 1000
        while pending:
            deferred = []
            for stage, work in pending:
                self._check_stop()
                try:
                    lease = self._scheduler.place(work, prepare=self._prepare_launch, host_id=hosts[work.operation_id])
                except DataflowError as exc:
                    if exc.retryable:
                        deferred.append((stage, work))
                        continue
                    raise
                ready.append((lease, stage))
            pending = deferred
            if pending:
                if time.monotonic() >= deadline:
                    raise DataflowError('CAPACITY_PENDING', 'Worker admission deadline exceeded before business activation', retryable=True)
                time.sleep(min(.01, max(0, deadline - time.monotonic())))
        for lease, stage in ready:
            self._activate(lease, stage)
        if self._durable_residency is not None:
            self._durable_residency.launched()
        for spec in self._physical.exchanges:
            seal = self._finite_seals.get(spec.channel_id)
            if seal is not None:
                self._deliver_finite(spec, seal)

    def _deliver_finite(self, spec, seal):
        from .dataflow_sinks import _artifact_json
        from .recovery_contract import identity
        for partition in spec.consumer_partitions:
            target = self._by_partition.get(partition)
            if target is None:
                continue  # Unstarted stage has no observation or contribution.
            delivery = identity([seal.digest, partition, 'scope-delivery'])
            if delivery in self._deliveries:
                continue
            self._deliveries[delivery] = (None, target)
            self._idle.discard(target._allocation_id)
            target.send_business({'kind': 'finite_input', 'edge_id': spec.edge_id,
                                  'seal_ref': _artifact_json(seal), 'delivery_id': delivery})

    def _retire_phase(self, image):
        # ACK is durable before any live participant disappears. The original
        # contribution keeps its original cut/boot in every subsequent carrier.
        receipt = image.confirmed_sinks[self._request.kind]
        for operation_id in tuple(self._scheduler._inflight):
            self._scheduler.complete(operation_id, receipt_ref=receipt)
        for endpoint in self._endpoints.values():
            try:
                endpoint.drain(deadline_ms=self._policy.control_timeout_ms)
            except BaseException as error:
                try:
                    self._drain_worker_observations(endpoint)
                except BaseException as observation_error:
                    error.add_note('Final observations also failed: ' + str(observation_error))
                raise
            else:
                self._drain_worker_observations(endpoint)
        for stage_id in self._phases[self._phase_index]:
            self._scheduler.drain(stage_id, deadline_ms=self._policy.control_timeout_ms)
        for endpoint in self._endpoints.values():
            self._services.journal_service.retire_worker(endpoint._assignment.lease)
        from .dataflow_staging import recover_staging
        recover_staging(self._request.action, bounds=self._bounds)
        if self._durable_residency is not None:
            self._durable_residency.committed(image, set(self._endpoints) <= self._done)
            self._phase_index = self._durable_residency.active_index
        else:
            self._completed_stages.update(self._phases[self._phase_index])
            self._phase_index += 1
        self._active_worker_allocations.difference_update(self._endpoints)
        self._current_worker_observations.clear()
        self._endpoints.clear()
        self._by_partition.clear()
        self._lease_by_operation.clear()
        self._idle.clear()
        self._done.clear()
        self._cut = None
        self._contributions.clear()
        self._checkpoint_requested = False
        self._checkpoint_deferred.clear()
        self._launch_phase()

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
        self._renew_workers()
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
                              "check_grant", "restrict_recovery", "queue", "native_request"):
                self._services.journal_service.submit(event)
            else:
                self._handle_event(event)
        progress = self._checkpoint_progress()
        # A finite input may already be completely committed while its join or
        # sort still emits many rows. Actual output/ACK progress must also drive
        # the original checkpoint policy; source-only progress stalls that cut.
        if (progress > 0 and set(self._endpoints) - self._done
                and (progress >= self._policy.checkpoint_rows
                     or (time.monotonic() - self._checkpoint_at) * 1000 >= self._policy.checkpoint_interval_ms)):
            self.request_checkpoint()
        self._advance_cut()
        if not events and not replies:
            pause = min(.002, deadline_ms / 1000)
            sockets = [getattr(endpoint, '_sock', None) for endpoint in self._endpoints.values()]
            if self._policy.provider == 'local' and sockets and all(sock is not None for sock in sockets):
                # The same driver still owns every read and control reply.
                # Wait for readiness instead of sleeping through a newly
                # produced row or RPC. Journal replies can finish while no
                # worker can make progress.
                # Their readiness wakes this same driver; results still come
                # exclusively from the original bounded poll_replies queue.
                reply_waitable = getattr(self._services.journal_service, 'reply_waitable', None)
                notifier = reply_waitable() if reply_waitable is not None else None
                if notifier is not None:
                    sockets.append(notifier)
                import select
                select.select(sockets, [], [], pause)
            else:
                time.sleep(pause)
        return ExecutionStatus(state=self._state, pending_events=self._pending_events,
                               confirmed_rows=self._confirmed_rows)

    def _checkpoint_frontiers(self):
        """Aggregate partitions of each original logical occurrence, not depth."""
        sources = set(self._graph.sources)
        source_rows, outputs = 0, {}
        # Observations are checked against the sealed assignments before entry.
        # Thus this map has at most bounds.max_nodes entries, independent of the
        # number of records, cuts, boots or physical partitions.
        for (occurrence, _), metrics in self._node_metrics.items():
            if occurrence in sources:
                source_rows += metrics['rows_output']
            else:
                outputs[occurrence] = outputs.get(occurrence, 0) + metrics['rows_output']
        return source_rows, outputs

    def _checkpoint_progress(self):
        source_rows, outputs = self._checkpoint_frontiers()
        progress = max(source_rows - self._last_checkpoint_rows,
                       self._checkpoint_deliveries,
                       max((rows - self._last_checkpoint_outputs.get(occurrence, 0)
                            for occurrence, rows in outputs.items()), default=0))
        # A newly active stage must be compared with its own ACKed frontier:
        # taking a global maximum before subtraction hides it behind old stages.
        # Normal observations already reject backwards counters. If an internal
        # restoration/reset nevertheless regresses a frontier, request a cut
        # conservatively instead of waiting for it to catch up to the old value.
        if (source_rows < self._last_checkpoint_rows or any(
                outputs.get(occurrence, 0) < rows
                for occurrence, rows in self._last_checkpoint_outputs.items())):
            progress = max(progress, self._policy.checkpoint_rows)
        return progress

    def _renew_workers(self):
        """Extend only a live grant belonging to this still-fenced driver.

        Renewal travels on the existing authenticated control channel. A driver
        that stops pumping cannot extend authorization, and an expired lease is
        never resurrected. No business operation is retried by renewal.
        """
        from dataclasses import replace
        from .dataflow_scheduler import lease_ttl_ms
        now = self._services.clock_ms()
        ttl = lease_ttl_ms(self._policy)
        for allocation_id, lease in tuple(self._scheduler.leases.items()):
            if lease.expires_at_ms - now > ttl // 2:
                continue
            if lease.expires_at_ms <= now:
                raise DataflowError('FENCED', 'Worker authorization expired before driver renewal')
            renewed = replace(lease, expires_at_ms=now + ttl)
            endpoint = self._endpoints[allocation_id]
            endpoint.send_business({'kind': 'renew_lease', 'run_id': self._run_id(),
                'attempt': self._attempt(), 'fence': self._fence(),
                'allocation_id': allocation_id, 'boot_id': lease.boot_id,
                'expires_at_ms': renewed.expires_at_ms})
            self._services.journal_service.renew_worker(renewed)
            self._scheduler.leases[allocation_id] = renewed

    def _handle_event(self, event):
        from .contracts.dataflow import thaw_record_value
        payload = thaw_record_value(event.payload) if event.payload is not None else {}
        endpoint = self._endpoints[event.allocation_id]
        lease = self._scheduler.leases[event.allocation_id]
        if event.kind == 'ready':
            if set(payload) != {'worker_process_id', 'runner_thread_id'} or any(
                    type(value) is not int or not 0 < value < 2**63 for value in payload.values()):
                raise DataflowError('INVALID_CONTRACT', 'worker identity observation must contain positive PID/TID')
            identity = {**payload, 'allocation_id': event.allocation_id,
                        'boot_id': event.boot_id, 'stage_id': lease.stage_id,
                        'partition_ids': list(lease.partition_ids)}
            previous = self._current_worker_observations.get(event.allocation_id)
            if previous is not None:
                if previous != identity:
                    raise DataflowError('FENCED', 'worker identity changed within the same allocation')
                return
            self._current_worker_observations[event.allocation_id] = identity
            self._observed_worker_count += 1
            # Durable phase rotation can create arbitrarily many boots. Keep an
            # exact count, but retain identity details within the control budget.
            import json
            detail_bytes = len(json.dumps(identity, separators=(',', ':')).encode('utf-8'))
            if (len(self._observed_workers) < self._bounds.max_partitions and
                    self._worker_detail_bytes + detail_bytes <= self._bounds.max_control_bytes):
                self._observed_workers[event.allocation_id] = identity
                self._worker_detail_bytes += detail_bytes
            return
        if event.kind == "failure":
            from .dataflow_failures import restore_failure
            self._failure = restore_failure(payload)
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
            from .dataflow_usage import observe_prompt_usage
            observe_prompt_usage(self, event, payload)
            from .dataflow_operator_observations import observe_operator
            observe_operator(self, event, payload)
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
                if (not self._terminal_subscription_closed
                        and len(self._terminal_queue) >= self._bounds.max_pending_events):
                    raise DataflowError("BUDGET_EXCEEDED", "terminal subscription credit exceeded")
                self._deliveries[delivery] = (endpoint, None)
                if self._terminal_subscription_closed:
                    # The worker already spooled this real output. A consumer
                    # closing its view releases transport ownership, including
                    # envelopes arriving after close, before the original cut.
                    self._ack(delivery)
                else:
                    self._terminal_queue.append(payload)
                self._terminal_seen += 1
                if self._limit is not None and self._terminal_seen >= self._limit:
                    self.satisfy_terminal()
            else:
                spec = next((x for x in self._physical.exchanges if x.edge_id == payload["edge_id"]), None)
                if (spec is None or payload["producer_partition"] not in spec.producer_partitions
                        or payload["target_partition"] not in spec.consumer_partitions):
                    raise DataflowError("FENCED", "delivery is outside physical channel membership")
                target = self._by_partition.get(payload["target_partition"])
                if self._durable_residency is not None and spec.mode == 'durable_incremental':
                    self._durable_residency.record(payload)
                    if target is None:
                        self._idle.discard(event.allocation_id)
                        return
                if target is None:
                    raise DataflowError('INVALID_CONTRACT', 'streaming consumer is absent without a durable park owner')
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
                        target = self._by_partition.get(producer)
                        if target is None:
                            continue  # A finite producer was already durably retired.
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
                    self._finite_seals[spec.channel_id] = seal
                    self._deliver_finite(spec, seal)
            elif payload["scope"] == "edge":
                spec = next(x for x in self._physical.exchanges if x.edge_id == payload["edge_id"])
                if payload["producer_partition"] not in lease.partition_ids:
                    raise DataflowError("FENCED", "EOI producer differs from allocation")
                if self._durable_residency is not None:
                    self._durable_residency.edge_eoi.add((spec.edge_id, payload['producer_partition']))
                for target_id in spec.consumer_partitions:
                    target = self._by_partition.get(target_id)
                    if target is not None:
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
                completed = set()
                for split in assignment.source_splits:
                    if split.split_id not in pruned:
                        continue
                    if self._terminal_stop_authorized(event, split.split_id):
                        continue  # Driver demand permits a prefix, never an EOI fact.
                    if not self._source_demand_satisfied(split.source_id):
                        raise DataflowError("FENCED", "source stop lacks downstream completion proof")
                    completed.add(split.split_id)
                self._checkpoint.observe_input_satisfied(completed)
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

    def _terminal_stop_authorized(self, event, split_id):
        authorized = self._terminal_stop_sources.get(event.allocation_id)
        return (authorized is not None
                and authorized[:4] == (event.boot_id, event.attempt, event.fence, event.plan_revision)
                and split_id in authorized[4])

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
        self._checkpoint_deliveries += 1
        if self._durable_residency is not None and self._durable_residency.actual_ack(delivery):
            if producer is not None:
                producer.send_business({"kind": "ack", "delivery_id": delivery})
                self._durable_residency.acked_live.add(delivery)
            return
        if producer is not None:
            producer.send_business({"kind": "ack", "delivery_id": delivery})

    def _advance_cut(self):
        if self._state in ("succeeded", "failed", "cancelled"):
            return
        all_workers = set(self._endpoints)
        durable = self._durable_residency
        if self._checkpoint_deferred:
            sources = all_workers - self._checkpoint_deferred
            if sources <= self._idle | self._done:
                # Stop admission before freezing consumers. A slow source read
                # must not prevent an already admitted batch from reaching its
                # downstream consumer while we wait for the input frontier.
                for allocation_id in self._checkpoint_deferred:
                    self._idle.discard(allocation_id)
                    self._endpoints[allocation_id].send_business({'kind': 'pause'})
                self._checkpoint_deferred.clear()
        if (self._cut is None and all_workers and (not self._deliveries or durable is not None and durable.can_cut())
                and all_workers <= self._idle and (durable is not None or self._stopping or self._checkpoint_requested or all_workers <= self._done)):
            self._source_exhausted = self._checkpoint._sources_exhausted()
            phase_done = all_workers <= self._done
            final = (phase_done and self._phase_index == len(self._phases) - 1
                     and self._checkpoint.inputs_complete())
            if durable is not None:
                from .dataflow_residency import participants_for
                final = durable.final(phase_done) and self._checkpoint.inputs_complete()
                self._cut = self._checkpoint.begin(final=final,
                    participants=participants_for(self._physical, self._phases[self._phase_index]),
                    residency=durable.metadata(phase_done))
            elif len(self._phases) > 1 or self._completed_stages:
                from .dataflow_residency import SCHEMA, participants_for
                from .dataflow_sinks import _artifact_json
                residency = {'schema': SCHEMA,
                    'active_stages': list(self._phases[self._phase_index]),
                    'completed_stages': sorted(self._completed_stages),
                    'phase_complete': phase_done,
                    'satisfied_source_splits': sorted(self._checkpoint._operator_complete),
                    'finite_seals': {key: _artifact_json(ref) for key, ref in self._finite_seals.items()}}
                self._cut = self._checkpoint.begin(final=final,
                    participants=participants_for(self._physical, self._phases[self._phase_index]),
                    residency=residency)
            else:
                self._cut = self._checkpoint.begin(final=final)
            for endpoint in self._endpoints.values():
                endpoint.request_cut(self._cut)
        if self._cut is not None and set(self._cut.participants) <= self._contributions.keys():
            from .dataflow_producer import NativeProducerService
            if isinstance(self._acquisition, NativeProducerService):
                self._acquisition.before_cut()
            if self._cut.final and self._acquisition is not None:
                self._native_summary = self._acquisition.finish()
            if durable is not None:
                durable.before_commit(self._cut, self._contributions)
            image = self._checkpoint.commit(self._cut.cut_id, sinks=self._sinks)
            # ObjectSink.prepare already transfers exactly its generation.
            # Calls finishing during publication append to the next generation.
            self._restore_image = image
            self._last_checkpoint_rows, self._last_checkpoint_outputs = self._checkpoint_frontiers()
            self._checkpoint_deliveries = 0
            self._checkpoint_at = time.monotonic()
            for endpoint in self._endpoints.values():
                endpoint.send_business({"kind": "receipt", "cut_id": self._cut.cut_id,
                                        "commit_id": image.commit_id})
            if durable is not None:
                if self._cut.final:
                    self._set_result(image)
                elif durable.final(all_workers <= self._done) and self._checkpoint.inputs_complete():
                    # Completion may arrive after begin froze a non-final cut.
                    # Obtain an actual final contribution from the still-live
                    # owner; never relabel an already committed cut as final.
                    self._cut = None
                    self._contributions.clear()
                    self._idle.clear()
                    for endpoint in self._endpoints.values():
                        endpoint.send_business({'kind': 'pause'})
                elif self._stopping:
                    self._set_result(image)
                else:
                    self._retire_phase(image)
            elif all_workers <= self._done and self._phase_index < len(self._phases) - 1:
                self._retire_phase(image)
            elif (all_workers <= self._done and self._checkpoint.inputs_complete()
                  and not image.cut.final):
                # Native calls can finish after begin froze a non-final cut.
                # Receipt that cut unchanged, then ask the same live owners for
                # a real final contribution before declaring the action done.
                self._cut = None
                self._contributions.clear()
                self._idle.clear()
                for endpoint in self._endpoints.values():
                    endpoint.send_business({'kind': 'pause'})
            elif self._stopping or all_workers <= self._done:
                self._set_result(image)
            else:
                self._cut = None
                self._contributions.clear()
                self._idle.clear()
                self._checkpoint_requested = False
                self._checkpoint_deferred.clear()
                for endpoint in self._endpoints.values():
                    endpoint.send_business({"kind": "resume"})

    def request_checkpoint(self):
        if self._cut is not None or self._checkpoint_requested or self._stopping:
            return
        self._checkpoint_requested = True
        self._idle.clear()
        sources = {allocation_id for allocation_id, endpoint in self._endpoints.items()
                   if endpoint._assignment.source_splits}
        # Durable residency has its own parked-frontier protocol. Finite phases
        # without sources likewise retain the ordinary all-participant pause.
        self._checkpoint_deferred = (set(self._endpoints) - sources
            if sources and self._durable_residency is None else set())
        for allocation_id, endpoint in self._endpoints.items():
            if allocation_id not in self._checkpoint_deferred:
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
        if self._request.kind in ("write_lance", "write_datasink", "write_file"):
            from ..data.datasink import WriteResult
            fact = self._action.context.journal.execution_commit(image.commit_id)
            receipt_data = fact["receipts"][self._request.kind]["data"]
            if getattr(self._action, "object_sinks", None):
                receipt_data = receipt_data["rows"]
            receipt = receipt_data.get("native_receipt")
            write = WriteResult(written_rows=rows, blocks_written=len(publication["manifest"]["partitions"]),
                                metadata={"native_receipt": receipt, "queue_settled": self._acquisition is not None and self._acquisition.binding.mode == "sink"})
        self._terminal_result = TerminalResult(kind=self._request.kind, rows=rows,
            scalar=rows if self._request.kind == "count" else publication["manifest"]["partitions"][0].get("scalar"),
            result_ref=image.confirmed_sinks[self._request.kind], write_result=write, complete=True,
            completion="source_exhausted" if self._source_exhausted else "terminal_satisfied")
        self._state = "succeeded"

    def _published_records(self, publication):
        from .dataflow_replay import PublishedRecordReader
        return PublishedRecordReader(publication, store=self._store, bounds=self._bounds)

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

    def _drain_worker_observations(self, endpoint):
        for event in endpoint.take_cleanup_observations():
            self._validate_event(event)
            self._handle_event(event)
        if self._checked is not None:
            self._checked.flush_observations(force=True)

    # -- E008 close -------------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        deadline_ms = self._policy.control_timeout_ms
        cleanup_error = None
        physical_closed = not self._scope_open
        # Finish actor aclose and reader teardown while the driver journal is
        # still reachable. Physical termination remains the final fallback.
        for endpoint in self._endpoints.values():
            try:
                if not endpoint.closed():
                    endpoint.drain(deadline_ms=deadline_ms)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
            finally:
                try:
                    self._drain_worker_observations(endpoint)
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
        if self._scope_open and self._scheduler is not None:
            try:
                self._scheduler.close(deadline_ms=deadline_ms)
                physical_closed = True
            except Exception as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                else:
                    cleanup_error.add_note("Physical cleanup also failed: " + str(exc))
        if self._channels is not None:
            close_channels(self._channels, reason="execution_close", deadline_ms=deadline_ms)
            self._channels = None
        try:
            from .dataflow_staging import recover_staging
            recover_staging(self._request.action, bounds=self._bounds)
        except Exception as exc:
            cleanup_error = cleanup_error or exc
        if self._acquisition is not None:
            self._acquisition.close()
        if self._checkpoint is not None:
            self._checkpoint.close()
        try:
            self._services.journal_service.close(deadline_ms=deadline_ms)
        except DataflowError:
            pass
        if getattr(self, '_stream_lance', None) is not None:
            self._stream_lance.close()
        stopped = getattr(self._services.journal_service, 'physically_stopped', lambda: False)()
        if physical_closed and stopped:
            self._store.close()
            from .dataflow_gc import collect_published_orphans, defer_run_gc
            if getattr(self._action.context, '_native_prompt_journal', None) is not None:
                defer_run_gc(self._action.context, self._bounds)
                self._published_gc = {'removed':0,'bytes':0,'blocked':'Deferred until original native journal closes'}
            else:
                self._published_gc = collect_published_orphans(self._action, bounds=self._bounds)
        else:
            self._action.context._published_gc_guard_failed = True
        self._endpoints.clear()
        self._active_worker_allocations.clear()
        self._current_worker_observations.clear()
        if physical_closed:
            from .dataflow_thread_programs import owner, release
            release(owner(self._action))
        if cleanup_error is not None:
            raise cleanup_error


class _OutputSubscription:
    def __init__(self, execution):
        self.execution = execution
        self.closed = False

    def read(self, *, max_records, deadline_ms):
        return self.read_admitted(max_records=max_records, deadline_ms=deadline_ms)

    def read_admitted(self, *, max_records, deadline_ms, admit=None):
        from .exchange.dataflow_transport import read_peer_many, MAX_PEER_BATCH
        from .exchange.dataflow_bridge import _decode_record_document
        from .dataflow_replay import record_encoded_bytes, read_buffer_reserve
        e = self.execution
        records = []
        byte_window = min(e._bounds.max_batch_bytes,
                          e._bounds.max_resident_bytes - read_buffer_reserve(e._bounds))
        admitted = 0
        payload_bytes = 0
        while not self.closed and e._replay is not None and len(records) < max_records:
            try:
                raw, record_id, sequence, size = e._replay.peek()
            except StopIteration:
                e._replay.close()
                e._replay = None
                break
            if admit is not None:
                admit(size, len(records), payload_bytes)
            required = max(len(raw), size)
            if records and admitted + required > byte_window:
                break
            if required > byte_window:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal replay row exceeds admitted read window')
            records.append(_decode_record_document(raw, record_id=record_id,
                sequence=sequence, max_bytes=e._bounds.max_row_bytes))
            admitted += required
            payload_bytes += size
            e._replay.pop()
        from itertools import islice
        # Keep the ACKed prefix before all newly produced records, including
        # when its next frame belongs to a later bounded read window.
        while not self.closed and e._replay is None and e._terminal_queue and len(records) < max_records:
            batch, size, endpoint = [], 0, None
            for payload in islice(e._terminal_queue, min(MAX_PEER_BATCH, max_records - len(records))):
                peer = payload['peer']
                owner = (peer['host'], peer['port'], peer['token'])
                if endpoint is not None and owner != endpoint:
                    break
                if (records or batch) and admitted + size + peer['bytes'] > byte_window:
                    break
                if peer['bytes'] > byte_window:
                    raise DataflowError('BUDGET_EXCEEDED', 'terminal row exceeds admitted read window')
                endpoint = owner
                batch.append(payload)
                size += peer['bytes']
            if not batch:
                break
            frames = read_peer_many(tuple(item['peer'] for item in batch), bounds=e._bounds)
            try:
                for payload, raw in zip(batch, frames):
                    size = record_encoded_bytes(raw, maximum=e._bounds.max_row_bytes)
                    if admit is not None:
                        admit(size, len(records), payload_bytes)
                    records.append(_decode_record_document(raw, record_id=payload['record_id'],
                        sequence=payload['sequence'], max_bytes=e._bounds.max_row_bytes))
                    e._terminal_queue.popleft()
                    e._ack(payload['delivery_id'])
                    admitted += payload['peer']['bytes']
                    payload_bytes += size
            finally:
                frames.close()
        return tuple(records)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.execution._terminal_subscription_closed = True
        replay, self.execution._replay = self.execution._replay, None
        if replay is not None:
            replay.close()
        while self.execution._terminal_queue:
            payload = self.execution._terminal_queue.popleft()
            self.execution._ack(payload["delivery_id"])
