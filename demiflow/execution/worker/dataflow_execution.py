"""Unified v3 worker executor (lane B, contract C07).

One DataflowWorkerExecutor serves both Local and Ray Core hosts. It owns the
assignment, kernel/reader/channel tables, a bounded local cut/buffer view, a
finite inflight ledger and the services/mailbox ports. Control and business
threads are separated: the control methods (request_cut/accept_receipt/cancel)
only touch lock-protected bounded state and never run user code, while step()
runs kernels on the business thread owned by the host. The executor never holds
a driver HostEndpoint or RunJournal and never commits a snapshot.

Upstream lane symbols are imported lazily at call time against their frozen
signatures; no ImportError fallback or stub is provided.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..contracts.dataflow import (
        Bounds, CutContribution, CutRequest, ExchangeSpec, KernelSpec,
        LoadedProgram, Record, RecoveryImage, RuntimeServices, SourceCursor,
        SourcePoll, WorkerAssignment, WorkerControlMailbox, WorkerEvent,
        WorkerStep,
    )

_STATES = ("constructed", "loaded", "restored", "started", "draining", "cancelled", "closed")
_EVENT_SCHEMA = "demiflow.dataflow_event.v3"
_MAX_LOCAL_EVENTS = 4096


class _KernelServices:
    """Borrow the original ports/data with a kernel's stop check and physical grant.

    Do not rebuild RuntimeServices: its validation copies constants/program
    mappings, whereas this invocation-local view adds only two references.
    """
    __slots__ = ('_original', 'check_stop', 'bounds', 'measured_workspace')

    def __init__(self, original, check_stop, bounds=None):
        self._original, self.check_stop = original, check_stop
        self.bounds = original.bounds if bounds is None else bounds
        self.measured_workspace = bounds is not None

    def __getattr__(self, name):
        return getattr(self._original, name)


def _dataflow_error(code, message, *, retryable=False, context=None):
    from ..contracts.dataflow_errors import DataflowError
    return DataflowError(code, message, retryable=retryable, context=context or {})


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _operation_id(*parts):
    return hashlib.sha256(_canonical(list(parts)).encode("utf-8")).hexdigest()


class DataflowWorkerExecutor:
    """Worker-side execution context for one assignment (C07)."""

    def __init__(self, *, services: "RuntimeServices", mailbox: "WorkerControlMailbox") -> None:
        if services is None or mailbox is None:
            raise _dataflow_error("INVALID_CONTRACT", "services and mailbox are required")
        self._services = services
        self._mailbox = mailbox
        self.assignment: "WorkerAssignment | None" = None
        self._state = "constructed"
        self._kernels: dict = {}
        self._readers: dict = {}
        self._channels = None
        self._input_specs: tuple = ()
        self._output_specs: tuple = ()
        self._kernel_specs: dict = {}
        self._node_specs: dict = {}
        self._programs: dict = {}
        self._graph = None
        self._physical = None
        self._sealed = None
        self._restore_image: "RecoveryImage | None" = None
        self._restored_cursors: dict = {}
        self._restored_states: dict = {}
        self._pending_cuts: deque = deque()
        self._snapshot_contributions: dict = {}
        self._receipts: dict = {}
        self._released_cuts: set = set()
        self._inflight: deque = deque()
        self._effect_refs: list = []
        self._prepared_sinks: dict = {}
        self._cancel_reason: str | None = None
        self._cancel_event = threading.Event()
        self._control_lock = threading.RLock()
        self._business_active = False
        self._admit_open = False
        self._closed = False
        self._cache: Path | None = None
        self._owned_services_cache: Path | None = None
        self._event_sequence = 0
        bounds = services.bounds
        self._max_pending = max(1, int(bounds.max_pending_events))
        self._max_inflight = max(1, int(bounds.max_pending_events))
        self._max_row_bytes = int(bounds.max_row_bytes)
        self._max_batch_bytes = int(bounds.max_batch_bytes)
        self._timeout_ms = int(bounds.timeout_ms)

    # ---- identity / event helpers (business or control thread) ----
    def _identity(self):
        assignment = self.assignment
        lease = assignment.lease
        from ..contracts.dataflow_codec import runtime_scope_id
        return {
            "run_id": assignment.run_id,
            "attempt": assignment.attempt,
            "fence": assignment.fence,
            "allocation_id": lease.allocation_id,
            "boot_id": lease.boot_id,
            "plan_revision": assignment.plan_revision,
            "scope_id": runtime_scope_id(assignment.plan_id, assignment.attempt),
            "controller_epoch": assignment.attempt,
        }

    def _next_operation_id(self, kind):
        self._event_sequence += 1
        assignment = self.assignment
        return _operation_id(assignment.action_id, assignment.lease.allocation_id,
                             assignment.lease.boot_id, kind, self._event_sequence)

    def _event(self, kind, *, payload=None, payload_ref=None, operation_id=None):
        from ..contracts import dataflow as df
        ident = self._identity()
        return df.WorkerEvent(
            schema=_EVENT_SCHEMA, kind=kind,
            operation_id=operation_id or self._next_operation_id(kind),
            run_id=ident["run_id"], attempt=ident["attempt"], fence=ident["fence"],
            allocation_id=ident["allocation_id"], boot_id=ident["boot_id"],
            plan_revision=ident["plan_revision"], scope_id=ident["scope_id"],
            controller_epoch=ident["controller_epoch"],
            payload_ref=payload_ref, payload=payload,
        )

    def _emit(self, event):
        reply = self._mailbox.request(event, deadline_ms=self._timeout_ms)
        if reply.status == "error":
            raise _dataflow_error(reply.error_code or "INTERNAL_PROTOCOL",
                                  "control reply rejected worker event",
                                  context={"operation_id": event.operation_id})
        return reply

    def _participant_ids(self):
        from ..contracts.dataflow_codec import compute_participant_id
        assignment = self.assignment
        ids = []
        for stage in assignment.stages:
            for partition in assignment.lease.partition_ids:
                if partition in stage.partitions:
                    ids.append(compute_participant_id(stage.stage_id, partition))
        return tuple(dict.fromkeys(ids))

    def _check_grant(self):
        self._services.journal.check_grant()
        self._services.check_stop()

    def _check_finite_stop(self):
        self._services.check_stop()
        if self._cancel_event.is_set():
            raise _dataflow_error('CANCELLED', 'business execution cancelled')

    # ---- B002 load ----
    def load(self, assignment: "WorkerAssignment") -> None:
        from ..seal.plan import load_sealed_dataflow
        with self._control_lock:
            if self._state != "constructed":
                raise _dataflow_error("INVALID_CONTRACT", "load requires a fresh executor",
                                      context={"state": self._state})
            bound = self._services.assignment
            if bound is not None and bound.plan_id != assignment.plan_id:
                raise _dataflow_error("FENCED", "assignment plan_id differs from services binding",
                                      context={"plan_id": assignment.plan_id})
            if bound is not None and bound.lease.boot_id != assignment.lease.boot_id:
                raise _dataflow_error("FENCED", "stale boot cannot load assignment",
                                      context={"boot_id": assignment.lease.boot_id})
            self._validate_assignment(assignment)
            self._cache = Path(tempfile.mkdtemp(prefix="demiflow-worker-cache-"))
            try:
                graph, physical, pin = load_sealed_dataflow(
                    assignment.sealed, action_id=assignment.action_id,
                    store=self._services.store, bounds=self._services.bounds)
                sealed = assignment.sealed
                if physical.plan_id != assignment.plan_id:
                    raise _dataflow_error("CODE_MISMATCH", "loaded plan_id differs from assignment")
                if physical.plan_revision != assignment.plan_revision:
                    raise _dataflow_error("FENCED", "loaded plan_revision differs from assignment")
                sealed_stages = {s.stage_id: s for s in physical.stages}
                if any(sealed_stages.get(stage.stage_id) != stage for stage in assignment.stages):
                    raise _dataflow_error("CODE_MISMATCH", "worker stage differs from its sealed physical plan")
                programs = self._services.programs
                owned = {occurrence for stage in assignment.stages for occurrence in stage.occurrences}
                if set(programs) != set(pin["programs"]).intersection(owned):
                    raise _dataflow_error("CODE_MISMATCH", "verified program membership differs")
                self._prepare_factories(assignment, graph, physical)
            except BaseException:
                self._cleanup_cache()
                raise
            self.assignment = assignment
            self._graph = graph
            self._physical = physical
            self._sealed = sealed
            self._programs = programs
            self._input_specs = tuple(assignment.inputs)
            self._output_specs = tuple(assignment.outputs)
            self._state = "loaded"

    def _validate_assignment(self, assignment):
        if assignment.schema not in ("demiflow.dataflow_assignment.v3", "demiflow.dataflow_assignment.v4"):
            raise _dataflow_error("INVALID_CONTRACT", "unsupported assignment schema")
        if not assignment.stages:
            raise _dataflow_error("INVALID_CONTRACT", "assignment has no stages")
        if not assignment.lease.partition_ids:
            raise _dataflow_error("INVALID_CONTRACT", "assignment has no partitions")
        if assignment.plan_revision < 0 or assignment.attempt < 1:
            raise _dataflow_error("INVALID_CONTRACT", "assignment revision/attempt invalid")
        bounds = assignment.bounds
        for name in ("max_resident_bytes", "max_pending_events", "max_row_bytes", "timeout_ms"):
            if int(getattr(bounds, name)) <= 0:
                raise _dataflow_error("BUDGET_EXCEEDED", "assignment bound must be positive",
                                      context={"bound": name})
        resident = sum(int(stage.resident_bytes) for stage in assignment.stages)
        if resident > int(bounds.max_resident_bytes):
            raise _dataflow_error("BUDGET_EXCEEDED", "stage residency exceeds worker budget",
                                  context={"resident_bytes": resident})

    def _prepare_factories(self, assignment, graph, physical):
        from dataclasses import replace
        from ..contracts.dataflow import thaw_record_value

        if bool(graph.jobs) != (assignment.model_bindings is not None):
            raise _dataflow_error("INVALID_CONTRACT", "worker assignment version differs from sealed graph")
        assigned = {occ: stage for stage in assignment.stages for occ in stage.occurrences}
        node_specs = {}
        constants = self._services.constants.get('constants', {})
        for node in graph.nodes:
            if node.occurrence_id in assigned:
                windows = assigned[node.occurrence_id].kernel_memory_bytes or {}
                memory = windows.get(node.occurrence_id)
                if memory is not None:
                    node = replace(node, kernel=node.kernel.with_memory_bytes(memory, sealed_bounds=physical.bounds))
                binding = constants.get(node.occurrence_id, {})
                if 'column_binding' in binding:
                    spec = node.kernel
                    if spec.kind not in ('SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp'):
                        raise _dataflow_error('INCOMPATIBLE_HISTORY', 'sealed column binding on an incompatible node')
                    field = 'names' if spec.kind == 'RenameColumnsOp' else 'columns'
                    config = thaw_record_value(spec.configuration)
                    config[field] = list(binding['column_binding'])
                    node = replace(node, kernel=replace(spec, configuration=config))
                node_specs[node.occurrence_id] = node
        if not node_specs:
            raise _dataflow_error("INVALID_CONTRACT", "no assigned occurrences found in graph")
        self._node_specs = node_specs
        self._kernel_specs = {occ: node.kernel for occ, node in node_specs.items()}
        self._split_by_source = {}
        for split in assignment.source_splits:
            self._split_by_source.setdefault(split.source_id, []).append(split)
        self._snapshot_by_source = {snap.source_id: snap for snap in physical.sources}

    @staticmethod
    def _measured_services(spec, services):
        from dataclasses import replace
        working = spec.profile.requests['memory_bytes'] - spec.configuration.get('task_load_bytes', 0)
        bounded = replace(services.bounds, max_resident_bytes=min(working, services.bounds.max_resident_bytes))
        return _KernelServices(services, services.check_stop, bounded)

    def _cleanup_cache(self):
        if self._cache is not None and self._cache.exists():
            shutil.rmtree(self._cache, ignore_errors=True)
        self._cache = None

    # ---- B003 restore ----
    def restore(self, image: "RecoveryImage | None") -> None:
        from ..exchange.dataflow_channels import restore_channels
        with self._control_lock:
            if self._state != "loaded":
                raise _dataflow_error("INVALID_CONTRACT", "restore requires a loaded executor",
                                      context={"state": self._state})
            if image is None:
                self._state = "restored"
                return
            participants = set(self._participant_ids())
            cursors, states = {}, {}
            channel_refs = {}
            for contribution in image.contributions:
                if contribution.participant_id not in participants:
                    continue
                for cursor in contribution.sources:
                    cursors[cursor.split_id] = cursor
                for occ, ref in contribution.operator_states.items():
                    if ref is None:
                        raise _dataflow_error("INVALID_CONTRACT", "missing operator state ref",
                                              context={"occurrence_id": occ})
                    states[occ] = ref
                for channel_id, ref in contribution.channels.items():
                    if ref is None:
                        raise _dataflow_error("INVALID_CONTRACT", "missing buffer/channel ref",
                                              context={"channel_id": channel_id})
                    from ..contracts.dataflow import ArtifactRef
                    channel_refs[channel_id] = ArtifactRef(**dict(ref))
                for ref in contribution.buffers:
                    if ref is None:
                        raise _dataflow_error("INVALID_CONTRACT", "missing buffer ref cannot restore")
            specs = self._input_specs + self._output_specs
            self._channels = restore_channels(specs, channel_refs, services=self._services)
            self._restored_cursors = cursors
            self._restored_states = states
            self._restore_image = image
            self._state = "restored"

    # ---- B004 start ----
    def start(self) -> None:
        from ..dataflow_models import restore_actor_state
        from ..dataflow_sources import open_source_reader
        from ..dataflow_source_context import source_run_scope, source_run_view
        from ..exchange.dataflow_channels import open_channels
        from ..kernels.dataflow import make_kernel
        with self._control_lock:
            if self._state not in ("loaded", "restored"):
                raise _dataflow_error("FENCED", "business input requires activation",
                                      context={"state": self._state})
            self._check_grant()
            facts = self._services.journal.read_action(self.assignment.action_id)
            # Pure finite scans observe executor cancellation. Callable/native
            # persistence keeps its original service and admitted-effect drain.
            # This view shares the same authenticated ports/grant/assignment.
            self._finite_services = _KernelServices(self._services, self._check_finite_stop)
            self._source_run = source_run_view(self._services, limits=facts['limits'])
            with source_run_scope(self._source_run):
                assignment = self.assignment
                from ..dataflow_shared import shared_actors
                shared = shared_actors(self._kernel_specs, self._services.programs, assignment, graph=self._graph)
                if self._restored_states:
                    for group in set(shared.values()):
                        if not set(group.binding['members']) <= set(self._restored_states):
                            raise _dataflow_error('INCOMPATIBLE_HISTORY', 'Shared actor recovery is missing a member image')
                for occ, spec in self._kernel_specs.items():
                    if self._node_specs[occ].kind in ("source", "terminal"):
                        continue
                    services, finite = self._services, self._finite_services
                    if spec.uses_measured_workspace(self._physical.bounds):
                        services = self._measured_services(spec, services)
                        finite = self._measured_services(spec, finite)
                    for partition in assignment.lease.partition_ids:
                        kernel = make_kernel(spec, services=services, partition_id=partition,
                                             finite_services=finite)
                        if occ in shared:
                            kernel._shared_actor = shared[occ]
                        self._kernels[(occ, partition)] = kernel
                        kernel.start()
                        state_ref = self._restored_states.get(occ)
                        if state_ref is not None:
                            kernel.restore(state_ref)
                            instance = getattr(kernel, "instance", None)
                            if instance is not None:
                                restore_actor_state(instance, ref=state_ref, services=self._services)
                        self._kernels[(occ, partition)] = kernel
                # Only an explicitly versioned sealed stage may bind this finite
                # compute optimization. Old stages never infer a new region.
                from ..contracts.relational_region import interpret_region
                from ..kernels.relational_region import RelationalRegionRuntime
                self._relational_regions = {}
                for stage in assignment.stages:
                    region_plan = interpret_region(self._graph, stage)
                    if region_plan is None:
                        continue
                    partition = assignment.lease.partition_ids[0]
                    members = {occ: self._kernels[(occ, partition)] for occ in region_plan.occurrences}
                    region = RelationalRegionRuntime(region_plan, kernels=members,
                        services=self._finite_services, edges=self._graph.edges)
                    for kernel in members.values():
                        kernel.bind_relational_region(region)
                    self._relational_regions[region_plan.region_id] = region
                if self._restored_states:
                    for group in set(shared.values()):
                        if group.restored != set(group.binding['members']):
                            raise _dataflow_error('INCOMPATIBLE_HISTORY', 'Shared actor state was not fully restored')
                for source_id, splits in self._split_by_source.items():
                    snapshot = self._snapshot_by_source.get(source_id)
                    if snapshot is None:
                        raise _dataflow_error("SOURCE_CHANGED", "no snapshot for assigned source",
                                              context={"source_id": source_id})
                    spec = self._kernel_specs[source_id]
                    services = self._services
                    if spec.uses_measured_workspace(self._physical.bounds):
                        services = self._measured_services(spec, services)
                    for split in splits:
                        cursor = self._restored_cursors.get(split.split_id)
                        with source_run_scope(self._source_run):
                            reader = open_source_reader(snapshot, split, cursor=cursor, services=services)
                        self._readers[split.split_id] = reader
                if self._channels is None:
                    self._channels = open_channels(self._input_specs + self._output_specs,
                                                   services=self._services)
                self._admit_open = True
                self._state = "started"
                from .dataflow_graph_runner import WorkerGraphRunner
                self._runner = WorkerGraphRunner(self)

    def run(self):
        """Begin business work only after the host accepts its K3 assignment."""
        from ..dataflow_source_context import source_run_scope
        with source_run_scope(self._source_run):
            # The business thread and its async/thread calls copy this context.
            self._runner.start()

    def receive(self, command):
        if command.get('kind') == 'renew_lease':
            assignment = self.assignment
            if any(command.get(key) != getattr(assignment, key) for key in ('run_id', 'attempt', 'fence')):
                raise _dataflow_error('FENCED', 'Lease renewal belongs to another run/attempt')
            self._services.journal.renew_lease(command)
            return
        self._runner.receive(command)

    def step(self, *, max_records, deadline_ms):
        # Control may cancel while a business call runs; never hold its lock
        # across user code, and never read input before activation/after cancel.
        with self._control_lock:
            if self._state not in ('started', 'draining') or not hasattr(self, '_runner'):
                raise _dataflow_error('FENCED', 'business step requires activation')
            if self._cancel_event.is_set():
                from ..contracts.dataflow import WorkerStep
                return WorkerStep(processed=0, blocked=False, end_of_input=True, events=())
            runner = self._runner
        from ..dataflow_source_context import source_run_scope
        with source_run_scope(self._source_run):
            return runner.step(max_records=max_records, deadline_ms=deadline_ms)

    # ---- B006 request_cut ----
    def request_cut(self, request):
        if request.plan_revision != self.assignment.plan_revision:
            raise _dataflow_error("FENCED", "cut revision differs from assignment")
        if not set(self._participant_ids()).issubset(request.participants):
            raise _dataflow_error("INVALID_CONTRACT", "cut excludes current participant")
        self._runner.receive({"kind": "cut", "request": request})

    # ---- B007 snapshot ----
    def snapshot(self, request):
        from ..dataflow_source_context import source_run_scope
        with source_run_scope(self._source_run):
            return self._runner.snapshot(request)

    # ---- B008 accept_receipt ----
    def accept_receipt(self, commit_id: str, *, cut_id: str) -> None:
        with self._control_lock:
            if not commit_id or not cut_id:
                raise _dataflow_error("INVALID_CONTRACT", "receipt needs commit_id and cut_id")
            if cut_id in self._released_cuts:
                if self._receipts.get(cut_id) not in (None, commit_id):
                    raise _dataflow_error("INVALID_CONTRACT", "conflicting receipt for cut",
                                          context={"cut_id": cut_id})
                return
            if cut_id not in self._snapshot_contributions:
                raise _dataflow_error("INVALID_CONTRACT", "receipt for an unknown cut",
                                      context={"cut_id": cut_id})
            if cut_id in self._receipts and self._receipts[cut_id] != commit_id:
                raise _dataflow_error("INVALID_CONTRACT", "conflicting receipt for cut",
                                      context={"cut_id": cut_id})
            self._receipts[cut_id] = commit_id
            self._released_cuts.add(cut_id)
            self._snapshot_contributions.pop(cut_id, None)

    # ---- B009 cancel ----
    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        if deadline_ms <= 0:
            raise _dataflow_error("INVALID_CONTRACT", "cancel deadline must be positive")
        with self._control_lock:
            if self._closed:
                return
            # Cancel is followed by a generic K3 Drain during cleanup. Keep
            # the original reason, just as AsyncInstance does: neither revoke
            # admitted effect drain nor upgrade an earlier ordinary abort.
            if self._cancel_reason is None:
                self._cancel_reason = reason
            for kernel in self._kernels.values():
                stop = getattr(kernel, 'request_stop', None)
                if stop is not None:
                    stop(self._cancel_reason)
            self._admit_open = False
            self._cancel_event.set()
            if hasattr(self, "_runner"):
                self._runner.wake()
            if self._state in ("started", "restored", "loaded"):
                self._state = "draining"

    # ---- B010 close ----
    def close(self) -> None:
        if hasattr(self, "_runner"):
            self._runner.close()
        from ..exchange.dataflow_channels import close_channels
        with self._control_lock:
            if self._closed:
                return
            self._closed = True
            self._admit_open = False
            self._cancel_event.set()
            self._state = "closed"
        errors = []
        original = None
        if self._channels is not None:
            try:
                close_channels(self._channels, reason=self._cancel_reason or "closed",
                               deadline_ms=self._timeout_ms)
            except BaseException as exc:
                errors.append(exc)
        self._final_progress = []
        final_bytes = 2
        for key, kernel in list(self._kernels.items()):
            try:
                from ..dataflow_source_context import source_run_scope
                with source_run_scope(self._source_run):
                    kernel.close()
                    aclose = getattr(kernel, "aclose", None)
                    if aclose is not None:
                        aclose()
            except BaseException as exc:
                errors.append(exc)
        # Shared owner close occurs at its last borrower; collect all final
        # observations only after every kernel has released its lifecycle.
        for key, kernel in list(self._kernels.items()):
            # The normal mailbox is closed during teardown. Carry only
            # bounded, actual callback/handoff/hook outcomes in the original
            # Drain response. Plain functions have no actor lifecycle, but
            # their final failed invocation and prior misses are still facts.
            try:
                runner = getattr(self, '_runner', None)
                if runner is not None:
                    payload = runner._metrics_payload(key[0])
                    if any(payload.get(field) is not None for field in (
                            'actor_lifecycle', 'callback_observations', 'output_handoff',
                            'prompt_usage', 'model_observations')):
                        from ..contracts.dataflow_codec import encode_wire
                        import json
                        raw = encode_wire(self._event('progress', payload=payload),
                            type_name='WorkerEvent', bounds=self.assignment.bounds)
                        # Wire envelopes overestimate each payload's bytes;
                        # count once instead of reserializing the growing list.
                        final_bytes += len(raw) + 1
                        if final_bytes > self.assignment.bounds.max_control_bytes:
                            raise _dataflow_error('BUDGET_EXCEEDED', 'final worker observations exceed control budget')
                        self._final_progress.append(json.loads(raw)['payload'])
            except BaseException as exc:
                errors.append(exc)  # Continue physical cleanup; preserve the first error.
        for split_id, reader in list(self._readers.items()):
            try:
                from ..dataflow_source_context import source_run_scope, source_run_view
                view = getattr(self, '_source_run', None) or source_run_view(self._services)
                with source_run_scope(view):
                    reader.close()
            except BaseException as exc:
                errors.append(exc)
        self._kernels.clear()
        self._readers.clear()
        self._channels = None
        self._cleanup_cache()
        close_store = getattr(self._services.store, 'close', None)
        if close_store is not None:
            close_store()
        if self._owned_services_cache is not None:
            shutil.rmtree(self._owned_services_cache, ignore_errors=True)
            self._owned_services_cache = None
        try:
            self._mailbox.close()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise BaseExceptionGroup("worker cleanup failed", errors)
