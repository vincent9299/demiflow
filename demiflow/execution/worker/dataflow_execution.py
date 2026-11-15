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
        self._active_cut: "CutRequest | None" = None
        self._aligned_inputs: set = set()
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
        self._credit_leases: dict = {}
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
        return {
            "run_id": assignment.run_id,
            "attempt": assignment.attempt,
            "fence": assignment.fence,
            "allocation_id": lease.allocation_id,
            "boot_id": lease.boot_id,
            "plan_revision": assignment.plan_revision,
            "scope_id": assignment.action_id,
            "controller_epoch": assignment.attempt,
        }

    def _next_operation_id(self, kind):
        self._event_sequence += 1
        assignment = self.assignment
        return _operation_id(assignment.action_id, kind, self._event_sequence)

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

    # ---- B002 load ----
    def load(self, assignment: "WorkerAssignment") -> None:
        from ..dataflow_seal import load_sealed_dataflow
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
                programs = self._services.programs
                if set(programs) != set(pin["programs"]):
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
        if assignment.schema != "demiflow.dataflow_assignment.v3":
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
        assigned = set()
        for stage in assignment.stages:
            assigned.update(stage.occurrences)
        node_specs = {}
        for node in graph.nodes:
            if node.occurrence_id in assigned:
                node_specs[node.occurrence_id] = node
        if not node_specs:
            raise _dataflow_error("INVALID_CONTRACT", "no assigned occurrences found in graph")
        self._node_specs = node_specs
        self._kernel_specs = {occ: node.kernel for occ, node in node_specs.items()}
        self._split_by_source = {}
        for split in assignment.source_splits:
            self._split_by_source.setdefault(split.source_id, []).append(split)
        self._snapshot_by_source = {snap.source_id: snap for snap in physical.sources}

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
                if contribution.boot_id != self.assignment.lease.boot_id:
                    raise _dataflow_error("FENCED", "contribution boot differs from current boot")
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
                    channel_refs[channel_id] = ref
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
        from ..exchange.dataflow_channels import open_channels
        from ..kernels.dataflow import make_kernel
        with self._control_lock:
            if self._state not in ("loaded", "restored"):
                raise _dataflow_error("FENCED", "business input requires activation",
                                      context={"state": self._state})
            self._check_grant()
            assignment = self.assignment
            for occ, spec in self._kernel_specs.items():
                for partition in assignment.lease.partition_ids:
                    kernel = make_kernel(spec, services=self._services, partition_id=partition)
                    kernel.start()
                    state_ref = self._restored_states.get(occ)
                    if state_ref is not None:
                        kernel.restore(state_ref)
                        instance = getattr(kernel, "instance", None)
                        if instance is not None:
                            restore_actor_state(instance, ref=state_ref, services=self._services)
                    self._kernels[(occ, partition)] = kernel
            for source_id, splits in self._split_by_source.items():
                snapshot = self._snapshot_by_source.get(source_id)
                if snapshot is None:
                    raise _dataflow_error("SOURCE_CHANGED", "no snapshot for assigned source",
                                          context={"source_id": source_id})
                for split in splits:
                    cursor = self._restored_cursors.get(split.split_id)
                    reader = open_source_reader(snapshot, split, cursor=cursor,
                                                services=self._services)
                    self._readers[split.split_id] = reader
            if self._channels is None:
                self._channels = open_channels(self._input_specs + self._output_specs,
                                               services=self._services)
            self._admit_open = True
            self._state = "started"

    # ---- B005 step ----
    def step(self, *, max_records: int, deadline_ms: int) -> "WorkerStep":
        from ..contracts import dataflow as df
        from ..exchange.dataflow_channels import reserve_credit, route_record
        from ..kernels.dataflow import finish_kernel
        if max_records <= 0 or deadline_ms <= 0:
            raise _dataflow_error("INVALID_CONTRACT", "step budgets must be positive")
        with self._control_lock:
            if self._closed or self._state == "closed":
                raise _dataflow_error("CANCELLED", "executor is closed")
            if self._cancel_event.is_set():
                return df.WorkerStep(processed=0, blocked=False, end_of_input=True,
                                     events=(self._event("closed", payload={"reason": self._cancel_reason}),))
            if self._state != "started":
                raise _dataflow_error("FENCED", "business input requires activation",
                                      context={"state": self._state})
            self._business_active = True
        events = []
        processed = 0
        blocked = False
        end_of_input = False
        deadline = self._services.clock_ms() + deadline_ms
        try:
            self._check_grant()
            budget = max_records
            for split_id, reader in self._readers.items():
                if budget <= 0 or self._services.clock_ms() >= deadline:
                    break
                poll = reader.next_record()
                if poll.status == "record" and poll.record is not None:
                    self._route_source_record(poll.record, split_id)
                    processed += 1
                    budget -= 1
                elif poll.status == "eoi":
                    end_of_input = True
                    events.append(self._event("eoi", payload={"split_id": split_id}))
                elif poll.status == "pending":
                    blocked = True
            if budget > 0 and self._services.clock_ms() < deadline:
                consumed = self._consume_input(budget)
                processed += consumed
                budget -= consumed
            cut_events = self._process_pending_cut()
            events.extend(cut_events)
            if self._all_sources_eoi() and not self._pending_cuts:
                end_of_input = True
        finally:
            with self._control_lock:
                self._business_active = False
        return df.WorkerStep(processed=processed, blocked=blocked,
                             end_of_input=end_of_input, events=tuple(events))

    def _route_source_record(self, record, split_id):
        from ..exchange.dataflow_channels import reserve_credit, route_record
        for spec in self._output_specs:
            lease = reserve_credit(spec.channel_id, self.assignment.lease.boot_id,
                                   rows=1, size_bytes=int(record.encoded_bytes),
                                   operation_id=self._next_operation_id("credit"),
                                   state=self._channels)
            self._credit_leases[lease.operation_id] = lease
            routed = route_record(record, spec=spec, state=self._channels)
            self._publish_routed(routed, spec)

    def _consume_input(self, budget):
        """Consume inbound input via the frozen v3 module-level channel API.

        Input records arrive on the control mailbox (WorkerControlMailbox.drain);
        ChannelSet itself only exposes close(). Credit is reserved (X005) before
        any paid decode (X003 resolve_record) or kernel push, so no credit means
        no early paid call (B005). Input cut barriers align the pending cut.
        """
        from ..contracts import dataflow as df
        from ..exchange.dataflow_channels import reserve_credit, resolve_record
        input_channels = {spec.channel_id: spec for spec in self._input_specs}
        if not input_channels:
            return 0
        consumed = 0
        for event in self._mailbox.drain(max_events=self._max_pending):
            payload = event.payload or {}
            channel_id = payload.get("channel_id")
            if channel_id not in input_channels:
                continue
            if event.kind == "cut":
                with self._control_lock:
                    self._aligned_inputs.add(channel_id)
                continue
            if consumed >= budget:
                break
            encoded_payload = payload.get("encoded")
            if encoded_payload is None:
                continue
            lease = reserve_credit(channel_id, self.assignment.lease.boot_id,
                                   rows=1, size_bytes=int(encoded_payload.get("encoded_bytes", 0)),
                                   operation_id=self._next_operation_id("credit"),
                                   state=self._channels)
            self._credit_leases[lease.operation_id] = lease
            encoded = df.EncodedRecord(**encoded_payload)
            record = resolve_record(encoded, store=self._services.store,
                                    bounds=self._services.bounds)
            for (occ, partition), kernel in self._kernels.items():
                for port in self._kernel_specs[occ].inputs:
                    for out in kernel.push(port.name, record):
                        self._route_source_record(out, partition)
            consumed += 1
        return consumed

    def _publish_routed(self, routed, spec):
        from ..exchange.dataflow_channels import encode_record, publish_segment
        encoded = tuple(encode_record(item.record, bounds=self._services.bounds) for item in routed)
        if encoded:
            publish_segment(encoded, spec=spec, store=self._services.store,
                            operation_id=self._next_operation_id("publish"))

    def _all_sources_eoi(self):
        if not self._readers:
            return False
        return all(getattr(reader, "_eoi", False) for reader in self._readers.values())

    def _process_pending_cut(self):
        events = []
        with self._control_lock:
            request = self._active_cut
        if request is None:
            return events
        if self._inputs_aligned(request):
            contribution = self.snapshot(request)
            events.append(self._event("cut", payload={"cut_id": request.cut_id,
                                                      "participant_id": contribution.participant_id}))
            self._emit(events[-1])
            with self._control_lock:
                self._active_cut = None
                self._aligned_inputs.clear()
        return events

    def _inputs_aligned(self, request):
        if not self._input_specs:
            return True
        return all(spec.channel_id in self._aligned_inputs for spec in self._input_specs)

    # ---- B006 request_cut ----
    def request_cut(self, request: "CutRequest") -> None:
        from ..exchange.dataflow_bridge import cut_to_barrier
        with self._control_lock:
            if self._state not in ("started", "restored", "loaded"):
                raise _dataflow_error("FENCED", "cut requires an active executor",
                                      context={"state": self._state})
            if not request.cut_id:
                raise _dataflow_error("INVALID_CONTRACT", "cut_id must be driver-produced")
            if request.final:
                raise _dataflow_error("INVALID_CONTRACT", "worker cannot originate a final cut")
            participants = set(self._participant_ids())
            if not participants.issubset(set(request.participants)):
                raise _dataflow_error("INVALID_CONTRACT", "cut participants exclude this worker",
                                      context={"cut_id": request.cut_id})
            if request.plan_revision != self.assignment.plan_revision:
                raise _dataflow_error("FENCED", "cut plan_revision differs from assignment")
            if len(self._pending_cuts) >= self._max_pending:
                raise _dataflow_error("BUDGET_EXCEEDED", "pending cut ledger is full")
            self._pending_cuts.append(request)
            self._active_cut = request
            self._aligned_inputs.clear()
            sequence = request.sequence
            for spec in self._output_specs:
                barrier = cut_to_barrier(request, channel_id=spec.channel_id, sequence=sequence)
                self._emit(self._event("cut", payload=barrier,
                                       operation_id=_operation_id(request.cut_id, spec.channel_id)))
            if not self._input_specs:
                self._aligned_inputs.update(spec.channel_id for spec in self._output_specs)

    # ---- B007 snapshot ----
    def snapshot(self, request: "CutRequest") -> "CutContribution":
        from ..contracts import dataflow as df
        from ..dataflow_models import snapshot_actor_state
        from ..dataflow_sinks import prepare_sink_partition
        from ..exchange.dataflow_channels import snapshot_channels
        with self._control_lock:
            if self._active_cut is None or self._active_cut.cut_id != request.cut_id:
                raise _dataflow_error("INVALID_CONTRACT", "snapshot requires an aligned active cut",
                                      context={"cut_id": request.cut_id})
            if self._business_active:
                raise _dataflow_error("INVALID_CONTRACT", "snapshot must run at a business barrier")
        if not self._inputs_aligned(request):
            raise _dataflow_error("INVALID_CONTRACT", "inputs are not aligned for cut")
        sources = tuple(reader.snapshot() for reader in self._readers.values())
        operator_states = {}
        for (occ, partition), kernel in self._kernels.items():
            ref = kernel.snapshot()
            instance = getattr(kernel, "instance", None)
            if instance is not None and self._kernel_specs[occ].state != "none":
                ref = snapshot_actor_state(instance, services=self._services) or ref
            if ref is not None:
                operator_states[occ + ":" + partition] = ref
        channels = snapshot_channels(self._channels, cut=request, store=self._services.store)
        buffers = tuple(ref for ref in channels.values() if ref is not None)
        if any(ref is None for ref in channels.values()):
            raise _dataflow_error("INVALID_CONTRACT", "unaligned buffer cannot be sealed")
        sinks = {}
        for (occ, partition), kernel in self._kernels.items():
            spec = self._kernel_specs[occ]
            if spec.kind == "terminal":
                prepared = prepare_sink_partition(iter(()), spec=spec, services=self._services)
                sinks[occ + ":" + partition] = prepared
                self._prepared_sinks[occ + ":" + partition] = prepared
        participant = self._participant_ids()[0]
        contribution = df.CutContribution(
            cut_id=request.cut_id, participant_id=participant,
            boot_id=self.assignment.lease.boot_id, sources=sources,
            operator_states=operator_states, channels=channels, buffers=buffers,
            effects=tuple(self._effect_refs), sinks=sinks, final=False)
        with self._control_lock:
            self._snapshot_contributions[request.cut_id] = contribution
        return contribution

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
            for operation_id, lease in list(self._credit_leases.items()):
                self._credit_leases.pop(operation_id, None)
            self._snapshot_contributions.pop(cut_id, None)

    # ---- B009 cancel ----
    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        if deadline_ms <= 0:
            raise _dataflow_error("INVALID_CONTRACT", "cancel deadline must be positive")
        with self._control_lock:
            if self._closed:
                return
            self._cancel_reason = reason
            self._admit_open = False
            self._cancel_event.set()
            if self._state in ("started", "restored", "loaded"):
                self._state = "draining"

    # ---- B010 close ----
    def close(self) -> None:
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
        for key, kernel in list(self._kernels.items()):
            try:
                kernel.close()
                aclose = getattr(kernel, "aclose", None)
                if aclose is not None:
                    aclose()
            except BaseException as exc:
                errors.append(exc)
        for split_id, reader in list(self._readers.items()):
            try:
                reader.close()
            except BaseException as exc:
                errors.append(exc)
        self._kernels.clear()
        self._readers.clear()
        self._channels = None
        self._cleanup_cache()
        if self._owned_services_cache is not None:
            shutil.rmtree(self._owned_services_cache, ignore_errors=True)
            self._owned_services_cache = None
        try:
            self._mailbox.close()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            original = errors[0]
            raise _dataflow_error("INTERNAL_PROTOCOL", "worker cleanup failed",
                                  context={"failures": len(errors)}) from original