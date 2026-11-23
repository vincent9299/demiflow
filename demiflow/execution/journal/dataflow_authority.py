"""Driver-owned journal authority and bounded command service.

The driver service only validates identity/allowlist/byte budgets and calls the
original journal/effect implementations through fixed handlers; it never exposes
a generic SQL/method proxy. Durable effect facts and their budgets share one fenced RunJournal transaction.
The separate worker client never opens this database; physical artifact handling
is delegated to the original storage port.
"""
from __future__ import annotations

from demiflow.execution.contracts.dataflow_codec import image_request_budget
from demiflow.execution.recovery_contract import UnresolvedCommit

from demiflow.execution.artifacts import resolve_local_artifact
from demiflow.execution.storage.private_journals import inspect_private_journal as _existing_size, inspect_private_lease_history as _lease_history

from demiflow.execution.contracts.dataflow_kinds import NATIVE_BATCH_SCHEMA as SCHEMA, NATIVE_BATCH_KINDS as KINDS

import json
from demiflow.execution.storage.staging import apply_artifact_command
from demiflow.execution.contracts.dataflow_codec import decode_artifact_reference as _ref_from_json, encode_artifact_reference as _ref_to_json, validate_control_payload as _payload_bytes, decode_effect_command as _effect_command, decode_artifact_command as _artifact_command, decode_queue_command as _queue_command, decode_native_command as _native_command
from collections import OrderedDict
import queue
import socket
import threading
import time

from typing import TYPE_CHECKING, Mapping

from demiflow.execution.contracts.dataflow import ArtifactCommand, ArtifactReply, CompletedControl, ControlReply, EffectCommand, EffectReply, NativeJournalCommand, NativeJournalReply, QueueCommand, QueueReply, WorkerEvent
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError, document, identifier, identity

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import ArtifactRef, ArtifactStore, Bounds, WorkerLease
    from demiflow.execution.run_context import RunAction

_EFFECT_KINDS = frozenset({"lookup", "reserve", "mark_sent", "confirm", "unknown", "release_unsent"})

_QUEUE_OPS = frozenset({"poll", "acquire", "finalize", "status", "search"})
_NATIVE_OPS = frozenset({"lookup", "reserve", "reserve_http_retry", "response", "failed",
                         "references", "media_references", "bind_membership", "stage_image", "offline_lookup", "stream_lance", "batch_membership"})
_COMMAND_KINDS = frozenset({"effect", "reserve", "artifact", "queue", "native_request",
                            "read_action", "read_configuration", "restrict_recovery", "check_grant"})

_MAX_SERVICE_WORKERS = 4


def _now_ms() -> int:
    return int(time.time() * 1000)


def _lease_matches(action: "RunAction", event: WorkerEvent) -> None:
    context = action.context
    if event.run_id != context.ref.run_id:
        raise DataflowError("FENCED", "event belongs to another run")
    if event.attempt != context.attempt:
        raise DataflowError("FENCED", "event belongs to another attempt")
    if event.fence != context.fence:
        raise DataflowError("FENCED", "event fence is not the current owner")
    identifier(event.allocation_id)
    identifier(event.boot_id)
    if type(event.controller_epoch) is not int or event.controller_epoch < 0:
        raise DataflowError("INVALID_CONTRACT", "controller_epoch must be nonnegative")


class DriverJournalService:
    """Bounded driver-side journal authority (H001-H004, H036, H056, H066-H069, H075)."""

    def __init__(self, action: "RunAction", *, store: "ArtifactStore", bounds: "Bounds"):
        self._action = action
        self._store = store
        self._bounds = bounds
        self._journal = action.context.journal
        self._fence = action.context.fence
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, bounds.max_pending_events))
        self._inflight: dict[str, WorkerEvent] = {}
        self._completed: list[CompletedControl] = []
        self._pending_payloads: dict[str, WorkerEvent] = {}
        self._lock = threading.Lock()
        self._closed = False
        self._native = None
        self._recovery_policy = None
        self._stream_lance = None
        self._leases = {}
        self._worker_occurrences = {}
        self._native_action_id = None
        # Transport receipts only: the native journal remains the sole effect
        # authority. Keep at most one control-window of small reserve replies;
        # eviction/restart falls back to its conservative active/unknown checks.
        self._native_reserve_replies = OrderedDict()
        self._native_reserve_owners = {}
        self._native_reserve_lock = threading.Lock()
        self._model_admission_stop = None
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        # Two fixed control descriptors, owned by this service (not artifact
        # handles). A single readiness byte announces the original reply queue;
        # it never carries or acknowledges a result.
        self._reply_reader = self._reply_writer = None
        try:
            self._reply_reader, self._reply_writer = socket.socketpair()
            self._reply_reader.setblocking(False)
            self._reply_writer.setblocking(False)
            for index in range(min(_MAX_SERVICE_WORKERS, max(1, bounds.max_pending_events))):
                worker = threading.Thread(target=self._serve, name="dataflow-journal-%d" % index, daemon=True)
                worker.start()
                self._workers.append(worker)
        except BaseException:
            with self._lock:
                self._closed = True
                self._close_reply_waitable_locked()
            self._stop.set()
            deadline = time.monotonic() + min(1, bounds.timeout_ms / 1000)
            for worker in self._workers:
                worker.join(timeout=max(0, deadline - time.monotonic()))
            raise

    def reply_waitable(self):
        """Borrow a readiness descriptor; only poll_replies may drain it."""
        with self._lock:
            return None if self._closed else self._reply_reader

    def _notify_reply_locked(self):
        if self._reply_writer is not None:
            try:
                self._reply_writer.send(b'\0')
            except BlockingIOError:
                pass  # An already readable notifier is sufficient.

    def _drain_reply_waitable_locked(self):
        if self._reply_reader is not None:
            try:
                self._reply_reader.recv(1)
            except BlockingIOError:
                pass

    def _close_reply_waitable_locked(self):
        reader, writer = self._reply_reader, self._reply_writer
        self._reply_reader = self._reply_writer = None
        try:
            if reader is not None:
                reader.close()
        finally:
            if writer is not None:
                writer.close()

    def bind_bounds(self, bounds: "Bounds") -> None:
        """Bind the effective control window before admitting workers."""
        self._bounds = bounds

    def bind_image_declarations(self, declarations):
        """Bind the sealed node image options before any worker is admitted."""
        self._image_declarations = declarations

    def bind_stream_writer(self, writer) -> None:
        if writer.action is not self._action:
            raise DataflowError('FENCED', 'Stream writer belongs to another action')
        if self._stream_lance is not None and self._stream_lance is not writer:
            raise DataflowError('FENCED', 'Action already has a stream writer')
        self._stream_lance = writer

    def check_model_recovery(self) -> None:
        if self._action.replaying and self._journal.source_binding(
                self._action.action_id + '-native-model-journal') is not None:
            self._native_journal().check_recovery()

    def bind_recovery_policy(self, policy):
        """Bind the authenticated physical policy before any worker can call."""
        from demiflow.execution.contracts.dataflow import RuntimePolicy
        if not isinstance(policy, RuntimePolicy):
            raise DataflowError('INVALID_CONTRACT', 'recovery admission requires the sealed runtime policy')
        with self._lock:
            if self._recovery_policy is not None and self._recovery_policy != policy:
                raise DataflowError('FENCED', 'sealed recovery policy cannot be replaced')
            if self._leases and self._recovery_policy is None:
                raise DataflowError('FENCED', 'recovery policy must precede worker registration')
            self._recovery_policy = policy

    def register_worker(self, lease, occurrences):
        if len(self._leases) >= self._bounds.max_partitions and lease.allocation_id not in self._leases:
            raise DataflowError('BUDGET_EXCEEDED', 'Worker authority table exceeds bound')
        self._leases[lease.allocation_id] = lease
        self._worker_occurrences[(lease.allocation_id, lease.boot_id)] = frozenset(occurrences)
        if self._native is not None and hasattr(self._native, 'register_worker'):
            self._native.register_worker(lease, occurrences)

    def retire_worker(self, lease):
        """Forget only active authority after original-provider confirmed exit."""
        self._check_grant()
        with self._journal._connection() as db:
            row = db.execute("SELECT cleanup FROM resource_allocations WHERE allocation_id=?",
                             (lease.allocation_id,)).fetchone()
        if row is None or row[0] != 'CONFIRMED':
            raise DataflowError('INVALID_CONTRACT', 'Worker cleanup is not confirmed')
        self._forget_worker(lease)

    def discard_prepared_worker(self, lease):
        """Drop a staged grant only before submission or after confirmed exit."""
        self._check_grant()
        with self._journal._connection() as db:
            row = db.execute("SELECT cleanup FROM resource_allocations WHERE allocation_id=?",
                                  (lease.allocation_id,)).fetchone()
        if row is not None and row[0] != 'CONFIRMED':
            raise DataflowError('FENCED', 'Submitted workers require confirmed retirement')
        self._forget_worker(lease)

    def _forget_worker(self, lease):
        owner = (lease.allocation_id, lease.boot_id)
        with self._native_reserve_lock, self._lock:
            old = self._leases.get(lease.allocation_id)
            if old is None:
                return
            if old.boot_id != lease.boot_id:
                raise DataflowError('FENCED', 'Cannot retire another worker boot')
            if any((event.allocation_id, event.boot_id) == owner
                   for event in self._pending_payloads.values()):
                raise DataflowError('INVALID_CONTRACT', 'Worker has unfinished journal RPCs')
            if self._native is not None and hasattr(self._native, 'retire_worker'):
                self._native.retire_worker(lease)
            del self._leases[lease.allocation_id]
            self._worker_occurrences.pop(owner, None)
            for operation, bound in tuple(self._native_reserve_owners.items()):
                if bound == owner:
                    self._native_reserve_owners.pop(operation, None)
                    self._native_reserve_replies.pop(operation, None)

    def renew_worker(self, lease):
        self._check_grant()
        old = self._leases.get(lease.allocation_id)
        from dataclasses import replace
        if (old is None or old.expires_at_ms <= _now_ms()
                or lease.expires_at_ms <= old.expires_at_ms
                or replace(lease, expires_at_ms=old.expires_at_ms) != old):
            raise DataflowError('FENCED', 'Cannot renew expired or different worker authority')
        self._leases[lease.allocation_id] = lease

    # -- admission / completion (H066-H068) ---------------------------------
    def submit(self, event: WorkerEvent) -> bool:
        if self._closed:
            return False
        _lease_matches(self._action, event)
        _payload_bytes(event, self._bounds)
        if event.kind not in _COMMAND_KINDS:
            raise DataflowError("INVALID_CONTRACT", "event kind is not a journal command")
        with self._lock:
            if self._closed:
                return False
            prior = self._pending_payloads.get(event.operation_id)
            if prior is not None:
                if prior != event:
                    raise DataflowError("INVALID_CONTRACT", "Operation id reused for a different control request")
                return True
            # An unread reply still consumes a slot. Never silently evict it.
            if len(self._pending_payloads) >= self._bounds.max_pending_events:
                return False
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                return False
            self._inflight[event.operation_id] = event
            self._pending_payloads[event.operation_id] = event
        return True

    def poll_replies(self, *, max_replies: int) -> tuple[CompletedControl, ...]:
        return self._take_replies(max_replies=max_replies, worker=None)

    def poll_worker_replies(self, *, max_replies: int, allocation_id: str,
                            boot_id: str) -> tuple[CompletedControl, ...]:
        """Deliver this boot's original completed RPCs during K3 cleanup."""
        identifier(allocation_id)
        identifier(boot_id)
        return self._take_replies(max_replies=max_replies, worker=(allocation_id, boot_id))

    def _take_replies(self, *, max_replies, worker):
        if type(max_replies) is not int or max_replies <= 0:
            raise DataflowError("INVALID_CONTRACT", "max_replies must be a positive int")
        with self._lock:
            batch, retained = [], []
            for completed in self._completed:
                if (len(batch) < max_replies and (worker is None or
                        (completed.allocation_id, completed.boot_id) == worker)):
                    batch.append(completed)
                else:
                    retained.append(completed)
            self._completed = retained
            for completed in batch:
                self._pending_payloads.pop(completed.reply.operation_id, None)
            # Other workers retain both reply order and the readiness byte.
            # This is the original queue and accounting, not a retry path.
            if not self._completed:
                self._drain_reply_waitable_locked()
        return tuple(batch)

    def close(self, *, deadline_ms: int) -> None:
        if type(deadline_ms) is not int or deadline_ms < 0:
            raise DataflowError("INVALID_CONTRACT", "deadline_ms must be nonnegative")
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._close_reply_waitable_locked()
        self._stop.set()
        deadline = time.monotonic() + deadline_ms / 1000.0
        for worker in self._workers:
            remaining = max(0.0, deadline - time.monotonic())
            worker.join(timeout=remaining)
        with self._lock:
            for operation_id, event in self._inflight.items():
                self._completed.append(CompletedControl(
                    allocation_id=event.allocation_id, boot_id=event.boot_id,
                    reply=ControlReply(operation_id=operation_id, status="error", payload=None,
                                       payload_ref=None, error_code="UNKNOWN_EFFECT")))
            self._inflight.clear()
        if self._native_reserve_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            try:
                self._native_reserve_replies.clear()
                self._native_reserve_owners.clear()
            finally:
                self._native_reserve_lock.release()
        if self._native is not None:
            close = getattr(self._native, "close", None)
            if callable(close):
                close()
        from demiflow.execution.journal.dataflow_embedding_journal import close_private_embedding_journals
        close_private_embedding_journals(self, deadline=deadline)

    def physically_stopped(self):
        """A stop request or empty inflight dictionary is not thread exit proof."""
        native = getattr(getattr(self, '_native', None), 'native_journal', None)
        thread = getattr(native, '_batch_thread', None)
        return (self._closed and all(not worker.is_alive() for worker in self._workers)
                and not (thread is not None and thread.is_alive())
                and not (getattr(self, '_private_close_thread', None) is not None
                         and self._private_close_thread.is_alive())
                and getattr(self, '_private_close_error', None) is None)

    def bind_queue(self, service: object) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "service is closed")
        action_id = getattr(service, "action_id", None)
        if action_id != self._action.action_id:
            raise DataflowError("INVALID_CONTRACT", "native service belongs to another action")
        with self._lock:
            if self._native is not None:
                if self._native is service:
                    return
                raise DataflowError("INVALID_CONTRACT", "native service already bound")
            self._native = service
            self._native_action_id = action_id

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                event = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            reply = self.dispatch_reply(event)
            completed = CompletedControl(allocation_id=event.allocation_id, boot_id=event.boot_id, reply=reply)
            with self._lock:
                self._inflight.pop(event.operation_id, None)
                if not self._closed:
                    was_empty = not self._completed
                    self._completed.append(completed)
                    if was_empty:
                        self._notify_reply_locked()
            self._queue.task_done()

    def dispatch_reply(self, event: WorkerEvent) -> ControlReply:
        """Same data-only RPC outcome for normal pumping and K3 cleanup.

        Rejected fresh admission must release its original waiting worker too;
        propagating ServiceStopped out of the control loop strands that RPC
        and can prevent other admitted responses from being drained.
        """
        try:
            return self.dispatch(event)
        except Exception as exc:
            from demiflow.execution.contracts.dataflow_failures import failure_payload
            return ControlReply(operation_id=event.operation_id, status="error", payload=failure_payload(exc),
                                payload_ref=None, error_code=getattr(exc, 'code', 'UNKNOWN_EFFECT'))

    # -- validation + fixed dispatch (H002) ---------------------------------
    def dispatch(self, event: WorkerEvent) -> ControlReply:
        if self._closed:
            raise DataflowError('FENCED', 'Journal service is closed')
        _lease_matches(self._action, event)
        _payload_bytes(event, self._bounds)
        if event.kind not in _COMMAND_KINDS:
            raise DataflowError("INVALID_CONTRACT", "event kind is not a journal command")
        from demiflow.execution.contracts.dataflow import thaw_record_value
        payload = thaw_record_value(event.payload) if event.payload is not None else {}
        try:
            lease = self._leases.get(event.allocation_id)
            if (lease is None or lease.boot_id != event.boot_id or lease.expires_at_ms <= _now_ms()):
                raise DataflowError('FENCED', 'Command has no live worker authority')
            self._check_grant()
            if event.kind == "effect":
                reply = self.effect(_effect_command(payload), lease=None)
                value = {"operation_id": reply.operation_id, "status": reply.status,
                         "result_ref": _ref_to_json(reply.result_ref), "receipt_ref": _ref_to_json(reply.receipt_ref)}
            elif event.kind == "reserve":
                self.reserve(event.operation_id, payload.get("amounts", {}), lease=None)
                value = {"operation_id": event.operation_id}
            elif event.kind == "artifact":
                reply = self.artifact(_artifact_command(payload), lease=None)
                value = {"operation_id": reply.operation_id, "present": reply.present, "charged_bytes": reply.charged_bytes}
            elif event.kind == "queue":
                lease = self._leases.get(event.allocation_id)
                if lease is None or lease.boot_id != event.boot_id:
                    raise DataflowError('FENCED', 'Queue command has no current worker authority')
                reply = self.queue(_queue_command(payload), lease=lease)
                value = {"operation_id": reply.operation_id, "status": reply.status,
                         "delivery_ref": _ref_to_json(reply.delivery_ref), "row_ref": _ref_to_json(reply.row_ref),
                         "native_fact_ref": _ref_to_json(reply.native_fact_ref), "retry_after_ms": reply.retry_after_ms}
            elif event.kind == "native_request":
                command = _native_command(payload)
                if command.operation_id != event.operation_id:
                    raise DataflowError('INVALID_CONTRACT', 'Native command operation_id differs from its event')
                if command.node_id not in self._worker_occurrences.get((lease.allocation_id, lease.boot_id), ()):
                    raise DataflowError('FENCED', 'Worker does not own the native request occurrence')
                reply = (self._native_reserve_reply(event, command, lease=lease)
                         if command.operation == 'reserve' else self.native_request(command, lease=lease))
                from demiflow.execution.contracts.dataflow_codec import _thaw_json
                value = {"operation_id": reply.operation_id, "value": _thaw_json(reply.value), "value_ref": _ref_to_json(reply.value_ref)}
            elif event.kind == "read_action":
                value = self._read_action(payload.get("action_id"))
            elif event.kind == "restrict_recovery":
                value = self._restrict_recovery(payload.get("occurrence_id"))
            elif event.kind == "read_configuration":
                value = self._read_configuration()
            else:
                self._check_grant()
                value = {"operation_id": event.operation_id}
        except DataflowError as exc:
            from demiflow.execution.contracts.dataflow_failures import failure_payload
            return ControlReply(operation_id=event.operation_id, status="error", payload=failure_payload(exc),
                                payload_ref=None, error_code=exc.code)
        return ControlReply(operation_id=event.operation_id, status="ok", payload=value,
                            payload_ref=None, error_code=None)

    def _native_reserve_reply(self, event, command, *, lease):
        """Repeat a successful reserve RPC in this driver epoch, without a new lease.

        Do not cache lookups (their None result can become stale), failed calls,
        or HTTP-retry commands (each native HTTP attempt has distinct rules).
        The canonical wire digest binds the retained reply to the original
        worker, boot, attempt, fence and payload, not only a reusable RPC id.
        """
        import hashlib
        from demiflow.execution.contracts.dataflow_codec import encode_wire
        signature = hashlib.sha256(encode_wire(event, type_name='WorkerEvent', bounds=self._bounds)).hexdigest()
        with self._native_reserve_lock:
            if self._closed:
                raise DataflowError('FENCED', 'Journal service is closed')
            self._check_grant()
            current = self._leases.get(event.allocation_id)
            if current is None or current.boot_id != event.boot_id or current.expires_at_ms <= _now_ms():
                raise DataflowError('FENCED', 'Native reserve has no live worker authority')
            prior = self._native_reserve_replies.get(event.operation_id)
            if prior is not None:
                original, reply = prior
                if original != signature:
                    raise DataflowError('INVALID_CONTRACT', 'Operation id reused for a different native reserve')
                self._native_reserve_replies.move_to_end(event.operation_id)
                return reply
            reply = self.native_request(command, lease=lease)
            if not self._closed:
                self._native_reserve_replies[event.operation_id] = (signature, reply)
                self._native_reserve_owners[event.operation_id] = (event.allocation_id, event.boot_id)
                while len(self._native_reserve_replies) > self._bounds.max_pending_events:
                    evicted, _ = self._native_reserve_replies.popitem(last=False)
                    self._native_reserve_owners.pop(evicted, None)
            else:
                self._native_reserve_replies.clear()
                self._native_reserve_owners.clear()
            return reply

    # -- effect state machine (H003) ----------------------------------------
    def effect(self, command: EffectCommand, *, lease: "WorkerLease | None") -> EffectReply:
        if command.kind not in _EFFECT_KINDS:
            raise DataflowError("INVALID_CONTRACT", "unknown effect kind")
        identifier(command.operation_id)
        identifier(command.effect_id)
        identifier(command.actor_id)
        identifier(command.request_digest)
        legacy = self._action.context.directory / ".dataflow-effects" / (command.effect_id + ".json")
        if legacy.exists():
            raise DataflowError("INCOMPATIBLE_HISTORY", "Legacy file effect requires explicit reconciliation; replay is forbidden")
        ceilings = self._budget_ceilings(command.budget)
        if command.kind == "confirm" and (command.payload_ref is None or not self._store.inspect(command.payload_ref)):
            raise DataflowError("CORRUPT_ARTIFACT", "Effect result is not durably readable")
        record = self._journal.transition_effect(self._fence, self._action.action_id, command, ceilings=ceilings)
        return EffectReply(operation_id=command.operation_id, status=record['state'],
                           result_ref=_ref_from_json(record.get('result_ref')),
                           receipt_ref=_ref_from_json(record.get('receipt_ref')))

    def _budget_ceilings(self, names):
        from demiflow.execution.storage.objects import ARTIFACT_STAGING_BUDGET
        declared = {ARTIFACT_STAGING_BUDGET: self._bounds.max_spill_bytes,
                    'requests': self._action.context.limits['max_requests'],
                    'output_rows': self._bounds.max_output_rows}
        unknown = set(names) - declared.keys()
        if unknown:
            raise DataflowError('INVALID_CONTRACT', 'Undeclared budget components: ' + ','.join(sorted(unknown)))
        return {name: declared[name] for name in names}

    # -- budget admission (H004) --------------------------------------------
    def reserve(self, operation_id: str, amounts: Mapping[str, int], *, lease: "WorkerLease | None") -> None:
        identifier(operation_id)
        if not isinstance(amounts, Mapping) or not 1 <= len(amounts) <= 4:
            raise DataflowError("INVALID_CONTRACT", "bounded budget component set required")
        charges = {}
        ceilings = self._budget_ceilings(amounts)
        for name, amount in amounts.items():
            identifier(name)
            if type(amount) is not int or amount <= 0:
                raise DataflowError("INVALID_CONTRACT", "budget amount must be a positive int")
            charges[name] = (amount, ceilings[name])
        self._journal.reserve_budgets(self._fence, operation_id, charges)

    # -- artifact (H036) ----------------------------------------------------
    def artifact(self, command: ArtifactCommand, *, lease: "WorkerLease | None") -> ArtifactReply:
        return apply_artifact_command(self._action, command, bounds=self._bounds)

    # -- native queue (H056) ------------------------------------------------
    def queue(self, command: QueueCommand, *, lease: "WorkerLease | None") -> QueueReply:
        if command.operation not in _QUEUE_OPS:
            raise DataflowError("INVALID_CONTRACT", "unknown queue operation")
        identifier(command.operation_id)
        if self._native is None:
            raise DataflowError("INVALID_CONTRACT", "native acquisition service is not bound")
        handler = getattr(self._native, command.operation, None)
        if not callable(handler):
            raise DataflowError("INVALID_CONTRACT", "native service does not support this operation")
        return handler(command, lease=lease)

    def stop_model_admission(self, reason):
        # This gate belongs to the original driver reserve authority. A reserve
        # already past the gate may finish; no worker claim grants admission.
        if self._model_admission_stop is None:
            self._model_admission_stop = reason

    def _check_model_admission(self):
        reason = self._model_admission_stop
        if reason is not None:
            from demiflow.services.request_limits import cooperative_stop_error
            cooperative = cooperative_stop_error(reason)
            if cooperative is not None:
                raise cooperative
            raise DataflowError('CANCELLED', 'model request admission is closed')

    # -- native model journal (H069) ----------------------------------------
    def native_request(self, command: NativeJournalCommand, *, lease: "WorkerLease | None") -> NativeJournalReply:
        if command.operation not in _NATIVE_OPS:
            raise DataflowError("INVALID_CONTRACT", "unknown native journal operation")
        identifier(command.operation_id)
        identifier(command.action_id)
        identifier(command.node_id)
        if command.action_id != self._action.action_id:
            raise DataflowError("FENCED", "native command belongs to another action")
        if command.operation in {'reserve', 'reserve_http_retry'}:
            self._check_model_admission()
        request = self._native_request(command)
        if request.request_key != command.request_key:
            raise DataflowError("INVALID_CONTRACT", "native request_key does not match the request")
        if command.operation == 'batch_membership':
            return batch_membership_request(self, command, request)
        if command.operation == 'stream_lance':
            service = self._stream_lance
            if service is None:
                raise DataflowError('FENCED', 'Action declares no streaming Lance writer')
            return service.request(command, request)
        if command.operation == 'offline_lookup':
            from demiflow.execution.journal.dataflow_offline import offline_lookup
            return offline_lookup(self, command, request)
        if command.node_id in getattr(self, '_private_embedding_nodes', {}):
            from demiflow.execution.journal.dataflow_embedding_journal import private_embedding_request
            return private_embedding_request(self, command, request)
        from demiflow.execution.journal.dataflow_prompt_journal import is_private_prompt_request, private_prompt_request
        if is_private_prompt_request(self, command, request):
            return private_prompt_request(self, command, request)
        journal = self._native_journal()
        self._journal.bind_source(self._fence, self._action.action_id + '-native-model-journal',
                                  {'schema': 'demiflow.native_model_journal.v1',
                                   'action_id': self._action.action_id})
        return _NATIVE_HANDLERS[command.operation](self, journal, command, request)

    def _native_journal(self):
        if self._native is not None and hasattr(self._native, "native_journal"):
            return self._native.native_journal
        from demiflow.execution.journal.prompt import RunPromptJournal
        run = self._action.context
        with self._lock:
            journal = getattr(run, '_native_prompt_journal', None)
            if journal is None:
                journal = RunPromptJournal(run)
                try:
                    journal.check_recovery()
                except BaseException:
                    journal.close()
                    raise
                run._native_prompt_journal = journal
            journal.stores.update(getattr(self._action, 'object_sinks', {}))
            if getattr(journal, '_dataflow_object_lease', None) is None:
                journal._dataflow_object_lease = self._store.native_owner_lease()
            return journal

    def _native_request(self, command: NativeJournalCommand):
        from demiflow.operator_llm.journal import RequestRecord
        if command.request_ref is None:
            raise DataflowError("INVALID_CONTRACT", "native command needs a request ref")
        raw = b"".join(self._store.read(command.request_ref, max_bytes=self._bounds.max_control_bytes))
        body = json.loads(raw)
        return RequestRecord(body)

    def _read_action(self, action_id):
        identifier(action_id)
        if action_id != self._action.action_id:
            raise DataflowError("FENCED", "read_action is scoped to the bound action")
        record = self._action.record
        if record is None:
            raise DataflowError("UNKNOWN_EFFECT", "action declaration is unavailable")
        # Workers already load their sealed program/graph through authenticated
        # artifacts. This control read supplies only the action identity and
        # admitted run limits. Repeating the recursive business declaration in
        # every reply both wastes the control window and adds an envelope level
        # to a declaration that may already use its full structural budget.
        # The original declaration remains unchanged in the driver's journal.
        return {"action_id": action_id, "limits": dict(self._action.context.limits)}

    def _restrict_recovery(self, occurrence_id):
        identifier(occurrence_id)
        policy = self._recovery_policy
        if policy is None:
            raise DataflowError('INVALID_CONTRACT', 'sealed recovery policy is not bound')
        if policy.required_recovery != 'attempt_only':
            from demiflow.execution.recovery_contract import RecoveryContractError
            raise RecoveryContractError('Actor needs snapshot_state/restore_state: '
                'required_recovery=' + policy.required_recovery + ' forbids attempt_only state')
        # The worker can only tighten the bound action's recovery guarantee.
        # Persist before business calls, including crashes before the first cut.
        fact = {'schema': 'demiflow.state_recovery_restriction.v1',
                'recovery': 'attempt_only',
                'reason': 'Actor runtime resources require snapshot_state/restore_state for resume'}
        self._journal.bind_source(self._fence,
            self._action.action_id + '-state-recovery-restriction', fact)
        return {'occurrence_id': occurrence_id, 'recovery': 'attempt_only'}

    def _read_configuration(self):
        identity_doc = self._journal.recovery_identity()
        if identity_doc is None:
            raise DataflowError("UNKNOWN_EFFECT", "sealed configuration is unavailable")
        result = {"limits": identity_doc["limits"], "run_id": identity_doc["run_id"]}
        if self._native is not None and hasattr(self._native, 'binding'):
            from dataclasses import asdict
            result['queue_binding'] = asdict(self._native.binding)
            result['fetch_occurrences'] = list(getattr(self._native, '_fetches', {}))
        return result

    def _check_grant(self):
        if self._journal.recovery_identity() is None:
            raise DataflowError("FENCED", "no current owner")
        with self._journal._connection() as db:
            row = db.execute('SELECT attempt,fence FROM runtime_owner').fetchone()
        if row is None or row[1] != self._fence:
            raise DataflowError("FENCED", "worker grant is not the current owner")


def _native_reserve_context(command):
    from demiflow.operator_llm.stream_recovery import CALL_RESERVATION
    if command.reservation is None:
        return None
    reservation_id, charges = command.reservation
    return CALL_RESERVATION.set((reservation_id, dict(charges)))


def _native_lookup(service, journal, command, request):
    if command.options.get('with_references', False):
        response, references = journal.lookup_with_references(request)
        value = None if response is None else {'response': response, 'references': references}
    else:
        value = journal.lookup(request)
    return NativeJournalReply(operation_id=command.operation_id, value=value, value_ref=None)


def _native_reserve(service, journal, command, request):
    from demiflow.operator_llm.stream_recovery import CALL_RESERVATION
    token = _native_reserve_context(command)
    try:
        reserved = journal.reserve(request)
    finally:
        if token is not None:
            CALL_RESERVATION.reset(token)
    return NativeJournalReply(operation_id=command.operation_id, value=bool(reserved), value_ref=None)


def _native_reserve_http_retry(service, journal, command, request):
    options = command.options
    from demiflow.operator_llm.stream_recovery import CALL_RESERVATION
    token = _native_reserve_context(command)
    try:
        reserved = journal.reserve_http_retry(request, expected_attempt=options["expected_attempt"],
                                   expected_status=options["expected_status"], max_attempts=options["max_attempts"])
    finally:
        if token is not None:
            CALL_RESERVATION.reset(token)
    return NativeJournalReply(operation_id=command.operation_id, value=bool(reserved), value_ref=None)


def _native_response(service, journal, command, request):
    if command.value_ref is None:
        raise DataflowError("INVALID_CONTRACT", "response needs a value ref")
    raw = b"".join(service._store.read(command.value_ref, max_bytes=service._bounds.max_control_bytes))
    references = journal.response(request, json.loads(raw))
    # The native client needs the same request/response references on first
    # execution and replay. Dropping them changes call metadata and Arrow schema.
    return NativeJournalReply(operation_id=command.operation_id, value=references, value_ref=None)


def _native_failed(service, journal, command, request):
    if command.value_ref is None:
        raise DataflowError("INVALID_CONTRACT", "failed needs a value ref")
    raw = b"".join(service._store.read(command.value_ref, max_bytes=service._bounds.max_control_bytes))
    error = json.loads(raw)
    if (not isinstance(error, dict) or type(error.get('type')) is not str
            or len(error['type']) > 128 or type(error.get('detail')) is not str):
        raise DataflowError('INVALID_CONTRACT', 'Malformed native failure record')
    # Reconstruct only a data-bearing exception, never import worker-supplied code.
    failure = type(error['type'], (Exception,), {})(error['detail'])
    if 'call' in error:
        failure.call = error['call']
    journal.failed(request, failure, error.get("elapsed_s", 0))
    return NativeJournalReply(operation_id=command.operation_id, value=None, value_ref=None)


def _native_references(service, journal, command, request):
    value = journal.references(request)
    return NativeJournalReply(operation_id=command.operation_id, value=value, value_ref=None)


def _native_media_references(service, journal, command, request):
    register = command.options.get("register", False)
    if type(register) is not bool:
        raise DataflowError("INVALID_CONTRACT", "media_references register must be a bool")
    value = journal.media_references(request, register=register)
    return NativeJournalReply(operation_id=command.operation_id, value=list(value), value_ref=None)


def _native_bind_membership(service, journal, command, request):
    key = identity(['embedding-request-membership', request['native_occurrence']])
    if (command.options.get('membership_key') != key
            or command.options.get('request_key') != command.request_key):
        raise DataflowError("INVALID_CONTRACT", "bind_membership differs from the native request")
    service._journal.bind_source(service._fence, key, {'request_key': command.request_key})
    return NativeJournalReply(operation_id=command.operation_id, value=None, value_ref=None)


def _native_stage_image(service, journal, command, request):
    return stage_image(service, journal, command, request)


_NATIVE_HANDLERS = {
    "lookup": _native_lookup,
    "reserve": _native_reserve,
    "reserve_http_retry": _native_reserve_http_retry,
    "response": _native_response,
    "failed": _native_failed,
    "references": _native_references,
    "media_references": _native_media_references,
    "bind_membership": _native_bind_membership,
    "stage_image": _native_stage_image,
}


def bind_batch_contracts(service, graph, physical):
    service._batch_nodes={node.occurrence_id:{
        'max_batch':int(node.kernel.configuration.get('batch_size') or node.kernel.configuration.get('max_batch')
                        or node.kernel.configuration.get('default_batch_size') or 256),
        'partitions':tuple(stage.partitions)}
        for node in graph.nodes if node.kernel.kind in KINDS
        for stage in physical.stages if node.occurrence_id in stage.occurrences}

def batch_membership_request(service,command,record):
    declared=getattr(service,'_batch_nodes',{}).get(command.node_id)
    value=dict(record)
    if (declared is None or command.options or command.reservation is not None or command.value_ref is not None
            or set(value)!={'schema','partition','ordinal','members'} or value['schema']!=SCHEMA
            or value['partition'] not in declared['partitions']
            or type(value['ordinal']) is not int
            or not 0<=value['ordinal']<service._action.context.limits['max_commits']):
        raise DataflowError('INVALID_CONTRACT','Batch membership differs from its sealed native owner')
    members=value['members']
    operation=identity([SCHEMA,command.action_id,command.node_id,value['partition'],value['ordinal'],
                        'read' if members is None else 'bind'])
    if operation!=command.operation_id:
        raise DataflowError('INVALID_CONTRACT','Batch membership operation identity differs')
    key=identity([SCHEMA,command.action_id,command.node_id,value['partition'],value['ordinal']])
    journal=service._journal
    existing=journal.source_binding(key)
    if members is None:
        return NativeJournalReply(command.operation_id,existing['members'] if existing else None,None)
    if type(members) is not list or not 1<=len(members)<=declared['max_batch']:
        raise DataflowError('INVALID_CONTRACT','Native batch membership count exceeds its declaration')
    for item in members:
        if type(item) is not list or len(item)!=2:
            raise DataflowError('INVALID_CONTRACT','Native batch member lacks its payload identity')
        identifier(item[0]);identifier(item[1])
        if len(item[1])!=64:raise DataflowError('INVALID_CONTRACT','Native batch member digest is invalid')
    fact={'schema':SCHEMA,'action':command.action_id,'node':command.node_id,**value}
    raw=document(fact,service._bounds.max_control_bytes).decode()
    limits=service._action.context.limits
    with journal.execution_guard(service._fence) as db:
        old=db.execute('SELECT document FROM runtime_sources WHERE action_id=?',(key,)).fetchone()
        if old is not None and old[0]!=raw:
            raise DataflowError('INCOMPATIBLE_HISTORY','Native batch changed original record membership or payload')
        journal.reserve_budgets_in_transaction(db,key,{
            'native_batch_count':(1,limits['max_commits']),
            'native_batch_records':(len(members),limits['max_output_rows']*service._bounds.max_nodes),
            'native_batch_metadata':(len(raw.encode()),limits['max_control_bytes'])})
        db.execute('INSERT OR IGNORE INTO runtime_sources VALUES (?,?)',(key,raw))
    return NativeJournalReply(command.operation_id,members,None)


_PRIVATE_STORAGE_PREFIX = 'private-sqlite-'
_PRIVATE_STORAGE_MINIMUM = 4 * 1024**2

def bind_private_storage(service):
    """Called once after both sealed model binders, before worker launch."""
    configurations = [*getattr(service, '_private_embedding_nodes', {}).values(),
                      *getattr(service, '_private_prompt_nodes', {}).values()]
    if not configurations:
        return
    paths = sorted({resolve_local_artifact(item['private_journal']['path']) for item in configurations})
    ceiling = service._bounds.max_spill_bytes
    from demiflow.operator_llm.sqlite_journal import MAX_DISK_BYTES
    journal = service._journal
    # The original reservation table is the authority for both existing path
    # caps and aggregate usage. Selection + reservation is one original fence
    # transaction, so concurrent prepares cannot over-assign the private half.
    with journal.execution_guard(service._fence) as db:
        used = db.execute("SELECT coalesce(sum(amount),0) FROM runtime_reservations "
                          "WHERE name='artifact_staging_bytes' AND reservation_id LIKE ?", (_PRIVATE_STORAGE_PREFIX+'%',)).fetchone()[0]
        caps, fresh, leases = {}, [], {}
        for path in paths:
            reservation = _PRIVATE_STORAGE_PREFIX + identity(str(path))
            row = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' "
                             'AND reservation_id=?', (reservation,)).fetchone()
            if row is None:
                fresh.append(path)
            else:
                caps[path] = row[0]
        if fresh:
            capacity = min(MAX_DISK_BYTES, (ceiling // 2 - used) // len(fresh))
            if capacity < _PRIVATE_STORAGE_MINIMUM:
                raise DataflowError('BUDGET_EXCEEDED', 'Private SQLite paths exceed their shared 4 MiB per-path minimum')
            caps.update((path, capacity) for path in fresh)
        for path, capacity in caps.items():
            if not _PRIVATE_STORAGE_MINIMUM <= capacity <= min(MAX_DISK_BYTES, ceiling // 2):
                raise RecoveryContractError('Original private SQLite storage capacity is incompatible')
            leases[path] = _lease_history(path, capacity)
            _existing_size(path, capacity)
            journal.reserve_budgets_in_transaction(db, _PRIVATE_STORAGE_PREFIX + identity(str(path)),
                {'artifact_staging_bytes': (capacity, ceiling)})
    fact = {'schema': 'demiflow.private_model_storage.v1', 'paths': [
        {'path': str(path), 'max_disk_bytes': caps[path], 'classic_lease_files': {
            **leases[path], 'max_new_files': service._action.context.limits['max_requests'],
            'filesystem_metadata_byte_hard_limit': False}} for path in paths]}
    journal.bind_source(service._fence, service._action.action_id+'-private-model-storage', fact)
    service._private_storage_caps = {str(path): capacity for path, capacity in caps.items()}

def private_storage(service, declared_path):
    """Resolve only an already sealed path; no worker path is accepted."""
    path = resolve_local_artifact(declared_path)
    capacity = getattr(service, '_private_storage_caps', {}).get(str(path))
    if capacity is None:
        raise DataflowError('INVALID_CONTRACT', 'Private SQLite has no original storage reservation')
    _existing_size(path, capacity)
    return path, capacity


def stage_image(service, journal, command, request):
    from demiflow.execution.seal.plan import _artifact_from_json
    from demiflow.operator_llm.media import store_image, media_limits
    options = getattr(service, '_image_declarations', {}).get(command.node_id)
    if options is None or set(request) != {'image_ref'}:
        raise DataflowError('INVALID_CONTRACT', 'Image command has no sealed image declaration')
    ref = _artifact_from_json(request['image_ref'])
    raw = b''.join(service._store.read(ref, max_bytes=media_limits(options)['max_image_bytes']))
    owner = service._action.object_sinks[options['object_store']]
    result = store_image(raw, options, store=owner)
    return NativeJournalReply(command.operation_id, result, None)


def check_image_replay(action):
    """One action watermark in the existing prepared artifact descriptor.

    Legacy participants use the action watermark. Native row-local prompt
    participants reconcile each occurrence through the existing model journal.
    """
    if getattr(action, "native_image_effects", False):
        return  # Each occurrence is reconciled from the same native commit journal.
    with action.context._object_resource_lock:
        if not getattr(action,'replaying',False) or getattr(action,'_image_replay_checked',False):return
        confirmed=0
        for fact in action.context.journal.execution_commits(action.action_id):
            if fact['prepared'] is None:continue
            for prepared in fact['prepared']['sinks'].values():
                objects=prepared.get('objects',{})
                if type(objects) is dict:confirmed=max(confirmed,objects.get('image_requests',0))
        charged=action.context.journal.budget_consumed(image_request_budget(action.action_id))
        if charged!=confirmed:
            raise UnresolvedCommit('Image external result is unresolved; automatic regeneration is forbidden')
        action._image_replay_checked=True
