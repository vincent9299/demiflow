"""Driver journal authority and worker journal client (H001-H010, H036-H037,
H056-H057, H066-H075; C07/C09/C10/C16).

The driver service only validates identity/allowlist/byte budgets and calls the
original journal/effect implementations through fixed handlers; it never exposes
a generic SQL/method proxy. The worker client is a bounded typed RPC port that
never opens the driver database. Durable effect facts and their budgets share one fenced RunJournal transaction;
pins live in the same authority through apply_artifact_command.
"""
from __future__ import annotations

import json
import queue
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

from .contracts.dataflow import (
    ArtifactCommand, ArtifactReply, CompletedControl, ControlReply, EffectCommand,
    EffectReply, NativeJournalCommand, NativeJournalReply, QueueCommand, QueueReply,
    WorkerEvent,
)
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import RecoveryContractError, document, identifier, identity

if TYPE_CHECKING:
    from .contracts.dataflow import ArtifactRef, ArtifactStore, Bounds, WorkerLease
    from .run_context import RunAction

_EFFECT_KINDS = frozenset({"lookup", "reserve", "mark_sent", "confirm", "unknown", "release_unsent"})
_ARTIFACT_KINDS = frozenset({"pin", "unpin", "inspect", "reserve_staging", "grow_staging", "settle_staging", "release_staging"})
_QUEUE_OPS = frozenset({"poll", "acquire", "finalize", "status", "search"})
_NATIVE_OPS = frozenset({"lookup", "reserve", "reserve_http_retry", "response", "failed",
                         "references", "media_references", "bind_membership", "stage_image"})
_COMMAND_KINDS = frozenset({"effect", "reserve", "artifact", "queue", "native_request",
                            "read_action", "read_configuration", "restrict_recovery", "check_grant"})
_EFFECT_STATES = ("absent", "reserved", "sent", "confirmed", "unknown", "unsent")
_MAX_SERVICE_WORKERS = 4


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ref_from_json(value):
    from .contracts.dataflow import ArtifactRef
    if value is None:
        return None
    if type(value) is not dict or set(value) != {"digest", "size_bytes", "store_id", "key", "media_type"}:
        raise DataflowError("INVALID_CONTRACT", "artifact ref payload is unsupported")
    return ArtifactRef(**value)


def _ref_to_json(ref):
    if ref is None:
        return None
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}


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


def _payload_bytes(event: WorkerEvent, bounds: "Bounds") -> None:
    if event.payload is not None and event.payload_ref is not None:
        raise DataflowError("INVALID_CONTRACT", "payload and payload_ref are mutually exclusive")
    if event.payload is None and event.payload_ref is None:
        raise DataflowError("INVALID_CONTRACT", "event needs payload or payload_ref")
    if event.payload is not None:
        from .contracts.dataflow import thaw_record_value
        raw = document(thaw_record_value(event.payload))
        if len(raw) > bounds.max_control_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "control payload exceeds max_control_bytes")


def apply_artifact_command(action: "RunAction", command: ArtifactCommand, *, bounds: "Bounds") -> ArtifactReply:
    """Single pin/unpin/inspect/reserve_staging transaction shared by the driver
    port and the verified worker service (H074)."""
    from .dataflow_store import ARTIFACT_STAGING_BUDGET
    if command.kind not in _ARTIFACT_KINDS:
        raise DataflowError("INVALID_CONTRACT", "unknown artifact command kind")
    identifier(command.operation_id)
    identifier(command.owner)
    journal = action.context.journal
    fence = action.context.fence
    if command.kind == 'grow_staging':
        if command.ref is not None or command.size_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Growth requires a positive staging size without a ref')
        charged = journal.grow_dataflow_staging(fence, command.owner, command.size_bytes,
                                                ceiling=bounds.max_spill_bytes)
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=charged)
    if command.kind in {'settle_staging', 'release_staging'}:
        ref = command.ref
        if command.kind == 'release_staging' and ref is not None:
            raise DataflowError('INVALID_CONTRACT', 'Release cannot carry an artifact')
        if command.kind == 'release_staging':
            root = action.context.directory / '.dataflow-store'
            if command.size_bytes != 0 or next(root.glob('*/staging/.pending-' + command.owner + '-*'), None) is not None:
                raise DataflowError('INVALID_CONTRACT', 'Release requires deletion of owned staging files')
        if command.kind == 'settle_staging':
            if ref is None:
                raise DataflowError('INVALID_CONTRACT', 'Settlement requires a durable artifact')
            if command.size_bytes != ref.size_bytes:
                raise DataflowError('INVALID_CONTRACT', 'Settlement size differs from its artifact')
            from .dataflow_store import RunArtifactStore
            store = RunArtifactStore(root=action.context.directory / '.dataflow-store',
                store_id=ref.store_id, journal=None, bounds=bounds, readonly=True)
            if not store.inspect(ref):
                raise DataflowError('CORRUPT_ARTIFACT', 'Settlement object is not durable')
        charged = journal.settle_dataflow_staging(fence, command.owner, ref)
        return ArtifactReply(operation_id=command.operation_id, present=ref is not None, charged_bytes=charged)
    if command.kind == "reserve_staging":
        if type(command.size_bytes) is not int or command.size_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "reserve_staging needs positive size_bytes")
        if command.size_bytes > bounds.max_spill_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "staging reservation exceeds ceiling")
        journal.reserve_budgets(fence, command.operation_id,
                                {ARTIFACT_STAGING_BUDGET: (command.size_bytes, bounds.max_spill_bytes)})
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=command.size_bytes)
    if command.ref is None:
        raise DataflowError("INVALID_CONTRACT", "artifact command needs a ref")
    ref = command.ref
    if command.kind == "pin":
        journal.bind_artifact_owner(fence, ref, owner=command.owner, operation_id=command.operation_id)
        return ArtifactReply(operation_id=command.operation_id, present=True, charged_bytes=0)
    if command.kind == "unpin":
        journal.release_artifact_owner(fence, ref, owner=command.owner, operation_id=command.operation_id)
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=0)
    owners, _ = journal.artifact_owners(ref, limit=1)
    return ArtifactReply(operation_id=command.operation_id, present=bool(owners), charged_bytes=0)


class DriverArtifactPort:
    """Narrow driver artifact port bound to the original action journal (H071-H073).

    Constructed before the store; it never depends on the store or service.
    """

    def __init__(self, action: "RunAction", *, bounds: "Bounds"):
        self._action = action
        self._bounds = bounds

    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None:
        from .dataflow_store import ARTIFACT_STAGING_BUDGET
        identifier(operation_id)
        if type(amounts) is not dict or set(amounts) != {ARTIFACT_STAGING_BUDGET}:
            raise DataflowError("INVALID_CONTRACT", "driver port only admits artifact staging budget")
        amount = amounts[ARTIFACT_STAGING_BUDGET]
        if type(amount) is not int or amount <= 0:
            raise DataflowError("INVALID_CONTRACT", "staging amount must be a positive int")
        self._action.context.journal.reserve_budgets(
            self._action.context.fence, operation_id,
            {ARTIFACT_STAGING_BUDGET: (amount, self._bounds.max_spill_bytes)})

    def artifact(self, command: ArtifactCommand) -> ArtifactReply:
        return apply_artifact_command(self._action, command, bounds=self._bounds)


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
        self._leases = {}
        self._worker_occurrences = {}
        self._native_action_id = None
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        for index in range(min(_MAX_SERVICE_WORKERS, max(1, bounds.max_pending_events))):
            worker = threading.Thread(target=self._serve, name="dataflow-journal-%d" % index, daemon=True)
            worker.start()
            self._workers.append(worker)

    def register_worker(self, lease, occurrences):
        if len(self._leases) >= self._bounds.max_partitions and lease.allocation_id not in self._leases:
            raise DataflowError('BUDGET_EXCEEDED', 'Worker authority table exceeds bound')
        self._leases[lease.allocation_id] = lease
        self._worker_occurrences[(lease.allocation_id, lease.boot_id)] = frozenset(occurrences)
        if self._native is not None and hasattr(self._native, 'register_worker'):
            self._native.register_worker(lease, occurrences)

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
        if type(max_replies) is not int or max_replies <= 0:
            raise DataflowError("INVALID_CONTRACT", "max_replies must be a positive int")
        with self._lock:
            batch = self._completed[:max_replies]
            del self._completed[:max_replies]
            for completed in batch:
                self._pending_payloads.pop(completed.reply.operation_id, None)
        return tuple(batch)

    def close(self, *, deadline_ms: int) -> None:
        if type(deadline_ms) is not int or deadline_ms < 0:
            raise DataflowError("INVALID_CONTRACT", "deadline_ms must be nonnegative")
        with self._lock:
            if self._closed:
                return
            self._closed = True
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
        if self._native is not None:
            close = getattr(self._native, "close", None)
            if callable(close):
                close()

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
            try:
                reply = self.dispatch(event)
            except Exception as exc:
                from .dataflow_failures import failure_payload
                reply = ControlReply(operation_id=event.operation_id, status="error", payload=failure_payload(exc),
                                     payload_ref=None, error_code=getattr(exc, 'code', 'UNKNOWN_EFFECT'))
            completed = CompletedControl(allocation_id=event.allocation_id, boot_id=event.boot_id, reply=reply)
            with self._lock:
                self._inflight.pop(event.operation_id, None)
                if not self._closed:
                    self._completed.append(completed)
            self._queue.task_done()

    # -- validation + fixed dispatch (H002) ---------------------------------
    def dispatch(self, event: WorkerEvent) -> ControlReply:
        _lease_matches(self._action, event)
        _payload_bytes(event, self._bounds)
        if event.kind not in _COMMAND_KINDS:
            raise DataflowError("INVALID_CONTRACT", "event kind is not a journal command")
        from .contracts.dataflow import thaw_record_value
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
                if command.node_id not in self._worker_occurrences.get((lease.allocation_id, lease.boot_id), ()):
                    raise DataflowError('FENCED', 'Worker does not own the native request occurrence')
                reply = self.native_request(command, lease=lease)
                from .contracts.dataflow_codec import _thaw_json
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
            from .dataflow_failures import failure_payload
            return ControlReply(operation_id=event.operation_id, status="error", payload=failure_payload(exc),
                                payload_ref=None, error_code=exc.code)
        return ControlReply(operation_id=event.operation_id, status="ok", payload=value,
                            payload_ref=None, error_code=None)

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
        from .dataflow_store import ARTIFACT_STAGING_BUDGET
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

    # -- native model journal (H069) ----------------------------------------
    def native_request(self, command: NativeJournalCommand, *, lease: "WorkerLease | None") -> NativeJournalReply:
        if command.operation not in _NATIVE_OPS:
            raise DataflowError("INVALID_CONTRACT", "unknown native journal operation")
        identifier(command.operation_id)
        identifier(command.action_id)
        identifier(command.node_id)
        if command.action_id != self._action.action_id:
            raise DataflowError("FENCED", "native command belongs to another action")
        journal = self._native_journal()
        request = self._native_request(command)
        if request.request_key != command.request_key:
            raise DataflowError("INVALID_CONTRACT", "native request_key does not match the request")
        self._journal.bind_source(self._fence, self._action.action_id + '-native-model-journal',
                                  {'schema': 'demiflow.native_model_journal.v1',
                                   'action_id': self._action.action_id})
        return _NATIVE_HANDLERS[command.operation](self, journal, command, request)

    def _native_journal(self):
        if self._native is not None and hasattr(self._native, "native_journal"):
            return self._native.native_journal
        from ..operator_llm.stream_recovery import RunPromptJournal
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
            return journal

    def _native_request(self, command: NativeJournalCommand):
        from ..operator_llm.journal import RequestRecord
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
        return {"action_id": action_id, "declaration": record["declaration"],
                "limits": dict(self._action.context.limits)}

    def _restrict_recovery(self, occurrence_id):
        identifier(occurrence_id)
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
        return result

    def _check_grant(self):
        if self._journal.recovery_identity() is None:
            raise DataflowError("FENCED", "no current owner")
        with self._journal._connection() as db:
            row = db.execute('SELECT attempt,fence FROM runtime_owner').fetchone()
        if row is None or row[1] != self._fence:
            raise DataflowError("FENCED", "worker grant is not the current owner")


def _effect_command(payload) -> EffectCommand:
    return EffectCommand(operation_id=payload["operation_id"], kind=payload["kind"],
                         effect_id=payload["effect_id"], actor_id=payload["actor_id"],
                         request_digest=payload["request_digest"],
                         payload_ref=_ref_from_json(payload.get("payload_ref")),
                         budget=payload.get("budget", {}))


def _artifact_command(payload) -> ArtifactCommand:
    return ArtifactCommand(operation_id=payload["operation_id"], kind=payload["kind"],
                           ref=_ref_from_json(payload.get("ref")), owner=payload["owner"],
                           size_bytes=payload.get("size_bytes", 0))


def _queue_command(payload) -> QueueCommand:
    from .contracts.dataflow import QueueBinding
    binding = payload["binding"]
    return QueueCommand(operation_id=payload["operation_id"], operation=payload["operation"],
                        binding=QueueBinding(**binding), record_id=payload.get("record_id"),
                        delivery_ref=_ref_from_json(payload.get("delivery_ref")),
                        row_ref=_ref_from_json(payload.get("row_ref")), node_id=payload.get('node_id'))


def _native_command(payload) -> NativeJournalCommand:
    return NativeJournalCommand(operation_id=payload["operation_id"], operation=payload["operation"],
                                action_id=payload["action_id"], node_id=payload["node_id"],
                                request_key=payload["request_key"],
                                request_ref=_ref_from_json(payload.get("request_ref")),
                                value_ref=_ref_from_json(payload.get("value_ref")),
                                reservation=payload.get("reservation"), options=payload.get("options", {}))


def _native_reserve_context(command):
    from ..operator_llm.stream_recovery import CALL_RESERVATION
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
    from ..operator_llm.stream_recovery import CALL_RESERVATION
    token = _native_reserve_context(command)
    try:
        reserved = journal.reserve(request)
    finally:
        if token is not None:
            CALL_RESERVATION.reset(token)
    return NativeJournalReply(operation_id=command.operation_id, value=bool(reserved), value_ref=None)


def _native_reserve_http_retry(service, journal, command, request):
    options = command.options
    from ..operator_llm.stream_recovery import CALL_RESERVATION
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
    from .dataflow_images import stage_image
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


class WorkerJournalClient:
    """Bounded typed worker journal port; never opens the driver database (H005-H010,
    H037, H057, H070)."""

    def __init__(self, *, transport, lease: "WorkerLease", bounds: "Bounds"):
        self._transport = transport
        self._lease = lease
        self._bounds = bounds
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()
        self._grant_doc = None

    def _grant(self):
        if self._grant_doc is None:
            store = getattr(self._transport, "store", None)
            if store is None:
                raise DataflowError("INVALID_CONTRACT", "transport exposes no store for the worker grant")
            raw = b"".join(store.read(self._lease.grant_ref, max_bytes=self._bounds.max_control_bytes))
            self._grant_doc = json.loads(raw)
        return self._grant_doc

    def _event(self, kind: str, operation_id: str, payload) -> WorkerEvent:
        grant = self._grant()
        return WorkerEvent(schema="demiflow.dataflow_event.v3", kind=kind, operation_id=operation_id,
                           run_id=grant["run_id"], attempt=grant["attempt"], fence=grant["fence"],
                           allocation_id=self._lease.allocation_id, boot_id=self._lease.boot_id,
                           plan_revision=grant.get("plan_revision", 0), scope_id=grant["scope_id"],
                           controller_epoch=grant.get("controller_epoch", 0),
                           payload_ref=None, payload=payload)

    def _request(self, kind: str, operation_id: str, payload):
        identifier(operation_id)
        deadline_ms = self._bounds.timeout_ms
        with self._lock:
            if len(self._pending) >= self._bounds.max_pending_events:
                raise DataflowError("BUDGET_EXCEEDED", "worker journal pending table is full")
            self._pending[operation_id] = time.monotonic() + deadline_ms / 1000.0
        try:
            reply = self._transport.request(self._event(kind, operation_id, payload), deadline_ms=deadline_ms)
        finally:
            with self._lock:
                self._pending.pop(operation_id, None)
        if reply.operation_id != operation_id:
            raise DataflowError("INVALID_CONTRACT", "reply operation_id does not match the request")
        if reply.status == "error":
            if reply.payload:
                from .dataflow_failures import restore_failure
                raise restore_failure(reply.payload)
            raise DataflowError(reply.error_code or "UNKNOWN_EFFECT", "driver rejected the journal command")
        if reply.status != "ok":
            raise DataflowError("UNKNOWN_EFFECT", "journal command did not complete")
        from .contracts.dataflow import thaw_record_value
        return thaw_record_value(reply.payload) if reply.payload is not None else {}

    def effect(self, command: EffectCommand) -> EffectReply:
        payload = {"operation_id": command.operation_id, "kind": command.kind, "effect_id": command.effect_id,
                   "actor_id": command.actor_id, "request_digest": command.request_digest,
                   "payload_ref": _ref_to_json(command.payload_ref), "budget": dict(command.budget)}
        value = self._request("effect", command.operation_id, payload)
        return EffectReply(operation_id=value["operation_id"], status=value["status"],
                           result_ref=_ref_from_json(value.get("result_ref")),
                           receipt_ref=_ref_from_json(value.get("receipt_ref")))

    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None:
        self._request("reserve", operation_id, {"amounts": dict(amounts)})

    def artifact(self, command: ArtifactCommand) -> ArtifactReply:
        payload = {"operation_id": command.operation_id, "kind": command.kind,
                   "ref": _ref_to_json(command.ref), "owner": command.owner, "size_bytes": command.size_bytes}
        value = self._request("artifact", command.operation_id, payload)
        return ArtifactReply(operation_id=value["operation_id"], present=value.get("present"),
                             charged_bytes=value.get("charged_bytes", 0))

    def queue(self, command: QueueCommand) -> QueueReply:
        from dataclasses import asdict
        payload = {"operation_id": command.operation_id, "operation": command.operation,
                   "binding": asdict(command.binding), "record_id": command.record_id,
                   "delivery_ref": _ref_to_json(command.delivery_ref), "row_ref": _ref_to_json(command.row_ref),
                   "node_id": command.node_id}
        value = self._request("queue", command.operation_id, payload)
        return QueueReply(operation_id=value["operation_id"], status=value["status"],
                          delivery_ref=_ref_from_json(value.get("delivery_ref")),
                          row_ref=_ref_from_json(value.get("row_ref")),
                          native_fact_ref=_ref_from_json(value.get("native_fact_ref")),
                          retry_after_ms=value.get("retry_after_ms", 0))

    def native_request(self, command: NativeJournalCommand) -> NativeJournalReply:
        from .contracts.dataflow_codec import _thaw_json
        payload = {"operation_id": command.operation_id, "operation": command.operation,
                   "action_id": command.action_id, "node_id": command.node_id, "request_key": command.request_key,
                   "request_ref": _ref_to_json(command.request_ref), "value_ref": _ref_to_json(command.value_ref),
                   "reservation": _thaw_json(command.reservation), "options": _thaw_json(command.options)}
        value = self._request("native_request", command.operation_id, payload)
        return NativeJournalReply(operation_id=value["operation_id"], value=_thaw_json(value.get("value")),
                                  value_ref=_ref_from_json(value.get("value_ref")))

    def read_action(self, action_id: str) -> Mapping[str, object]:
        identifier(action_id)
        return self._request("read_action", identity(["read-action-v3", action_id]), {"action_id": action_id})

    def restrict_recovery(self, occurrence_id: str) -> None:
        identifier(occurrence_id)
        self._request('restrict_recovery', identity(['restrict-recovery-v3',
            self._lease.boot_id, occurrence_id]), {'occurrence_id': occurrence_id})

    def read_configuration(self) -> Mapping[str, object]:
        return self._request("read_configuration", identity(["read-configuration-v3", self._lease.boot_id]), {})

    def check_grant(self) -> None:
        self.check_lease()
        self._request("check_grant", identity(["check-grant-v3", self._lease.boot_id]), {})

    def check_lease(self):
        if self._lease.expires_at_ms <= _now_ms():
            raise DataflowError('FENCED', 'Worker grant has expired')

    def renew_lease(self, command):
        """A fixed control message, never a worker-requested budget extension."""
        from dataclasses import replace
        lease = self._lease
        expected = {'kind', 'run_id', 'attempt', 'fence', 'allocation_id', 'boot_id', 'expires_at_ms'}
        if set(command) != expected:
            raise DataflowError('INVALID_CONTRACT', 'Unsupported lease renewal shape')
        grant = self._grant()
        if any(command[key] != grant[key] for key in ('run_id', 'attempt', 'fence')):
            raise DataflowError('FENCED', 'Lease renewal differs from sealed worker grant')
        expiry = command['expires_at_ms']
        if (command['allocation_id'] != lease.allocation_id or command['boot_id'] != lease.boot_id
                or lease.expires_at_ms <= _now_ms() or type(expiry) is not int
                or expiry <= lease.expires_at_ms):
            raise DataflowError('FENCED', 'Stale, foreign or expired lease renewal')
        self._lease = replace(lease, expires_at_ms=expiry)
