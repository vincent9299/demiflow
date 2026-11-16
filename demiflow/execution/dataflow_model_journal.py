"""Worker-side native model journal adaptation (lane M, C09).

The driver owns the single native prompt journal/database. A worker holds only
a bounded port plus sealed read-only constraint values; every model request,
response and failure travels as a typed ``NativeJournalCommand`` RPC. The remote
wrappers reuse the original business recovery methods (record/execute/decode for
Prompt, scope_request/bind_actor/drain for Embedding) and never open SQLite or a
second ledger. CALL_RESERVATION is carried explicitly into the reserve command so
the driver applies budget and request intent in one native savepoint.
"""
from __future__ import annotations

import inspect
import json
import math
from typing import Mapping

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    Bounds,
    NativeActionView,
    NativeJournalCommand,
    NativeRunView,
    RuntimeServices,
    WorkerJournalPort,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.operator_llm.journal import RequestRecord, canonical, request_key
from demiflow.operator_llm.stream_recovery import (
    CALL_RESERVATION,
    NativePromptRecovery,
    _BorrowedJournal,
)
from demiflow.embeddings.stream_recovery import NativeEmbeddingRecovery

_MAX_RECORD_BYTES = 16 * 1024**2


def _rpc_operation_id(action_id, node_id, key, operation):
    # IDENTITY item 9: the generic RPC operation binds action/occurrence/record/
    # operation/original request digest. The native request_key IS the original
    # paid-effect digest, so retries and recovery keep the same operation_id.
    return identity(['native-journal-rpc', action_id, node_id, key, operation])


class RemotePromptJournal:
    """Narrow journal surface consumed by the original Prompt/Embedding clients.

    Holds the worker port and sealed read-only constraints (max_record_bytes,
    timeout_s, read_only, limit) plus the action/node binding. It owns no database
    and no SQLite path; ``close`` releases this worker's waiters only and never
    closes the driver journal. The method surface is a fixed whitelist — there is
    no ``__getattr__`` forwarding to arbitrary driver methods.
    """

    def __init__(self, *, port: WorkerJournalPort, action: NativeActionView, bounds: Bounds) -> None:
        if not isinstance(action, NativeActionView):
            raise DataflowError('INVALID_CONTRACT', 'RemotePromptJournal requires a NativeActionView')
        if not isinstance(bounds, Bounds):
            raise DataflowError('INVALID_CONTRACT', 'RemotePromptJournal requires Bounds')
        ceiling = action.context.limits['max_requests']
        if type(ceiling) is not int or ceiling < 0:
            raise DataflowError('INVALID_CONTRACT', 'Native journal requires a finite nonnegative request ceiling')
        if type(bounds.max_control_bytes) is not int or bounds.max_control_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Native journal requires a positive control byte budget')
        self._port = port
        self._action = action
        self._node_id = None
        self._store = None
        self.read_only = False
        self.limit = ceiling
        self.max_record_bytes = min(_MAX_RECORD_BYTES, bounds.max_control_bytes)
        timeout_s = bounds.timeout_ms / 1000.0
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Native journal requires a positive finite RPC deadline')
        self.timeout_s = timeout_s
        self._closed = False
        self._waiters = set()

    # -- binding (set by the M recovery binder; not a constructor field) -------
    def _bind(self, node_id, store):
        self._node_id = node_id
        self._store = store

    def _check_open(self):
        if self._closed:
            raise DataflowError('CANCELLED', 'Remote prompt journal is closed')
        if self._node_id is None or self._store is None:
            raise DataflowError('INVALID_CONTRACT', 'Remote prompt journal is not bound to a node')

    # -- bounded artifact staging ---------------------------------------------
    def _stage(self, value) -> ArtifactRef:
        encoded = canonical(value).encode('utf-8')
        if len(encoded) > self.max_record_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Native journal record exceeds the admitted byte budget')
        return self._store.put(iter([encoded]), max_bytes=self.max_record_bytes,
                               media_type='application/json')

    def _read(self, ref: ArtifactRef):
        data = b''.join(self._store.read(ref, max_bytes=self.max_record_bytes))
        if len(data) > self.max_record_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Native journal artifact exceeds the admitted byte budget')
        return json.loads(data)

    def _command(self, operation, request, *, value_ref=None, reservation=None, options=None):
        key = request_key(request)
        return NativeJournalCommand(
            operation_id=_rpc_operation_id(self._action.action_id, self._node_id, key, operation),
            operation=operation,
            action_id=self._action.action_id,
            node_id=self._node_id,
            request_key=key,
            request_ref=self._stage(request),
            value_ref=value_ref,
            reservation=reservation,
            options=options or {})

    def _rpc(self, command):
        self._check_open()
        self._action.context.check_stop()
        waiter = object()
        self._waiters.add(waiter)
        try:
            reply = self._port.native_request(command)
        finally:
            self._waiters.discard(waiter)
        if reply.operation_id != command.operation_id:
            raise DataflowError('INTERNAL_PROTOCOL', 'Native journal reply operation_id mismatch')
        if reply.value_ref is not None:
            if reply.value is not None:
                raise DataflowError('INTERNAL_PROTOCOL', 'Native journal reply carries both value and value_ref')
            return self._read(reply.value_ref)
        from .contracts.dataflow_codec import _thaw_json
        return _thaw_json(reply.value)

    # -- the nine narrow methods ----------------------------------------------
    def lookup(self, request: Mapping) -> Mapping | None:
        # confirmed results reuse; pending/unknown raise (never a cache miss).
        return self._rpc(self._command('lookup', request))

    def lookup_with_references(self, request: Mapping):
        value = self._rpc(self._command('lookup', request, options={'with_references': True}))
        if value is None:
            return None, None
        return value['response'], value['references']

    def reserve(self, request: Mapping) -> bool:
        reservation = CALL_RESERVATION.get()
        if reservation is None:
            raise DataflowError('INVALID_CONTRACT', 'Native reserve requires a CALL_RESERVATION budget')
        # reservation == (reservation_id, charges); charges keep (amount, ceiling)
        # so an explicit 0 ceiling is not collapsed to None. One RPC carries both
        # budget and intent; no separate generic effect.reserve is issued.
        return bool(self._rpc(self._command('reserve', request, reservation=reservation)))

    def reserve_http_retry(self, request: Mapping, **kwargs) -> bool:
        allowed = {'expected_attempt', 'expected_status', 'max_attempts'}
        if set(kwargs) != allowed:
            raise DataflowError('INVALID_CONTRACT', 'Unsupported HTTP retry reservation fields')
        expected_attempt = kwargs['expected_attempt']
        expected_status = kwargs['expected_status']
        max_attempts = kwargs['max_attempts']
        if (type(expected_attempt) is not int or expected_attempt < 1
                or type(max_attempts) is not int or not 2 <= max_attempts <= 6
                or type(expected_status) is not int or not 400 <= expected_status <= 599):
            raise DataflowError('INVALID_CONTRACT', 'Invalid bounded HTTP retry reservation')
        reservation = CALL_RESERVATION.get()
        if reservation is None:
            raise DataflowError('INVALID_CONTRACT', 'Native HTTP retry requires a CALL_RESERVATION budget')
        options = {'expected_attempt': expected_attempt, 'expected_status': expected_status,
                   'max_attempts': max_attempts}
        return bool(self._rpc(self._command('reserve_http_retry', request,
                                            reservation=reservation, options=options)))

    def response(self, request: Mapping, record: Mapping) -> Mapping:
        # Bounded result is staged to an artifact first, then the original
        # response is committed; the driver returns the original references.
        value_ref = self._stage(record)
        return self._rpc(self._command('response', request, value_ref=value_ref))

    def failed(self, request: Mapping, error: BaseException, elapsed: float) -> None:
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
            raise DataflowError('INVALID_CONTRACT', 'Native failure elapsed must be finite and non-negative')
        # Only the existing error contract travels (type/detail/elapsed/call);
        # never an arbitrary pickle. Definite-unsent evidence stays in call.
        payload = {'type': type(error).__name__, 'detail': str(error), 'elapsed_s': elapsed,
                   **({'call': error.call} if getattr(error, 'call', None) else {})}
        value_ref = self._stage(payload)
        self._rpc(self._command('failed', request, value_ref=value_ref))

    def references(self, request: Mapping) -> Mapping:
        return self._rpc(self._command('references', request))

    def media_references(self, record: Mapping, *, register: bool = False) -> Mapping:
        if type(register) is not bool:
            raise DataflowError('INVALID_CONTRACT', 'media_references register must be a bool')
        names = self._rpc(self._command('media_references', record, options={'register': register}))
        return tuple(names)

    def close(self) -> None:
        # Release this worker's waiters only. The driver keeps its sole native
        # journal; another node's confirming request is unaffected.
        self._closed = True
        self._waiters.clear()


class RemotePrivateEmbeddingJournal(RemotePromptJournal):
    """Explicit delivery cache; the driver owns the original private database."""
    def __init__(self, *, services, action, node_id, declaration):
        super().__init__(port=services.journal, action=action, bounds=services.bounds)
        self._bind(node_id, services.store)
        self.read_only = declaration.get('read_only', False)
        self.timeout_s = min(self.timeout_s, declaration.get('timeout_s', 30))

    def reserve(self, request):
        # The driver derives ceilings and the stable action/node/key admission.
        # No caller-supplied path, charge or native request reservation crosses.
        return bool(self._rpc(self._command('reserve', request)))


class RemotePromptRecovery(NativePromptRecovery):
    """Prompt recovery backed by the driver native journal, not a local SQLite.

    Reuses the original ``bind_client`` record/execute/decode wrapping. The
    constructor binds a RemotePromptJournal instead of opening RunPromptJournal.
    """

    def __init__(self, actor, action: NativeActionView, node_id: str, *, services: RuntimeServices) -> None:
        # Deliberately do not call super().__init__: it opens RunPromptJournal.
        self.actor, self.action, self.node_id = actor, action, node_id
        journal = RemotePromptJournal(port=services.journal, action=action, bounds=services.bounds)
        journal._bind(node_id, services.store)
        self.journal = journal
        # native_image_effects is fixed when the view is constructed (M024); the
        # frozen view is read-only here, never mutated.
        predicate = getattr(actor, 'when', None)
        self.semantic_owners = (() if predicate is None or inspect.isfunction(predicate) or inspect.isbuiltin(predicate)
                                else (predicate,))
        self.row_local = not self.semantic_owners
        self.copy_inputs = predicate is not None

    def bind_client(self, client):
        from .dataflow_offline import RemoteOfflinePromptClient
        if isinstance(client, RemoteOfflinePromptClient):
            if client.journal is not self.journal:
                raise RecoveryContractError('Offline input port differs from the bound node')
            return  # External input lookup has no provider send/reservation.
        journal = getattr(client, 'journal', None)
        if journal is self.journal:
            # M025 pre-injected this remote journal so the constructor's
            # http_error_retry guard passed. Clear it briefly so the original
            # wrapping re-binds a borrowed wrapper and installs record/execute/
            # decode scopes; a different private journal stays authoritative.
            client.journal = None
        super().bind_client(client)


class RemoteEmbeddingRecovery(NativeEmbeddingRecovery):
    """Embedding recovery backed by the driver native journal.

    Reuses the original scope_request/bind_actor/drain. Embedding has no Prompt
    coordinator and never borrows bind_client. Membership is rewritten to the
    driver native_request.bind_membership operation.
    """

    def __init__(self, actor, action: NativeActionView, node_id: str, *, services: RuntimeServices) -> None:
        # Deliberately do not call super().__init__: it opens RunPromptJournal.
        self.actor, self.action, self.node_id = actor, action, node_id
        if actor.options.get('sqlite_journal') is not None:
            raise RecoveryContractError('Native embedding recovery cannot coexist with a private SQLite journal')
        journal = RemotePromptJournal(port=services.journal, action=action, bounds=services.bounds)
        journal._bind(node_id, services.store)
        bound = actor.options['max_response_bytes'] + 64 * 1024
        if bound > journal.max_record_bytes:
            raise RecoveryContractError('Native response ceiling exceeds the admitted journal record budget')
        self.journal = journal
        self.semantic_owners = ()
        self.row_local = True
        self.copy_inputs = False
        self.inflight = {}
        self._response_bound = None

    def verify_request_membership(self, request) -> None:
        # Reuse the original embedding-request-membership key and request_key;
        # the driver applies the original journal.bind_source. A known batch
        # occurrence can never silently acquire different input membership.
        key = identity(['embedding-request-membership', request['native_occurrence']])
        command = self.journal._command('bind_membership', request,
                                        options={'membership_key': key, 'request_key': request.request_key})
        self.journal._rpc(command)


class RemoteCodexRecovery(RemotePromptRecovery):
    """Bind the original Codex session protocol to the same request journal."""

    def bind_codex_client(self, client):
        if client.journal is not None:
            raise RecoveryContractError('Native Codex recovery cannot use a private journal')
        client.journal = _BorrowedJournal(self.journal)
        prepare = client.prepare

        def scoped(request):
            from .row_scope import current_row_scope
            scope = current_row_scope()
            if scope is None:
                raise RecoveryContractError('Codex session lost its source occurrence')
            result = prepare(request)
            client.source = RequestRecord({**client.source,
                'native_response_bytes': self.journal.max_record_bytes,
                'native_occurrence': identity([self.action.action_id, self.node_id, scope.occurrence])})
            return result

        client.prepare = scoped


def make_native_action_view(services: RuntimeServices, *, node_id: str) -> NativeActionView:
    """Read-only action_id/limits/fence/check_stop plus the bounded budget port.

    Does not impersonate a full RunContext/RunAction: the worker has no
    run.directory, journal.path or publication_guard. Unknown capabilities are
    rejected by the port, never forwarded.
    """
    assignment = services.assignment
    action_id = assignment.action_id
    facts = services.journal.read_action(action_id)
    limits = facts['limits']
    context = NativeRunView(limits=limits, fence=assignment.fence, check_stop=services.check_stop)
    return NativeActionView(action_id=action_id, context=context, object_sinks={}, native_image_effects=True)


def _rebuild_request_gate(actor, concurrency):
    from demiflow.inference import request_admission_config
    policy = getattr(actor, '_request_policy', None)
    if policy is not None:
        policy = dict(policy)
    normalized = request_admission_config(policy, concurrency=concurrency)
    if normalized is None:
        return None
    from demiflow.execution.adaptive_requests import AdaptiveRequestGate
    return AdaptiveRequestGate(normalized)


def bind_prompt_adapter(actor, *, services: RuntimeServices, node_id: str) -> None:
    action = make_native_action_view(services, node_id=node_id)
    recovery = RemotePromptRecovery(actor, action, node_id, services=services)
    actor._runtime.native_recovery = recovery
    coordinator = actor._runtime.coordinator
    coordinator.bind_execution(action.context, node_id)
    # The group-journal flag precedes the first reserve so CALL_RESERVATION is
    # staged (and carried across BlockingIOPool) rather than double-charged.
    coordinator._native_group_journal = True
    actor._runtime.request_gate = _rebuild_request_gate(actor, actor._runtime.concurrency)


def bind_embedding_adapter(actor, *, services: RuntimeServices, node_id: str) -> None:
    action = make_native_action_view(services, node_id=node_id)
    recovery = RemoteEmbeddingRecovery(actor, action, node_id, services=services)
    actor._native_recovery = recovery
    # The original astart binds actor._journal via recovery.bind_actor(); the
    # per-batch budget flows through native_request_budget -> journal.reserve in
    # one savepoint. Embedding has no Prompt coordinator.


def bind_agent_adapter(actor, *, services: RuntimeServices, node_id: str) -> None:
    action = make_native_action_view(services, node_id=node_id)
    environment = getattr(actor, 'environment', None)
    runtime_kind = getattr(environment, 'runtime', 'demiflow') if environment is not None else 'demiflow'
    coordinator = actor._runtime.coordinator
    if runtime_kind == 'codex':
        # Reserve session budget and intent atomically, with both run and node
        # ceilings. An incomplete session remains unknown and cannot restart.
        recovery = RemoteCodexRecovery(actor, action, node_id, services=services)
        recovery.row_local = False
        coordinator.bind_execution(action.context, node_id)
        coordinator._native_group_journal = True
        actor._runtime.native_recovery = recovery
        return
    # demiflow runtime: model subcalls reuse the native journal and the same run
    # quota via the original dynamic tool executor; no per-row Dataset/toolloop.
    recovery = RemotePromptRecovery(actor, action, node_id, services=services)
    recovery.row_local = False  # the agent environment is not purely row-local
    actor._runtime.native_recovery = recovery
    coordinator.bind_execution(action.context, node_id)
    coordinator._native_group_journal = True


def bind_media_adapter(actor, *, services: RuntimeServices, node_id: str) -> None:
    options = actor._runtime.options or {}
    if services.store is None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Image output requires a reachable artifact store')
    if not options.get('object_store'):
        raise DataflowError('INVALID_CONTRACT', 'Image output requires a declared native object participant')
    action = make_native_action_view(services, node_id=node_id)
    recovery = RemotePromptRecovery(actor, action, node_id, services=services)
    actor._runtime.native_recovery = recovery
    # The worker holds only the reachable store handle; the driver native object
    # participant owns registration/retention. Original ObjectRef/effect identity
    # is unchanged.
    from .dataflow_images import RemoteImageStore
    actor._runtime.artifact_store = RemoteImageStore(recovery.journal, services.store, options)
    coordinator = actor._runtime.coordinator
    coordinator.bind_execution(action.context, node_id)
    coordinator._native_group_journal = True
    from demiflow.execution.object_sink import image_request_budget
    coordinator._artifact_request_budget = image_request_budget(action)
