"""Driver-managed native acquisition and queue settlement service (v3, C16).

The four frozen queue modules (raw_fetch/queue_execution/queue_sinks/queue_source)
remain the single transaction authority. This service adapts them behind the
worker ``queue`` RPC port: it never rewrites their protocol, opens a second
SQLite ledger, or hands native journal/lock objects to a worker.
"""
from __future__ import annotations

import asyncio
import json
import queue as _queue
import threading
import time
from collections import OrderedDict
from dataclasses import asdict
from typing import Mapping, Union

from ..data.plan import AsyncMapOp, LogicalPlan
from ..data.sources import DatasourceSource, SourcePlan
from .contracts.dataflow import (
    ArtifactRef,
    DriverServices,
    QueueBinding,
    QueueCommand,
    QueueReply,
    RuntimeServices,
    WorkerLease,
)
from .contracts.dataflow_errors import DataflowError, DataflowRecoveryError
from .queue_ack import QueueAcknowledge
from .queue_contract import QueueIncomplete
from .queue_execution import (
    configure_explicit,
    finalize_task,
    queue_source,
    validate_topology,
)
from .queue_sinks import QueueResultSink
from .queue_source import QueueDatasource
from .raw_fetch import RawFetch
from .recovery_contract import RecoveryContractError, identity
from demiflow.execution.worker.row_scope import RowScope, set_row_scope
from demiflow.execution.pipeline.run_context import RunAction, action_scope

Json = Union[None, bool, int, float, str, list, dict]

_DELIVERY_MEDIA = 'application/json'
_ROW_MEDIA = 'application/json'
_FACT_MEDIA = 'application/json'
_OCCURRENCE_DOMAIN = 'dataflow-occurrence-v3'


def _occurrence(action_id: str, index: object) -> str:
    return identity([_OCCURRENCE_DOMAIN, action_id, index])


def _actor_of(op: AsyncMapOp) -> object:
    target = op.callable.target
    return getattr(target, '__self__', target)


class _Claim:
    """One driver-owned lease/claim entry; never serialized to a worker."""

    __slots__ = (
        'delivery', 'occurrence', 'allocation_id', 'boot_id', 'delivery_ref',
        'row_ref', 'acquisition_fact', 'receipt_row', 'receipt_row_ref',
        'final_fact', 'finalized', 'descriptor',
    )

    def __init__(self, delivery, occurrence, lease, delivery_ref, row_ref):
        self.delivery = delivery
        self.occurrence = occurrence
        self.allocation_id = lease.allocation_id
        self.boot_id = lease.boot_id
        self.delivery_ref = delivery_ref
        self.row_ref = row_ref
        self.acquisition_fact = None
        self.receipt_row = None
        self.receipt_row_ref = None
        self.final_fact = None
        self.finalized = False
        self.descriptor = None


from .dataflow_search import NativeSearchOperations


class NativeAcquisitionService(NativeSearchOperations):
    """Sole driver binding of the native QueueDatasource/RawFetch (OBJECT-OWNERS).

    Lifecycle: configure -> start -> poll/acquire/finalize -> finish/close.
    Construction claims nothing and opens no database. The claim table, IO
    inflight and reply cache are bounded by the driver budget; the control
    thread never runs blocking row claims.
    """

    def __init__(self, action: RunAction, *, services: DriverServices) -> None:
        if not isinstance(action, RunAction):
            raise DataflowError('INVALID_CONTRACT', 'native acquisition requires a driver RunAction')
        if not isinstance(services, DriverServices):
            # Workers only hold RuntimeServices; native acquisition is driver IO.
            raise DataflowError('INVALID_CONTRACT', 'native acquisition service is driver-only')
        self._action = action
        self._worker_occurrences = {}
        self._worker_authority_enforced = False
        self._services = services
        self._state = 'constructed'
        self._lock = threading.Lock()
        self._io_condition = threading.Condition(self._lock)
        self._source: QueueDatasource | None = None
        self._fetch: RawFetch | None = None
        self._fetches: dict[str, RawFetch] = {}
        self._sink: QueueResultSink | None = None
        self._mode = 'raw_fetch'
        self._binding: QueueBinding | None = None
        self._contract: dict | None = None
        self._generation: str | None = None
        self._claims: dict[str, _Claim] = {}
        self._handoff: _queue.Queue = _queue.Queue(maxsize=1)
        self._replies: OrderedDict[str, QueueReply] = OrderedDict()
        self._pending: dict[str, threading.Event] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._io_thread: threading.Thread | None = None
        self._rows_thread: threading.Thread | None = None
        self._drained = threading.Event()
        self._closed = threading.Event()
        self._failure: BaseException | None = None
        self._inflight = 0
        self._max_inflight = 1
        self._io_concurrency = 1
        self._max_pending = max(1, services.bounds.max_pending_events)
        self._raw_acquisition_count = 0

    @property
    def action_id(self):
        return self._action.action_id

    @property
    def binding(self):
        return self._binding

    def register_worker(self, lease, occurrences):
        self._worker_authority_enforced = True
        self._worker_occurrences[(lease.allocation_id, lease.boot_id)] = frozenset(occurrences)

    def retire_worker(self, lease):
        self._worker_occurrences.pop((lease.allocation_id, lease.boot_id), None)

    def _check_role(self, lease, role):
        if not self._worker_occurrences and not self._worker_authority_enforced:
            return  # Direct driver callers still require the native owning lease.
        occurrence = {'poll': self._binding.source_occurrence,
                      'acquire': self._binding.fetch_occurrence,
                      'finalize': self._binding.final_occurrence}[role]
        if occurrence not in self._worker_occurrences.get((lease.allocation_id, lease.boot_id), ()):
            raise DataflowError('FENCED', 'Worker does not own this native operation')

    # -- Q002 configure -------------------------------------------------
    def configure(self, source: SourcePlan, plan: LogicalPlan, sink: object | None, *, graph=None) -> QueueBinding:
        with self._lock:
            if self._state != 'constructed':
                raise DataflowError('INVALID_CONTRACT', 'native acquisition already configured')
        if not isinstance(source, SourcePlan) or not isinstance(plan, LogicalPlan):
            raise DataflowError('INVALID_CONTRACT', 'configure requires a SourcePlan and LogicalPlan')
        native = queue_source(source)
        action_id = self._action.action_id
        if native is None and graph is not None:
            from .dataflow_fetch import configure_fetch_graph
            return configure_fetch_graph(self, source, plan, graph)
        if native is not None:
            if not isinstance(native, QueueDatasource):
                raise DataflowError('INVALID_CONTRACT', 'queue source must be a native QueueDatasource')
            if isinstance(sink, QueueResultSink):
                if native.settlement != 'sink':
                    raise DataflowError('INVALID_CONTRACT', 'automatic settlement requires sink mode')
                (index, fetch), contract = validate_topology(native, plan, sink, action=self._action)
                native.configure(self._action, fetch, sink, contract, 'node-' + str(index))
                mode = 'sink'
                final_index: object = len(plan.operations)
            elif sink is None and plan.operations and type(plan.operations[-1]) is AsyncMapOp \
                    and type(_actor_of(plan.operations[-1])) is QueueAcknowledge:
                if native.settlement != 'explicit':
                    raise DataflowError('INVALID_CONTRACT', 'explicit ACK requires explicit settlement')
                configure_explicit(native, plan, self._action)
                fetch = native.fetch_actor
                contract = native.contract
                index = int(native.raw_node_id.split('-', 1)[1])
                mode = 'explicit'
                final_index = len(plan.operations) - 1
            else:
                raise DataflowError('INVALID_CONTRACT', 'queue source requires QueueResultSink or a graph-tail ack_queue')
            fetch.bind_execution(self._action.context, action_id + '-node-' + str(index))
            source_occurrence = _occurrence(action_id, 'source')
            final_occurrence = _occurrence(action_id, final_index)
            pool = native.pool
            generation = None
            self._max_inflight = native.max_inflight
            self._handoff = _queue.Queue(maxsize=native.max_inflight)
            self._source = native
            self._sink = sink if isinstance(sink, QueueResultSink) else native.sink
        else:
            if sink is not None:
                raise DataflowError('INVALID_CONTRACT', 'ordinary fetch does not settle a queue sink')
            fetches = [(i, op) for i, op in enumerate(plan.operations)
                       if type(op) is AsyncMapOp and type(_actor_of(op)) is RawFetch]
            if len(fetches) != 1:
                raise DataflowError('INVALID_CONTRACT', 'ordinary fetch requires exactly one RawFetch declaration')
            index, op = fetches[0]
            fetch = _actor_of(op)
            fetch.bind_execution(self._action.context, action_id + '-node-' + str(index))
            contract = fetch.recovery_identity()
            mode = 'raw_fetch'
            source_occurrence = None
            final_occurrence = None
            pool = None
            generation = None
            self._source = None
            self._sink = None
        self._fetch = fetch
        self._mode = mode
        self._contract = contract
        self._io_concurrency = fetch.concurrency
        if graph is not None:
            fetch_nodes = [n.occurrence_id for n in graph.nodes
                           if n.kernel.configuration.get('native_operation') == 'acquire']
            final_nodes = [n.occurrence_id for n in graph.nodes
                           if n.kernel.configuration.get('native_operation') == 'finalize']
            if len(fetch_nodes) != 1:
                raise DataflowError('INVALID_CONTRACT', 'Native acquisition needs one sealed fetch occurrence')
            fetch_occurrence = fetch_nodes[0]
            source_occurrence = graph.sources[0] if native is not None else None
            final_occurrence = (final_nodes[0] if final_nodes else graph.terminals[0]) if native is not None else None
        else:
            fetch_occurrence = _occurrence(action_id, index)
        binding = QueueBinding(
            action_id=action_id,
            source_id=source_occurrence,
            mode=mode,
            contract_digest=identity(contract),
            generation=generation,
            pool=pool,
            source_occurrence=source_occurrence,
            fetch_occurrence=fetch_occurrence,
            final_occurrence=final_occurrence,
        )
        with self._lock:
            self._binding = binding
            self._state = 'configured'
        return binding

    def can_recover_without_dataflow_cut(self):
        """Queue claims/commits have their own original native journal proof.

        This narrowly admits a pre-cut queue attempt only. Stateless one-to-one
        topology has already been validated; native startup reconciles the exact
        bound queue and task commits before issuing another claim or request.
        Ordinary sources, raw fetch and partial Dataset commits cannot use it.
        """
        journal = self._action.context.journal
        if journal.execution_commits(self.action_id):
            return False
        if self._mode == 'native_search':
            return bool(self._action.context.resuming and
                        self._prior_search_binding == asdict(self._binding))
        if self._mode == 'raw_fetch' and self._fetches:
            # No Dataset cut has published rows. Native RawFetch reuses its
            # original commit/budget scopes, including ACK-before-return gaps.
            return bool(self._action.replaying and self._prior_fetch_binding == asdict(self._binding))
        if self._source is None:
            return False
        binding = journal.source_binding(self.action_id + '-queue')
        return bool(binding and binding.get('consumer_contract') == identity(self._contract)
                    and binding.get('queue_uuid') and binding.get('generation'))

    # -- Q003 start -----------------------------------------------------
    def start(self, *, recovery_image=None, residency=None, queue_state=None) -> None:
        with self._lock:
            if self._state != 'configured':
                raise DataflowError('INVALID_CONTRACT', 'native acquisition must be configured before start')
            self._state = 'starting'
        self._loop = asyncio.new_event_loop()
        self._io_thread = threading.Thread(
            target=self._loop.run_forever, name='demiflow-v3-queue-io', daemon=True)
        self._io_thread.start()
        try:
            self._run_io(self._astart(), timeout_s=self._services.bounds.timeout_ms / 1000)
            from demiflow.execution.native_queue.dataflow_queue_recovery import restore_claims
            restore_claims(self, queue_state, has_cut=recovery_image is not None)
        except BaseException:
            self._close_owners()
            self._stop_loop()
            with self._lock:
                self._state = 'closed'
            raise
        if self._source is not None:
            self._generation = self._source.generation
            self._rows_thread = threading.Thread(
                target=self._pump_rows, name='demiflow-v3-queue-rows', daemon=True)
            self._rows_thread.start()
        with self._lock:
            self._state = 'started'

    async def _astart(self):
        with action_scope(self._action):
            if self._source is not None:
                await self._source.astart()
            if self._fetch is not None:
                await self._fetch.astart()
            for fetch in self._fetches.values():
                await fetch.astart()
            runtime = getattr(self, '_search_runtime', None)
            if runtime is not None:
                await runtime.start()

    def _pump_rows(self) -> None:
        try:
            for scoped in self._source.rows():
                while not self._closed.is_set():
                    try:
                        self._handoff.put(scoped, timeout=0.05)
                        break
                    except _queue.Full:
                        continue
                if self._closed.is_set():
                    break
        except BaseException as exc:  # noqa: BLE001 - surfaced to poll/finish
            with self._lock:
                if self._failure is None:
                    self._failure = exc
        finally:
            self._drained.set()

    # -- Q004 poll ------------------------------------------------------
    def poll(self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply:
        self._check_started()
        self._check_lease(lease)
        self._check_role(lease, 'poll')
        self._validate_command(command, 'poll', require_refs=False)
        self._validate_binding(command.binding)
        if self._source is None:
            raise DataflowError('INVALID_CONTRACT', 'ordinary fetch has no queue deliveries to poll')
        return self._run_operation(command, lambda: self._poll_impl(command, lease))

    def _poll_impl(self, command: QueueCommand, lease: WorkerLease) -> QueueReply:
        self._raise_failure()
        with self._lock:
            if (len(self._claims) >= self._max_inflight
                    and not (self._drained.is_set() and self._handoff.empty())):
                return QueueReply(command.operation_id, 'pending', None, None, None, 50)
        try:
            scoped = self._handoff.get_nowait()
        except _queue.Empty:
            if self._drained.is_set():
                self._raise_failure()
                return QueueReply(command.operation_id, 'eoi', None, None, None, 0)
            return QueueReply(command.operation_id, 'pending', None, None, None, 50)
        delivery = scoped.scope.delivery
        occurrence = scoped.scope.occurrence
        descriptor = self._delivery_descriptor(delivery)
        delivery_ref = self._put_json(descriptor, media_type=_DELIVERY_MEDIA,
                                      max_bytes=self._services.bounds.max_control_bytes)
        row_ref = self._put_json(scoped.row, media_type=_ROW_MEDIA,
                                 max_bytes=self._services.bounds.max_row_bytes)
        claim = _Claim(delivery, occurrence, lease, delivery_ref, row_ref)
        claim.descriptor = descriptor
        with self._lock:
            if len(self._claims) >= self._max_inflight:
                raise DataflowError('BUDGET_EXCEEDED', 'native claim table is full')
            self._claims[delivery.task_id] = claim
        return QueueReply(command.operation_id, 'record', delivery_ref, row_ref, None, 0)

    # -- Q005 acquire ---------------------------------------------------
    def acquire(self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply:
        self._check_started()
        self._check_lease(lease)
        self._validate_command(command, 'acquire', require_refs=True)
        self._validate_binding(command.binding)
        if self._fetches:
            if command.node_id not in self._fetches or command.node_id not in self._worker_occurrences.get(
                    (lease.allocation_id, lease.boot_id), ()):
                raise DataflowError('FENCED', 'Worker does not own this native fetch occurrence')
        else:
            self._check_role(lease, 'acquire')
        return self._run_operation(command, lambda: self._acquire_impl(command, lease))

    def _acquire_impl(self, command: QueueCommand, lease: WorkerLease) -> QueueReply:
        if self._source is None:
            fetch = self._fetches[command.node_id] if self._fetches else self._fetch
            descriptor = self._read_json(command.delivery_ref, max_bytes=self._services.bounds.max_control_bytes)
            if descriptor != {'occurrence': command.record_id}:
                raise DataflowError('FENCED', 'Raw acquisition occurrence differs')
            row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
            self._admit_io()
            try:
                receipt = self._run_io(self._acquire_io(row, None, command.record_id, fetch=fetch),
                    timeout_s=self._services.bounds.timeout_ms / 1000)
            finally:
                self._release_io()
            fact = self._acquisition_fact(receipt, fetch=fetch)
            with self._lock:
                self._raw_acquisition_count += 1
            return QueueReply(command.operation_id, 'confirmed', command.delivery_ref,
                self._put_json(receipt, media_type=_ROW_MEDIA, max_bytes=self._services.bounds.max_row_bytes),
                self._put_json(fact, media_type=_FACT_MEDIA, max_bytes=self._services.bounds.max_control_bytes), 0)
        claim = self._claim_for(command.delivery_ref, lease)
        if claim.acquisition_fact is not None:
            return self._acquisition_reply(command, claim)
        row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
        if type(row) is not dict:
            raise DataflowError('INVALID_CONTRACT', 'acquire requires exactly one request record')
        from demiflow.execution.native_queue.dataflow_queue_recovery import current_delivery_row
        native_row = current_delivery_row(row, claim)
        self._admit_io()
        try:
            receipt = self._run_io(
                self._acquire_io(native_row, claim.delivery, claim.occurrence),
                timeout_s=self._services.bounds.timeout_ms / 1000)
        finally:
            self._release_io()
        # Flow provenance stays bound to the original sealed delivery ref;
        # only native lease operations see a renewed attempt's reserved epoch.
        receipt = {**receipt, '_queue_delivery': row['_queue_delivery']}
        fact = self._acquisition_fact(receipt)
        row_ref = self._put_json(receipt, media_type=_ROW_MEDIA,
                                 max_bytes=self._services.bounds.max_row_bytes)
        fact_ref = self._put_json(fact, media_type=_FACT_MEDIA,
                                  max_bytes=self._services.bounds.max_control_bytes)
        with self._lock:
            claim.acquisition_fact = fact
            claim.receipt_row = receipt
            claim.receipt_row_ref = row_ref
        return QueueReply(command.operation_id, 'confirmed', command.delivery_ref,
                          row_ref, fact_ref, 0)

    async def _acquire_io(self, row, delivery, occurrence, *, fetch=None):
        with action_scope(self._action):
            scope_token = set_row_scope(RowScope(occurrence, delivery))
            try:
                return await (fetch or self._fetch)(row)
            finally:
                set_row_scope(None)

    # -- Q006 finalize --------------------------------------------------
    def finalize(self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply:
        self._check_started()
        self._check_lease(lease)
        self._check_role(lease, 'finalize')
        self._validate_command(command, 'finalize', require_refs=True)
        self._validate_binding(command.binding)
        if self._source is None:
            raise DataflowError('INVALID_CONTRACT', 'ordinary fetch has no queue task to finalize')
        return self._run_operation(command, lambda: self._finalize_impl(command, lease))

    def _finalize_impl(self, command: QueueCommand, lease: WorkerLease) -> QueueReply:
        claim = self._claim_for(command.delivery_ref, lease)
        if claim.acquisition_fact is None:
            raise DataflowError('INVALID_CONTRACT', 'finalize requires a confirmed acquisition')
        row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
        self._validate_final_row(row, claim)
        if claim.finalized:
            prepared = claim.final_fact['prepared']['sinks']['result']
            if identity({k: v for k, v in row.items() if k != '_queue_delivery'}) != prepared['row_sha256']:
                raise DataflowError('UNKNOWN_EFFECT', 'Restored final row differs from its original native commit')
            reply = self._final_reply(command, claim)
            with self._lock:
                self._claims.pop(claim.delivery.task_id, None)
            return reply
        from demiflow.execution.native_queue.dataflow_queue_recovery import current_delivery_row
        native_row = current_delivery_row(row, claim)
        self._admit_io()
        try:
            fact = self._run_io(self._finalize_io(claim.delivery, native_row),
                                timeout_s=self._services.bounds.timeout_ms / 1000)
        except QueueIncomplete:
            # Exhausted retryable observation is persisted without ACK; never success.
            fact = self._last_final_fact(claim.delivery)
            fact_ref = self._put_json(fact, media_type=_FACT_MEDIA,
                                      max_bytes=self._services.bounds.max_control_bytes) if fact else None
            with self._lock:
                claim.final_fact = fact
                claim.finalized = True
                self._claims.pop(claim.delivery.task_id, None)
            return QueueReply(command.operation_id, 'unknown', command.delivery_ref,
                              command.row_ref, fact_ref, 0)
        finally:
            self._release_io()
        fact_ref = self._put_json(fact, media_type=_FACT_MEDIA,
                                  max_bytes=self._services.bounds.max_control_bytes)
        with self._lock:
            claim.final_fact = fact
            claim.finalized = True
            self._claims.pop(claim.delivery.task_id, None)
        return QueueReply(command.operation_id, 'confirmed', command.delivery_ref,
                          command.row_ref, fact_ref, 0)

    async def _finalize_io(self, delivery, row):
        with action_scope(self._action):
            return await self._source.io.run(finalize_task, self._source, delivery, row)

    # -- Q007 finish ----------------------------------------------------
    def finish(self) -> Mapping[str, Json]:
        self._check_started()
        if self._mode == 'native_search':
            return {'resources': self.search_metrics()}
        timeout_s = self._services.bounds.timeout_ms / 1000
        if self._source is not None:
            if not self._drained.wait(timeout_s):
                raise DataflowError('DEADLINE_EXCEEDED', 'queue source did not drain before finish')
            self._raise_failure()
            deadline = time.monotonic() + timeout_s
            while True:
                with self._lock:
                    if not self._claims:
                        break
                if time.monotonic() > deadline:
                    raise DataflowError('DEADLINE_EXCEEDED', 'queue tasks remain unconfirmed at finish')
                time.sleep(0.01)
            self._source.assert_settled()
            return dict(self._source.snapshot_metrics())
        deadline = time.monotonic() + timeout_s
        while self._inflight:
            if time.monotonic() > deadline:
                raise DataflowError('DEADLINE_EXCEEDED', 'raw fetch inflight remains at finish')
            time.sleep(0.01)
        # Each acquisition was checked against its irreversible ACK before it
        # was returned. Keep a counter, not an unbounded second commit index.
        return dict(claimed=self._raw_acquisition_count, settled=self._raw_acquisition_count,
                    recovered=0, exhausted=0, stop_reason='raw_fetch_complete',
                    active=0, blocked_artifact_domain=0)

    # -- Q008 close -----------------------------------------------------
    def close(self) -> None:
        with self._lock:
            if self._state == 'closed':
                return
            prior = self._state
            self._state = 'closed'
        self._closed.set()
        errors: list[BaseException] = []
        if self._source is not None:
            try:
                self._source.request_stop()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
        if self._rows_thread is not None:
            self._rows_thread.join(timeout=self._services.bounds.timeout_ms / 1000)
        self._drain_inflight()
        if prior in {'started', 'starting'}:
            errors.extend(self._close_owners())
        self._stop_loop()
        if errors:
            raise errors[0]

    def _close_owners(self) -> list[BaseException]:
        errors: list[BaseException] = []
        if self._loop is None:
            return errors
        runtime = getattr(self, '_search_runtime', None)
        if runtime is not None and runtime.task is not None:
            try:
                self._run_io(runtime.close(), timeout_s=self._services.bounds.timeout_ms / 1000)
            except BaseException as exc:
                errors.append(exc)
        for fetch in ([self._fetch] if self._fetch is not None else []) + list(self._fetches.values()):
            try:
                self._run_io(fetch.aclose(), timeout_s=self._services.bounds.timeout_ms / 1000)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
        if self._source is not None:
            try:
                self._run_io(self._source.aclose(), timeout_s=self._services.bounds.timeout_ms / 1000)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
        return errors

    def _stop_loop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if self._io_thread is not None:
            self._io_thread.join(timeout=self._services.bounds.timeout_ms / 1000)
        if loop is not None and not loop.is_closed():
            loop.close()
        self._loop = None

    def _drain_inflight(self) -> None:
        deadline = time.monotonic() + self._services.bounds.timeout_ms / 1000
        while self._inflight:
            if time.monotonic() > deadline:
                raise DataflowError('DEADLINE_EXCEEDED', 'native IO inflight did not drain on close')
            time.sleep(0.01)

    # -- shared validation / helpers -----------------------------------
    def _check_started(self) -> None:
        with self._lock:
            if self._state != 'started':
                raise DataflowError('INVALID_CONTRACT', 'native acquisition service is not started')

    def _check_lease(self, lease: WorkerLease) -> None:
        if not isinstance(lease, WorkerLease):
            raise DataflowError('INVALID_CONTRACT', 'queue command requires a worker lease')
        if lease.expires_at_ms <= self._services.clock_ms():
            raise DataflowError('FENCED', 'worker lease expired')

    def _validate_command(self, command: QueueCommand, operation: str, *, require_refs: bool) -> None:
        if not isinstance(command, QueueCommand):
            raise DataflowError('INVALID_CONTRACT', 'queue request requires a QueueCommand')
        if command.operation != operation:
            raise DataflowError('INVALID_CONTRACT', 'queue operation does not match its endpoint')
        if not isinstance(command.operation_id, str) or not command.operation_id:
            raise DataflowError('INVALID_CONTRACT', 'queue operation requires a stable operation_id')
        if require_refs:
            if command.delivery_ref is None or command.row_ref is None:
                raise DataflowError('INVALID_CONTRACT', 'acquire/finalize require delivery_ref and row_ref')
        else:
            if command.row_ref is not None or command.delivery_ref is not None:
                raise DataflowError('INVALID_CONTRACT', 'poll/status must not carry business payload refs')

    def _validate_binding(self, binding: QueueBinding) -> None:
        configured = self._binding
        if not isinstance(binding, QueueBinding) or configured is None:
            raise DataflowError('INVALID_CONTRACT', 'queue command requires its configured binding')
        for field in ('action_id', 'source_id', 'mode', 'contract_digest', 'pool',
                      'source_occurrence', 'fetch_occurrence', 'final_occurrence'):
            if getattr(binding, field) != getattr(configured, field):
                raise DataflowError('INVALID_CONTRACT', 'queue binding differs from its configured contract')
        if binding.generation is not None and binding.generation != self._generation:
            raise DataflowError('INVALID_CONTRACT', 'queue binding generation is not current')

    def _claim_for(self, delivery_ref: ArtifactRef, lease: WorkerLease) -> _Claim:
        descriptor = self._read_json(delivery_ref, max_bytes=self._services.bounds.max_control_bytes)
        task_id = descriptor.get('task_id') if type(descriptor) is dict else None
        with self._lock:
            claim = self._claims.get(task_id)
        if claim is None:
            raise DataflowError('FENCED', 'queue delivery is not owned by this service')
        if delivery_ref != claim.delivery_ref or descriptor != claim.descriptor:
            raise DataflowError('INVALID_CONTRACT', 'queue delivery descriptor changed its native identity')
        if not self._worker_occurrences and (lease.allocation_id != claim.allocation_id or lease.boot_id != claim.boot_id):
            raise DataflowError('FENCED', 'queue delivery owner changed')
        return claim

    def _validate_final_row(self, row, claim: _Claim) -> None:
        if type(row) is not dict:
            raise DataflowRecoveryError('INVALID_CONTRACT', 'finalize requires exactly one final record')
        display = dict(task_id=claim.descriptor['task_id'], fingerprint=claim.descriptor['fingerprint'], attempt=claim.descriptor['epoch'])
        if row.get('_queue_delivery') != display:
            raise DataflowRecoveryError('INVALID_CONTRACT', 'final row changed its native delivery display')
        result = row.get(self._fetch.output)
        if type(result) is not dict or type(result.get('acquisition_ref')) is not dict:
            raise DataflowRecoveryError('INVALID_CONTRACT', 'final row lost its acquisition receipt')
        if result != claim.receipt_row.get(self._fetch.output):
            raise DataflowRecoveryError('INVALID_CONTRACT', 'final row changed its confirmed acquisition receipt')

    def _delivery_descriptor(self, delivery) -> dict:
        return dict(generation=delivery.generation, task_id=delivery.task_id,
                    fingerprint=delivery.fingerprint, epoch=delivery.epoch,
                    worker_id=delivery.worker_id, lease_until=delivery.lease_until,
                    domain=delivery.domain)

    def _acquisition_fact(self, receipt: dict, *, fetch=None) -> dict:
        result = receipt[(fetch or self._fetch).output]
        commit_id = result['acquisition_ref']['commit_id']
        if self._source is not None:
            from .queue_commit import QueueCommitStore
            with self._source.channel.database.connection() as db:
                fact = QueueCommitStore._fact(db, commit_id)
        else:
            fact = self._action.context.journal.execution_commit(commit_id)
        if fact is None or fact['status'] != 'acked':
            raise DataflowError('UNKNOWN_EFFECT', 'acquisition commit is not confirmed')
        return fact

    def _last_final_fact(self, delivery):
        from .queue_commit import QueueCommitStore
        with self._source.channel.database.connection() as db:
            row = db.execute('SELECT final_scope FROM queue_tasks WHERE generation=? AND task_id=?',
                             (delivery.generation, delivery.task_id)).fetchone()
            if row is None or row[0] is None:
                return None
            run_id = identity([self._source.channel.database.queue_uuid, delivery.generation])
            commit_id = identity([run_id, row[0], 1])
            return QueueCommitStore._fact(db, commit_id)

    def _acquisition_reply(self, command: QueueCommand, claim: _Claim) -> QueueReply:
        # Replaying a pre-acquisition cut may repeat pure maps before fetch.
        # Preserve their current row while reusing only the original paid result.
        row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
        from demiflow.execution.native_queue.dataflow_queue_recovery import current_delivery_row
        current_delivery_row(row, claim)
        receipt = {**row, self._fetch.output: claim.receipt_row[self._fetch.output]}
        row_ref = self._put_json(receipt, media_type=_ROW_MEDIA, max_bytes=self._services.bounds.max_row_bytes)
        fact_ref = self._put_json(claim.acquisition_fact, media_type=_FACT_MEDIA,
                                  max_bytes=self._services.bounds.max_control_bytes)
        return QueueReply(command.operation_id, 'confirmed', command.delivery_ref,
                          row_ref, fact_ref, 0)

    def checkpoint_state(self):
        from demiflow.execution.native_queue.dataflow_queue_recovery import snapshot_claims
        return snapshot_claims(self)

    def claim_window_full(self):
        if self._source is None:
            return False
        with self._lock:
            return len(self._claims) >= self._max_inflight

    def _final_reply(self, command: QueueCommand, claim: _Claim) -> QueueReply:
        fact_ref = self._put_json(claim.final_fact, media_type=_FACT_MEDIA,
                                  max_bytes=self._services.bounds.max_control_bytes) if claim.final_fact else None
        status = 'confirmed' if claim.final_fact and claim.final_fact['status'] == 'acked' else 'unknown'
        return QueueReply(command.operation_id, status, command.delivery_ref,
                          command.row_ref, fact_ref, 0)

    def _admit_io(self) -> None:
        deadline = time.monotonic() + self._services.bounds.timeout_ms / 1000
        with self._io_condition:
            while self._inflight >= self._io_concurrency:
                if self._closed.is_set():
                    raise DataflowError('CANCELLED', 'native IO admission was cancelled')
                self._action.context.check_stop()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DataflowError('DEADLINE_EXCEEDED', 'native IO admission timed out')
                self._io_condition.wait(min(.05, remaining))
            self._inflight += 1

    def _release_io(self) -> None:
        with self._io_condition:
            self._inflight -= 1
            self._io_condition.notify_all()

    def _raise_failure(self) -> None:
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def _run_io(self, coro, *, timeout_s: float):
        if self._loop is None:
            raise DataflowError('INVALID_CONTRACT', 'native IO loop is not running')
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout_s)
        except TimeoutError:
            future.cancel()
            raise DataflowError('DEADLINE_EXCEEDED', 'native IO exceeded its driver deadline', retryable=True)

    def _run_operation(self, command: QueueCommand, handler) -> QueueReply:
        op_id = command.operation_id
        timeout_s = self._services.bounds.timeout_ms / 1000
        with self._lock:
            cached = self._replies.get(op_id)
            if cached is not None:
                return cached
            event = self._pending.get(op_id)
            if event is None:
                if len(self._pending) >= self._max_pending:
                    raise DataflowError('CAPACITY_PENDING', 'queue operation table is full', retryable=True)
                event = threading.Event()
                self._pending[op_id] = event
                owner = True
            else:
                owner = False
        if not owner:
            if not event.wait(timeout_s):
                raise DataflowError('DEADLINE_EXCEEDED', 'queue operation retry timed out', retryable=True)
            with self._lock:
                cached = self._replies.get(op_id)
            if cached is not None:
                return cached
            raise DataflowError('DEADLINE_EXCEEDED', 'concurrent queue operation produced no confirmed reply', retryable=True)
        try:
            reply = handler()
            with self._lock:
                if reply.status != 'pending':
                    self._replies[op_id] = reply
                while len(self._replies) > self._max_pending:
                    self._replies.popitem(last=False)
            return reply
        finally:
            with self._lock:
                self._pending.pop(op_id, None)
            event.set()

    def _put_json(self, value, *, media_type: str, max_bytes: int) -> ArtifactRef:
        raw = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
        if len(raw) > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'queue artifact exceeds its driver byte budget')
        return self._services.store.put(iter([raw]), max_bytes=max_bytes, media_type=media_type)

    def _read_json(self, ref: ArtifactRef, *, max_bytes: int):
        if not isinstance(ref, ArtifactRef):
            raise DataflowError('INVALID_CONTRACT', 'queue payload requires an artifact ref')
        raw = b''.join(self._services.store.read(ref, max_bytes=max_bytes))
        if len(raw) > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'queue artifact exceeds its driver byte budget')
        return json.loads(raw)


def queue_service_request(command: QueueCommand, *, services: RuntimeServices) -> QueueReply:
    """Worker-side typed ``queue`` RPC (Q009); refs only, no native objects.

    QueueReader/fetch/ack kernels call this single entry. It forwards to the
    driver through WorkerJournalPort.queue and never pickles a QueueDatasource,
    native database or lock, nor writes a second queue state.
    """
    if not isinstance(services, RuntimeServices):
        raise DataflowError('INVALID_CONTRACT', 'queue request requires worker RuntimeServices')
    if not isinstance(command, QueueCommand):
        raise DataflowError('INVALID_CONTRACT', 'queue request requires a QueueCommand')
    if command.operation not in {'poll', 'acquire', 'finalize', 'status', 'search'}:
        raise DataflowError('INVALID_CONTRACT', 'unknown queue operation')
    if command.operation in {'acquire', 'finalize', 'search'}:
        if command.delivery_ref is None or command.row_ref is None:
            raise DataflowError('INVALID_CONTRACT', 'acquire/finalize require delivery_ref and row_ref')
    elif command.row_ref is not None or command.delivery_ref is not None:
        raise DataflowError('INVALID_CONTRACT', 'poll/status must not carry business payload refs')
    services.check_stop()
    reply = services.journal.queue(command)
    if not isinstance(reply, QueueReply):
        raise DataflowError('INTERNAL_PROTOCOL', 'queue port returned a non-QueueReply')
    if reply.operation_id != command.operation_id:
        raise DataflowError('INTERNAL_PROTOCOL', 'queue reply operation_id differs from its request')
    if reply.status not in {'record', 'pending', 'eoi', 'confirmed', 'unknown'}:
        raise DataflowError('INTERNAL_PROTOCOL', 'queue reply has an unknown status')
    services.check_stop()
    return reply
