"""Lane Q regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib.util
import itertools
from pathlib import Path
import sys
import threading
import time
import types

import pytest


# --- spec-bound C-lane test-domain substitutes ---------------------------
def _install_contract_fakes() -> None:
    if importlib.util.find_spec('demiflow.execution.contracts.dataflow') is not None:
        return  # real C-lane implementation present (integration)
    import demiflow.execution.contracts as pkg

    df = types.ModuleType('demiflow.execution.contracts.dataflow')

    @dataclass(frozen=True)
    class ArtifactRef:
        digest: str
        size_bytes: int
        store_id: str
        key: str
        media_type: str

    @dataclass(frozen=True)
    class Bounds:
        max_nodes: int
        max_edges: int
        max_partitions: int
        max_depth: int
        max_control_bytes: int
        max_row_bytes: int
        max_batch_bytes: int
        max_resident_bytes: int
        max_spill_bytes: int
        max_state_bytes: int
        max_pending_events: int
        max_open_files: int
        max_output_rows: int
        timeout_ms: int

    @dataclass(frozen=True)
    class WorkerLease:
        allocation_id: str
        boot_id: str
        stage_id: str
        partition_ids: tuple
        host_id: str
        endpoint_ref: str
        grant_ref: ArtifactRef
        expires_at_ms: int

    @dataclass(frozen=True)
    class QueueBinding:
        action_id: str
        source_id: object
        mode: str
        contract_digest: str
        generation: object
        pool: object
        source_occurrence: object
        fetch_occurrence: str
        final_occurrence: object

    @dataclass(frozen=True)
    class QueueCommand:
        operation_id: str
        operation: str
        binding: QueueBinding
        record_id: object
        delivery_ref: object
        row_ref: object

    @dataclass(frozen=True)
    class QueueReply:
        operation_id: str
        status: str
        delivery_ref: object
        row_ref: object
        native_fact_ref: object
        retry_after_ms: int

    @dataclass(frozen=True)
    class DriverServices:
        action: object
        journal_service: object = None
        store: object = None
        resources: object = None
        policy: object = None
        bounds: object = None
        hosts: tuple = ()
        clock_ms: object = None

    @dataclass(frozen=True)
    class RuntimeServices:
        store: object = None
        journal: object = None
        bounds: object = None
        assignment: object = None
        clock_ms: object = None
        check_stop: object = None
        constants: object = None
        programs: object = None

    for name, value in dict(ArtifactRef=ArtifactRef, Bounds=Bounds, WorkerLease=WorkerLease,
                            QueueBinding=QueueBinding, QueueCommand=QueueCommand, QueueReply=QueueReply,
                            DriverServices=DriverServices, RuntimeServices=RuntimeServices).items():
        setattr(df, name, value)

    err = types.ModuleType('demiflow.execution.contracts.dataflow_errors')

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            super().__init__(message)
            if code == 'UNKNOWN_EFFECT' and retryable:
                raise ValueError('UNKNOWN_EFFECT must not be retryable')
            self.code = code
            self.message = message
            self.retryable = retryable
            self.context = dict(context or {})

    err.DataflowError = DataflowError
    sys.modules['demiflow.execution.contracts.dataflow'] = df
    sys.modules['demiflow.execution.contracts.dataflow_errors'] = err
    pkg.dataflow = df
    pkg.dataflow_errors = err


_install_contract_fakes()

from demiflow import data  # noqa: E402
from demiflow.collect import FetchPolicy, HttpSource  # noqa: E402
from demiflow.objects import LocalObjectStore  # noqa: E402
from demiflow.execution.commit import CommitCoordinator  # noqa: E402
from demiflow.execution.dataflow_queue import NativeAcquisitionService, queue_service_request  # noqa: E402
from demiflow.execution.contracts.dataflow import (  # noqa: E402
    ArtifactRef, Bounds, DriverServices, QueueBinding, QueueCommand, QueueReply,
    RuntimeServices, WorkerLease,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError  # noqa: E402
from demiflow.execution.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message  # noqa: E402
from demiflow.execution.queue_ack import QueueAcknowledge  # noqa: E402
from demiflow.execution.queue_commit import producer_action  # noqa: E402
from demiflow.execution.queue_contract import QueueIncomplete  # noqa: E402
from demiflow.execution.queue_execution import validate_topology  # noqa: E402
from demiflow.execution.queue_sinks import QueuePublishSink, QueueResultSink  # noqa: E402
from demiflow.execution.queue_source import QueueDatasource  # noqa: E402
from demiflow.execution.raw_fetch import RawFetch  # noqa: E402
from demiflow.execution.recovery_contract import RecoveryContractError, SourcePosition, identity  # noqa: E402
from demiflow.execution.pipeline.run_context import RunAction, RunContext  # noqa: E402
from demiflow.data.plan import AsyncMapOp  # noqa: E402
from test_raw_fetch import origin_server, BODY  # noqa: E402


# --- in-process tree binding (IL012) -------------------------------------
def _tree_root() -> Path:
    import demiflow
    return Path(demiflow.__file__).resolve().parent.parent


def _assert_tree() -> None:
    import demiflow
    tree = _tree_root()
    assert Path(demiflow.__file__).resolve().is_relative_to(tree)
    for name in ('demiflow.execution.dataflow_queue', 'demiflow.execution.queue_source',
                 'demiflow.execution.raw_fetch', 'demiflow.execution.queue_execution',
                 'demiflow.execution.queue_sinks'):
        mod = sys.modules.get(name)
        if mod is not None and getattr(mod, '__file__', None):
            assert Path(mod.__file__).resolve().is_relative_to(tree), name


@pytest.fixture(autouse=True)
def _tree_binding():
    _assert_tree()
    yield
    _assert_tree()


# --- test-domain artifact store / clock / services -----------------------
class MemoryStore:
    """Content-addressed in-memory ArtifactStore substitute (refs only)."""

    def __init__(self):
        self._objects = {}
        self._owners = {}
        self._lock = threading.Lock()

    def put(self, chunks, *, max_bytes, media_type):
        raw = b''.join(chunks)
        if len(raw) > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'store put exceeds max_bytes')
        digest = hashlib.sha256(raw).hexdigest()
        with self._lock:
            self._objects[('mem', digest)] = raw
        return ArtifactRef(digest=digest, size_bytes=len(raw), store_id='mem',
                           key=digest, media_type=media_type)

    def read(self, ref, *, max_bytes):
        with self._lock:
            raw = self._objects[(ref.store_id, ref.key)]
        if len(raw) > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'store read exceeds max_bytes')
        yield raw

    def inspect(self, ref):
        with self._lock:
            return (ref.store_id, ref.key) in self._objects

    def retain(self, ref, *, owner):
        with self._lock:
            self._owners.setdefault((ref.store_id, ref.key), set()).add(owner)

    def release(self, ref, *, owner):
        with self._lock:
            self._owners.get((ref.store_id, ref.key), set()).discard(owner)


class Clock:
    def __init__(self):
        self.ms = 1_000_000

    def __call__(self):
        return self.ms

    def advance(self, delta):
        self.ms += delta


def _make_bounds(**over):
    base = dict(max_nodes=64, max_edges=128, max_partitions=8, max_depth=16,
                max_control_bytes=1024 * 1024, max_row_bytes=256 * 1024,
                max_batch_bytes=1024 * 1024, max_resident_bytes=64 * 1024 * 1024,
                max_spill_bytes=64 * 1024 * 1024, max_state_bytes=16 * 1024 * 1024,
                max_pending_events=256, max_open_files=64, max_output_rows=100000,
                timeout_ms=30000)
    base.update(over)
    return Bounds(**base)


def _make_driver_services(action, store, bounds, clock):
    return DriverServices(action=action, journal_service=object(), store=store,
                          resources=object(), policy=__import__("demiflow.planning.dataflow_policy", fromlist=["resolve_runtime_policy"]).resolve_runtime_policy({}, {}, provider="local")[0], bounds=bounds,
                          hosts=(), clock_ms=clock)


def _lease(clock, *, allocation='alloc-1', boot='boot-1', ttl=60000):
    return WorkerLease(allocation_id=allocation, boot_id=boot, stage_id='stage-1',
                       partition_ids=('p0',), host_id='host-1', endpoint_ref='ep-1',
                       grant_ref=ArtifactRef('a' * 64, 1, 'mem', 'grant', 'application/json'),
                       expires_at_ms=clock() + ttl)


def _cmd(binding, operation, op_id, *, delivery_ref=None, row_ref=None, record_id=None):
    return QueueCommand(operation_id=op_id if len(op_id) == 64 else hashlib.sha256(op_id.encode()).hexdigest(), operation=operation, binding=binding,
                        record_id=record_id, delivery_ref=delivery_ref, row_ref=row_ref)


def _read_json(store, ref):
    import json
    return json.loads(b''.join(store.read(ref, max_bytes=8 * 1024 * 1024)))


# --- native queue fixtures ------------------------------------------------
_RUN_SEQ = itertools.count()


@pytest.fixture
def channel(tmp_path):
    config = QueueConfig(backend='sqlite', queue_id='fixture', path=str(tmp_path / 'q.sqlite'))
    return QueueChannel(config)


def _channel_at(tmp_path, name):
    config = QueueConfig(backend='sqlite', queue_id='fixture', path=str(tmp_path / name))
    return QueueChannel(config)


def _producer_source():
    return ProducerSourceSpec(kind='manifest', source_id='fixture-source',
                              uri='file:///fixture', sha256='1' * 64)


@contextmanager
def _native_parent(tmp_path):
    with RunContext.start(tmp_path / ('producer-' + str(next(_RUN_SEQ)))) as run, \
            run.action({'kind': 'producer-fixture'}) as parent:
        yield parent
        parent.complete({'kind': 'producer-fixture', 'finished': True})


def _publish(parent, channel, producer, index, messages=(), end=True, eos=False):
    occurrence = identity(['fixture', index, 'eos' if eos else 'tasks'])
    action = producer_action(parent, channel, producer, occurrence, 'fixture-contract')
    sink = QueuePublishSink(action, producer, 'fixture-contract')
    prepared = dict(format=2, occurrence=occurrence, source_position=index, output_ordinal=0,
                    messages=list(messages), unit_end=end, eos=eos)
    return CommitCoordinator(action).commit(
        1, outputs={'publish': (sink, prepared)},
        position=SourcePosition(producer.source.source_id, index))


def _seed(channel, tmp_path, url, *, tasks=1, pool='p'):
    producer = channel.open(pool, producer_id='producer', source=_producer_source())
    with _native_parent(tmp_path) as parent:
        messages = [task_message('t' + str(i), {'id': i, 'request': {'uri': url + '/file'}}, pool=pool)
                    for i in range(tasks)]
        _publish(parent, channel, producer, 0, messages)
        _publish(parent, channel, producer, 1, [], eos=True)
    channel.seal(producer)
    return producer


def _queue_dataset(channel, tmp_path, *, settlement='sink', explicit=False, tasks=1):
    ds = data.read_queue(channel.config, pool='p', settlement=settlement,
                         max_tasks=tasks, max_inflight=1, idle_timeout_s=2)
    ds = ds.fetch(store=LocalObjectStore(tmp_path / 'objects'),
                  policy=FetchPolicy(max_object_bytes=128 * 1024, http=HttpSource(allow_private=True)),
                  concurrency=1)
    if explicit:
        ds = ds.ack_queue(channel.config, keep_row=True)
    return ds


@contextmanager
def _service_context(tmp_path, ds, sink, *, bounds=None, clock=None):
    clock = clock or Clock()
    bounds = bounds or _make_bounds()
    store = MemoryStore()
    with RunContext.start(tmp_path / ('driver-' + str(next(_RUN_SEQ))),
                          limits={'max_row_bytes': 256 * 1024, 'max_batch_bytes': 1024 * 1024}) as run, \
            run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, store, bounds, clock)
        svc = NativeAcquisitionService(action, services=services)
        binding = svc.configure(ds._source, ds._plan, sink)
        ok = False
        try:
            yield svc, binding, store, clock
            ok = True
        finally:
            svc.close()
            if ok:
                action.complete({'kind': 'run_stream', 'queue': 'settled'})


class _DriverPort:
    """Test WorkerJournalPort/DriverAuthority routing queue RPC to the driver."""

    def __init__(self, lease):
        self.service = None
        self.lease = lease
        self.commands = []

    def bind_queue(self, service):
        self.service = service

    def queue(self, command):
        self.commands.append(command)
        op = command.operation
        if op == 'poll':
            return self.service.poll(command, lease=self.lease)
        if op == 'acquire':
            return self.service.acquire(command, lease=self.lease)
        if op == 'finalize':
            return self.service.finalize(command, lease=self.lease)
        return QueueReply(command.operation_id, 'pending', None, None, None, 0)


def _runtime_services(store, port, bounds, clock, check_stop=None):
    from demiflow.execution.contracts.dataflow import WorkerAssignment, SealedExecution
    ref = port.lease.grant_ref
    sealed = SealedExecution(ref, ref, (), ref, ref, (), ref)
    assignment = WorkerAssignment("demiflow.dataflow_assignment.v3", "run", "action", 1,
        "fence", "a" * 64, 0, port.lease, sealed, (), (), (), (), (), bounds)
    return RuntimeServices(store=store, journal=port, bounds=bounds, assignment=assignment,
                           clock_ms=clock, check_stop=check_stop or (lambda: None),
                           constants={}, programs={})


def _poll_record(svc, binding, lease, *, prefix='poll', deadline_s=15.0):
    """Worker poll loop: the rows pump claims asynchronously, so poll until a
    record arrives (pending is temporarily-empty, not end-of-input)."""
    deadline = time.monotonic() + deadline_s
    n = 0
    while time.monotonic() < deadline:
        n += 1
        reply = svc.poll(_cmd(binding, 'poll', prefix + '-' + str(n)), lease=lease)
        if reply.status == 'record':
            return reply
        if reply.status == 'eoi':
            raise AssertionError('end-of-input before any record')
        time.sleep(0.02)
    raise AssertionError('no record before poll deadline')


def _drive_one(svc, binding, store, lease, *, op_prefix='op'):
    """poll -> acquire -> finalize one delivery; return (poll, acquire, finalize)."""
    poll = _poll_record(svc, binding, lease, prefix=op_prefix + '-poll')
    assert poll.status == 'record'
    row = _read_json(store, poll.row_ref)
    acquire = svc.acquire(_cmd(binding, 'acquire', op_prefix + '-acquire',
                               delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
    assert acquire.status == 'confirmed'
    final_row = _read_json(store, acquire.row_ref)
    finalize = svc.finalize(_cmd(binding, 'finalize', op_prefix + '-finalize',
                                 delivery_ref=poll.delivery_ref, row_ref=acquire.row_ref), lease=lease)
    return poll, acquire, finalize, row, final_row


# --- Q001 construction ----------------------------------------------------
def test_q001(tmp_path, channel):
    _seed(channel, tmp_path, 'http://127.0.0.1:1', tasks=1)
    before = channel.state('p')['tasks']
    ds = _queue_dataset(channel, tmp_path)
    clock = Clock()
    with RunContext.start(tmp_path / 'driver') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        # Construction claims nothing and opens no native window.
        assert channel.state('p')['tasks'] == before
        assert svc._source is None and svc._fetch is None and svc._state == 'constructed'
        assert channel.completion(channel.snapshot('p', kind='tasks').generation, 't0') is None
        # Worker-side creation is rejected: workers only hold RuntimeServices.
        worker = _runtime_services(MemoryStore(), _DriverPort(_lease(clock)), _make_bounds(), clock)
        with pytest.raises(DataflowError) as caught:
            NativeAcquisitionService(action, services=worker)
        assert caught.value.code == 'INVALID_CONTRACT'
        with pytest.raises(DataflowError) as caught:
            NativeAcquisitionService(object(), services=services)
        assert caught.value.code == 'INVALID_CONTRACT'
        svc.close()
        action.complete({'kind': 'run_stream', 'constructed': True})


# --- Q002 configure -------------------------------------------------------
def test_q002(tmp_path, channel):
    _seed(channel, tmp_path, 'http://127.0.0.1:1', tasks=1)
    ds = _queue_dataset(channel, tmp_path)
    sink = QueueResultSink(channel.config, pool='p')
    clock = Clock()
    with RunContext.start(tmp_path / 'driver') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        binding = svc.configure(ds._source, ds._plan, sink)
        assert isinstance(binding, QueueBinding)
        assert binding.mode == 'sink' and binding.action_id == action.action_id
        assert binding.pool == 'p' and binding.fetch_occurrence
        (index, fetch), contract = validate_topology(ds._source.datasource, ds._plan, sink)
        assert binding.contract_digest == identity(contract)
        assert ds._source.datasource.parent is action
        assert ds._source.datasource.fetch_actor is fetch
        # Re-configure is rejected.
        with pytest.raises(DataflowError) as caught:
            svc.configure(ds._source, ds._plan, sink)
        assert caught.value.code == 'INVALID_CONTRACT'
        svc.close()
        action.complete({'kind': 'run_stream', 'configured': True})

    # Explicit graph-tail ACK mode.
    dse = _queue_dataset(channel, tmp_path, settlement='explicit', explicit=True)
    with RunContext.start(tmp_path / 'driver-explicit') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        binding = svc.configure(dse._source, dse._plan, None)
        assert binding.mode == 'explicit' and binding.final_occurrence
        assert isinstance(dse._source.datasource.sink, QueueResultSink)
        svc.close()
        action.complete({'kind': 'run_stream', 'explicit': True})

    # A non-pure transform mixed into the auto-ACK topology is still rejected.
    dsbad = _queue_dataset(channel, tmp_path)

    def stateful(row):
        return row

    dsbad = dsbad.map(stateful, catch=(ValueError,))
    with RunContext.start(tmp_path / 'driver-bad') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        with pytest.raises(RecoveryContractError):
            svc.configure(dsbad._source, dsbad._plan, QueueResultSink(channel.config, pool='p'))
        svc.close()
        action.complete({'kind': 'run_stream', 'rejected': True})


# --- Q003 start -----------------------------------------------------------
def test_q003(tmp_path, channel, monkeypatch):
    _seed(channel, tmp_path, 'http://127.0.0.1:1', tasks=1)
    ds = _queue_dataset(channel, tmp_path)
    sink = QueueResultSink(channel.config, pool='p')
    with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
        # start before configure is rejected (fresh service).
        fresh = NativeAcquisitionService(svc._action, services=svc._services)
        with pytest.raises(DataflowError) as caught:
            fresh.start()
        assert caught.value.code == 'INVALID_CONTRACT'
        fresh.close()
        svc.start()
        native = ds._source.datasource
        assert native.generation and native.worker_id
        assert svc._fetch.io is not None and svc._fetch.transfer is not None
        assert svc._generation == native.generation
        # The native window is registered against the driver action.
        assert native.parent is svc._action
        with pytest.raises(DataflowError) as caught:
            svc.start()
        assert caught.value.code == 'INVALID_CONTRACT'

    # astart failure closes the already-opened source owner.
    ds2 = _queue_dataset(channel, tmp_path)
    clock = Clock()
    with RunContext.start(tmp_path / 'driver-fail') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        svc.configure(ds2._source, ds2._plan, QueueResultSink(channel.config, pool='p'))

        async def broken(self):
            raise RuntimeError('fetch-astart-failed')

        monkeypatch.setattr(RawFetch, 'astart', broken)
        with pytest.raises(RuntimeError, match='fetch-astart-failed'):
            svc.start()
        assert ds2._source.datasource.closed.is_set()
        assert svc._state == 'closed'
        svc.close()
        action.complete({'kind': 'run_stream', 'start_failed': True})


# --- Q004 poll ------------------------------------------------------------
def test_q004(tmp_path, channel):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=2)
        ds = _queue_dataset(channel, tmp_path, tasks=2)
        sink = QueueResultSink(channel.config, pool='p')
        with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
            svc.start()
            lease = _lease(clock)
            poll = _poll_record(svc, binding, lease, prefix='p1')
            assert poll.status == 'record'
            assert poll.delivery_ref is not None and poll.row_ref is not None
            row = _read_json(store, poll.row_ref)
            assert row['request']['uri'] == url + '/file'
            first_task = row['_queue_delivery']['task_id']
            descriptor = _read_json(store, poll.delivery_ref)
            assert descriptor['task_id'] == first_task
            assert descriptor['generation'] == producer.generation
            # Temporarily-empty: an active claim fills max_inflight=1, the window
            # is not complete, so poll is pending (not end-of-input).
            pending = svc.poll(_cmd(binding, 'poll', 'p2'), lease=lease)
            assert pending.status == 'pending' and pending.row_ref is None
            # Stable operation retry does not claim a second task.
            again = svc.poll(_cmd(binding, 'poll', poll.operation_id), lease=lease)
            assert again.delivery_ref == poll.delivery_ref
            with channel.database.connection() as db:
                claimed = db.execute('SELECT claimed FROM queue_workers WHERE generation=?',
                                     (producer.generation,)).fetchone()[0]
            assert claimed == 1
            # Lease expiry is fenced.
            clock.advance(60001)
            with pytest.raises(DataflowError) as caught:
                svc.poll(_cmd(binding, 'poll', 'p3'), lease=lease)
            assert caught.value.code == 'FENCED'
            # Pool mismatch in the binding is rejected.
            wrong = QueueBinding(**{**binding.__dict__, 'pool': 'other'})
            clock.advance(-60001)
            with pytest.raises(DataflowError) as caught:
                svc.poll(_cmd(wrong, 'poll', 'p4'), lease=lease)
            assert caught.value.code == 'INVALID_CONTRACT'
            # Settle the first delivery; the source then claims the second.
            acquire = svc.acquire(_cmd(binding, 'acquire', 'd1-acq',
                                       delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            svc.finalize(_cmd(binding, 'finalize', 'd1-fin',
                              delivery_ref=poll.delivery_ref, row_ref=acquire.row_ref), lease=lease)
            poll2 = _poll_record(svc, binding, lease, prefix='p-second')
            second_task = _read_json(store, poll2.row_ref)['_queue_delivery']['task_id']
            assert second_task != first_task
            acquire2 = svc.acquire(_cmd(binding, 'acquire', 'd2-acq',
                                        delivery_ref=poll2.delivery_ref, row_ref=poll2.row_ref), lease=lease)
            svc.finalize(_cmd(binding, 'finalize', 'd2-fin',
                               delivery_ref=poll2.delivery_ref, row_ref=acquire2.row_ref), lease=lease)
            # The full task window is claimed and settled: end-of-input.
            deadline = time.monotonic() + 10
            while True:
                eoi = svc.poll(_cmd(binding, 'poll', 'p5'), lease=lease)
                if eoi.status == 'eoi':
                    break
                assert eoi.status == 'pending' and time.monotonic() < deadline
                time.sleep(0.02)


# --- Q005 acquire ---------------------------------------------------------
def test_q005(tmp_path, channel):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=1)
        ds = _queue_dataset(channel, tmp_path)
        sink = QueueResultSink(channel.config, pool='p')
        with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
            svc.start()
            lease = _lease(clock)
            poll = _poll_record(svc, binding, lease, prefix='a-poll')
            acquire = svc.acquire(_cmd(binding, 'acquire', 'a1',
                                       delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            assert acquire.status == 'confirmed'
            assert acquire.native_fact_ref is not None
            receipt = _read_json(store, acquire.row_ref)
            result = receipt['result']
            assert result['status'] == 'ok' and result['terminal'] is True
            assert result['acquisition_ref']['commit_id']
            assert calls['/file'] == 1
            # The acquisition commit is the original acked native fact.
            fact = _read_json(store, acquire.native_fact_ref)
            assert fact['status'] == 'acked'
            assert fact['commit_id'] == result['acquisition_ref']['commit_id']
            # Stable retry returns the same fact without re-download.
            retry = svc.acquire(_cmd(binding, 'acquire', 'a1',
                                     delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            assert retry.native_fact_ref == acquire.native_fact_ref
            # A new operation on the same delivery recovers the commit, no re-download.
            recovered = svc.acquire(_cmd(binding, 'acquire', 'a2',
                                         delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            assert recovered.status == 'confirmed' and calls['/file'] == 1
            # Owner change is fenced.
            other = _lease(clock, allocation='alloc-2', boot='boot-2')
            with pytest.raises(DataflowError) as caught:
                svc.acquire(_cmd(binding, 'acquire', 'a3',
                                 delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=other)
            assert caught.value.code == 'FENCED'
            # Tampered delivery identity is rejected.
            tampered = dict(_read_json(store, poll.delivery_ref), epoch=999)
            bad_ref = store.put(iter([__import__('json').dumps(tampered).encode()]),
                                max_bytes=65536, media_type='application/json')
            with pytest.raises(DataflowError) as caught:
                svc.acquire(_cmd(binding, 'acquire', 'a4',
                                 delivery_ref=bad_ref, row_ref=poll.row_ref), lease=lease)
            assert caught.value.code == 'INVALID_CONTRACT'


# --- Q006 finalize --------------------------------------------------------
def test_q006(tmp_path, channel, monkeypatch):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=1)
        ds = _queue_dataset(channel, tmp_path)
        sink = QueueResultSink(channel.config, pool='p')
        ack_calls = []
        original_ack = QueueDatasource.acknowledge

        def spy_ack(self, position, context):
            ack_calls.append(position)
            return original_ack(self, position, context)

        monkeypatch.setattr(QueueDatasource, 'acknowledge', spy_ack)
        with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
            svc.start()
            lease = _lease(clock)
            poll = _poll_record(svc, binding, lease, prefix='f-poll')
            # finalize before acquire is rejected.
            with pytest.raises(DataflowError) as caught:
                svc.finalize(_cmd(binding, 'finalize', 'f-early',
                                  delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            assert caught.value.code == 'INVALID_CONTRACT'
            acquire = svc.acquire(_cmd(binding, 'acquire', 'f-acq',
                                       delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
            final_row = _read_json(store, acquire.row_ref)
            # A final row that lost its acquisition receipt is rejected, not success.
            broken = dict(final_row)
            broken['result'] = {'status': 'ok'}
            broken_ref = store.put(iter([__import__('json').dumps(broken).encode()]),
                                   max_bytes=256 * 1024, media_type='application/json')
            with pytest.raises(DataflowError) as caught:
                svc.finalize(_cmd(binding, 'finalize', 'f-bad',
                                  delivery_ref=poll.delivery_ref, row_ref=broken_ref), lease=lease)
            assert caught.value.code == 'INVALID_CONTRACT'
            # Atomic native finalize: result commit + ACK, original fact returned.
            finalize = svc.finalize(_cmd(binding, 'finalize', 'f1',
                                         delivery_ref=poll.delivery_ref, row_ref=acquire.row_ref), lease=lease)
            assert finalize.status == 'confirmed' and finalize.native_fact_ref is not None
            fact = _read_json(store, finalize.native_fact_ref)
            assert fact['status'] == 'acked'
            proof = channel.completion(producer.generation, 't0')
            assert proof['settled'] is True and proof['status'] == 'ok'
            assert proof['final_commit'] == fact['commit_id']
            with channel.database.connection() as db:
                token, lease_until = db.execute(
                    'SELECT token,lease_until FROM queue_tasks WHERE generation=? AND task_id=?',
                    (producer.generation, 't0')).fetchone()
            assert (token, lease_until) == (None, None)
            # The separate acknowledge() path is never used.
            assert ack_calls == []
            # Stable finalize retry returns the same confirmed fact.
            retry = svc.finalize(_cmd(binding, 'finalize', 'f1',
                                      delivery_ref=poll.delivery_ref, row_ref=acquire.row_ref), lease=lease)
            assert retry.native_fact_ref == finalize.native_fact_ref
            assert channel.state('p')['queue_drained']


# --- Q007 finish ----------------------------------------------------------
def test_q007(tmp_path, channel):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=2)
        ds = _queue_dataset(channel, tmp_path, tasks=2)
        sink = QueueResultSink(channel.config, pool='p')
        with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
            svc.start()
            lease = _lease(clock)
            for i in range(2):
                poll, acquire, finalize, row, final_row = _drive_one(
                    svc, binding, store, lease, op_prefix='t' + str(i))
                assert finalize.status == 'confirmed'
            metrics = svc.finish()
            assert metrics['claimed'] == 2 and metrics['settled'] == 2
            assert metrics['exhausted'] == 0 and metrics['active'] == 0
            assert metrics['stop_reason'] in {'window_complete', 'queue_drained'}
            assert calls['/file'] == 2
            assert channel.state('p')['queue_drained']

    # An unconfirmed claimed task makes the whole action incomplete.
    channel2 = _channel_at(tmp_path, 'q-open.sqlite')
    producer = _seed(channel2, tmp_path, url, tasks=1)
    ds = _queue_dataset(channel2, tmp_path)
    with _service_context(tmp_path, ds, QueueResultSink(channel2.config, pool='p'),
                          bounds=_make_bounds(timeout_ms=800)) as (svc, binding, store, clock):
        svc.start()
        lease = _lease(clock)
        poll = _poll_record(svc, binding, lease, prefix='u-poll')
        assert poll.status == 'record'
        with pytest.raises(DataflowError) as caught:
            svc.finish()
        assert caught.value.code == 'DEADLINE_EXCEEDED'
        assert channel2.completion(producer.generation, 't0') is None


# --- Q008 close -----------------------------------------------------------
def test_q008(tmp_path, channel):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=1)
        ds = _queue_dataset(channel, tmp_path)
        sink = QueueResultSink(channel.config, pool='p')
        clock = Clock()
        store = MemoryStore()
        with RunContext.start(tmp_path / 'driver') as run, run.action({'kind': 'run_stream'}) as action:
            services = _make_driver_services(action, store, _make_bounds(), clock)
            svc = NativeAcquisitionService(action, services=services)
            binding = svc.configure(ds._source, ds._plan, sink)
            svc.start()
            lease = _lease(clock)
            poll, acquire, finalize, row, final_row = _drive_one(svc, binding, store, lease)
            io_thread = svc._io_thread
            svc.close()
            assert svc._state == 'closed'
            assert not io_thread.is_alive()
            assert ds._source.datasource.closed.is_set()
            assert svc._fetch.io is None
            # Committed versions and recovery evidence survive close.
            assert channel.completion(producer.generation, 't0')['settled']
            svc.close()  # idempotent
            action.complete({'kind': 'run_stream', 'queue': 'settled'})

    # Close with an acquired-but-unfinalized delivery keeps its lease/domain facts.
    channel2 = _channel_at(tmp_path, 'q-open.sqlite')
    producer = _seed(channel2, tmp_path, url, tasks=1)
    ds = _queue_dataset(channel2, tmp_path)
    clock = Clock()
    store = MemoryStore()
    with RunContext.start(tmp_path / 'driver-open') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, store, _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        binding = svc.configure(ds._source, ds._plan, QueueResultSink(channel2.config, pool='p'))
        svc.start()
        lease = _lease(clock)
        poll = _poll_record(svc, binding, lease, prefix='c-poll')
        acquire = svc.acquire(_cmd(binding, 'acquire', 'c-acq',
                                   delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), lease=lease)
        acq_fact = _read_json(store, acquire.native_fact_ref)
        svc.close()
        # No manufactured ACK; the original acquisition commit and object remain.
        assert channel2.completion(producer.generation, 't0') is None
        with channel2.database.connection() as db:
            status = db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',
                                (acq_fact['commit_id'],)).fetchone()[0]
            lease_until = db.execute('SELECT lease_until FROM queue_tasks WHERE generation=? AND task_id=?',
                                     (producer.generation, 't0')).fetchone()[0]
        assert status == 'acked' and lease_until is not None
        action.complete({'kind': 'run_stream', 'queue': 'open_delivery_retained'})

    # Close before start releases nothing and is safe.
    ds = _queue_dataset(channel, tmp_path)
    clock = Clock()
    with RunContext.start(tmp_path / 'driver-unstarted') as run, run.action({'kind': 'run_stream'}) as action:
        services = _make_driver_services(action, MemoryStore(), _make_bounds(), clock)
        svc = NativeAcquisitionService(action, services=services)
        svc.configure(ds._source, ds._plan, QueueResultSink(channel.config, pool='p'))
        svc.close()
        assert svc._state == 'closed'
        action.complete({'kind': 'run_stream', 'unstarted': True})


# --- Q009 worker queue RPC port ------------------------------------------
def test_q009(tmp_path, channel):
    with origin_server() as (url, calls):
        producer = _seed(channel, tmp_path, url, tasks=1)
        ds = _queue_dataset(channel, tmp_path)
        sink = QueueResultSink(channel.config, pool='p')
        with _service_context(tmp_path, ds, sink) as (svc, binding, store, clock):
            svc.start()
            lease = _lease(clock)
            port = _DriverPort(lease)
            port.bind_queue(svc)
            worker = _runtime_services(store, port, _make_bounds(), clock)
            # poll through the typed port: refs only, record sidecar preserved.
            deadline = time.monotonic() + 15
            n = 0
            while True:
                n += 1
                poll = queue_service_request(_cmd(binding, 'poll', 'w-poll-' + str(n)), services=worker)
                if poll.status == 'record':
                    break
                assert poll.status == 'pending' and time.monotonic() < deadline
                time.sleep(0.02)
            row = _read_json(store, poll.row_ref)
            assert '_queue_delivery' in row and row['request']['uri'] == url + '/file'
            assert all(isinstance(c, QueueCommand) for c in port.commands)
            # acquire + finalize through the same port.
            acquire = queue_service_request(_cmd(binding, 'acquire', 'w-acq',
                                                 delivery_ref=poll.delivery_ref, row_ref=poll.row_ref), services=worker)
            assert acquire.status == 'confirmed' and calls['/file'] == 1
            finalize = queue_service_request(_cmd(binding, 'finalize', 'w-fin',
                                                  delivery_ref=poll.delivery_ref, row_ref=acquire.row_ref), services=worker)
            assert finalize.status == 'confirmed'
            assert channel.completion(producer.generation, 't0')['settled']
            # The worker holds no native datasource/journal object.
            assert not hasattr(worker, 'datasource')
            assert not hasattr(worker.journal, 'connection')
            assert worker.assignment.lease == lease
            # Contract violations are rejected at the port boundary.
            with pytest.raises(DataflowError) as caught:
                queue_service_request(object(), services=worker)
            assert caught.value.code == 'INVALID_CONTRACT'
            with pytest.raises(DataflowError) as caught:
                queue_service_request(_cmd(binding, 'poll', 'w-bad', row_ref=poll.row_ref), services=worker)
            assert caught.value.code == 'INVALID_CONTRACT'
            with pytest.raises(DataflowError) as caught:
                queue_service_request(_cmd(binding, 'acquire', 'w-noref'), services=worker)
            assert caught.value.code == 'INVALID_CONTRACT'
            with pytest.raises(DataflowError) as caught:
                queue_service_request(_cmd(binding, 'poll', 'w-driver'), services=svc._services)
            assert caught.value.code == 'INVALID_CONTRACT'
            # Cancellation propagates through check_stop.
            def stop():
                raise DataflowError('CANCELLED', 'worker stopped')

            stopping = _runtime_services(store, port, _make_bounds(), clock, check_stop=stop)
            with pytest.raises(DataflowError) as caught:
                queue_service_request(_cmd(binding, 'poll', 'w-stop'), services=stopping)
            assert caught.value.code == 'CANCELLED'
            # A mismatched reply operation_id is a protocol error.
            class BadPort(_DriverPort):
                def queue(self, command):
                    return QueueReply('b' * 64, 'pending', None, None, None, 0)

            bad = _runtime_services(store, BadPort(lease), _make_bounds(), clock)
            with pytest.raises(DataflowError) as caught:
                queue_service_request(_cmd(binding, 'poll', 'w-mismatch'), services=bad)
            assert caught.value.code == 'INTERNAL_PROTOCOL'