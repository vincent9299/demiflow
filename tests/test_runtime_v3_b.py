"""Lane B unit contracts for the unified v3 worker runtime (gate G-B).

Each test calls the real production symbol. Upstream lanes that have not landed
in this isolated tree are replaced by strict, spec-bound test-domain fakes
installed in sys.modules; production code keeps plain lazy imports (no fallback).
Every test process asserts that demiflow and its loaded submodules come from
this worktree (IL012).
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import types
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

TREE = Path(__file__).resolve().parents[1]


def _assert_tree_binding():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE), demiflow.__file__
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            file = getattr(module, "__file__", None)
            if file:
                assert Path(file).resolve().is_relative_to(TREE), (name, file)


_assert_tree_binding()


# ---------------------------------------------------------------------------
# Spec-bound frozen DTOs (mirrors interfaces.pyi field sets).
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ArtifactRef:
    digest: str = "d" * 64
    size_bytes: int = 1
    store_id: str = "store"
    key: str = "k"
    media_type: str = "application/json"


@dataclass(frozen=True)
class Bounds:
    max_nodes: int = 4096
    max_edges: int = 16384
    max_partitions: int = 4096
    max_depth: int = 64
    max_control_bytes: int = 1 << 20
    max_row_bytes: int = 1 << 20
    max_batch_bytes: int = 1 << 22
    max_resident_bytes: int = 1 << 24
    max_spill_bytes: int = 1 << 24
    max_state_bytes: int = 1 << 22
    max_pending_events: int = 64
    max_open_files: int = 256
    max_output_rows: int = 1 << 20
    timeout_ms: int = 5000


@dataclass(frozen=True)
class WorkerLease:
    allocation_id: str = "alloc"
    boot_id: str = "boot"
    stage_id: str = "s0"
    partition_ids: tuple = ("p0",)
    host_id: str = "host"
    endpoint_ref: str = "ep"
    grant_ref: ArtifactRef = field(default_factory=ArtifactRef)
    expires_at_ms: int = 1 << 40


@dataclass(frozen=True)
class SealedExecution:
    logical_ref: ArtifactRef = field(default_factory=ArtifactRef)
    physical_ref: ArtifactRef = field(default_factory=ArtifactRef)
    programs: tuple = ()
    code_pin_ref: ArtifactRef = field(default_factory=ArtifactRef)
    identity_bridge_ref: ArtifactRef = field(default_factory=ArtifactRef)
    source_refs: tuple = ()
    constants_ref: ArtifactRef = field(default_factory=ArtifactRef)


@dataclass(frozen=True)
class InputPort:
    name: str = "in"
    requirement: str = "incremental"
    ordering: str = "none"
    schema_ref: object = None


@dataclass(frozen=True)
class KernelSpec:
    kind: str = "map"
    implementation_ref: object = None
    configuration: dict = field(default_factory=dict)
    inputs: tuple = (InputPort(),)
    state: str = "none"
    effect: str = "pure"
    resource_role: str = "cpu"
    profile: object = None
    output_bound_bytes: int = 1 << 20
    mergeable: bool = True


@dataclass(frozen=True)
class GraphNode:
    occurrence_id: str = "occ0"
    semantic_id: str = "sem0"
    kind: str = "transform"
    kernel: KernelSpec = field(default_factory=KernelSpec)
    program_ref: object = None
    shared_token: str = "0"


@dataclass(frozen=True)
class GraphEdge:
    edge_id: str = "e0"
    producer: str = "occ0"
    output_port: str = "out"
    consumer: str = "occ1"
    input_port: str = "in"


@dataclass(frozen=True)
class DataflowGraph:
    schema: str = "demiflow.dataflow_graph.v3"
    action_id: str = "action"
    dag_id: str = "dag"
    nodes: tuple = (GraphNode(),)
    edges: tuple = ()
    terminals: tuple = ()
    sources: tuple = ("occ0",)


@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str = "occ0"
    kind: str = "items"
    immutable_identity: dict = field(default_factory=dict)
    finite: bool = True
    replay: str = "seek"
    manifest_ref: object = None


@dataclass(frozen=True)
class SourceSplit:
    split_id: str = "split0"
    source_id: str = "occ0"
    locator: dict = field(default_factory=dict)
    start_cursor: dict = field(default_factory=dict)
    estimated_bytes: object = None
    required_domain: str = "local"


@dataclass(frozen=True)
class StageSpec:
    stage_id: str = "s0"
    occurrences: tuple = ("occ0",)
    partitions: tuple = ("p0",)
    profile: object = None
    replica_min: int = 1
    replica_target: int = 1
    replica_max: int = 1
    resident_bytes: int = 1024
    output_bytes: int = 1024
    state_bytes: int = 1024
    kernel_refs: tuple = ()


@dataclass(frozen=True)
class ExchangeSpec:
    channel_id: str = "ch0"
    edge_id: str = "e0"
    producer_partitions: tuple = ("p0",)
    consumer_partitions: tuple = ("p0",)
    mode: str = "memory_incremental"
    routing: str = "forward"
    keys: tuple = ()
    scope_id: object = None
    route_epoch: int = 0
    credit_rows: int = 16
    credit_bytes: int = 1 << 20
    storage_bytes: int = 1 << 22
    read_domain: str = "local"
    reason: str = "test"


@dataclass(frozen=True)
class ResidencyPlan:
    simultaneous_groups: tuple = ()
    control_reserved_bytes: int = 1024
    consumer_reserved_slots: int = 1
    required_spill_bytes: int = 1024


@dataclass(frozen=True)
class FiniteScope:
    scope_id: str = "scope"
    generation: int = 0
    membership: tuple = ()
    completion_condition: str = "input_eoi"


@dataclass(frozen=True)
class RuntimePolicy:
    provider: str = "local"
    durability: str = "ephemeral"
    default_partitions: int = 1
    max_replicas: int = 1
    fusion: bool = True
    checkpoint_rows: int = 1024
    checkpoint_interval_ms: int = 1000
    fairness_quantum_bytes: int = 1 << 16
    control_timeout_ms: int = 1000
    store_id: str = "store"
    required_recovery: str = "attempt_only"


@dataclass(frozen=True)
class PhysicalDataflow:
    schema: str = "demiflow.physical_dataflow.v3"
    dag_id: str = "dag"
    plan_id: str = "plan"
    plan_revision: int = 1
    stages: tuple = (StageSpec(),)
    exchanges: tuple = ()
    residency: ResidencyPlan = field(default_factory=ResidencyPlan)
    scopes: tuple = ()
    sources: tuple = (SourceSnapshot(),)
    splits: tuple = (SourceSplit(),)
    policy: RuntimePolicy = field(default_factory=RuntimePolicy)
    bounds: Bounds = field(default_factory=Bounds)
    recovery: str = "attempt_only"
    limitations: tuple = ()
    constants_ref: ArtifactRef = field(default_factory=ArtifactRef)


@dataclass(frozen=True)
class WorkerAssignment:
    schema: str = "demiflow.dataflow_assignment.v3"
    run_id: str = "run"
    action_id: str = "action"
    attempt: int = 1
    fence: str = "fence"
    plan_id: str = "plan"
    plan_revision: int = 1
    lease: WorkerLease = field(default_factory=WorkerLease)
    sealed: SealedExecution = field(default_factory=SealedExecution)
    stages: tuple = (StageSpec(),)
    inputs: tuple = ()
    outputs: tuple = (ExchangeSpec(),)
    source_splits: tuple = (SourceSplit(),)
    restore_refs: tuple = ()
    bounds: Bounds = field(default_factory=Bounds)


@dataclass(frozen=True)
class SourceCursor:
    split_id: str = "split0"
    offset: dict = field(default_factory=dict)
    next_sequence: int = 0
    end_of_input: bool = False
    lease_ref: object = None
    pending_refs: tuple = ()


@dataclass(frozen=True)
class Record:
    record_id: str = "r0"
    source_position: dict = field(default_factory=dict)
    payload_ref: object = None
    inline: object = None
    encoded_bytes: int = 8
    sequence: int = 0


@dataclass(frozen=True)
class SourcePoll:
    status: str = "record"
    record: object = None
    retry_after_ms: int = 0


@dataclass(frozen=True)
class CutRequest:
    cut_id: str = "cut0"
    sequence: int = 1
    plan_revision: int = 1
    route_epoch: int = 0
    participants: tuple = ("s0:p0",)
    final: bool = False


@dataclass(frozen=True)
class CutContribution:
    cut_id: str = "cut0"
    participant_id: str = "s0:p0"
    boot_id: str = "boot"
    sources: tuple = ()
    operator_states: dict = field(default_factory=dict)
    channels: dict = field(default_factory=dict)
    buffers: tuple = ()
    effects: tuple = ()
    sinks: dict = field(default_factory=dict)
    final: bool = False


@dataclass(frozen=True)
class RecoveryImage:
    commit_id: str = "commit"
    cut: CutRequest = field(default_factory=CutRequest)
    contributions: tuple = ()
    confirmed_sinks: dict = field(default_factory=dict)
    source_vector: tuple = ()


@dataclass(frozen=True)
class WorkerEvent:
    schema: str = "demiflow.dataflow_event.v3"
    kind: str = "progress"
    operation_id: str = "op"
    run_id: str = "run"
    attempt: int = 1
    fence: str = "fence"
    allocation_id: str = "alloc"
    boot_id: str = "boot"
    plan_revision: int = 1
    scope_id: str = "action"
    controller_epoch: int = 1
    payload_ref: object = None
    payload: object = None


@dataclass(frozen=True)
class ControlReply:
    operation_id: str = "op"
    status: str = "ok"
    payload: object = None
    payload_ref: object = None
    error_code: object = None


@dataclass(frozen=True)
class CreditLease:
    operation_id: str = "op"
    channel_id: str = "ch0"
    reader_id: str = "boot"
    rows: int = 1
    size_bytes: int = 8


@dataclass(frozen=True)
class RoutedRecord:
    reader_id: str = "boot"
    delivery_id: str = "d0"
    record: Record = field(default_factory=Record)


@dataclass(frozen=True)
class EncodedRecord:
    record_id: str = "r0"
    encoding: str = "json"
    data: object = None
    ref: object = None
    encoded_bytes: int = 8
    decoded_upper_bytes: int = 64
    sequence: int = 0


@dataclass(frozen=True)
class LoadedProgram:
    kind: str = "callable"
    value: object = None
    digest: str = "d" * 64


@dataclass(frozen=True)
class WorkerStep:
    processed: int = 0
    blocked: bool = False
    end_of_input: bool = False
    events: tuple = ()


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.context = context or {}


# ---------------------------------------------------------------------------
# Test-domain fakes for upstream lanes (strict frozen signatures).
# ---------------------------------------------------------------------------
class FakeJournal:
    def __init__(self):
        self.grant_checks = 0
        self.opened_db = False

    def check_grant(self):
        self.grant_checks += 1

    def native_request(self, command):
        raise AssertionError("not used in unit scope")

    def queue(self, command):
        raise AssertionError("not used in unit scope")

    def effect(self, command):
        raise AssertionError("not used in unit scope")

    def artifact(self, command):
        raise AssertionError("not used in unit scope")

    def reserve(self, operation_id, amounts):
        pass

    def read_action(self, action_id):
        return {}

    def read_configuration(self):
        return {}


class FakeStore:
    def __init__(self):
        self.objects = {}

    def put(self, chunks, *, max_bytes, media_type):
        return ArtifactRef()

    def read(self, ref, *, max_bytes):
        yield b"{}"

    def inspect(self, ref):
        return True

    def retain(self, ref, *, owner):
        pass

    def release(self, ref, *, owner):
        pass


class FakeMailbox:
    def __init__(self):
        self.events = []
        self.closed = False
        self.fail_next = False

    def request(self, event, *, deadline_ms):
        self.events.append(event)
        if self.fail_next:
            self.fail_next = False
            return ControlReply(operation_id=event.operation_id, status="error",
                                error_code="INTERNAL_PROTOCOL")
        return ControlReply(operation_id=event.operation_id, status="ok")

    def drain(self, *, max_events):
        return tuple(self.events[:max_events])

    def deliver(self, reply):
        pass

    def close(self):
        self.closed = True


class FakeKernel:
    instances = 0

    def __init__(self, spec, *, partition_id, slow=False, fail_close=False):
        FakeKernel.instances += 1
        self.spec = spec
        self.partition_id = partition_id
        self.started = False
        self.closed = False
        self.aclosed = False
        self.slow = slow
        self.fail_close = fail_close
        self.instance = None
        self.pushed = []
        self._state_ref = None

    def start(self):
        self.started = True

    def push(self, port, record):
        self.pushed.append((port, record))
        if self.slow:
            time.sleep(0.2)
        yield record

    def finish(self, port):
        return iter(())

    def snapshot(self):
        return self._state_ref

    def restore(self, ref):
        self._state_ref = ref

    def close(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("kernel close failed")

    def aclose(self):
        self.aclosed = True


class FakeReader:
    def __init__(self, snapshot, split, *, cursor, records=(), eoi_after=True, slow=False):
        self.snapshot_obj = snapshot
        self.split = split
        self.cursor = cursor
        self._records = list(records)
        self._index = 0
        self._eoi = False
        self.closed = False
        self.eoi_after = eoi_after
        self.slow = slow

    def open(self, split, cursor):
        pass

    def next_record(self):
        if self.slow:
            time.sleep(0.3)
        if self._index < len(self._records):
            record = self._records[self._index]
            self._index += 1
            return SourcePoll(status="record", record=record)
        self._eoi = True
        return SourcePoll(status="eoi")

    def snapshot(self):
        return SourceCursor(split_id=self.split.split_id, next_sequence=self._index,
                            end_of_input=self._eoi)

    def close(self):
        self.closed = True


class FakeChannels:
    def __init__(self, specs, *, credit=True):
        self.specs = specs
        self.closed = False
        self.credit = credit
        self.published = []
        self.routed = []
        self._inbox = {}
        self.fail_close = False

    def consume(self, channel_id):
        return list(self._inbox.get(channel_id, ()))

    def close(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("channel close failed")


def _make_services(assignment, *, mailbox=None):
    journal = FakeJournal()
    store = FakeStore()
    services = types.SimpleNamespace(
        store=store, journal=journal, bounds=assignment.bounds, assignment=assignment,
        clock_ms=lambda: int(time.time() * 1000), check_stop=lambda: None,
        constants={}, programs={})
    return services


def _install_fakes(monkeypatch, *, kernel_slow=False, kernel_fail_close=False,
                   channel_credit=True, channel_fail_close=False, records=()):
    """Install spec-bound fake upstream modules; return a control namespace."""
    ns = types.SimpleNamespace()
    ns.kernel_slow = kernel_slow
    ns.kernel_fail_close = kernel_fail_close
    ns.channel_credit = channel_credit
    ns.channel_fail_close = channel_fail_close
    ns.records = records
    ns.reader_slow = False
    ns.make_kernel_calls = 0
    ns.opened_journal_db = False

    dto = types.ModuleType("demiflow.execution.contracts.dataflow")
    for name in ("ArtifactRef", "Bounds", "WorkerLease", "SealedExecution", "InputPort",
                 "KernelSpec", "GraphNode", "GraphEdge", "DataflowGraph", "SourceSnapshot",
                 "SourceSplit", "StageSpec", "ExchangeSpec", "ResidencyPlan", "FiniteScope",
                 "RuntimePolicy", "PhysicalDataflow", "WorkerAssignment", "SourceCursor",
                 "Record", "SourcePoll", "CutRequest", "CutContribution", "RecoveryImage",
                 "WorkerEvent", "ControlReply", "CreditLease", "RoutedRecord", "EncodedRecord",
                 "LoadedProgram", "WorkerStep"):
        setattr(dto, name, globals()[name])
    errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")
    errors.DataflowError = DataflowError
    codec = types.ModuleType("demiflow.execution.contracts.dataflow_codec")
    codec.encode_wire = lambda value, *, type_name, bounds: json.dumps(asdict(value)).encode()
    codec.decode_wire = lambda raw, *, type_name, bounds: WorkerAssignment(**json.loads(raw))

    seal = types.ModuleType("demiflow.execution.dataflow_seal")
    ns.graph = DataflowGraph()
    ns.physical = PhysicalDataflow()
    ns.sealed = SealedExecution()

    def load_dataflow(directory, action_id, *, store, bounds):
        return ns.graph, ns.physical, ns.sealed

    def verify_dataflow_program(ref, *, expected_pin, store, bounds):
        if expected_pin.get("code_pin") != ns.sealed.code_pin_ref.digest:
            raise DataflowError("CODE_MISMATCH", "pin mismatch")
        return LoadedProgram()

    seal.load_dataflow = load_dataflow
    seal.verify_dataflow_program = verify_dataflow_program

    artifacts = types.ModuleType("demiflow.execution.dataflow_artifacts")
    ns.bundle_dir = None

    def fetch_runtime_bundle(ref, *, cache, store, bounds):
        Path(cache).mkdir(parents=True, exist_ok=True)
        ns.bundle_dir = Path(cache)
        return Path(cache)

    artifacts.fetch_runtime_bundle = fetch_runtime_bundle

    services_mod = types.ModuleType("demiflow.execution.dataflow_services")

    def make_runtime_services(assignment, *, transport, cache):
        return _make_services(assignment)

    services_mod.make_runtime_services = make_runtime_services

    hosts = types.ModuleType("demiflow.execution.dataflow_hosts")

    class ControlMailbox(FakeMailbox):
        def __init__(self, *, bounds, check_stop):
            super().__init__()

    hosts.ControlMailbox = ControlMailbox

    kernels = types.ModuleType("demiflow.execution.kernels.dataflow")

    def make_kernel(spec, *, services, partition_id):
        ns.make_kernel_calls += 1
        return FakeKernel(spec, partition_id=partition_id, slow=ns.kernel_slow,
                          fail_close=ns.kernel_fail_close)

    kernels.make_kernel = make_kernel
    kernels.invoke_unary = lambda spec, record, *, instance, services: iter((record,))
    kernels.invoke_batch = lambda spec, records, *, instance, services: iter(records)
    kernels.finish_kernel = lambda kernel, *, port, services: iter(())

    sources = types.ModuleType("demiflow.execution.dataflow_sources")

    def open_source_reader(snapshot, split, *, cursor, services):
        return FakeReader(snapshot, split, cursor=cursor, records=ns.records, slow=ns.reader_slow)

    sources.open_source_reader = open_source_reader

    channels = types.ModuleType("demiflow.execution.exchange.dataflow_channels")
    ns.channels = None

    def open_channels(specs, *, services):
        ns.channels = FakeChannels(specs, credit=ns.channel_credit)
        ns.channels.fail_close = ns.channel_fail_close
        return ns.channels

    def restore_channels(specs, refs, *, services):
        ns.channels = FakeChannels(specs, credit=ns.channel_credit)
        return ns.channels

    def route_record(record, *, spec, state):
        state.routed.append(record)
        return (RoutedRecord(record=record),)

    def reserve_credit(channel_id, reader_id, *, rows, size_bytes, operation_id, state):
        if not state.credit:
            raise DataflowError("BUDGET_EXCEEDED", "no credit")
        return CreditLease(operation_id=operation_id, channel_id=channel_id, reader_id=reader_id)

    def snapshot_channels(channels, *, cut, store):
        return {spec.channel_id: ArtifactRef() for spec in channels.specs}

    def close_channels(channels, *, reason, deadline_ms):
        channels.close()

    def encode_record(record, *, bounds):
        return EncodedRecord(record_id=record.record_id)

    def publish_segment(records, *, spec, store, operation_id):
        ns.channels.published.extend(records)
        return ArtifactRef()

    channels.open_channels = open_channels
    channels.restore_channels = restore_channels
    channels.route_record = route_record
    channels.reserve_credit = reserve_credit
    channels.snapshot_channels = snapshot_channels
    channels.close_channels = close_channels
    channels.encode_record = encode_record
    channels.publish_segment = publish_segment

    bridge = types.ModuleType("demiflow.execution.exchange.dataflow_bridge")
    bridge.cut_to_barrier = lambda request, *, channel_id, sequence: {
        "cut_id": request.cut_id, "channel_id": channel_id, "sequence": sequence}

    models = types.ModuleType("demiflow.execution.dataflow_models")
    models.restore_actor_state = lambda instance, *, ref, services: None
    models.snapshot_actor_state = lambda instance, *, services: ArtifactRef()

    sinks = types.ModuleType("demiflow.execution.dataflow_sinks")
    sinks.prepare_sink_partition = lambda records, *, spec, services: ArtifactRef()

    modules = {
        "demiflow.execution.contracts.dataflow": dto,
        "demiflow.execution.contracts.dataflow_errors": errors,
        "demiflow.execution.contracts.dataflow_codec": codec,
        "demiflow.execution.dataflow_seal": seal,
        "demiflow.execution.dataflow_artifacts": artifacts,
        "demiflow.execution.dataflow_services": services_mod,
        "demiflow.execution.dataflow_hosts": hosts,
        "demiflow.execution.kernels.dataflow": kernels,
        "demiflow.execution.dataflow_sources": sources,
        "demiflow.execution.exchange.dataflow_channels": channels,
        "demiflow.execution.exchange.dataflow_bridge": bridge,
        "demiflow.execution.dataflow_models": models,
        "demiflow.execution.dataflow_sinks": sinks,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    ns.modules = modules
    return ns


@pytest.fixture
def fakes(monkeypatch):
    _assert_tree_binding()
    return _install_fakes(monkeypatch)


def _executor(fakes, assignment=None):
    from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
    assignment = assignment or WorkerAssignment()
    services = _make_services(assignment)
    mailbox = FakeMailbox()
    executor = DataflowWorkerExecutor(services=services, mailbox=mailbox)
    return executor, services, mailbox


# ---------------------------------------------------------------------------
# B001-B010 DataflowWorkerExecutor
# ---------------------------------------------------------------------------
def test_b001(fakes):
    from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
    assignment = WorkerAssignment()
    services = _make_services(assignment)
    mailbox = FakeMailbox()
    executor = DataflowWorkerExecutor(services=services, mailbox=mailbox)
    assert executor.assignment is None
    assert executor._state == "constructed"
    assert fakes.make_kernel_calls == 0
    assert services.journal.opened_db is False
    assert executor._kernels == {} and executor._readers == {}


def test_b002(fakes):
    from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    assert executor._state == "loaded"
    assert executor.assignment is not None
    # stale boot fails before business (services bound to the live boot)
    other = WorkerAssignment(lease=WorkerLease(boot_id="stale"))
    fresh, _, _ = _executor(fakes)
    with pytest.raises(DataflowError) as exc:
        fresh.load(other)
    assert exc.value.code == "FENCED"
    # wrong code pin fails before business
    fakes.sealed = SealedExecution(code_pin_ref=ArtifactRef(digest="f" * 64))
    bad, _, _ = _executor(fakes)
    with pytest.raises(DataflowError) as exc2:
        bad.load(WorkerAssignment())
    assert exc2.value.code == "CODE_MISMATCH"


def test_b003(fakes):
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    contribution = CutContribution(
        participant_id="s0:p0", boot_id="boot",
        sources=(SourceCursor(split_id="split0"),),
        operator_states={"occ0": ArtifactRef()},
        channels={"ch0": ArtifactRef()}, buffers=(ArtifactRef(),))
    image = RecoveryImage(contributions=(contribution,))
    executor.restore(image)
    assert executor._state == "restored"
    assert "split0" in executor._restored_cursors
    # missing buffer ref rejected
    executor2, _, _ = _executor(fakes)
    executor2.load(WorkerAssignment())
    bad = CutContribution(participant_id="s0:p0", boot_id="boot",
                          channels={"ch0": None}, buffers=(None,))
    with pytest.raises(DataflowError) as exc:
        executor2.restore(RecoveryImage(contributions=(bad,)))
    assert exc.value.code == "INVALID_CONTRACT"


def test_b004(fakes):
    fakes.records = (Record(record_id="r0"), Record(record_id="r1"))
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    # no activation -> no source read
    with pytest.raises(DataflowError) as exc:
        executor.step(max_records=1, deadline_ms=100)
    assert exc.value.code == "FENCED"
    executor.start()
    assert executor._state == "started"
    assert fakes.make_kernel_calls >= 1
    assert all(k.started for k in executor._kernels.values())
    # slow business read must not block control cancel (lock released in business)
    fakes.reader_slow = True
    fakes.records = (Record(record_id="slow"),)
    slow_exec, _, _ = _executor(fakes)
    slow_exec.load(WorkerAssignment())
    slow_exec.start()
    done = threading.Event()

    def run_step():
        slow_exec.step(max_records=1, deadline_ms=1000)
        done.set()

    thread = threading.Thread(target=run_step)
    thread.start()
    time.sleep(0.05)
    start = time.monotonic()
    slow_exec.cancel("test", deadline_ms=100)
    assert time.monotonic() - start < 0.15
    thread.join(timeout=2)
    slow_exec.close()


def test_b005(fakes):
    fakes.records = (Record(record_id="r0"),)
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    result = executor.step(max_records=4, deadline_ms=500)
    assert result.processed >= 1
    assert fakes.channels.published
    # no credit -> paid call not executed early
    fakes.channel_credit = False
    executor2, _, _ = _executor(fakes)
    executor2.load(WorkerAssignment())
    executor2.start()
    with pytest.raises(DataflowError) as exc:
        executor2.step(max_records=1, deadline_ms=100)
    assert exc.value.code == "BUDGET_EXCEEDED"


def test_b006(fakes):
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    request = CutRequest(cut_id="driver-cut", participants=("s0:p0",))
    executor.request_cut(request)
    assert executor._active_cut.cut_id == "driver-cut"
    barriers = [e for e in mailbox.events if e.kind == "cut"]
    assert barriers
    # worker cannot originate final cut
    with pytest.raises(DataflowError) as exc:
        executor.request_cut(CutRequest(cut_id="x", participants=("s0:p0",), final=True))
    assert exc.value.code == "INVALID_CONTRACT"
    # cut_id must be driver-produced
    with pytest.raises(DataflowError):
        executor.request_cut(CutRequest(cut_id="", participants=("s0:p0",)))


def test_b007(fakes):
    fakes.records = (Record(record_id="r0"),)
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    request = CutRequest(cut_id="cut0", participants=("s0:p0",))
    executor.request_cut(request)
    contribution = executor.snapshot(request)
    assert contribution.final is False
    assert contribution.participant_id == "s0:p0"
    assert contribution.boot_id == "boot"
    assert contribution.buffers
    # empty input side still contributes a cursor
    assert isinstance(contribution.sources, tuple)


def test_b008(fakes):
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    request = CutRequest(cut_id="cut0", participants=("s0:p0",))
    executor.request_cut(request)
    executor.snapshot(request)
    executor.accept_receipt("commit-1", cut_id="cut0")
    # duplicate receipt is idempotent (no second release)
    executor.accept_receipt("commit-1", cut_id="cut0")
    # wrong cut rejected
    with pytest.raises(DataflowError) as exc:
        executor.accept_receipt("commit-2", cut_id="unknown")
    assert exc.value.code == "INVALID_CONTRACT"


def test_b009(fakes):
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    executor._effect_refs.append(ArtifactRef())
    executor.cancel("user", deadline_ms=100)
    assert executor._state == "draining"
    assert executor._cancel_event.is_set()
    assert executor._effect_refs  # sent effects preserved
    result = executor.step(max_records=1, deadline_ms=100)
    assert result.end_of_input is True


def test_b010(fakes):
    fakes.kernel_fail_close = True
    executor, services, mailbox = _executor(fakes)
    executor.load(WorkerAssignment())
    executor.start()
    kernels = list(executor._kernels.values())
    readers = list(executor._readers.values())
    with pytest.raises(DataflowError):
        executor.close()
    # one owner close failure still closes the rest
    assert all(k.closed for k in kernels)
    assert all(r.closed for r in readers)
    assert mailbox.closed
    # idempotent second close
    executor.close()


# ---------------------------------------------------------------------------
# B011-B018 WorkerRuntime v3 path
# ---------------------------------------------------------------------------
def _v3_launch(fakes, tmp_path, *, assignment=None):
    assignment = assignment or WorkerAssignment()
    mailbox = FakeMailbox()
    return {
        "directory": str(tmp_path), "run_id": "run", "scope_id": "action",
        "allocation_id": "alloc", "boot_id": "boot", "controller_epoch": 1,
        "capabilities": ["cpu"], "grant": _grant(),
        "dataflow_v3": {"assignment": assignment, "transport": FakeMailbox(),
                        "mailbox": mailbox, "cache": str(tmp_path / "cache"),
                        "entrypoint": "dataflow_worker_v3", "code_pin": "d" * 64,
                        "bounds": asdict(assignment.bounds)},
    }


def _grant():
    return {"grant_id": "g", "run_id": "run", "attempt": 1, "fence": "fence",
            "scope_id": "action", "allocation_id": "alloc", "boot_id": "boot",
            "controller_epoch": 1, "expires_at_ms": int(time.time() * 1000) + 120000}


def _runtime(fakes, tmp_path):
    from demiflow.execution.worker.runtime import WorkerRuntime
    launch = _v3_launch(fakes, tmp_path)
    return WorkerRuntime(launch, control=lambda frame: None)


def test_b011(fakes, tmp_path):
    from demiflow.execution.worker.runtime import WorkerRuntime
    runtime = _runtime(fakes, tmp_path)
    assert runtime._v3 is True
    assert runtime.state == "REGISTERED"
    # v3 must not open a driver journal database
    assert not (tmp_path / "run.sqlite").exists()
    assert runtime.authority is None


def test_b012(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime._verify({"manifest_id": "m", "sha256": "s"})
    assert runtime.state == "REGISTERED"


def test_b013(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime.state = "VERIFIED"
    checksums = runtime._restore([{"state_id": "s", "sha256": "abc", "path": "p", "bytes": 1}])
    assert checksums == ["abc"]
    assert runtime._v3_executor is not None


def test_b014(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime.state = "READY"
    runtime._activate({"grant_ref": "g"}, int(time.time() * 1000))
    assert runtime._v3_executor._state == "started"
    assert runtime.authority is None


def test_b015(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    receipt = runtime._execute("VerifyArtifactAndCapabilities",
                               {"artifact_manifest_ref": {"manifest_id": "m", "sha256": "s"},
                                "required_capabilities": ["cpu"]},
                               int(time.time() * 1000))
    assert receipt["kind"] == "Verified"
    assert runtime.state == "VERIFIED"


def test_b016(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime.state = "READY"
    runtime._activate({"grant_ref": "g"}, int(time.time() * 1000))
    runtime.state = "ACTIVATED"
    runtime._drain({"reason": "done", "deadline_ms": 100})
    assert runtime._admit_open is False
    assert runtime._v3_executor._state == "draining"


def test_b017(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime.state = "READY"
    runtime._activate({"grant_ref": "g"}, int(time.time() * 1000))
    runtime._cancel({"reason": "stop", "deadline_ms": 100})
    assert runtime._v3_executor._cancel_event.is_set()


def test_b018(fakes, tmp_path):
    runtime = _runtime(fakes, tmp_path)
    runtime.state = "READY"
    runtime._activate({"grant_ref": "g"}, int(time.time() * 1000))
    runtime.close()
    assert runtime._v3_executor is None
    runtime.close()


# ---------------------------------------------------------------------------
# B019-B020 bootstrap v3 entry
# ---------------------------------------------------------------------------
def test_b019(fakes, tmp_path):
    from demiflow.execution.worker.bootstrap import _validate_v3_launch
    good = {"dataflow_v3": {"entrypoint": "dataflow_worker_v3", "code_pin": "d" * 64,
                            "assignment": {}}}
    _validate_v3_launch(good)
    with pytest.raises(DataflowError) as exc:
        _validate_v3_launch({"dataflow_v3": {"entrypoint": "wrong", "code_pin": "d" * 64,
                                             "assignment": {}}})
    assert exc.value.code == "INVALID_CONTRACT"
    with pytest.raises(DataflowError) as exc2:
        _validate_v3_launch({"dataflow_v3": {"entrypoint": "dataflow_worker_v3",
                                             "code_pin": "short", "assignment": {}}})
    assert exc2.value.code == "CODE_MISMATCH"


def test_b020(fakes, tmp_path):
    from demiflow.execution.worker.bootstrap import _run_v3_worker
    launch = _v3_launch(fakes, tmp_path)
    driver_sock, child_sock = socket.socketpair()
    result = {}

    def worker():
        try:
            result["runtime"] = _run_v3_worker(launch, child_sock)
        except BaseException as exc:
            result["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    # driver reads the Register envelope then sends Cancel
    from demiflow.execution.worker.bootstrap import recv_message, send_message
    register = recv_message(driver_sock)
    assert register["kind"] == "Register"
    cancel = {
        "protocol_version": {"major": 1, "minor": 0}, "kind": "Cancel",
        "scope_id": "action", "run_id": "run", "operation_id": uuid.uuid4().hex,
        "controller_epoch": 1, "authority_ref": "g", "allocation_id": "alloc",
        "boot_id": "boot", "payload": {"operation_id": uuid.uuid4().hex, "reason": "test"},
    }
    send_message(driver_sock, cancel)
    reply = recv_message(driver_sock)
    assert reply["response_to"] == cancel["operation_id"]
    thread.join(timeout=5)
    driver_sock.close()
    assert "error" not in result, result.get("error")
    assert result["runtime"].state == "RETIRED"