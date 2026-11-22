"""Design-only v3 declarations. Never import this file into production.
All records frozen + recursively immutable; wire schemas in CONTRACTS.md.
Existing types (SourcePlan, LogicalPlan, RunAction, ResourceProfile, etc.) retain
repository ownership. Json/Row are deliberately distinct from code-bearing refs.
"""
from typing import Protocol, Literal, Mapping, Iterator, AsyncIterator, Callable, Sequence
from pathlib import Path
from dataclasses import dataclass
from demiflow.resources.contracts import ResourceProfile, WorkerLaunchSpec
from demiflow.data.sources import SourcePlan
from demiflow.data.plan import LogicalPlan, CallableSpec
from demiflow.execution.run_context import RunAction
from demiflow.data.datasink import WriteResult

Json = None | bool | int | float | str | list['Json'] | dict[str, 'Json']
Row = Mapping[str, object]  # typed business payload, never used as a wire identity
Provider = Literal['local', 'ray_core']
Mode = Literal['memory_incremental', 'durable_incremental', 'finite_sealed']
Recovery = Literal['checkpoint', 'final_commit', 'delivery_only', 'attempt_only']

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
class ActionRequest:
    kind: str
    source: SourcePlan
    plan: LogicalPlan
    arguments: Mapping[str, Json]
    sink: Datasink | None
    programs: tuple[ActionProgram, ...]
    provider: Provider
    action: RunAction

@dataclass(frozen=True)
class InputPort:
    name: str
    requirement: Literal['incremental', 'finite_complete']
    ordering: Literal['none', 'source_partition', 'key_sequence', 'global']
    schema_ref: ArtifactRef | None

@dataclass(frozen=True)
class KernelSpec:
    kind: str
    implementation_ref: ArtifactRef | None
    configuration: Mapping[str, Json]
    inputs: tuple[InputPort, ...]
    state: Literal['none', 'auto_json', 'adapter', 'nonrecoverable']
    effect: Literal['pure', 'journaled', 'unknown_replay_forbidden']
    resource_role: Literal['cpu', 'gpu', 'remote_api', 'io']
    profile: ResourceProfile
    output_bound_bytes: int
    mergeable: bool

@dataclass(frozen=True)
class GraphNode:
    occurrence_id: str
    semantic_id: str
    kind: Literal['source', 'transform', 'relational', 'terminal']
    kernel: KernelSpec
    program_ref: ArtifactRef | None
    shared_token: str

@dataclass(frozen=True)
class GraphEdge:
    edge_id: str
    producer: str
    output_port: str
    consumer: str
    input_port: str

@dataclass(frozen=True)
class DataflowGraph:
    schema: Literal['demiflow.dataflow_graph.v3']
    action_id: str
    dag_id: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    terminals: tuple[str, ...]
    sources: tuple[str, ...]

@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str
    kind: str
    immutable_identity: Mapping[str, Json]
    finite: bool
    replay: Literal['seek', 'durable_spool', 'lease_queue', 'attempt_only']
    manifest_ref: ArtifactRef | None

@dataclass(frozen=True)
class SourceSplit:
    split_id: str
    source_id: str
    locator: Mapping[str, Json]
    start_cursor: Mapping[str, Json]
    estimated_bytes: int | None
    required_domain: str

@dataclass(frozen=True)
class SourceCursor:
    split_id: str
    offset: Mapping[str, Json]
    next_sequence: int
    end_of_input: bool
    lease_ref: ArtifactRef | None
    pending_refs: tuple[ArtifactRef, ...]

@dataclass(frozen=True)
class Record:
    record_id: str
    source_position: Mapping[str, Json]
    payload_ref: ArtifactRef | None
    # 2026-11-17 compatibility amendment: closed typed business values;
    # Row remains the mapping contract for named-column operations.
    inline: object | None
    encoded_bytes: int
    sequence: int

@dataclass(frozen=True)
class StageSpec:
    stage_id: str
    occurrences: tuple[str, ...]
    partitions: tuple[str, ...]
    profile: ResourceProfile
    replica_min: int
    replica_target: int
    replica_max: int
    resident_bytes: int
    output_bytes: int
    state_bytes: int
    kernel_refs: tuple[ArtifactRef, ...]

@dataclass(frozen=True)
class ExchangeSpec:
    channel_id: str
    edge_id: str
    producer_partitions: tuple[str, ...]
    consumer_partitions: tuple[str, ...]
    mode: Mode
    routing: Literal['forward', 'hash', 'rebalance', 'broadcast', 'target']
    keys: tuple[str, ...]
    scope_id: str | None
    route_epoch: int
    credit_rows: int
    credit_bytes: int
    storage_bytes: int
    read_domain: str
    reason: str

@dataclass(frozen=True)
class RuntimePolicy:
    provider: Provider
    durability: Literal['ephemeral', 'durable']
    default_partitions: int
    max_replicas: int
    fusion: bool
    checkpoint_rows: int
    checkpoint_interval_ms: int
    fairness_quantum_bytes: int
    control_timeout_ms: int
    store_id: str
    required_recovery: Recovery

@dataclass(frozen=True)
class PhysicalDataflow:
    schema: Literal['demiflow.physical_dataflow.v3']
    dag_id: str
    plan_id: str
    plan_revision: int
    stages: tuple[StageSpec, ...]
    exchanges: tuple[ExchangeSpec, ...]
    residency: ResidencyPlan
    scopes: tuple[FiniteScope, ...]
    sources: tuple[SourceSnapshot, ...]
    splits: tuple[SourceSplit, ...]
    policy: RuntimePolicy
    bounds: Bounds
    recovery: Recovery
    limitations: tuple[str, ...]
    constants_ref: ArtifactRef

@dataclass(frozen=True)
class SealedExecution:
    logical_ref: ArtifactRef
    physical_ref: ArtifactRef
    programs: tuple[ArtifactRef, ...]
    code_pin_ref: ArtifactRef
    identity_bridge_ref: ArtifactRef
    source_refs: tuple[ArtifactRef, ...]
    constants_ref: ArtifactRef

@dataclass(frozen=True)
class ReadyWork:
    stage_id: str
    partition_id: str
    input_refs: tuple[ArtifactRef, ...]
    resident_bytes: int
    operation_id: str

@dataclass(frozen=True)
class WorkerLease:
    allocation_id: str
    boot_id: str
    stage_id: str
    partition_ids: tuple[str, ...]
    host_id: str
    endpoint_ref: str
    grant_ref: ArtifactRef
    expires_at_ms: int

@dataclass(frozen=True)
class WorkerAssignment:
    schema: Literal['demiflow.dataflow_assignment.v3']
    run_id: str
    action_id: str
    attempt: int
    fence: str
    plan_id: str
    plan_revision: int
    lease: WorkerLease
    sealed: SealedExecution
    stages: tuple[StageSpec, ...]
    inputs: tuple[ExchangeSpec, ...]
    outputs: tuple[ExchangeSpec, ...]
    source_splits: tuple[SourceSplit, ...]
    restore_refs: tuple[ArtifactRef, ...]
    bounds: Bounds

@dataclass(frozen=True)
class CutRequest:
    cut_id: str
    sequence: int
    plan_revision: int
    route_epoch: int
    participants: tuple[str, ...]
    final: bool

@dataclass(frozen=True)
class CutContribution:
    cut_id: str
    participant_id: str
    boot_id: str
    sources: tuple[SourceCursor, ...]
    operator_states: Mapping[str, ArtifactRef]
    channels: Mapping[str, Mapping[str, Json]]
    buffers: tuple[ArtifactRef, ...]
    effects: tuple[ArtifactRef, ...]
    sinks: Mapping[str, ArtifactRef]
    final: bool

@dataclass(frozen=True)
class RecoveryImage:
    commit_id: str
    cut: CutRequest
    contributions: tuple[CutContribution, ...]
    confirmed_sinks: Mapping[str, ArtifactRef]
    source_vector: tuple[SourceCursor, ...]

@dataclass(frozen=True)
class EffectCommand:
    operation_id: str
    kind: Literal['lookup', 'reserve', 'mark_sent', 'confirm', 'unknown', 'release_unsent']
    effect_id: str
    actor_id: str
    request_digest: str
    payload_ref: ArtifactRef | None
    budget: Mapping[str, int]

@dataclass(frozen=True)
class EffectReply:
    operation_id: str
    status: Literal['absent', 'reserved', 'sent', 'confirmed', 'unknown', 'unsent']
    result_ref: ArtifactRef | None
    receipt_ref: ArtifactRef | None

@dataclass(frozen=True)
class WorkerEvent:
    schema: Literal['demiflow.dataflow_event.v3']
    kind: Literal['ready', 'progress', 'cut', 'effect', 'artifact', 'reserve', 'read_action', 'read_configuration', 'check_grant', 'queue', 'native_request', 'failure', 'eoi', 'closed']
    operation_id: str
    run_id: str
    attempt: int
    fence: str
    allocation_id: str
    boot_id: str
    plan_revision: int
    scope_id: str
    controller_epoch: int
    payload_ref: ArtifactRef | None
    payload: Mapping[str, Json] | None

@dataclass(frozen=True)
class TerminalResult:
    kind: str
    rows: int | None
    scalar: Json
    result_ref: ArtifactRef | None
    write_result: WriteResult | None
    complete: bool
    completion: Literal["source_exhausted", "terminal_satisfied", "cancelled", "incomplete"]

@dataclass(frozen=True)
class CapabilityReport:
    supported: bool
    reasons: tuple[str, ...]
    recovery: Recovery
    required_domains: tuple[str, ...]
    required_profiles: tuple[ResourceProfile, ...]

@dataclass(frozen=True)
class HostDescriptor:
    host_id: str
    provider: Provider
    runtime_revision: str
    store_domains: tuple[str, ...]
    logical_resources: Mapping[str, float]
    isolation_capabilities: tuple[str, ...]

@dataclass(frozen=True)
class HistoricalBinding:
    schema_version: str
    original_plan_ref: ArtifactRef
    original_code_pin_ref: ArtifactRef
    compatible: bool
    reason: str | None
    restore_image: RecoveryImage | None

class ArtifactStore(Protocol):
    def put(self, chunks: Iterator[bytes], *, max_bytes: int, media_type: str) -> ArtifactRef: ...
    def read(self, ref: ArtifactRef, *, max_bytes: int) -> Iterator[bytes]: ...
    def read_into(self, ref: ArtifactRef, destination: bytearray) -> None: ...
    def inspect(self, ref: ArtifactRef) -> bool: ...
    def retain(self, ref: ArtifactRef, *, owner: str) -> None: ...
    def release(self, ref: ArtifactRef, *, owner: str) -> None: ...

class SourceReader(Protocol):
    def open(self, split: SourceSplit, cursor: SourceCursor | None) -> None: ...
    def next_record(self) -> SourcePoll: ...
    def snapshot(self) -> SourceCursor: ...
    def close(self) -> None: ...

class Kernel(Protocol):
    def start(self) -> None: ...
    def push(self, port: str, record: Record) -> Iterator[Record]: ...
    def finish(self, port: str) -> Iterator[Record]: ...
    def snapshot(self) -> ArtifactRef | None: ...
    def restore(self, ref: ArtifactRef | None) -> None: ...
    def close(self) -> None: ...

class WorkerJournalPort(Protocol):
    def native_request(self, command: NativeJournalCommand) -> NativeJournalReply: ...
    def queue(self, command: QueueCommand) -> QueueReply: ...
    def effect(self, command: EffectCommand) -> EffectReply: ...
    def artifact(self, command: ArtifactCommand) -> ArtifactReply: ...
    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None: ...
    def read_action(self, action_id: str) -> Mapping[str, Json]: ...
    def read_configuration(self) -> Mapping[str, Json]: ...
    def check_grant(self) -> None: ...

class HostEndpoint(Protocol):
    def deliver_reply(self, reply: ControlReply) -> None: ...
    def activate(self, assignment: WorkerAssignment) -> None: ...
    def poll(self, *, max_events: int, deadline_ms: int) -> tuple[WorkerEvent, ...]: ...
    def request_cut(self, request: CutRequest) -> None: ...
    def cancel(self, reason: str, *, deadline_ms: int) -> None: ...
    def closed(self) -> bool: ...

# Internal orchestration values; not extra persisted authorities.
from demiflow.data.plan import LogicalOp
from demiflow.data.datasource import Datasource, ReadTask
from demiflow.data.datasink import Datasink
from demiflow.execution.executors.base import DatasetExecutor
from demiflow.resources.manager import ResourceManager
from demiflow.execution.recovery_contract import SourcePosition

@dataclass(frozen=True)
class PlanInputBinding:
    port: str
    source: SourcePlan
    plan: LogicalPlan
    path: tuple[str, ...]

@dataclass(frozen=True)
class OperatorDefinition:
    kind: str
    compiler_symbol: str
    kernel_symbol: str
    inputs: tuple[str, ...]

@dataclass(frozen=True)
class SourceDefinition:
    kind: str
    reader_symbol: str
    cursor_fields: tuple[str, ...]

@dataclass(frozen=True)
class TerminalDefinition:
    kind: str
    adapter_symbol: str
    result_kind: str

@dataclass(frozen=True)
class ResidencyPlan:
    simultaneous_groups: tuple[tuple[str, ...], ...]
    control_reserved_bytes: int
    consumer_reserved_slots: int
    required_spill_bytes: int

@dataclass(frozen=True)
class EncodedRecord:
    record_id: str
    encoding: str
    data: bytes | None
    ref: ArtifactRef | None
    encoded_bytes: int
    decoded_upper_bytes: int
    sequence: int

@dataclass(frozen=True)
class RoutedRecord:
    reader_id: str
    delivery_id: str
    record: Record

@dataclass(frozen=True)
class ProducerCompletion:
    partition_id: str
    generation: int
    status: Literal['succeeded', 'failed', 'cancelled', 'unknown']
    last_sequence: int
    manifest_ref: ArtifactRef

@dataclass(frozen=True)
class CreditLease:
    operation_id: str
    channel_id: str
    reader_id: str
    rows: int
    size_bytes: int

@dataclass(frozen=True)
class AggregateState:
    kind: str
    schema_version: str
    state_ref: ArtifactRef
    count: int

@dataclass(frozen=True)
class DomainProof:
    host_id: str
    store_id: str
    challenge_digest: str
    namespace_digest: str
    durable: bool

@dataclass(frozen=True)
class DataLocation:
    domain: str
    temporary_locator: str | None
    durable_ref: ArtifactRef

@dataclass(frozen=True)
class WorkerStep:
    processed: int
    blocked: bool
    end_of_input: bool
    events: tuple[WorkerEvent, ...]

@dataclass(frozen=True)
class ExecutionStatus:
    state: Literal['preparing', 'running', 'draining', 'succeeded', 'failed', 'cancelled']
    pending_events: int
    confirmed_rows: int

@dataclass(frozen=True)
class NodeObservation:
    occurrence_id: str
    attempt: int
    plan_id: str
    observed: bool
    metrics: Mapping[str, Json]

@dataclass(frozen=True)
class RuntimeServices:
    store: ArtifactStore
    journal: WorkerJournalPort
    bounds: Bounds
    assignment: WorkerAssignment
    clock_ms: Callable[[], int]
    check_stop: Callable[[], None]
    constants: Mapping[str, Json]
    programs: Mapping[str, LoadedProgram]

@dataclass(frozen=True)
class DriverServices:
    action: RunAction
    journal_service: DriverAuthority
    store: ArtifactStore
    resources: ResourceManager
    policy: RuntimePolicy
    bounds: Bounds
    hosts: tuple[HostDescriptor, ...]
    clock_ms: Callable[[], int]

class ControlTransport(Protocol):
    def request(self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply: ...

class ActionHandle(Protocol):
    def rows(self) -> Iterator[Record]: ...
    def result(self) -> TerminalResult: ...
    def close(self) -> None: ...

class CallableInstance(Protocol):
    def __call__(self, row: object) -> object: ...

class NativeEffectTransport(Protocol):
    def lookup(self, command: EffectCommand) -> EffectReply: ...
    def submit(self, command: EffectCommand) -> EffectReply: ...

class MaterializedHandle(Protocol):
    def num_blocks(self) -> int: ...
    def rows(self) -> Iterator[object]: ...
    def release(self) -> None: ...

class ChannelSet(Protocol):
    def close(self) -> None: ...

class RoutingState(Protocol):
    def next_ordinal(self, channel_id: str) -> int: ...

@dataclass(frozen=True)
class SourcePoll:
    status: Literal['record', 'pending', 'eoi']
    record: Record | None
    retry_after_ms: int

@dataclass(frozen=True)
class ScheduleChoice:
    work: ReadyWork | None
    retry_after_ms: int
    reason: str | None

@dataclass(frozen=True)
class ArtifactCommand:
    operation_id: str
    kind: Literal['pin', 'unpin', 'inspect', 'reserve_staging']
    ref: ArtifactRef | None
    owner: str
    size_bytes: int

@dataclass(frozen=True)
class ArtifactReply:
    operation_id: str
    present: bool | None
    charged_bytes: int

@dataclass(frozen=True)
class ControlReply:
    operation_id: str
    status: Literal['ok', 'error', 'pending']
    payload: Mapping[str, Json] | None
    payload_ref: ArtifactRef | None
    error_code: str | None

class SocketConnection(Protocol):
    def send_bytes(self, value: bytes) -> None: ...
    def recv_bytes(self, maxlength: int) -> bytes: ...
    def poll(self, timeout: float) -> bool: ...
    def close(self) -> None: ...

@dataclass(frozen=True)
class NativeOperation(LogicalOp):
    kind: str
    configuration: Mapping[str, Json]
    bindings: Mapping[str, Json]
    factory: CallableSpec

@dataclass(frozen=True)
class FiniteScope:
    scope_id: str
    generation: int
    membership: tuple[str, ...]
    completion_condition: Literal['input_eoi', 'terminal_limit', 'queue_drain_declared']

@dataclass(frozen=True)
class ProgramDeclaration:
    occurrence_id: str
    callable_spec: CallableSpec | None
    source_spec: SourcePlan | None
    action_program: ActionProgram | None
    code_identity: Mapping[str, Json]

@dataclass(frozen=True)
class CompiledDataflow:
    graph: DataflowGraph
    declarations: tuple[ProgramDeclaration, ...]

class DataflowError(Exception):
    code: str
    retryable: bool
    context: Mapping[str, Json]

@dataclass(frozen=True)
class QueueBinding:
    action_id: str
    source_id: str | None
    mode: Literal['sink', 'explicit', 'raw_fetch']
    contract_digest: str
    generation: str | None
    pool: str | None
    source_occurrence: str | None
    fetch_occurrence: str
    final_occurrence: str | None

@dataclass(frozen=True)
class QueueCommand:
    operation_id: str
    operation: Literal['poll', 'acquire', 'finalize', 'status']
    binding: QueueBinding
    record_id: str | None
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None

@dataclass(frozen=True)
class QueueReply:
    operation_id: str
    status: Literal['record', 'pending', 'eoi', 'confirmed', 'unknown']
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None
    native_fact_ref: ArtifactRef | None
    retry_after_ms: int

@dataclass(frozen=True)
class CompletedControl:
    allocation_id: str
    boot_id: str
    reply: ControlReply

class WorkerControlMailbox(Protocol):
    def request(self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply: ...
    def drain(self, *, max_events: int) -> tuple[WorkerEvent, ...]: ...
    def deliver(self, reply: ControlReply) -> None: ...
    def close(self) -> None: ...

@dataclass(frozen=True)
class NativeJournalCommand:
    operation_id: str
    operation: Literal['lookup', 'reserve', 'reserve_http_retry', 'response', 'failed', 'references', 'media_references', 'bind_membership']
    action_id: str
    node_id: str
    request_key: str
    request_ref: ArtifactRef
    value_ref: ArtifactRef | None
    reservation: tuple[str, Mapping[str, tuple[int, int]]] | None
    options: Mapping[str, Json]

@dataclass(frozen=True)
class NativeJournalReply:
    operation_id: str
    value: Json
    value_ref: ArtifactRef | None

@dataclass(frozen=True)
class NativeRunView:
    limits: Mapping[str, Json]
    fence: str
    check_stop: Callable[[], None]

@dataclass(frozen=True)
class NativeActionView:
    action_id: str
    context: NativeRunView
    object_sinks: Mapping[str, object]
    native_image_effects: bool

@dataclass(frozen=True)
class LoadedProgram:
    kind: Literal['source', 'callable', 'aggregate', 'sink', 'native']
    value: object
    digest: str
    load_private: Callable[[], object] | None = None

@dataclass(frozen=True)
class ActionProgram:
    name: str
    kind: Literal['aggregate', 'sink']
    value: object

class ArtifactAuthorityPort(Protocol):
    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None: ...
    def artifact(self, command: ArtifactCommand) -> ArtifactReply: ...

class DriverAuthority(Protocol):
    def submit(self, event: WorkerEvent) -> bool: ...
    def poll_replies(self, *, max_replies: int) -> tuple[CompletedControl, ...]: ...
    def close(self, *, deadline_ms: int) -> None: ...
    def bind_queue(self, service: object) -> None: ...
