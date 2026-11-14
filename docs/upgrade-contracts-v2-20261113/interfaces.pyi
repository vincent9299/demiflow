"""Draft under revision; dispatch_ready=False. See review-20261114/CRITICAL-REVISION.md. Not a runtime module."""
from dataclasses import dataclass
from typing import Any, Iterator, Literal, Mapping, TypedDict, ContextManager
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import SourcePlan
from demiflow.execution.run_context import RunContext, RunAction

JSONValue = None | bool | int | float | str | list['JSONValue'] | dict[str,'JSONValue']
RecoveryGuarantee = Literal['native_checkpoint','final_commit','delivery_only','attempt_only']
BufferKind = Literal['fixed_batch','concurrent_batch','timed_batch','grouped','prepend']
CheckpointStageKind = Literal['fixed_batch','concurrent_batch','timed_batch','grouped','prepend','native_row_local']
RuntimeValue = Any  # bounded existing payload domain; not arbitrary untrusted objects
class ArtifactRef(TypedDict):
    path: str
    sha256: str
    bytes: int
class CodeSourceRef(ArtifactRef):
    source: str
class ProgramRef(ArtifactRef):
    code_pin_digest: str
    code_sources: list[CodeSourceRef]
class ManifestRef(TypedDict):
    manifest_id: str
    sha256: str
class SealedOperator(TypedDict):
    logical_node_id: str
    occurrence_id: str
class OperatorBinding(TypedDict):
    program_index: int
    stage_id: str
class ProgramBinding(TypedDict):
    program_id: str
    input_path: list[str | int]
    plan_ref: ProgramRef
    source_identity_ref: str
    sealed_operators: list[SealedOperator]
    operator_bindings: list[OperatorBinding]
@dataclass(frozen=True)
class ProgramSlice:
    program_id: str
    input_path: tuple[str | int, ...]
    source: SourcePlan
    plan: LogicalPlan
    sealed_operators: tuple[Mapping[str,str], ...]
    operator_bindings: tuple[Mapping[str,int | str], ...]
@dataclass(frozen=True)
class ActionDecision:
    action_kind: str
    backend: Literal['local_pull','local_stream','datafusion','arrow_batch','lance_scan']
    route: Literal['pull','stream_delivery','stream_materialize','native_relational','arrow_batch','lance_scan','queue_stream','static_graph']
    recovery: RecoveryGuarantee
@dataclass(frozen=True)
class StatePolicy:
    mode: Literal['function','factory','hooks','auto_json','unsupported']
    ownership: Literal['none','partitionable','shared_colocated']
    adapter_present: bool
    reason: str | None
class AutoState(TypedDict):
    schema: Literal['demiflow.auto_state.v1']
    fields: dict[str,JSONValue]
class ArtifactManifestRef(ArtifactRef):
    format: Literal['paged-artifacts-v1']
class BlockDescriptor(TypedDict):
    blocks: list[ArtifactRef] | ArtifactManifestRef
    rows: int
class StageBufferSnapshot:
    """UNFROZEN: pending cut/slot payload proof; not an implementable wire schema.
    Old fixed-batch {batch_ordinal,data} and native row-local formats stay intact.
    """
    ...
@dataclass(frozen=True)
class ExecutionPlacement:
    execution_id: str
    placement: Literal['driver','worker']
    group_id: str | None
    backend: Literal['local_stream','local_pull','datafusion','arrow_batch','lance_scan']
    stage_ids: tuple[str,...]
    profile: Mapping[str,Any]
    programs: tuple[Mapping[str,Any],...]
    stage_limits: Mapping[str,Mapping[str,int]]
    recovery: RecoveryGuarantee
# Existing four layout classes are retained exactly; ExecutionLayout adds only:
# executions: tuple[ExecutionPlacement,...] = ()
# Existing declarations/constructors are captured in class-state.json.

class StageBufferAdapter:
    stage: Any
    action: RunAction
    stage_key: str
    kind: str
    closed: bool
    def __init__(self, stage, *, action: RunAction, stage_key: str) -> None: ...
    async def align(self, barrier, pending) -> StageBufferSnapshot: ...
    def restore(self, snapshot: StageBufferSnapshot) -> None: ...
    def close(self) -> None: ...
class NativeEmbeddingRecovery:
    actor: Any
    action: RunAction
    node_id: str
    journal: Any  # existing RunPromptJournal, borrowed
    inflight: dict[str, Any]  # actual executing Tasks, not effect/unknown state
    semantic_owners: tuple[Any,...]
    row_local: bool
    copy_inputs: bool
    def __init__(self, actor, action: RunAction, node_id: str) -> None: ...
    def bind_actor(self) -> None: ...
    def scope_request(self, request): ...  # existing RequestRecord
    async def drain(self, *, timeout_s: float) -> None: ...
def admit_action_plan(source: SourcePlan, plan: LogicalPlan, *, action_kind: str, backend: str='local', sink_spec=None, available_backends: tuple[str,...]=()) -> ActionDecision: ...
def classify_operator_state(target, *, phase: Literal['declaration','instance'], ownership: str='none') -> StatePolicy: ...
def snapshot_auto_state(owner, *, max_bytes: int) -> AutoState: ...
def restore_auto_state(owner, state: AutoState, *, max_bytes: int) -> None: ...
def make_stage_buffer_adapter(stage, *, action: RunAction, stage_key: str, restored=None) -> StageBufferAdapter | None: ...
def compile_dataset_logical_dag(*, action_id: str, source: SourcePlan, plan: LogicalPlan, terminal_kind: str, sink_ids) -> tuple[dict,tuple[ProgramSlice,...]]: ...
def stage_native_programs(action: RunAction, program_slices: tuple[ProgramSlice,...]) -> tuple[ProgramBinding,...]: ...
def verify_native_programs(source: SourcePlan, plan: LogicalPlan, *, programs, action_id: str) -> None: ...
def compile_native_topology(action: RunAction, source: SourcePlan, plan: LogicalPlan, *, decision: ActionDecision) -> tuple[dict,dict,ArtifactRef]: ...
def load_native_execution(run: RunContext, action_id: str, *, execution_id: str) -> tuple[Any,ExecutionPlacement]: ...
def execute_native_plan(executor, source: SourcePlan, plan: LogicalPlan, *, layout, execution_id: str, arrow: bool=False, sink_schema=None) -> Iterator[RuntimeValue]: ...

# New second-review declarations; callers are assigned in plan.json.
def native_request_budget(run: RunContext, *, node_id: str, max_requests: int | None) -> ContextManager[str]: ...
def source_callable_roots(source: SourcePlan) -> tuple[Any, ...]: ...
def resolve_prefetch_credit(inflight_slots: int, *, inflight_bytes: int, credit_budget: Mapping[str,int], credit_explicit: bool) -> dict: ...
# RecoveryGuarantee is descriptive execution metadata, not a public mode switch.
# queue proof field "final_commit" is a different namespace; no value conversion.
