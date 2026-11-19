"""In-process SDK requests and host port; no execution owner is imported here.

These requests carry the original declarations, not a persisted plan format.
Only the selected executor interprets them and owns an action's lifetime.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping, Protocol, TYPE_CHECKING, Iterator, TypeAlias, overload

if TYPE_CHECKING:
    from .aggregate import AggregateFnV2
    from .datasink import Datasink
    from .native_options import NativeOptions
    from .plan import LogicalPlan
    from .sources import SourcePlan
    from ..lance.model import LanceWriteSpec, LanceWriteReceipt
    from ..execution.contracts.dataflow import MaterializedHandle
    from ..execution.stream import StreamStats
    from .stats import ExecutionMetadata


@dataclass(frozen=True)
class InputSpec:
    kind: Literal['public_items', 'items', 'arrow', 'numpy', 'pandas']
    value: Any
    override_num_blocks: int | None = None


@dataclass(frozen=True)
class _DatasetAction:
    source: SourcePlan
    plan: LogicalPlan


@dataclass(frozen=True)
class Count(_DatasetAction):
    pass


@dataclass(frozen=True)
class Materialize(_DatasetAction):
    pass


@dataclass(frozen=True)
class Take(_DatasetAction):
    limit: int


@dataclass(frozen=True)
class TakeAll(_DatasetAction):
    limit: int | None
    original_limit: int | None


@dataclass(frozen=True)
class TakeBatch(_DatasetAction):
    batch_size: int
    batch_format: str | None


@dataclass(frozen=True)
class RequireColumns(_DatasetAction):
    columns: list[str]
    exact: bool


@dataclass(frozen=True)
class CollectSummary(_DatasetAction):
    max_rows: int
    max_bytes: int


@dataclass(frozen=True)
class Aggregate(_DatasetAction):
    aggregates: tuple[AggregateFnV2, ...]


@dataclass(frozen=True)
class IterRows(_DatasetAction):
    pass


@dataclass(frozen=True)
class IterBatches(_DatasetAction):
    prefetch_batches: int
    batch_size: int | None
    batch_format: str | None
    normalized_format: str
    drop_last: bool


@dataclass(frozen=True)
class Schema(_DatasetAction):
    fetch_if_missing: bool


@dataclass(frozen=True)
class Columns(_DatasetAction):
    fetch_if_missing: bool


@dataclass(frozen=True)
class SizeBytes(_DatasetAction):
    pass


@dataclass(frozen=True)
class WriteDatasink(_DatasetAction):
    sink: Datasink
    native_options: NativeOptions | None


@dataclass(frozen=True)
class WriteLance(_DatasetAction):
    spec: LanceWriteSpec
    native_options: NativeOptions | None


@dataclass(frozen=True)
class WriteFile(_DatasetAction):
    format: Literal['parquet', 'json', 'csv']
    path: str
    options: Mapping[str, Any]
    native_options: NativeOptions | None


@dataclass(frozen=True)
class RunStream(_DatasetAction):
    on_progress: Callable[..., Any] | None
    on_drain: Callable[..., Any] | None
    log_every: int
    cancellation: Any
    stall_timeout: float | None
    stop_file: Any
    source_batch_size: int | None


ActionSpec = (
    Count | Materialize | Take | TakeAll | TakeBatch | RequireColumns |
    CollectSummary | Aggregate | IterRows | IterBatches | Schema | Columns |
    SizeBytes | WriteDatasink | WriteLance | WriteFile | RunStream
)


# Native schema, batch and aggregate values retain their backend-defined types.
# The overloads retain precise returns for actions with a common value contract.
ActionResult: TypeAlias = "int | list[object] | list[dict[str, Any]] | Iterator[object] | MaterializedHandle | LanceWriteReceipt | StreamStats | object | None"


class ExecutionPort(Protocol):
    """Only the host capabilities actually consumed by SDK assembly."""
    @overload
    def execute(self, request: Count | SizeBytes) -> int: ...
    @overload
    def execute(self, request: Take | TakeAll) -> list[object]: ...
    @overload
    def execute(self, request: CollectSummary) -> list[dict[str, Any]]: ...
    @overload
    def execute(self, request: RequireColumns | WriteDatasink | WriteFile) -> None: ...
    @overload
    def execute(self, request: Columns) -> list[str] | None: ...
    @overload
    def execute(self, request: Materialize) -> MaterializedHandle: ...
    @overload
    def execute(self, request: IterRows | IterBatches) -> Iterator[object]: ...
    @overload
    def execute(self, request: WriteLance) -> LanceWriteReceipt: ...
    @overload
    def execute(self, request: RunStream) -> StreamStats: ...
    @overload
    def execute(self, request: TakeBatch | Schema | Aggregate) -> Any: ...
    def execute(self, request: ActionSpec) -> ActionResult: ...
    def prepare_input(self, request: InputSpec) -> MaterializedHandle: ...
    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any: ...
    def columns(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool) -> list[str] | None: ...
    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str: ...
    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None: ...
    def operator_llm_usage(self) -> dict[str, int]: ...
    def close(self) -> None: ...



class PlanView(Protocol):
    """In-process rebinding view; persistent plans still hold the original Dataset."""
    _source: SourcePlan
    _plan: LogicalPlan

    def replace_plan(self, source: SourcePlan, plan: LogicalPlan) -> PlanView: ...


@dataclass(frozen=True)
class CheckpointReadBinding:
    source: SourcePlan
    plan: LogicalPlan
    reference: Mapping[str, str | int] | None

# One context slot shared by SDK assembly and Pipeline drivers. No default host
# is constructed here: selection remains the existing public assembly's job.
_current_executor: ContextVar[ExecutionPort | None] = ContextVar('demiflow_dataset_executor', default=None)


@contextmanager
def _use_executor(executor: ExecutionPort):
    token = _current_executor.set(executor)
    try:
        yield
    finally:
        _current_executor.reset(token)


def normalize_batch_format(value: str | None) -> str:
    """The SDK and its action adapter use the same original format check."""
    formats = {'default', 'numpy', 'pandas', 'pyarrow'}
    normalized = 'default' if value is None else str(value)
    if normalized not in formats:
        raise ValueError(f'batch_format must be one of {sorted(formats)}')
    return normalized
