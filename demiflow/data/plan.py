"""Backend-neutral logical plan for demiflow datasets.

The plan is immutable and contains no execution logic. Backends compile the
same source + plan into their native execution model.
"""

from __future__ import annotations

import inspect
import copy
import uuid
from ..operator_llm.model import PromptPack
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple
from .native_options import NativeOptions


@dataclass(frozen=True)
class CallableSpec:
    """A function, callable instance, or callable class plus construction args."""

    target: Callable[..., Any]
    constructor_args: Tuple[Any, ...] = ()
    constructor_kwargs: Mapping[str, Any] = field(default_factory=dict)
    call_args: Tuple[Any, ...] = ()
    call_kwargs: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        target: Callable[..., Any],
        *,
        fn_args: Optional[Sequence[Any]] = None,
        fn_kwargs: Optional[Mapping[str, Any]] = None,
        fn_constructor_args: Optional[Sequence[Any]] = None,
        fn_constructor_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> "CallableSpec":
        if not callable(target):
            raise TypeError("Dataset transform requires a callable")
        if not inspect.isclass(target) and (fn_constructor_args or fn_constructor_kwargs):
            raise TypeError("fn_constructor_args/kwargs require a callable class")
        return cls(
            target=target,
            constructor_args=tuple(fn_constructor_args or ()),
            constructor_kwargs=dict(fn_constructor_kwargs or {}),
            call_args=tuple(fn_args or ()),
            call_kwargs=dict(fn_kwargs or {}),
        )

    @property
    def name(self) -> str:
        return getattr(self.target, "__name__", self.target.__class__.__name__)

    @property
    def is_class(self) -> bool:
        return inspect.isclass(self.target)

    def instantiate(self) -> Callable[..., Any]:
        if self.is_class:
            return self.target(*self.constructor_args, **dict(self.constructor_kwargs))
        if not inspect.isfunction(self.target) and not inspect.ismethod(self.target):
            return copy.deepcopy(self.target)
        return self.target


class LogicalOp:
    """Marker base for typed logical operations."""


@dataclass(frozen=True)
class MapStreamOptions:
    """Explicit local streaming policy; absence preserves ordinary lazy map."""

    concurrency: int = 1
    queue_depth: int | None = None
    execution: str = "inline"
    catch: Tuple[type[Exception], ...] = ()
    label: str | None = None
    callable_scope: str = "stage"

    def __post_init__(self):
        for name in ("concurrency", "queue_depth"):
            value = getattr(self, name)
            if name == "queue_depth" and value is None:
                continue
            if type(value) is not int or value < 1:
                raise ValueError(f"map {name} must be a positive integer")
        if self.execution not in ("inline", "thread"):
            raise ValueError("map execution must be 'inline' or 'thread'")
        if self.callable_scope not in ("stage", "worker"):
            raise ValueError("map callable_scope must be 'stage' or 'worker'")
        if self.label is not None and (not isinstance(self.label, str) or not self.label.strip()):
            raise ValueError("map label must be a non-empty string")
        if not isinstance(self.catch, tuple) or any(
            not isinstance(exc, type) or not issubclass(exc, Exception) for exc in self.catch
        ):
            raise TypeError("map catch must be a tuple of Exception classes; cancellation is never caught")

    def validate_callable(self, spec):
        if self.callable_scope == "worker" and not spec.is_class:
            raise TypeError("map callable_scope='worker' requires a callable class and fn_constructor_args/kwargs")
        target = spec.target
        if inspect.iscoroutinefunction(target) or inspect.iscoroutinefunction(getattr(target, '__call__', None)):
            raise TypeError("map requires a synchronous callable; use map_async for async work")


@dataclass(frozen=True)
class MapOp(LogicalOp):
    callable: CallableSpec
    native_options: NativeOptions | None = None
    stream_options: MapStreamOptions | None = None


@dataclass(frozen=True)
class FlatMapOp(LogicalOp):
    callable: CallableSpec
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class MapBatchesOp(LogicalOp):
    callable: CallableSpec
    batch_size: Optional[int] = None
    batch_format: str = "default"
    zero_copy_batch: bool = False
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class BoundMapOp(LogicalOp):
    callable: CallableSpec
    inputs: Mapping[str, str]
    output: Optional[str] = None
    outputs: Optional[Mapping[str, str]] = None
    native_options: NativeOptions | None = None
    stream_options: MapStreamOptions | None = None


@dataclass(frozen=True)
class OperatorLLMMapOp(LogicalOp):
    """A model node owns its prompt, transport options and request budget."""
    prompt_name: str
    config: PromptPack
    inputs: Mapping[str, str]
    output: Optional[str] = None
    outputs: Optional[Mapping[str, str]] = None
    native_options: NativeOptions | None = None
    options: Mapping[str, Any] | None = None
    max_requests: int | None = None
    node_id: str = field(default_factory=lambda: uuid.uuid4().hex)


@dataclass(frozen=True)
class FilterOp(LogicalOp):
    callable: CallableSpec
    native_options: NativeOptions | None = None


class StreamStage:
    """流式算子便利基类（**非承重件**，2026-09-07 定位降级）。

    平台对零继承零依赖：map_async 的 actor 检测是韧性鸭子（任一策略
    属性在场即按 actor 解析，缺失字段逐项回落默认）。本类只提供字段
    默认值与协议文档——想省样板的算子继承它，追求纯粹的普通类自带
    label/concurrency/queue_depth/catch 声明即可（漏声明的字段走默认，
    不会被误判为 fn 路径）。

    设计分类学：平台注入形态二元——fn（无状态函数，map/map_async）与
    actor（有状态可调用类）。本基类即 actor 槽位的便利形态（Ray 执行器
    对 callable class 保 actor 语义，ray.py 同源）：状态经 __init__ 绑定
    （浏览器/锁/池），aclose 生命周期钩子由 run_stream 退出期统一调用。

    与 SearchEngine 协议的分工：协议适合无状态源实现（结构化鸭子类型）；
    本基类适合管线级算子——并发/队列深度/认缺白名单/统计名随算子声明
    （子类可给默认值，组装层可覆写），依赖经 __init__ 绑定，逻辑写在
    __call__（row -> row | None | list[row]，None=认缺、list=展开，
    同步或异步皆可）。Dataset.map_async（actor 形态）读取策略字段构造 AsyncMapOp。
    """

    label: str = ""
    concurrency: int = 1
    queue_depth: int | None = None
    catch: tuple = ()          # 认缺异常白名单：命中只计数不断链
    hard_timeout: float | None = None
                               # 单行算子调用硬超时（秒，None=不启用，
                               # 活性层 2026-09-14）：超时该行计 miss

    async def __call__(self, row):
        raise NotImplementedError


@dataclass(frozen=True)
class AsyncMapOp(LogicalOp):
    """async 流式算子（streaming 执行路径专用，2026-09-04 新增）。

    fn(row) -> row | None | list[row]：None=认缺丢弃并计数；list=展开
    （flat 语义合一）。concurrency=该级 worker 数（并发封顶）；
    queue_depth=本级输入缓冲行数（None=concurrency，不是字节或 RSS 上限）；
    catch=认缺异常白名单（命中只计数不断链，白名单外
    异常终止整链）；label=统计名（缺省取函数名）。
    Dataset.run_stream() 或本地 materialize() 消费；普通惰性路径显式拒绝。
    """
    callable: CallableSpec
    concurrency: int = 1
    queue_depth: int | None = None
    catch: Tuple[type[BaseException], ...] = ()
    label: str | None = None
    hard_timeout: float | None = None   # 单行硬超时；超时计 miss（活性层）
    execution: str = "inline"          # thread: 同步阻塞 fn，由本级专用线程池执行


@dataclass(frozen=True)
class DeduplicateOp(AsyncMapOp):
    """Online keyed uniqueness, retaining durable first-arrival ownership."""
    on: Tuple[str, ...] = ()
    key: str = ''
    output: str = 'deduplication'


@dataclass(frozen=True)
class AgentMapOp(AsyncMapOp):
    """One outer async node, with a declared row-local operator environment."""
    environment: Any = None


@dataclass(frozen=True)
class BatchMapOp(LogicalOp):
    """async 攒批算子（streaming 专用，2026-09-14）：引擎在级前攒批。

    fn(list[row]) -> list[row] | None：一次调用消化 max_batch 行（条数
    满 / flush_interval 到 / 尾部三触发），输出经既有 list 展开扇出。
    机制动机：限速 API 按请求计费而批端点一次背 N 条——攒批把条目
    吞吐从请求吞吐解耦（如 MediaWiki 50 题/次，配额杠杆 ×50）。批
    调用异常按 catch 白名单整批计 miss，批内容记入 stats.dead_batches
    供幂等重跑回收（账本续跑即重试，不设平台级 DLQ）。
    惰性路径遇到本算子显式拒绝（与 AsyncMapOp 同款不对称）。
    """
    callable: CallableSpec
    max_batch: int = 50
    flush_interval: float | None = None   # 秒；首行入批起算的刷出窗口
    concurrency: int = 1
    queue_depth: int | None = None
    catch: Tuple[type[BaseException], ...] = ()
    label: str | None = None
    hard_timeout: float | None = None     # 单批调用硬超时；同 AsyncMapOp


@dataclass(frozen=True)
class EmbeddingMapOp(BatchMapOp):
    """Native batched embedding node using the shared local streaming substrate."""
    model_contract: Mapping[str, Any] = field(default_factory=dict)
    inputs: Mapping[str, str] = field(default_factory=dict)
    output: str = 'embedding'


@dataclass(frozen=True)
class StreamGroupBatchesOp(BatchMapOp):
    on: Tuple[str, ...] = ()
    output: str = 'items'
    chunk_bytes: int = 32 * 1024**2
    max_groups: int = 64
    buffer_bytes: int = 64 * 1024**2


@dataclass(frozen=True)
class StreamPrependOp(AsyncMapOp):
    """Read a finite synchronous Dataset at this node before its upstream rows."""
    source_dataset: Any = None
    max_rows: int = 100000
    max_row_bytes: int = 1024**2


@dataclass(frozen=True)
class LimitOp(LogicalOp):
    limit: int


@dataclass(frozen=True)
class SelectColumnsOp(LogicalOp):
    columns: Tuple[str, ...]


@dataclass(frozen=True)
class DropColumnsOp(LogicalOp):
    columns: Tuple[str, ...]


@dataclass(frozen=True)
class RenameColumnsOp(LogicalOp):
    names: Tuple[str, ...] | Mapping[str, str]


@dataclass(frozen=True)
class AddColumnOp(LogicalOp):
    column: str
    callable: CallableSpec
    batch_format: str = "pandas"
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class RandomSampleOp(LogicalOp):
    fraction: float
    seed: Optional[int] = None


@dataclass(frozen=True)
class SortOp(LogicalOp):
    keys: Tuple[str, ...]
    descending: bool | Tuple[bool, ...] = False
    boundaries: Optional[Tuple[int | float, ...]] = None


@dataclass(frozen=True)
class RepartitionOp(LogicalOp):
    num_blocks: Optional[int] = None
    target_num_rows_per_block: Optional[int] = None
    shuffle: bool = False
    keys: Optional[Tuple[str, ...]] = None
    sort: bool = False


@dataclass(frozen=True)
class RandomShuffleOp(LogicalOp):
    seed: Optional[int] = None
    num_blocks: Optional[int] = None


@dataclass(frozen=True)
class RandomizeBlockOrderOp(LogicalOp):
    seed: Optional[int] = None


@dataclass(frozen=True)
class LogicalPlan:
    operations: Tuple[LogicalOp, ...] = ()

    def append(self, operation: LogicalOp) -> "LogicalPlan":
        return LogicalPlan(self.operations + (operation,))

    @property
    def is_empty(self) -> bool:
        return not self.operations


_FORBIDDEN_BOUND_PARAM_NAMES = {"row", "raw", "record", "_row"}


def normalize_bound_inputs(inputs: Mapping[str, str] | Sequence[str]) -> Mapping[str, str]:
    if isinstance(inputs, Mapping):
        normalized = {str(k): str(v) for k, v in inputs.items()}
    elif isinstance(inputs, Sequence) and not isinstance(inputs, (str, bytes, bytearray)):
        normalized = {str(name): str(name) for name in inputs}
    else:
        raise TypeError("Dataset.map inputs must be a mapping or sequence of field names")
    if not normalized or any(not k or not v for k, v in normalized.items()):
        raise TypeError("Dataset.map field-bound inputs must be non-empty")
    return normalized


def normalize_outputs(outputs: Optional[Mapping[str, str]]) -> Optional[Mapping[str, str]]:
    if outputs is None:
        return None
    normalized = {str(k): str(v) for k, v in outputs.items()}
    if not normalized or any(not k or not v for k, v in normalized.items()):
        raise TypeError("Dataset.map outputs must be a non-empty mapping")
    return normalized


def validate_bound_signature(spec: CallableSpec, inputs: Mapping[str, str]) -> None:
    target = spec.target
    if inspect.isclass(target):
        target = target.__call__
        signature = inspect.signature(target)
        parameters = {k: v for k, v in signature.parameters.items() if k != "self"}
    else:
        signature = inspect.signature(target)
        parameters = dict(signature.parameters)
    for name, parameter in parameters.items():
        if name in _FORBIDDEN_BOUND_PARAM_NAMES:
            raise TypeError(f"{spec.name}: field-bound map fn must not accept {name!r}")
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(f"{spec.name}: field-bound map fn must not accept *args")
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError(f"{spec.name}: field-bound map fn must not accept **kwargs")
        if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError(f"{spec.name}: field-bound map fn must not use positional-only parameters")
    bindable = {
        name for name, parameter in parameters.items()
        if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    static_kwargs = set(spec.call_kwargs)
    unknown_kwargs = static_kwargs - bindable
    if unknown_kwargs:
        raise TypeError(f"{spec.name}: fn_kwargs bind unknown parameter(s): {sorted(unknown_kwargs)}")
    overlap = set(inputs) & static_kwargs
    if overlap:
        raise TypeError(f"{spec.name}: parameters cannot be bound by both inputs and fn_kwargs: {sorted(overlap)}")
    unknown = set(inputs) - bindable
    if unknown:
        raise TypeError(f"{spec.name}: inputs bind unknown parameter(s): {sorted(unknown)}")
    required = {
        name for name, parameter in parameters.items()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    missing = required - set(inputs) - static_kwargs
    if missing:
        raise TypeError(f"{spec.name}: missing input binding(s): {sorted(missing)}")


class BoundCallable:
    """Runtime lowering for a BoundMapOp; backends only execute row callables."""

    def __init__(self, operation: BoundMapOp) -> None:
        self._operation = operation
        self._fn = operation.callable.instantiate()

    def __call__(self, row: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(row, Mapping):
            raise TypeError(f"{self._operation.callable.name}: field-bound map expects a mapping row")
        kwargs = {}
        for parameter, field_name in self._operation.inputs.items():
            if field_name not in row:
                raise KeyError(
                    f"{self._operation.callable.name}: missing row field {field_name!r} "
                    f"for parameter {parameter!r}"
                )
            kwargs[parameter] = row[field_name]
        result = self._fn(
            *self._operation.callable.call_args,
            **kwargs,
            **dict(self._operation.callable.call_kwargs),
        )
        if self._operation.stream_options is not None and inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError('map synchronous callable returned an awaitable; use map_async')
        if self._operation.output is not None:
            return {**row, self._operation.output: result}
        if self._operation.outputs is not None:
            if not isinstance(result, Mapping):
                raise TypeError(
                    f"{self._operation.callable.name}: outputs mapping requires fn to return a mapping"
                )
            updates = {}
            for result_key, row_field in self._operation.outputs.items():
                if result_key not in result:
                    raise KeyError(
                        f"{self._operation.callable.name}: missing result key {result_key!r}"
                    )
                updates[row_field] = result[result_key]
            return {**row, **updates}
        if result is not None:
            raise TypeError(
                f"{self._operation.callable.name}: field-bound map without output(s) must return None"
            )
        return dict(row)


class StandardCallable:
    """Runtime wrapper that instantiates callable classes inside an executor."""

    def __init__(self, spec: CallableSpec) -> None:
        self._spec = spec
        self._fn = spec.instantiate()

    def __call__(self, row: Any) -> Any:
        return self._fn(row, *self._spec.call_args, **dict(self._spec.call_kwargs))


def is_stream_operation(operation) -> bool:
    """One dispatch predicate shared by streaming actions and checkpoint bridges."""
    return isinstance(operation, (AsyncMapOp, BatchMapOp)) or (
        isinstance(operation, (MapOp, BoundMapOp)) and operation.stream_options is not None
    )


def reject_streaming_map_options(plan, backend):
    """Reject before reading sources, constructing workers or opening a sink."""
    if any(isinstance(op, (MapOp, BoundMapOp)) and op.stream_options is not None
           for op in plan.operations):
        from ..errors import UnsupportedExecutionOptionError
        raise UnsupportedExecutionOptionError(
            f"Dataset.map streaming options are not supported by {backend} lazy execution; "
            "use local run_stream(), materialize() or checkpoint()"
        )


@dataclass(frozen=True)
class VectorSearchOp(AsyncMapOp):
    """One query vector per row, with a candidate list added to that row."""
    query: str = ''
    output: str = ''
    search_spec: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SearchWebOp(AsyncMapOp):
    """Native web search node; request/result fields are recorded in the plan."""
    requests: str = ''
    output: str = ''
    checkpoint: str | None = None
    checkpoint_operator: str = 'search_web'
    checkpoint_lease_s: float = 300.0


@dataclass(frozen=True)
class FetchDocumentsOp(AsyncMapOp):
    requests: str = ''
    output: str = ''


@dataclass(frozen=True)
class FetchImagesOp(AsyncMapOp):
    requests: str = ''
    output: str = ''


@dataclass(frozen=True)
class RegisterDocumentsOp(AsyncMapOp):
    request: str = ''
    output: str = ''


@dataclass(frozen=True)
class RegisterDocumentsBatchOp(BatchMapOp):
    request: str = ''
    output: str = ''


@dataclass(frozen=True)
class ReadDocumentsOp(AsyncMapOp):
    request: str = ''
    output: str = ''


@dataclass(frozen=True)
class SaveLanceOp(BatchMapOp):
    uri: str = ''
    stage: str = ''
    key: str = ''
    output_ref: str | None = None
    mode: str = 'overwrite'


@dataclass(frozen=True)
class ImageMapOp(AsyncMapOp):
    """Native image model request with a mandatory versioned template."""
    template: Mapping[str, Any] = field(default_factory=dict)
    model_contract: Mapping[str, Any] = field(default_factory=dict)
    inputs: Mapping[str, str] = field(default_factory=dict)
    output: str = ''
    image_encoding: str = 'png'
