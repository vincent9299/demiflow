"""Backend-neutral logical plan for demiflow datasets.

The plan is immutable and contains no execution logic. Backends compile the
same source + plan into their native execution model.
"""

from __future__ import annotations

import inspect
import copy
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
class MapOp(LogicalOp):
    callable: CallableSpec
    native_options: NativeOptions | None = None


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


@dataclass(frozen=True)
class OperatorLLMMapOp(LogicalOp):
    prompt_name: str
    config_path: str
    inputs: Mapping[str, str]
    output: Optional[str] = None
    outputs: Optional[Mapping[str, str]] = None
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class FilterOp(LogicalOp):
    callable: CallableSpec
    native_options: NativeOptions | None = None


class StreamStage:
    """流式算子规范（继承式，2026-09-04·九）：策略字段 + __call__ 实现。

    与 SearchEngine 协议的分工：协议适合无状态源实现（结构化鸭子类型）；
    本基类适合管线级算子——并发/队列深度/认缺白名单/统计名随算子声明
    （子类可给默认值，组装层可覆写），依赖经 __init__ 绑定，逻辑写在
    __call__（row -> row | None | list[row]，None=认缺、list=展开，
    同步或异步皆可）。Dataset.map_stage 读取全部策略字段构造 AsyncMapOp。
    """

    label: str = ""
    concurrency: int = 1
    queue_depth: int | None = None
    catch: tuple = ()          # 认缺异常白名单：命中只计数不断链

    async def __call__(self, row):
        raise NotImplementedError


@dataclass(frozen=True)
class AsyncMapOp(LogicalOp):
    """async 流式算子（streaming 执行路径专用，2026-09-04 新增）。

    fn(row) -> row | None | list[row]：None=认缺丢弃并计数；list=展开
    （flat 语义合一）。concurrency=该级 worker 数（并发封顶）；
    queue_depth=下游缓冲深度（None=concurrency；载字节载荷时深度即
    内存上界）；catch=认缺异常白名单（命中只计数不断链，白名单外
    异常终止整链）；label=统计名（缺省取函数名）。
    仅由 Dataset.run_stream() 消费；惰性路径遇到本算子将显式拒绝。
    """
    callable: CallableSpec
    concurrency: int = 1
    queue_depth: int | None = None
    catch: Tuple[type[BaseException], ...] = ()
    label: str | None = None


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
