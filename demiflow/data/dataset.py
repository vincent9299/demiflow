"""Lazy, backend-neutral Dataset API compatible with Ray Data concepts.

Demiflow is a Ray-compatible superset: standard row map/filter APIs coexist
with the first-class field-bound map extension.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from .datasink import Datasink, WriteResult, validate_write_result
from .plan import (
    AddColumnOp,
    AsyncMapOp,
    BoundMapOp,
    CallableSpec,
    DropColumnsOp,
    FilterOp,
    FlatMapOp,
    LimitOp,
    LogicalPlan,
    MapOp,
    MapBatchesOp,
    OperatorLLMMapOp,
    RandomSampleOp,
    RandomShuffleOp,
    RandomizeBlockOrderOp,
    RepartitionOp,
    RenameColumnsOp,
    SelectColumnsOp,
    SortOp,
    normalize_bound_inputs,
    normalize_outputs,
    validate_bound_signature,
)
from .sources import MaterializedSource, SourcePlan
from .native_options import parse_native_options
from ..observability import observe_action, observe_batches, observe_rows

_BATCH_FORMATS = {"default", "numpy", "pandas", "pyarrow"}



def _columns(value: str | Sequence[str], label: str) -> tuple[str, ...]:
    columns = (value,) if isinstance(value, str) else tuple(value)
    if not columns or any(
        not isinstance(column, str) or not column for column in columns
    ):
        raise ValueError(f"{label} requires non-empty column names")
    if len(columns) != len(set(columns)):
        raise ValueError(f"{label} columns must be unique")
    return columns


def _batch_format(value: Optional[str]) -> str:
    normalized = "default" if value is None else str(value)
    if normalized not in _BATCH_FORMATS:
        raise ValueError(
            f"batch_format must be one of {sorted(_BATCH_FORMATS)}"
        )
    return normalized


def _batch_row_count(batch: Any) -> int:
    rows = getattr(batch, "num_rows", None)
    if rows is not None:
        return int(rows)
    if isinstance(batch, Mapping):
        if not batch:
            return 0
        try:
            return len(next(iter(batch.values())))
        except TypeError:
            return 0
    try:
        return len(batch)
    except TypeError:
        return 0


def _optional_seed(value: Optional[int], label: str) -> Optional[int]:
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int)
    ):
        raise TypeError(f"{label} seed must be an integer or None")
    return value


class Dataset:
    """A lazy source and immutable transformation plan.

    Source and transform methods build a plan; terminal actions such as
    ``take``, ``take_all``, ``count``, ``materialize``, and ``write_*`` execute
    it. Multiple actions on the same non-materialized Dataset may execute the
    full upstream plan repeatedly, including external reads, Python callables,
    and Operator LLM requests. Use ``materialize()`` when computed rows must be
    reused by more than one action.

    Keep row-level processing and detail writes in Dataset plans. Use Driver
    collection only for genuinely bounded inspection or aggregation; do not
    collect rows and recreate them with ``ctx.data.from_items`` solely to write
    the same detail rows. Distributed execution has no stable global row order
    without an explicit supported sort.
    """

    def __init__(
        self, source: SourcePlan, plan: LogicalPlan, executor: Any,
    ) -> None:
        self._source = source
        self._plan = plan
        self._executor = executor

    def map(
        self,
        fn: Callable[..., Any],
        *,
        inputs: Optional[Mapping[str, str] | Sequence[str]] = None,
        output: Optional[str] = None,
        outputs: Optional[Mapping[str, str]] = None,
        fn_args: Optional[Sequence[Any]] = None,
        fn_kwargs: Optional[Mapping[str, Any]] = None,
        fn_constructor_args: Optional[Sequence[Any]] = None,
        fn_constructor_kwargs: Optional[Mapping[str, Any]] = None,
        backend_options=None,
    ) -> "Dataset":
        """Append one lazy Python row transformation.

        Without ``inputs``, ``fn`` receives the complete row mapping and its
        returned mapping becomes the complete output row. With ``inputs``, the
        mapping direction is callable argument name to Dataset row field name;
        existing fields are preserved. ``output`` stores the complete callable
        return value in one row field. ``outputs`` maps callable return keys to
        destination row fields. Use at most one of ``output`` and ``outputs``.
        Demiflow plans worker resources and parallelism at action time.
        """
        spec = CallableSpec.create(
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        native_options = parse_native_options(backend_options, family="row_transform")
        if inputs is None:
            if output is not None or outputs is not None:
                raise TypeError("Dataset.map output(s) require inputs")
            operation = MapOp(spec, native_options)
        else:
            if output is not None and outputs is not None:
                raise TypeError("Dataset.map accepts either output or outputs, not both")
            normalized_inputs = normalize_bound_inputs(inputs)
            normalized_outputs = normalize_outputs(outputs)
            normalized_output = str(output) if output is not None else None
            if normalized_output == "":
                raise TypeError("Dataset.map output must be non-empty")
            validate_bound_signature(spec, normalized_inputs)
            operation = BoundMapOp(
                spec,
                normalized_inputs,
                normalized_output,
                normalized_outputs,
                native_options,
            )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def flat_map(
        self, fn: Callable[..., Iterable[Mapping[str, Any]]], *,
        fn_args: Optional[Sequence[Any]] = None,
        fn_kwargs: Optional[Mapping[str, Any]] = None,
        fn_constructor_args: Optional[Sequence[Any]] = None,
        fn_constructor_kwargs: Optional[Mapping[str, Any]] = None,
        backend_options=None,
    ) -> "Dataset":
        """Append a lazy transform that emits zero or more row mappings per input row."""
        operation = FlatMapOp(
            CallableSpec.create(
                fn, fn_args=fn_args, fn_kwargs=fn_kwargs,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
            ),
            parse_native_options(backend_options, family="row_transform"),
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def map_async(
        self,
        fn: Callable[..., Any],
        *,
        concurrency: int = 1,
        queue_depth: Optional[int] = None,
        catch: tuple = (),
        label: Optional[str] = None,
    ) -> "Dataset":
        """Append one async streaming transformation（streaming 路径专用）。

        fn(row) -> row | None | list[row]，同步或异步函数均可（awaitable
        会被等待）。None = 认缺丢弃并计数；list = 展开（flat 语义合一，
        不设单独 flat 变体）。concurrency = 该级 worker 数即并发封顶；
        queue_depth = 该级输入缓冲深度（None = concurrency；载字节载荷时
        深度×载荷即内存上界，按需收窄）。catch = 认缺异常白名单：命中只
        计数不断链（网络类瞬态/确定性失败），白名单外异常终止整链。
        计划中含本算子时，终结动作必须用 run_stream()（惰性动作将拒绝）。
        """
        if int(concurrency) < 1:
            raise ValueError("map_async concurrency must be >= 1")
        if queue_depth is not None and int(queue_depth) < 1:
            raise ValueError("map_async queue_depth must be >= 1 or None")
        operation = AsyncMapOp(
            CallableSpec.create(fn),
            int(concurrency),
            None if queue_depth is None else int(queue_depth),
            tuple(catch),
            label,
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def run_stream(
        self,
        *,
        on_progress=None,
        on_drain=None,
        log_every: int = 0,
    ):
        """streaming 路径终结动作：驱动含 map_async 的计划至完成。

        同步入口（内部自建事件循环；不要再包在 asyncio.run 里调用）。
        返回 StreamStats（per-stage 计数 + 认缺归集）。on_progress(stats)
        在首级每消费 log_every 行时回调（同步/异步皆可）；on_drain(stats)
        收尾钩子在完成与 Ctrl-C/异常路径都执行——钩子内必须落盘的同步
        写放最前（await 段在中断路径可能被取消截断，契约见 stream.py）。
        """
        from ..execution.stream import run_stream as _run_stream
        rows = self._executor.iter_rows(self._source, LogicalPlan())
        return _run_stream(
            rows, self._plan,
            on_progress=on_progress, on_drain=on_drain, log_every=log_every,
        )

    def map_batches(
        self, fn: Callable[..., Any], *, batch_size: Optional[int] = None,
        batch_format: Optional[str] = "default", zero_copy_batch: bool = False,
        fn_args: Optional[Sequence[Any]] = None,
        fn_kwargs: Optional[Mapping[str, Any]] = None,
        fn_constructor_args: Optional[Sequence[Any]] = None,
        fn_constructor_kwargs: Optional[Mapping[str, Any]] = None,
        backend_options=None,
    ) -> "Dataset":
        """Append a lazy batch transform using the requested backend-supported batch format."""
        if batch_size is not None and int(batch_size) <= 0:
            raise ValueError("map_batches batch_size must be positive or None")
        operation = MapBatchesOp(
            CallableSpec.create(
                fn, fn_args=fn_args, fn_kwargs=fn_kwargs,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
            ),
            None if batch_size is None else int(batch_size),
            _batch_format(batch_format), bool(zero_copy_batch), parse_native_options(backend_options, family="batch_transform"),
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def map_prompt(
        self,
        prompt: str,
        *,
        config: str,
        inputs: Mapping[str, str] | Sequence[str],
        output: Optional[str] = None,
        outputs: Optional[Mapping[str, str]] = None,
        backend_options=None,
    ) -> "Dataset":
        """Append one lazy Candidate-owned Operator LLM transformation.

        ``prompt`` and ``config`` name Candidate-owned prompt resources. ``config`` names a
        top-level YAML file inside ``pipeline/``: use ``"map_prompt.yaml"``;
        do not use a variable, absolute or nested path, or
        ``"pipeline/map_prompt.yaml"``.

        The file must use ``demiflow_prompt_pack_v2``. This minimal generic
        example is valid (business prompt text and model values must still come
        from the current Goal)::

            schema_version: demiflow_prompt_pack_v2
            prompts:
              example:
                version: example-v1
                model:
                  name: user-provided-model-name
                  transport: openai_compatible
                  base_url_env: MODEL_BASE_URL
                  api_key_env: MODEL_API_KEY
                schema_retries: 1
                response_schema:
                  type: object
                  additionalProperties: false
                  required: [result]
                  properties:
                    result: {type: string}
                template: |
                  Structured input:
                  {{ payload | json }}
                  Image:
                  {{ image | image }}

        Each prompt has exactly ``version``, ``model``, ``response_schema``,
        optional ``schema_retries``, and ``template``. ``template`` is a
        non-empty YAML string, not an OpenAI role/content list. Placeholder
        forms are ``{{ name }}`` or ``{{ name | text }}`` for text/scalars,
        ``{{ name | json }}`` for strict JSON, and ``{{ name | image }}`` for
        image bytes with detectable PNG/JPEG/GIF/WebP type, data-image URLs,
        HTTP(S) URLs, ``ImageValue``, or a sequence of image values. Parts are
        emitted in template order.

        A model requires ``name``, ``transport``, and ``api_key_env`` plus
        exactly one of ``base_url`` and ``base_url_env``. Transport is
        ``azure_openai`` or ``openai_compatible``. Azure requires
        ``api_version``; OpenAI-compatible forbids it. Environment names match
        ``[A-Z_][A-Z0-9_]*`` and configuration stores names, never secrets.

        ``response_schema`` must have object root. Supported keywords are
        ``type``, ``properties``, ``required``, ``additionalProperties``,
        ``items``, ``minItems``, ``maxItems``, ``minLength``, ``maxLength``,
        ``minimum``, ``maximum``, ``exclusiveMinimum``, ``exclusiveMaximum``,
        ``enum``, ``const``, and ``format``; formats are ``date`` and
        ``date-time``. Responses are
        strict JSON and ``schema_retries`` is 0 or 1.

        ``inputs`` maps template argument names to Dataset row field names and
        must exactly cover all placeholders. Use exactly one of ``output`` and
        ``outputs``. ``output`` is valid only for one required top-level
        response property and writes that property's value, not the complete
        response object. For multiple required properties, ``outputs`` maps
        every response property name to its destination row field.

        This transform executes on workers when a terminal action runs. More
        than one action on a non-materialized Dataset may repeat model calls;
        materialize before reuse when repetition is unintended.
        """
        name = str(prompt or "").strip()
        if not name:
            raise TypeError("Dataset.map_prompt requires a non-empty prompt name")
        config_path = _prompt_config_path(config)
        if output is not None and outputs is not None:
            raise TypeError("Dataset.map_prompt accepts either output or outputs, not both")
        if output is None and outputs is None:
            raise TypeError("Dataset.map_prompt requires output or outputs")
        normalized_output = str(output) if output is not None else None
        if normalized_output == "":
            raise TypeError("Dataset.map_prompt output must be non-empty")
        operation = OperatorLLMMapOp(
            name,
            config_path,
            normalize_bound_inputs(inputs),
            normalized_output,
            normalize_outputs(outputs),
            parse_native_options(backend_options, family="row_transform"),
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def filter(
        self,
        fn: Callable[..., bool],
        *,
        fn_args: Optional[Sequence[Any]] = None,
        fn_kwargs: Optional[Mapping[str, Any]] = None,
        fn_constructor_args: Optional[Sequence[Any]] = None,
        fn_constructor_kwargs: Optional[Mapping[str, Any]] = None,
        backend_options=None,
    ) -> "Dataset":
        """Append a lazy predicate; Demiflow plans bounded parallelism."""
        spec = CallableSpec.create(
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        operation = FilterOp(
            spec,
            parse_native_options(backend_options, family="row_transform"),
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
        )

    def limit(self, limit: int) -> "Dataset":
        """Append an early-stop limit; Local stops upstream iteration promptly."""
        value = int(limit)
        if value < 0:
            raise ValueError("Dataset.limit requires a non-negative limit")
        return Dataset(
            self._source, self._plan.append(LimitOp(value)), self._executor,
        )

    def select_columns(
        self, cols: str | list[str],
    ) -> "Dataset":
        """Select columns lazily using Ray Data-compatible arguments."""
        columns = _columns(cols, "Dataset.select_columns")
        operation = SelectColumnsOp(
            columns,
        )
        return Dataset(self._source, self._plan.append(operation), self._executor)

    def drop_columns(
        self, cols: str | Sequence[str],
    ) -> "Dataset":
        """Append a lazy transform that removes the named columns and fails if a column is absent."""
        columns = (cols,) if isinstance(cols, str) else tuple(cols)
        if any(not isinstance(column, str) or not column for column in columns):
            raise ValueError("Dataset.drop_columns requires non-empty column names")
        if len(columns) != len(set(columns)):
            raise ValueError("Dataset.drop_columns columns must be unique")
        operation = DropColumnsOp(columns)
        return Dataset(self._source, self._plan.append(operation), self._executor)

    def rename_columns(
        self, names: Sequence[str] | Mapping[str, str],
    ) -> "Dataset":
        """Append a lazy transform that renames columns using a mapping or aligned name sequence."""
        if isinstance(names, Mapping):
            normalized: tuple[str, ...] | Mapping[str, str] = {
                str(old): str(new) for old, new in names.items()
            }
            if not normalized or any(
                not old or not new for old, new in normalized.items()
            ):
                raise ValueError(
                    "Dataset.rename_columns requires non-empty names"
                )
            if len(set(normalized.values())) != len(normalized):
                raise ValueError(
                    "Dataset.rename_columns target names must be unique"
                )
        else:
            normalized = _columns(tuple(names), "Dataset.rename_columns")
        operation = RenameColumnsOp(normalized)
        return Dataset(self._source, self._plan.append(operation), self._executor)

    def add_column(
        self, col: str, fn: Callable[..., Any], *,
        batch_format: Optional[str] = "pandas", backend_options=None,
    ) -> "Dataset":
        """Append a lazy batch callable that computes one new column."""
        column = str(col or "")
        if not column:
            raise ValueError(
                "Dataset.add_column requires a non-empty column name"
            )
        operation = AddColumnOp(
            column, CallableSpec.create(fn), _batch_format(batch_format),
            parse_native_options(backend_options, family="batch_transform"),
        )
        return Dataset(self._source, self._plan.append(operation), self._executor)

    def random_sample(
        self, fraction: float, *, seed: Optional[int] = None,
    ) -> "Dataset":
        """Append a lazy Bernoulli row sample; the output size is not exact."""
        value = float(fraction)
        if not 0.0 <= value <= 1.0:
            raise ValueError(
                "Dataset.random_sample fraction must be in [0, 1]"
            )
        seed = _optional_seed(seed, "Dataset.random_sample")
        return Dataset(
            self._source,
            self._plan.append(RandomSampleOp(value, seed)),
            self._executor,
        )

    def sort(
        self, key: str | Sequence[str],
        descending: bool | Sequence[bool] = False,
        boundaries: Optional[Sequence[int | float]] = None,
    ) -> "Dataset":
        """Append a Ray-only global sort; Local execution fails closed."""
        keys = _columns(key, "Dataset.sort")
        if isinstance(descending, bool):
            normalized_descending: bool | tuple[bool, ...] = descending
        else:
            normalized_descending = tuple(descending)
        if (
            len(normalized_descending) != len(keys)
            or any(not isinstance(item, bool) for item in normalized_descending)
        ):
            raise ValueError(
                "Dataset.sort descending must be a bool or one bool per key"
            )
        normalized_boundaries = None
        if boundaries is not None:
            normalized_boundaries = tuple(boundaries)
            if any(
                isinstance(item, bool) or not isinstance(item, (int, float))
                for item in normalized_boundaries
            ):
                raise TypeError("Dataset.sort boundaries must be numeric")
        return Dataset(
            self._source,
            self._plan.append(SortOp(
                keys, normalized_descending, normalized_boundaries,
            )),
            self._executor,
        )

    def repartition(
        self, num_blocks: Optional[int] = None,
        target_num_rows_per_block: Optional[int] = None, *,
        shuffle: bool = False, keys: Optional[Sequence[str]] = None,
        sort: bool = False,
    ) -> "Dataset":
        """Append a Ray-only repartition transform using exactly one supported block-count argument."""
        if (num_blocks is None) == (target_num_rows_per_block is None):
            raise ValueError(
                "Dataset.repartition requires exactly one of num_blocks or "
                "target_num_rows_per_block"
            )
        if num_blocks is not None and int(num_blocks) <= 0:
            raise ValueError("Dataset.repartition num_blocks must be positive")
        if (
            target_num_rows_per_block is not None
            and int(target_num_rows_per_block) <= 0
        ):
            raise ValueError(
                "Dataset.repartition target_num_rows_per_block must be positive"
            )
        normalized_keys = None if keys is None else _columns(
            tuple(keys), "Dataset.repartition keys",
        )
        return Dataset(
            self._source,
            self._plan.append(RepartitionOp(
                None if num_blocks is None else int(num_blocks),
                None if target_num_rows_per_block is None
                else int(target_num_rows_per_block),
                bool(shuffle), normalized_keys, bool(sort),
            )),
            self._executor,
        )

    def random_shuffle(
        self, *, seed: Optional[int] = None,
        num_blocks: Optional[int] = None,
    ) -> "Dataset":
        """Append a Ray-only global random shuffle."""
        normalized_seed = _optional_seed(seed, "Dataset.random_shuffle")
        if num_blocks is not None and int(num_blocks) <= 0:
            raise ValueError("Dataset.random_shuffle num_blocks must be positive")
        return Dataset(
            self._source,
            self._plan.append(RandomShuffleOp(
                normalized_seed,
                None if num_blocks is None else int(num_blocks),
            )),
            self._executor,
        )

    def randomize_block_order(
        self, *, seed: Optional[int] = None,
    ) -> "Dataset":
        """Append a Ray-only randomization of block order without shuffling rows inside blocks."""
        return Dataset(
            self._source,
            self._plan.append(RandomizeBlockOrderOp(
                _optional_seed(seed, "Dataset.randomize_block_order"),
            )),
            self._executor,
        )

    def materialize(self) -> "MaterializedDataset":
        """Execute and pin this Dataset for reuse in the current run.

        Returns a new ``MaterializedDataset`` and does not mutate the original.
        Use it before multiple actions to avoid repeating external reads,
        callables, or Operator LLM requests. The handle is current-run only and
        must not be returned from ``PipelineProgram.run``.
        """
        with observe_action("materialize", self._source, self._plan, self._executor) as observer:
            handle = self._executor.materialize(self._source, self._plan)
            known_row_count = getattr(handle, "row_count", None)
            row_count = int(known_row_count or 0)
            block_count = len(getattr(handle, "blocks", ()) or ())
            observer.rows = row_count
            observer.batches = block_count
            observer.complete(materialized=True)
        return MaterializedDataset(
            MaterializedSource(
                handle, None if known_row_count is None else int(known_row_count),
            ),
            LogicalPlan(), self._executor,
        )

    def take(self, limit: int = 20) -> list[dict[str, Any]]:
        """Execute and return at most ``limit`` rows to the Driver.

        Use for bounded sampling or inspection. Fewer rows may be returned, and
        global order is not stable without explicit sorting.
        """
        maximum = max(0, int(limit))
        with observe_action("take", self._source, self._plan, self._executor) as observer:
            rows = self._executor.take(self._source, self._plan, maximum)
            observer.rows = len(rows)
            observer.complete(limit=maximum, early_stopped=len(rows) == maximum)
        return rows

    def take_all(self, limit: Optional[int] = None) -> list[dict[str, Any]]:
        """Execute and collect accepted rows in Driver memory.

        Without ``limit`` every row is collected. With ``limit``, Demiflow
        verifies that no additional row exists and raises rather than silently
        truncating. Use only for statically bounded analysis. Do not use
        ``take_all`` followed by ``ctx.data.from_items`` solely to write the
        same detail rows. On a non-materialized Dataset this action can repeat
        every upstream operation.
        """
        with observe_action("take_all", self._source, self._plan, self._executor) as observer:
            if limit is None:
                stream = self._executor.iter_rows(self._source, self._plan)
                rows = []
                for row in stream:
                    rows.append(row)
                    observer.progress(rows=1)
            else:
                maximum = int(limit)
                if maximum < 0:
                    raise ValueError("Dataset.take_all limit must be non-negative")
                rows = self._executor.take(
                    self._source, self._plan, maximum + 1,
                )
                observer.rows = len(rows)
                if len(rows) > maximum:
                    raise ValueError(
                        f"Dataset contains more than the take_all limit of {maximum} rows"
                    )
                observer.complete(limit=limit)
        return rows

    def take_batch(
        self, batch_size: int = 20, *, batch_format: Optional[str] = "default",
    ) -> Any:
        """Execute and return one bounded batch in the requested supported batch format."""
        size = int(batch_size)
        if size <= 0:
            raise ValueError("Dataset.take_batch batch_size must be positive")
        with observe_action(
            "take_batch", self._source, self._plan, self._executor,
        ) as observer:
            batch = self._executor.take_batch(
                self._source, self._plan, size,
                batch_format=_batch_format(batch_format),
            )
            observer.rows = _batch_row_count(batch)
            observer.batches = 1
            observer.complete(batch_size=size, batch_format=batch_format)
        return batch

    def count(self) -> int:
        """Execute the lazy plan and count rows without returning row payloads."""
        with observe_action("count", self._source, self._plan, self._executor) as observer:
            count = self._executor.count(self._source, self._plan)
            observer.rows = int(count)
            observer.complete()
        return count

    def aggregate(self, *aggs: Any) -> Any:
        """Aggregate values using mergeable functions; this is a terminal action."""
        from .aggregate import AggregateFnV2
        if not aggs:
            raise ValueError("Dataset.aggregate requires at least one aggregation")
        if any(not isinstance(aggregate, AggregateFnV2) for aggregate in aggs):
            raise TypeError("Dataset.aggregate accepts demiflow AggregateFnV2 instances")
        names = [aggregate.name for aggregate in aggs]
        if len(names) != len(set(names)):
            raise ValueError("Dataset.aggregate names must be unique")
        with observe_action(
            "aggregate", self._source, self._plan, self._executor,
            aggregate_count=len(aggs),
        ) as observer:
            result = self._executor.aggregate(self._source, self._plan, tuple(aggs))
            observer.complete()
        return result

    def _aggregate_columns(
        self, aggregate_type: Any, on: str | Sequence[str] | None,
        **kwargs: Any,
    ) -> Any:
        scalar = isinstance(on, str)
        columns = [on] if scalar else (
            list(on) if on is not None else self.columns()
        )
        if not columns:
            return None
        result = self.aggregate(*(
            aggregate_type(column, **kwargs) for column in columns
        ))
        if scalar and result is not None:
            return result[next(iter(result))]
        return result

    def sum(
        self, on: str | Sequence[str] | None = None,
        ignore_nulls: bool = True,
    ) -> Any:
        """Execute a terminal mergeable sum over one or more columns."""
        from .aggregate import Sum
        return self._aggregate_columns(Sum, on, ignore_nulls=ignore_nulls)

    def min(
        self, on: str | Sequence[str] | None = None,
        ignore_nulls: bool = True,
    ) -> Any:
        """Execute a terminal mergeable minimum over one or more columns."""
        from .aggregate import Min
        return self._aggregate_columns(Min, on, ignore_nulls=ignore_nulls)

    def max(
        self, on: str | Sequence[str] | None = None,
        ignore_nulls: bool = True,
    ) -> Any:
        """Execute a terminal mergeable maximum over one or more columns."""
        from .aggregate import Max
        return self._aggregate_columns(Max, on, ignore_nulls=ignore_nulls)

    def mean(
        self, on: str | Sequence[str] | None = None,
        ignore_nulls: bool = True,
    ) -> Any:
        """Execute a terminal mergeable arithmetic mean over one or more columns."""
        from .aggregate import Mean
        return self._aggregate_columns(Mean, on, ignore_nulls=ignore_nulls)

    def std(
        self, on: str | Sequence[str] | None = None, ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> Any:
        """Execute a terminal mergeable standard deviation with the requested delta degrees of freedom."""
        from .aggregate import Std
        return self._aggregate_columns(
            Std, on, ddof=ddof, ignore_nulls=ignore_nulls,
        )

    def iter_rows(self) -> Iterable[dict[str, Any]]:
        """Execute and consume rows incrementally without building a driver list."""
        return observe_rows(
            "iter_rows", self._executor.iter_rows(self._source, self._plan),
            self._source, self._plan, self._executor,
        )

    def iter_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        **_: Any,
    ) -> Iterable[Any]:
        """Execute and consume bounded blocks with executor-native batch formats."""
        batches = self._executor.iter_batches(
            self._source,
            self._plan,
            prefetch_batches=prefetch_batches,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
        )
        return observe_batches(
            "iter_batches", batches, self._source, self._plan, self._executor,
        )

    def schema(self, fetch_if_missing: bool = True) -> Any:
        """Return the backend-native Dataset schema, fetching source metadata when requested."""
        return self._executor.schema(
            self._source, self._plan, fetch_if_missing=fetch_if_missing
        )

    def columns(self, fetch_if_missing: bool = True) -> list[str] | None:
        """Return known column names, fetching source metadata when requested."""
        return self._executor.columns(
            self._source, self._plan, fetch_if_missing=fetch_if_missing,
        )

    def size_bytes(self) -> int:
        """Execute or inspect the plan as required to return its estimated size in bytes."""
        with observe_action(
            "size_bytes", self._source, self._plan, self._executor,
        ) as observer:
            value = self._executor.size_bytes(self._source, self._plan)
            observer.complete(size_bytes=value)
        return value

    def stats(self) -> str:
        """Return backend execution statistics for this Dataset plan."""
        return self._executor.stats(self._source, self._plan)

    def execution_metadata(self):
        """Return backend-native execution metadata; Candidate code should not use this internal diagnostic surface."""
        return self._executor.execution_metadata(self._source, self._plan)

    def write_datasink(
        self, datasink: Datasink, *, backend_options=None,
    ) -> None:
        """Execute the lazy plan through a formal Datasink."""
        target = ""
        with observe_action(
            "write_datasink", self._source, self._plan, self._executor,
            sink_type=type(datasink).__name__, sink_target=target,
        ) as observer:
            result = validate_write_result(self._executor.write_datasink(
                self._source, self._plan, datasink,
                native_options=parse_native_options(backend_options, family="sink"),
            ))
            observer.rows = int(result.written_rows or 0)
            observer.batches = int(result.blocks_written or 0)
            observer.complete(
                failed_rows=result.failed_rows,
                result_target=str(result.target or target),
            )
        return None

    def write_lance(
        self, uri: str, *, expected_version: int | None = None,
        storage_options: Mapping[str, str] | None = None,
        backend_options=None,
    ) -> None:
        """Create or append this Dataset through one managed Lance write.

        An absent target is created and an existing target is appended after
        exact Arrow schema validation. ``expected_version`` enables real
        compare-and-append. The action returns ``None``; an indeterminate
        commit raises ``LanceWriteError`` and requires reconciliation. Lance
        writes do not overwrite, evolve schema, or provide data idempotency.
        """
        from ..lance.model import LanceWriteSpec

        spec = LanceWriteSpec(
            uri=uri, expected_version=expected_version,
            storage_options=storage_options,
        )
        target = spec.uri
        with observe_action(
            "write_lance", self._source, self._plan, self._executor,
            sink_type="LanceWrite", sink_target=target,
        ) as observer:
            receipt = self._executor.write_lance(
                self._source, self._plan, spec,
                native_options=parse_native_options(backend_options, family="sink"),
            )
            if receipt.status == "indeterminate":
                from ..errors import LanceWriteError
                raise LanceWriteError(receipt)
            observer.rows = int(receipt.written_rows or 0)
            observer.complete(
                result_target=target,
                write_receipt=receipt.to_dict(),
                reconciliation_required=False,
            )
        return None

    def _write_file(
        self, format_name: str, path: str, options: Mapping[str, Any],
    ) -> None:
        with observe_action(
            f"write_{format_name}", self._source, self._plan, self._executor,
            sink_target=str(path),
        ) as observer:
            result = validate_write_result(self._executor.write_file(
                self._source, self._plan, format_name, str(path), dict(options),
            ))
            observer.rows = int(result.written_rows or 0)
            observer.batches = int(result.blocks_written or 0)
            observer.complete(
                failed_rows=result.failed_rows,
                result_target=str(result.target or path),
                metadata=dict(result.metadata),
            )

    def write_parquet(
        self, path: str, *, filesystem=None, backend_options=None, **options: Any,
    ) -> None:
        """Execute and write rows to a backend-native Parquet output location; returns None."""
        _write_options(options, filesystem, backend_options)
        self._write_file("parquet", path, options)

    def write_json(
        self, path: str, *, filesystem=None, backend_options=None, **options: Any,
    ) -> None:
        """Write rows as JSON Lines to a backend-native output location.

        This terminal action returns ``None``. Local currently writes one JSON
        Lines file; Ray delegates to Ray Data and may treat ``path`` as a
        directory containing part files. A ``.json`` suffix therefore does not
        guarantee one physical file across backends. Dataset row fields become
        top-level JSON record fields; select required conclusion fields directly
        instead of wrapping them in an extra container field.
        """
        _write_options(options, filesystem, backend_options)
        self._write_file("json", path, options)

    def write_csv(
        self, path: str, *, filesystem=None, backend_options=None, **options: Any,
    ) -> None:
        """Execute and write rows to a backend-native CSV output location; returns None."""
        _write_options(options, filesystem, backend_options)
        self._write_file("csv", path, options)


class MaterializedDataset(Dataset):
    """A Dataset whose source blocks are pinned in its executor materialized store."""

    def num_blocks(self) -> int:
        """Return the number of pinned blocks in this current-run materialized Dataset."""
        return self._executor.num_blocks(self._source, self._plan)


def _write_options(options, filesystem, backend_options):
    if filesystem is not None: options["filesystem"] = filesystem
    native=parse_native_options(backend_options, family="sink")
    if native is not None: options["_demiflow_native_options"] = native


def _prompt_config_path(value: str) -> str:
    from pathlib import PurePosixPath
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError("Dataset.map_prompt config must be a non-empty static path")
    path = PurePosixPath(value)
    if path.is_absolute() or len(path.parts) != 1 or path.suffix not in {".yaml", ".yml"}:
        raise ValueError("Dataset.map_prompt config must be a top-level pipeline YAML file")
    return value
