"""Lazy, backend-neutral Dataset API compatible with Ray Data concepts.

Demiflow is a Ray-compatible superset: standard row map/filter APIs coexist
with the first-class field-bound map extension.
"""

from __future__ import annotations

from pathlib import Path
from copy import deepcopy
from ..operator_llm.model import PromptPack
from ..operator_llm.parser import load_prompt_pack

from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from .datasink import Datasink, WriteResult, validate_write_result
from .plan import (
    AddColumnOp,
    AsyncMapOp,
    BatchMapOp,
    BoundMapOp,
    CallableSpec,
    DropColumnsOp,
    FilterOp,
    FlatMapOp,
    LimitOp,
    LogicalPlan,
    MapOp,
    MapStreamOptions,
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
    is_stream_operation,
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
        stages: tuple = (),
    ) -> None:
        self._source = source
        self._plan = plan
        self._executor = executor
        # map_stage 挂载的规范算子实例（run_stream 退出期收尾 aclose 用；
        # 惰性路径不带 stages，终结动作收尾为空操作）
        self._stages = tuple(stages)

    def join(self, other, *, on, right_on=None, how="inner", suffix="_right", chunk_bytes=32*1024*1024):
        """Local equijoin. Checkpoint async inputs first; null keys do not match.

        left joins preserve unmatched rows with right fields absent. semi/anti
        joins preserve left cardinality; inner/left emit all matching pairs.
        The platform plans spillable native relations and Python boundaries;
        callers use Dataset operations without selecting an engine. Join output
        order is unspecified; no final global sort is added. ``chunk_bytes`` remains accepted for
        compatibility; it does not set the native process memory budget.
        """
        if getattr(self._executor, 'NAME', None) == 'local':
            from ..execution.local_kernel import keyed_dataset
            return keyed_dataset(self, 'join', on, right=other, right_on=right_on,
                                 how=how, suffix=suffix, chunk_bytes=chunk_bytes)
        from .local_relational import join
        return join(self,other,on,right_on,how,suffix,chunk_bytes)

    def reduce_by_key(self, on, reducer, *, initial=None, chunk_bytes=32*1024*1024):
        """Group by key with platform-managed spillable native ordering.

        The Python reducer runs in stable group order and owns its accumulator
        size; it is not assumed to be associative. Sorting runs in the native
        engine, while arbitrary reducers retain Python semantics and are not
        rewritten as parallel partial aggregates.
        """
        if getattr(self._executor, 'NAME', None) == 'local':
            from ..execution.local_kernel import keyed_dataset
            return keyed_dataset(self, 'reduce', on, reducer=reducer, initial=initial, chunk_bytes=chunk_bytes)
        from .local_relational import reduce_by_key
        return reduce_by_key(self,on,reducer,initial,chunk_bytes)

    def exclude_keys(self, other, *, on):
        """Exclude matching keys from a local Lance scan, retaining scan order.

        Only keys and internal row IDs enter the managed native relation. Read
        surviving payloads from the same pinned snapshot in bounded batches.
        Duplicate left rows survive independently; null keys do not match.
        This requires an untransformed read_lance scan without SQL projection;
        put row transforms after this filter. It does not collect a Python set
        of all keys or materialize the payload into a join/sort scratch table.
        """
        from .sources import LanceSource
        from ..lance.model import LanceScanSpec
        from .local_relational import keys
        from ..execution.local_kernel import keyed_dataset
        if getattr(self._executor,'NAME',None)!='local':
            raise NotImplementedError('exclude_keys currently supports local Lance scans')
        if (not isinstance(self._source,LanceSource) or not isinstance(self._source.query,LanceScanSpec)
                or self._plan.operations or self._source.query.projection or self._source.native_options is not None):
            raise ValueError('exclude_keys requires an untransformed read_lance scan without projection/native options')
        fields=keys(on)
        if not fields or len(set(fields))!=len(fields) or any(not isinstance(k,str) or not k or k=='_rowid' for k in fields):
            raise ValueError('exclude_keys requires distinct nonempty key names excluding _rowid')
        return keyed_dataset(self,'exclude_keys',fields,right=other.select_columns(fields),how='anti')

    def group_batches(self, on, *, max_rows=32, output="items", chunk_bytes=32*1024*1024,
                      flush_interval=None, max_groups=64, buffer_bytes=64*1024**2):
        """Group rows, optionally using timed streaming microbatches.

        A positive flush_interval selects an online node without a sort/barrier.
        It emits {key fields, output: rows}, at most max_rows per key, on timeout,
        EOF, or bounded-buffer pressure. It cannot claim group_last. chunk_bytes
        and buffer_bytes bound retained Python-object estimates, not process RSS.
        Without flush_interval the existing complete-group semantics are retained.
        """
        if flush_interval is not None:
            import math
            from functools import partial
            from .plan import StreamGroupBatchesOp
            from ..execution.stream_grouping import pack_group
            keys = (on,) if isinstance(on, str) else tuple(on)
            if (not keys or len(keys) > 32 or len(set(keys)) != len(keys)
                    or any(not isinstance(k, str) or not k for k in keys)
                    or not isinstance(output, str) or not output or output in keys):
                raise ValueError('Streaming grouping requires distinct key and output fields')
            if type(flush_interval) not in (int, float) or not math.isfinite(flush_interval) or not 0 < flush_interval <= 3600:
                raise ValueError('Streaming grouping requires a positive finite flush_interval')
            for name, value, ceiling in [('max_rows', max_rows, 10000), ('max_groups', max_groups, 1024),
                    ('chunk_bytes', chunk_bytes, 1024**3), ('buffer_bytes', buffer_bytes, 1024**3)]:
                if type(value) is not int or not 1 <= value <= ceiling:
                    raise ValueError('Invalid streaming grouping bound: ' + name)
            if buffer_bytes < chunk_bytes:
                raise ValueError('buffer_bytes must cover one chunk_bytes batch')
            declared = self.batch_map(partial(pack_group, on=keys, output=output), max_batch=max_rows,
                flush_interval=flush_interval, concurrency=1, queue_depth=1, label='group_batches')
            operation = declared._plan.operations[-1]
            return Dataset(self._source, self._plan.append(StreamGroupBatchesOp(**vars(operation),
                on=keys, output=output, chunk_bytes=chunk_bytes, max_groups=max_groups, buffer_bytes=buffer_bytes)),
                self._executor, self._stages)
        if getattr(self._executor, 'NAME', None) == 'local':
            from ..execution.local_kernel import keyed_dataset
            return keyed_dataset(self, 'groups', on, max_rows=max_rows, output=output, chunk_bytes=chunk_bytes)
        from .local_relational import group_batches
        return group_batches(self,on,max_rows,output,chunk_bytes)

    def map_cached(self, fn, *, cache_dir, version, synchronous=False, cache_when=None):
        """Map with atomic per-input results and separate error records.

        The default accepts async callables. ``synchronous=True`` accepts only
        synchronous callables and runs as a normal map, including in the local
        process kernel. Inputs and outputs must be JSON-compatible. Version plus the entire input defines cache identity.
        Optional cache_when(output) selects completed results to persist/reuse;
        false still delivers the row, e.g. a budget-exhausted item never submitted.
        """
        from .local_relational import CachedMap, CachedSyncMap
        if type(synchronous) is not bool:
            raise TypeError('synchronous must be a boolean')
        if cache_when is not None and not callable(cache_when):
            raise TypeError('cache_when must be a synchronous callable or None')
        if synchronous:
            return self.map(CachedSyncMap(fn, cache_dir, version, cache_when=cache_when))
        return self.map_async(CachedMap(fn,cache_dir,version,cache_when=cache_when))

    def union(self, *others):
        """Concatenate datasets lazily, retaining this Dataset's execution context.

        Local implementation uses native read tasks; does not materialize rows.
        Upstream transforms run under their original contexts. No schema coercion.
        """
        from .api import DataAPI
        from .records import UnionDatasource
        if self._executor.__class__.__module__ != 'demiflow.execution.executors.local':
            raise NotImplementedError('union currently supports local execution')
        if not all(isinstance(ds,Dataset) for ds in others):raise TypeError('union requires Datasets')
        from .plan import reject_streaming_map_options
        for dataset in (self, *others):
            reject_streaming_map_options(dataset._plan, 'local union')
        if getattr(self._executor, '_local_kernel', None) is not None:
            from ..execution.local_kernel import UnionSource, PlanInput
            datasets = (self, *others)
            if any(ds._executor is not self._executor for ds in datasets):
                raise ValueError('parallel union inputs must belong to the same local_execution context')
            inputs = []
            for ds in datasets:
                if isinstance(ds._source, UnionSource) and not ds._plan.operations:
                    inputs.extend(ds._source.inputs)
                else:
                    inputs.append(PlanInput(ds._source, ds._plan))
            return Dataset(UnionSource(tuple(inputs)), LogicalPlan(), self._executor)
        return DataAPI(self._executor).read_datasource(UnionDatasource((self,*others)))

    def checkpoint(self, path, *, version):
        """Execute to an atomic JSONL snapshot and return a replayable Dataset.

        This is a terminal boundary. Reuse requires the same explicit version;
        use map_cached before it to resume expensive per-row work after failure.
        """
        from .local_relational import checkpoint
        return checkpoint(self,path,version)

    async def checkpoint_async(self, path, *, version):
        """Await the native checkpoint from an existing event loop (e.g. Jupyter).

        The synchronous executor runs in a worker thread, including its own loop.
        Row caching, atomic output, version checks and exception propagation are
        identical to checkpoint; this does not add another execution engine.
        """
        import asyncio
        return await asyncio.to_thread(self.checkpoint, path, version=version)

    def checkpoint_lance(
        self, uri: str, *, schema, fingerprint: str,
        storage_options: Optional[Mapping[str, str]] = None,
        max_rows_per_batch: int = 8192,
    ) -> "Dataset":
        """Execute to an atomic Lance snapshot and return a replayable Dataset.

        This is a terminal boundary pinned to the committed Lance version. The
        explicit ``schema`` is enforced on every row batch; a zero-row result
        commits a valid empty table. Completion is registered in a sidecar
        receipt next to the table: replays with the same ``fingerprint``
        reopen the pinned version without re-executing the plan, while a
        different fingerprint at the same location is an error. A receiptless
        existing table at the target is never overwritten: it is either
        recovered through a matching pending receipt or rejected explicitly.
        Concurrent writers are rejected via an advisory file lock; only local
        filesystem targets are supported. Expensive per-row work can resume
        with map_cached before this boundary, as with checkpoint. Plans
        containing async operators are bridged through the streaming
        executor, matching the native JSONL checkpoint behavior.
        """
        from ..lance.checkpoint import checkpoint_lance
        return checkpoint_lance(
            self, uri, schema=schema, fingerprint=fingerprint,
            storage_options=storage_options,
            max_rows_per_batch=max_rows_per_batch,
        )

    async def checkpoint_lance_async(
        self, uri: str, *, schema, fingerprint: str,
        storage_options: Optional[Mapping[str, str]] = None,
        max_rows_per_batch: int = 8192,
    ) -> "Dataset":
        """Await checkpoint_lance from an existing event loop (e.g. Jupyter)."""
        import asyncio
        return await asyncio.to_thread(
            self.checkpoint_lance, uri, schema=schema, fingerprint=fingerprint,
            storage_options=storage_options,
            max_rows_per_batch=max_rows_per_batch,
        )

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
        concurrency: Optional[int] = None,
        queue_depth: Optional[int] = None,
        execution: Optional[str] = None,
        catch: Optional[tuple] = None,
        label: Optional[str] = None,
        callable_scope: Optional[str] = None,
    ) -> "Dataset":
        """Append a synchronous Python row transformation and optional stream policy.

        Without ``inputs``, ``fn`` receives the complete row mapping and its
        returned mapping becomes the complete output row. With ``inputs``, the
        mapping direction is callable argument name to Dataset row field name;
        existing fields are preserved. ``output`` stores the complete callable
        return value in one row field. ``outputs`` maps callable return keys to
        destination row fields. Use at most one of ``output`` and ``outputs``.
        Demiflow plans worker resources and parallelism at action time.

        普通惰性动作按需向上游要行，算子同步组合、无级间队列；
        流式动作使用每级 worker 协程及有界队列，支持级间并发与背压。
        map 接收同步 callable；map_async 接收同步或异步 callable。
        A map in a streaming chain defaults to one inline worker and one queued
        row. Explicit concurrency/queue_depth/execution/catch/label/callable_scope
        select local streaming execution, including a map-only run_stream().
        Ordinary lazy actions and Ray reject explicit streaming options; local
        materialize() and checkpoint actions bridge through run_stream().

        execution='thread' moves synchronous calls to a stage-owned bounded
        thread pool. concurrency defaults to 1, queue_depth to concurrency,
        catch to (), and callable_scope to 'stage'. Stage-scoped callables and
        explicit arguments are shared and must be thread-safe. 'worker' requires
        a callable class: each logical worker lazily constructs its own instance
        using fn_constructor_args/kwargs. Constructors and astart/astop/aclose
        hooks run on the event loop; thread affinity is not guaranteed. Hooks
        must be nonblocking, and thread calls must have their own I/O deadlines.
        Cancellation drains calls before cleanup; threads cannot be killed.

        Concurrent output is unordered. catch only accepts Exception subclasses;
        listed row errors are dropped and counted, other errors stop the stream.
        This API does not add operator checkpoint or hard_timeout support; the
        existing run_stream(checkpoint=...) sink recovery remains available.
        Queues bound row counts, not payload bytes or arbitrary callback memory.
        See docs/streaming-map.md for lifecycle, resource and replay contracts.
        """
        spec = CallableSpec.create(
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        native_options = parse_native_options(backend_options, family="row_transform")
        values = dict(concurrency=concurrency, queue_depth=queue_depth, execution=execution,
                      catch=catch, label=label, callable_scope=callable_scope)
        stream_options = None
        if any(value is not None for value in values.values()):
            stream_options = MapStreamOptions(**{key: value for key, value in values.items() if value is not None})
            stream_options.validate_callable(spec)
            if native_options is not None:
                raise ValueError('map streaming options cannot be combined with backend_options')
        if inputs is None:
            if output is not None or outputs is not None:
                raise TypeError("Dataset.map output(s) require inputs")
            operation = MapOp(spec, native_options, stream_options)
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
                stream_options,
            )
        return Dataset(
            self._source, self._plan.append(operation), self._executor, self._stages,
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
            self._source, self._plan.append(operation), self._executor, self._stages,
        )

    def map_async(
        self,
        fn: Callable[..., Any],
        *,
        concurrency: Optional[int] = None,
        queue_depth: Optional[int] = None,
        catch: Optional[tuple] = None,
        label: Optional[str] = None,
        hard_timeout: Optional[float] = None,
        execution: str = "inline",
        checkpoint=None,
        checkpoint_operator: Optional[str] = None,
        checkpoint_lease_s: float = 300.0,
    ) -> "Dataset":
        """Append one async streaming transformation（**推模型**执行路径）。

        执行模型（两轴定版）：每级 N 个 worker 协程从有界输入队列取行、
        算完推给下一级队列——节点解耦、级间并发、背压由 queue_depth 承载；
        与 map 族（拉模型：终结动作按需向上游要行）相对。"async" 标记
        callable 形态轴：同步/异步函数皆可（awaitable 会被等待）。

        fn(row) -> row | None | list[row]，同步或异步函数均可（awaitable
        会被等待）。None = 认缺丢弃并计数；list = 展开（flat 语义合一，
        不设单独 flat 变体）。concurrency = 该级 worker 数即并发封顶；
        queue_depth = 该级输入缓冲深度（None = concurrency；载字节载荷时
        深度×载荷即内存上界，按需收窄）。catch = 认缺异常白名单：命中只
        计数不断链（网络类瞬态/确定性失败），白名单外异常终止整链。
        用 run_stream() 执行，或 materialize() 固定结果后连接标准 writer；
        普通惰性动作不直接执行异步链。

        输入多态（fn/actor 二元注入，2026-09-07 收敛 map_stage 入此）：
        fn 为裸函数时策略取 kwargs（concurrency 缺省 1）；fn 为 actor
        形态（带齐 label/concurrency/queue_depth/catch/__call__ 的可调用
        类实例，StreamStage 即其继承式基类）时策略随对象，显式 kwargs
        覆盖对象声明；actor 记入计划算子表供 run_stream 退出期 aclose。

        ``execution='thread'`` runs a synchronous blocking callable in a
        stage-owned thread pool, with at most ``concurrency`` calls in flight.
        It preserves contextvars and the row/None/list/catch contract. The
        callable is shared and must be thread-safe. Async callables and
        hard_timeout are rejected: threads cannot be forcibly stopped. Supply
        I/O deadlines inside the callable; cancellation drains running calls
        before actor cleanup. The default 'inline' preserves existing behavior.
        """
        # actor 检测（韧性鸭子）：任一策略属性在场即按 actor 解析，
        # 缺失字段逐项回落默认——普通类漏声明一个字段不会被静默误判
        # 为 fn（策略丢失无告警）；functools.partial 等无策略属性的
        # 可调用对象照常走 fn 路径
        policy_attrs = ("label", "concurrency", "queue_depth", "catch",
                        "hard_timeout")
        is_actor = any(hasattr(fn, a) for a in policy_attrs)
        if is_actor:
            conc = int(concurrency if concurrency is not None
                       else getattr(fn, "concurrency", 1))
            depth = (queue_depth if queue_depth is not None
                     else getattr(fn, "queue_depth", None))
            ctch = tuple(catch) if catch is not None \
                else tuple(getattr(fn, "catch", ()))
            lbl = label or getattr(fn, "label", None) or type(fn).__name__
            hto = (hard_timeout if hard_timeout is not None
                   else getattr(fn, "hard_timeout", None))
        else:
            conc = int(concurrency if concurrency is not None else 1)
            depth = queue_depth
            ctch = tuple(catch) if catch is not None else ()
            lbl = label
            hto = hard_timeout
        if conc < 1:
            raise ValueError("map_async concurrency must be >= 1")
        if depth is not None and int(depth) < 1:
            raise ValueError("map_async queue_depth must be >= 1 or None")
        if execution not in ('inline', 'thread'):
            raise ValueError("map_async execution must be 'inline' or 'thread'")
        if execution == 'thread':
            import inspect
            if hto is not None:
                raise ValueError('thread execution cannot enforce hard_timeout; use callable I/O deadlines')
            if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, '__call__', None)):
                raise TypeError('thread execution requires a synchronous callable')
        if checkpoint is not None:
            if execution != 'inline':
                raise ValueError('operator checkpoint currently requires inline execution')
            from pathlib import Path
            from ..execution.operator_checkpoint import OperatorCheckpoint, CheckpointedCallable
            if isinstance(checkpoint, dict):
                checkpoint_path = checkpoint.get('path')
                checkpoint_operator = checkpoint.get('operator', checkpoint_operator)
                checkpoint_lease_s = checkpoint.get('lease_s', checkpoint_lease_s)
            else:
                checkpoint_path = checkpoint
            if not checkpoint_path:
                raise ValueError('map_async checkpoint requires a path')
            ledger = OperatorCheckpoint(Path(checkpoint_path),
                operator=checkpoint_operator or lbl, lease_s=checkpoint_lease_s)
            fn = CheckpointedCallable(fn, ledger, checkpoint_operator or lbl)
            is_actor = True
        operation = AsyncMapOp(
            CallableSpec.create(fn.__call__ if is_actor else fn),
            conc,
            None if depth is None else int(depth),
            ctch,
            lbl,
            None if hto is None else float(hto),
            execution,
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
            self._stages + (fn,) if is_actor else self._stages,
        )


    def batch_map(
        self,
        fn: Callable[..., Any],
        *,
        max_batch: int,
        flush_interval: Optional[float] = None,
        concurrency: int = 1,
        queue_depth: Optional[int] = None,
        catch: Optional[tuple] = None,
        label: Optional[str] = None,
        hard_timeout: Optional[float] = None,
        checkpoint=None,
        checkpoint_operator: Optional[str] = None,
        checkpoint_lease_s: float = 300.0,
    ) -> "Dataset":
        """Append a batching async transformation（引擎攒批，2026-09-14）。

        fn(list[row]) -> list[row] | None。引擎在级前把连续行攒成批：
        条数满 max_batch / 首行起 flush_interval 秒 / 上游 EOF 三触发；
        批输出经既有 list 展开扇出，批调用异常按 catch 整批计 miss 并
        记入 stats.dead_batches（幂等重跑即重试）。动机与契约详见
        plan.BatchMapOp；惰性路径遇到本算子将显式拒绝（同 AsyncMapOp）。
        """
        if int(max_batch) < 1:
            raise ValueError("batch_map max_batch must be >= 1")
        if checkpoint is not None:
            from pathlib import Path
            from ..execution.operator_checkpoint import OperatorCheckpoint, CheckpointedCallable
            if isinstance(checkpoint, dict):
                checkpoint_path = checkpoint.get('path')
                checkpoint_operator = checkpoint.get('operator', checkpoint_operator)
                checkpoint_lease_s = checkpoint.get('lease_s', checkpoint_lease_s)
            else:
                checkpoint_path = checkpoint
            if not checkpoint_path:
                raise ValueError('batch_map checkpoint requires a path')
            ledger = OperatorCheckpoint(Path(checkpoint_path),
                operator=checkpoint_operator or label or getattr(fn, '__name__', 'batch_map'),
                lease_s=checkpoint_lease_s)
            fn = CheckpointedCallable(fn, ledger, checkpoint_operator or label or 'batch_map')
        operation = BatchMapOp(
            CallableSpec.create(fn.__call__ if checkpoint is not None else fn),
            int(max_batch),
            None if flush_interval is None else float(flush_interval),
            int(concurrency),
            None if queue_depth is None else int(queue_depth),
            tuple(catch) if catch is not None else (),
            label,
            None if hard_timeout is None else float(hard_timeout),
        )
        return Dataset(
            self._source, self._plan.append(operation), self._executor,
            self._stages + ((fn,) if checkpoint is not None else ()),
        )

    # map_stage 已于 2026-09-07 移除：actor 形态输入由 map_async 多态承接
    # （策略随对象、显式 kwargs 覆盖、记入计划算子表供 aclose）。注入
    # 形态二元：map（惰性 fn）与 map_async（流式 fn|actor）。

    def _native_web_node(self, actor, node_type, fields, concurrency, queue_depth, label):
        declared = self.map_async(actor, concurrency=concurrency, queue_depth=queue_depth, label=label)
        operation = declared._plan.operations[-1]
        return Dataset(self._source, self._plan.append(node_type(**vars(operation), **fields)),
                       self._executor, declared._stages)

    def search_web(self, *, requests, output, session, max_candidates=5,
                   request_concurrency=2, concurrency=8, queue_depth=None, when=None,
                   checkpoint=None, checkpoint_operator='search_web', checkpoint_lease_s=300.0,
                   label='search_web'):
        """Search each row's bounded list of {request_id,query,bindings} requests.

        Optional per-request language, pageno, safesearch, time_range, engines,
        categories, engine_data and network are passed to the session. With
        WebSession(search=SearchConfig(...)), bundled sources execute natively.

        Preserve row grain; output is a list of candidate/technical receipts.
        Session is a lazy WebSession shared with fetch nodes in this action.
        Neither snippets nor engine rank are semantic acceptance decisions.
        ``checkpoint`` is an execution ledger path (or mapping with path,
        operator and lease_s); it records each opaque request task and its
        result. It is independent of business sinks such as ``save_lance``.
        """
        from ..collect.operators import SearchWeb
        from .plan import SearchWebOp
        self._validate_web_node(requests, output, when, request_concurrency, max_candidates)
        if not callable(getattr(session,'search',None)) or not callable(getattr(session,'aclose',None)):
            raise TypeError('search_web session must provide search and aclose')
        if checkpoint is not None:
            from pathlib import Path
            if isinstance(checkpoint, dict):
                checkpoint_path = checkpoint.get('path')
                checkpoint_operator = checkpoint.get('operator', checkpoint_operator)
                checkpoint_lease_s = checkpoint.get('lease_s', checkpoint_lease_s)
            else:
                checkpoint_path = checkpoint
            if not checkpoint_path:
                raise ValueError('search_web checkpoint requires a path')
            checkpoint_path = str(Path(checkpoint_path))
        else:
            checkpoint_path = None
        actor = SearchWeb(requests, output, session, when, max_candidates, request_concurrency,
                          checkpoint=checkpoint_path, checkpoint_operator=checkpoint_operator,
                          checkpoint_lease_s=checkpoint_lease_s)
        return self._native_web_node(actor, SearchWebOp,
            dict(requests=requests, output=output, checkpoint=checkpoint_path,
                 checkpoint_operator=checkpoint_operator, checkpoint_lease_s=checkpoint_lease_s),
            concurrency, queue_depth, label)

    def fetch_documents(self, *, requests, output, session, known=None, per_request=2,
                        max_attempts=None, max_new_documents=None, request_concurrency=2,
                        url_concurrency=2, concurrency=8, queue_depth=None, when=None, label='fetch_documents'):
        """Acquire row-local {request_id,urls,bindings} groups into independent objects.

        Optional known column supplies existing document receipts. Reuse merges
        bindings; row-wide quotas reserve in request order. Output contains
        documents, request receipts and touched_urls, never document bodies.
        """
        from ..collect.operators import FetchDocuments
        from .plan import FetchDocumentsOp
        self._validate_web_node(requests, output, when, request_concurrency, url_concurrency)
        if not callable(getattr(session,'fetch',None)) or not callable(getattr(session,'aclose',None)):
            raise TypeError('fetch_documents session must provide fetch and aclose')
        for value in (per_request,max_attempts,max_new_documents):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError('Document quotas must be nonnegative integers or None')
        actor = FetchDocuments(requests, output, session, when, known, per_request,
                               max_attempts,max_new_documents,request_concurrency,url_concurrency)
        return self._native_web_node(actor, FetchDocumentsOp, dict(requests=requests,output=output), concurrency, queue_depth, label)

    def fetch_images(self, *, requests, output, session, request_concurrency=2,
                     max_requests=64, max_request_bytes=256*1024,
                     concurrency=8, queue_depth=None, when=None, label='fetch_images'):
        """Acquire an ordered, bounded list of image requests in each row.

        Requests contain request_id and url or expected sha256; optional
        image_uri supplies a local object and bindings are opaque labels.
        Optional declared_width/declared_height describe this exact URL and
        allow pre-HTTP screening by image_policy.filters; omit if uncertain.
        declared_file_bytes and mime_type similarly describe the selected file.
        ImageFetchPolicy.declared_rejection provides the same pure preflight
        for callers selecting candidates before applying their download quota.
        Return the same request with a result containing an independent image
        reference, dimensions, technical status and acquisition receipt. Search
        pages and thumbnails must be selected explicitly by the consumer.
        Shared content reuse, HTTP/decode limits, cache and cleanup are owned
        by WebSession(image_library=..., image_policy=...). No semantic review.
        """
        from ..collect.image_fetch import FetchImages
        from .plan import FetchImagesOp
        self._validate_web_node(requests, output, when, request_concurrency, max_requests, max_request_bytes)
        if max_requests > 4096 or max_request_bytes > 16*1024*1024:
            raise ValueError('Image request row exceeds supported metadata limits')
        if not callable(getattr(session, 'fetch_image', None)) or not callable(getattr(session, 'aclose', None)):
            raise TypeError('fetch_images session must provide fetch_image and aclose')
        actor = FetchImages(requests, output, session, when, request_concurrency, max_requests, max_request_bytes)
        return self._native_web_node(actor, FetchImagesOp, dict(requests=requests, output=output),
                                     concurrency, queue_depth, label)

    def register_documents(self, *, request, output, library, max_bytes=2*1024*1024,
                           max_document_bytes=8*1024*1024, concurrency=4, queue_depth=None,
                           when=None, label='register_documents', batch_size=1, prepare_workers=4, index_cache_mb=64,
                           publish_pause_s=0.):
        """Register local source documents or exact redirects, without source HTTP.

        Each row supplies document_ref, format='wikitext_sections' with source
        and sections, or format='redirect' with url/target_url. Exact aliases,
        source_id and revision are optional. Verify/copy objects before indexing.
        batch_size>1 prepares sources in prepare_workers processes and publishes
        each batch in one transaction. Output is a technical receipt, not a
        credibility decision. concurrency controls batches in that mode.
        index_cache_mb sets the batch publisher's SQLite page-cache target in MiB,
        not a hard bound on total process memory. publish_pause_s leaves a gap
        without an index transaction between batch commits for shared consumers.
        """
        from ..collect.document_library import DocumentLibrary, RegisterDocuments, BatchRegisterDocuments
        from .plan import RegisterDocumentsOp, RegisterDocumentsBatchOp
        import math
        self._validate_web_node(request, output, when, max_bytes, max_document_bytes,batch_size,prepare_workers,index_cache_mb)
        if type(publish_pause_s) not in (int,float) or not math.isfinite(publish_pause_s) or publish_pause_s<0:
            raise ValueError('publish_pause_s must be finite and nonnegative')
        if publish_pause_s and batch_size==1:
            raise ValueError('publish_pause_s requires batch_size > 1')
        if not isinstance(library, DocumentLibrary):
            raise TypeError('register_documents requires a DocumentLibrary declaration')
        if batch_size>1:
            actor=BatchRegisterDocuments(request,output,library,when,max_bytes,max_document_bytes,
                                         prepare_workers=prepare_workers,index_cache_mb=index_cache_mb,publish_pause_s=publish_pause_s)
            declared=self.batch_map(actor,max_batch=batch_size,concurrency=concurrency,
                                    queue_depth=queue_depth,label=label)
            operation=declared._plan.operations[-1]
            return Dataset(self._source,self._plan.append(RegisterDocumentsBatchOp(
                **vars(operation),request=request,output=output)),self._executor,declared._stages)
        actor = RegisterDocuments(request, output, library, when, max_bytes, max_document_bytes)
        return self._native_web_node(actor, RegisterDocumentsOp, dict(request=request,output=output),
                                     concurrency, queue_depth, label)

    def read_documents(self, *, request, output, context, document_concurrency=2,
                       timeout_s=30, max_bytes=8*1024*1024, concurrency=4, queue_depth=None,
                       when=None, label='read_documents', reuse_workers=0, reuse_worker_max_tasks=128):
        """Verify document objects, select whole blocks and fit the actual prompt.

        Request contains documents/questions/retained/requests/new_tokens/total_tokens.
        PromptContext declares the prompt, TokenBudget and a pure business input
        builder. CharacterBudget instead uses new_chars/total_chars and reports
        material_chars/prompt_chars. Output reports unread ranges and failures.
        Reused isolated workers recycle after reuse_worker_max_tasks operations
        (1..8192, default 128); timeout/cancellation still terminates immediately.
        """
        from ..collect.reading import ReadDocuments, PromptContext
        from .plan import ReadDocumentsOp
        self._validate_web_node(request, output, when, document_concurrency, max_bytes)
        import math
        if type(timeout_s) not in (int,float) or not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError('read_documents timeout_s must be positive and finite')
        from ..operator_llm.tokens import TokenBudget, CharacterBudget
        if not isinstance(context,PromptContext) or not isinstance(context.budget,(TokenBudget, CharacterBudget)) or not callable(context.build_inputs):
            raise TypeError('read_documents requires PromptContext with a pure input builder')
        context.budget.validate_model(context.prompt.model.name)
        if type(reuse_workers) is not int or not 0 <= reuse_workers <= concurrency * document_concurrency:
            raise ValueError('reuse_workers must be 0 or within the node document concurrency')
        if type(reuse_worker_max_tasks) is not int or not 1 <= reuse_worker_max_tasks <= 8192:
            raise ValueError('reuse_worker_max_tasks must be 1..8192')
        actor = ReadDocuments(request,output,context,when,document_concurrency,timeout_s,max_bytes,
                              reuse_workers,reuse_worker_max_tasks)
        return self._native_web_node(actor, ReadDocumentsOp, dict(request=request,output=output), concurrency, queue_depth, label)

    @staticmethod
    def _validate_web_node(request, output, when, *limits):
        if any(not isinstance(v,str) or not v for v in (request,output)) or request == output:
            raise ValueError('Request and output must be distinct nonempty column names')
        if when is not None and not callable(when): raise TypeError('when must be a row predicate')
        if any(type(v) is not int or v < 1 for v in limits): raise ValueError('Concurrency/size limits must be positive integers')

    def enqueue(self, config, *, messages='messages', queue_depth=4):
        """Persist bounded message lists to SQLiteQueue; no broker process.

        Each row has <=257 messages, split into atomic transactions of <=32
        messages / 16 MiB. Stable identities make partial publication replayable.
        The caller opens/seals channels; failure must not pretend producer EOF.
        """
        from ..execution.sqlite_channel import QueuePublisher, channel_config
        actor=QueuePublisher(channel_config(**config),messages)
        return self.map_async(actor,concurrency=1,queue_depth=queue_depth,label='enqueue')

    def ack_queue(self, config, *, result='queue_result', queue_depth=4, keep_row=False):
        """Atomically persist the bounded result and acknowledge its queue task."""
        from ..execution.sqlite_channel import QueueReader, QueueAcknowledger, channel_config
        settings=channel_config(**config)
        readers=[a for a in self._stages if isinstance(a,QueueReader) and a.config==settings]
        if len(readers)!=1:raise ValueError('ack_queue requires its own read_queue source')
        if type(keep_row) is not bool:raise ValueError('keep_row must be boolean')
        actor=QueueAcknowledger(settings,result,readers[0],keep_row=keep_row)
        return self.map_async(actor,concurrency=1,queue_depth=queue_depth,label='ack_queue')

    def admit_rows(self, *, path, key, unique_on=(), quotas=(), output='admission', when=None,
                   max_entries=100000, max_key_bytes=16384, max_disk_bytes=256*1024**2,
                   queue_depth=1):
        """Online durable deduplication and first-arrival quotas, without a group barrier.

        Each quota is {'on': [fields], 'limit': N}; an empty on means global.
        Output retains every row with status admitted/duplicate/limited/skipped.
        Filter admitted explicitly before an external action. Reservations commit
        before delivery and are not refunded after failure. Same-policy replay
        admits the original owners once, regardless of replay order. Only keys
        and accepted counters persist, never row payloads. Bounds fail explicitly;
        SQLite main pages are capped (rollback journal may use the same size).
        The action owns one writer and a bounded set of delivered key digests.
        """
        from ..execution.stream_admission import StreamAdmission
        actor = StreamAdmission(path=path, key=key, unique_on=unique_on, quotas=quotas,
            output=output, when=when, max_entries=max_entries, max_key_bytes=max_key_bytes,
            max_disk_bytes=max_disk_bytes)
        return self.map_async(actor, concurrency=1, queue_depth=queue_depth, label='admit_'+output)

    def deduplicate(self, on, *, path, key, output='deduplication', when=None,
                    keep_duplicates=False, max_entries=100000, max_key_bytes=16384,
                    max_disk_bytes=256*1024**2, queue_depth=1):
        """Online first-arrival uniqueness by configured scalar fields; no quotas.

        key identifies each input row durably. Same-policy replay restores the
        original winning identities once per action, independent of row order;
        external effects still require their own request journal. Rows outside
        when pass through with a skipped receipt. By default duplicate rows are
        filtered; keep_duplicates=True retains them with a duplicate receipt.
        Only key digests persist. max_entries/disk/key bounds fail explicitly,
        rather than changing business eligibility or buffering entire groups.
        """
        from .plan import DeduplicateOp
        if not isinstance(on, (str, list, tuple)) or not 1 <= (1 if isinstance(on, str) else len(on)) <= 32:
            raise ValueError('deduplicate requires 1..32 declared field names')
        fields = (on,) if isinstance(on, str) else tuple(on)
        if any(not isinstance(name, str) or not 1 <= len(name) <= 256 for name in fields):
            raise ValueError('deduplicate field names must contain 1..256 characters')
        if type(keep_duplicates) is not bool:
            raise TypeError('keep_duplicates must be boolean')
        declared = self.admit_rows(path=path, key=key, unique_on=fields, quotas=(),
            output=output, when=when, max_entries=max_entries, max_key_bytes=max_key_bytes,
            max_disk_bytes=max_disk_bytes, queue_depth=queue_depth)
        operation = declared._plan.operations[-1]
        typed = DeduplicateOp(**{**vars(operation), 'label': 'deduplicate_'+output},
                              on=fields, key=key, output=output)
        result = Dataset(self._source, self._plan.append(typed), self._executor, declared._stages)
        return result if keep_duplicates else result.filter(lambda r: r[output]['status'] != 'duplicate')

    def lookup_lance(self, source, *, on, columns=None, output='matches', max_matches=1, max_bytes=1024**2):
        """Attach bounded exact-key matches from a fixed Lance version per row.

        Does not buffer the input stream. A single actor owns a 128-key/8MiB
        result cache. Oversized or ambiguous matches fail explicitly. Limits
        cover returned Arrow/Python payloads, not Lance's internal allocation.
        """
        from ..execution.stream_lookup import LanceLookup
        return self.map_async(LanceLookup(source, on, columns, output, max_matches, max_bytes),
            concurrency=1, queue_depth=1, label='lookup_'+output)

    def prepend(self, other, *, max_rows, max_row_bytes=1024**2):
        """Inject a finite synchronous Dataset into this point of a live stream.

        The prefix is read one row at a time on a reader thread, with downstream
        backpressure. It must contain no streaming actors or external requests.
        This is useful for pending rows from fixed checkpoints; no upstream live
        stage is materialized or restarted. Bounds cover returned Python rows,
        not allocations inside the source reader (configure its scan budgets).
        """
        from .plan import StreamPrependOp
        if not isinstance(other, Dataset) or other._stages or any(
                is_stream_operation(op) for op in other._plan.operations):
            raise ValueError('prepend requires a finite synchronous Dataset without actors')
        if type(max_rows) is not int or not 0 <= max_rows <= 10000000:
            raise ValueError('prepend max_rows must be 0..10000000')
        if type(max_row_bytes) is not int or not 1 <= max_row_bytes <= 64*1024**2:
            raise ValueError('Invalid prepend max_row_bytes')
        node = self.map_async(lambda row: row, concurrency=1, queue_depth=1, label='prepend')
        op = node._plan.operations[-1]
        return Dataset(self._source, self._plan.append(StreamPrependOp(**vars(op),
            source_dataset=other, max_rows=max_rows, max_row_bytes=max_row_bytes)),
            self._executor, self._stages)

    def save_lance(self, uri, *, schema, key, stage, max_batch=32, flush_interval=5., queue_depth=64,
                   output_ref=None, mode='overwrite', when=None, max_rows=1000000,
                   max_row_bytes=16*1024**2, max_key_bytes=4096):
        """Commit verified microbatches and pass rows downstream, with one writer.

        Declaration performs no I/O. run_stream owns target locks, initial empty
        snapshots, tail flush and cleanup. stats.outputs[stage] is a fixed URI /
        version receipt, also exposed to on_drain on a failed action.
        Optional output_ref adds this microbatch's committed uri/version to each
        downstream row. It is not persisted in this table or inferred from head.
        mode='append' retains the existing snapshot. Repeated keys must have
        identical saved content; changed content fails instead of overwriting.
        when selects which rows to persist; other rows pass through unchanged.
        """
        from ..execution.stream_lance import StreamLanceSink
        from .plan import SaveLanceOp
        import math
        if not isinstance(stage,str) or not stage: raise ValueError('save_lance stage is required')
        if type(queue_depth) is not int or queue_depth < 1: raise ValueError('save_lance queue_depth must be positive')
        if type(flush_interval) not in (int,float) or not math.isfinite(flush_interval) or flush_interval <= 0:
            raise ValueError('save_lance flush_interval must be positive and finite')
        actor = StreamLanceSink(uri,schema,key=key,stage=stage,output_ref=output_ref,
            mode=mode, when=when, max_rows=max_rows, max_row_bytes=max_row_bytes,
            max_key_bytes=max_key_bytes)
        declared = self.batch_map(actor.__call__,max_batch=max_batch,flush_interval=flush_interval,
                                  concurrency=1,queue_depth=queue_depth,label='save_'+stage)
        op = declared._plan.operations[-1]
        return Dataset(self._source,self._plan.append(SaveLanceOp(**vars(op),uri=actor.uri,stage=stage,key=key,output_ref=output_ref,mode=mode)),
                       self._executor,self._stages+(actor,))

    def run_stream(
        self,
        *,
        on_progress=None,
        on_drain=None,
        log_every: int = 0,
        cancellation=None,
        queue_factory=None,
        stall_timeout: Optional[float] = None,
        stop_file=None,
        source_batch_size=None,
        checkpoint=None,
    ):
        """驱动含 map_async 或显式流式 map 策略的本地计划至完成。

        同步入口（内部自建事件循环；不要再包在 asyncio.run 里调用）。
        stop_file 出现时停止新投喂，并按 ServiceStopped 收尾已发出的原生模型请求。
        返回 StreamStats（per-stage 计数 + 认缺归集）。on_progress(stats)
        在首级每消费 log_every 行时回调（同步/异步皆可）；on_drain(stats)
        收尾钩子在完成与 Ctrl-C/异常路径都执行——钩子内必须落盘的同步
        写放最前（await 段在中断路径可能被取消截断，契约见 stream.py）。
        """
        from ..execution.stream import run_stream as _run_stream
        if getattr(self._executor, 'NAME', None) != 'local':
            from ..errors import UnsupportedExecutionOptionError
            raise UnsupportedExecutionOptionError('run_stream is only supported by the local executor')
        import inspect
        from ..execution.stream_resources import StreamResources
        from ..execution.sqlite_channel import QueueReader
        queue_source=any(isinstance(actor,QueueReader) for actor in self._stages)
        if source_batch_size is None:source_batch_size=1 if queue_source else 256
        if queue_source and source_batch_size!=1:
            raise ValueError('Queue sources require source_batch_size=1 to avoid waiting for future messages')
        resources = StreamResources(self._stages,queue_factory)
        if checkpoint is not None:
            from ..execution.stream_checkpoint import StreamCheckpoint
            if not isinstance(checkpoint, StreamCheckpoint):
                raise TypeError('checkpoint requires StreamCheckpoint')
            sink_names = {a.stage for a in resources.actors if getattr(a, 'is_stream_sink', False)}
            if set(checkpoint.state['outputs']) - sink_names:
                raise ValueError('Checkpoint stages are missing from the resumed graph')
            for actor in resources.actors:
                if getattr(actor, 'is_stream_sink', False):
                    if actor.mode != 'append':
                        raise ValueError('A checkpoint graph requires append sinks')
                    actor.checkpoint = checkpoint
        if type(source_batch_size) is not int or not 1<=source_batch_size<=256:
            raise ValueError('source_batch_size must be 1..256')
        async def drain(stats):
            resources.snapshot(stats)
            if on_drain is not None:
                result = on_drain(stats)
                if inspect.isawaitable(result): await result

        rows = self._executor.iter_rows(self._source, LogicalPlan())
        return _run_stream(
            rows, self._plan,
            on_progress=on_progress, on_drain=drain, log_every=log_every,
            cancellation=cancellation, queue_factory=resources.make_queue,
            on_start=resources.prepare, on_close=resources.close, stall_timeout=stall_timeout,
            stop_file=stop_file, source_batch_size=source_batch_size,
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
        config: str | Path | PromptPack,
        inputs: Mapping[str, str] | Sequence[str],
        output: Optional[str] = None,
        outputs: Optional[Mapping[str, str]] = None,
        options: Mapping[str, Any] | None = None,
        max_requests: int | None = None,
        backend_options=None,
    ) -> "Dataset":
        """Append a model node with its own configuration and request budget.

        ``config`` is a PromptPack or an actual YAML path, not a registered alias.
        Relative paths use the execution resource directory (cwd for plain scripts).
        ``options`` applies only to this node, in both sync and async execution.
        ``max_requests`` caps new provider requests by this node across its rows,
        workers and schema retries. Replays do not consume requests. Declaring
        another node creates an independent budget, even for the same prompt.

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
        emitted in template order. ``{{ name | numbered_image }}`` accepts the
        same image values and puts ``Image 1:``, ``Image 2:``, etc. directly
        before each image, so references need not rely on counting pictures.

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
        if max_requests is not None and (type(max_requests) is not int or max_requests < 0):
            raise ValueError("max_requests must be a nonnegative integer or None")
        if options is not None and not isinstance(options, Mapping):
            raise TypeError("options must be a mapping or None")
        if isinstance(config, PromptPack):
            pack = config
        elif isinstance(config, (str, Path)) and str(config).strip():
            pack = load_prompt_pack(Path(getattr(self._executor, 'resource_root', Path.cwd())) / config)
        else:
            raise TypeError("config must be a PromptPack or YAML path")
        if output is not None and outputs is not None:
            raise TypeError("Dataset.map_prompt accepts either output or outputs, not both")
        if output is None and outputs is None:
            raise TypeError("Dataset.map_prompt requires output or outputs")
        normalized_output = str(output) if output is not None else None
        if normalized_output == "":
            raise TypeError("Dataset.map_prompt output must be non-empty")
        operation = OperatorLLMMapOp(
            name,
            pack,
            normalize_bound_inputs(inputs),
            normalized_output,
            normalize_outputs(outputs),
            parse_native_options(backend_options, family="row_transform"),
            options=deepcopy(dict(options)) if options is not None else None,
            max_requests=max_requests,
        )
        from ..operator_llm.http_options import validate_prompt_options
        validate_prompt_options(options)
        from ..operator_llm.runtime import validate_prompt_binding
        validate_prompt_binding(operation, pack)
        return Dataset(
            self._source, self._plan.append(operation), self._executor, self._stages,
        )

    def search_vectors(
        self, *, query: str, output: str, uri: str, vector_column: str,
        columns: Sequence[str], top_k: int = 20, version: int | None = None,
        metric: str | None = None, filter: str | None = None,
        filter_column: str | None = None, storage_options=None,
        concurrency: int = 4, queue_depth: int | None = None,
        options: Mapping[str, Any] | None = None, label: str = 'search_vectors',
    ) -> "Dataset":
        """Search Lance once per input row, preserving rows and adding top-k hits.

        query names the input vector column; vector_column names the table's
        vector field. columns explicitly projects hit fields; _distance is added
        by Lance. output is a list sorted by ascending distance (empty when no
        matches), never flattened. Equal-distance tie order is unspecified.
        filter is a fixed SQL prefilter; filter_column optionally supplies a
        per-row SQL prefilter, combined with AND. Invalid input/search fails the
        action rather than dropping a row or pretending it has no matches.

        Each action lazily opens one table snapshot and shares its bounded
        caches across thread workers. version=None pins the head at first use;
        pass an explicit published version for reproducibility. concurrency and
        queue_depth bound in-flight searches and queued rows. options controls
        native ANN parameters (nprobes, refine_factor, HNSW ef) and
        cache/scan/result budgets. Explicit ef must cover top_k * refine_factor;
        max_search_candidates bounds expanded candidates and the HNSW beam. See
        docs/vector_search.md. The node does not create or update indices.

        Local streaming only: chain after map_embeddings and before downstream
        nodes, execute with run_stream() or materialize(). Completion order may
        differ from input order. Native calls drain before cancellation cleanup.
        """
        from ..lance.search import VectorSearch, positive_int
        from .plan import VectorSearchOp
        if getattr(self._executor, 'materialize_stream', None) is None:
            raise NotImplementedError('search_vectors requires the local streaming executor')
        positive_int(concurrency, 'concurrency')
        if queue_depth is not None:
            positive_int(queue_depth, 'queue_depth')
        actor = VectorSearch(query=query, output=output, uri=uri,
            vector_column=vector_column, columns=columns, top_k=top_k,
            version=version, metric=metric, filter=filter,
            filter_column=filter_column, storage_options=storage_options,
            options=options, label=label)
        declared = self.map_async(actor, concurrency=concurrency,
            queue_depth=queue_depth, label=label, execution='thread')
        operation = declared._plan.operations[-1]
        typed = VectorSearchOp(**vars(operation), query=query, output=output,
                               search_spec=actor.spec)
        return Dataset(self._source, self._plan.append(typed), self._executor,
                       declared._stages)

    def map_embeddings(
        self, *, model, inputs, output='embedding', batch_size=16,
        concurrency=1, queue_depth=None, flush_interval=None, prefetch_batches=0,
        call_output=None, error_output=None, options=None, max_requests=None,
        service=None, request_gate=None, request_policy=None, label='map_embeddings',
    ) -> "Dataset":
        """Encode images or text through a native, bounded async embedding node.

        ``model`` is demiflow.embeddings.EmbeddingModel. ``inputs`` maps one of
        image/text to a row column. Images are SHA256 ObjectRefs, optionally a
        list of alternate locations for the same bytes. Existing columns survive;
        output holds a validated float32 vector. error_output retains input
        failures; provider, service, protocol and journal failures stop the action.

        batch_size bounds inputs per HTTP request, concurrency bounds requests,
        queue_depth bounds upstream buffering. prefetch_batches adds bounded batch
        workers for CPU preparation while HTTP is occupied; it does not increase
        request concurrency. options.prepare_workers bounds image preparation;
        options.io_workers bounds journal work; options.response_workers separately
        bounds response parsing/validation. options.batch_request_bytes enables byte packing within each
        row batch; an oversized individual input still obeys max_request_bytes.
        options.batch_decode_pixels additionally splits by aggregate image pixels;
        larger single images still obey max_decode_pixels without being resized.
        Groups run sequentially within a worker and retain independent journal
        identities. request_gate can share fixed or adaptive request
        admission. request_policy declares an operator-owned adaptive gate and
        is mutually exclusive with request_gate. GPU placement remains explicit in service=VLLMService(...) or
        ManagedHTTPService(...); None uses an external endpoint. Managed services
        start only on a journal miss and close at action exit. SQLite request
        journals preserve exact batches; uncertain calls require explicit recovery.

        Execute with run_stream(), or materialize() before relational work/writers.
        Local streaming only. See docs/embeddings.md for options and examples.
        """
        from ..embeddings.runtime import EmbeddingActor
        from ..embeddings import embedding_execution_config
        from ..inference import _resolve_request_gate
        from .plan import EmbeddingMapOp
        if getattr(self._executor, 'prompt_actor', None) is None:
            raise NotImplementedError('map_embeddings currently requires the local streaming executor')
        execution = embedding_execution_config(batch_size=batch_size, concurrency=concurrency,
            queue_depth=queue_depth, flush_interval=flush_interval, prefetch_batches=prefetch_batches,
            max_requests=max_requests, request_policy=request_policy, options=options)
        request_gate = _resolve_request_gate(policy=execution['request_policy'],
                                             gate=request_gate, concurrency=concurrency)
        actor = EmbeddingActor(model=model, inputs=inputs, output=output,
            call_output=call_output, error_output=error_output, concurrency=concurrency,
            label=label, options=execution['options'], max_requests=max_requests,
            service=service, request_gate=request_gate)
        declared = self.batch_map(actor.__call__, max_batch=batch_size,
            concurrency=concurrency + prefetch_batches, queue_depth=queue_depth,
            flush_interval=flush_interval, label=actor.label)
        operation = declared._plan.operations[-1]
        typed = EmbeddingMapOp(**vars(operation), model_contract=actor.model.contract(),
                               inputs=actor.inputs, output=output)
        return Dataset(self._source, self._plan.append(typed), self._executor,
                       self._stages + (actor,))

    def document_embeddings(self, *, model, document='document_input', **kwargs) -> "Dataset":
        """Encode verified normalized-document spans through map_embeddings.

        ``document`` names a column containing document_ref (ObjectRef), spans
        (block_id/start/end Unicode offsets), prefix, and exact text_sha256.
        The action's bounded preparation pool reads and verifies objects, retains
        a bounded block cache, and reuses native batching, admission, HTTP,
        journals, cancellation and managed services. No body text is added to
        result rows. All map_embeddings execution/output options are accepted.
        """
        return self.map_embeddings(model=model, inputs={'document': document}, **kwargs)

    def map_image_async(self, *, template, model, inputs, output, call_output,
                        error_output, journal_path, object_store, max_requests,
                        when=None, concurrency=1, queue_depth=1, limits=None,
                        service=None, image_encoding='png', label='map_image_async'):
        """Render a versioned template and generate one image per row.

        Template is {name, version, template}; it uses the standard prompt
        renderer. Model selects diffusers or an explicit image HTTP API.
        Inputs bind row fields to placeholders; images are bounded binary lists.
        image_encoding='preserve' keeps validated single-frame JPEG/PNG/WebP
        bytes; other formats become PNG. Default 'png' retains legacy encoding.
        The actor owns transport/model lifetime, exact input artifacts,
        finite persistent request budget and uncertain-call protection.
        Optional service=ManagedHTTPService(...) owns a lazy HTTP deployment;
        HTTP requests may overlap, while direct Diffusers execution is serial.
        No retries or provider fallbacks. Local streaming only; use separate
        nodes/processes with explicitly placed GPUs for parallel model arms.
        See docs/image_generation.md for resource and cancellation boundaries.
        """
        from ..image_generation import ImageGenerator
        from .plan import ImageMapOp
        if getattr(self._executor, 'prompt_actor', None) is None:
            raise NotImplementedError('map_image_async requires local streaming')
        if type(concurrency) is not int or not 1 <= concurrency <= 8 or type(queue_depth) is not int or not 1 <= queue_depth <= 8:
            raise ValueError('Image actor concurrency and queue_depth must be in 1..8')
        if model.get('backend') == 'diffusers' and concurrency != 1:
            raise ValueError('Diffusers actor concurrency must be 1; use explicitly placed independent GPU nodes')
        actor = ImageGenerator(template=template, model=model, inputs=inputs,
            output=output, call_output=call_output, error_output=error_output,
            journal_path=journal_path, object_store=object_store,
            max_requests=max_requests, when=when, limits=limits, service=service,
            image_encoding=image_encoding)
        declared = self.map_async(actor, concurrency=concurrency, queue_depth=queue_depth, label=label)
        operation = declared._plan.operations[-1]
        typed = ImageMapOp(**vars(operation), template=actor.spec,
                           model_contract=actor.config, inputs=actor.inputs, output=output,
                           image_encoding=actor.image_encoding)
        return Dataset(self._source, self._plan.append(typed), self._executor, declared._stages)

    def map_prompt_async(
        self, prompt: str, *, config: str | Path | PromptPack,
        inputs: Mapping[str, str] | Sequence[str],
        output: Optional[str] = None, outputs: Optional[Mapping[str, str]] = None,
        concurrency: int = 1, queue_depth: Optional[int] = None,
        catch: tuple = (), label: Optional[str] = None,
        when=None, call_output: Optional[str] = None, error_output: Optional[str] = None,
        options: Mapping[str, Any] | None = None, max_requests: int | None = None,
        service=None, request_gate=None, request_policy=None, token_budget=None,
        routes=None, route=None, isolate_route_failures=False,
    ) -> "Dataset":
        """Native streaming prompt actor, sharing map_prompt's prompt-pack contract.

        Same template, model, inputs/output(s), JSON schema and schema_retries.
        Adds map_async worker/queue/error policies. Execute with run_stream()
        or checkpoint(); checkpoint before later joins or lazy transforms.
        Configuration and limits belong to this node. Non-local executors
        currently reject this async interface explicitly. HTTP is truly asynchronous; transport
        errors are not silently retried. Repeated actions can repeat calls.
        options={'offline_dir': ...} materializes the same model context
        for external authors and validates submitted responses through this
        actor. Missing responses produce PromptResponsePending; no HTTP call
        or provider-budget reservation occurs in offline mode.

        ``options={'sqlite_journal': {'path': 'calls.sqlite', 'timeout_s': 30}}``
        durably reuses exact HTTP requests without logging image base64. Call
        metadata includes request_id/journal_path; lookup and commits stay off
        the event loop. Unanswered reservations are not silently retried.

        ``options={'stream': True}`` receives SSE internally and publishes one
        complete, schema-validated response per row. Default False retains the
        ordinary JSON request. One HTTP client/pool is shared by this node;
        default max_connections equals concurrency. connect_timeout_s,
        read_timeout_s (idle gap), write_timeout_s and pool_timeout_s are distinct
        from timeout_s (total HTTP exchange). Truncated streams are journaled
        as uncertain failures without retries. ``gateway='litellm'`` explicitly
        sends zero-retry/no-fallback controls. Generic endpoints receive none.
        See docs/prompt_http.md for bounds, logs and recovery semantics.

        ``request_gate`` shares admission/circuit state across nodes for new calls.
        ``request_policy`` declares an operator-owned adaptive gate; supply one
        of these two interfaces. Its maximum cannot exceed node concurrency.
        ``token_budget`` validates exact rendered messages, including schema, before
        journal lookup/reservation. Neither policy changes the request cache key.

        ``service=VLLMService(...)`` or ``ManagedHTTPService(...)`` declares a node-owned local server. None
        keeps the external endpoint behavior. The actor starts it only before
        an uncached request and closes it when this stream action exits, including
        failure/cancellation. Service failures abort the node, even with
        error_output. Service placement is not part of request/cache identity.
        Managed services require local async execution and cannot be combined
        with offline or alternate transports. Two nodes in the same stream can
        overlap; materialize between models that must reuse the same GPUs.

        ``routes={key: {config, options, concurrency, max_requests, ...}}``
        and ``route='column'`` dispatch assigned rows inside one worker pool.
        Total concurrency and each route's concurrency are both bounded. The
        caller assigns rows upstream; the platform does not choose a model or
        retry a failed row on another model. Child actors retain native journal,
        lifecycle and metrics. ``isolate_route_failures=True`` records provider
        stop/service failures in error_output; user stop and storage failures
        still stop the action. Upstream assignment must bound route backlog to
        avoid filling all workers with one route's waiting rows.
        """
        if routes is not None:
            from ..operator_llm.routed import RoutedPromptActor
            if (not isinstance(routes, dict) or not 1 <= len(routes) <= 32
                    or not isinstance(route, str) or not route):
                raise ValueError('Prompt routes require 1..32 declarations and a route column')
            if type(isolate_route_failures) is not bool:
                raise ValueError('isolate_route_failures must be boolean')
            actors, limits = {}, {}
            allowed = {'config', 'options', 'concurrency', 'max_requests', 'service',
                       'request_gate', 'request_policy', 'token_budget'}
            for name, spec in routes.items():
                if not isinstance(name, str) or not name or not isinstance(spec, dict) or set(spec)-allowed:
                    raise ValueError('Invalid prompt route declaration')
                capacity = spec.get('concurrency', concurrency)
                if type(capacity) is not int or not 1 <= capacity <= 256:
                    raise ValueError('Prompt route concurrency must be 1..256')
                params = dict(config=config, options=options, concurrency=concurrency,
                    max_requests=max_requests, service=service, request_gate=request_gate,
                    request_policy=request_policy, token_budget=token_budget)
                params.update(spec)
                declared = self.map_prompt_async(prompt, **params, inputs=inputs, output=output,
                    outputs=outputs, when=when, call_output=call_output, error_output=error_output,
                    queue_depth=1, label=(label or prompt)+':'+name)
                actors[name] = declared._stages[-1]
                actors[name].label = (label or prompt)+':'+name
                limits[name] = capacity
            if type(concurrency) is not int or not 1 <= concurrency <= 512:
                raise ValueError('Routed prompt total concurrency must be 1..512')
            actor = RoutedPromptActor(actors, limits, route, isolate_failures=isolate_route_failures)
            declared = self.map_async(actor, concurrency=concurrency, queue_depth=queue_depth,
                catch=catch, label=label)
            return Dataset(declared._source, declared._plan, declared._executor,
                           declared._stages+tuple(actors.values()))
        if route is not None or isolate_route_failures:
            raise ValueError('route/isolate_route_failures require routes')
        from ..inference import _resolve_request_gate
        request_gate = _resolve_request_gate(policy=request_policy, gate=request_gate, concurrency=concurrency)
        if request_gate is not None and not callable(getattr(request_gate, 'enter', None)):
            raise TypeError('request_gate must provide an async admission context')
        if token_budget is not None and not callable(getattr(token_budget, 'validate', None)):
            raise TypeError('token_budget must validate actual OperatorLLMRequest messages')
        if service is not None:
            from ..services import VLLMService, ManagedHTTPService
            if not isinstance(service, (VLLMService, ManagedHTTPService)):
                raise TypeError('service must be VLLMService, ManagedHTTPService or None')
            if any(key in (options or {}) for key in ('offline_dir', 'offline_store', 'codex_exec', 'codex_agent')):
                raise ValueError('Managed service requires online HTTP prompt execution')
        declared = self.map_prompt(prompt, config=config, inputs=inputs,
                                   output=output, outputs=outputs, options=options,
                                   max_requests=max_requests)
        factory = getattr(self._executor, 'prompt_actor', None)
        if factory is None:
            raise NotImplementedError('map_prompt_async currently requires the local streaming executor')
        if when is not None and not callable(when):raise TypeError('when must be a row predicate')
        if any(v is not None and (not isinstance(v,str) or not v) for v in (call_output,error_output)):
            raise TypeError('call_output/error_output must be nonempty column names')
        destinations=set((outputs or {}).values()) | ({output} if output else set())
        extra=[v for v in (call_output,error_output) if v]
        if len(set(extra))!=len(extra) or destinations.intersection(extra):raise ValueError('Prompt output columns must be distinct')
        operation = declared._plan.operations[-1]
        if token_budget is not None and callable(getattr(token_budget,'validate_model',None)):
            token_budget.validate_model(operation.config.prompt_definitions[operation.prompt_name].model.name)
        actor = factory(operation) if service is None else factory(operation, service=service)
        if not any(key in (options or {}) for key in ('offline_dir', 'offline_store', 'codex_exec', 'codex_agent')):
            from ..operator_llm.http_options import validate_http_options
            validate_http_options({**(options or {}),
                'max_connections': (options or {}).get('max_connections', concurrency)})
        actor._runtime.request_gate = request_gate
        actor._runtime.token_budget = token_budget
        actor._runtime.concurrency = concurrency
        actor.when=when;actor.call_output=call_output;actor.error_output=error_output
        return self.map_async(actor, concurrency=concurrency, queue_depth=queue_depth,
                              catch=catch, label=label)

    def agentmap_async(self, prompt: str, *, config, **kwargs) -> "Dataset":
        """One row -> model/operator interactions -> one final business result.

        config is one demiflow_agent_v2 YAML path or resolved AgentConfig.
        It owns tasks, model, runtime, tools and budgets. The node supplies row
        mappings, scheduling and journal/replay storage; max_requests may only
        tighten the configured ceiling. No separate environment is accepted.
        map_prompt_* retains its separate demiflow_prompt_pack_v2 entry.

        For runtime=demiflow, set options.stream=true in the agent config to
        receive HTTP SSE on every model turn, including tool continuations and
        schema repairs. This reuses map_prompt_async's transport, timeouts,
        bounded assembly and journal; one complete business result is emitted
        per row. Omitted/false keeps JSON HTTP. Node options are storage-only;
        do not put stream in node options or request_options. Codex owns its
        own transport and does not accept these HTTP options.
        """
        from ..agent import AgentConfig, load_agent_config
        if 'environment' in kwargs:
            raise TypeError('agentmap_async accepts one agent config; move environment into config')
        if isinstance(config, (str, Path)):
            config = load_agent_config(Path(getattr(self._executor, 'resource_root', Path.cwd())) / config)
        if not isinstance(config, AgentConfig):
            raise TypeError('agentmap_async config must be AgentConfig or demiflow_agent_v2 YAML')
        environment = config.environment
        limit = kwargs.get('max_requests')
        if limit is not None and (type(limit) is not int or limit < 0):
            raise ValueError('max_requests must be a nonnegative integer')
        kwargs['max_requests'] = config.max_requests if limit is None else min(limit, config.max_requests)
        kwargs['config'] = config.prompt_pack
        kwargs['options'] = config.node_options(kwargs.get('options'))
        options = kwargs['options']
        if environment.runtime == 'codex':
            if kwargs.get('max_requests') is None:
                raise ValueError('Codex agent requires a finite max_requests session budget')
            options.setdefault('codex_agent', {})
            from ..operator_llm.codex_agent import validate_options
            validate_options(options)
            if kwargs.get('service') is not None:
                raise ValueError('Codex runtime cannot use a managed HTTP service')
            kwargs['options'] = options
        elif 'codex_agent' in options:
            raise ValueError('options.codex_agent requires environment.runtime=codex')
        # Declaration only: reuse the same node/client/coordinator/lifecycle.
        # The newly-created actor is private to this returned immutable plan.
        declared = self.map_prompt_async(prompt, **kwargs)
        actor = declared._stages[-1]
        environment.prompt(actor._prompt)  # validate generated schema before execution
        actor.environment = environment
        actor.label = kwargs.get('label') or 'agent:' + prompt
        from .plan import AgentMapOp
        operation = declared._plan.operations[-1]
        agent_op = AgentMapOp(**{**vars(operation), 'label': actor.label}, environment=environment)
        return Dataset(self._source, self._plan.append(agent_op), self._executor, declared._stages)

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
            self._source, self._plan.append(operation), self._executor, self._stages,
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
        callables, or Operator LLM requests. Local async chains use run_stream
        with the same bounded queues, error handling and actor cleanup. The
        synchronous action must run outside an active event loop (or in a thread).
        The handle is current-run only and
        must not be returned from ``PipelineProgram.run``.
        """
        with observe_action("materialize", self._source, self._plan, self._executor) as observer:
            if any(is_stream_operation(op) for op in self._plan.operations):
                # 异步链复用本地流式执行器；缓存由 executor 管理，不把行交回业务层收集。
                execute = getattr(self._executor, "materialize_stream", None)
                if execute is None:
                    raise NotImplementedError("async materialize is only supported by the local executor")
                handle = execute(self)
            else:
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
        mode: str = "append", schema=None,
        on=None, update_columns=None, when_not_matched=None, return_receipt=False,
        backend_options=None,
    ):
        """将 Dataset 以追加、覆盖或部分列 merge 提交到 Lance。

        ``mode='create'`` 只创建不存在的表，原子拒绝已存在的表，不接受 expected_version。
        ``mode='append'`` 保留默认的创建/追加行为，已有表要求 schema 一致；
        ``mode='overwrite'`` 用本次数据及 schema 替换当前内容，旧版本仍可读取。
        显式 ``schema`` 约束 Arrow 类型，并允许无数据时创建或覆盖为空表；
        无 schema 的空输入仍报错。``expected_version`` 在提交时检查并发冲突；
        compare-and-append 仍要求至少一行。返回值为 ``None``，提交结果不确定时
        抛出带回执的 ``LanceWriteError``，不自动重试或去重。
        这是同步 Dataset 的终结动作；异步算子仍通过 ``run_stream()`` 执行。

        ``mode='merge'`` 按 ``on`` 主键更新已有表，只更新输入中的非主键列；
        ``update_columns`` 可显式限定列，输入多列或缺列时报错。未提交列保留，
        显式 null 清空。键要求唯一且非空。``when_not_matched`` 默认 error，
        也可 ignore 或 insert；插入时省略的目标列为 null。新增字段须先使用
        ``data.add_lance_columns``。整个 Dataset 一次提交，不按 worker 提交。
        ``return_receipt=True`` 返回含实际固定版本的写入回执，默认仍返回 None。
        """
        from ..lance.model import LanceWriteSpec

        spec = LanceWriteSpec(
            uri=uri, expected_version=expected_version,
            storage_options=storage_options, mode=mode, schema=schema,
            on=on or (), update_columns=update_columns, when_not_matched=when_not_matched,
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
        return receipt if return_receipt else None

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

    def release(self):
        """Release cached rows/spills after the last consumer finishes.

        Derived plans share this cache and cannot be executed after release.
        Releasing does not rerun inputs or affect unrelated materializations.
        """
        release = getattr(self._executor, 'release', None)
        if release is None:
            raise NotImplementedError('This executor does not support explicit cache release')
        release(self._source)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.release()

    def num_blocks(self) -> int:
        """Return the number of pinned blocks in this current-run materialized Dataset."""
        return self._executor.num_blocks(self._source, self._plan)


def _write_options(options, filesystem, backend_options):
    if filesystem is not None: options["filesystem"] = filesystem
    native=parse_native_options(backend_options, family="sink")
    if native is not None: options["_demiflow_native_options"] = native
