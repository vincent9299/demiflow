"""demiflow 本地流式执行：async/batch 算子及显式流式 map 策略。

设计（collect 能力融合一期；拓扑与 collect_v2.chain 同构）：
- 常驻 worker 协程组 + 有界 asyncio.Queue：无序发射（吞吐优先）、
  sentinel 逐级排空、run-to-completion；
- 单事件循环承载全部算子：进程级客户端/限速闸门（如消费方 collect_v2.infra）
  保持单 loop 单例语义，不碎片化；
- 认缺分级：AsyncMapOp.catch 白名单内异常只计数不断链；白名单外异常
  经 watchdog 收敛后终止整链（真异常语义，与 chain 的 annotate/sink 同口径）；
- async 算子接收同步/异步 fn；显式流式 map 只接收同步 fn；
  返回 None=认缺丢弃、list=展开；
- FilterOp 折叠：async 算子之间的 filter 变成前级算子的输出后过滤
  （首级之前则是输入前过滤）；其余 sync 算子（limit/select/sort...）
  在 streaming 计划中显式拒绝——流式与惰性两条路径各司其职；
- 取消/中断：Ctrl-C → 停止投喂、finally 执行 on_drain 收尾钩子；
  钩子契约：必须落盘的同步写放钩子最前（await 段在中断路径可能被
  取消截断，进程退出时未关客户端无数据损失风险）；
- 队列和在途调用按行数有界，不能据此承诺字节或 RSS 上限。峰值还包括
  reader 预读、源块、每个 worker 的输入/输出和临时对象、writer 缓冲。
  具体所有权与限制见 docs/streaming-map.md。
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Mapping

from ..data.plan import AsyncMapOp, BatchMapOp, BoundMapOp, FilterOp, MapOp, FlatMapOp, SelectColumnsOp, DropColumnsOp, RenameColumnsOp, LogicalPlan, StandardCallable, StreamGroupBatchesOp, StreamPrependOp, is_stream_operation
from .stream_map import StreamMapRuntime
from ..data.runtime_contract import RuntimeContractError, validator
from .request_limits import ServiceStopped

from ..errors import StallError

from .row_scope import ScopedRow, RowScope, set_row_scope

SENTINEL = object()


from .stream_barrier import _CheckpointBarrier, CheckpointControl

_FEED_CHUNK = 256            # 源头同步迭代器的分块拉取粒度（每块一次线程切换）
_WATCHDOG_INTERVAL = 0.2     # 真异常收敛巡检周期（秒）
_DRAIN_TIMEOUT = 2.0         # 取消后等待在飞任务平息的上限（秒）



class StreamStats:
    """流式执行计数（引擎口径；业务口径由管线在算子闭包内自持）。"""

    def __init__(self) -> None:
        self.stages: dict[str, dict[str, int]] = {}
        self.timings = {}
        self.metrics = {}
        self.stage_policies = {}
        self.outputs = {}
        self.miss: dict[str, int] = {}
        self.dead_batches: list = []      # 攒批整批失败内容（活性层，有界）
        self.dead_batches_dropped: int = 0  # 超 1000 批后的只计数不留存

    def stage(self, name: str) -> dict[str, int]:
        return self.stages.setdefault(name, {"in": 0, "emitted": 0})

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    def observe_duration(self, name, seconds):
        from .request_limits import LatencySummary
        if name not in self.timings:
            self.timings[name] = LatencySummary()
        self.timings[name].observe(seconds)

    def timing_summary(self):
        return {name: value.summary() for name, value in self.timings.items()}

    @property
    def emitted(self) -> int:
        """末级产出行数（管线最终输出；per-stage 细账看 stage(name)/stages）。"""
        if hasattr(self, '_committed_output'):
            return self._committed_output
        if not self.stages:
            return 0
        return next(reversed(self.stages.values()))["emitted"]

    def summary(self) -> str:
        st = "、".join(f"{k}[in={v['in']}→out={v['emitted']}]"
                      for k, v in self.stages.items())
        miss = "、".join(f"{k}×{v}" for k, v in
                         sorted(self.miss.items(), key=lambda x: -x[1]))
        return st + (f"\n认缺：{miss}" if miss else "")


class _Stage:
    __slots__ = ("name", "fn", "concurrency", "queue_depth", "catch",
                 "pre_filters", "post_filters", "hard_timeout",
                 "max_batch", "flush_interval", "execution", "executor", "pending", "flat_map", "grouping", "prefix", "validation", "observation", "recovery_buffer", "native_recovery", "native_snapshot", "batch_ordinal", "scope_id", "native_pending", "native_restore", "native_input_lock", "native_emit_lock", "native_idle",
                 "operation_index", "stage_budget", "buffer_adapter")

    def __init__(self, name, fn, concurrency, queue_depth, catch,
                 pre_filters, post_filters, hard_timeout=None,
                 max_batch=None, flush_interval=None, execution='inline', flat_map=False):
        self.operation_index = None
        self.stage_budget = None
        self.name = name
        self.fn = fn
        self.concurrency = concurrency
        self.queue_depth = queue_depth
        self.catch = catch
        self.pre_filters = pre_filters
        self.post_filters = post_filters
        self.hard_timeout = hard_timeout
        self.max_batch = max_batch          # 非 None 即攒批级（BatchMapOp）
        self.flush_interval = flush_interval
        self.execution = execution
        self.executor = None
        self.pending = set()
        self.flat_map=flat_map
        self.grouping = None
        self.prefix = None
        self.validation = None
        self.observation = None
        self.recovery_buffer = None
        self.native_recovery = None
        self.native_snapshot = False
        self.native_pending = {}
        self.native_restore = []
        self.native_input_lock = asyncio.Lock()
        self.native_emit_lock = asyncio.Lock()
        self.native_idle = asyncio.Event()
        self.native_idle.set()
        self.batch_ordinal = 0
        self.scope_id = None
        self.buffer_adapter = None


def _materialize(plan: LogicalPlan, *, stage_limits=None) -> list[_Stage]:
    """Lower async actors and their row-wise synchronous maps to bounded stages."""
    from .capabilities import validate_stream_plan
    validate_stream_plan(plan)
    stages: list[_Stage] = []
    head_filters: list[Callable] = []
    from .checked import current
    checked = current()
    for op_index, op in enumerate(plan.operations):
        count_before = len(stages)
        if isinstance(op, AsyncMapOp):
            name = op.label or op.callable.name
            stages.append(_Stage(
                name, StreamMapRuntime(op) if op.callable.is_class else op.callable.instantiate(), op.concurrency,
                op.queue_depth or op.concurrency, op.catch,
                head_filters if not stages else [], [],
                op.hard_timeout, execution=op.execution))
            if not op.callable.is_class:
                stages[-1].validation = validator(stages[-1].fn, op.callable.runtime_contract)
            if isinstance(op, StreamPrependOp):
                stages[-1].prefix = op
            head_filters = []
        elif isinstance(op, BatchMapOp):
            name = op.label or op.callable.name
            stages.append(_Stage(
                name, StreamMapRuntime(op) if op.callable.is_class else op.callable.instantiate(), op.concurrency,
                op.queue_depth or op.concurrency, op.catch,
                head_filters if not stages else [], [],
                op.hard_timeout, op.max_batch, op.flush_interval))
            if not op.callable.is_class:
                stages[-1].validation = validator(stages[-1].fn, op.callable.runtime_contract)
            if isinstance(op, StreamGroupBatchesOp):
                stages[-1].grouping = op
            head_filters = []
        elif isinstance(op, FilterOp):
            pred = StandardCallable(op.callable)
            if checked is not None:
                checked.progress(checked.operation_bindings.get(id(op)), rows_input=0, rows_output=0, calls=0)
            if stages:
                stages[-1].post_filters.append(pred)
            else:
                head_filters.append(pred)
        elif isinstance(op, (MapOp, BoundMapOp)) and op.stream_options is not None:
            options = op.stream_options
            stages.append(_Stage(options.label or op.callable.name, StreamMapRuntime(op),
                                 options.concurrency, options.queue_depth or options.concurrency,
                                 options.catch, head_filters if not stages else [], [],
                                 execution=options.execution))
            head_filters = []
        elif isinstance(op, SelectColumnsOp):
            def project(row,columns=op.columns):
                return {name:row[name] for name in columns}
            stages.append(_Stage('select_columns',project,1,1,(),head_filters if not stages else [],[]))
            head_filters=[]
        elif isinstance(op, DropColumnsOp):
            def drop(row,columns=op.columns):
                missing=set(columns)-row.keys()
                if missing:raise KeyError(f'columns not found: {sorted(missing)}')
                return {k:v for k,v in row.items() if k not in columns}
            stages.append(_Stage('drop_columns',drop,1,1,(),head_filters if not stages else [],[]))
            head_filters=[]
        elif isinstance(op, RenameColumnsOp):
            def rename(row,names=op.names):
                targets=[names.get(k,k) for k in row] if isinstance(names,Mapping) else names
                if len(targets)!=len(row) or len(set(targets))!=len(targets):
                    raise ValueError('rename_columns must preserve width and unique names')
                return dict(zip(targets,row.values()))
            stages.append(_Stage('rename_columns',rename,1,1,(),head_filters if not stages else [],[]))
            head_filters=[]
        elif isinstance(op, BoundMapOp) or isinstance(op,MapOp) and not inspect.isfunction(op.callable.target):
            stages.append(_Stage(op.callable.name,StreamMapRuntime(op),1,1,(),head_filters if not stages else [],[]))
            head_filters=[]
        elif isinstance(op, (MapOp,FlatMapOp)):
            # Pure row validation/application can follow a native prompt actor;
            # callers should not have to disguise it as asynchronous work.
            stages.append(_Stage(op.callable.name, StandardCallable(op.callable), 1, 1, (),
                                 head_filters if not stages else [], [],flat_map=isinstance(op,FlatMapOp)))
            head_filters = []
        else:
            raise ValueError(
                f"streaming 路径支持 map_async、batch_map、map、flat_map 与 filter，"
                f"遇到 {type(op).__name__}（惰性动作走 take/write 系列则不支持本算子组合）")
        if len(stages) > count_before:
            stages[-1].operation_index = op_index
            if checked is not None:
                stages[-1].observation = checked.operation_bindings.get(id(op))
                checked.progress(stages[-1].observation, rows_input=0, rows_output=0, calls=0)
    if not stages:
        raise ValueError("run_stream 需要至少一个 map_async 算子")
    if stage_limits is not None:
        for stage in stages:
            if stage.operation_index is not None and stage.operation_index in stage_limits:
                stage.stage_budget = stage_limits[stage.operation_index]
    # Repeated functions/prompts are distinct nodes. Collapsing their names
    # merges counts and incorrectly pairs per-node queues with statistics.
    used = set()
    for stage in stages:
        original, index = stage.name, 1
        while stage.name in used:
            index += 1
            stage.name = f'{original}#{index}'
        used.add(stage.name)
    return stages


async def _call(fn, row):
    out = fn(row)
    if inspect.isawaitable(out):
        out = await out
    return out


def _call_sync(fn, row):
    out = fn(row)
    if inspect.isawaitable(out):
        if inspect.iscoroutine(out):
            out.close()
        raise TypeError('thread execution callable returned an awaitable')
    return out


async def _call_stage(stage, row, fn=None):
    fn = stage.fn if fn is None else fn
    if stage.execution == 'inline':
        return await _call(fn, row)
    if stage.executor is None:
        # Per-stage resources keep blocked image reads away from journal I/O,
        # the source feeder and other stages. Workers bound submissions.
        stage.executor = ThreadPoolExecutor(
            max_workers=stage.concurrency, thread_name_prefix='demiflow-stream-' + stage.name)
    context = contextvars.copy_context()
    future = asyncio.get_running_loop().run_in_executor(
        stage.executor, context.run, _call_sync, fn, row)
    stage.pending.add(future)
    def completed(done):
        stage.pending.discard(done)
        # A cancelled worker may no longer await a late failure. Retrieve it
        # without changing what the original waiter observes.
        if not done.cancelled():
            done.exception()
    future.add_done_callback(completed)
    # Cancelling a coroutine must not claim the underlying thread has stopped.
    return await asyncio.shield(future)


async def _drain_threads(stages):
    for stage in stages:
        if stage.executor is not None:
            try:
                await asyncio.gather(*tuple(stage.pending), return_exceptions=True)
            finally:
                stage.executor.shutdown(wait=True, cancel_futures=True)
                stage.executor = None


def _predicate(predicate, row):
    from .checked import current
    checked = current()
    contract = predicate._spec.runtime_contract if isinstance(predicate, StandardCallable) else None
    key = contract.get('observation') if contract else None
    if checked is not None:
        checked.progress(key, calls=1, rows_input=1, rows_output=0)
    try:
        result = predicate(row)
    except BaseException:
        if checked is not None:
            checked.progress(key, errors=1)
        raise
    if checked is not None:
        checked.progress(key, rows_output=int(bool(result)), dropped=int(not result))
    return result


def _post_filter(filters, out):
    """输出后过滤：保持 None/list 语义，全滤掉返回 None。"""
    if out is None or not filters:
        return out
    outs = out if isinstance(out, list) else [out]
    kept = [r for r in outs if all(_predicate(p, r) for p in filters)]
    if not kept:
        return None
    return kept if isinstance(out, list) else kept[0]


def _make_worker(stage: _Stage, q_in: asyncio.Queue, q_out,
                 stats: StreamStats, on_progress, log_every: int, consume=None, unit_driver=None,
                 worker_slot=0):
    name = stage.name
    map_worker = stage.fn.new_worker() if isinstance(stage.fn, StreamMapRuntime) else None
    from .checked import current
    checked = current()
    from .run_context import current_run
    run = current_run()
    batch_limits = checked.limits if checked is not None else run.limits if run is not None else None
    # Opaque stages already align every worker before forwarding a barrier.
    # Only native in-flight snapshots need an additional emission gate.
    emission_gate = stage.native_emit_lock if stage.native_snapshot else contextlib.nullcontext()

    def progress(**counts):
        if checked is not None:
            checked.progress(stage.observation, **counts)

    def check_row(row):
        if checked is not None:
            from ..data.runtime_contract import check_size
            checked.check_stop()
            check_size(row, checked.limits['max_row_bytes'])

    # Each worker has one invocation in flight. Keep deadline provenance local
    # to that worker; TimeoutError alone says nothing about who raised it.
    hard_timeout_expired = False

    async def invoke(value, *, batch=False):
        nonlocal hard_timeout_expired
        hard_timeout_expired = False
        started = time.monotonic()
        try:
            if stage.hard_timeout is None:
                return await _call_stage(stage, value, map_worker)
            callback_timeout = None

            async def call():
                nonlocal callback_timeout
                try:
                    return await _call_stage(stage, value, map_worker)
                except asyncio.TimeoutError as exc:
                    callback_timeout = exc
                    raise

            try:
                return await asyncio.wait_for(call(), stage.hard_timeout)
            except asyncio.TimeoutError as exc:
                # wait_for forwards callback exceptions, including TimeoutError
                # during cancellation. Only its own expiry is a runtime miss.
                hard_timeout_expired = exc is not callback_timeout
                raise
        finally:
            # Service/gate time belongs to the operator; input queue waiting,
            # batch collection and downstream backpressure do not.
            elapsed = time.monotonic() - started
            stats.observe_duration(name, elapsed)
            if checked is not None:
                checked.duration(stage.observation, elapsed)

    async def worker():
        st = stats.stage(name)
        if stage.max_batch is not None:
            return await batch_worker(st)   # 攒批级：走批循环
        while True:
            native_key = None
            if stage.native_snapshot:
                async with stage.native_input_lock:
                    row = await q_in.get()
                    if isinstance(row, _CheckpointBarrier):
                        if row.final:
                            await stage.native_idle.wait()
                        async with stage.native_emit_lock:
                            await row.arrive(stage)
                        continue
                    if row is not SENTINEL:
                        native_key = id(asyncio.current_task())
                        pending_row = row
                        if stage.pre_filters or getattr(stage.native_recovery, 'copy_inputs', False):
                            # A Python predicate may mutate its input. Preserve
                            # its original value so recovery applies it once.
                            from copy import deepcopy
                            check_row(row.row)
                            copying = asyncio.create_task(asyncio.to_thread(deepcopy, row.row))
                            try:
                                original_input = await asyncio.shield(copying)
                            except asyncio.CancelledError:
                                await copying
                                raise
                            pending_row = ScopedRow(original_input, row.scope)
                        stage.native_pending[native_key] = pending_row
                        stage.native_idle.clear()
            else:
                row = await q_in.get()
            if row is SENTINEL:
                return
            if isinstance(row, _CheckpointBarrier):
                pending = None
                adapter = stage.buffer_adapter
                if adapter is not None and adapter.kind == 'prepend':
                    pending = {"position": adapter.prefix_position,
                               "exhausted": adapter.prefix_exhausted}
                await row.arrive(stage, pending, worker_slot=worker_slot)
                continue
            scope = row.scope if type(row) is ScopedRow else None
            row = row.row if type(row) is ScopedRow else row
            set_row_scope(scope)
            check_row(row)
            st["in"] += 1
            if any(not _predicate(p, row) for p in stage.pre_filters):
                stats.add_miss(f"{name}:filtered")
            else:
                if map_worker is not None:
                    # Constructor/start failures are lifecycle failures, never
                    # row misses even if catch includes Exception.
                    await map_worker.prepare()
                if stage.validation is not None:
                    stage.validation.inputs((row,), {})
                progress(calls=1, rows_input=1, rows_output=0)
                try:
                    out = await invoke(row)
                except (RuntimeContractError, ServiceStopped):
                    progress(errors=1)
                    raise
                except stage.catch as exc:   # noqa: PERF203 - 认缺白名单
                    progress(errors=1, dropped=1)
                    stats.add_miss(f"{name}:{type(exc).__name__}")
                except asyncio.TimeoutError:
                    if not hard_timeout_expired:
                        progress(errors=1)
                        raise
                    stats.add_miss(f"{name}:hard_timeout")   # 活性层：必醒
                    progress(errors=1, dropped=1)
                else:
                    async with emission_gate:
                        if isinstance(out, list) and batch_limits is not None:
                            from ..data.runtime_contract import check_size
                            check_size(out, batch_limits['max_batch_bytes'])
                        if scope is not None and scope.delivery is not None and type(out) is not dict:
                            raise RuntimeContractError('Task callbacks must return exactly one record; drop/expansion cannot settle a task')
                        if stage.validation is not None:
                            stage.validation.output(out)
                        if stage.flat_map:
                            # Pull one child at a time under downstream backpressure;
                            # never turn an arbitrary expansion iterator into a list.
                            children=iter(out)
                            try:
                                for index,child in enumerate(children):
                                    check_row(child)
                                    if unit_driver is not None:unit_driver.observe_expansion(name, child)
                                    progress(rows_output=1)
                                    if index and index%256==0:await asyncio.sleep(0)
                                    if not all(_predicate(p, child) for p in stage.post_filters):
                                        stats.add_miss(f"{name}:filtered")
                                        continue
                                    if q_out is not None:await q_out.put(ScopedRow(child,scope.child(index)) if scope else child)
                                    elif consume is not None:
                                        set_row_scope(scope.child(index) if scope else None)
                                        await _call(consume, child)
                                        set_row_scope(scope)
                                    st["emitted"]+=1
                            finally:
                                close=getattr(children,'close',None)
                                if close is not None:close()
                        else:
                            progress(rows_output=0 if out is None else len(out) if isinstance(out, list) else 1)
                            out = _post_filter(stage.post_filters, out)
                            if out is None:
                                stats.add_miss(f"{name}:drop")
                            else:
                                rows = out if isinstance(out, list) else (out,)
                                for child_index,r in enumerate(rows):
                                    child_scope = scope.child(child_index) if scope and isinstance(out,list) else scope
                                    check_row(r)
                                    if unit_driver is not None:unit_driver.observe_expansion(name, r)
                                    if q_out is not None:
                                        await q_out.put(ScopedRow(r,child_scope) if child_scope else r)
                                    elif consume is not None:
                                        set_row_scope(child_scope)
                                        await _call(consume, r)
                                        set_row_scope(scope)
                                    st["emitted"] += 1
                        if native_key is not None:
                            stage.native_pending.pop(native_key)
                            if not stage.native_pending:
                                stage.native_idle.set()
            if native_key is not None and native_key in stage.native_pending:
                async with stage.native_emit_lock:
                    stage.native_pending.pop(native_key)
                    if not stage.native_pending:
                        stage.native_idle.set()
            # 进度钩子挂首级消费口径（chain 的「实例进度」同位）
            if (log_every and stage is FIRST_STAGE_REF.get()
                    and st["in"] % log_every == 0 and on_progress is not None):
                await _call(on_progress, stats)

    async def _emit(out, st):
        """批输出的展开/后滤/下发（与逐行级同语义）。"""
        if out is None:
            return
        outs = out if isinstance(out, list) else [out]
        from .row_scope import current_row_scope
        scope = current_row_scope() if stage.scope_id is not None else None
        for index, r in enumerate(outs):
            child_scope = scope.child(index) if scope is not None else None
            check_row(r)
            if not all(_predicate(p, r) for p in stage.post_filters):
                continue
            if q_out is not None:
                await q_out.put(ScopedRow(r, child_scope) if child_scope else r)
            elif consume is not None:
                set_row_scope(child_scope)
                await _call(consume, r)
            st["emitted"] += 1

    def _record_dead(batch, reason):
        if len(stats.dead_batches) < 1000:
            if checked is not None:
                # Managed diagnostics retain counts, not arbitrary business
                # payload. Durable domain evidence remains in its own ledger.
                stats.dead_batches.append({'stage': name[:128], 'reason': reason[:128],
                                           'row_count': len(batch), 'payload': 'not_retained'})
            else:
                stats.dead_batches.append({"stage": name, "reason": reason, "rows": batch})
        else:
            stats.dead_batches_dropped += 1

    async def batch_worker(st):
        """攒批级 worker：条数满/时间窗/EOF 三触发，fn(list)->list。"""
        if stage.grouping is not None:
            await grouped_worker(st)
            return
        loop = asyncio.get_running_loop()
        adapter = stage.buffer_adapter
        if adapter is not None:
            restored, _ordinal = adapter.take_restored(worker_slot)
            restored = restored or []
        else:
            restored = stage.recovery_buffer or []
            stage.recovery_buffer = None
        while True:
            batch, restored, deadline = restored, [], None
            replay_size = (adapter.replay_batch_size(worker_slot)
                           if adapter is not None and stage.native_recovery is not None else None)
            if batch and stage.flush_interval is not None:
                # A restored remainder restarts its advisory flush window; the
                # durable contract is the rows and ordinal, not the old deadline.
                deadline = loop.time() + stage.flush_interval
            from ..data.runtime_contract import check_size
            batch_bytes = 64 + sum(check_size(row, batch_limits['max_row_bytes']) + 16 for row in batch) if batch_limits else 64
            while True:
                timeout = None
                if batch and stage.flush_interval is not None and replay_size is None:
                    timeout = deadline - loop.time()
                    if timeout <= 0:
                        break                        # 时间窗到，刷出
                try:
                    row = await asyncio.wait_for(
                        adapter.read(q_in, worker_slot) if adapter is not None else q_in.get(),
                        None if timeout is None else timeout)
                except asyncio.TimeoutError:
                    break                            # 时间窗到，刷出
                if row is SENTINEL:
                    if batch:
                        await _flush(batch)
                    return                           # 尾批已刷，退役
                if isinstance(row, _CheckpointBarrier):
                    if row.final and batch:
                        await _flush(batch)
                        batch, batch_bytes = [], 64
                    await row.arrive(stage, batch, worker_slot=worker_slot)
                    if stage.native_recovery is not None:
                        # Buffered effects have drained at this cut. Do not
                        # reserve the next paid batch while its publication can
                        # still fail; recovery must start at this exact boundary.
                        await row.wait()
                    continue
                row = row.row if type(row) is ScopedRow else row
                check_row(row)
                if any(not _predicate(p, row) for p in stage.pre_filters):
                    stats.add_miss(f'{name}:filtered')
                    continue
                if batch_limits is not None:
                    from ..data.runtime_contract import check_size
                    batch_bytes += check_size(row, batch_limits['max_row_bytes']) + 16
                    if batch_bytes > batch_limits['max_batch_bytes']:
                        raise RuntimeContractError('Stream batch byte budget exceeded')
                if not batch:
                    if stage.flush_interval is not None:
                        deadline = loop.time() + stage.flush_interval
                batch.append(row)
                st["in"] += 1
                if (log_every and stage is FIRST_STAGE_REF.get()
                        and st["in"] % log_every == 0
                        and on_progress is not None):
                    await _call(on_progress, stats)
                if len(batch) >= (replay_size or stage.max_batch):
                    break                            # 条数满，刷出
            if batch:
                await _flush(batch)

    async def grouped_worker(st):
        from .stream_grouping import group_key, row_size
        op, groups, retained = stage.grouping, {}, 0
        loop = asyncio.get_running_loop()
        adapter = stage.buffer_adapter
        if adapter is not None:
            restored_groups, _ordinal = adapter.take_restored(worker_slot)
            for key, rows, deadline_ms in restored_groups or []:
                size = sum(row_size(r, op.chunk_bytes) for r in rows)
                groups[key] = [list(rows), size, loop.time() + max(0, deadline_ms) / 1000]
                retained += size

        async def flush(key):
            nonlocal retained
            rows, size, deadline = groups.pop(key)
            retained -= size
            await _flush(rows)

        def snapshot_groups():
            now = loop.time()
            return [(key, rows, max(0, int((deadline - now) * 1000)))
                    for key, (rows, _size, deadline) in groups.items()]

        while True:
            now = loop.time()
            for key in [k for k, (_, _, deadline) in groups.items() if deadline <= now]:
                await flush(key)
            timeout = max(0., min(g[2] for g in groups.values()) - loop.time()) if groups else None
            try:
                row = await asyncio.wait_for(q_in.get(), timeout)
            except asyncio.TimeoutError:
                continue
            if row is SENTINEL:
                for key in list(groups):
                    await flush(key)
                return
            if isinstance(row, _CheckpointBarrier):
                if row.final:
                    for key in list(groups):
                        await flush(key)
                    pending = []
                else:
                    pending = snapshot_groups()
                await row.arrive(stage, pending, worker_slot=worker_slot)
                continue
            st['in'] += 1
            if any(not _predicate(predicate, row) for predicate in stage.pre_filters):
                stats.add_miss(f'{name}:filtered')
                continue
            size = row_size(row, op.chunk_bytes)
            key = group_key(row, op.on)
            if key in groups and groups[key][1] + size > op.chunk_bytes:
                await flush(key)
            while groups and (retained + size > op.buffer_bytes or
                              key not in groups and len(groups) >= op.max_groups):
                await flush(next(iter(groups)))
            if key not in groups:
                groups[key] = [[], 0, loop.time() + stage.flush_interval]
            groups[key][0].append(row); groups[key][1] += size; retained += size
            if len(groups[key][0]) >= stage.max_batch:
                await flush(key)

    async def _flush(batch):
        if stage.scope_id is not None:
            adapter = stage.buffer_adapter
            if adapter is not None:
                ordinal = adapter.next_ordinal(worker_slot)
                if stage.native_recovery is not None:
                    adapter.seal_batch_size(worker_slot, ordinal, len(batch))
                set_row_scope(RowScope(adapter.scope_identity(worker_slot, ordinal)))
            else:
                from .recovery_contract import identity
                set_row_scope(RowScope(identity(['fixed-batch', stage.scope_id, stage.batch_ordinal])))
                stage.batch_ordinal += 1
        st = stats.stage(name)
        if map_worker is not None:
            await map_worker.prepare()
        if stage.validation is not None:
            stage.validation.inputs((batch,), {})
        progress(calls=1, rows_input=len(batch), rows_output=0)
        try:
            out = await invoke(batch, batch=True)
        except (RuntimeContractError, ServiceStopped):
            progress(errors=1)
            raise
        except stage.catch as exc:
            progress(errors=1, dropped=len(batch))
            stats.add_miss(f"{name}:{type(exc).__name__}")
            _record_dead(batch, type(exc).__name__)
        except asyncio.TimeoutError:
            if not hard_timeout_expired:
                progress(errors=1)
                raise
            progress(errors=1, dropped=len(batch))
            stats.add_miss(f"{name}:hard_timeout")
            _record_dead(batch, "hard_timeout")
        else:
            if stage.validation is not None:
                stage.validation.output(out)
            progress(rows_output=0 if out is None else len(out) if isinstance(out, list) else 1)
            if out is None:
                stats.add_miss(f"{name}:drop")
                _record_dead(batch, "drop")
            else:
                await _emit(out, st)

    return worker


FIRST_STAGE_REF = contextvars.ContextVar('stream_first_stage', default=None)


def _local_queue(depth: int):
    """进程内有界队列（默认传输实现；D2 的分布式实现替换此工厂）。"""
    return asyncio.Queue(maxsize=depth)


async def _arun(source_iter, stages, stats, *, on_progress, on_drain,
                log_every, cancellation, queue_factory=None,
                stall_timeout=None, stop_file=None, source_batch_size=_FEED_CHUNK, consume=None,
                progress=None, unit_driver=None, on_stop=None) -> None:
    from pathlib import Path
    from .request_limits import ServiceStopped
    from .checked import current
    checked = current()
    stop_path = Path(stop_file) if stop_file is not None else None
    if stop_path is not None and await asyncio.to_thread(stop_path.exists):
        raise ServiceStopped('operator_stop_file')
    make_queue = queue_factory or _local_queue
    queues = []
    for s in stages:
        if s.stage_budget is not None:
            item_bytes = s.stage_budget.get('output_bytes')
            total_bytes = s.stage_budget.get('inflight_bytes')
            max_bytes = total_bytes if item_bytes is not None else None
            if queue_factory is None:
                from .request_limits import ObservedQueue
                queues.append(ObservedQueue(
                    s.queue_depth, max_item_bytes=item_bytes, max_bytes=max_bytes))
            else:
                queues.append(make_queue(
                    s.queue_depth, max_item_bytes=item_bytes, max_bytes=max_bytes))
        else:
            queues.append(make_queue(s.queue_depth))
    from .raw_fetch import RawFetch
    from ..collect.operators import SearchWeb
    raw_occurrences = any(type(getattr(s.fn, '__self__', s.fn)) in {RawFetch,SearchWeb} for s in stages)
    track_occurrences = raw_occurrences or any(s.native_recovery is not None for s in stages)
    if raw_occurrences and any(s.max_batch is not None or s.grouping is not None or s.prefix is not None for s in stages):
        raise RuntimeContractError('Raw acquisition requires a row-local stream; materialize buffered transformations first')
    if progress is not None:
        progress.stats = stats
    if progress is not None:
        from .stage_buffers import classify_stage_buffer, PROVEN_KINDS
        for s in stages:
            kind = classify_stage_buffer(s)
            if kind is not None and kind not in PROVEN_KINDS:
                raise RuntimeContractError('Automatic stream progress requires row-local stages; grouped state needs its own commit contract')
    for stage in stages:
        stats.stage(stage.name)
        stats.stage_policies[stage.name] = {
            'concurrency': stage.concurrency, 'queue_depth': stage.queue_depth,
            'execution': stage.execution,
            'callable_scope': (stage.fn.scope
                               if isinstance(stage.fn, StreamMapRuntime) else 'stage'),
            'catch': [f'{exc.__module__}.{exc.__qualname__}' for exc in stage.catch],
        }
    FIRST_STAGE_REF.set(stages[0])
    # An opt-in astop hook releases execution-owned resources when the actor's
    # last node finishes, before unrelated downstream work drains. Shared actor
    # objects may occur at several nodes; never stop one after its first node.
    from collections import Counter
    owners = {id(stage): getattr(stage.fn, '__self__', stage.fn) for stage in stages}
    remaining = Counter(id(owner) for owner in owners.values())
    stopped = set()

    async def stop_stage(stage):
        key = id(stage)
        if key in stopped:
            return
        stopped.add(key)
        owner = owners[key]
        remaining[id(owner)] -= 1
        stop = getattr(owner, 'astop', None)
        if remaining[id(owner)] == 0 and stop is not None:
            completed = False
            try:
                result = stop()
                if inspect.isawaitable(result):
                    await result
                completed = True
            finally:
                if checked is not None:
                    checked.actor_lifecycle(owner, 'stopped', completed)

    control = None

    async def feed():
        it = iter(source_iter)
        feed_context = contextvars.copy_context()
        ordinal = 0
        for index, stage in enumerate(stages):
            for restored in stage.native_restore:
                await queues[index].put(restored)
            stage.native_restore = []
        while True:
            if checked is not None:
                checked.check_stop()
            if progress is not None and progress.stop_requested():
                raise RuntimeContractError("Terminal preparation stopped")
            if cancellation is not None and cancellation.requested:
                if unit_driver is not None:raise ServiceStopped('producer_cancelled_before_eos')
                break
            chunk = await asyncio.to_thread(
                feed_context.run, lambda: [r for _, r in zip(range(source_batch_size), it)])
            if not chunk:
                break
            for row in chunk:
                if unit_driver is not None:unit_driver.begin_unit()
                source_ordinal = ordinal
                ordinal += 1
                if progress is not None and source_ordinal < progress.inputs:
                    progress.select(row)
                    continue
                if track_occurrences and type(row) is not ScopedRow:
                    from .recovery_contract import identity
                    row = ScopedRow(row, RowScope(identity(['source-occurrence',source_ordinal])))
                if control is not None:
                    from ..data.runtime_contract import check_size
                    amount = check_size(row.row if type(row) is ScopedRow else row,
                                        progress.run.limits['max_row_bytes'])
                    await control.admit(amount)
                    async with control.gate:
                        await queues[0].put(row)
                        progress.select(row)
                        control.accepted(progress.ordinal, amount)
                    # Let a due trigger acquire the gate without stopping data
                    # for the lifetime of the coordinating checkpoint.
                    if control.changed.is_set():
                        await asyncio.sleep(0)
                else:
                    await queues[0].put(row)
                if unit_driver is not None:
                    boundary = _CheckpointBarrier(stages, queues, unit_driver.end_unit, track=track_commit)
                    for _ in range(boundary.tokens(stages[0])):await queues[0].put(boundary)
                    await boundary.wait()
        if unit_driver is not None:await unit_driver.end_source()
        # Restore-only emissions precede the final cut. An actor exposing this
        # hook must see its live inputs drained before it enumerates recovery.
        recovery = [(index, getattr(owners[id(stage)], 'recovery_rows', None))
                    for index, stage in enumerate(stages)]
        if control is not None and any(hook is not None for _, hook in recovery):
            await control.finish()
        emitted_recovery = False
        for index, recover in recovery:
            if recover is None:
                continue
            rows = recover()
            if inspect.isawaitable(rows):
                rows = await rows
            for row in rows or ():
                emitted_recovery = True
                if stages[index].max_batch is not None and isinstance(row, list):
                    for item in row:
                        await queues[index].put(item)
                else:
                    await queues[index].put(row)
        if control is not None:
            if control.finished.is_set() and emitted_recovery:
                boundary = _CheckpointBarrier(stages, queues, None, progress=progress,
                                              final=True, track=track_commit)
                boundary.next_ordinal = progress.ordinal
                control.pending = boundary
                for _ in range(boundary.tokens(stages[0])):
                    await queues[0].put(boundary)
                await boundary.wait()
            else:
                await control.finish()

    commit_tasks = set()
    def track_commit(task):
        commit_tasks.add(task)
        all_tasks.append(task)
        def retire(completed):
            # Failed tasks remain visible to the watchdog until shutdown.
            if not completed.cancelled() and completed.exception() is None:
                commit_tasks.discard(completed)
                all_tasks.remove(completed)
        task.add_done_callback(observe_completion)
        task.add_done_callback(retire)

    stage_tasks: list[list[asyncio.Task]] = []
    for i, s in enumerate(stages):
        q_out = queues[i + 1] if i + 1 < len(stages) else None
        stage_tasks.append([asyncio.create_task(
            _make_worker(s, queues[i], q_out, stats, on_progress, log_every, consume, unit_driver,
                         worker_slot=slot)())
            for slot in range(s.concurrency)])
    if progress is not None:
        control = CheckpointControl(progress, stages, queues, track_commit)
        bind_control = getattr(progress, "_bind_checkpoint_control", None)
        if bind_control is not None:
            bind_control(control)
    control_tasks = [asyncio.create_task(control.run(), name='demiflow-checkpoint-control')] if control else []
    feed_task = asyncio.create_task(feed())
    all_tasks = [feed_task, *control_tasks, *(t for g in stage_tasks for t in g)]
    # Watchdog reports the first failure, but several workers may fail together
    # (e.g. a shared budget). Observe every completed task, including late errors
    # after cancellation; retrieving does not suppress propagation below.
    def observe_completion(task):
        if not task.cancelled():
            error = task.exception()
            if error is not None and not fatal:
                fatal.append(error)
                _cancel_all()
    for task in all_tasks:
        task.add_done_callback(observe_completion)
    fatal: list[BaseException] = []

    def service_stop():
        from .request_limits import ServiceStopped
        if not fatal:
            return None
        if isinstance(fatal[0], ServiceStopped):
            return fatal[0]
        # A failed sibling (including local storage) must stop scheduling, but
        # must not abandon an already paid model exchange. Preserve the actual
        # fatal exception for the caller; this reason is only a drain signal.
        return ServiceStopped('pipeline_failed:' + type(fatal[0]).__name__)

    def _cancel_all() -> None:
        if control is not None:
            control.fail(fatal[0] if fatal else asyncio.CancelledError('stream stopped'))
        if on_stop is not None:on_stop()
        for t in all_tasks:
            if not t.done() and not t.cancelling():
                # Stop row scheduling immediately. Native journaled model
                # requests recognize this reason and finish their existing
                # bounded exchange before relinquishing the cancelled worker.
                t.cancel(service_stop())

    async def watchdog():
        """真异常收敛 + 零吞吐停摆检测（活性层，2026-09-14）。

        停摆口径：全局进度（各级 in+emitted 之和）在 stall_timeout 秒内
        零变化 → StallError，message 携带挂起任务栈转储与 net 闸门超龄
        持有诊断。历史四次夜跑静默凝固皆属此类（沿革见 errors.StallError）。
        """
        last_progress, last_tick = -1, time.monotonic()
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL)
            if checked is not None:
                try:
                    checked.check_stop()
                    checked.flush_observations()
                except BaseException as exc:
                    fatal.append(exc)
                    _cancel_all()
                    return
            if progress is not None and progress.stop_requested():
                fatal.append(ServiceStopped('terminal_preparation_stopped'))
                _cancel_all()
                return
            if control is not None:
                try:
                    control.check()
                except BaseException as exc:
                    fatal.append(exc)
                    _cancel_all()
                    return
            if stop_path is not None and await asyncio.to_thread(stop_path.exists):
                fatal.append(ServiceStopped('operator_stop_file'))
                _cancel_all()
                return
            for t in all_tasks:
                if t.cancelled():
                    fatal.append(asyncio.CancelledError('stream worker cancelled'))
                    _cancel_all()
                    return
                if t.done() and not t.cancelled() and t.exception() is not None:
                    fatal.append(t.exception())
                    _cancel_all()
                    return
            if stall_timeout is None:
                continue
            stage_progress = sum(v["in"] + v["emitted"]
                           for v in stats.stages.values())
            now = time.monotonic()
            if stage_progress != last_progress:
                last_progress, last_tick = stage_progress, now
                continue
            if now - last_tick < stall_timeout:
                continue
            fatal.append(StallError(_stall_dump(all_tasks)))
            _cancel_all()
            return

    def _stall_dump(tasks) -> str:
        """停摆现场：挂起任务栈 + net 闸门持有诊断（活性层取证包）。"""
        parts = ["pipeline stalled: task stacks follows"]
        for t in tasks:
            if t.done():
                continue
            frames = t.get_stack(limit=6)
            if not frames:
                continue
            # 纯属性拼行：format_stack 的 linecache 在循环内对挂起帧
            # 会挂死（实测），file:line:func 足够取证
            loc = " <- ".join(
                f"{f.f_code.co_filename.rsplit('/', 1)[-1]}:"
                f"{f.f_lineno}:{f.f_code.co_name}" for f in frames)
            parts.append(f"--- {t.get_name()}: {loc}")
        try:
            _net = __import__("sys").modules.get("demiflow.collect.net")
            # 事件循环内 import 会死锁(实测),故只取已加载引用——
            # 真实采集管线 net 必已常驻,诊断可用;纯流式用例静默跳过
            if _net is not None:
                stale = _net.gate_stalls()
                if stale:
                    parts.append("net gates held: " + repr(stale))
        except Exception:
            pass
        return "\n".join(parts)[:20000]

    wd = asyncio.create_task(watchdog())
    try:
        try:
            await feed_task
        except asyncio.CancelledError:
            # Watchdog cancellation reports its cause; external cancellation
            # must propagate so callers cannot commit a partial materialization.
            if fatal:
                raise fatal[0]
            raise
        if fatal:
            raise fatal[0]
        if feed_task.done() and not feed_task.cancelled() \
                and feed_task.exception() is not None:
            raise feed_task.exception()
        # 源头投喂完毕 → sentinel 逐级注入、逐级 join（chain.py 同款收尾）。
        # 活性层（2026-09-14）：drain 的 sentinel put 可能因工人被看门狗
        # 取消而永久阻塞（满队列无消费者）——put/gather 均走有界等待，
        # 超时且 fatal 非空时立即上抛（原引擎潜伏死角，流式停摆场景首证）。
        async def _abort_check():
            if fatal:
                raise fatal[0]

        for i, group in enumerate(stage_tasks):
            for _ in group:
                while True:
                    try:
                        await asyncio.wait_for(queues[i].put(SENTINEL), 2)
                        break
                    except asyncio.TimeoutError:
                        await _abort_check()
            while True:
                done, _ = await asyncio.wait(set(group), timeout=2)
                if len(done) == len(group):
                    break
                await _abort_check()
            # A stop may cancel every coroutine while their thread calls are
            # still draining. Cancelled tasks have no exception(), so checking
            # only task exceptions would turn a stopped action into success.
            await _abort_check()
            ex = next((t.exception() for t in group
                       if not t.cancelled() and t.exception() is not None), None)
            if ex is not None:
                raise ex
            if any(t.cancelled() for t in group):
                raise asyncio.CancelledError('stream worker cancelled')
            await stop_stage(stages[i])
    except BaseException as exc:
        from .request_limits import ServiceStopped
        if isinstance(exc, Exception) and not fatal:
            fatal.append(exc)
        _cancel_all()
        with contextlib.suppress(BaseException):
            timeout = _DRAIN_TIMEOUT
            if service_stop():
                # Materialized actor stages expose a bound __call__ method;
                # the bounded shutdown contract belongs to its owner.
                timeout = max([timeout] + [
                    getattr(getattr(stage.fn, '__self__', stage.fn),
                            'cancel_drain_timeout_s', 0)
                    for stage in stages
                ])
            from .run_context import current_run
            run = current_run()
            if run is not None:
                timeout = min(timeout, run.limits['cancel_grace_s'])
            _, pending = await asyncio.wait(all_tasks, timeout=timeout)
            if pending:
                for task in pending:
                    task.cancel()  # Hard deadline: ordinary cancellation.
                await asyncio.wait(pending, timeout=_DRAIN_TIMEOUT)
        raise
    finally:
        wd.cancel()
        drain = asyncio.create_task(_drain_threads(stages))
        try:
            await asyncio.shield(drain)
        except asyncio.CancelledError:
            await drain
            raise
        # Cancellation/failure also releases every started network actor. The
        # action-level aclose remains an idempotent fallback for partial start.
        stop_errors = []
        for stage in stages:
            try:
                await stop_stage(stage)
            except BaseException as exc:
                stop_errors.append(exc)
        # Release per-stage staging buffers on every exit path. Committed state
        # references are owned by StateArtifacts/CommitCoordinator and untouched.
        for stage in stages:
            adapter = getattr(stage, "buffer_adapter", None)
            if adapter is not None:
                adapter.close()
        if on_drain is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(_call(on_drain, stats))
        if stop_errors:
            raise BaseExceptionGroup('Stream actor stop failed', stop_errors)


def run_stream(source_iter, plan: LogicalPlan, *,
               on_progress=None, on_drain=None, log_every: int = 0,
               cancellation=None, queue_factory=None, on_start=None, on_close=None, stall_timeout=None,
               stop_file=None, source_batch_size=_FEED_CHUNK, consume=None, progress=None, unit_driver=None, on_stop=None,
               stage_limits=None) -> StreamStats:
    """同步驱动入口：建事件循环跑至完成（或 Ctrl-C/异常终止），返回 StreamStats。

    queue_factory(depth, *, max_item_bytes=None, max_bytes=None) 是行传输缝
    （调度层内部，算子/编排零感知）：缺省进程内有界队列；分布式实现（如 redis
    支撑的跨节点队列）替换此工厂即可，行需可序列化、stage 由各 worker 侧自行构造。
    有 stage 预算时额外传入 max_item_bytes（单行字节上限）与 max_bytes（在途总字节），
    适配器须据此约束队列；无 stage 预算时仅传 depth。
    """
    stats = StreamStats()

    async def execute():
        drained = False
        stages = []
        async def drain(result):
            nonlocal drained
            drained = True
            if on_drain is not None: await _call(on_drain, result)
        try:
            stages = _materialize(plan, stage_limits=stage_limits)
            if progress is not None:
                progress.bind_stages(stages)
            if on_start is not None:
                result = on_start()
                if inspect.isawaitable(result): await result
            if progress is not None:
                await progress.restore_stages()
            await _arun(source_iter, stages, stats,
                        on_progress=on_progress, on_drain=drain,
                        log_every=log_every, cancellation=cancellation,
                        queue_factory=queue_factory, stall_timeout=stall_timeout, stop_file=stop_file,
                        source_batch_size=source_batch_size, consume=consume, progress=progress, unit_driver=unit_driver,on_stop=on_stop)
        finally:
            try:
                if not drained: await drain(stats)
            finally:
                try:
                    errors = []
                    for stage in stages:
                        if isinstance(stage.fn, StreamMapRuntime):
                            try:
                                await stage.fn.aclose()
                            except BaseException as exc:
                                errors.append(exc)
                    if errors:
                        raise BaseExceptionGroup('Stream map cleanup failed', errors)
                finally:
                    if on_close is not None:
                        result = on_close()
                        if inspect.isawaitable(result): await result

    asyncio.run(execute())
    return stats


# run_stages（stage 列表便捷入口）已于 2026-09-07 移除：收尾语义下沉到
# Dataset.run_stream 后，链式声明（from_items().map_async()×N.run_stream()）
# 是唯一编排形态——历史沿革：09-05 作为便捷入口引入，收敛期回归原始
# Dataset API 风格后退役。


def _close_stages(stages: list) -> None:
    """规范算子可选 aclose() 钩子（同步/异步皆可，best-effort）。

    2026-09-07：收尾逻辑下沉到 Dataset.run_stream finally 后，本函数保留
    供外部工具（冒烟脚本等）显式收尾使用。
    """


async def _close_platform():
    from .resource_registry import stream_cleanups
    for callback in stream_cleanups():
        result=callback()
        if inspect.isawaitable(result): await result
