# Dataset.map 的本地流式调度

普通 `map(fn)` 保持现有惰性执行契约。在流式链中，不带新参数的 `map`
仍为单 worker、单行输入队列、inline 执行，异常策略保持原行为。
本能力让调用方通过公开 `Dataset.map` 声明调度策略，由平台统一执行。
它不选择业务模型、改变请求身份、重试业务调用或自动重启运行中的流程。

## 接口和执行路径

```python
from demiflow import data

def prepare(row, *, scale):
    return {**row, "value": row["value"] * scale}

prepared = (
    data.from_items([{"value": i} for i in range(12)])
    .map(prepare, fn_kwargs={"scale": 2},
         concurrency=4, queue_depth=2, execution="thread", label="prepare")
    .materialize()
)
print(prepared.take_all())
```

| 参数 | 未指定时的流式值 | 合同 |
| --- | --- | --- |
| `concurrency` | `1` | 正整数，当前节点最多同时处理的行数；不接受 bool、浮点或字符串 |
| `queue_depth` | `concurrency` | 正整数，当前节点输入队列最多等待的行数 |
| `execution` | `'inline'` | `'inline'` 在事件循环执行；`'thread'` 使用当前节点专用线程池 |
| `catch` | `()` | Exception 子类的 tuple；匹配的行异常记 miss 并丢弃该行，不重试 |
| `label` | callable 名称 | 非空统计名；重复名称沿用平台的 `#2` 等节点后缀 |
| `callable_scope` | `'stage'` | `'stage'` 共用一个 callable；`'worker'` 每个逻辑 worker 独立构造实例 |

这些参数任意一个取非 `None` 值，就在逻辑计划上声明流式策略。
因此 `concurrency=1`、`catch=()`、`execution='inline'` 也属于显式声明；
只传 `None` 与不传相同。普通 `fn_args/fn_kwargs`、构造参数和字段绑定接口保留。
不读取 callable 上的同名调度属性；`map` 的策略以调用参数为准。
inline 同步调用仍占用事件循环，单独增加 concurrency 不会使它并行。
thread 适用于阻塞 I/O 或释放 GIL 的计算；不保证纯 Python CPU 回调线性加速。

| 使用路径 | 行为 |
| --- | --- |
| Local `run_stream()` | 执行新策略；只有显式流式 map 的计划也可以执行 |
| Local `materialize()` | 复用同一流式执行器，成功清理资源后交付物化句柄 |
| `checkpoint()` / `checkpoint_lance()` | 从第一个流式节点连接现有 checkpoint 桥；固定结果可重放 |
| Local `take/count/iter_rows/write_*` 等普通惰性动作 | 显式报错，不忽略流式参数；先 `materialize()` 再消费 |
| Ray | 不支持这些流式参数，也不支持 `run_stream()`；在集群及数据源访问前拒绝 |
| `join/reduce_by_key`、惰性分组与 `union` 消费带新策略的输入 | 先物化或 checkpoint；不能将调度策略隐藏在关系来源中 |
| `prepend(other)` 的 other | 必须为有限同步 Dataset；不能含显式流式 map，即使并发为 1 |

`backend_options` 和新策略不能同时指定。关系算子没有因此获得流式执行能力；
需要关系计算的前缀先按现有方式固定，再接流式链。
`data.local_execution(workers=...)` 的惰性任务并发与流式节点的 `concurrency`
是不同预算；流式线程池按节点声明创建，不把它偷偷改成惰性 worker 数。

## callable 所有权与生命周期

`callable_scope='stage'` 保持一个 action 内共享一个 callable 的语义。
普通函数及其闭包、`fn_args/fn_kwargs` 引用在节点内共用；调用方负责线程安全。
callable 实例沿用 `CallableSpec` 的 action 级复制规则，不能依赖原实例上的计数器
读取执行统计。含不可复制连接或锁的对象应传类并通过构造参数初始化。
bound method 沿用其原 owner 引用，不因 action 重跑而复制 owner。

需要隔离状态时，显式传 callable **类**及 `callable_scope='worker'`：

```python
class Normalize:
    def __init__(self, offset):
        self.offset = offset
        self.processed = 0

    def __call__(self, row):
        self.processed += 1
        return {**row, "value": row["value"] + self.offset}

stats = (
    data.from_items([{"value": i} for i in range(100)])
    .map(Normalize, fn_constructor_args=[3], callable_scope="worker",
         concurrency=4, queue_depth=2, execution="thread", label="normalize")
    .run_stream()
)
```

平台不要求继承基类。每个逻辑 worker 在第一条实际处理的行到达时创建实例，
同 worker 逐行复用；一个实例不会同时执行两行。实例数量最多等于节点并发数，
空输入及全部被前置 filter 排除时不创建实例。再次执行同一 Dataset 使用新实例。
`'worker'` 不接受函数、partial 或预构造实例，避免把复制闭包误认为状态隔离。

隔离的是实例自身状态。构造参数、fn 参数及全局对象不递归复制，传入的共享可变
依赖仍须由调用方隔离或同步。worker 指逻辑消费者，**不保证固定 OS 线程**：
线程池可在不同调用之间更换线程。线程专属连接等资源不能依靠此模式获得线程亲和性。

显式流式 map 管理实例的 `astart → __call__ → astop → aclose` 生命周期。
`astart` 每实例最多一次；`astop` 在节点排空后执行；`aclose` 在 action 退出时执行。
失败和取消也清理已构造实例，start 失败仍尝试 stop/close；构造本身失败时，
构造函数负责尚未交付实例的局部资源。清理一个实例失败仍会尝试其他实例的清理。

构造与生命周期钩子运行在事件循环；钩子可返回 awaitable，但应避免阻塞循环。
`execution='thread'` 只移动逐行调用。线程调用继承调用方 contextvars。
运行中的线程调用排空后才 stop/close，不在仍使用资源时提前释放实例。
不带新参数的旧 map 不自动获得新的实例隔离或生命周期管理，以保留兼容性。

## 行语义、异常和取消

新策略保留现有流式行协议：完整行 map 的返回值为行、`None` 或行列表；
`None` 记 drop，列表按现有协议展开。字段绑定 map 保留未修改字段，
`output` 将完整返回值放入一列，`outputs` 将返回字段逐项绑定。
字段绑定函数返回 `None` 可以是合法列值，不能将它等同于完整行 map 丢行。

map 的 callable 必须同步。声明时拒绝已知 async 函数及 async `__call__`；
普通函数隐藏返回 awaitable 时也会在执行中报错，不将 coroutine 写入输出。
异步工作继续使用 `map_async`。

`catch` 只处理逐行调用错误。未声明的错误，包括普通 `TimeoutError`，终止流；
构造、start、stop、close、读源和写表失败不由行 catch 吞掉。
`BaseException`、取消、KeyboardInterrupt 等不能作为新 map 的 catch 类型。
异常路径不返回成功的物化句柄；已提交 sink 版本按原回执保存，不回滚旧表。

本接口不增加单算子 `hard_timeout` 或 operator checkpoint 参数。
线程无法被安全强杀；有阻塞 I/O 的 callable 必须自行设置有限 deadline。
STOP、外部取消及其他节点失败会停止后续投喂，并等待已启动线程完成再清理。
这一等待不保证固定上限，不能用 stage watchdog 冒充线程硬超时。
`map_async(execution='thread')` 仍拒绝 hard_timeout 和其 operator checkpoint 组合。

## 顺序和恢复

concurrency=1 的 inline 同步节点按收到行的顺序调用；上游并发可能已经打乱顺序。
多线程、多 worker 输出不保证顺序，平台不增加全局排序或等待前序慢行。
有顺序依赖的路由、累计器和进度逻辑应显式保持其所需语义。

调度参数不自动进入模型请求或业务 checkpoint 身份，也不自动使它们兼容。
请求复用取决于实际 payload、模型、模板、部署和 endpoint 等完整身份。
例如按到达顺序分配 endpoint 的路由，在上游改并发后可能改变请求身份；
平台不能仅凭“业务对象相同”跨路由复用响应。

action 的持久 checkpoint 与单算子日志不同：

- JSONL/Lance checkpoint 复用调用方显式版本/指纹绑定的完整结果，沿用其原恢复规则。
- `run_stream(checkpoint=StreamCheckpoint(...))` 可与线程 map 共用；仍由业务声明身份、
  固定来源、pending 范围及 append sinks。线程 map 不改变 sink 的提交/冲突检查。
- `prepend` 的 pending Dataset 保持同步。需要重新并发处理时，可在注入后的主流
  声明 map 节点；不能把异步 Dataset 直接当作 pending 来源。
- 普通 map 不提供 exactly-once 副作用保证；未提交行可能在恢复时重新执行。

## 有限资源和观测

每个线程节点最多创建 `concurrency` 个线程；每个逻辑 worker 同时只提交一个调用。
排队行不直接全部提交到 executor，因此其无界内部工作队列不会随输入总量增长。
下游变慢会通过有限队列向上游传播背压；失败和退出回收当前 action 的池。
实例数量、生命周期记录和线程 future 集合同样受节点并发数约束。

这些限制是**行数和调用数**限制，不能解释为字节或整个进程 RSS 的硬上限。
评估峰值时须计入 source_batch_size、reader 自身预读、各级 queue_depth、在途调用、
等待下游接收的结果、输入/输出副本和 writer 缓冲。若每节点输入、完整输出与
回调临时对象分别最多为 Bin、Bout、T 字节，可按
`queue_depth × Bin + concurrency × (Bin + Bout + T)` 加源/末端缓冲估算。
Bout 包含尚未展开完的整个返回列表；共享底层缓冲、native 库分配和自建缓存
还需单独核算，未知对象大小时不能使用这个估算声称内存安全。
本次没有给任意 Python 对象、单条巨型载荷或第三方分配增加通用内存硬限制。
应使用有界输入、投影、独立对象引用和 callable 自身的单行预算；不能用增加并发
掩盖未知的单行大小。需要可强杀隔离的工作继续使用已有进程隔离能力。

`stats.metrics['stage_policies']` 记录实际并发、队列、execution、callable_scope 和 catch；
`stages/queues/stage_processing_latency` 沿用现有计数、队列峰值和时间摘要。
阶段时长排除输入排队及下游背压，但包含线程调度、GIL 等待和返回事件循环的延迟，
不能解释为纯 CPU 时间；并发调用耗时之和也不等于墙钟耗时。
现有时延分位数使用 250 ms 桶，毫秒级差异应参考 total_s/count 及独立墙钟测量。

本地 pytest 使用隔离数据验证调用并发、线程退出、实例隔离、部分启动清理、
慢消费者背压、异常与 STOP、参数拒绝、字段绑定、物化及 checkpoint 重放。
Ray 测试只覆盖实际编译器的前置拒绝，不启动或认证分布式执行；本能力不支持 Ray 流式。
这些检查不包含业务全量、真实模型吞吐或特定第三方 callable 的线程安全认证。

## 2026-10-07 实现审查与验证记录

实现分为三层：`MapStreamOptions` 保存并校验逻辑策略；`StreamMapRuntime`
绑定原有 `StandardCallable/BoundCallable` 并管理实例；`execution.stream`
继续负责所有队列、worker 调度、线程池与排空。未另建 executor 或业务适配层。
`is_stream_operation` 统一物化、checkpoint 和 prepend 对新策略的识别。

边界审查修复了三处退出问题：STOP 在生产者结束后到达时可能误报成功；
回调抛出 `CancelledError` 时 worker 被视为无错误完成；Lance checkpoint
消费者失败后后台桥接可能继续生产。这些场景现在均有回归测试，失败不返回
成功物化句柄，checkpoint 写入失败会取消并等待本次后台流完成清理。

最终相关回归 **225 passed**，命令如下（使用工作区现有 Python 环境）：

```bash
PYTHONPATH=. OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 ../env/bin/python -B -m pytest \
  tests/test_stream_map.py tests/test_stream.py tests/test_stream_threads.py \
  tests/test_stream_cancel_propagation.py tests/test_stream_admission.py \
  tests/test_stream_checkpoint_migration.py tests/test_stream_checkpoint_resume.py \
  tests/test_stream_deduplicate.py tests/test_stream_grouping_lookup.py \
  tests/test_stream_lance_references.py tests/test_async_materialize.py \
  tests/test_map_prompt_async.py tests/test_prompt_routes.py \
  tests/test_web_operator_lifecycle.py tests/test_lance_io.py tests/test_data_module.py \
  -q -p no:cacheprovider
```

另一次包含 local relational/kernel/join/union 的扩展回归为 **204 passed，8 failed**。
失败集中在 `test_join_matches_legacy_canonical_semantics` 的 6 个参数组合和
`test_reuse_join_partitions_on_both_sides_and_reject_changed_keys` 的 2 个组合，
均为逐列表比较依赖未承诺的 join 顺序。这 8 个失败在改动前文件快照的隔离副本
中全部复现；基线的 anti 组合另出现一个顺序失败。它们保留为已有测试／契约差异，
未通过新增排序改变平台行为，也未将扩展回归报告为全绿。
