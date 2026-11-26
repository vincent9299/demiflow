# 流式执行与并发

`Dataset.map` 默认逐行串行执行。给它加上并发参数，这个节点就按流式方式运行：多行同时处理，节点之间用有界队列连接，下游慢了会自动让上游等待。

## 快速开始

```python
from demiflow import data

def prepare(row, *, scale):
    return {**row, "value": row["value"] * scale}

result = (
    data.from_items([{"value": i} for i in range(12)])
    .map(prepare, fn_kwargs={"scale": 2},
         concurrency=4, queue_depth=2, execution="thread", label="prepare")
    .materialize()
)
print(sorted(row["value"] for row in result.take_all()))
# [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]
```

并发执行不保证输出顺序，所以例子里先排序再打印。

## 调度参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `concurrency` | `1` | 这个节点最多同时处理多少行 |
| `queue_depth` | 等于 `concurrency` | 这个节点的输入队列最多排多少行 |
| `execution` | `'inline'` | `'inline'` 在事件循环里执行；`'thread'` 用这个节点专用的线程池 |
| `catch` | `()` | 异常类型的 tuple；命中的行被丢弃并计数，不重试 |
| `label` | 函数名 | 统计里显示的节点名 |
| `callable_scope` | `'stage'` | `'stage'` 整个节点共用一个可调用对象；`'worker'` 每个 worker 一个独立实例 |

选择 `execution`：

- 函数里有阻塞 I/O（HTTP 请求、读文件），或调用会释放 GIL 的库，用 `'thread'`。
- `'inline'` 的同步函数会占住事件循环，只增大 `concurrency` 不会让它并行。
- 纯 Python 的 CPU 计算用线程不会线性加速。
- 异步函数用 `map_async`；`map` 只接受同步函数。

## 怎么执行

| 方法 | 行为 |
| --- | --- |
| `run_stream()` | 执行整条链，返回统计对象，不保留结果行 |
| `materialize()` | 执行并把结果固定下来，之后可以 `take_all()`、`count()` 或接关系运算 |
| `take` / `iter_rows` / `write_lance` 等 | 直接执行并交付结果或写出 |

`join`、`reduce_by_key`、`union` 等关系运算不能直接接在带并发参数的节点后面，先 `materialize()`。

这些流式参数目前只支持本地执行，Ray 后端会在开始前报错。

## 处理异常

没有声明的异常会终止整条流。只想跳过坏行时用 `catch`：

```python
def parse(row):
    if row["value"] % 5 == 0:
        raise ValueError("bad row")
    return row

stats = (
    data.from_items([{"value": i} for i in range(12)])
    .map(parse, catch=(ValueError,), concurrency=2, label="parse")
    .run_stream()
)
print(stats.stages)   # {'parse': {'in': 12, 'emitted': 9}}
print(stats.miss)     # {'parse:ValueError': 3}
```

- `catch` 只管逐行调用抛出的异常。读数据源、写表、对象构造和清理的失败不会被它吞掉。
- 流因异常终止时，调用方拿到的是原异常类型（`ValueError`、`KeyError`、`TimeoutError` 等常见内置类型）。自定义的子类会还原成它最近的内置父类，例如 `class BadRecord(ValueError)` 还原为 `ValueError`，报文不变。
- `run_stream(stall_timeout=秒数)`：所有节点在这段时间内都没有进展时抛 `StallError`。
- 线程不能被强制终止。有阻塞 I/O 的函数要自己设置超时。

## 有状态的处理：每个 worker 一个实例

需要每个 worker 各自持有状态（连接、计数器、模型句柄）时，传入**类**并设置 `callable_scope='worker'`：

```python
class Normalize:
    def __init__(self, offset):
        self.offset = offset

    def __call__(self, row):
        return {**row, "value": row["value"] + self.offset}

stats = (
    data.from_items([{"value": i} for i in range(100)])
    .map(Normalize, fn_constructor_args=[3], callable_scope="worker",
         concurrency=4, execution="thread", label="normalize")
    .run_stream()
)
```

- 实例在第一行到达时创建，最多 `concurrency` 个；一个实例不会同时处理两行。
- 可选的生命周期方法：`astart`（开始前一次）、`astop`（节点处理完后）、`aclose`（退出时）。失败和取消时也会清理。
- 隔离的只是实例自己的属性。构造参数里传进来的共享对象仍然是共享的，需要自己保证线程安全。
- worker 不绑定固定的操作系统线程，不要依赖线程亲和性。
- 默认的 `'stage'` 下，整个节点共用一个函数或实例，线程安全由调用方负责。

## 返回值规则

- 整行 `map`：返回一行、`None`（丢弃这行）或行的列表（展开成多行）。
- 用 `inputs` / `output` / `outputs` 绑定字段时，未涉及的字段原样保留；此时返回 `None` 是一个合法的列值，不表示丢行。

## 顺序与重复执行

- `concurrency=1` 的 inline 节点按收到的顺序处理。多线程、多 worker 的输出顺序不保证；依赖顺序的逻辑要自己处理。
- 普通 `map` 不保证副作用只发生一次：运行中断后恢复时，尚未提交的行可能重新执行。
- 断点恢复使用 `RunContext`，见 README 的“Resume where you stopped”。

## 内存估算

并发和队列限制的是**行数**，不是字节数。一个节点的驻留量大致为：

```
queue_depth × 单行输入大小 + concurrency × (单行输入 + 单行输出 + 临时对象)
```

并发大于 1 的逐行节点（带并发的 `map`、`map_async`、模型调用等）还有一个按字节计的调用窗口。每个调用开始前，要在窗口里预留它这一行输入的实际字节数，再加一整行输出（行上限 `max_row_bytes`）；调用结束后归还。窗口放不下时，新的调用等前面的调用结束再开始，不报错。窗口至少能容纳一个调用，所以不超过行上限的单行总能被处理。同时进行的调用数是 `concurrency` 和窗口能容纳数中较小的那个。

平台的 `fetch` 节点是例外：它只往行里追加一份回执，回执在发出前已按 `max_receipt_bytes`（不超过 16KiB）检查过，对象本体留在对象存储里。所以每个 fetch 调用只预留这份回执可能占的大小（约 160KiB），默认额度下声明的并发都能同时进行。输出超出预留时，节点报 `BUDGET_EXCEEDED`，不会多占内存。

默认 256MB 额度、16MB 行上限下，“读取 → 一个并发节点 → 输出”这样的管道最多同时进行 7 个调用，管道里同时运行的阶段越多，能同时进行的调用越少。想要更多，调大 `memory_bytes`，或者调小行上限 `max_row_bytes`。实际达到的并发见下面统计里的 `call_concurrency`。

单行很大时，降低 `concurrency` 和 `queue_depth`，或者先投影出需要的字段。平台不会替任意 Python 对象设置内存硬上限。

## 查看统计

`run_stream()` 返回的对象：

| 属性 | 内容 |
| --- | --- |
| `stats.stages` | 每个节点的输入和输出行数 |
| `stats.miss` | 被 `catch` 丢弃的行数，按节点和异常类型计 |
| `stats.emitted` | 最后一个节点的输出行数 |
| `stats.metrics['stage_policies']` | 每个节点实际生效的并发、队列和执行方式 |
| `stats.metrics['call_concurrency']` | 每个并发节点：设定并发 `capacity`、实际最高并发 `peak`、因内存窗口等待的次数 `byte_waits`、窗口字节数 `byte_limit`、每个调用预留的输出 `output_reserve`；`limited_by` 为 `'memory'` 表示并发被内存压低了。`take_all` 等动作在 `local_execution` 的 `stats['call_concurrency']` 里有同样的内容 |
| `stats.metrics['measured_memory']` | 按实际大小运行的 join、reduce 步骤：`granted_bytes` 分到的内存、`preferred_bytes` 按最大行需要的内存、`row_bytes_hint` 大约能保证的单行大小；没有这种步骤时为空。见 [关系运算](relational.md#内存放不下最大行时)。`local_execution` 的 `stats['measured_memory']` 相同（没有时不出现） |
| `stats.summary()` | 上述计数的一行文字摘要 |
