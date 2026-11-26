# 关系运算：关联、分组、排序

Dataset 提供关联、分组、排序、去重这类跨行运算。写法和其他 Dataset 方法一样，都是惰性的，遇到 `take_all()`、`count()`、`write_lance()` 等终结动作才执行。数据量超过内存时由平台自动落盘，不需要选择执行引擎。

## 关联：`join`

```python
from demiflow import data

users = data.from_items([{"uid": 1, "name": "Ann"}, {"uid": 2, "name": "Bo"}, {"uid": 3, "name": "Cy"}])
orders = data.from_items([{"uid": 1, "amount": 30}, {"uid": 1, "amount": 12}, {"uid": 3, "amount": 7}])

users.join(orders, on="uid").take_all()
# [{'uid': 1, 'name': 'Ann', 'amount': 30}, {'uid': 1, 'name': 'Ann', 'amount': 12},
#  {'uid': 3, 'name': 'Cy', 'amount': 7}]
```

| `how` | 结果 |
|---|---|
| `inner`（默认） | 两边都有的键，每对匹配输出一行 |
| `left` | 保留左边所有行；没匹配上的行不带右边字段 |
| `semi` | 左边有匹配的行，只含左边字段，行数不增加 |
| `anti` | 左边没有匹配的行 |

```python
users.join(orders, on="uid", how="anti").take_all()
# [{'uid': 2, 'name': 'Bo'}]
```

- 两边键名不同时用 `right_on`：`users.join(payments, on="uid", right_on="user")`。
- 两边有同名的非键字段时，右边的字段加后缀 `suffix`（默认 `_right`）。
- 键为 null 的行不参与匹配。键重复时输出所有配对。
- 输出顺序不保证；需要顺序请显式 `sort`。
- 键按类型区分：整数 `1`、浮点 `1.0`、字符串 `"1"` 是不同的键。

## 分组计算：`reduce_by_key`

把同一个键的行依次交给一个函数累计：

```python
totals = orders.reduce_by_key(
    "uid",
    lambda state, row: {"uid": row["uid"], "total": (state or {}).get("total", 0) + row["amount"]},
)
totals.take_all()
# [{'uid': 1, 'total': 42}, {'uid': 3, 'total': 7}]
```

- 函数签名是 `reducer(state, row) -> state`。每组第一次调用时 `state` 是 `initial`（默认 `None`）。
- 每组最后的 `state` 作为一行输出；`None` 不输出。
- 组内按原来的顺序调用。平台不假设这个函数可交换或可结合，不会并行拆分它。
- 累计状态的大小由你的函数决定，平台不限制。

## 按键分批：`group_batches`

把同一个键的行收成列表，每批最多 `max_rows` 行，适合“每组一起送给模型”：

```python
orders.group_batches("uid", max_rows=10, output="items").take_all()
# [{'uid': 1, 'items': [{'uid': 1, 'amount': 30}, {'uid': 1, 'amount': 12}], 'group_index': 0, 'group_last': True},
#  {'uid': 3, 'items': [{'uid': 3, 'amount': 7}], 'group_index': 0, 'group_last': True}]
```

一组超过 `max_rows` 时拆成多批，`group_index` 是批次序号，`group_last` 标记该组的最后一批。

在流式链里可以给 `flush_interval=秒数`：不等数据全部到齐，按时间或缓冲压力出批；这时无法知道哪一批是最后一批，不提供 `group_last`。

## 整体聚合：`aggregate`

```python
orders.aggregate(data.Count(), data.Sum("amount"), data.Mean("amount"))
# {'count()': 3, 'sum(amount)': 49, 'mean(amount)': 16.333333333333332}
```

可用的聚合：`Count`、`Sum`、`Min`、`Max`、`Mean`、`Std`、`AbsMax`。`aggregate` 是终结动作，直接返回结果。

## 排序、合并、限量

```python
orders.sort("amount", descending=True)        # 全局排序；多列时传列表
users.union(more_users)                       # 上下拼接，不转换 schema
orders.limit(100)                             # 只取前 100 行，上游会及时停止
orders.random_shuffle(seed=7)                 # 全局随机打乱
```

## 按键排除与查找

**排除已处理的键**：从一张 Lance 表里去掉另一个 Dataset 中出现过的键，保持原扫描顺序。只有键参与计算，正文等大字段不进入关联，适合大表上的“跳过已完成”。

```python
done = data.from_items([{"uid": 0}, {"uid": 3}])
todo = data.read_lance("/data/users.lance").exclude_keys(done, on="uid")
```

`exclude_keys` 必须直接接在 `read_lance` 后面，其他变换放在它之后。

**逐行查找**：给每一行附上一张固定版本 Lance 表里键相同的行，不缓冲输入流，适合流式链里补充少量字段。

```python
enriched = orders.lookup_lance(
    {"uri": "/data/users.lance", "version": 1},
    on="uid", columns=["name"], output="user",
).materialize()
# [{'uid': 1, 'amount': 30, 'user': [{'name': 'Ann'}]}, ...]
```

- 表必须用 `{"uri": ..., "version": ...}` 指定固定版本。
- `max_matches`（默认 1，最多 64）：匹配行数超过它时报错，不截断。
- `max_bytes`（默认 1 MiB）：单行附带结果的大小上限。

## 与流式节点一起使用

关联和分组需要完整的输入。上游有带并发参数的 `map`、`map_async` 或模型调用时，先 `materialize()` 再做关系运算：

```python
scored = rows.map_async(score, concurrency=8).materialize()
scored.join(labels, on="id")
```

## 资源

平台对每个关系查询设有内存、线程和临时磁盘上限，按容器大小自动调整，超出时查询失败并报错，不会悄悄换一种算法。需要收紧预算时：

```python
with data.local_execution(native_resources={"memory_bytes": 2 * 1024**3, "threads": 2}):
    result = users.join(orders, on="uid").take_all()
```

可设置的项：`memory_bytes`、`max_rss_bytes`、`max_scratch_bytes`、`max_row_bytes`、`threads`、`partitions`、`batch_rows`、`timeout_s`、`admission_timeout_s`。它们只能调低，不能超过平台默认值。

需要知道的限制：

- 这些上限是保护措施，不是对整个进程内存的硬保证；单行特别大的数据仍可能超出预算而失败。
- 查询失败、超时或被取消时不会重跑你的函数，也不会自动加内存。

### 内存放不下最大行时

平台先按“每一行都和行上限 `max_row_bytes` 一样大”给 `join` 和 `reduce_by_key` 留内存：join 约 1 个批次加 6 行，reduce 约 12 行再加两份状态上限 `max_state_bytes`。`local_execution(memory_bytes=...)` 设得较小、这样排不下时：

- 行上限、批次上限（reduce 还有状态上限）都是默认值时，join 和 reduce 改为按实际大小运行：先只给它们一个批次加一行，再把剩下的内存尽量补回给它们，运行时按每一行、每个分组状态的实际字节计算。行不大时照常运行；某一行或某个分组状态在分到的内存里放不下时，运行中报 `BUDGET_EXCEEDED`，报错写明分到多少内存、按最大行需要多少。
- 这种计划开始时日志里有一条警告，写明是哪个步骤、分到多少内存、单行大约超过多大可能失败。`local_execution` 的 `stats['measured_memory']` 和 `run_stream()` 的 `stats.metrics['measured_memory']` 里有同样的内容。
- 在运行上限（`RunContext.start(limits=...)`）里显式设置了这些上限时，平台仍按最大行检查，放不下就在开始前拒绝，不调用任何函数。想在花钱之前就确定一定能跑完，就显式设置 `max_row_bytes`、`max_batch_bytes`（reduce 再加 `max_state_bytes`）。
- 能按最大行排下的计划不受影响。
