# 写入 Lance 表

`Dataset.write_lance` 把一个 Dataset 提交为 Lance 表的一个新版本。整个 Dataset 一次提交，要么全部生效，要么表保持原版本。

## 四种写入方式

```python
from demiflow import data

rows = data.from_items([{"id": 1, "title": "a"}, {"id": 2, "title": "b"}])

rows.write_lance("/data/items.lance", mode="create")      # 只在表不存在时创建
rows.write_lance("/data/items.lance")                     # 默认 append：不存在则创建，存在则追加
rows.write_lance("/data/items.lance", mode="overwrite")   # 用本次数据替换当前内容
```

| `mode` | 行为 | 说明 |
|---|---|---|
| `create` | 表不存在时创建 | 表已存在则报错；不接受 `expected_version` |
| `append`（默认） | 创建或追加 | 已有表要求 schema 一致 |
| `overwrite` | 替换当前内容和 schema | 旧版本仍可按版本号读取 |
| `merge` | 按主键更新已有行的部分列 | 见下一节 |

其他参数：

- `schema`：显式指定 Arrow 类型。给了 schema，空输入也能创建或覆盖为空表；没有 schema 的空输入报错。
- `expected_version`：提交时检查表仍在这个版本，否则报并发冲突。
- `return_receipt=True`：返回写入回执，其中有实际提交的版本号；默认返回 `None`。
- `storage_options`：对象存储的访问参数。

## 只更新部分列：`mode='merge'`

```python
patch = data.from_items([{"id": 1, "score": 0.9}, {"id": 2, "score": 0.4}])
patch.write_lance("/data/items.lance", mode="merge", on="id")
```

- `on` 是主键列（可以是多列）。键必须唯一、非空。
- 默认更新输入里所有非主键列；`update_columns=[...]` 可以显式限定，输入的列与它不一致时报错。
- 输入里没有的列保持原值；输入里显式的 null 会把该值清空。
- 目标表里找不到的键由 `when_not_matched` 决定：

| `when_not_matched` | 行为 |
|---|---|
| `error`（默认） | 报错，不提交 |
| `ignore` | 跳过这些行 |
| `insert` | 插入为新行，输入里没有的列为 null |

- merge 不能新增字段。要加列，先用 `data.add_lance_columns`。

## 失败时会怎样

- 键重复、键为空、列不匹配、类型不符、找不到键（默认策略下）都会在提交前报错，表保持原版本。
- 提交结果不确定时（例如提交过程中连接中断）抛 `LanceWriteError`，异常里带回执。平台不会自动重试，也不会自动去重；请先按回执核对表的实际版本再决定是否重写。
- 旧版本始终保留，可以按版本号读回。

## 大表上的 merge

物理写入方式由平台选择，没有需要调的参数。满足下列条件时，平台只重写被更新的列，不读也不重写其余列：

- `when_not_matched` 为默认的 `error`；
- 输入至少更新一个非主键列，并且至少有一个目标列不在输入里；
- 输入直接来自固定版本的 Lance 读取（没有过滤和回调），且行数不超过目标表；
- 涉及的字段不是 Blob 或 Arrow 扩展类型。

其他情况按 Lance 的默认方式写入，无索引时通常会重写整行。

需要知道的限制：

- 只改少量行也可能写出较多数据：替换的列文件以数据分片为单位。
- 大补丁的 merge 需要较多内存。平台会限制它的内存和临时磁盘用量，超出时停止并报错，表保持原版本；不会自动换一种更省内存的方式重试。
- 这项优化目前只在 Linux 本地执行时启用。
