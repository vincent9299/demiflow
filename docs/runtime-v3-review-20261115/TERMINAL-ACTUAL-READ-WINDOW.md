# 终端真实读取窗口与转换共驻（独立复审后的修复）

## 结论与范围

固定 `47f4cfa301caaec6c9e79862634cbba2e5407ac7` 只读复审发现一项实际 P1 回退：固定扣除四个**声明最大行**会把默认单 worker 的合法小预算读窗口扣成负数。根裁决后，在隔离树修复真实读取工作区，并接通读取与列式转换的共驻额度。最终 **33 passed / 84.67s**。

这使用户能在原小预算下读取真实执行结果、重新读取原 ACK 历史，并在确实没有空间时于第三方分配前拒绝；同时保持单 Dataset / 原 journal / 原 ACK 的权威。没有调整 Run 限额、隐藏缩小显式 `max_row_bytes` 或 batch size，没有新预算账本。

可写树为 `gpt_v3_artifact_recovery_20261115`。先合根 `1effecb`，合后 `demiflow/` 与 `tests/` 相同；后单独消费根的转换估算修复 `293f23b` / `71453b6`。本包不覆盖根另行补充的标准 Arrow extension 支持。

## 第一手回退证据

使用精确 `git archive` 的 `47f4cfa` 与父提交，在 `/tmp` 各自断言真实导入路径后执行相同公共链路：

```python
with RunContext.start(...), data.local_execution(
    workers=1, memory_bytes=96 * 1024, worker_mode='thread'
):
    data.from_items([{'i': 1}]).take_all()
    # 独立 action 同样验证 iter_batches(batch_size=2, batch_format='pyarrow')
```

实际 Bounds：`row=batch=24576`、`resident=98304`。原新增 `4*(row+1)=98308`，窗口为 `-4`，新版本两条链路都拒绝；父版本两条都成功。没有命中代码中的额度上限，而是估算规则与原默认窗口规则冲突。

原始探针：`/tmp/demiflow-read-admission-review-47f4cfa/small-budget.json`、`/tmp/demiflow-read-admission-review-before/small-budget.json`。探针内的 workers=2 / 96 KiB 在原公共每 worker 最小内存检查就失败，不属于本问题证据。

## 最小修复的具体边界

- `RunArtifactStore.read/_read` 只增可选 `chunk_bytes`，原默认不变；必须为非 bool 的正整数且不超过原 chunk 上限。它直接约束原文件 `read()`，没有在外层切片伪装小读取；`max_bytes` 仍是整个 ref 的读取上界。原共享租约、FD 计数、摘要和异常关闭不变。
- 历史 `PublishedRecordReader` 使用最多 4 KiB 的物理 chunk，在拼接、提取帧、UTF-8 text、外层 JSON 文档和 raw 编码之前检查实际工作区。它仍跨原 segment 读取，原对象格式、顺序、digest 与旧提交不改。工作区随实际 frame 和 chunk 而变，不按四个最大业务行收费。
- live 路径在 TCP 载荷读取前，按原 locator 的真实 frame 大小预留旧 frame、接收 bytearray / fragment 与返回 bytes 的共驻；完全不预留没有创建的历史读取器。
- `read_admitted` 分开「工作区增长」和「新增业务行」准入。读 EOF / 组帧不消耗 row slot，精确 `take_all` limit 不会因为探测而变成溢出。业务 decode 仍在 metadata / 额度检查之后，原 ACK 时点不变。
- `RuntimeActionHandle.conversion_reserve()` 仅保留最多 64 条本次 read 的 size 与原 yield 计数基点。转换时计入未 yield 的 Record、当前 yield 一条及仍活的 reader buffer；已关闭 collector 返回 0。没有改 `_iter_records` / `close`。
- 根的 `_convert_batch` 接口把上述 reserve 仅从 resident 余额扣除，独立 `max_batch_bytes` 仍保持原值。读取器暂停时不再保留可引用已交付 Record 窗口的 callback。

## 真实组合验收

新增测试 `test_runtime_v3_terminal_read_workspace.py` 共 7 项：

1. 默认单 worker / 96 KiB：`take_all`、`iter_rows`、Arrow `iter_batches` 首跑及原 final ACK 恢复，恢复禁止启动 worker，提交列表逐项相等，运行目录 FD 为 0。
2. 原 ArtifactStore 真文件可选物理 chunk；原整 ref 上界、错误类型、损坏摘要和失败 FD 释放保留。
3. 两个真实 Arrow schema 的原 final ACK 恢复：第一段 null fixed binary，后面 32 条不同 schema 的记录已经被预读。实际仍活 reserve **19,789 bytes**。在 **64 KiB** resident 下，第三方 Arrow 构造尚未调用即拒绝；在 **96 KiB** 下，两段原 schema / 值完全保留。两臂均零 worker、旧 ACK 不变、FD 为 0。
4. 暂停的读取器不会通过上次 credit callback 继续持有已交付的窗口。

原 20×6 KiB 恢复反例仍验证：读取窗口不超过原 batch bound、累计已 decode 载荷与当时工作区不超过原 resident、额度不足时未把全部 20 条 decode 后才拒绝。仅将实现细节「恰好 5 条」改为实测窗口断言；旧 ACK、零 worker 和 FD 断言完整保留。

最终选择：7 个新测试 + 10 个原恢复测试 + 根的转换共享窗口反例 + 15 个原 T 契约测试 = **33 全绿**。日志和 XML：`/tmp/demiflow-read-workspace-final.log` / `.xml`。

中间暴露的相邻转换问题已由根修复：原 2×6000 字符带整数列 / 32 KiB 正例不能由过度取整的 builder 估算拒掉。真实 Arrow 日志显示 data `6016 → 12032`、binary offset 为 64 字节；根在 `293f23b` / `71453b6` 按实际逐值扩容修正，最终原测试预算及业务断言未改。新 schema 反例的初始 16 条后继没有触及边界，改成 32 条后继锁定共驻；没有改真实 Run 的正/反预算。

## 验证边界与交接

机器证据 `TERMINAL-ACTUAL-READ-WINDOW.json` 绑定最终 production / test 摘要、XML、实际 19,789 字节观察及旧红基线。七承重对 `362d8528` 字节相同；AST 源片段确认 `RuntimeActionHandle._iter_records`、`RuntimeActionHandle.close`、`_OutputSubscription.close` 对 `1effecb` 没有改动，保留 residency 的独立 early-close 工作。

这是读取 / 转换共享窗口的定向验证，不是新一轮全量 Local / Ray / 产品验收，也不声称普遍进程 RSS 硬隔离。旧 ACK、身份、字段格式、排序、并发资源权威均沿用原实现；没有发现这一接缝另有已实证 P0/P1。未运行或修改主仓、生产数据、共享环境、外部服务。
