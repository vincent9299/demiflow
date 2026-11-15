# 工件预留与实际字节结算

本轮实现针对 B07 中重复内容/失败写入永久累计 `max_bytes` 的缺陷。所有计数仍在现有 RunJournal 中；没有第二个 allocator 或数据库。隔离分支继续实施，本文不是最终验收。

## 契约增量

`ArtifactCommand` 增加两个明确动作，DTO、wire codec、driver/worker 消费端同步修改：

- `settle_staging`：`operation_id=identity([reservation_id,'settle'])`，`owner=reservation_id`，`ref` 为已完成且可校验的工件，`size_bytes=ref.size_bytes`。driver 验证文件后，在一个带 fence 的事务中把暂存上限替换为唯一 `(store_id,digest)` 内容的实际字节。重复结算返回同一结果，换 ref/大小或改成退款拒绝。
- `release_staging`：`operation_id=identity([reservation_id,'release'])`，`owner=reservation_id`，`ref=None,size_bytes=0`。仅在该 reservation 命名的暂存文件已删除后退还；driver 再检查其暂存文件不存在。确认结果不明的发布不得走此动作。

`RunJournal.settle_dataflow_staging(fence, reservation_id, ref)` 是唯一结算入口，沿用 `runtime_budgets` 与 `runtime_reservations`；新增同库有界记录 `runtime_dataflow_objects` 与 `runtime_dataflow_settlements` 保存内容计费和幂等结算依据。仍受原 control store/预留条数上限与 fence 约束。

`RunArtifactStore.put` 的顺序为预留上限 → 带 reservation 名的临时文件 → 有界写入/校验 → fsync 文件和发布目录 → 删除并 fsync 暂存目录 → driver 结算实际唯一字节。重复内容写入的独立临时 inode 删除前不退款。创建文件、输入迭代器、字节超限等明确失败清理后退款；如果发布或 authority 回执不明，保留原预留。

输入给 `put` 的已分配 bytes chunk 允许大于 1 MiB，但写入/哈希按最多 1 MiB memoryview 分片，不复制整个 chunk；总量在写入前受 `max_bytes` 检查。调用者仍负责该 bytes 的编码/驻留预算，此变更不是进程 RSS 硬上限承诺。

## 实跑证据

- `/tmp/demiflow-artifact-v3-2.log`：11 passed，4 个工件反例加 7 个真实原生模型场景。30 次重复写只计一份实际字节；失败写、文件创建失败、超限正确退款；未知发布保留额度并可幂等结算；大 chunk 保真。
- `/tmp/demiflow-artifact-current.log`、`.xml`：9 passed，在进一步增加“有暂存文件不能退款”校验后，5 个工件反例、2 个效果事务反例及真实 map/部分 checkpoint 恢复通过。
- `/tmp/demiflow-artifact-regression.log`、`.xml`：65 passed，原 33 项新增失败逐 nodeid + M 27 项 + 工件 5 项。未删除/跳过/放宽原 33 项负例。

## 仍须继续

本轮没有把进程崩溃留下的未知暂存或已发布但尚未登记结算的文件自动退款。必须先接入旧 worker 物理退出确认与 orphan reconciliation，再释放这类所有权；不能只凭旧 attempt/fence 或“看起来没有引用”删除。持久内容 GC 与最后恢复点/准备中提交/未知效果的根引用也仍需闭环。此保留项与真实宿主恢复、资源续租/退出一起推进。
