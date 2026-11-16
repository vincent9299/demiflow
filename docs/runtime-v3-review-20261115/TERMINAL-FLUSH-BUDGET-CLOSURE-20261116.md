# 原终端 flush 共驻预算修复

这次是已有资源正确性缺陷的修复；此前终端 wire 复用微优化仍完整撤回，不恢复快路，不声称性能门通过。

## 已复现的问题与最小修复

真实 Dataset：6 条约 6KB 行 → repartition(2) → map/filter/map → take_all，原 max_row_bytes=max_batch_bytes=8192。旧实现完成正确结果及原 ACK，却在逻辑块切换时将整个 19,482 字节旧尾部作为一个读取片段，与当前 6,494 字节 spool 同时存活：25,976 > 原 payload 16,384。固定红基线 `6c2e46b`：2 行守卫通过、6 行守卫失败；源绑定回执见配套 JSON。

不能只将一次 read 截到 max_batch：原 store 的 for-loop 在 next(chunks) 时，仍持有上一片段的 chunk/view；同时可能存在两片段。最终只修改 `worker/terminal_blocks.py::flush`，不改 store 权威、codec、seal、segment 格式、提交或额度：

- terminal 片段上限为 min(65536, floor((max_batch_bytes−1)/2), 原 ScratchStream I/O 上限)。原 document 可恰等于 max_row_bytes，再加既有换行；从原 batch 窗口留这 1 字节，保留所有原合法行。
- 两片段及当前 spool 的总和 ≤ 2×floor((batch−1)/2)+row+1 ≤ batch+row。
- 片段上限不足 1 时，在任何 scratch read 前明确拒绝。不能以 max(1,...) 反向超配。
- 唯一其他生产使用方为 finite buffer 的 partitioned=False 单固定 key。append 不发生旧块切换；flush 在后续 checkpoint/manifest 阶段发生，没有当前行随 append 共驻。因此该路径两片段各 ≤floor(batch/2)，并仍受原 I/O 上限约束。
- 原典型 256KiB batch 继续读取 64KiB；不增加读块以换吞吐，不引入第二计费或缓存。

修复 `d1db15e`；测试夹具修正 `2efa22a`。所有生产追加改动仅此文件 10 新行/1 替换。独立依赖 `245ea8c`（本树 `c59abf9`）由图 lane 提供：按原 control/payload 后余额限制可选 index metadata，修正既有 40KiB 准入；没有减少 payload、修改 TerminalBlocks 或扩大 grant。不要将这个依赖误计成本 lane 的生产实现。

## 验收证据

固定 `c59abf9`，14 项全部通过，33.21 秒；源文件全摘要一致、冻结七件一致、测试前后干净、0 子进程残留。

1. 真实 6 行两块：新旧两个读取片段各 4,095，当前 spool 6,494，实测同时活跃字节最大 14,684 ≤ 16,384。原 2 行守卫也通过。观察发生于真实 ScratchStream.read 和原 store 消费栈；按对象身份去重，不把多个引用重复计费。提前读取上一片段的整数 size/id，避免观察器留下过时帧局部对象快照。
2. 奇数 batch 向下取整，较大 batch 同时受原 scratch I/O 限制；原真实 RunArtifactStore、SegmentForest 40 行两块、每块20行，原叶内容 SHA256、字节、边界和 restore 后的原 manifest 完全相同。
3. 1/2 字节 batch 不可容纳原终端共驻窗口时，读取前拒绝；精确等于原 max_row 的完整编码 document 加原换行仍成功。
4. 原 finite 段回读/追加与增量 cut、原终端 metadata 准入、两个 nonfinal prefix 恢复参数通过。原失败时的预算、数据、limit 断言均未修改。

相关现有终端回读/预算、最终 ACK 重放、真实 block→batch→Lance 写入及同 run 恢复也在第一组合中通过。首组合结果必须完整保留：`d1db15e` 为25 passed/5 failed；其中3个新测试夹具错误（无效 block SHA/普通 JSON 误当原 payload codec）由 `2efa22a` 修正，2个原40KiB准入失败由独立 `245ea8c` 修正。最终14项是按这5个失败及受影响边界固定复跑，不将不同 revision 的计数拼成一次全绿。

原完整轻 CPU 案例另外单次运行：20,000 输入→10,000输出、最多4 workers、256MiB、120秒。完整输入摘要、输出多重集摘要和最终性序列与原基线一致，40/40原ACK，原资源池 active/charged 清零，0 子进程残留、源码不漂移。本次是功能不变量核验，可能与其他 lane 功能验证并行；时间不用于性能结论，不能替代五轮门。

完整 nodeid、测试命令、解释器、三个私有环境变量、PYTHONPATH、源码摘要、trial/hash、红绿来源见同名 JSON 指向的不可变回执。所有业务数据和输出位于 `/tmp/demiflow-*`；没有改主仓、共享环境或冻结七件。

## 交接与范围

父树若未包含独立守卫，按 `64a4730` → `6c2e46b` → `d1db15e` → `2efa22a` 集成；原 `245ea8c` 已由总控接入则不要重复取本树 cherry-pick。`4fc3a16` 是撤回微优化及同文件系统更正的历史证据文档，保持原负记录。绝不能集成已撤回的 `7b11585` wire 复用候选。

这份修复关闭前一份 I08 调查 §6 的旧 flush 预算缺口。性能调查本身已经收口：无可靠端到端加速交付，原 I08 性能门仍未通过。真实 Ray 全模块组合以总控合并版本继续验收；本 lane 不把 Local 证据代称 Ray 通过。
