# Dataset 完整链路继续修复与验收 E

本轮生产/测试合流截至 `7c034a8`，仍在隔离分支 `engine/gpt-runtime-v3-review-20261115`。整体未完成，没有把已修复计数换算为产品或引擎 100%。

## 原始意图与本轮相关性

- 产品：统一、真正可用的工作台，看懂、配置、运行真实 pipeline，分析、比较、评测数据。保留 D1.9、开发/管道/数据/README、Notebook Python/SQL、Case/媒体及历史；当前运行图必须来自原封存计划。
- 平台：同一 Dataset 和统一 runtime，Local/Ray Core 是后端；正确、稳定、高性能、通用、架构干净，有限 CPU/GPU/内存/磁盘，真实业务完整提交及恢复；原 RunJournal、CommitCoordinator、allocator、历史身份继续唯一。

本轮重点是“用户原输入 → 原运行 → 原结果/原写出 → 失败恢复”的实际可用性。有限资源拒绝本身正确，但原预算下合法业务仍不能跑通时，不算满足能力意图。

## 已集成内容

| 内容 | 当前实现与证据边界 |
| --- | --- |
| 大 literal 输入 | 复用原 RunArtifactStore、类型化 Record、SegmentForest；大载荷不再塞进 plan pickle。小输入和旧大载体仍按原格式读取。items/Arrow/NumPy/pandas、重复/有符号零、split seek、原 ACK/GC/恢复有真实证据。原 identity 算法不改；NumPy/Arrow 的旧 pickle reduce 内部分配不冒称恒定 RSS。 |
| 同一 worker 的读入预算 | literal/finite reader 的索引、缓冲、peer pending row 与 kernel 共用原 grant；增长前准入，打开失败释放 held metadata。没有新增第二 allocator。 |
| 严格恢复 | DataflowExecution 在加载/验证原 physical 后绑定该 policy，DriverJournalService 在首个业务调用前禁止 checkpoint → attempt_only 降级；恢复时当前较宽松默认值不能覆盖历史要求。锁/Task 工厂拒绝后原 stop/drain/close 执行，0 次业务调用。 |
| 回调上下文与错误 | worker 的构造、start、同步/异步/线程回调、snapshot/restore/close 看到同一只读 run 身份与实际收紧限额；不能创建第二 action/journal。节点 budget 名称复用原 identifier，Lance 输出超限保留原 ExecutionBudgetExceeded，不变成超时或 INVALID_CONTRACT。 |
| 有限前沿与批处理时序 | 原 sealed frontier 恢复、终端非 adopt 有限输入中途 ACK、active cut FIFO、cut/receipt 不重启 idle timed batch 均已合流。重复 cut 测试额外收紧源预读 credit 至 4，保持原 24 行、至少 4 次提交及历史载荷只写一次断言。 |

严格 policy 新增后，总控实测两条直接构造 RPC authority 的旧测试未绑定 policy 而失败。`7c034a8` 只让专属 fixture 按原 resolver 的 policy 在注册 worker 前绑定；生产 fail-closed 守卫未削弱。该 fixture 修复的组合复验待本报告之后执行。

## 固定版本实测

各行是独立固定版本，**不是最新 HEAD 同批全绿**。总控已逐份核验原输入 SHA-256 与各自 Git 提交字节一致、执行前后未改、三阶段齐全、结束无活跃子进程。

| 固定版本 | 原回执 | 实际结果 |
| --- | --- | --- |
| `d729ca3` | `/tmp/demiflow-root-d729-core-r1-evidence/verification.json` | 163 项：162 通过，1 项旧预读窗口观察不确定。 |
| `d39f965` | `/tmp/demiflow-finite-cut-credit-r1-evidence/verification.json` | 上述同一原 selector 修正 source credit 后通过；保留原失败。 |
| `77cacbf` | `/tmp/demiflow-literal-input-r8/receipt.json` | 26 项通过，含原 public Dataset 中途 ACK/GC/恢复、4 格式、旧载体及损坏/预算边界。 |
| `19a4375` | `/tmp/demiflow-history-actor-native-r2/receipt.json` | 12 项通过：原 1100 输入/1762 输出及四故障窗口、actor 状态、已 ACK 不重做、严格恢复及清理。 |
| `61c755d` | `/tmp/demiflow-history-sealed-requirement-r1/receipt.json` | 新增历史 policy 不被当前默认值削弱 1 项通过；不声称此版本重跑 12 项。 |
| `9840bbd` | `/tmp/demiflow-history-image-final-r1-evidence/verification.json` | 图像最终 write_lance 三故障窗口通过；corrupt_committed 先遇 reserved-without-response，尚未到原 checksum 校验，保留失败。 |

逐文件审计结果：[机器记录](INTEGRATION-CONTINUATION-20261117-E.json)。literal 详细边界见 [source 报告](LITERAL-INPUT-REVIEW-20261117.md)。

## 真实未完成项

1. **图像部分完成后协作取消 → 恢复**：存在预留后未保存完整响应的请求，原 unknown 防重发保护拒绝恢复。要修复响应与取消生命周期，不能删除 intent、重发付费请求或把 unknown 当可恢复。最终发布窗口通过不替代此问题。
2. **大行模型 → 原生 sink 的共同驻留**：候选已有原三宽行故障恢复通过，但总控发现 segment index、实际 Python batch、解码 record、Arrow parent、writer 共驻计量仍有遗漏；负责人在补同一原预算的增长前检查。候选尚未合流，不据 13 项局部通过宣布完成。
3. **原 64/128MiB 宽 join**：输入封存缺陷已关闭，当前失败转移到真实 planner 准入。原数据、预算、断言不变；需要减少实际副本/合理 phase，不能把资源拒绝当功能通过或直接提高预算。
4. **组合终验与性能**：待上述生产合流，验 Local/Ray 同一 Dataset、原提交恢复、产品真实图/结果/Notebook；原 U3、受影响 GPU、正式 I08 仍未完成。功能测试与性能静默窗口分开，旧零 ACK 与新恢复保证不同不能宣称同比加速。
5. **V035 历史裁决**：按可执行 fixture 分批保留原反例；真实 SIGKILL 从 thread 内杀 pytest 的旧夹具要迁成自有进程，不能变成普通异常。真实生产源/外部 PostgreSQL/两机当前入口等环境缺口独立保留，不算通过。

## 工作区纪律

主仓 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65` 未修改或运行测试；原 dirty 列表、18 文件摘要与此前记录一致，七件冻结字节相等。没有 push、合主仓、接管服务或 SSH copy。这里只更新隔离树实现、测试与证据。
