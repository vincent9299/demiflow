# 图分区恢复收口与正式性能前的剩余问题 H

原始意图保持：产品让用户看懂、配置、运行真实 pipeline，并分析、比较、评测数据；引擎提供同一 Dataset 下正确、稳定、高性能、通用、资源有界且架构干净的计算平台。功能恢复通过不等于性能达标，也不等于两大意图全部完成。

## 已集成的实质修复

隔离工作树为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`，分支 `engine/gpt-runtime-v3-review-20261115`，本次代码与测试集成到 `20c4ebd`。

- 单 worker 可以依次执行原图的多个 reader partition；保留原分区、路由序列、参与者与 ACK，不把没有运行的分区伪装成已完成。新分区波次信息进入原 cut 的版本化载体；旧 v1 历史按原定义恢复。
- 普通后缀融合保留显式 router 边界；新分区启动不再误用同 stage 其他分区的恢复镜像。
- 显式 `storage_bytes` 保持原来的**每通道**上限，同时受整个 run 的磁盘预算约束。自动从 memory 转为 durable 也保留该上限。这里修正 G 报告中“把池分给真实边”的不准确概括。
- worker 真实连接重置按原 `UNKNOWN_EFFECT` 处理；不增加重试、发送或提交。历史代码修改精确拒绝在新 worker 启动前，原 ACK 不变。

## 验收证据及其边界

| 固定版本 | 实际结果 | 证据 |
| --- | --- | --- |
| root `cea8226` | 11 项通过、33 个完整阶段：波次恢复、显式/自动 durable 存储边界、两版元数据上限、三个真实 SIGKILL 代码固定双臂、原 delivery barrier | `/tmp/demiflow-root-graph-transport-combined-r1-evidence/verification.json` |
| 波次叶 `2ea4b77` | 8 项通过：三种交换模式，广播/轮转/哈希/指定目标路由，原真实 Ray durable parking | `/tmp/demiflow-graph-waves-r5-evidence/verification.json` |
| 波次叶 `416929f` | 4 项通过：1 字节前置拒绝、自动 durable 仍拒绝、1 MiB 原每通道正例 | `/tmp/demiflow-graph-waves-r6-evidence/verification.json` |
| 历史组合 `fba93b3` | Ray 5 项完整通过；Local 批次中原恢复 5 项通过，另 1 项旧错误类型观察失败，consumer 中途到批次 900 秒、storage 未运行，原失败/未完成回执保留 | `/tmp/demiflow-v035-core-ray-graph-combined-r1/receipt.json`；`/tmp/demiflow-v035-core-local-graph-combined-r1/receipt.json` |
| 历史组合 `5995caa` | 仅补前批未闭环的 3 个原节点，全部通过，314.11 秒；生产与 fba 相同，只修 code-pin 测试的精确错误类型和增加原 ACK/无新 worker 断言 | `/tmp/demiflow-v035-core-local-remaining-r2/receipt.json` |

历史 Local 8、Ray 5 的通过证据跨两个测试提交，不能称某一 HEAD 一次全套通过。原数据、业务预算、真实 SIGKILL/actor exit(17)、精确输出与原 ACK 保护没有削弱。单 worker/三分支/24 行的增量恢复出现约 120 次实际 ACK/波次；这说明正确性已验证，也暴露小并发切换成本，不能把它称为高性能证明。

历史负责人进一步归并原 Local 18/Ray 6 的精确节点：24 项最后有效观察全部通过，分属 5 个明确 HEAD。总控独立核对每个节点对应回执的 SHA256 与 setup/call/teardown，24 项均匹配。原先另列的 33 项新失败也已在既有 `CONTRACT-SUITES-CLOSURE.json` 与原 XML 中逐项关闭（33/33，版本 `6b2a731`）；它们与本轮 24 项没有节点重叠，本次未重复跑。完整索引为 `/tmp/demiflow-v035-core-test-migration-20261117/core-mapping.json`，仍不表示全仓或最新 HEAD 全套通过。

前批固定 `8928e5b` 的 Local/Ray 各 98 入口、168 场景、工作台/Notebook/Case、原图像故障窗口、队列与实际 H100 证据继续见 [G](INTEGRATION-CONTINUATION-20261117-G.md)。未受影响的已验部分不重复全量跑，也不改写成当前 HEAD 的新回执。

总控的独立摘要审计在同名 JSON，逐条对照各自 Git blob；包含失败/未完成回执。图修复另经独立只读审查，发现的自动 durable 存储上限遗漏已修并实际复验。

## 尚未关闭

1. **40 KiB 原生写出**：候选 `866395b` 在不改变原 R/B/M、数据和断言下通过原写出恢复及共驻边界等 9 项；但独立复审确认外层 manifest、常驻 columns 与 snapshot 编码副本未完整计入新 terminal workspace。该 P1 正在原叶补修，候选暂未集成，9 绿不代表预算生命周期闭合。
2. **I08 正式性能**：最终版本尚未冻结，正式 18 格/234 次试验仍未启动。原预热、五对交替、实际取消、1.2 比值门和原数据/预算不变；历史 B0 与当前恢复保证不同，必须并列披露，不能冒称同保证加速。两机缺测不得由同机 Ray 推断。
3. **环境与完整历史边界**：旧已授权两机 SSH 入口仍公钥认证失败；PostgreSQL 无测试 DSN。原固定生产数据不擅用。V035 的全量历史节点没有全部最新版本回执；没有证据不算通过，与核心意图无关的外围补全也不无限扩大成交付前置。

主仓保持 `e40a6dd`、原 11 项 dirty/18 文件摘要，七件冻结保持；未 push、合主仓或修改在运服务。
