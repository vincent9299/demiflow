# 统一入口的实际集成验收与未完成项

2026-11-16。隔离树 `gpt_runtime_v3_review_20261115`，分支 `engine/gpt-runtime-v3-review-20261115`。本轮仍以工作台真实可用、引擎统一正确且有限资源为目标；以下不是整体意图完成声明。

## 已修复并实际验证

旧 chain 入口原先直接调用 `execute_action(...).result()`，丢失当前 run 的预算，返回内部 TerminalResult，也没有关闭 ActionHandle。`19a5741` 改为同一 `make_action_request/run_dataset_action` 公共执行入口；校验借用 action 的 source/完整 operations 及 Local/Ray provider，沿用当前 run/executor 的约束，返回原可重复读取的物化 handle，保留原提交权威并关闭运行资源。

新 L015 用真实执行事实替换已退休 driver 的 AST 断言：实际 sealed graph/physical、worker assignment、原 allocation profile、通道、driver 唯一 CommitCoordinator、失败清理和完成结果恢复。恢复测试锁定同一原封存与 ACK，UDF 文件标记不增加；它证明完成结果重读，不冒充非 final worker 重建。原非 final batch/group/timer/prepend 恢复由另六项真实故障测试覆盖。

旧 native/stream/Engine 接入已集成：execution 预算与 Arrow schema 进入同一原封存；类型转换同时计入输入和新输出；借用 action 不被另建或提前完成；原 v2 载体按显式版本读取，v3 执行仍用唯一原 journal。Ray worker 的预算来自原 RUNNING allocation，并核对 spec、身份和实际 Ray CPU/memory，没有新增资源分配者。同步 native pool 退休选项、`take_batch(0)` 真零读取、聚合 program 标识也已修复。

## 根节点固定版本验收

| 固定源码 | 实际结果 | 覆盖与限制 |
|---|---|---|
| `19a5741` | Local 2 passed | 物化返回/重复读取/原预算/关闭；超大行原限制拒绝且零提交 |
| `298d194` | Local 8 passed | L015 的真实封存、分配、通道、提交权威、失败清理、完成结果恢复 |
| `1065b94` | Local 61 passed、3 failed | 本轮 chain/native/F/原 L010/grant 组合；三个新准入反例的夹具错误保留，不称整组绿 |
| `f38aced` | Local **101 passed / 173.44 秒** | 当前历史适配、原 native 选项、双载体、旧 Arrow 限额、Engine、六种真实 remainder 恢复，以及修正后的三个准入反例；无 skip |
| `f38aced` | 真实 Ray **13 passed / 16.31 秒** | 同一 chain 正常/预算/身份拒绝及完整 L015 实际执行；三阶段记录全通过、私有集群关闭、子进程为空 |

逐输入摘要已与各自 Git 提交字节核对，无漂移；[根节点机器回执](COMPAT-INTEGRATION-PRIMARY-VERIFICATION.json)保留精确 selector、原报告位置及通过/失败 nodeid。Local 原 runner 提供完整 JUnit，没有单独三阶段回执；Ray runner 提供三阶段。没有把两类证据混写。

native owner 的固定 Local 73、实际 Ray 8、grant 定点 12、真实 Ray 三模型恢复 3 项也已逐输入核对。最后三项涵盖 Prompt/Embedding/Agent × 发送前/发送后未确认/确认后三窗口，未知请求保持拒绝自动重发，原请求额度均为 1。见 [独立核验](NATIVE-ADAPTER-PRIMARY-VERIFICATION.json) 与 [详细修复](NATIVE-ADAPTER-CLOSURE-20261116.md)。它们分别绑定自己的提交，不能当成 root 最新 HEAD 全部通过。

## 保留的红记录及复核纠正

chain 最初红基线真实复现错误返回和丢预算。其第一次新夹具把 close 放在 action owner 已退出之后，导致本任务私有临时目录的五个 allocation 未清理；随后在原 RunJournal/ResourceManager 协议下取得本任务 fence 并清理，全部 RELEASED/CONFIRMED，原执行状态及提交未变。原记录 `/tmp/demiflow-root-chain-red-r1-cleanup.json`，不抹成最初就清理成功，也未操作主仓/共享任务资源。

新三个身份拒绝反例先查询尚未创建的 resource 表，又在作用域内吞掉预期失败，导致空 action 被误作正常结束。两次红回执保留；修正后先让真实异常退出 owner，再断言 execute_action 从未调用、原提交为空、资源 owner 表根本未建立。最终 Local/Ray 原三个反例都通过，生产准入逻辑未为此放宽。

独立复审重开了 A020 的一个历史断言：旧 `ex.aggregate(..., ())` 空输入应返回 None，不能用 `aggregate(Sum(...))` 或公开无参数 aggregate 的拒绝代替。`13210ea` 已通过原 Count 程序在同一 aggregate action 计算空性，再仅转换低层返回；没有第二遍源读取、第二 action 或新公共协议。该修复和原 nodeid 已加入当前集成，根节点原用例复验另行记载。在这项复验完成前，历史 90 项的复核并集暂计 **82 已有合格证据、7 图入口待修、1 空聚合待复验**；owner 旧文档的 83 数字以此纠正为准。

## 仍未完成

- 旧显式 graph 入口此前忽略 target plans；修复正在独立 lane 实现。原 3-worker 小预算下已有初步实际分支结果，四种路由、恢复和最终集成尚未交付，不能据此关闭 L017。
- 正式 V035 原先只有 collection 和泛称替代断言，正在接精确 nodeid、环境分类与真实执行回执；8119 条 collection 不等于通过。
- 当前普通 CPU 案例约 14–15 秒，历史约 1.872 秒。启动导入优化的 7.92% 和 GC 小筛选的 5.17% 分别有原对照，未组合测量不能相加，更不代表正式五轮性能达标。启动原始输入另经 [根节点复核](STARTUP-PRIMARY-VERIFICATION.json)。
- 最终同版本 Dataset API/Local/Ray/工作台组合验收尚待剩余改动完成。已有 Notebook/SQL/Case/GPU/多机证据保留其准确版本和边界。

主仓只读核对：`e40a6dd`、原 11 项 dirty 状态/18 份文件摘要保持；承重七件与 `49b666f` 逐字一致，见 [边界回执](MAIN-BOUNDARY-COMPAT-VERIFICATION.json)。没有 push、合入主仓或修改共享环境。
