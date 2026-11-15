# 检查点进度：按每个逻辑节点的已 ACK 前沿计数

隔离基线 `11ad827`（已同步 root `886775e`）。生产仅修改 `execution/dataflow.py`；承重七件与 `49b666f` 字节一致，主仓未动。没有提高 checkpoint_rows、timer、内存、磁盘或其他预算。

## 原契约与裁定

旧 `execution/stream_barrier.py::CheckpointControl.accepted` 以 `ordinal - last_ordinal >= progress.batch_rows` 请求检查点；`admit` 的 checkpoint_unconfirmed_rows/bytes 独立控制未确认驻留上限。统一运行时 C08 要求全参与者一致切面、原 CommitCoordinator 发布/回执/ACK 顺序，没有把 checkpoint_rows 定义为所有算子操作数之和。业务 batch、传输、持久 segment、checkpoint 的独立性是 I06 原始意图。

`9a68bcc` 修补“源已结束而有限 join 继续输出”的检查点盲区时，加入所有非 source 节点 rows_output 总和。一行经 join→map→terminal 累计三次，造成图越深越频繁切面；这不是资源上限，也不是原有序号语义。

本次按总控确认的裁定保留该盲区修复，改为：同一逻辑 occurrence 的各 partition 输出先求和，分别减去该 occurrence 在上次成功 ACK 的前沿，再取各 occurrence 增量的最大值。源的原总增量、真实 delivery ACK 数仍参与 max；阈值、定时器与完整原切面协议不变。不能用“所有当前节点最大值减去所有历史节点最大值”，否则较早节点的大历史会遮蔽后续阶段的新进度。

前沿字典最多为原封存图的 max_nodes 大小，不随行数、cut 数或 boot 数增长。仅在原 checkpoint coordinator.commit 成功返回后更新；请求、begin、未确认/失败 commit 均不推进。正常进度事件仍对同 boot 计数回退报 FENCED；若内部恢复/重置造成回退，额外防御是请求一次原切面，避免等待计数追上旧值而无限推迟。没有更改持久历史结构、恢复身份或新增账本。

## 实际验收

最新有效结果 **13 passed / 0 failed / 0 skipped**，逐 nodeid、输入摘要、每轮原始结果在同名 JSON；不是声称同一次整组全绿。

- 40 层同样 9 行不会因深度达到阈值 10，任一节点达到 10 才触发。
- 早期节点已 ACK 100 万行，后续节点从 2 到 12 仍会独立触发。
- 同 occurrence 两 partition 合并后判断阈值；前沿只有一项。
- 输出回退、输出观测暂缺、源回退均保守请求；请求不前移 ACK 基线。
- 原源分区累计、真正 `_ack` 调用计数、原定时触发保持。
- 原 `_advance_cut` 的 commit 抛错不改前沿/ACK计数/时钟；成功返回才更新，随后发送原 receipt/resume。
- 新真实 Dataset `两个有限输入→join→慢速map→count`，源结束且已确认后，join 输出达到阈值自动请求检查点。两有限输入交接的真实 ACK 数为 2，低于阈值 64，timer 为 60000ms；因此它们不可能替代该输出进度触发。没有手工调用 request_checkpoint。原 commit 成功后在 join 尚未完成的游标处中断，再从同一个 RunRef 恢复，真实结果为 1000，原 commit 仍是 acked。
- 原 `test_finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity` 未改，通过完整 100×10 的每对组合与重复多重性恢复。
- 原 31 行 `test_narrow_chain_batch_boundaries_and_limit_match_serial` 未改，通过 batch/limit/serial 对照。

原始轮次：`/tmp/demiflow-checkpoint-frontiers.xml` 为 12 passed / 1 failed（新自然切面夹具过快，1000 行在观测刷新前已完成，没有中途断点）；R2 新夹具加入真实慢 map 后已到断点，但“必须零 delivery ACK”的新断言误把两个有限源 handoff 排除；R3 精确改为真实 ACK 低于原阈值、源 EOI、原计时未到的因果条件，完整恢复 **1 passed，13.53 秒**。生产代码在三轮之间未修改；未修改任何原测试断言或预算。

命令使用本树 `PYTHONPATH=$PWD:$PWD/tests`、`env/bin/python -B -m pytest`，独立 `/tmp/demiflow-checkpoint-frontiers[-rN]` basetemp/resource root，运行中未改 Python/HEAD。首轮为新文件、原 finite join 恢复和原默认短链；后两轮只复跑新增自然切面用例。原始日志与 XML 均保留。

此包尚未重跑合流后 40k 压力、多主机或完整 API 矩阵；不宣称达到整体性能门。它修的是原进度口径与图深度耦合，原 ACK 数仍可能因真实跨阶段交接而较大，不假称所有物理布局切面次数完全相同。
