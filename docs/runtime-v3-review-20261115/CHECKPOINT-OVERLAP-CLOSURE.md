# 原生 checkpoint publication overlap 收口

交付分支：`engine/gpt-checkpoint-overlap-20261117`。隔离树：`gpt_v3_artifact_recovery_20261115`。
基线为总控 `2fd6d23` 加已交付 Arrow carrier 的等价 cherry-pick `79ad4e9`。
本轮不修改主仓、不 push、不新建提交/恢复/资源分配权威。

## 已实现的用户行为

单个常驻物理阶段内，所有实际 worker 完成局部对齐并封存不可变 contribution 后，原 driver 在一个后台 task 中运行原 `DataflowCheckpointCoordinator.commit`。
原 `CommitCoordinator`、`RunJournal`、sink prepare/inspect/commit、持久 ACK、原 cut/record/commit 身份全部沿用。
整个执行任意时刻最多一个 publication task、一个待提交 cut；不是并行发布多个事务。

原 journal 已记录 prepared 后，ObjectSink.prepare 已转移该代对象，driver 才开放下一代有限输入。
driver 同时继续转发实际数据、ACK、journal RPC、EOI、取消；后台 task 不承担图调度。
真实测试证明：首 cut 的 actor state 与输出仍恰好是 0–3 时，实际 worker 已处理到输入 8。
Local 和真实 Ray Core 使用同一实现；Ray 验证阻止所有 Ray Data 导入。

`checkpoint_unconfirmed_rows/bytes` 由 driver 在实际 source owner 间分摊，各份额之和不超过原 run 上限。
worker 激活不会擅自读取 source，必须接收原控制通道的 resume/有限份额。
每一行业务按真实 Record.encoded_bytes 计入未确认窗口；读取下一行前保守预留 max_row_bytes，单行超额确定拒绝。
窗口比 max_row_bytes 小但实际行合法时允许第一行，再等待实际 ACK；这可能更保守，但不会静默超额。
窗口不足以同时激活所有 source owner 时，在 ACK 后轮转份额，EOI owner 不会永久占住唯一 credit。
这是流量准入，不创建第二个 allocator，也不承诺第三方库的硬进程 RSS 上限。

receipt 只释放对应 cut 冻结时的行/字节与进度，发布期间前进的部分继续保持未确认。
checkpoint 行触发按每条 sealed edge 的 ACK frontier 及各 occurrence 输出取最大进度，避免纵向 pipeline 深度重复计行。
真实 64 行 Lance 写出、8 行 checkpoint 下，首 cut 输入恰好 8，全部 cut 不超过 9；目标仍只有一个版本、完整 64 行。
中间切片准备的是原 NativeTerminalSink 的 durable prefix manifest；外部 Lance 原生 prepare/publish 仍在 final 一次完成，未声称中间切片已原生发布。

取消、业务失败、checkpoint timeout 会先停止新输入，再保持原控制服务可用，排空已经开始的 publisher。
真实发布产生的 ACK 不撤销；未完成 action 不伪造 result。两个原 deadline（alignment 起点和发布完成后超时）与总 state 字节守卫保留。
原 publication 函数已开始后不能安全杀线程；其外部 I/O 的返回期限仍属于对应原生 sink/SDK，不声称总能在 checkpoint_timeout_s 内物理中止。

用户在旧 cut 发布时完成 take/关闭迭代，旧 ACK 后还会封存实际交付的新前缀。
真实反例是首 cut 1 行、用户 take(3)：最终历史 prefix manifest 必须是 3 行，由第二个真实 ACK 确认，不能拿旧 1 行 cut 标成功。

## 变更边界

生产文件只有：

- `execution/dataflow.py`：一个 publication task、持续控制泵、prepare 后恢复输入、原 source credit 分摊、原 edge frontier 计数、取消/排空、真实 prefix 二次切片。
- `execution/dataflow_checkpoint.py`：source EOI 单调观察使用独立短锁，避免被原 publisher 的长锁挡住；原 commit 总 state 守卫保持。
- `execution/worker/dataflow_graph_runner.py`：有限 source credit、cut 冻结的未确认计数、receipt 释放、source 完成的流量调度提示。

冻结七件逐字节等于 `2fd6d23`。Record、CutRequest、CutContribution、RecoveryImage、WorkerAssignment 等冻结 DTO 未改；旧恢复游标、旧工件和旧身份公式未重写。
source_done 控制提示只用于分配流量份额；历史 finality 仍必须由原 source cursor/contribution 验证。

只对单常驻阶段的非final publication 开放 overlap。
有限阶段退场、durable residency 交接仍按原物理退出与 ACK 顺序停顿；没有拿额外常驻资源换取“全图同时执行”。
B 组件测试的模拟 driver 显式发有限 resume credit，保留原 source 读取、取消、背压、恢复、封存等断言，并新增“无份额不读源”的正向测试。
旧 overlap 测试的 driver 私有 Event 与旧 sink/state 名已迁为真实跨 worker 文件证据、原 v3 kernel image、原 prepared output；原 0–3 状态、0–3 输出、worker 到 8 的业务断言完整保留。

## 验收证据与范围

详见同目录 `CHECKPOINT-OVERLAP-CLOSURE.json`：收据摘要、解释器、三个隔离域、PYTHONPATH、命令、各 setup/call/teardown、各版本生产摘要与最终输入差异均列出。

- `/tmp/demiflow-overlap-r5/receipt.json`：38 passed，核心窗口、原 overlap/deadline、进度和 worker wakeup。
- `/tmp/demiflow-overlap-adjacent-r6/receipt.json`：54 passed，原 D/E、有限与 durable residency、终端关闭、物理清理、总 state 守卫。
- `/tmp/demiflow-overlap-ray-r9/receipt.json`：1 passed，私有 2 CPU/0 GPU 真 Ray Core，有限 6 行窗口与原 ACK。
- `/tmp/demiflow-overlap-final-r10/receipt.json`：10 passed/1 failed；失败是新测试把前缀准备误挂到仅 final 调用的 LanceSink.prepare，已在 r11 改观察点并保留原外部一次发布断言。
- `/tmp/demiflow-overlap-workers-r11/receipt.json`：22 passed，B 全组件加真实 Lance 行触发。
- `/tmp/demiflow-overlap-prefix-r12/receipt.json`：6 passed，新 take(3) 重叠反例、只确认封存 frontier、原四条 terminal-close。

这些不是一个最终快照上的全量套件。r5/r6 后有 deadline/重复 contribution/失败排空硬化；r9/r10/r11 后增加 terminal-prefix 修正，r12 在最终生产字节上验证对应受影响组合。
所有列入通过的 nodeid 最近一次都有 setup/call/teardown 三阶段通过，无 deselect、xfail、rerun 或收集冒充执行。
之前 r1/r2/r3 的新测试 helper/窗口轮转问题以及 r7/r8 的 Ray fixture 路径/API 错误均保留在 /tmp 原收据；没有覆盖原失败记录。

## 后续明确未闭环

- `tests/test_native_delivery_barrier.py::test_delivery_timeout_quarantines_worker_and_retains_run_lease` 的持有线程 early-close 隔离，继续作为下一独立修复；本轮不宣称已修。
- 多机 publication overlap 未在本轮执行。
- 全历史回归由总控承接，未用本轮局部绿色替代全局验收。
- shared actor owner 的新 driver 准入两行由总控集成，未混入本轮固定输入证据。
