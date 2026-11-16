# 原停驻窗口修复：1 worker 的实际公平场景

本记录只覆盖有界停驻推进缺陷，**不等于 I08 全部性能达标**。原 fast_cpu 约 26 秒、旧参考约 1.87 秒的差距仍在。

## 第一手缺陷

Performance lane 的原 `/tmp/demiflow-resource-fair-smoke2/`：真正持有原 LocalCapacityPool 的 63/64 CPU，DemiFlow 只可用 1 worker / 1 CPU。原有限 Items 64×4KiB，共享到两个有 snapshot/restore hooks 的 SlowBranch，再 union → iter_rows，内存 16MiB、timeout 120s、checkpoint_rows=64、checkpoint_interval_ms=1000。

120 秒后只有 12/128 个终端行，已有 79 个原 ACK 和 80 个真实 worker，所有退出均 CONFIRMED。封存交换已有 credit_rows=256、credit_bytes=262144；不能把缺陷归因于 1 行 credit。

原 ACK carrier 逐个检查：cut 1–41 才读完源，每轮以最大行宽预先固定成 8 行；cut 45 后 union 收尾出现 “union 输出 1 行 → terminal 处理 1 行 → union 空 cut → union 输出 1 行”的三次启动周期。`park` 立即设 `_paused`，而可重放 cooperative finish 遇到 `_paused` 直接跳过；重启时真实 ACK 后的 resume 与 park 到达时序决定偶然能推进一行，不能形成实际有界工作窗口。

## 修复边界

只修改 `WorkerGraphRunner`：

- 原 `park` 表示处理已准入队列，并在现有信用窗口内继续可重放 finish；达到边界后才报告 idle。原 `pause` / cut 仍冻结，未知状态与 reducer 不获得新的可重放承诺。
- 每一步 finish 仍最多输出 1 行，每步仍处理控制、取消与原 deadline；没有凑批等待。
- `_park_can_output` 只读取原 `ChannelSetState.reader_credit/outstanding_leases/resident_bytes`，不保留第二份额度。按下一行 `max_row_bytes`、静态扇出路径和 broadcast 实际接收者进行保守预检。实际发送仍调用原原子 `reserve`；未知 UDF 扩张保留原 fail-closed 准入，未猜倍率。
- source 在读下一行前做同一余量检查，保留每 worker 64 个未完成 delivery 和原 checkpoint_rows；不预读下一条业务数据。没有已占信用时，第一条实际行仍由原 reserve 判断，保留“实际行小于声明最大行”的原支持。
- 原 routing identity、record identity、snapshot schema、信用归还、提交/ACK、退租完全不变；没有改 planner / 七个冻结文件 / 新增调度器。

## 实际结果

候选基于 root `0712606`，在本树 merge `de62994` 后应用窄 diff；最终生产提交为 `28b4ffc`。harness 取 performance 的固定 `938a78c`（前置 `100aca1`）：只读 worker 内原信用表采样；旧父进程样本仍诚实为 0，新断言使用真正 worker 的记录。没有修改 workload/预算/timeout。

最终固定提交完整证据：`/tmp/demiflow-park-fair-r2/verification.json`，含全部生产源和实际 harness 前后 SHA256、进程内树断言、三份私有资源目录、实际命令与 dirty 状态。`case/proof.json`、`case/run/run.sqlite`、`case/credit-*.json` 是原事实。

| 项目 | 原红 | 修复候选 |
|---|---:|---:|
| 原输入 | 64×4KiB | 64×4KiB |
| 原结果 | 12/128 行后超时 | 128/128 正确 |
| 原上限 | 1 worker / 16MiB / 120s | 完全相同 |
| 原 ACK | 79 | 35，全部 acked |
| 真实 worker 分配 | 80 | 35，全部 CONFIRMED |
| 总时间 | 120s 超时 | harness 65.093s；业务 64.377s |
| 首条输出 | 原未完整完成 | 53.942s |
| 原 charged 峰值 | 原表见旧证据 | workers=1 / cpu_millis=1000 / memory_bytes=622592 |
| holder | 真实占 63 CPU | 全程存活；图完成后实际退出，再释放原 pool |

中间候选 r1 的证据仍原样保留：128 行、40 ACK/40 worker、67.669s，源码 diff 和 dirty 状态诚实记在其 verification；不当作最终固定提交验收。

不是配对五轮性能统计：harness 观测修正使业务类代码身份变化，新的 plan_id 不应伪称与旧红相同；输入、参数、额度与公开语义相同。I08 正式性能矩阵仍由总控安排。

逐个读取原 worker-frontier.metrics 并校验所有引用 SHA：旧 79 cut 中有 10 个既无业务行前沿变化、又未完成阶段的 cut；最终新 35 cut 中该计数为 0。旧核验 JSON 是 r1 目录中的 `old-cut-audit.json`，最终新核验是 r2 目录中的 `cut-audit.json`，并未把正常 source/中间阶段的 0 terminal rows 误判成空 cut。仍有启动时间计入 1 秒 checkpoint interval 导致的 1 行短 cut，35 次启动也仍偏多。本次保留它，未静默改变 checkpoint 时间定义。

## 验收

- 原 `0712606` 的 `_advance_input_completions` 函数在同一新反例中真实返回 0，期望推进失败：`/tmp/demiflow-park-window-old-method-red.log`。通过 AST 加载原函数字节到独立测试进程，未改运行中的候选文件。
- 新窗口 + 原 wake/idle 组：`/tmp/demiflow-park-window-unit-r2.log`，19 passed；之后移除一项只手设 cut 字段的弱测试，真实 cut 由下列公开恢复用例验证。
- 原 durable ACK / 断点恢复 / 丢 ACK / 源扇出 / 有限输出 / EOI 集合：`/tmp/demiflow-park-regression-r1/`，28 passed / 1 deselected，259.17s；生产摘要稳定。之后自审补充 resume 先于 park 的窗口边界，独立反例先红后绿，最终 `28b4ffc` 的新完整组位于 `/tmp/demiflow-park-regression-r2/`，**29 passed / 1 deselected，276.39s**，正常 exit；`verification.json` 将全部生产与 V3 测试的运行前后摘要绑定到该提交，全部一致。
- 最终 19 项轻量窗口/wake/idle 组通过，`/tmp/demiflow-park-window-unit-r3.log`。两个最终组有 5 项新窗口测试重叠，共 43 个不同测试；不冒称 48 项独立覆盖。原 Ray 专项仍明确 deselect，待总控最终组合的真实 Ray 验收，不计为已覆盖。

主仓没有写入或运行测试；没有安装、push、停止其他任务或复制 SSH 文件。

集成入口：生产及专用测试只取 `28b4ffc`；实际 harness 是 performance lane `938a78c` 的原文件。七个冻结件的校验保存在 r2 回归目录 `frozen-seven.json`，与 `362d8528` 全等。最终本 lane 没有残留业务进程、私有 Ray 集群或 holder。
