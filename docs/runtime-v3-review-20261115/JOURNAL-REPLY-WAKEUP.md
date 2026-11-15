# 原 journal 回执就绪唤醒

基线：root `9ed9ae6`，本隔离树合并为 `cb5184883346df19e4941b18bbd08b8977dff759`。生产只改 `dataflow_authority.py::DriverJournalService` 与 `dataflow.py::DataflowExecution.pump` 的本地等待接线；不改 join、block clone、检查点阈值、预算、原提交协议或主仓。

## 问题与实现

原 journal 服务线程把真实回执放进 `_completed` 后，没有通知 driver；driver 正在等待 worker socket，而 worker 恰在等这份回执才能继续，故必须等下一次最多 2 ms 的轮询。4k 诊断中，pump 的 5686 次等待耗时 10.873 秒；不能把这些等待全部当成可消除开销，业务计算也会合法等待。

现在每个原服务固定拥有一个非阻塞 socketpair，两个控制句柄不混入 artifact 文件窗口。`_completed` 从空变为非空时在原锁内写一个就绪字节；部分读取仍保留可读，最后一个回执出队后在同一锁内清空字节。新完成请求必须重新通知，不能发生“先清空后覆盖”的丢唤醒。通知不携带结果、不确认效果、不另存回执。

Local 原 select 同时监听 worker socket 和该就绪句柄。读取结果仍走原 `poll_replies`，原 2 ms 最大等待、deadline、源/输出进度和 checkpoint policy 均保留。Ray 原 poll 分支不增加阻塞。

初始化失败会关闭两端并停止已启动服务线程。正常/异常 close 先在原锁内标记 closed、关闭两端；迟到 dispatch 不会再通知或发布新完成回复，原在途 UNKNOWN_EFFECT 语义保持。后续 native close 抛错也不会遗留这两个描述符。

## 真实验证

- 原基线 `/tmp/demiflow-journal-wakeup-red.xml`：新增两项 **2 failed**，证明没有回执就绪通知入口。
- 控制专项 `/tmp/demiflow-journal-wakeup-green1.xml`：**11 passed / 4.43 s**。真实 service 线程与原 RunJournal reserve；完成发生在空 poll 后；部分 drain；最后一次 drain 与新的 append 精确竞争；close 时已执行但未返回的请求；迟到结果不变成第二回执；socket 配置失败和第二个线程启动失败；native close 抛错；Local/Ray 等待分支；原 mailbox 两项。
- 组合 `/tmp/demiflow-journal-wakeup-real.xml`：**14 passed / 22.65 s**。包含上述 11 项及真实 Dataset 的 Lance after_prepared 中断恢复、Prompt 原 native journal 首跑/恢复不增 HTTP、Embedding 原单请求与预算事务。无外部真实模型服务，只有自有 loopback provider。
- `source_before`/`source_after` 完整摘要相等，实际 import 路径与固定基线绑定在 `/tmp/demiflow-journal-wakeup-real-verification.json`。

## 同条件短对照

`/tmp/demiflow-journal-wakeup-benchmark.py` 使用同一个真实服务实现、原 grant/fence 检查与 SQLite reserve；只比较 driver 等待列表是否包括就绪通知。每臂先独立预热 10 次，再测 80 次，交替顺序重复三轮；每臂原 journal 准确记账 90 次，原 2 ms 上限不变。

中位数：原 timer 等待 **0.16984 s**，通知等待 **0.05911 s**，减少约 65%。这只是受控顺序 RPC 的等待改善，**不是 40k 业务吞吐提高 65% 的声明**。完整每次 operation ID、预算原值、源摘要及计时位于 `/tmp/demiflow-journal-wakeup-benchmark/results.json`。原 40k/120 s 与最终 Local98/168 矩阵留待主控合流后的固定候选，未在本批重复运行。

本批源码及证据索引见 `JOURNAL-REPLY-WAKEUP.json`。七份承重文件字节冻结不变；不 push、不合主仓、不改变任何在运服务。
