# 真实恢复入口、未知提交与预算错误闭环

基线：根树 `dec2e23` 加此前 provider 修复 `40c45b2`，合流点 `9846342`。
本批未改 run_context 的恢复准入；验证根 `07abd99` 的入口后仍由原 coordinator 判定。

## 三条原红逐项处理

1. **FormalSink 回执**：v3 外层键为 `write_datasink`，原 sink 的 receipt 完整保存在
   `data.native_receipt`。测试现在核原 `output` sink、同一个 commit_id、committed
   状态及 written_rows=2，并用实际 worker 文件证明输入只计算两行，恢复不重算，
   `NeverPrepare.prepare` 仍不得执行。
2. **未知提交**：新增 `DataflowUnresolvedCommit(DataflowError, UnresolvedCommit)`，
   固定 `UNKNOWN_EFFECT`、不可重试。commit/recover 保持同一分类和两种 catch 表面；
   固定词汇的控制消息及 pickle 都保留它，旧 UnresolvedCommit wire tag 仍兼容。
   NativeTerminalSink.inspect 仅在原同 commit 的 rows receipt 已验证、native inspect
   却无确认时拒绝，而不把该不确定状态返回成“尚未调用”。这阻止了 LyingSink 恢复
   重复调用 native.commit。首跑与恢复均 prepared、无 ACK，native.commit 总数为 1。
   尚未发布的真实 prepared 可恢复完成；错 commit 的 rows marker 不能取得证明。
3. **启动前准入**：移除过时 concurrency=65 任意门槛，保留原测试目的。
   显式 memory=64KiB、batch=8KiB、row=4KiB、state=8KiB，8 并发/queue_depth=1 的
   实际工作集不可行，应在 source、actor 构造和 actor 调用之前拒绝。
   这些动作都用 worker 可见文件观察，不用隔离进程看不到的父列表；无提交事实。

## 额外契约验证

- 纯 run_stream 有原 v3 seal、没有 journal cut 时仍拒绝恢复，callback 从未执行。
- prepared 尚未 native publish：恢复复用原输入，只提交一次，正常取得 ACK。
- 伪造另一 commit 的 rows marker：同样 UNKNOWN_EFFECT，不执行 native.commit。
- `ExecutionBudgetExceeded` 使用固定类型词汇和有界 ASCII budget 名，保留原 message。
  UNKNOWN/运输失败不能伪装成确定预算拒绝；错误分组中两种类别也保持区分。
  真实 process worker 抛出的原 budget 名/消息完整到达 driver，调用次数为 1。
- coordinator 对这个已知预算类原样透传，避免把明确准入拒绝误贴 INVALID_CONTRACT
  或 UNKNOWN_EFFECT。run_journal.py 仅含总控授权的 `message=None` 构造签名小改动，
  没有修改 grow/storage_usage（总控拥有）。

## 证据

基线 `test_execution_recovery_entrypoints.py`：3 failed / 4 passed，19.37 秒。
最终组合 **30 passed / 0 failed，50.73 秒**：原入口 7 项、新契约 17 项，
原 D004/D005/D008/D009（含真实子进程崩溃恢复）、错误分组与取消词汇回归。
完整命令、源码摘要、冻结七件校验、XML 路径见同名 JSON。

红日志 `/tmp/demiflow-entry-red.log`；最终 `/tmp/demiflow-entry-final2.log`。
曾有一次最终命令选择器拼错，退出 4 且零测试，已更正后完整执行；未计为验收。
没有削弱业务断言、扩预算、增加新提交账本、自动重试未知外部效果。
未改主仓、未 push、未操作生产服务。后续总控 grow/性能与其他 lane 补丁需合流验收。
