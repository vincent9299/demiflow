# 有限 worker 下的真实流式后缀闭环

基线为根已集成 `3c63e7f`，本隔离树同步提交 `80d792f` 与其 tree 完全相同。
未修改、测试主仓，未 push。承重七件与冻结基线逐字相同。

## 已实现

1. `map(execution='thread') → save_lance → run_stream` 在原 2 worker 下执行。
   合并的只是 SaveLance 的被动 RPC 代理及相邻增量变换/终端；原 writer、
   NativeStreamLanceService、RunJournal、CommitCoordinator 留在 driver。
   图上的 `journaled`、`io`、状态/恢复声明不改，单 writer partition 不复制。
   所有原 stage 的实际 CPU/memory grant 相加；未知 effect、设备隔离、超预算拒绝合并。
2. 共享源 prepend 的原 15 秒反例修复。基线实际 5 stage 超过 workers=4，
   转 durable 后在 15 秒内经历 7 次进出/cut，虽只读源一次仍超时。
   现在只合并 prepend 与后缀，恰 4 个真实 allocation；source/Map/Limit 保持原 owner，
   两输入端口与 prefix finite scope 保留，结果为 `1,2,3,1,2,3,4`，源只读 `1,2,3,4`。
3. 普通 `from_iter → map → take(1)` 同样支持原 2 worker，thread/process 都覆盖。
   原任务复制/批次语义由原 kernel 保持，不把普通 task 降成共享 live closure。
   新编译外部增量 channel 的行 credit 取原 task_batch_rows 与既有预算更小值，
   字节 credit 不放宽，finite scope 不限总行数。真实 worker 文件观察锁定读取 ≤25，
   修复了原父进程 list 在 process 路径观察不到读取而可能假绿的问题。
4. 已确认 final 的 live action 可读取历史结果：先经原 checkpoint coordinator
   从原 journal 恢复并认证 final cut，再只核当前声明代码和原源归档摘要，
   不查询已释放 registry、不重构用户 actor、不重跑业务/写入。
   非 final live 仍拒绝跨 attempt 恢复；真实修改代码仍拒绝。原计划/身份不重编。

## 验证

最终组合 **49 passed / 0 failed，265.72 秒**。命令、全部 Python 输入摘要、
XML 路径和冻结件检查见同名 JSON。包括本批 10 个新反例/正例、原 prepend 的
前缀及时可见/空输入/取消/共享与非共享崩溃恢复、原 native Save 的确认微批恢复、
外来版本不接管、未知效果拒绝、失败保已确认版本、原 P 放置和单 worker 恢复。
原 `test_stream_native_recovery_preserves_sealed_policy_and_new_run_can_append` 已完整通过。

中间真实红证据：`/tmp/demiflow-stream-seams-red1.log` 为共享 prepend 超时；
`red2.log` 为 native suffix 预算拒绝；`green2.log` 首跑成功后 final-live 读取误拒；
`green4.log` 暴露真实预读 58/65 行及三个本批测试夹具问题，均在最终组合中修复验证。
原 local_kernel 两臂在同作用域吞掉失败 action 后退出失败，是未确认 action 的诚实
RunContext 语义；测试迁移 agent 已分离成功和失败作用域，本批不伪造 action 成功。

## 实际边界

- 本批只在 Local 对上述后缀做窄共址，不声称新增 Ray thread/GPU 能力；设备与
  原生模型仍独立资源边界，不将它们泛化为可融合 effect。
- live action 的中途跨 attempt 恢复仍明确不支持，只有原 final 结果可读。
- 存量已封存物理计划保持原字节；新共址/credit 只应用于新编译。
- root 的后续 small-budget metadata/metrics 改动未纳入本批基线，需总控合流验证。
- 产品长目录的资源退出 evidence_ref 超长是独立后续修复，不计入本批已完成项。
