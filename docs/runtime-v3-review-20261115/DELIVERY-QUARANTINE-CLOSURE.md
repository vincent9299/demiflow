# 原生 delivery early-close 的线程隔离与真实退出

分支：`engine/gpt-delivery-quarantine-20261117`，基线 `4eb8846`。
隔离工作树：`gpt_v3_artifact_recovery_20261115`，主仓未修改、未运行测试、未 push。

原 `test_native_delivery_barrier.py::test_delivery_timeout_quarantines_worker_and_retains_run_lease` 逐字不改。
基线实测失败于第 2 个调用的 15 秒等待：第 1 行虽已交付，driver 为获得普通 UDF 的稳定 snapshot 一直等待，未将原 cancel_grace_s 接到该 terminal stop。
红收据：`/tmp/demiflow-delivery-quarantine-red-r1/receipt.json`（17.59 秒）。

## 最终行为

1. take/迭代关闭触发 terminal satisfaction 后，原 cancel_grace_s 开始约束前缀对齐等待。已开始的 publication 仍按上一提交排空，不截断外部事务。
2. 不能在 grace 内形成合法前缀时停止新输入；原 ThreadHostEndpoint 的 K3 Cancel/Drain 使用真实有限等待，后续 close 复用已在执行的原 cleanup future，不排入重复清理命令。
3. 仍有原 thread unit / owned thread 活着时，不释放原 store、journal service、运行 lease 或物理资源。把一个只做原关闭/回收的线程登记到既有 `RunContext.quarantine_worker`，禁止替换 owner 恢复。
4. 隔离线程通过原 DataflowScheduler、ResourceManager、ThreadResourceProvider 对原 ABANDON 意图继续对账；不再调度图、不创建 worker、不新增 allocator/提交权威。
5. 原 provider 得到真实退出证据、原 journal/native 服务也实际停止后，才关闭 store、回收可达性已证明的工件、释放原程序 owner；RunContext 原 finalizer 随后释放运行 lease。
6. 用户释放 held UDF 后，原测试的 5 秒 finalizer deadline 保持不变，并已通过。正常 early-close 保持真实非final prefix ACK，不进入隔离。

cancel_grace_s 约束的是停止/对齐及各原控制等待；本轮不声称所有不同阶段的总墙钟延迟恰好等于 1 秒。
无法安全杀死用户线程；用户线程一直不返回时，资源和运行 lease 必须继续被原 owner 持有。

## 清理 claim 的真实缺陷与配套修复

候选初期的强化验收发现：held UDF 已退出但部分 sibling control host 仍等待约 30 秒。
第一手线程栈与原 resource snapshot 在 `/tmp/demiflow-quarantine-debug-r5.log`：一项 cleanup CONFIRMED，另一项 cleanup NONE，后者的 control host 从未收到 shutdown。

原 `DataflowScheduler.close/_retire_after_close_failure` 每轮 claim 16 个操作；首个 ensure_absent 消耗短期限后，manager 不执行剩余操作，但剩余操作已经被 claim，必须等原 claim lease 超时。
最终只把这两个**关闭循环**改为每次 claim 1 个可实际尝试的原操作，继续沿用原 claim/退出/记账协议。
未修改 resources 层规则、未伪造退出、未缩短历史 claim lease、未直接退还仍活着的资源。

## 生产范围

- `execution/dataflow.py`：terminal-stop deadline、有限清理 grace、真实 thread unit 未退出时延后关闭 store/service、调用原 RunContext 隔离机制。
- `execution/dataflow_thread_host.py`：K3 等待的实际 deadline、复用原 cleanup future。
- `execution/dataflow_scheduler.py`：两个关闭循环每次领取一个清理操作。

原 RunContext、operator/shared actor、worker 协议与冻结七件未改。
总控的 progress observer / resume observation frequency hunk 位于其他方法，不在本轮修改范围。

## 证据

`DELIVERY-QUARANTINE-CLOSURE.json` 保存完整 nodeid 三阶段结果、收据哈希、命令、解释器、隔离域、最终生产摘要及冻结件证明。
两组测试运行期间源码均不变，生产字节均与最终交付相同：

- `/tmp/demiflow-delivery-quarantine-r6/receipt.json`：8 passed；原隔离测试、3 个实际延迟释放时序、原 4 条 terminal-close。
- `/tmp/demiflow-delivery-quarantine-adjacent-r7/receipt.json`：39 passed；强化资源池 BOUND/RELEASED 断言、线程宿主、原清理重试、控制清理、原 E007/E008/E015、RunContext lease、checkpoint deadline、publication 取消和 take-prefix 组合。

合计 44 个唯一 nodeid 的最近 setup/call/teardown 全部通过，无 skip/xfail/deselect/rerun。
隔离中明确检查：run.active、运行 lease、store writer、journal service、物理未确认记录与原 pool reservation 仍在；恢复被原运行锁拒绝。
释放后明确检查：原 5 秒内 finalizer 退出、全部原资源 cleanup CONFIRMED/pool RELEASED、store/service 关闭、原控制 unit 清空；不存在伪造的 action result。

早期 r2/r4 的 finalizer 延迟失败保留原收据，未扩大 5 秒断言来掩盖问题。
本轮没有重新执行 Ray/GPU/多机全套；原 thread 隔离问题发生在同进程 Local backend，全局组合回归由总控继续。
