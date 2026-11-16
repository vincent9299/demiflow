# V017 路由与恢复：原执行链真实验收（2026-11-16）

同一代码提交 `17a93bc69cfaac8215d263710d61dd102b6f5e69` 的正式 Local 与真实 Ray Core 门均已通过。覆盖既有 Dataset 业务路由、共享上游、原生物理交换，以及原非 final ACK 后实际杀 worker 的恢复。**这不是新增公共 target/broadcast DSL，也不是多机路由故障或整体交付完成的证明。**

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`；分支：`engine/gpt-v3-regression-20261115`。两大原始意图保持：产品看到原执行事实；引擎以同一 Dataset、同一有限资源与提交恢复权威执行。没有第二执行器、资源分配器或提交账本。

## 两处生产修复

1. `60e8d30`，`DataflowExecution.prepare`：原生调用者已在当前 action 首次封存合法物理计划时，读取原 journal binding，沿既有载体、action、代码 pin、平台版本与 source 校验执行，不重新编译第二份计划。当前 attempt 的 thread live program 校验原 registry owner；历史 thread 结果仍要求原最终 ACK。只有 prepared 文件而没有原已提交 cut 的 resume 仍被拒绝。新增真实正反例锁定 process/thread、错误 action、载体篡改、摘要正确但实际程序 pin 错误，以及无 cut 的历史恢复拒绝。
2. `24e2cb0`，`native_environment_names`：为已封存 `RoutedPromptFactory` 的平台 `PromptFactory` 子声明提取环境变量名字，并交给原 allocation 环境投射。只读取精确被动类型，最多 32 路、原深度 4、总名字 128；不构造 actor、不读取任意属性、不转发无关环境。变量值仅用于当前 worker 环境，不进入封存身份。真实 Ray 集群先启动、模型 URL 后设置，排除了依赖 driver 启动时环境继承而偶然成功的情况。

`d860639` 补实际反序列化代码 pin 重算负例；`17a93bc` 补共享上游的已提交前缀跨 attempt 只执行一次断言。生产修改仅上述两个文件；其余为真实场景与测试。

## 正式门跑了什么

每个恢复案例都实际运行原 worker，在原非 final ACK 之后杀死属于本执行的 worker，随后经原 `RunContext.resume` 读取同一 `dag_id/plan_id`。断言原提交前缀逐项不变、新 attempt/fence 生效、原 channel 的正数 routing ordinal 恢复，最终原 ACK 和结果正确。Local 使用对应 bootstrap PID 的 SIGKILL；Ray 使用对应实际 `HostedDataflowWorker` actor 的 `ray.kill(no_restart=True)` 并验证实际死亡。

| 层次与案例 | 真实结果及断言 |
|---|---|
| 公共 Dataset 共享源 → 两条模型业务路由及 echo → union | 12 输入、24 输出、12 次 HTTP，每路模型 6 次；源和消费者 PID 分离，实际只有一个共享源，每 attempt 的源调用不因分支重复；已提交前 2 条跨恢复只计算一次 |
| 公共 Dataset join → repartition → map_async | 12 条完整结果，原 hash/rebalance 交换；恢复的是实际 streaming channel 的非零 ordinal 后缀，不能只凭有限输入重新读出正确结果通过 |
| 原生物理合同 hash/rebalance/target | 使用原 compiler 产物，在**首次封存前**配置两个真实消费者分区，走原 seal、worker、交换、提交、恢复；每种最终 12 条，target 与每条记录声明的物理 reader 一致 |
| 原生物理合同 broadcast | 同一原生链路，12 输入 → 24 输出；每条由两个不同消费者 PID 各处理一次；恢复后 reader 集合、多重性和 ordinal 不漂移 |
| 未知业务模型 / 未知物理 reader | 明确失败；未知模型零 HTTP；两者均无成功 commit/ACK，不默选另一路 |
| 重复在途投递 / 原 credit 与 fence | 六条执行链均向原 handler 重复投递一个真实未结算事件，原 handler 明确拒绝且在途集合不变；四种原 channel 组件另锁定 credit、持久快照、恢复与 fence。组件验证不冒称额外公共 API 场景 |

原生物理合同案例调用原 `make_action_request`、`lower_action`、source snapshot、compiler、`seal_dataflow`、`DataflowExecution`，不是 mock compiler、手写执行器或封存后改计划。首次执行明确属于底层物理合同验收，恢复仍经原公共 Dataset action。公开 API 与底层合同两层的证据在 JSON 中分栏，未合称“公开 target/broadcast lowering 已实现”。

## 固定版本证据

| 环境及精确选择 | 结果 | 原始回执 |
|---|---|---|
| Local：正式 `test_route_recovery` + `test_runtime_v3_presealed_entry.py` 全文件 + `test_runtime_v3_routed_environment.py` 全文件 | 16 passed / 0 skipped，152.20 秒 | `/tmp/demiflow-v017-local-final-evidence/verification.json` |
| 同机真实 Ray Core：正式 `test_route_recovery` | 1 passed / 0 skipped，176.03 秒 | `/tmp/demiflow-v017-ray-final-evidence/verification.json` |

两组各有原 `scenario-route_recovery.json`、JUnit、完整命令、进程内导入路径与测试报告。每组 2188 个源码/测试/打包输入前后无变化，两个集合相同；另实际遍历 `git archive 17a93bc` 对比这 2188 项 SHA-256，零不符。七件冻结文件逐字等于 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`。

所有命令显式绑定本树 `PYTHONPATH`，资源 ROOT/HOME/DOMAIN 都在本次 `/tmp/demiflow-*` 私有目录。Ray 为本次创建的 8 CPU / 0 GPU 私有集群，结束 `shutdown_completed=true`；两组 `children_after_environment_close=[]`。Local 原 fixture FD 为 53→53；Ray 为 38→41，在原容差内，**不称零增长**。两环境 Ray Data 导入探针均为零；两组都观察到 9 次真正的公共 `execute_action` 调用，底层首次封存调用不伪装成公共入口调用。

配套 [机器回执](ROUTE-RECOVERY-CLOSURE-20261116.json) 保留所有证据文件 SHA-256、每条链路的实际死亡身份、计划/attempt/fence、提交前缀、正数 ordinal 与结果数。报告提交只新增文档，不改变已经测试的生产/测试字节。

## 保留红证据和范围

初次真实 Ray `60e8d30` 在预启动集群中因 routed 子模型环境缺失失败，原回执保留于 `/tmp/demiflow-v017-ray-fixed-evidence/verification.json`。本次通过来自上述环境投射修复，未提前设置模型环境以掩盖问题，未扩大原预算或跳过失败分支。

本项不证明：新增公共物理路由 DSL、业务名称到物理 reader 的新 compiler 映射、两机路由恢复、整机断电/网络分区、Ray 小预算公平运行、完整性能矩阵或全工作台交付。上述事项仍按主控各自的真实证据审定。未在主仓改文件/运行测试，未 push 或合入主仓，未修改共享环境或停止其他任务。
