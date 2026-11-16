# Managed image cancellation and original recovery — 2026-11-17

本轮修复把真实工作台 130 行图像 pipeline 的 prepared / ACK 后取消、原 RunRef 恢复、对象和 Lance 提交接通。最终同一候选的 40 项验收全部通过；以下保留历史红证据、版本边界与尚未由本 lane 验证的范围，不声称整个项目 100%。

- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_image_cancel_20261117`
- 分支：`engine/gpt-image-cancel-20261117`
- 基线：`63fe9084a83eb91bddefce3340425501ee4738d8`；原测试迁移 `4149efb` / `9840bbd` 的本树 cherry-pick 为 `f113076` / `b027608`。
- 最终代码及测试候选：`3e8b058d797435c0f00f355a76d138447dbc9d23`。本说明与 JSON 为其后的文档提交。
- 机器证据：同目录 `IMAGE-CANCEL-RECOVERY-20261117.json`，另存 `/tmp/demiflow-image-cancel-final/verification.json`。

## 原缺陷与修复边界

原案例在 source frontier 128 的真实非最终 prepared cut 暂停并取消。原实现可能在原 10 秒退出期限后被强制终止，留下已准入但没有完整响应的付费请求；同 RunRef 恢复因此正确报 `automatic regeneration is forbidden`。本轮没有删除该 UNKNOWN 保护，也没有加大任何预算、取消宽限期或请求期限。

1. **合作取消的来源。** `RuntimeActionHandle` 只有读取到原 RunJournal 的 `stopping/cancelled` 事实，才选择固定内部 run-stop 原因。它与原 operator stop-file 原因共用唯一 allowlist/helper。Driver 停止新的模型请求准入；已准入效应允许沿原 journal 排空。显式早关、超时、worker 丢失或任意错误不能获得这个权限。
2. **生命周期传播。** Worker/AsyncInstance 保留最早取消原因，后续通用 Drain 不能覆盖；原 mailbox 在允许的合作停止中保持可达。RoutedPromptActor 识别同一个固定全局停止语义，不再将 run cancellation 转成 provider route 的业务错误行；独立 provider 故障隔离继续保留。
3. **已完成异步回复仍须投递。** 原 Local process、Ray 及独立 thread host 的 K3 清理循环只处理新 event，漏掉原异步 service 已完成队列。三个后端现在在原等待循环、原 deadline 内，按 allocation + boot 取出并投递本 worker 的回复。正常 poll 与定向 poll 共用同锁、同 `_completed/_pending_payloads`；其他 worker 的顺序和通知字节不丢失。没有新队列、服务线程、reader、请求重发或持久权威。
4. **新准入拒绝也必须给出原错误回复。** K3 early RPC 直接调用 `dispatch` 时，`ServiceStopped` 会逃出控制循环，使原 mailbox waiter 永远等不到回复；正常异步 `_serve` 却会封装错误。现将原 `_serve` 的 `failure_payload/ControlReply` 封装抽为 `dispatch_reply`，两条路径复用。直接 `dispatch` 的原语义、lease/boot/role 检查以及已知错误白名单不变。真实 service + mailbox 的两条反例先稳定失败，再转绿。
5. **恢复后的检查点计算。** Worker 恢复了原累计行数，Driver 原先仍以零为基线，导致第二个非最终 130 cut 后才最终 130。现从同一认证过的 worker-frontier artifact 读取原 metrics，按 participant/stage/partition 校验归属和非负整数；同一 colocated frontier 只读取、计数一次。恢复 `_node_metrics` 并初始化检查点基线。Edge ACK 两侧均按 attempt 重新计量，保持零，不伪造历史 ACK。最终 ACK 的只读结果重放提前返回，不额外依赖 worker metrics。

## 最终同候选验收

| 回执 | 结果 | 关键内容 |
|---|---:|---|
| `/tmp/demiflow-image-cancel-r18-controls/receipt.json` | 37 passed / 111 phases | 两种固定停止、ordinary close/timeout、不重复计费、原 native/private journal、真实 process/thread 模型响应、routed HTTP 隔离、迟到回复/另一 worker/错误 boot、原共享 reader、真实错误回复及 frontier ownership/旧格式边界 |
| `/tmp/demiflow-image-cancel-r19-prefix/receipt.json` | 2 passed / 6 phases | 原 prepared / ACK 后 130 行完整工作台链路，所有原严格断言保留 |
| `/tmp/demiflow-image-cancel-r21-ray/receipt.json` | 1 passed / 3 phases | 真 Ray Core、原 routed native ACK 中断恢复、原请求数与收费、原已提交前缀不变 |

合计 **40 passed / 120 原 setup-call-teardown 阶段**，均为 `3e8b058`，每份 2,975 个输入摘要逐一与该提交核对，0 不匹配；source before/after 一致，无遗留 children。R21 使用总控原先已使用的 `/tmp/demiflow-ray-api-optional-env/bin/python`；fixture 自建、关闭私有 3 CPU Ray 集群，没有连接共享集群。

两条工作台案例均证明：

- 原 RunRef 首 attempt 正常取消退出（returncode 1），第二 attempt 成功（returncode 0）；本轮没有触发 SIGKILL。
- 每条恰好 130 个唯一 HTTP 请求、130 个完整 native response、0 个错误记录、130 条准确结果；没有重新请求已付费答案。
- 原图像对象内容与 pixels 一致，Lance 目标仍是 version 1；外部历史文件、原 attempt 查询及原 recovery limits 不变。
- 原 materialize cuts 严格为 source `128 → 130`、final `False → True`，二者都 ACK；对应 terminal rows `126 → 130`。保留 native in-flight 快照语义，没有把快照降成排空或改变原两-cut 断言。
- 原请求、对象、行和输出合计预算断言全部通过。完整原 facts 也包含于证据 JSON。

复跑使用私有四域 + TMPDIR、工作树 PYTHONPATH、进程内 import-root 断言和原完整阶段守卫。命令模板：

```sh
<python> -P -B /tmp/demiflow-image-cancel-runner-20261117/run.py \
  --execute --group <final_controls|two_prefix|ray_recovery> \
  --candidate-head 3e8b058d797435c0f00f355a76d138447dbc9d23 \
  --output /tmp/demiflow-<new-private-evidence-directory>
```

前两组 `<python>` 为 `/yzp/zhaozy/yangzepeng/0905/env/bin/python`，Ray 为上述私有可选环境。各回执另有 exact command、解释器、runner/manifest 摘要及绑定副本。

## 红绿取证与实际边界

- R1/R3/R7：保留原 paid response 未完成、UNKNOWN 拒绝、原 10 秒退出期限的红证据。
- R9/R10：付费响应与业务结果已接通，但 `[128,130,130]` 与原 `[128,130]` 不符；未改断言。R12 的 ACK 后窗口在原 metrics 修复后严格通过，但 prepared 仍出现间歇未完成响应，未据其他绿项宣布闭环。
- R17：原 service + mailbox 的 operator-stop / run-stop 两条确定失败，均因 K3 early dispatch 直接抛出 `ServiceStopped`、未给原 waiter 回复；最终 R18 包含其转绿。
- **另五条原图像保护窗口**在 `a959cc9` 的 R10 已通过：真正 provider-before-artifact UNKNOWN、committed-object checksum 损坏、final prepared、final ACK、final ACK 对象损坏。它们未在最终候选重跑，不计入上述 40 项；后续整合验收应保留这一版本边界。
- R13 为诊断 watchdog 引发的启动期 SIGSEGV，未到业务 fault；R14/R15 仅诊断成功，不计入最终 40。R16 外部 py-spy 被宿主 ptrace 权限拒绝，未取得失败线程栈，不据它声称定位成功。确定接缝来自源码与 R17 原生反例。
- R20 在普通 env 缺 Ray 而 skip，守卫判非通过；随后 R21 在真实既有 Ray 环境通过。未把 skip 算 pass。
- 与 root 最新 strict recovery policy / 原 native-RPC fixture 修正 `7c034a8` 的组合由总控负责。本 lane 不声称多机/GPU/I08 性能/PostgreSQL或整个产品已全部验收。

## 文件与提交交接

生产范围为 11 文件：`dataflow.py`（恢复基线 + 原计数校验复用）、`dataflow_authority.py`、`dataflow_handles.py`、`dataflow_hosts.py`、`dataflow_operator_observations.py`、`dataflow_thread_host.py`、`kernels/async_instance.py`、`request_limits.py`、`worker/dataflow_execution.py`、`worker/dataflow_graph_runner.py`（仅 close 中 mailbox 判定）、`operator_llm/routed.py`。

测试范围为 `test_runtime_v3_checkpoint_metrics.py`、`test_runtime_v3_control_batching.py`（补既有 NS fixture 的 service 依赖）、`test_runtime_v3_control_drain.py`、`test_runtime_v3_cooperative_model_stop.py`、`test_runtime_v3_routed_prompt.py`。`test_managed_image_joint.py` 和其原 fault driver 没有在本轮修复中修改。

在 root 已有两份 image 测试迁移的前提下，按顺序 cherry-pick：

```text
21f6898 a1875f5 ee11b6f e02607d 6febfca a959cc9
b82dfe7 7db554e e288d6e 3e8b058
```

`ee11b6f` 为新控制反例的测试提交，虽然原提交标题写成行为描述；生产 queue 改动在 `e02607d`。`6febfca` 修正该测试对冻结 JSON payload 的构造。后续取本说明/JSON 的文档提交即可。

主仓未修改、未测试；原 main HEAD/status/18 dirty hashes 逐一相等。七个冻结文件在每轮 runner 对 `49b666f` 逐字校验。未 push、未合并主仓、未修改共享环境或运行服务。
