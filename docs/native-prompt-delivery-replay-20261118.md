# Native Prompt 交付回放修复

状态：两处恢复缺陷已修复，并在合入 Claude／P4 的隔离组合中通过最终验收。
不表示平台全仓全绿或已部署到在线服务。

## 缺陷与修复契约

基线 `74d2f9941377a0a2ca82e492d7be3a6b02e3d014` 中，真实 RefGen 链路
`map_prompt_async → map → save_lance → map_prompt_async → write_lance`
在中间表已提交后硬退出，恢复时原生日志返回相同模型响应，但交付行中的
`call_output.reused`、`attempts[*].reused` 从 false 变成 true。
下游原生批次摘要因此不同，正确地报 `INCOMPATIBLE_HISTORY`。

本次只明确普通单次 Prompt 的两个已有输出契约：

- **交付给下游的调用事实**描述原始提供方调用；同一 action/node/row 的原生日志回放保留原始事实，包括原始 `reused=false`。只投影调用 trace 自己的这两个位置，任意响应内容和业务字段保持。
- **本 attempt 的监控**仍观察实际 trace，先记账、后生成交付值。真实复用为 true，新消耗 token 为 0；原生请求账本不会再次扣费。

这不是把真实监控的复用伪装成新请求。原生日志的键包含 action、节点和行 occurrence，命中的就是这次原始调用；显式私有缓存没有这种交付契约，因此保持原行为。下游应从持久调用事实理解原始执行，从监控／请求账本理解本 attempt 的消耗。

调用事实修复位于 `operator_llm/runtime.py`、`operator_llm/stream_recovery.py`：
`NativePromptRecovery.delivery_trace` 在原生 client 绑定成功后启用；
`PromptActor` 在成功和行错误交付时使用它。实际观测、错误分类、重试判断均在投影之前。
没有修改 writer、批次成员／payload 摘要算法、提交协议、预算或历史身份检查，没有添加第二份持久账本。
复制量只随已经受请求／响应限额约束的调用 trace 与尝试列表增长，不复制完整响应树或 Dataset。

## 组合复跑发现的第二个原因

第一次修复完整 F1 曾通过，但随后 `business-final.xml` 复跑为 9 passed / 1 failed。
继续读取失败的原生 membership 与实际回放帧，确认新的失败不是 trace 内容：
首跑先完成 synth、再完成 reuse；回放先完成 reuse。**同一 record ID 的整帧 SHA 完全相同，只是到达位置不同。**
这个失败被保留，没有以首次偶然通过结束验收。

`execution/dataflow_batch_membership.select_replay_batch` 现在从待处理记录中按原始 membership
选出原批次；每条完整 payload 摘要和出现次数继续逐一检查，缺失成员不提交。
`execution/kernels/operator.py` 在原 batch 字段内保留先到的后续记录，再按原顺序交给原 writer。
所有未交付记录始终留在原快照字段中，跨 yield／checkpoint 不藏在局部 iterator；恢复不创建另一份账本。

并发计算仍按原有空闲 slot 推进，不做全局排序，也不把模型并发降为 1。
重排同时受原 `max_batch_bytes` 与 `max_batch + max_pending_events` 条数上界约束；
超过窗口明确报 `BUDGET_EXCEEDED`，不无限缓存、不截断或跳过记录。
这一约束意味着超出已配置窗口的极端乱序不能宣称无条件恢复；需要能容纳实际乱序的原有资源预算。
相同待处理批次在 poll 时复用只含列表身份、长度和 ordinal 的扫描标记，不反复哈希大行。
标记不保存业务数据，恢复时清空。

## 隔离与边界

- 修复树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_prompt_replay_20261118`。
- 分支：`fix/prompt-replay-delivery-20261118`，基于上述产品基线。
- 显式私有 journal、offline 输入和多轮 Agent 保持原契约；本次不声称解决它们的所有恢复问题。
- 源码身份保护仍生效。旧平台版本创建的 RunRef 不在新版本上续跑；验收的首次运行和恢复使用同一修复版本。
- 不改主仓，不覆盖另一会话正在集成的 P4／Claude 分支，不更新在线服务源码或重启 59866。

## 已完成验证

证据目录：`/tmp/demiflow-seven-flows-20261118`。

1. `tests/test_native_prompt_delivery_replay.py`：HTTP／Codex × 正常／schema 行错误，**4 项通过**。真实子进程硬退出于已确认的 `save_lance` 提交，再以原 RunRef 恢复；原中间版本仍为 1，最终行与原提交逐字值相同，外部请求恰好两次，恢复 attempt=2；恢复监控分别观察一次复用和一次新调用，复用没有新 token 消耗。
2. 原生 journal、配额、offline、私有缓存、模型监控等集合：**63 passed / 18 failed / 3 skipped**，见 `platform-replay-2.xml`。其中包含上述 4 项；3 个 Ray 用例因本解释器未安装 Ray 跳过，不计通过。
3. 原生写表、提交故障点、未知结果保护、模型 barrier 与 native execution：**29 passed / 1 failed**，见 `sink-regression.xml`。
4. 上述 **19 个失败全部在未修改的 74d2f99 基线复现**，失败 nodeid 一致；仅归一化工作树绝对路径后，异常消息全部一致。证据 `baseline.xml`、`baseline-comparison.json`。18 项旧测试传入动态 `request_gate`，被既有 lowering 一致性检查拒绝；另一项旧测试在 `map` 返回非 mapping 时被拒绝。本次没有改这些测试、准入或运行接口，也没有把失败列为通过。
5. 完整 RefGen 联合交付：原产品基线 `recovery-final.xml` 为红；修复平台 `recovery-fixed.xml` **1 passed**。4 组唯一交付，原中间提交保留，作者／图片／定稿分别只调用 2 次。七流程最终组合证据在业务树 `docs/seven-flows-20261118/VERIFICATION.md`。

上述为第一层修复的集合：**92 passed / 19 基线已有失败 / 3 skipped**，不是全仓全绿认证。
第二层顺序修复先通过 11 项（原生批次顺序、修改／缺失反例及原有定时批次恢复）与一次完整 F1 恢复；
追加有界窗口及保留慢调用旁路推进的最终组合，结果见同目录 `order-final.xml`、`business-closed.xml`。
模型 HTTP／CLI 用本地替身，Dataset、worker、请求日志、Lance 和提交恢复使用实际实现；不代表真实模型回答质量、生产跑数或 GPU 服务验证。

组合测试首次把多个硬退出场景共用一个容量目录，留下 61 个 BOUND 分配、累计 64000 cpu_millis，
使后续 5 个用例在业务启动前报 CAPACITY_PENDING。该轮已中断并保留 `order-final.xml/log`；
不能把这份中断记录当成绿色验收。单独定时恢复复验 `timer-check.xml` 通过。
新测试现按场景独立容量目录，**同一场景的 crash/resume 两次进程仍共用该目录**，
未清除分配或放松容量守卫。后续完整组合使用新的私有测试目录。

## 最终合流验收

修复提交 `2abc147`（调用事实）和 `690a7f8`（批次顺序）在本修复分支合入
产品 `07cf9348251e73c738fadf5bc1df5f93e521100a`，形成测试提交
`c6fc4469b7f26f1a1b24c6ca102608f21e684066`。没有更新共享产品分支或在线源码。

| 合流后验证 | 结果 | 证据 |
|---|---|---|
| 并发乱序、修改／缺失／预算反例、HTTP／Codex 行错误、原定时批次、慢请求旁路和 pending snapshot | 23 passed，207.49秒 | `merged-core.xml/log` |
| 原生 Lance 写出／故障点、native execution、工作台边界、统一 kind 契约 | 41 passed，211.86秒 | `merged-sink.xml/log` |
| 七条 RefGen 正常路径与完整 F1 硬退出恢复 | 10 passed，208.41秒 | `merged-business.xml/log` |
| 七个默认业务入口真实编译 | 节点16/24/15/19/10/29/11，全部成功 | `compiled-merged.json`、`compile-merged.log` |

最终平台相关 **64 项通过**，业务 **10 项通过**，这两组不与前述历史通过数叠加冒充不同用例。
确定性的乱序恢复新用例在未修改的产品基线上失败，见 `order-red.xml`；只替换测试中的平台根路径，未改断言。
验收输入、报告和截图摘要保存在同证据目录的 `verification.json`；它核对全部平台源码与测试提交的字节一致性。
59866 仍运行固定 `74d2f99`，展示七条业务流程；本恢复补丁与新架构组合尚未切换该服务。

## 复跑方式

在本修复树运行 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest`，显式设置
`PYTHONPATH=<本修复树>`、`PYTHONDONTWRITEBYTECODE=1`、`OPENBLAS_NUM_THREADS=1`、
`OMP_NUM_THREADS=1`、`MKL_NUM_THREADS=1`；`DEMIFLOW_RESOURCE_HOME`、
`DEMIFLOW_RESOURCE_ROOT` 和 `--basetemp` 指向新建的 `/tmp/demiflow-*` 私有目录。
新测试的子进程还断言实际导入的源码根目录，避免 editable 安装静默指向主仓。

```bash
python -B -m pytest tests/test_native_prompt_delivery_replay.py \
  tests/test_runtime_v3_codex_prompt_journal.py \
  tests/test_runtime_v3_model_quota_recovery.py tests/test_prompt_http_retry.py \
  tests/test_prompt_offline.py tests/test_runtime_v3_model_observations.py \
  tests/test_runtime_v3_private_prompt_journal.py -q -p no:cacheprovider

python -B -m pytest tests/test_runtime_v3_stream_lance.py \
  tests/test_native_prompt_barrier.py tests/test_runtime_v3_native_execution.py \
  -q -p no:cacheprovider

python -B -m pytest tests/test_native_batch_replay_order.py \
  tests/test_native_prompt_delivery_replay.py tests/test_runtime_v3_batch_timing.py \
  tests/test_runtime_v3_stream_lance.py \
  tests/test_native_barrier.py::test_native_inflight_cut_allows_free_slots_past_slow_call \
  tests/test_runtime_v3_inflight_calls.py -q -p no:cacheprovider
```

不得复用正在运行或留作证据的 `--basetemp`；pytest 会清理它。
