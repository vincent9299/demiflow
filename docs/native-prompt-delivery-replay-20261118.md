# Native Prompt 交付回放修复

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

修改限于 `operator_llm/runtime.py`、`operator_llm/stream_recovery.py`：
`NativePromptRecovery.delivery_trace` 在原生 client 绑定成功后启用；
`PromptActor` 在成功和行错误交付时使用它。实际观测、错误分类、重试判断均在投影之前。
没有修改 writer、批次成员／payload 校验、提交协议、预算或历史身份检查，没有添加第二份持久账本。
复制量只随已经受请求／响应限额约束的调用 trace 与尝试列表增长，不复制完整响应树或 Dataset。

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

合计平台集合 **92 passed / 19 基线已有失败 / 3 skipped**，不是全仓全绿认证。模型 HTTP／CLI 用本地替身，Dataset、worker、请求日志、Lance 和提交恢复使用实际实现；不代表真实模型回答质量、生产跑数或 GPU 服务验证。

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
```

不得复用正在运行或留作证据的 `--basetemp`；pytest 会清理它。
