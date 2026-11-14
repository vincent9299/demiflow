# DemiFlow v2 升级 · 12 路实现交付总览（供 GPT 完整 Review）

- 生成：2026-11-14 07:20Z，协调会话 `61d0e9a3-8c47-4a54-9885-3e022e95a3f7`
- 共同运行基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`；设计依据 `13d19b5`；派工提交 `afbe9f3`
- 设计工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_contracts_20261113`，目录 `docs/upgrade-contracts-v2-20261113/`
- 回执目录：`/tmp/demiflow-upgrade-delivery-20261114/`（扁平，`<TASK_ID>.json` + `<LANE>.json`，schema `demiflow.task_delivery.v1`）
- 机器可读清单：**`lanes.json`**（同目录，含 40 位完整 HEAD、逐卡状态、门、独占文件）
- 集成台账：`/tmp/demiflow-dispatch-20261114/integration-ledger.json`（13 项 IL001–IL013）
- 边界守卫：`/tmp/demiflow-dispatch-20261114/scope_check.py`；进度表：`status.py`；心跳：`escort.sh`

> **本轮授权范围**：12 路并行独立实现，中间不安排外部 Review。全部交付后由 GPT 与协调者集中 Review，问题由 GPT 统一修改、集成、复验。
> **交代码 + 自测完成 ≠ 整体验收通过。** 旧 `dispatch_ready=false` 不阻止本轮实现，但不得伪改成"已验收"。

---

## 1. 当前进度（07:20Z 实测）

**164/198 卡 tested；10/12 路已完整交付。** 剩 S（16 卡 implementing）与 V（16 claimed + 2 needs_decision）。

| 路 | 会话 ID | 工作树（`_demiflow/worktrees/` 下） | 分支 | HEAD | 脏 | 卡 | 状态 |
|---|---|---|---|---|---|---|---|
| F | `ccae69dc-99d1-5587-ab77-9c4913f0c707` | `v2_f_20261114` | `engine/v2-f-20261114` | `07d32bbb` | 0 | 5 | 5 tested ✅ |
| A | `744a2500-cd27-584c-a394-84ed1f4b97e0` | `b_option_20261113` | `engine/option-admission-20261113` | `4122d64a` | 0 | 29+1 | 30 tested ✅ |
| S | `d3457822-fbcf-5fff-a4c2-3aee47ee7658` | `e_dfamily_20261113` | `engine/d-family-20261113` | `4489ba0d` | 26 | 16 | implementing ⏳ |
| R | `ba07308b-23ab-51df-a473-bb329956197d` | `n2_retired_20261113` | `engine/checkpoint-retirement-20261113` | `afe72954` | 5 | 32 | 32 tested ✅ |
| Q | `32b16a42-08ef-55cb-a798-c430ed336d5b` | `n3_resource_20261113` | `engine/resource-adapter-20261113` | `f921605e` | 0 | 8 | 8 tested ✅ |
| M | `8636f749-8a9b-5213-a725-c581296c47dc` | `n1_prompt_20261113` | `engine/prompt-admission-20261113` | `e50d75d8` | 0 | 15 | 15 tested ✅ |
| L-contract | `f77b8c0d-e08f-574f-a90a-4564ff8ad60d` | `v2_l_contract_20261114` | `engine/v2-l-contract-20261114` | `09ec0bf1` | 0 | 11 | 11 tested ✅ |
| L-chain | `dcafbeb1-7aa9-5d77-a953-ca05dd1d9159` | `v2_l_chain_20261114` | `engine/v2-l-chain-20261114` | `9122aaee` | 0 | 12 | 12 tested ✅ |
| L-workers | `fb99a969-6bb1-5c32-a590-d44d1d284b92` | `v2_l_workers_20261114` | `engine/v2-l-workers-20261114` | `40c8b5c0` | 0 | 15 | 15 tested ✅ |
| L-native | `80c096b9-17b7-5a67-a940-b2874296af55` | `v2_l_native_20261114` | `engine/v2-l-native-20261114` | `14a90e15` | 0 | 7+6 | 13 tested ✅ |
| W | `d5a2d23b-5d85-5382-acc1-c99b4dff2fa6` | `d_workbench_20261113` | `engine/workbench-test-debt-20261113` | `362c482b` | 0 | 27 | 27 tested ✅ |
| V | `3c427b08-70bc-5164-ae0a-a7caca1d14a5` | `v2_v_20261114` | `engine/v2-v-20261114` | `bbd93ffc` | 1 | 21 | 3 tested / 16 claimed / 2 needs_decision ⏳ |

所有会话 `authorityEndpointId = 4031365b-98ba-4633-bd39-ba0dd8e470b5`（host `devbox-codex`）。
**已停止的旧总控 `57bfe8f5-c6c0-40fc-843b-12a27629ba7e` 不得发送任何回报。**

会话复用（未新建重复实现）：A←B-option、S←E/D-family、R←N2、Q←N3、M←N1（N1 已授权在途成果归 M 接收）、W←D-workbench。L 拆 4 路，F/V 新建。

## 2. 边界完整性（协调者独立复核，非采信自述）

- **`VIOLATIONS 0`**：12 个工作树全部通过 `scope_check.py`。55 个独占文件零冲突，同一文件仅一个装配负责人。
- **7 个字节冻结文件全部逐字节相符**（依据 `review-20261114/frozen-files.json`；L-native 另经独立复核）。
- **符号范围保护有效**：`recovery_identity.py`→`describe`；`contracts/operator.py`→`_adapter_present`,`compile_operator_spec`；`stream.py`→`_Stage.__slots__`,`_Stage.__init__`,`_materialize`,`_arun`,`_make_worker`,`run_stream`。
- **主仓 `/yzp/zhaozy/yangzepeng/0905/demiflow` 代码级脏文件 = 0**（仅存量文档；主仓 HEAD `e40a6dd`）。未修改、未在主仓跑测试。
- 未 push、未合主仓、未重启服务、未用付费模型、未用 SSH copy。测试产物只落 `/tmp/demiflow-*`。
- 脏文件说明：R 的 5 个与 M 的 2 个是**派发前既有**的测试改动，已按"保留既有脏改动"原样保留；S 的 26 个是在途实现。

## 3. 必须先读：IL012 验证环境陷阱（高优先）

`env/lib/python3.11/site-packages/__editable__.demiflow-0.3.0.pth` 把 `demiflow` 映射到**主仓** `/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow`。已实测：

```
cd /tmp && env/bin/python -c "import demiflow; print(demiflow.__file__)"
→ /yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/__init__.py     # 主仓，不是工作树
```

**12 个工作树均无根 `conftest.py` / `pytest.ini`**（逐个查过）。因此只有两种方式真正导入本树补丁：

1. `PYTHONPATH=<worktree>` 显式导出；或
2. 在工作树内用 `python -m pytest`（`-m` 把 cwd 放进 `sys.path[0]`）。

裸 `pytest tests/...`（console script）、`python -B /tmp/probe.py`、或在别的 cwd 下运行，**会静默测主仓**，可能产生假绿或假红。

回执命令字段审计结果：A/F/L-contract/L-chain/L-workers/M/R/V/W 共 187 条检查显式带 `PYTHONPATH=$PWD`；Q 用 `python -m pytest`（cwd 在工作树即安全）；L-workers 探针在脚本内 `assert pathlib.Path(demiflow.__file__).is_relative_to(WT)`（最强证明）；**L-native 与部分 F/W 条目只记了 `pytest tests/...` 缩写，无 cwd/env 字段，无法从回执确认 pinning**。

→ **集成时每条头部检查都要带进程内断言 `demiflow.__file__` 在目标树内重跑。** 这是验证环境修正，不否定任何补丁本身。IL013 的错误消息分歧很可能正是此陷阱的产物。

## 4. 集成台账 13 项（详见 `integration-ledger.json`）

| ID | 状态 | 内容摘要 |
|---|---|---|
| **IL001** | open | Q007：`tests/test_u4_reservation.py::test_derive_prefetch_max_pending_protocol_floor` L267-269 断言 floor 把 ceiling 4→64 扩张。集成时须改为 fail-closed（权威 RESPONSE.md #6/Q43；CRITICAL-REVISION.md:130/136）。 |
| IL002 | resolved_in_lane | Q008→L-workers `graph.py` 迁移已完成，commit `a3683a8`；`graph.py:549-550`,`:1001-1002` 已用 `resolve_prefetch_credit`。 |
| IL003 | ruled_and_relayed | L016 `verify_preserve`→`implement`（`run_ray_chain` finally 关闭），已执行 `5e8525c`。 |
| IL004 | verified_compatible_pending_merge | A016↔L045 签名已兼容：`executors/local.py:562` `_apply_plan(..., execution=None)` vs `dataset_native.py:505` `execution=self.execution`。 |
| **IL005** | open | 3 个 `test_dataset_native.py` 失败在**纯净 bbd93ff 即存在**：`test_unsupported_typed_key_preserves_original_codec_error`（期望 'JSON serializable'，实得 'Unsupported runtime value type'）、`test_empty_inputs_and_opaque_python_types_keep_original_semantics`、`test_partition_callbacks_preserve_task_state_and_failure_before_commit`。证据 `/tmp/demiflow-baseline-probe`、`/tmp/demiflow-l-native-tests/baseline_preexisting.log`。**非本轮引入。** |
| IL006 | ruled | A005 涉及的 32 条 B/F 历史断言判为"设计取代"，V 重写、GPT 重分类。`reject_streaming_map_options` 已收窄，8 个调用点逐个核对（local×5 / Ray×2 / local relational×1）。 |
| IL007 | relayed_to_V | 关系型计划带 streaming options 时，拒绝必须**先于任何 source 读取/事件**；`take(0)` 不得读源。 |
| IL008 | open_low_priority | `'Ray'` 字面量 vs `RayDatasetExecutor.NAME='ray'`（`ray.py:230`；local `NAME="local"` `local.py:112`；base `NAME=""`）。低影响，可后延。 |
| **IL009** | **escalated_awaiting_ruling** | `prompt_facts` 拒绝 `request_policy`，与 C07:160 冲突，且 `M.md:11` 明令"prompt_facts守卫不动"。**无派工归属。单独一页裁决材料：`IL009-RULING.md`。** |
| IL010 | resolved_in_lane | V019→A-U01：`Dataset.sort` 标量 bool 归一化，commit `4122d64a`，17 passed + 155 passed 回归。残留裁决拆为 IL013。 |
| IL011 | verified_legitimate_documentation_requested | S 删除既有 `tests/test_d2_recovery_identity_counterexamples.py` 经核查是**迁移**到 `tests/test_upgrade_v2_unit_S013.py`（14 测试覆盖原 7 条，其中 1 条刻意加强为 fail-closed），非凑绿。已要求 S 补旧→新映射文档。 |
| **IL012** | **open_high_priority** | 上述 editable-install 环境陷阱。 |
| **IL013** | **ruled_keep_admission_valueerror** | **已裁决：保留 admission 的 `ValueError('Operation has no pull backend capability contract')`，不改准入。** 依据：① `tasks/A007.md:44` 明确把 `ValueError` 列为合法错误类型（与 `UnsupportedExecutionOptionError`、`RecoveryContractError` 并列），故准入抛 ValueError **在卡契约内**；② `CONTRACTS.md:73` 只对「本地收到 Ray-only **native_options**」强制 `UnsupportedExecutionOptionError`，SortOp 不是 native_options，且 `admit_action_plan` 的 native_options 分支已正确抛该错误；③ **C03 明令「复用 C02、现有 check_action/check_call，不另设 capability 表」**，为 Ray-only 算子特判消息等于新增第二套判定表，违反 C03；④ 把 SortOp 加进 pull 白名单会让它**在 Local 真执行**，违反 fail-closed 与「不自动切 target」，禁止；⑤ `_require_ray_backend` **不是死代码**，`local.py:741`（`_apply_plan` 内）与 `dataset_native.py:569` 仍在调用，是准入之后的第二道守卫。**协调者已 pin 到 A 树实测**：scalar default/False/True、sequence `(False,)`、materialize 五种形态错误完全一致，Local 不执行 sort、不切 target。**我原先点名的 Ray 专用消息来自主仓（IL012 陷阱），是我的验收标准写错了，不是 A 的实现缺陷。** A-U01 测试钉本树真实 ValueError 是正确的，无需改。V019 应钉语义 + 该 ValueError，不得断言 Ray 专用消息。文案可改善（改 `check_call` 的 reason 集，属 A 独占）但属低影响改进，按精力分级**后延、不作为交付前置**。 |

### 另一处无归属缺陷：M-KI-02（归属需更正）

`plan_facts.py:123` / `data/runtime_contract.py`：`data.from_items(...).map_embeddings(...).materialize()` → `RuntimeContractError: Unknown actual operation contract: EmbeddingMapOp`。在纯净 bbd93ff 复现（`tests/test_embeddings.py` 28 failed / 33 passed，28 条同一错误）。

- M 回执把它归给"C 组"——**归属错误**：C 已拆成 4 路，`plan_facts.py` 的负责人是 **F**。
- **F 已实现修复**：`v2_f_20261114/demiflow/execution/plan_facts.py:284` 对 `ops.EmbeddingMapOp` 返回 `{'contract':'demiflow.embeddings','arguments':{model_contract,inputs,output}}`；基线该文件 `EmbeddingMapOp` 出现 0 次。
- `demiflow/data/runtime_contract.py` **仍无归属**。
- → **必须在集成树端到端复验，不得直接宣称已解决。**

## 5. 各路门与独占文件（Review 入口）

完整逐卡状态、门列表、独占文件见 `lanes.json`。要点：

- **F**（5 卡，门 EMBED/FACTS/GRAPH/HISTORY/LAYOUT/PREPEND/RELATIONAL/VECTOR）：`plan_facts.py`、`planning/dataset_dag.py`、`planning/graph_dag.py`
- **A**（29+1，门 ADMISSION/DELIVERY/FACTS/HISTORY/LAYOUT/QUEUE/RECOVERY/RELATIONAL/STREAM/WRITE）：`data/dataset.py`、`data/plan.py`、`execution/capabilities.py`、`execution/executors/local.py`、`execution/stream_delivery.py`
- **S**（16，门 ADMISSION/BOUNDS/DELIVERY/HISTORY/IDENTITY/LAYOUT/RECOVERY/STATE/VECTOR）：`contracts/operator.py`、`operator_policy.py`、`operator_precheck.py`、`operator_state.py`、`recovery_identity.py`、`stream_map.py`、`lance/search.py`、`planning/traits.py`
- **R**（32，门 BOUNDS/BUFFER/EMBED/GRAPH/HISTORY/LAYOUT/RECOVERY/STATE/UNKNOWN/VECTOR/WRITE）：`run_context.py`、`stage_buffers.py`、`stream.py`、`stream_barrier.py`、`stream_materialize.py`、`worker/graph_progress.py`、`worker/progress.py`。**buffer 协议与实现一起闭合**，保留 fail-closed、逐 kind 凭证据放开。
- **Q**（8，门 ADMISSION/BOUNDS/CODEC/GRAPH/RECOVERY/STREAM）：`block_sink.py`、`payload_limits.py`、`reservation.py`、`stream_resources.py`
- **M**（15，门 BOUNDS/BUFFER/EMBED/HISTORY/IDENTITY/PROMPT/UNKNOWN）：`embeddings/runtime.py`、`embeddings/stream_recovery.py`、`operator_llm/http_options.py`、`operator_llm/stream_recovery.py`
- **L-contract**（11）：`native_plan.py`、`plan_interpreter.py`、`sealed_plans.py`
- **L-chain**（12）：`chain.py` **仅此一个文件**
- **L-workers**（15）：`graph.py`、`queue_chain.py`、`worker/execution.py`、`worker/graph_execution.py`、`worker/layout.py`、`worker/queue_transform.py`
- **L-native**（7+6，门 BOUNDS/LAYOUT/RELATIONAL/WRITE）：`arrow_batch_sink.py`、`dataset_native.py`、`native_resources.py`、`local_tasks.py`。**实际资源额度与临时写出一起闭合**，沿用既有资源权威，未新建第二 allocator。6 个补充单元：`TaskScheduler.__init__/__enter__/run/__exit__`、`rechunk`、`apply_batch`。
- **W**（27，门 DEMO/HISTORY/WORKBENCH/WRITE）：`workbench/api.py`、`graph/model.py`、`graph/source_view.py`、`runs/facts.py`、`runs/manager.py`、`runs/worker.py`、`web/workbench-graph.js`、`web/workbench-state.js`、`web/workbench.js`
- **V**（21，20 个门）：**独占 `tests/test_upgrade_v2_acceptance.py`**，其他组不得改。

新测试命名：`tests/test_upgrade_v2_unit_<TASK_ID>.py`。`contract_closure_required: true` 仅 R 与 L-native。

## 6. 已验证 / 未验证（不要混淆）

**已验证（协调者第一手）**
- 12 路文件归属互斥、0 越界；7 冻结文件字节相符；符号范围未越界；主仓代码 0 脏。
- 10 路 164 卡回执 state=tested 且带真实测试证据（命令 + 日志路径 + 结果）。
- IL002/IL004/IL010/IL011 的代码级事实已逐个读源码核对。
- IL005 的 3 个失败、M-KI-02 的 28 个失败在纯净基线复现。
- IL012 的 editable-install 映射实测确认。

**未验证（必须集成后做，不得当作已通过）**
- **任何跨组组合行为**。12 路各自独立工作树，未合流；单组绿不代表组合绿。
- V 的 21 项验收：仅 3 项 tested，16 claimed，2 needs_decision。**整体验收未通过。**
- M-KI-02 修复的端到端效果（F 已改 `plan_facts.py`，但未在含 `runtime_contract.py` 的集成树验证）。
- IL001 的 fail-closed 改写、IL007 的拒绝时序、IL009 的裁决后行为。IL013 已裁决（保留 admission ValueError），V019 断言待按裁决更新。
- Ray 相关组合：`env-ray`（`/yzp/zhaozy/yangzepeng/0905/env-ray/bin/python`）有 **ray 2.60.0**，默认 env 没有。L-chain 曾误称"环境无 Ray"，已纠正。重测试单路、轻测试最多两路，避免争抢在运训练资源。

## 7. 建议 Review 顺序

1. **先读 §3 / IL012**，确立"每条检查都导入正确树"的验证前提，否则后续结论都不可靠。
2. **裁决 IL009**（`IL009-RULING.md`，一页）——它仍卡住 V 的验收断言。**IL013 已由协调者裁决完毕**（保留 admission ValueError，依据 A007.md:44 / CONTRACTS.md:73 / C03），GPT 只需复核该裁决并让 V019 按此断言。
3. 更正 M-KI-02 归属（C→F），确认 `runtime_contract.py` 的装配负责人。
4. 按 `lanes.json` 逐路读 diff（`git diff bbd93ff..<HEAD>`），优先 R / L-native（`contract_closure_required`）与 A（30 卡、面最广）。
5. 处理 IL001（fail-closed 改写）、IL005（基线既有失败的重分类）、IL006（32 条历史断言重分类）、IL008（低优先，可后延）。
6. 等 S、V 交付后合流，跑 V 的 21 项验收 + 各组头部检查（带树内断言），再判定整体验收。

## 8. 硬约束（Review 与修改期间继续有效）

- 禁止修改或运行测试的主仓：`/yzp/zhaozy/yangzepeng/0905/demiflow`。
- 不 push、不合主仓、不重启现有服务；不用付费模型（必要时只替换外部 transport）；不用 SSH copy。
- 禁止直接修改其他组的工作树；同一文件仅一个装配负责人（以 `batches.json` 为准，**不得改 GPT 的 `batches.json`**）。
- 承重冻结文件与符号范围保护继续有效。
- 测试产物只写 `/tmp/demiflow-*`；轻测试≤2 路、重测试单路；只跑与改动相关的必要测试。
- 生产代码不得放 stub、复制实现或静默 fallback 凑通过；不得改业务并发为 1、删守卫或放宽断言换绿。
- 保留既有脏改动与 Notebook 输出；不重写已有 Notebook/Case/开发历史等独立产品生命周期。
- 工作台显示真实封存计划与实际观测，不从静态图重算第二份执行账本；worker 贡献结果与状态，持久提交仍由驱动负责；queue 纯转换 worker 没有 progress 不等于缺陷；保留 DataFusion/Arrow 等计算内核。

**两大原始意图**
- 产品：统一、真正可用的工作台，让我看懂、配置、运行 pipeline，并分析、比较、评测数据。
- 平台：正确、稳定、高性能、通用、架构干净的统一计算平台，完整支撑实际业务；同一封存计划真正约束执行，资源与提交权威唯一，历史绑定当时代码，恢复正确。

---

## 附：文件索引

| 路径 | 内容 |
|---|---|
| `/tmp/demiflow-gpt-handoff-20261114/HANDOFF.md` | 本文件 |
| `/tmp/demiflow-gpt-handoff-20261114/IL009-RULING.md` | IL009 一页最小裁决材料 |
| `/tmp/demiflow-gpt-handoff-20261114/lanes.json` | 12 路权威机器可读清单 |
| `/tmp/demiflow-dispatch-20261114/integration-ledger.json` | 13 项集成台账 |
| `/tmp/demiflow-dispatch-20261114/manifest.json` | 派发绑定（会话/工作树/卡/独占文件/门） |
| `/tmp/demiflow-dispatch-20261114/scope_check.py` | 边界守卫（可重复运行） |
| `/tmp/demiflow-dispatch-20261114/status.py` | 逐卡进度表 |
| `/tmp/demiflow-upgrade-delivery-20261114/` | 217 份回执原件 |