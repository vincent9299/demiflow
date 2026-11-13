# C 包设计稿：写出路径统一封存 seam（design-first，v4）

基线：65acab2，worktree c_runtime_20261113。
状态：路线 (b) 已批准并同轮 implement；本稿 v4 按实现期实证修订两处口径——
supported_slice 复用 chain_v1（§2）、streamed 层3 经队列工厂真实消费 stage_budget（§3/§7/§8，
新增 stream.py 改动）。反例与消费测试以 tests/test_c_write_seam.py 实际形态为准。

## 0. 根因、已有事实与 KeyedSource 前置核实

write_lance 三条实际执行分支（streamed / relational / fallback）都直接进
LanceSink → CommitCoordinator，从不编译/封存物理计划。journal 无
`<父 action_id>-sealed-plan` 槽，工作台展示图无权威执行计划可读。

已有（不重建）：node_observations 真实观测、runtime_commits acked、
manifest.digest 与展示图 binding.basis_digest 逐字相同、checked.report_digest
由 workbench/runs/worker.py:93 传入。本轮只补 F1 封存 + 执行绑定 + 身份桥。

**KeyedSource 前置核实（总控强制项，已完成，运行时探针实证）**：
真实工作台入口 flows/questions.py:select 产生 KeyedSource（join 折进 source）
+ BoundMapOp + FilterOp，has_stream=False、has_relational=True，落 relational 分支。
探针 /tmp/demiflow-c-write-probe/probe_keyed.py 复刻该形态，结果：
- `source_identity(KeyedSource)` 走 recovery_identity.py:141 的 `describe` 兜底；
  describe 显式处理 PlanInput（:80-82，executor 被排除）、frozen dataclass（:93-97）、
  reducer 可调用对象（:56-73/:104-106）。**支持，无契约缺口。**
- `unified_source_traits(KeyedSource)`：KeyedSource 继承 SourcePlan，走
  traits.py:10 isinstance 兜底，category=source。**支持。**
- `compile_logical_dag(terminal_kind="write_lance", sink_ids=("lance",))`：
  4 节点（KeyedSource/BoundMapOp/FilterOp/write_lance），dag_id 跨重编译稳定，
  source 身份对输入行敏感。`compile_physical_execution_plan`：4 stages/3 channels 通过。
- `freeze_source` 显式支持 KeyedSource（source_snapshot.py:13-18，递归冻结 left/right）。
结论：承重七件无需改动，无 B 侧契约疑问，按裁决同轮 implement。

## 1. 选择 (b)：保持原生执行，在其上编译+封存+强制绑定

理由：
- prepare_stream_blocks 的原生 stream driver + LanceSink 单一 publication
  已正确工作（总控探针证实 2 行结果、1 次 acked commit）。
- 搬进 managed worker 需重写 terminal 适配、恢复对账、资源释放，风险大。
- 原始意图要的是"封存计划就是实际执行的那份、工作台看到的就是它"，
  不要求换执行引擎。
- (b) 改动面小（4 文件 ~60 行），不触及提交语义。

## 2. seam 落点与 action id 对齐

write_lance（local.py:1171-1237）分支顺序：
```
:1185  source = freeze_source(action, source)        # 封存用这份冻结 source
:1207  if streamed: return write_lance(spec, None)   # 分支① streamed
:1208  pure Lance copy（无 operations）               # 延后
:1223  arrow_batch_sink eligible                      # 延后
:1229  if has_relational(source, plan): execute(...)  # 分支② relational
:1233  fallback rows→batches                          # 分支③ fallback
```

落点（两处，覆盖三分支）：
- 分支①：在 `:1207 if streamed:` 内、`return write_lance(spec, None)` 前封存。
- 分支②③：在 arrow_batch_sink 检查之后、`:1229 if has_relational` 之前封存。
  此处 streamed/pure-copy/arrow_batch_sink 均已 return，剩余路径恰为
  relational + fallback，一个封存点覆盖两者。延后分支不经过此点，不被封存。

**action id 对齐**：F1/执行绑定/身份桥全部封在**父 action_id** 下
（seal_plan_facts / seal_execution_bindings / _seal_chain_identity_bridge 都用
action.action_id）。工作台 workbench/runs/facts.py:105 读父 id 的
`-sealed-plan` 槽，对齐，不改 workbench/。streamed 的子 action '-stream'
只做内部 checkpoint 对账，不影响封存槽 key。

**复用而非另抄**：
- 编译/封存管线复用 chain.py 现有 helper：compile_logical_dag、
  compile_physical_execution_plan、seal_plan_facts、_seal_chain_bindings、
  _seal_chain_identity_bridge。K1 manifest 构建抽成 `_build_sealed_manifest(run, plan_ref)`
  纯 helper，compile_chain_topology 与新 seal_write_topology 共用（行为不变）。
- compile_chain_topology 增加 defaulted kwargs `terminal_kind="materialize"`、
  `sink_ids=("output",)`，现有调用者字节不变；write 传 write_lance/("lance",)。
- **supported_slice 复用 chain_v1（实现期核实，透明上报）**：B 的
  SUPPORTED_SLICES = {chain_v1, queue_transform_v1, static_fanout_v1}
  （plan_interpreter.py:32）是**绑定拓扑形状**标签，不是 terminal 种类。
  write 绑定是单 group、fixed source、单分区、前向 channel 的线性链，形状即
  chain_v1；write_lance 这个 terminal 种类由 logical_dag 承载并单独校验。
  故 seal_write_topology 传 supported_slice="chain_v1"，**不臆造 write_lance_v1**
  （臆造会被 B 的 _validate_bindings 以 unsupported_layout 拒绝）。B 契约字节不改。
- **布局抽取复用 worker/layout.py:load_worker_layout（只 import，不改 worker/）**：
  ```
  launch_spec = {"execution_plan": {"action_id": action.action_id,
                 "group_id": "chain-group", "bindings_ref": bindings_ref}}
  placement, checkpoint_policy, stage_limits = load_worker_layout(launch_spec, run, None)
  ```
  authority=run（有 .directory），assignment=None 跳过 validate_worker_assignment。
  group_id 与 _seal_chain_bindings 的 programs 一致（"chain-group"）。
  stage_limits 按 program_index 键控，值 {inflight_rows,inflight_bytes,output_bytes}。

seal_write_topology 返回一个封存句柄，封装：
- `.verify(plan)`：调用现有 verify_loaded_plan(plan, plan_ref, sealed_operators)。
- `.layout()`：上述 load_worker_layout 复用，返回 (checkpoint_policy, stage_limits)。
- topology / logical_dag / physical / bindings_ref。

## 3. 强制绑定：三层 + 逐分支消费诚实口径

绑定门 `.verify(plan)` 在内层 write_lance 闭包**发布前**调用（recover/commit 之前），
fail-closed：封存计划 ≠ 实际执行计划则拒绝，绝不发布。

**层 1 code-pin**：identity(plan_code_pin(plan)) != plan_ref["code_pin_digest"] → 拒绝。
**层 2 算子结构**：recomputed logical_node_id 序列 != sealed_operators → 拒绝。
层 1+2 由现有 verify_loaded_plan（chain.py:856-875）提供，三分支都强制。

**层 3 资源布局——只在该路径确有可消费旋钮处强制，逐分支诚实写明**：

| 分支 | 层1 code-pin | 层2 结构 | 层3 资源布局消费 |
|------|------|------|------|
| streamed | 强制 | 强制 | **真实消费**：stage_limits→_run_stream→run_stream→_materialize→stage.stage_budget；run_stream 把 output_bytes/inflight_bytes 作为 max_item_bytes/max_bytes 传给队列工厂（write 路径 queue_factory=StreamResources.make_queue，它确有这两个旋钮），工厂建 ObservedQueue(max_item_bytes=output_bytes, max_bytes=inflight_bytes)；checkpoint_policy→terminal.checkpoint_policy→stream_barrier interval/admit |
| relational | 强制 | 强制 | **不消费**：DataFusion 原生 execute() 无 stage 队列、无 ObservedQueue、无 barrier 旋钮。封存预算仅为契约事实（B 可解释），执行不消费任何一项。不声称 native physical 驱动 DataFusion |
| fallback | 强制 | 强制 | **经 run.limits 一致消费**：_rows_as_batches(byte_limits=current_run().limits) 直接消费 max_row_bytes/max_batch_bytes/batch_rows；封存 stage_budgets 由同一 run.limits 推导，二者按构造一致。无独立 stage 队列旋钮，不声称独立强制 |

**relational 绑定对象纠正（总控指出）**：local.py:1176 的 native `physical`
被 relational execute() 丢弃。封存与绑定对着**真正执行的 source+plan**
（freeze_source 后的 source + LogicalPlan），不对着被丢弃的 native physical，
不声称它驱动 DataFusion。compile_physical_execution_plan 产出的 physical 仅作为
封存契约文档（供 B 解释器/工作台读），其 stage_budgets 由 run.limits 推导以良构。

**checkpoint_policy 必须真正落到 progress 对象（总控指出，非装饰）**：
stream_barrier.py:134/155/208 通过 `getattr(self.progress, 'checkpoint_policy', None)`
读取。streamed 路径的 progress 就是 StreamMaterialization 实例
（stream_materialize.py:92-93 `progress=terminal`）。因此 prepare_stream_blocks
必须 `terminal.checkpoint_policy = checkpoint_policy`（设置实例属性），
barrier 才真实消费 interval_ms/unconfirmed_bytes/unconfirmed_rows。
只把参数透传到 _run_stream 而不设到 terminal 上是装饰——本稿明确设到实例。

**反例测试（必须落在 relational 分支，非仅 streamed；tests/test_c_write_seam.py）**：
- R1 结构失配（隔离层2）：用真实 KeyedSource+BoundMapOp+FilterOp relational 形态
  封存，执行 plan 不变（code-pin 仍匹配），仅扰动**封存侧**算子序列
  （handle.topology["operators"] 反序）调 `.verify(plan)` → RecoveryContractError
  "operator identity differs"。code-pin 先过、结构再拒，证明层2 独立绑定结构。
  （注：code-pin 与 logical_node_id 同源于 describe+index 且保序，不存在 code-pin
  相同而结构不同的真实 plan 对，故隔离层2 须扰动封存侧。）
- R2 code-pin 失配（层1）：同形态封存后，以篡改业务函数的 plan（enrich→enrich_drift，
  code-pin 变）调 `.verify` → RecoveryContractError "code-pin differs"。
- R3 端到端 relational 失配先于发布：monkeypatch seal_write_topology 封存 drifted plan，
  驱动真实 relational write_lance（执行原 plan），整个 RunContext 包在 pytest.raises 内，
  断言 RecoveryContractError "code-pin differs" 先于 CommitCoordinator.commit、
  target.lance 未创建、journal 无 acked commit。
- 黄金路径（三分支）：真实 relational/fallback/streamed write_lance 端到端，断言
  F1/绑定封在父 id 下、工作台 facts reader available=true、logical_dag.nodes 种类正确、
  四方身份（run/action/dag/plan）相连、发布行正确。身份桥需 checked 报告上下文，
  由 E 的真实闭包验收测试覆盖（直接单元 write 无 checked 报告，桥诚实不封）。
- streamed 层3 消费机制测试：真实 streamed write，spy 捕获 StreamMaterialization 实例
  与 StreamResources.ObservedQueue 构造 kwargs，断言 layout() 回传 run.limits 推导的
  checkpoint_policy/stage_limits、terminal.checkpoint_policy==policy、且确有队列以
  max_item_bytes=output_bytes/max_bytes=inflight_bytes 建成——证明层3 真实消费而非透传。

## 4. native commit authority 不被破坏

seam 只影响执行前的 compile+seal+verify。执行阶段完全不变：
- prepare_stream_blocks 内部 StreamMaterialization(staging_only=True)
- ObjectRowsSink / LanceSink 唯一 final sink
- CommitCoordinator(action).recover / commit 唯一提交权威
- keyed append-overwrite CAS 不变
- action.complete 唯一终局记录
- 子 action '-stream' 的 checkpoint 对账不变

不创建新 CommitCoordinator，不改变 commit sequence，不改变 recover 语义。
stage_limits/checkpoint_policy 只约束流内部队列和 barrier，不触及 publication。

## 5. 崩溃恢复

1. resume → write_lance 再进入对应分支 → seal_write_topology 幂等：
   相同 source+plan → 相同 dag_id（content-addressed）→ seal_plan_facts 同内容幂等。
3. 改业务代码 → code_pin_digest 变 → dag_id 变 → F1 内容变 →
   bind_source 拒绝（"changed since seal; resume refused"）。
4. verify_loaded_plan 在 resume 时再次执行：若 plan 与封存不一致 → 拒绝。
5. prepare_stream_blocks 恢复逻辑不变：
   - CommitCoordinator.recover 检查 final Lance commit 已 acked → 返回 receipt。
   - 未 acked → 重放已 acked child checkpoint descriptors → 恢复流。
6. 资源释放：stopped/done/quarantine 不变。单一 run.sqlite 不变。

定位封存计划：journal.source_binding(action_id + "-sealed-plan")（父 id）。
已 acked 位置：CommitCoordinator.recover → facts[-1].intent.position。

## 6. 覆盖范围诚实划定（按裁决修订）

| 形状 | 覆盖？ | 理由 |
|------|--------|------|
| relational（:1229-1232，has_relational→DataFusion） | **YES** | 真实工作台入口；层1+2 强制，层3 不消费（诚实写明） |
| fallback rows→batches（:1233-1237） | **YES** | 层1+2 强制，层3 经 run.limits 一致消费 |
| streamed（:1207，has_stream=True） | **YES** | 层1+2+3 全强制，checkpoint_policy 落 progress |
| pure Lance copy（:1208-1222，无 operations） | DEFER | 无算子链，封存空 DAG 不是实际执行的计划；本轮延后 |
| column merge（:1215-1221） | DEFER | pure copy 子路径，随 pure copy 延后 |
| arrow_batch_sink（:1223-1227） | DEFER | 本轮延后；不封存非实际执行计划冒充覆盖 |

延后分支不调用 seal_write_topology，D reader 诚实返回 no_sealed_plan_facts。

## 7. 有限资源

stage_budgets 由 run.limits 推导（第三轮已修）：
- output_bytes = max_row_bytes + 256
- inflight_bytes = max(output_bytes, max_batch_bytes)
- inflight_rows = batch_rows

新 seam 下：prepare_stream_blocks → _run_stream → run_stream(stage_limits=...)
→ _materialize 设 stage.stage_budget → run_stream 队列构造把 output_bytes/inflight_bytes
作为 max_item_bytes/max_bytes 传给队列工厂 → ObservedQueue。

**实现期实证修订（stream.py）**：write 路径的 _run_stream 恒传
queue_factory=StreamResources.make_queue（dataset.py:1122）。原 run_stream 仅在
`queue_factory is None` 时才读 stage_budget 建 ObservedQueue（chain worker 路径），
write 路径走 else 分支 `make_queue(depth)` 用 run.limits 默认界，stage_budget 被忽略
（层3 对队列沦为装饰）。StreamResources.make_queue 本就接受 max_item_bytes/max_bytes
（stream_resources.py:44）——确有可消费旋钮。故修 run_stream：stage_budget 存在时
无论 queue_factory 是否为 None 都把界传下去（None→直接 ObservedQueue；非 None→
make_queue(depth, max_item_bytes=, max_bytes=)）。三个 worker 调用点
（execution.py:89/graph_execution.py:185/queue_transform.py:86）均 queue_factory=None，
行为字节不变；公共 run_stream 无 stage_limits（stage_budget=None）走原 else，不变。
仅 write 路径开始真实消费封存 stage_budget。宽行/背压/字节语义与 chain worker 一致。

## 8. 影响面与回归

预计改动文件：
1. execution/chain.py：抽 `_build_sealed_manifest` helper；compile_chain_topology
   加 defaulted terminal_kind/sink_ids；新增 seal_write_topology + 封存句柄（装配区）。
2. execution/executors/local.py：write_lance 两处落点 +compile+seal+verify；
   内层闭包发布前调 verify；streamed 传 layout 给 prepare_stream_blocks。
3. execution/stream_materialize.py：prepare_stream_blocks +stage_limits/checkpoint_policy
   参数；`terminal.checkpoint_policy = checkpoint_policy`；_run_stream 传 stage_limits。
4. data/dataset.py：_run_stream +stage_limits=None 透传到 run_stream（None 时不变）。
5. execution/stream.py：run_stream 队列构造在 stage_budget 存在时把界传给队列工厂
   （write 路径真实消费，§7）；queue_factory=None 分支行为字节不变。

不改动：worker/（只 import load_worker_layout）、graph.py、queue_chain.py、
LanceSink、CommitCoordinator、ObjectRowsSink、承重七件、workbench/、E 测试。

回归安全：
- compile_chain_topology 新 kwargs 有默认值，现有 chain/graph/queue 调用者字节不变。
- _build_sealed_manifest 抽取为行为保持重构，由现有 chain 测试守护。
- stage_limits=None / checkpoint_policy 未设时 run_stream/barrier 行为不变。
- verify_loaded_plan 是现有函数，不新增逻辑；seal_write_topology 是新函数。
- 延后分支不经过封存点，行为不变。
- stream.py 改动只影响 stage_budget 存在且 queue_factory 非 None 的 write 路径；
  三个 worker 调用点 queue_factory=None、公共 run_stream stage_budget=None，均不变。

回归实测（本轮）：
- tests/test_c_write_seam.py 8 passed（R1/R2/R3 + 三分支黄金 + streamed 消费 + 延后诚实缺席）。
- test_u2_chain_local + test_stream_admission + test_c_runtime：56 passed。
- test_u2_chain_commit + test_u2_chain_faults + test_datafusion_execution：33 passed。
- E test_upgrade_business_acceptance（未改）：22 passed（真实闭包）。
- test_stream.py 有 3 个**先前已存在**失败（test_queue_factory_transport_seam 等，
  自定义 queue_factory 被 StreamResources.make_queue 拒绝）；已用 HEAD 原版 stream.py
  复跑确认同样失败，与本轮改动无关（本轮 else 分支字节未改）。
- 承重七件 git diff HEAD 为空，字节冻结。

## 9. 已核实清单

- KeyedSource 被冻结 dag.py 身份/编译路径支持（运行时探针 PASSED，§0）。
- terminal_kind 无固定集合校验（dag.py:122/128/146 透传），"write_lance" 可用。
- load_worker_layout(launch_spec, authority, assignment) 可本地复用（worker/layout.py:11-61）。
- checkpoint_policy 消费点 stream_barrier.py:134/155/208 读 progress 实例属性。
- run_stream 已接受 stage_limits（stream.py:1119）→ _materialize（:1136）。
- Dataset._run_stream 当前无 stage_limits，需加（None 时不变）。
- relational execute() 丢弃 native physical；封存绑定 source+plan。
- 父 action_id 与工作台读取对齐（facts.py:105 用父 id）。
- 子 action '-stream' 只用于内部 checkpoint，不影响封存槽 key。
- node_observations / runtime_commits / manifest 已存在，不重建。
- supported_slice 是拓扑形状标签（B SUPPORTED_SLICES），write 线性链复用 chain_v1，
  terminal_kind=write_lance 由 logical_dag 承载；臆造 write_lance_v1 会被 B 拒绝（§2）。
- write 路径 queue_factory=StreamResources.make_queue 恒非 None；原 run_stream 仅在
  queue_factory=None 时消费 stage_budget。make_queue 接受 max_item_bytes/max_bytes，
  修 run_stream 传界后 write 路径真实消费（§7，stream.py）。
- 三个 worker run_stream 调用点 queue_factory=None，公共 run_stream stage_budget=None，
  stream.py 改动对二者行为字节不变（回归 56+33 passed 守护）。
- test_stream.py 3 失败为 HEAD 先前已存在（自定义 transport 被拒），与本轮无关。
