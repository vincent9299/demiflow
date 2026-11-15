# DemiFlow v3：按原始意图审查及隔离修复记录

> 本文保留初次审查时的基线结论。继续实施已打通真实 Dataset 主链路和部分原生模型链路；当前证据与未关闭项见 [CORE-PROGRESS.md](CORE-PROGRESS.md) 与 [NATIVE-PROGRESS.md](NATIVE-PROGRESS.md)。下文“完全未接通”不能再当作最新状态；整体尚未最终验收仍成立。

**裁决：尚未达到可交付状态，不能合入主仓，也不能把本报告当作最终验收通过。**

“19 路代码交齐”属实；“同一公开 Dataset pipeline 在 Local/Ray 上真实运行、得到结果、提交并恢复”不成立。当前不是少量显示问题，而是业务执行循环、跨 worker 数据交接、终端订阅、提交回执尚未组成闭环。本文同时记录已落地修复与未关闭项，避免把 Review、编译通过、worker 启动、模块测试混作完整产品交付。

## 1. 两个原始 root 与证据边界

- **产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。保留已确认 demo、开发对话、Notebook Python/SQL、媒体/Case/标注与历史。
- **平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local 和 Ray Core 只是承载后端；CPU/GPU/API/IO 独立调度，三种交接、有限资源、原生提交恢复和历史保护必须一起成立。

原始引擎会话：[引擎架构讨论](codex://threads/01a1af6f-55a9-7550-a78d-89e732f1d510)。逐条原文及时间线见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md) 和其引用的 `evidence/intent-sources.json`。本轮不把外围完善提升成交付前置，也不将移除 Ray Data 误作统一引擎已经完成。

### 样本、树和授权

- Qwen 集成输入：`362d8528c27ae60a0d6ae0ccc986a0ff93847de8`，`integration/runtime-v3-20261114`。
- 修复树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。
- 修复分支：`engine/gpt-runtime-v3-review-20261115`。
- 原移交材料：`/tmp/demiflow-integration-evidence/GPT-REPAIR-HANDOFF.md`。
- 主仓 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65` 只读；不在主仓跑测试、不合并、不推送、不改服务、不安装共享环境、不访问付费模型/生产数据库。其他 lane 的树未改。
- 本轮验证显式固定 `PYTHONPATH=<修复树>`，用指定环境的 `python -B -m pytest`。裸 `pytest` 不作为证据。
- 测试与日志存于 `/tmp/demiflow-gpt-runtime-v3-review-20261115`；最终输入摘要、日志和验证命令索引见同目录的机器可读证据以及本报告配套 `verification.json`。

## 2. 总体链路审查：断在哪里

```text
Dataset 公开 API
  → ActionRequest / 同一个 RunAction                         已有
  → compiler / source snapshot / physical plan / sealed pin 修复后真实 roundtrip 通过
  → 单一 ResourceManager / Local 进程或 Ray actor            修复后真实启动、激活、退出通过
  → DataflowWorkerExecutor.step 的宿主调用                   缺失
  → 按图接入、投递、消费、释放 credit、传播 EOI               未闭合
  → TerminalSubscription / 有界 rows / count / materialized  driver 生产端缺失
  → 全参与者 cut / 原生 commit / receipt / ACK               模块可用，运行路径未接通
  → 工作台同一封存计划 + 节点观测 + 代码桥                   计划读取已修，真实桥和观测未闭合
```

**公开业务反例**：最小 `data.from_items(...).map(...).take(3)`，以及 `read_json → map → filter → map_batches → take_all` 从公开 API 进入引擎后，在真实 worker 激活时抛 `unknown kernel kind`。原因是 worker 对 source/terminal 也调用普通变换 kernel 工厂。不是测试 double、不是 Ray 环境缺失。即使单修该点，后面还存在下列独立断点：

1. `worker/bootstrap.py` 和 `worker/dataflow_host.py` 没有调用 `DataflowWorkerExecutor.step()` 的业务循环。
2. `_publish_routed()` 写完 segment 就丢弃返回引用；没有消费端投递。`_consume_input()` 反而从发往 driver 的 mailbox outbox 取输入。
3. `RuntimeActionHandle` 要求 `execution.services/request/subscribe_terminal()`，`DataflowExecution` 没有这些生产端。
4. `pump()` 把 progress/eoi/cut 等全部提交给只允许 journal commands 的服务；`finish()` 尚未收齐真实 contribution 就请求最终提交，随后构造无结果引用的 TerminalResult。

不能通过“driver 自己遍历 Python 数据并返回列表”、回退旧 Local 执行器、假造完成事件、跳过封存/cut 或新增第二套提交账本把黄金路径做绿；这些做法会违背本次升级的原始意图。

## 3. 已落地并有真实接缝证据的修复

| 修复组 | 实际修改 | 证据与限制 |
|---|---|---|
| DTO / 编译 / 封存 | 合法的草稿 sha256、occurrence/edge 绑定、严格 JSON 与内层冻结兼容；平台节点不强索业务 program；实际平台 revision 固定 | 真实 compiler → source snapshot → physical → seal → reload；不是公开 action 成功 |
| 多输入与源 | 每个源绑定自己的 occurrence/splits；worker 只接本 stage 的源；take(0) 不读取/序列化原不可 pickle 闭包 | 两源 union 编译、拆分及 take(0) 预备阶段验证；union 执行尚未验收 |
| 公开输入历史 | 恢复只读 blocks/row_count，准确传递 override_num_blocks，保留基线 literal materialization 的分块和身份 | 历史形状逐值/身份对照；不添加非 None recovery_reference |
| 行保真 | list/tuple/bytes/正负零的不可变 Record 和数据 codec；source→kernel→exchange/sink 使用一致编码 | 实际 store 往返、嵌套结构、CallableSpec fn_args/kwargs、reader pending 重放 |
| 单一分配 | endpoint 复用 driver ResourceManager；先 stage launch 再分配；完整 desired plan 保留已有 worker；pending 分配不丢身份 | 两个真实 Local 进程分配/清理；等待队列完整调度仍待闭环 |
| Worker 装载 | 解码真实 assignment；从原 store 读封存事实；平台 pin 与逐 program pin 校验；不打开 worker RunJournal | Local/Ray 真实托管 worker 加载并激活 map；不把激活当业务成功 |
| 控制 RPC | Local 命令和 socket 接收分线程；真实 mailbox 请求/回复；Ray 并发回复；K3 生命周期与 v3 WorkerEvent 分离 | 真宿主启动与 journal 请求；慢流、断连、完整业务背压仍需补证 |
| 预算与 journal 回复 | 重复 read/check_grant 能重新收到回复；已完成未消费回复计入上限；不静默丢弃；staging 使用 spill 上限 | 连续请求与未读回复背压；累计 staging/GC 仍未闭环 |
| Cut / 原生提交 | 原生 participant 标识在 B/D/X 一致；RecoveryImage 合法草稿；协调器按 action 绑定 | 真实 journal + sink + cut commit/recover，含 tuple/bytes；尚非运行中多 worker barrier |
| 工作台 / 历史读取 | 读取 writer 实际 `.dataflow-store/<store_id>`；原生 seal schema/字段一致；只读 store 无 mkdir/预算/写 journal | 真实封存 action 的工作台/历史读取；读前读后目录清单相同 |
| 诚实身份桥 | 移除把 execution occurrence 当 report node、把 graph digest 当 report digest 的伪桥；未绑定时明确 unavailable | 反例锁定；真实 CheckedExecution→v3 bridge 仍缺生产端 |
| Ray 退出确认 | kill 后等待真实 actor death；不因 kill 前排队的 identity RPC 成功而提前返回 UNCONFIRMED | Ray 2.60 本机实际激活、取消、清理通过；不声称多机 |
| 排序 spill | Record/只读 mapping 不再直接 pickle；数据-only 类型 codec 保留 provenance/tuple/bytes；单帧与 FD 不得绕预算 | 实际排序往返及预算反例；混合数值排序、完整并发峰值尚待验证 |
| 历史准入 | 按 PhysicalDataflow wire 解码原计划；比较原 platform pin；无 journal-proven image 不声称可恢复 | 原平台与错误平台反例；v3 历史 image 装配仍未闭合 |
| 验收诚实 | 24 个未实现语义的 scenario 不得输出通过证据；删除 W fake 模块冒用真实 `__file__` | guard 测试证明不能假绿，不是那 24 个业务场景通过 |

未修改承重七件：raw_fetch.py、queue_execution.py、queue_sinks.py、queue_source.py、planning/dag.py、contracts/exchange.py、resources/contracts.py。对 v3 新契约的增量见 §7。

## 4. 仍阻塞完整验收的问题（以源码为准）

### B01 / P0：宿主、业务循环、终端没有连起来（I01/I06/I09/I14）

涉及 `execution/dataflow.py`、`worker/bootstrap.py`、`worker/dataflow_host.py`、`worker/dataflow_execution.py`、`dataflow_handles.py`。

宿主只有控制循环；worker 启动构造所有 kind 的普通 kernel；T 端订阅所需 E 端方法缺失。局部补一个属性不能解决这个问题。关闭条件必须是公开 API 在真实 Local 进程和 Ray Core actor 中同语义运行，take/iter/count/materialize/write 均由同一执行驱动产生结果，take 的提前结束、取消、超时和错误退出都有真实清理。

### B02 / P0：Exchange 没有从生产端接到消费端（I03/I05/I06）

涉及 `worker/dataflow_execution.py:_route_source_record/_consume_input/_publish_routed`、`exchange/dataflow_channels.py`、宿主传输。

segment 发布引用被丢掉、mailbox 入出方向混用、一次读取 budget 之外的消息可能被丢弃。源数据直接走输出边，不执行融合 stage 内 kernel；消费端向所有 kernel/port 广播，不按真实边和输入 port 路由。credit 被 reserve 后没有消费释放闭环。关闭条件为三种模式分别验证实际传输、有限背压、双输入/共享前缀、输出 multiplicity、EOI 与恢复后的路由序列，不能用独立 encode/decode 替代。

### B03 / P0：运行中的 barrier/cut/receipt 仍然断裂（I07/I13）

涉及 `DataflowExecution._handle_event/finish`、`DataflowWorkerExecutor.request_cut/_process_pending_cut/snapshot/restore`、`dataflow_checkpoint.py`。

- driver 丢掉真实 contribution，重新构造空 sources/state/channels/sinks。
- worker 拒绝 driver 发送的 `final=True` cut，错误理解为“worker 发起最终 cut”。
- step 中 `_business_active=True` 时请求 snapshot，snapshot 又拒绝 active 状态。
- 原状态以 `occurrence:partition` 保存、以 `occurrence` 查恢复。
- 历史 contribution 的 boot 被要求等于新 worker boot，合法换 worker 恢复会被拒。
- 任一源 EOI 就设置全局 source_exhausted；没有全部源、全部边和全部终端对齐。

关闭条件为多 worker cut 真实投递、全 payload 保存、唯一 driver 提交、receipt 后释放/ACK，以及 prepare/commit/ACK 各崩溃点重启反例。当前真实 coordinator 单元接缝通过不能消除上述问题。

### B04 / P0：原生业务声明与执行工厂不一致（I09）

`dataflow_api.make_native_operation` 将 cache/lookup/queue 等全部送到只认旧模型 Op 的 `build_model_factory`；后者连新 NativeOperation 都不接受。Dataset 多处调用 make_native_operation 后丢弃返回值，仍构造旧 wrapper；名字还有 `embeddings/embedding`、`fetch/raw_fetch`、`enqueue/queue_produce` 等差异。模型 compiler 把业务字段名当输入 port，与图的 `main` 不一致；program 来源与 worker 构造也不齐。

不能用删 preflight、放宽守卫或任意 kind 返回空 KernelSpec 收口。必须使公开声明、唯一 registry、封存工厂、worker 构造、原生 request journal 是同一个对象/契约链；cache 的七条历史真缺陷必须由状态适配修复，不放宽 operator_precheck。

### B05 / P0：效果事实有第二份持久账本（I07/I10）

`dataflow_authority._EffectLedger` 在 `.dataflow-effects/*.json` 保存 effect 状态；budget 在原 journal，二者不原子。mark_sent/confirm 收到重复请求会拒绝，reserve/响应丢失/driver 崩溃之间仍可能留不一致。`lease=None` 分派没有逐 allocation authority 验证；预算名字默认都拿 resident bytes 当上限也不成立。

需要复用原生 journal 的效果/请求事实、预算和幂等权威；不能靠再封装这个目录称作“没有第二份账本”。未知付费效果不自动重放是硬边界。

### B06 / P1：真实放置、GPU、远程读取域未兑现（I04/I08/I11）

`_driver_hosts` 固定 Local 为 8CPU/8GiB；Ray host 声明从 driver 复制 runtime/store domain。`verify_host_domain` 只在 driver 读 challenge 就为任意 host 出证明。`resolve_data_location` 拼出 `ray-object://<digest>`，没有真实 Ray object。scheduler 所有 place 指向 hosts[0]，WorkerRequest placement=None；声明 host 不等于 Ray 实际放置。GPU device pool 没在 driver assembly 接入。

需要节点上原地代码/数据可读证明、实际 placement/资源 observation、CPU/GPU 分离与有界副本。单机 Ray actor 通过只证明 Core 宿主可启动。

### B07 / P1：资源有界仍有实质空洞（I08）

`SqlReader._open_spool` 先 `tuple(_capture())` 再检查字节；Arrow IPC 先 read_all；部分 join/group/batch 只按行数组包；转换后才检查副本；排序/聚合/缓冲状态与 stash 的实际驻留需要组合计量。`RunArtifactStore.put` 预留 max_bytes 的累计计费，缺乏失败/重复写的释放与孤儿块回收闭环。不可凭 small-row 测试或 RSS 终止代替主动预算。

### B08 / P1：历史恢复保护仍不完整（I13）

F 输入身份已修；原承重/历史公式未改。W v3 loader 仍没有从原生 committed cut 建出 restore_image；准入已改为明确拒绝缺证明。正常 resume 应读取原 sealed plan，而 E.prepare 还会重新编译封存。旧 schema 与新 DTO 的适配必须保留旧身份和旧代码事实；未完成的 37 条旧预期迁移与 7 条真缺陷需要逐 nodeid 对账。

### B09 / P1：产品只有部分读取通，不能视作整体产品通过（I12）

工作台现在能读原 store 的真实计划，伪 bridge 已撤。但真实报告节点→执行 occurrence 的映射、NodeObservation、历史源码、同 run/attempt 控制和结果尚无组合证据。Notebook、Case、媒体与开发对话没有进行浏览器回归，不能从引擎测试推导它们无回归。

### B10 / P0（验收）：原有“黄金场景”证据不足（I11/I14）

24 个 scenario 原实现存在用小 map 代替队列/模型/多 worker、用字符串代替 GPU/remote journal、用 kill sleeper 代替 commit crash、用 collect-only 代替全历史测试等问题。当前明确 NotRunError，且配套 guard 锁定不得假绿。余下真实场景也须通过公开路径并记录真实事件；现有单元测试数不能折算功能百分比。

### B11 / P1：状态缓冲及数据类型还有独立断点（I08/I09/I13）

排序与 join 的实际数据编码往返已修复并验过，但 `OperatorKernel._SpoolPort._flush/snapshot` 仍直接 pickle 含 MappingProxy 的 Record，有限输入/缓冲快照会失败。不能从裸排序函数通过推导整个排序算子和恢复通过。现行 X 数据 codec 的闭集合还不覆盖 numpy ndarray/set 等公开输入与历史业务类型；须按原 API 逐类型证明保真或显式界定，不允许默默改为 list/string。

## 5. 对照用户原始意图

| 意图 | 当前判断 | 用户实际可见/可依赖的边界 |
|---|---|---|
| I01 同一 Dataset / 引擎 | 未达到 | 两端进入统一 v3 入口，但公开 action 仍失败 |
| I02 Ray Core、退出 Ray Data | 源码层已达到 | 当前 demiflow Python 源码无 Ray Data 导入/调用；实际 Core actor 启动成功 |
| I03 三种交接 | 部分模块实现 | 模式与 codec/ledger 存在，业务传输未闭合 |
| I04 CPU/GPU 独立调度 | 未达到 | stage/profile 有结构，实际 placement/GPU 未兑现 |
| I05 多输入/输出/共享 | 部分编译实现 | 多源图能编译；运行路由/共享计算未验收 |
| I06 及时交接/背压 | 未达到 | 控制回复有界已验证，业务 credit/first-row 未贯通 |
| I07 提交、ACK、恢复 | 部分原生接缝通过 | coordinator/sink 真提交恢复通过；运行 cut 与效果权威仍阻塞 |
| I08 有限资源/高性能 | 部分 | 输入/帧/控制 budget 有修复；全路径峰值、磁盘和吞吐未证明 |
| I09 完整业务 | 未达到 | 原生声明/模型/队列/缓存整链仍不能作为交付 |
| I10 清晰职责/单一权威 | 部分 | 物理分配重归一个 ResourceManager；效果文件账本仍违背目标 |
| I11 多机与诚实证据 | 未达到 | 假绿已纠正；仅本机 Local/Ray 宿主证据，没有多机业务证明 |
| I12 统一真实工作台 | 部分 | 实际封存计划只读读取通过；真实节点桥、观测和完整 UI 未验收 |
| I13 历史保护 | 部分 | 原输入身份、冻结七件保持；完整旧版本恢复/回归仍未完成 |
| I14 完整交付 | 未达到 | 不以 19 路完成、任务卡数或局部 passed 数声称完成 |

## 6. 修复与复验的合理顺序

先完成一个不可拆开的集成纵切：**E driver ↔ B 宿主业务循环 ↔ X 双向数据/credit ↔ T 真实终端 ↔ D cut/receipt**。它们可以按冻结接口分工，但交付条件必须是同一个真实公开 pipeline 跑通，不是分别再次交符号级单测。

随后在同一条纵切上接入 M/Q 原生模型/队列/缓存与统一 journal；再验两机放置、GPU 与故障恢复；最后工作台真实桥/观测和既有产品回归。历史保护和资源上限应沿途验证，不能等最后才发现不可兼容。

这一顺序不增加阶段 0、卡片计数、重复设计稿等流程产物；每一步都产生用户可实际运行的能力。当前分支只是一份保留证据的修复候选，尚不具备上线/主仓合入条件。

## 7. 本轮 v3 契约迭代（不改旧身份公式）

- 增加 `CAPACITY_PENDING`（AMEND-001 第 14 类错误）：临时准入等待；`BUDGET_EXCEEDED` 与 `UNKNOWN_EFFECT` 不可伪装可重试。
- v3 physical partition 使用原生 K3/K4 可接受的 `stage-<digest>-pN`；participant 统一由 `compute_participant_id(stage_id, partition_id)` 生成有界 sha256。消费者 B/D/X 共用，不能各拼一版。
- `build_input_handle(..., override_num_blocks=None)` 增量传递原 API 的分块意图；`blocks/row_count` 只读且身份与旧实现一致。
- Worker 从已验证的 SealedExecution refs 读取，不在 worker 重开 journal，也不把 code-pin JSON 当运行 zip。
- `RunArtifactStore(..., readonly=True)` 用于历史读取；真实 store_id 来自原封存 ref。
- 未建立真实 report binding 时，写 `identity_bridge_unavailable.v3` 明确原因，不编造 node/digest 映射。
- v3 排序临时数据改用现有 data-only codec，保留 typed Record；未改既有历史结果格式。

## 8. 验证口径

本轮将证据拆为：真实值/模块接缝、真实 Local/Ray 宿主、公开业务路径、全 lane 测试、历史测试、浏览器/多机/故障/性能。前两类通过不能代替后五类。

已有真实接缝组合：**165 passed**（30 个真实接缝实例、24 个证据诚实 guard、88 个 DTO/codec、23 个既有 worker 测试）；它们不是 164 个业务场景。输入摘要在运行前后相同。公开业务路径仍为失败。全量和后续修复的最终结果以配套 `verification.json` 为准，禁止从本文局部数字推导最终验收。


### 本轮最终测试结果（不等于接受）

| 验证 | 结果 | 口径 |
|---|---|---|
| `real-proof-final` | 165 passed，22.46 秒 | 实际值/模块接缝、Local 宿主、诚实 guard、DTO 和旧 worker；运行前后源码摘要一致 |
| `ray-host-final` | 退出码 0；activated=true；cleanup_confirmed=true | Ray 2.60，本机 1 host，非业务 pipeline、非物理 GPU、非两机 |
| `public-golden-final` | 失败：`unknown kernel kind` | 真公开 API、真 Local worker，不替换 driver/kernel |
| `all-runtime` | 全局 240 秒超时，未完成 | 不算通过；没有完整 JUnit 结果，不能把进度字符折成计数 |
| `lanes-final` | 被中断 | 常规 lane 测试阻塞后停止本轮自己的测试进程；不计通过 |
| `lanes-diagnostic` | **559 collected / 225 passed / 334 failed**，122.67 秒 | 临时每测试阶段 15 秒 deadline；两项明确 TimeoutError，不跳过测试、不把 timeout 当通过；是诊断跑，不是正式门绿 |
| 静态/边界 | Python AST 无语法错误；diff --check 通过；七件逐字等于 267b914 与 362d852 | 不代表运行语义正确 |
| 全历史 / 浏览器 / 两机 / 物理 GPU / 性能 | 未完成或未运行 | 不得用上述数字替代 |

与 Qwen 清洁基线 306 failed / 253 passed 对比：5 个原失败变通过，33 个原通过变失败，净增加 28 个失败。**没有隐藏这 33 个新失败，也未宣布无回归。** 其中有接口修复后的旧 double 不匹配（缺 resources/store_root/bounds、忽略 override_num_blocks）、测试继续期待错误源 kind/封存字段、以及此前未到达的真实业务失败。完整逐 nodeid 差异见 `lane-test-delta.json`；这些断言仍须逐条迁移或补修，不能整批删掉。165 项真实接缝通过不抵消此处失败。

### 产品 root 的完整边界

| 原始分支 | 目标 | 本轮证据状态 |
|---|---|---|
| R1.1 / R1.2 / R1.3 | 同一工作台四 tab、已确认 D1.9、紧凑自然交互 | 本轮未做浏览器验收，未宣称完成 |
| R2.1 | 图、节点输入输出、参数、真实源码 | 原封存图只读读取通过；桥/观测未闭合 |
| R2.2 | 配置→检查→定位→真实提交 | 新 v3 公开 action 失败，不能交付 |
| R2.3 | 同一 run/attempt 的日志、状态、停止、恢复、结果 | 宿主清理有证据；业务恢复/结果未闭合 |
| R3.1 / R3.2 / R3.3 | 数据目录/分布、Notebook Python/SQL、通用媒体输出 | 本轮未验；不以引擎测试代替 |
| R4.1 / R4.2 / R4.3 | 分组选样、多方案多字段比较、持久标注/历史/冲突 | 本轮未验；既有业务/Notebook/标注数据未动 |
| R5.1 | 清晰职责、复用、清理重复体系 | 分配权威已修，效果第二账本仍阻塞 |
| R5.2 | 有限资源、性能、真实失败取消恢复 | 部分修复，尚未整体验收 |
| R5.3 | 项目/数据/dirty/历史保护 | 主仓只读、七件字节保持；全历史回归仍未完成 |
