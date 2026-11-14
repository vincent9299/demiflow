# Qwen 独立 Review：DemiFlow 函数级升级设计（upgrade-contracts-v2-20261113）

审查对象：`_demiflow/worktrees/gpt_contracts_20261113/docs/upgrade-contracts-v2-20261113/`（设计树 HEAD `4efaa35`）
源码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`，读取树 `integration_c_20261113`（clean，已核 HEAD）
对照在途树：b_option `e517110`、c_budget `90d7ee4`(dirty 1)、e_dfamily `4489ba0`、d_workbench `962218d`、n1_prompt `bbd93ff`(dirty 2)、n2_retired `bbd93ff`(dirty 5)、n3_resource `bbd93ff`(dirty 0，零提交)
本次只做 Review 与报告：未改主仓、未在主仓跑测试、未 push、未合主仓、未停止任何在途工作、未向任何会话发消息。

---

## 一、结论

**修订后分发；其中七处关键方案需重做，不能按现卡直接编码。验收口径必须先修——否则无论实现多少，"全绿"都不成立（新旧两侧同时假绿，见 Q53/Q06/Q50）。**

> 本报告为多遍重审的累积件，**共 8 遍取证**：
> **Pass 1**（设计文档＋机器索引＋定点任务卡＋bbd93ff 源码）产出 Q01–Q39；
> **Pass 2**（以用户此前确认的架构图与模块边界权威为锚）产出 Q40–Q46，并精确化收窄 Q31；
> **Pass 3**（五轮历史 Review＋§6/§7 两份总控裁决对账）产出 Q47–Q49；
> **Pass 4**（全 144 卡逐张通读）并入 P4-01~15，我复核后改判 2 条、加硬 1 条；
> **Pass 5**（45 文件签名／冻结字节／117 逐 id 分配）产出 **Q53–Q55**；
> **Pass 6**（20 意图根→卡→门→命令→断言闭合审计）产出 Q50–Q51；
> **Pass 7**（基线摘要机器复算）产出 Q52；
> **Pass 8**（我亲自跑运行时探针）把 **Q27/Q50 从静态推断升级为实测事实**；
> **Pass 9**（我亲自跑运行时探针）把**第一优先项剩下的四条 P0——Q01/Q02/Q03/Q47——全部升级为实测事实**，并新增 **Q56**（`native_snapshot` 使 cut 快照只封存第一个 worker 的 pending）；
> **Pass 10**（我亲自跑运行时探针）**撤回了我自己判的 Q04（P0）**——C04 本就明文固定了 factory 默认 ownership、S001/S004/S009/S010/S011 调和链齐全，我把基线现状误当了设计缺陷；残留的窄风险另立 **Q57（P1，已实测）**。**"需重做七处"数量不变**（Q04 原不在这七处内），但**第一优先项减少一条 P0**。
> **完整重审实际次数＝2 遍（Pass 1、Pass 4），其余 8 遍为维度锚定深审，不计入**；逐遍状态与时间口径见**文末进度表**与**表 4.8**。**"10 遍完整重审"未完成，本报告不声称已完成**；时长要求已由真人于 2026-11-14 放宽（"不一定要 3 个小时""review 完即可"），故以**覆盖完整、结论可核**为收口标准。

- 可直接分发的部分：F（算子事实）的契约方向与源码一致；Q001 的 signed-zero 缺陷定位准确；A005/A006 明确撤销 B 的能力退步（`78d6492` 已核为退步源）；C08 的 v2 schema 与 ProgramSlice/ProgramBinding 分局部/全图方案经子审查实跑验证正确且最小；async ACK 化的调用点与 Future 类型已核可行。
- 需重做的七处（**数量不变**：Pass 10 撤回的 Q04 原本就不在这七处内，它只在表 3 第一优先项与表 1 的 S 行；其中第 1/2/3/7 项已有第一手运行时证据）：
  1. **A↔L 的 take/iter 封存入口**（Q27/Q28/Q29，**Pass 8 已升级为运行时实证**）：`Dataset.take/iter_rows/count` 只有 `observe_action`，**没有 RunAction**——我在隔离树实测三者 `current_action()` 均为 `None`（而 `current_run()` 已设置），对照 `dataset.py:1897 materialize` 用 `action(...)` 确实开 RunAction；而 `compile_native_topology` 的第一个参数就是 `RunAction`。`count()` 的嵌套错误发生在 `iter_stream_rows` 内部自开的 `run.action`，不是 A004 卡所说的"第二个公开 action"；真正的原生关系分支在 `_apply_plan`。这三处文件/函数**全部无主无任务**。"一份封存布局驱动所有已支持路线"这条平台意图在设计里没有闭合。**最小修正因此很具体：让这三个终端也经 `action(...)` 开 RunAction，不必新造运行上下文。**
  2. **M 的 Embedding 原生恢复**（Q01，**Pass 9 已升级为运行时实证，且两个断点都实测**）：借用的 `RunPromptJournal` 在 reserve 时强制要求执行预算预留，而该预留只由 Prompt 的 coordinator 设置。**我实测两条路径都撞墙**：走 M004 自己的 `bind_actor` → `stream_recovery.py:148` 抛 `RecoveryContractError: Native request has no execution budget reservation`；改为复用 Prompt 的 `bind_client` → `:206` 抛 `AttributeError: 'EmbeddingActor' object has no attribute 'coordinator'`（用**真实** `EmbeddingActor` 跑出）。**对照臂**：同一请求同一 journal，只手动设上预留，`reserve()` 即返回 `True` ⇒ **唯一缺失量就是执行预算预留**，最小修正方向被实测确认。
  3. **R 的通用 buffer 切点**（Q02/Q03＋新增 Q56，**Pass 9 已升级为运行时实证**）：`StageBufferSnapshot` 规定的多数字段在现运行时里没有任何持有者——**我实测 `groups/deadline/prefix_position/next_output_ordinal/row_ordinals` 五个字段在真 `_Stage` 上连 `setattr` 都直接 `AttributeError`（`__slots__` 30 项不含它们）**；grouped 阶段根本没有 barrier 分支（**AST 实测 `grouped_worker` 的 `arrive` 调用点数＝0**，且 `batch_worker:576` 在到达自己 :605 分支前就 return 进去），barrier 落到分组路径会抛 `TypeError`，缺 `arrive` 则只能等 `TimeoutError: Checkpoint coordination deadline exceeded`；并发 batch 的聚合点 `stream_barrier.arrive` 无主无任务，**且实测每 cut 只 align 一次、只拿一个 worker 的 batch；`native_snapshot=True` 时 token 压成 1，拿的是第一个到达者的（Q56）**。这不是"接线"工作量，是要先改运行时数据结构与切点协议。
  4. **W 的 occurrence 图**（Q07/Q14/Q15）：`planning/dag.py` 是冻结文件且边端点键集固定为 `{logical_node_id, port}`，`mountLogical.update` 签名冻结且无运行状态通道，occurrence→源码的后端入口未派发。按现卡实现要么改冻结文件、要么改冻结签名、要么画出一张没有边可连的图。
  5. **L 的源侧代码固定**（Q31/Q09/Q46）：`plan_code_pin` 与 `_business_dependency_modules` 都只遍历 `plan.operations`，join 的 reducer 是 **source 字段**；子审查实跑证明改 reducer 导入的 helper 后 pin 与 source identity 均不变，恢复不拒绝。C08:191 声称 source_identity_ref 覆盖"callable 源的代码语义"，但 `describe` 不走 `__globals__`，覆盖不到。**注意收窄**：operations 侧的依赖改码已被历史真实探针证明 fail-closed，缺口只在 source 侧。叠加 L030 对 write/model 子计划误用 cloudpickle 权威（Q09），而架构权威明文"不能以 cloudpickle 字节充当通用持久协议"（Q46）。
  6. **R 的 buffer 层与既有交换权威重复**（Q42/Q43，Pass 2 新增）：`execution/exchange/`（1410 行，**在跑**，被 graph/queue_chain/chain/三个 worker 文件 import）与 `execution/contracts/exchange.py`（375 行 K4/K5 协议）已实现架构图 C14 要求的"带 channel/epoch/sequence 的 barrier 消息协议＋InputGate 多输入对齐"。设计树对 `InputGate`/`DataEnvelope`/`DataRef`/`ChannelSequence`/`route_epoch`/`contracts/exchange` **全部零命中**，却新建 `stage_buffers.py` 与另一套 ordinal 序号空间。更危险的是 `reservation.py:255-271 derive_prefetch` 明文规定 `max_pending` 不得低于 `max_pending_floor`，"否则 gate 溢出、**接收方在 cut barrier 到达前死掉**"，而 C06 要求预算"只更紧或相等"、设计对该下限零命中。**两个 agent 各自按卡完成，组合即在恢复最关键的时刻崩。**
  7. **R 的 buffer 适配器拆掉了已修保证的承重墙**（Q47，Pass 3 新增，**Pass 9 已升级为运行时实证**）：R006 要删除 `stream_materialize.py:196-202` 的 fail-closed 拒绝，而**该守卫自己的注释就写着 "graph cut deadlock"**——正是历史 F3 家族。**我实测该守卫今天确实对 4 种 kind 生效**（concurrent batch c=2 / timed batch / grouped / prepend 全部被拒，plain row 与 fixed batch c=1 通过），**而 Q02/Q03/Q56 已实测这 4 种的持有者与对齐路径都不存在** ⇒ 删除的是一个真在承重、且下游确实没有替代路径的保护。F3 的联合触发条件（`full = 未确认满额 or capacity_requested`，与 cut 取或）唯一持有者是 `execution/stream_barrier.py:211-214`，**该文件未派发**，且它读取 progress 的 **8 个属性**构成一份无人认领的隐式契约；C05 新引入的 `next_output_ordinal` 在设计树只有 2 次命中、**均为 schema 键列表，无语义定义**（Pass 9 并列实测三个 ordinal 空间：源行 `[0,1,2,3,4]`、已刷批次 `batch_ordinal=0`、per-stage 输出**无持有者**）。**这是"新机制绕过已闭合保证"的最危险形态：bbd93ff 已把 F1–F6 全部修好，而现卡会把其中一处修复的前提拆掉。**
- 验收口径存在系统性漏洞，**新旧两侧都验不到**：
  - **旧侧（Q53，Pass 5 发现、我亲自复算，本轮最严重的新发现）**：**117 条历史确定性失败中，73 条（62%）所在的 6 个测试文件不被任何一条门命令执行**。我解析逐 id 审计载体得 118 行/118 id（扣 1 条墙钟抖动 T＝117），解析 27 个 bash 块得 52 个唯一测试文件、与 `gates.json.required_existing_files` 双向差集为空；逐文件求和＝**被覆盖 44／未覆盖 73**。6 个未覆盖文件（`test_stream_map` 57 条、`test_stream_checkpoint_resume` 8、`test_stream_deduplicate` 3、`test_stream_threads` 2、`test_stream_lance_references` 2、`test_stream_checkpoint_migration` 1）在基线**全部真实存在**却零命中。**清掉这 117 条正是本轮升级的立项理由**；更糟的是 N2 候选正在 dirty 修改的 5 个文件里有 4 个属于该集合 ⇒ **退休 checkpoint 迁移的正确性无门可证**。设计树内也不存在 117→0 或按 id 差集的任何机器守卫。
  - **新侧（Q06，三遍独立命中）**：**21 个新验收函数没有任何一条门命令会执行它们**（新验收文件在命令中命中 0、散文命中 1）。
  - 其余：浏览器门可以全 skip 变绿（Q17）；机器索引里没有 117→0、按 id 新增失败=0、冻结七文件字节守卫（Q18/Q52）；真正断言 canonical 编码的 `test_s6_commit_window_quota.py` 不在 CODEC 门内（Q35）。
  - **叠加后果（Q50，Pass 8 我亲自跑）**：take/iter_rows/count **完全不咨询 unified↔legacy 分流缝**（seam 调用 0 次、`_apply_plan` 3 次），**而结果全部正确** ⇒ V001 的"结果正确"断言在**统一执行一次都没发生**的情况下也会全绿。照 TEST-COMMANDS.md 逐条跑完 27 个门，可以在**一个新功能都没验证、62% 的历史缺陷修复也没验证、且"统一计划驱动执行"根本没发生**的情况下宣布全绿。

**必须同时说清设计的强项（机器可核，不是文档自述）**：我亲自重算 144 卡基线三元组，**96/96 函数 SHA＋行号＋签名全对、42/42"新增无基线"声明诚实、39/39 文件摘要零漂移**；20 根→27 门→144 卡→64 既有测试文件**结构完全闭合**；**承重四件与 `planning/dag.py` 确实零派发**（最关键的队列承重边界被实际遵守，不只是口头承诺）；§6 裁决的不变量被 **Q003 与 C06 两处独立保护**；冻结签名与基线签名被正确区分，**没有把目标签名谎称为基线事实**；F1–F6/F7b/F7c/F9/§6/8.5/8.6b 在 bbd93ff **均已修好**（我逐条回源码核，非采信历史文档）。**所以"按卡独立编码＋漂移可检测"这条工单前提在摘要层面确实成立**，问题不在机械层。

依据不是文档长度或工作量，而是上述每条都有源码行级证据、可复现的断开调用链，或（**Q01/Q02/Q03/Q27/Q47/Q50/Q56**）**我亲自跑出的实测结果**、（Q53/Q06/基线三元组）**我亲自跑出的机器复算结果**。**七处"需重做"里已有五处（第 1/2/3/7 项及其新增 Q56）具备第一手运行时证据，不再是纸面推断。**设计的方向（单一封存布局驱动真实执行、状态/buffer/效果三分同一 cut、来源等级与运行状态分离、不改业务批次凑绿）是正确的，且 REVIEW-10-PASSES.md 已诚实声明"十遍完整重审尚未完成"。问题集中在三处落差：**契约与真实运行时持有者/权威之间的落差**、**验收命令与验收意图之间的落差**（新旧两侧都断），以及**新机制与已闭合历史保证之间的落差**（Q47 最典型）。

---

## 二、直接回答：不同 agent 现在按卡独立实现，最可能在哪里合不起来

按"最可能先炸"的顺序：

1. **A ↔ L 的封存入口根本没有 RunAction（Pass 8 我已实测，不再是推断）**。L007 `compile_native_topology(action: RunAction, ...)` 被 A006/A007 列为 callee，但 `Dataset.take/iter_rows/count` 只用 `observe_action`。**我在隔离树 bbd93ff 上亲自跑探针：三者进入时 `current_action()` 均为 `None`，而 `current_run()` 已设置**；对照 `dataset.py:1897 materialize` 用 `with action("materialize", …)` 确实开 RunAction。`Dataset.take/iter_rows/take_all` 三个函数都没派发。A 的 agent 会在 executor 层拿到一个不存在的 action，L 的 agent 会按签名要求一个真 action。**C08:216"take/iter 也要走新布局"这条最关键的平台意图，在现卡下无法实现。**好消息是缺口很窄：**有 Run、缺 action**，所以最小修正是让这三个终端也经 `action(...)`，不必新造运行上下文。

2. **A004 的 count 修复路径不成立**。`count()` 确实绑了 action，但 `iter_stream_rows` 在内部又开 `with run.action(declaration)`（stream_delivery.py:41），撞上 `run_context.py:282` 的嵌套守卫。A004 卡写"不调用第二公开 action"——问题不在公开 action，而在 delivery 内部。`stream_delivery.py` 无主无任务，A004 的 callee 只有 A001。**thread map.count() 现在就是抛错的，按卡修完仍然抛错。**

3. **真正的原生关系分支无主**。`local.py:562-569 _apply_plan` 里 `has_relational → execute(self, source, plan)` 不带 execution，10 个调用点（iter_batches/aggregate/schema/size_bytes/write_file/write_datasink 等）都走它。`_apply_plan` 没有任何任务。**即使 take/write 接上了新布局，其余 terminal 仍在执行期自由选 backend，"单一封存布局支配全部执行"不成立。**

4. **M ↔ Prompt journal（跨包权威借用；Pass 9 我已实测，两个断点都真抛）**。M 的 agent 会照 C07 写 `bind_actor` 装 `_BorrowedJournal`、`scope_request` 加两个键，单测用固定 schema 样例能过；但真实 `_exchange` 一调 `journal.reserve` 就撞 `payload.budget is None`——**我在隔离树跑出了逐字错误 `RecoveryContractError: Native request has no execution budget reservation`（`stream_recovery.py:148`）**。若该 agent 改为复用 Prompt 的 `bind_client` 绕开，则撞第二个断点：**用真实 `EmbeddingActor` 跑出 `AttributeError: 'EmbeddingActor' object has no attribute 'coordinator'`（`:206`）**。Prompt 侧的预留权威在 `operator_llm/runtime.py:85`（**全代码库唯一的 `CALL_RESERVATION.set()`**），该文件不在 45 个派发文件里，M 无权改，也没有别的任务负责。**对照臂证明只差预留这一件事**：手动设上 `CALL_RESERVATION` 后同一 reserve 返回 `True`。**两边都"按卡完成"，组合即崩。**

5. **R ↔ stream_barrier / _Stage 数据结构（Pass 9 我已实测）**。R 的 agent 会实现 `StageBufferAdapter.align` 返回完整快照，但 `groups/deadline/prefix_position/next_output_ordinal/row_ordinals` 在 `_make_worker` 里是闭包局部变量，`_Stage.__slots__`（30 项）没有它们——**实测这五个字段连 `setattr` 都直接 `AttributeError`，agent 想自行补都补不上**。`arrive` 每 cut 只调一次 align 并只传一个 worker 的 batch：**concurrency=2 实测 align 调用 1 次、只收到后到者的 batch；`native_snapshot=True` 时 `tokens()` 压成 1，收到的是先到者的（Q56）**。grouped 更彻底：**AST 实测 `grouped_worker` 的 `arrive` 调用点数为 0**，barrier 落进分组路径实测抛 `TypeError`，缺 arrive 则实测只能等 `TimeoutError: Checkpoint coordination deadline exceeded`。R015 卡只说"把 pending/ordinal/计时安全点接 buffer_adapter"，没有授权新增 stage 字段与计数器，`stream_barrier.py` 无主。**concurrency=2 的验收反例必然失败，且失败原因不在任何一个被派发的函数里。**

5b. **R 会拆掉 bbd93ff 刚修好的承重墙（Q47，最危险的一类；Pass 9 我已实测守卫真在生效）**。R006 要删除 `stream_materialize.py:196-202` 的全局 buffered 拒绝，而**那段守卫自己的注释就写着 "graph cut deadlock / unproven group preservation"**——正是历史 F3。**我实测该守卫今天对 4 种 kind 确实拒绝**（concurrent batch c=2、timed batch、grouped、prepend），另 2 种（plain row、fixed batch c=1 无窗）通过；**而第 5 条已实测这 4 种的持有者与对齐路径都不存在**。F3 的修复（`stream_barrier.py:211-214` 把"未确认满额"与"到达 cut"取或）唯一持有者是 `stream_barrier.py`，**该文件未派发**，它还读取 progress 的 **8 个属性**（align/seal/group_ends/run/batch_rows/inputs/ordinal/pending_cut）构成一份无人认领的隐式契约；C05 新增的 `next_output_ordinal` 在设计树只有 2 次命中、**都是 schema 键列表，没有语义定义**。**R 的 agent 按卡"用 adapter 替换全局拒绝"是合规动作，却会把 F3 的修复前提拆掉，而没有任何门会发现。**

5c. **R 与既有交换权威重复造轮子（Q42/Q43）**。`execution/exchange/`（1410 行，**在跑**）与 `execution/contracts/exchange.py`（375 行 K4/K5）已实现架构图 C14 要求的 barrier 消息协议与 InputGate 多输入对齐；设计对 `InputGate`/`DataEnvelope`/`ChannelSequence`/`route_epoch` **全部零命中**，却新建 `stage_buffers.py` 与第三套 ordinal 序号空间。更硬的一处：`reservation.py:255-271 derive_prefetch` 明文规定 `max_pending` 不得低于 `max_pending_floor`，"否则 gate 溢出、**接收方在 cut barrier 到达前死掉**"，而 C06 要求预算"只更紧或相等"、设计对该下限零命中。**两个 agent 各自按卡完成，组合即在恢复最关键时刻崩。**

6. **S ↔ E/N3 的既有实现与既有测试**。S001 要统一状态策略，但 E 已交付的 `4489ba0` 把无钩子实例判为无状态、把 ContextVar 抹成类型名，并写了断言这些行为的测试；C04 规则 1–3 明确禁止这两件事。S013 必须**反转** E 的已交付行为并改/删 E 的测试，设计只说"候选逐函数吸收"，没说哪些断言要反转。同时 C04 的 auto_json 会把 `tests/test_stream_map.py` 里带锁、concurrency=3 的 `Stateful` 判为 unsupported 而拒绝——那是一个**现在通过**的用例，且该文件不在任何门里。**E/S/N3 三个 agent 各自按卡做，会得到互相矛盾的身份规则和一条静默能力退步。**

7. **A ↔ L 的 recovery 枚举**。A001 产出四值 `recovery`（含 `delivery_only`/`attempt_only`），L004/L007 消费的 wire 与 `ExecutionPlacement` 只允许两值。这两个字面量在 144 张卡里**一次都没出现**。A 的 agent 与 L 的 agent 会各自猜映射：要么枚举变死值，要么 take/iter 被封存时抛错（正好打掉设计要恢复的能力），要么被静默强转成 `final_commit`（对纯 delivery 谎称可恢复，C08 明文禁止）。

8. **L ↔ chain.py 的两种封存权威 + 源侧漏 pin**。L030 要求每个 ProgramSlice 都走 `stage_plan_artifact`（cloudpickle）。但同文件 `seal_write_plan_ref` 的注释明确说明：write 路径**故意不 pickle**，因为物化的 PromptActor 带运行时锁不可 pickle，且 pickle 字节跨 attempt 漂移会削弱 gap④。L012（write seal）→ L007 → L030 正好把 write/model 计划送进 pickle 路径。另外 `plan_code_pin` 只遍历 operations，join 的 reducer 在 source 里，改它导入的 helper 不会被 pin 住。**write_lance + 模型算子这条最重要的链，会在封存阶段失败、悄悄丧失 resume 拒绝能力，或对源侧改码完全不设防。**

9. **A 的后端能力声明无生产者**。`available_backends` 只出现在 A001（消费者），144 张卡无任何生产者，默认 `()`；C03 又规定"未提供时只选择普通 local 路线"。**A008 独立实现必然丢掉 Arrow/列合并快路，静默退化为行路径，而且没有任何门会发现。**

10. **W 内部与 W ↔ 后端**。W020 要"按 occurrence 绘制、相同 logical id 保留不同节点"，但冻结的 dag 边只有 logical 端点；W018 要"独立运行状态"，但 `mountLogical` 只导出 `update/select/fit/zoom/dispose`，没有 paint/snapshot 通道；W019 改 `chooseLogicalNode` 用 occurrence 定位，但 `entry.physical.stages` 是按 `logical_node_id` 键的，且真正取源码的 `loadSource`/`chooseNode` 未派发。**前端六个函数都"完成"后，点击一个多次出现的节点仍然拿不到它自己的源码与运行状态。**

11. **全部实现完仍不满足意图的地方（本轮最重要的答案）**：即使上面都修好，验收侧仍会给出**假绿**，而且是新旧两侧同时假绿：
    - **(a) 旧侧——Q53，我亲自复算**：**117 条历史确定性失败中 73 条（62%）所在的 6 个测试文件不被任何一条门命令执行**（`test_stream_map` 57、`test_stream_checkpoint_resume` 8、`test_stream_deduplicate` 3、`test_stream_threads` 2、`test_stream_lance_references` 2、`test_stream_checkpoint_migration` 1）。**清掉这 117 条正是本轮升级的立项理由**，而设计树内不存在 117→0 或按 id 差集的任何机器守卫，`root-map.json` 20 项意图中 `test_` 命中 0。N2 候选正在 dirty 改的 5 个文件里 4 个在此集合 ⇒ 退休 checkpoint 迁移正确性无门可证。
    - **(b) 新侧——Q06**：21 个新验收函数没有被任何命令执行（三遍独立命中）。
    - **(c) 统一执行根本没发生也会绿——Q50，我亲自跑**：take/iter_rows/count **不咨询 unified↔legacy 分流缝**（seam 0 次、`_apply_plan` 3 次），**而结果全部正确**；V001 只断言"结果正确"，所以**统一执行一次都没发生，V001 照样全绿**。这直接违反架构 U5"没有无声退回"。
    - **(d)** 浏览器门可全 skip；RAY 门在指定 env 里没有 ray；门只覆盖 458 个测试文件中的 52 个；DEVELOPMENT 门对开发历史零命中；真正断言 canonical 编码的 s6 文件不在 CODEC 门内；六个字节冻结文件既未枚举也无守卫（Q52）。
    - **所以"27 门全绿"既不等于 R.1.1/R.1.2/R.1.3/R.3.1/R.3.3/R.4.3/R.5.3 的产品意图被验证过，也不等于 P.unified_execution 成立，更不等于"117 条历史失败已清"。**

---

## 三、发现（按工单格式）

### Q01 / P0
- **对应 root 与实际用户后果**：P.recovery、P.business、R.2.3。向量/embedding 节点一旦走原生恢复，第一次请求预留即失败；用户看到的是"支持恢复"的向量流水线在恢复路径上直接报错，或永远退回不可恢复的旧路径。
- **设计文件/任务 ID**：CONTRACTS.md C07；M003/M004/M007/M008；interfaces.pyi `NativeEmbeddingRecovery`。
- **实际源码文件/符号/所审 commit**：`demiflow/operator_llm/stream_recovery.py:110`（`payload.budget = CALL_RESERVATION.get()`）、`:152` 附近（`if item.payload.budget is None: raise RecoveryContractError('Native request has no execution budget reservation')`）、`:20`（ContextVar default=None）；`demiflow/operator_llm/runtime.py:84-85`（唯一 `.set()`，且在 `coordinator.reserve()` 内、需 `_native_group_journal`）；`demiflow/embeddings/runtime.py:167`（`await io(journal.reserve, request)`）、`:78`（`self._runtime = self`，无 coordinator）。bbd93ff。
- **最小反例或具体断开的调用链**：`EmbeddingActor.astart → native_stream_recovery → bind_actor（装 _BorrowedJournal）→ _exchange → journal.reserve → RunPromptJournal._enqueue（budget=None）→ _apply_operation → raise`。M003–M012 无一创建预留；`operator_llm/runtime.py` 不在 45 个派发文件内。
- **问题属于**：源码事实错误 + 漏任务。
- **建议的最小修正**：新增一个 M 叶任务，在 Embedding 原生路径上建立与 Prompt 同权威的执行预算预留（`run.journal.reserve_budgets` + `CALL_RESERVATION`，charges 绑 node 的 max_requests），或为借用包装定义一个不破坏 Prompt 计费的免预留模式并写进 C07。二者择一，不能留给实现者猜。
- **受影响的函数、字段、caller/callee**：`NativeEmbeddingRecovery.__init__/bind_actor`、`EmbeddingActor.astart/_exchange`、`RunPromptJournal._enqueue/_apply_operation`、`CALL_RESERVATION`；caller M006→M004→M003。
- **应新增或保留的验证**：真实本地 HTTP、`batch_size=2 / concurrency=2` 下 reserve 成功；unknown 零重发；重用不重复计费；Prompt 侧计费不回退。
- **是否阻塞**：阻塞编码（跨包权威缺失，独立实现必合不起来）。

### Q02 / P0
- **对应 root 与实际用户后果**：P.recovery、R.5.2。崩溃/取消后 pending 行、分组边界、prepend 位置丢失或重复；用户看到恢复后少行、重行，或分组结果错乱。
- **设计文件/任务 ID**：C05；R001–R005、R007、R010、R015；interfaces.pyi `StageBufferSnapshot`。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/stream.py:98-101`（`__slots__` 无 groups/deadline/prefix 位置/output ordinal）、`:583`（`batch, restored, deadline` 为 `batch_worker` 局部）、`:633/667`（`groups`/`retained`/`deadline` 为 `grouped_worker` 局部）、`:387-408`（prefix `count = 0` 局部，`iter(op.source_dataset.iter_rows())` 每次从头读）、`:679-682`（`batch_ordinal` 仅在 `scope_id is not None` 时自增）。bbd93ff。
- **最小反例**：逐字段核对 `StageBufferSnapshot`——`rows`(row-local) 可由 `native_pending` 提供；`groups`、`flush_remaining_ms`、`prefix_position`、`next_output_ordinal`、`row_ordinals`、`pending_batches.input_ordinals` 在 `_Stage` 上**没有任何持有者或计数器**。`align` 无法取到它们。
- **问题属于**：契约未冻结 + 漏任务（数据结构变更未派发）。
- **建议的最小修正**：先在 C05 冻结每个快照字段的持有者与计数器归属，再给 R015 明确授权把 batch/groups/deadline/prefix 位置/output ordinal 提升为 stage 字段；`_Stage.__slots__` 与 `__init__` 作为同一个装配事务列出全部新增字段，不只 `buffer_adapter`。
- **受影响**：`StageBufferAdapter.align/restore`、`_make_worker`、`_Stage.__init__/__slots__`、`StreamStageProgress.align`。
- **应新增或保留的验证**：`concurrency=2` 崩溃后 pending 全恢复、不重不漏；group timeout/pressure；空上游 prepend 恢复不重读源。
- **是否阻塞**：阻塞编码。

### Q03 / P0
- **对应 root 与实际用户后果**：P.recovery、R.5.2。grouped 阶段无法切 cut；并发 batch 只封存最后一个 worker 的 pending。
- **设计文件/任务 ID**：C05；R006/R007/R015；FILE-OWNERSHIP.md（45 文件）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/stream.py:631-670`（`grouped_worker` 只处理 SENTINEL 与数据行，**没有 `_CheckpointBarrier` 分支**）；`demiflow/execution/stream_barrier.py:37-38`（`tokens = 1 if native_snapshot else stage.concurrency`）、`:57-65`（`arrivals[index] == tokens` 时只调一次 `progress.align(index, self, batch)`，batch 是最后到达者的）。bbd93ff。
- **最小反例**：grouped 阶段收到 barrier → 走 `row_size(row, op.chunk_bytes)` / `group_key(row, op.on)` → 崩或污染分组。concurrency=N 时前 N-1 个 worker 的 pending batch 从未进入 align。
- **问题属于**：漏任务 + 源码事实错误。
- **建议的最小修正**：给 `stream_barrier.py` 指定文件负责人并派发 `arrive` 的聚合任务（收集全部 worker pending 后再 align）；R015 为 grouped_worker 增 barrier 对齐分支；`pending_batches` 的复数语义与聚合点写进 C05。
- **受影响**：`_CheckpointBarrier.arrive/tokens`、`StreamStageProgress.align`（签名是否需扩）、`grouped_worker`、`StageBufferAdapter.align`。
- **应新增或保留的验证**：concurrency=2 各 worker batch 均封存；grouped 阶段跨 group 超时后恢复不重。
- **是否阻塞**：阻塞编码与装配。

### Q04 / ~~P0~~ → **已撤回（Pass 10 自我更正）；残留部分见 Q57 / P1**

> **本条原判为 P0"契约矛盾＋漏任务，没有任何卡负责调和"。Pass 10 逐张读 S001/S004/S005/S009/S010/S011 与 C04 全文后，确认该判断错误，现予撤回。**
> 撤回理由（两条，均已逐字核对）：
> 1. **C04 明文已经把规则定死**，不是留白：「默认状态所有权明确固定：普通function为none；**factory或auto_json如果调用者尚未声明状态所有权，采取shared_colocated，不把未知当作无状态**。」同段还明文「**Counter 必须能跨 checkpoint 恢复计数**」。即"声明期即预置 shared_colocated"正是我原先建议的最小修正，**设计已经写了**。
> 2. **调和任务存在且齐全**：S001 `classify_operator_state(target, *, phase, ownership='none')` 是唯一策略产出者，`phase` 显式区分 `declaration`/`instance`，`StatePolicy.mode` 含 `'factory'`；**S004（precheck）、S009（`_adapter_present`）、S010（`compile_operator_spec`）、S011（`unified_operation_traits`）四个消费点全部在 callee 里列了 S001**。S011 的行为原文就是"实例和类的 state ownership 同源"。我原先"traits 无主"的说法不成立——`planning/traits.py` 在 FILE-OWNERSHIP.md:44 属 S，由 S011 派发。
>
> **我的错误成因**：只读了 `traits.py:70-73` 的基线行为与 C04:98 那一句禁令，**没读 C04 同节的默认所有权段落，也没读 S011 卡**，就把"基线现状"当成了"设计目标"。这是把基线事实误当设计缺陷，属本次 Review 最应自查的一类错误。
>
> **仍然成立的部分**：P4-06/Q05（auto_json 并发=1 准入拒绝零实现者、`OperatorTraits` 无 concurrency 字段）**不受本次撤回影响**，另条独立记录。**新发现的残留风险另立 Q57（P1）**，已运行复现。

- **Pass 10 顺带实测确认的基线事实（供 S 的 agent 参考，不是设计缺陷）**：同一个无钩子有状态业务类，**声明为类工厂被接受、声明为实例被拒绝**——`Counter` 类工厂 `precheck_operator_state` 返回 `False` 但不抛（`operator_precheck.py:24`／`contracts/operator.py:114` 的 `inspect.isclass` 短路跳过了实例状态守卫）；`Counter()` 实例则抛 `STATE_ADAPTER_MISSING: callable instance carries state without a state adapter`。`HookedCounter` 类工厂得 `shared_colocated/True`。另实测 **`OperatorTraits` 11 个字段里没有 `adapter_present`**（它只由 `_adapter_present(target)` 派生并进 compiled spec 的 `state` 字典），与 C04"compile_operator_spec 的 state.adapter_present"表述一致，**不是缺陷**。探针：`/tmp/demiflow-qwen-pass10/probe_q04.py`。

### Q57 / P1（Pass 10 新增，替代 Q04 的残留部分，已运行复现）
- **对应 root 与实际用户后果**：P.correctness、R.2.3。C04 把 factory 的 ownership 定为 `shared_colocated`，而**未派发的** `validate_operator_spec:182-183` 与已派发的 `compile_operator_spec:111-113` 都要求 `ownership != 'none'` 时 `adapter_present` 必须为 `True`。S009 又明文"不构造 class"，因此 `_adapter_present` **无法核实**未来实例是否 auto_json 安全。若 S009 的 agent 按字面"adapter_available＝已验证可用"实现而返回 `False`，则**每一个无钩子类工厂在封存期即被拒**——正好打掉用户最关心的"普通类实例真崩溃恢复"。
- **设计文件/任务 ID**：C04（默认所有权段 + `precheck_operator_state … class 尚未构造时允许 deferred`）；S009（`_adapter_present`）、S011（`unified_operation_traits`）、S010（`compile_operator_spec`）；`validate_operator_spec` **未派发**。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/contracts/operator.py:111-113`（compile 内的同一规则）、`:182-183`（`validate_operator_spec`，AST 确认其范围为 152-201，**FUNCTION-TASKS.md 只派了 `_adapter_present` 与 `compile_operator_spec` 两个符号**）、`:67-73`（`_adapter_present` 只探两个方法）。bbd93ff。
- **最小反例（实测，真 `compile_operator_spec`）**：

  | target | traits ownership | 实测结果 |
  |---|---|---|
  | `Counter`（无钩子） | `none`（基线现状） | **COMPILED**，`state={'ownership':'none','adapter_present':False}` |
  | `Counter`（无钩子） | `shared_colocated`（**C04 强制值**） | **REJECTED `STATE_ADAPTER_MISSING: stateful operator lacks snapshot_state/restore_state`** |
  | `HookedCounter` | `shared_colocated` | COMPILED，`state={'ownership':'shared_colocated','adapter_present':True}` |

  即**只做 S011（traits 升为 shared_colocated）而 S009 仍返回诚实的 False，组合即拒**。探针 `/tmp/demiflow-qwen-pass10/probe_q04b.py`（类放在可导入模块 `q04_counter.py`，绕开 `_callable_path` 对 `__main__` 的正当拒绝）。
- **问题属于**：契约未冻结（"deferred 的 adapter_present 取值"未写明）。**不是漏任务**——四个消费点都有主。
- **建议的最小修正**：在 C04 增一句——**`phase='declaration'` 且 `mode='factory'` 时 `adapter_present` 取"延后可用"即 `True`，真实可用性在实例检查点必验**；并在 S009 卡的行为栏写明这一点（现文"不再只探两个方法而忽略 auto_json；不构造 class"不足以推出该取值）。另建议把 `validate_operator_spec` 列为 S 的 verify_preserve 对端，明示 `:182` 规则本轮不变。
- **受影响**：`_adapter_present`、`compile_operator_spec`、`unified_operation_traits`、`precheck_operator_state`、`validate_operator_spec`（未派发但是契约对端）。
- **应新增或保留的验证**：无钩子 `Counter` **类工厂**在声明期通过封存、在 `_MapWorker` 构造实例后完成 validate/register、并跨 checkpoint 恢复计数（C04 已明文要求）；不支持形态（含锁/句柄）在**实例检查点**拒绝而非声明期。
- **是否阻塞**：不阻塞编码起步，但**阻塞 S 包内部的组合正确性**；两句话即可冻结，成本极低。

### Q05 / P0
- **对应 root 与实际用户后果**：P.correctness（能力不退步）、R.5.2。现通过的带锁类 actor 与 concurrency=3 用例会被新规则拒绝，用户既有流水线直接不可用。
- **设计文件/任务 ID**：C04:98（auto_json 同 owner 并发必须为 1，更高并发准入明确拒绝）、C04:96（含锁实例拒绝）。
- **实际源码文件/符号/所审 commit**：`tests/test_stream_map.py:91-110`（`@parametrize('scope,instances',[('stage',1),('worker',3)])`，`class Stateful` 持 `self.lock = threading.Lock()`、无 snapshot/restore）；`tests/test_native_barrier.py:83` 附近（`SharedCounter` 有 hook、concurrency=3 共享）。bbd93ff。
- **最小反例**：`Stateful` 含锁 → auto_json 判 unsupported → 准入拒绝（E 现放行）；即使无锁，`worker/3` 也被"并发必须为 1"误伤。`tests/test_stream_map.py` **不在 27 个门任何一个的 required_existing_files 里**，退步不会被发现。
- **问题属于**：架构过度 + 验收缺口。
- **建议的最小修正**：去掉"并发必须为 1"的硬拒，改为复用既有 cut/owner-gate 共享对齐（`SharedCounter` 已证 concurrency=3 可行）；有 hook 的 actor 不受 auto_json 限制；把 `test_stream_map.py`、`test_native_barrier.py` 纳入 STATE/DELIVERY 门。
- **受影响**：`classify_operator_state`、`precheck_operator_state`、`admit_action_plan`。
- **应新增或保留的验证**：`Stateful(stage,1)/(worker,3)` 与 `SharedCounter` 回归必须保留原断言。
- **是否阻塞**：阻塞装配与最终验收。

### Q06 / P0
- **对应 root 与实际用户后果**：全部 20 个 root。新功能可以在零验证的情况下被宣布通过。
- **设计文件/任务 ID**：TEST-COMMANDS.md 全部 27 条命令；gates.json 的 `new_acceptance_tests`；V001–V021。
- **实际源码文件/符号/所审 commit**：`tests/test_upgrade_v2_acceptance.py` 在 bbd93ff **不存在**；TEST-COMMANDS.md 只在第 7 行提到它，**没有任何一条命令包含该文件或 `-k` 选择**。
- **最小反例**：21 个新验收函数（signed-zero roundtrip、nested program code pin、actor counter crash resume、embedding batch recovery、write create recovery、workbench history end-to-end…）合计 0 条执行命令。逐条跑完 27 门 = 只跑旧回归。
- **问题属于**：验收缺口。
- **建议的最小修正**：每个门的命令追加 `tests/test_upgrade_v2_acceptance.py` 并用 `-k` 选中该门的 `new_acceptance_tests`；在 gates.json 增加 `min_passed` 与 `forbid_skip` 字段；先派发一个 V 装配任务负责该文件的 imports/fixture。
- **受影响**：全部 V 任务、gates.json、TEST-COMMANDS.md。
- **应新增或保留的验证**：门命令必须实际执行到每个 new_acceptance_test 函数名，并在回执里记录 passed 计数。
- **是否阻塞**：阻塞最终验收。

### Q07 / P0
- **对应 root 与实际用户后果**：R.2.1（真实图、准确来源）。同一函数两次出现无法在图上真正分开；若强行加 occurrence 边则改 dag_id，废掉全部历史封存。
- **设计文件/任务 ID**：C09/C12 的 occurrence 映射补充；W020（`mountLogical.update`）、W021（`mountLogical.select`）。
- **实际源码文件/符号/所审 commit**：`demiflow/planning/dag.py:296-297`（`set(endpoint) != {"logical_node_id","port"}` → raise，**冻结文件**）；`demiflow/workbench/web/workbench-graph.js:67`（`update(dag,nodeMap,labels,selectedId,basis)`，冻结签名）、`:71-72`（按 logical id 归并 occurrences）、`:88`（`×N 次出现` badge）、`:99`（只导出 update/select/fit/zoom/dispose）。bbd93ff。
- **最小反例**：W020 卡要求"按 occurrence_id 绘制；相同 logical id 多次出现保留不同节点"，但 `dag.edges` 端点只有 logical id，无法为两个 occurrence 节点分别连边；`dag_id_for` 绑内容，加 occurrence 边即改 dag_id。
- **问题属于**：契约矛盾 + 源码事实错误。
- **建议的最小修正**：把 W020 的目标改为"保留 logical 顶点布局，occurrence 作为节点内二级选择"（现有 picker 与 `chooseLogicalNode` 的 group 分支已具雏形），并在 C09 明确 occurrence 不进入 dag 边与 dag_id。
- **受影响**：`mountLogical.update/select`、`renderLogical`、`dag.edges`、`_bridge_node_map`。
- **应新增或保留的验证**：双 occurrence 节点的边归属反例；历史 dag_id 不变断言。
- **是否阻塞**：阻塞编码。

### Q08 / P1
- **对应 root 与实际用户后果**：P.unified_execution、R.2.2、R.5.1。take/iter 的封存路线要么报错要么谎称可恢复。
- **设计文件/任务 ID**：C03（recovery 四值）vs C08（executions[].recovery 两值）；interfaces.pyi `ActionDecision` / `ExecutionPlacement`；A001、L004、L007。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/plan_interpreter.py:32`（`SUPPORTED_SLICES`）、`:41-42`（v1 键集）、`:230-234`（schema 硬校）。bbd93ff。
- **最小反例**：`grep -l "delivery_only\|attempt_only" tasks/*.md` → **0 命中**。A001 产出四值，L007/L004 消费两值，映射未定义。
- **问题属于**：契约矛盾。
- **建议的最小修正**：在 C03/C08 之间写死映射表（pull→attempt_only 时是否封存、stream_delivery→delivery_only 时是否封存、若封存则 wire 用哪个值），或把 wire 枚举扩为四值并同步 interfaces.pyi 与 L001 的键集校验。
- **受影响**：`admit_action_plan`、`compile_native_topology`、`seal_execution_bindings`、`_validate_bindings`、`ExecutionPlacement.recovery`。
- **应新增或保留的验证**：thread map.take(1) 与纯 delivery 各一条，断言不声称跨 attempt 可恢复。
- **是否阻塞**：阻塞编码。

### Q09 / P1
- **对应 root 与实际用户后果**：P.recovery、R.2.2、R.5.3。write_lance + 模型算子在封存阶段失败，或 pickle 漂移导致 resume 不再拒绝改码。
- **设计文件/任务 ID**：C08:210；L030（`stage_native_programs`）、L012（`seal_write_topology`）、L007。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/chain.py:849-870`（`stage_plan_artifact` 用 cloudpickle，pin_key = `action_id + "-code-pin-" + sha256(pickle)`）、`:873-905`（`seal_write_plan_ref` 注释明确：物化 PromptActor 带运行时锁不可 pickle、字节跨 attempt 漂移会削弱 gap④，故 write 路径只用 code-pin、pin_key 固定 per-action）、`:1233`（write seam 实际调 `seal_write_plan_ref`）。bbd93ff。
- **最小反例**：L012→L007→L030 对含模型 actor 的 write 子计划调 `stage_plan_artifact` → cloudpickle 抛错；即使成功，pin_key 随 pickle 漂移 → `_seal_plan_code` 找不到旧 pin → 静默重绑。全部 144 张卡**无一提到 `seal_write_plan_ref`**。
- **问题属于**：源码事实错误 + 契约矛盾。
- **建议的最小修正**：在 C08 区分两类 ProgramRef 封存权威（driver 原生执行用 code-pin-only 的 `seal_write_plan_ref` 语义；worker pickle 投递保留 `stage_plan_artifact`），并让 L030 按 placement 选择；pin_key 必须 per-action/per-input_path 稳定，不含 pickle 摘要。
- **受影响**：`stage_native_programs`、`seal_write_topology`、`compile_native_topology`、`_seal_plan_code`、`ProgramRef.code_pin_digest`。
- **应新增或保留的验证**：join 右子计划改 UDF 代码后 resume 必须拒绝；含模型 actor 的 write 封存成功且跨 attempt 摘要稳定。
- **是否阻塞**：阻塞编码与装配。

### Q10 / P1
- **对应 root 与实际用户后果**：P.recovery、R.2.3。ACK 超时后迟到回执可能推动错误的 cut；异常类型不一致导致上游捕获不到。
- **设计文件/任务 ID**：C08:256（"超时抛 TimeoutError"）vs L034/L035 异常栏（只有 PlanContractError/RecoveryContractError）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/worker/execution.py:114-140`（sync `_request_commit`，`future.result()` 无期限；`_pending` 只在成功路径清空）、`:142-146`（`deliver_receipt(receipt)` **不按 operation_id 键控**，直接 resolve 当前 `_pending`）；`demiflow/execution/worker/graph_execution.py:218-247`（graph 侧按 operation_id 键控，安全）；`demiflow/execution/stream_barrier.py:83`（已支持 isawaitable）；`demiflow/execution/worker/progress.py:5`（docstring 已写 "request_commit coroutine"）、`:84`（lambda 包装）。bbd93ff。
- **最小反例**：L034 加 120s deadline 后超时抛出，`_pending` 仍指向旧 future；若执行体继续到下一个 cut，`_pending` 被覆盖，cut N 的迟到 ACK 会 resolve cut N+1 的 future。`deliver_receipt` 不在任何任务里，L034 又被禁止改同文件其他函数。
- **问题属于**：契约矛盾 + 漏任务。
- **建议的最小修正**：L034/L035 异常栏补 `TimeoutError`（与 C08 统一）；在 L034 卡内明确要求超时路径清空/隔离 `_pending` 并按 operation_id+sequence 匹配迟到 ACK；或给 `deliver_receipt` 派发窄任务。async 化本身已核可行（`concurrent.futures.Future` + 唯一调用点 + barrier 已 await）。
- **受影响**：`WorkerChainExecutor._request_commit/deliver_receipt`、`GraphWorkerExecutor._request_contribution`、`WorkerStreamProgress.seal`。
- **应新增或保留的验证**：event-loop heartbeat 未冻结；迟到 ACK 不推动新 cut；超时后 unknown 保留、不自动重发。
- **是否阻塞**：阻塞编码。

### Q11 / P1
- **对应 root 与实际用户后果**：P.correctness、R.5.2。auto_json 的并发准入规则无人实现，状态可能在并发下被错误快照。
- **设计文件/任务 ID**：C04:98；S001（签名无 concurrency 入参）、S004/S005、A001/A002。
- **实际源码文件/符号/所审 commit**：`grep -n "concurrency\|并发" tasks/S0*.md tasks/S01*.md` → **0 命中**；`grep -l shared_colocated tasks/*.md` → 只有 S001。A001 的 callee_tasks 为空，A002 只 callee `operation_capability`。bbd93ff。
- **最小反例**：`classify_operator_state(target,*,phase,ownership)` 看不到计划的 concurrency；唯一遍历真实 plan callable 的 S005 卡未提并发；A 侧不调用 classify。规则无实现者。
- **问题属于**：漏任务。
- **建议的最小修正**：与 Q05 一并处理——若保留并发约束，把它明确派给 S005（能看到 MapStreamOptions）或 A001，并写进卡的 behavior 与 callee；若按 Q05 建议去掉硬拒，则从 C04 删除该句。
- **受影响**：`classify_operator_state`、`precheck_plan_state`、`admit_action_plan`。
- **应新增或保留的验证**：若保留约束，需一条 concurrency>1 的准入拒绝反例；否则需一条 concurrency=3 共享 actor 的通过用例。
- **是否阻塞**：阻塞编码。

### Q12 / P1
- **对应 root 与实际用户后果**：P.correctness、R.5.3（历史身份保护）。不同 ContextVar 业务值/不同 initial_state 被抹平 → 身份碰撞 → 恢复读到别的配置的结果。
- **设计文件/任务 ID**：C04 身份规则 1–3；S013（`recovery_identity.describe`）。
- **实际源码文件/符号/所审 commit**：e_dfamily `4489ba0` 的 `tests/test_d2_recovery_identity_counterexamples.py:65`、`:88`（断言无钩子 callable `"initial_state" not in ida/id1`）、`:126-128`（断言 ContextVar → `{"runtime_resource":"_contextvars.ContextVar"}`，值被抹掉）。C04:107-109 明确禁止这两件事。
- **最小反例**：S013 按 C04 实现 → E 的 D2 测试必失败；E 的 agent 按已交付测试维护 → S013 必失败。设计只说"候选逐函数吸收"，未指出哪些断言要反转。
- **问题属于**：在途漂移 + 契约矛盾。
- **建议的最小修正**：在 S013 卡里明确"反转 D2 的无钩子 initial_state 抹除与 ContextVar 类型化"，并同卡派发对应测试断言的修改；禁止两套相反断言并存。
- **受影响**：`describe`、`plan_code_pin`、E 的 D2 测试文件。
- **应新增或保留的验证**：threshold 0.5/0.9、不同 ContextVar 值、不同 initial_state 身份不碰撞；跨进程等值身份稳定。
- **是否阻塞**：阻塞装配。

### Q13 / P1
- **对应 root 与实际用户后果**：P.recovery。S 的补丁基于过期基线，与 E 已交付实现冲突，合流时整包回退风险。
- **设计文件/任务 ID**：baseline.json；S006/S007/S008/S016/S013。
- **实际源码文件/符号/所审 commit**：baseline.json 把 `operator_state.py`/`recovery_identity.py` 绑到 bbd93ff（哈希已核匹配）；E 已交付 `4489ba0`，其 `operator_state.py:51-70` 已改为豁免无钩子实例且 `register` 对无钩子实例 early-return（→ owners 空、snapshot 空、resume 计数归零）。
- **最小反例**：S007 按 bbd93ff 的"validate 抛错"版本写补丁，无法直接落到 E 的"early-return"版本；设计未给重基指令。
- **问题属于**：在途漂移。
- **建议的最小修正**：把 S 的状态卡基线重基到 `4489ba0`（或明确"以 bbd93ff 为编码基线、合流时由 S 装配负责人逐函数吸收 E 补丁"并列出吸收清单）；S007 必须把 early-return 改为 auto_json 捕获。
- **受影响**：`OperatorStates.validate/register/snapshot/__init__`、`describe`。
- **应新增或保留的验证**：保留 E 的类豁免；新增无钩子实例 resume 计数不归零的反例。
- **是否阻塞**：阻塞编码与装配。

### Q14 / P1
- **对应 root 与实际用户后果**：R.2.1、R.2.3。逻辑图上永远看不到运行状态，用户无法区分"封存了"与"跑成功了"。
- **设计文件/任务 ID**：C09（"来源等级与执行状态是两个不同字段"）；W018（renderLogical）、W020。
- **实际源码文件/符号/所审 commit**：`workbench-graph.js:99`（mountLogical 只导出 update/select/fit/zoom/dispose，**无 paint**）、`:67`（update 签名冻结、无 snapshot 参数）；`workbench.js:857` 附近只有物理图 `graphView.paint`。bbd93ff。
- **最小反例**：W018 要"显示独立运行状态"，但没有任何参数或导出函数能把 observation/receipt 送进逻辑图。
- **问题属于**：漏任务。
- **建议的最小修正**：派发 `mountLogical.paint(snapshot)`（或等价窄接口）并给 W018 明确 callee；若不做，则从 W018 卡删除该句并在 C09 记录为已知边界。
- **受影响**：`mountLogical`、`renderLogical`、`logicalEntry`。
- **应新增或保留的验证**：图上 running/failed 与 dag_id 分离的断言；有 F1/dag_id 但无 receipt 时不显示成功。
- **是否阻塞**：阻塞编码。

### Q15 / P1
- **对应 root 与实际用户后果**：R.2.1。点击一个多次出现的逻辑节点，拿不到它自己的源码/参数；重绘后高亮回退。
- **设计文件/任务 ID**：C09/C12；W006（`bound_source`）、W019（`chooseLogicalNode`）。
- **实际源码文件/符号/所审 commit**：`workbench.js:502`（`loadSource` 只发 `graph_binding/node_id/kind`，从不发 logical_node_id/occurrence_id）、`:317-320`（reportId 不在当前图时 `locateNode(...)` 后 **return**，`logicalSelected` 与 `logicalView.select` 未更新）；`workbench/graph/model.py:409` 附近 `_resolve_bridge_node`；`workbench/graph/logical.py`、`source_view.py`、`parameters.py` **均未派发**。bbd93ff。
- **最小反例**：W006 在后端加 occurrence 精确分支，但前端 `loadSource` 从不传 occurrence → 新分支无消费者；W019 改完 `chooseLogicalNode`，`chooseNode/loadSource` 仍按 report node 走旧路。
- **问题属于**：漏任务。
- **建议的最小修正**：派发 `loadSource` 与 `chooseNode` 的窄任务（传 occurrence/logical 身份、修早退分支），并给 `logical.py`/`source_view.py` 指定文件负责人。
- **受影响**：`chooseLogicalNode`、`loadSource`、`chooseNode`、`bound_source`、`_resolve_bridge_node`。
- **应新增或保留的验证**：occurrence 点击后源码身份一致；跨页 locate 后高亮不回退。
- **是否阻塞**：阻塞装配。

### Q16 / P1
- **对应 root 与实际用户后果**：R.2.1。切到 occurrence 键后，分区数/worker 角色/预算等物理事实全部读不到。
- **设计文件/任务 ID**：C09/C12（"DOM/select/onSelect 键统一 occurrence_id"）；W019、W015。
- **实际源码文件/符号/所审 commit**：`demiflow/workbench/runs/facts.py:255`（`stages[stage['logical_node_id']] = ...`）、`:271`（`_bridge_node_map(entries)` 基线无 key_kind 参数）；`workbench.js:328`（`entry.physical.stages[id]`，id 为 logical）。bbd93ff。
- **最小反例**：把选择键换成 occurrence_id 后，`physical.stages[occurrence_id]` 恒为 undefined。
- **问题属于**：契约矛盾。
- **建议的最小修正**：C09 补一句"physical.stages 仍按 logical_node_id 键；occurrence 只用于 bridge/观测/选择"，或在 W014/W015 派发 stages 的 occurrence 映射（不改物理计划权威）。
- **受影响**：`_physical_summary`、`_sealed_entry`、`_bridge_node_map`、`chooseLogicalNode`。
- **应新增或保留的验证**：分区数/worker 角色/budgets 非空断言。
- **是否阻塞**：阻塞装配。

### Q17 / P1
- **对应 root 与实际用户后果**：R.1.1–R.1.3、R.3.x、R.4.x、R.5.3、P.unified_execution。产品意图的门可以全 skip 变绿；Ray 路线无法验证。
- **设计文件/任务 ID**：gates.json `require_real_environment`；TEST-COMMANDS.md 的 DEMO/CASE/NOTEBOOK/WORKBENCH/DEVELOPMENT/RAY/BOUNDS。
- **实际源码文件/符号/所审 commit**：`tests/test_forge_markdown_reading.py:60-65`（`pytest.importorskip('playwright.sync_api')` + Chromium 不存在则 `pytest.skip`）。环境探针：`/yzp/zhaozy/yangzepeng/0905/env/bin/python` 有 playwright/pytest/datafusion/lance/pyarrow、**无 ray**；`demiflow-env/bin/python` 无 playwright/pytest；系统 python3 全缺。
- **最小反例**：门命令没有 `-rs`、没有 `--strict-markers`、没有最小 passed 数、没有禁止 skip；gates.json 也没有对应字段。全 skip → exit 0 → 门"通过"。
- **问题属于**：验收缺口。
- **建议的最小修正**：门命令统一加 `-rs`，gates.json 增 `min_passed`/`forbid_skip`/`environment` 字段；RAY 门显式指向有 ray 的环境（`env-ray`），否则记 blocked 而不是 passed。
- **受影响**：7 个 require_real_environment 门、root-map 的 R.1/R.3/R.4/R.5.3 与 P.unified_execution。
- **应新增或保留的验证**：每个门记录 passed/skipped/blocked 计数与环境指纹。
- **是否阻塞**：阻塞最终验收。

### Q18 / P1
- **对应 root 与实际用户后果**：R.5.1、R.5.3、P.correctness。既有 117 个失败是否真清零、承重文件是否被改动，都没有机器守卫。
- **设计文件/任务 ID**：gates.json、TEST-COMMANDS.md、baseline.json、tools/check_design.py。
- **实际源码文件/符号/所审 commit**：`grep -c "frozen\|117\|7fe1f565\|id-diff" gates.json TEST-COMMANDS.md` → **0/0**；baseline.json 的 39 个文件**不含**冻结七文件中的六个（raw_fetch/queue_execution/queue_sinks/queue_source/planning/dag/resources/contracts），只含 recovery_identity.py；check_design.py 的 `source_baseline` 只校这 39 个。
- **最小反例**：实现期改动了 `planning/dag.py` 或 `queue_sinks.py`，没有任何门或校验会发现；117 个失败按 id 的新增/消除差集也没有命令。
- **问题属于**：验收缺口。
- **建议的最小修正**：把在途已用的三条硬纪律写进机器索引——冻结七文件字节/聚合摘要守卫、11 文件范围内按 id 的"新增失败=0"差集、117→0 的收口计数；baseline.json 或独立 guard 文件覆盖六冻结文件。
- **受影响**：check_design.py、gates.json、全部实现任务的提交前检查。
- **应新增或保留的验证**：每次合流跑守卫并记录摘要；保留 155 通过的 identity/seal 守卫作为独立门。
- **是否阻塞**：阻塞最终验收。

### Q19 / P1
- **对应 root 与实际用户后果**：R.2.1、R.5.3。工作台"识别 v1/v2"没有 v2 生产者，历史载体读取分支无对象。
- **设计文件/任务 ID**：C09；W001（`read_sealed_plan`）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/chain.py:41`（`SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"`）、`:973`（`seal_plan_facts` 写 v1）；`seal_plan_facts` **不在派发任务里**；全设计没有 F1 v2 的 schema/键集。bbd93ff。
- **最小反例**：W001 卡"复用中立 reader 识别 v1/v2"，但 `read_sealed_plan` 读的是 F1 facts（v1），C08 的 v2 是另一份文档 `execution_bindings.v2`；两者被混为一谈。
- **问题属于**：契约矛盾。
- **建议的最小修正**：明确 W001 只读 F1 v1（降为 verify_preserve），或先派发 F1 v2 写入任务并冻结其键集；把"bindings v2 的读取"归到 L008/W001 的哪一侧写清楚。
- **受影响**：`read_sealed_plan`、`seal_plan_facts`、`_carrier`、`facts.SCHEMA`。
- **应新增或保留的验证**：v2 载体缺失/损坏分别显示；历史 v1 不自动升级。
- **是否阻塞**：阻塞编码。

### Q20 / P2
- **对应 root 与实际用户后果**：R.1.1、R.3.1、R.3.3、R.4.3、R.5.3。多个产品叶只有门名没有实际验证对象。
- **设计文件/任务 ID**：gates.json、TEST-COMMANDS.md、root-map.json；V015/V017。
- **实际源码文件/符号/所审 commit**：门覆盖 **52 / 458** 个测试文件（已核计数）；DEVELOPMENT 门的两文件对 conversation/chat/history 关键词零命中；`test_forge_development_chat*`、`test_forge_analysis_catalog/source_query`、`test_forge_outputs_browser/media`、`test_forge_analysis_case_review/case_pages` 不在任何门。（关键词命中统计与文件清单由 W/V 子审查提供，我核了 52/458 与 DEVELOPMENT 门文件清单。）
- **最小反例**：R.3.3 要求逐类型实测 11 类富输出，媒体门只覆盖 png/wav；R.1.1 要求旧开发历史完整，DEVELOPMENT 门无对应文件。
- **问题属于**：验收缺口。
- **建议的最小修正**：把上述文件纳入对应门；V015/V017 卡写明逐类型/逐历史断言清单。按全局精力分级，R.3.3 的冷门类型可后延，但必须显式记为未覆盖而不是通过。
- **受影响**：CASE/NOTEBOOK/DEVELOPMENT/WORKBENCH 门、V015/V017。
- **应新增或保留的验证**：逐类型输出实测；开发历史 conversation/chat 保全。
- **是否阻塞**：阻塞最终验收（不阻塞编码）。

### Q21 / P2
- **对应 root 与实际用户后果**：P.business。M009 按卡描述传错类型 → 构造器 TypeError。
- **设计文件/任务 ID**：M009 输入表 `request_gate`（"既有 request_policy 准入对象"）。
- **实际源码文件/符号/所审 commit**：`demiflow/embeddings/runtime.py:66-67`（`if request_gate is not None and not isinstance(request_gate, RequestGate): raise TypeError`）、`:76`（默认 `RequestGate(concurrency)`）。bbd93ff。
- **最小反例**：request_policy 是 Prompt 侧声明式对象，与 RequestGate 不是同一类型。
- **问题属于**：源码事实错误（文档）。
- **建议的最小修正**：M009 输入表改为"RequestGate/AdaptiveRequestGate 或 None"。
- **受影响**：`EmbeddingActor.__init__`。
- **应新增或保留的验证**：构造器类型拒绝反例保留。
- **是否阻塞**：不阻塞（装配前更正即可）。

### Q22 / P2
- **对应 root 与实际用户后果**：全部 root 的验收可执行性。
- **设计文件/任务 ID**：FILE-OWNERSHIP.md（V 装配）；V001–V021。
- **实际源码文件/符号/所审 commit**：`tests/conftest.py` **不存在**；fixture 散在 `test_c_write_seam.py:451`（pidfds）、`test_forge_markdown_reading.py:60`（browser）、test_forge_workspace（isolated/demo）。21 个任务共用一个文件，imports/fixture 无任务 ID。
- **最小反例**：V013/V016 需完整 playwright journey、V018 需基线-候选双跑峰值/吞吐、V019 需 ray；单个测试函数承载不了，且没有装配任务先建文件。
- **问题属于**：漏任务 + 架构过度（粒度）。
- **建议的最小修正**：先派一个 V 装配任务（imports/fixture/文件骨架）；V013 拆三段、V018 拆边界与性能、V019 显式指定 env-ray。
- **受影响**：全部 V。
- **应新增或保留的验证**：装配后每个 V 函数可独立收集与执行。
- **是否阻塞**：阻塞验收（不阻塞其他包编码）。

### Q23 / P2
- **对应 root 与实际用户后果**：P.correctness（身份保真）。
- **设计文件/任务 ID**：INTEGRATION-ACCEPTANCE.md:85 声称 7 项后延"全部提前分配"。
- **实际源码文件/符号/所审 commit**：`grep -l "NaN\|non-finite" tasks/*.md` → 只有 Q001，且其文本是"使用 IEEE 字节签名区别 signed-zero/NaN"（**编码侧**）；S013/V003 无 NaN/非有限输入身份内容。
- **最小反例**：后延项"非有限输入身份"没有身份侧任务；VectorSearch 适配器（S014/S015）、buffer 适配器（R001–R005）、prepend（R001/R015/V008）确有归属。
- **问题属于**：漏任务（轻）。
- **建议的最小修正**：给 S013 或 V003 补一句非有限输入身份断言，或把该项显式记为仍未分配。
- **受影响**：`describe`、V003。
- **应新增或保留的验证**：NaN/Inf 输入的身份不碰撞且跨进程稳定。
- **是否阻塞**：不阻塞核心交付。

### Q24 / P2
- **对应 root 与实际用户后果**：P.correctness、R.5.1。在途漂移导致同一函数两份实现。
- **设计文件/任务 ID**：baseline.json；F002、S011、Q006。
- **实际源码文件/符号/所审 commit**：n3_resource `bbd93ff` clean **零提交零改动**（已核）；c_budget `90d7ee4` dirty 1 = `demiflow/execution/plan_facts.py` +56/-11（已核 diff --stat），即 F002 `operation_facts` 的基线已漂移。
- **最小反例**：F002 按 bbd93ff 编码，C 的 dirty 改动涉及 VectorSearch/group_batches/model_map 的事实与身份，合流时两边都改同一函数。
- **问题属于**：在途漂移。
- **建议的最小修正**：F002 重基或明确由 C 的装配负责人吸收；N3 的域（traits/precheck/stream_resources）要么派发要么显式记为未开工。
- **受影响**：`operation_facts`、`unified_operation_traits`、`precheck_operator_state`、`stream_limits`。
- **应新增或保留的验证**：合流前跑 FACTS/STATE/BOUNDS 门并记录基线 commit。
- **是否阻塞**：阻塞装配。

### Q25 / P2
- **对应 root 与实际用户后果**：R.5.1（错误权威一致）。
- **设计文件/任务 ID**：C10:234。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/contracts/errors.py:36`（`class ProtocolError(RecoveryContractError)`）；`demiflow/execution/operator_precheck.py:13,22,27`（实际抛 ProtocolError）；`plan_interpreter.py:55`（PlanContractError 是另一个类）。S001 卡正确写了 ProtocolError。
- **最小反例**：C10 说"PlanContractError 的 reason…S 使用 STATE_ADAPTER_MISSING"，与源码和 S 卡不一致。
- **问题属于**：文档措辞。
- **建议的最小修正**：C10 改为"S 使用 ProtocolError(STATE_ADAPTER_MISSING/INVALID_SPEC)"。
- **受影响**：C10 文本。
- **应新增或保留的验证**：无需新增。
- **是否阻塞**：不阻塞。

### Q26 / P2
- **对应 root 与实际用户后果**：R.2.1、R.5.3。历史读图若被要求做环境级 verify_seal，环境变更后历史不可读或读取成本失控。
- **设计文件/任务 ID**：W002/W004/W005 输入表（`report_digest`："已经 verify_seal 验证的 manifest.report_digest"）。
- **实际源码文件/符号/所审 commit**：`demiflow/workbench/runs/manager.py:600`（`verify_seal(target, manifest, *, environment=True)`）、`:773`（唯一调用点）、`:462 _load` / `:509 show`（读路径**不调用** verify_seal）。bbd93ff。
- **最小反例**：卡把"已 verify_seal"当作输入前提，但读路径并未建立该前提；实现者若照做会在每次读图时重哈希全量源与平台文件。
- **问题属于**：契约措辞 / 潜在验收缺口。
- **建议的最小修正**：把输入描述改为"读路径已校 content digest；environment 级 verify_seal 只在 prepare/start 侧"，并明确禁止读路径调 verify_seal。
- **受影响**：`read_identity_bridge`、`overlay_sealed`、`get`。
- **应新增或保留的验证**：旧环境历史图仍可读；读取字节上界断言。
- **是否阻塞**：不阻塞编码（装配前更正）。

### Q27 / P0
- **对应 root 与实际用户后果**：P.unified_execution、R.1.1。用户最常用的 `take/iter_rows` 根本无法进入封存布局；"一份封存布局驱动所有已支持路线"这条平台意图在最常见的入口上不成立。
- **设计文件/任务 ID**：C08:216（"local take/iter_rows 的 native_relational 路线同样要 compile/load→execute_native_plan"）；L007 `compile_native_topology(action, source, plan, *, decision)`；A006/A007。
- **实际源码文件/符号/所审 commit**：`demiflow/data/dataset.py:1920-1933`（`Dataset.take` 用 `with observe_action("take", ...) as observer:` 后直接 `self._executor.take(...)`，**没有 `run.action(...)`**）；`demiflow/execution/run_context.py:280-283`（`current_action()` 是 action 上下文唯一来源）；`demiflow/data/dataset.py` 的 `iter_rows`/`take_all` 同形态。bbd93ff。
- **最小反例或具体断开的调用链**：`Dataset.take → observe_action（不建 RunAction）→ LocalExecutor.take → iter_rows → _apply_plan`。L007 的第一个形参类型是 `RunAction`，此路径上 `current_action()` 为 `None`，无法构造 `action_id`，也无法 `stage_plan_artifact(action, slice.plan)`。`Dataset.take`、`Dataset.iter_rows`、`Dataset.take_all` 三个函数在 144 张卡中**零命中**。
- **问题属于**：源码事实错误 + 漏任务（入口未派发）。
- **建议的最小修正**：二选一并写进 C08：(a) 新增 A/L 叶任务把 `Dataset.take/iter_rows/take_all` 改为经 `run.action(...)` 建 RunAction（并处理 observe_action 的既有观察语义不丢）；或 (b) 把 `compile_native_topology` 的 action 形参改为可由 `(action_id, run)` 显式构造的轻量句柄，并在 C08 冻结该句柄的来源。不能留给实现者猜。
- **受影响**：`Dataset.take/iter_rows/take_all`、`compile_native_topology`、`stage_native_programs`、`execute_native_plan`；caller A006/A007→L007。
- **应新增或保留的验证**：`ds.map(f).take(3)` 与 `write_lance` 走同一条 compile/load→execute 漏斗，且 bindings_ref 真实存在；observe_action 的既有 metadata 不回退。
- **是否阻塞**：阻塞编码（关键方案需重做）。

### Q28 / P0
- **对应 root 与实际用户后果**：P.unified_execution、R.1.2。`count()` 在流式计划上仍然抛嵌套错；A004 按卡实现后问题依旧，用户看到"统一平台"最常见的聚合动作不可用。
- **设计文件/任务 ID**：C03（"流式计划 + count 复用内部执行/有界计数，不能调用另一个公开 Dataset action 触发嵌套"）；A004（callee 只有 A001）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/stream_delivery.py:41`（`with run.action(declaration) as action:` —— delivery **内部自开 action**）、`:31`（`if limit == 0: return`，在任何静态检查之前）；`demiflow/execution/run_context.py:280-283`（`if current_action() is not None: raise RecoveryContractError("Nested Dataset actions need an explicit completed data boundary")`）；`demiflow/execution/executors/local.py:975-981 count`。bbd93ff。
- **最小反例或具体断开的调用链**：`Dataset.count（已绑 action）→ executor.count → iter_rows → has_stream → iter_stream_rows → run.action(declaration) → current_action() is not None → raise`。A004 卡描述的病因（"调用第二个公开 Dataset action"）与真实病因（delivery 内部 `run.action`）不符；`stream_delivery.py` 不在 45 个派发文件内、`iter_stream_rows` 零卡命中。
- **问题属于**：源码事实错误 + 漏任务。
- **建议的最小修正**：给 `stream_delivery.py` 指定负责人并派发窄任务：`iter_stream_rows` 在已有 action 时复用外层 action（或接受显式 action 参数），不新开；同时把 `:31` 的 `limit==0` 早退移到静态合法性检查之后，以满足 C03 的 take(0) 规则。A004 的病因描述与 callee 同步更正。
- **受影响**：`iter_stream_rows`、`LocalExecutor.count/iter_rows/take`、`RunContext.action`、`admit_action_plan`；caller A004→A001。
- **应新增或保留的验证**：thread map 计划 `.count()` 成功且不触发嵌套守卫；`take(0)` 先做静态检查（非法计划仍拒绝）且源读取=0/Actor 构造=0。
- **是否阻塞**：阻塞编码（关键方案需重做）。

### Q29 / P0
- **对应 root 与实际用户后果**：P.unified_execution、P.bounded。即使 take/write 接上新布局，`iter_batches/aggregate/schema/size_bytes/materialize/write_datasink/write_<format>` 仍在执行期自由选 backend，封存布局不支配全部真实执行；资源窗口也到不了这些 terminal。
- **设计文件/任务 ID**：C08:216、C12（"资源窗口必须由封存布局传到底层 reader/queue/stage/sink"）；L012/L013；FILE-OWNERSHIP.md。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/executors/local.py:562-569`（`_apply_plan`：`reject_streaming_map_options(plan,'local')` 后 `if _native: ... if has_relational(source, plan): self.plan(...); yield from execute(self, source, plan)` —— **不带 `execution=`**）；9 个调用点：`:752 iter_rows`、`:763 iter_batches`、`:839 materialize`、`:1011 aggregate`、`:1065 schema`、`:1085 size_bytes`、`:1161 write_datasink`、`:1242 write_lance`、`:1265 write_<format>`。`_apply_plan` 在 144 张卡中零命中。bbd93ff。
- **最小反例或具体断开的调用链**：`Dataset.take → LocalExecutor.take:993 → iter_rows:752 → _apply_plan:568 → dataset_native.execute(self, source, plan)`。C08 要求 `execute(..., execution=None)` 由绑定调用传入 placement，但唯一把 `_apply_plan` 与 `execute` 连起来的函数无主，`execution` 永远为 None。
- **问题属于**：漏任务（真实分发点未派发）。
- **建议的最小修正**：把 `_apply_plan` 列为 L 的派发函数（或新增叶任务），明确它如何取得 `ExecutionPlacement` 并透传 `execution=`；同时在 C08 列出"哪些 terminal 必须经此漏斗"的完整清单，避免只接 take/write 两个入口就宣布完成。
- **受影响**：`_apply_plan`、`dataset_native.execute`、`Engine.__init__/session`、9 个 terminal；caller L012/L013→L007→L010。
- **应新增或保留的验证**：对每个 terminal 断言实际使用的 backend/profile 与封存 placement 一致；改封存 profile 后底层 options 真实变化（不是只改文档预算）。
- **是否阻塞**：阻塞编码与装配（关键方案需重做）。

### Q30 / P1
- **对应 root 与实际用户后果**：P.performance、R.1.1。Arrow/列合并快路与 lance_scan 静默退化为行路径，用户感到"升级后变慢"，且没有任何门会发现。
- **设计文件/任务 ID**：C03（`available_backends: tuple[str,...] = ()`，"未提供时只选择普通 local 路线，不凭字符串启用后端"）；A001；A008。
- **实际源码文件/符号/所审 commit**：`available_backends` 在设计树 `tasks/` 下**只出现在 A001.md**（消费者），无任何生产者卡；`demiflow/execution/executors/local.py:983-993`（`self._local_kernel is not None` 是真实 Arrow 能力判据）、`demiflow/execution/dataset_native.py`（datafusion 能力判据）。bbd93ff。
- **最小反例或具体断开的调用链**：`admit_action_plan(...)` 默认 `available_backends=()` → 按 C03 只选普通 local → `route='pull'`；A008（Arrow 路线）拿不到非空声明，其分支永不触发。executor 侧没有任何任务负责把 `_local_kernel`/datafusion 可用性翻译成这个有限声明。
- **问题属于**：漏任务（新公开接口只有消费者没有生产者，违反 C10"新公开接口不能只有生产者没有消费者"的对偶）。
- **建议的最小修正**：新增一个 A 或 L 叶任务，在 executor 侧从既有真实能力（`_local_kernel` 非空且未关闭、`spec.schema` 存在、datafusion 可导入）派生 `available_backends`，并在 A001 卡写明其唯一生产点；C03 补一句"该声明由 executor 能力派生，不由用户字符串指定"。
- **受影响**：`admit_action_plan`、A008 的 arrow_batch 分支、`LocalExecutor.take/_apply_plan`。
- **应新增或保留的验证**：有 local kernel 时同 workload 走 arrow_batch 且吞吐不低于基线；无 kernel 时明确拒绝而非静默换路。
- **是否阻塞**：阻塞装配（不阻塞 A001 本身编码）。

### Q31 / P0
- **对应 root 与实际用户后果**：P.recovery、P.correctness、R.5.3。改了 join reducer 或它导入的 helper 之后，恢复**不拒绝**，用旧封存计划跑新代码，产出静默错误结果。这是历史保护的核心承诺被打穿。
- **设计文件/任务 ID**：C08:191（`source_identity_ref=identity(describe(source))`，"覆盖该子计划真实源声明和 callable 源的代码语义"）；L030/L031；F004。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/chain.py:755-775`（`plan_code_pin` **只遍历 `plan.operations`**，docstring 明写 "describe never walks `__globals__`"）、`:826-846 _seal_plan_code`；join 的 reducer 是 **source 字段**（`KeyedSource.kind=='join'`，见 `dataset_native.py:156`），不在 operations 里。bbd93ff。
- **最小反例或具体断开的调用链**：L+F 子审查实跑探针（`/tmp/demiflow-qwen-lf-review/`）：构造 join 计划 → 取 pin 与 `identity(describe(source))` → 只改 reducer 所导入 helper 的函数体 → 重取，**两者均不变**。我已在 `chain.py:755-775` 逐行核对遍历范围，确认该结论来自源码结构而非探针偶然。
- **问题属于**：契约声称与源码能力不符 + 漏任务。
- **建议的最小修正**：在 C08 明确 source 侧代码固定的**确切范围与手段**（例如对 `KeyedSource` 的 callable 字段显式走 `plan_code_pin` 同权威的 code-record 采集，或明确声明"外部 helper 不在固定范围内"并据此拒绝 durable 路径）。不能保留"覆盖 callable 源的代码语义"这句无法兑现的声称。给 L031 补对应窄任务。
- **受影响**：`plan_code_pin`、`_seal_plan_code`、`describe`、`source_identity_ref`、`verify_native_programs`；caller L030→L031。
- **应新增或保留的验证**：改 reducer 本体、改 reducer 导入的 helper 两种反例都必须使恢复拒绝；已有 v1 身份的规范化字节不变（碰撞反例）。
- **是否阻塞**：阻塞编码（关键方案需重做）。

### Q32 / P1
- **对应 root 与实际用户后果**：P.recovery、R.5.2。ACK 超时后迟到回执仍可能推动新 cut；或 driver 永久等待，用户看到"卡住不动"的恢复。
- **设计文件/任务 ID**：C08:256（async ACK、"默认本次等待上限 120 秒（若已有更短运行/控制 deadline 则取更短）"、"超时抛 TimeoutError，保留 unknown"）；L034/L035；Q10。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/runtime_limits.py`（`RECOVERY_LIMITS.checkpoint_timeout_s = 60`，**已存在且比 120 更短**）；`demiflow/execution/worker/execution.py:114-140`（同步 `_request_commit`、`future.result()` 无超时、`_pending` 只在成功时清）、`:142-146`（`deliver_receipt(receipt)` **不按 operation_id 键控**）；`demiflow/execution/stream_barrier.py:57-83`（`fail()` 不取消已发布的 publish 任务）；对照 `worker/graph_execution.py:218-247`（graph 侧**已按 operation_id 键控**，安全）。bbd93ff。
- **最小反例或具体断开的调用链**：L+F 子审查 heartbeat 探针：同步版 150ms 内仅 1 次 tick 且 5.001s 才抛 TimeoutError（event loop 被冻结）；async 版 0.202s 内 15 次 tick。证明 async 化本身必要且可行。但 chain 侧超时后 `_pending` 未清 → `carried=True` → 不发 WORKER_FAILURE 帧 → driver 无限等；迟到 `deliver_receipt` 无 operation_id 隔离。L034 的"只操作本对象"描述也错（它 `self._send(cut)` 做 IPC、改 progress、推进 gc）。
- **问题属于**：契约与既有常量冲突 + 漏任务 + 卡内事实错误。
- **建议的最小修正**：C08 把 120s 改为"取 `min(RECOVERY_LIMITS.checkpoint_timeout_s, 运行/控制剩余 deadline)`"并说明 barrier 剩余时长如何传到 ACK 等待（L034 的冻结签名看不到它，需显式入参）；给 `deliver_receipt` 与 `stream_barrier.fail` 指定负责人并派发 operation_id 键控 + publish 任务取消 + `_pending` 超时清理。
- **受影响**：`_request_commit`、`_request_contribution`、`deliver_receipt`、`_CheckpointBarrier.fail/arrive`、`RECOVERY_LIMITS`；caller L034/L035→progress.seal→stream_barrier。
- **应新增或保留的验证**：event-loop heartbeat 未冻结（不是只测"最终返回"）；迟到 ACK 不推动新 cut；超时后 driver 在有限时间内收到失败而非挂死。
- **是否阻塞**：阻塞装配。

### Q33 / P1
- **对应 root 与实际用户后果**：P.bounded、P.performance。资源窗口在文档里收紧、执行里仍按默认值跑，峰值内存超订；用户遇到 OOM 而"预算已声明"。
- **设计文件/任务 ID**：C12:250-252（"session 实际 DataFusionOptions 的 pool/RSS/scratch/threads/rows 来自原 run/envelope/native 配置与封存 profile/stage_limits 的交集"）；Q005/Q006；L010。
- **实际源码文件/符号/所审 commit**：`demiflow/resources/contracts.py:36`（`_PROFILE_RESOURCES = frozenset({"cpu_millis","memory_bytes","scratch_bytes"})` —— **无 threads、无 rows**，且该文件是冻结七文件之一）；`demiflow/execution/runtime_limits.py:4`（`DEFAULT_LIMITS.memory_bytes = 256MiB`，无 threads 键）。bbd93ff。
- **最小反例或具体断开的调用链**：C12 要求 threads 来自 profile，但 profile 的合法资源键集**不含 threads**，且该键集在冻结文件里 → 按现契约无法实现。A+Q 子审查数值探针（`/tmp/demiflow-qwen-aq-probe/`）：固定驻留 224MiB + DataFusion pool 256MiB = 480MiB，对 `run.memory_bytes=256MiB` 超订 1.875×；我核对了两个常量来源，**该算式的具体驻留构成我未独立复算**，按探针报告记。
- **问题属于**：契约与冻结键集冲突 + 公式未冻结。
- **建议的最小修正**：C12 删掉 threads/rows "来自 profile" 的表述，改为明确来源（native 配置或 stage_limits），或走冻结文件的既有加性扩展程序；同时把"固定驻留 + 动态保留"的**唯一计算公式**写进 C06/C12，并指明谁是权威（避免 Q 与 L 各算一份）。
- **受影响**：`native_resources.dataset_session`、`Engine.session`、`stream_limits`、`ExecutionPlacement.stage_limits`、`_PROFILE_RESOURCES`。
- **应新增或保留的验证**：同一 workload 同一预算下峰值不超订；改封存 profile 后真实 options 变化；吞吐/峰值双跑对比（>10% 必须解释）。
- **是否阻塞**：阻塞装配。

### Q34 / P1
- **对应 root 与实际用户后果**：P.bounded、R.1.1。Arrow 批路径不做字节/磁盘记账，"有界"承诺在这条路线上不成立。
- **设计文件/任务 ID**：C08（`arrow_batch_sink.execute(..., execution=None)` 加性参数、"绑定调用必须消费 profile 和 stage 预算"）；C12；A008。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/arrow_batch_sink.py:78-97`（`def execute(executor, source, plan, schema)` 无 execution 形参；`options = executor._local_kernel` 直接借用全局；`with TaskScheduler(options, stats)`）；`demiflow/execution/local_tasks.py:425-470`（无字节/磁盘准入记账）。`local_tasks.py`、`local_kernel.py`、`data/runtime_contract.py` 均不在 45 个派发文件内。bbd93ff。
- **最小反例或具体断开的调用链**：`arrow_batch_sink.execute → TaskScheduler(executor._local_kernel, stats)`。即使 A008 加上 `execution=` 形参，真正做调度与落盘的 `local_tasks.py` 无主，预算无法落到实际 I/O；C12"只改文档里的预算而执行仍读当前默认值不合格"正好命中。
- **问题属于**：漏任务（预算接入点未派发）。
- **建议的最小修正**：把 `local_tasks.py` 的字节/磁盘准入点列为 Q 或 L 的派发函数（最小窄补丁），或在 C12 明确 Arrow 路线的预算由哪一层强制并给出该层的负责人；A008 卡补 callee。
- **受影响**：`arrow_batch_sink.execute`、`TaskScheduler`、`local_tasks` 的落盘/记账、`ExecutionPlacement.stage_limits`。
- **应新增或保留的验证**：单行超 `inflight_bytes` 时明确失败而非 `max(1)` 继续；输出暂存超 `output_bytes` 时准入拒绝；磁盘上限真实生效。
- **是否阻塞**：阻塞装配。

### Q35 / P0
- **对应 root 与实际用户后果**：P.correctness、R.5.3。数据行编码把 `-0.0` 与 `0.0` 合并、把同位 NaN 的不同对象当成不同值，导致块 sha256 与 receipt 不符、恢复被误拒或数据被静默改写。
- **设计文件/任务 ID**：C06（`_canonical_row` 保留浮点符号与 NaN 位模式、"float 签名使用明确的 IEEE-754 字节，而不是 Python `==`"）；Q001/Q002/Q003；CODEC 门。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/…/_canonical_row`（Q001 已定位）；`demiflow/data/runtime_contract.py:34 check_size`（complex 已被拒绝）；`tests/test_s6_commit_window_quota.py:432`（真正断言 canonical 编码字节稳定性的测试）。bbd93ff。
- **最小反例或具体断开的调用链**：A+Q 子审查探针（`/tmp/demiflow-qwen-aq-probe/`）实测：`{'negative':[-0.0],'positive':[0.0]}` 被合并；同位 NaN 的不同对象编码长度 38 vs 48 字节（违反 s6:432 的稳定性断言）；set 跨 `PYTHONHASHSEED` 长度不变但 sha256 变（47 字节，cfc88→a0e2f→5e664）——**真实失败形态是块 sha256/receipt 不符，不是 C06 提到的 `Budget reservation changed`**。我核对了 `check_size:34` 与 s6:432 的存在与断言内容；三组数值为探针实测，我未复跑。
- **问题属于**：源码缺陷（设计已正确定位）+ 验收门缺口 + C06 失败形态描述偏差。
- **建议的最小修正**：把 `tests/test_s6_commit_window_quota.py` 加入 CODEC 门命令；C06 把 set/frozenset 的失败形态更正为"块摘要/receipt 不符"，并明确要求跨 PYTHONHASHSEED 反例；complex 已被 `check_size` 拒绝，Q001 不必为它扩范围。
- **受影响**：`_canonical_row`、`BlockSink.append`、`check_size`、CODEC 门命令。
- **应新增或保留的验证**：signed-zero 与 NaN 位模式保真；同位 NaN 不同对象编码一致；set 跨 PYTHONHASHSEED 的稳定编码或明确边界拒绝；s6 配额窗口断言保留。
- **是否阻塞**：阻塞验收（Q001 方向正确，可直接编码）。

### Q36 / P2
- **对应 root 与实际用户后果**：P.unified_execution。同一对象出现在两个 input_path 时被折叠成一次执行，与 C08 的"不自动折叠"相反，导致 operator→stage 映射与实际执行不符。
- **设计文件/任务 ID**：C08:189（"相同对象在两个位置不自动折叠成共享执行，只有明确共享声明才能复用"）；L030/F004。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/dataset_native.py:146-151`（`def typed(self, item): ... cache_key = (id(source), id(plan)); if cache_key in self.typed_cache: return self.typed_cache[cache_key][2]`）。bbd93ff。
- **最小反例或具体断开的调用链**：`union(ds, ds)` 或 join 两侧共享同一 source 对象 → `typed()` 命中同一 `id` 键 → 返回同一编译结果 → 两个 ProgramSlice 复用一份 typed 计划，而 C08 要求两个独立 program_id。
- **问题属于**：契约与既有缓存语义冲突（低风险，但需明写）。
- **建议的最小修正**：在 C08 或 L030 卡注明 `typed_cache` 的 `id` 键只是**编译缓存**、不构成执行共享声明；ProgramSlice 的划分以 input_path 为准，缓存命中不得合并 binding。
- **受影响**：`DataFusionEngine.typed`、`typed_cache`、`stage_native_programs`。
- **应新增或保留的验证**：同一对象两个 input_path → 两个 program_id、两份 operator_bindings；缓存仍生效（性能不回退）。
- **是否阻塞**：不阻塞（装配前澄清）。

### Q37 / P2
- **对应 root 与实际用户后果**：R.5.1（错误权威一致）。卡内 errors 清单与"只操作本对象"描述失真，实现者按卡写测试会漏掉真实异常类型，或误判改动范围。
- **设计文件/任务 ID**：L034/L035、L010、L015–L019、L039、L021/L024/L027 的卡内 errors / mode 字段。
- **实际源码文件/符号/所审 commit**：`worker/execution.py:114-146`（`_request_commit` 实际 `self._send(cut)` IPC + 改 progress + `advance_gc`，并非"只操作本对象"；需 TimeoutError/CancelledError）；`local_tasks.py`（RuntimeError/TypeError/NotImplementedError）；`plan_interpreter.py` 的 `_load_plan`（纯读，无运行期状态写）。bbd93ff。
- **最小反例或具体断开的调用链**：L034 卡写"只操作本对象"，实现者据此不做 IPC 取消传播 → 超时后 cut 已发出但无回执隔离（与 Q32 叠加）。L021/L024/L027 把 `_load_plan` 标 `runtime_state`，会让实现者以为可以写状态。
- **问题属于**：卡内元数据错误（caller/callee/errors/mode 是手填的，工单已提示）。
- **建议的最小修正**：批量更正上述卡的 errors 清单与 mode 标注；L034/L035 的"只操作本对象"改为如实描述 IPC + progress + gc 三个副作用及其 owner。
- **受影响**：上述 12 张卡的元数据；不影响签名。
- **应新增或保留的验证**：无需新增；装配时按更正后的 errors 清单核对。
- **是否阻塞**：不阻塞。

### Q38 / P2
- **对应 root 与实际用户后果**：P.correctness。F002 若在 bbd93ff 上重新实现，会与 c_budget 已交付的同一函数产生两份实现，合并时冲突或覆盖。
- **设计文件/任务 ID**：F002；C02；Q24（同类漂移）。
- **实际源码文件/符号/所审 commit**：c_budget 树 `90d7ee4` + dirty `demiflow/workbench/runs/plan_facts.py`（+56/-11）：已实现 kind 单一权威、`map_batches/add_column/sample/model_map` 的 arguments、新增 `_specialization_domain`，**改动中没有任何任务 ID 标注**。L+F 子审查读 diff 确认；我核对了 `git status --porcelain` 与 `diff --stat`。
- **最小反例或具体断开的调用链**：F002 的实现者从 bbd93ff 起手写同一函数 → 与 c_budget 的 dirty 版本在装配期二选一，另一份的语义（`_specialization_domain`）丢失。
- **问题属于**：在途漂移 / 吸收责任未划。
- **建议的最小修正**：在 F002 卡注明"以 c_budget `90d7ee4`+dirty 的 `plan_facts.py` 为既有实现基线，逐函数吸收并补 `_specialization_domain` 的契约描述"，或明确由装配任务负责合并；不要重新实现。
- **受影响**：`operation_facts`、`_specialization_domain`、F002 卡。
- **应新增或保留的验证**：吸收后 C02 的 arguments 键集逐 kind 断言；已有受支持类型输出逐字节兼容。
- **是否阻塞**：不阻塞编码（装配前必须划清）。

### Q39 / P1
- **对应 root 与实际用户后果**：P.unified_execution、R.1.1。route 枚举有两个值永远不产生；真实准入决策点远多于 A 派发的 5 个；`unified_chain_eligible` 静默回退 legacy，与 C03 明文禁止相反。用户看到的仍是"部分路线受封存支配"。
- **设计文件/任务 ID**：C03（route 八值枚举、`:74`"任何消费者不得捕获失败后偷偷走 legacy"）；A001–A008；FILE-OWNERSHIP.md。
- **实际源码文件/符号/所审 commit**：`queue_stream`、`static_graph` 两个字面量在设计树 `tasks/` 下**零命中**（无生产者也无消费者）；`demiflow/execution/chain.py:310-329 unified_chain_eligible`（`cloudpickle.dumps(plan)` 失败即 `return False` 并 debug 日志回退 legacy，设计树零命中）；未派发的真实决策点：`LocalExecutor.plan/_apply_plan/write_datasink/materialize_stream`、`Dataset.take/take_all/materialize/union`。另 pull 路线 `take(1)` 实际按块读取（32/64 行），不是行级惰性。bbd93ff。
- **最小反例或具体断开的调用链**：`unified_chain_eligible(plan) → dumps 失败 → False → legacy in-driver`。C03:74 禁止这种静默回退，但该函数是**既有受控 MVP-1 seam**（docstring 明写），设计既未派发它也未在 C03 说明它属于例外。A 的 agent 与 chain 的 agent 会对此各持一种理解。
- **问题属于**：枚举死值 + 漏任务 + 契约与既有 seam 边界未划。
- **建议的最小修正**：(a) 删除 `queue_stream`/`static_graph` 两个 route 值，或给它们指定真实生产者与消费者；(b) 在 C03 明确 `unified_chain_eligible` 是保留的受控 seam 还是必须改为明确拒绝，并据此派发或豁免；(c) 把上列未派发决策点逐个归入 A/L 的窄任务或显式记为"本轮不改，受支持边界如实声明"；(d) C03 的"保持惰性、有限读取"改为按块粒度的如实表述。
- **受影响**：`admit_action_plan.route`、`unified_chain_eligible`、上列 8 个未派发函数。
- **应新增或保留的验证**：每个 route 值都有真实产生路径与消费路径（机器可核）；seam 回退要么消失要么被显式断言为受控边界；`take(1)` 的实际读取行数如实断言。
- **是否阻塞**：阻塞装配（(a)(b) 需在分发前定，(c)(d) 可记未覆盖）。

---

## 三之二、Pass 2 追加发现：以既有架构图为锚（Q40–Q46）

依据不再只是 GPT 的设计文档，而是用户此前已确认的架构权威：
`_demiflow/platform_coordination_20261104/unified_execution_architecture_20261107/` 下的
`architecture_current_v1.mmd`、`architecture_target_v1.mmd`、`architecture_source_evidence_v1.json`（15 个源码锚点＋迁移动作）、
`module_boundary_map_v1.json`（五模块 A 资源/B 算子执行/C 交换/D 提交恢复/E 编排，含 barrier 四模块归属）、
`execution_ownership_and_language_v1.json`、`data_layer_backend_boundary_v2.json`、`complete_architecture_and_migration_v1.json`（U0–U6 与验收矩阵）。

### Q40 / P1
- **对应 root 与实际用户后果**：P.unified_execution、R.1.1。用户以为"统一平台"已交付，实际只交付封存/身份/状态层；目标架构图的独立 worker、Local/Ray 统一、CPU/GPU 调度本轮都不在。
- **设计文件/任务 ID**：C08（v2 `native_driver_v1` 只接 driver）、C11（"能力不足必须明确显示受支持边界"）、INTEGRATION-ACCEPTANCE.md。
- **实际源码文件/符号/所审 commit**：架构锚点比对——15 个锚点中 **10 个未派发**：`execution/contracts.py`（bbd93ff 已改为 `contracts/` 包，锚点本身过期）、`execution/driver.py:96 execute_driver`（仍二选 LocalDatasetExecutor/RayDatasetExecutor）、`planning/model.py`、`execution/local_tasks.py`、`execution/executors/ray.py`、`execution/commit.py`、`execution/source_snapshot.py`、`execution/queue_commit.py`、`execution/package_loader.py`、`execution/stream_barrier.py`。设计树对 `RunCoordinator`/`ResourceProvider`/`WorkerProtocol`/`Exchange`/`epoch`/`三种交接` **全部零命中**；`ResourceManager` 仅在 L015–L019 作为"原登记根 Path"被引用。bbd93ff。
- **最小反例或具体断开的调用链**：架构 `complete_architecture_and_migration_v1.json` 的 U5 exit 明文要求"read/write/take/count/aggregate/materialize/checkpoint 等**所有公开 action 接统一路径**"、"**没有无声退回 Ray Data**"、E 模块 acceptance"**旧路径退役而非永久并存**"，且总则明文"**不能用'之后补上某个 terminal/adapter/恢复'宣布整体完成**"。设计的 v2 backend 枚举 `{local_stream,local_pull,datafusion,arrow_batch,lance_scan}` **不含 ray**，`driver.py` 未派发，`unified_chain_eligible` 仍静默回退（Q39）。
- **问题属于**：受支持边界未如实声明（不是要求本轮做完架构迁移）。
- **建议的最小修正**：在 CONTRACTS.md 或 INTEGRATION-ACCEPTANCE.md 增一节"本轮在架构 A→B→(C+D)→E 序列中的位置"，逐项列出：本轮交付（E 的最小计划/工件边界＋D 的状态/buffer/效果契约）、本轮不交付（B 共享 WorkerRuntime、C 三模式统一、Ray Core 承载、旧路径退役），并把验收措辞从"驱动所有已支持路线"收紧为"驱动本轮列明的路线"。架构自身已把 Ray 切换列为后续目标，所以不交付是合理的，**不声明才是问题**。
- **受影响**：C08/C11 文本、INTEGRATION-ACCEPTANCE.md、27 门的通过语义。
- **应新增或保留的验证**：一份机器可核的"架构锚点×本轮交付/延后"对照表；延后项不得计入通过。
- **是否阻塞**：阻塞验收口径（不阻塞编码）。

### Q41 / P1
- **对应 root 与实际用户后果**：P.bounded、P.recovery。多 stage 流水线在 cut 提交时撞既有参与者数/聚合字节上限，恢复直接失败。
- **设计文件/任务 ID**：C05（"与 Actor 状态共同进入原 commit"、"group/批次与全局行、状态、字节预算共同约束"）；R002/R007/R015。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/commit.py:33 commit(sequence,*,outputs,position,states=None,acknowledge=None)`、`:43-47`（`1<=len(outputs)<=MAX_SINKS`、`len(states)>MAX_STATES` 即 raise）、`:54-56`（`state_bytes=sum(len(document(value, max_state_bytes)))`，超 `max_state_bytes` 即 raise）；`recovery_contract.py:17-18`（`MAX_SINKS=64`、`MAX_STATES=256`）；`runtime_limits.py:12`（`max_state_bytes=16MiB`）。设计树对 `MAX_STATES`/`MAX_SINKS`/`max_state_bytes` **零命中**（`CommitCoordinator`/`StateArtifacts` 有命中，即权威被消费但上限被忽略）。bbd93ff。
- **最小反例或具体断开的调用链**：每个 stage 的 `StageBufferSnapshot` 若各占一个 `states` 键，N 个 stage → N 个参与者；聚合控制文档字节受 16MiB 约束。C05 只说"字节预算共同约束"，未指明权威是 `commit.py:54-56` 的聚合上限，也未给每 stage 快照的字节配额。
- **问题属于**：契约未冻结既有上限。
- **建议的最小修正**：C05 明写"buffer 快照作为 `CommitCoordinator.commit(states=...)` 的参与者，受 `MAX_STATES=256` 与聚合 `max_state_bytes=16MiB` 约束；大行必须走 BlockSink/manifest，不得内联"，并给 R002 的 `BlockDescriptor` 一个明确字节配额来源。
- **受影响**：`StageBufferAdapter.align`、`CommitCoordinator.commit`、`StateArtifacts.put`、`RECOVERY_LIMITS`。
- **应新增或保留的验证**：多 stage（≥8）cut 提交成功且聚合字节可核；超限时在读取前明确拒绝而非提交中途失败。
- **是否阻塞**：阻塞装配。

### Q42 / P0
- **对应 root 与实际用户后果**：P.unified_execution、P.recovery、R.5.2。stream 阶段与 graph/queue worker 用**两套不同的 cut/barrier 协议**，"同一 cut 提交"不成立；跨路线恢复语义不一致，用户看到同一条流水线在不同 backend 下恢复行为不同。
- **设计文件/任务 ID**：C05（新建 `execution/stage_buffers.py` 的 `StageBufferAdapter`、`BlockDescriptor`、`next_batch_ordinal`/`next_output_ordinal`）；R001–R016；FILE-OWNERSHIP.md。
- **实际源码文件/符号/所审 commit**：**既有交换权威已存在且在跑**——`demiflow/execution/exchange/`（1410 行：`channel.py` 159、`graph_channel.py` 187、`graph_router.py` 438、`carriage_local.py` 230、`carriage_ray.py` 187、`segments.py` 60、`segment_gc.py` 135）；`channel.py:21-108 ChannelProducer`（`route_epoch`、`ChannelSequence`、`credit_rows/credit_bytes`）、`:74-87 barrier(checkpoint_id,*,epoch,topology_revision,producer_sequence_bound,source_progress_ref)`、`:108-151 ChannelConsumer`（`_gate.push`）；`demiflow/execution/contracts/exchange.py`（375 行，K4/K5 §9/§10：`DATA_REF_SCHEMA`、`DATA_ENVELOPE_SCHEMA`、`_ENVELOPE_KEYS={schema_version,channel_id,sequence,partition_id,route_epoch,producer,records,bounds}`、`:293 InputGate` 的 §10 不变量"barrier 只有在所有 pre-cut 数据被消费后才生效、对齐期间已到通道的 post-barrier 数据被 held、空/结束通道仍是同一 cut 的参与者、缓冲有界"）。**live 调用点**：`queue_chain.py:187`、`graph.py:148/522/954/955`、`worker/graph_execution.py:19/20/21/74/84`、`worker/graph_progress.py:25`、`worker/queue_transform.py:21`、`chain.py:1110`。设计树对 `InputGate`/`DataEnvelope`/`DataRef`/`ChannelSequence`/`ChannelCursor`/`route_epoch`/`data_envelope`/`data_ref`/`contracts/exchange`/`carriage`/`segments`/`graph_channel` **全部零命中**。bbd93ff。
- **最小反例或具体断开的调用链**：架构 `module_boundary_map_v1.json` 的 `barrier_ownership` 明文把"barrier 作为有序通道控制消息、输入 gate 负责多输入对齐"判给 **C 模块**，"发起和协调全局检查点、持久确认、超时失败及恢复"判给 **D**，"响应切点、快照自己的状态/有限在途"判给 **B**。GPT 把三者合并进一个新 R 包并新建 `stage_buffers.py`，既不引用 C 的既有 `InputGate`/`ChannelSequence`，也不派发 `stream_barrier.py`（Q03）。R 的 agent 会按 C05 造 `next_batch_ordinal` 序号空间，而 graph/queue worker 走的是 `channel sequence + route_epoch` 序号空间——**两个序号空间无法在同一个 cut 里对齐**。
- **问题属于**：漏任务 + 重复权威（架构级）。
- **建议的最小修正**：C05 必须先声明与既有 K4/K5 的关系，二选一并写死：(a) `StageBufferAdapter` 是 B 模块的**本地快照适配**，其 ordinal 必须映射到既有 `ChannelSequence`/`producer_sequence_bound`，多输入对齐一律由 `InputGate` 负责，R 不得自造对齐；或 (b) 明确 stream 路径本轮不接 exchange，`StageBufferAdapter` 只服务同进程 stage，并在 C05/验收里如实声明"跨 worker cut 不在本轮"。同时给 `stream_barrier.py`、`exchange/`、`contracts/exchange.py` 指定归属或明确冻结豁免。
- **受影响**：`StageBufferAdapter.align/restore`、`_Stage`、`stream_barrier.arrive`、`InputGate.push`、`ChannelProducer.barrier`、`GraphInputReader`、`worker/graph_progress.py:153` 的 InputGate 对齐。
- **应新增或保留的验证**：同一 run 内 stream stage 与 graph worker 的 cut 序号可互相解释；`InputGate` §10 不变量（pre-cut 全消费才生效、post-barrier held、空通道仍参与、缓冲有界）在新 buffer 层下仍成立；既有 `test_u3_graph_*` 与 exchange 一致性测试不回退。
- **是否阻塞**：阻塞编码（关键方案需重做）。

### Q43 / P0
- **对应 root 与实际用户后果**：P.bounded、P.recovery。预算"只收紧"会把对齐缓冲压到协议下限以下，**接收方在 cut barrier 到达前就死掉**——正好是恢复最关键的时刻。
- **设计文件/任务 ID**：C06:144（`stream_limits(...)` "只返回更紧或相等的预算"）；Q005/Q006；C12（stage_limits 不是建议）；L015–L019（graph 侧 `resource_root`）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/reservation.py:252 LEGACY_PREFETCH={append_slots:64,source_batch_size:256,queue_depth:4096,max_pending:1024}`、`:255-271 derive_prefetch(inflight_slots=None,*,max_pending_floor=1)`，docstring 明文："`max_pending_floor` is a protocol minimum: the frozen InputGate alignment buffer must hold a full credit grant of data envelopes (worst case one row each = credit rows), so max_pending never drops below it even when the admitted budget is smaller — **otherwise the gate overflows and the receiver dies before the cut barrier arrives**"；`:271 bounds["max_pending"]=max(bounds["max_pending"],max_pending_floor)`。**真实调用点**：`graph.py:546` 与 `graph.py:979`，均 `derive_prefetch(prefetch_slots, max_pending_floor=budget["rows"])`。`graph.py` 已派发给 L。设计树对 `derive_prefetch`/`max_pending_floor`/`reservation.py`/`LEGACY_PREFETCH` **零命中**。bbd93ff。
- **最小反例或具体断开的调用链**：Q 的 agent 按 C06 实现"只更紧或相等"，L 的 agent 按 C12 把 `stage_limits.inflight_rows` 压到小于 channel `credit_budget.rows` → `max_pending < max_pending_floor` → `InputGate.push` 抛 `ProtocolError("INVALID_SPEC","alignment buffer exceeds budget")` → 接收 worker 在 cut 前死亡。两个 agent 都"按卡完成"。
- **问题属于**：契约与既有冻结协议下限冲突 + 漏命名。
- **建议的最小修正**：C06 增一句硬例外："收紧不得违反既有协议下限；`max_pending` 必须 ≥ `derive_prefetch(..., max_pending_floor=credit_budget.rows)` 的结果，`reservation.py` 的 `LEGACY_PREFETCH`/`derive_prefetch` 语义保留不改"，并把 `reservation.py` 列为 L/Q 的只读依赖（不派发但必须核）。C12 的 `inflight_rows` 语义补"与 channel credit 的关系"。
- **受影响**：`stream_limits`、`derive_prefetch`、`InputGate.max_pending`、`graph.py:546/979`、`ExecutionPlacement.stage_limits`。
- **应新增或保留的验证**：`inflight_rows < credit rows` 时准入明确拒绝或自动抬到 floor（择一写死），不得让 gate 溢出；`prefetch_slots=None` 默认路径逐字节不变（既有 U3 承诺）。
- **是否阻塞**：阻塞编码。

### Q44 / P1
- **对应 root 与实际用户后果**：P.bounded、R.1.2。流式/物化路径仍先全源捕获再下发，用户的大源流水线首条延迟与峰值内存不满足"有界、及时输出"。
- **设计文件/任务 ID**：C03（"流式计划 + materialize → 原生 checkpoint、状态/效果适配器与预算先验"）；C08（source_bindings）；架构锚点 C11。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/source_snapshot.py:38-68 capture_source`：`:50-57` `rows=iter(rows_factory()); for row in rows: sink.append(row)` —— **无条件遍历整个源**后才 `:58 coordinator.commit(1,...)`；`:8-35 freeze_source` 只固定 Lance 版本，不固定 callable 代码（佐证 Q31）。**live 调用点**：`executors/local.py:829`（materialize）、`graph.py:555/988`、`chain.py:1306/1551`、`stream_materialize.py:14`。架构 `architecture_source_evidence_v1.json` C11 迁移要求："固定版本可重读源采用分片游标；不可重读源先增量持久捕获再释放相应记录给下游，**取消不必要的全源等待**"。该文件未派发，设计树对 `source_snapshot`/`capture_source`/`freeze_source` 零命中。bbd93ff。
- **最小反例或具体断开的调用链**：`Dataset.materialize → local.py:829 capture_source(action, lambda: self.iter_rows(source, LogicalPlan()))` → 全源遍历 → 才 commit → 才下发。C03 声称的"预算先验"发生在全源捕获之后。
- **问题属于**：架构迁移项未交付且未声明（与 Q40 同类，但此项直接影响"有界"这条核心意图）。
- **建议的最小修正**：二选一——(a) 派发 `source_snapshot.py` 的窄任务实现分片游标/增量捕获；或 (b) 在 C03/C11 如实声明"本轮 materialize 仍先固定全源快照，增量捕获延后"，并把首条延迟/峰值的验收口径按此调整，不得声称已满足及时输出。
- **受影响**：`capture_source`、`freeze_source`、`LocalExecutor.materialize`、`stream_materialize`、`graph.py`/`chain.py` 的源固定点。
- **应新增或保留的验证**：大源下首条输出延迟与峰值内存的真实测量；若延后，明确记为未覆盖而不是通过。
- **是否阻塞**：阻塞验收口径。

### Q45 / P2
- **对应 root 与实际用户后果**：R.5.1。两套编号语义竞争，跨会话沟通与验收对账时把" M01"理解成不同东西，导致漏验或重复实现。
- **设计文件/任务 ID**：GPT 的包字母 A/F/S/R/Q/M/L/W/V 与任务号 M001–M012；架构 `module_boundary_map_v1.json` 的五模块 A/B/C/D/E 与 `complete_architecture_and_migration_v1.json` 的 M01–M09、U0–U6。
- **实际源码文件/符号/所审 commit**：架构明文 M01–M09 = "原始获取、业务验证、来源展开、搜索、enqueue、read_queue、结果持久化、ack_queue、后处理"（九段业务流水线），验收矩阵要求"M01–M09 × 三类来源 × Local/Ray"；GPT 的 M 包是模型/embedding 效果任务。架构 `dependency_cautions` 明文"上一轮 U0–U6 是完整验收检查表，本轮五模块是代码责任图；**保留完整检查表，避免两套阶段编号竞争**"。
- **最小反例或具体断开的调用链**：总控说"M01 已通过"，架构语义指原始获取阶段，GPT 语义指 M001 prompt 任务；27 门中没有任何门覆盖"M01–M09 × 三类来源"矩阵。
- **问题属于**：文档/索引措辞。
- **建议的最小修正**：在 plan.json 或 README 增一张编号对照表（架构 A–E ↔ GPT 包；架构 M01–M09 ↔ 业务阶段 ↔ 覆盖它的门），并把 U0–U6 检查表作为独立附录保留，不与 27 门混编。
- **受影响**：plan.json、root-map.json、gates.json 的可读性与对账。
- **应新增或保留的验证**：无需新增测试；需一张机器可核的对照表。
- **是否阻塞**：不阻塞。

### Q46 / P1（Q09 的架构背书与精确化）
- **对应 root 与实际用户后果**：P.recovery、R.5.3。write_lance＋模型算子这条最重要的链在封存阶段抛错，或悄悄丧失 resume 拒绝能力。
- **设计文件/任务 ID**：C08:210（`stage_native_programs` 对每个 ProgramSlice 调 `stage_plan_artifact`）；L030/L012。
- **实际源码文件/符号/所审 commit**：`chain.py:849-870 stage_plan_artifact`：`:861 raw=cloudpickle.dumps(plan)` **无 try/except**，docstring 自称"**in-run** pickle delivery path"；`chain.py:873-903 seal_write_plan_ref` docstring 明文："materialized PromptActor instances carry runtime locks (**unpicklable**) and those bytes **drift across attempts, weakening gap④**"，故 write 路径用 `identity(plan_code_pin)` 作 bundle digest、pin_key 固定为 `action_id+"-code-pin-write"`。**架构权威背书**：`execution_ownership_and_language_v1.json` 的 `executable_package_design.compatibility` 明文"匿名闭包/捕获资源的跨进程可封装范围需显式验证，**不能以 cloudpickle 字节充当通用持久协议**"。两者返回键集均为 `{path,sha256,bytes,code_pin_digest,code_sources}`，与 `ProgramRef` 逐键匹配。bbd93ff。
- **最小反例或具体断开的调用链**：`write_lance(model_map(...)) → L012 → L007 → L030.stage_native_programs → stage_plan_artifact(action, slice.plan) → cloudpickle.dumps(含已物化 PromptActor 的 plan) → TypeError/PicklingError`。
- **问题属于**：契约选错既有权威（**最小修正即可，不需重做方案**）。
- **建议的最小修正**：C08:210 改为按交付方式选权威："交付到独立 worker 的子计划用 `stage_plan_artifact`（in-run pickle＋code-pin）；在本进程内执行的子计划（write/模型）用 `seal_write_plan_ref`（code-pin-only，固定 per-action pin_key）"。两者已返回同一 ProgramRef 形状，L030 只需分派，不需新序列化权威。
- **受影响**：`stage_native_programs`、`ProgramBinding.plan_ref`、`verify_native_programs`、L012/L030。
- **应新增或保留的验证**：含已物化 PromptActor 的 write 计划封存成功；改业务码后 resume fail-closed；跨 attempt pickle 字节漂移不影响 write 路径身份。
- **是否阻塞**：阻塞编码（但修正量小）。

### Q31 精确化（重要更正，避免过度声称）
Pass 1 的 Q31 表述需收窄。**operations 侧的依赖改码已被真实探针证明是 fail-closed 的**：
`chain.py:652-736 _business_dependency_modules(plan)` 对 by-reference 纯 UDF 走 `__globals__` BFS（≤64 模块、≤8MiB、循环安全、不执行代码），
`plan_code_pin` 的 `dependencies` 键固定这些模块源码摘要；
历史 `codex_independent_review_20261112_round3/code-pin-results.json` 是**真实执行**的证据：`dependency` 与 `direct` 两种改码均返回
`RecoveryContractError: Worker plan code-pin differs; business code edited since seal (fail-closed)`，`unchanged` 正常出 rows；
`codex_independent_review_20261113/verification.json` 的 closure-resume 探针进一步证明"真实公开 Local API＋worker SIGKILL＋只改直接依赖＋同 RunRef resume"被拒绝。
**缺口精确限于 source 侧 callable**：`_business_dependency_modules` 内部的 `udfs()` 生成器只遍历 `plan.operations`，
`plan_code_pin` 的 operations 循环同样只遍历 `plan.operations`；join 的 reducer 是 `KeyedSource` 的 **source 字段**，
`freeze_source` 只固定 Lance 版本不固定代码。因此 Q31 的正确表述是：
"**source 侧 callable（join reducer）及其 imports 不在 code-pin 范围内**"，而不是"导入的 helper 不被 pin"。
C08:191 声称 `source_identity_ref` 覆盖"callable 源的代码语义"仍不成立，修正建议不变。

### Pass 2 同时确认的正面事实（不构成发现）
- 架构 `portability_rules`"不使用 PID、Ray ObjectRef 或语言内存地址作为唯一恢复依据"：设计的 `program_id=identity([action_id,input_path])`、`execution_id=identity([action_id,"driver",route])` **符合**；`typed_cache` 的 `id()` 只是编译缓存（Q36 已按此定性）。
- 架构 `WorkerLaunchSpec`"仅指向统一 worker 入口、工件清单和 bootstrap 控制信息"：设计保留 `{action_id,group_id,bindings_ref}` **符合**。
- 架构 C10"不在 ResourceManager 或 Provider 新建业务提交账本"：设计 C05"不互相新建账本"、`CommitCoordinator`/`StateArtifacts` 被 147/148 个卡文件引用，**方向符合**（上限未命名见 Q41）。
- 架构 `language_recommendation.reuse_first`"已存在的 DataFusion/Arrow 继续复用，不能把 Python 入口等同全链逐条解释"：设计 C08"DataFusion 内部优化由 DataFusion 所有，平台只表达 backend stage 真实粒度"**符合**。
- 三种交接模式在冻结 `dag.py:311-323 select_exchange` 中已存在（`finite_sealed`/`durable_incremental`/`memory_incremental`），设计消费 `exchange_mode`/`durable_incremental`；但 `finite_sealed` 在设计树零命中，而 grouping/dedup 消费者正是走该模式（并入 Q42 的对齐范围）。

---

## 三之三、Pass 3 追加发现：五轮历史 Review 与两份总控裁决的对账（Q47–Q49）

本轮把主仓 `demiflow/docs/` 下五轮历史 Review（20261110、20261112 三轮、20261113）与两份总控裁决
（§6 提交崩溃窗口额度、§7 单行上限）逐条拉回 **bbd93ff 实际源码**复核，再对照 v2 设计的派发范围。
**先说最重要的正面结论：历史 P1 在本设计基线上几乎全部已关闭**，因此本报告不重复报已修项；
真正的风险从"旧缺陷仍在"转为"**新机制绕开已关闭的保证**"。

### 历史项对账表（我逐条回到源码核过，非采信历史文档）

| 历史项 | 裁决/复审定级 | bbd93ff 实际状态（行级证据） | v2 设计处置 |
|---|---|---|---|
| F1 恢复后 rebalance 从 0 重新路由 | P1 | **已修**：`graph_router.py:161,174` 新增 `initial_ordinal`；`worker/graph_execution.py:139` 传 `(self.restore or {}).get("egress_ordinal", 0)` | L026 `_run` = verify_preserve，**保护到位** |
| F2 finite_sealed 未封口即交付 | P1 | **已修**：`graph_router.py:369-372 _sealed_or_incremental` 门；生产 `graph_execution.py:203 output_router.seal(...)` | 同上，verify_preserve 保护 |
| F3 cut > 未确认预算 → 合法小图超时 | P1 | **已修**：`stream_barrier.py:211-214` `full = (len(unconfirmed) >= max_rows or capacity_requested)`；`trigger = ((ordinal >= replay_cut or full) if replay_cut is not None else ...)` | **`stream_barrier.py` 未派发**；见 Q47 |
| F4 hash/target 路由未接通 | P1 | **已修**：`graph.py:538-539,575,681` 与 `:971-972,1008,1106` 已传 `key_fields/target_classes/target_field` | L017/L018 verify_preserve |
| F5 相同 map 两次 → 重复节点/自环 | P2 | **已修（线性链）**：`planning/dag.py:109 node_id = identity([semantic, index])` | dag.py 属冻结七文件，不改 |
| F6 worker 失败后额度不回收 | P2 | **已修**：`graph.py:892-905 finally → manager.close(...) + reconcile_once`，注释"Only an unconfirmed exit keeps its charge" | L017/L018 verify_preserve |
| F7 / F7b / F7c 依赖、闭包、默认参数未固定 | P1×3 | **已修**：`chain.py:496-514 _by_value_reachable` 已扫 `__defaults__`/`__kwdefaults__`；`:555 _by_value_business_modules`；`:697-716 by_value_udfs` 播种同一 BFS | L014 `verify_loaded_plan` 已派发 |
| F8 加载拒绝未传回 driver | P2 | 已修（历史第三轮已确认） | — |
| F9 按值注册致正常首跑被误拒 | P1 | **已修**：`chain.py:395 _module_registered_by_value`、`:439/:465 return not _module_registered_by_value(...)` | 设计零命中该机制名，但函数未派发→不回归 |
| F10 `sealed_at * 1000` 时间错误 | P2 | 工作台前端项 | 设计 `sealed_at` 零命中；**未覆盖**（低意图影响，可后延） |
| N1 JSON 控制通道改变合法中间值 | P1 | 已修（DataRef 承载） | C05 沿用 BlockDescriptor，方向一致 |
| N2 普通异常只得到超时 | P1 | 已修 | C08 错误传播 |
| N3 失败后不释放额度 | P2 | 已修（同 F6） | verify_preserve |
| **§6 提交崩溃窗口额度重复计费** | **P1，亲跑 17 轮 REFUSED** | **已修，`f83ad3a`（2026-11-13 07:48）已在 bbd93ff 祖先链**：`block_sink.py:169-172`、`chain.py ChainResultSink.prepare`、`lance_sink.py:94-99` 全部改为 `identity({'commit','sink','component','block':ordinal})`；`row_bytes` 预留**从 `_flush` 移入 `prepare`** | **Q003 明文保留**："保持 commit/sink/component/ordinal reservation；同额复用、异额拒绝；不通过退款/换 ID 绕开恢复问题"。`ChainResultSink.prepare`/`lance_sink.py`/`run_journal.py` **均未派发**→不回归 |
| §7(a) 单行上限 | **evidence-only，无需修复** | 公开边界 `check_size` 已强制 | 设计**零** `LocalSegmentStore` 任务→**未把取证项误升级为必修，符合精力分级** |
| 8.5 工件未真正约束业务代码 | P1 | **已修**：`graph_execution.py:63 verify_loaded_plan(plan, plan_ref, sealed_operators)`（20261113 §5 记为待集成，本基线已集成） | L014 已派发 |
| 8.6b `collect_inflight` 随历史增长 | P1 组件级 | **已修**：`effect_keys.py:50 journal.calls(pending_only=True)`，docstring 明确"not the whole completed history" | — |
| 8.6a 逐行 segment write + fsync | 性能/有界项 | **仍在**：`worker/graph_progress.py:176 self._segment_store.write([row], ...)` | **未派发**；见 Q48 |

**基线祖先链已核**：`git merge-base --is-ancestor` 对 `f83ad3a`/`a39a344`/`15823f0`/`bd26c58` 全部为真。
即本设计基线**晚于**20261113 复审快照，历史文档中"待集成/未合流"的多数项已落地。

### Q47 / P0
- **对应 root 与实际用户后果**：P.recovery、P.bounded_resources、R.5.2。R006 把一条**已证明有效的 fail-closed 拒绝**换成尚未证明的新机制，而该拒绝的注释理由正是"graph cut deadlock"——即历史 F3 的同一故障类。若新 adapter 的 ordinal 与 barrier 触发条件不对齐，用户会看到**合法小图在崩溃恢复后卡到超时**，或分组/并发批次的 pending 行丢失/重复。
- **设计文件/任务 ID**：C05；R006（`StreamStageProgress.bind_stages`，implement）、R002/R003（`StageBufferAdapter.align/restore`）、R010/R012（`_Stage.__init__`/`_arun`）；`stream_barrier.py` **无文件负责人、无任务**。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/stream_materialize.py:196-202` 现行 fail-closed：`if (stage.grouping is not None or stage.prefix is not None or stage.max_batch is not None and (stage.concurrency != 1 or stage.flush_interval is not None)): raise RecoveryContractError("Buffered native stage needs its P3 state adapter before partial commits")`，其上注释明文"Concurrent/timed/grouped/prepend buffers hold rows across cuts under window semantics a naive cut snapshot cannot align (**graph cut deadlock** / unproven group preservation); keep the honest rejection rather than fake recovery (S3 boundary)"；`:209-211` 另有 `stage.batch_ordinal = restored["batch_ordinal"]` 与 `restored["data"]["rows"] >= stage.max_batch` 两道恢复守卫，**都在 R006 要重写的同一函数内**。`stream_barrier.py` 从 `self.progress` 读 **8 个属性**：`align`(:65)、`seal`(:75)、`group_ends`(:99)、`run`(:137)、`batch_rows`(:177)、`inputs`(:195)、`ordinal`(:204)、`pending_cut`(:230)，另经 getattr 读 `capacity_requested`/`checkpoint_policy`/`source`。`progress.ordinal` 是**源行序号**（`stream_materialize.py:181-184 select` 每源行 +1，`key < self.inputs` 跳过已提交行）。bbd93ff。
- **最小反例**：R006 卡片要求"用 adapter 替换全局 buffered 拒绝，保留未经适配种类拒绝"。C05 新增 `next_output_ordinal`（**全设计仅 2 次命中，均为 schema 键名列表：`interfaces.pyi:68`、`CONTRACTS.md:130`，无任何语义定义**）。于是同时存在三个 ordinal 空间：源行 `progress.ordinal`、提交位置 `SourcePosition.next_ordinal`、per-stage `next_output_ordinal`，**设计未声明三者映射**。而 `SourcePosition`、`capacity_requested`、`egress_ordinal`、`ChannelSequence` 在设计树**命中数均为 0**；`stream_barrier` 仅 2 次命中且都在 C08 谈 async ACK，**从未涉及 cut 触发条件**。历史 F1 的修复方向原话已警告"**不是简单用源行号替代输出行号（filter/flat-map 的输入输出数量可能不同）**"——这正是 `next_output_ordinal` 与 `progress.ordinal` 必须区分的原因。
- **问题属于**：契约未冻结（ordinal 语义与 barrier 契约）+ 漏任务（`stream_barrier.py` 无主）+ 删除已证明保护而未替代证明。
- **建议的最小修正**：(1) 在 C05 用一段话冻结三个 ordinal 的定义域与换算，明确 `next_output_ordinal` 是 stage 输出计数、**不得**用于 `progress.ordinal >= replay_cut` 比较；(2) 给 `stream_barrier.py` 指定文件负责人，把 8 属性 progress 契约与 F3 联合触发条件（`ordinal >= replay_cut` **或** `full`）写成 verify_preserve 卡；(3) R006 必须保留 `:209-211` 两道恢复守卫，并对"解除拒绝"的每一种 kind 给出跨 cut 崩溃恢复的实际证据，未证明的 kind 继续拒绝。
- **受影响**：R002/R003/R006/R007/R008/R010/R012、`stream_barrier.py`、`StreamMaterialization.__init__/seal/_commit`（均未派发但是契约对端）。
- **应新增或保留的验证**：复跑历史 F3 场景（9 行、1 下游、`cut_rows=6`、`checkpoint_unconfirmed_rows=4`、identity 算子）在 grouped/concurrent kind 上不超时；filter/flat-map 使输入输出行数不等时恢复归属不变；`restored["data"]["rows"] >= max_batch` 仍拒绝。
- **是否阻塞**：阻塞编码与装配。**本项同时提升 Q03 的确信度**——Q03 指出 `stream_barrier.py` 无主，Pass 3 独立确认其为 F3 已修保证的唯一持有者。

### Q48 / P2
- **对应 root 与实际用户后果**：P.bounded_resources、P.performance。逐行新建 generation + 文件 + 目录 fsync，inode 与同步 I/O 随总行数线性增长；大数据量下磁盘峰值与耗时不可控。
- **设计文件/任务 ID**：无（`worker/graph_progress.py` 未派发）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/worker/graph_progress.py:176 self._segment_store.write([row], scope_ref=self.occurrence_id, ...)`——每行一次调用；历史 20261110 §8.6a 已记录同一事实，本基线未变。bbd93ff。
- **最小反例**：N 行 → N 次 `write([row])`；`LocalSegmentStore` 的块阈值合并能力在该调用形态下不生效。
- **问题属于**：漏任务（历史已记录项未纳入本轮范围）。
- **建议的最小修正**：**按精力分级可后延**，但需在设计中显式声明"本轮不改逐行 segment write，性能未验收"，避免"有界资源已闭合"的整体宣称覆盖此项。
- **受影响**：`graph_progress.py`、`exchange/carriage_local.py`。
- **应新增或保留的验证**：若后续处理，用可控小预算证明 inode 不随 N 增长（历史 §7 bounded-scratch 测试已有同款断言可复用）。
- **是否阻塞**：不阻塞编码；阻塞"性能/有界资源整体完成"的宣称。

### Q49 / P2
- **对应 root 与实际用户后果**：P.bounded_resources。§6 修复把 `row_bytes` 预留粒度从"每次 flush 一条"改为"**每 commit 每 block ordinal 一条**"，预留条数随 cut 数 × 块数增长；触及上限后合法输出被拒。
- **设计文件/任务 ID**：C06（Q 负责资源/预算）；`runtime_reservation_count` 在设计树零命中。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/run_journal.py:1013-1015`：`count = db.execute('SELECT items FROM runtime_reservation_count WHERE singleton=1').fetchone()`；`if count + len(pending) > 100000: raise RecoveryContractError('Reservation count exceeds budget')`。`block_sink.py:167-172` 每 block 一条 `row_bytes` + 每 commit 一条 `output_rows`；`BLOCK_BYTES = 4MiB`。bbd93ff。
- **最小反例**：约 9000 个 cut × 每 cut 10 块即逼近 100000 条上限（纯算术推导，**我未实跑该规模**，故定级 P2 并标注为推导而非实证）。
- **问题属于**：契约未冻结（新增上限未命名，与 Q41 同族）。
- **建议的最小修正**：在 C06 与 BOUNDS 门列出 `runtime_reservation_count ≤ 100000` 及其增长变量（cut 数 × 块数），与 `MAX_STATES=256`/`MAX_SINKS=64`/`max_state_bytes=16MiB` 一并命名。
- **受影响**：Q003、BOUNDS 门、长 run 恢复验收。
- **应新增或保留的验证**：多 cut × 多块的预留计数上界断言；不必实跑 9000 cut，可用小 ceiling 等价触发。
- **是否阻塞**：不阻塞编码。

### Pass 3 的自我更正（避免过度声称）
1. **派发文件数是 45，不是我此前记的 44。** 直接程序化统计 `plan.json` 的 `file` 字段去重得 **45**，与设计正文一致。此前 44 是我对 markdown 正文做正则抽取的产物（漏了带连字符文件名），**我的计数错误，设计正文正确**。已通知 Pass 5 以 `plan.json` 为准。
2. **§6 修复并未被设计忽略。** 我最初的标识符扫描（`reservation_id`/`clean_orphans`/`runtime_reservations` 命中 0）会误导出"设计无视刚提交的 P1 修复"这一错误结论。实际 **Q003 用自然语言明文保留了该不变量**（"commit/sink/component/ordinal reservation；同额复用、异额拒绝"），且 `run_journal.py:1004-1005` 确实对同 id 异额抛 `Budget reservation changed`——**卡片表述与源码一致**。另两个被同一 commit 修改的 sink（`ChainResultSink.prepare`、`lance_sink.py`）未派发，因此不回归。
3. **C05 比我此前代表的更完整。** 它已冻结 `next_batch_ordinal`/`prefix_position`/`flush_remaining_ms`/`pending_batches.{input_ordinals,output_ordinal,effect_keys,status}`、"inflight/unknown 不能伪装这两个状态"、"不保存本机 monotonic 绝对值"、旧 fixed-batch 兼容、cut/恢复顺序、以及"已有 native row-local adapter 的 fields/methods 保留"。Q02/Q03 的争点**不是键集缺失**，而是**这些键在 `_Stage` 上无持有者**（Q02）与**barrier 聚合点无主**（Q03）——两者经 Pass 3 复核仍然成立，但不应被读成"C05 什么都没定"。

### Pass 3 同时确认的正面事实（不构成发现）
- **C08 的基线事实已核实**：`stream_barrier.py:83 inspect.isawaitable(result)` 确实存在，故 C08"无需再造线程提交器"的判断成立。
- **verify_preserve 用得准**：graph 侧四个承载 F1/F2/F4/F6 已修保证的函数（`run_local_graph`/`run_ray_graph`/`_load_plan`/`start`/`_run`）全部是 verify_preserve，不是 implement。这是本设计**最正确的一处保守选择**。
- **§7(a) 取证纪律被遵守**：设计零 `LocalSegmentStore` 任务，没有把裁决为 evidence-only 的项升级为必修，符合真人"精力按意图相关性分级"。
- **代码固定机制未被重写**：`_is_by_reference_udf`/`_by_value_reachable`/`_module_registered_by_value` 所在函数均未派发，F7/F7b/F7c/F9 的修复不会因本轮实现而回归。

---

## 三之四、Pass 6 追加发现：20 意图根 → 卡 → 门 → 命令 → 断言的闭合审计（Q50–Q51，并复算确认 Q06）

本轮按工单要求逐层程序化核对四类断裂：**有卡无门 / 有门无命令 / 有命令无断言 / 断言不触达意图**。
前三层的**结构**闭合得出乎意料地好；断裂集中在第四层——**命令跑不到新断言**。

### 闭合审计结果（程序化，可复算）

| 层 | 检查 | 结果 |
|---|---|---|
| 根→门 | `root-map.json` 20 根引用的门是否都在 `gates.json` | **0 缺失** |
| 门→根 | 27 门是否都被至少一个根引用 | **0 孤儿门** |
| 门→卡 | 每门 `leaf_tasks` 是否都在 `plan.json` 144 任务内 | **0 未知任务**；无空 leaf 门 |
| 门→既有测试 | 64 个 `required_existing_files` 是否在 bbd93ff 真实存在 | **64/64 存在，0 缺失** |
| 门→命令 | 27 门是否都在 `TEST-COMMANDS.md` 有 bash 命令 | **27/27 有命令** |
| 命令→新断言 | 命令是否包含 21 个新验收测试所在文件 | **0/27 包含 → Q06（P0，Pass 1 已报，本轮复算确认）** |
| 断言→意图 | 新断言是否真能判定意图达成 | 多数强；ADMISSION 缺路径身份 → Q50 |
| 纪律 | `--basetemp=/tmp/demiflow-v2-<gate>`、`PYTHONPATH="$PWD"`、`-p no:cacheprovider`、不在主仓跑 | **齐备**（basetemp 75 处命中），且明文"不是已通过报告" |

### Q06 的独立复算确认（**不是新发现**，Pass 1 已报；此处给出精确计数与更强修正）
- **为什么重列**：Pass 1 的 Q06 已指出"21 个新验收函数没有任何一条门命令会执行它们"。Pass 6 用**程序化逐节抽取**独立复算，确认该结论**完全成立**，并把它从"文档阅读结论"升级为**可复算的机器事实**。为避免虚增发现数，本项**不另编号**。
- **精确计数（可复算）**：按 `^## ` 切分 `TEST-COMMANDS.md` 得 **27 节**，逐节抽取 ```bash``` 块内 `tests/*.py`：**27/27 命令只列既有测试文件**；`test_upgrade_v2_acceptance` 在 `## ADMISSION` 之后的全文命中 **0**。`gates.json` 中 **20 个门**声明 `new_acceptance_tests`（共 21 个函数名），其自身命令**全部**选不中。V001/V013 卡内亦**无任何 pytest 命令**。
- **两个具体样本**：ADMISSION 命令 = `pytest ... tests/test_native_delivery_barrier.py tests/test_stream_admission.py`（叶任务含 V001）；WORKBENCH 命令 = `pytest ... tests/test_workbench_sealed_facts.py tests/test_forge_workbench_config_run_browser.py tests/test_forge_workbench_runtime_browser.py`（叶任务含 V013）。
- **设计自身的矛盾**：`TEST-COMMANDS.md:7` 明文"新验收文件 `tests/test_upgrade_v2_acceptance.py` 尚待 V 实现，**不能因为不存在而跳过验收**"——**提出了要求却没落进任何命令**，机械执行下必然被跳过。
- **比 Pass 1 更强的修正建议**：除追加 `-k`/`::` 选择外，在 `tools/check_design.py` 增一条**静态校验**：每个门的 `new_acceptance_tests` 必须能被该门命令选中，否则设计校验非零退出。文字要求会被忽略，静态校验不会。
- **更强的验证要求**：修正后**故意让一条新断言失败**，确认对应门**变红**。当前装置下它必然仍绿——这才是"全绿≠意图达成"的直接证明。
- **是否阻塞**：**阻塞分发**（Pass 1 原判为"阻塞最终验收"，Pass 6 上调：因为它使**全部 20 根**的验收证据失效，不只是延迟验收）。

### Q50 / P1
- **对应 root 与实际用户后果**：P.unified_execution、R.2.2、R.5.1。新准入的 take/take_all/count/write 终端即使**结果正确**，也可能全部走 legacy in-driver 路径；用户与设计都以为"统一计划驱动执行"已达成，实际没有。这正是架构 U5 退出条件"**没有无声退回**"要禁止的情形。
- **设计文件/任务 ID**：C10；V001（ADMISSION 唯一新验收测试）；A002/A005/A006；`root-map.json` 的 `P.unified_execution → [LAYOUT, GRAPH, QUEUE, RAY, STREAM]`（**不含 ADMISSION**）。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/chain.py:310-329 unified_chain_eligible` 是 unified 与 legacy 的**唯一分流缝**，失败时仅 `logger.debug(...)`（默认日志级别下**静默**）后 `return False`。全仓 `grep -rn unified_chain_eligible` 只有**一个调用点**：`execution/executors/local.py:863`，位于 **`materialize_stream`（:853-882）**内。`iter_rows(:747)`、`iter_batches(:754)`、`count(:975)`、`take(:983)`、`take_batch(:995)`、`materialize(:817)`、`aggregate`、`write_*` **都不查该缝**，直接走 `_apply_plan`。设计树中 `unified_chain_eligible` 命中 **0**、`legacy_fetches` 命中 **0**。bbd93ff。
- **最小反例**：V001 的断言全文是"合法 thread map 的 take/take_all/count/write 各自结果正确；真正无效选项拒绝前 events 为空；take(0) 不读源"——**没有任何一句断言实际走了 unified 路径**。结合 Q27（take/iter 无 RunAction）、Q28（count 病因错）、Q29（`_apply_plan` 无主），V001 在**完全未实现统一执行**的情况下也会绿。对比 V010（LAYOUT）就有路径身份断言："**改变合法封存预算能影响执行**"——同一份设计里已有正确写法，只是没用到 ADMISSION。
- **问题属于**：**断言不触达意图** + 契约未冻结（分流缝未命名）。
- **建议的最小修正**：(1) V001 增一句路径身份断言，复用仓内既有惯例（历史 §7 裁决用的 `legacy_fetches == 0 and unified_fetches == 0`），或断言"改变封存预算能影响 take/count 的实际执行"；(2) 在 C10 命名 `unified_chain_eligible` 为唯一分流缝，并规定**新准入终端必须经该缝**、fallback 不得只在 debug 级别可见；(3) 把 ADMISSION 加入 `P.unified_execution` 的门集合，或明确声明本轮 take/count 仍走 legacy 且不宣称统一。
- **受影响**：V001、A002/A005/A006、`executors/local.py` 的 9 个 `_apply_plan` 调用点、`root-map.json`。
- **应新增或保留的验证**：take/count 在 unified 与 legacy 两路下计数可区分；人为让 plan 不可 pickle 时 fallback **可观测**且不冒充统一完成。
- **是否阻塞**：阻塞"统一执行已达成"的宣称；不阻塞其余包编码。

### Q51 / P2
- **对应 root 与实际用户后果**：P.recovery、P.unified_execution、P.resources_performance。7 个门共承载 **39 个 implement 任务**却**零新验收测试**，只靠既有测试文件；这些文件早于本轮新增 schema（如 `demiflow.stage_buffer.v1`），因此"既有测试绿"不等于新机制被验收。
- **设计文件/任务 ID**：`gates.json` 的 DELIVERY(5 implement)、FACTS(3)、PREPEND(1)、QUEUE(3)、RECOVERY(14)、STREAM(5)、UNKNOWN(8)；合计 39。
- **实际源码文件/符号/所审 commit**：RECOVERY 既有文件 = `tests/test_u2_chain_faults.py`、`tests/test_u3_graph_faults.py`、`tests/test_native_barrier.py`（均存在于 bbd93ff，但均早于 `stage_buffers.py`——该文件在设计中是**新增**，基线不存在）。
- **最小反例**：RECOVERY 的 14 个 implement 任务引入新快照 schema，其 3 个既有测试文件无法引用该 schema。实际覆盖只能经 `P.recovery → BUFFER(V008)/WRITE(V011)/HISTORY(V020,V021)` 的**门并集**取得，而 `gates.json` **未声明这种并集关系**。
- **问题属于**：验收口径未声明（不是断言缺失——并集里确有强断言）。
- **建议的最小修正**：在 `gates.json` 或 `INTEGRATION-ACCEPTANCE.md` 显式写出"根本意图由门并集判定"，并对这 7 个门标注"本门既有测试只保回归，新语义由 X 门的新测试判定"。**按精力分级不必为每门新造测试**，避免无必要重复。
- **受影响**：7 个门的实现者对"何时算完成"的判断。
- **应新增或保留的验证**：无需新增测试；只需并集声明 + Q06 修正后命令真能跑到并集中的新测试。
- **是否阻塞**：不阻塞编码。

### Pass 6 同时确认的正面事实（不构成发现）
- **根→门→卡→既有测试四层结构完全闭合**：20 根、27 门、0 缺失门、0 孤儿门、0 未知叶任务、**64/64 既有测试文件真实存在**。这是我审过的设计里结构最完整的一份，`check_design.py` 的静态校验方向正确。
- **21 张 V 卡的断言高度贴合意图**，不是泛泛"结果正确"：V002"真进程退出 resume 累加不归零"、V005"同 cut 多次 prepare+crash/resume 字节与额度不漂移"（正是 §6 裁决门 1/2）、V007"concurrency=2/max_batch=2 不强制降为 1"、V009"两个真 crash 窗口 unknown 零重发"、V012"worker 不拥有 CommitCoordinator，driver 唯一提交"、V019"Ray 不可用则明确未验，不得 skip 计通过"、V021"只修改右子 plan 代码必须拒绝 resume"。**问题不在断言写得差，而在 Q06 让它们跑不到。**
- **测试纪律齐备且诚实**：`TEST-COMMANDS.md` 开头明文"本文件列未来验收命令，**不是已通过报告**"；`--basetemp=/tmp/demiflow-v2-<gate>` 逐门隔离；`INTEGRATION-ACCEPTANCE.md` 明文"skip 不计通过"、"mock interpreter/journal/commit 得到的绿色不算集成通过"、"不能提前承诺全局 100%"。
- **117 失败分工被保留为历史输入而非验收范围**：§4 明文"不能只跑旧失败清单"，并要求把原已通过但受影响的用例纳入——口径正确。

---

---

## 三之五、Pass 7 追加：基线摘要机器复算与冻结边界审计（Q52）

本轮不复述文档，而是**独立重算**设计的基线摘要装置，并把冻结边界与既有总控裁决对照。
结论：**摘要装置是我审过的设计里最可靠的一处（全对）**；**冻结边界的声明与守卫则缺失**。

### 机器复算结果（我亲自跑，脚本 `/tmp/demiflow-qwen-pass7/verify_baseline.py`）

用 `ast.get_source_segment` + `sha256` 独立重算 `plan.json` 全部 144 任务的基线三元组，与 bbd93ff 逐条比对：

| 项 | 结果 |
|---|---|
| 有基线的 Python 卡 | **96** 张 |
| 基线函数 SHA-256 匹配 | **96/96**，0 不符 |
| 基线行号匹配 | **96/96**，0 不符 |
| 基线签名匹配 | **96/96**，0 不符 |
| 声称"新增无基线"的卡 | **42** 张；**42/42 在基线确实不存在**（无一张谎称新增） |
| JavaScript 卡 | 6 张（W016–W021），另计 |
| `baseline.json` 文件级 sha256 | **39/39 匹配，0 drift，0 缺失** |

**这直接推翻了我可能给出的一个负面猜测**：独立 agent 完全可以靠卡片里的三元组发现漂移，装置是真实有效的。
合计 96+42+6 = **144**，与任务总数一致。

### Q52 / P1
- **对应 root 与实际用户后果**：R.5.1、R.5.3、P.correctness。实现者**无法知道哪六个文件字节冻结**，也没有任何机器守卫会在改动时发现；同时设计与既有总控裁决的"不改"边界交叉，却未声明取代关系。两个 agent 各自理解边界，合流时才发现动了不该动的承重文件。
- **设计文件/任务 ID**：`CONTRACTS.md:113`（C04 末句）；`tools/check_design.py`；`baseline.json`；R011（`stream.py:_materialize`，implement）、S013（`recovery_identity.py:describe`，implement）、S009/S010（`contracts/operator.py`，implement）。
- **实际源码文件/符号/所审 commit**：
  - `CONTRACTS.md:113` 原话："S 可提出 recovery_identity.py 的必要最小修复……**其余六个承重文件字节冻结**。"——**设计树全文没有任何一处枚举这六个文件**（`grep -rn "字节冻结\|frozen_files\|byte.frozen"` 仅命中该句本身）。
  - `tools/check_design.py` 中 `frozen`/`FROZEN` 命中 **0**；`baseline.json` 的 39 文件里，`raw_fetch.py`/`queue_execution.py`/`queue_sinks.py`/`queue_source.py`/`planning/dag.py`/`contracts/exchange.py` **全部 ABSENT**；唯二在列的承重相关文件 `recovery_identity.py`、`contracts/operator.py` **恰好正是被派发 implement 的两个**。即守卫覆盖的正好是允许改的，不覆盖的正好是禁止改的。
  - 既有裁决 `unified_execution_architecture_20261107/p1_l2_plan_interpreted_20261112/design_refined_verdict.md:49` 明文："**冻结/承重零触碰 APPROVE**：contracts K1–K6／dag.py 核心／承重四件字节不变／**describe 不改**／**stream.py（_materialize/run_stream）唯一引擎不改**／workbench/forge 不改。"
  - bbd93ff 侧已核：`recovery_identity.py`、`stream.py`、`contracts/operator.py` 均存在且被派发。
- **最小反例**：实现者读 v2 设计，看到 C05"R 拥有 stream.py、stream_materialize.py 的接入"与 R011 `_materialize` = implement，于是改 `_materialize`；另一位实现者读既有 L2 裁决"stream.py（_materialize/run_stream）唯一引擎不改"，于是拒绝该补丁。**两份都是有效权威，设计没有声明谁取代谁。** 同时任何一方改 `queue_sinks.py` 都不会被 `check_design.py` 或任何门发现。
- **问题属于**：契约未冻结（冻结集合未枚举）+ 验收缺口（无机器守卫）+ 边界取代未声明。
- **建议的最小修正**：(1) 在 C04 或 `FILE-OWNERSHIP.md` **逐名枚举**六个字节冻结文件；(2) 在 `baseline.json` 或独立 guard 文件加入这六个文件的 sha256，并让 `check_design.py` 校验（**成本极低，39 文件的机制已存在，只是没放进去**）；(3) 用一句话显式声明"本设计取代 L2 裁决对 `stream.py:_materialize` 与 `contracts/operator.py` 的不改约束，范围限于 R011/S009/S010 的冻结签名"，`describe` 的解冻已由 C04:113 声明、无需重复。
- **受影响**：`check_design.py`、`baseline.json`、R011/S009/S010/S013、全部实现者的提交前检查。
- **应新增或保留的验证**：合流时跑冻结守卫并记录六文件摘要；故意改一个冻结文件确认守卫**变红**。
- **是否阻塞**：阻塞合流验收；不阻塞多数包编码。

### Pass 7 同时确认的正面事实（强，且改变我对设计质量的评估）
- **基线三元组 96/96 全对、42/42 新增声明诚实、39/39 文件摘要零漂移**（上表）。这是**机器可核的事实**，不是文档自述。它意味着"按卡独立编码 + 漂移可检测"这条工单前提在**摘要层面确实成立**。
- **冻结签名与基线签名被正确区分**：L010 冻结 `execute(..., execution=None)` / 基线 `execute(executor, source, plan, *, arrow=False, sink_schema=None)`（实际 `dataset_native.py:551` 确无 `execution` 参数）；L036 同理（`Engine.__init__` 基线 :104 确无该 kw）。**设计没有把目标签名谎称为基线事实**——这是我原本准备重点检查的失真形态，未发现。
- **C12 的缺陷诊断准确**：`Engine.batch_rows`（`dataset_native.py:125`）确实 `return min(self.session.options.batch_rows, max(1, (8*2**20)//max(1, rel.payload_row_bytes)))`，L038 卡片"单行超限拒绝，不能 max(1) 掩盖字节超限"**精确对应真实代码**。
- **§6 保护再加一层**：C06 明文"**不得通过取消 `Budget reservation changed` 守卫解决漂移**"，与 `run_journal.py:1004-1005` 的 `raise RecoveryContractError('Budget reservation changed')` 精确对应。加上 Q003 的 component/ordinal 保留要求，**§6 裁决的不变量在设计里被两处独立保护**。（这进一步更正我 Pass 3 初查时"标识符零命中"的误导。）
- **承重四件与 `planning/dag.py` 确实零派发**：`raw_fetch.py`/`queue_execution.py`/`queue_sinks.py`/`queue_source.py`/`planning/dag.py` 均不在 45 个派发文件中。**最关键的队列承重边界被实际遵守**，不只是口头承诺。
- **C08 的 `inspect.isawaitable` 基线事实已核**（`stream_barrier.py:83`），故"无需再造线程提交器"成立。

---

## 三之六、Pass 4 并入：144 卡逐张核对（子审查交付，我对决定性结论逐条复核）

Pass 4 子审查已交付，产物在 `/tmp/demiflow-qwen-pass4/`（`report_body.md` 15 条发现 P4-01–P4-15、
`plan_table.tsv` 144 行覆盖表、`sigcheck.json`、`raises.json`、`verify_cards.py` 等）。
**它的覆盖声明**：`verify_cards.py` 证明 144 张卡全部是 `plan.json` 的忠实渲染（0 差异），故读 plan.json 全量 = 逐卡读完；
caller/callee 双向核对 **0 断裂、0 重复认领、0 一文件多 owner**；未跑 pytest，行为结论来自逐行读源码。

### 我亲自复核并确认的 Pass 4 决定性发现

**P4-02 / P4-03（卡内参数语义与源码直接矛盾）——我在行级确认，并把范围收窄到恰好 3 张卡。**
这是 Pass 1–3 我没发现的一类缺陷：`tools/build_plan.py` 的 `PARAMS` 描述模板按参数名复用，撞进了语义不同的同名参数。

| 卡 | 卡内参数描述 | 源码事实（bbd93ff） | 判定 |
|---|---|---|---|
| **R010** `_Stage.__init__` | `execution` = "ExecutionPlacement 或 None；绑定调用不得为 None" | **同卡**冻结/基线签名即 `execution='inline'`；`stream.py:105` 默认 `'inline'`、`:262 if stage.execution=='inline'`；`data/plan.py:89-90` 校验 `not in ("inline","thread") → ValueError("map execution must be 'inline' or 'thread'")` | **矛盾成立**，按卡实现会破坏 `.map(execution='thread')` |
| **W001** `read_sealed_plan` | `target` = "CallableSpec.target；函数、类工厂或实例；编译阶段不可构造" | `workbench/runs/facts.py:96` docstring 明文 "``target`` is the run root (``manager.directory``)" | **矛盾成立** |
| **W002** `read_identity_bridge` | 同上 | 同一文件同一约定 | **矛盾成立** |

**我做的范围收窄（Pass 4 未给全量计数）**：程序化扫描 144 卡的 `input` 字段——
`execution` 描述为 ExecutionPlacement 的共 **5** 张（R010/L010/L011/L036/L039），其中 **只有 R010 错**，
L010/L011/L036/L039 是契约明文允许的新增 `execution=None`，**正确**；
`target` 描述为 CallableSpec.target 的共 **4** 张（S001/S009/W001/W002），其中 **S009 正确**
（`contracts/operator.py:67-73 _adapter_present(target)` 确实按 function/method/instance 处理），
S001 的文件 `operator_policy.py` 在基线不存在（属 42 张新增卡之一）。
**故确认缺陷恰好 3 张卡：R010、W001、W002。修正成本极低（改三行描述），但不改会直接写错实现。**

**P4-13（基线摘要不含装饰器）——我实跑确认，并据此限定我 Pass 7 的正面评价。**
`ast.get_source_segment` 对 FunctionDef 从 `def` 行起切片，**不含装饰器**。实测：
`data/dataset.py:2007 Dataset.count` 的装饰器是 `_flow_api('count', datasets=('self',), role='query', execution='action')`，
而切片首行是 `def count(self) -> int:`，装饰器文本**不在切片内**。
该装饰器设置的 `__demiflow_flow__` 被 `forge/platform.py:226` 与 `:360`（`vars(value).get("__demiflow_flow__") != record["native_semantics"]`）消费。
**后果**：A003/A004 的实现者若丢掉 `@_flow_api`，**基线函数 SHA 仍然匹配**、卡片不提示装饰器、没有任何门会发现，
直到 Forge 侧语义校验失配。**所以我 Pass 7 说的"摘要装置可靠"必须限定为：函数体级可靠，装饰器级不覆盖。**
同类受影响卡（Pass 4 列出，我未逐一实测）：M010/L037 `@property`、A003/A004 `@_flow_api`、
W008/W009/W012 `@operation_budget.controlled`、R013/Q005 `@contextmanager`。

**P4-15（44 vs 45 文件口径）——已解决，且证实我 Pass 3 的自我更正方向正确。**
`plan.json` 去重 `file` = **45**，其中唯一测试文件是 `tests/test_upgrade_v2_acceptance.py`。
**45 = 44 个生产/源码文件 + 1 个共享验收测试文件**。设计正文的 45 正确；"44"是生产文件口径。
我此前正则抽取得 44 是**我的抽取缺陷**，不是设计错误。

**P4-01 / P4-10（count 嵌套与 stream_delivery 无主）——与我 Q28/Q29 同源，Pass 4 给出更精确的落点。**
Pass 4 把嵌套的真正发生点定位到 `execution/stream_delivery.py:33 execution_scope()`、`:41 run.action`，
并确认 `stream_delivery.py`、`executors/base.py:69 count`、`local.py:975 count`、`local.py:747 iter_rows`
**全部不在 45 派发文件内**；`iter_stream_rows(executor,source,plan,*,limit=None)` 也**没有 decision/layout 形参**，
A006/A007 声称的 callee 无法接收 ActionDecision/ExecutionPlacement。
**这比我 Q28 的表述更精确，我采纳并以此为准**：修复点不是"第二个公开 action"，而是 `stream_delivery.py:41`，且该文件无主。

### Pass 4 与我既有发现的重合（不重复计数）
- P4-06（auto_json concurrency 准入无实现者、`OperatorTraits` 无 concurrency 字段）：Pass 4 补了 `planning/model.py:51` 的字段缺失证据，**Pass 10 我实测再次确认 11 个字段无 concurrency**。**本条独立成立，不随 Q04 撤回而失效**——C04 明文"声明更高并发时准入明确拒绝，不偷偷降并发"，而 144 卡无一实现该拒绝。
- P4-07 / P4-08（冻结 DAG 边端点键集无 occurrence；`_physical_summary` 未派发且按 logical 折叠）≈ 我 Q07/Q14/Q15，Pass 4 补了 `facts.py:247/:255` 的具体折叠点。
- P4-12（S007/S008 标"只操作本对象"但实际 `owner.bind_execution` / 改借来的 `budget.snapshot_bytes`）≈ 我 Q34 的 L034/L035 同类，是新实例。

### 我追加独立复核的三条（原记"未复核"，本轮已回到源码与契约行级核实）

子审查交付全文后，我把其中三条**阻塞级**结论亲自复核完毕。事实全部成立，但**两条的严重度我按意图影响改判**，一条我做得比子审查更窄更硬。

#### P4-06 / P1 — 确认成立，且证据比子审查更硬：C04 的 auto_json 并发准入规则零实现者

- 契约原文 `CONTRACTS.md:98`：「auto_json初版要求同owner调用并发为1；**声明更高并发时准入明确拒绝，不偷偷降并发**」。
- 源码 `execution/operator_precheck.py:17 precheck_operator_state(callable_spec, traits)`：全函数只读 `traits.get("state_ownership", "none")`（`:19`），其余逻辑是 `_adapter_present` 与实例 `__dict__` 探测；`:32 precheck_plan_state(plan)` 在 `:39-40` 只传 `{"state_ownership": getattr(traits, "state_ownership", "none")}`。**两个函数都收不到 concurrency，也没有任何并发判定分支。**（我读了该文件全 40 行。）
- 我程序化扫全 144 卡：提到 `auto_json` 的只有 **7 张**（S001/S004/S006/S007/S008/S009/V003），**无一提到并发=1 或拒绝更高并发**。提到 `concurrency`/`并发` 的只有 **4 张，且全部属于别的域**：R006 是禁止语「不改 batch/concurrency 凑绿」；R010 与 M009 是 `_Stage.__init__` / `EmbeddingActor.__init__` 的形参说明（`int>=1`、计入驻留预算）；V007 是 **embedding** concurrency=2 的恢复测试，与 auto_json 无关。
- S004 卡的 traits 输入枚举为 `state_ownership/cardinality/ordering/effect_kind/batching`——**无 concurrency**；S005 卡 behavior 只说「遍历真实 plan callable，用同一 policy；不建 actor，不做任何源/网络 IO」。
- **意图相关性（为什么这条不能后延）**：auto_json 正是历史 E 项「`1d7b350` 把无 hook 当无状态」的修复机制，而"同 owner 并发=1"是它的**安全包络**。C04:98 同句还规定 auto_json 默认 `shared_colocated`。缺这条准入拒绝，共享 owner 的普通数据实例可在并发>1 下被放行 → 跨并发调用状态互踩，恰好回到用户最关心的"普通类实例真崩溃恢复"失效形态。**阻塞装配，P1 成立。**
- 最小修正：把 `MapStreamOptions.concurrency` 与 callable_scope 接进 precheck（S004/S005 的 traits 需增字段，`planning/model.py:51 OperatorTraits` 亦需增），并在 V 包加一条「auto_json concurrency=2 准入拒绝、且不静默降为 1」的用例。

#### P4-04 / 子审查 P1 → 我改判 **P1（3 卡）＋ P2（18 卡）**

- 契约原文 `CONTRACTS.md:236`：「durable_write **必须写明**原 RunJournal/commit/StateArtifacts 中的 owner」。
- 我程序化复核 21 张 durable_write 卡（A008/R002/R007/R013/Q003/M007/L004/L007/L012/L015–L019/L030/W007–W009/W011–W013）：**每卡恰好 1 行提到这三个 owner，21 行全部是通用模板行**「durable_write 必须通过原 RunJournal/CommitCoordinator/StateArtifacts」；**非模板的具体 owner 指定 = 0**。子审查计数准确，契约字段确实缺失。
- 但"21 卡全部阻塞装配"过度。我按 `mode` 与有无基线拆分：
  - **13 张 verify_preserve**：写路径已存在于冻结代码，owner 由基线唯一确定；卡又明令「只修改这个函数/方法」「沿用本函数现有依赖，不新增跨层执行权威」→ 实际是**文档字段缺失，P2**。
  - **5 张 implement 但有基线**（R007:334、L004:178、L012:1187、A008:1171、M007:148）：behavior 已用散文钉住唯一权威——L004「**不创建第二状态库**」、A008「**唯一 LanceSink 提交**；恢复复用旧绑定」、R007「原 native row-local 快照与 buffer 快照及 Actor 状态**同一 cut**」、M007「同 request_key 查已知响应…**fresh/replay 不双计费**」→ owner 可推定，**P2**。
  - **3 张 implement 且无基线（全新函数）**：**R002 `StageBufferAdapter.align`**（`execution/stage_buffers.py` 在 bbd93ff **不存在**，我已核）、**L007 `compile_native_topology`**、**L030 `stage_native_programs`**。这三处产生**新的持久状态路径，没有可继承的既有 owner**，C10 的必填要求实质未满足 → **P1，阻塞装配**。
- 这个收窄与我的 Q02/Q03 同源：R002 正是"快照字段无持有者"那张卡。**补 owner 与补持有者是同一件事，不应算两处独立工作量**；L007/L030 的 owner 问题则与 Q09（封存权威 pickle vs code-pin）是同一个决策的两面。

#### P4-05 / 子审查 P1 → 我改判 **P2**

- 事实确认：`operator_llm/stream_recovery.py:57 check_recovery` → `:61 with self._lock:` → `:62 db = self._connect()` → **`:63 db.execute('CREATE INDEX IF NOT EXISTS native_unresolved_calls ON calls(request_key) WHERE response_json IS NULL')`**。确为对 journal SQLite 的 schema 写入。M002 卡标 `副作用类别：read_only`，而模板行写「pure/read_only 不允许构造用户 Actor/读源/**写 journal**」→ **标签与源码矛盾，子审查成立**。
- 改判理由（按意图影响，不按标签洁癖）：
  1. 该卡是 **verify_preserve**，卡首行即「只修改这个函数/方法」，agent 的默认动作是保留而非重写；
  2. `CREATE INDEX IF NOT EXISTS` **幂等**，且它是 `:64-67` 那条 `SELECT request_key … WHERE response_json IS NULL … LIMIT 1` 的**支撑索引**——删掉它只让启动检查退化为全表扫描，**不改变 `UncertainPromptCall` 的判定语义**，即不触碰「unknown 不自动重发」这条 root；
  3. 函数注释 `:58-59` 明写「One indexed startup check, before any new model dispatch」，索引本就是设计意图的一部分。
- 所以真实风险是**分类学缺口**（幂等 schema 初始化在四值枚举里无合适标签）＋ agent 可能因标签自相矛盾而卡住或误删索引致启动变慢，**不是恢复语义破坏**。装配前顺手改标签即可：M002 标 `runtime_state` 并注明「幂等启动索引，owner＝RunPromptJournal 自身 shard」，或在 C10 增设「幂等 schema 初始化」豁免。

### 我仍未独立复核的 Pass 4 结论（如实标注，不采信为已核）
P4-09（三个 `start` 卡称"拒绝"实为静默 return）、P4-11（27 卡 errors 漏列函数体直接 raise）、P4-14（W020 的 JS 切片延伸到 EOF）。
这三条**证据形态可核、脚本在 `/tmp/demiflow-qwen-pass4/` 可复算**，但我未逐条回到源码行级复核，按工单口径记为"子审查报告、主审未复核"，不并入我的已核证据账。
其中 P4-09 与我已亲自核过的源码事实方向一致（`worker/graph_execution.py:89 if self._thread is not None: return` 确为静默 return，见 restored 源码片段），但我未逐一核对另两个 `start` 与三张卡的返回描述，故仍不计入已核。

## 三之七、Pass 8：Q27 / Q50 的第一手运行时实证（我亲自跑，不再是静态推断）

前七遍里 Q27/Q50 的证据形态是**行级源码推断**。本轮我在隔离树 `integration_c_20261113`（bbd93ff，跑前跑后均 `git status --porcelain` 为空）上，
以 `/tmp/demiflow-qwen-pass8/run` 为工作目录亲自执行运行时探针，把这两条升级为**实测事实**。
解释器 `/yzp/zhaozy/yangzepeng/0905/demiflow-env/bin/python -B`，脚本首行断言 `demiflow.__file__` 必须落在隔离树内（已打印确认），
**主仓未跑任何测试、未改任何文件**（主仓 HEAD `e40a6dd`，11 项既有 dirty 原样保留）。

### 实测结果（脚本 `/tmp/demiflow-qwen-pass8/probe_routes.py`，可复跑）

探针手法：包裹 `Dataset.take/iter_rows/count` 记录进入时的 `current_action()` / `current_run()`；
同时给 `chain.unified_chain_eligible`（unified↔legacy 唯一分流缝）与 `LocalDatasetExecutor._apply_plan` 装计数器。
计划为 `from_items(2 行).map(lambda …)`，`local_execution(workers=1, worker_mode='thread')`。

| 观测项 | 实测值 |
|---|---|
| `take(1)` 结果 | `[{'k':1,'v':1,'w':2}]` — **正确** |
| `iter_rows()` 结果 | 2 行，`w` 分别为 2/4 — **正确** |
| `count()` 结果 | `2` — **正确** |
| `take` 进入时 `current_action()` | **`None`** |
| `iter_rows` 进入时 `current_action()` | **`None`** |
| `count` 进入时 `current_action()` | **`None`** |
| 三者 `current_run()` | **均已设置（True）** |
| `unified_chain_eligible` 被调用次数 | **0** |
| `_apply_plan` 被调用次数 | **3** |

### 这两条实测把结论钉死

**Q27 由推断变为实证，且比原描述更精确。** 三个终端**有 Run、没有 RunAction**（`current_run()` 为真而 `current_action()` 为 `None`）。
所以缺口不是"没有运行上下文"，而是**没有 action 级上下文**；而 `compile_native_topology(action, …)` 的第一形参正是 `RunAction`。
源码侧的机制我已核：`data/dataset.py:1897 materialize` 用 `with action("materialize", …) as checked, observe_action(…) as observer`——**它开 RunAction**；
take/iter_rows/count 只有 `observe_action`。**最小修正因此非常具体：让这三个终端也经 `action(...)` 开 RunAction，而不是新造运行上下文。**

**Q50 由推断变为实证，且我实测复现了"green-on-legacy"。** 三个终端**完全不咨询分流缝**（seam 计数 0），直接走 `_apply_plan`（计数 3），
**而结果全部正确**。这意味着 V001 的断言全文——"合法 thread map 的 take/take_all/count/write 各自结果正确；真正无效选项拒绝前 events 为空；take(0) 不读源"——
**在统一执行一次都没发生的情况下也会全绿**。我不是推测这个风险，我是在 bbd93ff 上把它跑出来了。
这正是架构权威 U5 退出条件"**没有无声退回**"要禁止的情形，也印证 `root-map.json` 把 `P.unified_execution` 映射到 `[LAYOUT, GRAPH, QUEUE, RAY, STREAM]` 而**不含 ADMISSION** 是口径漏洞。

### 我未能确立的部分（如实标注，不夸大）

对照探针 A（`materialize()` + thread map，想证明"缝在 stream 路径上确实被咨询"）**两次均抛
`RecoveryContractError: Worker did not register`**，随后 `execution_scope` 退出时抛 `Run has unconfirmed actions, outputs, state or source ACKs`。
我先怀疑是探针脚本内 lambda 的 cloudpickle 归属问题，改为 `/tmp` 下可导入模块的模块级函数 `double_w` 重试，**同样失败**。
**我不据此声称产品缺陷**：`dataset.py:1312` 的 docstring 提示该路径应"execute with run_stream() or materialize()"且"同步 action 必须在活动事件循环之外（或在线程内）"，
失败更可能来自我的调用方式/探针环境，而非 bbd93ff 的既有行为；本轮没有继续深挖（按全局精力分级，它不影响 Q27/Q50 的成立）。
**因此"缝在 materialize_stream 上确实被咨询"这一对照仍只有静态证据**：`unified_chain_eligible` 全仓唯一调用点 `executors/local.py:863`，位于 `materialize_stream`（:853-882）内。
探针产物全部限于 `/tmp/demiflow-qwen-pass8/`。

---

## 三之八、Pass 5 并入：117 历史失败归因、冻结字节与在途候选漂移（Q53–Q55）

子审查交付 `/tmp/demiflow-qwen-pass5/REPORT.md`（P5-01~11 ＋ 附件 A–E）。**我对其三条决定性结论亲自复算，全部成立**，其中一条是我前七遍都没发现的**验收盲区**，一条带安全后果。

### Q53 / P0（新发现，我独立复算确认）
- **对应 root 与实际用户后果**：`P.correctness`、`P.business`、R.2.2。**117 条历史确定性失败中，73 条（62%）所在的 6 个测试文件不被任何一条门命令执行。**用户可见后果：升级后"流式 map 的准入/取消/前缀/去重/checkpoint 恢复与迁移是否真修好"**无法被验收证明**；27 门全绿仍可留着 73 条原始失败。这与 Q06 是**两个不同的盲区**——Q06 是"新功能没被验证"，Q53 是"**旧缺陷的修复也没被验证**"，而后者正是本轮升级的立项理由。
- **设计文件/任务 ID**：`TEST-COMMANDS.md`（27 个 bash 块）、`gates.json.required_existing_files`、`INTEGRATION-ACCEPTANCE.md:85`（唯一一句聚合分工）、`root-map.json`；受影响族 A/B/E/G/H 与 N2 迁移（`INTEGRATION-ACCEPTANCE.md:92`）。
- **实际源码文件/符号/所审 commit**：逐 id 原始载体＝`_demiflow/worktrees/c_budget_20261113/docs/upgrade-implementation-20261113/plan-facts-single-authority-design.md` §1.2（文档行 119–236）。基线 bbd93ff；c_budget HEAD `90d7ee4`。**设计树从不引用该审计文档**。
- **最小反例或具体断开的调用链**（下列计数**全部由我亲自程序化复算**，不采信子审查数字）：
  - 我解析 §1.2：**表行 118、唯一 id 118**（无重复、无静默丢弃）；扣 1 条墙钟抖动 T（`test_stream.py::test_unordered_emission_no_head_of_line_blocking`）＝**117**。
  - 我解析 27 个 bash 块：**52 个唯一测试文件**，与 `gates.json.required_existing_files` **双向差集均为空**。
  - 逐文件分布与门覆盖（我复算）：

    | 失败所在文件 | 条数 | 被任一门执行 |
    |---|---|---|
    | `tests/test_stream_map.py` | 56＋1 裸名＝**57** | **否** |
    | `tests/test_vector_search_stream.py` | 23 | 是（VECTOR） |
    | `tests/test_prompt_http_stream.py` | 12 | 是（PROMPT） |
    | `tests/test_stream_checkpoint_resume.py` | 8 | **否** |
    | `tests/test_inference_execution.py` | 4 | 是（PROMPT） |
    | `tests/test_stream_deduplicate.py` | 3 | **否** |
    | `tests/test_stream_grouping_lookup.py` | 3 | 是（BUFFER/PREPEND） |
    | `tests/test_stream.py` | 2（＋1 T 不计） | 是（STREAM） |
    | `tests/test_stream_threads.py` | 2 | **否** |
    | `tests/test_stream_lance_references.py` | 2 | **否** |
    | `tests/test_stream_checkpoint_migration.py` | 1 | **否** |

    **被覆盖 44 ／ 未覆盖 73 ＝ 117**（我逐项求和核对通过）。6 个未覆盖文件**在 bbd93ff 全部真实存在**（我逐个 `.exists()` 核过），却既不在 52 文件命令集合、也不在 gates 集合。
  - 裸名行核实：§1.2 第 77 行只写 `test_external_cancel_drains_map_calls_before_action_cleanup`，我 grep 到其真实位置 **`tests/test_stream_map.py:546`**，归入该文件计数正确。
  - 具体反例：该 D 族用例（抛点 `recovery_identity.py:107`）修好后**没有任何门会跑它**。更严重的是 **N2 候选正在 dirty 修改的 5 个文件里有 4 个属于未覆盖集合**（`test_stream_map`/`test_stream_checkpoint_resume`/`test_stream_lance_references`/`test_stream_checkpoint_migration`）⇒ **退休 checkpoint 迁移的正确性无门可证**。
  - `root-map.json` 20 项意图中 `test_` 命中 **0**；设计树内不存在 117→0 或新旧 id 差集的任何机器守卫。
- **问题属于**：验收缺口（历史失败→门命令的映射断开）＋缺少回归差集守卫。
- **建议的最小修正**：(a) 把 6 个未覆盖文件按语义并入现有门（`test_stream_map`→STREAM/DELIVERY/ADMISSION；`_checkpoint_resume`/`_migration`→RECOVERY/PREPEND；`_deduplicate`/`_lance_references`→BUFFER/WRITE；`_threads`→STREAM/BOUNDS），**或新增一个 `HISTORY117` 门一次性跑 11 个失败文件**（后者成本更低，符合精力分级）；(b) 以 §1.2 的 118 id（扣 T）为基线集合加机器守卫，要求 `NEW=0 且 剩余失败=0` 并输出逐 id 差集；(c) T 条按审计 §0.2 的隔离重跑 3 次口径单独记录，**不得放宽断言**。
- **受影响的函数、字段、caller/callee**：`data/plan.py:reject_streaming_map_options`→`executors/local.py:take/iter_rows/write_lance`（B 族）；`execution/plan_facts.py:operation_facts`→`payload_limits.py:payload_scope`（A 族）；`operator_state.py:validate/register`＋`recovery_identity.py:describe`（D 族）；`capabilities.py`（E 族）；`run_context.py:283`（F 族）；`payload_limits.py:119`（G 族）；`stream_resources.py:51`（H 族）。
- **应新增或保留的验证**：117 逐 id 差集门（含 NEW=0）；保留现有 52 文件门；N2 迁移逐条映射原测试意图并在门中可见。
- **是否阻塞**：**阻塞最终验收**。不补则"历史失败已清"不可判定。

### Q54 / P1（新发现，带安全后果；我逐行核实）
- **对应 root 与实际用户后果**：R.5.3（业务/历史保护）、`P.recovery`。`operation_facts` 的输出进入 `plan_identity`/dag_id/封存计划/工作台历史——**都是不可改写的永久载体**。把 transport `options` 原样拷入 ⇒ **可能把凭据与本机路径写进永久历史**；且凭据轮换会改变身份，导致旧运行被拒（与"凭据轮换不改身份"的既有语义相反）。
- **设计文件/任务 ID**：`CONTRACTS.md` C02 的 `model_map` 行明文要求「**options 的非秘密语义**」；`tasks/F002.md`。**注意：这不是设计缺陷，设计已写对；缺陷在待吸收的在途候选实现里。**
- **实际源码文件/符号/所审 commit**：c_budget `90d7ee4` ＋ 1 个 dirty 文件 `demiflow/execution/plan_facts.py`（我核 `git status --porcelain` 确为唯一 dirty）。其 `elif kind == "model_map":` 分支含 **`options=dict(operation.options) if operation.options is not None else None`——无任何过滤**（我直接读 diff 确认）。基线侧：`operator_llm/http_options.py:87` **`if credentials_env and set(request) & {'api_key','extra_headers'}: raise`**——**该守卫以 `gateway_credentials_env` 已设置为前提**；未设置时 `request_options.api_key` 可通过校验。同文件 `:93` docstring 明文「Load transport-only credentials, **never part of a request journal or hash**」。白名单（`:10-18`）还含 `journal_dir`/`stream_log_dir`/`object_store`/`gateway_credentials_env` 等本机路径与凭据相关键。
- **最小反例或具体断开的调用链**：`map_prompt_async(..., options={"request_options":{"api_key":"sk-…"}})` 且**未设** `gateway_credentials_env` → `validate_http_options` 放行（`:87` 条件不成立）→ dirty `operation_facts` 原样写入 facts → `plan_identity`/dag_id → `seal_execution_bindings`（L004）→ `workbench/runs/facts.py:read_sealed_plan`（W001）永久留存。**可达性为推断（我未跑测试）**；"校验允许该键"与"facts 无过滤"两点为**行级已验证**。另：随机 `node_id` 已被正确排除（这点候选做对了）。
- **问题属于**：候选实现与契约不符（吸收时必须修正），非设计缺陷。
- **建议的最小修正**：F002 的 `model_map` 分支对 `options` 做**白名单＋语义化**：只保留影响调用语义的非秘密键（timeout/limits/stream/model_revision/verify_model 等），显式剔除 `request_options` 内凭据与头、`gateway_credentials_env`、`journal_dir`/`stream_log_dir`/`object_store`；或改用既有有界语义摘要。**并把过滤规则写进 F002 卡**，否则独立实现者无法一致。
- **受影响的函数、字段、caller/callee**：`plan_facts.py:operation_facts`→`recovery_identity.py:plan_identity/describe`→`sealed_plans.py:seal_execution_bindings`→`workbench/runs/facts.py:read_sealed_plan`；上游 `dataset.py:map_prompt_async`→`operator_llm/client.py`。
- **应新增或保留的验证**：新增反例——含 `request_options.api_key`/`journal_dir` 的 options **不得出现在 facts/dag_id/封存字节中**；保留"凭据轮换不改身份"与"model/deployment 变更需新 model_revision"的既有断言。
- **是否阻塞**：**阻塞 F002 吸收**（必须在写入任何封存载体前修正）；不阻塞其他包编码。

### Q55 / P2（新发现；我读基线全函数确认）
- **对应 root 与实际用户后果**：`P.correctness`、R.5.1。F001 是**承重事实函数**（决定 kind → 影响 dag_id 字节），却派为 `implement`，而基线已满足 C02 ⇒ 独立实现者可能做无谓改动，导致既有身份字节漂移、历史运行被拒。
- **设计文件/任务 ID**：`tasks/F001.md`（mode=implement，behavior「唯一原生 kind 权威；保留原生特化，拒绝伪装第三方；新旧事实兼容」）；C02 基础返回键。
- **实际源码文件/符号/所审 commit**：我读了 bbd93ff 的 `execution/plan_facts.py:79-102` 全函数：kinds 表已含 13 类（Map/BoundMap 同 kind）；`:91-95` 同模块 `isinstance` 回退保留原生特化并拒绝第三方伪装；`:96-97` 未知→`unknown`；返回键恰为 `{kind, driver, native_options, stream_options, bound}`＝C02 基础键。c_budget 树该函数**未 dirty**（与基线逐字相同）。卡称基线行 79、sha `ec987ee7…`，与我 Pass 7 的 96/96 复算一致。
- **最小反例或具体断开的调用链**：无待改点 ⇒ 任何"实现"都是改动承重函数。具体风险：`:98` 现为 `native = getattr(operation,'native_options',None) if type(operation) in kinds else None`；若实现者把它扩到子类，`SearchWebOp`/`SaveLanceOp` 的 native 事实改变 → **既有 dag_id 字节漂移**，违反 C02 逐字节兼容。
- **问题属于**：设计卡口径（动作类型与基线现状不符）。
- **建议的最小修正**：F001 改 `verify_preserve`，或卡内注明「基线已满足 C02，本卡只验证不改动」，并把 5 个返回键与 13 类 kind 映射列为断言。
- **受影响的函数、字段、caller/callee**：caller `operation_facts`（F002）、`capabilities.py:validate_stream_plan`（A002）；callee `data/plan.py` ops 类型表。
- **应新增或保留的验证**：13 类 kind 映射＋第三方伪装子类拒绝＋`SearchWebOp/SaveLanceOp` 回退 kind；若无改动则函数 sha 保持 `ec987ee7…`。
- **是否阻塞**：不阻塞。

### Pass 5 与我既有发现的重合（不重复计数，但补了精度）
- **P5-01（冻结七件无机器守卫、六件设计树零命名）≈ 我 Q52**。Pass 5 补了逐文件 sha256 与聚合值、`gates.json` 无 freeze 字段、七件中仅 `recovery_identity.py` 在 baseline.json。**两遍独立得到同一结论，Q52 的 P1 可上调为 P0 级验收阻塞**（我保留 P1 标注，但在表 3 提到第一优先）。
- **P5-03（21 新验收函数 0 命令、7 门新验收列表为空）≈ 我 Q06**，是我 Pass 6 已复算确认的同一事实，**第三次独立命中**。
- **P5-05（E 候选 D1/D2 与 C04:96/107-109 相反、六处反例断言固化冲突、S013:51 样板要求保留原断言）≈ 我 Q12/Q13**。Pass 5 把断言精确到行（`test_d2_recovery_identity_counterexamples.py` L65/L88/L126-128/L138/L142/L172-173），并指出**该测试文件不被任何卡命名、不在 52 门文件内**——这一点与 Q53 叠加，意味着**冲突断言既不会被门跑到、也不会被卡纠正**。我采信其行号列举（子审查行级取证），未逐行重读该测试文件。
- **P5-07（c_budget dirty 已实现 F002 全部内容 → 应吸收而非重实现）≈ 我 Q24/Q38**，并补了关键细节：dirty 把 `operation_facts:119-123` 的第二套精确类型集合改为 `if kind == "unknown": raise`（单一权威），新增 `_specialization_domain` 覆盖 5 个原生特化且字段与 C02 逐项一致。**我顺手在基线读到了 A 族根因的直接证据**：`operation_facts:119-121` 确实另维护一份 12 类 `supported` 集合，`:122-123` 对不在集合者抛 `RuntimeContractError`，`:124-125` 对 native_options 再抛一次——与审计"A 族 35 条抛点 `:123`＋1 条 `:125`"精确对应。**这条我升级为已核。**
- **P5-06（b_option `78d6492` 是能力退步源、`e517110` 可保留）≈ 我 Q05**，一致。
- **口径纠正（我采纳）**：`raw_fetch.py` 在 **`execution/`** 下，不在我先前写的位置；F002 的文件是 **`execution/plan_facts.py`**，`workbench/runs/plan_facts.py` **不存在**（该目录只有 `facts.py`）。表 4 已按此更正。

### 我未独立复核的 Pass 5 结论（如实标注）
P5-10（11 处卡漏写返回注解）、P5-11（E 树 commit 声称的七件聚合 sha 不可复现）、附件 B 的 34 个契约符号逐条核对、附件 A 的逐文件 sha256 数值。
这些**工件在 `/tmp/demiflow-qwen-pass5/` 可复算**，但我未逐条重算；其中签名主结论（96 既有卡行号＋sha＋签名全等）与我 **Pass 7 亲自跑的 96/96 完全一致**，属两遍独立互证，我采信。

---

## 三之九、Pass 9：四条 P0 的第一手运行时复现（Q01 / Q02 / Q03 / Q47 全部升级为实测）

Pass 8 把 Q27/Q50 做成实测后，报告第一优先项里仍有 **Q01/Q02/Q03/Q47 四条 P0 只有行级源码推断**。
我在 §五 第 7 条给自己留的话是"若继续投入，应优先把这四条做成运行复现，而不是再扩外围文档核对"。本轮就只做这件事。

**取证口径**：隔离树 `integration_c_20261113`（`bbd93ff`，**跑前跑后 `git status --porcelain` 均为空**，主仓 `e40a6dd` 的 11 项在途改动始终未动）；
解释器 `/yzp/zhaozy/yangzepeng/0905/demiflow-env/bin/python -B`；工作目录 `/tmp/demiflow-qwen-pass9/run`；
每个脚本开头 `assert demiflow.__file__.startswith(TREE+'/')` 并打印确认，**杜绝误导入受保护主仓**。
**全程不联网**：Q01 的 `EmbeddingActor` 只在**声明期**构造，`astart()` 从未调用，无 httpx、无模型请求、无付费调用。
脚本：`probe_q01.py`、`probe_q02.py`、`probe_q03.py`、`probe_q47.py`（均在 `/tmp/demiflow-qwen-pass9/`）。

### 9.1 Q01（P0）两个断点全部复现，且有干净对照臂

Q01 原本推断"Embedding 借用 journal 后第一次 native reserve 必抛"。本轮把它拆成**两条独立路径**分别实测——这很重要，因为它证明**无论 M 的 agent 选哪种实现方式都会撞墙**：

**臂 A（M004 自己的 `bind_actor` 路线）**：真 `RunPromptJournal` + 普通 dict 请求，不设 `CALL_RESERVATION`：

```
CALL_RESERVATION.get() = None
RAISED: RecoveryContractError
MESSAGE: Native request has no execution budget reservation
  at demiflow/operator_llm/sqlite_journal.py:414  return item.future.result()
  at demiflow/operator_llm/sqlite_journal.py:527  result = self._apply_operation(db, item, effects)
  at demiflow/operator_llm/stream_recovery.py:148  raise RecoveryContractError('Native request has no execution budget reservation')
```

**臂 B（对照）**：同一请求、同一 journal，唯一差别是手动 `CALL_RESERVATION.set((reservation, charges))`：

```
CALL_RESERVATION.get() = ('probe-reservation-id', {'requests': (1, 100000)})
reserve() returned: True
control PASSED -> the only difference is the reservation
```

**臂 C（复用 `NativePromptRecovery.bind_client` 路线）**：构造**真实** `EmbeddingActor`（真 `EmbeddingModel`，声明期），再走真 `NativePromptRecovery.bind_client`：

```
real EmbeddingActor built at plan time; actor._runtime is actor = True
hasattr(actor._runtime, "coordinator") = False
RAISED: AttributeError
MESSAGE: 'EmbeddingActor' object has no attribute 'coordinator'
  at demiflow/operator_llm/stream_recovery.py:206  self.actor._runtime.coordinator._native_group_journal = True
```

**结论升级**：Q01 不再是推断。**A/C 两臂互为独立断点**：走自己的 `bind_actor` → `:148` 抛预算缺失；复用 Prompt 的 `bind_client` → `:206` 抛缺 coordinator。
对照臂 B 证明**唯一缺失量就是执行预算预留**，因此 §三 Q01 给出的最小修正（新增一个 M 叶任务建立与 Prompt 同权威的预留，或在 C07 定义免预留模式）**方向被实测确认，且必须二选一写进契约，不能留给实现者猜**。
附带纠正我自己先前的一处方法名错误：`RunPromptJournal` **没有 `drain()`**；`_enqueue` 末尾是 `return item.future.result()`（`sqlite_journal.py:414`），**错误是同步抛出的**，不需要轮询。第一版探针的 `AttributeError: no attribute 'drain'` 是**我的探针 bug，不是产品缺陷**，已修正后重跑。

### 9.2 Q02（P0）快照字段在真 `_Stage` 上连"挂上去"都做不到

```
_Stage.__slots__ count = 30
groups               hasattr=False  attach-> AttributeError: '_Stage' object has no attribute 'groups'
deadline             hasattr=False  attach-> AttributeError: '_Stage' object has no attribute 'deadline'
prefix_position      hasattr=False  attach-> AttributeError: '_Stage' object has no attribute 'prefix_position'
next_output_ordinal  hasattr=False  attach-> AttributeError: '_Stage' object has no attribute 'next_output_ordinal'
row_ordinals         hasattr=False  attach-> AttributeError: '_Stage' object has no attribute 'row_ordinals'
stage.batch_ordinal (the one ordinal that DOES exist) = 0
```

`__slots__` 的存在把 Q02 从"设计没写清楚"提升为**机械性阻塞**：R 的 agent 即使想自行补字段，`setattr` 也会直接 `AttributeError`。
这正面支持 Q02 的最小修正——**`_Stage.__slots__` 与 `__init__` 必须作为同一个装配事务列出全部新增字段**，而不是只派发一个 `buffer_adapter`。

**并且本轮实测到一条比原 Q02 更尖锐的新事实（记为 Q56，见 9.5）**：barrier 的 `align` 每 cut 只触发一次，且 `native_snapshot` 会把 token 数从 concurrency 压成 1。

### 9.3 Q03（P0）grouped 阶段无 barrier 分支：AST 与运行时双证

**AST 证据**（不靠肉眼读行，直接枚举 `arrive` 调用点及其所属函数）：

```
_make_worker                 lines  323- 709  arrive() call lines=[449, 605, 425]
_make_worker.batch_worker    lines  574- 629  arrive() call lines=[605]
_make_worker.grouped_worker  lines  631- 670  arrive() call lines=NONE

batch_worker 体首三条语句：
  line 576: if stage.grouping is not None:
                await grouped_worker(st)
                return
```

即 `batch_worker` **在到达自己 :605 的 barrier 分支之前就 return 进了 `grouped_worker`**，而后者一个 `arrive` 都没有。

**运行时证据**（把真 `_CheckpointBarrier` 喂给 grouped 路径 :659-660 实际会调的两个函数）：

```
row_size(barrier, 1<<20)   -> RAISED TypeError: Streaming grouping accepts nested mappings/sequences and scalar payloads
group_key(barrier, ('k',)) -> RAISED TypeError: '_CheckpointBarrier' object is not subscriptable
```

**缺 `arrive` 的后果也实测**（`checkpoint_timeout_s=1`，无 worker 调 arrive）：

```
wait() RAISED TimeoutError: Checkpoint coordination deadline exceeded
barrier.status = failed
```

**结论升级**：Q03 的"崩或污染分组"与"卡到超时"两种后果都被实测。这条同时把 Q47 的"删除已证明保护"落到实处——**现行拒绝不是保守多余，而是下游真的没有对齐路径**。

### 9.4 Q47（P0）现行 fail-closed 守卫确实对四种 kind 生效

用真 `_Stage` 走真 `StreamStageProgress.bind_stages`（守卫是循环体第一条语句，故 bare 实例即可精确判断守卫是否触发；**任何更晚的报错都证明已越过守卫**）：

| kind | 实测结果 |
|---|---|
| plain row stage | 越过守卫（随后 `AttributeError: no attribute 'state'`） |
| fixed batch c=1 无时间窗 | 越过守卫（随后 `AttributeError: no attribute 'action'`） |
| **concurrent batch c=2** | **REJECTED by :199-202** |
| **timed batch c=1 window=.5** | **REJECTED by :199-202** |
| **grouped** | **REJECTED by :199-202** |
| **prepend/prefix** | **REJECTED by :199-202** |

拒绝文案逐字为 `Buffered native stage needs its P3 state adapter before partial commits`。
**R006 要解除的正是这 4 种 kind**，而 9.2/9.3 已实测这 4 种的持有者与对齐路径都不存在。

三个 ordinal 空间也在运行时并列确认：

```
progress.select() over 5 source rows -> [0, 1, 2, 3, 4]   (源行空间)
_Stage.batch_ordinal initial = 0                          (已刷批次数空间)
hasattr(_Stage, "next_output_ordinal") = False            (设计的 per-stage 输出空间：无持有者)
```

**结论升级**：Q47 的"删除已证明有效的保护、替代机制未证明"由实测支撑。
其最小修正三条（C05 冻结三 ordinal 换算且禁止 `next_output_ordinal` 参与 `progress.ordinal >= replay_cut` 比较；给 `stream_barrier.py` 指定负责人并把 8 属性 progress 契约写成 verify_preserve；R006 保留 `:209-211` 两道恢复守卫、逐 kind 给出跨 cut 证据）**均被本轮证据加强，不变**。

### 9.5 Q56 / P1（本轮新增）：`native_snapshot` 让快照在**第一个** worker 到达时就拍

- **对应 root 与实际用户后果**：P.recovery、R.5.2。原生模型阶段（正是 M 包要恢复的那类）在 concurrency>1 时，cut 快照比其他 worker 排空**更早**拍下，恢复后丢行/重行。
- **设计文件/任务 ID**：C05；R002/R003/R015；`stream_barrier.py` **无主无任务**。
- **实际源码文件/符号/所审 commit**：`demiflow/execution/stream_barrier.py:36-38 tokens()`（`return 1 if getattr(stage,"native_snapshot",False) else stage.concurrency`）、`:60-65`（`arrivals[index] == tokens(stage)` 才 `await self.progress.align(index, self, batch)`）。bbd93ff。
- **最小反例（实测）**：同一 stage、concurrency=2、两个 worker 各带自己的 pending batch 到达同一个 cut：

  ```
  普通 stage：  tokens=2  align 调用 1 次  收到 [[{'g':'b','rows':[3]}]]   worker1 的 batch 可见=False
  native_snapshot=True： tokens=1  align 调用 1 次  收到 [[{'g':'a','rows':[1,2]}]]
  ```

  即普通 stage 拿到**最后**到达者的 batch，native stage 拿到**第一个**到达者的 batch；**两种情形都只有一个 worker 的 pending 进入 align**。
- **问题属于**：漏任务（聚合点无主）+ 契约未冻结（`pending_batches` 复数语义）。
- **建议的最小修正**：把"聚合全部 worker 的 pending 后再 align"作为 `stream_barrier.py` 的显式派发任务，并在 C05 写明 `native_snapshot` 的 token=1 是**per-stage 单次快照**语义、不是"第一个 worker 的局部快照"。
- **受影响**：`_CheckpointBarrier.tokens/arrive`、`StreamStageProgress.align`、`StageBufferAdapter.align`。
- **应新增或保留的验证**：concurrency=2 且 `native_snapshot=True` 时，两 worker 的 in-flight 行在恢复后不重不漏。
- **是否阻塞**：阻塞编码。**本项是 Q02/Q03 的实测副产物，把"聚合缺失"从 grouped 一种 kind 扩大到原生模型 kind。**

### 9.6 本轮的证据形态变更（用于更新表 3／表 4.7）

| 发现 | Pass 8 后的形态 | **Pass 9 后的形态** |
|---|---|---|
| Q01 | 行级源码推断 | **实测**（A 臂 `:148` 抛＋C 臂 `:206` 抛＋B 臂对照成功） |
| Q02 | 行级源码推断 | **实测**（5 字段 `setattr` 全部 `AttributeError`） |
| Q03 | 行级源码推断 | **实测＋AST**（`grouped_worker` arrive=NONE；两个 TypeError；TimeoutError） |
| Q47 | 行级源码推断 | **实测**（4/6 kind 确认被现行守卫拒绝；三 ordinal 空间并列） |
| Q56 | — | **实测**（新增） |

**未升级为实测的仍如实保留**：Q05 的并发丢 batch、Q54 的凭据可达性、Pass 8 的 materialize 对照臂（两次 `Worker did not register`，属我的调用形态/环境限制，**不据此声称产品缺陷**）。

---

## 三之十、Pass 10：Q04 的运行复现 —— 结果是**撤回我自己的 P0**，并留下一条更窄的实测残留（Q57）

Pass 9 做完后，第一优先项里只剩 Q04 与用户核心意图（"普通类实例真崩溃恢复"）直接相关，所以我按 §五 第 7 条给自己的建议继续做它。
**本轮最重要的产出不是新增缺陷，而是发现我自己判错了一条 P0。** 这必须写在显眼处。

**取证口径**：与前两轮完全一致——隔离树 `integration_c_20261113`（`bbd93ff`）、`demiflow-env/bin/python -B`、工作目录 `/tmp/demiflow-qwen-pass10/run`、脚本断言并打印模块来源、不联网、产物限 `/tmp`。
探针：`probe_q04.py`（三种声明形态 × 真 traits/precheck）、`probe_q04b.py`（真 `compile_operator_spec` 的 ownership×adapter 耦合）、`mod/q04_counter.py`（可导入的业务类）。

### 10.1 撤回 Q04：设计**已经**定死了规则，调和任务也**齐全**

我原判 Q04 为 P0，写的是"封存 `ownership=none` 而运行期需 `shared_colocated`，C04 自身禁止这种升级，**没有任何卡负责调和**"。逐字读完后，两条依据都不成立：

**（1）C04 明文固定了默认所有权**（我原先漏读的同节段落）：

> 默认状态所有权明确固定：普通function为none；**factory或auto_json如果调用者尚未声明状态所有权，采取shared_colocated，不把未知当作无状态**。已有原生hooks按其明确声明保留partitionable/shared_colocated。

同节还明文写着「**Counter 必须能跨 checkpoint 恢复计数**」。
也就是说，我原先给出的"最小修正＝声明期即预置 shared_colocated"**正是设计已经写好的方案**。`StatePolicy.mode` 含 `'factory'`，`classify_operator_state` 的 `phase` 显式区分 `declaration`/`instance`，所谓 "deferred" 指的是**实例检查延后**，不是 ownership 悬空——`ownership` 枚举始终只有 `none/partitionable/shared_colocated` 三值，不存在我担心的"第四种未定义取值"。

**（2）调和任务存在且四消费点齐全**（我原先只读了 traits 基线，没读 S011 卡）：

| 卡 | 文件 · 符号 | 行为原文要点 | callee |
|---|---|---|---|
| **S001** | `execution/operator_policy.py` · `classify_operator_state` | 唯一策略；"factory 与 instance 分开"；"**没有 hooks 绝不直接等同无状态**" | — |
| S004 | `execution/operator_precheck.py` · `precheck_operator_state` | "调用统一 policy…实例无钩子但可安全 auto_json 时用平台适配，不默认静默放过" | **S001** |
| S009 | `execution/contracts/operator.py` · `_adapter_present` | "统一 policy 的 adapter_available；不再只探两个方法而忽略 auto_json；不构造 class" | **S001** |
| S010 | `execution/contracts/operator.py` · `compile_operator_spec` | "state.ownership 与 adapter_present 来自同一策略；factory 真正实例检查留运行期" | S009 |
| **S011** | `planning/traits.py` · `unified_operation_traits` | "**实例和类的 state ownership 同源**；不因为不是 class 就把 native state adapter 丢掉" | **S001** |

且 `FILE-OWNERSHIP.md:44` 明确 `demiflow/planning/traits.py` 属 **S**、由 **S011** 派发。**我原先"traits 无主"的说法是错的。**

**错误成因（自查）**：我只读了 `traits.py:70-73` 的**基线行为**和 C04:98 那**一句禁令**，就把基线现状当成了设计目标，没读同节的默认所有权段落、也没读 S011 卡。
这正是本次工单要求我警惕的"把基线事实误当设计缺陷"。**Q04 予以撤回，不计入需重做项。**

### 10.2 顺带实测确认的基线事实（供 S 的 agent 参考，**不是设计缺陷**）

`probe_q04.py` 用真 `unified_operation_traits` / `precheck_operator_state` / `_adapter_present` 跑三种声明形态：

| 声明形态 | `is_class` | 封存 `state_ownership` | `_adapter_present` | `precheck_operator_state` | 运行期实例 |
|---|---|---|---|---|---|
| **`Counter` 类工厂**（无钩子） | True | `none` | False | **ACCEPTED**（返回 False） | `__dict__={'n':0}`，无 hooks |
| **`Counter()` 实例**（无钩子） | False | `none` | False | **REJECTED** `STATE_ADAPTER_MISSING: callable instance carries state without a state adapter` | 同上 |
| `HookedCounter` 类工厂 | True | `shared_colocated` | True | ACCEPTED（返回 True） | 有 hooks |

即**同一个有状态业务类，声明为类工厂被接受、声明为实例被拒绝**——因为 `operator_precheck.py:24` / `contracts/operator.py:114` 的 `inspect.isclass(target)` 短路跳过了实例状态守卫。
这是**基线的既有不对称**，正是设计要改的对象，**不是设计的缺陷**。

另一条实测事实：**`OperatorTraits` 的 11 个字段里没有 `adapter_present`**（`category/worker_model/lifecycle/policy_class/elastic/input_mode/cardinality/ordering/state_ownership/effect_kind/batching`）。
它只由 `_adapter_present(target)` 派生并进 compiled spec 的 `state` 字典——**与 C04"compile_operator_spec 的 state.adapter_present"表述一致，也不是缺陷**。我特意核这一点，是为了避免再犯 10.1 那类错误。

### 10.3 残留的真实风险 → Q57（P1，已运行复现）

把 C04 强制的 ownership 值喂给**真的、已派发的** `compile_operator_spec`：

```
_adapter_present(Counter) today = False
（S009 明文"不构造 class"，因此无法核实未来实例是否 auto_json 安全）

Counter      / ownership='none'             -> COMPILED  state={'ownership':'none','adapter_present':False}
Counter      / ownership='shared_colocated' -> REJECTED  STATE_ADAPTER_MISSING: stateful operator lacks snapshot_state/restore_state
HookedCounter/ ownership='shared_colocated' -> COMPILED  state={'ownership':'shared_colocated','adapter_present':True}
```

第二行正是 **C04 强制的取值**。它被拒，是因为 `compile_operator_spec:111-113` 与 **未派发的** `validate_operator_spec:182-183`（AST 确认函数范围 152-201；`FUNCTION-TASKS.md` 对该文件只派了 `_adapter_present` 与 `compile_operator_spec` 两个符号）都要求 `ownership != 'none'` ⇒ `adapter_present must be True`。

**所以组合风险是精确的**：S011 把 factory 的 ownership 升为 `shared_colocated`（C04 要求），**必须**同时让 S009 对未构造的 factory 返回**"延后可用"的 `True`**；否则每个无钩子类工厂在封存期即被拒，**正好打掉用户最关心的意图**。
而 S009 现文"统一 policy 的 adapter_available；不再只探两个方法而忽略 auto_json；不构造 class"**推不出这个取值**——按字面"已验证可用"实现就会返回 False。

**这是契约未冻结（一句话可修），不是漏任务。** 最小修正：C04 增一句"`phase='declaration'` 且 `mode='factory'` 时 `adapter_present` 取延后可用即 `True`，真实可用性在实例检查点必验"，并在 S009 行为栏写明；另把 `validate_operator_spec` 列为 S 的 verify_preserve 对端，明示 `:182` 本轮不变。

**探针的一处自我修正**：第一版把 `Counter` 定义在 `__main__`，被 `_callable_path:48` 正当拒绝（`callable has no durable artifact entry`）。那是**产品的正确行为、我的探针问题**；改为可导入模块 `q04_counter.py` 后重跑，才得到上表。**不把第一版的报错算作缺陷。**

### 10.4 本轮对报告结论的净影响

- **"需重做七处"数量不变**：Q04 原本就不在这七处内（它只在表 3 第一优先项与表 1 的 S 行）。**第一优先项减少一条 P0**，其余六处需重做项不变，且其中五处已有第一手运行时证据。
- **新增 Q57（P1）**，成本极低（两句话），但直接关系用户核心意图，建议随第一优先项一并冻结。
- **P4-06 / Q05 不受影响**：auto_json 并发=1 准入拒绝零实现者、`OperatorTraits` 无 concurrency 字段（本轮再次实测确认 11 字段无 concurrency），仍成立。
- **自我更正记录 +1**：这是本次 Review 第 9 处自我更正，也是**唯一一处撤回自己判定的 P0**。

---

## 四、四张简表

### 表 1：各包可用性

| 包 | 可用（可直接编码） | 需改（先修契约/补任务） | 未覆盖 |
|---|---|---|---|
| F 事实 | F003 方向与源码一致；C02 键集与 native domain 来自声明；F004 的 ProgramSlice 局部/全图拆分经实跑证明必要且有效；**F001 基线已满足 C02（我读全函数确认）** | F002 应**吸收** c_budget dirty 而非重实现（Q24/Q38/P5-07）；**吸收时必须过滤 `model_map.options`，否则凭据/本机路径进永久封存载体（Q54，安全）**；**F001 派 implement 无待改点，应改 verify_preserve（Q55）**；F004 需与 L030 封存权威对齐（Q09） | `operator_llm/flow_contract.py`、`lance/flow_contract.py` 无任务；非有限输入身份（Q23）；source 侧 callable 的代码固定（Q31） |
| A 准入 | A002/A005/A006/A007/A008 方向正确；B 退步已明确撤销（`78d6492` 已核为退步源） | **Q27 take/iter/count 无 RunAction（Pass 8 我实测 `current_action()=None`×3）、Q28 count 病因错、Q29 `_apply_plan` 无主（三处关键方案需重做）**；**Q50 三终端不咨询分流缝、结果却正确 ⇒ V001 green-on-legacy（Pass 8 我实测 seam 0 次）**；Q08 recovery 四值→两值；Q30 `available_backends` 无生产者；Q39 route 死值 + seam 边界；Q11 A001 无 callee；**P4-06 auto_json 并发=1 准入零实现者** | `Dataset.take/iter_rows/take_all/materialize/union`、`plan/write_datasink/materialize_stream`、`stream_delivery.py`、`executors/base.py:count` 未派发 |
| S 状态 | StatePolicy 词汇与源码 `_OWNERSHIP` 一致；S012 定位 `_MapWorker.prepare` 正确；S009 对 `_adapter_present` 的 target 语义描述正确；**Pass 10 更正：C04 已明文固定 factory/auto_json 默认 ownership=shared_colocated，S001 唯一策略＋S004/S009/S010/S011 四消费点齐全，原 Q04"契约矛盾＋漏任务"已撤回** | ~~Q04 ownership 矛盾（已撤回）~~；**Q57 factory 的 adapter_present 延后取值未冻结（Pass 10 实测 `Counter`+`shared_colocated` 被 compile 拒）**；Q05 能力退步、Q11 无主、Q12 反转 E、Q13 重基；**P4-06 C04:98 并发=1 准入拒绝零实现者（我扫全 144 卡确认）**；**P5-05 E 候选六处反例断言与 C04 相反、S013:51 样板要求保留原断言**；P4-12 S007/S008"只操作本对象"不实 | `__getattr__`/描述器检测谓词、max_bytes 来源、原子 restore 机制、VectorSearch 字段表未冻结；`validate_operator_spec` 未派发但是 `:182` 规则的对端；`operator_policy.py` 为新增文件 |
| R buffer | 三分权威（S 状态/R 缓冲/M 效果）同一 cut 的方向正确 | **Q02 字段无持有者（Pass 9 实测 5 字段 `setattr` 全 `AttributeError`）、Q03 grouped 无 barrier（Pass 9 AST＋TypeError＋TimeoutError 实测）＋聚合无主、Q56 native token=1 只封存首个 worker（Pass 9 实测）**；**Q47 R006 删除的 fail-closed 守卫自陈"graph cut deadlock"（F3 家族），Pass 9 实测其对 4/6 kind 真在拒绝，而唯一持有联合触发的 `stream_barrier.py` 未派发**；**Q42/Q43 与既有 `execution/exchange/`＋`contracts/exchange.py` 重复权威，且 `max_pending_floor` 下限零命中**；**P4-04 R002 全新 durable_write 无 owner**；P4-02 R010 `execution` 类型误标 | `stream_barrier.py`（8 个 progress 属性契约）、`stream_grouping.py` 无任务；C05 `next_output_ordinal` 无语义定义；N2 旧 checkpoint 迁移边界未划 |
| Q 资源编码 | Q001 signed-zero/IEEE 字节定位准确（探针实测复现）；Q002/Q003 保留原预留守卫；C06 与源码一致 | **Q35 s6 测试不在 CODEC 门 + set 失败形态描述偏差**；Q33 C12 公式未冻结且 threads 不在冻结 profile 键集；新快照字段字节未入同一 barrier 预计（探针数值，我未复算） | `local_tasks.py`/`local_kernel.py`/`data/runtime_contract.py` 无任务（Q34）；set/frozenset 跨 PYTHONHASHSEED 反例未派 |
| M 模型 | Prompt 侧 M001/M002 verify_preserve 正确；M010 drain=0、M007 scope 定位准确；M011 借用包装 close 为 no-op 已核 | **Q01 预算预留缺失（关键方案需重做）**；Q21 request_gate 类型描述错 | `operator_llm/runtime.py` 无任务；N1 共享 shard 协议改动边界未划 |
| L 布局 | v1 三样例被真实解释器接受；v2 dag+physical 过真实 validator；v2 键差恰为 executions↔programs；`budgets` 三键与冻结 dag.py 一致；ProgramRef 与 `_seal_plan_code` 返回键逐键匹配；async ACK 化路径已核可行且组合安全；per-slice `verify_loaded_plan` 实跑能拒 join-right UDF 改码而根门通过 | **Q31 源侧代码不固定、Q09 封存权威冲突（关键方案需重做）**；Q27/Q29 入口与漏斗无主；Q08 recovery；Q10/Q32 TimeoutError + 迟到 ACK + 60s/120s 冲突；Q33/Q34 预算落不到真实 I/O；Q36 typed_cache 折叠；Q37 卡内元数据错 | `deliver_receipt`、`_group_channel_ids`、`local_tasks.py` 无任务；`native_driver_v1` 不在 SUPPORTED_SLICES（需加）；v2 worker 明确拒绝已写清（正确） |
| W 工作台 | 来源等级 declared/sealed 与运行状态分离的方向正确；W007–W013 verify_preserve 合理 | **Q07 occurrence 图（关键方案需重做）**、Q14 无状态通道、Q15 源码链无主、Q16 stages 键、Q19 v2 无生产者、Q26 措辞 | `logical.py`/`source_view.py`/`parameters.py`、`loadSource`/`chooseNode`、`_physical_summary`、`_join_sealed_observations` 无任务 |
| V 验收 | **结构闭合是我审过最完整的一份**：20 根→27 门→144 卡→64 既有测试文件，0 缺失门/0 孤儿门/0 未知叶（我程序化核）；**21 张 V 卡断言高度贴合意图**（V002 真进程退出 resume 不归零、V005 同 cut 多次 crash 字节额度不漂移、V009 两个真 crash 窗口 unknown 零重发、V012 worker 不拥有 CommitCoordinator、V021 只改右子 plan 必须拒 resume）；测试纪律诚实（"本文件不是已通过报告"、逐门 basetemp 隔离、"skip 不计通过"） | **Q53 73/117 历史失败所在 6 文件不被任何门执行（我亲自复算 44+73=117）——比 Q06 更致命，因为清掉这 117 条正是立项理由**；**Q06 21 新验收函数 0 命令（三遍独立命中）**；Q17 skip、Q18 无硬守卫、**Q52 冻结六件未枚举且无机器守卫**、Q35 CODEC 门漏 s6、Q20 覆盖 52/458、Q22 装配无主；Q51 7 门 39 implement 零新测试（只需声明门并集，不必新造） | 性能双跑方法、浏览器 journey 拆分、ray 环境；117 逐 id 载体在设计包外且设计从不引用 |

### 表 2：生产者→消费者（我实际核过的链路与断点）

| 链路 | 核实结果 | 证据 |
|---|---|---|
| F001 `operation_capability` → A002 `validate_stream_plan` | **通**（A002 callee 明列 F001） | A002 卡；capabilities.py 基线 |
| A001 `admit_action_plan` → A003/A004/A006/A007/A008 | **通**（caller 列表一致） | plan.json caller_tasks |
| A001.recovery(4值) → L007/L004.wire(2值) | **断**：映射未定义，两字面量零卡命中 | Q08 |
| A006/A007 → L007 `compile_native_topology` → F004 + L030 + L004 | **通**（caller/callee 双向一致） | L007 卡 |
| L030 → `stage_plan_artifact`（每子计划） | **断**：write/model 子计划应走 code-pin-only 权威 | Q09；chain.py:873-905 |
| L030.ProgramRef → `_seal_plan_code` 返回键 | **通**：`{path,sha256,bytes,code_pin_digest,code_sources}` 与 ProgramRef 逐键匹配 | chain.py:846,869 |
| L002 `interpret_physical_plan` → 单一漏斗 → queue_chain/graph/chain/worker | **通**：四处 driver 侧均调 interpret+load_bindings+load_sealed_plan；四处 worker 侧均调 load_worker_layout | queue_chain.py:215-220、graph.py:641-646/1055-1060、chain.py:1200-1201；worker/graph_execution.py:104、worker/execution.py:76、worker/queue_transform.py:82、chain.py:1179 |
| `ExecutionLayout.executions` 加性字段 → v1 wire 兼容 | **通（已排除疑虑）**：生产代码无 `to_wire(layout)`；唯一整体序列化在 test_execution_layout.py:441，该测试不断言全键集 | plan_interpreter.py:130-145；test_execution_layout.py:438-449 |
| `physical.stages[].budgets` → `ExecutionPlacement.stage_limits` | **通**：冻结 dag.py 恰好三键、正整数≥1 | dag.py:425-434 |
| S001 → S004/S006/S007/S009/S011 | **通**（caller 一致） | plan.json |
| S001.auto_json → 封存 ownership 一致性 | **通（Pass 10 更正）**：C04 明文"factory或auto_json…采取shared_colocated"，S001 为唯一策略，S004/S009/S010/S011 四消费点均列 S001 为 callee。**原判断"断"已撤回** | ~~Q04~~ 已撤回；残留见下一行 |
| **S011 升 ownership → 未派发的 `validate_operator_spec:182` 要求 adapter_present=True** | **断（运行时实证）**：真 `compile_operator_spec` 上 `Counter`+`shared_colocated` 抛 `STATE_ADAPTER_MISSING`；`none` 则通过。S009 禁构造 class ⇒ 必须取"延后可用=True"，而 C04/S009 均未写明 | **Q57／Pass 10 我亲自跑**：`/tmp/demiflow-qwen-pass10/probe_q04b.py` |
| S005 → R009 `prepare_stream_blocks` | **通**，但只覆盖 materialize 路径，不覆盖 take/iter delivery | S005/R009 卡 |
| R005 `make_stage_buffer_adapter` → R006 `bind_stages` | **通**（caller=R006） | plan.json |
| R002 `align` → R007/R012/R015 | **断**：快照字段无持有者；并发聚合点无主 | Q02/Q03 |
| M005 `native_stream_recovery` → R006 | **通**（caller=R006，消费点真实存在 stream_materialize.py:239） | plan.json；源码 |
| M004 `bind_actor` → `_BorrowedJournal` → `journal.reserve` | **断**：reserve 需预算预留，Embedding 无 coordinator | Q01 |
| M007 `_exchange` → M008 `scope_request` / M012 `_fresh_exchange` | **通**：`_fresh_exchange` 唯一调用点在 `_exchange:176`，先 scope 后 fresh 成立 | runtime.py:176,189 |
| M011 `aclose` verify_preserve → `_BorrowedJournal.close` | **通**：借用包装 close 为 no-op，不会关 run journal | stream_recovery.py:176-177；runtime.py:318-319 |
| L034/L035 async ACK → `progress.seal` lambda → `stream_barrier` isawaitable | **通**：`concurrent.futures.Future` 可 wrap_future；两侧各只有一个调用点 | worker/execution.py:17,125；progress.py:84；graph_progress.py:296；stream_barrier.py:83 |
| L034 超时 → `deliver_receipt` 迟到 ACK 隔离 | **断**：chain 侧 `_pending` 无 operation_id 键控且该函数无主 | Q10；worker/execution.py:142-146 |
| W015 `_bridge_node_map(key_kind)` → W018 → W020/W021 → W019 | **断**：occurrence 边与冻结 dag 冲突；无运行状态通道；stages 按 logical 键 | Q07/Q14/Q16 |
| W019 → `loadSource`/`chooseNode` → W006 `bound_source` | **断**：前端入口未派发，从不传 occurrence | Q15 |
| W001 "识别 v1/v2" → F1 v2 生产者 | **断**：F1 只有 v1，`seal_plan_facts` 无任务 | Q19 |
| V001–V021 → 门命令 | **断**：0 条命令执行新验收文件（Pass 1 首报，Pass 5、Pass 6 两次独立复算命中） | Q06；我复算 27 bash 块＝52 唯一文件，新验收文件命中 0、散文命中 1 |
| **117 历史失败 id → 门命令** | **断（新，最严重的验收盲区）**：44 条被覆盖／**73 条不被任何门执行**；6 个文件真实存在却零命中；无 id-diff 守卫 | **Q53**；我亲自解析审计 §1.2＝118 行/118 id，扣 T＝117；52 文件与 gates 双向差集空；裸名用例实在 `test_stream_map.py:546` |
| **`Dataset.take/iter_rows/count` → RunAction → L007 `compile_native_topology(action,…)`** | **断（运行时实证）**：三者 `current_action()` 均为 `None`（`current_run()` 已设置）；对照 `dataset.py:1897 materialize` 用 `action(...)` 确实开 RunAction | **Q27／Pass 8 我亲自跑**：`/tmp/demiflow-qwen-pass8/probe_routes.py` |
| **take/iter_rows/count → `unified_chain_eligible` 分流缝** | **断（运行时实证）**：seam 调用 **0** 次、`_apply_plan` **3** 次，而三者结果全部正确 ⇒ **V001 会 green-on-legacy** | **Q50／Pass 8 我亲自跑**；缝唯一调用点 `executors/local.py:863`（在 `materialize_stream` 内） |
| **M `bind_actor` → `RunPromptJournal.reserve` → 执行预算预留** | **断（运行时实证）**：`stream_recovery.py:148` 抛 `RecoveryContractError: Native request has no execution budget reservation`；对照臂手动设 `CALL_RESERVATION` 后 `reserve()=True` ⇒ 唯一缺失量就是预留 | **Q01／Pass 9 我亲自跑**：`/tmp/demiflow-qwen-pass9/probe_q01.py` 臂 A/B |
| **M 复用 `NativePromptRecovery.bind_client` → `actor._runtime.coordinator`** | **断（运行时实证）**：真 `EmbeddingActor` 上 `:206` 抛 `AttributeError: 'EmbeddingActor' object has no attribute 'coordinator'`（`_runtime is actor` 为 True） | **Q01 第二断点／Pass 9 我亲自跑**：同上臂 C |
| **`StageBufferSnapshot` 五字段 → `_Stage` 持有者** | **断（运行时实证）**：`__slots__` 30 项不含它们，`setattr` 全部 `AttributeError` ⇒ agent 连自行补字段都做不到 | **Q02／Pass 9 我亲自跑**：`probe_q02.py` 臂 A |
| **`_CheckpointBarrier.arrive` → `progress.align` → 全部 worker 的 pending** | **断（运行时实证）**：每 cut align 仅 1 次；c=2 只收后到者 batch；`native_snapshot=True` 时 `tokens()=1`，收先到者 batch | **Q03＋Q56／Pass 9 我亲自跑**：`probe_q02.py` 臂 B/C |
| **grouped 阶段 → barrier 对齐** | **断（AST＋运行时实证）**：`grouped_worker` arrive 调用点 **0**；`batch_worker:576` 先 return 进 grouped；barrier 落分组路径抛 `TypeError`×2；缺 arrive 则 `TimeoutError: Checkpoint coordination deadline exceeded` | **Q03／Pass 9 我亲自跑**：`probe_q03.py` |
| **R006 解除拒绝 → 4 种 buffered kind 的替代对齐路径** | **断（运行时实证）**：`:199-202` 今天确实拒绝 concurrent batch/timed batch/grouped/prepend 四种；而这四种的持有者与对齐路径由上面三行证明不存在 | **Q47／Pass 9 我亲自跑**：`probe_q47.py` |
| A004 `count` 去嵌套 → 真实嵌套点 | **断（病因错）**：嵌套发生在 `stream_delivery.py:33 execution_scope()`／`:41 run.action`，不是"第二个公开 action"；`iter_stream_rows`/`base.count:69`/`local.count:975` 均未派发 | Q28；P4-01/P4-10（我采纳其更精确定位） |
| A006/A007 → `iter_stream_rows` 消费 ActionDecision/ExecutionPlacement | **断**：签名 `iter_stream_rows(executor,source,plan,*,limit=None)` **无 decision/layout 形参**且文件未派发 | P4-10；`execution/stream_delivery.py:25` |
| 原生关系分支 `_apply_plan` → 统一布局 | **断**：无主无任务 | Q29 |
| `plan_code_pin`/`_business_dependency_modules` → source 侧 callable 代码 | **断**：两者只遍历 `plan.operations`；join 的 reducer 是 **source 字段**；子审查实跑改 helper 后 pin 与 source identity 均不变 | Q31/Q46；`chain.py:676-686`、`:697-707`、`:747-777` |
| 新 `stage_buffers.py` ↔ 既有 `execution/exchange/`＋`contracts/exchange.py` | **断（重复权威）**：既有 1410＋375 行已实现 C14 的 barrier 消息协议与 InputGate 多输入对齐；设计对 `InputGate`/`DataEnvelope`/`ChannelSequence`/`route_epoch` **零命中** | Q42/Q43 |
| `reservation.py:255-271 derive_prefetch` 的 `max_pending_floor` ↔ C06"只更紧或相等" | **断**：下限零命中；源码明文低于下限会"gate 溢出、接收方在 cut barrier 到达前死掉" | Q43 |
| R006 删除 buffered fail-closed 守卫 → `stream_barrier.py` 联合触发 | **断**：被删守卫注释自陈"graph cut deadlock"（F3 家族）；barrier 未派发，其 8 个 progress 属性契约无主；C05 `next_output_ordinal` 无语义定义（2 命中均为 schema 键列表） | **Q47**；`stream_materialize.py:196-202`、`stream_barrier.py:211-214` |
| C04:98 auto_json 同 owner 并发=1 准入拒绝 → 实现者 | **断**：7 张 auto_json 卡无一实现；`precheck_operator_state`/`precheck_plan_state` 收不到 concurrency；4 张 concurrency 卡全属别的域 | **P4-06（我复核并加硬）**；`operator_precheck.py:17/:32/:39-40` |
| R002/L007/L030 新 durable_write → C10 必填 owner | **断（3 卡）**：三张均为无基线全新函数，`stage_buffers.py` 基线不存在；另 18 卡为文档缺口（13 verify_preserve＋5 有基线且散文已钉权威） | **P4-04（我改判 P1×3＋P2×18）**；`CONTRACTS.md:236` |
| C04:113"其余六个承重文件字节冻结" → 枚举/机器守卫 | **断**：设计树全文无枚举；`check_design.py` frozen 命中 0；baseline.json 39 文件缺该 6 件；与既有 L2 裁决"stream.py 不改"交叉未声明取代 | **Q52**（Pass 5 P5-01 独立同结论） |
| W occurrence → 冻结 `dag.py` 边端点键集 | **断**：`dag.py:296` 端点键集为 `{logical_node_id, port}`，无 occurrence；`_physical_summary` 未派发且 `facts.py:255` 按 logical 折叠 | Q07/Q14/Q16；P4-07/P4-08 |
| c_budget dirty `plan_facts.py` → F002 吸收 | **通（应吸收而非重实现）**：已改单一权威 `if kind=="unknown": raise`，`_specialization_domain` 5 特化与 C02 逐项一致 | Q24/Q38；P5-07；我读到基线 A 族根因 `operation_facts:119-123` 第二套 12 类集合 |
| 同上 `model_map.options` → 封存永久载体 | **断（安全）**：dirty 分支 `options=dict(operation.options)` 无过滤；`http_options.py:87` 守卫以 `credentials_env` 已设为前提；`:93` 明文"never part of a request journal or hash" | **Q54**（可达性为推断，两处行级事实已核） |
| F001 `operation_capability` → C02 基础键 | **通（但卡口径错）**：基线 `:79-102` 已满足 13 类 kind＋5 返回键＋同模块回退；派 implement 无待改点，改动即字节漂移风险 | **Q55**；P5-09 |
| Q003 保留 component/ordinal ＋ C06 禁取消守卫 → `run_journal.py:1004` | **通（双重保护，§6 不变量在设计里被两处独立守住）** | `run_journal.py:1004-1005`；Q003 卡；C06 |
| 144 卡基线三元组 → bbd93ff | **通（我亲自机器复算）**：96/96 sha＋行号＋签名；42/42 新增声明诚实；39/39 文件摘要零漂移 | Pass 7 `/tmp/demiflow-qwen-pass7/baseline_verification.json`；Pass 5 附件 B 独立同结论 |
| 20 根 → 27 门 → 144 卡 → 64 既有测试文件 | **通（程序化）**：0 缺失门、0 孤儿门、0 未知叶任务、64/64 文件真实存在 | Pass 6 |
| 历史 F1–F6/F7b/F7c/F9/§6/8.5/8.6b → bbd93ff | **通（已修，我逐条回源码核）**：见 Pass 3 对账表；8.6a 仍开 | Pass 3；`graph_router.py:161/174/369-372`、`stream_barrier.py:211-214`、`graph.py:538/892-905`、`dag.py:109`、`chain.py:496-514` |

### 表 3：必改 / 可后延（按原始意图影响分级）

分级依据是全局约束「精力按意图相关性分级」：**直接妨碍核心意图、关键架构边界或历史保护的优先保障；外围完善从简、可后延，但必须显式记为未覆盖，不得记为通过。**

#### 第一优先：不修则"实现完仍不满足用户意图"（核心意图直接被打掉）

| 项 | 级别 | 理由（对意图的实际影响） | 证据形态 |
|---|---|---|---|
| **Q53** 73/117 历史失败无门可跑 ＋ 无 id-diff 守卫 | P0·阻塞验收 | **本轮升级的立项理由就是清掉这 117 条**。62% 无门可证 ⇒ "历史失败已清"不可判定；N2 迁移 4/5 文件正在此集合内 | **我亲自复算**（118 id／52 文件／44+73） |
| **Q06** 21 个新验收函数 0 条执行命令 | P0·阻塞验收 | 新功能可零验证宣布通过；与 Q53 合起来＝**新旧两侧都验不到** | 三遍独立命中（Pass 1/5/6） |
| **Q27＋Q28＋Q29** take/iter/count 封存入口 | P0·需重做 | "一份封存布局驱动所有已支持路线"这条**平台意图在设计里没有闭合**；三处文件/函数全无主 | **Pass 8 我亲自跑**：`current_action()=None`×3 |
| **Q50** green-on-legacy（分流缝未被咨询） | P1·阻塞宣称 | 架构 U5 退出条件"没有无声退回"被违反；V001 只断言结果正确 ⇒ 统一执行一次没发生也会全绿 | **Pass 8 我亲自跑**：seam 0 次／`_apply_plan` 3 次／结果正确 |
| **Q47** R006 删 fail-closed 守卫而 `stream_barrier.py` 无主 | P0·需重做 | 被删守卫注释自陈"graph cut deadlock"＝**F3 家族**；barrier 是 F3 联合触发唯一持有者，8 属性契约无主 ⇒ 新机制绕过已修保证 | **Pass 9 我亲自跑**：4/6 kind 确认被 `:199-202` 拒绝；三 ordinal 空间并列 |
| **Q42＋Q43** buffer 层与既有交换权威重复 ＋ `max_pending_floor` 零命中 | P0·需重做 | 两个 agent 各自按卡完成，**组合即在恢复最关键时刻崩**（源码明文"接收方在 cut barrier 到达前死掉"） | 行级源码＋设计零命中（我核） |
| **Q02＋Q03＋Q56** buffer 快照字段无持有者／grouped 无 barrier／`arrive` 聚合无主 | P0·需重做 | 直接打掉"不丢不重"的恢复正确性；不是接线量，是要先改运行时数据结构与切点协议 | **Pass 9 我亲自跑**：5 字段 `setattr` 全 `AttributeError`；`grouped_worker` arrive=NONE（AST）＋两 TypeError＋TimeoutError；native token=1 只封存首个 worker |
| **Q01** Embedding 借用 journal 缺预算预留 | P0·需重做 | 第一次 native reserve 必抛 `RecoveryContractError` ⇒ 向量/模型恢复直接不可用 | **Pass 9 我亲自跑**：`:148` 抛＋`:206` 抛（真 EmbeddingActor）＋对照臂设预留后 `reserve()=True` |
| **P4-06＋Q05** auto_json 并发=1 准入无实现者 ＋ 能力退步 | P0/P1·需重做 | 用户最关心的"普通类实例真崩溃恢复"的**安全包络**；C04 明文"声明更高并发时准入明确拒绝，不偷偷降并发" | 我扫全 144 卡：7 张 auto_json 卡无一实现该拒绝；`OperatorTraits` 11 字段无 concurrency（Pass 10 实测） |
| **Q57** factory 的 `adapter_present` 延后取值未冻结 | P1·两句话可修 | 只做 S011 不做 S009 的配套取值 ⇒ **每个无钩子类工厂在封存期被拒**，正打掉用户核心意图 | **Pass 10 我亲自跑**：`Counter`+`shared_colocated` → `STATE_ADAPTER_MISSING` |
| **Q31＋Q46** 源侧 callable 代码不固定 ＋ cloudpickle 充当持久协议 | P0·需重做 | join reducer 是 source 字段；改码后 pin 与 identity 均不变、恢复不拒绝 ⇒ 历史保护失效。**缺口只在 source 侧**（operations 侧已被历史探针证明 fail-closed） | 子审查实跑＋我核 `chain.py:676-707/747-777` |
| **Q09** 封存权威冲突（pickle vs code-pin） | P0·需重做 | write＋模型这条最重要链；与 P4-04 的 L007/L030 owner 缺失是同一决策的两面 | 行级源码（我核） |
| **Q07＋Q14＋Q15＋Q16＋P4-07/08** occurrence 图 | P0·需重做 | 冻结 `dag.py:296` 端点键集无 occurrence、`mountLogical.update` 签名冻结且无运行状态通道、`_physical_summary` 按 logical 折叠 ⇒ 要么改冻结文件、要么画一张无边可连的图 | 行级源码（我核） |
| **Q12＋Q13＋P5-05** 反转 E 的 D2 断言 ＋ S 卡重基 | P0·阻塞装配 | 身份碰撞与状态丢失会进入承重路径；**冲突断言既不被门跑到（与 Q53 叠加）也不被卡纠正**（S013:51 要求保留原断言） | 子审查行级取证（我采信行号，未逐行重读该测试） |
| **Q05** 去掉并发硬拒/锁误伤 ＋ 纳入回归门 | P1·阻塞装配 | **能力不退步是硬底线**；`78d6492` 已核为退步源，A005/A006 已要求撤销 | 完整 diff（我核） |
| **Q08** recovery 四值→两值映射未定义 | P1·阻塞编码 | 决定 take/iter 能否封存，影响能力与诚实恢复；两字面量零卡命中 | 程序化零命中（我核） |
| **Q10** TimeoutError ＋ 迟到 ACK 隔离 | P1·阻塞编码 | 恢复正确性与异常权威一致；`deliver_receipt` 无主、`_pending` 无 operation_id 键控 | 行级源码（我核） |

#### 第二优先：阻塞装配/合流，但方案方向正确（只需写死映射、指定持有者或补守卫）

| 项 | 级别 | 理由 | 证据形态 |
|---|---|---|---|
| **Q52＋P5-01** 六个字节冻结文件未枚举、无机器守卫、与 L2 裁决取代关系未声明 | P1（两遍独立同结论，**验收上按 P0 对待**） | 实现者无法知道边界；`baseline.json` 覆盖的正好是允许改的、不覆盖的正好是禁止改的。**修成本极低：39 文件机制已存在** | 我核 `check_design.py` frozen 命中 0＋baseline 差集 |
| **Q54** 吸收 F002 时 `options` 无过滤 → 凭据/本机路径进永久载体 | P1·阻塞 F002 吸收 | 违反 C02"非秘密语义"与源码自身"never part of a request journal or hash"；凭据轮换还会改身份拒旧运行 | 两处行级已核，**可达性为推断** |
| **Q17＋Q18** skip 计账 ＋ 三条硬守卫（117→0／按 id 新增失败=0／冻结七件字节） | P0·阻塞验收 | 浏览器门可全 skip 变绿；没有硬守卫，"全绿"无意义 | 程序化（我核） |
| **Q35** `test_s6_commit_window_quota.py` 不在 CODEC 门 | P1·阻塞验收 | 真正断言 canonical 编码与 §6 不变量的测试跑不到 ⇒ 已知数据保真缺陷可留存 | 我核门文件集合 |
| **Q19＋Q24＋Q38＋P5-07** W001 v1/v2 语义、F002 吸收而非重实现 | P1·阻塞装配 | 避免两份实现与无对象分支；A 族 36 条根因＝两套并行准入权威（我在基线 `operation_facts:119-123` 直接读到） | 我核 dirty diff＋基线 |
| **P4-04（3 卡）** R002/L007/L030 新 durable_write 无 owner | P1·阻塞装配 | 全新持久状态路径无可继承 owner；与 Q02/Q03、Q09 是同一件事，**不应重复计工作量** | 我程序化核 21 卡＋基线存在性 |
| **P4-02＋P4-03** 三卡参数类型误标（R010 `execution`、W001/W002 `target`） | P1·阻塞编码 | 卡内描述与自身冻结签名/基线相反，会诱导错误实现。**范围恰为 3 卡**（我收窄：5 张写 execution 只 R010 错、4 张写 target 只 W001/W002 错，S009 正确） | 我逐行核 `data/plan.py:89-90`、`facts.py:96` |
| **Q30／Q39／Q11** `available_backends` 无生产者、route 死值＋seam 边界、A001 无 callee | P1·阻塞装配 | 新公共接口只有生产者没有消费者，违反 C10 | 程序化 caller/callee（我核） |
| **Q32／Q33／Q34** 迟到 ACK 60s/120s 冲突、C12 公式未冻结、预算落不到真实 I/O | P1·阻塞装配 | 预算与超时若不落到真实 I/O，"资源不超卖"只是账面成立 | 行级源码（我核） |

#### 可后延（按精力分级从简，但必须显式记为"未覆盖"，不得记为通过）

| 项 | 级别 | 处理方式 |
|---|---|---|
| Q55 F001 应改 verify_preserve | P2 | 装配前顺手改 mode，避免无谓改动承重函数致字节漂移 |
| P4-05 M002 `read_only` 标签与 CREATE INDEX 矛盾 | P2（**我从子审查 P1 改判**） | 幂等索引，删除只退化性能、不改 `UncertainPromptCall` 语义 ⇒ 改标签即可 |
| P4-04 其余 18 卡 owner 字段缺失 | P2（**我从子审查 P1 改判**） | 13 verify_preserve＋5 有基线且散文已钉权威 ⇒ 补字段，不阻塞 |
| Q51 7 门 39 implement 零新验收测试 | P2 | **不必为每门新造测试**；只需在 gates.json 声明"根本意图由门并集判定" |
| P4-11 27 卡 errors 漏列直接 raise | P2 | 按覆盖表逐卡补齐；装配期加"errors ⊇ 函数体直接 raise 集合"校验（子审查结论，我未复核） |
| P4-12 S007/S008"只操作本对象"不实 | P2 | 副作用描述列明 `bind_execution`/线程任务/budget 变更（Q34 同类新实例） |
| P4-13 装饰器不入基线 SHA | P2·验收加固 | 我已实证 `Dataset.count:2007` 的 `@_flow_api` 被排除，而 `forge/platform.py:226/:360` 消费 `__demiflow_flow__` ⇒ 摘要纳入装饰器或卡内显式列出 |
| P4-14 W020 JS 锚点越界到 EOF | P2·验收加固 | 对"最后一个函数"用显式结束边界（子审查结论，我未复核） |
| Q20 冷门富输出类型/开发历史细分门（覆盖 52/458） | P2 | 外围完善，不阻塞核心；root 覆盖表记"未覆盖" |
| Q21／Q25／Q26／Q37／P4-15 文档措辞与口径 | P2 | 装配前顺手改；P4-15 已澄清 **45＝44 生产＋1 共享测试文件**，非矛盾 |
| Q22 V 粒度拆分 | P2 | 装配任务必做（先建文件与 fixture），按门拆分可渐进 |
| Q23 非有限输入身份、Q36 typed_cache 折叠 | P2 | 边角保真，不阻塞核心交付 |
| Q48／Q49 Pass 3 历史项残余 | P2 | 见 Pass 3 对账表 |
| P4-09 三个 `start` 卡"拒绝"实为静默 return | P2 | 子审查结论、我未复核；改描述为"幂等 no-op"即可 |
| P5-10／P5-11 返回注解漏写、E 树聚合 sha 不可复现 | P2 | 子审查结论、我未复核 |

#### 环境受限

| 项 | 处理 |
|---|---|
| RAY 门（指定 env 无 ray） | **记 blocked**，不能算通过，也不能为它改验收口径（V019 已正确写明"Ray 不可用则明确未验，不得 skip 计通过"） |
| 浏览器 journey 门 | 可 skip 变绿 ⇒ 必须先修 Q17 的 skip 计账，否则记未验 |
| Pass 8 对照探针 A（materialize+thread map） | 我两次均抛 `Worker did not register`，**不据此声称产品缺陷**；该对照仍只有静态证据 |

### 表 4：证据账

#### 4.1 读过的设计文件（全文）
QWEN-REVIEW-BRIEF.md、CONTRACTS.md（C01–C12）、INTEGRATION-ACCEPTANCE.md、FILE-OWNERSHIP.md、interfaces.pyi、TEST-COMMANDS.md、REVIEW-10-PASSES.md、tools/check_design.py、design-validation.json、root-map.json、gates.json（全 27 门字段）、baseline.json（全 39 文件）、plan.json（144 任务全字段程序化分析）、tools/build_plan.py（基线摘要生成逻辑）。

#### 4.2 读过的任务卡
**Pass 4 子审查逐张通读全 144 卡**，并先逐字段证明 144 卡＝plan.json 忠实渲染（0 差异）；覆盖表在 `/tmp/demiflow-qwen-pass4/coverage_table.md`。
**我本人全文精读**：A001–A006、S001/S004/S005/S012/S013、L001/L002/L003/L007/L009/L010/L012/L030/L031/L034/L035/L036/L037/L038、M002/M009、W001/W002/W004/W005/W018/W020、V001/V003/V008、Q001/Q002/Q003/Q007/R002/R003/R006/R007/R008/R010、F001/F002。其余卡通过 plan.json 的 signature/behavior/caller/callee/errors 字段程序化核对。

#### 4.3 读过的源码（bbd93ff，integration_c 树；跑前跑后均断言 HEAD 与 clean）
planning/dag.py:109-138,285-300,395-464；planning/traits.py:60-80；planning/model.py:51；execution/plan_interpreter.py:32-50,95-150,222-234,530-660；execution/plan_facts.py:79-128；execution/stream.py:96-140,262,323-420,560-690；execution/stream_barrier.py:30-100,137,177,195,204,211-214,230；execution/stream_materialize.py:177-211,230-265,380-469；execution/stream_delivery.py:25,33,41；execution/chain.py:310-329,395-514,555,676-707,737-777,826-915,1200-1201,1233；execution/executors/local.py:747,754,817,853-882,883,975,983,995,1171；execution/operator_precheck.py（全 40 行）；execution/contracts/operator.py:1-30,67-73；execution/contracts/errors.py:36；execution/run_journal.py:983-1019；execution/dataset_native.py:104,122-125,551；execution/exchange/graph_router.py:161,174,309,364,369-372；execution/graph.py:538-539,575,611,681,892-905,971-972,1008,1106；execution/worker/execution.py:1-30,61,108-165；execution/worker/graph_execution.py:63,89,104,113,131,139,155,203,210-250；execution/worker/queue_transform.py:67,82；execution/worker/progress.py:70-85；execution/worker/graph_progress.py:92,176,280-300；execution/effect_keys.py:50；execution/reservation.py:255-271；operator_llm/stream_recovery.py:46-95,100-215；operator_llm/http_options.py:10-22,80-100,131-141；operator_llm/runtime.py:70-95；embeddings/runtime.py:48-90,148,160-200,306-342；data/plan.py:77,89-90,149,151,483,511；data/dataset.py:1305-1326,1888-1911,2007；data/flow_contract.py:46；forge/platform.py:226,360；workbench/runs/facts.py:93-99,245-300；workbench/runs/manager.py:462,509,600,773；workbench/web/workbench.js:70,148-260,285,309-335,498-512；workbench/web/workbench-graph.js:28-110；tests/test_stream_map.py:88-110,546；tests/test_execution_layout.py:430-460；tests/test_native_delivery_barrier.py:30；tests/test_forge_markdown_reading.py:60-70。

#### 4.4 读过的历史权威与裁决（主仓，只读）
`docs/demiflow升级代码Review-20261110.md`（F1–F6 全文 :51-135、§8.5/8.6 :246-275）；`docs/demiflow升级代码复审-20261113.md`（全 176 行，F9/F7c/F10/§6）；`docs/demiflow升级复审-§6提交崩溃窗口额度-裁决-20261112.md`（全 128 行，含真实探针 cycle 1→16→17 REFUSED）；`docs/demiflow升级复审-§7单行上限-裁决-20261112.md`（全 53 行，evidence-only）；20261112 三轮 review 的章节头；架构权威 `_demiflow/platform_coordination_20261104/unified_execution_architecture_20261107/`（target/current .mmd、**full_architecture_v2.html/.png**、15 个源码锚点 C01–C15、五模块 A–E、M01–M09 业务阶段、U0–U6 检查单）；`p1_l2_plan_interpreted_20261112/design_refined_verdict.md:49`（冻结/承重零触碰 APPROVE 清单）。
**117 逐 id 原始载体**：`c_budget_20261113/docs/upgrade-implementation-20261113/plan-facts-single-authority-design.md`（1344 行；§0.2 复跑口径、§1.2 行 119–236 全表、§2.7 后延 7 项、§3.5.4 的 28/117）。

#### 4.5 对照的在途树
e_dfamily `4489ba0`（operator_state.py:45-95、recovery_identity.py、tests/test_d2_recovery_identity_counterexamples.py 关键断言）；b_option `78d6492` 完整 diff（确认为能力退步源）与 `e517110`（dataset.py:1020-1022）；c_budget `90d7ee4` ＋ 唯一 dirty `execution/plan_facts.py`（56+/11−，我直接读 diff）；n3_resource/n1_prompt/n2_retired/d_workbench 的 HEAD 与 dirty 计数。
`git merge-base --is-ancestor` 核实：**f83ad3a／a39a344／15823f0／bd26c58 全部是 bbd93ff 的祖先**（这是 Pass 3 对账的前提）。

#### 4.6 实际执行的命令与探针

**只读 git**：`rev-parse HEAD`、`status --porcelain`、`log -1`、`show --stat`、`show <commit> -- <paths>`、`diff`、`merge-base --is-ancestor`。

**程序化分析（我亲自跑）**：plan.json/gates.json/baseline.json/root-map.json 的任务计数、caller/callee 双向闭包、门覆盖并集、字面量命中；27 个 bash 块的 52 唯一测试文件提取与 gates 双向差集；审计 §1.2 的 118 行/118 id 解析与逐文件分布、44/73 求和；21 张 durable_write 卡的 owner 模板行/具体行区分；全 144 卡的 auto_json（7 张）与 concurrency（4 张）扫描。

**机器复算（我亲自跑）**：`/tmp/demiflow-qwen-pass7/verify_baseline.py` 用 `ast.get_source_segment`+sha256 独立重算 144 卡基线三元组 → `baseline_verification.json`：`hash_ok=96, line_ok=96, sig_ok=96, new_no_baseline=42, js_skipped=6, problems=[]`；baseline.json 39/39 文件摘要零漂移。

**运行时探针（我亲自跑，Pass 8）**：`/tmp/demiflow-qwen-pass8/probe_routes.py`，解释器 `demiflow-env/bin/python -B`，工作目录 `/tmp/demiflow-qwen-pass8/run`，脚本断言 `demiflow.__file__` 落在隔离树内并打印确认。实测 take/iter_rows/count 的 `current_action()=None`、`current_run()` 已设置、seam 调用 0 次、`_apply_plan` 3 次、三者结果正确。对照探针 `probe_seam.py` 两次均抛 `Worker did not register`（含改为可导入模块级函数重试），**未确立对照，不据此声称产品缺陷**。

**运行时探针（我亲自跑，Pass 9）**：`/tmp/demiflow-qwen-pass9/` 下四个脚本，同一解释器与工作目录口径（`/tmp/demiflow-qwen-pass9/run`），每个都断言并打印模块来源；**全程不联网**（`EmbeddingActor` 只在声明期构造，`astart()` 从未调用）。
- `probe_q01.py`：臂 A `:148` 抛预算缺失；臂 B 设预留后 `reserve()=True`（对照）；臂 C 真 `EmbeddingActor` 上 `:206` 抛缺 coordinator。**第一版探针的 `AttributeError: no attribute 'drain'` 是我的方法名错误（`RunPromptJournal` 无 `drain()`，错误本就由 `sqlite_journal.py:414 return item.future.result()` 同步抛出），已修正重跑，不算产品缺陷。**
- `probe_q02.py`：`_Stage.__slots__`＝30 项，五字段 `hasattr=False` 且 `setattr` 全 `AttributeError`；真 `_CheckpointBarrier` 在 c=2 下 align 1 次只收后到者 batch；`native_snapshot=True` 时 `tokens()=1` 收先到者 batch。
- `probe_q03.py`：AST 枚举 `arrive` 调用点（`grouped_worker`＝NONE，`batch_worker`＝[605] 但 :576 先 return）；真 barrier 喂 `row_size`/`group_key` 各抛 `TypeError`；无 arrive 时 `wait()` 抛 `TimeoutError`、`status=failed`。
- `probe_q47.py`：真 `StreamStageProgress.bind_stages` × 真 `_Stage` 的 6 kind 矩阵，4 种被 `:199-202` 逐字拒绝、2 种越过守卫；三 ordinal 空间并列打印。

**只读性核验（Pass 9 跑前跑后各一次）**：隔离树 `integration_c_20261113` `git status --porcelain` 行数＝**0**、HEAD＝`bbd93ff`；主仓 `demiflow` dirty 项＝**11**（与入场时一致）、HEAD＝`e40a6dd`。**主仓未改、未跑测试、未推送、未合并、未停止任何在途工作。**

**运行时探针（我亲自跑，Pass 10）**：`/tmp/demiflow-qwen-pass10/` 下 `probe_q04.py`、`probe_q04b.py`、`mod/q04_counter.py`，同一解释器与只读口径，工作目录 `/tmp/demiflow-qwen-pass10/run`，不联网。
- `probe_q04.py`：真 `unified_operation_traits`/`precheck_operator_state`/`_adapter_present` × 三种声明形态（无钩子类工厂＝ACCEPTED、无钩子实例＝`STATE_ADAPTER_MISSING`、有钩子类工厂＝`shared_colocated/True`）；实测 `OperatorTraits` 11 字段**既无 `adapter_present` 也无 `concurrency`**。
- `probe_q04b.py`：真 `compile_operator_spec` 的 ownership×adapter 耦合三行表（`Counter`+`none`→COMPILED；`Counter`+`shared_colocated`→**REJECTED**；`HookedCounter`+`shared_colocated`→COMPILED）。**第一版把类定义在 `__main__` 被 `_callable_path:48` 正当拒绝，那是产品正确行为＋我的探针问题，改可导入模块后重跑，不算缺陷。**
- **本轮据此撤回自报 P0（Q04）**，另立 Q57（P1）。撤回依据是逐字读 C04 默认所有权段与 S001/S004/S005/S009/S010/S011 六张卡全文＋`FILE-OWNERSHIP.md:44`，不是探针。

**子审查执行的探针（我对其决定性结论逐条回源码复核）**：
- R+M：模块来源断言、check_design.py 退出 0。
- W+V：`/tmp/demiflow-qwen-wv-review/` 下 probe_occurrence/probe_drift/probe_undispatched。
- S：`/tmp/demiflow-qwen-s-probe/` 下 base/ehead 导出与 probe_counter/probe_resume。
- **L+F**：`/tmp/demiflow-qwen-lf-review/` 下 probe_codepin(1/2/3)、probe_bindings、probe_routes_ack —— **实跑证明改 reducer 导入的 helper 后 pin 与 source identity 均不变**、v1 三样例被真实解释器接受、v2 过真实 validator、per-slice `verify_loaded_plan` 能拒 join-right UDF 改码。
- **A+Q**：`/tmp/demiflow-qwen-aq-probe/` 下 probe_a–probe_f —— 真实 `data.local_execution` 下测 Actor 构造/take(0)/check_size(set)/set pickle 跨 seed。
- **Pass 4**：`/tmp/demiflow-qwen-pass4/`（verify_cards.py、sigcheck.py、errcheck.py、cardmap.py、dump.py），144 卡全量渲染核对＋签名/raises 静态抽取。
- **Pass 5**：`/tmp/demiflow-qwen-pass5/`（verify_signatures.py、dump_public_sigs.py），144 卡＋34 契约符号签名核对、冻结七件逐文件 sha256、117 载体解析。

**口径更正（重要，我推翻自己先前的写法）**：
1. 我先前写"本次也未在隔离树跑功能测试"——**不准确**。子审查在隔离树上跑过真实运行时探针，且 `/tmp/demiflow-qwen-aq-barrier/` 与 `/tmp/demiflow-qwen-lf-barrier/` 下存在 `test_delivery_timeout_quaranti0/run/run.sqlite`，对应真实用例 `tests/test_native_delivery_barrier.py:30 test_delivery_timeout_quarantines_worker_and_retains_run_lease`，说明**有定点 pytest 在 /tmp basetemp 下实际执行过**。我本人在 Pass 8 也亲自跑了运行时探针。**主仓始终未跑测试、未改文件。**
2. 我先前写"L+F 与 A+Q 两个子审查在本报告写入时仍未返回"——**已过时**。二者结论已返回并被我采信/复核（Q31/Q09/Q10/Q05 等即基于其取证），探针工件俱在；它们只是**未产出独立 REPORT.md**。Pass 4/Pass 5 是后续补派的全量核对，二者均已交付 REPORT.md。
3. 路径更正：`raw_fetch.py` 在 **`execution/`** 下；F002 的文件是 **`execution/plan_facts.py`**，`workbench/runs/plan_facts.py` **不存在**（该目录只有 `facts.py`）。
4. **我未运行 check_design.py**（它会写 design-validation.json，污染设计树）；引用的是树内已有结果。

#### 4.7 证据形态分级（哪些是实测、哪些是推断）

| 形态 | 覆盖的发现 |
|---|---|
| **我亲自运行时实测** | **Q27**（三终端无 RunAction）、**Q50**（seam 0 次／结果正确 ⇒ green-on-legacy）、**Q01**（`:148` 预算缺失抛＋`:206` 真 EmbeddingActor 无 coordinator 抛＋对照臂成功）、**Q02**（5 快照字段 `setattr` 全 `AttributeError`）、**Q03**（`grouped_worker` arrive=NONE 的 AST 证明＋barrier 触发两个 TypeError＋缺 arrive 致 TimeoutError）、**Q47**（4/6 kind 被现行 fail-closed 守卫拒绝＋三 ordinal 空间并列）、**Q56**（native token=1 只封存首个 worker）、**Q57**（真 compiler 上 `Counter`+`shared_colocated` 被拒；`OperatorTraits` 无 adapter_present/concurrency） |
| **我亲自机器复算** | Pass 7 基线三元组 96/96＋42/42＋39/39；**Q53** 的 118 id／52 文件／44+73；Q06 的 0 命令；P4-04 的 21 卡 owner 计数；P4-06 的 7 张/4 张扫描；Pass 6 的 20 根→27 门→64 文件闭合 |
| **我亲自逐行核源码/契约** | Q07/Q08/Q09/Q10/Q14/Q15/Q16/Q19/Q31/Q42/Q43/Q52/Q54/Q55、P4-02/P4-03/P4-05/P4-06/P4-13/P4-15、Pass 3 全部历史对账项 |
| **子审查实跑、我复核其决定性结论** | Q05/Q12/Q13、Q31 的改码不拒绝实证、L 包 v1/v2 fixture 与 ACK 可行性（Q01/Q02/Q03 已由 Pass 9、Q04 已由 Pass 10 提升为我亲自实测/复核） |
| **我亲自复核后撤回或降级的自报发现** | **Q04（P0 → 撤回）**：Pass 10 读 C04 默认所有权段＋S011 卡后确认设计已定死规则、调和任务齐全，原判把基线现状误当设计缺陷；残留另立 **Q57（P1，已实测）**。另有 P4-04（P1×21 → P1×3＋P2×18）、P4-05（P1 → P2）两条改判 |
| **行级源码推断（未运行复现）** | Q05 的并发丢 batch、**Q54 的凭据可达性**、prefix 实际重复、Q42/Q43 的接收方死亡时序 |
| **子审查报告、主审未复核** | P4-09/P4-11/P4-14、P5-10/P5-11、Pass 5 附件 A 的逐文件 sha 数值与附件 B 的 34 符号逐条（签名主结论与我 Pass 7 互证，采信） |
| **完全未验证** | 浏览器实际渲染、Ray、性能数值、21 个 V 函数的实际行为 |

**未验证/受限**：concurrency>1 的**端到端**实际丢 batch（Pass 9 已实测其**结构成因**：字段无持有者＋align 只见单 worker，但未跑崩溃-恢复全程）、prefix 实际重复、浏览器实际渲染、Ray、性能数值、V 函数行为。全部 JS 调用链、C01–C12 语义一致性、caller/callee 闭包为静态分析。**Pass 8 的 materialize 对照臂两次 `Worker did not register`，属我的调用形态/环境限制，不据此声称产品缺陷。**

#### 4.8 完整 Review 实际次数（如实记录，不把维度分项或包级复核计入）

**我本人完成的完整重审：2 遍。**
- **第 1 遍（Pass 1）**：设计包全部文档 ＋ 机器索引 ＋ 定点任务卡 ＋ bbd93ff 源码与在途候选逐条核对 → Q01–Q39。
- **第 2 遍（Pass 4）**：**全 144 卡逐张通读**（由我派出的子审查执行，先证明 144 卡＝plan.json 忠实渲染），**我对全部阻塞级结论逐条回到源码/契约行级复核**，并独立改判 2 条严重度（P4-04 P1→P1×3＋P2×18、P4-05 P1→P2）、加硬 1 条（P4-06）。

**另有 8 遍维度锚定深审，不计入完整重审次数**（每遍重新覆盖全设计的一个完整切面，但不是逐卡全文重读）：
Pass 2（以用户此前确认的架构图与模块边界为权威锚）、Pass 3（五轮历史 Review＋两份总控裁决的对账）、Pass 6（20 意图根→卡→门→命令→断言闭合审计）、Pass 7（基线摘要机器复算与冻结边界审计）、**Pass 8（Q27/Q50 第一手运行时实证）**、**Pass 9（Q01/Q02/Q03/Q47 四条 P0 第一手运行时复现，并新增 Q56）**、**Pass 10（Q04 运行复现 → 撤回自报 P0，新增 Q57）**。

**Pass 5**（45 文件签名/冻结字节/117 分配）由子审查执行并交付 REPORT.md，我复算其三条决定性结论（Q53/Q54/Q55）→ 计入维度深审的工作量，但**因其非我逐卡全文重读，不另计完整遍次**。

**用户要求的"反复完整 Review 10 遍"尚未完成（2/10）。本报告不声称已完成。**
GPT 的 REVIEW-10-PASSES.md 是 10 个**维度分项**复核，其本人已声明不等于 10 遍完整重审；我的 R+M／W+V／S／L+F／A+Q 五个子审查是**包级**复核，同样不是完整重审。

**时间口径（如实，按工件时间戳分段，不把空档计入）**：

| 取证段 | 起 | 止 | 时长 | 内容 |
|---|---|---|---|---|
| 第 1 段 | 2026-11-13 22:54 | 2026-11-14 00:32 | **1h38m** | 五个包级子审查探针 → Pass 4/5/7 机器复算 → Pass 8 运行时实证 |
| （空档） | 00:32 | 08:31 | ~8h | **零工件，不计入 Review 时长** |
| 第 2 段 | 2026-11-14 08:31 | 08:58 | **27m** | Pass 9：Q01/Q02/Q03/Q47 四条 P0 运行复现；Pass 10：Q04 复现→撤回＋Q57 |

**可核验的累计取证时长约 2 小时 05 分。** 原"至少 3 小时"的要求已由真人于 2026-11-14 放宽（原话"不一定要 3 个小时""review 完即可"），**故不再以时长为收口条件**；上表保留仅为可核取证记录，不用于充数。
此前 Pass 1 的文档通读与源码核对未留下带时间戳的独立工件，故不计入该可核区间（真实投入高于此数）。
**收口标准改为覆盖完整**：第一优先项已全部实测或已撤回，剩余推断项（Q05 并发丢 batch、Q54 凭据可达性、Q42/Q43 接收方死亡时序）已在表 4.7 如实标为推断，不影响分发决策。

---

### 文末进度表（§一 所引用的进度账）

| 遍次 | 锚点/范围 | 产出 | 状态 | 是否计入完整重审 |
|---|---|---|---|---|
| Pass 1 | 全部设计文档＋机器索引＋定点卡＋bbd93ff 源码 | Q01–Q39；四张简表初版 | 完成 | **是（第 1 遍）** |
| Pass 2 | 用户此前确认的架构图与五模块边界权威 | Q40–Q46；Q31 精确化收窄 | 完成 | 否（维度深审） |
| Pass 3 | 五轮历史 Review＋§6/§7 两份总控裁决 | 18 行历史对账表；Q47–Q49；3 处自我更正 | 完成 | 否（维度深审） |
| Pass 4 | **全 144 卡逐张通读**＋签名/raises/owner 核对 | P4-01~15；我复核并改判 P4-04/05、加硬 P4-06 | 完成（子审查执行＋我复核） | **是（第 2 遍）** |
| Pass 5 | 45 文件签名／冻结七件字节／117 逐 id 分配／在途漂移 | P5-01~11；我复算得 **Q53/Q54/Q55** | 完成（子审查执行＋我复算三条） | 否（维度深审） |
| Pass 6 | 20 意图根→卡→门→命令→断言闭合 | 闭合审计表；Q50/Q51；Q06 独立复算确认 | 完成 | 否（维度深审） |
| Pass 7 | 基线摘要机器复算＋冻结边界审计 | 96/96＋42/42＋39/39；Q52 | 完成 | 否（维度深审） |
| Pass 8 | **Q27/Q50 第一手运行时实证** | 三终端 `current_action()=None`；seam 0 次／结果正确 | 完成（对照探针 A 受限，已如实标注） | 否（维度深审） |
| Pass 9 | **Q01/Q02/Q03/Q47 四条 P0 第一手运行复现** | 四条全部升级为实测；新增 **Q56**（native token=1 只封存首个 worker） | 完成 | 否（维度深审） |
| Pass 10 | **Q04 运行复现 → 撤回自报 P0** | **Q04 撤回**（C04 已定死 factory ownership、S001 调和链齐全）；新增 **Q57（P1，已实测）**；实测 `OperatorTraits` 11 字段无 adapter_present/concurrency | 完成 | 否（维度深审） |
| 后续 | 仍是推断的项（Q05 并发丢 batch／Q54 凭据可达性／Q42-Q43 接收方死亡时序） | — | **未做** | — |

**累计发现**：Q01–Q57，**其中 Q04 已撤回**（Q31 经 Pass 2 收窄、Q06 经 Pass 5/6 两次复算确认、Q50 经 Pass 8 升级为实测、**Q01/Q02/Q03/Q47 经 Pass 9 升级为实测、Q57 经 Pass 10 实测**）＋ 并入的 P4-01~15 与 P5-01~11（重合项不重复计数）。
**自我更正记录**：44 vs 45 文件数（我错，设计对）；"标识符零命中⇒设计忽略 §6 修复"（误导，已更正）；C05 定义范围（避免过度声称）；Q50 误写为新 P0（实为 Q06 复算确认，已改）；`unified_chain_eligible` 调用点误置 `materialize`（实为 `materialize_stream`）；Pass 7 过度 praise 摘要装置（装饰器不覆盖）；表 4 "未在隔离树跑测试"与"L+F/A+Q 未返回"两处陈旧表述（已更正）；`raw_fetch.py`/`plan_facts.py` 路径（已更正）；探针方法名 `drain()` 不存在（Pass 9，我的 bug 非产品缺陷）；探针把类定义在 `__main__` 被正当拒绝（Pass 10，同上）；**Q04 由 P0 撤回（Pass 10，唯一一次撤回自报 P0——把基线现状误当设计缺陷）**；一度误写"需重做由七处降六处"（Q04 原不在七处内，当场核正）。

---

## 五、给分发决策的最小建议

按"对原始意图的实际影响"排序，**前 3 步不做完就不应分发编码**：

1. **先补验收口径（成本最低、收益最大，且不做则后面全部白做）**
   - **Q53**：把 6 个未覆盖文件并入现有门，**或新增一个 `HISTORY117` 门一次性跑 11 个失败文件**（后者更省，符合精力分级）；以审计 §1.2 的 118 id（扣 T）为基线加机器守卫，要求 `NEW=0 且 剩余失败=0` 并输出逐 id 差集。
   - **Q06**：把 21 个新验收函数写进门命令（每门追加 `tests/test_upgrade_v2_acceptance.py` 并用 `-k` 选中该门的 `new_acceptance_tests`），gates.json 增 `min_passed`/`forbid_skip`；先派一个 V 装配任务负责该文件的 imports/fixture。
   - **Q50**：V001 增一句**路径身份断言**（复用仓内既有惯例 `legacy_fetches == 0 and unified_fetches == 0`，或"改变封存预算能影响 take/count 的实际执行"——V010 已有正确写法可抄）；在 C10 命名 `unified_chain_eligible` 为唯一分流缝，规定新准入终端必须经该缝、fallback 不得只在 debug 级别可见；把 ADMISSION 加入 `P.unified_execution` 的门集合，**或明确声明本轮 take/count 仍走 legacy 且不宣称统一**。
   - **Q17/Q18/Q52**：skip 计账；**逐名枚举六个字节冻结文件**并把其 sha256 放进 `baseline.json`（39 文件机制已存在，只是没放进去，成本极低）；一句话声明本设计取代 L2 裁决对 `stream.py:_materialize` 与 `contracts/operator.py` 的不改约束。
2. **再修七处需重做的方案**（都是写死映射/指定持有者/指定权威，**不需要重写整体方案**）
   - Q01 Embedding 预算预留（或明确免预留模式）；Q02/Q03/Q56 buffer 字段持有者＋grouped barrier＋`arrive` 聚合；**Q47 先给 `stream_barrier.py` 派主并定义 C05 `next_output_ordinal` 语义，再谈 R006 拆守卫**；**Q42/Q43 明确新 buffer 层与既有 `execution/exchange/`＋`contracts/exchange.py` 的关系，并把 `max_pending_floor` 写进 C06**；**Q57 在 C04/S009 写明"factory 声明期 adapter_present 取延后可用=True"**（~~Q04~~ 已撤回，C04 本就定死了 ownership 规则）＋P4-06 并发=1 准入拒绝的实现者；Q07 occurrence 图与冻结 dag 的边界；Q09 封存权威（pickle vs code-pin）；Q31 源侧代码固定。
   - **Q27/Q28/Q29 的最小修正已实测定位**：让 `Dataset.take/iter_rows/count` 经 `action(...)` 开 RunAction（`materialize:1897` 已是现成范例）；派发 `stream_delivery.py`/`executors/base.py:count`/`local.py:count`/`_apply_plan`；给 `iter_stream_rows` 加 decision/layout 形参或改由已派发函数消费。
3. **补无主文件的归属**：`stream_barrier.py`（8 个 progress 属性契约）、`operator_llm/runtime.py`、`workbench/graph/logical.py`+`source_view.py`、前端 `loadSource`/`chooseNode`、`_physical_summary`（并改 occurrence 键）、`deliver_receipt`、`stream_delivery.py`。
4. **在途候选的吸收责任划清**
   - **S 卡重基到 E 的 `4489ba0`，并明确反转 D2 的六处断言**（L65/L88/L126-128/L138/L142/L172-173）；**不整包吸收 `1d7b350`/`4489ba0`**；把 `test_d2_recovery_identity_counterexamples.py` 纳入 STATE/IDENTITY 门（否则冲突断言既跑不到也纠不了）；S013:51 样板改为"保留符合 C04 的既有断言；与 C04 冲突的候选断言必须反转并记录理由"。
   - **F002 吸收 c_budget dirty 而非重实现**，但**吸收前必须修 Q54**：`model_map.options` 做白名单＋语义化，剔除凭据/头与 `journal_dir`/`stream_log_dir`/`object_store`，并把过滤规则写进卡。
   - **F001 改 verify_preserve**（Q55，基线已满足 C02，改动即字节漂移风险）。
5. **装配前顺手改的卡内错误（范围已收窄，不必扩大）**：**恰 3 张参数类型误标**（R010 `execution` 实为 `'inline'|'thread'`、W001/W002 `target` 实为 run 根路径）；**恰 3 张新 durable_write 补 owner**（R002/L007/L030）；M002 标签改 `runtime_state`（幂等启动索引）；P4-09 三个 `start` 描述改"幂等 no-op"；27 卡 errors 补齐；摘要纳入装饰器（P4-13）。
6. **按全局精力分级后延，但必须显式记"未覆盖"**：Q20/Q23/Q36、冷门富输出类型、Q51（只需声明门并集，**不必为每门新造测试**）、P4-11/P4-12/P4-14/P5-10/P5-11。**RAY 门记 blocked**，不能算通过，也不能为它改验收口径。
7. **本 Review 的收口状态（Pass 10 后定稿）**：第一优先项 **Q01/Q02/Q03/Q47 已在 Pass 9 全部做成运行复现**；**Q04 已在 Pass 10 复现后撤回**（我判错，C04 本就定死了规则），残留窄风险 Q57 也已实测。**仍是推断的**只剩 Q05 的并发丢 batch、Q54 的凭据可达性、Q42/Q43 的接收方死亡时序，已在表 4.7 如实标注，**不影响分发决策**。完整重审 2 遍＋维度深审 8 遍；时长要求已由真人放宽（"不一定要 3 个小时""review 完即可"），**本 Review 至此收口**。**一条方法论提醒留给后续复核者：凡判为"设计漏了/契约矛盾"的项，都应先读该契约整节＋对应卡全文再定级——Pass 10 已证明这类误判真实发生过一次（Q04）。**