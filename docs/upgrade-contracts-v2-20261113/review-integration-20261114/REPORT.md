# DemiFlow：核心修复与候选集成 Review（2026-11-14）

**结论：实现进展是真实的，但尚未按设计串成统一执行闭环。IL009 和本轮 IL012 验证入口问题已修；另有 7 项必须处理的核心问题，暂不合流、不宣称完成。**

本报告按照真人最新要求：先修明确升级裁决项，再审实际源码和组合，其他问题列出来，确认后统一修复。没有继续派发任务、修改其他会话工作树或接管它们的未提交文件。

## 1. 两大原始意图与本次判断

- 产品 root：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较和评测数据。
- 平台 root：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。

本批对产品最重要的交付是“页面上的运行计划与实际执行一致、历史选择和源码观测可追溯”；对平台最重要的是“公开 API 真正消费同一封存布局，状态/效果/资源跨恢复保持正确”。新接口存在、独立函数测试通过，都不能代替这两条。

当前：代码骨架保持单 journal、单提交权威和既有资源体系；但 A 的公开入口没有接入 L 的新执行器，F 的新原生事实漏掉业务参数，M/R 的批模型恢复协议冲突。**这些恰好是核心意图，不是边角料。**

## 2. 取证基线、进度与边界

隔离集成树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_integration_review_20261114`；分支 `engine/gpt-integration-review-20261114`。

- 运行共同基线 `bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。
- 设计/派工基线 `afbe9f3ee609ddf856b13d8be46dcc690c013347`。
- `6a5b5ab`：按独占文件所有权装配 11 路已提交源码及其已提交测试，不复制 R 的 5 个存量 dirty 测试，不复制 V 在途文件。
- `8e942a4`：IL009 声明式 admission 修复、测试和进程内来源验证入口。
- `33f642f`、`9f822f9`：可迁移的测试来源断言、验证入口对 spawn 子进程的修正；使用最终分支，不单独拿中间验证脚本。
- 主仓未修改、未运行测试；未 push、未合主仓、未重启服务或调用付费模型。7 个冻结文件与共同基线逐字节相同，见 [冻结摘要](evidence/frozen-audit.json)。

07:46 UTC 的回执统计：**11/12 路交付；181/198 张卡标 tested，15 claimed，2 needs_decision**。另 7 个补充单元为 L-native 6 + A-U01。181/198=91.4% 是执行方的卡状态，**不是独立验收通过率，也不是用户意图完成度**。V 仍在途，且 V019 回执仍保留已由 A-U01 修复的旧 sort bool 结论，需在集成基线上刷新。

| 路 | 本轮装配 HEAD | 对照设计的独立判断 |
|---|---|---|
| F | `07d32bbb` | DAG/原生事实有交付；原生事实与实际 actor、恢复身份仍有 IR-02 缺口 |
| A | `4122d64a` | 终端准入、sort 修复有效；核心 compile/load/execute 接线未完成（IR-01），与 R prepend 冲突（IR-05） |
| S | `6cd7f970` | class/状态快照/VectorSearch 有实现；普通实例 auto_json 的准入不符合 C04（IR-06） |
| R | `afe72954` | 5 种 buffer 与 slot 聚合已实现；纯批/分组有通过证据，prepend 和模型组合未闭合（IR-03/05） |
| Q | `f921605e` | signed zero 保真、prefetch/credit 收紧有实现，相关单测通过 |
| M | `e50d75d8` | 共享 journal 与预算 helper 有实现；声明式 prompt 由本轮补修，Embedding 真执行仍失败（IR-03） |
| L-contract | `09ec0bf1` | v2 解释/读写/布局有实现；多输入真实 DAG 编译失败（IR-04），消费者缺失（IR-01） |
| L-chain | `9122aaee` | code-pin source 依赖、write seal、清理路径有交付；不能替代 A 的公开入口接线 |
| L-workers | `40c8b5c0` | 异步有限 ACK 等待、超时失效、graph credit 迁移有交付；Ray 真实组合仍待验 |
| L-native | `14a90e15` | profile/stage 数值约束下传有交付；真实常驻/IPC 字节覆盖不完整（IR-07） |
| W | `362c482b` | sealed/declared 标注、occurrence 选择及来源字段有改动；完整产品 V 门仍待交付 |
| V | `bbd93ffc` + 在途文件 | 本轮未复制或改动；4 tested / 15 claimed / 2 needs_decision，不能当整体验收已完成 |

完整 HEAD、原始 dirty、文件归属和装配摘要见 [输入快照](evidence/input-snapshot.json)、[装配清单](evidence/assembled-files.json)。本轮结束前复核 11 路提交均未变化。

## 3. 已处理的核心裁决

### IL009：保留声明式 request_policy；继续拒绝不受封存约束的 gate/资源

修改 [Dataset.map_prompt_async](../../../demiflow/data/dataset.py) 与 [prompt facts/managed contract](../../../demiflow/operator_llm/flow_contract.py)。

- 在实际 PromptActor 上记录不可变的规范 policy 声明，以及本次构造的 owned gate 引用；不新增 OperatorLLMMapOp dataclass 字段，避免无 policy 的历史身份被额外 None 字段改变。
- `normalize_prompt` 接纳并规范化声明式 policy；静态分析与实际 facts 都写入同一 policy。
- `prompt_facts` 要求标准 AdaptiveRequestGate、同一 owned 引用、参数与声明一致，并拒绝未声明的 state_path、failure/pacing 配置。
- 仍拒绝显式共享 gate、替换后的 gate、environment/service/client/owner 等未封存资源。运行时的 adaptive 观测和容量变化不进入计划身份。
- 无 policy 的事实与修复前逐字结构相等，见 [前后对照](evidence/il009-before-after.json)。

**21 passed**：新增 14 项事实/防篡改/真实 SSE 测试、原 prompt 声明式并发测试、既有 6 项恢复/unknown/损坏块/预算测试。未知请求不自动重发的既有路径保持。这里没有把普通 prompt 的恢复测试冒充“policy 场景所有崩溃窗口全部验证”；该组合仍应进入 V009 的最终验收。

交接材料有两处需要纠正：示例 import 应为 `operator_llm.parser.parse_prompt_pack`；两个 SSE 旧测试实际上仍传显式 `request_gate`，不能要求它们原样通过。新增的 policy SSE 测试保留真实并发、连接复用、fresh 次数和 usage 断言；后续应迁移旧测试，不能撤掉守卫。

### IL012：测试实际导入本次集成树

新增 [pinned_pytest.py](pinned_pytest.py)：显式设置目标根目录、PYTHONPATH，在同一测试进程断言 `demiflow.__file__`；保存完整命令、解释器、HEAD/dirty、全部 Python 源码摘要和实际导入模块路径。子进程继承路径，pytest 临时目录、缓存及本轮工作台截图目录定向 `/tmp/demiflow-*`，并拒绝选择在运主仓作为测试根目录。

另修 6 个 F/S 测试文件的硬编码工作树路径，改用测试文件自身所属 checkout；**32 项来源/VectorSearch 断言定向复验全部通过**。未改共享 editable 安装，也没有在 12 个树里植入 conftest。

验证入口初稿缺少 `if __name__ == '__main__'`，第一次历史集合的 spawn 子进程重入了 pytest。该轮 `historical117` **全部作废，不作为项目证据**；已补 guard、通过真实 spawn 进程测试，并以 `historical-final` 重跑。源码/函数单测等日志只有一个入口 banner，未受该问题影响。报告保留这段纠正，不把验证工具自身故障归咎项目。

## 4. 待确认后统一修复的 7 项核心问题

### IR-01 · P0：公开终端没有消费新封存执行布局

位置：[local.py:562](../../../demiflow/execution/executors/local.py#L562)、`:767`、`:1217`；[A016 任务](../tasks/A016.md)、A007/A008/A017–A025、C08。

`iter_rows` 等仅调用准入后继续旧 `_apply_plan`；`write_lance` 丢弃 ActionDecision，调用 `seal_write_topology` 时不传 decision；L 的 `execute_native_plan` 在生产代码中没有实际调用者。更直接的是 `_apply_plan(execution != None)` 一律抛 `unsupported_layout`，而 L045 的 Python 子阶段明确向它传 sealed execution。

实测：真实 Lance copy 写成功，但同一 action 的 `action-1-sealed-plan` 为 None。不是“缺一个展示字段”，是布局尚未控制实际执行。函数单测允许 unbound 路径，并不能完成任务书要求的绑定调用。

修复范围：A 的各公开终端、`_apply_plan` 与 L-contract 调用缝；使用一次决定、compile/load 原封存 execution、明确传给消费者；不得再创建 action 或另做 backend 选择。

验收：take/iter/count/materialize/write 各路线从公开 API 执行；观测真实 `execute_native_plan` 被调用；改变封存 backend/profile/binding 应在读源/写 sink 前拒绝或改变真实执行约束；不能仅断言结果行正确。

### IR-02 · P0：新原生事实丢失业务配置，恢复身份发生碰撞

位置：[plan_facts.py:284](../../../demiflow/execution/plan_facts.py#L284)、[recovery_identity.py:166](../../../demiflow/execution/recovery_identity.py#L166)、`chain.plan_code_pin`；F002、C04/C07/C08。

`EmbeddingMapOp` 新 native facts 只记录 model_contract/inputs/output；`plan_identity` 与 code-pin 一旦拿到 native 就不再描述完整 operation，而附加 policy 又未含 max_batch/flush_interval。

对真实公共 API 构造的两个 pipeline 实测：

| 变化 | action identity 相同 | code pin 相同 | 完整 operation facts 相同 |
|---|---|---|---|
| batch_size 2→3 | 是 | 是 | 否 |
| 新增 call_output | 是 | 是 | 是 |
| 新增 error_output | 是 | 是 | 是 |
| 改 max_requests | 是 | 是 | 是 |

另将实际 actor.output 改为另一列，facts 仍不变。存在“声明和实际代码不一致却接受”的具体反例，见 [身份探针](evidence/native-identity-gaps.json)。不能靠 platform_revision 兜底：同一平台版本内改变这些业务声明即可复现。

修复范围：F 原生事实与实际 actor 独立核对、M 原生声明、共享 semantic/code-pin 投影。补齐影响输出、批边界、效果/预算语义的字段，并保护旧已接受格式；不得为解决碰撞重写历史记录。应检查本轮其他新 specialization 是否同类遗漏。

验收：上述变化均区分身份；actor/config tamper 在封存/恢复前拒绝；真实 resume 不得复用另一批边界或另一输出配置的历史结果。

### IR-03 · P0：Embedding 默认启动失败；收紧响应上限后仍在 checkpoint 失败

位置：[embeddings/stream_recovery.py:28](../../../demiflow/embeddings/stream_recovery.py#L28)、`:33`、[stream_materialize.py:236](../../../demiflow/execution/stream_materialize.py#L236)、[stream.py:632](../../../demiflow/execution/stream.py#L632)、`stream_barrier.py:78`。

两个独立阻断：

1. 默认 `max_response_bytes=64 MiB`，borrowed run journal 的 record ceiling 至多 16 MiB；`bind_actor` 加 64 KiB 后比较，默认模型 pipeline 启动即拒绝。
2. 将响应上限显式收紧为 1 MiB 后，真实两 worker、batch_size=2、checkpoint_rows=2 的 materialize 仍抛 `Native row-local stage arrives without a worker slot`。Embedding 是 batch stage，但 M 宣称 row_local=True，R 因此发一个 token；batch_worker 则按 slot 参与。输入余量和原生请求效果是两层状态，当前被同一个标志混在一起。

这是 **M/R 组合及我原契约的遗漏**：C07 明写 row_local=True，C05 又区分 buffer/native row-local，却没有把“batch 模型也有 native effect recovery”的组合规则定清楚；不能只归咎执行 agent，也不能只把布尔值改掉就算完成。

修复范围：统一模型响应默认值与 journal 的准入策略（可明确拒绝超限显式配置，不能静默截断）；让模型 effect adapter 与 stage buffer/cut participants 分别表达，通过同一提交闭合。

验收：默认及合理显式配置真实成功；多 worker 的每个 pending batch/ordinal 都封存；进程崩溃后已完成请求不重计费，unknown 不重发，批边界与输出不重复/丢失。

### IR-04 · P1：多输入 Dataset DAG 无法编成合法 native layout

位置：[native_plan.py:161](../../../demiflow/execution/native_plan.py#L161)、`:184` 与 `plan_interpreter.py:745`；F004→L006、C08。

L 使用 `nodes[1:-1]` 作为所有 operation，沿用了线性图假设；F 递归展开 join/union 后，这个切片包含子 source，且根 composite source 本身有输入边。对应 source binding 也只取第一个源。

实测真实编译函数、真实 sealer/interpreter：线性输入成功；local execution 中的 union、join 均失败，错误为 `driver execution must own operation stages only`。没有 mock compiler 或校验器，见 [完整探针](evidence/probe_native_compile_local.py) 和 [结果](evidence/native-compile-local.json)。

修复范围：按 DAG 节点角色/端口与 program bindings 识别源、合成节点、operation、terminal；每个真实源一一绑定，不依赖列表切片。不得放宽 interpreter 的 ownership 检查来吞下错误图。

验收：join/union/reduce 的真实多输入图经过 compile→seal→load→execute；所有节点/源/子程序覆盖准确，嵌套业务代码变更仍拒绝恢复。

### IR-05 · P1：prepend 内部再调公开 iter_rows，集成后嵌套 action 失败

位置：[stream.py:393](../../../demiflow/execution/stream.py#L393)、[dataset.py:2147](../../../demiflow/data/dataset.py#L2147)；A012 与 R012/R015。

A 给 `Dataset.iter_rows` 补了 action；R 的 prepend 仍在外层 materialize action 内调用 prefix dataset 的公开 `iter_rows()`。结果是 `Nested Dataset actions need an explicit completed data boundary`。

实测最小公开 API `from_items([1]).prepend(from_items([0])).materialize()` 失败；R 自带的两条 prepend 恢复测试在装配树也失败。纯 grouped 的公开 materialize 在同轮探针通过，不能把全部 buffer 一概判坏。

修复范围：prefix 作为父 action 的已绑定子输入执行，复用同一封存/身份/资源上下文；不撤掉 nested-action 守卫、不再调公开终端、不让内部读取重新建账。

验收：空源/非空源、前缀输出中及提交后崩溃、恢复 skip 位置、max_rows/max_row_bytes 边界均通过；只有父 action/原提交权威。

### IR-06 · P1：普通实例 auto_json 的分类与编译准入矛盾

位置：[traits.py:85](../../../demiflow/planning/traits.py#L85)、[operator_precheck.py:20](../../../demiflow/execution/operator_precheck.py#L20)；S001/S004/S011、C04。

C04 明确“factory 或 auto_json 未声明 ownership 时默认 shared_colocated”。当前 `classify_operator_state(Counter())` 返回 auto_json/shared_colocated/adapter_present=True，但 traits 只升级 hooks 实例，把普通 JSON 数据实例保持 none；precheck 随后拒绝它。

实测仅一个整数 count、concurrency=1 的普通 Counter 实例，`materialize` 失败 `STATE_ADAPTER_MISSING`，见 [探针](evidence/auto-state-gap.json)。S011 单测反而锁定 `PlainData()` ownership=none，需要一起修正，不能把单测绿色当契约已经满足。

修复范围：在封存前由单一 policy 确定 ownership/adapter/concurrency；构造后按同一规则复核。继续拒绝资源型/无法无损保存的状态、高并发无适配器状态。

验收：类工厂和普通数据实例均有正向跨 checkpoint 累加恢复；不执行用户 property/custom getattribute 探测；初始业务配置保持身份区分。

### IR-07 · P1：Arrow 的字节限制未覆盖实际保留内存与 IPC 文件

位置：[arrow_batch_sink.py:28](../../../demiflow/execution/arrow_batch_sink.py#L28)、`:56`、`:131`；L011/LNU05/LNU06、有限资源契约。

- rechunk 用 slice.nbytes 计费，但切片仍引用整个父缓冲。实测逻辑 8 B、真实保留 8192 B 的结果通过 max_bytes=16 检查。
- execute 虽收紧 options.batch_rows，reader 却仍用业务 size 读取，单个父批分配未受这个收紧值控制。
- apply_batch 以 Arrow 数据 nbytes 判断“输出 staging bytes”；实测 max_bytes=8 能写出 288 B IPC 文件，未计格式开销。若 output_bytes 仅表示逻辑 payload，必须另外证明实际 scratch 的准入，不能把两者当同一预算。

这些不是缺一个最终 RSS 测试，而是可复现的计量口径遗漏，见 [字节探针](evidence/arrow-byte-probe.json) 与 integration-gaps 的 parent-buffer 反例。

修复范围：明确 reader 父缓冲、在途任务、消费中结果、序列化副本与临时文件生命周期；以同一 run/资源权威先准入再持有，合理计量 retained buffers 和实际文件字节，不引入第二套额度。

验收：小预算宽行、零拷贝切片、多 pending task、放大输出、磁盘不足和取消；失败前不发布，清理后额度归还。不能仅用 SimpleNamespace budget 和小表证明整个 grant 生命周期。

## 5. 验证结果与不能据此声称的内容

| 集合 | 独立结果 | 正确解释 |
|---|---|---|
| IL009 修复/既有恢复 | 21 passed | policy 事实、SSE、拒绝边界和既有恢复保护有证据 |
| 11 路新增函数测试首轮 | 881 passed / 35 failed / 6 skipped | 其中 32 个为写死树路径；2 个 prepend 真组合失败；1 个 F002 旧 api_key fixture 被 M 新守卫拒绝 |
| 32 个来源断言修正 | 32 passed | 维持来源保护并可在集成树执行；没有删除断言 |
| 验证入口真实 spawn | 1 passed | 子进程导入本树，不重新启动 pytest |
| 117 项历史集合（有效复跑） | 60 passed / 57 failed | 不是 57 个新 bug，也不是可以忽略的 57 个测试 |
| 独立 5 条组合反例 | 1 passed / 4 failed | grouped 成功；封存遗漏、prepend、Embedding 默认准入、Arrow retained buffer 各失败 |
| 收紧响应后的 Embedding | 1 failed | 穿过默认预算阻断后，暴露独立 barrier 协议冲突 |
| 真实 native compiler | 线性成功，join/union 拒绝 | 接口形状兼容不能代替真正多输入组合 |
| 工作台/原生 delivery 定向集合 | 58 passed / 3 skipped | API、图/身份/源码与前端模块检查通过；浏览器用例未执行 |

历史失败逐项索引见 [分类](evidence/historical-failure-classification.json)：32 条旧全局拒绝断言被新能力取代；2 条仍传显式 gate；其余含上述真缺陷、旧 checkpoint/save 迁移和资源/源身份边界不一致。旧测试若不再适用，必须给出保持原用户语义的新正反例；不能靠批量 deselect 获得“全绿”。非有限数值源、未适配的 NoCopy 资源、自定义无字节预算队列三类失败尚待契约口径核对，不在本轮随意放宽。

F002 的“凭据不得进 facts”意图仍需验证，但当前测试在公共 API 构造时即被新 HTTP_OPTIONS 守卫拒绝，还没有到达 facts 检查；应更新 fixture/增加正确边界断言。不能把这 1 项认定为凭据已经泄漏。

Ray：主 env 无 Ray，env-ray 存在，本轮没有启动真实 Ray 集群。5 个 Ray cleanup 场景未执行；L018 即使装 Ray 也显式 skip 完整图编排，因此该测试不能充当 Ray graph 验收。最终由 V019/真实集成门给证据。未执行真实浏览器、Notebook/SQL、Case 多媒体全链路，未验性能包络，不宣称产品整体通过。

## 6. 产品 15 叶对齐与后续统一修复顺序

| 原始意图叶 | 本轮证据/缺口 |
|---|---|
| R1.1 统一侧栏/四 tab/开发阅读 | W 保持；V016/V017 未完整交付 |
| R1.2 D1.9 demo/响应式 | 尚无本轮真实浏览器验收 |
| R1.3 共享上下文、配置与说明 | W selection/status 改动可核；不等于全 UI 已验 |
| R2.1 真实图、参数、输入输出和源码 | IR-01/02/04 直接阻断全路线一致性 |
| R2.2 配置检查到真实运行 | 准入与部分终端已改善；IR-01/03/05/06 仍阻断 |
| R2.3 run/attempt 计划、观测、停止恢复 | W 来源/选择有实现；恢复正确性受 IR-02/03/05 影响 |
| R3.1 数据目录/表/版本/统计 | 本轮未重验，不能从引擎单测推导通过 |
| R3.2 Notebook/Python/SQL 全生命周期 | V014 待交付 |
| R3.3 通用多媒体呈现 | V015 待交付 |
| R4.1 Case 分组筛选采样翻页 | V015 待交付 |
| R4.2 共享材料与多方案可配置比较 | V015 待交付 |
| R4.3 持久评测/草稿/历史/冲突 | V015 待交付 |
| R5.1 单一平台、职责和合理架构 | 单权威方向保持；IR-01/04/05 是跨层接线缺口 |
| R5.2 有界资源/失败取消清理恢复 | IR-03/07 未关闭；Ray/性能门未验 |
| R5.3 业务/历史/dirty/Notebook 保护 | 主仓和其他树保持；IR-02 是平台历史绑定风险 |

确认后建议作为一批统一修复，按依赖顺序收口，而不是再开一轮泛化设计互审：

1. **封存与真实执行接通**：IR-01 + IR-04；同时补 IR-02 身份完整性，避免错误身份先写入新历史。
2. **恢复组合闭合**：IR-03 + IR-05 + IR-06；改清必要的组合契约，再修对应实现，保留单 action/commit/effect authority。
3. **真实资源约束闭合**：IR-07；完成实际持有/释放与字节反例。
4. **同一候选上的验收**：V 的 21 个业务函数、历史迁移映射、真实 Ray、工作台/Notebook/Case；由 GPT 与 Qwen 审同一个提交，问题由 GPT 统一修改。

不会为了卡数补外围功能。上述 7 项以外，边界提示文字、命名和非关键文档修饰可后延。完成这批仍须以真实业务及产品验收结果定完成度，不能预先承诺 100%。
