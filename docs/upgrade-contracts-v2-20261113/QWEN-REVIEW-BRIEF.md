# 给 Qwen 的独立 Review 交接

这是一份审查请求，不是实现完成报告。请以实际源码、原始用户意图和可复查证据为准，审查 GPT 新写的函数级实现设计。不要因为任务数量多、字段齐、脚本绿色就默认设计已经正确。

## 1. 用户真正要求的结果

产品 root：统一、真正可用的工作台，让用户看懂、配置、检查和运行 pipeline，分析、比较、评测数据。

平台 root：正确、稳定、资源有界、高性能、通用、职责清楚的统一数据计算平台，完整支撑实际业务。

本次设计要求：

- 细化到一个 agent 可以承担一个函数/方法；每项交付是可检查的代码。
- 把签名、参数、对象/类、输入输出、持久化格式、异常、副作用、资源和生命周期写清。
- 各 agent 对契约独立编码；文件装配和全局运行验证可以有依赖，编码不能等待别人临时决定接口。
- 从公开入口到真实执行、恢复、工作台和数据使用，必须能够整体连接起来。
- 不为边角料扩大工作；优先核心意图、历史保护、恢复正确性和关键架构边界。
- 用户还要求反复完整 Review 10 遍。GPT **没有完成这个要求**：实际做的是10个不同维度的分项复核，另有10项机械设计检查，不能算144项从头到尾完整重审10遍。请单独记录你实际做过的完整审查次数，不沿用“已经完整审过10遍”的假设。

## 2. 评审对象及边界

设计树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_contracts_20261113`

设计目录：`docs/upgrade-contracts-v2-20261113/`

设计分支：`docs/contracts-v2-20261113`

主体设计提交：`9ac53fd`。本交接说明及Review口径纠正是其后续文档补充；实际审查请记录所读HEAD。

共同代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

设计树没有实现这些新增能力。此前B/C/E/N1/N2/N3仍有自己的在途候选；先只读核对它们的新HEAD/dirty，列出相对本设计基线的漂移。不要把旧候选状态当作现在仍未变化，更不要直接整包覆盖或回退。

主仓 `/yzp/zhaozy/yangzepeng/0905/demiflow` 有在运业务，禁止修改或跑测试。不push、不合主仓、不重启服务，不使用付费模型；测试产物限 `/tmp/demiflow-*`。全局架构和完整记忆文档保持独立。

本单授权内容是Review与报告；不要求重新派发、暂停其他agent、接管生产执行或整体重写设计。设计意见和具体修订建议应能独立复核。

## 3. 先读哪些文件

| 顺序 | 文件 | 用途 |
|---|---|---|
| 1 | README.md、USER-EXPERIENCE.md | 原始两大root、完成后用户流程和公开API行为 |
| 2 | CONTRACTS.md、interfaces.pyi | 共享语义、数据shape、类字段与新增接口 |
| 3 | FUNCTION-TASKS.md、plan.json、tasks/*.md | 144个叶任务、签名、输入输出与调用关系 |
| 4 | FILE-OWNERSHIP.md、class-state.json、baseline.json | 文件唯一装配、原类字段、源码基线与hash |
| 5 | INTEGRATION-ACCEPTANCE.md、root-map.json、gates.json、TEST-COMMANDS.md | 全局链路、root映射、实际验收要求 |
| 6 | REVIEW-10-PASSES.md、design-validation.json、tools/check_design.py | 已经做过什么、校验器实际能证明什么 |
| 7 | fixtures/、DISPATCH.md | 固定接口样例与拟派工方式 |

校验器在本隔离树可以运行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" /yzp/zhaozy/yangzepeng/0905/env/bin/python docs/upgrade-contracts-v2-20261113/tools/check_design.py
```

这个命令仅作设计结构和基线兼容验证，不替代代码运行验收。不要无必要重建样例或刷新baseline，从而掩盖审查中的设计漂移。

## 4. 当前拆分是什么

| 包 | 任务数 | 主责任 | 重点交接 |
|---|---:|---|---|
| F | 5 | 原生算子事实、真实Dataset图编译 | 输出事实与ProgramSlice给A/Q/S/L |
| A | 8 | 公开入口准入及terminal路由 | ActionDecision给真实driver |
| S | 16 | 状态策略、身份、构造后检查、登记/恢复、VectorSearch | 一个策略贯穿预检/traits/OperatorSpec/runtime |
| R | 16 | buffer/barrier、分组/前缀/批次与恢复 | 与S状态、M效果一起进入同一cut |
| Q | 6 | 资源预算、有界队列、行编码与预留 | 真实字节/类型不变，原allocator/预留权威保留 |
| M | 12 | Prompt/Embedding原生效果恢复 | 借用run journal，实际Actor请求路径消费 |
| L | 39 | 封存、解释、driver/worker/原生后端、ACK等待 | 真正让封存布局支配执行 |
| W | 21 | 工作台事实、历史、控制、图映射及前端 | 真实计划、精确occurrence、准确运行状态 |
| V | 21 | 真实业务和产品验收函数 | 正常/失败/取消/恢复/资源/历史完整链路 |

合计144项、45个文件。111项标implement，其中含21个V验收函数；33项标verify_preserve，先核验已有实现，正确则保留。不是144个函数都需要重写，也不是要创建144个会话。

每个文件只有一个装配owner，但叶函数可以在独立worktree并行编码。非函数声明、imports、slots由文件owner装配。请重点判断这是否留下未派发的实际代码工作，是否会让owner成为新的串行实现瓶颈。

## 5. 哪些是已有证据，哪些不是

已有证据：

- 设计校验器核对39个已有源文件与代码基线相同，检查任务ID/函数/文件owner唯一、Python签名可解析及参数描述、文档链接和任务调用图。
- 3份v1样例（chain/queue/fanout）通过**真实现有解释器**。样例中的程序载体/manifest包含测试域引用，不是实际业务执行。
- 新driver样例的DAG/physical plan通过现有编译/校验；证明现有物理结构可以表达无worker组的driver布局。
- 两大root映射到20项意图条目及27个验收门；测试文件路径经过存在性检查。

没有得到的证据：

- execution_bindings.v2的新解释器正确、driver各路线都真实消费布局。
- auto_json、通用buffer和Embedding新恢复适配实际正确。
- 新V验收函数通过；这些函数尚未实现。
- 浏览器/Ray/真实业务/性能全局验收通过。
- 每张任务卡的语义都充分、完全无歧义；所有真实调用方都已找到并正确接线。
- 因为写了root映射就证明root已经满足；因为有20个条目就能计算真实完成度100%。

特别说明：check_design里的caller/callee主要来自人工填写的任务元数据，并不是对全部生产代码的完整调用图分析。它能发现一些结构错误，不能证明真实执行可达、await传播正确或生命周期无冲突。检查记录数量也不证明完整Review次数。

## 6. 需要你明确裁决的主要设计选择

这些是GPT提出的新方案，**不是已由实现证明或由此前讨论逐项确认的定论**。请指出保留、修改、缩小或删除，给最小可实施替代。

### A. ActionDecision是否真的由一处决定路线

对应A001–A008、C03。

新函数admit_action_plan只检查声明，不读源、不构造Actor。通过sink_spec和available_backends避免在不知道写入schema/实际kernel能力时选择Arrow。

核对：take/iter/count/materialize/write是否全部走正确入口；是否还存在另一份准入白名单或执行期自由eligible重选；公开count是否依然会触发嵌套action；take(0)是否先检查再零副作用返回。

必须保持合法`.map(execution='thread').take(1)`。B旧候选前移guard造成的退步已经有独立复现，不能因为B自测没含该文件就接受。

### B. auto_json是否必要、足够安全，是否引入新能力回退

对应S001–S016、C04。

方案将function/factory/hooks/auto_json/unsupported分开，类构造前延后检查，实例首行前必须检查。普通JSON数据字段可由平台适配；资源型Actor用明确hooks/identity。初版auto_json要求同owner并发1，未知factory采取保守状态所有权。

请重点审：

1. 这是解决真实用户意图所需的最小方案，还是超出当前缺口的扩建？是否应先完成原生Actor适配及明确拒绝？
2. factory编译时未知实例字段，如何让封存ownership/adapter_present与构造后策略一致？不能在运行期悄悄改拓扑。
3. auto_json读写字段、继承、自定义属性访问、别名、半套hook、状态预算和原子restore是否足够明确？
4. 并发限制是否误伤已有合法类；旧有效身份/恢复行为是否被悄悄改变？
5. 类Counter与直接实例、native VectorSearch和Embedding，是否都经过真实登记路径？
6. 不要由“直接无hook实例被预检拒绝”推断“类构造出的实例没有状态风险”。也不要整体回退E的类声明豁免。

### C. execution_bindings.v2是不是必要且最小

对应F004、L001–L014/L030–L033/L036–L039、C08/C12。

方案保留v1，增加v2 executions/driver placement，避免给原生driver伪造worker。真实关系/Arrow/Lance路线从同一封存布局选择backend和预算。

核对：有没有更小的兼容设计能满足相同结果；v2顶层/条目键集、版本分派、空计划、source/terminal与operation stage覆盖、driver/worker互斥是否完整；是否引入另一份状态/调度权威。

不要仅以“编译器能生成driver样例”认定执行路径成立。必须沿take/iter/write的真实分支逐一找消费者，并核对旧v1历史读取与恢复。

### D. 递归子计划、源码固定和身份映射能否真正闭合

对应F004、L030/L031及原stage_plan_artifact/verify_loaded_plan。

原verify_loaded_plan根据子程序局部index重算logical_node_id。方案用ProgramSlice/ProgramBinding分别保留局部算子身份和全图stage映射，每个PlanInput单独stage/verify。

核对：input_path能否完整表示KeyedSource/UnionSource及重复出现；共享和重复执行是否区分；漏段/多段/循环是否拒绝；source的describe摘要是否足以覆盖业务代码和固定数据；代码归档是否真的包含源factory及可达依赖；pickle或attempt变化是否重新污染历史身份。

最低反例：仅修改join右侧UDF，恢复必须拒绝；正常跨attempt恢复，dag和已封存program依据保持不变。

### E. buffer快照是否在真实切点形成一致状态

对应R001–R016、C05。

方案新增StageBufferAdapter，接StreamStageProgress以及真正的_make_worker循环，不只修改入口。快照包括pending rows/batches、ordinal、group引用、flush剩余时长和prepend位置。

请画出正常与崩溃窗口：输入接收→batch形成→调用开始→响应保存→输出发出→cut封存→commit ACK。每个窗口说明输入frontier、Actor状态、buffer、效果及输出归谁所有，恢复是否丢失/重复/越界。

重点审snapshot字段是否实际可取得，内存/字节是否计入原预算；跨group超时、并发batch、空上游prepend和取消如何处理。不能只写“排空”二字，也不能把batch/concurrency改1凑通过。

### F. Embedding借用Prompt journal是否协议兼容

对应M003–M012、C07。

源码里Embedding通过Actor._exchange/_fresh_exchange发HTTP，并没有PromptClient.record/execute/decode接口。方案改成bind_actor和scope_request，借用run-owned RunPromptJournal，保持Actor资源生命周期。

核对借用包装与lookup/reserve/response/failed/references的真实协议；max_requests/fresh/reuse计费；native_response_bytes；batch lineage/current_row_scope；取消drain和关闭顺序；未知请求是否拒绝自动重发。原Prompt的功能不能因复用而回退。

N1已获准的request_policy测试迁移与文档纠正作为在途候选接收，prompt_facts和私有journal拒绝守卫保留。

### G. 资源绑定是不是只停留在参数传递

对应Q001–Q006、L010/L011/L036–L039、C06/C12。

方案已把execution传到Engine/session和native_resources.dataset_session，不只在外层execute接参数。

请核对profile、stage_limits、run/envelope、DataFusionOptions、TaskScheduler之间**逐字段的换算和优先级**。哪些是上限、哪些是申请、哪些是阶段同时占用？组合stage怎样防双计和超分？disk/RSS/单行/IO batch和业务batch如何区分？如果仍只能写“取交集”而无法确定实际数值，应判契约未冻结充分。

特别审-0.0/+0.0、NaN位模式、bool/int/float、tuple/bytes、容器别名及跨进程重放。set/frozenset和不透明值的支持边界不能含糊；不要取消reservation changed守卫来掩盖非确定编码。

### H. ACK改async是否所有调用路径都正确await

对应L034/L035，及既有progress.seal、stream_barrier publication路径。

方案根据barrier已有inspect.isawaitable支持，将两种worker ACK回调改为async，有限等待，超时保留unknown。

请核所有生产/测试调用方、回调类型、Future线程归属、取消、迟到ACK、pending清理与资源释放。要证明event loop heartbeat继续运行，而不只是最终报超时。

**交接时已明确可见的文档问题**：共享契约C08写超时抛TimeoutError，但L034/L035任务卡异常栏仍只有PlanContractError/RecoveryContractError；应统一。runtime_state“只操作本对象”的泛化说明也不能准确描述发送贡献/等待driver等IPC副作用。请检查是否还有同类由生成模板引入的语义缺口。

### I. 工作台是否显示真实计划与精确节点

对应W001–W021、C09。

方案把来源等级declared/sealed与运行状态分开；保留旧logical node_map，新增occurrence_node_map。UI按occurrence定位，观测按action/attempt/report occurrence精确join。

核对renderLogical→mountLogical.update/select→chooseLogicalNode→源码/参数/观测的每个实际回调；同函数多次出现不能混状态，换run/attempt不能遗留选择。历史缺载体或观测缺页不能伪造事实。只撤一个标签不等于整个工作台闭环正确。

### J. 全局任务是否完整，又是否拆得过细

逐条检查真实主线：配置检查→Dataset API→准入→事实/图→封存/解释→driver/worker→状态/buffer/效果→提交→工作台→Notebook/Case。

请找出：未派发的类声明/构造器/注册表/调用方；只有生产者的helper；输入形状尚需临时裁决的函数；单函数无法独立测试的隐含依赖；共享文件owner的串行负担；V的长链测试是否过大或仅覆盖名字不覆盖行为。

20项root映射只是清单。请核查每个root有没有足够的真实验证或明确保留已有证据，尤其数据目录统计、所有媒体类型、Notebook取消重开、开发历史、Case状态筛选和冲突、性能、多机环境。不能只用“已有测试文件”证明产品意图满足。

## 7. 请按什么格式返回结论

先给明确结论：当前设计可直接分发、修订后分发、或关键方案应重做；说明依据，不以工作量或文档长度评价质量。

每个发现给出：

```text
编号/严重级别：
对应root与实际用户后果：
设计文件/任务ID：
实际源码文件/符号/所审commit：
最小反例或具体断开的调用链：
问题属于：源码事实错误 / 契约矛盾 / 漏任务 / 架构过度 / 验收缺口 / 在途漂移
建议的最小修正：
受影响的函数、字段、caller/callee：
应新增或保留的验证：
是否阻塞编码、装配或最终验收：
```

最后附四张简表：

1. 各包F/A/S/R/Q/M/L/W/V：可用、需改、未覆盖的内容。
2. 生产者→消费者：确实核过的链路与断点，不能只复制plan.json。
3. 必改/可后延：按原始意图影响分级，边角料不阻塞核心交付。
4. 证据账：读过的源码、实际执行的探针/命令、哪些只做静态分析、完整Review实际次数。

请明确回答：**如果不同agent现在按这份任务卡独立实现，最可能在哪些地方合不起来、语义不一致或实现完仍不满足用户意图？** 这比“文档写得很细、方向正确”更有用。
