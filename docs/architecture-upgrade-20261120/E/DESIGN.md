# E：P2 配置不变量门槛与窄拒绝设计 v1

本设计绑定 `main-20261119-3` / `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。任务树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt/compiler/config-invariants-20261120`，分支 `gpt/compiler/config-invariants-20261120`。作者 E；日期 2026-11-20；状态 **仅设计，待父协调/Fable 审阅；E2 不具备切换证据**。本提交不修改产品或测试实现。

建议批准 E1 的证据和窄校验准备工作。只有本文件第 7 节门槛实际全部满足且父协调裁定后，才由原作者做 E2；当前保留生产告警。设计通过不等于证据门槛通过，也不等于发版、预览换版或业务运行授权。

## 1. 原始意图、职责和已确定的边界

下附完整 root/15 叶原文，不以本任务的技术目标替换它。E 直接支持 R.2.2 的配置检查与清楚定位、R.5.1 的唯一契约来源、R.5.2 的副作用前拒绝、R.5.3 和 R.2.3 的历史保护。其余产品路径保持既有成果；不恢复已退出活跃入口的 Case UI，不要求重验未受影响的 Notebook、开发对话和前端。

计算平台长期目标是“把 DemiFlow 建成正确、稳定、高性能、通用且架构干净的统一数据计算平台，完整支撑用户的实际业务场景”。本任务沿用 A 资源、B 算子、C 交换、D 提交恢复、E 计划编排的所有权。这里任务编号 E 恰与引擎 E 同名，不产生新的执行 owner。

AR v1.3 的三块边界、C1–C6/R1–R4 全部保持：配置语义属于 SDK/编译合同；编译器检查自己新产生的图；运行、封存、发布、恢复和资源仍由原 owner 负责。工作台与 Forge 消费同一底座合同，不复制规则、不建运行账本，不新增统一校验服务。错误沿现有 DataflowError 向真实入口传播，不在 UI 另写一套判定。

P2 v4 §4.4/§5/§6 和 GPT 回复要求先补证据再收紧；2026-11-20 TASK-E 明确不能把未知键一刀切拒绝、历史不加严、没必需真实管道证据就保持告警。本设计保持已合入的 P1/P2/P4/P8 0–2，不重做 lowering/注册表，不搬 PlanInput/KeyedSource/UnionSource 等类型。P12 的索引 begin/publish 边界不受 E 改动；没有对 B/C/D 的代码依赖。公开导入修复及 GUARDS 登记由 A/父协调单写。

完整固定上下文根：`/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120/`。原件、快照、原话授权和摘要由该目录的 `context-manifest.json`、`USER-AUTHORIZATION.txt`、`CONTEXT.txt`、`REVIEW-HANDOFF.txt` 固定。本文的 `context-and-source.json` 记阅读范围与校验值；没有把历史暂停/待合入文字当作当前状态。

**原始 root 意图（R）**：之前的开发意图，是做一个统一、真正可用的 DemiFlow 工作台，让你看懂、配置和运行 pipeline，并灵活分析、比较和评测数据。

**完整意图分解（每轮保留结构）**：

- **R.1 统一、符合已确认 demo 的产品体验**
  - R.1.1：项目侧栏与开发、管道、数据、Readme四tab形成同一工作台，保留既有开发对话与阅读能力。
  - R.1.2：已确认D1.9 demo是原始意图的具体验收基线；真实前端按其结构、信息层级、常用交互和自然响应式实现，近期优先交付。
  - R.1.3：紧凑共享上下文、目录收起释放空间、少弹窗跳转；配置与操作可达，能力/错误说明清楚。
- **R.2 看懂、配置、检查并真实运行 pipeline**
  - R.2.1：以数据算子图理解AI写的实际代码；查看节点输入输出、算子参数、图配置和真实源码。
  - R.2.2：从配置到规范检查、问题定位与处理，再到真实提交；无效/局部图和未支持能力如实呈现。
  - R.2.3：按每个run/attempt查看封存图、真实节点观测、状态、日志、监控和结果，支持停止及受支持的恢复；当前编辑与历史分清。
- **R.3 灵活理解和分析数据**
  - R.3.1：查看项目数据目录、表、字段、版本及枚举/统计分布，理解数据内容和覆盖范围。
  - R.3.2：同一Notebook内真实Python/Jupyter与SQL；编辑、执行、取消、恢复、保存重开，并受控互用结果。
  - R.3.3：通用呈现表格、message/dialogue、text/md/code/json、图片、音频、视频、tag、html；支持范围、来源和降级真实明确。
- **R.4 可配置地比较和持久评测 Case**
  - R.4.1：第一部分：分组、筛选、选样和翻页，支持真实数据与服务端标注状态条件。
  - R.4.2：第二部分：共享输入/参考和多个输出方案，每个方案可含多字段材料；字段角色、预览、比较/画廊布局可配置。
  - R.4.3：第三部分：可配置评测/标注；整条Case、方案、字段范围正确分离，保存/下一条、草稿恢复、历史和冲突处理完整；配置变化无需反复找AI重写查看器。
- **R.5 支撑完整产品的工程质量与边界**
  - R.5.1：统一平台能力、清晰职责和合理架构，复用现有环境与公共接口；同批清理被替代代码，避免重复体系。
  - R.5.2：资源有界、性能稳定；真实集成覆盖正常、失败、取消、清理和恢复，不以局部测试计数替代产品验收。
  - R.5.3：保护项目、业务数据、dirty/staging、Notebook输出、开发对话和历史；来源、权限与持久化语义保持准确。

**贯穿全部工作的取舍原则（真人明确要求）**：核心是满足用户的原始意图，不为与意图无关的不足扩大工作。Review始终从原始意图出发，逐项判断对实际用户结果的影响：妨碍核心使用、关键架构边界或历史保护的问题必须处理；低影响瑕疵与无收益的完善可以后延，不作为交付前置。不得把局部技术指标、目录整齐或测试数量变成新的目标。

**意图原文保真规则（真人明确要求）**：以上root及15叶固定复用完整原文，不逐轮压缩词句、改写或以摘要替代，避免语义漂移。新增要求与执行进展分别记录，未经真人更正不修改原始意图。当前总控定时提醒和负责人阶段消息都必须携带完整意图及本原则；消息正文再单独说明本阶段目标、必要性、范围与完成条件。

## 2. 固定源码事实：改前

| 事实 | 122edf4 的实际位置 | 对设计的约束 |
|---|---|---|
| 生产只有最终图报告，且不拒绝 | `planning/dataflow_compile.py:capture_graph`，878 行；`planning/dataflow_configuration.py:report_configuration`，340 行 | 不能说各写入阶段已有生产报告 |
| 顺序是 capture_chain/terminal → graph decoration → codec/publication fit → shared bind → validate_graph → report → on_compiled | `capture_graph`，830–896 行 | 拒绝应在 program staging、source snapshot、seal、worker 之前；on_compiled 也必须晚于新图检查 |
| declaration/source/model/lowering/capture/budget 交织在节点捕获中；不是八张完整中间图 | `_Capture`、`compile_kernel_spec`、`dataflow_budget` | 不对尚无 occurrence/task 身份的半成品强套最终图约束 |
| schema 有 28 个 COMPILER_KEYS；operation dataclass fields 动态推导；source/model/terminal 白名单另有归属 | `dataflow_configuration.py` | 只对 compiler-owned 字段的现有条件做证明；声明同名字段优先，不移交用户业务参数校验 |
| 最终报告是 tuple[str]；检查异常被 warning 记录、返回 `()` | `report_configuration`，346–351 行 | 返回空不等于检查成功；证据须记录检查状态，不能只数 tuple |
| opt-in pytest fixture 只拦本进程 logger 的记录，未持久化覆盖；`graphs == 0` 不失败 | `tests/configuration_schema_gate.py` | 空覆盖不能算零报告；子进程、worker、Ray 未自动观察 |
| `configuration_findings` 对 unknown kind 提前返回；未知键与 invariant 都是字符串 | `_node_findings` | 拒绝不能按英文消息匹配；需要稳定分类 |
| output_carrier 用 producer→单个 consumer 字典，遇多边最后一条覆盖前面 | `configuration_findings`，331 行 | E1 必须明确所有真实边，而不是把当前实现直接当完备证明 |
| 恢复路径也重新编译 | `execution/dataflow_schema.py:validate_literal_schema_replay`，184 行；`execution/dataflow.py:prepare` completed_live_nodes 分支，563 行 | 不能全局把 report 改 raise，也不能新校验历史图 |
| policy 默认窗口最多编译 8 次，且 replay 会跳过该过程 | `planning/dataflow_policy.py:fit_default_stream_windows`，207 行；`dataflow_api.py:resolve_action_runtime`，256 行 | 一次入口≠一张检查图；默认窗口探测不等于待发布新图 |
| 真正新执行在 prepare 的无 existing seal 分支编译，然后 stage/snapshot/seal | `execution/dataflow.py`，443 行及后续 | 既有 seal 分支必须保持；不能声称在 RunAction 创建或上层 reserve 之前就拒绝 |
| 已封存图/worker 加载沿原载体读取 | `dataflow_seal`、`dataflow_services`、`dataflow_history` | 这些不是本次新 schema 接入点 |

编译调用者完整清单（本树 `rg lower_action|capture_graph|compile_kernel_spec`，不是历史方案行号）：

| 入口/调用者 | 当前用途 | E2 拟采用模式 |
|---|---|---|
| `lower_action` → `capture_graph` | 直接编译 API；唯一完整图构建实现 | 默认 `new`，完成各阶段检查 |
| `DataflowExecution.prepare` 无 existing seal 分支 | 产生将要封存的全新图 | `new`；不能从用户配置关闭 |
| `execution/plan_capture.py:_Capture.intercept` | 工作台/Forge/原生计划捕获，随后可能用于 preseal | `new`；回调在检查后 |
| `execution/dataflow_api.py:explain_dataset` | 新声明的能力解释 | `new`；无 snapshot/worker，错误沿原接口返回 |
| `planning/dataflow_policy.py:fit_default_stream_windows` | 最多 8 次临时预算探测，可用于已有 preseal 的同 attempt 入口准备 | 显式 `probe`，只报告；最终真正新编译仍严格 |
| `validate_literal_schema_replay` | 对封存 literal schema 做当前声明比较 | 显式 `history_revalidation`，只报告；SOURCE_CHANGED 原合同不变 |
| `prepare` completed live program 核验 | 验 final ACK 后的当前 callable 声明 | 显式 `history_revalidation`；原代码绑定判定保持 |
| `execution/capabilities.py` → `compile_kernel_spec` | 单算子流式预检，不是完整图 | 保持现有声明检查；不伪装最终图覆盖，不强制 occurrence/task/边约束 |

`capture_graph` 没有其他生产调用者；`lower_action` 的直接测试/语料调用保持默认 `new`。内部模式仅是调用目的，不写入 ActionRequest/KernelSpec/Graph，不接受 SDK `native_options`/环境变量/用户配置指定，不成为新执行权限。E1 不引入模式开关；E2 才一次性更新上述消费者和实现。

## 3. findings 分类与字段契约

拟在现有配置模块内让判定先产出内部记录：`code/category/stage/node_kind/kernel_kind/occurrence_id/key/message`。记录不进入封存、不是新的公共序列化类型。`configuration_findings(graph)` 继续为现有消费者呈现 tuple[str]；`report_configuration` 保持告警 API。编译准入只消费同一判定源的明确 `invariant`，不解析日志、不复制规则。

| 规则类别/拟稳定 code | 资格与条件 | 缺失、None、默认值 | 决策 |
|---|---|---|---|
| `unknown_kind` | schema 不认识该 kind | 不猜字段与语义 | 永久保持观测性 warning，本轮不拒绝 |
| `unknown_key` | key 不在本 node 当前已知准入集合 | 未知不等于错；不吞掉 key | warning，不因已知编译键出现在未登记位置就自动提升；独立 cross-invariant 仍可报告 |
| `occurrence_binding` | schema 已认识节点；configuration.occurrence_id == node.occurrence_id | capture 后必需；缺失或 None 都失败，不补默认值 | 确定 invariant 候选 |
| `program_binding` / `reducer_binding` | 字段存在时等于本 occurrence；writer 分别为 callable transform / reduce | 无 program 的节点合法；不新增“所有 transform 必须 program”要求；出现 None 则失败 | 确定 invariant 候选 |
| `terminal_kind` | terminal 的 configuration.kind == kernel.kind | terminal_capture 后必需 | 确定 invariant 候选 |
| `compiler_key_shape` | COMPILER_KEYS 中、该节点 admitted、且没有被 operation 同名声明字段拥有 | 本节字段表为准；不把所有可选键改必填，不强制新默认值 | 按键窄登记的 invariant 候选，E1 补齐类型反例 |
| `task_complete` | TASK_KINDS 且非 streaming_map，三个 task 键齐全 | 必需，不允许任意缺一个；True streaming_map 不带该三键 | 确定 invariant 候选 |
| `task_follower` | 四个 TASK_FOLLOWERS 只能不带 task 键或只带 task_group | 空合法；group 非空；不把 follower 当 origin | 确定 invariant 候选 |
| `task_outside_scope` | 已知其他 transform 不带 task 三键 | 存在即报告；不扩大 TASK_KINDS | 确定 invariant 候选 |
| `native_key_owner` | native_key 存在只随 acquire/search | finalize/producer 无 key 合法；不能补推业务输入 | 确定 invariant 候选 |
| `batch_default_conflict` | default_batch_size 不与 truthy declared batch_size 同时出现 | 未声明且无 executor 时缺失合法；沿用现有 truthiness，不顺便改 0/None 的 SDK 规则 | 确定 invariant 候选 |
| `source_reference` | lance_key_source 存在时指向本图 source | 缺失是否必需仍由 writer/现有声明决定；不加历史必填 | 确定 invariant 候选 |
| `shared_membership` | shared_actor mapping 的 members 包含本 occurrence | 无 shared 合法；不升级历史无 actor_binding_version 的图 | 确定 invariant 候选；runtime shared_binding 原校验保留 |
| `carrier_pair` | output_carrier 的真实出边有匹配 input_carrier；现 writer 仅 compact join→write_lance | 未启用 compact 时两键皆无合法；None 不可当合法 text | 暂不直接提升。E1 证明唯一消费边或按每条边校验，补 fanout/顺序反例，再定该项 |
| `check_error` | validator 内部异常/未完成 | 绝不算零 | E1 test/evidence 失败；生产继续 warning。E2 仅明确 invariant 能拒绝，不把 validator 异常当用户配置错误 |

28 个 compiler-owned 键的资格表如下，精确原始 `shape/meaning/stage` 与静态读取位置另见 `key-inventory.json`。本表只有“出现时”规则，没有写明必需的键一律允许缺失；出现 None 默认失败。typed operation 已拥有的同名声明字段不套 compiler shape。实际读者以源码为准，静态扫描不是动态覆盖证明。

| 键 | writer/阶段；主要消费者 | 类型与条件 | 既有缺失/历史处理 |
|---|---|---|---|
| occurrence_id | `_Capture._add_node` / capture；调度、program、worker | 非空 str，且等于 node | 新捕获必需；历史读取不接本校验 |
| program | `_add_node` / capture；`kernels/dataflow`、seal | 非空 str，出现时等于 node | 无 callable 的节点不要求 |
| reducer_program | `_capture_relational` / capture；`kernels/relational` | 非空 str，出现时等于 node | 旧 inline initial/reducer_kwargs 原读者保留 |
| lance_key_source | `_capture_relational` / capture；关系规划/读取 | 非空 str，所指本图节点为 source | 原非 exclude-key 节点缺失合法 |
| payload_columns | 同上；Lance key/关系路径 | list/tuple；本轮不增加元素契约 | 原 source 声明/reader 继续负责内容 |
| default_batch_size | `capture_chain`；`kernels/dataflow` | exact int ≥1，不能与 truthy batch_size 并存 | 无 executor 默认时不写 |
| task_batch_rows | `capture_chain`；task_scope | exact int ≥1（bool 拒绝），行数 | task 内必需，默认来自 CompileInputs，原 fallback 8192 |
| task_group | `capture_chain`；task_scope/交换/worker | 非空 str | task 内必需，四类 follower 可缺失；其他不带 |
| task_origin | `capture_chain`；task_scope | exact bool | task 内必需，不向 follower 补 False |
| shuffle_order_version | `compile_kernel_spec`；shuffle_order | 非空 str | 历史缺失沿原算法解释；本轮不把版本枚举加严 |
| streaming_map | `compile_kernel_spec`；streaming/operator/placement | 值只允许 True | 不带表示非 streaming；不要求写 False |
| callable_scope | lowering；shared/planning/worker | 非空 str | 本轮不新增 scope 枚举；原 owner 校验保持 |
| native_operation | `dataflow_native` lowering；acquisition/task/membership | 文本且属于 acquire/finalize/producer/search | 普通 callable 缺失合法；先检查类型再 membership |
| native_key | 同上；acquisition/search | 非空 str；只允许 acquire/search | 普通 callable 和其他 native 无 key |
| terminal_memory | terminal_budget、fit_codec_windows；sink/publication/terminal | 原两种结构：{schema,resident_bytes} 或 {payload_bytes,metadata_bytes[,carrier]}；schema/carrier 非空 str，resident/metadata exact int≥1，payload exact int | 不擅自把 payload≥0 当现有规则；先保留原 predicate 并列验证；缺失按既有 reader 处理 |
| publication_workspace | terminal_budget、fit_codec_windows；publication_budget | 恰 {schema,resident_bytes}；schema v1/v2；resident exact int≥1 bytes | 非 native publication 缺失合法；历史原版本读；复用 owner 常量对照，不新增协议 |
| input_carrier / output_carrier | terminal_budget；join/terminal/driver hydration | 非空 str；成对约束见上表 | 无 compact carrier 时都不需要 |
| reducer_memory_v1 | relational_profile；reducer_memory | Mapping；不在此复制完整 reducer validator | 旧 inline reducer 原额度语义原处校验 |
| graph_route | graph adapter；router kernel/placement | Mapping | 非 routed graph 缺失合法，不复制 routing schema |
| graph_reader | graph adapter；router/worker | 非空 str | 非分支首节点缺失合法 |
| graph_observe | graph adapter；graph result | True | 普通图不要求 |
| graph_private_tag | graph adapter；placement/worker | Mapping | 不在 schema 推断用户 map 是平台 tag |
| graph_literal_prefix | graph adapter；placement | list/tuple | 非 literal 同步前缀不要求 |
| graph_result | graph adapter；terminal/result/历史 | Mapping | 非 routed terminal 不要求；历史不加严 |
| actor_binding_version | bind_shared_actors；shared runtime | exact int≥1 | 旧无此标记图不改解释；本轮不新增只准 version=1 |
| actor_recovery_rows | bind_shared_actors；shared runtime | True | 没声明 recovery_rows 不写 |
| shared_actor | bind_shared_actors；shared_binding/runtime | 恰 owner/members/state_owner，members list/tuple，owner/state_owner 是成员，本 occurrence 是成员 | 缺失合法；更强的成员顺序、唯一性和至少2项仍由既有 owner 校验，不顺便扩本轮 schema |

source/model/dataclass 声明的字段只做当前准入观测，不新建字段值 schema。`declared_arrow_schema` 保持 `dataflow_schema` owner；`thread_attempt` 是封存之后写入，不是编译阶段新必需。历史键 initial/initial_program/reducer_kwargs，以及未由编译出口产生的 aggregates/cache/cache_namespace 不删除、不硬拒绝，不因此恢复弃用内核或放宽 capability。

## 4. E1 拟实现：让证据能区分“零”“未观察”“检查失败”

1. 在原 schema 模块内引入稳定 finding 分类；28 个 shape 对所有合法 JSON 类型必须是 total predicate。当前 `{}` 作为 native_operation 或 publication_workspace.schema 会触发 TypeError，report 又返回空：本次已实测，见 `check-error-counterexamples.json`。修复仅把这类明确类型错变成 shape finding，仍告警；不改图。
2. 保留 `configuration_findings`/`report_configuration` 的实际消费者。内部扫描以 nodes + `(producer_occurrence, consumer_occurrence)` 边视图为输入，不依赖无效 DataflowGraph DTO、不重算 dag_id、不复制 configuration 或序列化图。最终 public wrapper 同源使用它。
3. 在 `capture_graph` 的四个完整节点边界记录阶段：capture（含声明、模型、源和 terminal budget）；graph；budget（codec/publication fit）；shared/final（共享绑定后及既有 validate_graph 后）。细粒度 declaration/source/model 归于 capture 的 writer 覆盖，不能说它们各有一张独立完整图。单 `compile_kernel_spec` 预检保持独立范围。
4. 报告带结构化固定字段：阶段、进程 PID、action 的安全定位、node/edge 数、finding 分类计数、check_completed；现日志默认仍仅 warnings。成功 DEBUG 仍可让现插件观察。每阶段上限 20 条具体 finding，余项只计数；阶段是固定常数，最终报告不重复记录 shared 这一相同图；不输出配置值、模型正文、路径凭据。只追加有界元数据，不在生产落盘、不建观测数据库。
5. pytest 插件以结构化字段判断，保留少数确有反例的 `reports_on_purpose`；逐项说明，不能批量豁免。普通不编译的单测允许 0 图，但声明要作为编译证据的 case 必须有 expected阶段/产物或预期拒绝；少阶段、checker error、未记录 case、少进程完成回执都是缺口。
6. 新增测试/证据工具 `tests/configuration_schema_evidence.py`：显式安装进程内 observer、输出有限每进程 JSON 回执，不通过 import 自动改生产 logger。它复用实际判定，不重写 schema。父进程只汇总本轮声明的 PID/角色与已完成回执，不把自身 logger 当 worker 遥测；无完成记录是 unobserved/not-run，不能补成 0。
7. 优先给已有 compile corpus 增加稳定阶段观察与失败反例；不改函数身份夹具 `compile_contract_functions.py` 或结构 golden 来追绿。当前阶段探针在 shared-review 中只是临时测量工具，批准实施后持久的验证逻辑才落 tests。

阶段边界消息只观察已存在图，不产生源码/数据副作用。E1 仍不抛生产配置错误。任何 stage 检查自身异常应 warning + check_completed=false，测试明确失败，绝不返回“完整零报告”回执。现有 DataflowError、编译 bounds 拒绝、能力拒绝和错误顺序保持。

## 5. E2 条件设计：只有新编译产物拒绝

E2 不随 E1 自动落地，不以空 enforcement allowlist 的代码宣称完成。

门槛通过后，在同一 schema 判定上增加窄准入函数；明确 code/key 的已批准集合在代码内，不从日志文字推断、不以全部 category 默认通杀未来新规则。满足已批准 invariant 的 violation 抛 `DataflowError('INVALID_CONTRACT', ...)`，`retryable=False`。错误只含固定规则 code、固定 stage、node kind 和截断 occurrence（最多64字符）、有界 finding_count；消息≤1024字符，最多4个定位，不包含配置值、完整未知键或模型正文。用原错误词汇，不新增跨层错误类/协议。

`capture_graph`/`lower_action` 增加仅内部 keyword `configuration_mode`：默认 new；probe/history_revalidation 只报告。E2 同批改第2节全部消费者；history mode 不从图里某个“旧”字段推断，也不通过大范围异常捕获来豁免。未知模式当内部调用错误，以现有 INVALID_CONTRACT 在工作开始前拒绝。

- new：各成熟 stage 的 invariant 判断失败则在该 stage 返回前拒绝；其余 findings 继续 warning。最终 validate_graph 及全部成熟规则仍执行，然后才有 on_compiled / 返回编译结果。
- probe：默认窗口搜索只报告，保留现有预算调节；最终 new compile 不能借 probe 绕过。
- history_revalidation：两个显式调用点保持原 report-only 行为；新的输入 schema/代码比较继续按 SOURCE_CHANGED、CODE_MISMATCH 等旧合同拒绝。它不是“历史恢复免验证”。

对于 carrier_pair，E1 先枚举所有边而非字典最后一条；当前 compact writer 意图是同一 representation 的消费边全部匹配。若语料/真实运行存在合法多消费者且有不同协议，本项保持 warning 单列，不能扩大拒绝。其他新 code/key 也须逐条加入经审证据集合；当前缺口不是全部未知键通过的理由，也不是硬拒绝未知键的理由。

**副作用边界的精确定义：** 新图检查发生在 stage_dataflow_programs、snapshot_source、seal_dataflow、stage_runtime_bundle、worker/actor start、sink publication、外部模型请求之前，也在 on_compiled 回调之前。调用者可能已创建 RunAction、申请上层名额或进行无执行的 producer configure；本任务不声称整个应用入口零副作用，不移动 P12 begin/reserve、不新建回滚状态机。若运行时拒绝，原 owner 的失败和清理逻辑继续处理已存在的运行外壳，不伪造业务成功、不把历史记录擦掉。

不在 load_dataflow/load_sealed_dataflow、worker bundle 读取、KernelSpec/Graph DTO 构造器、read_history/result_reader 或恢复 journal 中插入新 schema validator。预封存 first-attempt 仍按原载体，不重新编译替代。

内部 checker error 继续是 warning + incomplete（生产不因未证明规则增加拒绝）。E1 必须消除所有已知合法 JSON 触发的检查异常，E2 发布验收中只要仍有 checker error 或注入反例无法到达相应规则就不得切换。本策略把新拒绝限于已证明的不变量；不是宣称 check_error 提供了 fail-closed 准入保证。

## 6. 资源、身份和历史契约

扫描以 V 节点、E 边、K 配置键为增长变量，O(V+E+K) 扫描时间，O(V+E) occurrence/消费者索引；流式生产报告只保留20条样例与计数，既有 `configuration_findings` 返回完整tuple则另有O(F)空间（F是输入规模内的finding数），不能把此API冒称固定内存；不复制模型配置和图字节。capture 原 max_nodes/max_edges/depth 和 KernelSpec 有界 Json 是输入边界，最终图仍由 validate_graph 校验。若实现使用 sorted unknown keys，其已有 K log K 成本要如实记账，不能声称严格线性；优先按既有遍历确定顺序并只保存有限诊断。多个阶段是固定常数次扫描；测试小 bounds、多 finding、空图失败、最大合法图与 checker 异常，不新增时间后台任务、网络、worker、FD/文件持有。

证据工具每 PID 最多保存分组计数与有界样例，完整 case 清单随固定 corpus 规模保存；单文件上限建议8MiB、最多64个声明进程、超限标 incomplete，不静默截断成通过。子进程结束/取消/超时没有 completion 就不能计零；工具不会为了收集证据接管/终止其他进程。

| 身份/历史项 | E1/E2 影响与承诺 |
|---|---|
| platform revision | 只 docs/tests 的本设计提交不改变；未来改包内 .py 会改变现有全树 hash。沿用现有拒绝跨版本恢复，绝不宣称 hash 不变 |
| 对象 module/qualname/source identity | 不搬类型或工厂；不改 serializable DTO、native_model_declaration 类方法。被改模块中的被身份描述函数 source_hash 仍可能变；双树 corpus 逐字段归因，不直接忽略 identity |
| 封存字节和路径 | 原 codec、图格式和键写入不变；validation metadata 不写入图。正常 corpus 图字节、declarations/report_bindings/labels 要对照；不会给旧图补键 |
| 历史只读 | 原载体和原源码读取合同保持；新 schema 不接读路径；历史 inline reducer 与缺 shuffle_version 保留 |
| 恢复拒绝 | 现 CODE_MISMATCH/SOURCE_CHANGED/INCOMPATIBLE_HISTORY 和原版本选择维持；两个重新编译比较显式 report-only；不承诺跨版本 resume |
| Forge snapshot | 平台 revision 与业务 Forge snapshot 不是同一身份；业务项目与历史任务不改，不重算/重写其认证；新版按现有绑定重新核验 |
| 回退 | 各阶段独立提交；未合入可保留候选停止E2。已发布回退经反向任务提交和两道闸；旧代码/原环境证据保留，不回写封存身份 |

## 7. 切换门槛、真实管道范围和缺口

门槛按证据，不按“过了一天”。历史 delivery 曾建议真实流水线零告警满一个发布周期；当前 TASK-E 采用明确固定范围的证据门槛。本文不以时间经过替代实跑，也不从用户休息推定例外。

E2 必须同时具备：

1. 固定源码 S、输入/配置摘要、Python/依赖、实际 import 来源、私有路径、命令和实际执行清单；完整 corpus 所有 case/refusal、所有相关写入阶段、编译结构基准均通过，无 unexplained findings/checker_error/缺回执。
2. 已明确真实管道范围内每个需编译进程都有完成记录，生产检查实际启用；阶段数与入口/新图计数可对账。只在父 pytest 插件观察不到的子进程不能当零。执行 worker 若按当前架构只加载 seal，则以固定调用链和实际进程角色记录标 `not_a_compiler`，不能标“0图零报告”。若 worker 能调用新编译路径，必须单独采集。
3. 正常完整图/声明/报告桥/标签的等价性；坏新图在上述副作用前拒绝；未知键 warning 不拒绝；历史读取、缺键、原版本选择、两个声明重验路径前后判定一致。新增失败按发版规则处理，已知失败不能算通过。
4. Fable/父协调确认本次可提升的 code/key 明确、证据无阻塞，再指令 E2；实现完成后固定候选复核。缺任何必需真实执行证据则 E2 未完成，交 gap 和补证计划。

**设计阶段的实际发现：**

| 范围 | 本次证据 | 能证明/不能证明 |
|---|---|---|
| 本树47项 schema/corpus/inventory/compile_inputs 测试 | 显式 `-p configuration_schema_gate`，47 passed，0 failed/error/skipped；12.98s | 驱动范围内当前行为；未覆盖真实业务进程 |
| 完整 corpus | 348 direct：293 compiled/55 refused；30 captured全编译；323个成功case | 结构与固定 baseline 相等；55拒绝逐项保留，不能算 schema 成功检查 |
| 各阶段只读探针 | capture/graph/budget/shared/final各354次、323 unique cases；0 findings/0 check errors | 中间阶段是本次 wrapper 观测；原生生产只 final 354条DEBUG；重复来自真实默认窗口编译，不是354个业务 |
| 负例探针 | native_operation={}、publication_workspace.schema={} 均 TypeError；report返回空但warning | 暴露 E1 必修证据歧义，不是 corpus 正例失败；原日志保存 |
| RefGen 七流程历史共享回执 | `refgen-formal-relocation-20261119/compile-seven-results.json`；7条compiled；REPORT列10项真实Dataset/Lance夹具通过 | 平台 e993cfd 且 UI 曾 dirty；未记录 schema 插件/阶段/子进程 complete，不能绑到122edf4零报告；替代外部模型，不是真模型执行 |
| 旧 P2 delivery | 323图和旧四组驱动观察零报告 | 旧提交/旧环境、driver scope；不转抄为本次成绩 |

建议真实管道清单从既有七流程开始：candidate_pool、instruction_variants、question_tiers、reference_comparison、reference_delivery、repair_pairs、supplement_candidates。七者不是任意全业务的替身，也不代表用户已授权新业务运行。本阶段只读取共享回执，没有导入业务代码、读取运行数据、运行这些业务或触发模型。

补证路径：先要求父协调/证据 owner 提供已有固定运行的代码/输入/环境/编译日志/进程角色回执；能充分绑定就只读复核。不能补成完整证据时，给出明确的私有复制 compile-only 或已有授权夹具重放方案，由父协调核对既有授权。编译检查不能算执行；凡需要真实模型/外部服务才能覆盖的路径，在无该证据时继续 warning，不启动付费请求。没有必要为了本任务把所有真实业务都跑一遍；必须说明所选范围为何涵盖本次拟拒绝规则，并由父协调裁定，未涵盖者单列。

当前 E2 的确定缺口：**无122edf4绑定的真实管道完整 schema 观测；无其子进程完成证明；无真实执行与全部拟提升不变量的覆盖映射；生产阶段结构化报告尚未实施；历史回归与副作用负例尚未运行。** 即使47测试和354次阶段检查均绿，也不能切换。

## 8. 文件、符号、消费者及清理清单

| 拟改文件/符号 | E1/E2 | 原因/消费者 |
|---|---|---|
| `planning/dataflow_configuration.py` | E1 分类、total predicates、阶段报告；E2 窄拒绝函数 | 唯一规则源；compiler和现有schema测试消费 |
| `planning/dataflow_compile.py:capture_graph/lower_action` | E1 阶段边界报告；E2 内部purpose | actual new compiler出口；不改lowering、writer或注册表 |
| `planning/dataflow_policy.py:fit_default_stream_windows` | 仅E2传probe | 保留预算探测，不把历史入口间接加严 |
| `execution/dataflow_schema.py:validate_literal_schema_replay` | 仅E2显式history_revalidation | 原SOURCE_CHANGED比较不变 |
| `execution/dataflow.py:prepare` completed live核验调用 | 仅E2显式history_revalidation | 新图分支默认new，既有seal分支不插validator |
| `tests/configuration_schema_gate.py` | E1 结构化记录、覆盖/失败状态 | 现opt-in fixture；保留精确反例豁免 |
| 新`tests/configuration_schema_evidence.py` | E1有限证据/阶段/进程汇总 | 仅明确测试入口使用，不生产自动import |
| `tests/test_dataflow_configuration.py` | E1分类/JSON类型反例；E2拒绝/unknown分离 | 不删现反例、不弱化key writer检查 |
| `tests/test_compile_contract_corpus.py` / 必要语料 | E1阶段覆盖；必要小增量case | 基准golden原样，新增case独立diff；不改函数身份夹具 |
| 新`tests/test_configuration_compile_boundary.py` | E2真实入口副作用哨兵/历史重验 | 只写有行为价值的合同测试 |
| 本目录design/coverage/contracts及回执 | 每阶段提交 | source-bound交付 |

保留：schema已知键/原owner验证、历史键、KernelSpec/Graph/codec、原生模型工厂、plan_facts、traits、三张registry键、P1端口、P4名单、P8已合内容、七冻结文件。保留report字符串API因为有真实测试消费者；结构化判定替换其内部判定后删除旧重复逻辑，不长期维护第二份规则表。没有产品目录/类型搬迁、旧模块删除或兼容转导入层。

**不得触碰** tests/merge_gate.py 的 GUARDS（建议清单交 A/父协调串行登记）、正式目录/integration/其他任务树、业务/Notebook/共享环境/服务、冻结七件。若事实发现需越范围，则先交父协调具体证据，不自行扩张。

## 9. 实施顺序和契约验收矩阵

- E1a：固定基线、现调用者与规则分类；修复 total-predicate 检查失败歧义；不改拒绝行为。提交后跑schema负例和有限JSON矩阵。
- E1b：阶段报告和opt-in证据工具；跑完整固定语料，核baseline结构/全图wire/声明/报告绑定/标签及55拒绝；对新增观察本身验证无新副作用。
- E1c：真实管道证据分层汇总。缺口保持明确；以E1候选固定SHA交父协调/Fable。门槛未过则到此交付，状态E1-ready/E2-not-ready；不偷偷预留默认拒绝开关。
- E2a（有条件）：加入已审的窄invariant集合和new/probe/history_revalidation消费者，错误有界；单独提交。
- E2b：下表受影响测试、源码/身份差分与最终review；若修改产品再跑受影响项，不把前轮结果套给后改SHA。交完整候选给父协调合流，自己不合流/发布。

| 验收 | 具体行为/反例 | 证据及未验状态 |
|---|---|---|
| 分类 | unknown key/kind保持warning；known shape、identity、task、native_key等逐项code；同名声明不误伤 | 现18种变异全保留，再补None/bool/list/mapping；新分类待实施 |
| total predicate | 28键在合法finite JSON各类型无异常；native_operation/publication反例成为shape；checker异常不计零 | 已证明两个基线缺陷；修复验收未跑 |
| corpus | 378条结果；323图各成熟阶段；capability拒绝不伪报成功 | 基线完成，候选待跑；完整命令在receipt |
| 阶段覆盖 | 正常每stage有completed；阶段失败有结束状态；缺stage或无图不是零 | 当前仅探针，产品/持久守卫待实现 |
| 默认值/报告桥 | six compile_inputs测试，map batch/partitions/task默认、None与缺失、execute/reuse和routed末端对应 | 基线6项在47内通过，候选重验 |
| 副作用前拒绝 | 注入新图identity/task/shape defect；stage_dataflow_programs/snapshot_source/seal/worker/sink/on_compiled哨兵均未调用；清理原运行外壳 | E2新合同用例待实现，不能用只调用validator替代 |
| 模型/原生 | prompt/embedding/agent/native variants/typed source/terminal，现测试真实编译；纯mock不外推provider/GPU | 基线corpus只编译；相关 `test_dataflow_native.py`、模型/流/共享actor定向执行待实施 |
| carrier多边 | 多条边顺序不影响结果；compact正常配对、孤立producer、错配consumer；逐条consumer条件 | 待实施/证明；不先提升该规则 |
| 历史旧键 | `test_runtime_v3_reducer_memory.py::test_legacy_unfinished_reduce_validates_original_grant_without_rewriting_seal`；无新键的old graph无需新报告 | 未跑，本阶段不冒称历史已验 |
| 原封存读取/恢复 | `test_runtime_v3_history_protection.py`、`test_runtime_v3_sealed_version_selection.py`、`test_runtime_v3_presealed_entry.py` | 未跑；候选前后比较，保留原失败/skip |
| 两条重新编译路径 | literal一致/不一致；completed live一致/代码变；用spy确保history mode，新规不改既有结论 | 新合同用例和受影响既有用例待跑 |
| 跨版本恢复 | 同版原路径可用；不同platform revision仍CODE_MISMATCH；旧载体字节不变 | 未跑；不在本轮改变身份承诺 |
| 子进程 | 私有child正例、child故意finding、child检查error、child无completion、driver零且child失败 | 待实现；主进程插件不能替代 |
| 冷导入/边界 | `test_runtime_v3_compile_imports.py`、package ratchet、相关model symbol测试 | 未跑；仅受影响集合，不跑四线 |
| 资源/诊断 | 固定小bounds、>20 findings、超长未知key、不输出配置值、取消/timeout回执incomplete | 待实施，不新增通用资源系统 |

当前47项结果来自 pytest 原XML collection/execution：47 collected/executed/passed；0 failed/error/skipped；0 timeout；本次选定47范围内0 not-run。其余上述验证未执行，不并入47通过数。没有执行合入闸、全仓四线、Ray、多机、真实模型、工作台浏览器或真实业务。旧发布四线 tested_commit 是3640116而 release_commit为122edf4，本次不把其成绩改绑。

## 10. 评审需要裁定和交接

请求父协调/Fable 审：是否批准 E1；阶段成熟边界与内部purpose是否足够保护历史；carrier规则的多边语义；窄E2集合与证据要求是否适当。建议接受所有核心缺口为E2阻塞，E1仍独立推进。真实业务范围和必需执行证据由父协调依据已有授权组织，不由本作者启动新业务。无需用户决定新身份策略或兼容承诺，因为本任务不改变它们。

本设计阶段交付 full base/design SHA、文档/源/证据 SHA-256、clean状态、原始日志、真实测试命令/环境和not-run清单；共享位置 `tasks/E/design-v1/`。父协调写review request/回应/状态，E作者不运行Fable、不更改CURRENT/INDEX/GUARDS。阶段完成返回并释放名额，后续修订与实现仍由原作者完成。
