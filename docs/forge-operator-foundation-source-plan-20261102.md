# Forge 通用算子源码级改造方案

日期：2026-11-02。状态：用户已批准，已按本方案实施；最终证据、实际支持范围及性能见 [实施验收](forge-operator-foundation-acceptance-20261102.md)。以下保留批准时的设计基线，具体内部名称以源码为准。正式验证、Review/Freeze 与生产交付仍不在本次范围。

## 1 模块依赖与最小增量

```text
Forge 源码分析 / 工作区 / 运行入口 / 页面
  ├─→ 原生契约协议及所属领域的纯语义函数
  ├─→ 原生后端能力判断
  └─→ CheckedExecution → 实际计划事实 → 现有执行器

原生领域模块 → 原生契约基础类型与工具
原生执行器 → 原生计划 / 能力 / 调用校验 / 观察 / 原生账本
原生层 ×→ Forge / 工作区格式 / 网页 / 业务项目
```

核心新增模块仅计划两处：`execution/plan_facts.py` 和 `execution/capabilities.py`，分别承载实际计划的无副作用描述和后端能力规则。它们没有实例生命周期、不保存第二份计划、不调度。领域语义较大时放在所属包的 `flow_contract.py`，例如 `operator_llm/flow_contract.py`；不为每个 API 创建 adapter 文件。

现有 `data/flow_contract.py`、`callable_contract.py`、`runtime_contract.py` 分别继续负责原生静态语义、绑定与实际值校验。不给业务增加统一基类，不新建 Operator、Node、Task、ResourceManager 或事件总线实体。

## 2 原生契约与规范化

### 2.1 `data/flow_contract.py`

升级为 `demiflow_api_flow_v2`，保留 `flow_api` 只附加声明、不包装执行函数的性质。声明区分纯数据字段和受信原生语义函数，目录只序列化数据与函数身份，不序列化或执行候选函数。

最小结构如下，字段名称在实现时保持封闭校验：

```text
contract_id / version / owner
role                         source | transform | bridge | action | value | query
dataset_ports                参数位置、单个/可变输入、输出角色
callable_ports               一个或多个 fn / actor / predicate 参数及其用途
normalize                    参数与默认值的原生规范化
resource_references          从字面配置列出需读取或绑定的资源
analyze                      字段、基数、顺序、效果、执行需求、可观察项
```

这些函数属于固定平台源码，接收有限的字面参数、输入字段、callable 描述和已读取资源内容；不接收 Forge 的 AST 对象、SourceTree、workspace 对象或数据库连接。分析结果是有大小限制的普通数据，不是另一份可执行程序。

统一内部调用形状为：`normalize(arguments)`、`resource_references(normalized_arguments)`、`analyze(normalized_arguments, *, inputs, callables, resources)`。前两步不读取文件；资源请求只包含已声明的路径/角色/读写需求，工作区核对并有界读取后再调用 analyze。每次最多 256 个资源引用，配置单文件 1 MiB、累计 16 MiB，并与既有源码/报告总预算共同准入。原生诊断返回字段路径和错误类别，由 Forge 补源码行号，不要求原生层认识 FL/OP 编号。

`contract_id` 识别可复用语义，不用公开方法名做分派。`kind` 保留作显示分类，但不再充当同时控制发现、字段推导、执行许可的万能枚举。当前 `KINDS` 的作用拆为协议合法性校验和已有语义实现；未知契约仍阻断，不能删除集合后全量放行。

规范化输出统一包含：实际有效参数、输出字段与证据来源、基数、顺序、资源/效果需求和执行模式。静态报告和实际计划核对共用相同基础规范化工具，但分别从源码事实和真实执行对象取得输入。

执行事实分开记录五个维度：后端（local 等）、驱动方式（pull/stream）、callable 形态（同步/awaitable）、执行位置（inline/thread/process）和实例作用域（stage/worker/action）。现有参数中同名 `execution` 的含义不改变；规范事实把这些维度分开，不能把“同步函数”“同步驱动”“线程 worker”混成一个标志。

### 2.2 签名与默认值

`forge/platform.py:_signature/_record` 继续发现真实签名。展示用 `repr` 字符串与机器绑定用默认值分开：可安全序列化的默认值用带类型的值保存；不透明默认值标记不可解析，不通过 `repr` 猜对象。

`callable_contract.bind_parameters` 保留“未提供”和显式 `None` 的区别；可变参数、可变 Dataset 端口按真实签名处理。不透明默认值若影响选中的调用，必须由原生规范化明确解释或要求显式参数，否则 incomplete。

将原生 API 与静态检查目前重复的通用参数验证下沉到现有原生绑定/语义函数，两边调用同一实现。例如字段绑定、重复目标列、并发正整数和 thread/async 冲突；不能只把现有 checker 条件搬到一个新注册表。

迁移时保留原生已有规范化行为，例如某些参数现有 int 转换与默认值规则；原始参数和规范参数分别可查。受管模式若需要更严格的输入限制，应明确写入策略并给出诊断，不能借抽取 helper 静默改变所有普通 Dataset 调用的接受范围。

### 2.3 字段与行数语义

`transfer_fields` 保留其既有规则，逐步按语义族组织纯函数，避免复制到 Forge。必须区分：

- 字段不存在、可缺失、值为 null、类型不支持四种情况。
- 全行替换 map 与保留原字段的字段绑定 map。
- 同步 map 一入一出、filter 零或一、flat_map 迭代展开、async row/None/list、batch 输入输出、join 多重性。
- 声明顺序、原生保证的顺序、未承诺顺序；不为统一显示给 join 自动排序。

`operator_check.annotation/compatible/check_value` 一起支持有限可判定的 union，使 `Row | list[Row] | None` 能表达原生 async 语义。类型不能使用 Any 逃避检查。容器深度、union 分支数、字段数有限；跨字段条件依赖不自动推理，必要时要求显式拆分或验证。

领域输出按实际路径推导。例如模型 `when` 未命中时原样返回，`error_output` 路径可能缺少成功字段；不能把成功响应 schema 当所有行的必需 schema。图片、结构化结果、错误字段和调用引用各自有原生类型。

### 2.4 所属领域模块

`operator_llm/parser.py:parse_prompt_pack/inspect_prompt_pack/resolve_prompt` 已有纯解析能力，直接复用。`operator_llm/flow_contract.py` 只增加参数、输入字段、条件输出、效果和实际 actor 描述，不建立新 Prompt 协议或客户端。

其他领域的规则同样归其原生包。配置值构造器只有在明确为纯声明、可静态描述时才允许出现在 Flow；检查阶段只保留构造事实，不调用构造器。业务 actor 构造仍受 OP-06 约束，构造期不能藏表读取、网络或服务启动。

解析函数只能使用传入的资源内容。文件读取由工作区受限解析器处理，不允许语义函数用参数路径自行访问任意文件、网络、环境变量或候选模块。新领域能力不通过在 Forge 内增加按模块名分支接入。

## 3 静态检查与工作区绑定

### 3.1 `forge/platform.py` 与 `platform_snapshot.py`

目录每项提供 API 角色、契约状态、契约实现身份与诊断。未知协议、缺必要声明或不受信实现保持 unknown/incomplete。查询是否会执行数据由契约决定，不能按方法名猜。

受信函数解析仅从已核对平台对象的原生声明获取，并验证来源属于同一平台快照；禁止接受用户 JSON 提供的任意 handler 导入路径。平台源码快照已覆盖私有实现，领域语义函数变更必须导致快照漂移。

后端/工作区准入不能写成永久目录属性。目录展示原生能力需求；某次检查再计算后端和策略结论，并绑定相应版本摘要。

### 3.2 `forge/flow_check.py:FlowChecker._native`

将当前长分支收敛为固定步骤：

1. 通过目录解析受信 API/契约，使用真实签名绑定参数。
2. 提取 Dataset 端口、普通参数、callable 事实和声明资源引用。
3. `OperatorChecker` 检查候选 fn/actor 的来源、构造、闭包、注解和协议。
4. 工作区按预算解析所需资源，原生分析函数产出字段/基数/效果/需求。
5. 调用原生后端能力检查，再检查工作区位置和运行策略。
6. 保存规范事实、节点来源与诊断；图仍从这份报告生成。

保留 Flow 的赋值、有限展开、静态分支、helper 和禁止隐藏接线规则。不新增模型方法名特判；删除流式“遇到这些 kind 就拒绝”集合及重复后端默认值判断，由真实组合规则给出细分诊断。静态确定的 API 不因此获得副作用许可。

动作与返回值依据 `role/result` 判断，不再用 `kind == write_table` 推导所有终结动作；桥接动作返回 Dataset，但仍有单独执行边界。

### 3.3 `forge/operator_check.py` 与 `source.py`

`describe/check` 输出序列化 callable 事实：fn 或 actor、同步/异步、构造参数、实际调用签名、返回类型、生命周期 hooks 及来源。运行准备不再重新解析 AST 建另一份生命周期判断。

允许在受限 Flow 中描述普通 actor 构造：绑定已检查的 `operators/` 类与字面构造参数，形成静态构造事实，不实例化；运行时仍执行普通 Python 构造。带生命周期 actor 的拥有者由原生执行路径识别，不能丢失到裸 `__call__` 后不再清理。

候选普通算子仍不能调用隐藏 Dataset / 模型 / 网络流程。扩展 async 类型不等于放松 OP-06 调用闭包。无法判定的属性、继承、动态导入和任意运行时对象保持拒绝。

### 3.4 `forge/workspace.py:resolve_arguments/_inspect_flow/_report_status`

增加有界资源绑定与状态核对。资源引用由原生契约从实际参数指出，工作区统一检查路径、根、所有权、符号链接、大小和摘要。配置放在现有 `flows/` 内，例如 `flows/prompts/`；不创建新的接线目录。

报告绑定增加声明文件摘要、契约协议版本、后端能力版本与本次准入事实。改 Prompt、schema、资源文件或相关环境绑定必须使报告失效；无关 Notebook 文件不参与。

运行时区分只读封存配置根、业务数据根、运行账本和 scratch。当前 `run_worker` 把 `resource_root` 设为 work，不能直接沿用来寻找未封存 Prompt。读取配置绑定到封存源，账本绑定本次持久运行目录，scratch 才可取消后清理。有效路径改写必须在准备页显示，并重新分析核对，禁止字符串替换或隐式环境回退。

敏感值不写报告/日志；只记录凭据引用和必要身份。影响允许范围的实际 endpoint 需在执行前解析、核对；凭据值不充当业务参数哈希。环境解析的变化不能绕过位置与网络范围准入。

## 4 后端能力与实际计划核对

### 4.1 `execution/capabilities.py`

提供无状态 `check_call` 与 `check_composition`，接收规范事实和后端/模式，返回有原因的支持结论。由原生同步、流式入口和 Forge 同时使用，覆盖组合而不只判断单节点。

将 `stream._materialize` 的无副作用组合校验、`plan.reject_streaming_map_options` 的通用模式约束整理到共同规则中；实际降低过程保留自己的类型分派。原生执行没有实现某个语义时不能仅靠增加能力声明通过测试。

`planning/traits.py` 中 `LogicalOp → transform` 的 fallback 不能移来当能力判断。资源规划和受管支持是不同问题。首版支持本地后端，不为尚未接通的 Ray 或外部资源返回乐观结论。

### 4.2 `execution/plan_facts.py`

从真实 `SourcePlan/LogicalPlan`、CallableSpec、原生 actor 和动作 spec 提取有限规范事实。遍历逻辑与 `KeyedSource/UnionSource` 归 execution 所有；`data/flow_contract.py` 不反向导入 `execution/local_kernel.py`。

必要的原生类型分派保留在这里，不能按类名后缀或 `getattr(..., None)` 猜未知操作。共享参数规范化，不共享“预期值副本”。字段明确白名单；不序列化任意对象 `__dict__`，不执行候选 property、repr 或实例提供的自证方法。

高级节点以模型为例：`PromptActor` 已保存 `_operation`、`_runtime`、when、call_output、error_output。由所属原生模块的事实读取函数描述这些实际对象并核对一致性，不增加另一份仅用于认证的 Prompt 配置。若 `_operation.config` 与 runtime 实际配置不同，必须拒绝。

优先复用现有原生对象中的语义来源；只有某种原生降低确实不可逆、源事实实际丢失时，才在该原生计划对象中补充最小不可变来源字段，并核对真实对象。不能先给所有 LogicalOp 加一个任意 metadata 字典，再把静态报告复制进去。

一个公开 API 降低为多个物理操作时保留对应集合；多个 API 被融合时保留组成节点。不能用“都变成 AsyncMapOp”作为语义相同的证据。union 展开必须保持输入出现次数，materialize 依靠同次实际 handle 绑定。

### 4.3 `execution/checked.py`

保留 `CheckedExecution` 作为可选原生检查上下文：

- `plan()` 使用实际事实提取与结构匹配，核对完整参数、数据端口、callable 身份、资源、效果及后端组合。
- `bind()` 继续核对封存源码，分别处理函数、类和受检实例；构造参数与实际拥有者必须一致。
- 匹配成功后建立本 action 的“报告节点 → 原生操作/融合位置”绑定，并下发实际值校验与观察标识。
- `action()` 只记录真实逻辑动作；执行器内部降低不增加用户动作、不允许嵌套受管动作。

观察绑定是一次执行的运行时元数据，不改变 Dataset 的业务语义，也不另建可执行图。不得按显示 label 识别节点；同节点的不同 action 消费分别记录。

### 4.4 `forge/runs.py` 与 `run_worker.py`

`_prepare()` 删除重复 supported 集合和 actor AST 重扫，改为消费完整报告、后端判定及工作区效果/预算准入。保留源码/平台封存、输出一一对应、固定输入、磁盘与记录准入。

准入可按通用规则限制：本地模式、固定读、create 写、声明目录、有限请求、有限 actor/并发/队列/批量。不能按 API 名称放行，也不能把输入声称的 effects 当运行时自证。

`execute()` 继续调用原 Flow、原生执行上下文和原 journal。统一截止时间与取消来源传给原生同步/异步入口；不新增任务队列、协程调度器、HTTP 客户端或请求重试逻辑。

本阶段维持第三阶段的平台源码与环境追溯边界，不把环境元数据升级宣称为可重建制品；严格交付环境仍属于已暂缓的方案。

## 5 调用校验与原生生命周期

### 5.1 `data/runtime_contract.py`、`data/plan.py`

复用 CallValidation 的输入和实际返回校验，扩展有限 union、批/行形状与预算。同步 `StandardCallable/BoundCallable` 保持绑定方式；async 在 await 完成后验证结果，不验证 coroutine 对象，也不重复调用函数。

flat_map 逐项验证并关闭迭代器，不先 list()；async 原生返回 list 时检查整个容器预算和每个元素，None 代表原生声明的零输出；batch 同时检查输入总行/字节、返回容器及逐行结构。调用参数、返回值和最终行字段检查的时点分别明确。

平台契约/资源校验放在业务 catch 之外，不能被 `catch=(Exception,)` 转成认缺。业务异常、原生模型 error_output、HTTP/schema 修复继续遵循原生声明；节点业务失败与整次运行硬失败不可混用。

### 5.2 `execution/stream.py` 与 `stream_map.py`

`_materialize()` 先完整校验，再构造 stage；带上 checked 映射和每个 callable 的检查数据。`_call_stage/_make_worker` 在真实调用和 await 边界校验，filter 的布尔返回与前/后过滤均保留原节点身份。

保留现有 stage/worker actor 范围、线程池、背压及并发语义。生命周期统一通过已有 `stream_map/stream_resources` 拥有者调用，不增加 Forge 资源管理器。actor 构造和 start 失败不进入行级 catch；部分初始化也必须尝试清理已取得的资源。

`Dataset.map_async` 当前按策略属性识别 actor；只有生命周期 hook 而无策略属性的实例不能因此漏登记。原生层明确识别普通函数、callable 类和实例，构造后在动作资源集合中登记实际拥有者，按对象身份去重。绑定方法保留其 `__self__` 所有权；stage/worker scope 仍按原生选项决定。`StreamMapRuntime` 已拥有的 worker 实例不再被外层重复 start/close。覆盖纯函数、无策略属性 actor、类注入、共享实例和部分构造失败五类验收。

流式原生 hard_timeout 当前可按策略计 miss；方案不得悄悄改成全局失败。图和报告需准确显示这一策略。工作区总 deadline、契约违例、硬预算和用户取消始终不能认作正常 miss。

### 5.3 `Dataset.materialize/run_stream` 与 `LocalDatasetExecutor.materialize_stream`

必须先修复原生接点再开放受管异步：

1. `materialize()` 的同步和异步分支都消费检查后的 source、plan 和必要 stage 拥有者。
2. `materialize_stream()` 使用现有 stream 驱动的内部消费接点，把受限 append 作为平台终端消费者；不再追加伪业务 map 后调用公开 `Dataset.run_stream()`。
3. 公开 `run_stream()` 有自己的动作检查；内部物化调用同一个私有驱动函数，复用相同 preflight、取消和资源关闭，但不重复开启逻辑动作。
4. 私有驱动先完成实际计划/资源核对，再准备 actor、读数据、发请求；验证失败零外部效果。
5. 物化完整排空并成功关闭资源后才发布 handle；失败只清理本次未提交缓存。

私有函数属于现有 stream 实现的必要复用接点，不提供新公共执行 API、不接受另一种业务 DAG。同步写表仍要求正确的物化边界；不会自动插入物化或改写 Flow。

### 5.4 取消与清理

工作区总 deadline → 原生停止投喂/停止新请求 → 排空或取消在途调用 → 关闭拥有的 actor/资源 → 监督进程必要时终止进程组。

线程调用不能被取消协程假装停止；等待运行中的线程后再关闭资源。若超过有限清理期限，被强制终止则记录未完成清理/在途结果未知，不能宣称所有 close 已执行。外部已发送请求也不能宣称已经撤回。

当前监督进程清理宽限与原生模型请求排空期限不同。准备阶段必须核对这组预算：支持可取消传输时按原生规则取消；需要排空的调用必须有有限截止时间并能落入允许的清理期限，否则该模式不准入。不得无限加长宽限以掩盖冲突。

## 6 资源核算与副作用证据

### 6.1 队列和在途载荷

在 `request_limits.ObservedQueue` 与现有 stream feed/worker/emit 接点增加可选字节预算，覆盖源预读、队列、正在调用的输入、已完成未消费的输出和批缓冲。保留原队列机制，无新消息总线。

准入至少按以下同时驻留对象核算：

```text
源读取及预读
+ Σ(队列容量 × 单项上限
    + worker 数 × 输入与已完成输出上限
    + 攒批缓冲与展开容器上限)
+ writer 缓冲 + journal 排队载荷 + 平台固定余量
```

队列释放与 worker 接管要覆盖交接窗口；不能 get 后立即忘记仍驻留的行。保守重复计量可以，少计引用父缓冲不可以。第三方内部临时分配未知时，不能把此公式叫进程 RSS 硬上限，保留原生池、地址空间/进程保护。

Arrow/NumPy 等不可按浅层 Python getsizeof 当真实内存；复用原生 buffer/编码边界，无法保证的形状明确限制。已完整分配的超大返回对象只能事后发现，须与分配前可控的队列/批准入区分说明。

`StreamStats.dead_batches` 当前最多保存 1,000 批但包含原始载荷。受管模式必须增加总字节预算或只保留有界诊断/固定引用，不能以批数有界宣称内存有界。历史默认行为变更需独立兼容测试。

### 6.2 预算作用域

| 预算 | 权威与作用域 |
| --- | --- |
| workspace/run | 原监督进程总时间、内存保护、磁盘与并发准入 |
| action/stage | 原生执行器的 worker、队列、批、展开和生命周期 |
| 请求 | 原生请求准入和持久账本；包括明确允许的重试/修复 |
| 数据提交 | 原生 writer 的行、批、磁盘、目标和回执 |
| 观察 | 有界聚合、写入频率、总条目与字节 |

先明确哪些预算每 action 重置、哪些跨 action 累计。当前 RequestGate.begin_action 会重置观察，不能直接当整次运行累计请求额度。首版可用持久节点额度和全部动作消费次数计算保守总上界；缺少可证明累计口径时拒绝声称全局硬限额。

共享资源按真实拥有者去重；同一上游多次 action 的请求上界不能只计算一次。取消/失败后不能重置请求账本冒充新额度。费用、token 仅在原生提供准确计量/准入时展示对应保证，未知不填零。

### 6.3 请求和提交

表写仍以原生 receipt/operation_id 为准；模型等外部效果仍以其原生请求记录为准。统一记录只保存受限引用及状态，未确认的请求/提交保持 uncertain，不自动重试。取消与失败保留已提交事实，不自动撤销外部效果。

持久请求账本不放在会被 reconcile 清理的 scratch。配置、账本、结果和临时文件有不同所有权及大小限额；`runs._tree_bytes`、准入、源查看与清理白名单须同时更新，不能新增目录后绕过既有配额。

## 7 节点观察与可视化

### 7.1 原生观察接点

继续使用 `observability.ActionObserver`、`data.stats.StageStats/ExecutionMetadata`、`stream.StreamStats`。增加可选的统一身份/观察接点，未启用受管上下文时保持原执行路径，不创建额外持久库。

逻辑身份为 `(run_id, action_id, node_id, occurrence)`；流式 stage 的显示名称、actor 对象地址、随机模型 node_id 不作为跨层身份。私有内部消费者也不成为 Flow 节点。

原生融合的 filter 保留自身计数接点；难以拆开的执行组耗时按组报告并列出成员。StageStats 扩展可选字段时保留已有字段和构造兼容。缺少 per-node 测量时保留 unknown，不按整次耗时平均分配。

### 7.2 `execution/run_journal.py`

现有 `action_events` 的唯一键为 `(action_id, kind)`，适合不可变动作事实，不适合持续更新进度。保留该表的语义，在同一 SQLite 中增加有界 `node_observations`：按上述身份保存单调序号及最近快照、终态和证据引用。

这是同一 RunJournal 的必要存储增量，不是第二套运行历史。进度可合并更新，终态不可被旧序号覆盖；进程崩溃前未落盘的采样显示 incomplete。动作/请求提交结论仍只来自其可靠账本，不由进度完成推断。

建议每节点每秒最多更新一次，每次文档至多 8 KiB、证据引用至多 16 条，合计至多 4,096 个执行实例和 8 MiB；全运行进度刷新最多每秒两次并批量合并。这些限额取交集：准备时按实际执行实例数为每个实例预留终态及最大观察文档空间，超过合计预算即拒绝，不能等运行结束才发现存不下。节点较多时只允许预先声明的较小观察档位，不能截断必需终态。最终快照绕过节流，但仍受体积上限。准入或写入失败不得假报完整观察。

指标只接受有限数值、固定类别和受控文本；完整行、请求正文和图片不进入观察表。schema/profile 的必需资源检查不得因进度写入失败被关闭。

### 7.3 UI / CLI / Python

`runs.show/list_runs` 和 `workspace_view` 输出版本化能力结果、动作事实、节点观察及证据引用。`web/app.js:renderReport/drawGraph` 使用这些字段；只对通用角色/状态进行渲染，不按 `map_prompt_async` 名称创建判断链。

证据查看通过其原生格式和受限读取器，不接受任意 journal_path。路径必须属于该 run 的已声明资源，固定版本表继续由现有结果入口解析；未知证据格式显示不可读取及原因。

调用详情的领域解释可由所属原生模块提供受控数据，前端使用通用字段表、文本/图片/JSON 组件。只读 GET 不执行算子、构造服务、重试请求或安装环境。Case 样本默认至多 20 行、1 MiB，图片沿用已有解码限制；查看后续页仍需有界读取。

模型证据复用 `operator_llm/call_ref.py` 的准确请求引用；新增查看路径前核对其单条读取上限、账本/分片清单大小和只读行为，不调用整本 `call_snapshot` 给页面做分页。逐行关联依赖原生 `call_output` 或明确保留的引用/业务键；未声明时显示缺少关联，不能自动给业务表加隐藏列。join/展开后的逐行血缘不能由节点级 DAG 推断。

## 8 版本与兼容

拟使用 `demiflow_api_flow_v2`、`demiflow_forge_platform_catalog_v3`、`demiflow_forge_flow_inspection_v2` 和 `demiflow_forge_run_manifest_v2`；包含目录的快照格式随实际封闭字段变化独立升版。工程标准内容版本从 2.1 更新，FL/OP/EX 不静默改变含义。新增统一能力规则以 UC 检查码标识。

旧报告/运行继续按原格式只读展示，保留原语义；新运行必须重新检查并生成新格式，不能把 v1 的 checked 自动转换成支持异步或外部效果。已有准备运行继续遵循原平台快照漂移规则，不悄悄切换检查器。

兼容解析是按格式读取历史，不是把旧执行器包装进新执行器。迁移按语义族完成后删除重复的新旧判定，不长期双写两份权威事实。

## 9 源码改动顺序与验收落点

| 顺序 | 文件 / 符号 | 改造与必须验证的内容 |
| --- | --- | --- |
| 1 | `data/flow_contract.py`、`callable_contract.py`、`forge/platform.py` | 协议、规范化、真实默认值；目录别名、未知契约、来源验证 |
| 2 | `forge/source.py`、`operator_check.py`、`flow_check.py`、`workspace.py` | AST/callable/资源事实；纯静态无执行、条件 schema、报告失效 |
| 3 | `execution/capabilities.py`、`plan_facts.py`、`checked.py` | 独立实际事实与统一后端约束；真实参数漂移、未知 op、多输入对应 |
| 4 | `data/dataset.py:materialize/run_stream`、`executors/local.py:materialize_stream` | 检查后计划贯通，内部驱动不重复 action；成功、空输入、取消、部分初始化 |
| 5 | `runtime_contract.py`、`plan.py`、`stream.py`、`stream_map.py`、`stream_resources.py` | await 后验证、flat/batch/actor 生命周期、fatal 与业务 catch 分离 |
| 6 | `request_limits.py`、stream feed/emit、原生领域账本 | 队列/在途/批/诊断字节预算，多 action 请求作用域与不确定结果 |
| 7 | 各原生能力所属模块及 Dataset API 声明 | 按族接入、共享语义测试；配置声明与实际对象一致，未知能力不猜测 |
| 8 | `forge/runs.py`、`run_worker.py`、`run_journal.py` | 准入、封存文件、取消桥接、持久观察及证据保留 |
| 9 | `workspace_view.py`、CLI、`web/app.js` 与导出 | 同门禁、同事实、通用角色渲染、只读行为、SSH 使用回归 |

步骤不是逐文件大规模重写：先迁移一个已支持同步语义，独立证明事实一致，再迁移其余类别。每段都保持可回归的原生 Dataset 路径；原生资源保护不足的类别先明确阻断，不靠 UI 隐藏。

测试复用 `test_forge_native_semantics.py`、`test_forge_flow_checks.py`、`test_forge_run_actor.py`、`test_forge_runs.py` 与故障/浏览器组，以及原生 stream、thread、grouping、prompt、Lance 用例。新增测试聚焦跨层对应和真实边界，不为每个新函数写同构自证测试。

## 10 增长方式与性能验收

契约分析按实际调用次数、节点、边和配置字节增长，全部计入现有准入；复用分析结果只能按完整输入摘要命中，缓存数和总字节受同一检查进程上限约束。

实际计划匹配每 action 构建一次有界索引，目标按 V+E 遍历；不能继续用反复截取完整 operations 前缀导致平方级复制。共享结构可以复用只读分析结果，但不得把两次实际 action 的执行、预算和事实合并。

节点指标在执行线程内更新有界计数，批量发送，禁止每行 SQLite commit、每行完整配置哈希或全量 journal 重读。业务数据不因可视化增加序列化副本。

验收比较同一数据与预算下普通原生执行、受管检查、开启节点观察三个模式：输出相同，同时记录吞吐、峰值 RSS、磁盘峰值/I/O、观察大小和取消耗时。若观察开销随历史总记录持续增长，或开启观察导致本可完成的有界数据链反复超预算，必须修订，不以功能测试通过代替。
