# 真实项目正式接入：源码级改造方案

日期：2026-11-03。状态：待 Review；符号/命令中标注“拟新增”的内容尚不存在。

依赖：[中粒度方案](forge-project-onboarding-design-20261103.md)、[复审与验收矩阵](forge-project-onboarding-review-20261103.md)。实现按 P0→P4，不一次放开所有权限。

## 1. 版本与迁移原则

拟新增 console registry v1、workspace v2；检查报告及受管 manifest 升级到下一版本，原生能力声明按 contract_id/version 演进。升级只发生在显式管理操作，不在 GET、轮询或读取旧记录时发生。

旧 workspace/report/run 保留原语义和绑定；新读端识别旧版本并按原保证展示。旧 checked 不升级成新能力的 checked。v1 深链接仅在原 root 已登记且匹配时转换，不提供任意路径路由。

运行实际调用原 Python Flow/原生 Dataset；报告中的符号值、依赖和图仅供核验/展示，不交给新调度器执行。

## 2. P0：DemiForge 规范和项目绑定

这些是 Demiurge 的显式标准化任务，与 demiflow 平台任务、业务迁移任务分别绑定范围。

| 文件 | 改造内容 | 边界/验证 |
| --- | --- | --- |
| `Demiurge/demiurge/demiforge/standards/pipeline-v1.0.0-candidate.7.md`（拟新增） | 定义唯一公开入口、平台契约接线、可选 Notebook、固定数据/效果/恢复证据 | 不写 identity、模型名或 demiflow 内部 API；不覆写旧标准 |
| `Demiurge/demiurge/demiforge/development/project.py` | 版本化登记支持项目相对入口及符号、业务说明/测试/算子位置；快照预算显式配置；完整流式哈希、前后 stat 核验、文件数/时间约束 | 旧格式按旧版本读；预算不减少覆盖；配置变更改变绑定 |
| `.../development/checks.py` | 依据新标准检查正式入口存在、职责位置和文档；Notebook 仅存在时检查；去除新标准下固定函数名/强制三格的矛盾 | 不执行候选模块；执行契约交由登记的公开平台验证命令 |
| `.../development/tasks.py`、runner/CLI | 新登记/预算纳入任务绑定；续接拒绝混用不同版本；必要时扩展错误输出 | 不给旧任务重置基线、删失败回执或自动接受 |
| `Demiurge/docs/demiforge-business-pipelines.md`、quickstart、canonical skill | 同步新标准、接入路径、检查/行为/质量三种证据范围 | 不新增强制对话或服务执行权 |
| `demiwtg/PIPELINE_SPEC.md` S02/S03/S10/S11、AGENTS | 明确统一 Flow 布局、模型/I/O 显式、Notebook 可选、历史数据保留 | 通用规范和项目业务语义分开；不追加互相冲突的新旧规则 |
| `demiwtg/.demiforge/project.json`、inventory、布局测试 | 登记迁移后的实际入口/算子/文档及隔离测试，固定新标准 | 不自动迁移全仓其他 pipeline；其他现役调用者只同步受影响引用 |

快照 256 MiB 问题先做完整监测清单的只读容量统计。新的预算机制仍有硬上限、耗时和总 I/O 限制，默认保持原边界；项目明确配置经审阅的容量。不得通过跳过 Notebook 输出/脏文件、只监测目标子目录或复用旧摘要声称通过。

Notebook 全文件仍哈希保护。结构检查只保留有界的 cell source/tags 等所需结构，已有大输出不装入检查进程内存，也不改写或删除；必须验证输出变化仍使完整快照变化。这是控制解析内存，不是将输出排除出监测。

## 3. P1：项目登记与 HTTP 路由

### 3.1 `demiflow/forge/console.py`（拟新增）

唯一新建的服务登记模块，函数级负责 `add/list/remove/load_registry/resolve_workspace`。保存有界 JSON 清单，复用现有 `read_control/write_control` 和 CAS 机制；并发变更使用清单专属文件锁，不依赖其中任意项目。

条目：workspace ID、root、名称及读取/运行权限上限。名称用于显示，ID 绑定工作区；root 不来自 HTTP query。重复同一 ID/root 幂等，不同绑定冲突。坏项目单独显示诊断，不使其他项目不可用。

拟新增 CLI：`forge console add/list/remove/serve --registry ...`。该清单是服务允许访问范围，不是认证状态。`serve` 复用 `WorkspaceServer`，不再建一套 HTTP 栈。

### 3.2 `forge/workspace.py` 与 `workspace_cli.py`

- `init_workspace` 增加显式非空初始化模式。检查冲突、路径与可写范围，控制文件最后提交；只清理本次未完成的自有文件，不回滚用户文件。
- v2 控制文件增加稳定 ID、明确的入口/配置索引、位置用途/权限、资源策略。禁止开放 schema 中未知执行字段。
- 新增入口登记/删除操作；读取上下文只返回已登记入口，helper 仍可被闭包检查，不能自行成为入口。
- `add_location/location_root/resolve_arguments` 从固定 `data/` 改为声明的位置与逐目标权限；默认仍只读，写入需要明确模式和所有权。
- 表引用控制支持可选展示和历史证据元数据。控制文件 CAS 摘要包含全部内容，数据绑定摘要只包含影响身份/读取的事实；改列标题不让 Flow stale。
- `_inspect_flow/_report_status` 绑定准确源码闭包、配置、原生契约版本、schema、固定表/对象位置与权限；不因项目无关文件改动重检全部历史。
- `workspace_changes` 只查询有界控制信息与索引 revision；不扫 data、对象库、模型 ledger 或全部历史运行。

拟新增用法：`workspace init ROOT --existing`、`flow register ROOT ENTRY --params ...`；准确参数在实现时与现有命令组整合。

### 3.3 `forge/workspace_view.py`

- 构造器接受登记清单；旧单工作区启动方式归一到同一 root 解析路径，保留其既有行为。
- 请求前统一完成鉴权→项目 ID 解析→路径/权限核验，再调用现有 workspace/runs API。每次请求保存局部 root，不通过修改共享 `server.root` 切换项目。
- 新增 `/api/projects`；工作区操作使用明确 ID 的路径/参数，禁止混用多个项目字段。
- 继续 loopback/token/Host/Origin/CSP，页端无任意 URI、shell、Python handler。读取默认开放，运行能力按项目显示并由服务端再核验。
- 单项目慢读受 worker deadline 约束；服务连接和活动工作预算仍有上界。

### 3.4 `forge/web/{app.js,view.js,index.html,style.css}`

增加项目选择与明确的项目名称/位置状态，全部缓存键和请求上下文带 workspace ID。切换时使旧请求 generation 失效，取消旧轮询；旧响应不能覆盖新项目。草稿按项目/入口隔离，历史查看不自动跳到最新结果。

新深链接用稳定 ID，表仍绑定固定版本/摘要，运行节点仍绑定 report。错误包括未登记、失效引用、只读、检查未完成、历史证据不足；不一律显示“暂无数据”。

表展示用受限声明驱动字段路径、列表和详情。提示“历史结果”与“受管运行结果”的来源；业务完成字段独立于进程状态。页面不执行业务状态公式。

## 4. P1/P2：真实数据、对象和证据

| 文件 | 具体改造 |
| --- | --- |
| `lance/storage.py:resolve_local_uri/_location_mapping` | 复用 `execution/location_index.py` 的有界解析/索引，支持 tables 精确键；消除大 manifest 的全量 `json.loads`。保持原路径解析顺序与歧义拒绝 |
| `execution/location_index.py` | 补 tables 类型校验、声明缓存目录/预算、所用映射记录的身份核验。缓存可重建；变化中拒绝混合快照；不让缓存成为来源权威 |
| `lance/preview.py:_local_layout/_open/_rows/media` | 接收受限解析后的位置与固定引用，核验当前版本实际用到的 fragment/对象；不枚举全表历史。增加有界嵌套投影/条件、图像引用范围校验；保持隔离分配保护 |
| `forge/workspace_worker.py` | worker 请求携带已校验的位置范围、预算、映射证据；候选项目不进 `sys.path`；超时/内存/响应字节限制由原协议管理 |
| `forge/workspace.py:table_binding/preview_binding/sample_table` | 区分逻辑引用、原生物理解析、数据绑定、展示元数据；失败不回退 head；摘要变化需重新绑定 |
| `objects.py`、`collect/documents.py`、`collect/image_resize.py` | 原生对象/文档读取和图像编码增加可检查的公开契约；支持显式本地根、哈希、字节/像素、调用错误。保留实际像素处理顺序和编码参数 |
| `collect/flow_contract.py`（拟新增）、必要的 `collect/wiki_syntax.py`（拟新增） | 原生资源效果与纯语法解析契约。wiki 仅语法记录，不含导语选择或模板业务名单；输出不泄露可调用的第三方对象 |
| `operator_llm/call_ref.py`、`forge/runs.py:evidence` | 复用 PromptRecordRef；历史证据只在显式声明位置按固定引用读取，限制大小，脱敏显示；不运行任何历史脚本 |

大索引首次构建明确显示进行中/失败，不放进普通三秒刷新循环。位置映射绑定采用所用记录和原生索引身份，不能每次哈希 784 MB 文件，也不能用 mtime 代替完整数据真实性证明。

历史表迁移不产生假 run：登记 `DatasetRef` 和可选原回执即可，当前绑定与历史来源可信程度分开。原始记录缺少源码摘要时不补造。

## 5. P2：原生契约与静态分析

### 5.1 `data/dataset.py`、`data/flow_contract.py`

给现有 `reduce_by_key/count/save_lance` 补所属原生契约；有限摘要 query 只允许有上限的聚合结果，禁止把 `take_all` 一般化放行。完善归约 accumulator 类型、初始化与输入别名规则。

增加通用原生 guard 入口（拟名 `data.require`）：接收可检查的标量条件与诊断，失败停止后续动作；统计值由原生 query 产生。不能使用可被优化掉的 Python assert 作为唯一生产门禁。guard 必须有动作依赖和运行记录。

schema 使用受限、纯声明的原生值构造契约，得到真实 Arrow schema；核对完整字段顺序、物理类型、nullability 和 metadata。既有 `pa.Schema` 不作为任意运行时 Python 对象直接豁免，也不简化成几个列名。新值构造器归 Lance 原生模块，业务 schema 仍由业务维护一份。

### 5.2 `forge/flow_check.py`、`source.py`、`operator_check.py`

- 维持 flows/operators 的目录边界；不新增任意源码根或可信业务插件注册。
- FlowChecker 增加 query/receipt/schema/checkpoint 等原生值的静态描述与受限消费者校验。仅允许可确定拓扑、固定调用顺序和失败停止；数据依赖 if/循环改变拓扑仍 incomplete/invalid。
- 动作边和数据边分别记录；运行时值只有从对应原生结果产生后才能消费，禁止用手写路径/伪 receipt 冒充。
- OperatorChecker 增加明确类型的 accumulator、局部新建容器操作及必要纯标准库操作；借用输入不可写，全局状态/未知调用仍拒绝。循环有静态结构与运行时大小边界，不宣称普遍终止证明。
- 第三方语法/图像库由原生能力承载，业务只消费声明结构。业务 helper 不反向调用 Flow、模型、表或服务。
- diagnostics 保留源码位置、缺失能力和保证范围；unknown 不能改写成通过。OP-07/08 运行保证来自实际验证，不靠注解。

当前 `_native` 对输入/输出同 URI 的拒绝须改为版本和效果判断：仅在固定读版本、声明 CAS 基线及合法输出归属成立时接受同表更新；动态 head 或无基线仍拒绝。不能只在 `runs.py` 放开 overwrite，而让 checker 或实际执行仍按 create 解释。

### 5.3 `operator_llm/flow_contract.py` 与原生模型模块

扩展 `normalize_prompt/prompt_resources/analyze_prompt/prompt_facts`：支持 SystemOne 单题/多题、图片字段及真实 answers/errors 结构，绑定有效配置、native question 顺序、调用身份和模型 revision。

`parser.py/systemone.py/runtime.py/messages.py/tokens.py` 只补实际缺失的通用预算/事实接口，不复制 HTTP 客户端、多题解析器或 Prompt 协议。现有单题行为不回改。字符/token/图片/问题展开各有量纲，静态上界与服务端 admission 分开。

模型返回 schema 类型不能把全部 answers 误作无字段 record；按原生多题协议给出 map/nullable/error 类型，运行时逐题校验。请求预算在重试和恢复中持续计量，未知请求状态明确保留。

### 5.4 `lance/flow_contract.py`、`execution/{capabilities.py,plan_facts.py,checked.py}`

- Lance 契约表达 schema、create/overwrite/受管 append、expected_version、sink/checkpoint 所有权和提交引用。
- `capabilities.py` 按原生组合与资源策略判定支持；原生路径和 Forge 共用，不新增 API 名称放行列表。
- `plan_facts.py` 从真实 reducer/query/StreamLanceSink/PromptActor/checkpoint 提取事实；不把静态报告复制到 actor 作为自证。
- `checked.py` 校验实际值来源、动作先后、stage 输出版本、guard 结果和 schema；运行计划与静态绑定不一致即失败。

## 6. P2：写入、恢复、状态与预算

### 6.1 `forge/runs.py`、`run_worker.py`

`prepare/_prepare` 接受明确的输出访问模式。实验新表仍使用已有默认行为；稳定表模式禁止自动重定向新表，必须有 CAS 基线。原位置按服务/工作区授权交集准入。

输出不再靠 `kind == write_table` 一处收集：由原生 sink/action 的已声明效果给出所有目标及其提交方式。每个目标必须有唯一归属；stage 不是额外的业务最终交付。

前置查询、guard、流式阶段、receipt 回读和最终写入以原 Python 的固定顺序执行。`run_worker` 不解释/调度图。所有候选代码、配置和原生契约来自封存，实际输出由原生 receipt 注册。

`limits_with` 区分部署上限、工作区预算和本次收紧值；移除把演示默认值当所有工作负载永久最大值的语义。声明改变重新 prepare。监督进程监控本次分配/增量产物，不周期遍历整个既有大表的所有历史文件来计算本次写入。

业务完成状态通过声明的有界摘要字段展示；进程成功与业务 complete 可不同，不能影响旧表事实或自动触发发布。

### 6.2 `execution/stream_checkpoint.py`、`stream_lance.py`

保持原 checkpoint 的提交意图/恢复算法及 content-key 一致性。增加受管资源描述、事实读取及与 CheckedExecution 的对应关系。

提供原生的固定 checkpoint 输出读取能力（拟新增 public entry）：无已提交 stage 时返回具有声明 schema 的空 Dataset；有输出时固定该引用并验证所有权。这样恢复图无需根据文件存在与否在 Python 中换拓扑。该入口只消费 checkpoint，不读取任意用户 JSON 作为版本来源。

阶段提交后真实 receipt 可直接供后续 read 使用；不可再取最新 head。空流也需完成原生 schema/空表提交。完整覆盖 guard 通过后才进入最终派生阶段。

checkpoint 与 ledger 保留在持久 run 目录，scratch 可清理；用户取消不删除已提交阶段。不把 append 开放成任意共享表追加。

### 6.3 `execution/run_journal.py`

增加恢复尝试序号与生命周期记录。原事实表目前以 `(action_id, kind)` 唯一，不能直接覆盖旧 action 事实实现重试；新格式按 attempt 区分执行事件/观察，checkpoint 仍唯一，已提交表事实仍带真实版本。

attempt 开始使用 CAS/租约，旧 worker 上报因 attempt 不匹配而拒绝；完成记录、日志和观察不能跨 attempt 混合。版本升级只对新受管 run 写入，历史数据库只读兼容，不原地改造业务旧账本。

只有同一不可变绑定的 interrupted/cancelled 或明确可恢复的技术失败允许 resume；业务低分/缺答行不自动重打，已经成功完成的 run 不重启。重试的资源/请求预算沿用原累计账本，不能重置额度。

### 6.4 `forge/index.py`（拟新增）

一个每工作区的可重建列表索引，解决所有 run/report 全扫与 128 项硬阻塞。只保存来源定位、ID、时间、manifest 摘要及更新标记；详情读取原事实。索引更新在事实提交之后，失败不回滚成功写入。

重建为显式有界管理操作，读取时报告 index revision/是否完整。不得静默漏掉未索引运行。活动运行状态仍直接查询有界活动集合；新事实提交通过统一 API 更新索引。没有定时扫描整库的后台 watcher。

在创建/恢复 run、提交新报告等影响列表完整性的操作之前，持有既有工作区控制锁写入有界待更新标记；提交源事实后更新索引，最后 CAS 清除标记。崩溃后按标记核对源事实并补索引；未完成期间页面显示索引待同步。run 的常规观察写入不逐行更新列表索引。待更新容量耗尽时拒绝新增控制操作，不能丢标记后继续宣称列表完整。

该标记只恢复派生目录，不协调 Lance 事务。执行互斥仍用实际运行租约和原生目标 CAS；列表缓存的状态不能用于允许 start/resume。已有运行在索引更新失败时继续记录真实 journal/receipt，不回滚业务提交。

## 7. P3：首批业务标准化的具体位置

**实施范围后续修订（用户已明确）**：整个 `training/` 不修改；用户继续使用旧 pipeline 跑数据，因此以下“旧实现退出/消费者切换”本次不执行。新 Flow、operators、声明资源和说明文档独立新增并登记；旧 pipeline、Notebook 入口和业务依赖保持原位及原内容，不自动更改其数据路径、账本或服务。新旧实现通过固定输入做等价性验证，切换时间由用户另行决定。旧登记继续保留原标准，新 Flow 单独绑定 candidate.7；不得以新增 Flow 的通过替旧入口升级认证。

下面的业务名仅用于迁移清单和验收，不写入平台源码。

| 原位置 | 目标与调整 |
| --- | --- |
| `shared/concepts/identity/concept_identity_pipeline.py` | 接线迁入 `flows/identity.py`；audit/import 为同入口配置分支。生命周期、目录写入、journal 管理改由统一公开入口负责，旧执行实现退出 |
| `shared/concepts/identity/operators/{local,labels,documents,local_schema,common}.py` | 迁入 `operators/identity/` 并补类型；拆开资源读取/语法解析/业务处理；保留模板策略、标签合并和身份语义 |
| `shared/images/concept_images/shared_concept_images_pipeline.py` | 接线迁入 `flows/concept_images.py`；去掉入口自写状态、文件分支和私有生命周期，使用原生 checkpoint/receipt/guard |
| `shared/images/concept_images/operators/{intersection,relations,contracts}.py` | 迁入 `operators/concept_images/`；保持候选、键、独立评分和交集规则；按 typed accumulator 拆清局部变更 |
| `shared/images/catalog/operators/relations.py:raw_identities` | 抽出所依赖的纯业务能力到统一业务 operators 的明确归属模块；修改活动消费者，不能为调用一个函数导入整个带 I/O 的 catalog 模块 |
| `shared/images/concept_images/operators/pixels.py` | 通用像素能力归原生图像模块，所有活动消费者直接使用同一实现；保持 EXIF/白底/缩放/编码顺序与摘要，不留导入转发垫片 |
| 两条业务的 configs/prompts | 现役声明资源归 `flows/<name>/`；有效模型配置单一来源，历史 run 中封存内容原位保留 |
| README/tests/Notebook | README 同步新唯一入口；测试验证业务行为和通用契约；Notebook 只调用公开入口/查看，保留输出和执行计数 |
| identity datasets、关系历史 datasets/runs | 原位置只登记固定引用；不为迁移代码改写表、路径、模型账本或原 summary |

像素函数有 dense_caption、concept_grounding_caption、GT/reference 等活动消费者；在实施前完整核对引用并同步必要 import/接口及其回归。归档与已封存训练 source_snapshot 不修改。若像素结果无法保持一致，不能按“结构整理”合入；须作为明确语义变更重新 Review。

新运行控制记录归 `.forge/runs`；历史业务 `runs` 保留并可只读引用。业务固定 summary 若保留为对外交付，其 schema/含义仍由业务定义，通过可检查的原生写入产生，不再作为 UI 私有运行数据库。

旧命令/Notebook 调用点切换到统一公开 Flow 入口，旧实现删除或进入明确历史归档，不能留下仍可独立执行的双入口。任何现役调用者未完成切换时该迁移阶段不能验收。

## 8. P4：测试、运行展示与分发

复用 `test_forge_workspace* / test_forge_run* / test_operator_foundation*`；新增项目路由/登记、checkpoint 恢复、历史位置读取等缺失测试文件，不把全部场景堆进一个浏览器测试。

分层验收：纯协议与类型→原生实际计划/隔离资源→故障注入→真实只读业务数据→浏览器交互→安装包外验证。使用当前原生测试运行器及浏览器能力，不为测试安装第二套执行环境。

真实操作台保留原端口和 SSH 入口；服务更新是独立部署动作，代码与隔离测试通过后才按授权切换。原演示工作区可以作为单独项目保留，不覆盖其数据和历史绑定。

所有引用的源码/配置/Prompt 按实际实施时重新核对；当前存在大量既有 dirty/untracked 文件，以任务开始清单保护。只改本方案关联项，不清理无关服务、模型实验或训练输出。

既有交接报告记录的全局布局失败必须保留。属于本任务必需验证的失败不能通过删除验证命令改成通过；若修复必须扩大到无关业务，明确作为另一个范围处理，在此之前不宣称相关全局检查通过。
