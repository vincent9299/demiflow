# demiflow 升级完整记忆文档

> 更新时间：2026-11-08（Asia/Singapore）  
> 交接对象：当前总控会话及后续执行者  
> 当前状态：已接手原会话的全局判断、架构约束和文档维护；历史暂停项不因交接自动恢复。  
> 本文和配套架构文档直接在当前工作区重写，引用本地文件；不依赖远程工作目录，也不是 SSH 拷贝。

## 1. 交接结论

我已回读当前会话“统筹升级 demiflow”、Forge 总控会话和 DemiFlow 引擎总控会话的公开用户/助手消息，并核对当前工作区的协调指南、仓库约束、架构基线、实现 Review 和恢复记录。

接手后的总控职责是：

1. 始终代表用户的原始意图，先判断整体产品结果、架构边界和交付质量，再处理局部技术事项。
2. 统筹 Forge 工作台与 DemiFlow 计算底座的依赖、接口和验收，不把其中一条线的局部通过当成全局完成。
3. 亲自 Review 方案、真实代码、失败/取消/清理/恢复证据和用户路径；测试数量、派发状态和文档完成不等于产品验收。
4. 保护当前仓库的 dirty/staging/untracked、业务数据、Notebook 输出、开发历史和已有证据。
5. 只在用户明确恢复或布置具体实施任务后继续历史暂停项；不自动重启旧负责人、旧提醒、服务或业务作业。

当前工作区有大量既有修改，交接文档不替换、不清理这些修改。任何实施前都要重新读取 `git status`，只改当前任务授权范围。

## 2. 原始意图

### 2.1 Forge / 工作台产品 root

**做一个统一、真正可用的 DemiFlow 工作台，让用户看懂、配置和运行 pipeline，并灵活分析、比较和评测数据。**

完整拆解如下，后续 Review 不得把它压缩成只剩一个页面或一个测试：

- **R.1 统一、符合已确认 demo 的产品体验**
  - R.1.1：项目侧栏与开发、管道、数据、Readme 四个入口形成同一工作台，保留既有开发对话和阅读能力。
  - R.1.2：D1.9 demo 是具体验收基线；真实前端按其结构、信息层级、常用交互和自然响应式实现。
  - R.1.3：共享上下文紧凑，目录收起能释放空间，减少无必要弹窗和跳转；配置、能力和错误说明可达且清楚。
- **R.2 看懂、配置、检查并真实运行 pipeline**
  - R.2.1：以数据算子图理解 AI 写出的实际代码；查看节点输入输出、算子参数、图配置和真实源码位置。
  - R.2.2：从配置、规范检查、问题定位与处理，到真实提交；无效、局部和未支持能力必须如实呈现。
  - R.2.3：按 Run/Attempt 查看封存图、真实节点观测、状态、日志、监控和结果；支持停止及受支持的恢复，分清当前编辑与历史运行。
- **R.3 灵活理解和分析数据**
  - R.3.1：查看项目数据目录、表、字段、版本及枚举/统计分布，理解数据内容和覆盖范围。
  - R.3.2：同一 Notebook 支持真实 Python/Jupyter 与 SQL；支持编辑、执行、取消、恢复、保存和重开，并受控互用结果。
  - R.3.3：通用呈现 table、message/dialogue、text/md/code/json、图片、音频、视频、tag、html；来源、范围和降级行为必须真实明确。
- **R.4 可配置地比较和持久评测 Case**
  - R.4.1：分组、筛选、选样、翻页及服务端标注状态条件。
  - R.4.2：共享输入/参考和多个输出方案；方案可含多个字段，字段角色、预览和比较/画廊布局可配置。
  - R.4.3：可配置评测/标注，正确分离 Case、方案和字段范围；保存/下一条、草稿恢复、历史和冲突处理完整，配置变化不需要反复找 AI 重写查看器。
- **R.5 支撑完整产品的工程质量与边界**
  - R.5.1：统一平台能力、清晰职责、合理架构，复用公共接口并同批清理被替代代码，避免重复体系。
  - R.5.2：资源有界、性能稳定；真实集成覆盖正常、失败、取消、清理和恢复，不以局部测试计数代替产品验收。
  - R.5.3：保护项目、业务数据、dirty/staging、Notebook 输出、开发对话和历史；来源、权限和持久化语义准确。

### 2.2 DemiFlow 计算底座 root

**让 DemiFlow 完整支撑实际数据场景，在正确性不退步的前提下做到高性能、稳定、通用且架构清晰。**

- **R1 完整能力链路**：原始获取、业务验证、来源展开、搜索、任务入队、领取、结果持久化、确认和后处理保持完整。
- **R2 统一执行与恢复**：单机和分布式共用 Dataset 计算语义，覆盖内存/持久化交接、shuffle、独立算子调度、barrier、checkpoint 和恢复。
- **R3 清晰架构**：A 资源、B 执行、C 交换、D 提交/恢复、E 编排各守职责；Forge 在上层；不重复建设运行账本、launcher、资源管理器或第二套图系统。
- **R4 正确性与质量**：在有限内存、磁盘、线程、句柄、读取和部署预算下保持正确、稳定和可解释；保留业务语义、已有成果、dirty/staging、历史及未知付费结果。
- **R5 可掌控交付**：用户掌控方向和取舍；总控负责协调、亲自 Review、阶段验收和组合验收，不让局部技术任务替代完整目标。

### 2.3 Review 取舍规则

每个问题先回答“影响哪个 R/AR 目标、是否影响真实用户结果、关键架构边界或历史保护”。必须关闭的缺陷与可后延的瑕疵分开记录；无关整洁、额外抽象、测试计数和目录搬迁不能自动成为交付前置。最新真人要求覆盖历史宽泛授权，历史判断只作为背景和证据。

## 3. 会话来龙去脉

### 3.1 Forge 线

用户最初要求建设一个可真正使用的工作台，后来确认 D1.9 demo 是体验和验收基线。讨论逐步收敛为：项目侧栏、开发/管道/数据/Readme 四 tab；算子图作为理解代码和运行的主体；Notebook/SQL 和通用富输出作为数据分析能力；Case 的分组筛选、输入输出材料和评测/标注作为同一分析页面的可配置部分。

Forge 总控随后完成了 R5.1 非引擎代码整理和 R2.2 真实示例链路。当前指南记录的真实示例为 `run r1158279da33d49e88217b8fc/attempt1`，读回一行 `c01`、`score=0.95`，输入版本和清理保持；相关本地证据在 [_demiflow/forge_coordination_20261104/r22_example_20261108](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/r22_example_20261108)。这只证明该阶段，不代表整个工作台的 R.1–R.5 已全部验收。

用户后来确认的架构方向是：计算底座、工作台产品、可复用 Forge 三块；工作台可以使用 Forge 公共 Context/Tools，Forge 不反向依赖工作台；业务项目保存代码、配置、运行记录和结果；不先抽统一运行 SDK/服务或第二份运行中心；SQL 当前可随 Notebook。

### 3.2 DemiFlow 线

引擎讨论首先澄清了 Ray Jobs、Python driver、Dataset API 和执行后端的层次。用户确认：外层可以指定任意 Python 程序作为作业入口，但被 DemiFlow 管理的数据计算必须通过 Dataset API 表达；Ray Data 不能成为 DemiFlow 的权威业务数据流语义。未来 Local、Ray、Yarn 都应承载 DemiFlow 自己的计划，后端只提供进程、容器、Actor/Task、传输和资源生命周期。

随后明确了 C1–C6 能力合同和 A–E 模块边界。barrier/checkpoint 候选已在独立分支完成并接受独立交付，但历史记录显示它尚未完成主仓合入和总控终验；其候选提交为 `e749182301547ec0c6c664bac9e715a094d3d175`。平台总控此前按用户要求暂停，保留方案和恢复入口，不把暂停记成完成。

### 3.3 当前交接请求

当前用户要求重新写“一份交接文档、一份架构文档”，并明确不要 SSH copy。因此本文和配套文档均以当前工作区的事实为准；远程会话只作为历史来源，不作为文件路径或当前状态的权威来源。

## 4. 关键时间线

时间线按设计决定和用户纠正组织。局部实现回合不能覆盖后来的真人决定；同一天内后发生的纠正优先。

| 时间 | 设计阶段和关键上下文 | 形成的决定/后续影响 | 主要会话或本地证据 |
|---|---|---|---|
| 2026-11-01 | Forge roadmap 和 Stage1–Stage4 逐步形成，工作台目标从“能运行”扩展为项目、图、数据、Notebook、Case 和交付闭环 | D1.9 逐步成为体验基线；产品交付与引擎优化分开记账 | [forge-roadmap-20261101](forge-roadmap-20261101.md)、[Forge 旧总控](codex://threads/01a1b17e-f012-7ee1-a368-c625dd4549f5?hostId=remote-ssh-discovered%3Adevbox-codex) |
| 2026-11-02～03 | 项目 onboarding、workspace audit、公开能力、Operator foundation、Codex panel、通用输出和媒体等设计/实现 | Forge 负责工程上下文和受控工具；工作台消费能力；样稿、真实能力和平台验收分别记录 | 见 [docs 设计索引](#设计文档链) 和对应 `forge-*-2026110*.md` |
| 2026-11-04 | DemiForge/Demiurge 能力向 demiflow 统一；统一 shell、项目 tab、Web redesign、源码覆盖、运行态管理和开发对话连续交付 | demiflow 成为对外主平台；旧入口、历史记录和兼容语义保留；不把 Forge 变成第二计算引擎 | [demiflow consolidation](demiforge-consolidation-plan-20261104.md)、[unified shell](demiflow-unified-shell-implementation-20261104.md) |
| 2026-11-05 上午 | 引擎线完成 SQLite 依赖只读盘点、性能/弱一致性复评、性能稳定性优化和架构复审 | 先保留语义和提交恢复；设计验收与实施/部署分开；有限资源、准确率、恢复和历史保护优先 | [execution foundation](execution-foundation-contract.json)、平台恢复记录、`sqlite_*` / `performance_*` 本地证据 |
| 2026-11-05 下午 | 工作台流程配置、算子图、检查、运行、Notebook/SQL、Case 三部分和通用富输出集中讨论 | 算子图成为理解 AI 代码的主体；独立运行页面不作为产品中心；过渡 UI 同批清理；图上“运行”必须真实提交 | [workflow workbench discussion](forge-workflow-workbench-discussion-20261105.md)、[runtime core flow review](forge-runtime-core-flow-review-20261105.md) |
| 2026-11-05 晚 | 用户明确 Review 要始终从原始意图出发，关系不大的不足不必做到完美 | 必须缺陷、可后延缺陷和不在范围分开；测试计数、目录整洁和局部抽象不能变成交付目标 | [workflow workbench review](forge-workflow-workbench-review-20261105.md) |
| 2026-11-06 | M01–M09 数据获取/分布式能力完成主仓集成与独立验收；Forge 新总控接任 | 主仓 dirty/staging/Notebook 保留；旧 P1–P5、真实多 host/COS 和生产切换不自动扩大范围 | [_demiflow 平台恢复记录](/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/resume.json) |
| 2026-11-07 早 | 用户重申 demo 不得丢失；Forge 继续以 D1.9 为验收基线 | 产品体验、真实 pipeline、数据分析、Case 和工程质量必须同时保留 | [Forge 原始意图](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/original_task_intents.md) |
| 2026-11-07 19:12 | 架构总意图 v1 定版；先重画架构图，再改文档和代码 | 三块架构、工作台→Forge 单向依赖、业务项目保存运行事实；不先抽统一运行服务/SDK | [architecture-master-intent-v1](architecture-master-intent-v1.md) |
| 2026-11-07 20:04～22:05 | 两条总控的角色、暂停和项目记录语义被真人重新澄清 | 总控要持续代表 root；应用各自入口复用同一项目记录，不复制账本；性能专项不作为 R5.1 前置 | [Forge resume](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/resume.md)、[platform resume](/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/resume.json) |
| 2026-11-07～08 | barrier/checkpoint 候选完成独立分支交付；Forge 完成 R5.1 非引擎合入及 R2.2 真实示例；引擎总控按用户要求暂停 | barrier 候选不能因独立通过就自动合回主仓；R5.1→R2.2 先于其他扩展；本地重写交接和架构文档 | [barrier owner](codex://threads/01a1b3cb-280c-7a22-ab06-487349f384c1?hostId=remote-ssh-discovered%3Adevbox-codex)、[R2.2 owner](codex://threads/01a1b71b-56c5-7d32-96ef-890e146f1a70?hostId=remote-ssh-discovered%3Adevbox-codex) |
| 2026-11-08 | 当前用户要求“一个完整记忆文档、一个独立架构文档”，并明确不要 SSH copy | 本文改为完整历史记忆；全局架构单独维护；会话链接和本地证据索引可追溯 | [当前统筹会话](codex://threads/01a1bb1f-c000-7811-bcc6-c876956818fe) |

## 5. 已确认的架构决策

1. **Python 入口与 Dataset API 分层**：Python 程序回答“这次作业从哪里启动”；Dataset API 回答“这次作业做什么数据计算”。两者不是并列的计算引擎入口。
2. **计划由 DemiFlow 拥有**：工作台不能通过 AST 猜图，Ray Data 不能拥有另一份业务图；逻辑计划、物理计划、Run/Attempt 身份和恢复事实必须能关联到同一份原生计划。
3. **执行后端只是承载适配**：Local、Ray、未来 Yarn 替换进程/容器/资源承载，不替换 DemiFlow 的业务图、通道、背压、checkpoint、commit、ACK、fence 或结果语义。
4. **Forge 是上层工程能力**：Forge 拥有 Context、Tools、规范、开发/运维工作流；它不变成工作台实现、计算引擎或底层资源 owner。
5. **工作台保留产品领域状态**：工作台拥有自己的项目、图、运行管理、Notebook/SQL、Case、媒体和开发会话入口；它可以复用 Forge 公共工程用例，但不复制底层运行账本。
6. **项目记录是业务事实位置**：业务项目或其显式配置存储保存代码、配置、run/attempt、日志、checkpoint、结果和来源；不在 Forge、工作台和引擎之间复制三份权威状态。
7. **当前不制造中间层**：本轮不先抽统一运行 SDK、HTTP/RPC 引擎服务、中央运行中心、第二个 scheduler、第二个 allocator 或兼容垫片；底层升级和真实复用收益出现后再评估迁移。

## 6. C1–C6 与 A–E 合同

| 合同 | 固定语义 |
|---|---|
| C1 能力与版本 | 支持哪些运行方式、操作、后端和协议版本；未支持能力明确拒绝 |
| C2 固定运行输入 | 代码、环境、入口、配置、Dataset 计划、输入输出、依赖和总预算的不可变引用 |
| C3 控制与身份 | 启动、观察、取消、受支持恢复；Run/Attempt、操作身份、expected 版本和期限 |
| C4 事实与结果 | 状态、日志、指标、节点观测、新鲜度、已提交结果引用和清理事实 |
| C5 恢复与兼容 | checkpoint/schema/工件兼容、剩余预算、未知结果和旧 attempt 保护 |
| C6 资源职责 | 上层核总额度，E 形成需求，A 分配、观察、对账和释放；父额度与子占用不重复计量 |

| 模块 | 唯一职责 |
|---|---|
| A | 资源规格、worker 生命周期、扩缩、重启、释放和物理资源计量；独立于 Forge |
| B | source/transform/sink、UDF、局部状态、有界缓存、增量输出和算子执行 |
| C | Channel、shuffle、分区、路由、成员、序列、背压、credit、取消和传输引用 |
| D | 状态迁移、checkpoint、commit、ACK、fence、恢复兼容、失败、未知结果和清理 |
| E | LogicalPlan/PhysicalPlan 编译、Run/Attempt 绑定、提交、观察和运行编排 |

## 7. 当前事实与未完成项

| 工作线 | 已有事实 | 未完成或限制 |
|---|---|---|
| Forge / 工作台 | R5.1 非引擎合入和 R2.2 真实示例已被当前指南接受；D1.9 仍是体验基线；当前无在途执行任务，自动唤起关闭 | 整体产品仍未按所有 R.1–R.5 独立验收；Source36 页面尾项、性能和部分组合证据需以新鲜原件复核；不得用历史样稿替代真实闭环 |
| DemiFlow 主线 | M01–M09 主仓能力、部分性能/稳定性修复和只读设计有已记录验收；架构总意图与实现 Review 已本地化 | barrier 候选尚未由本地当前状态证明已合入主仓；统一 exchange/worker、RayCore/RayData 边界、native 恢复/ACK 仍需恢复后按实际代码验收 |
| 跨线合同 | C1–C6、A–E、Dataset→Plan→Run/Attempt→Backend 已定为共同语义 | 具体类、字段、协议和实现不能从架构文档推定已完成；必须绑定当前 revision、调用方和真实证据 |
| 运行环境 | 既有 dirty/staging/untracked、Notebook 和历史保留；旧业务/合成数据会话按原用户要求不归引擎总控管理 | 不得因接手文档启动旧服务、定时器、业务作业或恢复排除的会话 |

## 8. 恢复和实施协议

恢复任何实施前按以下顺序读取和核对：

1. [Forge 协调指南](/yzp/zhaozy/yangzepeng/0905/FORGE_COORDINATION_GUIDE.md) 和 [demiflow/AGENTS.md](/yzp/zhaozy/yangzepeng/0905/demiflow/AGENTS.md)。
2. [架构总意图 v1](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/architecture-master-intent-v1.md)、[当前实现 Review](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/architecture-implementation-review-20261107.md) 和本文配套架构图。
3. [_demiflow/forge_coordination_20261104/resume.md](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/resume.md)、[_demiflow/platform_coordination_20261104/resume.json](/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/resume.json) 及最新证据文件。
4. 当前仓库 `git status`、目标分支/工作目录、实际调用者和未提交差异；不要把历史 session 的 cwd 或远端路径当成本地事实。
5. 只为当前用户明确授权的范围设计、实施、验证和清理；Forge 任务不改计算底座，明确的引擎任务另按其分支和隔离要求处理。

每轮总控报告至少包含：把控意图、把控架构与边界、亲自 Review 方案、决策与协调、跟进实施质量、独立验收收口。报告要区分“已实测、已审设计、模拟/样稿、未验证和明确不在范围”，不能把文档、测试计数或一条消息回执写成整体验收。

## 9. 历史设计会话索引

以下索引按“根会话 → 总控 → 设计/实现负责人 → 验收/排除”分层。链接用于回看公开上下文；真正的当前状态仍以本地证据和最新用户消息为准。

### 9.1 根会话和两个总控

| 会话 | 作用 |
|---|---|
| [统筹升级 demiflow](codex://threads/01a1bb1f-c000-7811-bcc6-c876956818fe) | 当前总总控；要求合并 Forge 和 DemiFlow 的全局理解，并生成记忆/架构文档 |
| [接手 Forge 优化会话](codex://threads/01a1b5d9-784a-73b0-ad7a-fea30b0ba36b?hostId=remote-ssh-discovered%3Adevbox-codex) | Forge 当前总控；D1.9、R5.1、R2.2 和工作台交付 |
| [接手 demiflow 优化总控](codex://threads/01a1b5db-2d47-7351-8cc0-a67fb25ae8b7?hostId=remote-ssh-discovered%3Adevbox-codex) | 引擎总控；Dataset、A–E、C1–C6、barrier、性能与恢复 |
| [落实 Forge 架构边界 R5.1](codex://threads/01a1b623-d405-79a1-9cea-8199a34c21de?hostId=remote-ssh-discovered%3Adevbox-codex) | R5.1 三块架构、工作台→Forge、边界切口和实施授权 |
| [接手 Forge 优化旧总控](codex://threads/01a1b17e-f012-7ee1-a368-c625dd4549f5?hostId=remote-ssh-discovered%3Adevbox-codex) | Forge 前任总控；保留早期交付、D1.9 和协调历史 |
| [接手 demiflow 优化旧总控](codex://threads/01a1af6f-55a9-7550-a78d-89e732f1d510?hostId=remote-ssh-discovered%3Adevbox-codex) | 引擎前任总控；角色纠正、长期暂停和 A–E 方案来源 |

### 9.2 Forge / 工作台设计链

| 会话 | 关键历史信息 | 对应本地材料 |
|---|---|---|
| [流程配置与算子图负责人](codex://threads/01a1aa6d-aebb-7073-b91a-2199b1606b4a?hostId=remote-ssh-discovered%3Adevbox-codex) | 统一流程配置、图、检查、运行和结果；用户后来确认算子图为主体，独立运行页不保留为中心 | `forge-workflow-workbench-*.md` |
| [运行主流程负责人](codex://threads/01a1a624-3122-76a1-b3c1-2366a74f531b?hostId=remote-ssh-discovered%3Adevbox-codex) | 运行配置、权限、提交、停止、恢复和公共接入；作为过渡能力先体验，后与图精确整合 | `forge-runtime-*.md` |
| [R2.2 真实示例负责人](codex://threads/01a1b71b-56c5-7d32-96ef-890e146f1a70?hostId=remote-ssh-discovered%3Adevbox-codex) | 主仓小示例完成配置→检查→修复→真实提交→结果读回 | `examples/forge_workspace/`、`r22_example_20261108/` |
| [Source36 页面/性能验收](codex://threads/01a1b615-ff9c-7e72-a9db-591be77540d0?hostId=remote-ssh-discovered%3Adevbox-codex) | 四图、重开、宽窄和性能的阶段验收；失败只代表该批证据不足，不等于整体功能不存在 | `stage_review_20261107_2036/` |
| [Source36 运行与安装准备](codex://threads/01a1b025-705a-76c3-b178-04eb28528297?hostId=remote-ssh-discovered%3Adevbox-codex) | 安装身份、入口和资源/性能验证准备；与产品验收分开 | `forge_coordination_20261104/stage_review_*` |
| [梳理概念与图片数据集](codex://threads/01a1ab9e-b8d2-7a43-a7bd-a66c9c76a00d?hostId=remote-ssh-discovered%3Adevbox-codex) | 图片/概念数据、清理和覆盖保护；回看时不能把业务数据会话误算成引擎设计验收 | 相关 `_demiflow` 业务证据 |

### 9.3 DemiFlow 引擎和平台设计链

| 会话 | 关键历史信息 | 对应本地材料 |
|---|---|---|
| [barrier/checkpoint 实施负责人](codex://threads/01a1b3cb-280c-7a22-ab06-487349f384c1?hostId=remote-ssh-discovered%3Adevbox-codex) | 独立分支 `e7491823…` 候选；主仓合入和终验仍需新鲜条件 | `_demiflow/native_barrier_implementation_20261107/` |
| [SQLite 依赖只读设计](codex://threads/01a1a798-f4f9-72d2-870b-eb51718c42e0?hostId=remote-ssh-discovered%3Adevbox-codex) | 盘点 SQLite 机制和去 SQLite 方案；设计完成，不等于迁移、删除或部署 | `sqlite_retirement_design_20261105/` |
| [弱一致性/性能复评](codex://threads/01a1a795-aa9c-7c41-abd7-a59a75267bf1?hostId=remote-ssh-discovered%3Adevbox-codex) | 正确性不降低前提下的性能和一致性路线；方案与实现分开 | `performance_consistency_reassessment_20261105.json` |
| [性能稳定性实施](codex://threads/01a1a994-f405-7cf3-8552-e58b365b813d?hostId=remote-ssh-discovered%3Adevbox-codex) | 有限资源、提交恢复和性能证据的主仓集成；保留 SQLite/FULL 和业务语义 | `performance_final_handoff_20261105.json` |
| [架构/性能稳定性复审](codex://threads/01a1aa0a-d4dc-74a3-9ff3-cec380914258?hostId=remote-ssh-discovered%3Adevbox-codex) | 只读审计、根因归并和后续方案；未授权的方案不能写成已实施 | `architecture_performance_stability_reaudit_20261105/` |
| [异步超时修复](codex://threads/01a1aac7-2631-7fd2-a2c3-698cbe6b44bd?hostId=remote-ssh-discovered%3Adevbox-codex) | 修复 batch_map 超时吞掉导致空表成功；保持生产冻结和回归边界 | `async_timeout_review_20261105/` |
| [M01–M09 分布式获取](codex://threads/01a1ab8e-b533-7492-af21-8de75b456b0f?hostId=remote-ssh-discovered%3Adevbox-codex) | 完整能力链路主仓集成与独立验收；真实多 host/COS 按范围未做 | `dataset_fetch_distributed_implementation_20261105/` |
| [资源基础/兼容性设计](codex://threads/01a1a451-f216-7b51-adb8-255b0b44325b?hostId=remote-ssh-discovered%3Adevbox-codex) | 资源 owner、容量、执行后端和底层公共合同的早期设计来源 | `foundation_*`、`execution-foundation-contract.json` |

### 9.4 明确排除或仅作背景的会话

| 会话 | 处理规则 |
|---|---|
| [试作 DeepSeek 出题与参考图合成数据](codex://threads/01a1abc7-2a4d-75d3-8b2f-0bdf84288df3?hostId=remote-ssh-discovered%3Adevbox-codex) | 用户明确要求引擎总控不再负责、监控或联系；仅保留“排除范围”事实，不把其业务结果当平台验收 |
| 旧的业务、演示和媒体负责人会话 | 只读取已归档的证据和交接，不自动重启、不覆盖新答案、不把样稿当正式能力 |

### 9.5 设计文档链

按层追踪时使用以下本地文档：

1. `FORGE_COORDINATION_GUIDE.md`：总控角色、R.1–R.5、Review、保护和交付方法。
2. `_demiflow/forge_coordination_20261104/original_task_intents.md`：Forge 原始意图和 AR 分解。
3. `docs/forge-roadmap-20261101.md`：按日期的产品路线、D1.9、R5.1→R2.2 顺序。
4. `docs/forge-workflow-workbench-discussion-20261105.md`、`design`、`review`：算子图、Notebook/Case、运行和清理决策。
5. `docs/forge-runtime-core-flow-review-20261105.md`、`docs/forge-runtime-management.md`：运行控制、权限、状态和证据边界。
6. `docs/architecture-master-intent-v1.md`：三块架构和目录/依赖定版。
7. `docs/architecture-implementation-review-20261107.md`：当前实现差距，尤其 Forge 工程规则、运行管理和 Context/Tools 切口。
8. `docs/execution-foundation-contract.json`：P0–P2 合同和后续 P3–P6 迁移边界。
9. `_demiflow/platform_coordination_20261104/resume.json`、`root-intent.json`、`coordination.json`：引擎总控暂停、已完成项、barrier 待办和排除范围。

### 9.6 补充历史会话（设计证据/协调，不是当前 owner）

这些会话在平台协调记录中留下了设计、资源、checkpoint、媒体或交接证据。它们用于按图索迹追溯，不代表当前仍在执行，也不因出现在记忆文档里自动恢复。

- [checkpoint 配置分析（2）](codex://threads/01a1a451-f216-7b51-adb8-255b0b44325b?hostId=remote-ssh-discovered%3Adevbox-codex)
- [历史 checkpoint 复核](codex://threads/01a1a4fa-3eb4-75c3-9787-9163e6b1206c?hostId=remote-ssh-discovered%3Adevbox-codex)
- [checkpoint 复核候选](codex://threads/01a1a507-b86c-71f1-a04f-a6df37f04583?hostId=remote-ssh-discovered%3Adevbox-codex)
- [checkpoint 复核候选二](codex://threads/01a1a533-fa4e-7b60-ac44-d432cd88e012?hostId=remote-ssh-discovered%3Adevbox-codex)
- [checkpoint 复核候选三](codex://threads/01a1a533-e36c-78e3-854b-20302c75d975?hostId=remote-ssh-discovered%3Adevbox-codex)
- [checkpoint 复核候选四](codex://threads/01a1a534-1084-74e3-a43d-03cd4a07e73a?hostId=remote-ssh-discovered%3Adevbox-codex)
- [媒体联合验收](codex://threads/01a1a534-3e0d-7de2-abba-0cc1cf728484?hostId=remote-ssh-discovered%3Adevbox-codex)
- [历史媒体/评审派发](codex://threads/01a1a534-2783-79d3-bc5a-188181335400?hostId=remote-ssh-discovered%3Adevbox-codex)
- [托管媒体进度审计](codex://threads/01a1a58d-55bf-76d2-9aff-fadfb995d225?hostId=remote-ssh-discovered%3Adevbox-codex)
- [媒体/回滚历史交接](codex://threads/01a1a21d-507b-7562-955a-fa4ea2be1f58?hostId=remote-ssh-discovered%3Adevbox-codex)
- [用户停止与范围确认](codex://threads/01a1a568-7fe8-74f1-abb3-0145a7e2a037?hostId=remote-ssh-discovered%3Adevbox-codex)
- [弱一致性五项优先级](codex://threads/01a1aa58-5cc3-77a1-a7b6-41869efaedc4?hostId=remote-ssh-discovered%3Adevbox-codex)
- [平台边界分析](codex://threads/01a1aa66-7645-7810-a7c7-b0a367b2c148?hostId=remote-ssh-discovered%3Adevbox-codex)
- [分布式获取场景确认](codex://threads/01a1aa75-1817-7a11-8750-4e8ffff97a9d?hostId=remote-ssh-discovered%3Adevbox-codex)
- [Forge 旧总控交接](codex://threads/01a1aa74-951d-75d3-924e-b41a1150dc73?hostId=remote-ssh-discovered%3Adevbox-codex)
- [更早 Forge 总控](codex://threads/01a1a775-c134-7743-87b3-801c0efad537?hostId=remote-ssh-discovered%3Adevbox-codex)
- [引擎旧总控交接前置](codex://threads/01a1af34-40e7-7d71-982f-67afaa2df9cd?hostId=remote-ssh-discovered%3Adevbox-codex)

若需要继续追溯某一项，先打开会话，再按同名本地证据目录核对源码快照、任务书、Review 和验证文件；不要只凭会话标题或旧助手摘要判断当前状态。

## 10. 本地文档和证据索引

- [配套全局架构文档](demiflow-forge-global-architecture-20261108.md)（独立维护，不并入本文历史记忆）
- [架构总意图 v1.3](architecture-master-intent-v1.md)
- [当前实现与架构边界 Review](architecture-implementation-review-20261107.md)
- [执行基础 P0–P2 合同](execution-foundation-contract.json)
- [Forge 当前交接](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/resume.md)
- [DemiFlow 平台恢复记录](/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/resume.json)
- [Forge 原始意图档案](/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/original_task_intents.md)

### 历史会话来源

- [统筹升级 demiflow](codex://threads/01a1bb1f-c000-7811-bcc6-c876956818fe)
- [接手 Forge 优化会话](codex://threads/01a1b5d9-784a-73b0-ad7a-fea30b0ba36b?hostId=remote-ssh-discovered%3Adevbox-codex)
- [接手 demiflow 优化总控](codex://threads/01a1b5db-2d47-7351-8cc0-a67fb25ae8b7?hostId=remote-ssh-discovered%3Adevbox-codex)

这些链接用于回看历史语境；当前实施状态以本地仓库、最新用户消息和本地证据为准。
