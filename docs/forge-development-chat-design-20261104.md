# DemiFlow 项目开发对话设计

日期：2026-11-04。状态：**设计与协议探针，待主控架构 Review；未实施、未发布**。

用户要求：在项目总览之前增加开发对话，背后调用 Codex CLI、DeepSeek CLI 等 code agent，并确保开发遵循现有规范与要求。本方案把它落实为项目内的「开发」tab，复用现有规范、代码 agent 和检查机制。不会把对话服务扩展为业务运行调度器或多代理平台。

本轮仅新增本文；未修改运行源码、共享环境、服务、真实项目或其配置。调查基于当前正式仓及只读运行态候选 `/tmp/forge-runtime-20261104-0oi_zj87/demiflow`；实施必须接收运行态最终交付，再重新冻结组合基线。不能把当前 app.js/style.css 覆盖回旧副本。

## 1. 建议与关键边界

1. 首版完整实现 **Codex app-server**，每个活动开发会话拥有一个独立 provider 进程。DemiFlow 管项目绑定、准入、规范上下文、事件保存和页面；Codex 管模型与工具循环。
2. DeepSeek 实际安装是 **DeepSeek Harness `dsh` 0.2.1-rc.2**。首选扩展路径为 ACP，不使用缺少取消/审批的 SDK 通道冒充等价接入；ACP 的输入和历史差异见第 9 节。
3. 默认不因加了新 tab 改变用户落点：显式深链接优先，其次每项目最后选择，首次进入仍为「总览」。「开发」始终排在「总览」之前；新建项目成功页提供「开始开发」。进入页面不会启动 CLI 或模型请求。
4. 每个物理项目根同时最多一个写开发 turn；不允许两个聊天或两个进程同时写同一根。锁只约束参与 DemiFlow 的开发客户端，不能证明外部编辑器、其他聊天没有写入，因此还要检查原始 dirty 基线和变化。
5. **提示词、操作权限、证据有效性是三个层次。** Agent 的“已通过”文本、工具卡片和 stdout 不能给项目授予检查、Review、Freeze 或验收通过。
6. 业务作业与开发 turn 分离：停止开发只停止它拥有的 provider/工具进程；不调用 run-cancel，不按 cwd、进程名或同用户身份查杀其他作业。开发入口不提供启动生产运行的工具。
7. 当前容器的 Codex bubblewrap 实测启动失败。默认方案要求可执行的受限 runner，失败时保留页面、历史和诊断，关闭带工具的开发启动，**不自动改为 full access**。cgroup 不可写仅意味着未获得聚合资源硬限制，不否定字节准入、队列、RLIMIT、超时及进程归属机制。

## 2. 产品路径

保留最新 DemiFlow 标识、Harness 侧栏、264/56px 折叠和手机项目抽屉。项目列表仍一行一个项目；右侧 tab 为：

```text
开发 | 总览 | 数据与 Case | 流程与配置 | 规范检查 | 运行记录

项目名称 / 开发                         当前会话 ▾    新对话
修改订单去重逻辑                        Codex · 就绪
项目与范围：orders · 3 个可写路径        [查看开发依据]

你：保留首次出现的记录，并补充边界验证。
助手：先核对现有入口、重复语义和任务记录……
  ✓ 已读取项目规范       [展开依据]
  › 正在执行检查         [展开命令及输出]
  Δ 2 个文件有变化        [查看差异]
  ◇ 验证记录：待更新      [打开规范检查]

[描述要修改的功能、问题或需要确认的设计……                 ]
                                             [发送 / 停止]
```

这是交互草图，数字和状态不是实际运行结果。

首次进入显示：“在当前项目中讨论方案、修改代码并核对检查结果。开发会遵循本项目规范；运行任务仍在运行记录中管理。”下面为输入框、已配置的 agent 选择及“查看项目说明”。未配置或不可用时显示具体原因和已有历史，不能让发送按钮默默失败；登录/部署问题由设置说明承接，不让用户填写协议、PID、token 或环境变量。

会话列表为当前项目的标题、最近时间、状态；默认 20 项，分页加载。标题先用首条用户消息的有界摘要，不额外调用模型起标题。新建会话绑定项目后不可切换根目录；切项目切换会话列表，草稿按项目和会话分别保留，草稿不自动发送。模型/provider 改变需要新会话；以后若增加迁移，只能作为带来源的摘要新会话，不能伪装原 provider 续聊。

消息渲染沿用 `web/reading.js` 的安全 Markdown 与 worker 生命周期，不开放原始 HTML、脚本、远程图片自动加载。流式 delta 先作为文本追加，完成的段落按节流频率交给既有渲染器；代码块、表格或单条消息超过预算时给出分页原文入口，不无声截掉内容。只在用户处于底部时跟随滚动；离开底部显示“有新内容”。键盘可发送、换行和停止，状态变化使用有界 aria-live 提示。

命令、文件变更、检查分别成卡片：

| 内容 | 主视图 | 展开内容与事实来源 |
| --- | --- | --- |
| 助手消息 | Markdown、正在输出/已完成 | provider 消息，不是检查事实 |
| 命令/工具 | 名称、运行/失败/取消、耗时 | argv 或 provider 的展示命令、实际 cwd、有限输出；不能把展示字符串再作为命令执行 |
| 变更 | 文件数、任务开始后的变化 | 有界 diff、旧/新摘要、外部变化提醒；“有变化”不宣称由本 agent 独自创作 |
| 检查 | 当前/历史、通过/失败/缺失/过期 | 现有检查器的精确任务、revision、日志/回执引用；另标 self/independent Review |
| 输入请求 | 问题、选项、自由文本 | request 身份、有效期、已提交状态；不自动以空答复通过 |
| 权限请求 | 具体操作与影响、允许一次/拒绝 | 后端检查可授权范围；超出本部署能力只可拒绝并说明原因 |

运行中的普通发送首版禁用，保留草稿；不做隐式排队，不把用户补充变成第二个重叠 turn。提供停止后继续；Codex `turn/steer` 先留为能力，单独验证后再开放“补充要求”。允许回答当前必要输入，和新增任务消息是不同操作。

浏览器刷新/断流只重新读取保存事件，不提交新任务。后台仍按原有预算继续；需要输入时停在输入状态并受有效期约束。停止按钮先显示“已请求停止”，确认工具子进程停止后才显示“已停止”。失败保留文字、差异和检查依据；“继续”是明确的新 turn，说明前次哪些动作已发生、哪些未知，绝不重发所有旧命令。

## 3. 项目与开发任务的适用关系

现有 `workspace_id` 是登记和 HTTP 授权身份，不能用聊天 ID 替代。项目开发种类来自明确登记/用户任务，不按目录名猜测，且同一个项目可有多种入口：

| 开发种类 | 权威入口 | 会话进入条件和结果解释 |
| --- | --- | --- |
| 平台开发 | 目标平台 AGENTS、与修改有关的公开契约/架构文档、当前真人要求 | 平台被明确指定为开发目标；复杂变更先设计和架构 Review。不能套用“业务任务必须只读 demiflow”阻断已授权平台任务 |
| 原生 Forge 作者开发 | 目标 AGENTS、`workspace.json` 所绑契约/工程标准、源码/参数、当前平台绑定 | 继续通过现有静态/行为/Review/Freeze 链取证；开发会话不授予 execution 能力 |
| DemiForge 已注册 pipeline | 目标 AGENTS、`.demiforge/project.json`、任务实际版本标准、canonical Skill、pipeline README/入口及必读 sources | 复用 `list/history/context/start/check/verify/review/handoff`；旧 accepted 仅是当时快照 |
| 未注册/不支持的原项目 | 实际 AGENTS、现有公开项目说明和范围 | 可以分析缺口、草拟登记；需要登记可信上下文/验证后才进入相应受管验收。显示“未接入该开发标准”，不能冒充认证 |

一个 session 可关联零个或一个当前 development task；另存相关旧 task 引用。尚未登记任务的初始设计阶段只读，并清楚显示未绑定。实现开始前把目标、精确写路径、non-goals、pipeline 和当前用户要求绑定；如果任务范围已经明确，直接完成所需登记，不机械重复询问。范围不明或相互冲突时先询问并继续不依赖答案的读取。

新注册和可信测试命令不能由网页提交任意 argv 自动纳入白名单。DemiForge 工具自己的 `--allow-tests` 不代表模型、网络或生产授权；同时适用会话的工具/网络权限。

任务另存 `phase=design|implementation` 及其许可来源。按当前任务要求需先 Review 的复杂开发，design 阶段只允许读取和指定设计文档写入；实施路径未开放。Review 决定必须来自有资格的实际操作和可核对的设计版本，agent 的“方案通过”文本不能切换 phase。已明确授权直接修复的简单任务可以直接进入 implementation，不人为增加每次批准。这个字段只管本次任务可执行范围，不替代原生 Forge 或 DemiForge 的 Review/验收记录；HTTP token 本身也不证明操作人的真人身份。

## 4. 规范上下文：有界、有来源、无平行标准

唯一规范正文继续在当前平台与项目既有文件。本文的提示词模板是**装配指令**，不是新的 pipeline-vN。不要复制一份“Web 专用规范”再逐渐漂移。

每个 turn 有不可变 `context_manifest`，保存 `kind, task_id, source_id, canonical_path, sha256, version/binding, required, selection, pages, delivered_pages, bytes, origin`。保存用户实际要求和范围原文；assistant 提议与真人确认分开记录。文件文本是上下文来源，不能借其中内容扩权或修改后端能力。

装配顺序：

1. 服务可信头：项目身份、实际 cwd、授权范围、provider 能力和预算、当前任务类型、禁止冒认验收的职责边界。
2. 适用 AGENTS 层级和具体任务目录的 AGENTS；只读有明确适用范围的文件。根目录路由规则保留其含义；执行 agent 不接收主控的所有会话名单、唤醒、广播或协调职责。
3. 适用原生契约/工程标准或 DemiForge **任务实际绑定版本**的标准和 Skill。canonical Skill 为 `demiflow/forge/development/skill/SKILL.md`；不把 candidate.9 强行套到旧任务上。
4. 目标 pipeline 规范、README、入口、测试及公开平台 API 相关页，含任务已保存的未解决问题与决定；按任务实际相关范围读，不递归灌入整个 workspace。
5. 最新真人要求与其对先前要求的修改，原任务目标和非目标；不以自动摘要覆盖原文，也不把历史 assistant 文本提升为授权。

对已注册 DemiForge：通过受信平台安装的 **公开 stdlib CLI/Skill launcher** 调用指定 `--project`，不从业务 cwd 导入 Python 模块，也不盲执行可被项目改写的 `.demiforge/forge` 脚本。先 `list/history`；绑定当前时再 `context`。`context --read` 会写送达回执，只有页面正文实际进入本轮 agent 上下文才调用并记录，不批量空读。backend 保留它传入 provider 的文本与摘要；送达仍不等于理解。完整必读页未送达不能把上下文标齐，也不能通过 check。

Agent 对受保护开发记录的读页、note、refresh、check、verify、self-review 和 handoff 请求，通过薄的受信工具桥接到上述 CLI；参数是枚举操作、已绑定 task ID 和经过预算校验的字段，不接受命令字符串。桥接执行前再次检查阶段、路径和测试效果授权。首次 start/登记变更必须有实际目标与范围许可。工具只让模型操作现有开发协议，不创建新检查器；独立 Review/接受许可不能由模型自行指定 identity。模型的 self-review 仍按当前工具契约保存并明确标识，是否已获验收由现有门禁与适用流程决定。

Codex 的此桥接使用本机 experimental schema 已存在的 `dynamicTools` / `item/tool/call`，只开放这个固定工具并验证实验接口；它不调用 Dataset，不构成第二个模型循环。接入的实验字段必须随 pinned 版本一起验收。D2 若采用 ACP，须在固定受信 profile 中提供同一桥接或等价经过核验的入口；不能为省略桥接而把全部 `.demiforge/tasks` 开放给任意文件工具写入。原有本机任务文件不是防篡改签名，不能宣称能防止同 UID 的外部用户修改记录。

初始注入预算建议 128 KiB，总任务受管上下文 512 KiB，每来源遵守现有 1 MiB/8,000 字符页边界。单轮最多 32 页、每次工具响应最多 64 KiB；总必读内容更大时按页继续并保留进度，不能以预算为由删掉规范或缩小 watch。无法在 provider 上下文能力内完成时明确失败/要求拆分任务；provider 自己的上下文压缩不是“全部规范一直驻留”的证明。每轮最小可信约束摘要重新给出，原始来源可按精确摘要取回。

在任务来源、规则或 README 变化后，沿现有 refresh/重新阅读/旧证据失效语义；tool/standard/project-config 绑定变化不自动 rebase。发现旧任务 stale，展示原基线/历史，要求按现有迁移流程处理；不能创建新 baseline 清掉越界事实。初始 dirty 内容、Notebook 完整输出及旧检查证据始终保留。

平台/native 任务没有 DemiForge 的读页回执，使用本会话 manifest 表示“送入本轮的来源”；不能给它伪造 DemiForge receipt。新接入层只调用公开开发 CLI，不把 `development.tasks` 内部 API 当稳定接口，也不让现有原项目 Web 识别层导入开发工具执行声明。

## 5. 模块与接口

建议新增小包 `demiflow.forge.development_chat`，从现有 `forge.development` 分开：前者是交互能力，后者仍是 stdlib、无 agent loop 的开发规范工具。新包不导入 Dataset 或业务 pipeline 来运行聊天。

| 模块 | 职责 | 不承担 |
| --- | --- | --- |
| `service.py` | 接收经过认证的项目操作、准入、会话/turn CAS、事件订阅 | 模型循环、业务作业调度 |
| `store.py` | 会话/turn/请求/事件的事务落盘、分页、配额与恢复状态 | 扫描所有 provider 私有历史或改写旧证据 |
| `context.py` | 三类任务的权威来源选择、受信 CLI 调用和 manifest | 另造标准、自动填写 Review pass |
| `provider.py` | 最小能力及规范化事件类型 | 把所有 provider 强行拟成完全相同功能 |
| `codex.py` | 固定版本 app-server 方法/事件/输入审批映射 | 复用 agentmap 的行语义和临时 artifact 机制 |
| `worker.py` | 一个活动会话的 stdio provider、生命周期和预算 | 常驻多代理控制平面、自动重试付费任务 |
| `http.py` | 小型路由与 SSE 编码，接入既有认证/项目解析 | 新账号系统、浏览器直接连接 provider |
| `web/development.js` | 页面、事件投影、草稿、输入卡片 | 授权、任意文件/命令 API |

`app.js` 仅增 tab、view 调用和释放钩子，`view.js` 增 `development` 与 `development_session`（后者绑定 session_id）视图；保留现有 v1/v2 深链接兼容。新增静态资源仍须通过既有静态资源允许列表与离线导出判断：离线页面只呈已选历史，不伪造可发送能力。

最小 provider 接口：

```python
class DevelopmentProvider:
    def capabilities(self) -> ProviderCapabilities: ...
    async def open(self, project, context, policy, provider_session=None) -> Handle: ...
    async def start_turn(self, handle, submission) -> ProviderTurn: ...
    async def events(self, handle) -> AsyncIterator[ProviderEvent]: ...
    async def respond(self, handle, pending_request, answer) -> None: ...
    async def interrupt(self, handle, provider_turn_id) -> None: ...
    async def inspect(self, handle, cursor, limit) -> HistoryPage: ...
    async def close(self, handle) -> CloseReceipt: ...
```

能力至少含 `persistent_resume, history_replay, paged_history, interrupt, input_requests, approvals, file_events, command_events, steer, usage, filesystem_enforcement`，取值 supported/unsupported/unverified 及原因、版本。服务按所需能力拒绝启动；不存在的方法不“最佳努力”假装完成。provider 原始 ID 和未知事件保留有界诊断，不直接放进 DOM；可显示有限“该事件类型尚未呈现”，关键身份/权限事件未知则失败关闭。

### 5.1 复用现有代码的范围

`operator_llm/codex_agent.py` 当前把 row、OperatorScope、声明的 Dataset tool、PromptContext、media/artifact、SQLitePromptJournal 和 one-shot ephemeral session 绑在一起；它将所有交互审批视为不支持并失败。不得直接实例化它作为项目开发服务，也不改它的行语义。

可抽取复用的只有有界 JSON 编码/严格解码、JSONL 帧读取、request-id 对应、stdio 关闭与任务自有进程清理。这些低层能力放一个中性小模块（例如 `demiflow/agent_transport.py`），不 import Forge/OperatorScope。抽取时同时让原算子调用同一实现并回归原行为，避免复制第二份传输循环；审批/通知派发继续由各 caller 实现。旧客户端 rpc 等待时顺序接收的方式不能照搬来处理多种 outstanding request，新的低层 reader 由单读循环分发到**有界** pending map 和通知队列。

最终运行态候选的 PID 身份、子进程归属与排空实现可作为低层机制复用/提炼的依据。不要直接把开发 session 塞进 `runs.Run`，不要新增原生 Dataset executor，也不要在本轮未冻结运行态时迁移 supervisor。

## 6. 身份、持久化与状态

| 身份 | 含义/约束 |
| --- | --- |
| workspace_id | 当前 registry 的项目；每次操作再次解析并验证根身份 |
| root_identity | 规范化无符号链接绝对路径、设备/inode；重命名/替换根不自动接续 |
| session_id | DemiFlow 生成的不可猜测 ID；绑定项目、provider、创建时策略与 owner domain |
| provider_session_id | Codex thread / ACP session；只由后端读取，浏览器不可传任意 provider ID 接管历史 |
| turn_id | DemiFlow 的一次用户提交；独立于 provider turn、业务 run/attempt |
| provider_epoch | 本 session 本次 provider 进程连接；请求 ID 只能在该 epoch 内回应 |
| provider_turn_id | 回应/事件匹配；缺失的协议用受控请求区间显式表示，不能猜成 native turn |
| submission_id | 浏览器生成的幂等键；与 session、请求正文摘要唯一绑定 |
| sequence | session 内持久单调事件游标；不使用时间戳排序或复用 provider 临时通知 ID |
| pending_request_id | epoch + provider request ID + turn/item + payload digest，过期/终态后不可回应 |
| process_identity | boot_id、PID/start ticks、启动 nonce、所属 supervisor/进程单元；不能只存 PID |

建议状态根由服务显式配置，位于受保护的用户状态目录，权限 0700，不在 agent 可写项目中。存储一个小型 SQLite（会话索引、turn、submission、pending request、事件），配套配额内的分段附件/输出文件；provider home 也在该状态根内，和其它 Codex/DSH 聊天完全隔离。不要扫描或导入使用者所有私人会话。

事务先持久化提交意图、用户文本、context hash、策略和容量预留，再 dispatch provider；先保存事件/完成事实再广播。SQLite busy/disk full 时停止接收新工作，保留停止/失败写入的预留容量，不能靠无限重试增长队列。DDL 仅在显式启用开发服务时初始化，GET 不修复、不迁移、不启动模型。

主 turn 状态为 `preparing → starting → running ↔ waiting_input/waiting_approval → stopping → completed|failed|cancelled|interrupted`。`preparing` 尚无付费动作，准入失败可回到未提交草稿。成功由 provider 完成通知和所属命令清理共同确定；`completed` 仅指这次对话完成，不等于代码验证通过。

独立字段描述 `connection=connected|disconnected|recovering`、`dispatch=not_sent|sent|acknowledged|unknown`、`cleanup=not_needed|pending|complete|unknown`、`evidence=current|stale|missing|historical`。失联不是失败或取消证明；停止未确认时保留 stopping/unknown，并保留写租约。

### 6.1 幂等与断流

相同 submission_id + 相同正文返回原 turn；相同 key 不同内容返回 409。相同 session 同时发两个新 turn，后者 409，不排队。数据库已保存而发送前崩溃，可明确确定 not_sent 时继续 dispatch；发送后未拿到 ack 则 unknown，不能自动重发。Codex 的 `clientUserMessageId` 可用于关联，本机 schema 有该字段，但尚未证实服务端去重保证，因此不能把它当 exactly-once 凭据。

SSE 只传持久 sequence；断线后按 `after_sequence` 补读。若段已被显式清理，返回 cursor_expired 和可用范围，不能跳过后声称完整。provider 事件先到、HTTP 提交 response 后到也可按 turn 关联，不追加第二条用户消息。

### 6.2 重启恢复

浏览器掉线不结束 worker。Web 服务计划退出时先停止新提交，停止其拥有的 active turn 并排空，保存 interrupted/cancelled 等事实；不把 stdio 连接当成可移交到新进程的持久连接。

崩溃重启先读自己的未终结记录，核对 OS 身份/租约，确认旧 provider 和后代是否仍存活；不能仅凭心跳过期强行新开 writer。已有活动来源不明时冻结继续操作，显示待核对。确认旧执行已停止后，才可用原 provider session 读历史并重新绑定；不能仅凭 PID 消失断言所有后代已退出。

Codex 历史用于补齐可核验的已完成 item/turn；本地缺失的 token delta 标历史缺口而非伪造逐字流。审批 RPC 与旧连接一起失效，旧按钮不可提交，新连接必须拿到新请求；用户之前的意愿作为历史事实，不能重放为任意新批准。原始任务基线和未解决问题不重拍。

空 Codex thread 在首条用户消息之前尚未 materialize，本机 `thread/turns/list` 明确拒绝。新建 UI 会话可只保存本地草稿；第一条消息时才创建 provider thread。空 thread 丢失后可以重建，因为明确未有模型 turn；已有 dispatch unknown 的 turn 不适用此例外。

## 7. 权限与并发冲突

开发授权是独立于 `allow_runs/submit/control` 的能力。不要在 legacy registry 上借用 `allow_runs` 自动放开开发。建议用小型、显式版本化的开发部署配置引用 workspace_id，给服务与项目的 dev_read/dev_start/dev_control、provider/model 白名单和资源上限；不为这项功能迁移业务 registry 格式。HTTP 复用已有 token、Origin/Host、项目身份与请求 schema 校验，默认能力关闭；token 是连接资格，不是某个人的审查签名。

| 面 | 后端约束 | 提示词/审查补充与保证边界 |
| --- | --- | --- |
| cwd | 由已登记 project root 解析；浏览器不可提交自由 cwd，续聊再次核验 | 不从消息里的“切到另一个仓库”自动换项目 |
| 可写路径 | 实际任务范围 ∩ 部署许可 ∩ provider 可执行权限，精确目录/文件，无路径跳转 | DemiForge `check` 另检查原始 baseline；检查发现不等于预防写入 |
| 可读路径 | 必要源码/规范、公开只读平台、可信工具运行环境；不全机开放 | 搜索/读取按页预算；项目数据、Notebook 输出不自动发给模型 |
| 控制文件 | provider home、认证、服务 token、dev journal、Git/平台控制目录不可由 agent 改写 | 来源 AGENTS/config 改动需独立任务流程，不能借本轮削弱约束 |
| 网络 | 模型服务流量与工具网络分开；首版工具网络禁用、外部 MCP/apps/hooks/web 工具禁用 | 有 provider 凭证不代表模型可直连生产 API；仍需后端具体能力支持 |
| Shell | 仅在受限 runner 中执行；使用固定 provider executable/参数，无浏览器 shell endpoint | 不允许自行安装依赖、触碰共享环境/服务；不是以字符串黑名单作沙箱 |
| 检查/验证 | 受信入口、登记 argv、实际授权、独立资源预算；日志绑定 snapshot | 不执行项目声明任意命令；self-review 不伪装 independent |
| 输入与批准 | 请求必须来自当前 epoch/turn/item，CAS 单次回应；超出服务许可不可授权 | UI 允许一次不表示永久、项目全权限或生产运行授权 |
| 凭证 | server 侧 credential reference/隔离 provider home，最小环境；浏览器不传 key，日志不回显 | key 不进 prompt/context/diff；不得继承 shell 中整份环境或个人 plugins |
| 停止 | 只控制本开发执行单元，interrupt 后有限等待/TERM/KILL/排空 | provider 返回取消不代表子进程归属全部清理完成 |

精确范围由无符号链接的路径组件解析；写目标避免跨根硬链接、特殊文件、控制目录及生产位置。首次执行和每次续聊重新核对登记与权限。工具对路径的执行时校验/沙箱必须抵御检查后替换，不能只在 HTTP 收到请求时 `resolve()` 一次。新建文件应授权具体父目录中的必要子范围，不因新文件不存在便放开整个仓库。

凭证仅供 provider 自身访问；其 native shell 使用明确的环境允许列表（Codex `shell_environment_policy.inherit=none` 等已核验设置），工具权限拒绝 provider home、服务 token 与认证文件。启动前审查有效配置层和允许的集成，禁止未知项目配置偷偷加载 MCP、hook 或 plugin；仅把一个空字典覆盖到配置上不作为“所有合并层已清除”的证明。无法得到受约束的有效启动配置则不启动。任务无法通过普通审批改写该配置或批准解除这些边界；新增能力另走明确的部署/项目许可更新。

Codex 0.164.0 的新 permission profile 可以声明 read/write/deny；首选以精确路径配置 profile，禁止混用会覆盖它的 legacy sandbox 设置。`thread/start`/`turn/start` 不再传 `sandbox: workspace-write` 来覆盖 profile；启动结果的 effective permission profile 必须核对。相关能力仍须正反向 fixture 验收，不能从 schema 或配置成功推导执行隔离通过。官方说明 profile 与旧 sandbox 设置不叠加，Linux 需要 bubblewrap/seccomp；工具网络策略也不约束模型服务流量。[OpenAI permissions](https://learn.chatgpt.com/docs/permissions)

首版用同一进程域的写租约保护 root_identity，检查嵌套根/写集合相交，跨 registry 指向同一物理项目也要冲突；读历史不占写租约。服务重启通过持久 lease/fencing token 防旧 worker 继续发布当前状态。原始 dirty tree 不是 agent 成果；用开始时的有界 source snapshot 与当前状态做 diff，同时保留 Git 视角。

外部编辑器不参与锁，严格的源码 CAS 也不能消除“shell 写文件同时另一工具改同文件”的竞态。界面如实说明共享工作区；发现非预期变化停止后续编辑并保留全部内容。对已知多方同时开发或重叠平台工作，选择隔离工作副本/已有 worktree，原始基线必须含授权范围内 dirty 内容和 Notebook 证据，禁止以 `git reset/clean` 创建“干净基线”。副本回应用原内容摘要条件校验，冲突交人工合并；不自动覆盖原根。本功能不做通用 worktree 管理平台。

### 7.1 当前宿主与最小落地路径

已在独立临时 HOME/CODEX_HOME、显式 permission profile 下执行无模型命令 `/usr/bin/true`，Codex 返回 `bwrap: Failed to make / slave: Permission denied`。即使配置握手返回 readOnly，也不能说实际命令沙箱可执行。本轮没有调整 mount/user namespace、安装 bwrap 或改宿主安全配置。

**推荐路径：** 启用一个能运行 Codex 原生受限沙箱的本地执行环境，保持当前注册项目路径和代码版本可核对；先让固定 runner launcher 在该环境完成正反向文件/网络/进程探针，再由现有 console 启动它。优先同机已授权宿主进程环境；若 console 在受限容器，可把同一开发 worker 放到具备所需 namespace 的受限宿主服务，服务端固定 Unix socket/可执行入口、固定工作根、相同 UID 策略和身份校验。首版不实现任意 SSH 主机列表、远程调度或环境自动安装。配置/部署批准由主控结合实际环境执行，当前无需用户处理。

runner 不可用时 `dev_start=false`，历史仍可读。可以先交付界面与 fake-provider 验收，但**不能把它称为已实现可用的代码开发功能**；真实工具型 turn 的端到端验收是上线门槛。无工具的讨论能力只有单独配置和验证后才能标可用，不能偷偷以无工具回复替代开发。

Review 备选是明确的 `trusted_local` 模式：在用户本机可信代码/单用户前提下使用现有 OS 权限、有限进程与资源保护，页面和能力注明“无文件/进程沙箱隔离”。这种模式无法保证 agent 不写同 UID 的其他位置、不能阻止任意代码影响其他进程；任务范围检查只检测已覆盖变更，生产边界仍依赖操作方信任。它不是推荐默认，也不由环境失败自动选择；是否接受这项产品能力交主控架构 Review，不在本轮向用户追加审批或要求。

## 8. 有限资源与生命周期

以下是首版可调默认的建议值，需要小预算故障验收后冻结。覆盖 UI → HTTP → store → provider → tool 子进程；部署上限先于项目/会话值生效，不自动扩额。容量不足返回原因，不新增后台排队系统。

| 增长对象 | 建议默认 / 上限 | 超限策略、实际保证 |
| --- | --- | --- |
| 活动 provider | 全局 2、每物理项目写会话 1；每会话活跃 turn 1 | 启动前原子预留；冲突 409，容量 503；不为历史页启动 provider |
| provider 空闲 | 60 秒无 turn 关闭进程，下一次显式 resume | 持久 history 保留；不维持数百空闲 CLI |
| turn 时间 | 15 分钟；显式部署上限 60 分钟 | watchdog 进入 stopping，停止其执行单元；等待输入仍计总时间 |
| 输入请求 | 每 turn 最多 4 个同时 pending；最长等 10 分钟且不超过 turn 剩余时间 | 到期拒绝/取消并记录，不当作真人同意 |
| 用户输入/HTTP body | 单消息 32 KiB，沿用 HTTP body 64 KiB | 解码前 Content-Length/流式累计限额；附件首版不开放 |
| 上下文 | 初始 128 KiB、受管总量 512 KiB、单来源 1 MiB/原页边界 | 超限明确停止/分页；不把必读静默裁掉 |
| RPC 单帧 | 入出各 1 MiB，JSON 深度 64、结构项 50,000 | 读取/解码前字节限制，严格 JSON；超限失败并停止，避免先 json.loads 超大对象 |
| provider 输出 | 每 turn 16 MiB / 20,000 帧；stderr 256 KiB | 输出到有界段而非整份驻内存；超限取消，尾部诊断标不完整 |
| 内部队列/pending RPC | 每连接 128 事件且 2 MiB；最多 16 pending RPC | 序列化前后副本计入；背压到 pipe，持续阻塞 5 秒失败；控制消息保留槽位 |
| SSE | 全局 2 条、每 session 2 条；每次最多 20 秒/256 KiB/128 事件，写超时 2 秒 | 游标重连；不占用原有昂贵工作 semaphore，保留既有 HTTP 控制容量；不因慢客户端囤 delta |
| UI | 最近 100 条 item、总文本 1 MiB、单 Markdown 64 KiB | 旧消息分页，已有 worker 超时/节点预算，离页释放订阅和 worker |
| 历史读取 | 一页 20 sessions/50 events，最大 response 256 KiB | 大命令输出按 64 KiB 分页；不读取后端全历史再截取 |
| 历史/日志总盘 | 每 session 128 MiB、每项目 512 MiB、全局 2 GiB；另预留 16 MiB 控制事实 | 启动前预留本轮上限，追加前检查；不足拒绝。清理需显式选择已终结记录，不能静默删基线/证据 |
| provider 自有持久存储 | 每会话 256 MiB、含 session DB/日志/缓存和 scratch | 扫描条目/时间有界，平台已知写路径计量；无文件系统 quota 时为采样保护，不声称写前总盘硬限 |
| 后代进程 | provider 树建议 32 进程，禁用多代理/后台任务插件 | 自有 supervisor 子树/收养后代采样；超限停止；没有 cgroup pids.max 时不能声称原子 fork 硬限 |
| RSS | 单活动会话 2 GiB 采样阈值，全局预留 4 GiB；约 200ms 采样 | 多进程 RSS可能共享页重复计数；峰值可能越过采样。cgroup 可用时加 memory.max，非必需的假设 |
| CPU | native 线程默认 2，单进程 RLIMIT_CPU 120 秒，turn wall 15 分钟 | RLIMIT 是单进程累计 CPU，不是整棵树配额；cgroup cpu.max 可用时才增加聚合速率硬限 |
| FD/连接 | worker/provider 每进程 RLIMIT_NOFILE 256；浏览器 SSE 及 RPC 独立限额 | 限制前必须统计 provider 必需 FD；不是整机 FD 上限；模型供应商内部连接并发未公开则标未量化 |
| 单文件 | RLIMIT_FSIZE 64 MiB；验证输出每命令继承现有 1 MiB/超时上限 | 不能等同整个日志目录或 scratch 总空间限制；合法大 Notebook 只能按项目既有预算只读/流式摘要 |
| 模型请求/费用 | 一次用户提交只触发一次 provider turn；并发/时间/上下文有界、记录 token usage | provider 内部重试/工具后模型调用次数未有可强制硬 cap；token 事件是观察，非精确费用。无价格/账户硬额度证据不显示“费用已封顶” |

对有确定费用上限要求的部署，只有供应商账户限额或经过验证的 provider 请求门禁才能开放对应策略；不能用限制 app-server turn 数冒充模型请求总量。无凭证/模型不可用时返回明确状态，不回退其它模型/供应商。usage 缺失显示未知，不补零。

supervisor 必须独立于 HTTP 请求线程存活，在 start_new_session/PDEATHSIG/子进程收养等已有平台机制上完成本执行单元的归属与回收。停止先发 provider interrupt，等待最多 5 秒，再对核验归属的执行单元 TERM，2 秒后 KILL，最后排空输出/子进程、保存结果再释放容量。原生取消成功仅证明协议请求处理；工具启动的脱离进程组子进程仍要覆盖。无法确认后代归属时记录 cleanup unknown、保留租约，不按扫描结果杀别人的进程。需要强后代隔离时采用已验证 PID namespace/cgroup；有限 UID 下的 RLIMIT_NPROC 不当作每任务限额。

输入/输出/日志同时驻留峰值要计入：JSON 原帧、解码对象、规范化事件、SQLite 写缓存、SSE 编码、前端文本和 Markdown 树。每帧有限不代表多队列总量有限。数据库和 provider 私有日志分别计量，不能只给自己日志加配额后称整个服务有界。

## 9. Provider 实际证据与选择

官方 app-server 面向产品内的会话、事件、审批；本设计采用 stdio，不开放 provider WebSocket 给浏览器。兼容性以本机生成的 schema 和行为探针为准，升级要重新核验。[OpenAI app-server](https://learn.chatgpt.com/docs/app-server)

### 9.1 Codex 0.164.0

本机入口 `/yzp/zhaozy/yangzepeng/0905/.codex-prefix/bin/codex`，实际为 `@openai/codex` Node launcher。`codex --version` 为 `codex-cli 0.164.0`。生成标准 JSON schema 320 份，另生成 experimental 版本；标准 bundle 摘要为 `c8ca21ee9e32cd3eec855bd9f69ef76eeff1c894d7592043b6a87e4a23d2eecc`。

| 能力 | 本机核对 | 首版使用/限制 |
| --- | --- | --- |
| 双向 JSONL | `initialize/initialized` 实际成功 | 单读循环、严格匹配 id；不带 `jsonrpc` 头 |
| 持久会话入口 | `thread/start` 的 `ephemeral=false` 与 `thread/read` 实际成功 | 尚未真实模型 turn/跨重启历史验收；不能把空线程当已持久化可恢复对话 |
| 分页历史 | schema 有 `thread/turns/list`、`thread/items/list`、cursor/limit | 空线程调用前者收到“not materialized yet”错误；映射为空草稿，不当成丢失历史 |
| 启动与取消 | schema 有 `turn/start/interrupt/steer`、thread/turn ID | 对无 active turn 的 interrupt 实际拒绝，不能据此声称活跃取消已验收 |
| 输入/审批 | schema 有 command/file/permissions approval、tool requestUserInput | 实际 pending/response/超时行为需 fake fixture 与授权真实小任务验收；不接未知请求 |
| 上下文 | schema 有 developerInstructions/config；thread.start 响应含有效 cwd/approval/sandbox | 不覆盖 baseInstructions；构造实际生效配置、保留 provider 自身基础行为 |
| token | schema 有 thread/tokenUsage/updated | 作为 observed usage；无全链费用硬限承诺 |
| 文件权限 | CLI 接受 permissions-profile；实际运行 bwrap 失败 | 当前宿主不能开放受限工具开发；见第 7.1 节 |

使用 `item/agentMessage/delta`、item started/completed、command output delta、fileChange、turn completed 及 token usage 等本机协议事件，实施时逐项从生成 schema 对齐。`turn/completed` 的 failed/interrupted 不当 completed；server request 必须带当前 thread/turn 归属。开启 experimentalApi 只在明确需要且已测字段存在时进行，不为便利打开所有实验功能。

### 9.2 DeepSeek Harness 的真实入口

PATH 中没有 `deepseek`、`deepseek-cli`、`harness`。实际入口为 `/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/dsh`，包名 `@deepseek-ai/dsh`，版本 `0.2.1-rc.2`，package repository 指向 `deepseek-ai/deepseek-harness`。现有 `demi-forge-harness` 是同一 Forge HTTP 的侧栏/面板插件，不能替代 developer agent 的协议后端。

读取本机安装包 README 和实现确认：

| 通道 | 已有能力 | 明确缺口/结论 |
| --- | --- | --- |
| `dsh --profile headless` | 一次新 session、最终答案后退出 | 不作为持续交互后端 |
| `dsh --profile sdk` | JSON-RPC initialize、prompt、resume、wait、cwd、session.event/status | 无 cancel/session-close；无 server→client 输入审批；messageId 仅表示入队，非 turn 结果；不选作首版对话后端 |
| `dsh --profile acp` | ACP v1；new/list/resume/close、prompt/cancel、update、request_permission | 无 elicitation、session/load、transcript replay、plans/terminal/client filesystem；profile 与权限策略必须审阅 |

ACP 在**独立临时 DSH_HOME、无继承凭证环境**下实际执行 initialize → session/new → session/close，返回 `deepseek-harness-acp`、protocolVersion 1、session close/list/resume 能力；总耗时约 23.2 秒，退出 0。没有发送 prompt，模型调用为 0。默认 profile 报 `deepseek-official/deepseek-v4-flash` 和 effort 选项，只能证明本地配置/目录，不能证明账户可用或该模型已运行。

DeepSeek 后续适配优先 ACP，并关闭 subagent、schedule、goal 自动续跑、任意 MCP 和非任务工具的 profile 能力；不能直接加载用户全量 DSH profile。对话历史由 DemiFlow 持久事件保存，resume 不重放缺失历史，必须标出已知缺口。需要真人补充时，ACP 没有 elicitation：agent 可在最终消息里提出问题，再以普通后续 turn 回答；不伪造暂停中的 tool input。页面明确能力差异，不显示一个不能工作的答题弹窗。

ACP 的取消只覆盖当时活动，后续 child 消息仍可能唤醒任务；因此本产品禁用代理后代/自主日程，并在取消确认后 close 该 session scope，必要时结束自有进程，下次显式 resume。固定 provider 进程只归一个开发 session，不影响其他 Harness 用户会话。permission request 只有 toolCallId 时必须先关联已收到工具卡的具体参数；没有充分操作说明不能泛化为允许。

ACP 协议本身不构成文件/命令沙箱；`authenticate` 立即成功也不代表可以暴露到网络。因此 DeepSeek 可展示“接口已发现、待适配与隔离验收”，默认不可选择发送。没有上述隔离和恢复验证前，不能在最终交付中写“已支持 DeepSeek 开发”。

### 9.3 可检查的探针证据

临时证据根：`/tmp/forge-devchat-design-20261104-jsrvlij4`。本轮所有模型 turn 为 0，没有读取/输出个人认证文件。`probe.py` 在私有临时 HOME/CODEX_HOME/DSH_HOME 启动进程，不使用共享账号；进程均结束。保留初次路径未创建/缺 node 的失败收据，再修正探针后复测；不删除失败来伪装一次成功。

| 文件 | 内容 |
| --- | --- |
| `evidence-manifest.json` | 调查源码/包文档和探针摘要、版本、schema bundle hash |
| `schema/`、`schema-experimental/` | 本机 CLI 直接生成的协议，不是照抄最新版网页 |
| `codex-probe.json`、`codex-events.json` | 实际 initialize/thread/start/read、空历史与无活动取消结果 |
| `dsh-acp-probe.json`、`dsh-acp-events.json` | ACP 握手、空会话创建/关闭 |
| `sandbox-probe.json` | 清空共享配置后的显式 profile 与 bwrap 失败 |
| `probe.py` | 可复核的无模型 JSONL 探针；仅用于本轮，不是产品实现 |

上述证据是协议/部署前提核验，不是工具型开发、计费、取消后代或真实业务验收。最终产品交付需要将必要非秘密收据复制到正式文档资产目录，不能依赖 `/tmp` 永久保留。

## 10. HTTP 与事件契约建议

沿用 `/api/workspaces/{workspace_id}/...` 和 `X-Forge-API-Contract`，通过 capabilities 暴露 `development_chat` 版本/状态/reasons。以下新增 endpoint 均只解析明确字段，禁止客户端提供 pid、provider executable、任意 argv、env、token、root 或 provider thread ID。

| 方法/端点 | 参数 | 返回 |
| --- | --- | --- |
| GET `dev-capabilities` | 无 | 已配置 provider、有效能力/策略摘要与不可用原因；不启动进程 |
| GET `dev-sessions` | cursor, limit | 当前项目的有界会话页 |
| POST `dev-session-create` | request_id, provider_id, task_ref? | 本地 session 草稿；不发模型请求 |
| GET `dev-session` | session_id | 当前状态、task/context 依据、最后 sequence |
| POST `dev-turn-start` | session_id, submission_id, expected_revision, text | 202 + 持久 turn_id/dispatch 状态；同 key 可查回 |
| GET `dev-events` | session_id, after_sequence | 有界 SSE；断流用同一 cursor 续读 |
| GET `dev-output` | session_id, item_id, offset, limit | 保存输出的有界页及完整性标志 |
| POST `dev-input` | session_id, turn_id, pending_request_id, expected_digest, answer | 单次响应收据；重复同答返回原值，矛盾/过期 409 |
| POST `dev-cancel` | session_id, turn_id, request_id | stop receipt；重复不改首次请求时间 |
| POST `dev-recover` | session_id, expected_revision | 显式核对 provider/OS/历史，不自动提交新 prompt |

规范化事件含 `{session_id, turn_id, sequence, provider_epoch, type, observed_at, data}`。类型为 message.delta/completed、item.started/updated/completed、input.requested/resolved、turn.state、context.bound、change.observed、check.reference、usage.observed、connection.state、history.gap。原始 reasoning/private chain 不作为本产品持久/展示内容；只呈 provider 的公开进度与结论。

检查引用只能由后端读取既有受信记录生成。即使 agent 输出恰好是一份 JSON，甚至自己改了某个 review 文件，也必须经过现有检查/证据绑定逻辑验证，显示记录的实际 reviewer 和来源，不能从 message 文本直接生成绿色徽标。

## 11. 关键提示词样稿

下面模板可按实际 manifest 和真实用户要求展开，尖括号均由服务提供。它不替代权限/资源门禁，也不声称让模型必然遵守。

```text
你正在 DemiFlow 中协助用户完成当前项目的一项开发任务。
项目：<name>；稳定身份：<workspace_id>；实际目录：<cwd>。
任务类型：<platform | native_forge | demiforge_registered | unregistered>。
当前任务/绑定：<task_id_or_unbound, binding_digest, source_manifest_id>。
本次目标：<verbatim_user_goal>。
最新真人补充：<verbatim_new_user_requirements>。
允许修改：<exact_authorized_paths>。明确非目标：<non_goals>。
实际工具/文件/网络能力：<effective_backend_policy>。
预算与未具备的保证：<effective_limits_and_limitations>。

首先读取适用于目标目录的 AGENTS 和下列权威来源。遵循任务实际绑定
的版本；不要把最新候选标准套给旧记录，不另建平行规范。来源缺失、
互相冲突或范围不明时说明具体缺口；不要自行推测业务输入或授权。
<authoritative_source_manifest_with_read_pages>

复杂开发先给出可实施设计：问题、模块职责、接口/数据流、失败恢复、
资源增长及预算、兼容性和验证方式；按当前项目要求完成架构 Review
后再实施。简单且已授权的改动直接完成，不为惯例增加批准步骤。
保持架构干净、边界清晰、实现精简，检查关键不变量与边界行为。
代码、内存、磁盘、CPU、FD、连接、进程、请求和时间都有有限预算。
使用平台现有公共抽象，不写第二套模型循环/业务调度器绕过平台缺口。

编辑前检查当前 dirty 差异与任务历史。保留他人改动、Notebook 输出、
原始任务基线与历史证据；不 reset/clean，不为通过检查缩小 watch、
放宽契约或重拍基线。不能把其他工具同步写入的变化认作自己的成果。
开发许可不等于生产执行、真实模型评测、网络或共享环境修改许可。
遇到超出能力的操作，报告缺口；不得尝试 full-access 或其他绕过。

验证使用实际适用的既有入口，记录命令、范围、结果与未验证项。
源码或依据变化后旧验证过期。你的自述和工具 stdout 不授予检查、
Review、Freeze 或验收通过；self-review 必须标明自身审查。
最终说明实际改动、证据、剩余限制和下一步，不虚构已完成动作。

你只负责这个项目任务。没有本次明确要求时，不建立子代理团队、
定时任务、联系其他聊天或接管其他任务；不要承担主控的会话管理职责。
```

DemiForge 分支追加而非复制标准正文：

```text
本任务使用已登记 DemiForge 流程。平台依赖 <readonly_platform_paths>
保持只读。先 list/history 核对原任务；绑定有效时读取 context 及每一页
必读来源；新任务在编辑前绑定实际 goal/write/non-goals。
按 <canonical_skill_path@sha256> 执行 note/refresh/check/verify/review/handoff。
--allow-tests 仅确认已登记可信测试，不授予生产/模型/网络权限。
只读历史 accepted 不代表当前通过；来源/工具/配置变化按原规则处理。
请逐项实际审查 DF-01..DF-07，不自动填 pass，不把自身审查称为独立 Review。
```

续聊头只补当前事实和新要求，不把整份旧聊天重拼为 system prompt：

```text
这是 session <id> 的新 turn，前一 turn <status>，停止/清理事实 <receipt>。
原任务基线 <digest> 保持不变。当前变更/外部冲突 <summary_and_refs>。
仍未解决的问题 <original_notes>。当前证据 <current/stale/missing>。
下列真人要求更新了原任务中的 <specific_scope>：<verbatim_text>。
先核对有变化的权威来源；不要自动重试前次效果未知的命令。
```

## 12. 分期与验收

**D0（本文）**：接口/规范调查、无模型协议与沙箱探针、可实施设计，主控 Review。本文没有形成产品代码完成状态。

**D1（Codex 完整闭环）**：接运行态最终基线；最小 store/provider/HTTP/UI、权限准入、有界事件、输入审批、取消恢复；先 fake provider 和无模型 fixtures，再在已允许的隔离环境中进行极小真实开发 turn。没有可用 sandbox/runner 时整体验收保留未通过，不能只交静态对话框宣称完成。原有算子 transport 抽取只做必要共用并验证兼容。发布前组合回归当前项目 tabs、Markdown、运行态控制与现有公共 API。

**D2（DeepSeek ACP）**：固定 dsh 版本/最小 profile、实际 OS 沙箱、事件映射、权限、取消/close/resume 和本地历史缺口语义；沿同一服务接口开放。实现与 UI 明确没有 elicitation、无 provider 历史 replay。SDK 不作为备用偷偷切换。后续 provider 按同一能力门槛接入，不扩展成通用 agent 市场。

| 验收场景 | 必须证明 | 手段/证据 |
| --- | --- | --- |
| 项目绑定/导航 | 左项目右 tabs、开发在总览前、每项目记忆、深链接/后退、根替换拒绝 | 真实浏览器多项目 fixture；320/390/1440px、键盘与无项目/不可用项目 |
| 上下文适用 | platform/native/registered/unregistered 四类不混规；实际版本、最新真人要求和来源齐全 | 固定 manifest fixture + public CLI integration；必读分页/超限/冲突 |
| dirty/Notebook | 开发前脏修改和完整输出/历史保留；外部变更使证据过期，不自动重拍 | 小文件与大输出 hash fixture，写 scope/readonly/watch 正反例 |
| 输入/审批 | 当前请求可答，其他项目/旧 epoch/重复矛盾/过期不能答；拒绝不扩权 | fake provider 乱序和并发请求；实际 provider 小型请求链另验 |
| 幂等发送 | 双击/重试只一个 turn；key 改正文拒绝；发送后断线未知不自动重试 | dispatch 前后逐点故障注入，持久记录/调用次数 |
| 实时展示 | delta、tool、diff、命令输出、终态有序；Markdown 安全，慢消费者有界 | 小预算 flood、chunk UTF-8、XSS/链接/深嵌套、切页释放 |
| 取消归属 | 只停目标 turn；停止等待工具与后代；其他开发/业务作业继续 | 两会话+独立 sentinel 进程，正常/TERM/KILL/脱组/父退出故障 |
| 断线/重启 | 浏览器不重发；worker/HTTP 死亡不接管他人；不丢既有完成事实/不伪造历史 | SSE 游标重连、provider EOF、中途崩溃与 PID 身份不符 |
| 文件/网络隔离 | 允许的写可用、范围外写/敏感读/平台写/控制目录写/工具联网失败 | 无模型沙箱正反探针 + provider 工具 fixture；当前 bwrap 失败须消除 |
| 资源 | 输入/帧/队列/DOM/历史/日志/磁盘拒绝与控制保留；CPU/RSS机制准确标注 | 可控小预算：超大帧、无限输出、慢盘、满盘、并发叠加、fork/FD、超时 |
| 费用事实 | 无模型探针为 0；真实任务有 token 观测；不声称内部请求/费用硬 cap | 已授权最小真实 turn 收据，provider 未公开项保留 unknown |
| 检查与 Review | agent 自述无法造绿；旧证据 stale；self 与 independent 分开 | 篡改卡片/stdout、过期摘要、缺日志/版本冲突 fixture |
| Codex 兼容 | 固定 schema、空线程、历史分页、input/approval、失败/中断语义 | 本机版本 transcript fixture + 小型真实端到端；operator 原回归 |
| DeepSeek 能力 | ACP 与 SDK 不混淆；无 elicitation/replay 明确；无隔离不启用 | 固定 package hash/握手；D2 独立端到端，不以 D0 空会话算通过 |
| 现有功能 | 运行管理、项目检查/数据/Case、折叠导航、只读原项目不回退 | 接运行态最终候选的组合测试、正式只读核验；来源清单/最终差异 |

## 13. 主控 Review 需要决定的事项

普通实现选择已在本文给出建议：Codex 优先、DeepSeek ACP 后续、首次默认总览、每项目单 writer、复用现有 Markdown 和检查、不默认启用网络/插件、不自动迁移历史。

剩余一个架构取舍是**受限 runner 的部署路径与是否另外接受可信本地模式**。建议先选能通过 Codex 原生文件权限探针的宿主环境，延续上述默认隔离；trusted_local 只有明确标出残余能力边界并获架构认可才实施，不能借其省掉沙箱验收。cgroup 为增强项，缺失时仍实现确定的字节/队列/RLIMIT/超时控制并诚实标采样限制。此项由主控结合当前服务宿主决定，不要求用户此刻安装环境或额外审批。

通过设计后仍由本任务负责人端到端实施、验证和交付；不拆出新的细任务或联系其他会话。
