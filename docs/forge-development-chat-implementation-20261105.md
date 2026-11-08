# DemiFlow 开发对话隔离实施记录

本记录保留原实施阶段与 App 接续证据。**当前真实 DeepSeek 最小开发已通过独立复核；最终运行态组合与总控终审尚未完成、未发布。** 最新事实见末尾「App 接续」节。实现沿 2026-11-04 设计及 2026-11-05 架构 Review，不创建第二个实施负责人，不修改正式运行源码、共享环境、registry/token 或现有服务。

## 隔离位置与基线

- 实现仓：`/tmp/forge-devchat-impl-20261104-_xgy1rah/demiflow`。
- 原始文件摘要、Git HEAD 与正式仓状态：上级目录 `baseline.json`、`formal-status.txt`。复制包含当时 dirty/untracked 内容，没有执行 reset、clean 或改动原 index；原仓 Notebook 未写入。
- 已审 Web 来源为正式仓，`app.js` 原摘要 `befc996700c4e1ea571ade2d8c587013eaadab3f05918bfb469405801528f96c`。`style.css` 没有修改，开发样式独立为 `development.css`。
- `execution/process_observation.py` 是从当前运行态候选只读复制的原生低层依赖，未改动该候选文件。最终合并应使用总控批准的对应版本，不能拿这份副本覆盖新运行态实现。
- 2026-11-05 00:40 +08 总控 `development_chat_integration_gate.json` 明确：运行态 v2 的 source manifest 为 `fea38e0240f87768557be25a705b48d92b8e37f9850c48387b9e2d8ac48d51b6`，`approved_for_final_integration=false`。本批不是最终运行态组合。

## 实现范围

`development_chat` 与既有 `development` 分离。会话由 HTTP 控制层登记，provider 自己管理模型/工具循环，不调用 Dataset，不复用 Run 状态或取消入口。开发开关独立于 `allow_runs`，显式 `--development-config` 才初始化数据库。GET 历史或打开页面不启动 provider。

新增「开发」tab 在总览前；首次进入仍到总览，沿用 v1/v2 深链接及每项目记忆。新会话是本地草稿。消息使用原 Markdown reader，流式文本不注入 HTML。草稿按项目/会话保存，32 KiB/份、最多 64 份；时间线最多 100 个 item / 1 MiB 文本，单条预览 64 KiB，原文与事件按游标分页。离页取消 reader 与轮询；刷新只读持久事件。运行中禁用重叠发送，停止先显示清理中。

HTTP 复用既有 Host/Origin/token、项目解析、64 KiB body 和 10 个请求容量。事件采用 **最多 1.5 秒的有界 JSON 长轮询**，不用 SSE 常驻连接；全服务最多两个开发事件读取，不占既有昂贵工作 semaphore。连接满载验收仅证明两个开发长轮询满载时停止可达，不能据此声称全部 10 个 HTTP 请求被任意慢客户端占满仍可达。

会话绑定物理根的 path/device/inode、provider、部署策略摘要。SQLite 事务先保存 submission、容量预留、epoch、所有权记录，再创建独立 subreaper。幂等键同内容返回原 turn，不同内容拒绝；同根/嵌套根/重复 registry 根不能有第二个本服务 writer。**跨不同开发 state root 的服务不共享锁**，部署必须使这些客户端使用同一状态域；外部编辑器也不参与此锁。

每轮 provider 都在独立进程启动，终态后关闭，下轮显式 resume；没有空闲常驻 provider。发送前保存 `dispatch=unknown`，结果未知不重试付费请求。服务重启不自动重发；缺失后代清理证明时保留租约和预留。`recover` 核对本地记录与进程身份，不凭 PID 不在即宣布后代已清理。当前没有自动修复 cleanup unknown 的管理入口。

## 规范与证据

- 平台任务：适用 AGENTS、部署明确指定的来源、当前真人要求与精确写范围。
- 原生作者任务：上述来源加 workspace.json；开发会话不授予原生检查/Review/Freeze 通过。
- 已登记 DemiForge：只通过 canonical Skill 的公开 stdlib launcher 执行 list/history/context/start；保留实际旧任务标准和 dirty baseline。任务无记录且已有 implementation 授权时才 start；不从业务 cwd 导入工具或执行业务可改写的薄脚本。
- 未登记项目：可展示实际来源与开发范围，但证据始终显示缺失，不冒认认证。

每轮保存原始 requirement、来源路径、摘要、页码、选择范围与来源信息。先 `prepared`，实际发送后 `sent`，Codex thread/start 或 resume 确认开发指令后才 `acknowledged` 并开始模型 turn；发送窗口失败为 unknown。CLI 已写读取回执不等于本轮送达。初始必读正文最多 128 KiB / 32 页，超限明确失败，不截掉规则；补充上下文总量 512 KiB。

薄工具桥仅执行枚举 public CLI 操作。refresh 后读取新页，provider 必须通过 `ack_context` 明确回应该页摘要，才恢复依赖上下文的检查；这证明回应对应内容，不证明模型理解。检查卡片只由后端 CLI 返回生成；self-review 固定记录为 `development-chat:self`，没有 independent reviewer 或 handoff --accept 工具。文本中的“通过”不转换成门禁。

全字节快照包含 Notebook 输出摘要；小文本另外保存有限预览，提供统一 diff。每文件预览 16 KiB / 512 行，全任务预览 128 KiB；大文本/Notebook 不自动送模型或截断成伪完整 diff。变化超出 watch 内授权写范围时终态失败，保留所有内容。范围检查是事后发现，不能防止 trusted_local 下的越界效果，也不宣称变化由 agent 独自创作。

## Provider 实际状态

Codex 固定 0.164.0。使用 app-server JSONL、持久 thread、turn/start/interrupt、当前输入/审批、文件/命令事件和 token 观测；审批仅当前 epoch/turn/request 的一次答复。配置使用私有 HOME/CODEX_HOME、关闭 MCP/apps/hooks/多代理/远程 plugin/search、shell 环境允许列表；拒绝项目 `.codex/config.toml`，核对有效配置与 cwd/model/sandbox。只抽取 JSON 编码与严格解码到中性 `agent_transport.py`，原行算子的模型循环和语义保持不变。

DeepSeek 固定 dsh 0.2.1-rc.2。已有 ACP 适配器、私有固定 profile、取消/close、事件与单次权限映射及公开 CLI 的 MCP 桥。禁用后代代理、目标自动续跑、外部检索、配置改写和账户集成；scope 为 trusted_local，不是沙箱。真实无模型 initialize/new/close 已通过，修正了 full-access + ask 与权限预设不一致的组合。没有 elicitation / provider transcript replay；resume 明示历史缺口。ACP 的 prompt 是请求区间，不伪造原生 turn ID；第一条相应活动或最终响应才确认 prompt 送达。**DeepSeek 对外发送仍关闭，尚未完成真实任务与完整生命周期验收。**

本批真实模型请求数为 **0**。总控明确当前无已授权 OPENAI/DEEPSEEK 环境凭证引用，不读取个人认证文件或私人历史。真实模型小任务门槛仍待授权引用；最多 3 次应用 turn、每次 5 分钟的既有上限保留，不通过假 provider 结果冒充真实可用。

## 资源与停止保证

默认两个活动 provider；每物理项目一个 writer、每会话一个 turn。每会话 128 turns、服务 256 sessions；每 turn 20,000 stdout 帧、16 MiB stdout、256 KiB stderr；JSON 帧 1 MiB，解码前限制深度 64 / 50,000 结构项。通知队列 128 项/2 MiB、RPC 最多 16 个且已返回未消费响应合计 2 MiB，写背压 5 秒超时。

SQLite 使用 DELETE journal、FULL 同步、1 MiB cache、max_page_count；控制文件和 rollback 分开核算。默认数据库预算 768 MiB，回滚预留另 768 MiB；控制 reserve 16 MiB 由 fallocate 实际占位，SQLITE_FULL 消耗 reserve 后禁止新提交并保留维护标记。数据库无法提交停止时，关闭本 turn 的父存活 pipe 仍触发自有 supervisor 清理，HTTP 不谎称已持久取消成功。

provider home 每会话 256 MiB 采样阈值、全服务持久 home 512 MiB 准入，计入已完成会话，不能只核算活动会话；history 另有 64/256/512 MiB 的 session/project/global 配额。没有自动清理历史功能，额度用尽需明确维护。服务扫描条目/耗时有界，磁盘设备与配置分区一致，启动前检查可用空间。目录采样不能防住两次扫描间第三方写入瞬时超额；RLIMIT_FSIZE 只约束单文件。

每活动单元 RSS 采样阈值 2 GiB；另外为控制进程/载荷副本预留固定预算。默认开发内存分区为 `2*(2 GiB+64 MiB)+64 MiB`，开发磁盘分区为 `2*768 MiB+512 MiB+16 MiB`。部署必须另列 run/other 分区与总额；启用业务运行时 run 分区不能是零。这是显式预留账目，不自动识别全部宿主负载，更不是 cgroup 硬限制。

原生低层 descendants/pidfd 机制只遍历专属 supervisor 后代；subreaper 收养脱组子进程，父存活 pipe 检测 Web 消失。正常取消先 interrupt，最多 5 秒后 TERM、2 秒后 KILL；完成需要排空后代后才释放 DB 预留。provider launcher 有 PDEATHSIG、每进程 CPU 120 秒、FD 256、单文件 64 MiB。RSS/进程数/总私有盘均是采样保护，无 cgroup 聚合 CPU/内存/pids 硬限制，provider 内部重试、请求数、token 费用没有硬封顶。

## 验证与剩余集成

过程证据位于隔离根的 `evidence/`：fake provider、context/public CLI、HTTP 负载、真实浏览器、Codex/DSH 无模型握手。最终同源码摘要的验收结果由交付 manifest 绑定；早期失败也保留。旧 Codex 算子测试大部分仍以 sqlite_journal 调用已拒绝 journal 的 Dataset 入口，不能作为当前 transport 行为回归；新增对共享编码/解码和原预算异常语义的直接验证。一次 Web 回归在源码仍变化时触发正确 stale 拒绝，该批证据作废，最终批必须固定源码后重跑。

总控最终 Review 前仍需要：授权凭证下的真实最小开发与取消/恢复；DeepSeek 实际工具与 public CLI 桥端到端；受审运行态的公共文件集成；组合源码冻结、受影响旧功能回归。默认 restricted 模式启动仍关闭，本机 bwrap 失败未改变，不调整宿主设置来绕开。

## App 接续：2026-11-05 01:32 +08 当前状态

本节更新上文的中间状态；上文无模型请求为 0、DeepSeek 未开放发送及凭证待配置等是旧阶段事实。当前 App 负责人为 `01a1a788-53b4-7160-9d32-2d74743ce86c`，继续同一工作副本；没有新增工作仓、后台编码负责人或 subagent。正式源码、现有服务及真实业务没有被本任务修改。**独立功能候选已实现；最终运行态组合及总控独立终审仍未完成。**

### 真实 DeepSeek 与认证边界

使用用户指定 `/yzp/zhaozy/yangzepeng/0905/.dsh/dsh` 对应的已安装程序与 Node 24.10.0。该启动器硬编码共享 DSH_HOME，因此适配器调用同一安装的 `node/bin/dsh`（解析到 `lib/bin.js`），通过私有固定 profile 保持本应用历史和临时文件独立。平台仅检查 `native_credentials` 引用的路径、权限和大小；由 CLI 原生 credentials/account 组件读取与使用认证。未读取或输出认证正文，未复制私人历史。`inherit_proxy=true` 仅显式传递 HTTP(S)_PROXY/NO_PROXY 引用及 Node 原生代理开关；不继承任意环境变量、不把代理值写入部署或数据库。

四次应用启动完整保留在 `evidence/deepseek-real/attempts.json`：

1. `deepseek-official` 无 API key，CLI 在推理前拒绝；没有源文件变更。
2. 现有 `deepseek-account` 路由在缺少代理的隔离环境中连接失败；无认证 Node 探针证实 DNS `EAI_AGAIN`，显式代理收到 HTTP 401。
3. 显式代理后，账户目录的 `console.info` 输出污染 ACP stdout；`dispatch=not_sent`，没有 prompt 发送。保持严格 JSON，未吞掉无效协议行。
4. 总控 Review 第 7 节追加一次、五分钟内的启动。自有 `deepseek_stdio.cjs` 将 CLI console 诊断转到已经有界的 stderr，未修改安装。此轮在 **28.36 秒**完成：修复 `stable_unique` 的首次出现顺序，补充空输入/负数/重复边界，实际调用 `python3 -B -m unittest -v`，四项通过；负责人独立重跑仍四项通过。

第 4 轮的两个变更均处于授权路径。原 dirty 注释保留；AGENTS 与 Notebook（含输出）全字节摘要未变；认证文件 inode/大小/mtime 未变。`delivery=acknowledged`、`dispatch=acknowledged`、`cleanup=complete`，独立确认 supervisor 已退出。采样峰值 328,085,504 bytes RSS、331,800 bytes 私有目录、3 个进程；这些是观察值，不是 cgroup 保证。ACP `usage_update` 的 used=3535..6810 是上下文使用量，不能当作实际计费 token、内部请求总数或费用硬上限。

真实任务及独立复核：`evidence/deepseek-real/receipt-4.json`、`events-4.json`、`independent-review.json`、`independent-unittest.log`。随后使用**仅本验收自己创建的 provider session**进行了无模型 initialize/resume/idle cancel/close，恢复到同一 session，正常关闭；见 `resume-probe.json`。仍不支持 provider transcript replay 或 elicitation，不伪装成已支持。活动轮次取消、权限问答和失败路径由实际 worker、进程树与协议 fixture 验证；没有额外付费轮次。

完成这些条件后只开放显式 trusted_local 部署下已配置的 DeepSeek 发送能力。默认部署仍关闭；restricted 仍拒绝启动。Codex 固定版本的协议、输入/审批、取消及真实无模型握手有证据，**本轮没有 Codex 真实推理验收**。不把 DeepSeek 的真实结果移作 Codex 的模型可用性证明。

### 接续修正与资源复核

- ACP `close` 失败也必须终止、排空自有后代。关闭控制数据库写入失败时，service 使用该 turn 的父存活 pipe，保留失败事实，不放弃清理。
- 差异观察/输出配额失败与后代清理分别记录；已经证实进程清理的事实不会仅因差异卡片写入失败而丢失。无法持久化的情况仍保留 unresolved 状态。
- 会话历史记账新增正文元数据：原始 requirement、context、完整基线、ownership、result 以及待答请求与实际答复均计入。原预发布历史在一个 SQLite 事务中补计，不改内容。每 turn 预留事件预算、两份最多 `2*context_total` 的上下文/基线、32 份请求/答复、32 份控制事件以及固定小字段；实际 SQLite 页面及 rollback 仍另有物理预算。
- metadata 的 context/baseline 各最多 1 MiB，process/result 各最多 16 KiB；provider 常驻审批信息每项最多 16 KiB、最多 128 项，不保留所有工具输出副本。SQLite 页面尺寸固定 4096，已有数据库大于新预算时拒绝，最小预算 128 KiB。
- 增加真实 SQLITE_FULL 小额度 fixture：控制 reserve 消耗后标维护压力，已提交 requirement 保留，未提交 context 回滚，停止控制仍可写入。另测保留 metadata 挤满下一 turn 的准入、控制取消写入失败后的进程回收。
- UI 展示 ACP 当前工具及具体参数；终态将助手文本交给安全 Markdown reader；显示恢复历史缺口和失败原因。差异/工具/用户正文均有真实事件分页原文。离页清理对话框及 reader，已在浏览器确认实际变更与原文一致。

本版同时保留原有四类上下文、既有公开开发 CLI、幂等发送、跨项目/重叠根单 writer、未知状态保留租约、父退出及脱组后代清理、慢事件读满载停止、320/390/1440px 等验证。没有通过放宽 sqlite_journal、平台契约或旧守卫求绿。

### 快照与验证状态

真实验收源码 manifest 为 `evidence/independent-freeze/source.json`，摘要 `3e972f91af2dce29ab812ef9c887b39a93853967087f7c967c8930ae82d9d991`。真实验收完成后仅 `policy.py` 的 provider available 投影从待资格变为可发送；launch、worker、transport、context、store 均未变。此资格开关版本另冻结在 `evidence/qualified-freeze/`，源码摘要 `662f8752206ac1ffb785dffd4b7f4bf028d3d68b474ed31f6b930c4a02215664`。不把两者称为同一快照，后者回归单独留证。

过程验证包括 `app-boundary.xml`（24 通过）、`app-browser-acp.xml`（ACP 审批/差异/Markdown/弹窗 1 通过）。第一次固定快照批次为 37 通过、33 浏览器跳过，原因是旧浏览器 fixture 需要 FORGE_CHROMIUM；没有把跳过视为验收。已显式指定既有 Chromium 重跑受影响回归，结果以 `qualified-final.xml/log` 为准。旧失败与异步等待修正的过程记录均保留。

本副本 wheel 构建成功，并核对其中新增 Python、Web、CJS 资源与源码逐字节一致；未安装到共享环境。原 wheel 对应资格开关前快照，后续最终组合需重建。共享文件的准确原基线已保存到 `evidence/integration-base/`。

### 剩余最终组合

运行态 v3 当前来源为 `/tmp/forge-runtime-20261104-0oi_zj87/combined-demiflow`，commit `3b41560460d2e3d16f6dd598deda0734582b8478`，源码 manifest `4fd4e69bb63e1ecf3e27ac78a5be6749b09c748302aad0fdd71130d6ee2daaef`；**这只是只读候选，尚无总控最终放行**。本任务未覆盖它。`runtime-v3-merge-preview` 仅是共享文件三方合并预演；console.py 与 workspace_view.py 有需要手动保留双方语义的冲突，其余共享文件自动合并预演无冲突。低层 process_observation 与该候选摘要一致。

最终组合须保留运行态 control_only、单准备 worker、控制 worker/waiter 和 snapshot cache，并并入开发专属 readers、Service 及关闭清理。主机账目须将运行态 run_domain 的实际部署上界与开发固定分区核对，不能把“run 分区非零”当成双方合计已被验证。运行态自身的历史/多设备/其他参与者口径以最终批准契约为准。完成明确版本组合后再冻结、复核受影响运行/项目功能与包装，交总控独立终审；不以当前独立候选宣称最终集成或发布完成。

## 独立候选交付：2026-11-05 01:50 +08

本节是上述过程记录之后的最新状态。真实 DeepSeek 最小开发已通过；独立候选已完成本轮实现与验证。总控尚未批准精确 runtime 版本，最终组合、组合回归及总控独立终审仍待完成。第四次真实应用启动已用完追加额度，本轮未再执行模型任务。

最后一批修正包括：关闭时最终数据库读取失败仍释放自有进程句柄和服务锁，同时向调用者报告失败；初始化及 HTTP 绑定失败释放已获得的服务所有权；重复 close 安全返回。私有磁盘记账取逻辑大小与分配块大小的较大值，并计入目录。策略快照最多 32 份，增加独立 4 MiB 控制目录预留；因此最终默认开发磁盘分区为 `2*768 MiB+512 MiB+16 MiB+4 MiB`。草稿仅保存有界数据库元数据，在 turn 获准后才创建私有目录，避免空草稿目录挤占两个活动会话的既定额度。没有增大 provider 或全服务私有磁盘额度。首次发送重载会话列表时重置分页游标，消除重复草稿行。

真实第 4 次验收的 331,800 bytes 私有目录峰值来自当时的逻辑大小采样，不能作为最终分配块口径的实测峰值；后续口径修正有小文件/目录分配块 fixture，未追加付费任务。

验证记录按各自快照区分：

| 证据 | 结果 | 适用范围 |
| --- | --- | --- |
| `qualified-final.xml/log` | 70 通过，0 跳过，404.37 秒 | 资格开关版本的开发、旧 Web、Markdown、项目浏览/移除及 console 广泛回归；早于最终关闭/磁盘/UI 收尾 |
| `mcp-bridge.xml` | 1 通过 | 实际 MCP 子进程调用已登记项目公开 CLI；刷新后逐页 read/ack，禁止伪造独立 Review/accept |
| `independent-final-v3/acceptance.xml/log` | 28 通过、1 失败 | 分配块记账发现草稿目录占用问题，原失败保留 |
| `independent-final-v4/acceptance.xml/log` | **29 通过，0 跳过，39.89 秒** | 修正后的四个开发对话测试文件，含实际 HTTP/浏览器、MCP、生命周期及资源故障 |

最终源码清单 `independent-final-v4/source.json` 的 SHA-256 为 `97368fce1e2f847967336c018fd7149ac2a559ea722721208142841a8ea5fc22`；它覆盖实现、包配置和 AGENTS，不含测试与说明文档。交付另记录测试摘要、变更文件摘要、包内容与复核结果；不把不同批次拼成一次同快照全量通过。最终 wheel 重建且逐字节核对变更的 Python、Web、CJS 包资源，未安装到共享环境。

浏览器截图现在保存到批次独立目录 `independent-final-v4/screenshots/`，桌面截图确认列表不再重复。原测试曾把截图写到固定 `evidence/screenshots/`，早期重跑覆盖了该处截图；不能声称最早截图不可变。原日志、XML 和 fixture 仍保留。截图中项目顶部的短暂「同步异常」已用 `browser-sync-probe.log` 核查为项目快照变动期间的旧状态，随后自动恢复；这不作为开发会话失败或网络始终正常的证据。

可审阅交付为 `evidence/independent-delivery/` 下的 patch、变更文件归档与 `files.json`，最终证据索引为 `independent-final-v4/delivery.json`。归档只含本任务代码、测试和实施文档，不含认证、私有 provider 历史或验收状态库；`execution/process_observation.py` 仅列为 runtime 依赖摘要，不由本补丁覆盖。

启用入口仍为 `python -m demiflow forge console --registry <registry.json> serve --development-config <deployment.json>`。配置显式登记项目、phase、来源、写入/watch 范围、授权说明、固定 provider 版本、私有 state root 和宿主 run/chat/other 分区。默认未配置时不创建开发状态、不启动代理。`trusted_local` 的权限与采样限制、DeepSeek 无 transcript replay/elicitation、Codex 未做真实推理验收等边界仍如上所述；本独立交付不改变正式服务配置。


## 总控 DC1/DC2/DC3 返修交付：2026-11-05 02:20 +08

总控第 8 节独立复现的三个生命周期问题均已在原副本修正。本节替代 v4「独立候选就绪」结论：**v5 已通过负责人复核，待总控重新审阅；尚未做最终 runtime 组合或发布。** 原 v4 源码清单、交付包、失败结果及探针保留，真实模型额度仍为四次已用尽，没有追加模型轮次。

### 启动与恢复

`Store.submit` 在登记同一 turn/epoch 及资源预留的事务中记录 `launch_phase=admitted`。这明确表示尚未调用创建进程；目录、unit、归属记录、pipe 等准备失败均有统一 fd 清理，可收口为失败。若当时数据库不能保存收口，后续恢复仍根据这份持久阶段证明处理，保留原 submission、turn、epoch，不重发原任务。旧数据库缺少该字段的记录迁移为 `unknown`，不能因 process=null 获得未启动证明。

调用 Popen 前持久化 `spawning`，unit 的路径、设备/inode 和 epoch 已保存，文件与父目录已同步。构造返回丢失不再等同于没有子进程；父端存活 pipe 关闭后，可能存在的 worker 自行进入停止清理，预留继续保留。复用原生 `unit_stopped`：仅在 worker 尚未 claim、同路径/设备/inode/nonce 的 unit 已确认无人持有时，才能证明没有 worker 可继续启动并收口。文件缺失或被替换不算证明。

worker 在任何工具执行前，事务校验 epoch 与继承 fd 的设备/inode，并记录 `claimed` 和 supervisor 身份。已 claim 的轮次仍需实际后代 drain 回执；即使 PID 消失或 unit 解锁，也不会推断任意工具后代已清理。此类丢失回执的记录继续保留 unresolved，未新增强制释放或新调度器。重启只把尚未确认的发送窗口转为 unknown，保留已提交 acknowledgement；启动清单与 worker 收口并发时，事务重读防止旧清单覆盖新的 cleanup=complete。

### 停止边界

服务在创建 worker 前保存固定的 workspace/session/turn/epoch/物理根关联。取消先验证这个关联；错误项目、会话、轮次和一般业务拒绝均不关闭存活 pipe。SQLite 连接、读写、事务等失败显式归类为 `StorageUnavailable`；只有已确认的自有目标遇到该类持久化故障，才使用独立 pipe 停止，并向请求方保留失败结果。数据库完全不可读时，同样能根据已保存关联停止正确目标；并行另一项目保持运行。服务退出的自有 worker 清理能力仍保留。

### 固定快照验收与交付

- v5 源码清单：`independent-final-v5/source.json`，1037 文件，SHA-256 `96234e0053265f6190049f5835f8fc03641288b95421057e320a552864a356ae`。
- 同时冻结 8 份测试/fixture，清单 SHA-256 `ba962b698de544b271d80f8a4a133ce3652b31aca659a40cabe617b28d1c53c9`。相对 v4 只有 service/store/worker 三个实现文件改变。
- 五个开发对话测试文件 **46 通过、0 跳过，53.04 秒**；含原生命周期、真实 HTTP/浏览器、资源压力、取消/恢复和 17 项新增故障检查。测试后核对源码及测试均未变化。
- 原故障脚本逐字节保留。原样重放在 DC1「第二轮原本会获准」的位置被正确拒绝而退出；为继续检查全部三个场景，另保存只调整结果捕获与修复断言的 replay 脚本及差异。固定候选重放确认：真实被暂停的 worker 仍存活时预留未释放；unit 分配失败后恢复 clean 且新提交可获准；错误会话停止后原任务仍 running。
- 新故障用例额外覆盖 worker 已启动真实 fake 工具和脱组后代后才丢失 Popen 返回，重启期间仍拒绝重复 writer，恢复运行并 drain 后才放开；前后 /proc/self/fd 集合核对覆盖启动失败 fd 清理。也覆盖归属保存前/后失败、unit 替换、旧数据库迁移、数据库不可读停止及无关并行任务保护。
- 两批过程失败也保留：旧 unknown 测试改为明确模拟旧记录；新增活跃 worker 重启检查暴露已确认发送状态被错误降为 unknown，已据此修正。没有删去失败用例或放宽已启动轮次的清理要求。
- 新 wheel 单独重建，变更/依赖的 26 个 Python/Web/CJS 条目与冻结源码逐字节核对；未安装共享环境。新交付放在 `evidence/independent-delivery-v5/`，不覆盖 v4 的 `independent-delivery/`；旧早期 screenshot 复用限制仍按上节说明。

最终索引：`evidence/independent-final-v5/delivery.json`。原失败及重放证据：`evidence/lifecycle-review-repair/`。当前独立候选仍未获得精确 runtime 公共文件基线的最终批准；下一步仍由同一负责人按批准版本进行语义合并、实际 run/chat 宿主预算核对和组合冻结验收，再交总控独立终审。

## 最终组合候选交付：2026-11-05 03:05 +08

本节为最新状态，替代上文的待组合描述。总控已关闭 DC1–DC3，并批准运行态 commit `4cf7e3e5ccc68ed554fe5d39966d8301762a506b`、源码清单摘要 `851fc7fedf6a5e8e35ccb337331b9f35c0254a752c6ba00601b27a1cd5684ad9`。负责人已在原始唯一隔离副本内完成组合与验收；**当前为待总控独立终审的组合候选，没有由本任务改写正式源码或重启正式服务。** 运行态单独发布的进展以总控记录为准，不据此推定开发开关已发布。

### 组合与预算

逐文件核对批准输入后，51 个本任务未修改的文件按运行态精确版本并入。console.py 和 workspace_view.py 冲突按双方语义处理，app.js 三方合并；保留运行态 control_only、常规 worker 1、控制 worker 1、有界 waiter、快照缓存、权限交集及开发独立 reader/服务关闭语义。原生执行仍复用现有运行域和 Dataset；process_observation.py 保持批准版本 `8ba7118078a1d9a39f67e28d94424784d8199c2f3adae47ab768e04946ba167e`。

新增 host_partition 只解析一个固定部署预算，不建立第二套调度、队列或用量数据库。run_domain 的 policy/reserve/history_admission 与开发发送共同核对实际运行域上限、完整工作区清单历史额度和设备分区；开发 state root 必须绑定同一份预算，组合 HTTP 必须使用相同运行域。未声明工作区/设备、任一额度不足、实际可用空间不足或运行域后续变大均拒绝。普通只读检查树在此部署下使用既有 RSS 采样保护，阈值 4 GiB；other 内存至少 5 GiB，包含该树及 1 GiB 共享控制余量。原准备树 768 MiB 和运行域其他边界保持原值。

同快照实际 fixture 的 run/chat/other 内存为 8 GiB + 4.1875 GiB + 5 GiB，共 17.1875 GiB，处于 24 GiB 声明分区内。单运行工作区在同设备的活动、全部历史、请求记录、域控制、开发及 other 磁盘合计 24,738,004,992 bytes，处于 32 GiB 分区内。计算与实际部署摘要保存在 `combined-final-v1/budget-report.json`。这是固定并发预留包络：已有输出和外部数据继续消耗真实剩余空间，不是历史表累计硬配额；不抵扣现有分配，准入可能保守提前拒绝。预算调整须停稳参与者，其他服务须另外计入余量。

### 同快照证据

组合于 02:48 +08 冻结，源清单覆盖 1,045 个文件，摘要 `ff9ec5f12e7538d36198eaa5dd0fda2ec937b0ff2aa06fbc51f58a2a23186cb9`；测试清单覆盖 283 个文件，摘要 `d10222bfebfa9797c453e8d00840e0a11c69efbf1d569a248712c0a23fef9ab7`。以下测试和清洁构建之间未改实现或测试，完成后逐文件复核摘要。说明文档单独列入交付清单，不与实现清单混同。

| 本组合证据 | 结果 | 范围 |
| --- | --- | --- |
| `combined-final-v1/development.xml/log` | **100 通过，0 跳过，477.53 秒** | 五个开发对话文件、组合准入/并行控制/浏览器，console、Web、Markdown、项目浏览/移除 |
| `combined-final-v1/runtime.xml/log` | **98 通过，0 跳过，602.87 秒** | 运行管理、协议、Review 修复、HTTP 集成、浏览器、运行基础、边界与公开 API |
| `combined-final-v1/package/cold.xml/log` | **12 通过，0 跳过，117.54 秒** | checkout 外仅从 wheel 解包目录导入；资源拒绝、实际原生运行与 fake 对话并行停止、部分提交保留、HTTP/恢复、1440/390/320px 页面 |

冷验证在测试前后断言所有已导入 demiflow 模块来自 wheel，子 worker 使用该包入口；未安装到共享环境。全部 1,042 个包内源码及资源与冻结清单逐字节相同。sdist 同时核对冻结源码、测试和交付文档；根 AGENTS.md 不在现有 sdist 打包规则中，仍在源码清单和工作副本内，不能称它也已入包。截图分别放置在本批次的 development、combined、runtime 和 package/screenshots 目录，没有覆盖先前证据。390px 开发与 320px 运行页已人工复核；320px 原有密集数据表仍较窄，页面无横向溢出不等于所有表格都适合阅读。

首次增量构建捕获旧 build/lib 缓存：运行态复制保留的源 mtime 早于旧构建产物，setuptools 把旧 datafusion.py 带入 wheel。失败包、日志和摘要保存在 `package/failed-incremental-build/`；清理本副本生成的 build 目录后重建，全资源校验和上述冷验证通过。没有修改冻结源码求绿。包内 CLI 核验也纠正了早期说明中的命令：本包没有 demiflow.__main__，有效入口是 `python -m demiflow.cli forge ...` 或已安装的 `demiflow forge ...`，见 `package/cli-verification.json`。

初次组合 smoke 未显式传入私有运行域，曾访问默认 `/var/tmp/demiflow-forge-0/preparation.lock`；观察到的是空锁文件，没有运行策略或活动槽位。无法证明该锁原先是否存在，故保留它，没有删除不明归属文件。全部后续组合、冻结回归及包验证均显式使用各自私有域。此例外不能表述为完全没有触及共享协调文件；正式源码、环境安装、业务数据和正式服务未由本任务修改。

### 交付与限制

相对已批准运行态基线的增量交付位于 `evidence/combined-delivery/`，包含 patch、41 个变更文件归档及前后摘要；原 v4/v5 交付和失败探针均保留。总索引为 `evidence/combined-final-v1/delivery.json`，列出包、测试、资源核算和所有输入版本。正式发布负责人须以此精确基线审阅/合入，不把旧独立 v5 patch 直接套到新运行态文件上。

完整三文件配置、真实 CLI 认证路径引用、命令、启用与回退顺序见 [组合部署方案](forge-development-chat-operations.md)。默认开发关闭，正式开启须使用实际项目授权和 reviewed 分区；说明中的 /srv 路径与 workspace ID 只是可审查的替换模板，不给新业务授权。本次新增模型请求为 0；沿用原第 4 次真实 DeepSeek 最小开发及独立 4 项测试证据，其源快照仍为原记录，不冒充在最终组合上重跑。Codex 仅协议/无模型检查，没有真实推理验收。

trusted_local 仍使用宿主权限，restricted 不可启动；采样保护不是 cgroup，provider 内部请求/token/费用和合计 CPU/线程未实现硬限制。DeepSeek elicitation/transcript replay 仍未支持。已 claim 但缺少后代 drain 回执的轮次继续保留 unresolved。运行态原 R7 三对测量的 ≤3% 目标未建立（第三对 4.046%，总计 0.992%），总控按 RM-20 接受有限证据；本组合未重新做性能评测，也没有把该目标写成已通过。
