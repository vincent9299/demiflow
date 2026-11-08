# Forge Web 改版源码实施方案

日期：2026-11-04。状态：待主会话下发实施包；此文件是计划，不代表下述正式改动已发生。仅涉及 `demiflow` 平台，业务、数据、Notebook、training、Demiurge、共享环境及现有服务只读。

## 已核对的真实接线

| 当前文件/函数 | 现状与最小改动 |
| --- | --- |
| `demiflow/forge/web/index.html` | 仅装载 `style.css`、`view.js`、`app.js`。加入阅读模块与本地 parser，保持 defer 顺序 |
| `web/app.js::start / projectHeading / markProject / clearProject` | 现有项目侧栏、横向功能导航、总览与空清单。改为项目选择器 + 五个任务导航；保持项目 ID、权限和空清单自动同步 |
| `overview / originalProject` | 去掉等权指标卡与总览全表堆叠；README 优先，识别细项转入流程/数据页，保留来源与缺口 |
| `projectDialog / removeProjectDialog` | 复用现有 POST、确认、清单摘要、并发保护；移动入口，不改登记/移除语义 |
| `projectChecks / renderAudit / syncStatus` | 精简默认展示；诊断、越界、过期、断线仍显式可见；完整规则/摘要按需展开 |
| `originalData` | 目前行详情为 `pre(json(value.rows[0]))`。改调共享字段阅读组件，保留本地 `sequence/pageSequence/selectionEpoch` 和同一 `project-sample` query |
| `tables::showCase / renderCase` | 接入共享阅读组件；列表保留纯文本摘要。保留 `contextActions`、业务键对照、图片和历史调用证据的原接口与绑定 |
| `web/view.js` | 这是纯视图引用协议，**不是** DOM 渲染文件。首期不需改；导航更名不改变任何 `kind`、v1/v2 编解码或验证 |
| `workspace_view.py::Handler.do_GET / _send` | 当前静态资源是四项显式白名单，单资源 512 KiB；CSP 限同源。新静态文件需逐项列入，不能开放任意目录 |
| `workspace_view.py::export_report` | 按固定字符串把三个 Web 资源内联到独立 HTML。新标签必须一并处理，保持转义、16 MiB 总预算和离线分支 |
| `pyproject.toml::tool.setuptools.package-data` | `web/*.js/css/html` 不包含未来子目录许可证。明确加入 parser 与许可证、worker 文件；wheel 与 sdist 都验收 |

不改 `workspace.py`、`project_discovery.py`、检查器、运行器、公共 HTTP 字段或数据契约来迁就页面；若发现后端契约缺口，单独报告，不在视觉包顺手扩展。

## 三个工作范围、两个实施归属

主会话 Review 已确定：A+C 由同一实施负责人独占 `app.js`、主样式、`index.html`、静态资源/打包/离线集成；B 独立交付纯阅读模块、固定 parser/许可和在线单 worker，不持有 token、HTTP 或项目状态。Case 首期一并改，展开的长文本默认 Markdown。本轮仍只交付设计，没有分派正式实施或并行编辑。同一正式文件只给一个写入负责人。

### A：视觉、导航与首次使用

A+C 负责人范围：`web/app.js` 中壳层/导航/总览/项目管理/状态展示函数、`web/style.css`，并拥有下文 C 的集成文件。`app.js` 只由此负责人编辑，B 提交模块接口和调用点建议，不同时修改调用方。

- 用 CSS token 与现有 DOM 工具 `$()` 实现侧栏、路径、阅读布局；不引入组件框架、路由库、状态仓库或构建服务。
- 五个入口分别沿用 `overview/tables/flows/checks/runs`；深层对象仍由 `pageKind()` 与原有 kind 归属。保留全部功能入口，包括选择源码、节点、运行与对照。
- 空清单仅在 `permissions.project_manage` 允许时显示管理按钮；`api-only`、单项目服务、授权失效分别保持原权限。新建/注册成功基于响应与新 `context` 展示引导，不把前端成功通知当作检查证据。
- 首页建议只依赖已读取事实：有可读数据→数据/Case；否则有入口/配置→流程；否则说明/源码与缺口。新建空目录与识别失败区别呈现，无新增持久化“已完成引导”状态。
- 不把 `context.runs_enabled=false` 写成所有操作只读；检查会保存报告。不能用本地布尔值推测后台授权，prepare/start/resume 原准入不变。
- 把规则/摘要/调试收进展开区，巡检违规与过期等仍有可见摘要。保留真实状态组合，不将 `static_status=checked` 汇总成“整体通过”。

完成条件：四种首页状态、旧导航与当前项目一致、窄屏/键盘可用。A 壳层和 B 阅读模块可各自形成可审阅结果，由 A+C 负责人顺序集成并完成 C；不单独发布一个只有配色的版本。

### B：阅读组件与 Case 接入

负责人先交付独立 `web/reading.js`（纯呈现）、`web/markdown-worker.js`（在线解析）和本地 parser；由 A/集成负责人修改 `app.js` 的三个调用点。阅读模块不持有 token、HTTP 客户端或项目状态，不建立第二套请求层。

推荐接口形状（不是新公共协议）：

```text
createTextReader({text, defaultMode, resolveLink, onDispose})
renderCaseFields({fields, mountText, mountRaw, actions})
```

项目 ID、数据绑定/版本、字符串 row_id 与 epoch 留在 `app.js`，由调用方闭包核对后挂载结果，不交给阅读模块管理。模块只管理当前文本、模式和有界 worker job。`actions` 从既有 `renderCase()` 闭包传入图片/证据/定位按钮。原项目来源没有这些能力时不传入，不伪造别名，不以“图片路径长得像”授予权限。

| 调用点 | 默认阅读 | 原文/结构 | 来源和竞争条件 |
| --- | --- | --- | --- |
| `overview` 的 `context.readme` | Markdown 预览 | 准确原文 | context 已保存内容；缺失/现有错误文字如实显示，不声称读取成功 |
| `renderCase` 已声明 `kind=text` | Markdown 阅读 | 准确字段原文 | 同一 `sample`、binding、字符串 row_id 与 selectionEpoch |
| `originalData` 或无展示声明字符串 | 展开的长字符串/多行文本默认 Markdown；短身份标量/路径紧凑显示 | 准确原文；嵌套对象/数组按结构查看 | 原项目 query 的 discovery digest、version、binding 不变；未读字段不推断 |
| 源码、配置、JSON、日志、调用证据 | 准确原文/结构 | 保留空白、null、空值、类型与顺序语义 | 原 source/parameters/project-document/run-source/evidence 接口和摘要不变 |

通用选择规则：先按已知原始类型/展示声明区分源码、配置、JSON、日志、image、evidence 和结构化对象；这些保持原类型。普通字符串 ≥240 字符或包含换行时，展开后默认 Markdown，短标量/路径保持紧凑，显式 `kind=text` 同样进入阅读组件。只使用类型、内容长度/多行与展示声明，不读取 `question/answer` 等业务字段名来推断正文角色。规则选择阅读模式，不改变原值或数据类型；所有文本都能切准确原文，超预算按下文明确降级。

避免给每一条表格记录、每一个折叠字段或每一个对照匹配同时创建 reader。每条默认只挂载当前字段；“原始记录”展开后才创建有界 JSON 视图。列表仍是短文本摘要。文本选择、原文切换和折叠状态按当前对象保留；切换项目或对象后释放旧组件，不保存 Case 正文到 URL/sessionStorage。

原项目 `project-sample` 没有 ForgeView 对象定位协议。首期保留当前 `tables` 路由内的临时选择与返回路径，不承诺复制完整原项目 Case 定位；不能往 `view.js` 偷加未经约定字段。已绑定表/运行结果仍沿用既有 `kind=case` 深链接。

### C：静态资源、离线兼容与集成验收

与 A 相同的负责人独占 `workspace_view.py`、`index.html`、`pyproject.toml` 及集成测试调整；持有 `app.js` 合并权。负责 A/B 集成顺序、最终 CSP、wheel/sdist、离线 HTML 与回归证据，不接管后端语义。B 的 `reading.js`、`markdown-worker.js`、parser 与许可由 B 交付，经 A+C 负责人接线。

正式实施在自有 `/tmp` 下的完整源码副本进行，用普通复制而非硬链接，记录复制来源及当时 dirty 内容摘要；测试 fixture、构建输出和证据同样留在自有临时范围，不安装/更新共享环境。当前运行服务所读的原始磁盘资产保持不动，避免新脚本配旧 HTML。主会话完成 Review 后统一发布、更新原服务；实施会话不能自行更换原 59865/55971 入口、令牌、登记或发布部分资产。

样稿中的 `sample.js` 是隔离交互，不应复制替换 `app.js`；仅复用已审阅的布局与阅读策略。样稿的 file URL、虚构状态、模拟登记/依据变化和假数据不进入正式包。

## 依赖与 Markdown 实现

现有 `markdown-it-py==4.2.0` 属于可选 search extra，不是 Web parser；不为它新增服务器端 Markdown 协议或强制安装 search。推荐新增固定的浏览器 markdown-it 本地发行文件；样稿锁定 14.1.0，123,618 bytes，SHA-256 为 `38c70a1e7ca91ab40e2d9e6e60129851a717ed1c7d4acbbdd41bf9503791cf68`。

来源：[官方发布](https://github.com/markdown-it/markdown-it/releases/tag/14.1.0)、[MIT 许可](https://github.com/markdown-it/markdown-it/blob/14.1.0/LICENSE)，npm 官方 tarball 的地址与内容摘要见样稿 `vendor/provenance.json`。解析库与浏览器依赖的许可证一起交付；实施前锁定经审阅的实际发行版本与构建来源。样稿收录的依赖许可证来源匹配 manifest 范围，但上游 tarball 没有构建 lockfile，不能声称这些来源版本就是该二进制的完整已验证 SBOM。若正式依赖审计要求可复现构建，应在自有临时目录按明确锁文件重建并记录所有依赖/许可，再封存本地资产；不更改共享环境，也不新增产品构建服务。

建议平铺 `web/markdown-it.min.js`，辅以 `web/markdown-it.LICENSE.txt`、`web/markdown-it.NOTICES.txt`，减少静态路由和打包层级。`index.html` 顺序：parser → `view.js` → `reading.js` → `app.js`，均 defer。worker 自己 `importScripts('/markdown-it.min.js')`；固定 URL，只加载同源封存资产。

不调用 `parser.render()` 后塞入 `innerHTML`。用成熟 parser 的 token，经固定标签集合构建 DOM；标题/列表/引用/表格/代码块保留语义。`html:false`、`linkify:false`、`typographer:false`、无插件；图片 token 变成 alt/未加载占位。仅允许预定元素与少量安全属性，未知 token 明确退回原文；不用宽松 sanitizer 弥补任意 HTML 插入。

链接 resolver 由 `app.js` 接管，不能由文档决定 HTTP 请求：

1. 绝对 HTTP(S) 用 `URL` 检查，无用户密码、控制字符、危险协议或凭据参数；用户点击新窗口，`noopener noreferrer`、`no-referrer`。无自动请求、链接摘要或预取，不携带 `X-Forge-Token`。
2. 相对路径先按 README 所在项目目录核对，拒绝越界、隐藏路径、符号链接与编码歧义，不直接拼当前网页 URL。
3. 只对当前 context 已知对象建立操作：Python 源码用 `source`；已识别配置用 `project-document(path, expected=sha256)`；已绑定表/明确原数据引用走相应现有查看接口。每次仍由后台复核权限/摘要。目标不在这些能力内就显示“引用未支持：原路径”，不开放任意 `.md`/本地文件接口。
4. 同文档锚点只在 reader 内滚动，不写 `location.hash`，不与 `#view` 冲突；目标标题不存在则明确提示。原文仍准确可核对。

## 端到端资源预算与释放

以下是正式实施的初始准入值，可收紧，不通过增加后台响应/行预算掩盖前端开销。

| 阶段 | 增长量/预算 | 超限或结束 |
| --- | --- | --- |
| 进入 parser 前 | JS 长度先快速拒绝，再测 UTF-8 ≤64 KiB、行数≤4096；单文本字段 | 整段降级为原文，不能先解析再测长度；后台 Case 投影预算继续生效 |
| 在线解析 | 1 个 worker，maxNesting 16，1 个运行任务 + 最多 1 个最新待办；同一文本/模式不重解析 | 500ms 后主线程终止 worker，提示“预览超时，查看原文”；切换新选择丢弃旧 job，不积累队列 |
| worker 结果 | 计入输入克隆、parser 对象与 token；token 总数≤6000、递归深度≤24，返回有限 token 数据 | 不发布部分树；worker 超时可回收其对象。输入上限约束分配规模，节点预算不冒充进程 RSS 硬上限 |
| DOM 构建 | 先在 detached fragment 核对 token 和预计节点（含 Text）≤8000、深度≤24；最多一个可见阅读区 | 成功后一次替换；失败丢弃 fragment 回原文；切换字段销毁旧树 |
| 原文/JSON | 每次挂载≤16 KiB，当前组件最多保留4块/64 KiB；按字符边界分块，保留可复制的准确原文 | 显示总长度/当前范围及前后块，不为预览复制全记录；JSON 输出前受既有单行/投影预算控制 |
| 对照/图片 | 延续现有分页、最多20匹配和显式资源按钮；当前正文与所选匹配独立挂载 | 关闭/切项目释放 worker/job/DOM/监听器；保留 `revokeImages()`、32 MiB/64 blob 既有上限 |

不增加全项目文档扫描、预解析缓存或整表内存副本。`app.js` 的 `viewEpoch/selectionEpoch/readController/pageSequence` 仍拥有请求新鲜度，调用方把 worker job 序号映射到当前 epoch/对象身份，模块不保存项目状态；晚到结果不能覆盖新选择。可见区保持旧绑定时要标明陈旧，不能切 head。worker 是有限渲染辅助，不是新后台执行引擎。

64 KiB 并不是 parser RSS 上限；500ms 由在线 worker 生命周期约束，也不约束既有 HTTP/业务执行。样稿只验证了输入/token/渲染退回策略，尚未实现正式 worker、原文分块或所有真实服务竞争测试；不得把样稿结果充当上述预算已在平台兑现。

## 打包、CSP 与离线报告

- HTTP：在现有 `do_GET` 的显式表中增加 parser、阅读模块、worker；许可证可明确列出或仅随包分发，不能启用任意路径 serving。所有脚本继续受单文件 512 KiB 限制和 nosniff/no-store 约束；保留 api-only、Host/Origin、鉴权及现有端口/转发。静态资产无需读项目或凭据。
- CSP：继续 `script-src 'self'`、`style-src 'self'`、`img-src 'self' blob:`、`connect-src 'self'`、`frame-ancestors 'none'`、`base-uri 'none'`；在线 worker 显式 `worker-src 'self'`，不加 unsafe-inline/eval、远程域名或 blob worker。业务图片不经 Markdown 自动加载。
- 包数据：新增 `web/*.txt`（或精确文件列表）；保留现有 js/css/html 模式。构建到自有临时目录并解包 wheel/sdist，确认脚本、worker、许可/通知齐全；不能只在源码 checkout 里测通。
- 离线：`export_report()` 必须内联 parser/reading，替换新增脚本标签，处理 JS 内可能出现的 `</script` 终止序列，保持报告 JSON 的 `< > &` 转义和16 MiB总上限。离线不加载 worker、不发请求、不访问 sessionStorage。报告主体和代码原文保留；如需预览报告内长说明，仅在同一64 KiB/token/节点预算内本地解析。离线无500ms硬时限，因此原文为可靠退路；不得声称与在线 worker 相同的时限保证。
- 离线不新增业务样本/README/Case 载荷；保持 `samples_included=false`、`current_binding=not_live` 及“离线快照、当前依据未复核”。不把 `.forge` 或当前文件嵌入导出。即使新增 parser 未被报告内容使用，独立 HTML 也不能残留根路径脚本请求。

## 验证计划与合入门槛

只为阅读安全、资源边界与保留交互契约增加有意义的用例，不把 CSS 属性逐条镜像成测试。旧用例中的按钮名称/DOM定位按新语义更新，不能删掉固定版本、冲突、新鲜度或副作用断言来换通过。

| 阶段 | 重点 | 已有测试依据/新增验证 |
| --- | --- | --- |
| A | 创建/注册/移除、四种首页、双客户端同步、无运行权限仍可检查 | `test_forge_project_browser.py`、`test_forge_project_removal.py`、`test_forge_project_discovery_browser.py` |
| B 阅读 | README、原项目 Case、普通表 Case 的长文本默认 Markdown；标题/列表/引用/代码/表格和准确原文；短标量/资源/结构化对象保持类型，不按业务字段名猜角色 | 新增纯阅读单元/浏览器用例；结合 `test_forge_onboarding_browser.py`、`test_forge_workspace_browser.py` |
| B 安全/规模 | HTML/事件/脚本/SVG、协议与编码绕过、相对路径、token、所有图片零自动请求；64 KiB、多字节、深嵌套、6000 token、8000节点、worker超时/取消、原文分块 | 小预算触发真实边界；记录请求而非只看有没有 img；晚到 worker不能覆盖新Case |
| C 竞争/版本 | 旧选择慢响应、新选择优先；表换绑409、固定版本不随head、业务键多匹配、历史attempt不变 | `test_forge_panel_browser.py`、`test_forge_workspace_sync_browser.py`、`test_forge_public_browser.py`、`test_forge_run_browser.py` |
| C 状态 | 检查缺口/静态/行为/审查分别可见，巡检违规/过期/断线/授权失效，不重放写操作 | `test_forge_audit_browser.py`、相关 project/public 浏览器用例 |
| C 交付 | 安装包缺资产失败、HTTP资源CSP、api-only、离线file URL和离线恶意script终止字符串、无样本与网络 | `test_forge_workspace.py` 的 HTTP/离线导出用例，加 wheel/sdist隔离检查 |
| C 可用性 | 1440/390/320px、页面无横滚，键盘/焦点、长标题/路径、reader表格局部滚动 | 真实 Chromium 截图；做10秒首次理解路径Review，记录观察而不宣称未经用户验证的可用性提升 |

本设计阶段只运行样稿 JavaScript 语法检查和独立 Chromium 验证，不运行会写正式平台/项目的检查，也不重启原服务。设计样稿证据见 [qa-results.json](assets/forge-web-redesign-20261104/qa-results.json)；它不是上表正式回归的结果。

## 已确定的实施决策与剩余边界

1. A+C 同一负责人独占主页面与集成；B 独立交付纯阅读、本地成熟 parser/许可、在线单 worker。先分别审阅再统一集成，不发布仅配色版本。B 不持有凭据、HTTP 或项目状态。
2. Case 全页阅读首期一并完成，保留当前表返回、同版本与已有业务键对照。原项目未声明的长文本默认 Markdown，准确原文始终可切；短标量、显式资源/结构和源码/配置/JSON/日志保持类型。
3. 原项目 Case 的可复制对象链接与任意项目文档阅读需要协议扩展，首期不做；已支持对象复用接口，其他引用如实说明。若主会话要求新增，另开协议范围 Review。
4. 在线单 worker、有限队列、500ms超时及原文降级纳入实施；离线报告保持现有原文主路径，不宣称相同硬时限。样稿只验证已实现的输入/token/DOM与静态交互，worker/分块/真实平台预算仍是实施验收项。
5. 在自有临时完整源码副本实现和验证，主会话 Review 后统一发布并更新原服务。依赖发行文件的生产构建来源与完整许可审计留在 B/C 交付门槛，不以样稿 tarball 的来源记录替代。

主会话下发前需为每包列精确写入文件并绑定新的源码状态。[源码锚点](assets/forge-web-redesign-20261104/source-anchors.json)记录本次只读核对的 Web、HTTP 壳层与打包文件摘要。设计编写期间检测到 `demiflow/forge/source.py` 的并行变化及其他会话新增检查证据文件，本会话未触碰、还原或归属这些变化。本仓库已有大量 dirty/untracked 内容，不能用 reset/clean 或覆盖整份 `app.js` 的方式消除冲突；集成只应用批准的局部改动，并保留原 Notebook 输出与其他任务修改。
