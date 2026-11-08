# Forge 与 Codex 面板联动：方案复审与验收矩阵

日期：2026-11-03。对象：[中粒度方案](forge-codex-panel-design-20261103.md)、[源码级方案](forge-codex-panel-source-plan-20261103.md)。这是基于源码的方案自审，不是独立评审或实现验收。

更新：用户已批准实施。以下保留设计阶段自审和原验收口径；当前实现状态、实测证据与未完成项见[实施验收](forge-codex-panel-acceptance-20261103.md)。

后续用户澄清不需要操作台发起对话；[修订方案](forge-shared-workspace-panel-design-20261103.md)以工作区事实同步为主。原 CP-18 保留历史实测状态，不再作为新方案的功能目标，也不改写为已通过。

## 1 第一轮：职责、依赖与最小范围

| 发现 | 修订后的决定 | 复审依据 |
| --- | --- | --- |
| 为接入 agent 增加第二套服务/运行管理会重复现有职责 | 页面 → 原 HTTP → workspace/runs；Codex 不进入执行核心 | workspace_view 已直接调用 workspace/runs，runs 复用 Dataset |
| 把 Plugin 当成任意宿主 UI 扩展 | 插件仅可选分发包装，实际面板使用浏览器；不声称支持原生自定义面板 | 官方插件与 UI 文档分别声明能力范围 |
| WebMCP 与 MCP server 重复提供同一页面动作 | 首轮只用页面工具；无独立 MCP 服务 | 本场景人和 agent 操作同一打开页面 |
| Skill 规范与当前 workspace 语义可能混用 | 保持现有 v1 Skill 独立，不用其三节点/Freeze 指令指导 workspace | integrations 中 Skill 仍明确 v1 |
| 把前端执行开关误认为 Codex 全局权限 | enable_runs 仅是 HTTP 会话准入；CLI 仍有其既有授权入口 | Codex 原有 shell/CLI 不经过 HTTP |

结论：两个新增 JS 模块各有明确职责；无后端反向依赖、第二执行器或新增持久业务实体。

## 2 第二轮：身份、数据流与竞态

| 发现 | 修订后的决定 | 反例 |
| --- | --- | --- |
| alias 每次解析可能指向不同固定版本 | preview binding 包含 root、解析 uri、完整 reference；读取前 expected_binding 核对 | 列表 v1 后替换为 v2，再点详情/图片 |
| 只使用 control digest 忽略 location 根变化 | 保留原 control digest，另定义有清晰名称的 binding_digest | 控制引用不变但只读 location 指向另一目录 |
| 新请求读取前检查、随后又解析 alias 会留下窗口 | worker 解析一次并消费同一 ref/uri | 核对后替换 alias |
| 大 row_id 经 JS Number 会丢精度 | 原生预览已输出字符串；全链继续字符串并验证 uint64 | 大于 2^53 的定位值 |
| 同表两个详情响应可能乱序覆盖 | 区分 viewEpoch/selectionEpoch，提交 DOM/选择/URL 前检查 | A 请求慢，后点 B 快，最终仍应 B |
| 图默认选首节点掩盖无效链接 | 显式 node_id 必须存在且属于指定报告/run | 旧 node_id 被用在另一份报告 |
| 从旧报告打开当前源码会混淆来源 | 当前源码带预期 source hash，历史 run 读封存源码 | 编辑后报告节点仍指旧版本 |
| 输出不在当前历史页就无法定位 | table describe 直接解析 run output 引用 | 刷新后打开第 21 次以前的运行结果 |

结论：引用指向实际来源，错误会显式出现；不依赖第一项/latest 或显示标签猜对象。并发保证限于受管读取协议，不宣称外部文件系统全局不可变。

## 3 第三轮：宿主协议、生命周期与资源

| 发现 | 修订后的决定 | 验证点 |
| --- | --- | --- |
| 凭印象使用 navigator.modelContext 或任意消息桥 | 采用核对过的 document.modelContext.registerTool；能力检测 | 真实宿主与缺失 API 场景 |
| 将整个嵌套引用放进注释元数据 | 使用不超过六个扁平字段，限制 256 字符/2,048 字节 | 长 workspace、alias、Unicode、无静默截断 |
| SVG 节点不是 request 要求的 HTML 目标 | 使用对应 HTML 详情卡片 | 调起注释时目标仍可见且 connected |
| 点击后先 await 会丢失用户手势 | 提前构建引用，点击内同步 request | accepted=false、权限拒绝、用户取消 |
| 以全局当前选择构造已打开注释 | 注释绑定点击时的卡片快照 | 注释打开后用户继续选另一行 |
| 额外 JS 使离线导出缺资源 | 同步更新 export_report；离线移除宿主模块 | file:// 导出无网络、无缺脚本、无宿主注册 |
| 工具返回整份报告/Case 会扩大上下文 | get_context 返回引用；read_case 投影且限制输出；底层复用既有有界预览 | 不把输出字节限额称为 native/RSS 上限 |
| 多工具并发/循环刷新占满 worker | 限一个视图操作和一个 Case 读取，无队列；已有 HTTP 限额保留 | busy、慢读、隐藏页面、失败不重试 |
| agent 导航覆盖用户未提交参数 | 一个有界 draft，切换其入口时明确 draft_active | 草稿不写 URL/工具结果、不自动提交 |
| UI 文件更新也影响平台快照 | 遵循当前 source_inventory，不放宽过滤 | 原工作区 stale、显式 bind、新检查 |

结论：宿主功能可选；状态和资源生命周期受限。没有“安装了插件就算已联动”或“mock 可用就算真实客户端已验收”的保证。

## 4 第四轮：端到端完整性与范围复核

按“打开 → 准确选择 → 讨论 → 编辑 → 复核 → 运行 → 看结果”逐项核对：

- 入口已有 serve/CLI，面板打开属于宿主，SSH 转发属于客户端/环境；方案没有要求远端 Python 控制本机。
- URL、注释、工具使用共同引用；表续读在服务端核对；节点绑定原 report。
- 工具可定位 run/report/case，Case 可通过同一 sample 或 CLI 准确读；没有必须全量镜像项目的步骤。
- 页面刷新只复核；重新 inspect/prepare/start 是明确动作；运行接口没有绕过既有门禁。
- historical run 与 current workspace 的状态/源码分开；无法访问旧表不自动改读最新。
- 文档明确单对象讨论范围；批量收藏、自动选中样本变成试跑输入没有被默默承诺。
- 不以本轮 UI 集成推进被用户暂缓的 Review/Freeze、生产调度或交付。

设计层面已修订上述已识别问题。真实客户端 API/权限、SSH 入口体验和运行时竞态仍须实施实测，列为明确门禁，不能在设计阶段写“已通过”。

交叉复核补充修订：图片 HTTP 仍为二进制，因此绑定摘要通过响应头透传；run 输出先 describe 才能生成完整定位目标；工具读取投影与原页面投影明确区分；URL 编码膨胀单独检查；token 规则明确针对业务响应/引用，保留既有首次认证地址。核对 `RunJournal.execution_view` 使用只读打开，不为新页面工具增加初始化/迁移/写入路径。

## 5 验收矩阵

以下 CP 编号仅是本方案验收索引，不新增平台 FL/OP/EX 规则。全部状态为待实施验证。

| 编号 | 验收内容 | 必测反例/通过口径 |
| --- | --- | --- |
| CP-01 架构 | Python 核心、运行层无 Codex/browser 依赖 | 卸除/禁用 codex.js，普通浏览器和 CLI 完整可用 |
| CP-02 定位 | 表、Case、report/node、run/node、run_compare 能刷新恢复和前后导航 | 无效类型/版本/字段/节点拒绝；不回退第一项 |
| CP-03 认证与 SSH | token-only 老链接可用；token+view 初始化不丢 view；复制链接不含 token | 新会话无 token 拒绝；同机不同转发端口有效；Host/Origin 不放宽 |
| CP-04 工作区 | URL/工具 workspace 与当前服务一致 | 切换到别的根/新服务后拒绝恢复旧对象，无跨根读取 |
| CP-05 表身份 | 描述/分页/详情/对照/图片均使用真实绑定 | alias replace/detach、位置根变化、版本删除触发失败，绝不读 head |
| CP-06 单次解析 | worker 用一次解析出的 ref/uri | 核对后修改 control 的故障注入，返回仍对应原 ref 或明确失败 |
| CP-07 行与键 | row_id 精确，业务键对照语义保留 | >2^53 定位、重复键、null、缺失匹配；不使用 row_id 跨表 join |
| CP-08 竞态 | 用户最后选择与 DOM/URL/对话引用一致 | A 慢 B 快、切页、旧图片、旧证据、旧报告响应不得覆盖新视图 |
| CP-09 注释 | 可见 HTML 卡片附准确、有效、有限元数据 | SVG 目标替代、元数据超限、权限拒绝、取消；不把 accepted 当 sent |
| CP-10 工具 | 四个封闭工具调用与人工同一操作函数 | 非法字段、未知目标、跨工作区、旧绑定拒绝；成功只在加载后返回 |
| CP-11 只读边界 | 站点工具不引起检查/准备/执行/补登记/模型调用 | 调用前后 report/run/输出/请求数量相同；导航只改视图 |
| CP-12 源码依据 | current source 核 hash，run-source 为封存内容 | 编辑当前算子后旧报告 stale，历史 run 源码保持原字节 |
| CP-13 运行闭环 | 同一示例完成检查/准备/启动/结果查看及 A/B | prepared 后编辑被原 start 拒绝；取消/部分提交保留原语义 |
| CP-14 草稿 | 参数输入在页内保留，不被工具自动覆盖/提交 | 不同 Flow 导航返回 draft_active；刷新不提交；敏感草稿不进 URL |
| CP-15 资源 | 描述/工具输入输出/并发有明确上限 | 超长 fragment、Unicode query 编码膨胀、宽字段、慢 worker、并发 busy、页隐藏、blob 清理；超限不截断假成功 |
| CP-16 普通/离线 | 无宿主、部分 API、导出 file:// 均可独立查看 | 无额外网络/注册/执行、无缺少 JS、导出仍 not_live |
| CP-17 分发与旧记录 | wheel/sdist 包含新增资源，旧 run/report 仍可读 | 平台变化正确 stale，不自动重绑/改历史/重启服务 |
| CP-18 真实客户端 | 当前 Codex 中注释发送、工具发现/定位、SSH 访问形成闭环 | 记录客户端实际版本与可用功能；mock 或存在工具元数据不能替代 |

## 6 测试与证据分层

1. 纯协议/HTTP：格式、绑定、预期摘要、权限及大小边界。
2. 真实 Chromium + 自有 Lance fixture：DOM、异步乱序、路由、run 对照及离线；宿主 API 注入只验证调用契约。
3. 真实 Codex 宿主：注释到对话、发现并调用 Site Tools、打开指定 Case、SSH 重连。需人工可复核证据，不记录 token。
4. 既有 Forge/通用算子相关回归与打包：保护运行和契约能力，测试通过后不无理由重复全量测试。

本轮没有新增测试通过数字。实施完成必须每项有真实结果或明确失败；CP-18 未通过不能宣称“丝滑集成已完成”。

## 7 本轮源码基线与交接

下列为分析时 SHA-256 前 16 位，仅用于确认阅读的 dirty 文件版本，不是新的运行鉴权摘要：

| 文件 | 摘要 |
| --- | --- |
| forge/workspace.py | d2af95936c3b1973 |
| forge/workspace_worker.py | a25c519cc85dd292 |
| forge/workspace_view.py | 63f7e0049ec3b5d8 |
| forge/workspace_cli.py | ae84d3d409cdb97d |
| forge/runs.py | cc2072fb5cfcab07 |
| forge/web/app.js | cf8f09745da03ed0 |
| forge/web/index.html | 1c3c1324be381cf1 |
| forge/web/style.css | 51c38b50fa6c7241 |
| lance/preview.py | 8e4e9aa9dc86d542 |
| forge/platform_snapshot.py | 747feeea96e4c2c9 |

本轮交付限于三份设计文档。实施前若相关文件变化，重新核对受影响职责/调用点，不覆盖既有 dirty 修改。当前 AGENTS 的旧 v1 描述与后续 workspace 能力按用户已批准阶段及现有版本契约区分，不因这次接入重回固定三节点流程。
