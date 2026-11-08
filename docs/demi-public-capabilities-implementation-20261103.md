# Demi 公共能力 C1–C4 实施与验收记录

状态：C1–C4 已完成分阶段 Review、定向回归和现有入口联调。不是全仓全量测试或生产模型质量认证。方案见 [中粒度设计](demi-public-capabilities-design-20261103.md)、[源码方案](demi-public-capabilities-source-plan-20261103.md)、[验收矩阵](demi-public-capabilities-review-20261103.md)。

本轮仅实施平台公共能力与客户端接入。旧 pipeline、整个 training/、Notebook 输出及生产数据不修改。现有用户入口继续是本机 55971 → 服务器 59865。

验证与原始改动基线目录：`/tmp/demi-public-c1-c4-j764jqiu`。目标平台原有 dirty / untracked 内容在修改前按文件备份，不能用 Git HEAD 覆盖它们。

## C1：公共用例

- 源码 / 参数读取归 workspace；media / evidence 经公开函数调用原有隔离 worker。
- 新增结果登记公共用例及 CLI；明确 attempt 和完整结果摘要，catalog 解析后再次核验完整 DatasetRef，create-only 发布别名。
- start / cancel / reconcile 区分省略、现代 attempt 和历史 null；取消在 journal 事务内防止旧 attempt 写入新一次运行。
- 原生 ExecutionConflict 不依赖 Forge；Forge 错误码经 worker 保存，CLI 保留原结果结构 / 退出码。
- Review 修正：移动 source_text 后静态资产仍需要 read_bounded，已恢复原静态读取依赖；中止早期未完成测试后重新验证，未放宽检查。
- `c1-focused.log`：**17 passed，144.91s**。包括新的公共入口、真实运行 / 固定结果、事务竞态、子进程错误和既有原生 attempt 测试。
- `c1-reviewed.json` 与 `c1-reviewed/` 保存阶段代码身份。

## C2：HTTP 契约

- 现有 handler 提供 capabilities、闭合读写字段、重复 / 非有限 JSON 拒绝、大小限制和结构化错误。
- 新写请求必须带 attempt；结果登记统一调用 attach_result。未知路径、权限不足、身份冲突、忙和内部失败分别报告。
- 仅 API 模式与可选私有 token 文件；端口冲突失败，不变更已有入口。
- 请求准入失败返回小型 JSON；重工作等待最多 5 秒，无额外无限队列。
- `c2-regression.log`：**48 passed，680.61s**，包含 workspace、console、CLI、恢复、原生 attempts 与新增 API 用例。
- 后续 Review 补上会话失效专用错误类型，使客户端能停止授权失败轮询；JSON 超限有明确资源错误码。`c2-review-retest.log`：**52 passed，106.06s**。最终补充真实文件所有权与非有限数值（含 `1e400` 溢出）验证，`c2-final.log` **2 passed，49.29s**。

## C3：独立 Web 与多端同步

Web 先在验证目录 `web-candidate/` 隔离测试，通过后与服务一同发布到原入口。测试期间未混用线上旧后端与新静态资产。

新增 capabilities 校验、写前置条件、有限请求等待、非 JSON 错误处理和授权失效停止轮询；保留现有项目布局、草稿、固定版本与单同步链。

新增真实浏览器用例：两页面 + CLI + 标准 HTTP 客户端使用同一运行；已接受 prepare 的响应丢失后不自动重放；非 JSON 失败恢复与授权失效停止轮询。`c3-browser.log` 首轮 23 passed、2 failed：一项旧错误文案断言已改为校验 409 / conflict 和不读取错误版本；准备运行失败已在独立页面复现：初始化 / blur / click 对同一路径的参数读取互相取代，使准备动作以 AbortError 静默退出。修复只在表单内复用同路径的 pending Promise；新增延迟响应测试，同时覆盖代理返回 JSON null。`c3-review-retest.log` 的媒体 / 源码冲突用例通过；`c3-final.log` **7 passed，365.74s**，包括修复后的完整 A/B、关闭浏览器后继续、结果复用、双端同步、响应丢失与草稿保护。首轮其余 23 项通过的结果保留。

## C4：外部接入与验收

- 已写 [HTTP 契约](forge-http-api.md)、[接入指南](forge-client-integration.md)、通用 workspace skill。skill-creator 的 quick_validate 已通过；没有安装或改写用户客户端配置。
- 应用户要求提供 [Harness 插件交接](demi-harness-plugin-handoff-20261103.md)，与独立 Web 并行开发；标明接口、分工与最终部署状态，凭据不进入文档。
- `c4-three-flows.log` 首轮 inventory 通过；两个业务测试因混合收集的包导入冲突未进入执行。在业务仓用 importlib 导入模式隔离重跑，`c4-business-retest.log` **2 passed，123.48s**，identity / concept_images 均完成真实封存执行、固定结果和模型证据核验；模型仅为本地 HTTP 夹具，无生产输入。
- 旧迁移基线中的 49 个 legacy 文件已只读比较，6 个与旧基线不同；本轮没有编辑这些文件，不把差异归因给本任务，也不回退用户的并行工作。当前观察状态另存 `legacy-observed.json`，最终再核对。
- 独立常驻巡检 / 任务允许修改范围识别按用户最新指示留待后续，本轮不增加监控服务。

- `c4-boundaries.log`：**13 passed，160.67s**，索引缺失 / pending / cursor、同步观察、部分提交取消与 supervisor 退出核对。
- 隔离构建 wheel 成功，解包后验证四个 Web 资产与候选一致；包外导入公共函数没有导入 Web，见 `wheel-evidence.json`。

## 实际入口与无 UI / 多端验收

原监听 `127.0.0.1:59865` 已更新；用户 SSH 映射仍为本机 **55971 → 服务器 59865**。原凭据及五个既有项目的登记、权限未改变。服务改用既有 CLI 的 `--token-file`，私有凭据存放在工作区外部的 `.forge/console.token`（0600）；没有自动开机启动或监控服务。`deployment.json` 记录端口与静态资产摘要，不包含 token。

新增一个明确隔离的 `public-capabilities-acceptance` 项目，展示最终源码下的操作效果：

| 入口 | 实际运行 | 结果 |
| --- | --- | --- |
| 纯 CLI | `rc3c349e03373451bb3f441f3` | A 配置，2 行，独立表 v1；保存为 headless_result |
| 纯 CLI 续接 | `r91f3b086d4b747849577b5d4` | 固定结果继续加工，1 行，新表 v1 |
| 现有 Web | `r81de61d50c2f4ad4a7e082b8` | B 配置，1 行，新表 v1；第二页面自动出现 |

前两次完成时，此新工作区尚未登记到任何 HTTP 服务；`headless-evidence.json` 保存全部 CLI 命令与结果。随后显式登记，并在原服务上验证 Web → 第二浏览器 → 标准 HTTP → CLI，run_id、attempt、完整输出引用一致，浏览器脚本错误为零。没有用相似参数推测写请求是否成功。

`live-acceptance.json` 另记录真实 demiwtg 的 3 个登记入口、9 张表，以及既有 identity / concept_images / inventory 封存运行的 58 / 62 / 16 个图节点和 3 / 4 / 2 个固定输出。此处查看的是保留的历史运行；本轮新业务运行证据来自前述隔离 fixture，不改写历史，也不重绑真实项目的平台快照。

实际截图位于同一验证目录：`demiwtg-flows-live.png`、`identity-fixture-live.png`、`relation-fixture-live.png`、`inventory-reconciliation-live.png`、`public-web-run-live.png`、`public-headless-result-live.png`。演示脚本首轮因复用页面时只改变 URL fragment、未重新加载授权初始化而停止；已改为完整导航后重验通过，失败日志保留。这不涉及重放已接受的 POST。

## MC-01–MC-20 验收核对

| ID | 结果与证据 |
| --- | --- |
| MC-01 | 通过：headless-evidence，纯 CLI 完整闭环并继续作为输入 |
| MC-02 | 通过：public_api、live-acceptance；共享准入、固定身份与结果，不比较随机 ID / 时间 |
| MC-03 | 通过：API-only 返回 capabilities / context，静态资产 404 |
| MC-04 | 通过：console 同名表按 ID 路由、坏项目隔离；现有六项目可读取 |
| MC-05 | 通过：workspace_sync_browser，外部源码 / 检查 / CLI 运行自动可见 |
| MC-06 | 通过：两个独立浏览器 context + HTTP + CLI，实际运行输出引用完全一致 |
| MC-07 | 通过：事务内注入 resume 竞态，旧 cancel 拒绝且新 attempt 无取消标记；null / bool / 旧 attempt 拒绝；reconcile 在执行锁内再核验（源码 Review） |
| MC-08 | 通过：既有 resume、native attempts CAS、工作区执行准入回归；无第二套 supervisor / 调度 |
| MC-09 | 通过：指定旧 attempt 结果可登记；错误 digest、占用别名、catalog 逻辑身份差异拒绝且不写别名 |
| MC-10 | 通过：已接受 prepare 丢失响应仅创建一条运行；其他 POST 共用无自动重放入口（Review） |
| MC-11 | 通过：草稿、历史 attempt、慢响应、Case 选择、隐藏暂停；另修复同参数 pending 读取竞态 |
| MC-12 | 通过：index pending / 缺失 / cursor、显式修复，刷新不伪造空结果 |
| MC-13 | 通过：会话 / Origin / Host、登记身份、越界路径、符号链接、闭合字段；认证失效停止页面轮询 |
| MC-14 | 通过：真实子进程冲突保留 code，HTTP 409，CLI 非零退出 |
| MC-15 | 通过本轮资源边界：64 KiB 请求拒绝、有限查询 / 响应、并发准入 JSON 503、平台 worker 上限及清理回归；不把编码上限当 RSS 保证 |
| MC-16 | 通过：凭据权限 / 所有权 / symlink / 长度、重启复用；端口冲突不换端口，实际部署保留凭据 |
| MC-17 | 通过：无 Web 导入的公共函数与 wheel；浏览器关闭后已接管运行继续 |
| MC-18 | 通过：深链接、图片 / Case、封存源码、历史、离线导出；最终 wheel 资产与已部署源码一致 |
| MC-19 | 通过通用性回归：两个真实 Flow 的隔离模型 fixture 与第三种 inventory；原入口能查看三类历史运行；不声称真实模型质量通过 |
| MC-20 | 通过本任务保护范围：final-protection 中 49 个 legacy 文件与任务观察基线一致，未编辑 training / Notebook / 生产数据；原有差异未回退 |

上列使用定向与已有回归，不是重新运行全部 demiflow 测试。各日志的用例有重叠，不能把分批数字相加成独立用例总数。

## 最终架构 Review

- CLI 与 HTTP 直接调用 workspace / runs，未新增 facade、业务数据库、执行器、宿主 SDK 或命令中间层。
- Source / parameters / media / evidence / attach 的规则归公共入口；HTTP 只负责请求、权限与编码。
- attempt 冲突类型归原生 errors，原生执行不依赖 Forge；worker 传递有界、白名单 code。
- alias 发布前核验最终完整 DatasetRef；变化提示不参与 CAS，历史引用不自动换成 latest。
- Web 的新增状态仅限授权状态、当前参数的 pending 请求；每页继续一条同步链。没有聊天、业务名称分支、自动 POST 重放或跨域放宽。
- 本轮新文件只有公共接口 / 接入资料、通用 workspace skill、实施记录及必要测试；其他已有 dirty 文件未重置。具体 Harness 插件由外部开发，并须单独完成真实宿主加载、卸载、代理权限与 UI 验收。

独立常驻巡检、完整内容指纹与任务允许修改范围的“预期外变更”识别，按用户指示作为后续内容补充；当前页面轮询不是该监控服务。旧 P4 的生产放行及历史迁移验收不因本轮接入验收自动获得通过。
