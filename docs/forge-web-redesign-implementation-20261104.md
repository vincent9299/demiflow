# Forge Web 改版实施与发布（2026-11-04）

状态：界面、阅读、打包、离线及集成验证完成，已按授权一次性发布完整 Web 资产并更新原查看服务；交付主会话终审。记录时间：2026-11-04T19:36:06+08:00（Asia/Singapore）。没有执行模型或生产 pipeline。

## 开发依据与隔离

依据 [设计](forge-web-redesign-design-20261104.md)、[源码方案](forge-web-redesign-source-plan-20261104.md) 与 [阅读组件交付](forge-markdown-reading-implementation-20261104.md)。唯一开发副本为 `/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow`，原 `baseline.json` 保持原字节，SHA-256 `ce6f56e7a81be6420d9eea0b7d8ed7f3c62a0f3228f89402cad21739eed70b2d`。样稿的模拟业务、请求及状态未进入应用。

等 DemiForge 迁移会话完成后，读取其 [实际交付](demiforge-consolidation-implementation-20261104.md)，将最新非 Web 源码、development 资源及打包配置合入副本，逐文件保存摘要；保留本次 Web diff 和 UI 测试。`pyproject.toml` / `MANIFEST.in` 从迁移后的版本追加 Web txt/json 资源，没有用旧配置覆盖迁移。首次组合回归发现了 `lance.schema → data.flow_contract → eager data facade` 冷启动循环，4 项真实提交用例失败。迁移负责人按主会话补充批准修复 facade 和受限目录发现；本会话只合入其正式交付。7 项正式修复文件/哈希逐项核对一致，重新合入后的 1,001 个代码/配置文件与本轮受测版本逐字节相同；67 项全部通过。旧失败日志保留，没有预热绕过或放宽门禁。

非 Web 合入清单见 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/integrated-source-manifest.json`；这些文件仅用于隔离集成验证，发布时没有反向覆盖正式非 Web 源码。

## 实际 diff

| 文件/范围 | 最终行为 |
| --- | --- |
| `web/app.js` | 项目选择器与五项侧栏任务导航；项目设置内保留移除确认、清单摘要和并发保护；顶栏持续显示项目、位置和有效权限。四种首次状态按当前内容推导，说明能力并给出下一步；源码/配置识别表进入流程页。 |
| `web/style.css` / `index.html` | 浅色、留白、收敛状态展示；1440/390/320px 布局、窄屏导航开关、焦点和键盘字段切换。保留真实诊断、覆盖缺口、巡检越界/过期与连接/授权异常。 |
| README 与两类 Case | README 默认 Markdown，可核对准确原文。Case 为完整字段阅读；混合项目的原数据链接保留用户选择，不被默认绑定表覆盖：优先展示声明，普通长字符串（≥240 字符）或多行文本进入预览；短标量、路径、结构化对象、JSON、图片与调用证据保留类型。只使用类型、内容和展示声明，不按业务字段名猜角色。字段切换保留当前 Case 的模式和结构展开状态；卸载释放 reader、worker job 与图片 blob。 |
| Case 定位与对照 | 保留固定绑定/版本、字符串 `row_id`、项目→表→Case、返回当前表、原有业务键对照的零/单/多匹配、历史 attempt 与证据来源。对照记录按需展开，表格仍为纯文本短摘要；保持原有 HTTP、视图代次及选择竞争保护。 |
| `reading.js/css`、worker、parser/许可 | 接入已交付独立组件，其实现与固定 parser 未更改。README 的已知源码/配置/唯一固定表及原数据引用只走现有 API；未支持的路径说明原因，不开任意文件读取。 |
| `workspace_view.py` | 仅静态资源显式白名单、`worker-src 'self'` CSP 与 export 接线；无新增业务 API/权限/后端协议。离线内联全部 js/css，转义任意大小写脚本终止序列，保留全文许可/通知和 16 MiB HTML 总预算。 |
| `pyproject.toml` / `MANIFEST.in` | 保留迁移资源并增加 `web/*.txt` / `web/*.json`，wheel/sdist 均包含完整 11 项 Web 资源。 |
| UI/集成测试 | 新增 `test_forge_web_redesign.py`；保留阅读组件 19 项；更新导航、字段动作、记录选择器与设置入口的既有用例定位。版本、冲突、新鲜度、权限、副作用与历史断言保留。 |

`web/view.js` 原字节保持，v1/v2 深链接没有扩展。未修改检查器、执行器、数据契约、业务源码、Notebook/training 或真实快照，没有自动重绑、认证、检查或修复真实项目。精确文件/摘要见 [verification.json](assets/forge-web-redesign-implementation-20261104/verification.json) 与 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/web-changes.patch`。

## 验证结果

最终集成副本：**67 passed，0 failed，0 error，0 skipped**，1177.306 秒（用例累计，三个隔离测试组并行）。JUnit：`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/final-regression.xml`；日志：`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/final-regression.log`。

- 真实 Chromium 与 HTTP：新建/注册/移除、双客户端及 CLI 自动同步、四种首次状态、README/两类 Case 的预览与准确原文、结构/短值/图片按钮、业务键零/多匹配、v1/v2 链接、固定版本与换绑 409、慢响应选择竞争、草稿、运行和历史 attempt、断线恢复与授权失效停止轮询。
- 阅读安全与资源：HTML/脚本/SVG 不执行，Markdown 图片零自动请求，链接无凭据/授权头/引用泄漏；64 KiB、多字节、行数、嵌套、token/消息/DOM 边界整体退回准确原文；500 ms 解析超时、2 秒 worker 启动超时、单 worker/有限待办、取消和真实晚到结果。新增整页用例验证旧 Case worker 结果不能覆盖新 Case。
- 所有静态资产均从显式白名单读取，单文件≤512 KiB，nosniff/no-store，在线无 unsafe-inline/eval、远程依赖或 blob worker；api-only 禁用新增资产。未为页面放宽后端预算。
- 1440/390/320px 均无页面级横滚；表格、源码和字段选择器各自在容器内滚动。键盘可开关导航、返回和切换字段。截图为真实界面观察，不宣称已完成用户可用性研究。

开发期曾发现选中导航装饰符改变无障碍按钮名、测试定位需随字段阅读更新等问题，均完成修正。曾因测试进行时修改副本而触发平台摘要变化；最终在同一冻结副本重跑，未放宽快照或门禁，也没有将早期失败算作通过。本轮只运行上述有意义的相关用例，不代表整个仓库全套测试通过。

## 打包与最终离线导出

11 项 Web 资产在 wheel 和 sdist 中逐字节匹配集成源码；development 的唯一 Skill、脚本及 9 份标准同时保留。包清单/摘要见 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/package/archive-verification.json`。

wheel 解包到独立目录，以包路径作为唯一源码来源运行：模块来源 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/package/wheel-extracted/demiflow/__init__.py`。HTTP 实际资源与 CSP、在线 worker、宽/窄屏浏览器、独立 HTML 导出全部通过；没有 checkout/旧仓兜底或共享环境安装。包外还执行了自有 3 行本地 fixture，实际提交并登记固定版本 2 行结果；没有模型或生产 pipeline。运行证据：`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/package/package-runtime.json`。

最终 `file://` 报告保留图、报告与准确源码；所有 js/css/许可均内联，无根路径残留、HTTP(S) 请求、worker 或 sessionStorage。恶意报告文本与混合大小写脚本终止串不能执行；`samples_included=false`、`current_binding=not_live`，没有新增 README/Case/业务样本。导出：`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/package/final-offline.html`。

parser 为未修改 markdown-it 14.1.0 官方发行文件，SHA-256 `38c70a1e7ca91ab40e2d9e6e60129851a717ed1c7d4acbbdd41bf9503791cf68`。额外从官方 npm 固定 tarball 核验 6 个包：发行 JS 与本地逐字节相同，各源包摘要及许可全文与随包内容一致。见 [独立封存核对](assets/forge-web-redesign-implementation-20261104/vendor-audit.json)。MIT 本文及浏览器依赖许可全文随包和离线报告保留，来源记录在 `markdown-it.provenance.json`。上游发行 tarball 没有构建 lockfile；本次封存与许可核对不构成完整二进制 SBOM 或可复现构建证明。

## 发布与回滚

发布时先停止原查看服务 PID 64057，然后发布完整资产和壳层，最后沿用原命令、原 registry、原 token 文件及 `--enable-runs` 启动 PID 48042。服务器 **59865** 与原本机 **55971** 转发用途保持一致，没有动其他服务或巡检。

发布前核对每个正式目标仍符合基线/迁移配置；备份当前真实字节和 Git 状态。发布后核对全部发布摘要、registry/token、已登记 workspace.json 与当前绑定的真实平台快照摘要一致。已有其他 dirty/untracked 修改及 staging 保留。

随后在正式服务做只读真实浏览器核验，访问五项导航、宽/窄/320px、项目设置和注册对话框取消：**21 个 GET，0 个写请求，0 个 JS 异常**。没有注册/移除/执行/重绑或写真实数据。HTTP 情况详见 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/live-verification.json`。

回滚依据为 `/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/release-files.json`、`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/release.json`、`/tmp/forge-web-implementation-20261104-7_yvkx32/evidence/rollback/` 及 `/tmp/forge-web-implementation-20261104-7_yvkx32/publish.py`：先停本次 viewer，只恢复本次目标文件的备份；仅删除本次新增文件，再以原命令/令牌/登记重启。不回退迁移或其他人的修改，不修改业务/历史/快照。发布脚本在失败时按同一范围回滚；文档/截图是随后写入的交付证据。

## 截图与边界

| 场景 | 宽屏 | 窄屏 |
| --- | --- | --- |
| 空清单 | [1440px](assets/forge-web-redesign-implementation-20261104/empty-1440.png) | [390px](assets/forge-web-redesign-implementation-20261104/empty-390.png) |
| README / 识别缺口 | [预览](assets/forge-web-redesign-implementation-20261104/readme-1440.png) / [原文](assets/forge-web-redesign-implementation-20261104/readme-source.png) | [390px](assets/forge-web-redesign-implementation-20261104/readme-390.png) |
| 普通固定表 Case | [1440px](assets/forge-web-redesign-implementation-20261104/bound-case-1440.png) | [390px](assets/forge-web-redesign-implementation-20261104/bound-case-390.png) |
| 原项目 Case | [1440px](assets/forge-web-redesign-implementation-20261104/original-case-1440.png) | [390px](assets/forge-web-redesign-implementation-20261104/original-case-390.png) |
| 正式服务 | [1440px](assets/forge-web-redesign-implementation-20261104/published-console-1440.png) | [390px](assets/forge-web-redesign-implementation-20261104/published-console-390.png) / [320px](assets/forge-web-redesign-implementation-20261104/published-console-320.png) |

另有 [新建后](assets/forge-web-redesign-implementation-20261104/created-390.png)、[已有项目](assets/forge-web-redesign-implementation-20261104/existing-1440.png)、[wheel 窄屏](assets/forge-web-redesign-implementation-20261104/wheel-online-390.png) 和 [离线报告](assets/forge-web-redesign-implementation-20261104/offline-report.png)。除正式服务核验图外均来自有界自有 fixture。

保留设计取舍：原项目 Case 没有新增可复制独立深链接；任意文档/外部资源读取未开放；离线解析没有在线 worker 的硬超时；字节/token/节点预算不等于进程 RSS 硬限制。未验证其他浏览器或真实系统剪贴板权限。源设计与 roadmap 由主会话维护，本报告不将旧检查、识别结果或迁移历史当作当前认证。
