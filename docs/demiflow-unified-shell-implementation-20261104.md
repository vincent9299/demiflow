# DemiFlow 统一侧栏、布局与品牌交付

日期：2026-11-04（Asia/Singapore）。承接用户要求复用 Harness 侧栏并与紧凑顶部合并，以及将界面品牌统一为 **DemiFlow**。

## 实际变化

- 同一套项目布局：左侧项目逐行展示，右侧显示当前项目的五个功能 tab。复用本机 Harness 0.2.1-rc.2 的 `ui-sidebar/SidebarRoot.module.css`、`ui-workspace/rows/WorkspaceBrowser.module.css`、`rows/Rows.module.css` 的结构与视觉参数，与已采用的紧凑标题栏统一。来源包与文件哈希见收据；MIT 许可保留在样式中。
- 侧栏品牌、项目区标题与管理入口集中：DemiFlow 标识位于顶行；新建与注册为“项目”行内的图标按钮，具备明确名称、悬停提示与键盘焦点。项目行使用文件夹图标与灰色选中背景。
- 桌面侧栏可在 264px 和 56px 间切换。窄栏以项目名称缩写呈现每个项目，完整名称与目录可悬停查看，暂不可用状态仍有提示。折叠不丢失项目及 tab，当前页面会话内刷新会保留侧栏偏好。
- 手机端继续使用完整项目抽屉，桌面折叠偏好不影响抽屉中的项目名称。Escape、收起按钮与项目选择均保留关闭和焦点返回行为。
- 左侧标识、浏览器标题、欢迎页、帮助、概览说明、移除提示及离线报告统一使用 DemiFlow。技术协议标识与原项目内容按原事实读取；未批量替换业务说明或历史证据。

## 验收与发布

11 项相关浏览器验收通过，包括折叠/展开、刷新恢复、宽窄屏、键盘、项目切换与各自 tab 记忆、新建/注册/移除、只读 README、状态浮层与离线报告。最终实际页面以 1440 / 390 / 320 像素核验，36 次 GET，0 次写入、0 浏览器错误、0 HTTP 错误。顶部仍为桌面 76px、手机 72px。

本次只发布六份文件：`demiflow/forge/web/app.js`、`style.css`、`index.html`，以及 `tests/test_forge_web_redesign.py`、`test_forge_project_browser.py`、`test_forge_project_removal.py`。前端只接既有 HTTP 和视图协议；未新增后端接口。

发布前停止唯一查看服务，确认原端口可重新绑定后再统一更新，使用捕获的原命令、工作目录与环境启动。当前 PID 为 58813，端口仍为 59865 / 55971。保留 registry、token、运行权限开关、真实项目 workspace.json、绑定平台快照与 Git 暂存区；未执行真实项目或重写检查依据。已冻结的 1002 份包代码/配置与正式文件一致，其他会话改动及此前交付记录保留。

[正式桌面](assets/demiflow-unified-shell-20261104/published-expanded-1440.png) · [折叠侧栏](assets/demiflow-unified-shell-20261104/published-collapsed-1440.png) · [390 像素](assets/demiflow-unified-shell-20261104/published-390.png) · [320 像素](assets/demiflow-unified-shell-20261104/published-320.png) · [手机项目栏](assets/demiflow-unified-shell-20261104/published-projects-390.png) · [多项目 fixture](assets/demiflow-unified-shell-20261104/fixture-multiple-projects-1440.png) · [验证收据](assets/demiflow-unified-shell-20261104/verification.json)

隔离副本、原始基线、差异与完整日志：`/tmp/demiflow-unified-shell-20261104-1ll8e8b3`。服务启动私有参数留在受限证据目录，未复制到仓库。

## 补投交付核验

补投标记：`forge_sidebar_resume:20261104-2127`。核验时间：2026-11-04T21:29:55+08:00。该追加已在本会话完成，且后续真人要求的 DemiFlow 品牌一并交付。

正式服务 PID 仍为 58813；原 59865 / 55971、token、registry、绑定快照和暂存区保持不变。六份交付文件与验收指纹完全一致，其余已冻结代码也未变化。新增 4 次只读 GET 验证了实际服务输出的 index.html / app.js / style.css 与交付字节一致，并核对了现有 HTTP 契约。

最终文件摘要如下（行数基于发布前备份）：

| 文件 | 差异行数 | 摘要 |
| --- | --- | --- |
| `demiflow/forge/web/app.js` | +36 / −18 | 侧栏折叠、项目缩写、管理入口合并、DemiFlow 文案与图标复用 |
| `demiflow/forge/web/style.css` | +15 / −13 | Harness 侧栏/项目行样式、264/56px 布局及移动抽屉 |
| `demiflow/forge/web/index.html` | +1 / −1 | 在线与离线页面标题统一为 DemiFlow |
| `tests/test_forge_web_redesign.py` | +61 / −2 | 折叠与刷新恢复、键盘、宽窄屏、项目切换及品牌验收 |
| `tests/test_forge_project_browser.py` | +2 / −2 | 新建/注册/检查流程中的欢迎页品牌断言 |
| `tests/test_forge_project_removal.py` | +1 / −1 | 移除最后项目后的欢迎页品牌断言 |

[最终实际差异](assets/demiflow-unified-shell-20261104/delivery.patch) · [补投核验收据](assets/demiflow-unified-shell-20261104/resume-verification-2127.json)

差异补丁基于本次侧栏发布前的逐文件备份，而非 Git HEAD。已在隔离目录实际应用补丁，六份结果均与正式文件 SHA-256 一致，可据此 Review 并按运行态候选的基线合并。原 11 项浏览器验收及 7 张截图仍对应当前交付；运行态候选的组合验收绑定其集成后的版本。
