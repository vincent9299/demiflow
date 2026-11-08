# 工作面板呈现调整与验收

本轮按用户最新范围，仅调整现有操作台的呈现，不实现开发对话或 Codex CLI 接入。

## 布局与边界

- 左侧工作区／项目列表取自既有 `/api/projects`，按稳定工作区 ID 切换；当前项目高亮，失效项目保留诊断。
- 顶部展示当前工作区与真实运行权限；四个功能入口为项目总览、数据表 / Case、Flows / 检查、运行记录。
- 刷新移至右上角，自动同步状态置于侧栏底部；窄屏时项目列表横向滚动，数据表和接线图保留各自滚动区域。
- `app.js` 只改变页面组装、导航按钮和选中状态。沿用既有 `switchProject`、`openView`、请求失效机制、参数草稿、URL 和自动同步；未增加 API、后端会话、项目实体或执行路径。
- `style.css` 负责布局、选中／焦点状态与响应式呈现；离线报告仍使用同一渲染器。

## Review 与测试

1. 对照修改前备份逐项检查 JS 差异：表、图、运行、Case、权限、提交与持久化逻辑没有被替换。旧项目下拉框已直接移除，没有隐藏副本或双重状态。
2. `node --check demiflow/forge/web/app.js` 通过。
3. 既有真实 Chromium 回归：`test_forge_panel_browser.py`、`test_forge_onboarding_browser.py`、`test_forge_workspace_browser.py` 共 **11 passed**。覆盖深链接／前进后退、过期响应、Case 与图片、每入口草稿、历史 attempt、接线源码／报告和离线导出。
4. 实际服务 **12 项界面检查通过，0 page error**：5 个项目、4 个功能入口、两个真实 Flow、项目切换、浏览器历史、刷新、重载及 320/360/760/1024/1440 宽度无整页横向溢出。最初两次检查脚本在页面尚未完成读取时就后退，现已等待导航完成再验证历史，不改产品路由来迁就脚本。
5. 实际服务 **7 项业务查看检查通过，0 page error**：identity 的 70 节点图、concept_images 的 62 节点图，真实历史 identity Case、关系审核 Case 与实际图片，以及两个业务 fixture 和第三种 inventory Flow 的成功运行／结果查看。历史检查报告仍保留各自绑定；本轮查看不能替代新的契约认证。
6. 使用本机转发入口对应的 Host/Origin 访问服务器：HTML、JS、CSS、项目 API 均 HTTP 200，服务返回的 JS/CSS 与当前源码逐字节一致。

验证目录：`/tmp/forge-onboarding-20261103-b5j6rbhw`。

| 证据 | 内容 |
| --- | --- |
| `p4-layout-browser-tests.log` | 11 项浏览器回归 |
| `workbench-layout-acceptance.json` | 12 项真实页面检查 |
| `p4-console-live-acceptance.json` | 7 项图／Case／运行／结果检查 |
| `workbench-forward-origin-check.json` | 现有转发 Host/Origin 及已部署资产核对 |
| `workbench-layout-desktop.png` | 真实项目总览截图 |
| `workbench-layout-flows.png` | 两个真实 Flow 的新布局截图 |
| `workbench-layout-mobile.png` | 窄屏截图 |

## 现有入口

继续使用本机 `55971` → 服务器 `127.0.0.1:59865` 的既有 SSH 转发，保留原会话 token 和项目登记。刷新现有浏览器页面即可；没有新增转发或替换历史数据。

本记录只验收呈现调整。整体 P4 的最终源码绑定、GP 矩阵汇总和 DemiForge 受管交接仍按实施记录收口，不以页面改版宣称全部迁移或生产验收完成。
