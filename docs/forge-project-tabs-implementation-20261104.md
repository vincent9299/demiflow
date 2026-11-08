# Forge 项目列表与功能 tab 布局交付

日期：2026-11-04。按用户最新要求：左侧一行一个项目，右侧展示所选项目的多个 tab。

左侧项目下拉框改为可滚动的项目列表；长名称保持一行并显示省略号，悬停可见完整名称及路径。项目总览、数据与 Case、流程与配置、规范检查、运行记录改为内容区顶部的横向 tab，当前项目的设置位于页头。窄屏保留项目抽屉，功能 tab 可横向滚动。

每个项目分别记住当前页面会话内最后选择的 tab；快速切换时立即保存选择。浏览器前进后退及定位链接继续选择明确的项目和视图。删除登记时清理该项目的 tab 记忆。项目列表自动同步复用既有项目行，保留暂不可用状态及其他项目的入口。无项目时隐藏功能 tab。

## 验证与发布

- 10 项相关真实浏览器验收通过：多项目/tab 隔离、快速切换、历史与深链接、不可用项目恢复、移动键盘操作、登记与移除、两类固定版本 Case、README、安全静态资源与离线报告、阅读 worker 释放。JavaScript 语法检查通过。
- 三个自有 fixture 覆盖多项目与长名称。正式清单当前只有一个项目；正式服务核验五个功能 tab、项目抽屉及 1440 / 390 / 320 像素宽度，共 25 次 GET，0 次写入、0 浏览器错误、0 HTTP 错误。
- 停止原查看服务后统一发布 app.js / style.css 和两份相应验收文件，再用原命令、环境、端口、token 文件与 registry 重启。59865 / 55971 保持原值。
- 正式 1001 份包代码/配置与已验证副本一致；Git 暂存区、registry、token、真实项目 workspace.json 和绑定平台快照均未改变。未执行真实项目、重绑快照或修改业务数据。历史交付报告保留原值。

[正式桌面效果](assets/forge-project-tabs-20261104/published-1440.png) · [390 像素](assets/forge-project-tabs-20261104/published-390.png) · [320 像素](assets/forge-project-tabs-20261104/published-320.png) · [项目抽屉](assets/forge-project-tabs-20261104/published-project-list-390.png) · [多项目 fixture](assets/forge-project-tabs-20261104/fixture-multiple-projects-1440.png) · [验证收据](assets/forge-project-tabs-20261104/verification.json)

本次隔离副本与完整日志：`/tmp/forge-project-tabs-20261104-5ef7zu7y`。此前 Web 重设计交付记录未重写；本次仅补充项目导航布局。
