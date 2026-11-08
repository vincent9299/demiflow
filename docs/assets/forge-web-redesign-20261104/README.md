# Forge Web 独立设计样稿

2026-11-04。用本地浏览器打开 [index.html](index.html) 即可；不需安装、构建、网络或运行服务。顶部灰条是审阅场景切换器，不是拟上线的产品导航。所有路径、数据、版本和状态均为虚构示例，不连接正式 Forge，不创建文件、不登记项目、不执行检查或流程。

| 场景 | HTML 入口 | 宽屏截图 | 窄屏截图 |
| --- | --- | --- | --- |
| 无项目：产品说明与起点 | [空状态](index.html?scene=empty) | [1440px](screenshots/empty-wide.png) | [390px](screenshots/empty-narrow.png) |
| 注册后：先读说明、尚未检查 | [登记后](index.html?scene=onboarded) | [1440px](screenshots/onboarded-wide.png) | [390px](screenshots/onboarded-narrow.png) |
| 内容识别不完整：原因与下一步 | [识别缺口](index.html?scene=incomplete) | [1440px](screenshots/incomplete-wide.png) | [390px](screenshots/incomplete-narrow.png) |
| 已有项目：说明、内容与证据 | [项目总览](index.html?scene=overview) | [1440px](screenshots/overview-wide.png) | [390px](screenshots/overview-narrow.png) |
| 已声明文本字段的长 Case | [Case 阅读](index.html?scene=case) | [1440px](screenshots/case-wide.png) | [390px](screenshots/case-narrow.png) |
| 原项目无展示声明的长文本 Case | [原项目 Case](index.html?scene=case&origin=original) | [默认 Markdown](screenshots/original-case-wide.png) | [390px 预览](screenshots/original-case-narrow.png) |

另外保留 [README 原文截图](screenshots/readme-source-wide.png) 和 [新建空项目截图](screenshots/created-wide.png)。新建场景也可以从无项目页点击“新建项目→预览新建结果”。原项目 Case 的长文本默认 Markdown 预览，随时能切准确原文；字段标签使用原键名，不根据题目/答案示例硬编码业务角色。短标量、显式结构化对象等保持原类型。

可交互审阅：五个任务导航、项目选择与设置、模拟新建/登记/移除、README/字段预览与原文、Case 切换/返回/前后历史、字段键盘方向键、业务键多匹配示例、窄屏导航、检查依据变化示例。页面明确标注所有模拟操作。没有真实权限、数据或执行状态被改变。

文件分工：

- `index.html` / `style.css`：本地入口、样式和响应式布局。
- `sample-data.js` / `sample.js`：虚构内容、场景与交互；不得复制替换正式 `app.js`。
- `reading.js`：markdown-it token → 白名单 DOM 的设计验证，不是生产模块。限制输入/token/DOM；HTML 不执行，图片仅占位，未连接的相对引用显示原因。
- `vendor/`：固定版本 markdown-it 的原样发行文件、许可证/通知和来源摘要。运行时无 CDN；通知来源与生产 SBOM 的区别见 `vendor/THIRD_PARTY_NOTICES.md`。
- `qa-results.json`：隔离 Chromium 验证结果。正式平台回归、worker 超时/取消、原文分块、真实深链接、HTTP 静态资产与离线导出接线尚待实施。
- `source-anchors.json`：本次只读核对的源码摘要；正式实施前重新绑定最新状态，不能据此覆盖并行修改。

浏览器使用已有 Chromium 与已有 Python Playwright，直接访问 `file://`；从未启动预览服务或连接 59865/55971。QA 将 HTTP(S) 请求全部拦截并记录，断言没有自动网络请求；有外部安全链接时只供用户主动点击。覆盖 1440×1080、390×844、320×740 的九种基本场景，以及原项目 Case 的两种宽度；检查内容语义、原文切换、恶意 Markdown、字节/token上限、键盘与历史。完整脚本作为本次自有证据保留在 `/tmp/forge-web-design-20261104-ekgudvwu/review.py`。

设计和实施边界见 [设计稿](../../forge-web-redesign-design-20261104.md) 与 [源码方案](../../forge-web-redesign-source-plan-20261104.md)。
