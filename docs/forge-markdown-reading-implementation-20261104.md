# Forge 独立 Markdown／原文阅读组件交付

日期：2026-11-04。状态：**组件已实现并通过独立测试，尚未发布；不是整个 Forge Web 改版验收。**

唯一代码工作副本为 `/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow`。正式仓库只新增本文；正式源码、业务、数据、Notebook、training、共享环境和原服务未由本任务修改、重启或调用。后续由主会话 Review，再由集成负责人统一接线与发布。

已读工作区及仓库 AGENTS.md、改版设计与源码方案、阅读样稿及 vendor 来源。本任务是明确授权的平台组件工作，不使用业务 pipeline 开发流程。未向其他聊天发送消息。

## 精确交付文件

下列八个文件均为指定工作副本中的新增文件，baseline 中不存在；交付时正式运行目录均不存在这些文件。

| 文件 | 字节 | SHA-256 |
| --- | ---: | --- |
| [reading.js](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/reading.js) | 25443 | `aa081d828bdc29682875026c520d0551cb186974e473390ce9af76d38b2de70e` |
| [reading.css](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/reading.css) | 3001 | `90a269c46458967c7d16e2e22b03f1a966ce9cb911880ab5ef3a6e55aa9eb786` |
| [markdown-worker.js](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/markdown-worker.js) | 228 | `5502dd763def7b7fbd680537e258c40a3454a4b579a633de8fe2952ca5362ac0` |
| [markdown-it.min.js](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/markdown-it.min.js) | 123618 | `38c70a1e7ca91ab40e2d9e6e60129851a717ed1c7d4acbbdd41bf9503791cf68` |
| [markdown-it.LICENSE.txt](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/markdown-it.LICENSE.txt) | 1073 | `792c48c5a849a15fdf9e37e8bcf9e6d1dd13b32b46c642a748a0a46a9919d473` |
| [markdown-it.NOTICES.txt](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/markdown-it.NOTICES.txt) | 8182 | `822151a3210d4accfb3bc9653275aa399e33ff7d8582cdaeecbbd9c231c626a2` |
| [markdown-it.provenance.json](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/demiflow/forge/web/markdown-it.provenance.json) | 1407 | `142e9baf5f5ad664aa8bf5a482a1f615f1c0ee93b30b43a20aeb1ccbebf8aea5` |
| [tests/test_forge_markdown_reading.py](/tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/tests/test_forge_markdown_reading.py) | 23456 | `954cf01c730895270fa8ec243f4554bb2dcd01f218fd869948319b36ca85280d` |

`/tmp/forge-web-implementation-20261104-7_yvkx32/baseline.json` 保持原样，SHA-256 为 `ce6f56e7a81be6420d9eea0b7d8ed7f3c62a0f3228f89402cad21739eed70b2d`。交付时正式仓库 app.js、view.js、style.css、index.html、workspace_view.py、pyproject.toml 的摘要仍匹配该 baseline。独立清单见 [delivery-manifest.json](/tmp/forge-markdown-reading-20261104/delivery-manifest.json)。其他负责人在共享副本的修改不属于本组件交付范围。

## 固定接口与调用方职责

公共 API 只有 `globalThis.ForgeReading.createTextReader` 及 frozen `limits`，不增加 Case／项目状态框架。

```javascript
const reader = globalThis.ForgeReading.createTextReader({
  text,                         // 必须为原始 string
  defaultMode: 'preview',        // 默认 preview；也支持 source
  identity,                     // 调用方提供的无凭据身份／代次
  resolveLink,                  // 可选，同步返回 null 或 {label, open}
  offline: false
});
host.replaceChildren(reader.element); // HTMLElement
// 切换字段、对象、项目或卸载页面前：
reader.dispose();                // 幂等；调用方随后移除／替换 element
```

identity 只保留在组件自身 owner 标记中，用于隔离晚到结果；不读取项目字段、不序列化、不进入 worker 消息或日志。每个 reader 另有独立 owner、generation 与 job 序号。调用方继续负责 HTTP 结果的新鲜度、视图 epoch、Case 行版本、权限、图片／证据操作及布局；必须先核对现有绑定，再挂载组件，替换时 dispose 旧实例。

README、显式 text 及普通长字符串何时调用组件由 app.js 决定。组件不按 `question/answer` 等业务字段名猜角色，也不接管短身份标量、源码、配置、JSON、日志、image/evidence 或结构化对象的类型分派。不要为所有折叠字段／表格行同时创建 reader；表格摘要仍由调用方保持有界纯文本。

`resolveLink(href)` 是同步、无请求的已支持对象分类器，只能返回已有能力的操作；真正读取放在 `open()`，由用户点击触发。模块只向 resolver 提交严格相对路径，拒绝 `..`、隐藏段、绝对路径、协议相对地址、冒号、query、反斜线、控制字符及 percent 编码歧义；`./` 前缀可用。首期不支持的编码路径等保持可读并显示完整“引用未支持”说明。label 限 200 个 UTF-16 单元，过限视为未支持。

HTTP(S) 使用 URL 校验：无用户密码、控制字符或反斜线、无已识别凭据 query／fragment 参数；凭据 key 会解码并处理大小写／camelCase。仅安全 `<a target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer">` 可活动，不追加 token、授权头或当前页面引用。文内锚点使用组件内 slug Map 与按钮滚动／聚焦，不设置全局 id 或 `location.hash`，不会覆盖 Forge `#view` 协议。缺失锚点有可见提示。

## 解析、DOM 与资源边界

固定 markdown-it 14.1.0，`html:false`、`linkify:false`、`typographer:false`、`maxNesting:16`，无插件。支持标题、段落、强调／删除线、列表、引用、行内／块代码、分隔线、表格和安全链接。不调用 parser.render，不使用 innerHTML；所有内容只进入白名单 DOM／Text。未知 token／失配结构整段退回原文。

脚本、HTML、事件及 SVG 作为文字。Markdown 图片只生成含 alt 的未加载占位，不生成 img、iframe、SVG、媒体元素、资源样式或文件读取。代码块只读，不高亮、不执行。

| 阶段 | 实际边界与行为 |
| --- | --- |
| parser 前 | JS 长度先拒绝，再逐码点核算 UTF-8 ≤65536 字节、行数≤4096；CR、LF、CRLF 正确计行。没有先 encode/split 整个超长输入的分配。超限不启动 worker，整段退回分页原文。 |
| parser | `maxNesting:16`；检测到 token 达到解析嵌套上限即整体降级，避免 markdown-it 饱和时产生部分结果。累计 token（包含 children）≤6000。第三方 parser 先生成自身 token 对象；此处分配不宣称被 token 预算或 RSS 硬上限约束。 |
| 在线调度 | 模块共享一个固定同源 `/markdown-worker.js`，仅一个运行 job 和最多一个最新待办；新待办替换旧待办并让后者明确退回原文。不建缓存池、不自动重试。 |
| worker 启动／运行 | 启动脚本及固定 imports 的就绪预算为2000 ms；ready 后输入提交开始另计500 ms，包含 parser 初始化、解析、投影和结果编码。到时终止 worker。主线程处理结果前复核实际 elapsed，不发布已过期结果。 |
| 结果消息 | 逐项投影前计账，计入重复展开的 href、JSON 转义、所有字符串／节点／分隔符，并预留256字节 envelope；单 href≤2048 UTF-8 字节。预算通过后才 stringify／encode，成功包作为实际≤512 KiB 的 UTF-8 ArrayBuffer 转移，避免结构树的大型克隆。主线程在解码前再检查 buffer 长度。错误／ready 消息为固定小对象，不发送部分树。 |
| DOM | 脱离文档构建，树深≤24（计 section/content 祖先），含 Text／占位／额外链接说明的节点预算≤8000；另预留32节点覆盖固定工具栏／状态等。完成后才一次替换发布，失败丢弃整树和已绑定监听器，退回原文。 |
| 原文 | 以4096个 UTF-16单元为基础划块，边界避开有效代理对，单块最多约12 KiB，因此严格低于16 KiB；当前只挂载≤4块、≤64 KiB。前后页按 O(1) 边界定位，无全量 split／分页索引。显示总 UTF-16长度、当前准确半开区间、页数及本页字节，所有页可查看；复制仅复制当前页原始 slice。原始 CRLF／空白／NUL／组合字符均保留，不静默截断。 |

500 ms 由主线程计时器与 elapsed 检查实施；浏览器主线程挂起时，实际 terminate 调用仍可能晚于该时刻，不能声称实时抢占。超过 elapsed 的结果仍会被拒绝。500 ms 不约束主线程有界 JSON 解码／DOM 构建、HTTP、业务执行或浏览器进程内存。

峰值对象包括调用方原始文本、最多两个已准入 job 的输入引用／worker 克隆、隔离 parser 内部对象、有界投影、预算通过后的有界 JSON 字符串／UTF-8缓冲、主线程解码对象和脱离文档的 DOM。原文保留输入字符串引用，并只额外挂载当前页；这些字节、节点、队列和时间预算均不冒充浏览器 RSS 上限。

完成、失败、超时、模式切换或 dispose 后清计时器、解绑 worker 回调、terminate worker、释放 job 的 text/owner/deliver；移除或替换待办会清其引用。模式切换／dispose 清旧 DOM 和所有内容监听器，dispose 还清按钮监听器、原始文本、resolver 与 owner。取消后的 worker 回调及异步引用错误都受 owner/generation/模式核对，不覆盖新选择。只有用户重新选择预览才新建 job，当前模式重复点击不会重解析。

## 离线和集成接线

`offline:true` 不启动 worker、不 import 外部脚本、不调用 HTTP／storage。已内联 parser 可按同一输入／token／消息投影／DOM预算同步预览；界面明确说明**没有在线 worker 的500 ms超时保证**。parser 缺失或预算失败仍能读准确原文。现有离线报告原文路径应继续可靠；建议报告的可靠主路径使用 `defaultMode:'source'`，需要预览时再显式切换。

集成负责人还需完成以下接线；本组件未修改这些文件：

1. 在线 HTML 载入本地 parser → reading.js → app.js（view.js 原顺序不变），以及 reading.css；建议 CSS 在主样式之后。worker 不加到页面 script 标签。
2. HTTP 静态白名单逐项允许 `/reading.js`、`/reading.css`、`/markdown-it.min.js`、`/markdown-worker.js`；worker 固定 `importScripts('/markdown-it.min.js', '/reading.js')`，复用同一解析核心，不产生 blob worker。CSP 保持同源并增加 `worker-src 'self'`，无需 unsafe-inline/eval 或外部域名。
3. wheel／sdist 保留 js/css，明确纳入两个 txt 和 provenance JSON；许可证／provenance 不要求新增公开 HTTP 路由。所有新增单文件低于现有512 KiB静态资源上限。
4. 离线 HTML 内联 parser、reading.js 和 reading.css，正确转义大小写不敏感的 script 终止序列，不留下根路径脚本／样式引用；调用阅读组件时始终传 `offline:true`。不内联 worker，不增加业务样本／README／Case 或当前 .forge 载荷，保留16 MiB总上限与既有离线状态说明。
5. README、普通／原项目 Case 调用点由 app.js 负责人接入，保持原请求、版本、选择 epoch 和 dispose。所有组件 CSS 选择器都以 `.forge-reader` 开始；子类为 `.reading-toolbar`、`.reading-status`、`.reading-content`、`.reader.reading-preview`、`.reading-source-nav`、`.reading-source-chunk`、`.md-table-wrap`、`.reading-link`、`.reading-link-reason`、`.reading-unsupported`、`.reading-image-placeholder`。主题可覆盖 `--reading-text/surface/border/accent/muted/code-surface`；字体继承，不含外部字体或资源。

## 依赖封存与许可范围

采用样稿已核对的**未修改官方14.1.0浏览器发行文件**，不是重新打包构建。来源为 [markdown-it 14.1.0官方发布](https://github.com/markdown-it/markdown-it/releases/tag/14.1.0)、[npm官方 tarball](https://registry.npmjs.org/markdown-it/-/markdown-it-14.1.0.tgz)；tarball SHA-256 为 `7c4fab4d4d39fe98206a7fbbed4b1e3578f2b70b6b69d309413e33eec140e19f`。实际 JS、许可证、通知的摘要见上表及 provenance JSON。

markdown-it 本体为 MIT，保留其实际全文。NOTICES 原样携带浏览器依赖通知全文：entities 的 BSD 许可、linkify-it、mdurl（含 Joyent 通知）、punycode.js、uc.micro 的 MIT 通知及各来源 tarball 摘要。CLI-only argparse 不在浏览器 bundle 中。

通知来源版本匹配上游 manifest 范围；上游 tarball 没有构建 lockfile。**这些来源版本／范围不是已验证完整二进制 SBOM，不声称可复现构建或完成额外生产依赖审计。** 若发布要求锁定全部传递依赖并重建，该验证尚未做，主会话需保留此边界。没有下载新运行时、CDN、产品构建服务或修改共享环境。

## 验证与证据

最终结果：**19 passed，0 failed／error／skip，16.36 s**。只运行新增阅读测试，未跑整套 Forge。

```bash
FORGE_CHROMIUM=/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome \
PYTHONDONTWRITEBYTECODE=1 /yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest \
  /tmp/forge-web-implementation-20261104-7_yvkx32/demiflow/tests/test_forge_markdown_reading.py \
  -q -p no:cacheprovider \
  --basetemp=/tmp/forge-markdown-reading-20261104/pytest \
  --junitxml=/tmp/forge-markdown-reading-20261104/pytest-results.xml
```

复用已有 pytest／Playwright／Chromium；HTTP fixture 只绑定自有 loopback 临时端口，不访问原服务。JS 两个模块的 `node --check` 通过。

覆盖真实模块与 worker 的正常 Markdown、准确原文字符及分页／逐页复制、恶意 HTML／SVG／脚本和各种图片零请求、用户点击链接的无 opener／referrer／授权头、相对 resolver 与不支持路径、Forge hash 不变、UTF-8／CRLF／嵌套／token／消息／链接／DOM 超限、实际转移缓冲字节、500 ms运行超时与独立2000 ms启动超时、单 worker／最新待办、模式取消／dispose／真实晚到结果、file URL 内联离线零 worker／零 storage／零资源请求。节点测试包含 worker 投影通过、主线程额外链接说明使节点超限的情况。

故障 fixture 只在实际 worker handler 外加入阻塞延迟，或暂存真实结果回调后晚投递；不替换 parser／预算，不镜像组件实现。首轮发现 markdown-it 嵌套饱和可产生部分结果后，现实现已对该边界整体降级，并保留用例。

证据均在自有 `/tmp/forge-markdown-reading-20261104`：

- [最终 JUnit](/tmp/forge-markdown-reading-20261104/pytest-results.xml)
- [交付摘要清单](/tmp/forge-markdown-reading-20261104/delivery-manifest.json)
- [真实载荷与阶段测量](/tmp/forge-markdown-reading-20261104/measurements.json)
- [390px组件截图](/tmp/forge-markdown-reading-20261104/reader-390.png)、[1440px组件截图](/tmp/forge-markdown-reading-20261104/reader-1440.png)

最终版本的独立 headless-shell 测量：普通文档启动36.0 ms、ready后结果6.9 ms，实际成功包770字节／预先计账940字节；240个重复长引用输入3993 UTF-16单元，启动15.5 ms、结果22.3 ms，实际成功包443154字节／预先计账443319字节。这里只是当前 fixture 的单次测量，不是浏览器性能或硬实时保证。400次相同长引用触发消息预算整体回原文，未先发送巨大结果。

## 未验证／未发布范围

主应用的 README、原项目及普通 Case 接入、字段默认选择规则、业务版本／绑定竞争、真实对象 resolver、整页导航／窄屏／键盘操作、实际服务 CSP／白名单、wheel／sdist 和 export_report 最终离线包仍由集成负责人验证。本轮 file URL 测试是组件隔离夹具，不是最终导出报告验收；截图也只展示独立组件。未验证其他浏览器、真实系统剪贴板权限（逐页复制使用浏览器剪贴板替身核对准确文本）、RSS峰值或完整构建 SBOM。

组件测试不能替代 Web 集成验收，本文及副本交付都不表示允许自行向原服务发布。
