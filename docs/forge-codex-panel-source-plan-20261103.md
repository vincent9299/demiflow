# Forge 与 Codex 面板联动：源码级改造方案

日期：2026-11-03。状态：用户已批准，代码已落地；实施与验证见[验收记录](forge-codex-panel-acceptance-20261103.md)。下文为获批源码级方案，实际通过项以验收记录为准。

后续交互目标已按用户意图调整为“Codex 对话、操作台共享事实并更新展示”。后续工作参考[共享工作区修订方案](forge-shared-workspace-panel-design-20261103.md)，不再扩展本文的 Case 讨论流程。

先读[中粒度方案](forge-codex-panel-design-20261103.md)。验收和复审见[矩阵](forge-codex-panel-review-20261103.md)。源码依据是本轮工作区文件，包含既有 dirty 实现，不以 Git HEAD 替代。

## 1 改造范围

仅新增两个生产 JS 文件：`web/view.js`、`web/codex.js`。其余直接扩展现有页面、HTTP 与预览入口。无新 Python 服务包、依赖、框架、数据库、执行器或聊天会话实体。

| 文件 | 计划变更 | 依赖边界 |
| --- | --- | --- |
| `forge/workspace.py` | 预览绑定摘要、单表描述、sample 预期绑定参数 | 原有 table_binding/worker/digest |
| `forge/workspace_worker.py` | table 描述操作；sample/media 核对后用同一解析结果读取 | 原有 preview，禁止第二次 alias 解析 |
| `forge/workspace_view.py` | table GET、expected_binding、源码摘要核对、JS 资源/导出 | 调用 workspace/runs；无宿主 SDK |
| `forge/workspace_cli.py` | table show 复用描述入口；sample/show 增加可选 expected-binding | 原有 CLI 分组及参数保持 |
| `forge/web/view.js`（新） | 视图类型、验证、fragment 编解码、定位文本 | 纯 JS，无 DOM/fetch/Codex |
| `forge/web/app.js` | 统一视图操作、选择快照、读取绑定、草稿与竞态处理 | 复用已有 api/render；调用两个 JS 模块 |
| `forge/web/codex.js`（新） | 官方能力检测、注释、四个站点工具 | 仅使用显式传入的页面函数 |
| `forge/web/index.html/style.css` | 脚本顺序、当前选择与讨论操作的必要样式 | 保留当前页面结构 |
| 相关测试、`docs/forge-workspace.md` | 边界回归、使用说明、真实宿主验收说明 | 测试数据使用自有临时工作区 |

`runs.py/run_worker.py`、检查器、Dataset、原生模型及执行层不因 Codex 接入变更。`lance/preview.py` 现有 row_id 字符串输出、投影和分配限制直接复用。

## 2 Python：数据身份放在工作区层

### 2.1 `workspace.py`

保留 `table_binding(root, workspace, alias) -> (control, ref, uri)`，避免改变已有 inspect/prepare 调用。增加纯函数 `preview_binding(root, control, uri)`，产生包含 `workspace/alias/uri/reference/digest` 的有界值，digest 使用现有 `content_digest({workspace, uri, reference})`。它是读取事实描述，不落盘。

现有 `workspace_context` 的每个 table 增加 `binding_digest`，保留原 `digest = content_digest(control)` 用途。禁止把二者都称为 alias 的“同一个 digest”。run 输出在真正打开时走下述 describe；不要求改 `runs.show` 返回结构。

增加 `describe_table(root, alias, *, expected_binding=None)`，转调原有 workspace worker 的 `table` 操作。返回既有 table show 字段 `status/reference/digest/metadata`，另加 binding。将 CLI 中嵌入的单表描述逻辑归并到这里，保证页面和 CLI 处理普通 alias 与 run output 一致。

`sample_table(..., expected_binding=None)` 只增加向 worker 传递字段；原参数/default 不变。sample 响应新增 binding，原 `version/schema_hash/rows` 等保留。CLI/Python 省略时表示读取调用时解析到的绑定；面板续读、定位和站点工具必须传入。

### 2.2 `workspace_worker.py:main`

`table/sample/media` 共用现有受限 worker：

1. 校验 alias/expected_binding（若给出须是 64 位小写十六进制字符串）。
2. `load_workspace`，调用一次 `table_binding`，生成 preview binding。
3. 如提供预期摘要，比较；不同则在读取业务数据或图片前失败。
4. table 调用 `preview.describe_table`；sample 调用 `preview.sample_table`；media 用此 ref/uri 读取行，并以同一次 workspace/control 解析图片位置。
5. table/sample 返回真实 binding；media worker 也返回 binding，HTTP 保持图片响应体并以 `X-Forge-Binding` 响应头给出摘要。sample 绝不能再次拿 alias 重新解析；media 也不能使用浏览器传来的文件路径。

不通过宽松 `getattr` 猜测能力，不增加自由 operation 分发。仍是有限固定操作分支。已有隔离分配限制、worker 截止时间及底层 pinned 元数据检查保留。

alias 在第 2 步后变化不改变该次已解析引用；返回身份仍对应实际读取。后续请求拿旧预期会失败。本轮没有声称锁住外部表或对文件系统做跨进程事务。

### 2.3 `workspace_view.py`

- 增加 `/api/table?alias=...&expected_binding=...`，单表描述而非全量加载历史查找。
- `/api/sample`、`/api/media` 解析可选 expected_binding；新面板的所有续读路径必须带它。
- media 的 HTTP 响应透传 worker 返回的绑定摘要；页面核对后才创建图片 blob URL，不把模型/图片内容放进 header。
- 本轮涉及的 table/sample/media/source query 使用固定允许键、禁止重复键；保留现有 query/path/body 限额。
- `source_text(root,path,*,expected_sha256=None)` 在同一次有界读取的 bytes 上核对摘要，再 decode。报告节点打开当前源码时带 `report.binding.sources[path]`；摘要不符明确提示当前代码已变，不能用新源码冒充旧报告依据。Flow 表单主动查看当前代码可以省略并标为当前工作树。run-source 保持走封存源码。
- 绑定失败沿用已有 ForgeError/HTTP 400，客户端保留错误内容，不解析错误英文来自动重绑。只对自身可判定状态返回 loading/stale/busy 等页面状态，避免新错误体系。
- 不增加 `/api/codex`、通用 `/api/tool` 或“执行任意命令”接口。业务 API 响应、工具结果、复制引用和注释不回传 token；首次连接仍使用既有 serve 输出的会话地址。

### 2.4 `workspace_cli.py`

`table show` 调用 describe_table；`table show/sample` 支持 `--expected-binding DIGEST`。其余命令保持。这样 agent 即使只用既有终端，也能基于页面引用执行相同的准确读取。preview 的 row_id 在 Python 是 int、JSON/JS 中继续是字符串。

## 3 前端：定位描述与唯一页面操作

### 3.1 `web/view.js`

保持当前无构建步骤的分发方式。使用一个文件级 IIFE 暴露 `ForgeView` 的少量具名纯函数，不建立插件注册表。计划函数为 `validateView/encodeView/decodeView/formatReference`。

所有类型共享：`v=1`、`workspace`（当前服务返回的绝对根）、`kind`。类型字段如下，未知字段或类型拒绝：

| kind | 必需字段 | 可选字段 |
| --- | --- | --- |
| overview/tables/runs | 无 | 无 |
| flows | 无 | entry、params；只定位已存在的 Flow/参数文件，不自动检查 |
| table | alias、binding_digest | columns、offset |
| case | alias、binding_digest、row_id | columns |
| inspection | report_digest | node_id |
| run | run_id | node_id、report_digest（选择节点时二者同时提供） |
| run_compare | left、right（run_id） | 无 |

验证：版本唯一；摘要 64 hex；run ID 按既有 `runs.directory` 格式；节点 ID 在加载报告后验证归属。row_id 必须是十进制字符串，范围 uint64，不经过 Number 转换；columns 至多 16 个名称，单名称最多 256 字符。分页 offset 为非负安全整数；传入后仍由后端验证。workspace/路径/alias 长度与整个描述的字节上限共同约束，不将 URL 作为访问任意目录的授权。

`#view=<URLSearchParams 编码的 JSON>`，解码前先检查 encoded fragment ≤16 KiB，解码后 JSON ≤4 KiB，再校验字段；禁止 `eval`、动态 import、任意 URL。常规 URLSearchParams 负责转义，不手工拼接路径字符串。

描述中无 token。encode/decode 不自己读 sessionStorage，不发请求。旧 token-only URL 认证后仍打开总览。未知 view 版本展示不可打开原因；用户可显式回总览，不能默默替换为第一张表或第一个节点。

### 3.2 `app.js` 状态

直接维护小型普通对象，无状态管理库：

- `view`：当前已成功加载的 view 描述。
- `selection`：当前用户选定卡片的描述与 observed_at；可与 A/B 页的外层 view 不同。
- `viewEpoch/selectionEpoch`：导航和详情读取代次。
- `viewState`：loading/ready/error，必要时带既有 report 状态。
- 一个已有运行表单的 `draft`，最多 64 KiB；只保留输入控件值，不含表正文/凭据/检查报告。

现有 `context` 仍是工作区索引缓存，可刷新；不能成为表实际版本或运行成功的权威。删除被上述状态取代的 selectedTable/currentPage 散落控制，不长期并存两套选择状态。

草稿仅页内保留，重载不承诺恢复；跨页回来恢复同一表单，不能被 agent 导航覆盖另一条 Flow。agent 在草稿未提交时请求切换入口/配置返回 draft_active，用户使用页面明确切换/清空。工具上下文只告知存在草稿，不泄露或自动提交其内容。

### 3.3 `app.js` 操作函数

将已有嵌套能力提到能够直接调用的页面函数，保持渲染逻辑原位，避免 DOM 点击自动化：

- `openView(target, options)`：验 shape 和 workspace → 根据有限 kind 调用现有展示函数 → 等到请求/渲染成功 → 提交 view/selection/URL。没有 execute 通用分支。
- `getContext()`：返回当前已经提交的引用、显示状态与更新时间，控制 ≤16 KiB；loading/error 不冒充 ready。
- `readCase(target, columns)`：验 case 描述，调用带 expected_binding 的原 sample；返回有界字段和真实身份，控制 ≤64 KiB。超限明确失败，不返回缩略内容充当完整结果。
- `refreshView()`：保留目标重新读取；inspection 使用 report_status；run 使用 show；case 用同一绑定。不会调用 inspect、prepare、reconcile 或 bind-platform。

`tables()` 内的 showTable/showCase 变成可明确指定目标的调用；run output 未在当前历史页时，通过 alias 直接 describe，不扫描所有运行。`openResult` 先 describe 准确的 run output，再构造带 binding_digest 的目标；普通 alias 从工作区索引点击时使用索引的预期绑定。`showRun/renderReport/drawGraph` 接收准确 node_id；指定节点不存在必须失败，只有无指定节点的普通打开才可沿用默认首项。

`run_compare` 复用现有 run-compare 请求和展示。按业务键对照仍保留多匹配/缺失语义，不推导行级血缘。A/B 卡片讨论使用被点击那一侧的固定引用。

DOM 更新前及 await 后都检查 epoch，覆盖列表、详情、业务键对照、参数文件读取、报告、源码、证据和媒体。API 可对只读 fetch 使用 AbortController；放弃客户端响应不代表 worker 已取消。POST 的状态以服务器事实为准，不因页面切换自动重发。

导航用 pushState，分页/节点选择的连续变化用 replaceState，back/forward 通过同一 openView 恢复；token 只在启动时提取。无法认证保留 view，显示重新连接入口；不清掉定位再读最新数据。

新查询在发出前还需检查 URL 编码后的完整 request target 不超过现有 8,192 字符限制；view 的 4 KiB 限制不能代替 HTTP query 限制。Unicode 字段名/路径可能放大编码长度，超限要求缩小投影，不能拆分后拼接成声称原子一致的一次结果。

刷新：手动按钮和工具用同一路径；visibility 恢复时仅对当前对象发一次需要的刷新，合并重复请求。run 继续现有状态轮询；检查/平台复核不纳入 1 秒轮询。失败保留错误和最后观察时间，不把旧 checked 标成刚复核。

## 4 Codex 集成模块

### 4.1 装配

`web/codex.js` 暴露 `ForgeCodex.init({getContext,openView,readCase,refreshView})` 与 `discuss(element, reference, comment)`。四个回调均属于 app；模块不自行 fetch、保存 token 或解析 Forge 报告。

普通浏览器或离线模式不注册宿主工具。仅使用已核对的官方接口：`document.modelContext?.registerTool` 与 `document.oai?.annotation?.request`，分别检测。无 user-agent 判断、私有协议、CLI 注入或伪造 API fallback。

一个 document 注册一次；不随 SPA 导航反复注册。记录各工具注册成功/失败，页面生命周期终止由宿主管理。若只有部分能力可用，按能力准确展示；不存在“插件已连接就证明面板已集成”的推断。

### 4.2 工具契约

| name | 输入 | 输出/失败 |
| --- | --- | --- |
| forge_get_context | 空对象，additionalProperties=false | workspace、当前 view/selection、观察时间、ready/loading/error、draft_present |
| forge_open_view | `{target: ViewV1}` | 加载后的 canonical target；workspace_mismatch、invalid target、not found、draft_active、busy 或已有 HTTP 错误 |
| forge_read_case | `{target: CaseViewV1, columns: string[]}`；columns 必填 1–16 | 本次 binding、row、columns；读取超限/版本失效明确失败 |
| forge_refresh_view | 空对象 | 当前 target 和重新读取状态；不返回创建操作成功 |

工具 schema 的公共 target 定义来自 view.js，避免登记 schema 与运行时验证各自手写不同字段集合。实现仅需要普通常量与验证函数，不引入 schema 代码生成器/服务层。

read_case 的 columns 参数表示这一次明确请求的投影，target.columns 仅描述原页面显示范围；响应返回实际读取的 columns，不冒充与旧卡片逐字相同。所有工具先检查参数的类型、允许字段、数组项数和字符串长度，再序列化检查总量；输入最多 16 KiB，禁止先展开任意深层对象后再裁剪。

四个工具的业务数据权限均是只读；open/refresh 的“改变页面显示”在 description 明确说明。即使宿主未强制 JSON Schema，execute 仍调用同一验证函数。工具返回原生 JSON 对象，格式以当前官方 registerTool 协议实测为准；不移用另一套 MCP server 消息封套。

### 4.3 注释上下文

Case 元数据示例（值是设计示意）：

```json
{
  "forge": "case_v1",
  "workspace": "/absolute/workspace",
  "table": "run:r0123456789abcdef01234567:target",
  "binding": "64位真实绑定摘要",
  "row": "8589934592"
}
```

这是最多五个字段的紧凑引用，binding 覆盖实际 ref/版本/位置，无需把整份 DatasetRef 放入受限元数据。节点使用 forge/workspace/report/node，加可选 run；运行使用 forge/workspace/run。columns 可从可见卡片和工具读取选择获得，不能声称模型已经读了未显示字段。

构造时先验证所有键/值及 UTF-8 总长度。工作区路径或 alias 超过宿主单值限制时，关闭本次原生注释入口并显示原因；保留正常查看与完整有界定位文本，不截断身份、压入隐藏脚本或引入 context-ID 服务。

每张可讨论卡片在渲染成功时就持有 immutable reference。点击时同步调用 request(connectedHTMLElement,{metadata,initialComment})，不先网络请求。页面只展示“已请求打开注释”，发送与否归宿主所有；没有可靠事件时不自行补一个 sent 状态。

## 5 装载、导出与分发

index.html 顺序为 view.js、codex.js、app.js，继续同源 defer 脚本；不为此加入 bundler/npm 运行依赖。HTTP assets 白名单增加两项，保留 CSP。

export_report 不可只替换旧 app.js 标记。导出时内联通用 view/app 脚本，移除 codex.js 标签；offline 分支不读取存储、不认证、不注册工具、不创建网络链接。使用现有大小上限并检查脚本嵌入转义。离线报告仍显示 not_live，不能恢复真实表。

MANIFEST.in 与 package-data 已覆盖 web/*.js，不预设需要改打包规则；通过 wheel/sdist 内容和包外启动验证确有新资源。平台 source_inventory 当前包含 JS/HTML 等全部受管文件，本轮不为避免 stale 改快照过滤。升级后当前平台绑定需要按已有显式流程更新，旧运行按其封存内容查看；不修改用户正在使用的工作区或服务。

插件分发另行处理：可在集成验收后将 workspace 使用说明包装为独立 Skill/插件。当前设计不需要新 MCP 服务，也不修改现有 v1 Skill 的契约。安装、更改客户端设置、切换服务都不是“方案完成”的副作用。

## 6 测试改造位置与阶段门禁

| 测试位置 | 内容 |
| --- | --- |
| `tests/test_forge_workspace.py` | 描述/采样身份、alias replace/detach、位置变化、媒体、CLI expected-binding、鉴权、源摘要 |
| `tests/test_forge_workspace_browser.py` | 刷新/back/forward、表/Case 定位、双侧选择、离线导出 |
| `tests/test_forge_run_browser.py` | run/node/结果/对比定位、SSH 不同本地端口、prepared 失效、历史与当前源码 |
| 新 `tests/test_forge_panel_browser.py` | 刻意乱序、草稿保护、超限、错误目标、宿主 API 缺失/拒绝/部分注册的浏览器验证 |
| 新 `tests/test_forge_codex_panel_browser.py` | 官方 API 形状的注入测试、真实鉴权 HTTP、工具和人工操作同结果；明确只是自动化协议测试 |
| 后续验收文档 | 真实 Codex 客户端/版本/SSH 映射及注释发送、工具发现/定位截图与事实；不记录 token |

先做宿主最小实验，再做身份/通用面板、接入宿主、端到端。每阶段 Review 已实现 diff 与对应矩阵；失败修正后只重跑受影响门禁和必要回归。

用户行为验收优先于实现同构测试：替换 alias 后旧链接必须失败；乱序时用户最终点击的行必须保留；用不存在节点链接不能打开首节点；只读工具执行前后 report/run/模型请求计数不得增加。相关旧 Forge/算子回归与打包测试通过后，再进行真实宿主闭环。

## 7 必须保留的保证边界

1. 官方支持说明、自动化 mock、实际客户端成功分别记录；不能互相替代。
2. 注册成功不等于获得宿主权限，打开注释不等于发送消息，发出导航不等于页面加载成功。
3. JS/HTTP 取消与运行取消不同；浏览器重连不会自动恢复计算或重新启动 run。
4. CLI 可在 HTTP runs_disabled 时执行其已授权操作；页面开关不扩展成 shell 安全策略。
5. preview binding 不变不代表外部图片字节不变、业务键唯一或逐行血缘存在。
6. 设计阶段的自审不充当实现证据；实施后的自动化测试与真实客户端验证分别记录，不能互相替代。
