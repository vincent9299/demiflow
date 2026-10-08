# 原生 Dataset.search_web

网络资源按算子实际执行期间管理；共享 WebSession 的最后一个网络算子结束后即回收，无需等待其后的非网络算子。原生 worker 内的 curl/浏览器连接仍由各后端管理。自动生命周期及 HTTPX 连接管理范围见 [连接管理](connection_management.md)。

可续期检索会话在首次 HTTP 前过期时，允许有界重新获取；已发生 HTTP 后过期则保存 `interrupted` 来源回执和原始 HTTP 证据，不自动重发，不作为目标网站故障，也不停止其他查询。重启和切换代理继续保留未知结果及已消耗预算。

下载可用 `WebSession(fetch_attempt_limit=N)` 收紧新请求的尝试次数（首次也计入，包含回退），须满足 `1 <= N <= retries + 1`。该运行上限不改变请求身份；已完成或未知回执保持原结果，修改或移除上限不会重置已消费的请求。来源选择、图片过滤和具体数值由调用方配置。

下载切换到固定代理池时，可在该池的 `reuse_completed` 中显式声明旧代理或 `{"session_pool": ...}`。文档和图片获取只重用其余请求参数一致的旧回执，保留成功、失败和未知状态及原始证据；此声明不启用旧会话池，也不自动重试旧请求。具体迁移范围由调用方配置。

图片下载并发是调度参数，新图片回执不再将 `ImageFetchPolicy.concurrency` 纳入身份。旧回执曾包含这一字段；变更并发时可用 `reuse_completed_concurrency=[旧值]` 显式兼容，最多16个旧值，每个在1..256内；当前并发的旧格式自动兼容。该迁移也覆盖 `reuse_completed` 指定的旧代理，仍保留失败和未知回执，不重置请求额度。

检索从住宅会话池切换到静态路由时，`WebSession.search_reuse_session_pools=[{"identity": "旧池摘要", "search": 原检索声明}]` 显式授权读取同一日志中的旧池回执。声明必须与当前检索语义一致（只允许代理变化），最多16个池、合计10万个历史profile；不创建旧会话、不恢复旧连接、不清空旧池的熔断记录。完整查询复用原成功、无结果和失败选择；未结算的单源请求保留未知状态，不因切换代理重试。原始来源回执和查询选择保持。历史缺失或超限明确失败。


下载 HTTP 准入先等待目标主机，再获取共享并发槽位，避免单个繁忙主机的排队占用其他站点的传输容量。每次重定向释放上一跳的主机和共享槽位后再申请下一跳；后续准入等待及传输仍受首次请求的原截止时间约束。每主机、全局连接和上层图片获取的并发上限均保留；不以无界预取绕过下游背压。主机争用、跨主机重定向和超时释放的本地回归见 `tests/test_fetch_admission.py`。

SearXNG 的检索适配器、五类请求处理器、语言/地区数据、结果类型及合并排序代码已随 demiflow 交付。原生路径不启动 SearXNG HTTP 服务，不需要其地址、端口、源码目录或独立 Python 环境。2026-10-01 经用户明确要求，已接入实际 demiflow 工作区与 concepts pipeline；新启动的入口使用原生检索，已运行的进程不会热切换。接入记录见 `native-search-handoff.md`。

## 安装与最小调用

需要 Python 3.11+、POSIX（Linux/macOS）；原生工作进程使用当前 Python 解释器。本次尚未发布到包索引，应安装交付源码或本地 wheel，不能假设索引上的同名 0.2.0 已包含这些改动：

```bash
python -m pip install '/path/to/delivery/dist/demiflow-0.2.0-py3-none-any.whl[search]'
```

从本次源码目录安装可使用 `python -m pip install '.[search]'`。同版本旧包可能被 pip 视为已安装；在准备好的目标隔离环境使用 `--force-reinstall` 安装交付 wheel，不要直接操作仍在运行的业务环境。SQL/Mongo 来源的可选驱动分别在 `search-mysql`、`search-mariadb`、`search-postgresql`、`search-mongodb` extra；MariaDB 驱动另受其系统客户端库要求约束。选择这些来源时仍需配置目标数据库。可选 Chromium 传输见下文；SearXNG 站点服务、偏好和插件 UI 不属于原生执行范围。

## Google 的显式 HTTP 与浏览器后端

业务继续使用同一个 `Dataset.search_web(requests=..., output=..., session=web)`。来源声明可选：

```python
SearchConfig(
    engines=({"name": "google", "backend": "browser"},),  # 或 backend="http"
    language="zh-CN",
    workers=2,
    request_concurrency=2,
    host_concurrency=2,
    timeout_s=45,
    browser={
        "max_requests": 48,
        "max_total_bytes": 8 * 1024 * 1024,
        "max_rss_bytes": 1536 * 1024 * 1024,
        "max_context_queries": 24,
        "proxy_connections_per_worker": 8,
        "ready_timeout_s": 12,
    },
)
```

`http` 使用有界 HTTP 获取，`browser` 使用真正执行 JavaScript 的 Chromium。网页来源 `google` 与图片来源 `google images` 均可显式选择这两种后端；省略 `backend` 的来源仍走原适配器，不在运行中热切换。图片来源使用 `udm=2`，解析原图和来源页面的明确绑定，保留 `img_src`、可读标题和可用的 `thumbnail_src`；同一页面的不同原图分别返回。

图片解析器目前识别 `/imgres?imgurl=...&imgrefurl=...` 及成对的原图／页面 DOM 属性。浏览器执行 JS 后读取同一解析契约；不通过抓取页面中任意 URL、把缩略图当原图或把未知布局当空结果来填充候选。源码为平台 `native_search/google_images.py`，没有调用业务 collect 旧代码。图片搜索中图片字节仍被浏览器阻止，下载继续交给 `Dataset.fetch_images`。

如果需要 HTTP 失败后转 JS，可在 `WebSession.search_fallback` 显式声明同一 `google images` 来源的 `backend='browser'`，两边的回执与最终选择均保留。会话池 `interval_s` 限制**每次 HTTP**，包括 JS 子请求；不能直接照搬单次 HTML 搜索的 60 秒间隔，同时只给浏览器 45 秒期限。浏览器资源域名还需与代理链允许范围一致，例如 Google 页面所用的 gstatic/googleusercontent。具体账号、代理和访问节奏仍由调用方配置。

本地真实 HTTP／Chromium 验证覆盖图片 JS 渲染、原图绑定、同页多图、缩略图不代替原图、验证码、缓存及进程清理；这不代表真实 Google 已成功返回图片。线上结论必须依据对应图片后端的回执，不能从网页测试或其他引擎的成功推导。

浏览器为可选依赖，声明配置不启动浏览器。安装 `.[search,search-browser]` 后使用同一 Python 环境执行 `python -m playwright install chromium --no-shell`；当前依赖固定 Playwright 1.63.0 及其配套 Chromium。操作系统浏览器依赖也必须就绪，缺少时返回 `dependency_error`。HTTP 后端无需启动 Chromium。

每个原生 worker 最多一个浏览器、一个上下文和一个查询页面，worker 数决定最大浏览器数量；不同语言/网络上下文及配置的查询数上限触发回收。Cookie 仅存在临时上下文，不读取用户浏览器资料。驱动、下载及 Chromium 临时配置目录放在父进程拥有的 worker 目录，强制停止也会清理。代理及 CONNECT 链来自既有 SearchConfig/Secret 声明，供应商和地区不写入平台。浏览器可能保留空闲隧道；单个 CONNECT relay 的隧道容量为 `max(request_concurrency, workers * proxy_connections_per_worker)`，每 worker 默认 8、最多 32；HTTP 请求仍受独立准入限制。

CDP 在每个 HTTP 请求及重定向发送前暂停，沿用父进程全局/host 准入；每个 worker 同时只允许一个 HTTP 交换。文档、脚本、样式及 XHR/fetch 可用；图片、字体、媒体、子框架、service worker、专用 worker、websocket 和下载不属于此搜索渲染能力。自动弹窗受阻并关闭。子请求、重定向、DOM 输出字节、执行时间和上下文寿命有独立限制。解码字节与进程树 RSS 是观测式保护，存在采样/传输块超出量，不是严格 socket/浏览器堆硬上限；RSS 包括该 worker、驱动和浏览器子进程。取消、超时和执行结束清理本任务拥有的进程及临时目录。

`engine_receipts` 新增 `backend`、`parser_version`、`response_sha256` 和 `browser_metrics_json`。HTTP 子请求继续保存 host、方法、状态、耗时和已观测正文大小，不保存认证信息。`render_required` 表示 JS 门页，`consent_required` 表示同意页；二者与验证码、限流、正常空结果和解析失败分开。JS/同意页和显式浏览器资源上限不触发无效换 IP，也不作为代理故障样本。失败回执仍是失败，不自动恢复旧失败查询。

HTTP 200 本身不是成功条件：必须得到有效标题/来源 URL，或识别明确的正常无结果页面。未知空布局返回 `parse_error`。后端、解析代码及 Playwright 版本参与请求身份；本次平台升级产生新 runtime/profile，旧冻结业务 runtime 和历史账本保持，不把旧验收冒充新版验收。

### 2026-10-03 验收结论：实现已落地，Google 真实可用性未通过

使用 Playwright 1.63.0 / Chromium 153.0.8010.12 验证。`tests/test_google_backends.py` 与 `tests/test_proxy_chain.py` 共 35 项通过，相关原生搜索、Dataset、路由池、恢复及取消回归 148 项通过。实际 Chromium 覆盖 JS 执行、上下文复用、固定回执零网络复读、代理认证、目标站不得取得代理密码、字节/请求/RSS/重定向限制、取消/父管道关闭及临时目录清理；另用真实故障捕获的 CDP 事件序列回放认证重启死锁。

真实探测使用标准 `Dataset.search_web`，请求及失败保存在独立账本，没有修改 concepts 的冻结运行。HTTP 中英文各一次均为 `render_required`。修复后的 browser 在 US 和全球随机住宅出口各实际尝试一次英文查询，分别约 7.05 / 7.70 秒返回 `captcha`；同批中文查询因该出口暂停未发出，不能宣称中文浏览器成功。公司出口和普通有界面 Chromium 的独立对照也进入验证码页。公司与住宅对照中验证码页给出的客户端 IP 摘要不同，说明链式代理确实改变了 Google 看到的出口；仍不能只据此区分 IP 信誉、账户/会话状态和浏览器特征的影响。

修复了三个具体平台问题：CONNECT relay 的 407 缺少 Basic 认证挑战头；自建 CDP Fetch 拦截需要自行处理、并限定代理认证挑战；HTTPS 代理认证后 Chromium 用新 Fetch ID 再次通知同一个 Network ID，旧处理重复申请自己已经持有的 HTTP 名额，造成死锁。增加隧道容量的中间实验没有解决死锁，这一失败也保留，不能将其写成根因修复。

**有效 Google 搜索结果为 0，尚未执行成功查询的 24 条持续性与提并发验收；不能据本次结果切换生产搜索或宣布 Google 已稳定恢复。** 本轮没有模型请求。下一步需要能返回正常 Google 搜索结果的访问条件，再完成中英文相关性、同会话持续复用、并发 1→2 和资源/吞吐验收。已保存失败不会通过换日志自动重试。

完整原始记录和汇总位于工作区 `_demiflow/google_browser_validation_20261003/`：`report.json`、`acceptance_plan.json`、`native_canary_v2/report.json`（HTTP 与首次浏览器失败）、`native_canary_v5/report.json`（修复后 US）、`native_canary_global/report.json`（全球对照）、`transport_trace.log`（无凭据协议事件）、`headed_comparison/preflight_report.json`。这些是本次环境的证据，不随包安装，也不代表第三方 Google 服务的可用保证。

```python
from demiflow import data
from demiflow.collect import SearchConfig, Secret
from demiflow.collect.session import WebSession

web = WebSession(
    cache_path="runs/search/requests.sqlite",
    object_directory="runs/search/objects",   # 供同一 session 的 fetch_documents 使用
    search=SearchConfig(
        engines=("google", "wikisearch", "wikipedia", "mwmbl"),
        workers=4,
        request_concurrency=2,
        host_concurrency=1,
        host_interval_s=1,
        retries=0,
        # 如需代理，通过当前进程环境提供；不要把凭据写入配置文件。
        # proxy=Secret("SEARCH_PROXY"),
    ),
)

pipeline = data.from_items([{
    "concept_id": "opaque-id",
    "requests": [{
        "request_id": "query-1",
        "query": "天牛",
        "language": "zh-CN",
        "bindings": ["opaque-id"],
        "pageno": 1,
    }],
}]).search_web(requests="requests", output="search_results", session=web,
              concurrency=2, request_concurrency=1, queue_depth=2)

rows = []
stats = pipeline.map(lambda row: rows.append(row) or row).run_stream()
```

Dataset 管理 session 关闭。直接使用异步 `WebSession.search` 时，调用者应在 `finally` 中 `await web.aclose()`。同一 session 属于一个执行 action/event loop；多个 pipeline 应各自声明 session。多个 session 可以共享同一缓存文件，实现相同请求的互斥与重用。

`bindings` 和行 ID 是不透明业务标识。平台不识别 P1/P2、科属种、概念通过与否，不生成业务侧子 pipeline。

## 请求与来源配置

| 请求字段 | 行为与边界 |
| --- | --- |
| `query` | 非空查询；保留所选适配器自己的查询语法，例如词典的 `en-de hello`、货币的 `10 USD to EUR`、URL 搜索。 |
| `language` | 明确的 Babel/SearXNG locale，如 `en`、`zh-CN`、`ja`；可在 SearchConfig 设置默认值。没有默认值就必须逐请求声明，不按文字猜语言。`all` 是显式选择；`wikisearch` 拒绝 `all`。 |
| `pageno` | 从 1 开始。来源不支持翻页或超过其页数上限时，返回 `unsupported_parameters`。 |
| `safesearch` | 0/1/2。传给适配器；不支持时在 `ignored_parameters` 中说明，不伪造过滤保证。 |
| `time_range` | `None/day/week/month/year`；不支持的来源明确拒绝本次参数。 |
| `engines` | 本 session 已声明来源的子集；不会隐式追加 general 来源。来源名规范化为小写。 |
| `categories` | 对声明来源再取类别交集，不与 engines 做隐式并集。 |
| `engine_data` | 按来源名组织的分页 continuation 数据；下一页可使用 `response_json.engine_data`。 |
| `network` | 选择 SearchConfig.networks 中的请求网络配置，默认 `default`；不接受逐行明文认证参数。 |

`SearchConfig.engines` 支持注册表名称和来源配置字典。字典字段沿用该适配器的 SearXNG 配置，包括 `engine`、`categories`、`weight`、`timeout`、API endpoint 等。显式选择会启用默认 disabled 来源；默认 inactive 来源必须通过字典显式 `inactive=False`。这只代表允许加载，不证明当前可用。

```python
search = SearchConfig(
    engines=(
        "wikisearch",
        {"name": "my api", "engine": "braveapi", "api_key": Secret("BRAVE_SEARCH_KEY")},
    ),
    networks={
        "east": {"proxies": {"all://": Secret("SEARCH_PROXY_EAST")}},
        "west": {"proxies": {"all://": Secret("SEARCH_PROXY_WEST")}},
    },
)
```

代理支持底层 curl_cffi 的 HTTP/HTTPS/SOCKS 及认证方式。适配器的 POST、headers、cookies、Basic auth、TLS impersonation、证书验证等使用同一网络 ABI。不会暗中继承 HTTP_PROXY 等进程代理变量；必须显式引用所需环境变量。这里没有隐式多级代理串联；若出口需要代理链，应提供已配置的链式出口。

凭据使用 `Secret("ENV_NAME")`，普通 `SearchConfig.snapshot()` 只有变量名。凭据通过私有管道传给工作进程，不进入命令行、临时配置文件或普通回执；上游任意日志不直接转发，错误仅返回分类和异常类型。请求缓存身份使用持久私有盐的 HMAC 区分不同凭据。不要自行转储执行中对象的私有属性。SQLite 回执文件权限为 0600。

JSON 配置可用 `SearchConfig.from_mapping(mapping)`，或直接传给 `WebSession(search=mapping)`。例如 `{"engines":["wikisearch"],"proxy":{"secret_env":"SEARCH_PROXY"}}`。精确的 `secret_env` 映射会递归恢复为 Secret；声明和快照不读取环境变量，执行时才解析。语言必须是有效 locale；业务可在配置声明一次，然后写入每条请求。

来源自身的网络别名会解析必要的来源配置依赖，例如 Google Images 复用 Google 的网络声明；不会因此额外选择 Google 执行搜索。SQL、Mongo、Valkey 和 command 等非 HTTP 来源使用自己的协议配置，受来源并发、工作进程和总超时约束；HTTP 代理及 HTTP host 限流不作用于这些协议。

## 返回值、缓存与资源归属

`search_results` 保留请求标识及候选 `url/title/snippet/engines/result_kind`。`response_json` 保存上游完整合并结果：score、positions、类型化图片/文件/代码/论文/键值结果、answers、infoboxes、suggestions、corrections、engine_data。不能用候选投影代替完整结果；例如有效的非 HTTP 键值结果可以 `status=ok` 而候选为空。

`engine_receipts` 逐来源记录状态、attempts、稳定 receipt_id、参数能力及被忽略的参数。每次 attempt 还记录实际 HTTP host、方法、状态码、字节数、耗时和 Retry-After，不记录认证 headers 或请求 URL 查询串。取消或超时会保留中断前已获准发出的请求，尚未收到的状态码、字节数为 null；操作系统崩溃仍可能只留下不确定的占位回执。`parameters_json`、`runtime`、`profile` 用于追溯。保存到 Arrow/Lance 时使用更新后的 `demiflow.collect.contracts.SEARCH_RESULT`，避免旧手写 schema 丢失新字段。

| 聚合状态 | 含义 |
| --- | --- |
| `ok` | 所选来源成功，存在结果（完整类型结果可能没有 HTTP 候选）。 |
| `no_results` | 所选来源均正常执行，未返回结果。 |
| `partial` | 至少一个来源正常完成，至少一个来源失败、暂停或不支持参数；正常来源也可能为空。候选仍可使用，但取证范围不完整。 |
| `search_failed` | 没有来源正常完成。查看逐来源的 captcha/rate_limited/authentication_error/http_error/network_error/timeout/parse_error/initialization_error/dependency_error/configuration_error/unsupported_parameters/suspended/interrupted。 |
| `aggregation_failed` / `response_too_large` | 聚合或输出限制失败；已完成的来源回执保留。 |

部分适配器可能把网页结构变化静默解析为空；平台不能从空列表推断网站确实没有内容。明确抛出的解析错误、验证码、HTTP 限流和正常空响应在技术回执中区分。结果相关性、事实判断及图片可用性仍须下游判断。

| 机制 | 唯一归属 |
| --- | --- |
| 概念行/查询队列 | Dataset stream 的 concurrency/queue_depth；行内 `request_concurrency` 使用固定数量消费者。 |
| 来源执行 | SearchConfig.workers 固定上限；`source_concurrency` 默认 1，`source_interval_s` 控制每来源发起间隔。 |
| 实际 HTTP | SearchConfig.request_concurrency；每 host 的并发与间隔逐 HTTP hop 生效，初始化请求和重定向也计数。 |
| 连接 | 同一工作进程的 curl 会话复用；网络/语言隔离上下文变更会重建工作进程。 |
| 超时/取消 | timeout_s 约束被接纳的适配器操作，包含操作内限流等待；来源配置 timeout 取更小值。排队和冷启动分别受背压及 startup_timeout_s 管理。取消/超时终止并回收所属工作进程组、连接和私有派生索引目录。 |
| 重试 | demiflow 统一控制，默认 0；仅网络/超时及 HTTP 5xx 使用明确额度。验证码、认证及限流不自动重试。移除了上游“零重试仍断连补发”。 |
| 故障暂停 | 每 session、network、来源隔离；验证码/认证/限流立即暂停，连续其他失败达到 failure_limit 暂停。尊重更长的 Retry-After。 |
| 响应缓存 | SQLite 持久化每来源结果及失败；文件锁防止跨 session/进程重复占用同一请求；相同请求的并发等待者共享一次执行。 |
| 适配器令牌与静态查找表 | 令牌为工作进程私有、过期且有容量上限的内存缓存；货币等查找表保持完整 SQLite ABI，派生索引放在父进程管理的私有临时目录。 |

缓存身份包括源码/运行时与依赖版本、全部来源配置、自定义源码与 revision、凭据身份、语言、页数、安全搜索、时间、分页状态、网络和执行预算。回放时 attempts 保留原请求历史，当前新增请求数看 metrics.native_search.http_requests，不能把历史 attempts 再计为新增调用。原生 session 的顶层 search_requests 统计来源尝试，search_queries 统计查询，search_http_requests 统计实际 HTTP 请求。重跑同一身份复用成功和失败；修改配置会形成新身份。取消或崩溃留下 `interrupted`，不会擅自重试可能已经计费的请求。只有尚未发出请求的 `suspended` 回执允许在暂停期满后执行。要明确授权重试已耗尽/不确定的请求，使用新的缓存命名空间并保留旧库。

显式授权的历史代理／会话身份复用同样保留 `interrupted`：先查找完全匹配的成功证据；没有成功证据时返回旧未知回执并保留原 `receipt_id`，不能因为更换 SID 而再发一次请求。查询以每批最多 400 个键读取，额外只保留一条未知回执。已确定的目标站失败仍遵循原路线尝试限制，不把未知结果改判成失败或成功。首次来源执行因本地租约到期而拒绝准入，且 HTTP 计数未增长、没有任何 HTTP 回执及旧尝试时，才将本次新预留结算为无尝试的 `suspended`；有发送证据或无法确认的预留保持未知。

后台维护可能尚未执行第一次检查，因此分配搜索会话时也必须检查 `expiry_margin`，不能只检查“此刻尚未过期”。这与维护线程的空闲会话退役边界一致；不会延长租约、提高并发或移除请求途中租约失效后的保护性停止。验收覆盖后台首次分配、明确未发送时有限重取、持续本地拒绝的上限，以及发送后中断换身份仍零请求回放。

最后一个等待者取消时才中止共享请求；另一个等待者仍在使用时继续执行。进程硬杀意味着自定义适配器的 finally 不保证运行，因此外部有副作用的连接器必须自行提供幂等性；平台保留不确定回执而不自动重发。

max_bytes 限制单次 HTTP 调用累计解码响应（包括重定向），max_results 限制每来源结果数，管道输出有字节限制。它们不是整个 Python 进程 RSS 的硬上限；原始结果总内存仍随所选来源和接受的结果量变化。

## 自定义来源

提供随应用安装的 Python 模块，声明兼容的 request/response 或 offline search、可选 setup/init，以及上游能力字段。setup 应只做配置检查；网络初始化放入 init，受同一请求配额、代理和总超时控制。模块依赖和辅助模块也应随其包安装，包的 `__init__` 不应提前导入私有 `searx`。

```python
# my_sources/catalog.py
from urllib.parse import urlencode

categories = ["general"]
paging = True
language_support = True
api_url = "https://catalog.example/search"

def request(query, params):
    params["url"] = api_url + "?" + urlencode({
        "q": query, "page": params["pageno"], "language": params["searxng_locale"],
    })

def response(resp):
    return [{"url": item["url"], "title": item["title"], "content": item.get("summary", "")}
            for item in resp.json()["items"]]
```

```python
SearchConfig(engines=({
    "name": "catalog", "module": "my_sources.catalog", "revision": "catalog-1",
},), language="en")
```

`revision` 必须覆盖辅助代码、依赖及接口契约变化，平台同时记录入口文件摘要。来源代码是受信任的应用扩展，不是执行任意不可信代码的安全沙箱。适配器 HTTP 必须经过 `searx.network` ABI，才具有平台的逐请求限速、代理和回执保证。

## 来源清单与维护

- `search_source_inventory()`：随安装包交付的机器可读能力/验证清单。
- `native-search-sources.csv`：344 个原始命名来源及新增 wikisearch 的逐项状态。
- `native-search-inventory.json`：全部 257 个原始适配器模块、导入依赖、配置项、加载检查和分别记录的固定响应/真实验证状态。
- `native-search-validation.md`：本次验收及限制。
- `_vendor/searxng/BASELINE.json`、`baseline.tar.gz`、`LOCAL_CHANGES.md`、LICENSE/AUTHORS：精确导入基线与修改记录。

本地供给的 SearXNG 树带已有修改且没有独立 Git 元数据；不能虚构上游 commit。当前版本由完整文件摘要标识。升级时取得明确上游 revision，审查与原始快照的差异，重放有限集成修改，更新清单，运行所有五类处理器、固定响应、并发/取消/代理/缓存及安装包验收，再做预算明确的小批真实验证。不要把“文件已复制”或“可以 import”写成在线可用。

维护命令按顺序为 `tools/build_native_search_inventory.py`（静态扫描并清空验证声明）、`tools/audit_native_search.py`（无网络加载）、`tools/finalize_native_search_inventory.py`（生成随包清单/CSV）。最后一步可显式指定 `--live-report path/to/report.json`，要求报告的 baseline_id 匹配；未提供报告就不会标记任何来源在线通过。配置清单中的 required_configuration 是空值字段和 setup/init 索引参数的静态线索，可能包含可选字段；动态必需项仍以适配器 setup/init 文档为准。类型检查专用导入单独登记，不作为运行依赖。

## 按 Dataset API 的改动表

| Dataset API | 下层模块 | 修改前 | 修改后与边界 |
| --- | --- | --- | --- |
| `search_web(requests=..., output=..., session=...)` | data/dataset.py、collect/operators.py、session.py、native_search/* | 调用 WebSession 的统一语言 HTTP 搜索；外部源码/解释器由托管服务提供。 | 默认无 URL 时走随包原生来源；逐请求参数、完整结果、逐来源回执；保持一行输入对应一行输出和不透明 bindings。 |
| `search_web` 的查询展开 | collect/session.py::bounded | 为每个查询创建 task，再用 semaphore 控制执行。 | 固定数量消费者，任务数 O(concurrency)，结果仍保持请求次序。 |
| `search_web` 的旧显式 HTTP 路径 | collect/web.py、services/searxng.py | 现有托管/外部服务。 | 显式 search_url/service 仍可运行，增加逐请求标准参数；不自动启动新旧双路径，不隐式切换现役业务。旧服务类保留用于协调退役。 |
| `fetch_documents` | session.py、operators.py、web.py | WebSession 共享文档获取、缓存和配额。 | 获取策略与业务配额保持；可与原生检索共享同一个 session，搜索使用独立 SearchConfig，文档 HTTP 继续使用 WebSession 的 fetch 配置；共享 bounded helper 的任务数受限。 |
| `read_documents` | reading.py、documents.py | 文档校验/选择/配额。 | 无实现改动；回归验证保持。 |
| `run_stream` / `save_lance` | 现有资源管理、collect/contracts.py | 执行结束关闭 session，保存旧回执结构。 | 继续由执行器管理资源；更新 SEARCH_RESULT 后可保留新回执及 response_json。未修改公共业务表或新增隐藏 pipeline。 |

## 切换与退役

concepts 入口已完成以下接入；其他仍显式声明旧 HTTP 后端的消费者可按相同步骤迁移。

1. 基于实际未提交工作树做三方合并，保留同期公共文档库等改动。原始 BASELINE.json 记录开发起点；接入前快照、逐文件前后摘要另保存在 `_demiflow/native_search_integration_20261001`。
2. 在应用自身解释器安装 `[search]` 依赖。业务入口使用 `WebSession(search=...)`，移除原 service/search_url/python/runtime_directory/port 参数，声明来源、语言和凭据变量引用。
3. 用平台 SEARCH_RESULT 保存完整回执；部分来源失败时保留成功材料及失败事实。文档获取继续使用同一个 session 的公共文档库。概念判断仍由业务完成。
4. 新运行采用新配置；当前已运行进程自然结束。新旧缓存身份隔离，不把旧 HTTP 回执冒认为原生结果。切换不要求改写 `Dataset.search_web` 的调用。
5. 旧 SQLite、日志、原始响应和配置历史继续保留。只有确认所有消费者已退出后才退役具体旧服务；此次没有停止或删除服务。

```bash
python tools/check_native_search_cutover.py /path/to/current/demiflow
```

此命令只读比较原开发基线、交付副本和指定工作区，不执行切换；已合入的工作区应结合接入记录识别后续改动，不能用旧基线覆盖新工作。

### 2026-10-05 专业图片适配器与尺寸元数据

`searxng-results-4` 对图片/缩略图的协议相对地址按来源页面补协议，并保留合法的`resolution`声明及其`declared_width/declared_height`。不会从未知路径推导高清地址，也不把缩略图尺寸当原图尺寸。非法scheme、带凭据地址仍排除；图片地址和页面地址共同参与候选去重。该规范化文件现在参与native runtime指纹，避免结果投影改变后复用旧身份。

平台提供可显式声明的`module='demiflow.services.engines.<name>'`：`flickr_images`、`inaturalist_images`、`metmuseum_images`、`nasa_images`、`openverse_images`、`unsplash_images`。仍使用原生HTTP准入、请求回执和有界SearXNG工作进程；调用方必须声明revision、来源名、代理域名和预算，不绕开Dataset/search_web。此扩展未修改vendor目录或原目录库存，不能将“模块可配置”写成该源已稳定可用。

Flickr每页最多25条、最多3张尺寸详情（每张最多2次HTTP）；Commons用同一模块的commons_only。iNaturalist最多4条观测、每条最多20张元数据，只针对官方公开S3合同构造原图地址，其余用明确返回的地址。Met每次最多4个对象、每对象最多3张图片；NASA每页20条、最多3次清单请求；Openverse/Unsplash每页20条。尺寸、详细URL选择和方法见各模块注释，真实可用性与业务筛选结论由调用方另留原始回执。


### Additional institutional image adapters (2026-10-05)

Installed adapters `gbif_images`, `cleveland_images`, `artic_images`, `wellcome_images`,
`loc_images`, `usda_images`, and `fws_images` use the normal request/response runtime.
They share pure `image_source_utils`; the declared adapter revision must cover that
helper as well as the source module (the latter is also hashed by SearchConfig).
They do not create business datasets, run visual models, or perform image downloads.

GBIF defaults to 20 records × 3 media (ceilings 50 × 5), and Cleveland to 20 works ×
3 views (ceilings 50 × 5). AIC/Wellcome/LoC/FWS default to 4 results and at most 4
additional detail calls per search (page ceiling 10). LoC additionally limits each
item to 2 resources × 2 images × 20 rendition entries. ARS resolves at most 4
caption pages (ceiling 10). The runtime HTTP byte/redirect/deadline limits apply to
every detail request; parsed JSON/HTML retains at most the bounded source response
and current detail response, plus bounded projected output. This is not an RSS guarantee.

Only explicit URLs or documented IIIF requests are emitted. Metadata dimensions
belong to the selected rendition, and resized AIC height stays unknown until the
image header. AIC width never exceeds declared native width; FWS variants larger
than its declared Original are ignored. Cleveland/LoC/FWS optionally select a
rendition within declared size/byte limits instead of an oversized master. Unknown
metadata remains unknown. ARS challenge/empty bodies are failures, not successful
empty searches. FWS uses the site's public Image search endpoint and reads original
links from its media page; it does not invent a path from thumbnail names.

Native smoke tests demonstrated GBIF, Cleveland, AIC, Wellcome and FWS results in
the task environment; FWS needed an explicit company proxy network override.
ARS/LoC remained access failures. Network observations are time/route dependent;
offline adapter checks cover finite detail fan-out, wrong-rendition dimensions,
upscaling, restricted media, and challenge classification. Smithsonian/Europeana
are not part of these adapters; their credentialed integration is deferred by the user.

Review fixes (2026-10-05): explicit JPEG/PNG MIME metadata now permits extensionless
download URLs; explicit nonimage MIME overrides a misleading `.jpg` suffix. LoC
retains the exact download URL and its own dimensions. All custom JSON image search
adapters require the expected result array; missing/malformed collections are
parse failures rather than successful empty searches. The Met's documented
`total=0, objectIDs=null` remains a valid empty response. Flickr requires its photo
model; a missing model is a parse failure, while an empty legend remains valid.
Eleven offline source regressions passed, including the previously failing LoC
case. No new external download or throughput claim follows from these checks.

Known integration limits: a failed detail request currently fails that source's
whole search page, including earlier rows; native HTTP failure receipts are retained.
Per-detail partial results need a runtime receipt contract before changing this.
Source byte hints remain in raw results, while the normalized image candidate only
projects dimensions; byte limits are enforced by selected-variant logic and the
download transport. Smithsonian also publishes unsigned S3 metadata/media via
https://registry.opendata.aws/smithsonian-open-access/; its API key is not a
prerequisite for a separate bounded batch-metadata integration. That route is not
implemented here. The old GitHub archive redirects readers to the S3 distribution.
