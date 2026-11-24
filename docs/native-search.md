# 原生 Dataset.search_web

网络资源按算子实际执行期间管理；共享 WebSession 的最后一个网络算子结束后即回收，无需等待其后的非网络算子。原生 worker 内的 curl/浏览器连接仍由各后端管理。自动生命周期及 HTTPX 连接管理范围见 [连接管理](connection_management.md)。

可续期检索会话在首次 HTTP 前过期时，允许有界重新获取；已发生 HTTP 后过期则保存 `interrupted` 来源回执和原始 HTTP 证据，不自动重发，不作为目标网站故障，也不停止其他查询。重启和切换代理继续保留未知结果及已消耗预算。

检索从住宅会话池切换到静态路由时，`WebSession.search_reuse_session_pools=[{"identity": "旧池摘要", "search": 原检索声明}]` 显式授权读取同一日志中的旧池回执。声明必须与当前检索语义一致（只允许代理变化），最多16个池、合计10万个历史profile；不创建旧会话、不恢复旧连接、不清空旧池的熔断记录。完整查询复用原成功、无结果和失败选择；未结算的单源请求保留未知状态，不因切换代理重试。原始来源回执和查询选择保持。历史缺失或超限明确失败。


原生检索由 DemiFlow 自己实现（2026-11-24 起不再内置 SearXNG，见下文“兼容性声明”）。父进程负责准入、限速、回执、缓存与取消；私有 worker 子进程加载来源适配器、发起受准入的 HTTP、解析并合并结果。模块：`native_search/worker_network.py`（HTTP）、`sources.py`（适配器加载）、`results.py`（合并）、`errors.py`、`adapter.py`（适配器可导入的辅助函数）、`catalog.py`（内置来源名与默认配置）、`locales.py`（各来源的语言/地区参数）、`engines/`（内置适配器）。内置来源：google, bing, yandex, mwmbl, wikipedia, wikisearch, google images, bing images, yandex images, baidu images, brave.images, sogou images, quark images, duckduckgo images, qwant images, pinterest, wikicommons.images, public domain image archive, pexels, openverse, unsplash；另有 `demiflow.services.engines.*` 的 15 个平台图片适配器（以 `module=` 声明）和只读 SQLite 来源 `sqlite`。原生路径不启动任何 SearXNG 服务；需要其他来源时可用 `search_url` 连接自行部署的 SearXNG，或以 `module=` 提供自定义适配器。2026-10-01 起 concepts pipeline 使用原生检索，接入记录见 `native-search-handoff.md`。

## 兼容性声明（2026-11-24 去除内置 SearXNG）

本次交付删除 `demiflow/_vendor/searxng`（AGPL-3.0-or-later）。下列收窄与身份变化由用户在 2026-11-24 授权裁定。

**不变**

- 接口：`Dataset.search_web`、`WebSession` 各参数、`SearchConfig` 字段与 `from_mapping`。
- 来源声明：`module=` 加 `revision` 的声明方式；业务配置使用的全部来源名（上列 21 个）和 15 个平台图片模块路径。
- 回执与状态：`SEARCH_RESULT` 结构及其 `runtime` 字段（仍为 `native-search-1`）；状态分类和暂停规则。
- 合并：结果的字段、去重、打分和分组顺序。
- 请求构造：各内置来源的 URL、参数、请求头、Cookie 与 TLS 指纹。

后两项与旧实现的对照证据见交付报告。

**收窄**

- **来源名**：内置来源名由 344 个减为上列 21 个，其余名称报 `Unknown search source`。需要更多来源时，可用 `search_url` 连接自部署的 SearXNG，或用 `module=` 自定义来源。
- **处理器族**：只保留 online 与 offline 两类。online_dictionary、online_currency、online_url_search 返回 `unsupported_processor`。
- **非 HTTP 来源**：只保留只读 SQLite。MySQL、MariaDB、PostgreSQL、MongoDB、Valkey、command 来源及 `search-mysql`、`search-mariadb`、`search-postgresql`、`search-mongodb` extras 一并移除；`[search]` 只含 httpx、babel、lxml、curl_cffi。
- **自定义适配器契约**：
  - HTTP 与辅助函数改从 `demiflow.collect.native_search.adapter` 导入，取代 `searx.network`、`searx.utils`、`searx.exceptions`；
  - 请求参数 `locale` 取代 `searxng_locale`；
  - 失败用 `SourceCaptcha`、`SourceRateLimited`、`SourceAccessDenied`、`SourceError` 表示。
- **`services.searxng.SearxNGService`**：不再注入 DemiFlow 的 wikisearch，默认 engines 改为 `('wikipedia',)`；wikisearch 只作为原生来源提供。
- **`search_source_inventory()`**：返回新结构（`runtime`、`scope`、`sources`、`platform_sources`），由 `tools/build_native_search_inventory.py` 生成。
- **Pexels**：不再内置 Web 端密钥，可用 `fallback_secret_key` 提供。
- **Unsplash**：请求的 User-Agent 改为 DemiFlow 标识。

**持久格式**

- 回执 `reason` 中的异常类名改为 DemiFlow 名称（对应的 `status` 不变，下游应按 `status` 判定）：
  - `SearxEngineCaptchaException` → `SourceCaptcha`；
  - `SearxEngineTooManyRequestsException` → `SourceRateLimited`；
  - `SearxEngineAccessDeniedException` → `SourceAccessDenied`。
- 旧回执里的 `searx.result_types.*` 结果仍可解码、合并。
- 由带类型结果产生的 `response_json` 字段集合保持不变（`adapter.main_result`、`image_result`）。旧 KeyValue 类型的标题类扩展字段不再出现。

**源码身份与恢复**

- `runtime_id()` 不再包含 SearXNG 基线摘要，改由 `native_search` 与 `engines/` 的源码摘要和依赖版本组成。
- 因此本版本的检索 profile 与此前不同，相当于新的缓存命名空间：此前的来源回执不会被新运行零请求复用，未完成或新的查询会重新请求。用户已接受这一点。
- 实测的冷读与续跑行为见“兼容性实测”。


当前原生 Dataset 身份/资源准入只接精确 WebSession + 有界 SearchConfig HTTP adapter，
拒绝 browser、managed service、route/session pool、reuse/fallback 的新 Dataset 接线。
下面这些低层资源与历史配置仍保留，不代表全部已获当前原生 Dataset 或队列生产准入。
新队列 producer 用同一 SearchWeb 节点固定每个关键词的有限结果后增量发布，见
[原生任务队列](dataset-fetch.md)。真实供应商可用性仍以各来源自己的证据为准。

## 兼容性实测（2026-11-24）

**实测方法**：在私有目录中使用本地夹具 HTTP 服务，不访问外网；先由 main 9bad3137 的旧实现写入数据，再由本实现读取或续用。结果如下：

| 场景 | 结果 |
| --- | --- |
| 冷读旧缓存 | 旧来源回执（包括类型化的 MainResult）都能解码、合并，没有错误。 |
| 同一缓存、同一配置再次检索 | profile 不同（新的缓存命名空间）：3 个查询各重新请求 1 次；状态和候选 URL 与旧结果一致。 |
| 在旧路线池库上检索 | 打开旧库正常；同样重新请求，状态一致。 |
| 用 `restore_results` 导入旧回执 | 明确拒绝（`Restored result does not match declared request semantics`），没有写入，也没有请求。 |
| 续跑未完成的旧 Dataset 运行 | 本环境的内存准入使旧实现同样无法跑完该管线（`RESOURCE_INFEASIBLE`）；本实现面对这个运行，在任何副作用之前拒绝续跑（`RecoveryContractError`）。 |

**结论**：旧数据可以读取，不会被当作本版本的结果复用；本版本的运行在新命名空间下重新请求，请求预算按新请求计算。

**行为一致性证据**：
- 与旧实现逐项对照，没有差异：
  - 请求构造：9413 个离线用例；
  - 结果合并：13000 个随机用例；
  - 响应解析：21 个来源的合成响应。
- 其中的一个子集固化在 `tests/test_native_search_golden.py`（期望值来自旧实现）。

## 安装与最小调用

需要 Python 3.11+、POSIX（Linux/macOS）；原生工作进程使用当前 Python 解释器。本次尚未发布到包索引，应安装交付源码或本地 wheel，不能假设索引上的同名 0.2.0 已包含这些改动：

```bash
python -m pip install '/path/to/delivery/dist/demiflow-0.2.0-py3-none-any.whl[search]'
```

从本次源码目录安装可使用 `python -m pip install '.[search]'`（httpx、babel、lxml、curl_cffi）。同版本旧包可能被 pip 视为已安装；在准备好的目标隔离环境使用 `--force-reinstall` 安装交付 wheel，不要直接操作仍在运行的业务环境。可选 Chromium 传输见下文（`[search-browser]`）。

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

图片解析器目前识别 `/imgres?imgurl=...&imgrefurl=...` 及成对的原图／页面 DOM 属性。浏览器执行 JS 后读取同一解析契约；不通过抓取页面中任意 URL、把缩略图当原图或把未知布局当空结果来填充候选。源码为平台 `native_search/google_images.py`，没有调用业务 collect 旧代码。图片搜索中图片字节仍被浏览器阻止，原始字节下载使用 `Dataset.fetch`，实际图片解码另行执行。

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
    object_directory="runs/search/objects",   # 搜索声明兼容参数；搜索不写对象
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
| `query` | 非空查询；原样交给所选适配器。 |
| `language` | 明确的 Babel locale，如 `en`、`zh-CN`、`ja`；可在 SearchConfig 设置默认值。没有默认值就必须逐请求声明，不按文字猜语言。`all` 是显式选择；`wikisearch` 拒绝 `all`。 |
| `pageno` | 从 1 开始。来源不支持翻页或超过其页数上限时，返回 `unsupported_parameters`。 |
| `safesearch` | 0/1/2。传给适配器；不支持时在 `ignored_parameters` 中说明，不伪造过滤保证。 |
| `time_range` | `None/day/week/month/year`；不支持的来源明确拒绝本次参数。 |
| `engines` | 本 session 已声明来源的子集；不会隐式追加 general 来源。来源名规范化为小写。 |
| `categories` | 对声明来源再取类别交集，不与 engines 做隐式并集。 |
| `engine_data` | 按来源名组织的分页 continuation 数据；下一页可使用 `response_json.engine_data`。 |
| `network` | 选择 SearchConfig.networks 中的请求网络配置，默认 `default`；不接受逐行明文认证参数。 |

`SearchConfig.engines` 支持内置来源名和来源配置字典。字典中的字段覆盖目录默认值和适配器模块的同名选项，包括 `engine`、`categories`、`weight`、`timeout`、`base_url` 及适配器自己的选项（例如平台图片模块的 `min_short_side`）。`network` 可写选项映射（如 `{"proxies": {"all://": Secret("ENV")}}`）或另一个已声明来源的名字。显式选择会启用目录中默认 disabled 的来源。这只代表允许加载，不证明当前可用。

```python
search = SearchConfig(
    engines=(
        "wikisearch",
        {"name": "museum", "module": "demiflow.services.engines.metmuseum_images", "revision": "met-1"},
    ),
    networks={
        "east": {"proxies": {"all://": Secret("SEARCH_PROXY_EAST")}},
        "west": {"proxies": {"all://": Secret("SEARCH_PROXY_WEST")}},
    },
)
```

代理支持底层 curl_cffi 的 HTTP/HTTPS/SOCKS 及认证方式。适配器的 POST、headers、cookies、Basic auth、TLS impersonation、证书验证等都经过 `native_search/worker_network.py`。不会暗中继承 HTTP_PROXY 等进程代理变量；必须显式引用所需环境变量。这里没有隐式多级代理串联；若出口需要代理链，应提供已配置的链式出口。

凭据使用 `Secret("ENV_NAME")`，普通 `SearchConfig.snapshot()` 只有变量名。凭据通过私有管道传给工作进程，不进入命令行、临时配置文件或普通回执；上游任意日志不直接转发，错误仅返回分类和异常类型。请求缓存身份使用持久私有盐的 HMAC 区分不同凭据。不要自行转储执行中对象的私有属性。SQLite 回执文件权限为 0600。

JSON 配置可用 `SearchConfig.from_mapping(mapping)`，或直接传给 `WebSession(search=mapping)`。例如 `{"engines":["wikisearch"],"proxy":{"secret_env":"SEARCH_PROXY"}}`。精确的 `secret_env` 映射会递归恢复为 Secret；声明和快照不读取环境变量，执行时才解析。语言必须是有效 locale；业务可在配置声明一次，然后写入每条请求。

每个来源有自己的网络选项；`network` 写另一个已声明来源的名字时共用其网络选项，不会因此额外选择该来源执行搜索。只读 SQLite 来源不经 HTTP，受来源并发、工作进程和总超时约束；HTTP 代理及 host 限流不作用于它。

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

自定义来源是随应用安装的 Python 模块，以 `module=` 声明，形式与内置适配器相同：

- **在线来源**：提供 `request(query, params)` 和 `response(resp)`。
- **离线来源**：设 `engine_type = 'offline'`，提供 `search(query, params)`。
- **可选**：`setup(settings)` 做配置检查，返回假值表示拒绝；`init(settings)` 在首次搜索前运行。
- **模块属性**：`categories`、`paging`、`max_page`、`time_range_support`、`safesearch`、`language_support`、`about` 等声明能力。来源配置字典中的同名字段会覆盖它们。值为 `None` 的公开属性表示必填选项。

`params` 的内容：

- `query`、`pageno`、`safesearch`、`time_range`、`locale`（请求语言）、`language`、`category`、`engine_data`；
- 适配器要设置的 `url`，以及可选的 `method`、`headers`、`data`、`json`、`cookies`、`allow_redirects`、`max_redirects`、`raise_for_httperror`、`impersonate`。

`url` 为空表示拒绝本次请求。`response(resp)` 返回结果字典列表。`resp` 提供 `status_code`、`headers`、`text`、`content`、`json()`、`url`、`cookies` 和 `search_params`。

```python
# my_sources/catalog.py
from urllib.parse import urlencode

categories = ["general"]
paging = True
language_support = True
api_url = "https://catalog.example/search"

def request(query, params):
    params["url"] = api_url + "?" + urlencode({
        "q": query, "page": params["pageno"], "language": params["locale"],
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

`revision` 必须覆盖辅助代码、依赖及接口契约的变化，平台另外记录入口文件摘要。来源代码是受信任的应用扩展，不是执行不可信代码的沙箱。

适配器内的其他 HTTP 请求必须用 `demiflow.collect.native_search.adapter` 的 `get`、`post`、`request`、`multi_requests`，才享有逐请求准入、代理、字节上限、重定向规则和回执。该模块还提供：

- `SourceCaptcha`、`SourceRateLimited`、`SourceAccessDenied`、`SourceError`；
- `html_to_text`、`extract_text`、`ecma_unescape`、`size_text`；
- `main_result`、`image_result`：带完整字段的类型化结果，按不含协议的 URL 判重。

## 来源清单与维护

- `search_source_inventory()`：随安装包交付的机器可读清单。内容是内置来源与平台图片适配器的设置、能力和离线加载状态。
- `native-search-inventory.json`、`native-search-sources.csv`：同一清单的文档副本和逐项表格。
- `native-search-validation.md`：历次验收与限制。其中 2026-11 以前的记录描述的是内置 SearXNG 的实现。

维护命令：

- `tools/build_native_search_inventory.py`：离线重建清单，`--check` 只比较不写入。
- `tools/verify_native_search_install.py <wheel> --receipt <path>`：在检出目录之外安装 wheel，确认不含内置副本，并经 Dataset API 跑一次离线来源。
- `tools/validate_native_search_live.py`：预算明确的小批真实请求。

新增或修改内置来源时，要做三件事：
- 在 `catalog.py` 登记；
- 用固定响应写解析测试；
- 记录预算明确的真实验证。

不要把“可以 import”写成“在线可用”。

## 按 Dataset API 的改动表

| Dataset API | 下层模块 | 修改前 | 修改后与边界 |
| --- | --- | --- | --- |
| `search_web(requests=..., output=..., session=...)` | data/dataset.py、collect/operators.py、session.py、native_search/* | 调用 WebSession 的统一语言 HTTP 搜索；外部源码/解释器由托管服务提供。 | 默认无 URL 时走随包原生来源；逐请求参数、完整结果、逐来源回执；保持一行输入对应一行输出和不透明 bindings。 |
| `search_web` 的查询展开 | collect/session.py::bounded | 为每个查询创建 task，再用 semaphore 控制执行。 | 固定数量消费者，任务数 O(concurrency)，结果仍保持请求次序。 |
| `search_web` 的旧显式 HTTP 路径 | collect/web.py、services/searxng.py | 现有托管/外部服务。 | 显式 search_url/service 仍可运行，增加逐请求标准参数；不自动启动新旧双路径，不隐式切换现役业务。旧服务类保留用于运行自行安装的 SearXNG（2026-11-24 起不再注入 wikisearch）。 |
| `fetch` | raw_contract.py、raw_transport.py、execution/raw_* | 旧组合获取与解析入口已退役。 | 每行一个 HTTP/COS 原始对象，独立于搜索资源；解析与读取分离，见 [获取契约](dataset-fetch.md)。 |
| `read_documents` | reading.py、documents.py | 文档校验/选择/配额。 | 无实现改动；回归验证保持。 |
| `run_stream` / `save_lance` | 现有资源管理、collect/contracts.py | 执行结束关闭 session，保存旧回执结构。 | 继续由执行器管理资源；更新 SEARCH_RESULT 后可保留新回执及 response_json。未修改公共业务表或新增隐藏 pipeline。 |

## 切换与退役

concepts 入口已完成以下接入；其他仍显式声明旧 HTTP 后端的消费者可按相同步骤迁移。

1. 基于实际未提交工作树做三方合并，保留同期公共文档库等改动。原始 BASELINE.json 记录开发起点；接入前快照、逐文件前后摘要另保存在 `_demiflow/native_search_integration_20261001`。
2. 在应用自身解释器安装 `[search]` 依赖。业务入口使用 `WebSession(search=...)`，移除原 service/search_url/python/runtime_directory/port 参数，声明来源、语言和凭据变量引用。
3. 用平台 SEARCH_RESULT 保存完整回执；部分来源失败时保留成功材料及失败事实。公共文档库的独立低层消费者保留；新 raw 获取使用 Dataset.fetch，解析另行执行。概念判断仍由业务完成。
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
