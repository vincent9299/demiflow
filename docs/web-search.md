# 网页检索

`Dataset.search_web` 让每一行发起若干条检索请求，把各个来源的结果合并后写回这一行。检索由 DemiFlow 自己完成，不需要另外部署搜索服务：平台负责限速、并发、缓存、失败记录和取消，来源适配器负责访问具体网站。

安装时加上检索依赖：`pip install 'demiflow[search]'`。需要 Python 3.11+，Linux 或 macOS。

## 快速开始

```python
from demiflow import data
from demiflow.collect import SearchConfig
from demiflow.collect.session import WebSession

web = WebSession(
    cache_path="/data/search/requests.sqlite",
    object_directory="/data/search/objects",
    search=SearchConfig(
        engines=("wikipedia", "wikisearch"),
        language="en",
        workers=4, request_concurrency=2,
        host_concurrency=1, host_interval_s=1,
    ),
)

rows = (
    data.from_items([{
        "id": "c1",
        "requests": [{"request_id": "q1", "query": "longhorn beetle"}],
    }])
    .search_web(requests="requests", output="search_results", session=web, concurrency=2)
    .materialize()
    .take_all()
)

for result in rows[0]["search_results"]:
    print(result["request_id"], result["status"], [c["url"] for c in result["candidates"]])
```

`requests` 列是这一行的请求列表，`output` 列得到同样长度的结果列表。`session` 由 Dataset 在动作结束时关闭；每条管线声明自己的 session，多个 session 可以共用同一个缓存文件。

## 一条请求里能写什么

| 字段 | 含义 |
|---|---|
| `request_id` | 你自己的请求标识，原样带回 |
| `query` | 查询词，必填 |
| `language` | 语言，例如 `en`、`zh-CN`。`SearchConfig` 里设了默认值就可以省略；没有默认值时必须逐条给出，平台不会根据文字猜语言 |
| `pageno` | 页码，从 1 开始 |
| `safesearch` | 0、1 或 2 |
| `time_range` | `day`、`week`、`month`、`year` 或不设 |
| `engines` | 只用本次 session 已声明来源中的几个 |
| `categories` | 在已声明来源中按类别再筛 |
| `engine_data` | 翻页用的续接数据，取自上一页结果的 `response_json.engine_data` |
| `network` | 使用 `SearchConfig.networks` 里的哪一组网络配置 |
| `bindings` | 你自己的业务标识，平台不解释，原样带回 |

某个来源不支持你给的页码或时间范围时，该来源返回 `unsupported_parameters`，不会假装支持。

## 配置来源和网络

```python
from demiflow.collect import SearchConfig, Secret

search = SearchConfig(
    engines=("google", "wikipedia", {"name": "museum", "module": "my_sources.museum", "revision": "1"}),
    language="en",
    workers=4,                 # 同时执行的来源任务数
    request_concurrency=2,     # 同时进行的 HTTP 请求数
    host_concurrency=1,        # 对同一主机的并发
    host_interval_s=1,         # 对同一主机两次请求的间隔
    timeout_s=45,
    retries=0,                 # 默认不重试
    proxy=Secret("SEARCH_PROXY"),
)
```

- 内置来源的名字和能力见 [来源清单](native-search-sources.csv)。
- **凭据只通过环境变量提供**：`Secret("环境变量名")`。配置和记录里只出现变量名，不出现凭据本身。
- 平台不会自动使用 `HTTP_PROXY` 等进程代理变量；要用代理必须显式声明。
- 不同出口可以分组声明，请求用 `network` 选择：

```python
SearchConfig(
    engines=("wikisearch",),
    networks={
        "east": {"proxies": {"all://": Secret("SEARCH_PROXY_EAST")}},
        "west": {"proxies": {"all://": Secret("SEARCH_PROXY_WEST")}},
    },
)
```

- 配置也可以用字典：`WebSession(search={"engines": ["wikisearch"], "proxy": {"secret_env": "SEARCH_PROXY"}})`。

### 需要浏览器的来源

个别来源（例如 Google）可以选择用浏览器访问：

```python
SearchConfig(engines=({"name": "google", "backend": "browser"},), language="en")
```

浏览器是可选依赖：安装 `demiflow[search,search-browser]` 后执行 `python -m playwright install chromium --no-shell`。缺少依赖时该来源返回 `dependency_error`。这些网站是否返回正常结果取决于访问条件，可能出现验证码；请以实际结果里的状态为准。

## 读结果

每条请求对应一个结果对象：

| 字段 | 内容 |
|---|---|
| `request_id`、`query` | 对应的请求 |
| `status` | 这条请求的总体状态，见下表 |
| `candidates` | 候选列表，每项有 `url`、`title`、`snippet`、`engines`、`result_kind`；每条请求最多保留 `search_web(max_candidates=...)` 个，默认 5 |
| `response_json` | 完整的合并结果（JSON 字符串）：分数、位置、图片或文件等类型化结果、`engine_data` 等 |
| `engine_receipts` | 每个来源各自的状态、尝试次数和实际发出的 HTTP 请求记录 |

| `status` | 含义 |
|---|---|
| `ok` | 所选来源成功并有结果 |
| `no_results` | 所选来源都正常执行，没有结果 |
| `partial` | 至少一个来源正常完成，至少一个失败、被暂停或不支持参数。候选可以用，但覆盖不完整 |
| `search_failed` | 没有来源正常完成；具体原因看 `engine_receipts`，例如 `captcha`、`rate_limited`、`timeout`、`parse_error` |
| `response_too_large` | 结果超出输出上限 |

检索失败不会让整个动作失败：它作为状态写在结果里，由你的下游逻辑决定如何处理。

需要注意：网站改版时，个别来源可能解析出空列表而不报错，所以 `no_results` 不等于“网上确实没有”。结果是否相关、是否属实，仍然要由下游判断。

## 缓存、重试与暂停

- **缓存**：每个来源的结果和失败都保存在 `cache_path` 指向的 SQLite 文件里。相同的请求再次执行时直接复用，不再访问网站；多个进程同时发起相同请求时只执行一次。
- 来源配置、语言、页码、网络或凭据任何一项变化，都算新的请求。
- **重试**：默认 0 次。设置后只对网络错误、超时和 HTTP 5xx 重试；验证码、认证失败和限流不会自动重试。
- **暂停**：某个来源遇到验证码、认证失败或限流时，在本次 session 内立即暂停；其他失败连续达到上限也会暂停。被暂停的来源在结果里标为 `suspended`，其他来源不受影响。

## 并发由谁控制

| 层次 | 参数 |
|---|---|
| 同时处理多少行 | `search_web(concurrency=..., queue_depth=...)` |
| 一行内同时发几条请求 | `search_web(request_concurrency=...)` |
| 同时执行多少个来源任务 | `SearchConfig.workers` |
| 同时进行多少个 HTTP 请求 | `SearchConfig.request_concurrency` |
| 对同一主机的并发与间隔 | `SearchConfig.host_concurrency`、`host_interval_s` |

## 自定义来源

自定义来源是一个随项目安装的 Python 模块，用 `module=` 声明。

**离线来源**（数据在本地或由你自己访问）：提供 `search(query, params)`，返回或逐条产出结果字典。

```python
# my_sources/catalog.py
engine_type = "offline"
categories = ["general"]
paging = False
about = {"website": "local", "results": "JSON"}

ITEMS = {"beetle": [("https://example.org/longhorn", "Longhorn beetle", "A family of beetles.")]}

def search(query, params):
    for url, title, content in ITEMS.get(query, []):
        yield {"url": url, "title": title, "content": content}
```

```python
SearchConfig(engines=({"name": "catalog", "module": "my_sources.catalog", "revision": "1"},), language="en")
```

**在线来源**（访问一个网站）：提供 `request(query, params)` 和 `response(resp)`。

- `request` 在 `params` 里设置 `url`，以及可选的 `method`、`headers`、`data`、`json`、`cookies`；`url` 留空表示拒绝这次请求。
- `response` 从 `resp`（有 `status_code`、`headers`、`text`、`json()`、`url`）解析出结果字典列表。
- 模块属性 `categories`、`paging`、`max_page`、`time_range_support`、`safesearch`、`language_support` 声明这个来源的能力。
- 改了实现就更新 `revision`，否则会命中旧缓存。

`demiflow.collect.native_search.adapter` 提供适配器可用的辅助：发请求的 `get`、`post`，表示失败的 `SourceCaptcha`、`SourceRateLimited`、`SourceAccessDenied`，以及文本处理的 `html_to_text`、`extract_text`。通过这些辅助发出的请求同样受限速约束并被记录。

## 需要知道的限制

- 一次检索的结果有条数和字节数上限，超出时以 `response_too_large` 结束，不截断。
- 一个 session 最多声明 32 个来源、16 组网络，`workers` 最多 8，`retries` 最多 3。
- 进程被强制杀死时，自定义来源里的清理代码不保证执行；有外部副作用的来源需要自己保证幂等。
- 平台只提供检索能力。目标网站的可用性、访问条款和结果质量由使用者负责确认。
