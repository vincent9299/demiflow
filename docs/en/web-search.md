# Web search

`Dataset.search_web` lets every row issue several search requests and writes the merged results of all sources back to the row. DemiFlow performs the search itself; no separate search service has to be deployed. The platform takes care of rate limits, concurrency, caching, failure records and cancellation; source adapters take care of talking to particular sites.

Install with the search dependencies: `pip install 'demiflow[search]'`. Python 3.11+ on Linux or macOS is required.

## Quick start

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

The `requests` column is the list of requests of the row; the `output` column receives a list of results of the same length. The Dataset closes the `session` when the action ends; every pipeline declares its own session, and several sessions can share one cache file.

## What a request can contain

| Field | Meaning |
|---|---|
| `request_id` | Your own request identifier, returned unchanged |
| `query` | The query, required |
| `language` | Language, e.g. `en`, `zh-CN`. It can be omitted when `SearchConfig` sets a default; without a default it must be given per request, and the platform never guesses the language from the text |
| `pageno` | Page number, starting at 1 |
| `safesearch` | 0, 1 or 2 |
| `time_range` | `day`, `week`, `month`, `year`, or unset |
| `engines` | Use only some of the sources declared for this session |
| `categories` | Narrow the declared sources by category |
| `engine_data` | Continuation data for paging, taken from `response_json.engine_data` of the previous page |
| `network` | Which network configuration of `SearchConfig.networks` to use |
| `bindings` | Your own business identifiers; not interpreted by the platform, returned unchanged |

When a source does not support the page number or time range you ask for, that source returns `unsupported_parameters`; it never pretends to support it.

## Configuring sources and networks

```python
from demiflow.collect import SearchConfig, Secret

search = SearchConfig(
    engines=("google", "wikipedia", {"name": "museum", "module": "my_sources.museum", "revision": "1"}),
    language="en",
    workers=4,                 # source tasks running at once
    request_concurrency=2,     # HTTP requests in flight at once
    host_concurrency=1,        # concurrency per host
    host_interval_s=1,         # interval between two requests to the same host
    timeout_s=45,
    retries=0,                 # no retries by default
    proxy=Secret("SEARCH_PROXY"),
)
```

- Names and capabilities of the built-in sources are in the [source list](../native-search-sources.csv).
- **Credentials are supplied only through environment variables**: `Secret("VARIABLE_NAME")`. Configurations and records contain the variable name, never the credential.
- The platform does not use process proxy variables such as `HTTP_PROXY` automatically; a proxy must be declared explicitly.
- Different exits can be declared as groups and selected per request with `network`:

```python
SearchConfig(
    engines=("wikisearch",),
    networks={
        "east": {"proxies": {"all://": Secret("SEARCH_PROXY_EAST")}},
        "west": {"proxies": {"all://": Secret("SEARCH_PROXY_WEST")}},
    },
)
```

- A dictionary works as configuration too: `WebSession(search={"engines": ["wikisearch"], "proxy": {"secret_env": "SEARCH_PROXY"}})`.

### Sources that need a browser

A few sources (Google, for example) can be accessed through a browser:

```python
SearchConfig(engines=({"name": "google", "backend": "browser"},), language="en")
```

The browser is an optional dependency: install `demiflow[search,search-browser]` and run `python -m playwright install chromium --no-shell`. Without the dependency the source returns `dependency_error`. Whether such sites return normal results depends on your access conditions, and captchas can occur; rely on the status in the actual results.

## Reading results

Each request has one result object:

| Field | Content |
|---|---|
| `request_id`, `query` | The request it belongs to |
| `status` | Overall status of the request, see the table below |
| `candidates` | Candidate list; each item has `url`, `title`, `snippet`, `engines`, `result_kind`; at most `search_web(max_candidates=...)` are kept per request, 5 by default |
| `response_json` | The complete merged result (a JSON string): scores, positions, typed results such as images or files, `engine_data` and more |
| `engine_receipts` | Per source: its status, attempts and a record of the HTTP requests actually sent |

| `status` | Meaning |
|---|---|
| `ok` | The selected sources succeeded and there are results |
| `no_results` | The selected sources all ran normally and returned nothing |
| `partial` | At least one source completed normally and at least one failed, was suspended or did not support a parameter. Candidates are usable, but coverage is incomplete |
| `search_failed` | No source completed normally; see `engine_receipts` for the reason, e.g. `captcha`, `rate_limited`, `timeout`, `parse_error` |
| `response_too_large` | The result exceeds the output limit |

A failed search does not fail the whole action: it is a status in the result, and your downstream logic decides what to do with it.

Be aware that after a site redesign a source may parse an empty list without raising, so `no_results` does not mean "the web really has nothing". Whether results are relevant and true is still for the downstream to judge.

## Caching, retries and suspension

- **Cache**: results and failures of every source are stored in the SQLite file at `cache_path`. The same request run again is served from it without visiting the site; when several processes issue the same request at once, it runs once.
- A change in source configuration, language, page, network or credentials makes a new request.
- **Retries**: 0 by default. When set, only network errors, timeouts and HTTP 5xx are retried; captchas, authentication failures and rate limits are never retried automatically.
- **Suspension**: a source that meets a captcha, an authentication failure or a rate limit is suspended at once for this session; other failures suspend it after a number in a row. A suspended source shows as `suspended` in the results; other sources are unaffected.

## Who controls concurrency

| Level | Parameter |
|---|---|
| Rows processed at once | `search_web(concurrency=..., queue_depth=...)` |
| Requests of one row at once | `search_web(request_concurrency=...)` |
| Source tasks running at once | `SearchConfig.workers` |
| HTTP requests in flight at once | `SearchConfig.request_concurrency` |
| Concurrency and interval per host | `SearchConfig.host_concurrency`, `host_interval_s` |

## Custom sources

A custom source is a Python module installed with your project and declared with `module=`.

**Offline source** (the data is local or you access it yourself): provide `search(query, params)` that returns or yields result dictionaries.

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

**Online source** (talks to a site): provide `request(query, params)` and `response(resp)`.

- `request` sets `url` in `params`, and optionally `method`, `headers`, `data`, `json`, `cookies`; an empty `url` refuses the request.
- `response` parses a list of result dictionaries from `resp` (which has `status_code`, `headers`, `text`, `json()`, `url`).
- The module attributes `categories`, `paging`, `max_page`, `time_range_support`, `safesearch` and `language_support` declare what the source can do.
- Update `revision` when the implementation changes, otherwise old cache entries are hit.

`demiflow.collect.native_search.adapter` provides the helpers an adapter may use: `get` and `post` for requests, `SourceCaptcha`, `SourceRateLimited` and `SourceAccessDenied` to signal failures, and `html_to_text` and `extract_text` for text. Requests sent through these helpers are rate-limited and recorded like any other.

## Limits to be aware of

- The result of one search is limited in item count and bytes; exceeding a limit ends with `response_too_large`, never with a truncation.
- A session declares at most 32 sources and 16 network groups; `workers` is at most 8 and `retries` at most 3.
- When a process is killed, cleanup code inside a custom source is not guaranteed to run; a source with external side effects has to be idempotent on its own.
- The platform provides the search capability only. Availability of the target sites, their terms of access and the quality of results are for the user to confirm.
