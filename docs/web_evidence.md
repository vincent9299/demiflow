# Native Dataset web documents and streaming delivery

Business pipelines use `Dataset.search_web`, `fetch_documents`, `read_documents`, `map_prompt_async`, `save_lance` and `run_stream`. A stream row keeps its original grain. Queries, URLs and blocks are bounded lists inside a row, not a second dataset or hidden subpipeline. The platform owns transport, provider adapters, document objects, concurrency, token admission, delivery and cleanup. Consumers own questions, semantic acceptance, budgets and output tables.

## Declare resources and the graph

```python
from demiflow.collect import SearchConfig
from demiflow.collect.session import WebSession
from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.reading import PromptContext
from demiflow.operator_llm.tokens import TextTokenCounter, TokenBudget

web = WebSession(
    search=SearchConfig(engines=('wikisearch', 'wikipedia'), language='en',
                        request_concurrency=8, source_interval_s=1),
    cache_path='/explicit/run/retrieval.sqlite',
    object_directory='/explicit/persistent/document_objects',
    document_library=DocumentLibrary(
        index_path='/explicit/shared/document_index.sqlite',
        object_directory='/explicit/persistent/document_objects',
        policy='reuse', max_age_s=None, lock_timeout_s=120),
    fetch_concurrency=16, host_concurrency=2,
    host_interval_s=1, timeout_s=30, connect_timeout_s=10,
    parse_timeout_s=30, max_bytes=2*1024*1024,
    max_document_bytes=8*1024*1024, redirects=3, retries=1,
)
counter = TextTokenCounter(verified_profile)
context = PromptContext(prompt_definition, TokenBudget(counter, 32000, 8000), build_inputs)

stream = (source
    .map(prepare_search_requests)
    .search_web(requests='queries', output='search_results', session=web,
                max_candidates=5, request_concurrency=2, concurrency=8, queue_depth=8)
    .map(prepare_fetch_requests)
    .fetch_documents(requests='urls', output='documents', session=web,
                     known='known_documents', per_request=2,
                     request_concurrency=2, url_concurrency=2,
                     concurrency=8, queue_depth=8)
    .map(prepare_read_request)
    .read_documents(request='read_request', output='reading', context=context,
                    document_concurrency=2, timeout_s=30,
                    concurrency=4, queue_depth=4)
    .map(prepare_model_inputs)
    .map_prompt_async('review', config=pack, inputs={'payload':'payload'},
                      output='response', error_output='error', call_output='call',
                      token_budget=context.budget, request_gate=shared_model_gate,
                      options={'sqlite_journal':{'path':journal_path,'max_requests':100}},
                      max_requests=100, concurrency=4, queue_depth=4)
    .save_lance(output_uri, schema=output_schema, key='record_id', stage='reviewed',
                max_batch=32, flush_interval=5, queue_depth=64))
stats = stream.run_stream()
fixed_output = stats.outputs['reviewed']
```

The preparation functions above are consumer-owned pure row transformations. `build_inputs(row, reading_result)` returns native prompt template variables; it must not read files, make requests or decide which blocks fit. The platform repeatedly invokes it to count the **complete rendered prompt**, including schema and mandatory context. The reading node never calls a model.

These methods construct typed logical plan nodes (`SearchWebOp`, `FetchDocumentsOp`, `ReadDocumentsOp`, `SaveLanceOp`), lowered by the existing streaming executor. `SearchWeb` and other internal classes are not the public composition interface. Declaration does not create clients, start services, initialize journals or write output tables. Model schema/profile validation remains local. Execution uses `run_stream`; local async materialization delegates to that same lifecycle.

## Row contracts

For a local text model, `HuggingFaceTokenCounter(tokenizer_path, models=[alias],
context_tokens=8192, max_output_tokens=1024, revision=deployment_revision,
chat_template_kwargs={'enable_thinking': False})` can replace `TextTokenCounter`
inside the same `TokenBudget`. It loads local tokenizer files only and counts
the native chat template, schema messages and generation prefix. The caller must
use the same tokenizer, template and template kwargs on the inference service;
the revision is an explicit deployment binding, not automatic server attestation.
Multimodal message content is rejected. A managed `VLLMService` may explicitly
set `language_model_only=True` to pass vLLM's `--language-model-only` flag; the
default is false and existing multimodal services are unchanged.

| API | Input column | Output column |
| --- | --- | --- |
| `search_web` | List of `{request_id, query, language, bindings}`. Bindings are opaque consumer labels. Each request can select declared engines, categories, page, safe-search, time range, continuation data and network. | Same requests with status, candidates, attempts, per-source engine_receipts, full response_json and runtime/config identity. Candidate: URL, title, snippet, engines, result kind. |
| `fetch_documents` | Ordered groups `{request_id, urls, bindings}`; optional known document receipts with `request_ids` and `bindings`. | `{documents, receipts, touched_urls}`. Documents contain independent raw/document references and technical metadata, plus merged request IDs/bindings. |
| `fetch_images` | Ordered bounded `{request_id, url?, sha256?, image_uri?, bindings?}`; explicit shared ImageLibrary. | Per-request result with original image_ref, verified raster metadata, local/download origin and failure/attempt receipts. Image search candidates retain img_src/thumbnail_src separately from source-page url. See [image acquisition](image-acquisition.md). |
| `register_documents` | One `{document_ref, aliases?, source_id?, revision?}` request per row. `aliases` is an explicit list of equivalent source URLs, not name matching. | One document receipt after verified object copying and successful shared-index registration; no source-page HTTP request or invented download attempt. |
| `read_documents` | Object with `documents`, `questions`, `retained`, `requests`, `new_tokens`, `total_tokens`. | `status`, `reason`, `selected`, `materials`, `readings`, `receipts`, `material_tokens`, `prompt_tokens`; a read failure can contain only status/reason. |

Reading documents contain `document_ref`, `url`, `bindings` and an `eligible` flag controlling automatic new selection. Questions contain `id` and `text`. Retained selections contain evidence ID, document reference, block ID and bindings. Explicit requests contain request ID, document reference, block IDs, optional section ID and bindings. A section includes descendants until the next heading at the same or shallower level.

`read_documents` verifies size, SHA256, schema and blocks. Earlier material is reconstructed exactly; missing retained evidence is a failure, not silently removed. Explicit block/section requests receive priority. Automatic selection ranks hits within each eligible document and alternates between questions. A first pass shares the remaining budget across documents; a second pass reuses unspent shares. Document order uses the strongest lexical hit, so very small budgets still favor that hit. This prevents a long document in the query language from excluding every source in another language. Duplicate bindings merge before admission, and complete blocks are never cut to fit. It reports selected IDs, heading counts and unread ranges. Scores allocate reading effort; they are not semantic proof or document rejection.

The new/total material budgets count the canonical generic material envelope. Prompt fitting separately counts the actual consumer payload and complete native messages, so renaming consumer fields does not evade the model input cap. Fitting first tests the full allowance. If that exceeds the complete-input cap, it tests mandatory context with zero new material, then performs at most six bounded bisection probes for optional new blocks. Only a measured fitting result is returned. Material-envelope size and consumer-payload size need not shrink at the same rate. Mandatory prior evidence/context stays intact; if even zero new material cannot fit, the result is `context_budget`. These local probes do not call a model or claim to find the globally optimal block set.

`collect.contracts` publishes Arrow components for candidates, attempts, document receipts, selected block references, reading catalogs and request receipts. Consumers compose their own business tables; the platform has no concept assessment schema.

## Fetch and parser behavior

`per_request` bounds successful documents per group. `max_attempts` and `max_new_documents` bound the entire row when supplied. With row-wide caps, groups dispatch in declared order and reserve before requests; otherwise groups and pages may overlap. URL tasks deduplicate within a row/session. Reusing an available document merges all request bindings and records the URL as touched, without consuming a new-document quota. Output order follows input order, independent of completion timing.

HTTP attempts have a wall deadline across redirects, byte ceilings before and after decompression, explicit proxy routing, and configured retries only for transient network errors, 429 and 5xx. Initial admission waits are outside the attempt deadline. Long Retry-After values end the retry instead of being ignored. Authentication/configuration failures and service failure circuits stop dispatch. Transport failures are receipts, not semantic negative evidence.

Raw bytes and normalized JSON are immutable independent objects addressed by URI and SHA256. Stage tables store references/metadata, never the body. The acquisition ledger identity excludes parser version; parsing identity includes it. An upgraded parser can reuse verified raw bytes. Successful results and exhausted failures replay in the same ledger. Interrupted reservations do not reset attempt quotas. Use a new run for deliberately fresh acquisition. The ledger currently provides in-session single flight; sharing the same ledger among independently writing processes is not a supported substitute for a run lock.

Parser revision `structured-html-3` preserves whole-body/article layout classes such as `has-sidebar`, `sidebar-right` and `ast-no-sidebar`. It removes explicit widget containers, prefers explicit article-body markup or main content over an arbitrary first article, and keeps title metadata out of fallback body text. Recovered article markup inside malformed HTML heads is retained.

The parser supports HTML/XHTML and plain text. It removes recognized navigation, sidebars, forms, advertisements and template controls; preserves body headings, table row/cell boundaries and captions; and separates source metadata. Sidebar login controls do not block a public main document. Structural heuristics do not detect every disguised advertisement or guarantee clean extraction for every website. PDF, OCR and browser rendering are not implicitly enabled. Parse failures retain acquired raw snapshots. Isolated parsing/reading workers have an enforceable timeout and cancellation drains their owned process work.

The internal `read_document` function only opens and verifies one saved normalized object. It supports `Dataset.read_documents` and the integrity checks behind `fetch_documents/register_documents`; it is not a download API, business stage or model call.

`demiflow.collect.reading.read_documents(request, *, context, ...)` is the shared single-row implementation of the Dataset operator. `agentmap_async` can invoke that same implementation inside its row-local operator environment without a nested Dataset. `CharacterBudget` uses `new_chars/total_chars` and `material_chars/prompt_chars` in place of the token fields; `TokenBudget` retains the existing contract. See [operator environment](operator_environment.md) for the structured model API, reference scope, budgets and recovery.

## Shared document library: configuration, lookup and import

`WebSession(document_library=None, ...)` preserves run-local acquisition. Passing a
`DocumentLibrary` enables a separate shared source index. The library owns the
object directory used by successful downloads; the session's existing
`object_directory` is the fallback when no library is declared. Neither library
nor session construction opens a database, creates directories or starts services.
The platform has no default project location.

| Library configuration | Default / meaning |
| --- | --- |
| `index_path` | Required local SQLite path. Separate from every run's `cache_path`. |
| `object_directory` | Required persistent local directory for immutable raw snapshots and normalized JSON objects. An initialized index is bound to this directory; accidentally configuring another directory fails. |
| `policy` | `'reuse'`: look up a compatible successful snapshot first. `'refresh'`: bypass shared lookup and acquire according to the run journal. |
| `max_age_s` | `None`: no age limit. A positive finite number requires a source retrieval timestamp within that many seconds. Import time does not refresh the source timestamp. |
| `lock_timeout_s` | 120 seconds, positive and finite. Maximum wait for another process acquiring the same URL. A timeout returns `library_busy` with zero HTTP attempts. |
| `parser_versions` | The current `structured-html-3` parser by default. Explicit nonempty list/tuple permits other normalized-document producers whose output contract the consumer accepts. Schema, raw/document SHA and size checks still apply. |

There are three distinct stores:

1. Search query/profile results stay in the existing search cache. This change
   does not make searches global or provide local full-text search.
2. The public library stores only verified successful document receipts and URL
   associations. Its objects are ordinary immutable files, not Lance records or
   internal Lance blobs. Content SHA256 deduplicates bytes; the URL index avoids
   downloading those bytes in the first place.
3. Each run's existing journal freezes its exact acquisition outcome. Completed
   failures and interrupted reservations stay local; they are never public
   documents. A repeat of the same request uses its saved outcome before looking
   at today's shared library. Historical HTTP attempts are not new HTTP activity.

For a new run request, `fetch_documents` claims the URL, checks the index, verifies
both objects and their matching source metadata, and returns the existing refs
on a hit. On a miss it uses the existing bounded downloader and isolated parser,
publishes objects, then commits the successful index entry. A parse or network
failure is not indexed. If a process dies after publishing objects but before
index commit, unreferenced objects may remain; no partial index entry is exposed.
Missing or corrupt indexed objects and storage/configuration errors stop the
action instead of silently redownloading or reporting a semantic rejection.

Same-URL misses coalesce across independent processes using POSIX file locks;
waiters recheck after acquiring the lock. Locks are released on normal exit,
cancellation and process exit. The lock directory contains at most 4096 hash
stripes, not a permanent file per URL. Collisions may serialize unrelated URLs
but do not merge their receipts. Refresh deliberately bypasses reuse. A new
refresh run may download again even if another run just downloaded the URL.
SQLite/flock must be supported by the shared filesystem; this is not a distributed
lock service or a guarantee for arbitrary object-store mounts.

Source lookup preserves scheme, query string and case-sensitive paths, lowercases
the hostname and removes fragments. The original and final URLs of a successful
redirect are registered. Other aliases must be explicitly supplied by an importer.
There is no title/name similarity, language merging, authentication sharing or
implicit assumption that different query parameters select the same document.
Use separate libraries for source representations that must not be mixed.
Among accepted entries, the latest original retrieval timestamp wins; equal
timestamps use a deterministic content-based entry ID. All prior entries and
objects remain available to fixed historical refs. The index does not promise
the latest live revision or infer revision ordering from an opaque revision string.

To force fresh acquisition, use a **new run journal** with `policy='refresh'`.
Replaying an existing run does not erase its saved responses, raw downloads,
failures or uncertain attempts. Changing public storage does not migrate old
run journals or automatically import every existing object directory.

### Native registration of existing normalized documents

```python
library = DocumentLibrary(index_path=index_path, object_directory=object_directory)
registered = (
    data.read_lance(source_uri, version=source_version)
    .map(lambda row: {**row, 'registration_request': {
        'document_ref': row['document_ref'],
        'aliases': row.get('equivalent_urls', []),
        'source_id': row.get('source_id', ''),
        'revision': row.get('revision', ''),
    }})
    .register_documents(request='registration_request', output='registration',
                        library=library, max_bytes=2*1024*1024,
                        max_document_bytes=8*1024*1024,
                        concurrency=4, queue_depth=8)
    .save_lance(output_uri, schema=output_schema, key='id', stage='registered')
    .run_stream()
)
```

The actual API is `Dataset.register_documents(*, request, output, library,
max_bytes=2097152, max_document_bytes=8388608, concurrency=4, queue_depth=None,
when=None, label='register_documents', batch_size=1, prepare_workers=4,
index_cache_mb=64, publish_pause_s=0.)`.
The default constructs `RegisterDocumentsOp`;
the existing stream engine owns workers, queues and cancellation. The caller
chooses source fields, fixed source version and output schema. A bad normalized
input is an `invalid_document` row receipt; unavailable input storage and publication
errors stop execution. Publication drains on cancellation. Registration is
idempotent for the same document, origin and source metadata, including the first
registration timestamp. An importer can add exact aliases without replacing
historical entries. Object copying may change the normalized document hash because
its embedded raw-object URI changes; raw bytes, source metadata, parser and blocks
are retained. Inputs are never deleted.

Three explicit request formats are accepted:

- `document_ref` references a verified `demiflow.document.v1` object with its raw snapshot.
- `format='wikitext_sections'` supplies `source={url,final_url,title,retrieved_at,...}`
  and `sections=[{title,text,level}]`. The optional `wikitext` dependency parses
  structure; templates, tables, math and superscripts/subscripts remain literal.
  Blocks are labelled `wikitext_source`, not rendered HTML. The raw object is an
  honest JSON snapshot of supplied sections and metadata, not original dump XML.
- `format='redirect'` supplies `url,target_url,aliases?`; it registers source
  redirection without copying a target body. Conflicting targets are rejected.
  Lookup follows at most `DocumentLibrary.max_alias_hops` redirects (default 8);
  missing targets, cycles or exhausted hops are misses. Direct documents take priority.

`batch_size>1` constructs `RegisterDocumentsBatchOp`. At most `concurrency`
batches prepare concurrently; one action-owned pool with `prepare_workers`
processes handles documents, while inexpensive redirect validation stays local.
Object preparation completes before a short SQLite transaction publishes each
batch. The action drains preparation/publication and closes workers on exit.
There is no all-corpus transaction: earlier committed batches survive failure.
One publisher thread owns a persistent SQLite connection and its configurable
page-cache target (`index_cache_mb`, MiB, allocated on demand). Each batch still
uses `BEGIN IMMEDIATE` and `synchronous=FULL`; `publish_pause_s` yields after
commit, with no SQLite transaction held, for other shared-library writers.

Publication checks existing keys in bounded windows, resolves duplicate IDs and
redirect-alias conflicts in input order, then groups inserts by table and key.
First stored receipts and registration timestamps remain unchanged; a conflicting
alias group publishes none of that row's aliases. A failure in any window rolls
back the entire original batch. The additional lookup windows have at most 256
rows / 2,048 keys / a conservative 4 MiB JSON encoding bound. Existing receipt
lengths are checked before retrieving a batch payload cache, capped at 4 MiB.
Unusually wide metadata uses ordinary sequential publication without that extra
cache; it retains the existing per-row validation contract. These are bounds on
added optimization buffers, not on the prepared input, returned receipts, native
SQLite allocations or overall process RSS. SQLite query parameter groups are
capped at 256, independently of producer batch size.

Unknown historical `retrieved_at` stays empty; it qualifies only without a freshness
limit and never supersedes a document with a known retrieval timestamp. This API
does not infer source-specific title aliases or source-table fields; callers do.

`DOCUMENT_RESULT.acquisition` is nullable for old/non-library results. New library
results contain `kind` (`download`, `import`, or `shared_library`), `origin`
(`download` or `import`), opaque `source_id`/`revision`, and `registered_at`.
`retrieved_at` remains the original source retrieval timestamp. A public hit has
an empty `attempts` list. Same-run replay retains the original receipt; use runtime
metrics to distinguish new work from journal replay. `library_hits`,
`library_misses`, and `library_registrations` count this action's work;
`fetch_attempts/http_hops` still count actual HTTP activity. The existing `reused`
metric describes journal/session reuse, not a sum of all reuse kinds.

Row quotas remain unchanged: `max_attempts` caps new URL acquisition operations
within the row, including public lookups; `max_new_documents` caps documents newly
added to that row's evidence, even if already present in the public library.
The `known` column's documents remain exempt. Public reuse saves network work
without expanding the consumer's evidence or reading budget.

Implementation ownership is `fetch_documents → WebSession/WebClient →
DocumentLibrary + LocalObjectStore` and `register_documents → RegisterDocumentsOp
/ RegisterDocuments → DocumentLibrary + LocalObjectStore`. Both consume the same
document contract. `read_documents` continues to verify and read returned refs;
it does not need the shared index. Model inputs and semantic review are unchanged.

## Legacy explicit HTTP backend (compatibility only)

The default backend now bundles the complete adapter catalog and executes native workers in the application interpreter. No service address, listening port, source checkout or second interpreter is needed. Use [native search configuration](native-search.md). An explicit service/search_url still selects the legacy path for consumers that have not migrated; the concepts pipeline no longer uses this path. Existing services and historical evidence are not stopped or deleted by upgrading the library.

```python
from demiflow.services.searxng import SearxNGService
service = SearxNGService(
    root='/explicit/workspace', name='web-search',
    python='/installed/search-env/bin/python',
    runtime_directory='/installed/searxng', runtime_revision='verified-source-revision',
    port=8080, engines=['wikisearch','wikipedia'],
    proxy_url=None, request_concurrency=8,
)
web = WebSession(service=service, search_url=service.search_url,
                 cache_path=ledger, object_directory=objects, search_language='en')
```

The runtime is an explicit installed dependency. The platform supplies the settings and `wikisearch` keyword engine; it does not import a business package, rewrite the installed runtime or install another platform. `wikipedia` is the upstream entity-summary engine; `wikisearch` uses keyword page search. The response adapter accepts normal results and infobox links, merges engine provenance, and classifies malformed candidates. It does not infer that an infobox link proves a business claim.

A managed profile fingerprints the declared runtime revision, engines, proxy route, profile revision and keyword-adapter code. The service supervisor compares its launch configuration before reuse. Explicit `engines` requests omit `categories=general`, which could union additional engines. External deployments require a caller-supplied `search_profile` revision; the platform cannot verify an operator's undeclared changes behind an external URL. Change that revision when the deployment changes. Native source/configuration identities replace this deployment profile on the default path.

`WebSession(search_interval_s=...)` spaces query starts across nodes sharing that session; the platform default is zero. This pacing does not claim a cross-process rate limit. HTTP 200 responses with failed/suspended engines are observed as technical failures after provider normalization, so they do not reset the service failure circuit as healthy HTTP responses. The triggering receipt is retained, and subsequent admission stops at the configured threshold. No hidden model or provider-response retry is introduced. Genuine empty results remain healthy technical responses.

The first uncached search starts or reuses the existing demiflow HTTP supervisor. A Dataset action holds a shared usage lease; finishing it releases the lease without stopping a persistent service used by others. Explicit stop refuses while a lease is held. Local file-lock request slots enforce service concurrency across runs/processes. These are local-host semantics, not a distributed scheduler. Cache-only runs do not start the service or acquire a service lease. Operators can inspect/stop the service through the existing `python -m demiflow.services.manage status|stop --root ... --name ...` interface.

## Model, delivery and runtime ownership

`map_prompt_async` owns schema validation, exact token admission, native HTTP/offline journals, cumulative `sqlite_journal.max_requests`, invocation traces and stable error categories. Categories are `pending_response`, `invalid_response`, `input_budget`, `request_budget`, `uncertain_call`, `provider_error`. The consumer maps these to its own workflow states. Request limits count submissions to the configured endpoint, not attempts hidden inside a proxy or provider. The native client does not automatically retry model calls; an external gateway may. The live validation found two automatic LiteLLM retries per failed request, so its journal count is not a provider-attempt or billing ceiling. A deployment must verify its downstream retry policy separately. A shared `RequestGate` is action-scoped; new actions rebind its loop primitives and observations. No business criterion or model name is built into these mechanisms.

`save_lance` is a nonterminal commit node. Business explicitly declares URI, schema, unique key and stage name. Execution acquires the target lock and creates a fresh empty snapshot even for empty input. Microbatches append against expected versions, verify ambiguous commits before any resend, and pass rows only after a confirmed commit. The action flushes the tail and exposes fixed URI/version references in `stats.outputs`, including through `on_drain` on failure. Old fixed versions remain readable. This is neither a cross-store transaction nor an upsert. The delivered-key uniqueness set grows with row count, while row queues and batches are bounded.

`run_stream` owns startup, cancellation draining, resource closing, queue observations and stage outputs. `stats.metrics` contains stage counts, processing latency, queue capacity/peak/wait, resource request/cache/byte metrics, model traces observed at the model nodes, and read-only cumulative native journal totals. It no longer depends on a business final-row observer to notice a paid model call. Missing usage remains unknown; reused historical tokens are not counted as new tokens. Journal totals cover earlier invocations too. Latency quantiles are upper bounds of 0.25-second buckets, not exact quantiles; concurrent durations cannot be summed into wall time.

Old collect pools register their own compatibility cleanup. Dataset no longer imports or mutates their private global variables. New nodes use explicit owned resources. Business-only metrics, such as a workflow's supplemental-review fraction, remain with the consumer.

## Verification

When a shared service raises `ServiceStopped`, `run_stream()` stops dispatching rows immediately. Native `map_prompt_async` calls with a persistent journal may finish an exchange already executing, within its original request timeout, and save its response or failure. The cancelled row is not published as success. The executor obtains the actor's bounded shutdown allowance through its materialized callable owner; after that allowance it issues ordinary cancellation. Explicit caller cancellation still aborts promptly. This is platform resource cleanup, not a consumer-specific retry or a guarantee that an interrupted upstream request was free. Reuse after recovery still requires the exact journal request identity.

`tests/test_native_web_dataset.py` exercises native plan types, lazy declaration, seed-only reuse, ordering, caps, existing-document bindings, ranking, duplicate bindings, parser/profile cache changes, malformed responses, empty/failing sinks, shared-service leases and capacity. Existing evidence, prompt, model-service, journal, nullable schema and streaming suites cover the reused mechanisms. Consumer tests exercise their complete formal pipeline and semantic state contracts. These tests establish engineering behavior, not factual accuracy or large-scale quality.


### 可选原生备用检索（2026-10-02）

`WebSession(search_fallback={search, routes?, route_attempts?, route_cooldown_s?, route_failure_limit?, max_pending?, max_result_bytes?})` 对原生主路由池增加独立备用来源；默认关闭，不替业务选择供应商。`Dataset.search_web` 的单查询可以提供 `fallback_parameters={language: ...}`，只覆盖备用请求语言，主请求语义和既有缓存身份保持。备用支持 language/pageno/safesearch/time_range 四类查询参数；不接受隐式重写 query 或 engine_data/network/engine 子集。概念的中英文选择由 concepts 声明，平台不按文本猜业务语言。

先复用已选备用结果，再检查主路由已有选择。主路由已有成功、空结果和失败都保持原样；不会批量补搜历史失败、改变既有证据或重审已完成概念。只有原来没有选定结果的查询可因新技术失败、原生恢复暂停或已记录的临时故障探测上限而使用备用。鉴权/配置故障、未知内部错误仍上抛。主路由暂停时，尚未准入HTTP的等待者可立即转备用；到期的一次主来源探测仍有机会恢复，故障次数、旧回执及恢复历史不清零。备用不能绕过已有模型累计额度。

备用选择写入独立 `native_search_fallback_results`，保留主请求选择身份/失败或停止原因、备用来源原始回执与实际语言。已有主结果和其错误回执不覆盖。重启或主来源恢复后仍复用原来选定的备用结果；修改备用语义属于新声明，不能冒称原证据相同。当前每个业务run依旧须由原生run锁保证独占执行，源请求账本也保留原并发认领。该能力不提供自动重审已有失败概念的旁路。

资源边界：最多8个来源、16个静态备用路由，每路最多8个adapter worker；native响应最多8MiB、结果最多500条。默认最多128个不同在途查询（硬上限1024），单查询最多32768字符、单语言等参数最多128字符。默认备用选择JSON上限16MiB（允许1KiB–64MiB），先查SQLite字节长度再读，增量编码超限拒绝；取消会等待已开始的SQLite提交，双方session均收尾。上游Dataset的并发/队列仍须控制同一查询的等待者及实际行载荷：这些声明不是整个进程的RSS保证，响应解析、保留结果、编码与返回副本都会占用内存。生产候选仅5路×2worker、2个来源、32个业务查询并发、4MiB原始来源响应。所有HTTP、失败及备用复用与主来源指标分列；主Google circuit停止不等于备用pipeline也停止。

平台路由/暂停/备用回归38项通过；冻结运行环境连同业务契约64项通过（两项此前已通过的慢测试未重复）。四个真实概念首次18查询/36HTTP无传输失败，后续同请求复用新增搜索HTTP为0。三条无业务变更的审定ID保持，另一条因购物页排除而有界重审。上述证实启动与复用行为，不代表所有检索结果相关、概念质量全通过或长期吞吐已验证。只读 `receipt_observation` 分列主停止原因、备用选中结果和观察时间，最多64个状态组，超限明确拒绝；不会把正常空结果算作传输失败。

### 可选 PDF 文本解析候选（2026-10-02，未启用生产）

`WebSession(pdf_parser={...})` 显式开启 PDF 文字层解析，默认 `None` 保持原请求/解析身份；可安装 `demiflow[pdf]`，当前依赖固定 `pypdf==6.19.0`。选项为 `max_input_bytes=8388608`、`max_text_bytes=2097152`、`max_pages=200`、`memory_mb=512`、`cpu_s=20`、`timeout_s=25`。范围由 `pdf_policy` 校验，声明不读取文件、不启动进程。PDF 配置及解析器/依赖版本加入 fetch 身份；下载身份保持，因此旧 `parse_error` 和原始 HTTP 回执保留，可从已保存 raw_ref 重新解析而无新 HTTP。HTML 规范正文与 parser version 保持，真实回归证明启用后 HTML document SHA 不变。

解析使用独立 Linux 子进程，在导入解析库前限制地址空间、CPU、单输出文件大小和文件描述符；另有限输入、页数、总文本、metadata 和墙钟期限。父级超时杀死并回收子进程，临时目录随调用结束释放。单个 PDF 超限则整份失败，不返回已处理的前几页。fetch 并发仍决定同时驻留的 worker 数；例如16路×512 MiB是子进程地址空间上限之和，不能称为整条 pipeline RSS 上限。原生 outer parse timeout 还包含启动、对象写入等时间，业务须留余量。不是面向任意超大 PDF 的无限解析器。

页面保留一基 page_number，空文字页在 parser.empty_text_pages 中明确列出；纯扫描文档返回 pdf_no_extractable_text。只提取文字层，不做 OCR，不声称读过插图、公式几何或表格版式。公开可读但仅限制编辑的空用户密码 PDF 可读取；需要非空密码则返回 pdf_password_required，不尝试密码。异常及资源失败给出有界类别，不倾倒任意正文或内部路径。

八项初始 PDF/缓存/资源测试及47项原生取证回归通过；测试包含128 MiB解压压力（父进程仅保留压缩小文件）、旧PDF原文复用、旧失败保留和HTML SHA不变。真实两份已下载标准材料（35页中文政府文档、13页ISO公开预览）均完成文字提取，SHA校验通过，新增HTTP/模型/公共库写入均0。首轮把所有加密标记拒绝的问题已修正为空密码可读性判断；原失败验证回执保留。该能力尚未切进正在运行的3315任务，模型引用与业务判断仍须正式入口定向验收。

候选后续验收补记：固定哈希抽取的12份真实PDF中最初10份返回文字，人工检查发现一份Identity-H字体缺ToUnicode却被猜成乱码。新增文字回调检查：实际使用的Type0 Identity-H/V字体缺Unicode映射、或无法识别所用字体时整份拒绝，不靠猜测语种把乱码当有效正文。相同12份加两份标准锚点重验，11/14成功；另三份分别为无可提取文字、严格PDF结构错误、缺Unicode映射。旧诊断保留，不混称最初10份全为有效文字。PDF相关10项测试（包括真实挂起worker的超时杀死/回收）通过；业务配置与补查复用3项通过。仍未在生产启用，不承诺覆盖所有字体、排版或图像内容。


### Domain fetch pool cooldown recovery

`fetch_proxy_routes[domain]` pools now accept `transient_cooldown_s` (default: `cooldown_s`) and `cooldown_wait_s` (default: 0). Network/5xx errors reaching `failure_limit` use the transient duration; 403/429 retain `cooldown_s`, and a longer Retry-After is never shortened. Existing persisted deadlines remain authoritative after a configuration change. Scheduling parameters do not change transport or document identities.

With a positive wait (at most 3600 seconds), a lease encountering an entirely cooling pool releases its condition and waits for the earliest declared deadline. No route or HTTP request is acquired during the wait. Notifications and repeated deadline extensions share the first cooldown wait's monotonic deadline; expiration raises the existing ServiceStopped boundary, while cancellation releases waiters immediately. It does not retry a cached failed URL or change the WebClient attempt allowance. Initial waiting remains outside the network deadline, while redirected hops still retain the original deadline. The pool has at most 128 routes and its waiting callers remain bounded by native fetch admission; no unbounded background probe queue is created. Route metrics include shared pool waiter/count/time values, which must not be summed once per route.

Regression coverage includes same-action wait/recovery with zero early HTTP, total wait under repeated deadline extension, cancellation and restart persistence, Retry-After versus transient cooldown, transport identity reuse, and default immediate-stop compatibility. Consumers select durations; the platform has no Wikipedia or proxy-vendor special case.

### Explicit recovery of failed primary search selections

`search_fallback.recover_primary_selections` optionally lists at most 4096 distinct SHA256 primary selection identities. The default preserves every old selection. An explicitly listed `search_failed` primary selection may make one durable fallback selection using the already declared secondary source, route, queue, request and byte bounds. The old primary result and source receipts are untouched, and the fallback record links its identity/profile with `recovery=explicit_failed_primary_selection`. Successful, empty, partial and unlisted primary results still replay. A saved fallback failure is not automatically retried, including after restart or removal of the allowlist. Authentication/configuration circuit stops remain fatal. This is caller-declared recovery, not a source health reset, new run, model retry, or guarantee of useful evidence. Consumers must inventory exact failed queries and budget any downstream assessment changes before execution.


Fallback routes may declare `search_fallback.adaptive`, using the existing native search admission/recovery policy. It requires declared routes and its maximum concurrency cannot exceed the fallback source HTTP limit. This enables bounded in-process cooldown waiting, drain-before-probe and persistent recovery limits for the secondary pool. The default remains unchanged; query/source identities and immutable selections do not include scheduling settings. Waiting consumes no source attempt; completed failures are not automatically searched again.
# 获取账本的只读文档来源

`data.read_document_receipts(path, sha256=..., max_journal_bytes=8*1024**3, max_receipt_bytes=8*1024**2, max_rows=2_000_000)` 读取关闭后的原生获取账本备份，输出 `receipt_id` 和标准 `DOCUMENT_RESULT`，包括 `document_ref=null` 的失败回执。没有文档字段的搜索及其他缓存项不属于本来源。接口不创建 WebClient、不发 HTTP、不修改原日志；先写为固定版本 Lance，再由业务做关联和宽表归并。

每次迭代先流式核文件 SHA，拒绝活动 WAL/journal 和超限文件。SQLite 只读、禁用 mmap、8 MiB 页缓存；设置 SQLite 单行长度上限，在 Python JSON 解码前检查回执字节，逐条交付，超过行数报错。迭代器独占连接并在正常结束或取消时关闭；Arrow 可在不同线程串行拉取同一迭代器。预算约束文件、值和平台缓存，不是整个 Python/Arrow 进程 RSS 保证。备份必须保持不可变，读取结束还检查文件身份和修改时间。


### Reusable isolation for Dataset document reading

`Dataset.read_documents(..., reuse_workers=N)` optionally keeps N action-owned subprocesses (default 0 preserves one-shot isolation). N cannot exceed node concurrency times document concurrency. The same document verification, full-block selection and prompt-budget functions run in either mode; the public single-row `read_documents` API is unchanged. Each generation receives the fixed prompt/tokenizer once and recycles after 128 operations. Each operation retains its deadline; timeout/cancellation kills and reaps its leased process group without retry. On Linux workers also receive a parent-death signal. Started workers close with the stream, including failed streams.

Messages have a 64 MiB serialized limit, admission precedes serialization, and pipe writes drain in 64 KiB chunks. Payloads, decoded document objects, tokenizer state, parent buffers and each worker's live allocations still count toward host memory; the serialized cap is not a hard RSS limit or a bound on cloudpickle's temporary allocations. Existing document byte limits and bounded node queues remain necessary. Oversized messages fail explicitly. Only pure CPU/file operations are appropriate for this pool. Metrics expose starts, calls, bytes, deadline failures, active workers, queue wait and operation time.

The managed vLLM service also accepts `omp_num_threads` (positive integer or None). It sets OMP_NUM_THREADS only in its owned child environment; None preserves the inherited setting. This allows serving CPU spin-wait contention to be measured without changing the parent reader's environment.


## 在线准入与持久配额

`Dataset.admit_rows(path=..., key=..., unique_on=[...], quotas=[...], output='admission', when=None, max_entries=100000, max_key_bytes=16384, max_disk_bytes=256*1024**2, queue_depth=1)` 是通用流式准入节点。每条输入立即独立检查，保留原行并附上 `status=admitted/duplicate/limited/skipped` 和原因。使用普通 `filter` 选择 admitted 后接网络或模型算子；技术错误不伪装成重复或超额。平台不解释 URL、概念或通过率。

配额 `limit=None` 表示没有累计条数上限；去重、计数及资源限制仍生效。同一账本允许显式提高配额（含有限改为无限）和 `max_entries`／`max_disk_bytes`，保留原身份、唯一键和计数，不重新发放历史额度。分组字段与顺序、身份字段、键字节边界不变；缩小额度或改变分组仍报错。`AdmissionQuotaReader.remaining()` 对无限额度返回 `None`；调用方应将其解释为无次数限制。

`key` 是消费者提供的稳定输入身份；`unique_on` 是去重字段。每个 quota 为 `{'on':[字段], 'limit':N}`，空 on 表示全局额度。先检查去重和全部配额，再原子占用，避免前一个额度消耗后又被后一个额度拒绝。先到先得，不提供全组 top-N 排序或语言交替优选。配额是尝试准入上限，下载失败不会退还。

```python
stream = candidates.admit_rows(
    path='runs/example/admission.sqlite', key='candidate_id',
    unique_on=['group_id', 'normalized_url'],
    quotas=[{'on': [], 'limit': 1000}, {'on': ['group_id'], 'limit': 20}],
    max_entries=1000,
).filter(lambda r: r['admission']['status'] == 'admitted')
# 接 fetch_images(...).save_lance(...).run_stream()，不中途 materialize。
```

准入提交后才把行交给下游。持久库只保存已准入身份、去重键摘要和计数，不保存行载荷；拒绝行不扩张持久状态。同一 journal 重启时仍只认可原来的准入身份，改变重放顺序不增加额度；同一 action 重复身份仅第一次放行。准入成功但下游未完成时可以重放原身份，下游外部副作用仍须使用自身请求 journal；本节点不承诺恰好一次外部请求。改变额度、去重键或资源配置须显式使用新的预算 journal，不能通过重启隐式重置。消费者必须让 `key` 随其语义输入变化。

节点只使用一个异步 worker；SQLite I/O 在线程中串行进行，取消会等待在途提交再关库。journal 有独占进程锁，SQLite timeout 5秒、cache 2MiB；`max_entries` 限制准入数（最高1000万）、最多16个配额，每键字段总长受 `max_key_bytes` 限制（最高64KiB），库页受 `max_disk_bytes` 限制（最高16GiB）。回滚日志另可能占同量磁盘；运行内已交付摘要集合最多 max_entries 个固定32字节摘要及 Python 集合开销。超界明确失败，不清理旧键后放开重复。首个源的候选无需等待其他源；单个搜索请求指定一个 engine 时仍使用同一个 `search_web` 算子、请求缓存和连接池。
