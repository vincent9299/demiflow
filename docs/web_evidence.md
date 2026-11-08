# 搜索、原始获取与文档读取

新获取链统一使用 [Dataset.fetch](dataset-fetch.md)，一行一个 HTTP/COS 原始对象；
图片解码和文档解析分别执行。Dataset.fetch_documents/fetch_images 已移除。
search_web 保留每行的有界查询列表及完整来源回执，业务负责选择候选、构造单 URI
请求及表达语义接受。fetch 的 ok 只代表原始对象持久与验证通过，不代表文档可读或业务合格。

```python
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.objects import LocalObjectStore

raw = data.from_items([{'id': 'page-1', 'request': {'uri': 'https://example.org/page'}}]).fetch(
    store=LocalObjectStore('/local/raw-documents'),
    policy=FetchPolicy(max_object_bytes=8*1024**2), concurrency=4)
# 用物化或持久 sink 完成原始获取。对成功回执显式调用 ParseDocument，
# 传 raw_ref、稳定 url、final_url、content_type、retrieved_at；解析不再 GET。
```

`ParseDocument` 的严格输入、隔离和输出界限见 [文档原语](document-acquisition.md)。
`read_documents` 只验证并读取已经保存的规范文档，按显式 PromptContext 对完整渲染
prompt 计量和选择整块，不调用模型。原始对象、规范文档和业务输出保持独立引用。
Agent 使用同一单行读取实现，不创建子 Dataset。

当前原生 Dataset.search_web 只准入精确 WebSession 与有界 SearchConfig HTTP adapter；
managed service、browser、route/session pool、reuse/fallback 等独立低层资源仍保留，
不等于它们已获当前原生 Dataset 或 queue producer 准入。构图不创建客户端或解析凭据。
队列生产先固定当前关键词的原生响应，再展开并增量发布；自动结算消费者不允许丢行/扩行。

`WebSession.fetch/fetch_image`、`WebClient.fetch`、`ImageClient` 及下载缓存/解析组合已移除。
独立解析、索引导入和历史读取各有自己的边界，不取得 queue task ACK 权限。

## 通用文档库与历史读取

`DocumentLibrary(index_path=..., object_directory=..., ...)` 的声明不做 I/O。
显式 `register/prepare/publish` 导入已有 raw/document 引用或抽取文本。
`lookup(url, max_bytes=..., max_document_bytes=...)` 只读兼容的既有成功文档，
核验 SHA、尺寸、schema、parser 与原始时间。miss 不触发网络；索引绑定对象目录，
错误配置、丢失或损坏抛错。`policy='refresh'` 不复用当前 lookup，新的获取须显式另接 fetch。

来源时间不因导入而刷新。URL 锁有有限超时，只读 lookup 可与 SQLite reserved writer 并行。
精确路径/query 身份和 fragment 别名规则保留。显式索引发布保留原子边界；
注册节点是否获当前原生执行准入须另查实际 native facts/capability。

### Registration of existing normalized documents

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
## 独立低层 HTTP 搜索资源（不在当前 Dataset native 准入内）

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

The first uncached search starts or reuses the existing demiflow HTTP supervisor. A directly managed resource usage scope holds a shared usage lease; finishing it releases the lease without stopping a persistent service used by others. Explicit stop refuses while a lease is held. Local file-lock request slots enforce service concurrency across runs/processes. These are local-host semantics, not a distributed scheduler. Cache-only runs do not start the service or acquire a service lease. Operators can inspect/stop the service through the existing `python -m demiflow.services.manage status|stop --root ... --name ...` interface.

## Model, delivery and runtime ownership

`map_prompt_async` owns schema validation, exact token admission, native HTTP/offline journals, cumulative `sqlite_journal.max_requests`, invocation traces and stable error categories. Categories are `pending_response`, `invalid_response`, `input_budget`, `request_budget`, `uncertain_call`, `provider_error`. The consumer maps these to its own workflow states. Request limits count submissions to the configured endpoint, not attempts hidden inside a proxy or provider. The native client does not automatically retry model calls; an external gateway may. The live validation found two automatic LiteLLM retries per failed request, so its journal count is not a provider-attempt or billing ceiling. A deployment must verify its downstream retry policy separately. A shared `RequestGate` is action-scoped; new actions rebind its loop primitives and observations. No business criterion or model name is built into these mechanisms.

`save_lance` is a nonterminal commit node. Business explicitly declares URI, schema, unique key and stage name. Execution acquires the target lock and creates a fresh empty snapshot even for empty input. Microbatches append against expected versions, verify ambiguous commits before any resend, and pass rows only after a confirmed commit. The action flushes the tail and exposes fixed URI/version references in `stats.outputs`, including through `on_drain` on failure. Old fixed versions remain readable. This is neither a cross-store transaction nor an upsert. The delivered-key uniqueness set grows with row count, while row queues and batches are bounded.

`run_stream` owns startup, cancellation draining, resource closing, queue observations and stage outputs. `stats.metrics` contains stage counts, processing latency, queue capacity/peak/wait, resource request/cache/byte metrics, model traces observed at the model nodes, and read-only cumulative native journal totals. It no longer depends on a business final-row observer to notice a paid model call. Missing usage remains unknown; reused historical tokens are not counted as new tokens. Journal totals cover earlier invocations too. Latency quantiles are upper bounds of 0.25-second buckets, not exact quantiles; concurrent durations cannot be summed into wall time.

Old collect pools register their own compatibility cleanup. Dataset no longer imports or mutates their private global variables. New nodes use explicit owned resources. Business-only metrics, such as a workflow's supplemental-review fraction, remain with the consumer.

## Verification

When a shared service raises `ServiceStopped`, `run_stream()` stops dispatching rows immediately. Native `map_prompt_async` calls with a persistent journal may finish an exchange already executing, within its original request timeout, and save its response or failure. The cancelled row is not published as success. The executor obtains the actor's bounded shutdown allowance through its materialized callable owner; after that allowance it issues ordinary cancellation. Explicit caller cancellation still aborts promptly. This is platform resource cleanup, not a consumer-specific retry or a guarantee that an interrupted upstream request was free. Reuse after recovery still requires the exact journal request identity.

The retained lower-level parser/library/reading tests cover their independent contracts; test_raw_fetch, test_raw_cos_protocol and test_queue_v2_* exercise the new public acquisition and task paths. Existing evidence, prompt, model-service, journal, nullable schema and streaming suites cover the reused mechanisms. Consumer tests exercise their complete formal pipeline and semantic state contracts. These tests establish engineering behavior, not factual accuracy or large-scale quality.


### 独立 WebSession 备用检索与历史验收（2026-10-02）

`WebSession(search_fallback={search, routes?, route_attempts?, route_cooldown_s?, route_failure_limit?, max_pending?, max_result_bytes?})` 对原生主路由池增加独立备用来源；默认关闭，不替业务选择供应商。独立 `WebSession.search` 的单查询可以提供 `fallback_parameters={language: ...}`，只覆盖备用请求语言，主请求语义和既有缓存身份保持。备用支持 language/pageno/safesearch/time_range 四类查询参数；不接受隐式重写 query 或 engine_data/network/engine 子集。概念的中英文选择由 concepts 声明，平台不按文本猜业务语言。

先复用已选备用结果，再检查主路由已有选择。主路由已有成功、空结果和失败都保持原样；不会批量补搜历史失败、改变既有证据或重审已完成概念。只有原来没有选定结果的查询可因新技术失败、原生恢复暂停或已记录的临时故障探测上限而使用备用。鉴权/配置故障、未知内部错误仍上抛。主路由暂停时，尚未准入HTTP的等待者可立即转备用；到期的一次主来源探测仍有机会恢复，故障次数、旧回执及恢复历史不清零。备用不能绕过已有模型累计额度。

备用选择写入独立 `native_search_fallback_results`，保留主请求选择身份/失败或停止原因、备用来源原始回执与实际语言。已有主结果和其错误回执不覆盖。重启或主来源恢复后仍复用原来选定的备用结果；修改备用语义属于新声明，不能冒称原证据相同。当前每个业务run依旧须由原生run锁保证独占执行，源请求账本也保留原并发认领。该能力不提供自动重审已有失败概念的旁路。

资源边界：最多8个来源、16个静态备用路由，每路最多8个adapter worker；native响应最多8MiB、结果最多500条。默认最多128个不同在途查询（硬上限1024），单查询最多32768字符、单语言等参数最多128字符。默认备用选择JSON上限16MiB（允许1KiB–64MiB），先查SQLite字节长度再读，增量编码超限拒绝；取消会等待已开始的SQLite提交，双方session均收尾。上游Dataset的并发/队列仍须控制同一查询的等待者及实际行载荷：这些声明不是整个进程的RSS保证，响应解析、保留结果、编码与返回副本都会占用内存。生产候选仅5路×2worker、2个来源、32个业务查询并发、4MiB原始来源响应。所有HTTP、失败及备用复用与主来源指标分列；主Google circuit停止不等于备用pipeline也停止。

平台路由/暂停/备用回归38项通过；冻结运行环境连同业务契约64项通过（两项此前已通过的慢测试未重复）。四个真实概念首次18查询/36HTTP无传输失败，后续同请求复用新增搜索HTTP为0。三条无业务变更的审定ID保持，另一条因购物页排除而有界重审。上述证实启动与复用行为，不代表所有检索结果相关、概念质量全通过或长期吞吐已验证。只读 `receipt_observation` 分列主停止原因、备用选中结果和观察时间，最多64个状态组，超限明确拒绝；不会把正常空结果算作传输失败。

### 独立 PDF 文字解析

`ParseDocument(..., pdf_parser={...})` 显式开启 PDF，默认关闭；
有限页数/输入/文本/内存/CPU/时间限制沿用 `pdf_policy`。
同一已有 raw_ref 可用不同配置再次解析，原获取回执和已有 HTML 不重写、不重下。
详见 [文档获取边界](document-acquisition.md)。

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
# 转换为一行一 URI 后接 fetch(...).save_lance(...).run_stream()。
```

准入提交后才把行交给下游。持久库只保存已准入身份、去重键摘要和计数，不保存行载荷；拒绝行不扩张持久状态。同一 journal 重启时仍只认可原来的准入身份，改变重放顺序不增加额度；同一 action 重复身份仅第一次放行。准入成功但下游未完成时可以重放原身份，下游外部副作用仍须使用自身请求 journal；本节点不承诺恰好一次外部请求。改变额度、去重键或资源配置须显式使用新的预算 journal，不能通过重启隐式重置。消费者必须让 `key` 随其语义输入变化。

节点只使用一个异步 worker；SQLite I/O 在线程中串行进行，取消会等待在途提交再关库。journal 有独占进程锁，SQLite timeout 5秒、cache 2MiB；`max_entries` 限制准入数（最高1000万）、最多16个配额，每键字段总长受 `max_key_bytes` 限制（最高64KiB），库页受 `max_disk_bytes` 限制（最高16GiB）。回滚日志另可能占同量磁盘；运行内已交付摘要集合最多 max_entries 个固定32字节摘要及 Python 集合开销。超界明确失败，不清理旧键后放开重复。首个源的候选无需等待其他源；单个搜索请求指定一个 engine 时仍使用同一个 `search_web` 算子、请求缓存和连接池。
