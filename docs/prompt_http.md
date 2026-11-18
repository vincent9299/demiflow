# Prompt Dataset operators: HTTP streaming and connection lifecycle

## Async execution and recovery (2026-11-04)

`map_prompt_async` no longer accepts `sqlite_journal` or `journal_dir`. Remove
these options when migrating a caller. The actor performs inference and forwards
results through bounded in-memory queues; it does not persist requests, replies,
reservations or lease files. Legacy journals remain readable through the separate
journal inspection API. The synchronous prompt API is outside this change.

For a managed row-local stream ending at the existing `materialize()` boundary,
the execution layer commits downstream output blocks and then confirms progress,
automatically. There is no business checkpoint argument or model-owned result
cache. Every 512 source rows forms a commit epoch; buffers flush at 4 MiB. Progress
stores input fingerprints with duplicate multiplicity because native joins are
unordered. Resume skips confirmed inputs and returns committed downstream rows;
unconfirmed work may run again. Fixed graph/config/operator/source identity binds
recovery. Complete materialization can be reused without model calls.

The recovery index reserves at most min(512 MiB, memory budget / 8); blocks,
queues, encoding copies, input/output rows and scratch have separate admission.
The index rejects overflow. There are at most 16,384 batches and 1,024 output
blocks per batch. Each commit publishes blocks, a batch receipt, then progress
with atomic replacement and fsync. Failed publication never confirms the batch.
Model concurrency is preserved within each epoch; downstream backpressure and
commit drain may briefly pause admission between epochs.

Automatic recovery currently requires a managed execution context and a row-local
stream terminal. Stateful/grouped/batch stages are rejected before model work;
unmanaged `run_stream()` has no implicit durable sink. These are implementation
limits, not guarantees of recovery for arbitrary Dataset graphs. Final Lance
publication remains the ordinary downstream business action. Existing sections
about per-request journals describe the synchronous/direct-client legacy APIs,
not the async Dataset operator.

Validation: `test_prompt_stream_recovery.py` covers process loss before/after
progress publication and during inference, duplicate rows and downstream failure;
`test_prompt_without_journal.py` covers image output without request artifacts.

## Caller-owned messages (single completion)

A prompt definition can explicitly select `input_mode: messages` instead of a
`template`. Bind `inputs={'messages': 'row_messages'}` on `map_prompt_async` (or
`map_prompt`). Business operators own rendering, history, branching and retries.
The platform validates and sends the complete ordered message list once; it
does not add a system/schema instruction, image labels, or trim/coalesce text.
Omitted `input_mode` retains the existing template contract and request identity.

Messages contain only `role` (system/user/assistant) and `content` (a string or a
list of text/image_url parts). Image URLs and optional detail are preserved;
there is no decoding, resizing or remote downloading during message preparation.
Tools, audio, arbitrary provider fields and Codex transports are rejected for
this mode. A template and automatic `schema_retries` are forbidden: the business
must decide how to repair/retry without silently changing the supplied history.
JSON output still uses the declared response schema for validation, but the
caller must include any necessary output instruction in its messages. With
`response_format: text`, raw text is returned as the existing string `result`
property and the HTTP request explicitly carries `response_format={type: text}`.

Optional `message_limits` can tighten the platform ceilings: `max_messages: 64`,
`max_parts: 256` (total list parts), `max_images: 16`, `max_bytes: 67108864`.
The byte admission counts escaped UTF-8 values and conservative JSON framing
before copying containers or reserving a provider call. Counting uses bounded
8192-character chunks; immutable strings remain shared. These are admitted
payload limits, not upstream allocation/RSS limits. Plan for serialization,
journal externalization and the bounded concurrency/queue retaining additional
copies; caller-supplied upstream rows must have their own finite budget.

Full roles/content/order determine request identity and are preserved in online
and offline logs. Existing SSE, quotas, uncertain-call handling, token budgets,
read-only replay and cancellation remain shared. Regression coverage:
`tests/test_prompt_messages.py` plus the existing prompt/stream/journal suites.

The public entry is the existing `Dataset.map_prompt_async` API. It does not add a token Dataset, a business HTTP client or another pipeline entry. `map_prompt(..., options=...)` shares the HTTP/response contract through its existing synchronous adapter; synchronous calls do not gain the asynchronous node's shared pool.

## SystemOne noul decisions

`map_prompt_async` also accepts `model.transport=systemone` for text and image `noul` decisions. This is the same prompt-pack and Dataset contract, not another configuration family or Dataset API. The existing `template` renders the single text `state` and optional image placeholders; `systemone` declares only `type: noul` and a nonempty `instructions` string. The prompt name is the question key. `api_key_env` is optional for unauthenticated local endpoints; `base_url` or `base_url_env` remains required.

```yaml
model: {name: AutoTrust-JEV-27B-v1.1, transport: systemone, base_url: http://127.0.0.1:8104/v1}
systemone: {type: noul, instructions: Does the text define the same concept and scope?}
schema_retries: 0
response_schema:
  type: object
  required: [result]
  properties:
    result:
      type: object
      required: [type, noul]
      properties:
        type: {type: string, enum: [noul]}
        noul: {type: number, minimum: 0, maximum: 1}
template: |
  {{ payload | json }}
```

The HTTP request is `POST {base_url}/systemone` with `model`, `state`, `questions` and optional `images` data URLs; no chat messages or generation parameters are added. The response must contain `answers.<prompt-name>` with `type=noul` and a finite probability in `[0,1]`, returned as the ordinary `result` object and checked by the same response schema. This is decision-head output, not a model-written confidence value. SystemOne supports template mode, one text state, optional images, JSON output, zero schema retries and `stream=False`. An explicit `type: choice` with 1..255 named `criteria` also validates the returned choice and probability distribution. Non-data image URIs, `score` and `input_mode=messages` are not enabled.

Request journals store the actual SystemOne payload and reuse complete responses under the existing request-identity, quota, timeout and cancellation rules. Offline request materialization uses the same SystemOne body. Regression coverage: `tests/test_prompt_systemone.py` plus the existing prompt/offline suites.

### Several questions about the same state and images

The existing `systemone` definition can repeat its versioned question over a
bounded row binding. This uses one native `/systemone` request with one state,
one images array, and ordered question IDs. It is independent of cross-request
`systemone_batch`, and does not change service scheduling or model math.

```yaml
systemone:
  type: noul
  instructions: 'Does the image depict {{ concept }}?'
  for_each:
    input: candidates
    key: concept_id
    max_questions: 50
    max_bytes: 131072
schema_retries: 0
response_schema:
  type: object
  required: [result]
  properties:
    result:
      type: object
      required: [answers, errors]
      properties:
        answers: {type: object, additionalProperties: true}
        errors: {type: object, additionalProperties: true}
template: |
  Evaluate each concept independently from the visible image.
  {{ image | image }}
```

Bind `inputs={'image': 'image', 'candidates': 'candidates'}` on the same prompt
node. A candidate is, for example, `{'concept_id': 'C1', 'concept': 'scissors'}`.
Instruction placeholders accept text/JSON fields from each item; they are not
recursively evaluated. The question type and optional `choice` criteria remain
owned by the YAML. Single-question definitions keep their existing behavior and
journal identities. Repeated definitions, including a one-item chunk, return
`result={'answers': {id: native_answer}, 'errors': {id: error_code}}`.

Missing or invalid individual answers are absent from `answers` and explicitly
marked `missing_answer` or `invalid_answer`. Valid answers from the same response
are retained. An invalid envelope or unknown answer ID fails the call. Raw HTTP
responses remain in the native journal. Business policy must handle these errors;
the platform never supplies a zero score or invents a negative decision.

Limits are required: 1..128 questions and 1..1 MiB of question data/rendered JSON,
with depth/node limits before serialization and a reservation before repeated
template expansion. Empty inputs must be skipped by the caller, and duplicate
IDs are rejected before HTTP. State/image bytes, decoded pixels, model encoding
capacity, concurrency, queues, timeouts and storage retain separate budgets.
Character counting includes rendered questions but excludes image tokens; it is
not a claim about a native model's total token consumption. The service's native
encoding admission still applies. Native multi-question behavior and performance
require a real-model pilot; mock tests cannot establish equivalence or throughput.

The request identity includes ordered question IDs and criterion order as well
as the actual state, image, rendered question text, prompt and model revision.
`max_requests` counts HTTP calls, not questions. Separate models require separate
prompt nodes/journals. Regression: `tests/test_prompt_systemone_questions.py`.

`Dataset.agentmap_async` with `runtime: demiflow` uses this same transport for every model turn. Put these execution options in the agent YAML's `options` mapping; node options only supply storage/replay. See [agent HTTP streaming configuration](operator_environment.md#http-流式配置). An async Dataset operator does not implicitly enable HTTP SSE.

```python
result = source.map_prompt_async(
    'review', config=prompt_pack,
    inputs={'payload': 'request'}, output='result',
    concurrency=4, queue_depth=4,
    call_output='model_call', error_output='model_error',
    options={
        'stream': True,
        'gateway': 'litellm',  # omit for a generic compatible endpoint
        'timeout_s': 600,
        'connect_timeout_s': 10, 'read_timeout_s': 120,
        'write_timeout_s': 30, 'pool_timeout_s': 30,
        'stream_include_usage': True,
        'sqlite_journal': {'path': '/absolute/review.sqlite', 'max_requests': 100},
    },
)
# result.run_stream(...)
```

The prompt pack still supplies model/endpoint, placeholders, schema and explicit schema retry allowance. A `request_gate` bounds calls shared by several nodes. Alternatively, `request_policy={'initial_concurrency': 2, 'max_concurrency': 4}` declares an operator-owned adaptive gate; its maximum must not exceed node concurrency. Supplying both interfaces is rejected before execution. The public `demiflow.inference.request_admission_config` validator is shared with embeddings. Policy observations reset per action and exclude journal replay. Streaming does not bypass token admission or either request budget.

## Parallel routes in one streaming node

`map_prompt_async(..., routes={'a': {'config': pack_a, 'concurrency': 4},
'b': {'config': pack_b, 'concurrency': 8}}, route='reviewer', concurrency=12)`
dispatches each row to the route named in its `reviewer` field. Each route uses
the ordinary prompt actor, with its own HTTP pool, journal options, metrics and
lifecycle; all routes share the outer worker/queue bounds. There are at most 32
routes; total concurrency is limited to 512 and each route to 256. Declare
route-specific `options`, `max_requests`, service, admission or token budget in
that route's mapping. Actual request parameters determine cache identity.

Upstream code chooses a route and must bound its outstanding assignments;
otherwise many rows waiting for one route can occupy all outer workers. The
operator does not choose a model or duplicate a row across routes. With
`isolate_route_failures=True` and `error_output`, provider stop/service failures
become row errors marked `route_unavailable`; caller-owned scheduling can stop
assigning that route. User stop, storage errors and programming failures still
stop the action. Already submitted uncertain requests are not automatically
sent to another route. Cancellation drains active calls under the native
actors' existing deadlines before closing their clients.

An explicitly unlimited journal uses `sqlite_journal.max_requests=None`; the
node's `max_requests` must also be None. This removes the cumulative call cap,
not the concurrency or payload bounds. Existing reservations and responses
remain in the same journal and its cumulative count is never reset.

## Batched journal storage (opt-in)

`sqlite_journal.throughput=None` (the default) keeps the historic layout: one
SQLite file, one `FULL`-synchronous commit per request, and one
`.leases/<key>` file per active reservation. An explicit mapping enables the
batched storage without a second journal API, a different request identity or a
change to business outputs:

```python
'sqlite_journal': {
    'path': '/local/fast/path/review.sqlite',
    'max_requests': 100,
    'throughput': {
        'shards': 4,
        'batch_size': 64,
        'flush_interval_s': 0.01,
        'queue_size': 256,
        'max_record_bytes': 8 * 1024 * 1024,
        'max_queue_bytes': 64 * 1024 * 1024,
        'wal': True,
    },
}
```

| Key | Default | Meaning |
| --- | --- | --- |
| `shards` | 1 | 1..8 native SQLite files. A multi-shard journal is `review.sqlite.d/` containing `manifest.json` and `000.sqlite`...; a key is routed by `int(key[:16],16) % shards` and never migrates. Legacy Lance import requires one shard. |
| `batch_size` | 64 | 1..4096 operations per group commit. |
| `flush_interval_s` | 0.01 | Maximum wait before committing a partial batch. A smaller value lowers latency and commit size. |
| `queue_size` | 256 | 1..4096 queued operations per shard. Producers wait when it is full. |
| `max_record_bytes` | 8 MiB | 1 KiB..64 MiB encoded size of one serialized request metadata or response. A larger record raises `PromptBudgetExceededError` before it is queued; it is never truncated or silently admitted. |
| `max_queue_bytes` | 64 MiB | Must be at least `max_record_bytes`, at most 512 MiB. Encoded bytes of all queued, batched and in-flight operations; admission blocks above it. |
| `wal` | False | Explicit opt-in. `synchronous` always stays `FULL`; `OFF` is never used. |

Each shard owns exactly one writer thread, one bounded queue and one SQLite
connection. `reserve` returns only after its `calls` row, its lease row and the
persistent request counter are committed; `response`/`failed` returns only after
the payload and the lease release are committed. A response committed by a
crashed process is therefore reusable on reopen without another provider call,
an unanswered reservation stays `uncertain` and is never automatically resent,
and a business row is published only after the response is durable.

Leases are rows in `writer_leases` (owner token, owner pid and the
`/proc/<pid>/stat` start time), not per-request files. A live owner blocks
another processor's reservation and explicit recovery. A lease whose owner pid
is gone (or whose start time changed, i.e. pid reuse) can be replaced, but only
when a reservation or an explicit `requeue_uncertain`/`requeue_http_errors`
re-establishes ownership; a plain lookup never resends an unanswered call. A
lease row written by the intermediate uncommitted layout, which has no owner
identity, is treated as stale. Opening a classic journal with `throughput` set
is an additive migration (adds `calls.ordinal` and the lease owner columns);
existing history stays valid and multi-shard mode must start from a new
directory.

One batch is one transaction with one savepoint per operation. A failed
operation rolls back only its own statements and fails only its own future; the
other operations still commit. A failed outer `BEGIN`/commit/connection fails
every accepted future exactly once and leaves neither a persisted row nor an
in-memory owner, so the caller can retry. Completion work (reference building)
runs inside the operation's savepoint, so an error there rolls that operation
back instead of killing the writer thread. `close()` drains the queue; a closed
journal, a full queue or a failed writer raises instead of leaving a caller
waiting forever. Queue and batch payload bytes share `max_queue_bytes`, so
throughput never buys itself unbounded memory.

`journal.throughput_metrics()` returns bounded counters and maxima per shard:
queued/committed reserve and response counts, failed operations, batch count,
batch rows and bytes, group-commit seconds, queue wait and current/peak queued
bytes. No per-request history is retained.

Recovery boundaries for several shards: `requeue_uncertain` and
`requeue_http_errors` first run a read-only preflight over every affected shard,
so a key that can never be recovered fails before any shard is mutated. Actual
application is per shard and is **not** a cross-file transaction; if one shard
fails, retrying with the same `operation_id` applies only the remaining shards
and is idempotent (`reused` becomes true once every shard has recorded it). A
single-shard journal still applies a multi-key recovery atomically.
`max_requests` is coordinated inside one process, not a distributed quota.
`call_snapshot`, `journal_totals` and `journal_observation` read independent
shard files and are not one cross-file snapshot; `journal_observation` merges
raw timing rows before computing mean/P50/P95. `call_page` keeps recency order
through the reservation ordinal and caps `offset + limit` at 1024 candidates,
returning oversized rows as explicit omitted entries. The ordinal is allocated
by one process and recovered from the largest stored value, so concurrent
processes may allocate the same ordinal; the strict "newest first" guarantee is
scoped to one coordinating process and ties fall back to the shard row id. A
multi-shard page enforces its byte caps per shard read and again on the merged
page, so intermediate candidates can briefly hold up to one page budget per
shard even though the returned page respects the single `max_page_bytes` bound.

Shared project filesystems (for example `/yzp/...`) should keep `wal=False`:
SQLite WAL needs shared-memory coordination and is only recommended on a local
low-latency filesystem. Multi-shard offline materialization remains unsupported
because it uses the direct single-shard connection; this is rejected rather than
silently routed to shard 0. Measured storage A/B and the real-provider canary
are recorded in [JOURNAL_THROUGHPUT.md](../benchmarks/JOURNAL_THROUGHPUT.md).


## Configuration and ownership

| Dataset configuration | Behavior and default |
| --- | --- |
| `options.stream` | Boolean; default False. True sends `stream=true` and receives SSE. False omits the wire field, preserving existing non-stream request identities. |
| `options.gateway` | None by default. Explicit `litellm` adds `num_retries=0`, `max_retries=0` and empty regular/context/content-policy fallback lists. No model/domain is hardcoded in demiflow. |
| `options.gateway_credentials_env` | Optional environment variable containing JSON with `api_key` and optional `extra_headers` (at most 16 headers / 16384 characters). Only explicit LiteLLM transport accepts it. Injected into the outgoing body, excluded from request journals/hashes. Rotating credentials on the same deployment preserves reuse; changing deployment still requires a new `model_revision`. Never put credentials in `request_options`. |
| `concurrency` / `queue_depth` | Row workers / bounded input queue. A worker remains occupied through the complete call and validation; chunks never create workers. |
| `options.max_connections` | HTTP pool cap; defaults to node concurrency. A smaller value queues inside HTTPX. |
| `options.max_keepalive_connections` | Idle connections; default min(20, pool cap), cannot exceed it. |
| `options.keepalive_expiry_s` | Idle connection retention; default 5 seconds. 0 opens a fresh connection for each request. This does not enable retries. |
| `options.prepare_workers` / `io_workers` / `response_workers` | 2 / 2 / 2; separate, client-owned bounded pools for request construction/hashing, journal I/O, and ordinary response JSON parsing/interpretation. Shared implementation and validation with embeddings. |
| `options.collect_journal_totals` | True preserves the final historical journal audit. False skips it while retaining per-action metrics and durable reservations/responses; skipped audits are explicitly listed in stream metrics. |
| `options.timeout_s` | Total HTTP exchange, including pool/connect/upload/body; default 120 seconds. Journal operations are outside this deadline and must finish safely. |
| `options.connect_timeout_s` | Connection establishment; default min(10, total). |
| `options.read_timeout_s` | Inactivity while waiting for headers/body bytes; default total. Each chunk renews the interval. This is not a first-semantic-text deadline. |
| `options.write_timeout_s` | Upload inactivity; default total. |
| `options.pool_timeout_s` | Waiting for a pool slot; default min(30, total). Dataset queue and RequestGate waits are separate from HTTP timeouts. |
| `options.stream_include_usage` | Default True requests final usage. Set False explicitly for unsupported providers; no automatic resubmission without it. Missing usage stays unknown. |
| `options.max_response_bytes` | Decoded HTTP body cap, default 8 MiB; also applies to non-stream/error JSON. Not a compressed wire-byte counter. |
| `options.max_event_bytes` | SSE event cap, default 1 MiB, no larger than response cap. |
| `options.max_stream_events` | Data event cap, default 100,000. Comments consume byte limits but are not data events. |
| `options.stream_log_dir` | Default None. Explicit directory saves a bounded `.sse` file per attempt, referenced in metadata. Contains model output/reasoning; no event file in normal operation. |

HTTP options are rejected for offline/codex execution. Validation happens at Dataset declaration without starting services or sending requests. Actual endpoint environment resolution remains runtime behavior. Streaming supports one text/JSON choice (index 0); tool/function/audio/refusal deltas are explicitly unsupported.

One lazy HTTPX AsyncClient belongs to each node/model during an action; each concurrent request has its own decoder/assembler. Responses close on success, HTTP/protocol failure, timeout and cancellation. Complete HTTP bodies are drained for connection reuse. The stream action closes the client/pool. The shared admission permit stays held through reception, persistence and error cleanup. Business code manages no connections.

Compressed JSON responses are decoded once by HTTPX while reading. The detached response used for the HTTP error contract drops the original compression and wire-length headers because its body is already decoded; the original content encoding remains in call metadata. This applies to both successful and error responses and does not change request identity or trigger retries. Gzip/deflate regression coverage is in `tests/test_prompt_http_compression.py`.

Blocking pools admit work before submission and drain started work before releasing capacity on cancellation. Their worker limits apply per client, so resource planning must sum concurrently active nodes/models. Waiting payloads are bounded separately by node concurrency and input queues; worker counts are not a process RSS limit. Each blocking function still needs finite I/O deadlines and allocation budgets. SSE assembly and prompt schema validation retain their existing behavior; this reuse does not combine prompts into embedding-style batch requests.

## Completion, failure and persistence

Success requires valid UTF-8/SSE framing, one supported completion, `finish_reason=stop`, `[DONE]`, and a completed HTTP body. Usage can arrive after finish and before DONE. Cumulative usage snapshots replace previous snapshots; they are never summed. Fragmented UTF-8/BOM/CRLF, multiline data and comment heartbeats are supported. The assembled response then passes the existing strict JSON/schema validator before becoming a row output.

The journal reserves the exact request before POST and saves a complete response before publication. Cancellation during a shielded response commit drains the commit; a saved response remains replayable, without a second error write. Storage failures abort execution rather than becoming business rejections.

EOF without DONE, truncation, HTTP-200 provider error events, malformed data, size limits and unsupported deltas are technical failures. `PromptStreamError` has category `incomplete_response`. HTTP non-200 remains `provider_error` with original status/body saved. Timeouts record `timeout_phase=connect|read_idle|write|pool|total`. A first chunk or valid-looking partial JSON never implies success.

Failure entries preserve partial assembled content, latest known usage, unfinished/last event, timings and error detail. Dataset error metadata carries `error_ref` and small technical fields; large partial text is not copied through business tables. A crash/kill can lose unsaved in-memory chunks, but the durable unanswered reservation still blocks blind resubmission. Optional SSE files aid diagnosis; they are not a resumable generation protocol.

Failed/uncertain streams are not automatically retried, even when schema retries are enabled. Existing schema retry applies only to a fully received response failing JSON/schema validation. Same-key reruns stop with `uncertain_call`. Review evidence/possible charges before using the existing `demiflow.operator_llm.recover` inspect/requeue workflow. Requeue is explicit, audited and does not refund durable quota. There is no automatic streaming-to-JSON fallback.

Complete saved HTTP error responses can opt into `http_error_retry`, for example
`{'statuses': [401], 'max_retries': 5, 'backoff_s': [2, 4, 8, 16, 30]}`.
The default is disabled. This requires a writable SQLite journal with an explicit
finite `max_requests`. Only fresh, fully received errors with a declared status
qualify; cached historical failures, uncertain requests, partial streams and
successful responses are never automatically released. Retries use the identical
request, re-enter shared admission, and count against both node and durable budgets.
The journal atomically archives the previous response and reserves the new attempt,
keeping old references readable. At most five retries are permitted; per-key attempt
counts include archived attempts and survive restarts. Backoff is cancellable and
holds no shared HTTP permit. Exhausted authentication retries retain the final error
and stop the shared gate; one recoverable authentication error does not stop it.
Each result's `attempts` trace and the cumulative journal retain the failures and
missing usage; a later success is not evidence that preceding attempts were free.

Replayed HTTP errors retain their original per-row error and response reference;
they do not stop the shared gate or count as fresh authentication observations.
This includes a complete error left when cancellation interrupts retry backoff.
Replay still does not automatically resubmit it. Consumers can retain the row
failure or explicitly requeue exact saved errors using the audited journal API.
Fresh authentication/configuration failures retain their existing fatal behavior
after any declared retry allowance is exhausted. Real HTTP regressions cover
cached 400/401/403/404 followed by a new successful row with no retry/refund.

For a provider's documented per-input rejection, `nonfatal_http_errors` can
declare exact status/code pairs, for example
`[{'status': 400, 'code': 'content_blocked'}]`. The code is read from the saved
JSON body's `error.code` and exposed as `provider_error_code` in call metadata.
At most 16 pairs are allowed, with codes limited to 128 characters. A matching
fresh response remains a failed row (requires `error_output`) without stopping
the shared gate; it is not a successful caption and this option never retries
or rewrites the input. Unlisted codes, missing codes, and mismatched HTTP
statuses retain the default fatal behavior. The declaration is execution
policy, not a change to the request payload or cache identity.

## Identity, metrics and practical limits

Identity contains the actual wire payload. Enabling streaming or the LiteLLM adapter changes it and creates a distinct request. Queue depth, concurrency, timeouts, connection limits and log location do not. Complete same-mode replays open no HTTP pool and create no event files. Switching mode must not conceal an unresolved paid attempt.

`SQLitePromptJournal(..., read_only=True).registered_keys(max_keys=100000)`
provides a bounded snapshot of prior request identities, including saved errors
and unanswered reservations. It reads no response bodies, changes no quota or
records, and rejects excess keys rather than truncating them. Key/set memory is
bounded to tens of MiB at the hard ceiling. Build it before the run's model
writers start, close the reader, and rebuild on resume. It can preserve prior
paid batching without repeated reads competing with live DELETE-journal commits;
it is not a live proof that a request has never been reserved. The consumer must
separately enforce unique input identities and exclusive run ownership.

`call_output` retains references, status, elapsed time, usage and reuse. Streaming adds `transport=sse`, `stream_complete`, `headers_received_s`, and `stream` containing bytes/events, first body byte, first data event, first nonempty content and maximum gap between body chunks. That gap excludes initial and EOF waits. These are client measurements, not inference times. Only selected technical response headers are saved, never authorization headers.

For a single-completion Prompt bound to the native run journal, delivered call
facts describe the **original provider call**. Replaying that same action/node/row
therefore preserves `call_output` (and a returned row error's call facts), including
the original `reused=false` flags in its attempts. This is necessary when downstream
transforms persist or hash the trace: an acknowledged `save_lance` batch must replay
identical data. The actor observes the actual lookup before producing those stable
facts, so current-attempt monitoring still reports `reused=true`, zero newly consumed
tokens and no new request charge. Do not derive current-attempt billing from a
persisted row's call facts. Explicit private caches, offline input stores and Agent
interaction traces keep their existing contracts; this rule does not reinterpret
those external-cache or multi-turn observations. Source/version protection remains
in force: this change does not migrate runs sealed by a different platform revision.

`run_stream().metrics.models` aggregates timings for new calls using bounded histograms. `native_journal_totals` separately reports the entire journal, including known usage from failed streams. Missing usage does not mean free; cached/reasoning subcounts are included in provider totals and not added again.

The LiteLLM adapter requests no router/SDK retry or fallback. Server policies can override request settings, so deployment must honor the contract. Saved response headers show reported retries/fallbacks when available. Provider-internal behavior and billing remain outside client control. Streaming helps proxy timeouts only when actual traffic arrives inside the proxy's inactivity window; it cannot guarantee every request succeeds.
