# Prompt Dataset operators: HTTP streaming and connection lifecycle

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

`run_stream().metrics.models` aggregates timings for new calls using bounded histograms. `native_journal_totals` separately reports the entire journal, including known usage from failed streams. Missing usage does not mean free; cached/reasoning subcounts are included in provider totals and not added again.

The LiteLLM adapter requests no router/SDK retry or fallback. Server policies can override request settings, so deployment must honor the contract. Saved response headers show reported retries/fallbacks when available. Provider-internal behavior and billing remain outside client control. Streaming helps proxy timeouts only when actual traffic arrives inside the proxy's inactivity window; it cannot guarantee every request succeeds.
