# Native embedding nodes

`Dataset.map_embeddings` turns text or verified image objects into vectors through
an OpenAI-compatible `/v1/embeddings` endpoint. It uses the existing local stream
workers, bounded queues, action lifecycle and SQLite HTTP request journal. Install
`demiflow[embeddings]`; model servers and their weights are separate dependencies.
The platform contains no selected model, business paths or publication policy.

## Normalized document spans

`Dataset.document_embeddings(model=model, document='document_input', **execution)`
is the document input form of the same native node. It reuses `map_embeddings`
batching, byte packing, request admission, three blocking pools, HTTP keepalive,
journals, managed services and cancellation. It does not start another runtime.

Each input is `{document_ref: {uri, sha256}, prefix, spans, text_sha256}`. Spans
contain `block_id`, `start`, `end`: half-open Unicode character offsets in the
verified normalized document. Text is exactly `prefix + '\n\n'.join(spans)`;
its SHA256 must match `text_sha256`. Optional `model_prefix` is prepended only
after that verification (for model-specific passage instructions). The receipt
records the final input text hash, document hash and span count. No body column
is added to the output row. Segmentation and document/section/passage meaning
remain consumer decisions; this node never truncates input.

Document limits in `embedding_options`: `max_document_bytes=8 MiB`,
`max_document_blocks=100000`, `max_document_spans=4096`,
`max_document_text_bytes=256 KiB`, `document_cache_bytes=128 MiB`,
`document_cache_entries=256`. The LRU retains verified block strings only and
counts their Python object sizes; set cache bytes to zero to disable retention.
One additional encoded/decoded document per preparation worker and each batch's
prepared strings/JSON can coexist with this cache. JSON decoding and allocator
overhead mean these limits are not an RSS hard cap. Cache lifecycle ends with
the action, including cancellation. Missing/corrupt objects, invalid intervals
and input budget errors obey `error_output`; provider/journal/storage commit
failures retain the existing fail-stop behavior. A cached URI is assumed immutable
for the lifetime of that action, as required by the ObjectRef content contract.

## Declaration and execution

```python
from demiflow import data
from demiflow.embeddings import EmbeddingModel

model = EmbeddingModel(
    name='my-embedding-model', revision='immutable-weight-revision',
    dimensions=4096, base_url='http://127.0.0.1:8002/v1',
    input_format='chat', normalize=True,
    encoding_parameters={'backend': 'vllm', 'version': 'declared-server-version',
                         'dtype': 'bfloat16', 'template_sha256': 'actual-template-digest'},
)
encoded = (
    data.from_items([{'id': 'query-1', 'query': 'a wooden table near a window'}])
    .map_embeddings(model=model, inputs={'text': 'query'}, output='embedding',
        batch_size=8, concurrency=4, queue_depth=8,
        call_output='embedding_call', error_output='embedding_error',
        options={'io_workers': 2, 'sqlite_journal': {'path': 'calls.sqlite'}})
    .materialize()
)
```

`inputs` maps exactly one modality (`text` or `image`) to a column. Text must be
a nonempty string. An image is an `ObjectRef` or `{uri, sha256}`; a list means
alternate locations **for the same SHA**, tried in order. It does not mean a
multi-image composition. Images are SHA-verified and fully decoded locally.
`EmbeddingModel(image_transport='png')` preserves the original contract: first
frame, EXIF-transpose, RGB conversion and PNG data URI. With
`image_transport='original_if_compatible'`, single-frame RGB JPEG/PNG/WebP without
orientation changes or transparency use their original bytes and MIME type,
including RGB images with an ICC profile. The profile is preserved; the compatible
backend must interpret it identically on the original and normalized paths. Other
images follow the same normalized PNG path. This avoids lossless
re-encoding of already compatible images; it does not resize or recompress JPEG.
The transport policy participates in the model contract; compatible-original v2
adds ICC passthrough and has a different identity from v1. PNG compression level
changes exact request bytes/cache keys, but not normalized pixels/vector identity.

`input_format='text'` sends standard `input: [str]`. `input_format='chat'` sends
vLLM's batched `messages: [[message], ...]`, one user message per input; use this
for multimodal models and their text queries. The deployed endpoint must support
that protocol. `request_options` carries explicit model processor/pooling options,
such as `mm_processor_kwargs`; reserved protocol fields cannot be overridden.
There is no automatic fallback to another protocol or model.

Responses are mapped by `data[].index`, regardless of response order. Cardinality,
unique indices, model name when supplied, dimension, finite values and nonzero
norm are checked. `normalize=True` applies L2 normalization before float32 output.
Existing input columns survive. `call_output` holds the batch request reference,
index, computation time, replay flag, latency, provider usage and selected image
URI. Usage describes the whole batch: do not sum it across output rows. Native
stream metrics count it once per request.

`error_output` preserves unreadable or invalid input as a row with null vector.
Configuration, service, provider, output-protocol and storage failures stop the
action. Empty/all-invalid input does not start a model service. Use `materialize()`
before relational operations or ordinary writers; `run_stream()` is the direct
stream action. Ray is explicitly unsupported.

## Resources and deployment

`batch_size` bounds inputs per request, `concurrency` bounds concurrent request
exchanges even when a supplied shared gate permits more. `prefetch_batches`
(default 0) adds batch workers to prepare subsequent inputs while HTTP is busy.
The total batch workers are `concurrency + prefetch_batches`; `queue_depth` bounds
upstream rows. No unbounded producer or future-per-image queue is created.

Account for `(concurrency + prefetch_batches) × max_request_bytes`, serialization
copies, response objects and completed output batches, plus simultaneous decoded
images. `prepare_workers` limits preparation jobs (one batch's images are decoded
sequentially); `io_workers` handles journals, while `response_workers` independently
handles response JSON parsing and vector checking. Each pool acquires a slot
before submitting a job, so conversion and journal work cannot occupy response
workers. Responses awaiting a worker remain bounded by the total batch workers
and response byte limit. Each node owns these three pools, its HTTP pool and
service owner for the action. Limits add across concurrently active nodes.
HTTP admission remains independent of CPU prefetch.

`max_image_bytes` bounds original and normalized encoded bytes; PNG buffers reject
writes before exceeding that budget. `max_decode_pixels` is checked before pixel
decode, and base64 expansion is checked before allocation. Aggregate requests are
also bounded. By default oversized batches stop with an explicit error. Set
`batch_request_bytes` to enable packing below a target size (at most
`max_request_bytes`). A group is split before constructing its aggregate JSON;
one item larger than the target travels alone only if its complete request fits
the hard limit. An individually oversized input follows `error_output`. Images
are read/decoded once, including the carry item that triggers a split. Groups
retain their original rows and independent exact-request journal identities.
Packing changes request grouping, not image pixels or the model contract. With
the same input order, row batch and packing targets, a rerun reconstructs the same
requests; changing these can change request keys. A batch worker processes its
groups sequentially, while other workers remain concurrent. It retains one
group plus one carry item; account for their content strings, encoded fragments
and aggregate HTTP bytes (multiple copies), not just the target size. Completed
vectors remain bounded by the original row `batch_size`. Decode/conversion
can retain multiple pixel buffers (roughly up to four RGB/RGBA-sized copies per
preparation worker), plus decoder-specific allocations; these are not an RSS hard
limit. Lower pixel/byte/worker budgets for constrained hosts. Input/output row
buffers and writer/materialization budgets remain separate platform constraints.
Cancellation drains started blocking work and closes owned resources.

`batch_decode_pixels` optionally splits image groups by their summed original
decoded pixel count, independently of encoded bytes. This matters for highly
compressible images: small HTTP bodies can still require large decoded buffers
on the server. A single image above this packing target travels alone and must
still satisfy `max_decode_pixels`; no image is resized by this option. The
largest request is therefore bounded by the larger of these two pixel budgets,
as well as the request byte limit. Account separately for concurrent requests,
backend preprocessing copies and model tensors; neither packing target is an RSS
limit. Successful per-image call metadata records `image_pixels`.
An operator stop or another node's failure stops new dispatch and lets already
admitted journaled requests finish within their original deadline, saving the
response without publishing a cancelled row. Explicit caller cancellation aborts
the exchange and records uncertainty. This drain rule is shared with prompt nodes.

```python
from demiflow.services import VLLMService
from demiflow.embeddings import embedding_execution_config

service = VLLMService({
    'model_path': 'models/my-model', 'runner': 'pooling',
    'chat_template': 'models/my-model/embedding_chat_template.jinja',
    'dtype': 'bfloat16', 'gpus': [0], 'max_num_seqs': 16,
}, root=workspace)
execution = embedding_execution_config(
    batch_size=16, concurrency=4, prefetch_batches=2,
    request_policy={'min_concurrency': 1, 'initial_concurrency': 2, 'max_concurrency': 4},
    options={'prepare_workers': 2, 'response_workers': 2, 'io_workers': 2,
             'batch_request_bytes': 32 * 1024**2, 'batch_decode_pixels': 64_000_000},
)
# source.map_embeddings(model=model, inputs={'image': 'image'}, service=service, **execution)
```

`embedding_execution_config(...)` is the public, JSON-compatible configuration
builder used by `Dataset.map_embeddings` itself. It normalizes and validates
worker, queue, packing, HTTP, journal and admission options without opening files
or starting threads/services. `embedding_options(...)` exposes the same runtime
option validation separately. Model semantics, service placement and business
input/output fields remain separate declarations. Store the normalized execution
configuration when reproducibility matters; do not duplicate its validators in
business pipelines. Hardware-specific measured values are explicit run settings,
not defaults for other models.

`VLLMService` and `ManagedHTTPService` start on the first uncached valid request,
and stop when the action finishes, fails or is cancelled. Fully cached requests
do not start them. `service=None` uses an external endpoint and never stops it.
Managed services use existing GPU/port locks and only terminate their own process
group. Materialize between models that must sequentially reuse the same GPUs.

For `VLLMService`, `resource_wait_timeout_s` optionally bounds the wait for busy
GPU/port resources. The default is `0` (fail immediately, preserving existing
behavior). With a positive finite timeout, the actor waits before starting its
child and before model HTTP execution; it releases partial lock acquisitions on
every unsuccessful attempt. Resource waiting is separate from
`startup_timeout_s`, uses `poll_interval_s`, and reports at
`startup_log_interval_s`. Timeout aborts the action as a resource failure;
`aclose()` cancels waiting without disturbing the current owner. This is not a
fair reservation scheduler and does not detect unrelated, uncooperative GPU
users; the existing service startup checks still apply. Tests cover release,
occupied listeners, partial-lock cleanup, timeout and cancellation with local
HTTP fixtures, without GPU/model calls.


`RequestGate` and `AdaptiveRequestGate` are the same admission mechanism used by
`map_prompt_async`. The adaptive gate changes effective concurrent requests based
on fresh response latency and transient errors, bounded by node workers and the
gate maximum. It excludes cache hits and does not retry failed requests. GPU count,
DP/TP, service replicas, memory fraction and processor budgets stay explicitly
configured; there is no automatic GPU/replica scaling or memory-driven batch sizing.

Both operators accept `request_policy={...}` to create an operator-owned adaptive
gate. `demiflow.inference.request_admission_config` can normalize the declaration
before constructing a graph. Its maximum must fit the operator concurrency; an
explicit `request_gate` and `request_policy` cannot be combined. Existing explicit
gate instances remain supported for intentional sharing across nodes. Each action
resets observations, and replayed calls do not consume or train the adaptive gate.

Embedding and the native async HTTP prompt client share the same bounded blocking
I/O pool implementation and common worker/keepalive/history-audit validation.
Admission precedes thread submission; cancellation drains started work before
releasing its slot, and action/client shutdown drains and closes owned pools.
Images/embedding batch semantics remain in this adapter: prompt requests are not
merged into an embedding-style batch. These bounds apply to submitted jobs;
waiting caller payloads, HTTP copies, materialization and backend memory still
need their separate budgets.

| Runtime option | Default |
| --- | --- |
| `io_workers` | 2; journal I/O pool |
| `prepare_workers` | 2; independent image preparation pool |
| `response_workers` | 2; independent response parsing/validation pool |
| `png_compress_level` | 6; lossless PNG compression, integer 0–9 |
| `profile_path` | None; optional JSON aggregate report for this action |
| `collect_journal_totals` | True; opt out of the full historical audit at action drain for large journals |
| `max_image_bytes` | 64 MiB, both original bytes and prepared PNG |
| `max_decode_pixels` | 100,000,000 |
| `max_request_bytes` / `max_response_bytes` | 96 / 64 MiB |
| `batch_request_bytes` | None; opt-in packing target, positive and no greater than `max_request_bytes` |
| `batch_decode_pixels` | None; opt-in positive aggregate image pixel target, with larger valid images sent alone |
| `timeout_s` | 300 s, total HTTP exchange |
| `connect_timeout_s` / `read_timeout_s` | 10 / 120 s; read is idle timeout |
| `write_timeout_s` / `pool_timeout_s` | 120 / 10 s |
| `trust_env` | False; no implicit proxy routing for local services |
| `sqlite_journal` | None; or `{path, timeout_s?, read_only?}` |

`api_key_env` belongs to `EmbeddingModel` and names a credential environment
variable. Keys do not enter request journals. An action without a journal can
repeat requests on rerun. `max_requests` counts batches, persistently across the
given journal, or within one action without a journal.

## Identity and recovery

When `profile_path` is set, `call_output.timings` records this action's preparation,
pool waits, read/SHA, decode, normalization/encoding, base64, serialization/hash,
admission/service waits, HTTP, journal, response parsing and vector checking.
Journal aggregates are also split by operation (`journal_lookup`, `journal_reserve`,
`journal_response`, `journal_references`, and `journal_failed` when used). Each
`*_queue_s` measures admission through actual thread start, including scheduling
delay; `*_s` measures execution, including any internal SQLite lock or sync wait.
Response pool waits are client CPU dispatch delays, not GPU queue measurements.
Fresh responses return their already committed references, avoiding a second
reference lookup. Packing profiles count original row batches in
`profiled_batches` and actual HTTP/journal exchanges in `call_records`.
Preparation of a carry image is charged to the preceding preparation step;
phase totals across the action include it once.
The action writes a constant-size JSON aggregate at close, including failures;
normal stream metrics expose the same phase aggregates. All rows of a batch share
its timings: count `index == 0` once, not each row. Phase work overlaps and nested
phases are included in preparation totals; their sums are **not** wall time or
GPU kernel time. Profile counters also include request/input/transport bytes and
original/PNG image counts. Use unique profile paths per independently running node.
The report is current-action diagnostic output, not durable inference evidence;
the existing journal remains the recovery authority. Serving startup and final
table publication must be measured separately by the consuming pipeline.
Managed services accept `vllm_options.enable_logging_iteration_details` (default
false) for backend diagnostics. Availability of actual iteration records depends
on the installed backend and serving path; vLLM 0.28 disables the default stats
logger with multiple API processes, so the four-process pilot emitted no iteration
records. Use backend metrics or a bounded trace to inspect that configuration;
do not infer scheduler occupancy from the flag alone.

`vllm_options.mm_processor_cache_gb` controls the backend's multimodal processor
cache (None preserves the backend default; 0 disables it). The backend replicates
this budget across API and data-parallel processes, so account for the process
count as well as the per-cache size. A one-pass image workload can disable this
memoization while preserving preprocessing and durable client response journals.
This setting does not change the model's pixels or encoder identity.
`vllm_options.enable_prefix_caching` is likewise optional (None preserves the
backend default). Disabling both caches is an explicit choice for workloads
without repeated image/prompt prefixes; it does not disable client journals.
More API processes do not by themselves guarantee balanced connection traffic.
Measure actual GPU occupancy and fresh-request throughput before raising limits.

Image data URI fragments use a specialized serializer for the known ASCII
MIME/base64 alphabet, producing exactly the sorted canonical JSON bytes. This
avoids another JSON escaping pass over large strings; SHA verification, complete
decode, preprocessing, HTTP body and request keys remain unchanged.

`EmbeddingModel.contract()` / `.fingerprint` include the declared revision,
protocol, output shape, normalization, request options and encoding parameters.
Declare the actual serving backend/version, precision and template identity.
The caller is responsible for matching deployed weights to the declaration;
the embeddings response cannot prove a remote model's weight revision. Endpoint,
GPU IDs, queue size and request concurrency do not define vector space.

The native SQLite journal keys the **exact batch HTTP body plus model contract**.
Embedding preparation serializes each input once, assembles the canonical HTTP
body from bounded fragments, then hashes the request envelope with those same
bytes. This preserves existing request keys while avoiding repeated serialization
of inline images. The canonical bytes/key invariant is covered by regression tests.
Completed provider responses persist before vector interpretation; malformed
successful responses replay the same validation failure. HTTP error responses are
retained with their status and can be explicitly released using
`SQLitePromptJournal.requeue_http_errors`. Transport failures and cancellation
leave unanswered reservations and require `requeue_uncertain` after confirming
the old writer has stopped. Both recovery APIs archive the prior attempt and
retain its budget usage. No retry or uncertainty deletion happens inside the node.

Request metadata replaces inline image data with content digests. Source images
remain in the object store. Replay therefore still reads/prepares input objects.
Changing batch membership/size may miss the exact-request cache. Per-image durable
reuse and public vector-table versioning belong to the consuming pipeline.

Read-only journals reject missing calls before deployment or HTTP dispatch.
Native stream metrics report this action's calls and admission separately from
whole-journal totals, including earlier attempts.
`collect_journal_totals=False` retains all per-action counters and durable
reservations/responses while skipping the final historical JSON aggregation.
The metrics report explicitly lists skipped journal audits; missing totals are
not reported as zero. This avoids scanning every historical embedding vector
merely to close a small or fully reused action. Explicit `journal_totals(path)`
remains available for a deliberate audit. Other native node defaults are unchanged.

PNG fallback omits an ICC profile only when it exceeds Pillow's PNG text-chunk
decompression budget. This avoids producing a PNG the backend cannot open from
a valid CMYK JPEG/TIFF. RGB pixels after the declared EXIF/mode normalization
remain identical; ordinary profiles and compatible original bytes are preserved.
The real WeMM/vLLM loader was checked with the triggering 1.5 MB profile.

`keepalive_expiry_s` (default 5, nonnegative seconds) configures idle HTTP
connection retention; 0 opens a fresh connection for each request while retaining
the request concurrency limit. This is transport configuration, not an inference
parameter or automatic retry policy. Real HTTP/1.1 tests verify reuse versus fresh
connections.

PNG normalization removes scalar/byte transparency metadata invalid for RGB
after conversion. Invalid EXIF parsing/serialization falls back to a readable
orientation, or stored pixels when the orientation cannot be read; malformed
EXIF is omitted from the transport PNG and counted in the profile. Decoder
SyntaxError/struct/EOF/zlib failures are input failures for the affected row.
Original image objects and their SHA remain unchanged.
