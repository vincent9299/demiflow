# Native image generation

Image output uses `Dataset.map_prompt_async`, the existing `PromptActor`, and one
complete `demiflow_prompt_pack_v2` configuration. A call produces one image; it
does not run a tool loop, select a fallback provider, or cache model results.
The managed local contract supports a fixed synchronous HTTP endpoint with an
explicit provider revision. Real model quality, managed services, Diffusers/GPU,
remote object stores, and asynchronous provider jobs require separate adapters.

```json
{
  "schema_version": "demiflow_prompt_pack_v2",
  "prompts": {
    "generate": {
      "version": "illustration-v1",
      "response_format": "image",
      "model": {
        "backend": "modelhub",
        "model": "your-declared-image-model",
        "api": "images_json",
        "base_url": "http://127.0.0.1:8016/v1",
        "api_key_env": "IMAGE_API_KEY",
        "revision": "your-provider-model-revision",
        "timeout_s": 60
      },
      "template": "{{ instruction }}{{ references | image }}"
    }
  }
}
```

```python
images = rows.map_prompt_async(
    'generate', config='flows/images.json',
    inputs={'instruction': 'instruction', 'references': 'references'},
    output='image', call_output='call', error_output='error',
    max_requests=8, concurrency=1, queue_depth=1,
    options={
        'object_store': 'assets/images', 'image_encoding': 'preserve',
        'prepare_workers': 1, 'response_workers': 1, 'io_workers': 1,
        'limits': {
            'max_images': 3, 'max_image_bytes': 1024 * 1024,
            'max_pixels': 1024 * 1024, 'max_total_pixels': 3 * 1024 * 1024,
            'max_text_bytes': 16384,
            'max_request_bytes': 8 * 1024 * 1024,
            'max_response_bytes': 2 * 1024 * 1024,
        },
    },
)
images.read_object(reference='image', output='pixels', root='assets/images',
    layout='sha256_prefix2', max_bytes=1024 * 1024
).select_columns(['image', 'pixels']).write_lance('datasets/generated.lance', mode='create')
```

The launcher's declared memory, output, scratch and request limits must admit the
node's explicit ceilings. In Forge, register the logical object destination as a
writable location. Parsing/checking reads configuration and resource declarations;
it does not open a client, read credentials, create objects, or start a service.
Paths resolve against the configured project/resource root.

## Protocols and values

| Explicit `model.api` | Native request |
| --- | --- |
| `images_json` | JSON `/images/generations` or `/images/edits` with ordered data URLs |
| `images` | Native multipart image API, preserving reference bytes and MIME types |
| `chat` | `/chat/completions`, text followed by ordered image parts |
| `chat_interleaved` | `/chat/completions`, template image/text part order preserved |
| `openrouter_images` | `/images` with ordered `input_references` |

This is protocol compatibility, not a claim about every vendor endpoint. Each
response must contain exactly one complete raster image. Reference placeholders
bind `list[bytes]`; reference order and duplicates are retained. `preserve` keeps
JPEG/PNG/WebP encoding after validation; other supported raster formats convert
to bounded PNG. `{% text_segment %}` preserves explicit adjacent text boundaries
for the interleaved protocol. Rendering does not reinterpret braces in input text.

`image` is `{uri: str, sha256: str}` or null on an ordinary row error. `call` is the
native trace or null; `error` is a string or null. `outputs={'image': 'destination'}`
is equivalent to `output='destination'`. `when=False` returns the original row
without a call or added columns. Storage, resource, cancellation and recovery
contract failures stop the action. There is no new image-specific Dataset API.

## Ownership, commit and delivery

The logical destination remains stable across incremental runs. The platform
allocates `logical/.demiflow-objects/<run>/<action>/<sink>/<prefix>/<sha256>` and
returns that real immutable URI. Callers do not pass a run id, change technical
paths, claim roots or manage GC. An existing nonempty logical destination is
valid; previous runs and foreign files remain untouched. Only registered files in
this run's own namespace participate in cleanup. Same-content storage reuse does
not suppress a model request.

| Native terminal | Resource and commit boundary | Delivery / recovery |
| --- | --- | --- |
| `materialize()` | Bounded native stream batches; objects and row references share one prepared record | Durable handle; inspect objects before restoring source progress |
| `write_lance()` / durable `write_datasink()` | One native stream; internal bounded row spool and writer batches; standard final sink commit includes objects | Native receipt; recovery of a prepared sink bypasses model generation |
| Bounded `take()` | Bounded stream queue and driver result; internal object/row handoff commits | Readable committed image before return; no replay/consumer-ACK promise |
| `iter_rows()` / `iter_batches()` | One queued handoff, at most 16 rows per handoff and byte ceiling; public batches also obey their byte limit | Readable committed image before yield; early close drains work and cleans unprepared artifacts; no replay promise |
| `run_stream()` | Existing native callbacks; object participants commit after the graph drains | Callback side effects remain delivery-only; no replay promise |

No terminal requires business code to add `materialize()` for correctness.
Standard `Datasink.prepare/commit/inspect/abort`, the existing RunJournal and its
single CommitCoordinator remain the lifecycle. A bounded object manifest and one
native object receipt per root/commit accompany row publication. This introduces
no object executor, request-result journal, selectable recovery mode, or provider
fallback.

The returned URI is physically readable during the graph. Generation can feed
`read_object`, vision scoring, or a subsequent edit directly. Before durable row
publication, the object participant rechecks the owner, encoded byte size and
SHA-256. Recovery inspects even confirmed objects; missing/corrupt committed
images and unknown native receipt outcomes block recovery instead of regenerating.

An external request charged without a prepared result also blocks automatic
regeneration on recovery. This uses one action-level request watermark in existing
prepared object metadata and the existing run budget counters; no per-request
response index is stored. Prepared objects and row references are recovered using
the same RunRef, fixed implementation and original limits. Delivery terminals do
not replay a previously delivered prefix.

## Resource bounds and failures

Actual `object_bytes` count encoded immutable files once per physical artifact;
`row_bytes` count the native row output. Both consume the same `output_bytes`
ceiling. Object manifest/receipt and namespace-owner metadata have separate bounded
control counters. Arrow reference bytes never stand in for image bytes.

Before HTTP, `output_admission_bytes` reserves
`max_requests * max_image_bytes`. Scratch admission reserves the conservative
encoded/staging/decode allowance `max_requests * (3 * max_image_bytes + 65536) +
1024`, shared with native row/state/output scratch. These are conservative capacity
ceilings; actual byte counters stay distinct. Unknown external work is not refunded.
The finite manifest, allocation and commit count ceilings also apply to large runs.

Stream payload admission includes request/response/base64 copies, reference and
output pixels, workers, queues and completed results waiting downstream. It is a
payload bound, not a process RSS measurement. Output validation reuses the existing
restricted decoder subprocess with byte/pixel/frame, address-space, CPU and wall
limits. Parent death terminates that decoder. Native library allocations still need
the enclosing process limit. Public iterator batches that cannot retain their
requested grouping within the byte budget fail explicitly before conversion.

HTTP sends `Accept-Encoding: identity` and rejects compressed image responses
before HTTPX decompression. Bodies and downloads are incrementally byte bounded;
image downloads have no model authorization, redirects or file URL support.
Malformed Base64, truncated/animated/oversized images and unsupported response
cardinality do not publish an image. ENOSPC and cancellation drain started native
work; cleanup removes only registered unprepared artifacts and retains prepared,
committed and foreign data.

The runnable synthetic HTTP, same-graph, terminal and process-loss regressions are
`tests/test_managed_image_execution.py`, `tests/test_object_participant.py`,
`tests/test_prompt_image.py` and `tests/test_prompt_without_journal.py`. They require
no model weights or GPU. Hardware power-loss durability and real provider behavior
are separate validation scopes.
