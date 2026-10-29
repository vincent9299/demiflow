# Image output through the unified prompt runtime

`Dataset.map_prompt_async` handles image output through the same
`demiflow_prompt_pack_v2` configuration entry as text and JSON. Set
`response_format: image` on the prompt definition. The image backend uses the same
`compile_template` / `render_template` implementation as text prompts. Supply a
named, versioned template, explicit model configuration, exact row
bindings, three distinct output/call/error columns, a journal path, object-store
directory and finite `max_requests`. Bind image placeholders to lists of bytes;
external image URLs are rejected. Execution uses the same `PromptActor`,
`AsyncOperatorLLMRuntime` and `AsyncOperatorLLMClient` as text and JSON.
This node is not yet exposed in the AgentMap operator catalog.

```yaml
schema_version: demiflow_prompt_pack_v2
prompts:
  generate:
    version: "1"
    response_format: image
    model:
      backend: modelhub
      model: configured-image-model
      api: images_json
      base_url: http://127.0.0.1:8005/v1
      api_key_env: ""
      revision: deployed-weights-and-adapter-version
      parameters: {seed: 42, size: 1024x1024}
    template: |
      {{ instruction }}
      {{ caption }}
      {{ images | image }}
```

```python
dataset.map_prompt_async(
    'generate', config='generation.yaml',
    inputs={'instruction': 'task', 'caption': 'reference_caption', 'images': 'images'},
    output='generated_image', call_output='generation_call', error_output='generation_error',
    options={'sqlite_journal': {'path': 'generation.sqlite'}, 'object_store': 'objects',
             'image_encoding': 'preserve'},
    max_requests=30, concurrency=1, queue_depth=1,
)
```

The example is declarative; the endpoint and deployment must already be configured
or supplied through the existing `service` argument. `response_format: image`
has a fixed single response property `image`, containing `{uri, sha256}`.
`output` binds that value; `outputs={'image': 'generated_image'}` is equivalent.
`response_schema` may be omitted, or must match the fixed schema. Schema retries
must be zero. Row text, including a full caption, is rendered once; template-like
text in a row is not reparsed. Image bytes remain in placeholder order. A backend
whose wire protocol has separate prompt/image fields applies its documented
layout; arbitrary text/image interleaving and per-image ViT/VAE routing are not
implied by the template syntax.

Image options are closed: `sqlite_journal: {path: ..., timeout_s: ..., read_only: ...}`,
`object_store`, optional `image_encoding`, `limits`, and the common bounded
I/O-worker/connection-pool settings. HTTP timeout and generation parameters remain in
the model configuration. Image output requires local `map_prompt_async`; sync
`map_prompt`, agent tasks, routed prompts, token budgets, request gates and
offline/Codex options reject it explicitly. Text/JSON configuration is unchanged.
The removed `map_image_async` Dataset method has no compatibility alias.

There is no separate image actor or public standalone generator. The former
`operator_llm/image.py` and top-level `image_generation.py` are removed, without
import aliases. `operator_llm/media.py` only validates/converts image payloads and
adapts wire formats; it cannot schedule work, call HTTP, reserve budgets or manage
journals/services. `diffusers_transport.py` only owns lazy local model execution
and draining its non-preemptible work. Both feed the ordinary prompt client.
The declared provider configuration is normalized into `PromptModel`; there is
no parallel `PromptDefinition.image_model`.

The image protocol retains its exact recorded request format, rather than adding
a pack hash or output-format flag to the provider request/cache key. Unchanged requests reuse old journals;
changed rendered captions, image bytes or model parameters do not. Historical
request artifacts and responses are not rewritten.

Supported backends are local Diffusers and explicitly selected HTTP
`images`, `images_json`, `chat`, or `openrouter_images`. Diffusers uses configured local weights,
GPU, seed and parameters. The OpenRouter adapter uses `/images` with
`input_references`; OpenAI-compatible image edits use ordered multipart images.
One output image is required. No API/provider fallback or automatic retry occurs.
`images_json` explicitly selects a local JSON Images contract: generations use
the usual JSON body, edits use an ordered `image` list of data URLs with the actual image MIME type. It is
distinct from the multipart `images` API and is never selected as a fallback.

`image_encoding="png"` is the backward-compatible default. It decodes each
input to RGB and re-encodes it as PNG. `image_encoding="preserve"` validates
and passes through single-frame JPEG, PNG and WebP byte for byte, including
metadata, color mode and resolution. Other Pillow-decodable single-frame formats
(such as GIF/BMP/TIFF) are converted to RGB PNG. Animated/multi-frame inputs are
rejected in preserve mode rather than silently selecting a frame. No resizing,
EXIF orientation normalization or lossy recompression is performed. A remote
provider must support the chosen wire format; rejection is not retried with a
different encoding. Direct Diffusers still receives decoded RGB pixels.

The policy is `map_prompt_async`'s `options.image_encoding`,
not a provider generation parameter. JSON and chat data URLs, OpenRouter
references and multipart filename/content-type use the detected format. Recorded
inputs contain those exact bytes. Non-default encoding of image requests is
included in request identity; default PNG and text-only request identities stay
unchanged. Switching encoding does not reinterpret historical journal entries.
The output-image encoding contract is unchanged.

Source and encoded image bytes are both limited by `max_image_bytes`; pixel/count
checks precede full decode, and PNG writes stop at the smaller of the image limit
and the remaining encoded request budget. Request accounting includes base64
expansion and a final serialized-size check. A compressed source fitting the
limit does not imply its PNG conversion fits. Preserve avoids that expansion for
supported formats but still fully decodes them for validation, one at a time.
Pixel limits, encoded buffers, base64/JSON copies and bounded in-flight requests
are separate allocations, not a process RSS guarantee.

HTTP nodes can declare `service=ManagedHTTPService(...)` with a matching loopback
endpoint and expected provider model. The shared prompt runtime owns startup/shutdown,
GPU/port locks and readiness checks through the standard service manager. Startup
is deferred until a cache miss has reserved request budget and saved the input;
empty, skipped and fully cached inputs do not load weights. Concurrent requests
share one service startup. Deployment errors stop the node and retain the failed
reservation; they are not retried. The service is released after in-flight native
work drains, including cancellation. Use the async Dataset lifecycle for managed services.
Request concurrency does not assert GPU inference parallelism: a server may
serialize its own mutable model. Service command flags affecting generation must
also be represented by an explicit model `revision` or request parameters.

`SharedHTTPService` can instead share one lazy deployment across separate Dataset
actors/processes. The common runtime acquires its request slots around transport and
release their leases on close. The normal persistent-service default is retained;
`stop_when_idle=True` stops the managed supervisor when the last user closes,
serialized with new lease acquisition. Closing one arm cannot stop another arm's
service. Explicit manager cleanup may still be needed after abrupt process death;
OS lease release alone does not invoke the last-user shutdown code.

Before inference the actor persists the complete rendered adapter input, including
ordered image bytes, through `JsonArtifactRef`. Base64 is externalized into
verified immutable files. HTTP JSON is sent from this recorded body; multipart
files contain those same recorded pixels; Diffusers gets the recorded prompt,
ordered RGB images and a CPU generator initialized with the recorded seed.
`call.input_ref` reads the complete input. The SQLite journal separately reserves
the full request identity and stores responses/errors. Template name/version/body,
rendered text, images, model/revision and output-affecting parameters participate
in identity; GPU placement and scheduling do not. Unanswered reservations are
uncertain and cannot be silently repeated. Completed responses are reused.

Default bounds per request: 16 images, 256 KiB text, 32 MiB per encoded image,
24 million decoded pixels per image / 48 million total, 64 MiB serialized request
and 64 MiB streamed HTTP response. Inputs over limits fail before reservation;
response overflow stops consumption and retains the failed request. Limits can be
explicitly tightened within these ceilings. `max_requests` bounds persistent reservations and therefore
artifact growth within one node. Queue depth is 1 by default, at most 8; HTTP concurrency is 1..8; local model
concurrency is 1. Parallel arms use separate nodes/processes and explicit devices.
The actor drains native work before cancellation cleanup and releases its model
and journal on stream exit. HTTP uses the existing async connection pool and a
total exchange/download deadline (at most 3600 seconds). No gateway credentials
are forwarded to output-image download URLs. Local GPU kernels
are not preemptible; model weights and library scratch memory remain the caller's
explicit process/GPU capacity responsibility, not an RSS guarantee from pixel
limits. No claim is made that arbitrary Diffusers parameters have a time bound.

Offline contract tests in `tests/test_prompt_image.py` exercise recorded input
versus transmitted payload, image order, replay, uncertain failures, byte/pixel
limits and the unified Dataset prompt node. The image prompt-pack tests also
cover full caption rendering, literal braces, existing native-journal replay,
empty/skipped input, output binding and rejected configuration. Production
inference is separate from tests. No service, environment, model, caption or
production data changes are part of this backend migration.

## Ordered image-generation input

Use `api: chat_interleaved` when the endpoint accepts ordered text/image content
on `/chat/completions`. The request has one user message; its content array follows
rendered `TextPart`/`ImagePart` order exactly, including adjacent text parts. It
uses the existing image response contract: exactly one image URL/data URL in the
assistant message's `content` or `images` array. This is an explicit API choice;
legacy `chat` still sends combined text followed by images, and `images`,
`images_json`, `openrouter_images` and Diffusers keep their existing layouts.

A static `{% text_segment %}` directive starts a new text segment. It is removed
from the model input. Adjacent literals and variable text otherwise keep merging
as before. Empty segments are omitted; whitespace in nonempty segments is retained.
The directive is interpreted only in template source, never in a caption,
instruction or JSON variable. Text/JSON prompt transports also receive these
ordered rendered parts; only a provider that preserves parts can preserve the
boundary. This adds no model-specific interpretation of captions or CFG.

```yaml
# Within the same complete, versioned prompt pack:
# response_format: image; model.api: chat_interleaved
# Other required model fields and budgets are unchanged.
template: |-
  Reference rules and concept: {{ concept }}
  {{ reference | image }}
  Reference caption: {{ caption }}{% text_segment %}

  Generation instruction: {{ instruction }}
```

The endpoint must preserve the two adjacent text entries after the image; joining
them loses the intended boundary. Platform tests compare recorded and transmitted
content. Business adapters must separately verify how these parts become model
inputs and guidance conditions. Switching APIs alone cannot correct a backend
that flattens the message.

At most 255 explicit boundaries are accepted at template compilation, and an
interleaved image request may contain at most 256 rendered parts. The count check
runs before media encoding/reservation. Existing text, image count, pixel, encoded
byte, serialized request, response, concurrency and persistent-request budgets
still apply. No limits are enlarged. Rendered content order and boundaries enter
the existing request record/cache identity; old records are never rewritten.
Tests cover 1/2/3 images with and without captions, literal directives in row
values, adjacent and empty boundaries, replay, changed segmentation, old flat
API behavior and rejection before transport. No production model calls are made.
