# Native image generation

`Dataset.map_image_async` is a native streaming node. It uses the same
`compile_template` / `render_template` implementation as prompt nodes. Supply a
versioned `{name, version, template}`, explicit model configuration, exact row
bindings, three distinct output/call/error columns, a journal path, object-store
directory and finite `max_requests`. Bind image placeholders to lists of bytes;
external image URLs are rejected. The single-request API is
`ImageGenerator.generate(values)`; the Dataset actor uses that implementation.
This node is not yet exposed in the AgentMap operator catalog.

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

The policy is an explicit argument of both `map_image_async` and `ImageGenerator`,
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
endpoint and expected provider model. The native actor owns startup/shutdown,
GPU/port locks and readiness checks through the standard service manager. Startup
is deferred until a cache miss has reserved request budget and saved the input;
empty, skipped and fully cached inputs do not load weights. Concurrent requests
share one service startup. Deployment errors stop the node and retain the failed
reservation; they are not retried. The service is released after in-flight native
work drains, including cancellation. Use the async actor/Dataset lifecycle for
managed services; standalone synchronous `generate` does not own an event loop.
Request concurrency does not assert GPU inference parallelism: a server may
serialize its own mutable model. Service command flags affecting generation must
also be represented by an explicit model `revision` or request parameters.

`SharedHTTPService` can instead share one lazy deployment across separate Dataset
actors/processes. Image actors acquire its request slots around transport and
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
explicitly adjusted. `max_requests` bounds persistent reservations and therefore
artifact growth within one node. Queue depth is 1 by default, at most 8; HTTP concurrency is 1..8; local model
concurrency is 1. Parallel arms use separate nodes/processes and explicit devices.
The actor drains native work before cancellation cleanup and releases its model
and journal on stream exit. HTTP has a configured finite socket timeout (at most
3600 seconds), plus an elapsed deadline checked while reading chunks; a blocking
read may finish as late as one socket timeout after the deadline. Local GPU kernels
are not preemptible; model weights and library scratch memory remain the caller's
explicit process/GPU capacity responsibility, not an RSS guarantee from pixel
limits. No claim is made that arbitrary Diffusers parameters have a time bound.

Offline contract tests in `tests/test_image_generation.py` exercise recorded input
versus transmitted payload, image order, replay, uncertain failures, byte/pixel
limits and the native Dataset node. Production inference is separate from tests.
