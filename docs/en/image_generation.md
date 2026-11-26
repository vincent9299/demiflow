# Image generation

Image generation uses `Dataset.map_prompt_async`; there is no separate API. Set `response_format` to `image` in the prompt pack: each call produces one image, writes it to an object directory, and the row receives a reference `{uri, sha256}` to it.

## Quick start

```json
{
  "schema_version": "demiflow_prompt_pack_v2",
  "prompts": {
    "generate": {
      "version": "illustration-v1",
      "response_format": "image",
      "model": {
        "backend": "modelhub",
        "model": "your-image-model",
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
    "generate", config="flows/images.json",
    inputs={"instruction": "instruction", "references": "references"},
    output="image", call_output="call", error_output="error",
    max_requests=8, concurrency=1, queue_depth=1,
    options={
        "object_store": "assets/images",
        "image_encoding": "preserve",
        "limits": {
            "max_images": 3, "max_image_bytes": 1024 * 1024,
            "max_pixels": 1024 * 1024, "max_total_pixels": 3 * 1024 * 1024,
            "max_text_bytes": 16384,
            "max_request_bytes": 8 * 1024 * 1024,
            "max_response_bytes": 2 * 1024 * 1024,
        },
    },
)
images.write_lance("/data/generated.lance", mode="create")
```

The `references` column is a list of reference image bytes (`list[bytes]`); order and duplicates are kept. Without reference images, simply leave the placeholder out of the template.

## Required parameters

Image output has a few more mandatory parameters than text output:

| Parameter | Requirement |
|---|---|
| `output` (or `outputs={"image": "column"}`) | Column for the image reference |
| `call_output`, `error_output` | Both must be given |
| `max_requests` | Must be finite |
| `options["object_store"]` | Directory the images are written to |
| `concurrency` | 1 to 8; `queue_depth` defaults to 1 and is at most 8 |

Image output does not support `routes`, `request_policy`, `token_budget` or `schema_retries`.

## Protocols

`model.api` selects the protocol of the model server explicitly:

| `model.api` | Request sent |
|---|---|
| `images_json` | JSON `/images/generations` or `/images/edits`, reference images as data URLs in order |
| `images` | Multipart image API, keeping the original bytes and types of reference images |
| `chat` | `/chat/completions`, text first and images after it in order |
| `chat_interleaved` | `/chat/completions`, keeping the order of text and images in the template |
| `openrouter_images` | `/images`, reference images in order in `input_references` |

This is compatibility of protocol form; whether a particular vendor endpoint works is yours to verify. Every response must contain exactly one complete image.

## Outputs

| Column | Content |
|---|---|
| `output` column | `{uri, sha256}`; null when the row failed |
| `call_output` column | Record of the call; may be null on failure |
| `error_output` column | Error description string; null on success |

- `image_encoding="preserve"` keeps the JPEG, PNG or WebP encoding returned by the model; other formats are converted to PNG.
- Rows for which `when=function` returns `False` make no model call and get none of these columns.
- The returned `uri` is readable immediately within the same pipeline and can feed later nodes directly, for example to read pixels, score, or edit again:

```python
images.read_object(reference="image", output="pixels", root="assets/images",
                   layout="sha256_prefix2", max_bytes=1024 * 1024)
```

## Where the files go

- `object_store` is a logical directory that can stay the same across runs; existing content is left alone.
- The platform allocates the real paths per run under `.demiflow-objects/` inside it and returns the real, immutable `uri` in the row. There is no run id to pass and no cleanup to manage.
- Images with the same content share storage, but that never saves a model call.

## Failure and recovery

- Model or image errors of a single row (the reply is not an image, the image is truncated, animated or too large) go to `error_output`, and no image is produced.
- Storage errors, exceeded resources and cancellation end the whole action. Cleanup removes only files of this run that were not committed yet.
- Recovery re-checks committed images. A missing or corrupt image, or a request that was charged but whose outcome is unknown, blocks automatic recovery instead of quietly generating again.

## Resource limits

Every item of `options["limits"]` is a hard limit; exceeding it fails:

| Item | Meaning |
|---|---|
| `max_images` | Number of reference images in one request |
| `max_image_bytes` | Bytes of one image (reference or output) |
| `max_pixels` / `max_total_pixels` | Pixels of one image / of all images in one request |
| `max_text_bytes` | Bytes of the prompt text |
| `max_request_bytes` / `max_response_bytes` | Bytes of one request / response |

Before running, the platform reserves output space as `max_requests × max_image_bytes`, so do not set these two far above what you need. The limits bound requests and the images themselves; they are not a hard limit on process memory.

## Limits to be aware of

- Only synchronous endpoints that return one image per call are supported; job-style asynchronous endpoints and multi-image replies are not.
- There is no automatic switch to another model or service on failure.
- Only local execution is supported at present.
