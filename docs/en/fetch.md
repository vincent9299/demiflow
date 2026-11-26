# Fetching, queues and documents

This guide covers three related things:

1. `Dataset.fetch`: downloads the one address of each row completely as a raw object;
2. task queues: separate "what to download" from "who downloads it" so that several processes share the work;
3. after the download: parsing documents, reading objects, handling images.

Fetching only downloads and verifies; it does not parse content. Parsing, image decoding and business filtering are separate later steps.

## Downloading: `fetch`

```python
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore

with RunContext.start("/data/runs/fetch-example"):
    rows = (
        data.from_items([
            {"id": "a", "request": {"uri": "https://example.org/page"}},
            {"id": "b", "request": {"uri": "https://example.org/missing"}},
        ])
        .fetch(store=LocalObjectStore("/data/raw"),
               policy=FetchPolicy(max_object_bytes=64 * 1024**2), concurrency=4)
        .materialize()
        .take_all()
    )

for row in rows:
    print(row["id"], row["result"]["status"], row["result"]["reason"])
# a ok None
# b failed not_found
```

The `request` column of each row is one request; the download result is written to the `result` column (rename them with `request=` and `output=`). Output order is not guaranteed.

### The request

| Field | Meaning |
|---|---|
| `uri` | Address: `http(s)://...` or `cos://bucket/path` |
| `expected_size` | Expected complete size in bytes, also the hard limit for writing |
| `expected_sha256` | Expected SHA256 of the content, verified after the download |
| `source_version` | Pins the source version, e.g. `{"kind": "http_etag", "value": "\"etag\""}` or `{"kind": "cos_version_id", "value": "..."}` |

### The result

| Field | Meaning |
|---|---|
| `status` | `ok`, `failed` (a definite failure such as 404, access denied, size or digest mismatch) or `retryable` (attempts used up; it might still succeed) |
| `raw_ref` | `{uri, sha256}` pointing to the stored raw object; present only for `ok` |
| `size_bytes` | Object size |
| `reason` | Failure reason, e.g. `not_found` |
| `attempts`, `http_requests` | Number of attempts and of requests actually sent |

- Only a 200 with complete content that passed verification and has been stored is `ok`.
- A definite failure stays as a row and does not fail the action; `fetch` has no `catch` and never drops a row whose download failed.
- Invalid configuration, cancellation and an unknown storage outcome end the action.
- Raw content is not decompressed, not parsed and not resumed.

### Download policy

Commonly used items of `FetchPolicy`:

| Item | Default | Meaning |
|---|---|---|
| `max_object_bytes` | 1 GiB | Size limit of one object |
| `max_attempts` | 3 | Most attempts per object |
| `total_timeout_s` | 600 | Total time limit of one object, shared by all attempts |
| `redirects` | 5 | Most redirects followed |
| `per_host_concurrency` | 2 | Concurrency per host |

### Authentication, proxies and safe defaults

```python
from demiflow.collect import CosSource, FetchPolicy, HttpSource, Secret

policy = FetchPolicy(
    max_object_bytes=64 * 1024**2,
    http=HttpSource(headers={"authorization": Secret("SOURCE_AUTH")},
                    credential_origin="https://example.org"),
    cos=(CosSource(bucket="bucket-appid", region="ap-region",
                   secret_id=Secret("COS_SECRET_ID"),
                   secret_key=Secret("COS_SECRET_KEY")),),
)
```

- Credentials are supplied only through environment variables: `Secret("VARIABLE_NAME")`.
- Proxy environment variables of the process are not used automatically; declare `HttpSource(proxy=Secret(...))` when a proxy is needed.
- TLS certificates are always verified; declare your own CA with `ca_file`.
- Only same-origin redirects are followed by default; list other HTTPS origins in `redirect_origins`. Authentication headers and cookies are removed when crossing origins.
- Private and local addresses are refused by default; declare `HttpSource(allow_private=True)` when you really need them.
- Using a short-lived signed URL directly as `uri` is not supported; use a stable address with declared authentication.

## Task queues: several processes share the downloads

A queue stores the task list durably: one producer writes tasks, one or more consumers claim, download and settle them. When a consumer is interrupted, tasks it did not settle are claimed again.

```python
from pathlib import Path
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore
from demiflow.queue import QueueChannel, QueueConfig, ProducerSourceSpec, QueueResultSink

config = QueueConfig(backend="sqlite", queue_id="raw-v1", path="/data/queues/raw-v1.sqlite")

# Producer: writes tasks from a fixed manifest
manifest = Path("/data/manifest.jsonl")   # each line has messages=[task_message(task_id, payload, pool="raw")]
channel = QueueChannel(config)
producer = channel.open("raw", producer_id="manifest-v1",
    source=ProducerSourceSpec(kind="manifest", source_id="manifest-v1",
                              uri=manifest.as_uri(), sha256=manifest_sha256))
with RunContext.start("/data/runs/producer"):
    data.read_json(str(manifest)).enqueue(config, producer=producer).run_stream()
channel.seal(producer)

# Consumer: start as many processes as you like; each claims different tasks
with RunContext.start("/data/runs/worker-1"):
    (data.read_queue(config, pool="raw", max_tasks=1000, max_inflight=8)
        .fetch(store=LocalObjectStore("/data/worker-raw"),
               policy=FetchPolicy(max_object_bytes=64 * 1024**2), concurrency=4)
        .write_datasink(QueueResultSink(config, pool="raw")))
```

Key points:

- Build tasks with `task_message(task_id, payload, pool=...)`; `task_id` must be stable, and the same `task_id` with different content is refused.
- The producer's source must be fixed: a literal, a local JSONL with its SHA, or a pinned version of a local Lance table. Producing runs only with `run_stream()`.
- `channel.seal(producer)` says that all tasks have been written. Consumers may start before that; while the queue is temporarily empty they keep waiting.
- The shape of the consuming chain is fixed: `read_queue → per-row processing → one fetch → per-row processing → QueueResultSink`. Rows cannot be dropped, expanded or batched across tasks in between.
- A task is marked done only after its result has been stored durably. Failed, cancelled and uncertain tasks are not acknowledged.
- To reject a result for business reasons, keep the original `result`, add a field that marks it, and filter after settlement.

Backends:

| Backend | Configuration | Notes |
|---|---|---|
| SQLite | `QueueConfig(backend="sqlite", queue_id=..., path=...)` | The file must be on a local disk, not on a network mount |
| PostgreSQL | `QueueConfig(backend="postgresql", queue_id=..., dsn=Secret("QUEUE_DSN"), namespace=...)` | Install `demiflow[queue-postgresql]`; several machines can share it |

Complete runnable examples are in the repository's `examples/` directory:

```bash
python -B examples/raw_fetch_queue.py      # one producer, two consumer processes
python -B examples/fetch_manifest.py       # fixed manifest → enqueue → download
python -B examples/fetch_metadata_links.py # fetch metadata first, parse links, then enqueue and download
python -B examples/fetch_keywords.py       # search → pick candidates → enqueue → download
```

They use local test servers only and do not touch the external network.

## After the download

### Reading a stored object

```python
from demiflow.objects import ObjectRef

ref = row["result"]["raw_ref"]
body = ObjectRef(**ref).read(max_bytes=8 * 1024**2)   # verifies SHA256 while reading
```

To read row by row inside a pipeline use `Dataset.read_object(reference="column", output="column", root="/data/raw", max_bytes=...)`: it reads only below the declared root, verifies SHA256 and never touches the network.

### Parsing a web page into a document

```python
from demiflow.collect.documents import parse_document

doc = parse_document(body, url=uri, final_url=uri, content_type="text/html")
doc["blocks"][:2]
# [{'kind': 'heading', 'text': 'Title', 'block_id': 'b000000', ...},
#  {'kind': 'paragraph', 'text': 'Public text of the page.', 'block_id': 'b000001', ...}]
```

- Parsing reads only the bytes you give it and never downloads again.
- The result is a normalized document organized in blocks: headings, paragraphs and tables are blocks with stable `block_id`s.
- PDF is not parsed by default; pass `pdf_parser={...}` to turn it on explicitly.
- Parsing extracts structure only; it does not judge whether a source is reliable.

Inside a pipeline, decode stored normalized-document bytes into records with `Dataset.decode_document(column="column", output="column")`.

### Selecting document content for a prompt

`Dataset.read_documents(request="column", output="column", context=...)` reads stored normalized documents, selects the relevant whole blocks for the questions and guarantees that they fit the prompt budget; it does not call a model. The same function can be opened as a tool to [agent operators](agents.md).

### Images

- `demiflow.collect.image_decode.decode_image(uri, sha256, policy)`: verifies the SHA and decodes in a separate process, returning width, height and more. `policy` must give `max_bytes`, `max_pixels`, `max_frames`, `decode_memory_bytes` and `decode_timeout_s`.
- `Dataset.encode_image(column="column", output="column", profile=..., max_side=1536)`: resizes verified image bytes and encodes them as a JPEG suitable for a model, without changing the original object.

A download receipt only says "what was downloaded". Whether the size and content of an image are acceptable is for you to judge after decoding.

## Limits to be aware of

- Concurrency and queues of `fetch` limit rows; the size of one object is limited by `max_object_bytes`.
- The automatically settled queue chain accepts only the one shape above; other shapes are refused before any task is claimed.
- A queue does not start remote processes; you start the consumers on each machine yourself.
