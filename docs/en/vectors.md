# Vectors: embedding, indexing, search

Three steps make a vector search pipeline:

1. `Dataset.map_embeddings` turns text, images or document spans into vectors;
2. `data.ensure_lance_vector_index` builds an index on the vector column of a Lance table;
3. `Dataset.search_vectors` runs one search per query row and writes the candidates back to that row.

Embedding calls a model server through an OpenAI-compatible `/v1/embeddings` endpoint. Install with `demiflow[embeddings]`; the model server and its weights are yours to provide.

## Quick start

```python
import pyarrow as pa
from demiflow import data
from demiflow.embeddings import EmbeddingModel

model = EmbeddingModel(
    name="my-embedding-model", revision="2026-01", dimensions=1024,
    base_url="http://127.0.0.1:8002/v1",
)

# 1. Embed and write. The vector column must be a fixed-size float32 list, hence the explicit schema.
schema = pa.schema([("id", pa.int64()), ("text", pa.string()),
                    ("embedding", pa.list_(pa.float32(), 1024))])
docs = data.read_lance("/data/docs_raw.lance").select_columns(["id", "text"])
receipt = (
    docs.map_embeddings(model=model, inputs={"text": "text"}, output="embedding",
                        batch_size=32, concurrency=4)
    .materialize()
    .write_lance("/data/docs.lance", mode="create", schema=schema, return_receipt=True)
)

# 2. Build the index. The returned uri/version is the pinned version that has the index.
index = data.ensure_lance_vector_index(
    "/data/docs.lance", expected_version=receipt.committed_version,
    vector_column="embedding",
)

# 3. Search.
hits = (
    data.from_items([{"q": "a wooden table near a window"}])
    .map_embeddings(model=model, inputs={"text": "q"}, output="query_embedding")
    .search_vectors(query="query_embedding", output="candidates",
                    uri=index["uri"], version=index["version"],
                    vector_column="embedding", columns=["id", "text"],
                    metric="cosine", top_k=3)
    .materialize()
    .take_all()
)
# hits[0]["candidates"] == [{'id': ..., 'text': ..., '_distance': 0.08}, ...]
```

## Embedding: `map_embeddings`

### Declaring the model

`EmbeddingModel` describes one fixed model revision and its protocol:

| Field | Meaning |
|---|---|
| `name`, `revision` | Model name and the immutable weight revision |
| `dimensions` | Vector dimension; a response that differs is an error |
| `base_url` | Server address |
| `input_format` | `'text'` (default) sends the standard `input: [str]`; `'chat'` sends vLLM's batched messages and is required for image models |
| `normalize` | `True` by default: L2 normalization before output |
| `api_key_env` | Name of the environment variable holding the key; keys never enter request records |
| `request_options` | Processing parameters passed through to the model server |
| `encoding_parameters` | Settings that affect vectors, such as backend version, precision and template; they separate caches |

Queries and stored vectors must use the same model declaration. Equal dimensions do not mean the same vector space.

### Inputs

`inputs` maps one kind of input to a column:

- `{"text": "column"}`: a non-empty string.
- `{"image": "column"}`: an image object reference `{uri, sha256}`. The SHA is verified and the image fully decoded locally. A list means alternate locations of the same image, not a multi-image input.
- `{"document": "column"}`: spans of a stored normalized document; `Dataset.document_embeddings(model=..., document="column")` is the convenient form.

### Outputs and failures

- The `output` column is a float32 vector; existing columns are kept.
- `call_output="column"`: information about the call (latency, cache hit, usage). Usage describes the whole batch; do not sum it across rows.
- `error_output="column"`: an unreadable or invalid input stays as a row with a null vector and the reason in this column. Without it such an input fails the whole action.
- Configuration errors, an unavailable service and responses that break the protocol always end the action; no empty vectors are produced.
- `materialize()` before relational operations or ordinary writers; `run_stream()` executes directly. Only local execution is supported at present.

### Concurrency and batching

| Parameter | Default | Meaning |
|---|---|---|
| `batch_size` | 16 | Most inputs per request |
| `concurrency` | 1 | Requests in flight at once |
| `queue_depth` | — | Most upstream rows waiting |
| `prefetch_batches` | 0 | Extra batches prepared ahead while requests are in flight |
| `max_requests` | unlimited | Upper bound on the number of request batches |
| `request_policy` | — | Adapts concurrency to latency and errors, e.g. `{"min_concurrency": 1, "initial_concurrency": 2, "max_concurrency": 4}` |

Commonly used `options`:

| Option | Default | Meaning |
|---|---|---|
| `timeout_s` | 300 | Total time limit of one request |
| `max_image_bytes` | 64 MiB | Size limit of one image |
| `max_decode_pixels` | 100 million | Decoded pixel limit of one image |
| `max_request_bytes` / `max_response_bytes` | 96 / 64 MiB | Size limits of one request and response |
| `batch_request_bytes` | off | When set, a batch is split into several requests by this target size |
| `prepare_workers` / `response_workers` / `io_workers` | 2 each | Threads for image preparation, response parsing and request records |

These limits bound requests and single objects, not the memory of the whole process. When memory is tight, lower `concurrency`, `batch_size` and the image limits.

### Model service

- `service=None` (default): uses the external server at `base_url`; the platform neither starts nor stops it.
- `service=VLLMService({...})`: managed by the platform. It starts when the first request that really needs the model arrives and stops when the action finishes, fails or is cancelled; it does not start when everything is served from cache.

```python
from demiflow.services import VLLMService

service = VLLMService({"model_path": "models/my-model", "runner": "pooling",
                       "dtype": "bfloat16", "gpus": [0]}, root=workspace)
rows.map_embeddings(model=model, inputs={"image": "image"}, service=service)
```

When two models have to use the same GPUs one after the other, `materialize()` between them.

## Indexing: `ensure_lance_vector_index`

Builds an `IVF_HNSW_SQ` index (cosine distance) on a fixed-size float32 vector column of a Lance table, on CPU.

```python
index = data.ensure_lance_vector_index(
    "/data/docs.lance", expected_version=version, vector_column="embedding",
    options={"num_partitions": 256, "m": 16, "ef_construction": 200},
)
```

Key entries of the returned dictionary:

| Key | Meaning |
|---|---|
| `uri`, `version` | The pinned version that has the index; search against it |
| `status` | `ready` means the index fully covers this version; an empty table gives `empty` |
| `action` | `build`, `rebuild`, `reuse` or `empty` |

- An existing index with the same parameters and full coverage is reused without creating a new version.
- New vectors in the table or changed parameters cause a complete rebuild. There is no incremental update yet.
- A failed build, a version changed by someone else or a failed verification raises; do not treat the table as indexed in that case.
- `num_partitions` (default 256) cannot exceed the row count of the table; choose a smaller value explicitly for small tables.
- Resource options: `threads` (default 8), `memory_bytes` (8 GiB), `max_rss_bytes` (20 GiB), `timeout_s` (3600 seconds).

## Search: `search_vectors`

Every input row supplies one query vector, is searched once, keeps the original row and gets the candidate list written to it.

| Parameter | Meaning |
|---|---|
| `query` | Column of the input row holding the query vector |
| `output` | Column the candidate list is written to |
| `uri`, `version` | The table searched and its pinned version; without `version` the latest version at the first open is pinned |
| `vector_column` | Vector column of the table |
| `columns` | Columns returned with each candidate; must be given explicitly |
| `metric` | Distance metric, e.g. `cosine` |
| `top_k` | Most candidates per query, default 20 |
| `concurrency` | Queries running at once, default 4 |

- Candidates are sorted by `_distance` ascending. It is a distance, not a similarity score.
- Every input keeps one row; no hit gives `[]`. Repeated queries are not deduplicated.
- Input, storage and search errors end the action; an error is never hidden behind empty candidates.
- Under concurrency the output order may differ from the input order.

### Filtering

- `filter="status = 'accepted'"`: a fixed SQL condition.
- `filter_column="column"`: an SQL condition or `None` read per row. When both are given they are combined with `AND`.

The filter applies before the search: the scope is restricted first and top-k is chosen afterwards. SQL conditions should be built by trusted programs.

### Index parameters

Passed through `options`:

| Option | Default | Meaning |
|---|---|---|
| `use_index` | `True` | Use the index; without one an exact scan runs automatically. `False` forces an exact scan, useful for measuring recall |
| `nprobes` | Lance default | Number of partitions probed |
| `ef` | Lance default | Candidate pool size of the HNSW search; an explicit value must be at least `top_k × refine_factor` |
| `refine_factor` | Lance default | Factor by which candidates are enlarged and re-ranked with the original vectors |
| `max_search_candidates` | 10000 | Upper bound on the candidate pool; exceeding it is an error at declaration time |
| `max_result_bytes` | 4 MiB | Size limit of one query result; exceeding it is an error, not a truncation |

Larger `nprobes`, `ef` and `refine_factor` raise recall and cost time.

### A single query

Outside a stream, for one search with one vector, use `data.vector_search_lance(uri, vector, vector_column=..., top_k=...)`; it returns a Dataset.

## Limits to be aware of

- Embedding, indexing and streaming search currently support local execution only.
- The budgets bound the size of requests, candidates and single objects; they are not a hard limit on process memory. For hard isolation, put the whole job into a separate process limited in memory and time.
- Search builds no index and changes no data; vectors written after the index was built take part in the search through an exact scan until the index is rebuilt.
