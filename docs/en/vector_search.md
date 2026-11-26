# Streaming vector search

`Dataset.search_vectors` is a streaming row operator: each row supplies one query vector, is searched once, keeps the original row and gets a list of candidates written to it. It connects directly to `map_embeddings` and to downstream review or question-writing nodes. Underneath it uses Lance's native `scanner(nearest=...)`; the executor reuses the existing thread workers, bounded queues and actor lifecycle. There is no nested Dataset action.

## Usage

```python
from demiflow import data

# model is the EmbeddingModel that the table being searched was embedded with;
# published_version is the published version that contains the index.
stream = (
    data.from_items([{'query_id': 'q1', 'text': 'a wooden table near a window'}])
    .map_embeddings(model=model, inputs={'text': 'text'}, output='query_embedding')
    .search_vectors(
        query='query_embedding', output='candidates',
        uri=scene_images_uri, version=published_version,
        vector_column='embedding', columns=['sha256', 'image_uri'],
        metric='cosine', top_k=20, concurrency=4, queue_depth=4,
        options={'use_index': True, 'nprobes': 16, 'ef': 200, 'refine_factor': 2},
    )
)
# Connect further downstream nodes after stream and finally execute with run_stream().
# For an ordinary Dataset writer or relational operation, materialize() first.
results = stream.materialize()
```

One output row looks like this:

```python
{
    'query_id': 'q1', 'text': 'a wooden table near a window',
    'query_embedding': [...],
    'candidates': [
        {'sha256': '...', 'image_uri': '...', '_distance': 0.13},
        # At most top_k items, sorted by _distance ascending.
    ],
}
```

`query` is the name of a column of the input row; `vector_column` is the name of the vector field of the table being searched. `columns` must state a non-empty projection explicitly, usually just identifiers and object URIs; `_distance` is added automatically, its meaning is determined by the metric, and it is a distance, not a uniform similarity score. The candidate list is sorted by distance; Lance's parallel reads may deliver result batches out of order, and the operator restores the global order within a bounded top-k. The relative order of candidates at the same distance is not guaranteed. The target vector column must be a fixed-size list of floats; the length, numeric type, finiteness and representable range of the input are checked. With `cosine` a zero query vector is refused. Semantic contracts such as model, weights and normalization are still the caller's to keep consistent; equal dimensions alone do not prove that vectors come from the same space.

Every valid input keeps one row; repeated queries are not deduplicated; no hit gives `[]`. Configuration, input, storage and search errors end the action; they are not turned into empty candidates and rows are not dropped silently. Completion order under concurrency may differ from input order; the query identifier travels with the original row. Local streaming execution is supported at present; Ray reports an error at declaration time.

## Filters and indexes

`filter="status = 'accepted'"` is a fixed SQL condition. `filter_column='candidate_filter'` reads an SQL string or `None` per row; when both are given they are combined with `AND`. The condition runs as a Lance prefilter: the search scope is restricted first and top-k is chosen afterwards. SQL conditions should be built by trusted programs. An ordinary downstream `.filter(...)` filters the query result rows, which is a different thing.

`use_index=True` lets Lance use a compatible vector index; without an index an exact scan still works. `False` forces an exact scan and can be used to evaluate ANN recall; `nprobes`, `refine_factor` and `ef` are then ignored. `nprobes` controls the number of IVF partitions probed, `refine_factor` the enlargement of the candidate set and re-ranking with the original vectors; `ef` of HNSW controls the size of the candidate pool of the graph search. These three parameters are passed straight to Lance for an index search. The index metric should match the query metric; what a parameter does depends on the actual index type. The operator builds no index, changes no data, and does not skip newly added data that is not indexed yet.

### Using HNSW

`IVF_HNSW_SQ` is the index type currently chosen for image search. On the building side `m`, `ef_construction` and the number of IVF partitions determine the index structure; the query side uses `nprobes` and `ef` and does not have to restate the index type in every row request. The input URI and version should come from the snapshot with the index published by the building side; `use_index=True` itself still lets Lance fall back to an exact scan when the index is missing.

An explicit `ef` must be a positive integer with `ef >= top_k * (refine_factor or 1)`. For example, with top_k=100 and refine_factor=5, ef>=500 is required; otherwise an error is reported at declaration time, and the user's `ef` is never enlarged automatically. When `ef` is omitted, the default of the currently pinned pylance 12 applies, that is the enlarged k plus k//2; the default grows with the number of re-ranking candidates.

`max_search_candidates` defaults to 10000; the number of candidates enlarged for re-ranking and `ef` are checked against it at declaration time. Without an explicit `ef` a conservative budget is made from the native default above (also reserved for non-HNSW indexes); an exact scan checks only top_k. It bounds the configured size of the candidate pool; it does not mean that the vectors actually visited, the graph nodes, index loading or process memory are bounded. Exceeding it is a clear error; reduce the parameters or raise the budget explicitly after checking the resources.

The table is opened at the first query of each action, and later queries share the same snapshot and caches. An explicit `version` pins a published version; when it is omitted, the head at the first open is pinned, and the action does not follow appends or index updates midway. After an index is built, use a table version that contains it. The metric `stats.metrics['resources']['VectorSearch:0']` records the actual version, the number of queries and the number of hits. Empty input does not open the table. The reference is released when the action ends and the next action opens the table again; this is not a resident HTTP search service across requests. The older `data.vector_search_lance` remains the data-source entry for a single query.

## Resource bounds

`concurrency` defaults to 4 and limits the queries running at the same time; `queue_depth` defaults to the concurrency and limits the input queue of this node. Each action owns its thread pool and table handle; no futures are created in advance for all inputs. Budgets of different actions are independent, and the caller still has to limit the number of actions running at once.

`options` takes the following closed set; an unknown key is an error:

| Option | Default | Effect |
| --- | --- | --- |
| `use_index` | `True` | Allow the use of a Lance index |
| `nprobes` / `refine_factor` | `None` | Use Lance's default search parameters |
| `ef` | `None` | Use Lance's default HNSW candidate pool size; an explicit value must cover the re-ranking candidates |
| `max_search_candidates` | 10000 | Declaration-time admission limit on re-ranking candidates and HNSW ef |
| `index_cache_size_bytes` | 256 MiB | Index cache budget of this table handle |
| `metadata_cache_size_bytes` | 32 MiB | Metadata cache budget of this table handle |
| `io_buffer_size` | 16 MiB | Native scan I/O buffer budget per query |
| `batch_size` | 64 | Target rows per returned batch, never more than top-k |
| `batch_size_bytes` | 1 MiB | Target bytes per returned batch, never more than the result budget |
| `max_result_bytes` | 4 MiB | Checked limit on the accumulated retained Arrow buffers of one query |
| `max_python_result_bytes` | 32 MiB | Budget for the estimated Python result, checked before conversion |
| `max_query_dimensions` | 65536 | Admission limit on query conversion and table dimensions |

All parameters must be positive integers (except `use_index`; the three ANN parameters allow `None`). Table fields are projected first, and scan readahead is fixed at one batch and one fragment. The row-container budget of top-k is checked at declaration; each returned batch has its Arrow buffers and its estimated Python conversion checked before `to_pylist()`, and exceeding a limit fails clearly without truncating candidates. The estimate covers row dictionaries, scalars, nested list and struct containers and headroom for text expansion, and reserves 24 bytes of sorting workspace per candidate; unsupported complex Arrow types are refused. Retained parent buffers and double counting may cause a conservative refusal; the caller can narrow the projection or adjust the budget.

The amount resident at one time should include: the two kinds of shared cache; `concurrency ×` the native scan and compute working set; the double copy of results during Arrow-to-Python conversion; the candidate lists held by finished workers, downstream queues and consumers; and the original input rows and the readahead of the upstream source. A node queue limits rows only and cannot provide a byte limit for arbitrary input fields or downstream consumers.

These parameters are not a hard limit on process RSS. Lance's decoding, index partition loading, distance computation and sorting allocate in addition, and a huge cell may be allocated before Arrow returns; a check after the return cannot prevent that allocation. The current thread executor also cannot forcibly end a native call in progress: on cancellation it stops new tasks, waits for started calls to return, and then cleans up the thread pool and table reference. Hard memory and time isolation across queries is still a platform gap; a deployment that needs a hard bound should put the whole action into a separate service process limited in memory and time, and must not take this operator's budgets for a process isolation guarantee. Stress tests with millions of rows, remote I/O faults or a hard RSS limit have not been done.

Verification covers row-by-row search on real temporary Lance tables, repeated inputs, empty results, the SQL prefilter, snapshot pinning, IVF_FLAT and IVF_HNSW_SQ indexes and exact scans, HNSW re-ranking and filtering, recall of data appended after indexing, refusals by the candidate pool and conversion budgets, the concurrency bound, back-pressure from a slow consumer and cleanup of in-flight calls after a failure; the tests of the general streaming executor cover cancellation and thread release.

With real 4096-dimensional image vectors, a small-sample acceptance of 2944 stored rows and 128 independent queries has also been completed: streaming Recall@100 is 100% with IVF_HNSW_SQ, top_k=100, nprobes=16, ef=500, refine_factor=5. This measures the reproduction of exact vector search, not the accuracy of semantic image retrieval; the parameters have not been tuned at the scale of millions. See the [verification record with real vectors](../../benchmarks/VECTOR_SEARCH.md) for how to reproduce it and for the full results.
