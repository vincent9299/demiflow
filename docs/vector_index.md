# Managed Lance vector index construction

`data.ensure_lance_vector_index(uri, expected_version=version, vector_column='embedding', options=...)`
is a table-level storage operation. It builds Lance's native `IVF_HNSW_SQ` index
with cosine distance on a local float32 fixed-size-list column. The current adapter
uses CPU only; it does not load an embedding model or modify `Dataset.search_vectors`.

The returned dictionary contains the **indexed** `uri/version`, input `source_version`,
index name/type/column/metric, actual parameters, UUIDs, row/fragment coverage,
resource budgets, and `action` (`build`, `rebuild`, `reuse`, or `empty`). Publish its
fixed reference only after the call succeeds. An empty table returns `status=empty`,
never `ready`.

```python
from demiflow import data

index = data.ensure_lance_vector_index(
    '/absolute/path/images.lance', expected_version=written_version,
    vector_column='embedding', options={
        'num_partitions': 256, 'm': 16, 'ef_construction': 200,
        'threads': 8, 'memory_bytes': 8 * 2**30,
        'max_rss_bytes': 20 * 2**30, 'timeout_s': 7200,
    })
indexed_reference = {'uri': index['uri'], 'version': index['version']}
```

`demiflow.lance.vector_index_config` validates configuration without opening data.

| Option | Default |
| --- | --- |
| name / index_type / metric | embedding_ivf_hnsw_sq / IVF_HNSW_SQ / cosine |
| num_partitions / m / ef_construction | 256 / 16 / 200 |
| sample_rate / max_iterations | 256 / 50 |
| threads | 8, limited by container CPU allowance |
| memory_bytes / max_rss_bytes | 8 / 20 GiB |
| max_scratch_bytes / max_index_bytes | 32 / 16 GiB |
| timeout_s / admission_timeout_s | 3600 / 3600 seconds |
| cgroup_headroom_bytes | 2 GiB |

`num_partitions` cannot exceed nonempty table row count; small pilots must choose
an explicit smaller value. Native HNSW requires `m >= 4`, with
`ef_construction >= m`. The index's SQ representation is approximate; original
float32 vectors remain unchanged and available for retrieval refinement.

## Publication, reuse, and coverage

The driver pins `expected_version`. An isolated native worker validates finite,
nonzero vectors in bounded batches and builds **uncommitted** index files. Only
the driver commits the manifest, with a strict version guard and no commit retries.
A second bounded inspection verifies actual index parameters and full row/fragment
coverage before returning `status=ready`. Races, build failures, guard failures,
or failed verification raise; the caller must not mark its release indexed.

Matching complete indices are reused without creating a table version. Reuse
requires both native statistics and platform evidence matching the index UUID,
schema and build parameters. An unrecorded external index is rebuilt rather than
assuming unknown training parameters. New vectors or changed parameters currently
cause a **complete replacement build**. This maintains full coverage; incremental
segment optimization is not implemented here. Historical snapshots retain their
index files. Coverage is for the returned fixed version, not future writes.

Per-table control files live under the existing `_demiflow/<table>/` control
directory, in `vector_indices/` and `index_jobs/`. Parameter evidence identifies
staged segments; readiness comes from validating the committed manifest. Failed
uncommitted files owned by this call are cleaned up. Published or uncertain files
are retained, never deleted on the assumption that commit failed. A hard driver
crash may leave uncommitted files for later explicit maintenance.

## Finite resources

Construction reuses Demiflow's native subprocess admission, CPU affinity, process
ownership, parent-death signal, sampled RSS/cgroup/scratch guards and deadlines.
Concurrent index jobs with the same CPU/RSS capacity share one admission slot in
the same cgroup. Different capacity profiles and other workloads are additionally
subject to cgroup headroom checks; this is not a global CPU allocator for all jobs.

Before training, a conservative multiple of the sampled vector matrix is checked
against memory_bytes, and disk headroom is checked against declared scratch/index
budgets. Native Lance receives a bounded memory pool, one shuffle partition worker
and eight shuffle batches; full input vectors are never collected in the driver.
One validation batch holds at most 256 vectors. Manifest/fragment metadata and
skewed HNSW partition allocations remain native allocations in the isolated worker;
the training estimate is not a proof of peak RSS. RSS/time are sampled every 0.1 s,
scratch and owned index size about every second; these are guards, not kernel hard
limits. Index output size is checked again after construction. Existing historical
indices are retained and do not count toward the new call's output allowance.

Vector centroids are excluded from statistics to avoid materializing large nested
lists. Index evidence is compact and bounded; the builder rejects more than 64
segments for the selected name or 100,000 table fragments instead of accumulating
unbounded control payloads.

Tests cover actual native build/reuse/replacement, appended rows, changed graph
parameters, version races before/during/after commit (including replacement of head while an older indexed snapshot remains valid), RSS/time/disk failures, empty
input and invalid vectors. Backend validated here: installed pylance 12.0.0 with
`create_index_uncommitted` and guarded `LanceOperation.CreateIndex` support.
