# Document Acquisition Boundaries

`Dataset.fetch(*, store=LocalObjectStore(...), request='request', output='result')`
每行接收一个 `{uri, expected_size?, expected_sha256?, source_version?}`，
返回原行与原始对象获取回执。它流式获取完整原始表示并完成原生 acquisition commit，
不解析、不访问文档索引。完整预算、HTTP/COS、恢复与结算见
[原始对象获取](dataset-fetch.md)。旧 Download 与 Dataset.fetch_documents 已移除。

The `ParseDocument` callable is removed. Callers parse a retained raw object in
their own map function with `demiflow.collect.documents.parse_document(body,
url=..., final_url=..., content_type=...)`; it never fetches the source URL.
Normalized document bytes are decoded in a flow with `Dataset.decode_document`.
Object references remain immutable URIs and SHA256 hashes.

`Dataset.lookup(source, key='url', output='existing')` is a cardinality-preserving
KV read. A source implements `get(key) -> {hit: bool, value: JSON}` and owns its
read and consistency bounds. Storage failures propagate. `SQLiteKV` opens reads
read-only, returns the latest committed value in one read transaction, and can
explicitly treat a nonexistent database as empty. It never creates on read.
`lookup_lance` remains the distinct fixed-version, multi-match table lookup.

For its default `max_matches=1`, an unindexed lookup with a plain column
projection first filters only the key column and returns at most two Lance row
IDs. Native key scanning uses up to 8192 rows per batch, a 64 KiB approximate
byte target, a 1 MiB I/O buffer, and one batch/fragment of readahead. It then
fetches matching payloads one row at a time from the same pinned dataset. Row
IDs remain correct across deletion and compaction; they are not row offsets.
This avoids applying the payload's one-row batch size to the entire key scan.
Sources declaring an index on the key, `max_matches>1`, and non-plain projections
retain the native one-row scanner. The choice is private to the operator.

The result limits are unchanged: each returned Arrow row and the accumulated
Python match payload must fit `max_bytes`, and an extra match raises rather
than truncates. Nested cached values are copied for each consumer. The existing
LRU holds at most 128 keys / 8 MiB of counted Python payload/key sizes; this is
not a process RSS limit. The key scan retains at most two uint64 result IDs,
not a full key index or the input stream. Scanner iterators close on success or
failure; cancellation still drains the blocking lookup before actor cleanup.
No source version, scalar index, journal or other persistent data is written.

The native byte target is approximate: a large value or decoded storage page
can exceed it. `max_bytes` covers returned Arrow/Python payloads, not Lance's
internal decode buffers, metadata, native allocations, or total RSS. The I/O
buffer setting is also not a total memory cap. Deployment resource isolation
remains necessary for those allocations, as for the previous native scanner.

This improves the cost of each unindexed point query, not its asymptotic
complexity: Q distinct uncached keys can still scan M source keys Q times.
Partially indexed sources use the native planner, including its unindexed
fragments; no indexed-fragments-only search is requested. Cross-row batching,
building indexes, and higher-match-count query optimization are separate work.

`benchmarks/lookup_lance.py` compares two captured source trees using only
synthetic fixed-version data, identical query lists and two CPU cores. It
records fresh-actor and repeated-pass latency, result checksums, RSS/temp-disk
samples, native scanner work, payload takes and process I/O. Scanner statistics
exclude payload takes; process I/O also includes monitoring. Cold means a new
actor/dataset handle, without flushing the OS page cache. The benchmark uses a
new output directory and does not install or alter the Python environment:

```bash
python -B benchmarks/lookup_lance.py \
  --baseline-root /path/to/captured-demiflow \
  --output /path/to/new-lookup-results --repeats 3
```

`SQLiteKV(path=..., namespace=...)` declares a local SQLite index. Registration
uses `Dataset.write_datasink(store.datasink(...))`; `sink.receipt` is populated
only after a successful action. Batches are independent atomic transactions,
not an action-wide transaction or an atomic commit with a Lance catalog.
Writes have count, JSON-value, batch-byte and namespace logical-byte bounds.
SQLite pages, keys, indexes, rollback journals and temporary initialization
files add physical overhead; filesystem quotas remain the physical disk limit.
Network filesystem locking and distributed consistency are not supported.

`map_cached` caches a callable's JSON result. With no bindings it retains the
whole-row map contract. With `inputs` and `output`/`outputs`, only bound argument
values enter the computation and only its return is cached, then attached to
the current row. Bound `None` and lists are values; unbound async `None` drops
and lists expand as usual. `cache_when(result)` selects both admission and
reuse. `when(row)` skips the call and preserves the row. Neither is a retry or
freshness policy. Callers must put every hidden output-affecting setting in
`version` or the callable's JSON `cache_identity`.

The local result cache uses the explicitly declared `cache_dir`, namespace,
version, actual inputs and optional callable identity. SQLite commits completed
values; 4096 persistent lock shards coordinate in-flight computations across
local processes, with a finite wait timeout. Collisions can serialize unrelated
keys. Cancellation releases locks and drains blocking storage before cleanup.
Actor hooks and declared resources are forwarded. A process crash may require
retrying an external side effect: this does not promise exactly-once HTTP.
Input/result bytes, entry count and logical cache bytes have explicit limits;
exhaustion raises, with no silent eviction. One bounded last error per lock
shard is retained. Earlier file-per-key caches are not silently interpreted as
the new SQLite layout: use a new cache directory/version for a controlled cutover.

The combined Dataset document acquisition method is retired. New flows express
raw acquisition, parsing and registration separately. DocumentLibrary retains explicit import, lookup and historical reads.
WebSession.fetch and WebClient.fetch are removed; no download/URL-cache/parse
combination participates in the new queue task completion protocol. Historical
object references remain readable unchanged. Existing registration/read APIs
retain their own capability and resource boundaries; their presence does not
grant an unsupported node native task or Forge admission.
Source-specific wikitext parsing has moved out of the platform. The old generic
registration API accepts already-normalized references or extracted plain text;
custom parser versions must be explicitly declared by its caller. Generic HTML
parser version 4 no longer contains Wikipedia CSS selectors.

Shared raw receipts keep their original immutable URI after a mount-root change.
Use the explicit current LocalObjectStore.resolve(raw_ref, domain=storage_domain)
binding before ordinary parsing. For descriptor-guarded local reads use the
existing bounded read_local_object with that current root; ordinary ObjectRef
read/verify retain legacy URI resolution behavior. The binding creates no
object, receipt, queue transition or global alias. See dataset-fetch.md.
