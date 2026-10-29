# Document Acquisition Boundaries

`Download(session=..., store=LocalObjectStore(...))` accepts exactly `{url}`
and returns a raw snapshot reference, requested/final URLs, content type,
retrieval time, status and transport attempts. It uses the declared WebSession
transport limits and journal. It does not parse or access a document index.

`ParseDocument(store=..., max_raw_bytes=..., max_document_bytes=...,
timeout_s=..., pdf_parser=...)` accepts exactly `raw_ref`, `url`, `final_url`,
`content_type`, `retrieved_at`. It reads and verifies that object, parses generic
HTML/plain text or explicitly enabled PDF, and writes a normalized document.
It never fetches the source URL. Parsing runs in a drained, timed child process;
this wall-time isolation is not an RSS hard limit. Source-specific parsing is
owned by consumers, using `publish_document` for the common document format.
Raw and normalized stores may be different. Only local object writes are
supported here. Object references remain immutable URIs and SHA256 hashes.
An optional caller-owned `request_gate` with async `enter()` admission bounds
parsing across multiple actors. It is independent of stores and index access.
Invalid/oversized normalized content returns a parse-error receipt retaining
the raw reference; storage errors propagate. No storage or process is created
by callable construction.

`Dataset.lookup(source, key='url', output='existing')` is a cardinality-preserving
KV read. A source implements `get(key) -> {hit: bool, value: JSON}` and owns its
read and consistency bounds. Storage failures propagate. `SQLiteKV` opens reads
read-only, returns the latest committed value in one read transaction, and can
explicitly treat a nonexistent database as empty. It never creates on read.
`lookup_lance` remains the distinct fixed-version, multi-match table lookup.

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

The pre-existing combined `fetch_documents`/DocumentLibrary APIs remain for
consumers that have not migrated. New acquisition flows must express reuse,
download, parsing and registration separately. They are not called by the new
primitive implementations. Historical object references are readable unchanged.
Source-specific wikitext parsing has moved out of the platform. The old generic
registration API accepts already-normalized references or extracted plain text;
custom parser versions must be explicitly declared by its caller. Generic HTML
parser version 4 no longer contains Wikipedia CSS selectors.
