# Unified Lance Arrow carrier closure

Implementation tree: `gpt_v3_artifact_recovery_20261115`; branch:
`engine/gpt-arrow-carrier-20261116`. Base `dcc85c8`; independent earlier fixes:
`a39ef19` (fresh-write CAS admission) and `da12bad` (append schema admission).
The main checkout was neither edited nor used for tests.

## What is implemented

Actual `LanceSource -> sealed unified graph -> native write_lance` can retain
Arrow batches through source reads, worker transfer, immutable terminal segments
and the original native sink. This includes source projection/filter/limit,
empty schema, nested/null data and exact pinned source versions. A middle
`LimitOp` preserves the original per-row identity and batch reference.

There is no driver source rescan, alternate graph compiler, second journal,
second allocator or replacement commit path. Native publication remains with the
original `CommitCoordinator`; the driver receives the executed output. Every
`Record` still represents **one business row**, including row credit, sequence,
metrics, source cursor, cuts and result counts.

The optimization is enabled only from the **actual sealed graph and physical
plan**: one source split, one source/terminal, a linear graph with only optional
Limit nodes, and one producer/consumer partition per edge. Terminals presently
admitted are `write_lance`, `materialize`, `iter_rows` and `count`. Other graphs
(sort/join/union/UDF/forks/multiple partitions) retain the original row encoding.
Exact public `size_bytes`/summary and bounded collector accounting retain their
original row route. This does **not** claim all operators are batch-aware.

## Carrier, identity and recovery

`execution/arrow_payload.py` implements
`application/vnd.demiflow.arrow-batch.v1`. The frozen `Record` DTO is unchanged:
`payload_ref`, `schema_ref`, `source_position`, `record_id` and `encoded_bytes`
are reused. Only this explicit MIME interprets a payload ref as a shared batch;
old MIME byte/count validation is unchanged.

One original store object holds a bounded JSON header and one uncompressed IPC
schema + record batch. Header fields bind version, source_id, split_id,
fragment_id, starting row, row count, exact schema digest and each row's bound.
The original source-position fields and record-id formula are unchanged. A
cursor optionally adds its versioned `arrow_batch` ref while preserving the
original next-row offset/sequence and pending-record protocol. Resume authenticates
that batch, replays the exact pending row and continues at the next logical row.
Old cursors remain accepted through their existing branch.

Consumers exhaust the original store read (including SHA validation) before
Arrow decoding. Metadata must have the closed bounded shape; IPC compression,
dictionaries/multiple record messages and mismatched row/schema/source bindings
are refused. The real worker terminal validates the payload before accepting its
bytes/schema, including co-located edges; the native sink validates again before
publication. Materialized handles, row consumers, the generic record resolver
and GC all understand the MIME. Each consumer keeps at most one cached batch.

The actual interrupted-run test stops after a real ACK with its source cursor
strictly inside a batch, advances the source head, then resumes the original
version. It produces all 64 original rows once and preserves the old commit
prefix. Actual published-object GC removes an unrelated object while retaining
shared batches reachable from the original materialized result and journal.

## Resource guarantee and deliberate fallback

The payload is stored once per batch, not once per row. The original store charges
its real IPC bytes. A row's `encoded_bytes` is a conservative bound for the old
typed-row encoding, calculated from Arrow types/buffers/slices without business
row materialization. The channel still reserves one row and that row-byte bound;
a small locator does not substitute for payload credit.

The carrier window is at most `min(max_batch_bytes/8, original stage grant/32)`.
Before IPC allocation, admission includes the declared native prefetch, retained
parent buffers, a previous cached batch and eight times the measured IPC size
estimate. The estimate uses Arrow's record-message size, serialized schema,
header and framing allowance; its actual serialized size is checked as well.
Source batch/fragment readahead is bounded to one. Ref metadata is preflighted
against the original transfer and pending/terminal frame before storing bytes.

Unsupported types, too-wide conservative estimates, retained parent buffers or
metadata frames that cannot fit use the original row path. They do not obtain a
new capability rejection merely because a batch representation is unsuitable.
The original row, conversion, spill and output guards remain. The narrow-window
test retains its genuine many-row conversion rejection, verifies no output was
published, and succeeds for the same nine 12 KB rows and byte ceilings with
`native_resources={'batch_rows': 1}`. Blob/Arrow native allocations remain subject
to finite workspace allowances and existing sampled isolation protections;
these are **not a hard process-RSS guarantee for third-party allocations**.

## Native Blob conversion and append admission

Only the real `lance.blob.BlobType`, with its known two/four-field storage layout,
is admitted as an additional known extension. Arbitrary extension hooks remain
fail-closed. Its storage buffers, five constructor pointer lists/null mask and
wrappers are admitted before the original `lance.blob_array` constructor. Exact
bytes, null/empty blobs and schema/field metadata survive real Dataset writes.
A deliberately insufficient conversion budget rejects before that constructor.

URI values retain the native external-reference semantics. The platform does
not enable `external_blob_mode='ingest'` or relax external-base admission. A real
registered-base overwrite followed by same-size replacement of the external
file reads the replacement via `take_blobs`, proving no driver full-file copy.
The existing low-level CAS **append BlobArray** path still fails in Lance's
fragment writer (`expected prepared array layout`); its submission protocol was
not expanded by this change. Root explicitly deferred that separate capability.

`da12bad` checks append schema before the staging writer, including the explicit
schema declaration. Valid existing explicit schema casts still run before the
block compatibility check. Mismatches are deterministic `InvalidLanceRequest`,
leave the old target unchanged and have no prepared/native receipt. An original
pending journal intent can exist because the coordinator creates it before
calling `prepare`; it is not a publication or ACK.

## Evidence and limits

Machine-readable evidence: `ARROW-CARRIER-CLOSURE.json`. It contains receipt
SHA-256s, source-before/after evidence, per-node latest outcomes, production file
hashes and the seven unchanged frozen-file hashes against `dcc85c8`.

- Final component batch: **313 passed** (source, codec, channel, conversion and GC).
- Local broad batch: **34 passed / 1 failed**. The failure was the new narrow-window
  fixture requiring a native constructor batch that did not fit. Its final test
  retains this rejection and adds an actually admitted identical-data path:
  **1 passed** in `window-r11`.
- After adding small-frame preflight, source/copy/recovery batch:
  **59 passed / 4 failed**. The four failures exposed use of a non-contractual
  `store.store_id` attribute in the new probe. It now reads the store domain from
  the existing schema ArtifactRef. All four and the affected actual
  copy/recovery/GC/Limit boundaries: **9 passed** in `frame-r13`.
- Final real Ray Core (fresh owned 2 CPU / 0 GPU cluster): **1 passed**, including
  process-visible no-Python-row probes, original schema, exact values and ACK.
- Across the recorded source-bound batches, **350 unique selectors have a latest
  three-phase pass**; no skips, xfails, retries or deselections were counted.
  This is explicitly **not** one full-suite run at one immutable commit. The
  final production hashes match the last Local/Ray receipts; wider unchanged
  component evidence remains attributed to its own preceding snapshot.

The original two Arrow-copy tests retain their business assertions. Their removed
executor-private spy was replaced with actual worker/driver carrier and fallback
probes; a positive multirow case proves 16 rows use two batch artifacts, bounded
batch reads and no Python-row decoding. The latest tests also cover Local process
and thread hosts, source-version pinning, source projection, empty/nested/null
schema, malformed/compressed payload rejection, a real store-byte corruption,
mixed-source/operator fallback, cancellation/slow consumption, tiny legal row
frames, wide rows and original grants returning to confirmed cleanup.

Not run here: the whole repository, multi-host Arrow-specific execution, GPU
carrier performance, all-operator batch execution, or the other lane's new
registered-write integration. Root owns remaining historical regression and
integration. No throughput or end-to-end engine completion percentage is inferred
from these correctness tests.
