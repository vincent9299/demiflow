# Native output types and actual-byte finite buffers

Two core defects were reproduced before changes:

- `save_lance(...).materialize()` lost `int32`, `large_string`, nested large
  strings, field nullability, field metadata, table metadata and field order.
- With equal maximum row/batch ceilings, 300 small records created 300 immutable
  input-spool artifacts. The row maximum was being used as an average row size.

Native Save now carries the input schema through its output records. An optional
receipt adds a nullable `{uri: string, version: int64}` struct; a row rejected by
`when` does not gain a dictionary key, and an Arrow result represents the missing
value as null. Empty results derive exactly the same schema from the original
sealed graph. Saved table projection/schema remain independent of passthrough
fields. On actual empty EOI the lazy kernel creates only the native driver RPC
proxy, then calls its original finish protocol; ordinary empty user factories
are still not constructed.

Finite input ports now group exact state-codec wire bytes with an 8192-record
ceiling. They admit the next record before retaining it, keep the per-row bound,
and use a byte ceiling that accounts for the retained rows and encoded segment
coexisting inside the original working-set grant. A single bounded record may
occupy its own segment. Checkpoints flush pending data to original immutable
artifacts and carry references; old snapshots containing pending records remain
readable and have their actual byte budget recomputed. No resource maximum is
increased, no new store is introduced, and prepend's shared-main persistence
before ACK remains unchanged.

Evidence:

- `/tmp/demiflow-save-spool-red.xml`: all three new counterexamples fail.
- `/tmp/demiflow-save-spool-green.xml`: 35 passed, three failures. Two failures
  exposed the integrated lazy-factory empty-Save bug, subsequently repaired;
  the remaining shared-source prepend failure is described below.
- `/tmp/demiflow-save-schema-r2.xml`: all six typed/empty/native cases pass.
- The finite-port tests use the actual RunArtifactStore and original journal,
  reduce 300 records to fewer than ten artifacts, restore the exact records,
  read old pending snapshots, and reject an oversized individual row.

The original `test_shared_source_prefix_runs_original_source_once` still times
out at the default four-worker budget after root integration. Replacing only
`_SpoolPort` and `_segment_rows` with exact `c28f5c8` originals reproduced the
same failure (`/tmp/demiflow-prepend-root-baseline.xml`, 16.60 s). The byte spool
was then restored. This remaining scheduling failure is reported to root and
is not relabeled as a passing test or bypassed by increasing its test budget.
