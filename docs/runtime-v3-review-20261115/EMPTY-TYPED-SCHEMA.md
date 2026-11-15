# Empty typed Dataset: original seal, original result manifest

Arrow/Pandas/Lance inputs now keep their declared schema even when no rows arrive. Real public `schema`, `take_batch(pyarrow)`, `materialize`, and same-RunRef restore preserve field order, nested/large types, nullability, field metadata and schema metadata. There are no synthetic records and no second result ledger.

## Metadata contract

- `dataflow_schema.KEY = declared_arrow_schema`: bounded Arrow IPC schema encoded in strict JSON. Literal native handle metadata is sealed in the original source node configuration. Lance records the projected schema of the **already pinned version** in the original SourceSnapshot immutable identity, never a later table head.
- Pure interpretation follows the sealed graph through known schema-preserving operators and projections. Unknown UDF outputs and heterogeneous declarations remain unknown; this does not reject previously valid heterogeneous row collections.
- A terminal worker writes optional total-level `schema_ref` (the existing closed ArtifactRef JSON) into its original sink manifest. Source schema is available even when EOI arrives without a record. The reference uses the original artifact store and budget authority.
- The existing combine authority validates equal declared schemas across partitions and rejects conflicts; it carries optional `schema_ref` into the original publication. Missing fields in legacy manifests remain missing.
- `StoredMaterializedHandle` retains and releases the schema reference with its own existing owner pins, exposes cached `declared_schema`, and preserves the optional reference in its existing passive pickle state. Historical state lacking the field remains readable.
- Empty `take_batch` reads the schema from the original confirmed result before closing the action handle. It does not reopen a source or execute a callback to guess types.

The record-schema byte/structural guard was extracted unchanged into `record_schema.encode_schema`; metadata is bounded before serialization. Decoder, derived metadata, result lookup and merge retain explicit bounds.

## Historical protection

Two additional real counterexamples showed that old literal action identity binds only empty blocks and row count: changing an empty Arrow/Pandas schema could therefore reuse an old result. New seals now validate their explicit schema facts in the existing `DataflowExecution.prepare` replay branch. The comparison uses pure declaration capture, never user execution. Old seals without the new field keep the previous compatibility path. Existing action, prompt, enum and code-pin identity algorithms are unchanged; no historical seal or receipt is rewritten.

## Verification

- Original public empty-type cases: **12 failed** before the repair, then **12 passed**.
- Empty literal schema drift on same RunRef: **2 failed** (silently reused original result) before the replay guard; now explicitly rejected while original commits remain unchanged.
- New tests plus historical Arrow sink and scalar tests: **49 passed / 131.46 s**, `/tmp/demiflow-regression-empty-r3.xml`.
- After narrowing heterogeneous metadata to unknown rather than adding a new rejection: **6 passed / 15.80 s**, `/tmp/demiflow-regression-empty-final.xml`. Includes both heterogeneous input forms, drift negatives, metadata budget/corruption, conflicting partition schemas and legacy missing-field compatibility.

Exact commands, source digests and per-node red evidence are in the paired JSON. No main checkout, production process, shared environment, protected source, or existing test assertion was modified by this batch.

The adjacent repartition work may add a contribution `partitions` list and segmented data refs; it must retain total-level `schema_ref` and preserve these original schema reads/pins. No partition or data_ref shape was changed here.
