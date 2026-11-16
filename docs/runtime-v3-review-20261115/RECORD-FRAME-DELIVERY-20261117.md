# Record carrier budget repair: fixed candidate and remaining integration gate

Production candidate: `b5530d923b1c67062fb0f2d03f2cb29b0b50dc77`, based on `dbeecf7816327ce270726d361b65e3cae694e166`. Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`; branch: `engine/gpt-payload-frame-budget-20261117`.

**This is not an all-green release claim.** Final Local execution is 150 passed, 1 optional Ray skip, and 1 retained positive failure: `test_real_near_row_limit_scalar_uses_bounded_old_frame`. Final actual Ray execution is 2 passed. Root owns the compiler profile repair and the original relational-region integration test. Existing test expectations and budgets were not changed.

## What changed

Typed business payload remains bounded by original R. A record document independently bounds metadata by original C and the total by R+C; outer JSONL escaping has its own derived ceiling. All three also fit the original resident grant. C is a metadata ceiling, never an extra grant. Public DTOs, media types, record/delivery identities, SHA validation, source cursors, every-row ACK, journals, commit protocol and persisted bytes remain unchanged.

The new private `dataflow_record_frames.py` measures payload and metadata before building the existing tagged JSON document. It checks raw JSON structure before parsing, including string/container storage, canonical validation, decoded binary values and temporary base64 copies. It accounts for actual surviving escape characters, so a tiny bytes leaf does not charge a neighboring wide string as another binary copy. NumPy zero-dimensional arrays use the same array/scalar distinction as the original codec. Bounded lexical metadata is independent of row length; measured allocation accounting is not a process RSS guarantee.

Writers stream small escaped pieces into the original terminal/finite scratch writer. The old JSONL bytes and source-pending bytes are preserved exactly. Reads admit both original bytes and reconstruction workspace before allocation, preserve each store's SHA/lease semantics, avoid collecting unbounded lists of tiny artifact fragments, and close active readers on errors and early close. Historical low-level codec callers retain their original default argument meanings; migrated callers explicitly distinguish document and payload ceilings.

Original peer ACKs release the worker's existing credit. Codec admission includes currently retained peer buffers and `kernel.retained_resident_bytes`; only the peer part can be waited away. Impossible fixed kernel/codec footprints fail immediately. A refused encoding does not advance a routing ordinal or change a later delivery identity. No allocator, queue, commit authority or persistent resource ledger was added.

## Exact production scope

| File under `demiflow/execution/` | Narrow connection |
| --- | --- |
| `dataflow_record_frames.py` (new) | Shape/metadata measurement, bounded carrier admission, original document/JSONL encode/decode/read helpers |
| `exchange/dataflow_bridge.py` | Optional payload/metadata decode limits; normalized canonical validation separates temporary peaks |
| `exchange/dataflow_channels.py` | Encode/resolve/route use explicit bounds; successful admission precedes ordinal consumption |
| `exchange/dataflow_transport.py` | Document ceiling, pre-receive admission and bounded read fragments; keys/SHA/token/ACK unchanged |
| `dataflow.py` | `_OutputSubscription.read_admitted` only: true payload versus frame bytes and resident replay/receive admission |
| `dataflow_sinks.py` | `_encode_record`, `prepare_sink_partition`, `_decode_record`, `NativeTerminalSink._blocks`; preserve original carrier bytes and reader cleanup |
| `dataflow_replay.py` | Prefix size probe, bounded outer frame reconstruction and original replay reader |
| `dataflow_handles.py` | Stored materialized v3 carrier reads; stream/Arrow cache cleanup |
| `dataflow_segments.py` | Optional bounded read chunk forwarding |
| `dataflow_batch_membership.py` | Membership hash uses the same unchanged canonical document bytes |
| `dataflow_finite_input.py` | Bounded full-scan/projected re-read; broadcast residual bytes; preserve original decoder instrumentation seam |
| `kernels/join_spool.py` | Optional payload bound propagated to original projected/fallback document decoder |
| `source_readers.py` | `_BaseReader` pending snapshot/read only; original pending MIME/cursor/bytes |
| `worker/dataflow_graph_runner.py` | Route/codec credit, terminal and finite frame write/read, retained kernel and peer accounting; no region/open/run algorithm change |
| `worker/dataflow_terminal.py` | Scalar terminal reads use original typed-payload bound and current stream residue |
| `worker/terminal_blocks.py` | `append_parts` streams exact measured bytes; read view exposes current buffered bytes |

Only one new test file was added: `tests/test_runtime_v3_record_frame_budget.py`. No historical test file was modified.

## Fixed-input verification

Final source and all test inputs are bound to the production commit: **2,954 input digests match its Git archive; zero missing/mismatched inputs**. Both final receipts have the same inputs, unchanged before/after, and full setup/call/teardown reports. Evidence, exact command/environment fields, earlier failed attempts and source hashes are in `RECORD-FRAME-BUDGET-20261117.verification.json`.

- `/tmp/demiflow-record-frame-final-r14/receipt.json`: **150 passed / 1 failed / 1 skipped**, 79.14 s. Covers original S source adapters, finite borrowing, join projection, codec encoder and terminal replay admission, plus new payload/frame cases. The skip is the new actual-Ray case in the non-Ray interpreter; it is executed in r15.
- `/tmp/demiflow-record-frame-ray-r15/receipt.json`: **2 passed**, 24.52 s. `/tmp/demiflow-ray-api-optional-env/bin/python`; owned local Ray Core clusters with 2 CPUs/0 GPUs, no preexisting `ray.init`, explicit shutdown. The new typed-row case confirms `provider='ray_core'`, original ACK and a Ray Data import blocker. The original Arrow multiline-copy case proves schema/batch/ACK behavior still holds.
- Original exact payload bytes, tuple/bytes/negative-zero fidelity, near-R quote/backslash carriers, R+1 payload, oversized metadata, forged `encoded_bytes`, insufficient codec workspace, zero-dimensional NumPy and binary temporary copies all have direct assertions. Nonfinal ACK interruption, actual resume and stored materialization reread are green in r14.
- Earlier r7's 97 passed and r8's 124 passed are historical intermediate evidence, not additive final-version pass counts. r12 was a selector typo and ran no tests. r13's projected-reader failures were a removed private instrumentation seam, restored in the final candidate without modifying those tests.

Every run pins this worktree in-process and through PYTHONPATH, uses private RESOURCE_ROOT/RESOURCE_HOME/FORGE_DOMAIN/LOCATION_CACHE and TMPDIR, and does not test or modify the main checkout. The frozen seven files match `49b666f` byte-for-byte. No push, main merge, shared environment change or GPU use occurred.

## Remaining gate, deliberately not hidden

The retained positive scalar test uses original total memory 1 MiB, R=4096 and batch=8192. The original default stage profile reserves 12288 bytes. A real ItemsReader row with 4046 typed bytes produces a 4094-byte document. Prewarmed direct codec measurements showed encode incremental peak 9839 plus its 2143-byte existing row, and decode incremental peak 12302 plus the existing 4094-byte input. Decode therefore cannot fit that 12 KiB allocation. The helper's encode admission currently rejects at 12641 rather than silently using unreserved memory.

Root has prepared compiler-only `a3e5b83` to reserve codec workspace inside the original global budget while keeping the original 40 KiB small-data capability and explicit worker limits. This candidate does not include that parallel change or rewrite historical sealed profiles. Root must combine it and independently run the unchanged near-R scalar positive, original 4 KiB/16 KiB relational tuple regression and original low-budget cases. No full-completion or formal performance-gate claim is made here.
