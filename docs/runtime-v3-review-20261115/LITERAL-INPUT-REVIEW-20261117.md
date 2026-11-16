# Large literal input: original artifact/cursor closure

Candidate source/test HEAD: `77cacbf2eff9e3b9fff608dc8c5c0ae1fc89da6e`.
Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_literal_input_20261117`.
Branch: `engine/gpt-literal-input-20261117`; base `dcd27b27fb605b3f2ba8a34ea05a5f1a3c3e89e0`.

**Source carrier/reader acceptance passed: 26 selectors, 78 original phases, fixed input, no surviving children. Original 6/10 MiB join acceptance is still pending the separately owned planner fix.** No claim of whole-engine, Ray, multi-host or performance completion.

## Actual failure and changed behavior

The public `data.from_items` constructor uses `MaterializedSource(InputMaterializedHandle)`, not `ItemsSource`. Both can carry legitimate input data larger than the actor/checkpoint `max_state_bytes`. Previously `_stage_one_program` pickled the entire input into the program artifact and incorrectly rejected it as oversized program state. R1 preserved the initial missing-public-wrapper failure; the final implementation handles the actual public wrapper as well as internal ItemsSource.

Large literal input now stays data: the source program contains a small private versioned `LiteralSourceReference`; the original `SourceSnapshot.manifest_ref` owns an input manifest and original typed record segments. The existing RunArtifactStore, artifact quota/allocator, SegmentForest, journal seal, GC traversal, SourceCursor index, driver commit and ACK remain the authorities. No new store, journal, cursor ledger or allocator was introduced.

Small input retains the original zero-source-data-artifact path. The threshold is computed by the same function for program and snapshot: serialized source size greater than `min(65536, max(1, max_state_bytes // 4))`. Recovery loads the sealed program/snapshot, so it does not recalculate or switch an old attempt's carrier. Existing large inline program/snapshot carriers remain readable with their original bounds.

Items and public materialized items/Arrow/NumPy/pandas are supported. Durable stored materialized handles are excluded from this conversion. Public source identity uses the exact original protocol-4 byte digest (`items_digest` or `handle_digest`); code-pin, logical identity and original outer source Record identity formulas are unchanged. The private staging row identity only authenticates position inside the source artifact.

## Scope and contracts

- `execution/dataflow_seal.py`: source-only small descriptor substitution before ordinary program serialization; original program budget/code-pin verification remains.
- `execution/dataflow_sources.py`: streaming original digest; large literal snapshot manifests; original source facts, split identities and historical carrier loading remain.
- `execution/literal_source.py`: private descriptor, bounded row staging, original segment index, lazy authenticated range reader; no public Dataset API.
- `execution/dataflow_segments.py`: optional range/offset traversal over original index; existing default traversal and full leaf authentication remain.
- `execution/source_readers.py`: new carrier integration; original SourceCursor/pending transfer; close/error cleanup; shared frame accounting, including pending records owned by other splits.
- `execution/worker/dataflow_graph_runner.py`: reader frame callback wiring and source retained memory added to original `_frame_call` peer/kernel/finite-reader accounting. No scheduler/publication/commit changes.
- `tests/test_runtime_v3_literal_source.py`: real store, original worker frame admission, public Dataset ACK/GC/recovery, corruption and compatibility coverage. Historical original tests were not weakened or changed.

## Resident ownership and release boundaries

| Phase | Simultaneously owned data and existing admission |
| --- | --- |
| Identity | Original caller input and protocol-4 Pickler alias memo; streaming writer hashes writes without constructing the whole final pickle bytes value. Original reducers may allocate intermediates; see limitations. |
| Driver staging | One input row, frozen Record and typed codec workspace, original forest roots and bounded prospective index JSON workspace. Row freeze is admitted before construction; original store admits physical artifact bytes. A pending next frame remains inside the already admitted codec/index peak across segment flush. |
| Manifest/root open | Manifest decode uses original shared frame helper. Manifest and root conversion are admitted together; manifest is then released and root metadata is charged before root index authentication. Failure closes the reader and resets retained metadata. |
| Segment traversal/read | Bounded original index stack plus source manifest root metadata; full leaf hash authentication before the first business row; fixed read chunks; `_ReplayLines` checks complete buffer-growth peaks. Range seek skips unrelated indexed subtrees rather than rebuilding a source list. |
| Decode and row publication | Source buffers/index, peer bytes, kernel state and finite reader state all share the original worker grant. Typed row/raw/frame temporaries use existing frame helpers. Source Record freeze checks the decoded/current copies before allocation. |
| Across split yields | A finished split can still retain its original unacknowledged Record. Its payload and metadata remain charged until original ACK/snapshot/close. A frame codec's own identical Record input only deduplicates its inline payload; metadata ownership remains counted. |
| Pending cursor handoff | Original source-pending frame is encoded/read through the same frame callback; recovery returns the original record ID and sequence, then resumes the original source index. |
| Close/error | Stream, iterator, metadata and pending row references are released; original artifacts remain subject to original journal/GC ownership. No successful publication is invented after quota/corruption failure. |

## First-hand evidence

Original receipts and complete source digests are referenced in `LITERAL-INPUT-VERIFICATION-20261117.json`; all recorded input bytes were compared to each receipt's own Git commit, not reinterpreted against the latest commit. Every completed batch reports no live children and unchanged source. Four domains plus TMPDIR/PYTHONPATH are private under `/tmp/demiflow-*` and actual import root is recorded.

| Batch | HEAD | Pass / fail | Meaning |
| --- | --- | ---: | --- |
| R1 | df3565e | 7 / 2 | Internal ItemsSource works; original public MaterializedSource still rejected at program sealing. |
| R2 | 1960fa9 | 11 / 2 | Public wrapper works; newly added index workspace reservation was too conservative (`2 * max_control_bytes`). |
| R3 | 5edb4a3 | 11 / 2 | Actual bounded index metadata reserve fixes that; original wide joins now pass source/program staging and fail unchanged planner residency. |
| R4 | ae46897 | 10 / 1 | Public nonfinal ACK, GC and full recovery passed. Final test wrongly reopened a completed run; original completed-run guard correctly refused. |
| R5 | b52630a | 14 / 0 | Correct final-ACK-before-run-completion fault window; pending handoff and typed source cases pass. |
| R6 | c21afdc | 2 / 1 | Small public and legacy carrier pass; corrupted input correctly rejected, but new test matched the wrong error wording. |
| R7 red | fa5e0f0 | 0 / 1 | Real root authentication budget failure leaves 1103 bytes of metadata retained: `_open` was outside close-on-error. |
| R8 | 77cacbf | 26 / 0 | Final source candidate, including all above closures and 8 original finite-reader shared-resource tests. |

R8 command:

```sh
/yzp/zhaozy/yangzepeng/0905/env/bin/python -P -B /tmp/demiflow-literal-input-runner-20261117/run.py --execute --group candidate --candidate-head 77cacbf2eff9e3b9fff608dc8c5c0ae1fc89da6e --output /tmp/demiflow-literal-input-r8
```

The actual public Dataset test uses 270 typed rows, an original nonfinal ACK then interruption, protects the complete source tree while GC removes an unrooted artifact, resumes the same RunRef with exact rows and signed zero, then interrupts after final ACK before run completion. The next resume returns the original result without `_prepare_launch`. It does not reopen a successfully completed run or create a second journal.

The original two payload-limit tests remain unchanged and unresolved in this leaf:

- `test_explicit_native_row_allowance_is_not_replaced_by_a_default_estimate`
- `test_native_default_row_floor_remains_compatible_when_pool_supports_it`

R3 reaches `_select_budgeted_residency` and `solve_residency`; original join stage resource policy exceeds the 64/128 MiB budgets. Planner owner has this defect. Do not widen the test budgets or reduce the data; rerun these exact selectors after integration.

## Limits and handoff

Preserving protocol-4 history does not promise constant RSS during driver identity computation: the alias memo remains, and original NumPy/Arrow reducers may allocate native serialization intermediates. The proof here is byte-identical identity and bounded runtime artifact reading, not a redesigned identity format or arbitrary reducer memory enforcement.

No new Ray, multi-host, GPU or I08 claim is made. Source-only Local evidence does not establish whole-product/engine completion. The remaining public wide-join planner result must be reported separately until combined acceptance passes.

Main repository was read only: original HEAD/status and all 18 dirty file hashes match the recorded baseline. Seven frozen source files match `49b666f`. No push, merge into main, shared service mutation or shared environment installation.

Cherry-pick production/test sequence from dcd base (root handles different-method runner merge if needed):
`df3565e`, `1960fa9`, `5edb4a3`, `c3439ca`, `ae46897`, `b52630a`, `c21afdc`, `fa5e0f0`, `77cacbf`.
