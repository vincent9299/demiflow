# Small native publication workspace closure — 2026-11-17

The original `test_durable_write_lance_recovers_without_empty_early_publication`
now succeeds with its unchanged **40 KiB total memory, 8 KiB R, 16 KiB B,
8 rows, checkpoint_rows=2 and recovery assertions**. An independent clean
`8928e5b` checkout reproduced its planning failure, so this was not introduced
by the concurrently developed partition-wave changes.

## Current delivery after independent metadata review

The initial nine passing tests did **not** cover the outer terminal manifest
and retained columns. Independent review identified that missing lifecycle
accounting; the earlier claim of a complete workspace closure was incomplete.
The follow-up now includes those objects in the original frame admission and
passes the sixteen-case selection below. This is a scoped result, not a claim
that the whole platform has completed final acceptance.

- Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_durable_publication_20261117`.
- Branch: `engine/gpt-small-terminal-metadata-delivery-20261117`.
- Integration: original `866395be1a6400d53ea8f31273c83c72d3992be0` on `8928e5b`, then metadata follow-up `ed4b38c`. The latter's parent `d8f9b03` contains only the original delivery documentation.
- Final tested commit: `51d606677ac2dc498a442c4d5eceb13b1621a640`.
- Follow-up delivery and tested commit have identical complete Git tree `9cf13a7c76ebc8e1f18727ddf5c32020af440a5f`.
- Original development branch/commits retained. No original test edited.

## New-plan contract and actual budget

The previous v1 plan reserved at least B+R=24 KiB for native conversion during
every source phase, leaving only 11 KiB for a source/terminal that itself required
at least B+R. It could not reach original durable parking at all. Merely scoping
publication to the terminal still left the fixed terminal minimum too large.

Only newly compiled native plans whose **legacy terminal/index/publication
minimum cannot coexist** now use `demiflow.terminal_workspace.v1` with an explicit
`resident_bytes` window. Previously feasible plans retain their old window
selection. Missing declarations and old v1 seals retain their original profile,
reservation, frame and identity interpretation.

For the original case, the same 40 KiB is split into:

| Owner | Bytes | Lifetime |
| --- | ---: | --- |
| Original control reservation | 5120 | Entire execution |
| New terminal workspace | 14336 | Original live worker allocation |
| Original native publication workspace | 21504 | Reserved beside active terminal/publication |

These add to exactly 40960 bytes. The constrained default divides optional
working space in two shares for terminal codec/index and three for native
input/array/writer copies, bounded by its old target. This is a choice of initial
working-set grants, not proof that any R-sized row will fit: both actual runtime
peak guards remain authoritative. Public R/B ceilings and total M are unchanged.

The existing v2 `PublicationReservation` supplies source-phase, scheduler and
native admission consistently. Original durable cuts/ACKs park upstream workers;
only confirmed physical exit releases their allocation. The terminal remains
resident through the native publication future; the new test observes its actual
14336-byte allocation at native commit and the exact 40960-byte global sum.
Neither an early empty cut nor an ACK alone publishes a Lance version or releases
a physical grant. No driver, journal or allocator implementation was added.

## Actual lifetime checks

- `TerminalBlocks` parses the new schema strictly, keeps the original state
  ceiling, and charges its live block/root metadata inside its workspace.
- The original shared frame check includes this metadata and queued commands
  beside existing peer buffers, reader state and kernel state.
- Current Record, encoded frame and bounded escaping buffers remain charged
  while switching blocks. Scratch reads are sized and checked **before reading**
  for both current and previous chunks plus the retained row and index.
- The same callback bounds `SegmentForest` restore reads/JSON decode and index
  encoding before allocation. Its old optional-argument-free path is unchanged.
- The already decoded incoming raw document is released before downstream
  encoding; no old extra copy is silently excluded while still live.
- The driver continues to use its v2 shared row/index/Arrow/writer admission;
  its workspace is not borrowed a second time by the worker.
- Only the new terminal workspace measures actual root/manifest/column trees
  jointly. Identity de-duplication avoids counting the same column list or
  partition descriptors twice when they also appear inside the outer manifest.
- Outer restore uses the original bounded `read_document` and `decode_metadata`
  frame calls. The decoded manifest stays charged throughout each child index
  restore; it is released afterward while the retained columns stay charged.
- `manifest_partitions` admits list/descriptor growth before construction and
  retains earlier partitions during subsequent index sealing. Snapshot releases
  the stored frontier bytes, then retains its live refs, partitions and manifest
  under the same frame. No driver publication memory is borrowed.
- New-window manifest encoding streams the exact original JSON bytes to the
  original store. Largest token, iterator/key-sorting state, UTF-8/Unicode and
  previous/current writer fragments are admitted before creating/advancing the
  encoder. Old window encoding still calls the original `document` unchanged.

The implementation uses six production files: publication-budget schema constant;
compiler window selection; `partition_stages` residency; terminal blocks;
optional segment-forest frame callback; and runner `__init__`, `_frame_call`,
`_receive_data`, `_restore_terminal`, `_set_terminal_columns`, `start`, `_push`
and `snapshot`. The follow-up changes only the last three production files,
plus one new test file. `dataflow.py`, durable residency, wave scheduling and resource
allocator are untouched. Narrow runner/placement diffs are under
`/tmp/demiflow-durable-publication-delivery-20261117/`.

## Evidence

| Receipt | Source | Result |
| --- | --- | --- |
| `/tmp/demiflow-durable-publication-r1-evidence/verification.json` | baseline `8928e5b` | Original node fails planning |
| `/tmp/demiflow-durable-publication-r2-evidence/verification.json` | initial candidate `ec591e8` | Real terminal decode rejects 11610 > 11264 bytes; guard was retained |
| `/tmp/demiflow-durable-publication-r3-evidence/verification.json` | intermediate `988bdb4` | Original node passes after repartitioning the same M |
| `/tmp/demiflow-durable-publication-r4-evidence/verification.json` | initial delivery `f2ed096` | 9 passed, 55.12 s; outer metadata omitted from this selection |
| `/tmp/demiflow-durable-publication-r5-evidence/verification.json` | first metadata fix `a8a46d2` | Original recovery and three small-window tests: 4 passed |
| `/tmp/demiflow-durable-publication-r6-evidence/verification.json` | metadata fixture/encoding check `8209f3a` | 3 passed, 2 failed: 12 columns actually fit; conservative whole-manifest encoding rejected a feasible two-block snapshot |
| `/tmp/demiflow-durable-publication-r7-evidence/verification.json` | final `51d6066` | **16 passed**, 63.27 s; **48 complete setup/call/teardown phases** |

Final selection includes the unchanged original 40 KiB recovery node, unchanged
80 KiB sort/native recovery, unchanged 64 MiB wide join, old sealed-window rules,
two original segment hash/restore variants, and three new checks:

1. Actual native publication followed by a lost return: one version is committed,
   original `prepared` final fact is resolved to ACK on resume, earlier ACK facts
   stay identical, and no worker can restart.
2. Same 40 KiB envelope and public R/B, a 4500-character business field: planning
   succeeds and both source and terminal really launch; terminal codec rejects
   the actual peak before native construction. No target/result is published.
3. Real artifact store, logical block switch, retained current row, multiple
   scratch reads, original bytes and restore: row/index/two-chunk coexistence is
   checked against the same 14336-byte workspace.

Seven additional selectors cover two-block unfinished restore at 2/12 columns
(success) and 24/48 columns (outer decode rejects before allocation), the live
outer manifest plus child index plus queued-control peak, shared-reference
snapshot accounting, and byte-identical streaming/long-token rejection.
These use a real RunArtifactStore and the original runner/frame/terminal methods
with a scaffolded participant assignment; they are component recovery checks,
not a separate full Dataset multi-column execution. The original 40 KiB Dataset
and native-publication recovery cases remain real end-to-end checks.

All 2987 recorded final inputs match the `ed4b38c` delivery tree byte-for-byte, with no
changes during the run, a clean checkout and no surviving child process. Four
resource domains, TMPDIR, D8, actual import root and the frozen seven are pinned
by the original runner. The [machine report](SMALL-NATIVE-WORKSPACE-CLOSURE-20261117.json)
contains hashes, exact commands, selectors and phase results. Only these two
report files change after that source-bound verification. Four pytest warnings
concern `record_property` with xunit2; the runner retains those properties and
all three phases directly, with no skipped tests.

The main/root checkout was not edited. No full matrix, GPU, two-host, full V035
or formal performance run was repeated. Managed working-set admission is not a
claim that process RSS equals the small pool; platform-owned metadata is still
explicitly admitted. No failure remains in this final sixteen-case selection.
Parent integration still combines this candidate with its separate wave changes.
