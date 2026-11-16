# Owned historical fixtures and native checkpoint invariants — 2026-11-17

This is the kv_native_sink lane's evidence, not a claim that every test passes on a single final platform commit. Every historical result remains bound to its own clean HEAD and input hashes. The root coordinator owns the final integrated verdict.

## Scope and isolation

- Tree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_owned_history_20261116`.
- Branch: `engine/gpt-owned-history-20261116`; initial acceptance base `ec3b6abbb62c0cdf7e45cc21511978c8aae0caf8`.
- Original assigned inventory: `/tmp/demiflow-root-remaining-safe-20261116.json`, group `reviewed_owned_fixtures`, exactly 516 nodeids in 42 ownership-reviewed files.
- Main repository stayed at `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`; no main-tree tests or writes. Tests pin this worktree via PYTHONPATH and the original `/yzp/zhaozy/yangzepeng/0905/env/bin/python`; private TMPDIR and resource root/home/Forge domain live under `/tmp/demiflow-*`.
- Tests retain fail-stop per completed batch, exact selectors, setup/call/teardown reporting, import provenance and before/after source snapshots. No skips, reruns, blanket expected rejection, or test omission counts as a pass. Old failed/incomplete receipts are retained.
- Frozen seven files are byte-identical to the acceptance base.

## Repairs owned by this lane

1. `tests/test_forge_audit_runs.py`: the old fixture never executed an original run. Public resume correctly rejects it for missing native recovery evidence. The scope-rejection test now observes the internal scope check directly and proves zero new attempt, commit or business output. Root reviewed the boundary.
2. `tests/test_forge_prompt_contract.py`: replace an independent sqlite request ledger plus deletion/rewrite of published output with declared request policy and interruption/resume of the original RunRef. Preserve partial answers, absent answer fields, fixed output version and exactly one external model request. Add an explicit old sqlite-journal configuration negative proving managed admission fails before any request. Root fixed the independently exposed old folded-stream residency rejection (`bf92cc9`) and ran this positive; that root result is not silently added to this lane's receipt counts.
3. `tests/test_forge_reader_scratch_race.py`: use a short, owned `/tmp/demiflow-auth-*` Unix socket path. Preserve peer identity, callback authorization and cleanup; do not relax the original callback checks.
4. `tests/test_lance_io.py`: preserve all eighteen historical checkpoint selector names while expressing their behavior through the current public Dataset.write_lance and original RunContext/RunRef. Registered output uses the native-action projection, not a new checkpoint protocol or old pending sidecars. The append-mismatch method remains its original owner's implementation, consumed from `e686bd8` with production `da12bad`.

## Eighteen historical guarantees, current native proof

All names below are in `tests/test_lance_io.py`.

| Original selector suffix | Current proof |
|---|---|
| `writes_pinned_replayable_snapshot` | Write with an actual native receipt, read its fixed version; a later direct overwrite does not change saved output. |
| `replay_skips_execution` | Interrupt the original run after ACK, resume its original RunRef; the source executes once and original commit records remain equal. |
| `rejects_changed_fingerprint` | A changed registration request rejects before the replacement source, preserving original journal commits. |
| `zero_rows_commits_valid_empty_table` | Explicit-schema empty output commits; original-run replay preserves the empty table and its version. |
| `enforces_explicit_schema` | Extra fields and incompatible scalar types reject with no target publication. |
| `nullable_fields_accept_missing` | Missing declared nullable fields round-trip as null. |
| `rejects_concurrent_writer` | Hold the actual registered-write lock; a concurrent caller rejects before invoking its source. |
| `retry_after_partial_attempt` | A foreign non-table directory is refused before input; the precise destination lookup error is recognized and every original byte and directory entry remains unchanged. |
| `never_overwrites_plain_written_table` | Create against a foreign complete Lance table rejects; old data and version stay unchanged. |
| `recovers_pending_publish` | Inject after native sink commit and before runtime receipt. Resume the same native registration run; original commit identities become ACKed without another source invocation or Lance version. No old pending sidecar is fabricated. |
| `failed_attempt_leaves_target_untouched` | A later bad row produces no target or native publication receipt and cleans owned Arrow staging; necessary original recovery evidence remains. |
| `replay_binds_caller_uri_after_move` | Move the owned dataset/control/run bundle twice. Confirmed output remains readable at the new location; original transaction/history URI remains unchanged and source is never reopened. |
| `replay_detects_missing_table` | Removing this fixture's confirmed table yields a missing-target error; no recreation or source execution, and original commit records remain unchanged. |
| `rejects_remote_uri` | The local registration boundary rejects a remote URI before local directory creation or source execution. |
| `async` | Invoke the synchronous public write through standard asyncio.to_thread in an actual async context, then read the resulting fixed version. |
| `chainable_after_save` | Continue public Dataset operations from the pinned native output. |
| `supports_async_operator_plan` | Real map_async stages commit and resume through the original RunRef; rows and per-row execution counts remain exact. |
| `async_from_running_loop` | Public write completes while an independent caller-loop task progresses; output remains correct. |

Prefix every suffix with `test_checkpoint_`. The retired convenience method is not restored. Test-only `_write_pinned` composes public write_lance(return_receipt=True) with read_lance(version=receipt.committed_version); it is not a production API, scheduler or receipt authority.

## Findings and consumed integration fixes

- Pytest 9 native unittest subtest reports initially looked like duplicate call phases. Root supplied `ae41997` with explicit native subtest identity, while retaining duplicate-main, failed-then-success, skipped, xfail and rerun rejection. Fifteen ambiguous old results were really rerun; no old receipt was reinterpreted as passing.
- The first eighteen-case native migration run at `76bf61a` passed sixteen. One failure was an obsolete exception wrapper assumption: current native preflight already refuses the foreign directory. The replacement check only accepts the exact actual target's Lance missing-dataset ValueError and additionally asserts zero source execution and unchanged directory contents.
- The second failure was a real registration defect: Path('s3://bucket') erased the URI shape, created a local `s3:` hierarchy, and invoked the source. The probe's cwd was private. Root supplied `2fd6d23`, validating URI syntax before Path conversion; its four rejection cases and positive local file URI case join the final native combination.
- Consumed `ec1ee1f` / `e686bd8` / `a445d12`: original native registration action and fixed transaction identity validation. Equal schema, count and version alone cannot identify a foreign replacement table.
- Consumed `da12bad`: deterministic append schema incompatibility rejects before staging/native publication, retaining any valid earlier intermediate cuts.

## Evidence rules and limitations

The companion JSON records every original selector's actual outcomes, source HEAD and receipt digest. Results spanning several HEADs prove those specific tested inputs; they do not imply every one of the 516 selectors was rerun on the final combined source. The original first-red receipts remain available. Changes and follow-up runs are attributable individually.

No Ray, multi-machine, performance or GPU claim is made by this historical-fixture lane. KV's separate thirty-case acceptance is recorded in KV-NATIVE-SINK-CLOSURE-20261116.md and is not folded into this inventory.

## Final fixed-source combination

- Tested HEAD `07ec3aa76e36eb6fda943ebfc10506d01bbd7ffb`: **29 passed**, 87 setup/call/teardown phases, no skip/deselection/collection errors, 130.47 seconds (outer runner).
- Exact 29: the eighteen historical checkpoint methods + original append schema mismatch + ten native registration controls (including four remote rejections and a local file URI positive).
- Receipt: `/tmp/demiflow-kv-owned-history-r8/checkpoint_native_combination-000/receipt.json`; SHA256 `204f4e32da681f7a13139ed0ef9c25ed9a2802ab279d70c66b33359d62e2c1e8`.
- All 2919 input digests were equal before/after execution and matched the tested HEAD before this documentation-only evidence commit.
- Original assigned 516 selectors were all executed. This lane has source-bound passing receipts for **502/516**; the remaining 14 are Prompt (1, root independently passed after bf92cc9), Lance artifact fixtures (4, owning lane), and native barrier fixtures (9, root). These are historical remaining entries, not a statement that root's current integration still fails them.
- Final documentation adds only this Markdown and companion JSON after the tested commit; production and test bytes remain those actually tested.

## Patch handoff

Root already integrated earlier lane commits `2bc442b`, `ee9b1d4`, `7d23a20`. The eighteen-case migration is `76bf61a`, followed by precise foreign-target refusal observation in `66e3316` and `7f1aa35`. Other commits in this branch are explicitly consumed owner/root patches and should not be reapplied if already integrated.

