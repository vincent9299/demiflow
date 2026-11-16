# Artifact and actual native Prompt historical closure — 2026-11-17

All four original artifact tests pass unchanged on root integration `21f52b0`. The remaining slow native-call historical test passes after replacing an invalid fake native actor with actual PromptActor, a bounded loopback HTTP fixture, declared request_policy and the original request journal. **Production code is unchanged.**

Tree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_native_prompt_20261117`. Branch: `engine/gpt-native-prompt-20261117`. Tested Prompt HEAD: `341699014e3c6465a9e677f620fcb8a014c8b1e0`. Code commits: `cadfa89`, `3416990`. Only NativeSlowFixture and test_native_inflight_cut_allows_free_slots_past_slow_call changed; every other function/class in test_native_barrier.py was compared textually to the safe root base.

## Native capability and recovered behavior

The current OperatorKernel only snapshots pending prompt/embedding invocations with a real row-local native recovery adapter. Arbitrary objects claiming native_stream_recovery are correctly not granted that capability. Invocation images retain actual Record, slot, invocation scope and emitted frontier. Recovery consults the original native journal.

The replacement test uses concurrency 3 and checkpoint_rows 2. HTTP request id0 waits for id7. Other HTTP responses wait for the first actual cut ACK, giving a deterministic check that the first cut contains pending id0 and publishes fewer than 2 rows. After that ACK, free slots continue until id7 arrives, releasing the slow HTTP response.

For a real partial recovery proof, the test delays delivery of id0's response acknowledgement **after the unmodified RunPromptJournal.response has durably saved it**. No response, receipt or journal success is synthesized. One existing control worker waits while the others continue; all twelve requests finish their durable response writes. The driver is then interrupted while its actual latest nonfinal checkpoint still contains pending id0. On the same original RunRef, restoration chooses exactly that commit_id, keeps its original commit fact unchanged, yields all twelve id/answer rows and uses exactly twelve total HTTP requests and twelve units of the original request budget. This exercises a pending checkpoint, not only replay of a final result.

The test delays only fixture HTTP dependencies and a real persisted reply. It does not replace the commit protocol, manufacture a recovery image, mutate the native snapshot policy or make it drain pending work.

## Fixed-source verification

| Group | HEAD | Result | Evidence |
|---|---|---|---|
| Four original artifact selectors | `21f52b0ef729a7899c208b59897cbb8bd6f43acd` | 4 passed, 12 phases, 2929 unchanged inputs | `/tmp/demiflow-kv-owned-history-r9/artifact_native-000/receipt.json` |
| Actual Prompt pending-cut recovery | `341699014e3c6465a9e677f620fcb8a014c8b1e0` | 1 passed, 3 phases, 2929 unchanged inputs | `/tmp/demiflow-kv-owned-history-r11/native_slow_prompt-000/receipt.json` |

Artifact receipt SHA256: `784b82956eaafa41a9a71ac660cc696b6ceba20b9a0d78e4e92c159ec9d580d4`.

Prompt receipt SHA256: `6507094f33fdc3aeb0c6138816f7b1b70476170b937ed4c30d410e42ab6b9247`.

The first Prompt attempt is retained as a red receipt: the observation hook mistakenly read request.body instead of the actual request.payload field. The first native snapshot assertions already passed; this was a test-harness error, corrected in 3416990. It is not presented as a production defect.

All tests used the pinned worktree/env interpreter, private resource root/home/Forge domain and TMPDIR, original exact-selector three-phase guard, clean fixed HEAD and before/after input hashes. No skip/deselect/retry was counted as passing. The evidence commit only adds this Markdown and its JSON. No main-tree writes or tests, shared-environment mutation, push or merge occurred.

The earlier owned516 ledger now has 507 source-bound passing selectors from this lane, spanning their recorded HEADs. Other root/owner closures are not silently counted here. This is not a statement that all516 pass on one final combined HEAD, nor a Ray/GPU/performance claim.
