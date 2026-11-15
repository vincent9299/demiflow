# Queue completion and native fetch recovery boundaries

Production tested: `559f74ef5d7655f67b04ffd482e8b7a6a72c7a87`, cherry-picked as `aea7bacc0e3e735376a074739aab2710bdec26b9` in the regression tree. The production diff against that parent is empty. This delivery changes tests and this evidence only.

## Original full matrix failure is retained

At parent `8e85e7a47c421714eafe16e6b044d23ad6b69f1a`, the unchanged Local matrix plus no-Ray-Data gate produced **1 failed / 1 passed, 954.58 s**. All 168 cases ran; 167 passed, only `queue-explicit-True` timed out at its original 600-second consumer limit. Both native tasks were settled, but the final Dataset ACK did not arrive. API status was 93 tested / 5 failed / 0 not_run; 106 real execute_action observations (the failed case did not reach its history action). Evidence, all 1764 before/after digests, fixed HEAD, 318 real imports and no doubles are retained at `/tmp/demiflow-regression-final2-api-evidence/`. This result is not rewritten as green by the later scoped checks.

Parent fixed a separately deterministic idle-notification race in `559f74e`. The parent owns its 8 red-to-green thread-ordering tests and other component evidence; this report owns the following actual business verification.

## Actual queue and recovery evidence

- The **unchanged original API-matrix `_queue` function**, both automatic sink settlement and explicit ACK settlement, passed with its original input, default resources, default timer and 600-second timeout. Each completed producer → two real HTTP fetches → native settlement → final Dataset ACK → two verified history results.
- Original producer recovery at both native-unit and Dataset-cut seams passed. Same RunRef recovery preserves the zero-output source row and exactly five queue tasks; no duplicate publication or unACKed native commit.
- Original `test_queue_v2_crash.py::test_sigkill_and_native_recovery[sqlite-True-final_checkpoint]` passed: actual SIGKILL, same RunRef recovery, one HTTP request, original final ACK/object budget/pending-file assertions unchanged.
- First targeted group: **4 passed / 1 failed / 2 skipped in 69.58 s**. Both skips are unconfigured task-private PostgreSQL. The failure is the old fetch test precondition discussed below; it is not hidden.
- Consumer SIGKILL run: **1 passed / 15 deselected in 18.06 s**.

## Corrected fetch-test precondition, preserving both business boundaries

Old nodeid: `test_nested_fetch_recovers_original_native_receipt_before_first_cut`. Its nested union has six distinct stages but the original default admits four active workers. The actual sealed plan chooses five durable-incremental edges. Before the first native fetch can run, it commits a source parked handoff with zero terminal rows/bytes and `final=false`. Therefore the old blanket `not execution_commits('action-1')` contradicted the valid physical plan. Increasing only the timer to 60 s still failed, proving the cause was mandatory residency handoff rather than the default one-second timer. Both failures and the physical/cut evidence remain under `/tmp/demiflow-regression-fetch-boundaries*` and `/tmp/demiflow-regression-idle-real-evidence/mandatory-source-park.json`.

The test is now `test_fetch_recovers_original_native_receipt_across_dataset_cut` with two explicit arms:

1. `after_empty_residency_cut`: preserve the **original two-fetch nested union**, default resources and default timer. Before recovery, assert all existing Dataset cuts are ACKed, zero rows/bytes and non-final; inspect the original committed state and require a real unconsumed source delivery under `demiflow.durable_residency.v1`. This replaces the false layout assumption with a stronger valid handoff proof.
2. `before_first_cut`: a single fetch over the same two input rows fits the original worker/resource envelope. An explicit 60-second checkpoint timer isolates this boundary without changing timeout or resource ceilings. Assert native raw ACK exists and **Dataset commits are entirely absent**, then resume the same RunRef.

Both retain the two exact row keys, two original native receipts including the interrupted receipt, ACKed receipt status, and exactly two HTTP requests. Both now also verify returned object bytes and that original journal usage is exactly two requests and twice the raw BODY bytes. No retries, budget expansion, request mocking or production guard relaxation was introduced. Final two-arm run: **2 passed in 36.96 s**.

Exact commands, tested head, logs/XML and SHA-256 bindings are in `NATIVE-RECOVERY-BOUNDARIES.json`. The helper runner asserts the actual imported demiflow path is inside this isolated worktree. Original full-matrix red is still a red result until separately revalidated on the repaired candidate. Main checkout was not modified or tested; no push or merge to main occurred.
