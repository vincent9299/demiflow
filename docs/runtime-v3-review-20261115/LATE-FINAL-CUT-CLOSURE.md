# Late EOI: complete only after a real final cut

The ordinary single-phase path could return complete results from an ACKed **non-final** cut. It then refused to read those completed results on a new attempt when a downstream live thread callable had legitimately released its attempt-only registry. This violates the same completed-result recovery guarantee already supported by the durable path.

The fix changes only `DataflowExecution._advance_cut`: after the current receipt, when every current worker is genuinely done and every source has real completion evidence, a non-final last-phase cut is followed by a fresh pause and actual final contributions from the still-live owners. The old ACK, flags, cut identity and receipts stay unchanged. The original coordinator generates the next cut and commits it. Intermediate phase retirement and incomplete early-stop prefix semantics are unchanged. Complete input takes this branch even when terminal satisfaction was also requested.

## Deterministic real failure

Original function bytes in this worktree and root `559f74ef5d7655f67b04ffd482e8b7a6a72c7a87` were identical. External script `/tmp/demiflow-late-final-proof-r2.py` ran a real public Dataset:

`from_items(one row) → native Embedding → explicit thread map → take_all`.

Only the local HTTP provider response and delivery time of original cut control messages were delayed. No worker event was reordered and no EOI, source cursor, contribution, receipt, or `_done` entry was fabricated.

1. Original `begin(final=False)` observed one of three workers complete.
2. Original native journal confirmed the one HTTP response. The real callback output was queued.
3. Original native cut delivery allowed the runner to publish that output and genuine EOI. All three workers completed before the terminal's original contribution arrived.
4. The coordinator ACKed the original non-final cut. The driver immediately returned the correct complete row with `image.cut.final=False`.
5. After normal worker/registry cleanup, resuming the same declaration failed with `INCOMPATIBLE_HISTORY: attempt-only live program has no final ACKed result`.

Original proof: `/tmp/demiflow-late-final-proof-r2/{proof,verification}.json`; one actual HTTP call and one original ACKed commit. The first external harness attempt stopped on a logging argument error; it is not counted as a product result.

The committed regression reads the actual native callback output queue to select the timing, without consuming or fabricating its contents. It has two arms: ordinary completion, and an actual terminal-satisfaction request after all real worker EOI notifications. Both require two real ACKs `[final=False, final=True]`, complete output, exactly one native request and one request-budget charge, and same-declaration resume with no newly launched worker endpoints.

## Verification, including failed rounds

All commands used the isolated worktree and explicit `PYTHONPATH=$PWD:$PWD/tests`, normal environment Python with `-B`, private `/tmp/demiflow-*` resource roots and pytest basetemp. No source edits occurred during a run.

| Run | Actual result | Evidence |
|---|---|---|
| Exact original root `559f74e` method injected into the new real regression, without changing repository source | **2 failed**: only one ACK instead of two | `/tmp/demiflow-late-final-original-method.xml` |
| Initial fixed real regression | **2 passed** | `/tmp/demiflow-late-final-tests.xml` |
| First broader selection | **13 passed, 4 failed** | `/tmp/demiflow-late-final-regression.xml` |
| Root `559f74e` idle fix + new final regression + four corrected suffix fixtures | **14 passed** | `/tmp/demiflow-late-final-integrated.xml` |
| Same integrated source: real native EOF crash/recovery, live final replay, non-final live refusal, finite phase recovery, and durable recovery at three boundaries | **7 passed** | `/tmp/demiflow-late-final-history.xml` |

The four broader failures were existing pure fixtures calling `fuse_thread_suffix` without its required `hosts` input. All four stopped at `TypeError` before executing the function. Tests-only commit `51d158e` supplies a bounded `HostDescriptor`; the original unknown-effect/device/budget/fusion assertions are untouched. Existing default-suffix and finite-fusion tests already cover insufficient per-host capacity. They were not duplicated.

The final fixed source is based on merge `9e058c5317b8ca62351240a3ae52ce403776b941`, which includes root `559f74e`. The paired JSON records the exact dirty input state, every Python input digest, every selected testcase, XML hashes, original actual failure events and frozen-file hashes. The final source digest comparison found zero changes during the integrated history run; all seven protected files remain byte-identical to the original protected baseline.

This establishes the Local real native/live completion and recovery boundary. It does not claim a fresh full API matrix, multi-host execution, physical GPU inference, production database access, or published-content GC validation. The main checkout and shared environments were not modified; no push or main merge was performed.
