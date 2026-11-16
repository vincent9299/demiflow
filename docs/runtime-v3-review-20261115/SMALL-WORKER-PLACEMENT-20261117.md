# Small-worker Dataset placement — 2026-11-17

Production/test commit **364c930**, branch `engine/gpt-v3-actor-runtime-closure-20261117`, isolated tree `gpt_v3_perf_mainpath_20261116`. Base includes root 8184c10, recovery ab4c121, root timeout precedence 41725d0, callback 2971628/c5d8751 and root control JSON thaw f86d36c. Main checkout and frozen seven files were not changed; no push or main merge.

## Behavior and authority

A synchronous multi-source Dataset containing a class callback, join or union could not use one worker: the old fusion admitted only a stateless single-source linear chain, while real opaque/stateful inputs correctly could not park. The new placement retains every original logical kernel, state mode, image, input port and finite spool inside the original GraphRunner. No source or actor is falsely declared recoverable. Original explicit profiles, device/affinity constraints and host capacity must still fit. CPU is summed for stateful/concurrent workloads; the existing serial max applies only to stateless synchronous CPU use. All live input/output/state windows are summed, not discounted by object equality.

Implicit row/batch/state windows now fit the actual simultaneous graph even when the caller supplied LocalKernelOptions. The caller's total memory, explicit payload/native limits and replayed original physical plan remain unchanged. A one-worker finite graph may require the same implicit fitting because those kernels coexist. Explicit oversized ceilings can still fail admission.

Arbitrary iterator, datasource and SQL/external reads cannot share a one-worker business pump with async/timed work. Only platform item/range/literal-empty/materialized bounded pulls are admitted there; a real negative test proves an opaque source is rejected before its factory executes. This is a placement restriction, not a new source-read guarantee.

For multiple-worker streaming suffixes, DeduplicateOp and SaveLanceOp can share the existing worker under their original journaled effect/state declarations. Their original journal continues to own claims/publication and limits. A real test inspects the deduplicate nonrecoverable state and its original state-byte reservation while proving the downstream business exception is reached.

The historical Lance microbatch reference test now expects real versions 1/2: the first data microbatch creates v1, without an empty initialization publication. It additionally proves v1 contains only a, v2 contains a/b, current version is exactly2, each emitted reference is immediately readable, and the second sink contains two rows. This migration was expressly approved by root; no empty commit was introduced to satisfy old version numbering.

## Evidence

All accepted receipts come from the original inventory runner with exact selectors, private import/domain assertions, three test phases and unchanged source hashes. JSON alongside records digests and exact production bytes.

- Initial diagnostic batch: 11 passed / 2 failed. One parameterized selector was incorrectly passed as a function selector; the outer runner rejected the batch-signature mismatch. These results are disclosed as diagnostic only, not accepted final evidence.
- Exact r2: 7 passed / 1 failed. All six original single-worker lifecycle/start-failure combinations passed. The remaining Deduplicate suffix condition was corrected without changing its original assertion.
- Exact r3: **24 passed / 1 failed** in172.77s. Original five class/join/union cases, six actor lifecycle cases, declared-payload guard plus all four partial-default combinations, two deduplicate behaviors, Lance microbatch references, actual native-state inspection and four row/batch deadline diagnostics passed. The sole failure was the new negative test catching its expected error inside local_execution, causing RunContext correctly to report an unconfirmed action at context exit. Moving the expected error boundary outside the context preserves the exact original DataflowError and source-not-opened assertions.
- Final callback/negative-source combination: **18 passed** in16.89s, including that corrected negative and all four real stream timeout/dead-row cases after the cooperative shield follow-up. This source snapshot includes the placement candidate as well as callback changes.
- **Real private Ray Core, max_active_workers=1: 1 passed in17.07s.** It reuses the original shared actor recovery test: interrupt an actual original ACK, restore the same single actor state once, preserve exactly12 rows and final count24, with one start/close per attempt. The fixture closes only the private cluster it owns. Worker/CPU/memory ceilings were not expanded by the test.

## Explicit remainder

The internal U3 graph_spec adapter creates a router with six named reader partitions but the current residency solver treats each whole stage as atomic. One worker therefore cannot host that stage. This is a real separate partition-residency gap: neither reader/partition identities nor worker budgets were reduced/increased here. Root owns its follow-on. Whole-product/engine acceptance remains an integration responsibility, not established by this component report.
