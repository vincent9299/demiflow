# Ready terminal metadata batching: not adopted

The bounded candidate passed its focused correctness checks but did not improve the original `fast_cpu` workload. Do **not** merge its production commits. I08 remains open. This document and the adjacent JSON preserve the result; they are independently cherry-pickable without the implementation.

## Fixed comparison

| | Baseline | Candidate |
|---|---|---|
| Commit | `5e70a886d01dbcf94b15e9a03d5d8191c9960013` | `aafc922d265a838f1dd6e6210b1d746cfdef7192` |
| Wall time | 13.224129678 s | 14.470270802 s |
| First result | 1.907305542 s | 1.907857757 s |
| Output | 10,000 rows | 10,000 rows |
| Original ACKs / final ACKs | 34 / 1 | 34 / 1 |
| Sampled peak RSS | 144,850,944 bytes | 144,482,304 bytes |
| End pool / owned children | empty / none | empty / none |

Both use the original 20,000-input map → filter → map → iter_rows workload, at most 4 workers, 256 MiB memory, 64 KiB row, 256 KiB batch, 2 MiB state, 2 GiB scratch and 120 s deadline. Both source checkouts are on the same `/yzp` filesystem; outputs are private `/tmp` paths. Original input and output digests agree. The existing comparison-environment guard returned no errors. No profiling was active.

The single pair is 9.423% slower for the candidate. It is a rejection screen, **not** a statistically established regression or a completed five-pair benchmark. No five pairs were started. During this screen the coordinator ran no tests or heavy CPU work; another lane ran a separate approximately 60-row, 3-CPU Ray recovery validation. This concurrent background is recorded rather than hidden.

Receipts: `/tmp/demiflow-terminal-ready-perf-baseline-r1/trial.json` and `/tmp/demiflow-terminal-ready-perf-candidate-r1/trial.json`. The adjacent JSON binds their bytes, source commits, original limits, results, ACKs, resource release and measured peaks. The actual 34 ACKs are disclosed; they do not replace any formal scenario's 40-ACK requirement.

## What the candidate actually does

It packs only already-ready, consecutive terminal metadata events for one boot and partition. It does not wait for a batch, cross another event, change business records, replace per-record ACKs or create a second commit authority. Public WorkerEvent/Record DTOs, old single-frame bytes, durable schemas and the seven frozen files are unchanged. The design is recorded in commit `540c3a6063d797ae6d9f6b7cf11b7e366c9e0792`.

The six production files are `dataflow_terminal_frames.py`, `contracts/dataflow_codec.py`, `dataflow.py`, `dataflow_hosts.py`, `worker/bootstrap.py` and `worker/dataflow_host.py`, all under `demiflow/execution/`. Their final bytes are bound in the JSON. Production candidate commits are `ed14a0c`, `4a39588` and `aafc922`; none is approved for adoption.

Focused counterexamples cover complete validation before constructing a batch header, mixed old/new frames, same-boot/partition constraints, old `poll(max_events=1)` progress, bounded sender workspace, receiver raw plus decoded co-residency, and Ray residual lifecycle credit. The receiver retains a single bounded residue queue. Third-party/Python memory estimates are managed admission bounds, not claims of a hard process-RSS limit.

The real fused Local proof packed 241 rows into 59 groups, and real Ray packed 163 rows into 37 groups. Each retained original results, ACKs and final replay without creating another worker. The unfused three-stage route produced zero groups because terminal and upstream ACK events alternate. It correctly uses original frames; grouping across ACKs was not introduced to manufacture a gain. The original five-occurrence performance workload uses the original safe co-location decision under the unchanged 4-worker cap.

## Verification and limits

Final production commit `aafc922` has 67 Local tests passed plus 3 real-Ray tests passed, each with passing setup/call/teardown and unchanged input hashes. One optional old Ray test was skipped in the ordinary environment; the three other real-Ray cases do not turn that skipped node into a pass. Real-Ray cases include fused/unfused transport and checkpoint publication overlap. They used `/tmp/demiflow-ray-api-optional-env/bin/python`, a private zero-GPU owned cluster, and explicit provider assertions.

The earlier `ed14a0c` had a broader 165-pass Local run and a 3-pass real-Ray run. These are separate, earlier-revision evidence, **not** 165 passes claimed for the final revision. All four receipts and hashes are in the JSON. Launch commands supplied private `DEMIFLOW_RUN_ROOT`, `DEMIFLOW_CAPACITY_ROOT`, `DEMIFLOW_LOCATION_JOURNAL`, `DEMIFLOW_LOCATION_CACHE`, `TMPDIR` and pinned `PYTHONPATH`. The old receipt format omits LOCATION_CACHE; the JSON labels it as launch-environment evidence rather than pretending it was captured in-process.

An original iterator-close defect was independently fixed by the coordinator's `70ea78d` and included equally in both comparison arms. Candidate commit `2b4b6e8` is its cherry-pick, not a second fix. Separately, `bfb1786` corrects an old Ray unit-test double to accept and assert the already-existing exact `resource_envelope` signature; it can be adopted without this candidate. Intermediate red runs remain under `/tmp/demiflow-terminal-ready-*`; final passes do not erase their provenance.

No main-checkout files, running user jobs, shared environment, GPU allocation or remote services were changed. No push or main-branch merge was performed. Full nine-scenario Local I08, five alternating pairs, and multi-host performance were not executed by this screen.

## Historical comparison correction

The often-quoted 1.872162861 s result at `267b9144c6bf7447c6822aa716f6d5c0eda16990` is explicitly a **warmup**, not a five-run median. Its real iter_rows run has a journal and sealed plan/source/code facts, but `local_pull` / driver placement / `recovery=attempt_only`, with zero runtime commits and zero receipts. Its resource-admission semantics also differ from the current durable-worker route. It is the same business workload, not the same recovery guarantee.

This correction does not delete or replace the original evidence, change the formal 20% gate, or declare current performance acceptable. Formal I08 must report the guarantee difference and run its unchanged approved contract. The next work is the formal execution checklist and environment readiness, waiting for the coordinator's final frozen candidate; no further micro-optimization is authorized by this result.
