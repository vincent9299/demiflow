# Real graph compatibility adapter — closure evidence

## Problem and resulting behavior

The prior Local/Ray graph entry passed only `ancestor.plan` to `execute_action`; target plans were never executed. It resolved an empty policy/bounds input, losing the executor worker ceiling and RunContext limits, and returned the ancestor terminal rather than a graph result.

The adapter now captures one shared source/ancestor plus distinct real target programs in one original v3 graph. Native exchange routing implements broadcast/hash/rebalance/target. Every route is part of the one sealed graph/physical plan; original source snapshots, code pins, SHA checks, allocation scopes, worker protocol, durable parking, journal, commit coordinator and ACK remain authoritative. A direct native materialize terminal accepts the captured named branch input ports; no extra Union runtime publication or per-target action is created.

Only compiler specialization, graph entry adaptation, partition membership filtering and bounded read-only EOI observation were added. No new public Dataset method, execution DTO, journal or ACK authority was added. A reused target plan is cloned at operation occurrence boundaries so independent branches do not accidentally share an actor.

## Budgets and truthful result projection

The caller worker envelope and RunContext byte/time budgets propagate through the existing `resolve_action_runtime`. Three workers remain the tested ceiling; additional stages progress by the existing original durable parking protocol. Graph explicit prefetch/credit cannot exceed the admitted grant. Automatic durable reference windows remain the original planner's bounded grant on unrelated edges; a one-row memory input queue is not mislabeled as a one-reference durable window.

`target_rows` and ancestor `egress_rows` come from authenticated final native contributions, including parked-stage original contributions on resume. `target_bindings` names actual compiled occurrences/partitions. `checkpoints` projects original ACKed commits. `workers` counts actual authenticated worker boots, not simultaneous workers; the peak worker ceiling is separately checked in tests. `final_arrival` means first observed node EOI during this invocation, not historical commit wall-clock time: completed-history replay explicitly returns unavailable (`None`) and starts no worker.

## Compatibility limits and unclaimed evidence

Old U3 tests which demand seven workers despite an executor workers=1 argument, inspect the retired child-journal/BlockSink receipt schema, or inspect retired `run-resources` layout are not evidence for the new execution authority. They were not silently deleted or counted as passing. The new tests retain their core business semantics—shared evaluation, independent programs/state, typed payload fidelity, real routing, nonfinal recovery and final-result replay—against original v3 facts.

A repository-wide AST scan found no non-default legacy location callers. Non-default `host_id` now explicitly rejects; a supplied `resource_root` must equal the current executor root (an exact alias is accepted). The actual provider/scoped resource authority remains unchanged, and real entry tests lock both rejections before execution. No multi-host or GPU graph claim is made by same-host CPU Local/Ray tests. Timing of concurrent functional tests is diagnostic only, not a performance comparison.

## Failure experiments retained

The original adapter defect has real red evidence at `/tmp/demiflow-graph-adapter-red-r2/verification.json` and initial red tests at commit `7e23adac27499cffaaa672d9b6a813f35e91f8d8`.

Development logs remain under `/tmp/demiflow-graph-*`: dev4 observer accidentally altered a pinned production helper (the original code-pin correctly rejected it); dev5 six-row broadcast exceeded the deliberately tiny 64 KiB state envelope (the three-row original 64 KiB cases remain); dev6/dev7 exposed test assumptions about immutable RunContext limits, origin cut IDs for parked stages, and active worker availability. dev8 completed real rebalance recovery but its old harness looked only in live `_deliveries`, missing real durable ownership. dev9 fixed the optional parked test mode and passed 10 tests. dev10 accidentally retained a result generator after releasing its view; the observer now consumes it while owning the original handle. None of these failures was hidden by relaxing production verification, commit, memory or worker limits.

## Final fixed-source acceptance

Production/test source is commit `8287f0fac12943ec0544a4a08b7f7ef477494043`, before this documentation-only commit. Both final runs import this isolated tree explicitly and keep three private `/tmp/demiflow-*` resource domains. Each run binds 2,197 input digests byte-for-byte to that commit; inputs/HEAD remain unchanged, the tree remains clean, the seven frozen files match `49b666f`, and no child process remains after cleanup.

| Final run | Result | Evidence |
| --- | --- | --- |
| Local graph + L017 + F/L/Y/P + original fanout/expansion | **46 passed; 0 failed/errors/skipped; 473.58 s** | `/tmp/demiflow-graph-fixed-r4-evidence/verification.json` |
| Real Ray Core L018 + four routing crash/recovery cases | **8 passed; 0 failed/errors/skipped; 459.48 s** | `/tmp/demiflow-graph-ray-fixed-r4-evidence/verification.json` |

The four routing cases each really kill an authenticated worker/actor after a positive original nonfinal routing ordinal, resume from the original journal, preserve dag_id/plan_id and the exact committed prefix, verify the resumed channel ordinal suffix and independent branch results, and reject an actual duplicate already owned by native durable entries. The duplicate sample itself records its exact counters/prefix size (often before the first committed result); it is not misrepresented as a separate post-final-ACK duplicate experiment.

Local additionally verifies three original workers as the peak upper bound; caller byte/time propagation; independent state when one target plan is reused; tuple/bytes/negative-zero preservation; finite and empty inputs; final-result replay without a new worker, commit or invented historical EOI time; early credit rejection before source work; and preservation of original 65-delivery/global-window rejection plus durable expansion/recovery tests.

L018 no longer performs source-text AST guesses or unconditionally skips. Its four historical nodeids share one real Ray execution and verify sealed rows/bytes credit, rejection of a smaller historical byte grant without rewriting the seal, caller worker policy, and completed-result replay with zero worker boots. Missing Ray in a different interpreter is a missing-environment skip, never a passing Ray gate; this final env-ray run has zero skips and blocks Ray Data import during execution.

## Corrections needed to make the three-worker graph progress

1. The original fallback converted every memory edge to durable when the complete graph did not fit. A new-plan-only optimization now deterministically tries at most 64 original-memory edges and asks the original `solve_residency` to validate each proposed co-resident group. CPU/GPU/memory/host/worker constraints remain that solver's authority. Explicit durable/finite edges and historical seals are untouched; infeasible proposals keep the last legal plan, and non-resource errors propagate.
2. A parked group can contain a truly co-resident memory consumer. The original `_publish` may wait on that memory credit under its existing deadline only while the global credit table is not full. Absent durable consumers, terminal/global exhaustion and oversized invocations retain the original hard rejection.
3. `_source_quantum` now reads reachable original durable rows/bytes grants from the sealed DAG, including downstream boundaries behind a memory peer. Parked mode uses this existing source admission quantum. **Checkpoint rows/interval are unchanged**; L017/L018 explicitly compare sealed policy to the original run configuration. Temporary memory credit does not itself force a per-row park, while original total slots/bytes and actual reserve remain authoritative.
4. When all actual source-split owners have reported idle due to that window, but at least one source has not reached EOI, a mixed durable group requests the original checkpoint so its consumers drain and pause too. The existing durable path pauses all active members; it does **not** have source-first deferred pause. Worker pause stops new source reads while still draining queued memory, ACKs and native callbacks before idle. The new three-worker `source -> fast -> slow` counterexample requires actual in-flight memory at the trigger and verifies all three rows at both targets, original ACKs, and unchanged `checkpoint_rows=64` / `interval=60000`. The old 60-second timeout now rejects the real two-output/one-slot expansion in 4.75 seconds through the original error. Source EOI does not trigger this extra pause, preserving consumer finish progress.

Round 1: Local 35 pass + two preexisting tiny-memory failures; Ray one pass + two 120-second timeouts and one actual `ActorDiedError` from the deliberately killed victim. Round 2: Ray four real routing recoveries pass, but real one-slot L018 setup rejects and Local two new small-window tests fail. Round 3: Ray eight pass and Local 44 pass; the long-interval source-idle counterexample still times out. Round 4 fixes that actual group synchronization and passes all final 54 tests. All round receipts and failures are retained and byte-bound in `GRAPH-ADAPTER-VERIFICATION.json`.

The optional parked recovery harness accepts a raw `ActorDiedError` only when its actor_id equals the already authenticated, actually killed victim and original Ray state proves DEAD. It preserves the original exception/code in evidence; it neither accepts arbitrary Ray failures nor fabricates a production UNKNOWN_EFFECT code. Existing recovery scenarios retain their default non-parked behavior.

## Baseline failures kept separate

`test_shared_source_branches_keep_multiplicity` and `test_two_stage_dataset_progresses_when_only_one_stage_fits` also fail in the direct parent `b4a96a7`, before any graph-adapter change, in original `solve_residency` for their tiny memory envelope. An isolated archive compares all 2,195 source/test files byte-for-byte to that parent, then reproduces exactly these two failures in 1.02 seconds. They were handed to the root coordinator for the separate whole-platform closure; they are neither counted as graph regressions nor hidden as passing. See `/tmp/demiflow-graph-parent-probe-verification.json`.

## Integration order and ownership

Apply `7e23ada` (real red tests), `b882042` (adapter), `5503e10` (memory-credit wait condition), `a4fdc23` (bounded original-solver grouping / real L018), `91b8bf3` (actual durable duplicate sample), `c263d17` (source quantum / memory backpressure), `8287f0f` (actual mixed-group pause) and this documentation commit. The native/history dependencies already present on this branch are separate lane deliveries; do not repeat their cherry-picks.

`executors/ray.py` changes only `materialize_graph`; root's `aggregate` fix stays intact. `test_runtime_v3_y.py` changes only `test_y026`; root's `test_y012` stays intact. F015/F016 changes are narrow and must preserve root's F013/F014 and native F018/F019. Worker `_publish` changes only its parked credit condition; native's validated-wire optimization is a separate statement range. Other graph hooks are the owned capture/placement, runner membership/EOI/source-quantum and driver observation/actual-group-pause seams. No push, main-checkout merge, shared environment modification or user-service operation was performed.
