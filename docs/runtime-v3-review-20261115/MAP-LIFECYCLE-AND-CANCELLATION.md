# Public map semantics and worker lifecycle repair

This batch stays in the isolated regression worktree. No main checkout, shared environment, production process, protected source, test skip, or public assertion was relaxed.

## Implemented behavior

- A callable class remains a declaration until its logical invocation slot receives work. Empty/all-filtered inputs do not construct it. An already supplied actor instance still owns start/stop/close even for an empty action.
- Checkpoint images retain empty slots. Restoring an empty slot leaves it empty; restoring an existing state constructs its actor, runs startup, and restores the original state. Slot identity and call ordinals retain the existing wire/image format.
- Synchronous `map`/bound `map` rejects returned awaitables and closes never-awaited coroutines. Async work remains supported through `map_async`. Lifecycle hooks still run asynchronously.
- A callback's `asyncio.CancelledError` crosses the fixed worker failure vocabulary with its type and message. Async callback cancellation also survives the local asyncio→concurrent Future bridge. Other unknown exceptions retain the existing closed failure mapping; no remote imports are allowed.
- Caller cancellation does not become partial terminal success; owned actors close and the original journal contains no commit for the failed action.

`tests/test_runtime_v3_actor_lifecycle.py` had two newly introduced lifecycle cases using an asynchronous actor through synchronous `map`. Those two now use a synchronous subclass with the same lifecycle hooks and event assertions; its three `map_async` forms retain their asynchronous actor. This follows the public `Dataset.map` synchronous contract rather than normalizing the implementation regression.

## Evidence

- New semantic counterexamples before the patch: **13 failed / 1 passed**, `/tmp/demiflow-regression-map-red.xml`. The already-supplied empty actor case was the one passing case.
- Full K + semantic counterexamples + lifecycle + native query/admission + materialize/cache/Arrow combination: **85 passed, 204.47 s**, `/tmp/demiflow-regression-map-r2.xml`. This preceded the final async cancellation-cause refinement.
- Final changed-source validation: **44 passed, 89.30 s**, `/tmp/demiflow-regression-map-final.xml`. Covers sync/async cancellation, lazy factory image round-trips, actual worker lifecycle, in-flight call restoration, and the three native join/key checks below.
- Runner asserts the process imports this worktree. Commands, per-node baseline failures, input digests, and report digests are recorded in the paired JSON.

The previous three materialize release failures passed in the 85-item combination after the parent `047c04e` release repair was included. Exact Arrow schema and batch assertions remained intact.

## Native join checks follow the current executor

Parent `047c04e` replaced the DataFusion join. The historical worker probe now observes/faults actual `join_spool.join_pairs`, and observes `ordering.build_sort_runs` to reject extra sorting. The two public tests still require exact join multiplicities, callbacks once, no forced sort, native failure propagation, no journal commit, and unchanged previous Lance version/content. The date-key rejection test also passes with the parent's prevalidation fix. These are actual worker probes via a test-owned `/tmp` startup module, not driver-side substitute execution.

## Remaining baseline evidence, not claimed green

Earlier broad probes on the prior source recorded large_payload 3 failures (stopped after 3), local_kernel 11 failures/9 passes, and stream_map 63 failures/17 passes. They have not been fully rerun or migrated here. Several are retired API/private observation expectations; thread provider PID preservation, task-local actor recreation, large-row resource-policy behavior, fold ordering and seed resolution were separately reported to the parent. This batch does not count those as complete.
