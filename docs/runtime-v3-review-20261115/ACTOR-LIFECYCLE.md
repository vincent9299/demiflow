# Unified worker actor lifecycle

The executed callable and its lifecycle/state owner are now resolved together.
Framework argument/field wrappers, `partial`, and bound `__call__` methods point
to the actual plain actor. A `CacheCall` retains its own owner and forwards its
hooks as before. No user function is invoked during planning or preflight.

Worker cleanup calls `astop` then `aclose` (or synchronous `close`) exactly once,
including empty input, failed startup, and failed calls. A failed stop hook does
not skip close. Cancellation first drains the real coroutine, including its
`finally` block; a second cancellation must not interrupt that block. Background
tasks and actual default-executor futures are drained before actor resources
close. Cancelling `asyncio.to_thread`'s awaiter does not prove its thread exited.
Cleanup hooks may themselves use the executor before final shutdown.

Noncooperative synchronous work cannot be killed safely. Drain expiry reports
`DEADLINE_EXCEEDED` and does not claim resource cleanup; the original worker
provider retains responsibility for actual process exit. A directly owned async
instance can finish cleanup after that work really exits without duplicate hooks.
User-created external executors/processes remain the actor's explicit lifecycle
responsibility; the runtime tracks its own invocation pool and event-loop default
executor, not arbitrary foreign resource ownership.

## Verification

Own tree: `gpt_v3_residency_20261115`; baseline `db02411`, plus parent Arrow commit
`f0f79cb` (local `4b91191`) and regression tests `aaca517` (local `37ea2c9`).

Final pinned command (no edits or commits while workers were running):

```sh
DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-actor-capacity4 PYTHONPATH="$PWD:$PWD/tests" \
 /yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q \
 tests/test_runtime_v3_actor_lifecycle.py tests/test_runtime_v3_k.py \
 tests/test_bound_result_cache.py::test_shared_wait_cancellation_and_actor_hooks \
 --basetemp=/tmp/demiflow-actor4 --junitxml=/tmp/demiflow-actor4.xml
```

37 passed in 19.61s; `/tmp/demiflow-actor4.log`. Five public Dataset cases prove
hooks occur in the actual worker process for instance, method, partial, class
construction, and explicit call arguments. Dedicated tests cover stop/start/close
failures, repeated cleanup, coroutine/background/real-thread draining, and the
counterexample where live blocking work must prevent resource close.

Earlier `/tmp/demiflow-actor1.xml` reran the three migrated materialization
success/failure/empty tests. Each passed its real-worker lifecycle assertions,
then failed on historical handle release (`Stale execution owner cannot publish
or mutate progress`). This independent storage defect was handed to the parent;
those three complete tests are **not claimed green** by this delivery.

No main-checkout edits or tests, no push, no shared-environment changes. The seven
frozen files remain byte-identical to `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`.
