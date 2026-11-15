# Ordinary map task state and explicit streaming actor scope

The historical `batch_rows=3` assertion `[1,2,3,1,2,3,1]` is a business-state contract, not a stale private diagnostic. It must remain until an explicit semantic replacement is agreed and implemented.

Evidence from the existing source and public documentation:

1. `docs/datafusion.md` §原生执行和 Python 边界 states that process-mode typed callbacks are deserialized and instantiated for every configured `batch_rows` task; closure, bound-method and callable-object state retain task boundaries.
2. `dataset_native.finish_typed_batch` loads the serialized operations on every batch and calls `local_tasks._apply`. `_apply` constructs `StandardCallable`/`BoundCallable` per task; `CallableSpec.instantiate` constructs classes or copies callable instances.
3. `docs/streaming-map.md` §callable 所有权与生命周期 separately defines explicit `callable_scope='stage'` and `'worker'`. Those scopes cannot silently replace the ordinary map task boundary.
4. A read-only component probe executed the real legacy `finish_typed_batch` on task sizes 3/3/1 for a class, a closure, and a bound method. All three produced `[1,2,3,1,2,3,1]`; the driver closure state remained `[0]`. Evidence: `/tmp/demiflow-regression-task-scope-probe.json`. This is explicitly a legacy component proof, not a claim that the current engine passes the public regression.
5. The unchanged public `test_partition_callbacks_preserve_task_state_and_failure_before_commit` previously produced `[1,2,3,4,5,6,7]` on the new engine and remains a real regression. Its class/closure/bound-method expectations and failure-not-published checks were not weakened.

The parent confirmed restoration of logical task boundaries for ordinary map. The pending implementation must use sealed logical task size/ordinal and checkpointed input frontier; it must not reset by accidental worker, residency phase, or attempt. Explicit streaming stage/worker scope remains its own declaration. Constructor effects and unrecoverable live objects need an explicit recovery boundary; this review does not claim arbitrary user-side effects are made exactly-once by snapshotting Python fields.


## Implementation follow-up

The task-scope implementation is now delivered with 94 passing combined checks. See `LOGICAL-TASK-STATE.md` and its byte-bound JSON for actual class/closure/bound-method execution, shared narrow-chain input task boundaries, and same-RunRef partial-task recovery. The historical diagnosis above is retained as evidence of the original regression, not a claim that it remains open. Parent integration still owns the concurrent ordering/task-provenance merge boundary.
