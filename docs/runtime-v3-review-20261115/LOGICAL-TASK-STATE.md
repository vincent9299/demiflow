# Logical callback tasks: implementation and recovery evidence

Ordinary map now preserves the historical input-task boundary instead of silently retaining one actor for the whole action. Explicit streaming `stage`/`worker` ownership remains unchanged. This fixes a business result regression, not a private diagnostic.

## Sealed contract and execution

- `capture_chain` seals `task_batch_rows`, `task_group`, and `task_origin` into the existing KernelSpec. Class/closure/bound-method declarations are recreated from the original loaded program per logical task; no extra persistent program or progress ledger is introduced.
- A first ordinary callback counts original task inputs. Narrow filter, flat_map and field projection chains carry the original task group and ordinal in `Record.source_position.dataflow_task`; all existing source coordinates remain. Filtered rows do not regroup survivors; expanded rows do not manufacture new callback tasks. Limit, MapBatches, relational and explicit streaming boundaries start a new ordinary task group.
- The original operator image stores `task = {batch_rows, input_frontier, ordinal, origin}`. Partial batches restore their state; a completed origin batch does not reconstruct an already completed actor. Task ordinals and provenance are validated, including reversed/absent/malformed values.
- Direct function-owned strict-JSON cells/defaults/attributes use a bounded data-only state adapter. The compiler declares state/profile consistently. Mutable external globals, aliased mutable cells and opaque/rebound dependencies do not receive a false recoverability claim. No callable or pickle is put in the checkpoint.
- Actor construction can restore local resources. This does not promise exactly-once arbitrary constructor effects or unjournaled external calls; the existing effect/recovery boundary remains authoritative.

## First-hand verification

Final combined run: **94 passed, 0 failed, 0 skipped, 271.00 seconds**. Command, all nodeids, source hashes, process import assertion, and evidence XML hashes are recorded in `LOGICAL-TASK-STATE.json`.

The public counter remains `[1,2,3,1,2,3,1]` for class, closure and bound method with `batch_rows=3`. Actual worker event files verify calls and lifecycle. Filter→projection→map yields `[1,2,1,1]`; doubled flat_map input preserves six outputs in each original three-input task; an explicit limit starts a new group. Streaming stage/worker stays `[1,2,3,4,5,6,7]`.

Two real driver tests interrupt immediately after a native journal ACK of a partial task, resume the same RunRef with new worker/batch defaults, and verify original sealed size 5, exact 23-row output, preserved commit identity, and no second call for the confirmed prefix. Class and closure paths both pass. The tests inject only at the driver interruption seam; original workers, store, journal and coordinator execute the work.

The combination also retains all selected sync-map hidden-awaitable/cancellation tests, actor lifecycle tests, kernel tests, the historical native counter/failure-publication test and all 19 empty-schema tests. Component snapshot/corruption tests are explicitly distinguished from these real worker/journal tests.

## Historical test adjustment

`test_partition_callbacks_preserve_task_state_and_failure_before_commit` keeps every counter, multiplicity, driver-state, failure-message and previous Lance-version assertion. Its expected failed overwrite now runs in a separate execution scope with the expected exception outside the scope. Previously the test swallowed the business failure inside the run and then incorrectly expected the run's unconfirmed-history guard to succeed. The first implementation iteration passed all business assertions and exposed precisely this final guard, recorded in task-r1.xml.

The same iteration also exposed a nested frozen-configuration reconstruction error in the new BoundMap setup; using the existing `_plain` conversion fixed it. The initial real-recovery probe incorrectly tried to decode a worker-frontier document as a kernel image; the harness now selects the actual kernel-image media type. Both failed iterations are retained in the JSON report, not counted as green.

## Integration boundaries and limits

The parent owns the concurrent numeric provenance ordering fix and must ignore `dataflow_task` for source-order comparison. The artifact lane's independent `batch_remaining_ms` field must survive the operator image merge. The residency lane's BlockPartition/segment changes remain independent of logical task IDs; it confirmed its new logical blocks do not create additional physical origin replicas.

This batch does not claim Ray testing, arbitrary global/opaque closure state restoration, new parallel-source ordinal assignment, or a clean result for all remaining historical files. No main-repository files, services or environments were modified; the seven protected modules still byte-match 49b666f.
