# Local streaming scheduling of Dataset.map

An ordinary `map(fn)` keeps the existing lazy execution contract. In a streaming chain, a `map` without the new parameters is still a single worker with a one-row input queue, executed inline, with the previous exception behaviour. This capability lets the caller declare a scheduling policy through the public `Dataset.map` and has the platform execute it. It does not choose business models, change request identities, retry business calls or restart a running flow.

## Interface and execution paths

```python
from demiflow import data

def prepare(row, *, scale):
    return {**row, "value": row["value"] * scale}

prepared = (
    data.from_items([{"value": i} for i in range(12)])
    .map(prepare, fn_kwargs={"scale": 2},
         concurrency=4, queue_depth=2, execution="thread", label="prepare")
    .materialize()
)
print(prepared.take_all())
```

| Parameter | Streaming value when omitted | Contract |
| --- | --- | --- |
| `concurrency` | `1` | Positive integer, the most rows this node processes at once; bool, float and string are not accepted |
| `queue_depth` | `concurrency` | Positive integer, the most rows waiting in this node's input queue |
| `execution` | `'inline'` | `'inline'` runs on the event loop; `'thread'` uses a thread pool dedicated to this node |
| `catch` | `()` | Tuple of Exception subclasses; a matching row exception counts a miss and drops the row, without retry |
| `label` | name of the callable | Non-empty name for statistics; repeated names get the platform's node suffixes such as `#2` |
| `callable_scope` | `'stage'` | `'stage'` shares one callable; `'worker'` constructs a separate instance per logical worker |

Any of these parameters with a value other than `None` declares a streaming policy on the logical plan. So `concurrency=1`, `catch=()` and `execution='inline'` are explicit declarations too; passing only `None` is the same as passing nothing. The ordinary `fn_args/fn_kwargs`, constructor arguments and field binding interfaces remain. Scheduling attributes of the same names on the callable are not read; the policy of `map` is what the call arguments say. An inline synchronous call still occupies the event loop, so raising the concurrency alone does not make it parallel. `thread` suits blocking I/O or computation that releases the GIL; a linear speed-up of pure Python CPU callbacks is not guaranteed.

| Path | Behaviour |
| --- | --- |
| Local `run_stream()` | Executes the new policy; a plan whose only streaming part is an explicit streaming map can be executed too |
| Local `materialize()` | Reuses the same streaming executor and delivers the materialized handle after resources were cleaned up successfully |
| `checkpoint()` / `checkpoint_lance()` | Explicit checkpoint orchestration has left the current native capability; recover with RunContext |
| Terminals supported locally: `take/iter_rows/write_*` | Reuse the native streaming result, delivery or direct sink adapters; the exact capability is decided by the unified entry check |
| Ray | Supports neither these streaming parameters nor `run_stream()`; refused before the cluster and the data sources are accessed |
| `join/reduce_by_key`, lazy grouping and `union` consuming an input with the new policy | Materialize first; a scheduling policy cannot be hidden inside a relational source |
| The `other` of `prepend(other)` | Must be a finite synchronous Dataset; it cannot contain an explicit streaming map, not even with concurrency 1 |

`backend_options` and the new policy cannot be given together. Relational operators gain no streaming execution from this; pin a prefix that needs relational computation in the existing way, then attach the streaming chain. The lazy task concurrency of `data.local_execution(workers=...)` and the `concurrency` of a streaming node are different budgets; a streaming thread pool is created as the node declares and is not quietly changed to the number of lazy workers.

## Ownership and lifecycle of the callable

`callable_scope='stage'` keeps the meaning of one callable shared within an action. A plain function, its closure and the references in `fn_args/fn_kwargs` are shared within the node; the caller is responsible for thread safety. A callable instance follows the action-level copy rule of `CallableSpec`, so execution statistics cannot be read from counters on the original instance. An object holding connections or locks that cannot be copied should be passed as a class and initialized through constructor arguments. A bound method keeps the reference to its original owner; the owner is not copied when an action runs again.

For isolated state, pass the callable **class** and `callable_scope='worker'` explicitly:

```python
class Normalize:
    def __init__(self, offset):
        self.offset = offset
        self.processed = 0

    def __call__(self, row):
        self.processed += 1
        return {**row, "value": row["value"] + self.offset}

    def snapshot_state(self):
        return {"processed": self.processed}

    def restore_state(self, value):
        self.processed = value["processed"]

stats = (
    data.from_items([{"value": i} for i in range(100)])
    .map(Normalize, fn_constructor_args=[3], callable_scope="worker",
         concurrency=4, queue_depth=2, execution="thread", label="normalize")
    .run_stream()
)
```

The platform does not require a base class. Each logical worker creates its instance when the first row it actually processes arrives and reuses it row by row; one instance never executes two rows at once. There are at most as many instances as the node's concurrency, and none are created for empty input or when an earlier filter excludes everything. Running the same Dataset again uses new instances. `'worker'` does not accept functions, partials or pre-built instances, so that a copied closure is not mistaken for isolated state.

What is isolated is the instance's own state. Constructor arguments, fn arguments and global objects are not copied recursively, and shared mutable dependencies passed in must still be isolated or synchronized by the caller. A worker is a logical consumer; **a fixed OS thread is not guaranteed**: the thread pool may switch threads between calls. Resources such as thread-bound connections cannot get thread affinity from this mode.

An explicit streaming map manages the instance lifecycle `astart → __call__ → astop → aclose`. `astart` runs at most once per instance; `astop` runs after the node has drained; `aclose` runs when the action exits. Failure and cancellation also clean up instances that were constructed, and a failed start still attempts stop and close; when construction itself fails, the constructor is responsible for the local resources of the instance it did not deliver. A failure to clean up one instance does not stop the cleanup of the others.

Construction and lifecycle hooks run on the event loop; a hook may return an awaitable but should avoid blocking the loop. `execution='thread'` moves only the per-row call. A thread call inherits the caller's contextvars. Stop and close happen only after running thread calls have drained; an instance is not released while its resources are still in use. An old `map` without the new parameters does not gain the new instance isolation or lifecycle management, for compatibility.

## Row semantics, exceptions and cancellation

The new policy keeps the existing streaming row protocol: a whole-row map returns a row, `None` or a list of rows; `None` counts a drop and a list expands by the existing protocol. A field-bound map keeps the fields it does not change; `output` puts the whole return value into one column, and `outputs` binds the returned fields one by one. A field-bound function returning `None` may be a legitimate column value and is not the same as a whole-row map dropping the row.

The callable of `map` must be synchronous. Known async functions and async `__call__` are refused at declaration; a plain function that hides an awaitable in its return also raises during execution, and a coroutine is never written to the output. Asynchronous work keeps using `map_async`.

`catch` handles only errors of the per-row call. Undeclared errors, an ordinary `TimeoutError` included, end the stream; failures of construction, start, stop, close, reading the source and writing a table are not swallowed by the row catch. `BaseException`, cancellation, KeyboardInterrupt and the like cannot be catch types of the new map. An exception path does not return a successful materialized handle; sink versions already committed stay as their receipts say, and old tables are not rolled back.

When an exception that ends the stream is passed to the caller of the action, its type is restored from a fixed list; the engine does not import a type by a name sent from a worker. Types on the list are returned as they are: `ValueError`, `TypeError`, `KeyError`, `IndexError`, `LookupError`, `TimeoutError`, `RuntimeError`, `AssertionError`, `OSError`, `FileNotFoundError`, `PermissionError`, `NotImplementedError`, `ZeroDivisionError`, `MemoryError`, and the platform's own error types. A type not on the list is returned as its nearest built-in parent that is on the list, with the message unchanged: for a `class BadRecord(ValueError)` raised in a callback, or a library's subclass of `ValueError` (for example pyarrow's parse error when reading CSV), the caller gets `ValueError`; the original class name is in the worker stack attached to the exception (`__notes__`). For a type that is neither on the list nor has such a parent (for example one that inherits only from `Exception`), the caller gets `DataflowError` (`INTERNAL_PROTOCOL`).

`run_stream(stall_timeout=seconds)`: when neither the input nor the output row counts of any node change for this long, `StallError` is raised; its message gives the length of the stall and the input row counts observed before, without task stacks.

This interface adds no per-operator `hard_timeout` or operator checkpoint parameter. A thread cannot be killed safely; a callable with blocking I/O must set a finite deadline itself. STOP, external cancellation and the failure of another node stop further feeding and wait for started threads to finish before cleanup. This wait has no guaranteed upper bound, and a stage watchdog must not be passed off as a hard thread timeout. `map_async(execution='thread')` still refuses `hard_timeout` and its combination with an operator checkpoint.

In `map_async` and `batch_map`, a `TimeoutError` raised by the callback itself (including an exception handed over by `asyncio.to_thread`) is likewise handled only by an explicit `catch`; without a matching rule the stream must end, and native materialize/write_lance neither completes the failed action nor publishes an empty result. A configured `hard_timeout` that has not fired does not change where a callback exception comes from. When the runtime's own `asyncio.wait_for` deadline expires, it still counts a `hard_timeout` miss under the existing contract, and a whole batch acknowledged as missing keeps dead_batches; an explicit catch takes precedence. During cancellation, a callback's return value or raised exception follows the semantics of `wait_for`; a deadline cannot be inferred from elapsed time alone. A synchronous blocking thread must still set a finite I/O deadline itself; a runtime timeout promises neither to kill threads nor to bound the whole cleanup time.

## Order and recovery

An inline synchronous node with concurrency=1 calls in the order rows are received; upstream concurrency may already have changed the order. Output of several threads or workers has no guaranteed order, and the platform adds no global sort and does not wait for slow earlier rows. Routing, accumulators and progress logic that depend on order should keep the semantics they need explicitly.

Scheduling parameters do not enter the identity of model requests or business checkpoints automatically, nor do they make those compatible automatically. Request reuse depends on the full identity: the actual payload, model, template, deployment, endpoint and so on. A router that assigns endpoints in arrival order, for example, may change request identities when the upstream concurrency changes; the platform cannot reuse responses across routes just because "the business object is the same".

The durable checkpoint of an action differs from a per-operator log:

- The native `RunContext` binds code, plan, sources and budgets, and saves state, results and recovery progress in one place.
- Explicit JSONL/Lance/operator checkpoint orchestration is refused by the capability gate; historical artifacts and old interface definitions do not represent a currently supported execution path.
- The pending Dataset of `prepend` stays synchronous. To process concurrently again, declare a map node on the main stream after the injection; an asynchronous Dataset cannot serve directly as the pending source.
- An ordinary map gives no exactly-once guarantee for side effects; rows not committed may run again on recovery.
- When a run is interrupted before its first cut is committed, actions such as materialization have no committed progress to rely on; `RunContext.resume` reports `INCOMPATIBLE_HISTORY` and a new run has to be started; intermediate files on disk are not a basis for recovery. The following cases can still continue, recomputing from the sealed source positions: writing a new Lance table that did not exist at the time (`mode='create'`); a first cut that only registered the intent and was not prepared yet; a model call action bound to the native model journal (confirmed replies are reused, and requests with an uncertain outcome are not resent automatically).

Barriers align only at the necessary local safe points. The source keeps feeding within the explicit budget of unacknowledged resources and does not wait for the final sink because the next checkpoint was triggered. For bounded in-flight snapshots of native HTTP models, reuse of known responses, protection of unknown requests and the completion boundary of every terminal, see the [native barrier protocol](../native-barrier-checkpoints.md). A direct streaming `count()` is still subject to the earlier nested-action restriction; count with `materialize().count()`. The barrier extension does not declare this existing failure path newly supported.

## Bounded resources and observation

Each thread node creates at most `concurrency` threads; each logical worker submits only one call at a time. Queued rows are not all submitted to the executor directly, so its unbounded internal work queue does not grow with the total input. A slow downstream propagates back-pressure upstream through bounded queues; failure and exit reclaim the pools of the current action. The number of instances, the lifecycle records and the set of thread futures are bounded by the node's concurrency as well.

These are limits on **rows and calls**; they cannot be read as a hard limit on bytes or on the RSS of the whole process. An estimate of the peak has to include source_batch_size, the reader's own readahead, the queue_depth of every stage, in-flight calls, results waiting to be taken by the downstream, input and output copies, and writer buffers. If the input of a node, its complete output and the callback's temporary objects are at most Bin, Bout and T bytes, estimate `queue_depth × Bin + concurrency × (Bin + Bout + T)` plus the buffers at the source and the end. Bout includes a whole returned list that is not fully expanded yet; shared underlying buffers, allocations of native libraries and self-built caches have to be accounted for separately, and when object sizes are unknown this estimate cannot be used to claim memory safety. No general hard memory limit was added for arbitrary Python objects, a single huge payload or third-party allocations. Use bounded inputs, projections, independent object references and the callable's own per-row budget; higher concurrency must not cover up an unknown row size. Work that needs isolation which can be killed keeps using the existing process isolation.

`stats.metrics['stage_policies']` records the actual concurrency, queue, execution, callable_scope and catch; `stages/queues/stage_processing_latency` keep the existing counts, queue peaks and time summaries. Stage duration excludes input queueing and downstream back-pressure but includes thread scheduling, waiting for the GIL and the delay of returning to the event loop, so it cannot be read as pure CPU time; the sum of the durations of concurrent calls is not wall-clock time either. The existing latency quantiles use 250 ms buckets; for differences of milliseconds refer to total_s/count and to independent wall-clock measurements.

Local pytest verifies, with isolated data, call concurrency, thread exit, instance isolation, cleanup after a partial start, back-pressure from a slow consumer, exceptions and STOP, refusal of parameters, field binding, materialization and checkpoint replay. The Ray tests cover only the up-front refusal by the real compiler and neither start nor certify distributed execution; this capability does not support Ray streaming. These checks include no full business run, real model throughput or thread-safety certification of a particular third-party callable.

## Implementation review and verification record, 2026-10-07

The implementation has three layers: `MapStreamOptions` stores and validates the logical policy; `StreamMapRuntime` binds the existing `StandardCallable/BoundCallable` and manages instances; `execution.stream` remains responsible for all queues, worker scheduling, thread pools and draining. No further executor or business adapter layer was built. `is_stream_operation` unifies how materialization, checkpoint and prepend recognize the new policy.

The boundary review fixed three exit problems: a STOP arriving after the producer had finished could be reported as success; a callback raising `CancelledError` had its worker treated as finished without error; after a Lance checkpoint consumer failed, the background bridge could go on producing. Each of these now has a regression test; a failure returns no successful materialized handle, and a failed checkpoint write cancels this background stream and waits for its cleanup.

The final related regression was **225 passed**, with this command (using the workspace's existing Python environment):

```bash
PYTHONPATH=. OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 ../env/bin/python -B -m pytest \
  tests/test_stream_map.py tests/test_stream.py tests/test_stream_threads.py \
  tests/test_stream_cancel_propagation.py tests/test_stream_admission.py \
  tests/test_stream_checkpoint_migration.py tests/test_stream_checkpoint_resume.py \
  tests/test_stream_deduplicate.py tests/test_stream_grouping_lookup.py \
  tests/test_stream_lance_references.py tests/test_async_materialize.py \
  tests/test_map_prompt_async.py tests/test_prompt_routes.py \
  tests/test_web_operator_lifecycle.py tests/test_lance_io.py tests/test_data_module.py \
  -q -p no:cacheprovider
```

Another, extended regression that included local relational, kernel, join and union tests was **204 passed, 8 failed**. The failures were 6 parameter combinations of `test_join_matches_legacy_canonical_semantics` and 2 of `test_reuse_join_partitions_on_both_sides_and_reject_changed_keys`, all list-by-list comparisons depending on a join order that is not promised. All 8 reproduced in an isolated copy of the file snapshot from before the change; the baseline's anti combination showed one more order failure. They are kept as existing differences between test and contract; platform behaviour was not changed by adding a sort, and the extended regression is not reported as all green.
