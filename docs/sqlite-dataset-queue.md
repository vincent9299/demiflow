# Dataset streams with an embedded SQLiteQueue

`SQLiteChannel` extends the existing `SQLiteQueue` with bounded messages,
producer closure and immutable scans. It uses a local SQLite file (WAL, FULL
synchronous mode). There is no broker service. Producer and consumer can run
as independently started processes on the same machine. Each owns its own
configuration, stop signal, budgets and archive. The queue does not launch either
pipeline. Do not put the live database on a network filesystem, including DPC.
Use a durable local volume and consistent backups to shared storage. A stable
symlink may point to the local file; the storage check resolves its target.

## Delivery and archival

```python
from demiflow import data
from demiflow.queue import SQLiteChannel, channel_config

settings = channel_config('/local/run/channel.sqlite')
queue = SQLiteChannel(**settings)
queue.open('audit')

# Each row supplies messages with task_id, fingerprint, pool and payload.
producer.enqueue(settings, messages='messages').run_stream()
queue.seal('audit')  # only after successful publication

# Can run concurrently with the producer, before seal.
(data.read_queue(settings, pool='audit')
    .map(process_or_replay)
    .ack_queue(settings, result='queue_result')
    .run_stream(source_batch_size=1))

# An independent archive reads immutable records, without claiming tasks.
through = queue.high_water('audit', results=True)
(data.read_queue_records(settings, pool='audit', through=through, results=True)
    .map(lambda row: row['value'])
    .write_lance('/local/results.lance', schema=RESULT_SCHEMA, mode='overwrite'))
```

`process_or_replay` receives either a new payload plus `_queue_delivery`, or a
previous completion in `_queue_result` with no delivery. It must preserve the
delivery until `ack_queue`, and supply a bounded JSON `queue_result`. Replayed
results should bypass external operations. Do not filter out claimed rows.
Queue sources automatically use `source_batch_size=1` and reject larger values:
filling a larger source batch before emission can block a small live queue.

An acknowledgement stores the result and marks the task done in one SQLite
transaction. Lance writes happen outside that transaction. An archive failure
therefore does not erase acknowledged results. Archive an explicit sequence
boundary; save the committed Lance URI/version and sequence together in the
pipeline's control receipt. Completed messages remain available for replay and
archival; acknowledgement does not delete them. Retain the queue while any
provenance references it. This API does not automatically compact or delete it.

## Identity, recovery and budgets

The producer supplies a stable task ID and a fingerprint of the complete
semantic input. Repeating the ID/fingerprint retains the first immutable
payload, including its provenance. A changed fingerprint, pool, budget class
or cost fails. Equivalent provenance references may differ only when the
producer has established semantic equivalence. The queue cannot infer this.

Use `set_budget(class, cap)` with each message's `budget_class` and `cost` for
pre-claim admission. External calls still need their own native request journal
and finite request cap. A queue acknowledgement is not a transaction with a
remote API: a crash after remote execution can leave its outcome unknown.
Demiflow model journals preserve completed responses and uncertain attempts;
queue replay must not silently retry an uncertain paid request.

Each consumer action registers a worker, heartbeats and owns its claims. Clean
failure releases unacknowledged tasks; restart can reclaim workers whose PID
and process-start identity prove they died. An old heartbeat alone does not
steal a living worker's claim. Tasks permit three claim attempts; exhaustion
remains failed, and a sealed queue with failed or budget-blocked work cannot
report a successful drain. Producer failure does not seal the channel.

Producer and consumer failures do not automatically stop the other application.
An unsealed empty queue means temporarily idle, not end-of-stream. Seal only when
this finite producer has no further messages; consumers drain accepted tasks.
Each consumer's stop file affects only that consumer. Application launchers must
not wrap both pipelines in a joint callback or thread lifecycle.

## Resource boundaries

- Defaults: 100,000 retained tasks, 2 MiB per payload/result, 2 GiB of main SQLite
  pages. Config validation caps these at 1,000,000 tasks, 16 MiB per value and
  16 GiB of database pages. Limits are immutable for an existing channel.
- JSON preflight limits nesting to 24, a container to 16,384 elements and uses a
  conservative string estimate before serialization. It rejects oversized values;
  it does not truncate them. Input producers also need their own field/group caps.
- A publication row contains at most 257 messages. Each SQLite transaction has
  at most 32 messages and 16 MiB of serialized payload. A larger row uses multiple
  transactions; stable IDs make partial publication replayable, not atomic.
- Each SQLite connection uses a 4 MiB page-cache target and a 10-second lock
  timeout. WAL checkpointing is requested every 256 pages. Main-page limits do
  not include WAL, filesystem bookkeeping, output Lance or temporary execution
  files; reserve separate disk headroom. Disk/identity/size errors stop the action.
- Scans fetch one value at a time and close the read transaction before yielding.
  Input scans use a pool index with its implicit rowid order. Completion scans
  visit the bounded completion rowid range before looking up each task's pool;
  they do not sort all tasks in the pool for every emitted record. Opening a
  writable channel adds the input index to older databases. No archive scan
  holds a long WAL snapshot. A scan reuses one connection but closes each SELECT
  cursor before yielding, releasing that read snapshot so writers can checkpoint.
  A consumer owns at most 256 claims;
  stream queues, preparation workers and model concurrency add their own bounded
  in-memory payloads. These are component bounds, not a process RSS guarantee.
- An open producer with no progress has a configurable idle deadline (default
  1,800 seconds, maximum one day). Explicit stop and producer sealing are separate.

Business selection, batching, source interpretation, prompt and output schema
remain in the pipeline. SQLite SQL, claiming, acknowledgement and worker recovery
remain inside demiflow.

## ACK 后的消费策略

`ack_queue(..., keep_row=True)` 在原子确认成功后继续交付原行及 `acknowledged`，便于下游 Dataset 节点更新有界的业务计数。默认仍只交付确认摘要。ACK 失败不会发出成功行；恢复时应从持久结果重建计数，不能依赖未确认的内存状态。
