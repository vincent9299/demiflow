# Streaming execution and concurrency

`Dataset.map` runs row by row, serially, by default. Give it concurrency parameters and the node runs as a stream: several rows are processed at once, nodes are connected by bounded queues, and a slow downstream makes the upstream wait.

## Quick start

```python
from demiflow import data

def prepare(row, *, scale):
    return {**row, "value": row["value"] * scale}

result = (
    data.from_items([{"value": i} for i in range(12)])
    .map(prepare, fn_kwargs={"scale": 2},
         concurrency=4, queue_depth=2, execution="thread", label="prepare")
    .materialize()
)
print(sorted(row["value"] for row in result.take_all()))
# [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]
```

Concurrent execution does not guarantee output order, which is why the example sorts before printing.

## Scheduling parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `concurrency` | `1` | The most rows this node processes at once |
| `queue_depth` | same as `concurrency` | The most rows waiting in this node's input queue |
| `execution` | `'inline'` | `'inline'` runs on the event loop; `'thread'` uses a thread pool dedicated to this node |
| `catch` | `()` | Tuple of exception types; a matching row is dropped and counted, not retried |
| `label` | function name | Node name shown in the statistics |
| `callable_scope` | `'stage'` | `'stage'` shares one callable across the node; `'worker'` gives each worker its own instance |

Choosing `execution`:

- Use `'thread'` when the function does blocking I/O (HTTP requests, file reads) or calls a library that releases the GIL.
- A synchronous `'inline'` function occupies the event loop; raising `concurrency` alone does not make it parallel.
- Pure Python CPU work does not speed up linearly with threads.
- Use `map_async` for async functions; `map` accepts synchronous functions only.

## How to run it

| Method | Behaviour |
| --- | --- |
| `run_stream()` | Runs the whole chain and returns a statistics object; result rows are not kept |
| `materialize()` | Runs and pins the result, ready for `take_all()`, `count()` or relational operations |
| `take` / `iter_rows` / `write_lance` and others | Run directly and deliver or write the result |

Relational operations such as `join`, `reduce_by_key` and `union` cannot follow a node with concurrency parameters directly; call `materialize()` first.

These streaming parameters currently work with local execution only; the Ray backend reports an error before it starts.

## Handling exceptions

An undeclared exception ends the whole stream. Use `catch` to skip bad rows instead:

```python
def parse(row):
    if row["value"] % 5 == 0:
        raise ValueError("bad row")
    return row

stats = (
    data.from_items([{"value": i} for i in range(12)])
    .map(parse, catch=(ValueError,), concurrency=2, label="parse")
    .run_stream()
)
print(stats.stages)   # {'parse': {'in': 12, 'emitted': 9}}
print(stats.miss)     # {'parse:ValueError': 3}
```

- `catch` covers only exceptions raised by the per-row call. Failures of reading the source, writing a table, constructing or cleaning up an object are not swallowed by it.
- When an exception ends the stream, the caller gets the original exception type for the common built-ins (`ValueError`, `KeyError`, `TimeoutError` and so on). A custom subclass is restored as its nearest built-in parent, so `class BadRecord(ValueError)` arrives as `ValueError` with the same message.
- `run_stream(stall_timeout=seconds)`: raises `StallError` when no node makes progress for that long.
- Threads cannot be killed. A function with blocking I/O must set its own timeout.

## Stateful processing: one instance per worker

When each worker needs its own state (a connection, a counter, a model handle), pass a **class** and set `callable_scope='worker'`:

```python
class Normalize:
    def __init__(self, offset):
        self.offset = offset

    def __call__(self, row):
        return {**row, "value": row["value"] + self.offset}

stats = (
    data.from_items([{"value": i} for i in range(100)])
    .map(Normalize, fn_constructor_args=[3], callable_scope="worker",
         concurrency=4, execution="thread", label="normalize")
    .run_stream()
)
```

- An instance is created when its first row arrives, at most `concurrency` of them; one instance never processes two rows at once.
- Optional lifecycle methods: `astart` (once before work), `astop` (after the node has finished), `aclose` (on exit). Cleanup also happens on failure and cancellation.
- Only the instance's own attributes are isolated. Shared objects passed in through constructor arguments are still shared, and their thread safety is yours to ensure.
- A worker is not bound to a fixed operating system thread; do not rely on thread affinity.
- With the default `'stage'`, the whole node shares one function or instance, and thread safety is the caller's responsibility.

## Return value rules

- Whole-row `map`: return one row, `None` (drop the row) or a list of rows (expand into several).
- With fields bound through `inputs` / `output` / `outputs`, fields not involved are kept as they are; returning `None` is then a legitimate column value and does not drop the row.

## Order and repeated execution

- An inline node with `concurrency=1` processes rows in the order received. Output order of several threads or workers is not guaranteed; logic that depends on order has to handle it.
- An ordinary `map` does not guarantee that side effects happen once: when an interrupted run is resumed, rows not yet committed may run again.
- Resuming uses `RunContext`; see "Resume where you stopped" in the README.

## Estimating memory

Concurrency and queues limit **rows**, not bytes. The amount resident in one node is roughly:

```
queue_depth × input row size + concurrency × (input row + output row + temporary objects)
```

When rows are large, lower `concurrency` and `queue_depth`, or project the fields you need first. The platform does not set a hard memory limit for arbitrary Python objects.

## Reading the statistics

The object returned by `run_stream()`:

| Attribute | Content |
| --- | --- |
| `stats.stages` | Input and output row counts of every node |
| `stats.miss` | Rows dropped by `catch`, counted per node and exception type |
| `stats.emitted` | Output row count of the last node |
| `stats.metrics['stage_policies']` | The concurrency, queue and execution mode actually in effect for every node |
| `stats.summary()` | A one-line text summary of the counts above |
