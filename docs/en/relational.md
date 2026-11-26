# Relational operations: join, group, sort

A Dataset offers operations across rows: joins, grouping, sorting, deduplication. They are written like every other Dataset method and are lazy; nothing runs until a terminal action such as `take_all()`, `count()` or `write_lance()`. When the data does not fit in memory the platform spills to disk; there is no engine to choose.

## Joining: `join`

```python
from demiflow import data

users = data.from_items([{"uid": 1, "name": "Ann"}, {"uid": 2, "name": "Bo"}, {"uid": 3, "name": "Cy"}])
orders = data.from_items([{"uid": 1, "amount": 30}, {"uid": 1, "amount": 12}, {"uid": 3, "amount": 7}])

users.join(orders, on="uid").take_all()
# [{'uid': 1, 'name': 'Ann', 'amount': 30}, {'uid': 1, 'name': 'Ann', 'amount': 12},
#  {'uid': 3, 'name': 'Cy', 'amount': 7}]
```

| `how` | Result |
|---|---|
| `inner` (default) | Keys present on both sides, one row per matching pair |
| `left` | Every left row; rows without a match carry no right fields |
| `semi` | Left rows that have a match, left fields only, no extra rows |
| `anti` | Left rows that have no match |

```python
users.join(orders, on="uid", how="anti").take_all()
# [{'uid': 2, 'name': 'Bo'}]
```

- Use `right_on` when the key has a different name on the right: `users.join(payments, on="uid", right_on="user")`.
- When both sides have a non-key field of the same name, the right one gets `suffix` (default `_right`).
- Rows with a null key do not match. Duplicate keys produce every pair.
- Output order is not guaranteed; `sort` explicitly when order matters.
- Keys are compared by type: the integer `1`, the float `1.0` and the string `"1"` are different keys.

## Grouped computation: `reduce_by_key`

Feeds the rows of each key, one after another, to a function that accumulates:

```python
totals = orders.reduce_by_key(
    "uid",
    lambda state, row: {"uid": row["uid"], "total": (state or {}).get("total", 0) + row["amount"]},
)
totals.take_all()
# [{'uid': 1, 'total': 42}, {'uid': 3, 'total': 7}]
```

- The signature is `reducer(state, row) -> state`. On the first call of a group `state` is `initial` (default `None`).
- The final `state` of each group becomes one output row; `None` produces no row.
- Calls within a group follow the original order. The platform does not assume the function is commutative or associative and does not split it for parallel execution.
- The size of the accumulated state is up to your function; the platform does not limit it.

## Batching by key: `group_batches`

Collects the rows of each key into lists of at most `max_rows`, which suits "send each group to a model together":

```python
orders.group_batches("uid", max_rows=10, output="items").take_all()
# [{'uid': 1, 'items': [{'uid': 1, 'amount': 30}, {'uid': 1, 'amount': 12}], 'group_index': 0, 'group_last': True},
#  {'uid': 3, 'items': [{'uid': 3, 'amount': 7}], 'group_index': 0, 'group_last': True}]
```

A group larger than `max_rows` is split into several batches; `group_index` numbers them and `group_last` marks the last batch of the group.

In a streaming chain you can pass `flush_interval=seconds`: batches are emitted by time or buffer pressure without waiting for all data; the last batch of a group is then unknown, so `group_last` is not provided.

## Whole-dataset aggregation: `aggregate`

```python
orders.aggregate(data.Count(), data.Sum("amount"), data.Mean("amount"))
# {'count()': 3, 'sum(amount)': 49, 'mean(amount)': 16.333333333333332}
```

Available aggregations: `Count`, `Sum`, `Min`, `Max`, `Mean`, `Std`, `AbsMax`. `aggregate` is a terminal action and returns the result directly.

## Sorting, concatenating, limiting

```python
orders.sort("amount", descending=True)        # global sort; pass a list for several columns
users.union(more_users)                       # concatenation, no schema coercion
orders.limit(100)                             # first 100 rows only; the upstream stops promptly
orders.random_shuffle(seed=7)                 # global random shuffle
```

## Excluding and looking up by key

**Excluding keys already handled**: removes from a Lance table the keys that appear in another Dataset and keeps the original scan order. Only keys take part in the computation; large fields such as bodies never enter the join, which suits "skip what is done" on big tables.

```python
done = data.from_items([{"uid": 0}, {"uid": 3}])
todo = data.read_lance("/data/users.lance").exclude_keys(done, on="uid")
```

`exclude_keys` must follow `read_lance` directly; put other transformations after it.

**Row-by-row lookup**: attaches to every row the rows with the same key from a pinned version of a Lance table, without buffering the input stream. It suits adding a few fields in a streaming chain.

```python
enriched = orders.lookup_lance(
    {"uri": "/data/users.lance", "version": 1},
    on="uid", columns=["name"], output="user",
).materialize()
# [{'uid': 1, 'amount': 30, 'user': [{'name': 'Ann'}]}, ...]
```

- The table must be given as `{"uri": ..., "version": ...}` with a fixed version.
- `max_matches` (default 1, at most 64): more matching rows than this is an error, never a truncation.
- `max_bytes` (default 1 MiB): size limit of the result attached to one row.

## Together with streaming nodes

Joins and grouping need complete inputs. When the upstream has a `map` with concurrency parameters, a `map_async` or a model call, `materialize()` first and then apply the relational operation:

```python
scored = rows.map_async(score, concurrency=8).materialize()
scored.join(labels, on="id")
```

## Resources

The platform puts memory, thread and temporary disk limits on every relational query and scales them with the container; when a limit is exceeded the query fails with an error instead of quietly switching algorithms. To tighten the budget:

```python
with data.local_execution(native_resources={"memory_bytes": 2 * 1024**3, "threads": 2}):
    result = users.join(orders, on="uid").take_all()
```

Settable items: `memory_bytes`, `max_rss_bytes`, `max_scratch_bytes`, `max_row_bytes`, `threads`, `partitions`, `batch_rows`, `timeout_s`, `admission_timeout_s`. They can only be lowered, never raised above the platform defaults.

Limits to be aware of:

- These limits are safeguards, not a hard guarantee on the memory of the whole process; data with very large single rows can still exceed the budget and fail.
- A query that fails, times out or is cancelled does not rerun your functions and does not get more memory automatically.

### When memory cannot hold maximum-size rows

The platform first sizes `join` and `reduce_by_key` as if every row were as large as the row limit `max_row_bytes`: about one batch plus 6 rows for a join, and about 12 rows plus two copies of the state limit `max_state_bytes` for a reduce. When `local_execution(memory_bytes=...)` is too small for that:

- If the row and batch limits (and, for a reduce, the state limit) are defaults, the join and reduce run by actual size: they first get one batch plus one row, the remaining memory is given back to them as far as it goes, and each row and group state is counted by its actual bytes. Ordinary rows run as usual; a row or group state that does not fit the memory the step received fails during the run with `BUDGET_EXCEEDED`, naming the memory it received and what maximum-size rows would need.
- Such a plan logs a warning when it starts, naming the step, the memory it received and roughly the largest row it is sure to hold. `stats['measured_memory']` of `local_execution` and `stats.metrics['measured_memory']` of `run_stream()` carry the same facts.
- If these limits are set explicitly in the run limits (`RunContext.start(limits=...)`), the platform still checks maximum-size rows and refuses the plan before it starts, without calling any of your functions. To know before spending anything that a run will finish, set `max_row_bytes` and `max_batch_bytes` (plus `max_state_bytes` for a reduce) explicitly.
- Plans that fit maximum-size rows are not affected.
