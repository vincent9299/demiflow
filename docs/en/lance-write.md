# Writing Lance tables

`Dataset.write_lance` commits a Dataset as a new version of a Lance table. The whole Dataset is committed once: either all of it takes effect or the table stays at its previous version.

## Four write modes

```python
from demiflow import data

rows = data.from_items([{"id": 1, "title": "a"}, {"id": 2, "title": "b"}])

rows.write_lance("/data/items.lance", mode="create")      # create only if the table does not exist
rows.write_lance("/data/items.lance")                     # default append: create if missing, else append
rows.write_lance("/data/items.lance", mode="overwrite")   # replace the current content with this data
```

| `mode` | Behaviour | Notes |
|---|---|---|
| `create` | Creates the table if it does not exist | Fails if the table exists; does not accept `expected_version` |
| `append` (default) | Creates or appends | An existing table requires the same schema |
| `overwrite` | Replaces the current content and schema | Old versions can still be read by version number |
| `merge` | Updates some columns of existing rows by key | See the next section |

Other parameters:

- `schema`: explicit Arrow types. With a schema, empty input can still create a table or overwrite it as empty; empty input without a schema is an error.
- `expected_version`: the commit checks that the table is still at this version and reports a concurrency conflict otherwise.
- `return_receipt=True`: returns the write receipt, which contains the version actually committed; the default return value is `None`.
- `storage_options`: access parameters for object storage.

## Updating some columns: `mode='merge'`

```python
patch = data.from_items([{"id": 1, "score": 0.9}, {"id": 2, "score": 0.4}])
patch.write_lance("/data/items.lance", mode="merge", on="id")
```

- `on` is the key column (several columns are allowed). Keys must be unique and non-null.
- By default every non-key column of the input is updated; `update_columns=[...]` names them explicitly, and input columns that do not match it are an error.
- Columns that are not in the input keep their values; an explicit null in the input clears the value.
- Keys that the target table does not have are handled by `when_not_matched`:

| `when_not_matched` | Behaviour |
|---|---|
| `error` (default) | Fails without committing |
| `ignore` | Skips those rows |
| `insert` | Inserts them as new rows; columns missing from the input are null |

- A merge cannot add fields. To add a column, use `data.add_lance_columns` first.

## What happens on failure

- Duplicate keys, null keys, mismatched columns, wrong types and missing keys (under the default policy) are reported before the commit, and the table stays at its previous version.
- When the outcome of a commit is uncertain (for example the connection drops during the commit), `LanceWriteError` is raised and carries the receipt. The platform neither retries nor deduplicates automatically; check the actual version of the table against the receipt before deciding to write again.
- Old versions are always kept and can be read back by version number.

## Merging into large tables

The platform chooses the physical write strategy; there is nothing to tune. When all of the following hold, it rewrites only the updated columns and neither reads nor rewrites the others:

- `when_not_matched` is the default `error`;
- the input updates at least one non-key column, and at least one target column is not in the input;
- the input comes straight from reading a pinned version of a Lance table (no filter, no callback) and has no more rows than the target table;
- the fields involved are not Blob or Arrow extension types.

In every other case Lance's default strategy is used, which usually rewrites whole rows when there is no index.

Limits to be aware of:

- Changing a few rows can still write a lot of data: replacement column files cover whole data fragments.
- Merging a large patch needs a lot of memory. The platform limits its memory and temporary disk use, stops with an error when a limit is exceeded and leaves the table at its previous version; it does not retry with a different strategy.
- This optimization is currently enabled only for local execution on Linux.
