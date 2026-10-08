"""Read-only import of retired keyed Lance snapshots; no record writer API."""
import json
from pathlib import Path


def read_legacy_record(root, relative_uri, key, *, version=None):
    import lance
    from .storage import resolve_local_uri
    path = resolve_local_uri(Path(root) / relative_uri)
    if not path.exists():
        return None
    escaped = key.replace("'", "''")
    rows = lance.dataset(str(path), version=version).scanner(
        columns=['payload'], filter=f"key = '{escaped}'", scan_in_order=True).to_table().to_pylist()
    return json.loads(rows[-1]['payload']) if rows else None


def scan_legacy_records(root, relative_uri, *, version, prefix=None):
    import lance
    from .storage import resolve_local_uri
    path = resolve_local_uri(Path(root) / relative_uri)
    predicate = "starts_with(key, '" + prefix.replace("'", "''") + "')" if prefix else None
    for batch in lance.dataset(str(path), version=version).to_batches(
            columns=['key', 'payload', 'written_version'], filter=predicate, batch_size=128):
        for row in batch.to_pylist():
            yield row['key'], json.loads(row['payload']), row['written_version']
