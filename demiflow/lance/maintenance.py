"""Explicit local-table retirement with release protection and durable ordinary JSON audit files.

Callers own retention policy and must establish that selected tables are inactive.
The platform validates exact paths, blocks surviving publication references, and
records removed registrations before changing the live catalog or deleting bytes.
"""
from contextlib import ExitStack
from pathlib import Path
from .control import control_directory, table_lock_path
import fcntl
import json
import re
import shutil

from ..execution.artifacts import immutable, read
from .registry import Catalog, ReleaseRegistry
from .storage import resolve_local_uri
from .legacy import read_legacy_record


def retire_tables(root, *, table_uris, release_ids=(), operation_id, reason):
    import lance
    import pyarrow as pa
    root = Path(root).resolve()
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', operation_id) or not reason:
        raise ValueError('Explicit operation ID and reason required')
    uris = sorted(set(table_uris))
    ids = sorted(set(release_ids))
    for uri in uris:
        p = Path(uri)
        if (p.is_absolute() or '..' in p.parts or p.suffix != '.lance'
                or p.parts[0] == 'registry'
                or resolve_local_uri(root/p) in {Path(Catalog(root).uri), Path(ReleaseRegistry(root).uri)} or not (root/p).resolve().is_relative_to(root)):
            raise ValueError('Unsafe retirement URI: ' + uri)
    journal = root / '_demiflow' / 'maintenance' / operation_id
    journal.mkdir(parents=True, exist_ok=True)
    spec = {'table_uris': uris, 'release_ids': ids, 'reason': reason}
    catalog, releases = Catalog(root), ReleaseRegistry(root)
    with ExitStack() as locks:
        # Same locks as ordinary registry writers; do not race a live publisher.
        for uri in (catalog.uri, releases.uri):
            p = table_lock_path(uri); p.parent.mkdir(parents=True, exist_ok=True)
            lock = locks.enter_context(p.open('a'))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        rows, published = catalog.rows(), releases.rows()
        kept_releases = [row for row in published if row['release_id'] not in ids]
        for row in kept_releases:
            serialized = json.dumps(row, ensure_ascii=False)
            if any(uri in serialized for uri in uris) or row.get('previous_release_id') in ids:
                raise ValueError('Surviving release protects retirement input: ' + row['release_id'])
        removed = [row for row in rows if row['relative_uri'] in uris]
        prior = (read(journal / 'plan.json') if (journal / 'plan.json').exists() else None)
        if prior is None:
            prior = read_legacy_record(root, f'datasets/records__{operation_id}.lance', 'plan')
            if prior is not None:
                immutable(journal / 'plan.json', prior)
        if prior is not None:
            if prior['selection'] != spec:
                raise ValueError('Retirement operation selection changed')
            if any(row not in prior['catalog_rows'] for row in removed):
                raise ValueError('Selected table was registered again after retirement started')
        else:
            if set(uris) - {row['relative_uri'] for row in removed}:
                raise ValueError('Every table must be registered before retirement')
            if set(ids) - {row['release_id'] for row in published}:
                raise ValueError('Unknown release selected for retirement')
            immutable(journal / 'plan.json', {'selection': spec, 'catalog_rows': removed,
                                'release_rows': [row for row in published if row['release_id'] in ids]})
        complete = (read(journal / 'complete.json') if (journal / 'complete.json').exists() else None)
        if complete is None:
            complete = read_legacy_record(root, f'datasets/records__{operation_id}.lance', 'complete')
            if complete is not None:
                immutable(journal / 'complete.json', complete)
        if complete is not None and not removed and not any(row['release_id'] in ids for row in published):
            if any(resolve_local_uri(root/uri).exists() for uri in uris):
                raise ValueError('Retired table path exists again')
            return complete
        for uri in uris:
            path = resolve_local_uri(root/uri)
            lock = locks.enter_context(table_lock_path(path).open('a'))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Retire live publication visibility first, then remove its table registrations.
        if len(kept_releases) != len(published):
            ds = lance.dataset(releases.uri)
            lance.write_dataset(pa.Table.from_pylist(kept_releases, schema=ds.schema), releases.uri, mode='overwrite')
        kept = [row for row in rows if row['relative_uri'] not in uris]
        if len(kept) != len(rows):
            ds = lance.dataset(catalog.uri)
            lance.write_dataset(pa.Table.from_pylist(kept, schema=ds.schema), catalog.uri, mode='overwrite')
        immutable(journal / 'registrations_retired.json', True)
        for uri in uris:
            path = resolve_local_uri(root/uri)
            if path.exists(): shutil.rmtree(path)
            (control_directory(path)/'checkpoint.json').unlink(missing_ok=True)
        immutable(journal / 'complete.json', {'tables': len(uris), 'releases': len(ids)})
    # Release locks before deleting their idle files.
    for uri in uris:
        directory = control_directory(resolve_local_uri(root/uri))
        (directory/'write.lock').unlink(missing_ok=True)
        if directory.exists() and not any(directory.iterdir()): directory.rmdir()
        if directory.parent.exists() and not any(directory.parent.iterdir()): directory.parent.rmdir()
    return (read(journal / 'complete.json') if (journal / 'complete.json').exists() else None)
