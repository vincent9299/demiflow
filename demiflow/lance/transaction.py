"""Single-writer local table edits with catalog publication after successful edits.

Readers pin catalog versions. A failed edit restores the previously registered
snapshot before releasing the lock, so a partial mutation is never published.
"""
from contextlib import contextmanager
from pathlib import Path
from .control import control_directory, table_lock_path
import fcntl
from .registry import Catalog
from .refs import DatasetRef
from .storage import schema_hash, resolve_local_uri


@contextmanager
def registered_table_edit(root, relative_uri, *, schema_name, schema_version):
    import lance
    import shutil
    root=Path(root).resolve();path=resolve_local_uri(root/relative_uri)
    if Path(relative_uri).is_absolute() or '..' in Path(relative_uri).parts or not path.resolve().is_relative_to(root):
        raise ValueError('Unsafe table URI')
    path.parent.mkdir(parents=True,exist_ok=True)
    with table_lock_path(path).open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        catalog=Catalog(root)
        versions=[r for r in catalog.registered() if r.resolve(root)==str(path)]
        prior=max(versions,key=lambda r:r.lance_version) if versions else None
        if path.exists() and prior is None:raise ValueError('Unregistered table requires explicit recovery')
        if prior and lance.dataset(str(path)).version!=prior.lance_version:
            raise ValueError('Table head differs from registered version; recover before editing')
        try:
            yield prior.open(root) if prior else None
            ds=lance.dataset(str(path))
            ref=DatasetRef(dataset_id=relative_uri.removesuffix('.lance'),relative_uri=relative_uri,
                lance_version=ds.version,schema_name=schema_name,schema_version=schema_version,
                schema_hash=schema_hash(ds.schema),row_count=ds.count_rows())
            catalog.register(ref)
        except BaseException:
            if prior:
                ds=lance.dataset(str(path))
                if ds.version!=prior.lance_version:
                    old=prior.open(root);old.restore()
                    restored=lance.dataset(str(path))
                    catalog.register(DatasetRef(**{**prior.to_dict(),'lance_version':restored.version}))
            elif path.exists():shutil.rmtree(path)
            raise
