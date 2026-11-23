"""Stable logical destinations, privately owned per-run physical namespaces.

All resource identity and allocation authority belongs to the existing RunJournal.
The one small native owner marker is not a request journal or a cross-run index.
"""
import os
import stat
from pathlib import Path
from urllib.parse import urlsplit

from demiflow.execution.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity
from demiflow.execution.storage.state_artifacts import atomic_bytes


def namespace_record(context, logical_root):
    from demiflow.objects import _local_path
    value=str(logical_root)
    if value.startswith('file:'):
        logical=_local_path(value)
    elif urlsplit(value).scheme:
        raise RecoveryContractError('Object resource requires a local logical destination')
    else:
        logical=Path(value).absolute()
    if '..' in logical.parts or len(str(logical).encode())>4096:
        raise RecoveryContractError('Invalid logical object destination')
    return dict(root_id=identity({'logical_root':str(logical)}), logical_root=str(logical),
        physical_root=str(logical/'.demiflow-objects'/context.ref.run_id),run_id=context.ref.run_id)


def owner_bytes(record):
    return document({'format':1,'root_id':record['root_id'],'run_id':record['run_id']})


def sync_directories(record, directory, *, synced=None):
    """Persist newly created namespace ancestors as well as the file shard."""
    directory=Path(directory)
    logical=Path(record['logical_root'])
    if not directory.is_relative_to(logical):
        raise RecoveryContractError('Object directory escapes its logical destination')
    pending = []
    while True:
        info = directory.stat()
        key = (str(directory), info.st_dev, info.st_ino)
        if synced is not None and key in synced and not pending:
            return  # Its contents were synced by the native object writer.
        pending.append((directory, key))
        if directory == logical.parent or synced is not None and key in synced:
            break
        directory = directory.parent
    for directory, _ in pending:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    if synced is not None:
        for _, key in pending:
            synced[key] = True
        while len(synced) > 512:
            synced.popitem(last=False)


def inspect_namespace(record):
    namespace=Path(record['physical_root'])
    marker=namespace/'.owner'
    if any(p.is_symlink() for p in (marker,*marker.parents)):
        raise UnresolvedCommit('Object namespace contains a symlink')
    try:
        descriptor=os.open(marker,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(descriptor,'rb') as stream:
            info=os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size>1024:
                raise UnresolvedCommit('Object namespace owner is not bounded metadata')
            raw=stream.read(1025)
    except OSError as exc:
        raise UnresolvedCommit('Object namespace owner is missing or inaccessible') from exc
    if raw!=owner_bytes(record):
        raise UnresolvedCommit('Object namespace owner differs')


def bind_namespace(context, logical_root):
    context.check_stop()
    record=namespace_record(context,logical_root)
    namespace=Path(record['physical_root'])
    if any(p.is_symlink() for p in (namespace,*namespace.parents)):
        raise RecoveryContractError('Object destination contains a symlink')
    for existing in context.journal.object_roots():
        other=Path(existing['physical_root'])
        if existing['root_id']!=record['root_id'] and (namespace.is_relative_to(other) or other.is_relative_to(namespace)):
            raise RecoveryContractError('Object namespaces must not overlap')
    new=context.journal.register_object_root(context.fence,record)
    marker=namespace/'.owner'
    if marker.exists():
        inspect_namespace(record)
        return namespace
    context.reserve('object_namespace_metadata_bytes',len(owner_bytes(record)),
        reservation_id=record['root_id'],ceiling=context.limits['max_control_bytes'])
    with context.journal.publication_guard(context.fence):
        if new:
            try:namespace.mkdir(parents=True,exist_ok=False)
            except FileExistsError as exc:
                raise UnresolvedCommit('Object namespace appeared during ownership claim') from exc
        else:
            namespace.mkdir(parents=True,exist_ok=True)
            # A failed owner publication can only leave its registered native
            # staging name here: object allocation starts after the owner exists.
            with os.scandir(namespace) as entries:
                for entry in entries:
                    if entry.name!='.owner.staging':
                        raise UnresolvedCommit('Unclaimed object namespace contains foreign data')
            pending=namespace/'.owner.staging'
            if pending.is_symlink():
                raise UnresolvedCommit('Object owner staging contains a symlink')
            pending.unlink(missing_ok=True)
        from demiflow.execution.outputs.block_sink import storage_usage
        raw=owner_bytes(record)
        if storage_usage(context)+len(raw)>context.limits['max_scratch_bytes']:
            raise RecoveryContractError('Object ownership metadata exceeds shared scratch budget')
        atomic_bytes(marker,raw)
        sync_directories(record,namespace)
    inspect_namespace(record)
    return namespace
