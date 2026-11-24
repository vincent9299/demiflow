"""Stable logical destinations, privately owned per-run physical namespaces.

All resource identity and allocation authority belongs to the existing RunJournal.
The one small native owner marker is not a request journal or a cross-run index.
"""

from types import SimpleNamespace

import hashlib
import uuid
import os
import stat
from pathlib import Path
from urllib.parse import urlsplit

from demiflow.execution.contracts.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity
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

        raw=owner_bytes(record)
        if context.journal.storage_usage()+len(raw)>context.limits['max_scratch_bytes']:
            raise RecoveryContractError('Object ownership metadata exceeds shared scratch budget')
        atomic_bytes(marker,raw)
        sync_directories(record,namespace)
    inspect_namespace(record)
    return namespace


def object_name(resource, path):
    return str(Path('materializations')/('@objects-'+resource['root_id'])/Path(path).relative_to(resource['physical_root']))


def verify_object(fact, *, root, resource, store, maximum):
    if type(fact) is not dict or set(fact) != {'path', 'bytes', 'sha256'}:
        raise RecoveryContractError('Invalid object descriptor')
    sha, size = fact['sha256'], fact['bytes']
    try:
        store.reference(sha)
    except (TypeError, ValueError) as exc:
        raise RecoveryContractError('Invalid object checksum') from exc
    expected = root / sha[:2] / sha
    if fact['path'] != object_name(resource, expected):
        raise RecoveryContractError('Object reference escapes its owner')
    if type(size) is not int or not 0 <= size <= maximum:
        raise RecoveryContractError('Object descriptor exceeds byte ceiling')
    # Held directory descriptors prevent parent substitution into symlinks.
    parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in expected.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        descriptor = os.open(expected.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    except OSError as exc:
        raise UnresolvedCommit('Object is missing or inaccessible') from exc
    finally:
        os.close(parent)
    with os.fdopen(descriptor, 'rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:
            raise RecoveryContractError('Object size differs')
        digest, consumed = hashlib.sha256(), 0
        while chunk := source.read(min(65536, size-consumed+1)):
            consumed += len(chunk)
            if consumed > size:
                raise RecoveryContractError('Object size changed during inspection')
            digest.update(chunk)
        after = os.fstat(source.fileno())
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise RecoveryContractError('Object changed during inspection')
    if consumed != size or digest.hexdigest() != sha:
        raise RecoveryContractError('Object checksum differs')


def stage_object(run, raw, *, root, resource, store, maximum, synced, admitted=False):
    run.check_stop()
    if type(raw) is not bytes or len(raw) > maximum:
        raise RecoveryContractError('Object exceeds its encoded byte ceiling')
    run.object_namespace(resource['logical_root'])
    sha = hashlib.sha256(raw).hexdigest()
    ref = store.reference(sha)
    target = root / sha[:2] / sha
    fact = dict(path=object_name(resource, target), bytes=len(raw), sha256=sha)
    # Do not adopt files created by another producer, even if they happen
    # to have the same content name inside this run's directory.
    with run.journal.execution_guard(run.fence) as db:
        owned = db.execute('SELECT 1 FROM runtime_artifacts WHERE path=?', (fact['path'],)).fetchone()
        if target.exists() and owned is None:
            raise RecoveryContractError('Object path is not owned by this run')
    run.reserve_output('object_bytes', len(raw), reservation_id='object-' + identity(fact),admitted=admitted)
    if target.exists():
        verify_object(fact, root=root, resource=resource, store=store, maximum=maximum)
        return ref, fact
    temporary = target.parent / ('.pending-' + uuid.uuid4().hex)
    run.allocate_artifact(target, size=len(raw), atomic=False)
    run.allocate_artifact(temporary, size=len(raw), atomic=False)
    with run.journal.publication_guard(run.fence):
        if any(p.is_symlink() for p in (target, *target.parents)):
            raise RecoveryContractError('Object path contains a symlink')
        try:
            store.put(raw, sha256=sha, temporary=temporary, max_bytes=len(raw))
        except ValueError as exc:
            raise RecoveryContractError('Object publication content differs') from exc
        verify_object(fact, root=root, resource=resource, store=store, maximum=maximum)
        sync_directories(resource,target.parent, synced=synced)
    run.release_temporaries([temporary, target.with_name(target.name + ".staging")])
    return ref, fact


def validate_image_bytes(run, raw, *, limits, decode_policy, decode):
    """Validate through bounded owned scratch; retain only the content proof.

    The caller supplies the original content decoder. This owns run files,
    their budgets, publication fencing, and the existing bounded proof cache.
    """
    import tempfile
    from collections import OrderedDict
    if len(raw)>limits['max_image_bytes']:raise ValueError('Image exceeds max_image_bytes')
    # Validation is content/policy specific. Retain only a bounded proof,
    # never decoded pixels; stage() still verifies the owned immutable blob.
    validation_key = (hashlib.sha256(raw).hexdigest(), tuple(sorted(limits.items())))
    with run._object_resource_lock:
        validated = getattr(run, '_validated_images', None)
        if validated is None:
            validated = run._validated_images = OrderedDict()
        known = validation_key in validated
        if known:
            validated.move_to_end(validation_key)
    if known:
        return
    scratch=run.directory/'scratch'
    scratch.mkdir(parents=True,exist_ok=True)
    with run.temporary_artifacts() as allocate, tempfile.TemporaryDirectory(prefix='image-pixels-',dir=scratch) as directory:
        path=Path(directory)/'encoded'
        allocate(path)
        run.reserve_artifact_bytes(path, len(raw))
        with run.journal.publication_guard(run.fence):
            run.journal.storage_usage()
            descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            with os.fdopen(descriptor,'wb') as output:output.write(raw)
        policy = decode_policy()
        try:
            def allocate_decoder(path):
                allocate(path)
                # Decoder metadata is independently bounded before the
                # child process allocates either JSON file.
                run.reserve_artifact_bytes(path, 32768)
            decode(path.as_uri(),hashlib.sha256(raw).hexdigest(),policy,
                temporary_directory=scratch,allocate=allocate_decoder)
        except OSError as exc:
            raise ValueError('Invalid image pixels: '+str(exc)) from exc
        except (MemoryError,RuntimeError) as exc:
            raise RecoveryContractError('Image decoder resource failure: '+str(exc)) from exc
    with run._object_resource_lock:
        validated[validation_key] = True
        while len(validated) > 256:
            validated.popitem(last=False)


def object_namespace(run_id, logical_root):
    """The object-root record a run with ``run_id`` owns under ``logical_root``.

    Hosts compare journal ``object_roots`` records against this instead of
    fabricating a context for ``object_resources.namespace_record``.
    """
    return namespace_record(SimpleNamespace(ref=SimpleNamespace(run_id=run_id)), logical_root)
