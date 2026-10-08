"""Immutable run artifacts and nonblocking single-writer locks.

Encoding preserves existing artifact identities; callers own layout and policy.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import stat as stat_mode
from pathlib import Path
import uuid
from functools import lru_cache


def resolve_local_artifact(value):
    """Resolve an explicitly moved file/directory without changing saved identity.

    Exact paths only: no basename search, directory-prefix fallback or chains.
    Migrations must flatten aliases in _demiflow/artifact_locations.json.
    """
    path = Path(value).expanduser().absolute()
    for root in path.parents:
        manifest = root / '_demiflow/artifact_locations.json'
        try:
            info = manifest.stat()
            if not stat_mode.S_ISREG(info.st_mode):
                continue
            mapping = _artifact_locations(str(manifest), info.st_mtime_ns, info.st_size)
        except FileNotFoundError:
            # An optional location manifest may disappear between stat and
            # open during maintenance. Treat this like an absent manifest;
            # a missing actual artifact still fails at its consuming read.
            continue
        target = mapping.get(path.relative_to(root).as_posix())
        if target is not None:
            relative = Path(target)
            if relative.is_absolute() or '..' in relative.parts or not relative.parts:
                raise ValueError('Invalid relocated artifact path: ' + target)
            resolved = (root / relative).resolve()
            if not resolved.is_relative_to(root.resolve()):
                raise ValueError('Relocated artifact escapes storage root')
            return resolved
    return path.resolve()


@lru_cache(maxsize=16)
def _artifact_locations(filename, modified_ns, size):
    document = json.loads(Path(filename).read_text())
    if document.get('version') != 1 or not isinstance(document.get('files'), dict):
        raise ValueError('Invalid artifact location manifest: ' + filename)
    return document['files']

def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2).encode()+b'\n'
def digest(value):return hashlib.sha256(value if isinstance(value,bytes) else encoded(value)).hexdigest()
def read(path):return json.loads(resolve_local_artifact(path).read_text())

def immutable(path,value):
    path=Path(path);data=encoded(value);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if path.read_bytes()!=data:raise ValueError(f'Existing result differs; use a new run: {path}')
        return False
    # Callers hold the run lock, but map workers inside one run may target the
    # same path with identical content (duplicate image SHA across concepts).
    # Unique temp names plus a hard link keep each write atomic and idempotent.
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('xb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(tmp,path)
            directory_fd = os.open(path.parent, os.O_DIRECTORY)
            try:os.fsync(directory_fd)
            finally:os.close(directory_fd)
        except FileExistsError:
            if path.read_bytes()!=data:raise ValueError(f'Existing result differs; use a new run: {path}')
            return False
        return True
    finally:tmp.unlink(missing_ok=True)

@contextmanager
def run_lock(run):
    run=Path(run);run.mkdir(parents=True,exist_ok=True)
    with (run/'.lock').open('a') as f:
        try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Another writer holds this run') from None
        try:yield
        finally:fcntl.flock(f,fcntl.LOCK_UN)

def run_is_active(run):
    """Read the existing single-writer lock without creating a run or taking it over.

    This is an observation, not a reservation or proof of forward progress.
    A writer must still acquire run_lock: another process can start after this
    check. Absence of a holder says nothing about successful completion.
    """
    try:
        stream = (Path(run) / '.lock').open('r')
    except FileNotFoundError:
        return False
    with stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(stream, fcntl.LOCK_UN)
        return False

def snapshot(path):
    path=Path(path).resolve()
    if not path.exists():return {'path':str(path),'exists':False}
    s=path.stat()
    return {'path':str(path),'exists':True,'size':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino}


import types

def code_record(code):
    """Stable executable description; excludes interpreter interning/cache state."""
    def constant(value):
        if isinstance(value,types.CodeType):return {'code':code_record(value)}
        if isinstance(value,tuple):return {'tuple':[constant(v) for v in value]}
        if isinstance(value,frozenset):return {'frozenset':sorted((constant(v) for v in value),key=lambda v:json.dumps(v,sort_keys=True))}
        return {'type':type(value).__name__,'value':repr(value)}
    return {'bytecode':code.co_code.hex(),'constants':[constant(c) for c in code.co_consts],
            'names':list(code.co_names),'varnames':list(code.co_varnames),
            'freevars':list(code.co_freevars),'cellvars':list(code.co_cellvars),
            'argcount':code.co_argcount,'posonlyargcount':code.co_posonlyargcount,
            'kwonlyargcount':code.co_kwonlyargcount,'flags':code.co_flags,
            'stacksize':code.co_stacksize,'exceptiontable':code.co_exceptiontable.hex()}

def file_record(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": digest(path.read_bytes())}
