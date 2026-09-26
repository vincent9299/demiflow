"""Immutable run artifacts and nonblocking single-writer locks.

Encoding preserves existing artifact identities; callers own layout and policy.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import uuid

def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2).encode()+b'\n'
def digest(value):return hashlib.sha256(value if isinstance(value,bytes) else encoded(value)).hexdigest()
def read(path):return json.loads(Path(path).read_text())

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
        tmp.write_bytes(data)
        try:os.link(tmp,path)
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
