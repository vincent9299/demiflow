"""Bounded source previews supplement full-byte hashes, never truncate results."""
import difflib
import hashlib
import os
from pathlib import Path
import stat
import time

from demiflow.workbench.conversations.common import plain_path
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import safe_path

TEXT_SUFFIXES = {'.py', '.md', '.json', '.yaml', '.yml', '.toml', '.js', '.css', '.html', '.txt'}
IGNORED_DIRS = {'__pycache__', '.pytest_cache', '.mypy_cache'}
IGNORED_SUFFIXES = {'.pyc', '.pyo'}


def _ignored(rel):
    parts = rel.split('/')
    return any(part in IGNORED_DIRS for part in parts) or Path(parts[-1]).suffix in IGNORED_SUFFIXES


def capture(root, paths, *, exclude=(), max_files=4096, max_bytes=64 * 1024**2):
    """Hash exact watched bytes and collect bounded text previews in a single read pass."""
    facts, total, visited = {}, 0, 0
    remaining_preview = 128 * 1024
    deadline = time.monotonic() + 10
    pending = [plain_path(p) for p in paths]
    excluded = set(exclude)
    while pending:
        rel = pending.pop()
        if rel in facts or rel in excluded or _ignored(rel):
            continue
        visited += 1
        if visited > max_files or time.monotonic() > deadline:
            reject('Source snapshot count/time budget exceeded', 'resource_limit')
        path = safe_path(root, rel, missing=True)
        if not path.exists():
            facts[rel] = None
        elif path.is_dir():
            with os.scandir(path) as entries:
                for entry in entries:
                    child = rel + '/' + entry.name
                    if child in excluded or _ignored(child):
                        continue
                    if len(pending) + visited >= max_files:
                        reject('Source snapshot entry budget exceeded', 'resource_limit')
                    pending.append(child)
        else:
            size = path.stat().st_size
            if total + size > max_bytes:
                reject('Source snapshot byte budget exceeded', 'resource_limit')
            want_preview = Path(rel).suffix in TEXT_SUFFIXES and size <= 16384 and size <= remaining_preview
            hashed = hashlib.sha256()
            buf = bytearray() if want_preview else None
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as stream:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                    reject('Source replaced during snapshot', 'conflict')
                left = size
                while left:
                    chunk = stream.read(min(65536, left))
                    if not chunk:
                        reject('Source truncated during snapshot', 'conflict')
                    hashed.update(chunk)
                    if buf is not None:
                        buf.extend(chunk)
                    left -= len(chunk)
                after = os.fstat(fd)
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    reject('Source changed during snapshot', 'conflict')
            total += size
            fact = dict(bytes=size, sha256=hashed.hexdigest())
            if Path(rel).suffix not in TEXT_SUFFIXES:
                fact['preview_unavailable'] = 'not_plain_source'
            elif not want_preview:
                fact['preview_unavailable'] = 'preview_byte_budget'
            else:
                try:
                    value = bytes(buf).decode('utf-8')
                    if len(value.splitlines()) > 512:
                        fact['preview_unavailable'] = 'preview_line_budget'
                    else:
                        fact['preview'] = value; remaining_preview -= len(buf)
                except UnicodeError:
                    fact['preview_unavailable'] = 'not_utf8'
            facts[rel] = fact
    return facts


def differences(before, after, writes):
    for name in sorted(before.keys() | after.keys()):
        old, new = before.get(name), after.get(name)
        if (old or {}).get('sha256') == (new or {}).get('sha256'): continue
        value = dict(path=name, before={k:v for k,v in (old or {}).items() if k!='preview'} if old else None,
            after={k:v for k,v in (new or {}).items() if k!='preview'} if new else None,
            within_scope=any(name==w or name.startswith(w+'/') for w in writes))
        if (old is None or 'preview' in old) and (new is None or 'preview' in new):
            # <=512 lines each side and <=32 KiB combined source input.
            chunks = list(difflib.unified_diff((old or {}).get('preview','').splitlines(keepends=True),
                          (new or {}).get('preview','').splitlines(keepends=True),fromfile='before/'+name,tofile='after/'+name))
            diff=''.join(chunks)
            if len(diff.encode())<=32768:value['diff']=diff
            else:value['diff_unavailable']='diff_byte_budget'
        else:value['diff_unavailable']='source_preview_unavailable; full hashes retained'
        yield value
