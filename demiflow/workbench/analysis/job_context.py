"""Fixed internal worker paths; explicit trusted mode never bypasses OCI checks."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import stat

from demiflow.workbench.analysis import contracts as c
from demiflow.forge.io import no_symlinks

FORMAT = 'demiflow_analysis_local_job_v1'
_active = None
_MOUNTS = {'/broker', '/work', '/scratch', '/sources', '/objects', '/pending', '/input', '/job',
           '/var/lib/demiflow-analysis/native-slots'}


def bounded_json(path, maximum=65536):
    fd = os.open(no_symlinks(Path(path)), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            c.reject('Worker metadata type/size differs')
        raw = stream.read(maximum+1)
    if len(raw) > maximum:
        c.reject('Worker metadata exceeds bound')
    return raw


@dataclass(frozen=True)
class JobContext:
    value: dict
    manifest_path: str | None = None
    manifest_digest: str | None = None

    @classmethod
    def load(cls, path, digest):
        c.sha(digest, 'internal context digest')
        raw = bounded_json(path)
        if hashlib.sha256(raw).hexdigest() != digest:
            c.reject('Original worker context changed', code='conflict')
        value = c.decode(raw)
        c.fields(value, {'format','mode','job_id','role','config','paths','cwd','python','code_root',
                         'controller','web_parent','origin_digest','owner_digest','deadline',
                         'original_fsize','limits','unit_fd'}, {'media_tool'}, label='fixed local job context')
        if value['format'] != FORMAT or value['mode'] != 'trusted_local':
            c.reject('Only explicit trusted worker contexts are accepted')
        from demiflow.workbench.analysis.oci import job_id
        job_id(value['job_id']); c.one_of(value['role'], {'kernel','native','media'}, 'local role')
        c.sha(value['origin_digest']); c.sha(value['owner_digest'])
        if set(value['paths']) != _MOUNTS:
            c.reject('Local job mount map differs')
        for name, target in value['paths'].items():
            target = Path(target)
            if not target.is_absolute() or '..' in target.parts or str(target) != value['paths'][name]:
                c.reject('Local worker path is not normalized')
            no_symlinks(target)
        for name in ('python','code_root','cwd'):
            path_value = Path(value[name])
            if not path_value.is_absolute() or '..' in path_value.parts:
                c.reject('Local launch path is not fixed')
            no_symlinks(path_value)
        return cls(value, str(Path(path)), digest)

    @property
    def mode(self): return self.value['mode']

    def path(self, name):
        parsed = PurePosixPath(name)
        if not parsed.is_absolute() or '..' in parsed.parts or str(parsed) != name:
            c.reject('Worker path is not canonical')
        for prefix in sorted(self.value['paths'], key=len, reverse=True):
            if name == prefix or name.startswith(prefix+'/'):
                suffix = name[len(prefix):].lstrip('/')
                return str(Path(self.value['paths'][prefix])/suffix)
        c.reject('Worker path is outside its fixed map')

    def install(self):
        global _active
        if _active is not None and _active.value != self.value:
            c.reject('Worker context cannot change in place', code='conflict')
        _active = self
        return self


def current(): return _active


def job_path(path):
    return str(path) if _active is None else _active.path(str(path))


def require_worker(role):
    if _active is None:
        from demiflow.workbench.analysis.sandbox import require_job
        return require_job(role)
    if _active.value['role'] != role:
        c.reject('Local worker role differs from its entrypoint')
    return _active.value


def worker_limit(name, default):
    return default if _active is None else _active.value['limits'].get(name, default)
