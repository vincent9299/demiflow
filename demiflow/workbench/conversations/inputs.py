"""Task-bound, read-only discovery of ordinary inputs outside the code grant.

The controller exposes fixed filesystem operations, never model-supplied code.
Native table decoders run in the existing readonly OS boundary. These reads do
not register a pipeline, change output paths, or provide execution permission.
"""
from __future__ import annotations

from contextlib import contextmanager
import fnmatch
import json
import os
from pathlib import Path
import select
import stat
import sys
import tempfile
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.common import integer, reject, wire
from demiflow.workbench.conversations.policy import turn_deadline


_PRIVATE = {'.ssh', '.codex', '.codex-home', '.config', '.aws', '.azure', '.kube',
            '.git', '.gnupg', '.netrc', '.npmrc', '.pypirc', '.git-credentials',
            '.env', 'access.token', 'auth.json', 'credentials.json'}
_SYSTEM = tuple(Path(p) for p in ('/proc', '/sys', '/dev', '/run', '/etc/shadow',
    '/etc/gshadow', '/etc/ssh', '/etc/ssl/private', '/etc/sudoers', '/etc/sudoers.d'))
_RESULT_BYTES = 48000
_READ_BYTES = 32768
_SCAN_ENTRIES = 2048
_MAX_FIELDS = 64
_INSPECTION_SECONDS = 7


def _within(path, root):
    return path == root or root in path.parents


def _private(path):
    # .forge/runs can contain ordinary pipeline output, so it is intentionally
    # not a generic secret directory. Actual authorities are passed by host.
    return any(p in _PRIVATE or p.startswith('.env.') or
        (p.startswith('demiflow-') and '-preview-' in p) for p in path.parts)


def _facts(info):
    return dict(device=info.st_dev, inode=info.st_ino, bytes=info.st_size,
                modified_ns=info.st_mtime_ns, changed_ns=info.st_ctime_ns)


@contextmanager
def _open_physical(path):
    """Keep ancestors pinned while opening; never follow an alias or a device."""
    parent = os.open('/', os.O_PATH | os.O_DIRECTORY)
    target = None
    try:
        parts = path.parts[1:]
        for part in parts[:-1]:
            child = os.open(part, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=parent)
            os.close(parent)
            parent = child
        target = (os.open('/', os.O_RDONLY | os.O_DIRECTORY) if not parts else os.open(parts[-1],
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent))
        info = os.fstat(target)
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            reject('Input must be an ordinary file or directory', 'operation_rejected')
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            reject('Hard-linked inputs are not covered by readonly inspection', 'operation_rejected')
        yield target, info
    finally:
        if target is not None:
            os.close(target)
        os.close(parent)


class InputAccess:
    """Host-created bridge capability, bound to one active turn and epoch.

    ``denied_paths`` must include every controller authority/credential root,
    deployment configuration and the Web service's private token directory.
    Runtime paths must be trusted installed code only, not a workspace parent.
    The model cannot change either set. ``deadline`` is Unix wall-clock time.
    """

    def __init__(self, store, tid, project, scratch, *, denied_paths,
                 runtime_paths, deadline=None):
        self.store, self.tid = store, tid
        self.root = Path(project['root']).absolute()
        self.project = project
        self.scratch = Path(scratch)
        self.denied = tuple(dict.fromkeys((*_SYSTEM,
            *(q for p in denied_paths for q in (Path(p).absolute(), Path(p).resolve())))))
        self.runtime = tuple(str(p) for p in runtime_paths)
        turn = store.turn(tid)
        self.epoch = turn['epoch']
        self.deadline = deadline if deadline is not None else turn_deadline(turn, store.limits)

    def require_active(self):
        turn = self.store.turn(self.tid)
        if (turn['epoch'] != self.epoch or turn.get('stop_at') or
                turn['cleanup'] == 'complete' or time.time() >= self.deadline):
            reject('Input access belongs to an inactive development turn', 'conflict')

    def _allowed(self, path):
        return not _private(path) and not any(_within(path, denied) for denied in self.denied)

    def _path(self, value):
        if value is None:
            path = self.root.parent
        else:
            if type(value) is not str or not value or len(value.encode()) > 4096 or '\x00' in value:
                reject('Input path must be a bounded local path', 'invalid_request')
            path = Path(value)
            if not path.is_absolute():
                path = self.root / path
            # Parent navigation is useful for discovery; normalize lexically,
            # while actual opening still refuses every symbolic-link component.
            path = Path(os.path.normpath(path))
        if len(path.parts) > 64 or not self._allowed(path):
            reject('This path belongs to a protected host control area', 'operation_rejected')
        return path

    def invoke(self, arguments):
        self.require_active()
        if type(arguments) is not dict:
            reject('Input arguments must be an object', 'invalid_request')
        action = arguments.get('action', 'list')
        fields = {'list': {'action', 'path', 'limit', 'offset'},
            'find': {'action', 'path', 'pattern', 'depth', 'limit'},
            'read': {'action', 'path', 'offset', 'bytes'},
            'inspect': {'action', 'path', 'columns', 'limit'}}
        if type(action) is not str or action not in fields or set(arguments) - fields[action]:
            reject('Use input action list, find, read or inspect with its fixed arguments', 'invalid_request')
        path = self._path(arguments.get('path'))
        try:
            with _open_physical(path) as (fd, info):
                source = dict(path=str(path), **_facts(info))
                if action in {'list', 'find'}:
                    if not stat.S_ISDIR(info.st_mode):
                        reject('Directory discovery requires a directory', 'invalid_request')
                    result = self._discover(path, fd, arguments, recursive=action == 'find')
                elif action == 'read':
                    result = self._read(fd, info, arguments)
                else:
                    result = self._inspect(path, info, arguments)
                if _facts(os.fstat(fd)) != _facts(info):
                    reject('Input changed during inspection; retry the current path', 'conflict')
                self.require_active()
                result.update(action=action, source=source, readonly=True)
                wire(result, _RESULT_BYTES)
                return result
        except FileNotFoundError as exc:
            raise ForgeError('Input path does not exist', code='not_found') from exc
        except UnicodeError as exc:
            raise ForgeError('Text input is not valid UTF-8 at this byte offset; '
                             'use inspect for Lance/Parquet tables', code='operation_rejected') from exc
        except OSError as exc:
            # Do not expose exception messages from a native parser or host
            # environment that might contain unrelated control paths/values.
            raise ForgeError('Input cannot be read as an ordinary physical local path',
                             code='operation_rejected') from exc

    def _discover(self, root, root_fd, arguments, *, recursive):
        limit = integer(arguments.get('limit', 60), 1, 128)
        offset = integer(arguments.get('offset', 0), 0, _SCAN_ENTRIES - 1)
        depth = integer(arguments.get('depth', 2 if recursive else 0), 0, 4)
        pattern = arguments.get('pattern', '*')
        if type(pattern) is not str or len(pattern.encode()) > 256 or '/' in pattern:
            reject('Find pattern must be a bounded filename glob', 'invalid_request')
        deadline = min(self.deadline, time.time() + 3)
        results, pending, scanned, skipped, truncated = [], [(root, 0)], 0, 0, False
        while pending:
            current, level = pending.pop()
            self.require_active()
            try:
                with _open_physical(current) as (fd, info):
                    if current == root and _facts(info) != _facts(os.fstat(root_fd)):
                        reject('Discovery directory changed', 'conflict')
                    with os.scandir(fd) as entries:
                        for entry in entries:
                            if scanned >= _SCAN_ENTRIES or time.time() >= deadline:
                                truncated = True
                                break
                            scanned += 1
                            path = current / entry.name
                            if not self._allowed(path):
                                continue
                            try:
                                item = entry.stat(follow_symlinks=False)
                            except OSError:
                                continue
                            directory = stat.S_ISDIR(item.st_mode)
                            if not (directory or stat.S_ISREG(item.st_mode) and item.st_nlink == 1):
                                continue
                            if recursive and directory and level < depth and path.suffix != '.lance':
                                pending.append((path, level + 1))
                            if recursive and not fnmatch.fnmatchcase(entry.name, pattern):
                                continue
                            if skipped < offset:
                                skipped += 1
                                continue
                            if len(results) >= limit:
                                truncated = True
                                break
                            results.append(dict(path=str(path), kind='directory' if directory else 'file',
                                                bytes=None if directory else item.st_size))
                            # Wide filenames should stop a page before transport
                            # truncates its JSON into a misleading successful read.
                            if len(wire(results)) > 30000:
                                results.pop()
                                truncated = True
                                break
            except (OSError, ForgeError):
                if current == root:
                    raise
                continue
            if truncated:
                break
        return dict(entries=results, scanned=scanned, truncated=truncated,
            next_offset=(offset + len(results)) if truncated and not recursive and scanned < _SCAN_ENTRIES else None,
            scope=dict(path=str(root), max_depth=depth, max_entries=_SCAN_ENTRIES),
            note='Discovery omits protected paths and aliases; narrow the path/pattern when truncated. Directory order may change.')

    def _read(self, fd, info, arguments):
        if not stat.S_ISREG(info.st_mode):
            reject('Text reading requires a regular file', 'invalid_request')
        offset = integer(arguments.get('offset', 0), 0, 2**63 - 1)
        maximum = integer(arguments.get('bytes', 8192), 1, _READ_BYTES)
        raw = os.pread(fd, maximum + 4, offset)
        skip = 0
        if offset > 0:
            while skip < min(3, len(raw)) and (raw[skip] & 0xC0) == 0x80:
                skip += 1
        actual_offset = offset + skip
        window = raw[skip:]
        visible = window[:maximum]
        # Omitted trailing UTF-8 continuation bytes belong to the next page.
        import codecs
        decoder = codecs.getincrementaldecoder('utf-8')('strict')
        content = decoder.decode(visible, final=len(window) <= maximum)
        pending, _ = decoder.getstate()
        consumed = len(visible) - len(pending)
        if visible and not consumed:
            reject('Text byte budget is too small for one UTF-8 character', 'invalid_request')
        if '\x00' in content:
            reject('Binary input requires table inspection or another supported reader', 'operation_rejected')
        return dict(text=content, offset=actual_offset, bytes=consumed,
            truncated=actual_offset + consumed < info.st_size,
            next_offset=actual_offset + consumed if actual_offset + consumed < info.st_size else None)

    def _inspect(self, path, info, arguments):
        limit = integer(arguments.get('limit', 5), 0, 10)
        columns = arguments.get('columns')
        if columns is not None and (type(columns) is not list or len(columns) > 16 or
                any(type(c) is not str or not c or len(c.encode()) > 256 for c in columns) or
                len(set(columns)) != len(columns)):
            reject('Inspect columns must be up to 16 column names', 'invalid_request')
        format_ = 'lance' if stat.S_ISDIR(info.st_mode) and path.suffix == '.lance' else (
            'parquet' if stat.S_ISREG(info.st_mode) and path.suffix.lower() in {'.parquet', '.pq'} else None)
        if format_ is None:
            reject('Inspect currently supports physical Lance directories and Parquet files; use read for text', 'operation_rejected')
        if any(_within(denied, path) for denied in self.denied):
            reject('A native input cannot contain a protected control area', 'operation_rejected')
        request = dict(path=str(path), format=format_, limit=limit, columns=columns)
        return self._native(request)

    def _native(self, request):
        from .sandbox import SandboxSpec, launch_sandbox
        deadline = min(self.deadline, time.time() + _INSPECTION_SECONDS)
        limits = self.store.limits
        # Controller state is wholly denied. Temporary decoder caches therefore
        # use a separate disposable private directory, never the input tree.
        with tempfile.TemporaryDirectory(prefix='demiflow-input-') as private:
            spec = SandboxSpec(read_paths=(request['path'],), write_paths=(),
                runtime_paths=self.runtime, protected_paths=(request['path'],),
                denied_paths=tuple(str(p) for p in self.denied), private_home=private,
                role='readonly', seconds=max(.01, deadline - time.time()),
                rss_bytes=min(768 * 1024**2, limits.rss_bytes),
                processes=min(24, limits.processes), file_bytes=8 * 1024**2,
                private_bytes=8 * 1024**2)
            env = dict(PATH='/usr/bin:/bin', LANG='C.UTF-8', HOME=private, TMPDIR=private,
                       OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
                       NUMEXPR_NUM_THREADS='1', RAYON_NUM_THREADS='2', TOKIO_WORKER_THREADS='2',
                       LANCE_CPU_THREADS='2', CUDA_VISIBLE_DEVICES='')
            process = launch_sandbox([sys.executable, '-I', '-B', str(Path(__file__).resolve())],
                                    spec=spec, env=env, cwd=private)
            streams = (process.stdin, process.stdout, process.stderr)
            output, errors = bytearray(), bytearray()
            try:
                process.stdin.write(wire(request, 8192))
                process.stdin.close()
                pipes = {process.stdout: output, process.stderr: errors}
                while pipes:
                    self.require_active()
                    if time.time() >= deadline:
                        reject('Input table inspection exceeded its time budget', 'resource_limit')
                    ready, _, _ = select.select(list(pipes), [], [], .1)
                    for stream in ready:
                        chunk = os.read(stream.fileno(), 4096)
                        if not chunk:
                            del pipes[stream]
                            continue
                        pipes[stream].extend(chunk)
                        if len(output) + len(errors) > _RESULT_BYTES:
                            reject('Input table inspection exceeded its response budget', 'resource_limit')
                process.wait(timeout=max(.01, deadline - time.time()))
                if process.returncode:
                    reject('Input table cannot be inspected within the readonly format/resource boundary; '
                           'try limit=0 for schema only or select fewer columns', 'operation_rejected')
                result = json.loads(output)
                if type(result) is not dict:
                    reject('Invalid table inspection response', 'operation_rejected')
                return result
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
                for stream in streams:
                    stream.close()


def _inspect_table(request):
    """Fixed child operation. No SQL, expressions, Python, plugins or imports from inputs."""
    import pyarrow as pa
    from demiflow.lance.preview import preview_value, sample_open_table
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    path = Path(request['path'])
    if request['format'] == 'lance':
        # Physical input locations must not consult workspace relocation indices
        # outside the selected table. Reuse the existing projected row reader.
        import lance
        # Validate metadata aliases before giving the native reader any bytes.
        # Only this one metadata directory is enumerated, with a finite cap;
        # no table content or historical data fragments are scanned/hashed.
        with _open_physical(path / '_versions') as (versions_fd, _):
            with os.scandir(versions_fd) as entries:
                for index, entry in enumerate(entries):
                    if index >= 4096:
                        raise ValueError('Manifest directory exceeds input inspection budget')
                    if entry.name.endswith('.manifest'):
                        with _open_physical(path / '_versions' / entry.name) as (_, info):
                            if not stat.S_ISREG(info.st_mode) or info.st_size > 16 * 1024**2:
                                raise ValueError('Invalid or oversized input manifest')
        latest = path / '_latest.manifest'
        if latest.exists() or latest.is_symlink():
            with _open_physical(latest) as (_, info):
                if not stat.S_ISREG(info.st_mode) or info.st_size > 16 * 1024**2:
                    raise ValueError('Invalid or oversized latest manifest')
        dataset = lance.dataset(str(path), index_cache_size_bytes=0,
                                metadata_cache_size_bytes=1024**2)
        version = dataset.version
        for name in (f'{version}.manifest', f'{2**64 - 1 - version:020d}.manifest'):
            manifest = path / '_versions' / name
            if manifest.exists():
                with _open_physical(manifest) as (_, info):
                    if info.st_size > 16 * 1024**2:
                        raise ValueError('Manifest metadata exceeds input budget')
        fragments = dataset.get_fragments()
        if len(fragments) > 1024:
            raise ValueError('Too many fragments for input inspection')
        checked = 0
        for fragment in fragments:
            candidates = [('data', f.path, getattr(f, 'base_id', None)) for f in fragment.data_files()]
            deletion = fragment.metadata.deletion_file
            if deletion is not None:
                if deletion.file_type not in {'array', 'bitmap'}:
                    raise ValueError('Unsupported deletion file')
                extension = 'arrow' if deletion.file_type == 'array' else 'bin'
                candidates.append(('_deletions', f'{fragment.fragment_id}-{deletion.read_version}-{deletion.id}.{extension}', deletion.base_id))
            if fragment.metadata.overlays or fragment.metadata.row_id_meta is not None:
                raise ValueError('External/overlay row metadata is not covered')
            for directory, relative, base in candidates:
                part = Path(relative)
                checked += 1
                if (checked > 4096 or base is not None or part.is_absolute() or '..' in part.parts or
                        ':' in relative or str(part) != relative or _private(part)):
                    raise ValueError('Only bounded physical input fragments are covered')
                with _open_physical(path / directory / part):
                    pass
        schema, count = dataset.schema, dataset.count_rows()
    else:
        import pyarrow.parquet as pq
        dataset = pq.ParquetFile(str(path), memory_map=False, pre_buffer=False,
                                thrift_string_size_limit=1024**2, thrift_container_size_limit=100000)
        schema, count, version = dataset.schema_arrow, dataset.metadata.num_rows, None
    names = request['columns'] if request['columns'] is not None else schema.names[:8]
    if any(name not in schema.names for name in names):
        raise ValueError('Requested input column does not exist')
    fields, schema_bytes = [], 0
    for field in schema:
        item = dict(name=field.name, type=str(field.type), nullable=field.nullable)
        size = len(wire(item))
        if len(fields) >= _MAX_FIELDS or schema_bytes + size > 12000:
            break
        fields.append(item)
        schema_bytes += size
    rows = []
    limit = request['limit']
    if limit and names:
        if request['format'] == 'lance':
            rows = sample_open_table(dataset, columns=names, limit=limit, maximum=16384)
        else:
            reader = dataset.iter_batches(batch_size=1, columns=names, use_threads=False)
            try:
                for batch in reader:
                    if batch.nbytes > 16384 or batch.get_total_buffer_size() > 65536:
                        raise ValueError('Decoded input sample exceeds its byte budget')
                    rows.extend(preview_value(row) for row in batch.to_pylist())
                    wire(rows, 16384)
                    if len(rows) >= limit:
                        break
            finally:
                close = getattr(reader, 'close', None)
                if close:
                    close()
    return dict(format=request['format'], version=version, row_count=count,
        schema=fields, schema_truncated=len(fields) < len(schema),
        columns=names, sample=rows, sample_limit=limit, sample_truncated=len(rows) < count,
        note='Finite readonly sample of this physical input; no pipeline execution or output changes.')


def _limit_native_allocations():
    """Bound native decoding in this disposable preview without reading proc."""
    import resource
    ceiling = 4 * 1024**3
    for inherited in resource.getrlimit(resource.RLIMIT_AS):
        if inherited != resource.RLIM_INFINITY:
            ceiling = min(ceiling, inherited)
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))


def main():
    _limit_native_allocations()
    request = json.loads(sys.stdin.buffer.read(8193))
    if (type(request) is not dict or set(request) != {'path', 'format', 'columns', 'limit'} or
            request['format'] not in {'lance', 'parquet'}):
        raise ValueError('Invalid fixed input inspection request')
    sys.stdout.buffer.write(wire(_inspect_table(request), _RESULT_BYTES - 4096))


if __name__ == '__main__':
    # The interpreter starts isolated; only the trusted installed package is
    # importable. Never add the selected dataset or the project to sys.path.
    main()
