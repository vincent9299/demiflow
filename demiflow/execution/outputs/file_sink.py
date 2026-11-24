"""Bounded single-file publication through the original native commit protocol.

Staging lives in the existing journal-owned object namespace on the target's
filesystem. Publication hard-links that inode, so recovery proves ownership by
inode plus content digest, never by coincidentally equal target bytes. Existing
files are replaced only if their bound identity still matches. A fixed flock
coordinates platform writers; unrelated external mutation is detected at each
inspection and is never adopted as our successful publication.
"""
from contextlib import contextmanager
import csv
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import stat

from demiflow.data.datasink import Datasink
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, UnresolvedCommit, identity


def _info(path):
    try:
        item = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(item.st_mode):
        raise RecoveryContractError('File output target must be a regular file')
    return dict(device=item.st_dev, inode=item.st_ino, bytes=item.st_size,
                mtime_ns=item.st_mtime_ns, ctime_ns=item.st_ctime_ns)


def _digest(path):
    result = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


class _BudgetFile:
    """Reserve the exact next encoded extent before the native writer writes."""
    def __init__(self, stream, run, path):
        self.stream, self.run, self.path = stream, run, path
        self.closed = False
    def write(self, value):
        self.run.check_stop()
        extent = self.stream.tell() + len(value)
        if extent > self.run.limits['max_output_bytes']:
            raise DataflowError('BUDGET_EXCEEDED', 'File output exceeds encoded byte budget')
        self.run.reserve_artifact_bytes(self.path, extent)
        return self.stream.write(value)
    def tell(self): return self.stream.tell()
    def flush(self): return self.stream.flush()
    def writable(self): return True
    def close(self):
        self.flush()
        self.closed = True


class FileSink(Datasink):
    def __init__(self, action, arguments):
        self.action, self.run = action, action.context
        self.format = arguments['format']
        self.target = Path(arguments['path']).absolute()
        if self.format not in ('json', 'csv', 'parquet'):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Unknown file output format')
        if '..' in self.target.parts or any(p.is_symlink() for p in (self.target, *self.target.parents)):
            raise RecoveryContractError('File output destination contains a symlink or traversal')
        self.options = dict(arguments.get('options') or {})
        if self.options.pop('filesystem', None) is not None or arguments.get('native_options'):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'File output requires a local filesystem')
        if self.format == 'json':
            if not self.options.pop('lines', True):
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'write_json requires lines=True for bounded streaming')
        if self.format != 'parquet' and self.options:
            raise TypeError('unsupported write_' + self.format + ' options: ' + str(sorted(self.options)))
        key = self.action.action_id + '-file-target'
        binding = self.run.journal.source_binding(key)
        declaration = {'target': str(self.target), 'format': self.format}
        if binding is None:
            binding = {**declaration, 'baseline': _info(self.target)}
            self.run.journal.bind_source(self.run.fence, key, binding)
        if any(binding[k] != v for k, v in declaration.items()):
            raise RecoveryContractError('File output target binding changed')
        self.baseline = binding['baseline']
        self.root = self.run.object_namespace(self.target.parent) / 'files' / action.action_id
        self.root.mkdir(parents=True, exist_ok=True)

    def recovery_identity(self):
        return {'kind': 'file', 'format': self.format, 'path': str(self.target), 'options': self.options}

    @contextmanager
    def _lock(self):
        lock = self.target.parent / ('.demiflow-file-' + identity(str(self.target)) + '.lock')
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RecoveryContractError('File publication lock must be regular')
            fcntl.flock(fd, fcntl.LOCK_EX)
            current = lock.lstat()
            opened = os.fstat(fd)
            if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                raise UnresolvedCommit('File publication lock was replaced')
            yield
        finally:
            os.close(fd)

    def prepare(self, blocks, context):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = self.root / (context.commit_id + '.prepared')
        self.run.allocate_artifact(path, commit_id=context.commit_id, atomic=False)
        if self.baseline is not None:
            self.run.allocate_artifact(self.root / (context.commit_id + '.publish'),
                                       commit_id=context.commit_id, atomic=False)
        with self._lock():
            if _info(self.target) != self.baseline:
                raise UnresolvedCommit('File output target changed before preparation')
        rows, writer, fields = 0, None, None
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, 'wb') as raw:
                stream = _BudgetFile(raw, self.run, path)
                try:
                    for block in blocks:
                        self.run.check_stop()
                        rows += block.num_rows
                        if rows > self.run.limits['max_output_rows']:
                            raise DataflowError('BUDGET_EXCEEDED', 'File output exceeds row budget')
                        if self.format == 'parquet':
                            if writer is None:
                                writer = pq.ParquetWriter(stream, block.schema, **self.options)
                            writer.write_table(pa.Table.from_batches([block]) if isinstance(block, pa.RecordBatch) else block)
                        else:
                            for row in block.to_pylist():
                                if self.format == 'json':
                                    encoded = (json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode()
                                else:
                                    text = io.StringIO(newline='')
                                    if fields is None:
                                        fields = list(row)
                                        csv.DictWriter(text, fieldnames=fields).writeheader()
                                    csv.DictWriter(text, fieldnames=fields).writerow(row)
                                    encoded = text.getvalue().encode()
                                stream.write(encoded)
                    if self.format == 'parquet' and writer is None:
                        writer = pq.ParquetWriter(stream, pa.schema([]), **self.options)
                    elif self.format == 'csv' and fields is None:
                        stream.write(b'\r\n')
                finally:
                    if writer is not None:
                        writer.close()
                raw.flush()
                os.fsync(raw.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        self._sync(path.parent)
        info = _info(path)
        self.run.reserve('output_rows', rows, ceiling=self.run.limits['max_output_rows'],
                         reservation_id=identity([context.commit_id, 'file-rows']))
        self.run.reserve_output('row_bytes', info['bytes'],
                                reservation_id=identity([context.commit_id, 'file-bytes']))
        return {'path': self.run.journal.artifact_name(path), 'bytes': info['bytes'],
                'sha256': _digest(path), 'rows': rows, 'inode': info['inode'],
                'device': info['device'], 'target': str(self.target), 'baseline': self.baseline}

    @staticmethod
    def _sync(directory):
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)

    def _prepared_path(self, prepared, context):
        path = self.root / (context.commit_id + '.prepared')
        if (prepared['path'] != self.run.journal.artifact_name(path)
                or prepared['target'] != str(self.target) or prepared['baseline'] != self.baseline):
            raise RecoveryContractError('File staging ownership differs')
        info = _info(path)
        if info is None or any(info[k] != prepared[k] for k in ('bytes', 'inode', 'device')) or _digest(path) != prepared['sha256']:
            raise RecoveryContractError('File staging identity or checksum differs')
        return path

    def _receipt(self, prepared, context):
        return {'commit_id': context.commit_id, 'sink_id': 'file', 'status': 'committed',
                'data': {'path': str(self.target), 'format': self.format,
                         'rows': prepared['rows'], 'bytes': prepared['bytes'], 'sha256': prepared['sha256']}}

    def inspect(self, prepared, context):
        self._prepared_path(prepared, context)
        info = _info(self.target)
        if info is not None and (info['device'], info['inode']) == (prepared['device'], prepared['inode']):
            if info['bytes'] != prepared['bytes'] or _digest(self.target) != prepared['sha256']:
                raise UnresolvedCommit('Published file changed after this commit')
            return self._receipt(prepared, context)
        if info == self.baseline:
            return None
        raise UnresolvedCommit('File output has an unknown or concurrent publication')

    def commit(self, prepared, context):
        with self._lock():
            receipt = self.inspect(prepared, context)
            if receipt is not None:
                return receipt
            path = self._prepared_path(prepared, context)
            if self.baseline is None:
                os.link(path, self.target, follow_symlinks=False)
            else:
                link = self.root / (context.commit_id + '.publish')
                if link.exists() or link.is_symlink():
                    linked = _info(link)
                    if (linked['device'], linked['inode']) != (prepared['device'], prepared['inode']):
                        raise UnresolvedCommit('File publication staging changed')
                else:
                    os.link(path, link, follow_symlinks=False)
                os.replace(link, self.target)
            self._sync(self.target.parent)
            return self._receipt(prepared, context)

    def abort(self, prepared, context):
        with self._lock():
            if self.inspect(prepared, context) is None:
                self._prepared_path(prepared, context).unlink()
                self._sync(self.root)
