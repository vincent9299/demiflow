"""Physical readers/writers and GC share one permanent, journal-bound inode.

The lock is not a liveness database. Only the original RunJournal records the
protocol and reclamation intents. A missing/replaced/legacy lock proves nothing.
"""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import stat
import threading
import time

from .contracts.dataflow_errors import DataflowError

_LOCAL = threading.local()
NAME = '.published-objects.lock'
HEADER = '.published-objects.v1'


def protocol_identity(root):
    path = Path(root) / HEADER
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return None
    try:
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(257)
        value = json.loads(raw)
        if (len(raw) > 256 or type(value) is not list or len(value) != 2
                or any(type(v) is not int or v < 0 for v in value)):
            raise ValueError('invalid lease header')
        return tuple(value)
    except (ValueError, TypeError) as exc:
        raise DataflowError('FENCED', 'Published object protocol header is invalid') from exc
    finally:
        os.close(fd)


def record_protocol(root, identity):
    """Physical evidence only; the native journal alone decides eligibility."""
    old = protocol_identity(root)
    if old is not None:
        if old != tuple(identity):
            raise DataflowError('FENCED', 'Published object protocol identity changed')
        return
    from .dataflow_store import _sync_directory
    path = Path(root) / HEADER
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        os.write(fd, json.dumps(list(identity), separators=(',', ':')).encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    _sync_directory(Path(root))


def exclusive_here(root):
    lease = getattr(_LOCAL, 'exclusive', {}).get(str(Path(root)))
    if lease is None:
        return False
    inode(lease.path, lease.fd)
    return True


def inode(path, fd):
    visible, held = path.lstat(), os.fstat(fd)
    if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1 or held.st_size != 0
            or (visible.st_dev, visible.st_ino) != (held.st_dev, held.st_ino)
            or not stat.S_ISREG(visible.st_mode)):
        raise DataflowError('FENCED', 'Published object lease inode changed')
    return held.st_dev, held.st_ino


class ObjectLease:
    def __init__(self, root, *, create=False, exclusive=False, expected=None, on_close=None,
                 wait_ms=0):
        from .dataflow_store import _no_symlink, _sync_directory
        self.fd = None
        self._on_close = on_close
        if type(wait_ms) is not int or not 0 <= wait_ms <= 5000 or (exclusive and wait_ms):
            raise ValueError('Only shared object leases may wait, for at most 5000 ms')
        self.path = Path(root) / NAME
        self.waited = False
        _no_symlink(self.path)
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
        if create:
            flags |= os.O_CREAT | os.O_EXCL
        fd = os.open(self.path, flags, 0o600)
        try:
            deadline = None
            while True:
                try:
                    fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if not wait_ms:
                        raise
                    self.waited = True
                    now = time.monotonic()
                    if deadline is None:
                        deadline = now + wait_ms / 1000
                    remaining = deadline - now
                    if remaining <= 0:
                        raise
                    time.sleep(min(.005, remaining))
            self.identity = inode(self.path, fd)
            if expected is not None and self.identity != tuple(expected):
                raise DataflowError('FENCED', 'Published object lease differs from original journal')
            if create:
                _sync_directory(self.path.parent)
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            try:
                os.close(fd)
            finally:
                if self._on_close is not None:
                    callback,self._on_close = self._on_close,None
                    callback()

    def __del__(self):
        self.close()


@contextmanager
def shared_access(root, expected=None, *, require_protocol=False, wait_ms=0):
    root = Path(root)
    if exclusive_here(root):
        yield
        return
    physical = protocol_identity(root)
    if require_protocol and physical is None:
        raise DataflowError('FENCED', 'Registered published object protocol header is missing')
    if expected is not None and physical is not None and tuple(expected) != physical:
        raise DataflowError('FENCED', 'Published object protocol differs from original identity')
    expected = expected if expected is not None else physical
    try:
        lease = ObjectLease(root, expected=expected, wait_ms=wait_ms)
    except FileNotFoundError:
        if expected is not None:
            raise DataflowError('FENCED', 'Registered published object lease is missing')
        # Legacy stores remain usable, but their journal is never GC eligible.
        yield
        return
    except BlockingIOError as exc:
        if wait_ms:
            raise DataflowError('CAPACITY_PENDING',
                'Published object maintenance exceeded the journal wait budget', retryable=True) from exc
        raise DataflowError('FENCED', 'Published objects are being reconciled') from exc
    try:
        if lease.waited and protocol_identity(root) != physical:
            raise DataflowError('FENCED', 'Published object protocol changed while waiting')
        yield
    finally:
        lease.close()


@contextmanager
def held_exclusive(lease):
    root = lease.path.parent
    old = getattr(_LOCAL, 'exclusive', {})
    _LOCAL.exclusive = {**old, str(root): lease}
    try:
        yield lease
        inode(lease.path, lease.fd)
    finally:
        _LOCAL.exclusive = old


@contextmanager
def exclusive_access(root, expected):
    root = Path(root)
    physical = protocol_identity(root)
    if physical != tuple(expected):
        raise DataflowError('FENCED', 'GC physical header differs from original journal')
    lease = ObjectLease(root, exclusive=True, expected=expected)
    try:
        with held_exclusive(lease):
            yield lease
    finally:
        lease.close()
