"""Physical readers/writers and GC share one permanent, journal-bound inode.

The lock is not a liveness database. Only the original RunJournal records the
protocol and reclamation intents. A missing/replaced/legacy lock proves nothing.
"""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import secrets
import stat
import threading
import time

from ..contracts.dataflow_errors import DataflowError

_LOCAL = threading.local()
NAME = '.published-objects.lock'
HEADER = '.published-objects.v1'


def validate_identity(value):
    """v1 is host-local; v2 is a shared filesystem object, never a host dev_t.

    Keep the established header filename so old readers fail closed on v2,
    rather than treating a new store as a legacy namespace with no protocol.
    """
    if type(value) not in (list, tuple):
        raise ValueError('invalid lease identity')
    value = tuple(value)
    if len(value) == 2 and all(type(v) is int and v >= 0 for v in value):
        return value
    if (len(value) == 5 and value[0] == 2 and type(value[0]) is int
            and all(type(v) is int and v >= 0 for v in value[1:4])
            and type(value[4]) is str and len(value[4]) == 32
            and all(c in '0123456789abcdef' for c in value[4])):
        return value
    raise ValueError('unsupported lease identity version or fields')


def journal_identity(protocol):
    """Read the original protocol row without upgrading any historical fact."""
    if len(protocol) == 3:
        return validate_identity(protocol[:2])
    if len(protocol) != 4 or type(protocol[3]) is not str:
        raise DataflowError('FENCED', 'Published object journal protocol is invalid')
    try:
        value = validate_identity(json.loads(protocol[3]))
        if len(value) != 5 or value[3] != protocol[1]:
            raise ValueError('journal inode binding differs')
        return value
    except (ValueError, TypeError) as exc:
        raise DataflowError('FENCED', 'Published object journal identity is invalid') from exc


def _shared_identity(root, fd, local, nonce):
    # f_fsid is the filesystem identity, unlike st_dev's host-local mount number.
    # Both the root and the lock object are pinned: copying a run/header elsewhere
    # cannot turn that copy into the originally journal-bound lock.
    root = Path(root)
    if os.pread(fd, 33, 0) != nonce.encode('ascii'):
        raise DataflowError('FENCED', 'Published object nonce differs from original lock')
    root_stat = root.lstat()
    if not stat.S_ISDIR(root_stat.st_mode):
        raise DataflowError('FENCED', 'Published object root is not a directory')
    fsid = os.fstatvfs(fd).f_fsid
    if fsid != os.statvfs(root).f_fsid:
        raise DataflowError('FENCED', 'Published object lock and root are on different filesystems')
    return validate_identity((2, fsid, root_stat.st_ino, local[1], nonce))


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
        if len(raw) > 256 or type(value) is not list:
            raise ValueError('invalid lease header')
        return validate_identity(value)
    except (ValueError, TypeError) as exc:
        raise DataflowError('FENCED', 'Published object protocol header is invalid') from exc
    finally:
        os.close(fd)


def record_protocol(root, identity):
    """Physical evidence only; the native journal alone decides eligibility."""
    identity = validate_identity(identity)
    old = protocol_identity(root)
    if old is not None:
        if old != tuple(identity):
            raise DataflowError('FENCED', 'Published object protocol identity changed')
        return
    from .objects import _sync_directory
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
    lease.check()
    return True


def inode(path, fd, *, size=0):
    visible, held = path.lstat(), os.fstat(fd)
    if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1 or held.st_size != size
            or (visible.st_dev, visible.st_ino) != (held.st_dev, held.st_ino)
            or not stat.S_ISREG(visible.st_mode)):
        raise DataflowError('FENCED', 'Published object lease inode changed')
    return held.st_dev, held.st_ino


class ObjectLease:
    def __init__(self, root, *, create=False, exclusive=False, expected=None, on_close=None,
                 wait_ms=0, protocol_version=2):
        from .objects import _no_symlink, _sync_directory
        self.fd = None
        self._on_close = on_close
        if type(wait_ms) is not int or not 0 <= wait_ms <= 5000 or (exclusive and wait_ms):
            raise ValueError('Only shared object leases may wait, for at most 5000 ms')
        if type(protocol_version) is not int or protocol_version not in (1, 2):
            raise ValueError('Unsupported object lease protocol version')
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
            physical = protocol_identity(root)
            expected = validate_identity(expected) if expected is not None else physical
            if physical is not None and expected is not None and physical != expected:
                reason = ('Published object protocol changed while waiting' if self.waited
                          else 'Published object protocol differs from original journal')
                raise DataflowError('FENCED', reason)
            if create:
                if physical is not None:
                    raise DataflowError('FENCED', 'Cannot recreate a registered published object lease')
                local = inode(self.path, fd)
                if protocol_version == 2:
                    nonce = secrets.token_hex(16)
                    if os.write(fd, nonce.encode('ascii')) != 32:
                        raise OSError('Incomplete published object lock nonce')
                    os.fsync(fd)
                    local = inode(self.path, fd, size=32)
                    self.identity = _shared_identity(root, fd, local, nonce)
                else:
                    self.identity = local
            elif expected is not None and len(expected) == 5:
                if physical != expected:
                    raise DataflowError('FENCED', 'Published object shared protocol header is missing')
                local = inode(self.path, fd, size=32)
                self.identity = _shared_identity(root, fd, local, expected[4])
            else:
                self.identity = inode(self.path, fd)  # Preserve v1's exact, host-local meaning.
            if expected is not None and self.identity != expected:
                raise DataflowError('FENCED', 'Published object lease differs from original journal')
            if create:
                _sync_directory(self.path.parent)
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise

    def check(self):
        local = inode(self.path, self.fd, size=32 if len(self.identity) == 5 else 0)
        current = (_shared_identity(self.path.parent, self.fd, local, self.identity[4])
                   if len(self.identity) == 5 else local)
        if current != self.identity:
            raise DataflowError('FENCED', 'Published object lease identity changed')

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
        if lease.waited:
            current = protocol_identity(root)
            # The first writer registers the header while holding this very
            # inode exclusively. A metadata reader may have observed no header
            # before waiting. Accept only that first registration, verified by
            # ObjectLease against the held inode/root/nonce after acquisition.
            # An existing identity (including an explicit journal identity)
            # can never be replaced or downgraded by this path.
            first_registration = (physical is None and expected is None
                                  and current == lease.identity)
            if current != physical and not first_registration:
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
        lease.check()
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
