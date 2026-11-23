"""Bounded checksum proofs, invalidated by replacement or metadata changes."""
from collections import OrderedDict
import hashlib
import os
import stat
import threading
import time

from demiflow.execution.recovery_contract import RecoveryContractError


class ArtifactProofs:
    def __init__(self, capacity=256):
        self.capacity = capacity
        self.values = OrderedDict()
        self.lock = threading.Lock()
        self.hashed_bytes = self.hits = 0

    @staticmethod
    def signature(value):
        return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)

    def verify(self, path, size, sha256):
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise RecoveryContractError('Artifact proof path contains a symlink')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise RecoveryContractError('Artifact proof size differs')
            key = (str(path), self.signature(before), sha256)
            with self.lock:
                # Some supported filesystems coalesce rapid timestamp updates.
                # A recent write cannot carry a reusable proof until it is older
                # than that granularity window; ctime prevents mtime restoration
                # from reviving a proof after an ordinary user modification.
                stable = max(before.st_mtime_ns, before.st_ctime_ns) < time.time_ns() - 2_000_000_000
                cached = stable and key in self.values
                if cached:
                    self.values.move_to_end(key)
                    self.hits += 1
            if not cached:
                digest = hashlib.sha256()
                for raw in iter(lambda: source.read(1024**2), b''):
                    digest.update(raw)
                if digest.hexdigest() != sha256:
                    raise RecoveryContractError('Artifact checksum differs')
            if (self.signature(before) != self.signature(os.fstat(source.fileno())) or
                    self.signature(before) != self.signature(path.stat())):
                raise RecoveryContractError('Artifact changed while verifying its proof')
            if not cached:
                with self.lock:
                    self.hashed_bytes += size
                    self.values[key] = True
                    while len(self.values) > self.capacity:
                        self.values.popitem(last=False)


def verify_artifact(run, path, size, sha256):
    with run._object_resource_lock:
        proofs = getattr(run, '_artifact_proofs', None)
        if proofs is None:
            proofs = run._artifact_proofs = ArtifactProofs()
    proofs.verify(path, size, sha256)
