"""Controls for exactly one delegated, nonce-owned analysis job cgroup.

The external administrator owns parent limits. This helper never changes those
limits or targets the original pipeline/development cgroups. A matching path
alone is not ownership; lease inode and boot/root identity must also agree.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import job_id
from demiflow.workbench.analysis.resource_profile import profile


def identity(path):
    value = no_symlinks(Path(path)).stat()
    if not stat.S_ISDIR(value.st_mode):
        c.reject("Owned cgroup must be a physical directory")
    return dict(device=value.st_dev, inode=value.st_ino)


def read_control(path, name, *, maximum=4096):
    if name not in {"memory.max", "memory.swap.max", "pids.max", "cpu.max", "memory.oom.group", "cgroup.events", "cgroup.procs", "cgroup.controllers", "cgroup.subtree_control"}:
        c.reject("Unknown fixed cgroup control")
    descriptor = os.open(no_symlinks(Path(path)/name), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        c.reject("Owned cgroup control exceeds byte limit", code="resource_limit")
    return raw.decode("ascii").strip()


def _write(path, name, value):
    if name not in {"memory.max", "memory.swap.max", "pids.max", "cpu.max", "memory.oom.group", "cgroup.kill"}:
        c.reject("Cannot write this cgroup control")
    c.text(value, "cgroup control value", 64)
    descriptor = os.open(no_symlinks(Path(path)/name), os.O_WRONLY | os.O_NOFOLLOW)
    try:
        data = (value + "\n").encode("ascii")
        if os.write(descriptor, data) != len(data):
            c.reject("Cgroup control write was incomplete", code="operation_failed")
    finally:
        os.close(descriptor)


def expected(role):
    c.one_of(role, {"kernel", "native", "media"}, "owned job resource role")
    resource = profile()[0]
    return {"memory.max": str(resource["memory"][role]), "memory.swap.max": "0",
            "pids.max": str(resource["pids"][role]), "cpu.max": resource["cpu_max"][role], "memory.oom.group": "1"}


class Cgroups:
    def __init__(self, delegated_root):
        root = no_symlinks(Path(delegated_root))
        if not root.is_absolute() or not root.is_relative_to(Path("/sys/fs/cgroup")) or root == Path("/sys/fs/cgroup"):
            c.reject("Only the dedicated host cgroup-v2 delegation is allowed")
        self.root = root
        self.jobs = root/"jobs"
        self.root_identity = identity(root)
        self.jobs_identity = identity(self.jobs)
        if not {"memory", "pids", "cpu"} <= set(read_control(self.jobs, "cgroup.controllers").split()):
            c.reject("Required cgroup-v2 controllers are unavailable")
        if not {"memory", "pids", "cpu"} <= set(read_control(self.jobs, "cgroup.subtree_control").split()):
            c.reject("Administrator has not enabled the fixed child controllers")

    def _root(self):
        if identity(self.root) != self.root_identity or identity(self.jobs) != self.jobs_identity:
            c.reject("Analysis cgroup delegation identity changed", code="conflict")

    def create(self, identifier, role):
        self._root(); job_id(identifier)
        path = self.jobs/identifier
        # Never adopt an existing directory after a race or lost response.
        path.mkdir(mode=0o700, exist_ok=False)
        owned = identity(path)
        try:
            for name, value in expected(role).items():
                _write(path, name, value)
            self.verify(identifier, owned, role)
        except BaseException:
            # An empty newly-owned cgroup may be removed. Failure to remove is
            # an unresolved lease; caller persists it instead of inventing free.
            try:
                if not self.populated(identifier, owned):
                    path.rmdir()
            except (OSError, ValueError):
                pass  # caller's creating intent remains unresolved; no free claim
            raise
        return dict(job_id=identifier, role=role, cgroup_identity=owned,
                    delegation_identity=self.root_identity, jobs_identity=self.jobs_identity)

    def _owned(self, identifier, owned):
        self._root(); job_id(identifier)
        path = self.jobs/identifier
        c.fields(owned, {"device", "inode"}, label="owned cgroup identity")
        c.integer(owned["device"], "cgroup device", 2**63-1)
        c.integer(owned["inode"], "cgroup inode", 2**63-1, 1)
        if identity(path) != owned:
            c.reject("Cgroup path no longer belongs to the original lease", code="conflict")
        return path

    def absent(self, identifier):
        """An absence observation in the SAME original delegation/jobs root.

        Absence does not grant cleanup by itself. The runtime additionally
        requires its prior durable cleanup_removing_cgroup stage.
        """
        self._root(); job_id(identifier)
        path = self.jobs/identifier
        try:
            info = os.stat(path, follow_symlinks=False)
        except FileNotFoundError:
            return True
        if not stat.S_ISDIR(info.st_mode):
            c.reject("Owned cgroup path has a different file type", code="conflict")
        return False

    def verify(self, identifier, owned, role):
        path = self._owned(identifier, owned)
        for name, value in expected(role).items():
            if read_control(path, name) != value:
                c.reject("Owned job cgroup limit changed: " + name, code="conflict")

    def populated(self, identifier, owned):
        path = self._owned(identifier, owned)
        values = read_control(path, "cgroup.events").split()
        if len(values) % 2:
            c.reject("Malformed owned cgroup events")
        fields = dict(zip(values[::2], values[1::2]))
        if fields.get("populated") not in {"0", "1"}:
            c.reject("Owned cgroup has no definite population observation")
        return fields["populated"] == "1"

    def kill(self, identifier, owned):
        # cgroup.kill targets the entire owned subtree, including subprocesses
        # in different process groups. It does not by itself prove cleanup.
        _write(self._owned(identifier, owned), "cgroup.kill", "1")

    def remove_empty(self, identifier, owned):
        path = self._owned(identifier, owned)
        if self.populated(identifier, owned):
            c.reject("Owned cgroup is still populated; lease retained", code="busy")
        path.rmdir()
