"""Small pidfd/deadline watcher, independent of Jupyter/Arrow and DB parsing.

The host supervisor must separately handle watchdog death/ancestor OOM. This
watcher cannot free a controller DB reservation or certify mount/file cleanup.
"""
from __future__ import annotations

from contextlib import ExitStack
import os
import select
import time

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import contracts as c


class ParentWatch:
    def __init__(self, identity):
        c.fields(identity, {"pid", "start", "boot"}, label="owned parent process")
        c.integer(identity["pid"], "owned parent PID", 2**31-1, 1)
        c.text(identity["start"], "process start tick", 32)
        c.text(identity["boot"], "host boot ID", 64)
        self.fd = None
        if process_identity(identity["pid"]) != identity:
            # A lost/unobservable/reused parent must stop its owned jobs. Do
            # not open or signal the unrelated process now using this PID.
            return
        try:
            if hasattr(os, "pidfd_open"):
                self.fd = os.pidfd_open(identity["pid"])
            else:
                # Some supported Python builds lack this Linux wrapper. Open
                # the kernel handle directly so this small watcher never has
                # to load the pipeline API into its fixed memory allowance.
                import ctypes
                import platform
                if platform.machine() not in {"x86_64", "aarch64"}:
                    raise RuntimeError("Managed stopping requires a supported pidfd ABI")
                libc = ctypes.CDLL(None, use_errno=True)
                descriptor = libc.syscall(434, int(identity["pid"]), 0)
                if descriptor < 0:
                    error = ctypes.get_errno()
                    raise OSError(error, os.strerror(error))
                self.fd = descriptor
        except OSError:
            return  # unavailable parent: stop owned work, never signal a PID
        if process_identity(identity["pid"]) != identity:
            os.close(self.fd)
            self.fd = None

    def stopped(self):
        return self.fd is None or bool(select.select([self.fd], [], [], 0)[0])

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class JobWatch:
    """One nonblocking tick per job; at most three live role slots are watched.

    A zero population during create is only an observation. Keep this watch
    until the controller proves full cleanup and explicitly unregisters it.
    """

    def __init__(self, cgroups, lease, *, controller, web_parent, deadline):
        if type(deadline) not in {int, float} or not 0 <= deadline <= time.monotonic() + 3600:
            c.reject("Watchdog deadline must be finite and within one hour")
        c.fields(lease, {"job_id", "role", "cgroup_identity", "delegation_identity", "jobs_identity"}, label="owned job lease")
        if lease["delegation_identity"] != cgroups.root_identity or lease["jobs_identity"] != cgroups.jobs_identity:
            c.reject("Watchdog lease belongs to a different delegation", code="conflict")
        self.cgroups, self.lease, self.deadline = cgroups, lease, deadline
        self.reason = None
        self.stack = ExitStack()
        self.watches = []
        try:
            for value in (controller, web_parent):
                watch = ParentWatch(value)
                self.stack.callback(watch.close)
                self.watches.append(watch)
        except BaseException:
            self.stack.close()
            raise

    def tick(self, *, stop_requested=False):
        if type(stop_requested) is not bool:
            c.reject("Watchdog stop intent must be boolean")
        identifier, owned = self.lease["job_id"], self.lease["cgroup_identity"]
        # Latch stop/deadline/parent loss even during an empty create window.
        # A later init must not revive a watch whose stop was already accepted.
        if any(watch.stopped() for watch in self.watches):
            self.reason = "owned_parent_stopped_or_unavailable"
        elif time.monotonic() >= self.deadline:
            self.reason = "deadline"
        elif stop_requested:
            self.reason = "stop_requested"
        populated = self.cgroups.populated(identifier, owned)
        if not populated:
            return dict(job_id=identifier, populated=False, reason=self.reason or "exited",
                        process_observed_stopped=True, namespace_file_cleanup_proven=False)
        if self.reason is not None:
            self.cgroups.kill(identifier, owned)
        return dict(job_id=identifier, populated=True, reason=self.reason,
                    cleanup_pending=self.reason is not None,
                    process_observed_stopped=False, namespace_file_cleanup_proven=False)

    def close(self):
        self.stack.close()
