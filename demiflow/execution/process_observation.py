"""Linux owned-process observation, independent of Dataset scheduling.

Only walk descendants of a supplied live owner. pidfds fence signals against
PID reuse; an inherited lock fences the startup window before a child binds
its parent-death signal. This is participant coordination, not a sandbox.
"""

import fcntl
import ctypes
import os
import platform
from pathlib import Path
import signal
import time

UNIT_ENV = "DEMIFLOW_EXECUTION_UNIT_FD"


def _pidfd(pid):
    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    # Older Python/libc builds can lack wrappers on a pidfd-capable kernel.
    # Linux x86_64 and asm-generic aarch64 syscall ABIs use these numbers.
    if platform.machine() not in {"x86_64", "aarch64"}:
        raise RuntimeError("Managed stopping requires a supported pidfd ABI")
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(434, int(pid), 0)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def _signal_fd(fd, signum):
    if hasattr(signal, "pidfd_send_signal"):
        signal.pidfd_send_signal(fd, signum)
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.syscall(424, int(fd), int(signum), ctypes.c_void_p(), 0) < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def require_pidfds():
    fd = _pidfd(os.getpid())
    try:
        _signal_fd(fd, 0)
    finally:
        os.close(fd)


def inherited_unit():
    value = os.environ.get(UNIT_ENV)
    if value is None:
        return ()
    fd = int(value)
    if fd < 3:
        raise ValueError("Invalid execution unit descriptor")
    os.fstat(fd)
    return (fd,)


def unit_stopped(path, identity=None):
    """Missing ownership evidence is unknown, never proof of stopped execution."""
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        if identity is not None:
            info = os.fstat(fd)
            if identity != {"device": info.st_dev, "inode": info.st_ino}:
                return False
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    finally:
        os.close(fd)


def _read(path, maximum=32768):
    with open(path, "rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise RuntimeError("Process observation byte budget exceeded")
    return raw


def descendants(owner, *, include_owner=False):
    pending, seen, result = [owner], set(), {}
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        if len(seen) >= 128:
            raise RuntimeError("Run process count budget exceeded")
        seen.add(pid)
        try:
            fields = _read(f"/proc/{pid}/stat").decode().rsplit(")", 1)[1].split()
            if include_owner or pid != owner:
                result[pid] = dict(
                    start=fields[19],
                    state=fields[0],
                    cpu_ticks=int(fields[11]) + int(fields[12]),
                    rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
                )
            with os.scandir(f"/proc/{pid}/task") as tasks:
                for count, task in enumerate(tasks):
                    if count >= 256:
                        raise RuntimeError("Run thread count budget exceeded")
                    try:
                        children = _read(task.path + "/children").split()
                    except FileNotFoundError:
                        continue
                    for child in children:
                        if len(pending) >= 128:
                            raise RuntimeError("Run process queue budget exceeded")
                        pending.append(int(child))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def signal_owned(owner, signum):
    # The owner is a dedicated subreaper. Even children in a different session
    # remain its descendants; never use a process name or a shared UID as proof.
    for pid, identity in descendants(owner).items():
        try:
            fd = _pidfd(pid)
            try:
                current = _read(f"/proc/{pid}/stat").decode().rsplit(")", 1)[1].split()
                if current[19] == identity["start"] and current[0] != "Z":
                    _signal_fd(fd, signum)
            finally:
                os.close(fd)
        except (FileNotFoundError, ProcessLookupError):
            pass


def drain_owned(process, *, timeout_s=5):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        signal_owned(os.getpid(), signal.SIGKILL)
        # Let Popen reap its direct child first, retaining the real exit code.
        process.poll()
        try:
            if process.returncode is not None:
                while os.waitpid(-1, os.WNOHANG)[0]:
                    pass
        except ChildProcessError:
            return True
        time.sleep(0.02)
    return False


class ProcessSampler:
    """Rates only for a stable observed live set; vanished work stays partial."""

    def __init__(self):
        self.previous = None

    def sample(self, owner):
        now = time.monotonic()
        rows = descendants(owner, include_owner=True)
        members = tuple(sorted((pid, row["start"]) for pid, row in rows.items()))
        ticks = sum(r["cpu_ticks"] for r in rows.values())
        io = {"read_bytes": 0, "write_bytes": 0, "rchar": 0, "wchar": 0}
        io_available = True
        for pid in rows:
            try:
                raw = dict(
                    line.split(":", 1)
                    for line in _read(f"/proc/{pid}/io").decode().splitlines()
                )
                for key in io:
                    io[key] += int(raw[key])
            except (OSError, ValueError, KeyError):
                io_available = False
        stable = self.previous is not None and self.previous[1] == members
        cpu = rates = None
        if stable:
            before, _, old_ticks, old_io, old_available = self.previous
            elapsed = now - before
            if elapsed > 0 and ticks >= old_ticks:
                cpu = (ticks - old_ticks) / os.sysconf("SC_CLK_TCK") / elapsed
            if (
                elapsed > 0
                and io_available
                and old_available
                and all(io[k] >= old_io[k] for k in io)
            ):
                rates = {k + "_per_s": (io[k] - old_io[k]) / elapsed for k in io}
        self.previous = (now, members, ticks, io, io_available)
        return dict(
            cpu_cores=cpu,
            rss_bytes=sum(r["rss_bytes"] for r in rows.values()),
            process_count=len(rows),
            io=rates,
            scope="observed_worker_descendants",
            coverage="sampled" if stable else "partial",
            reasons=[] if stable else ["process_set_changed_or_first_sample"],
        )
