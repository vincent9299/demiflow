"""Fixed-size deployment leases for participating Forge Local callers.

No queue, runtime database, automatic adoption, or scheduler. Persistent slot
owners survive launcher loss; only stopped proof permits reuse. Native query
pools remain per TMPDIR and are included in the outer reservation.
"""

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
import fcntl
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import json
import time
import threading
import sys

from demiflow.forge.io import no_symlinks
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import read_json
from demiflow.forge.io import fsync_directory
from demiflow.forge.schema import ForgeError
from demiflow.forge import operation_budget
from demiflow.workbench import host_partition

MiB = 1024**2
CONTROL_RSS = 384 * MiB
MANAGEMENT_RESERVE = 512 * MiB
SEALED_BYTES = 192 * MiB


@contextmanager
def lock(path=None):
    path = host_partition.location() if path is None else Path(path)
    no_symlinks(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    fsync_directory(path.parent)
    fd = os.open(path / "admission.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ForgeError(
                "Deployment admission is busy; retry", code="busy"
            ) from None
        yield path, host_partition.policy(path)
    finally:
        os.close(fd)


@contextmanager
def preparation_lock():
    """One bounded inspector/seal operation per participating deployment."""
    path = host_partition.location()
    no_symlinks(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(
        path / "preparation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ForgeError(
                "Deployment preparation capacity is full; retry the same request ID",
                code="busy",
            ) from None
        yield fd
    finally:
        os.close(fd)


def _allocated(owner, *, deadline, include_outputs=True):
    """Current allocated blocks, not logical sparse-file lengths.

    Only used at admission, bounded across owned roots. Subtracting growth
    since admission avoids charging already allocated bytes against free space
    twice. Unknown coverage refuses new admission, never releases live owners.
    """
    from demiflow.workbench.runs.manager import directory

    target = directory(owner["root"], owner["run_id"])
    roots = [target]
    if owner.get("object_locations"):
        from demiflow.workbench.runs.storage import owned_objects
        roots.extend(owned_objects(target, owner["object_locations"]))
    if include_outputs:
        roots.extend(Path(o["path"]) for o in owner["outputs"])
    total = {}
    count = 0
    seen = set()
    for root in roots:
        no_symlinks(root)
        pending = [(root, 0)]
        while pending:
            path, depth = pending.pop()
            if depth > 24 or time.monotonic() > deadline:
                raise ForgeError(
                    "Device allocation probe exceeds time/depth budget",
                    code="resource_limit",
                )
            try:
                stat = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            identity = (stat.st_dev, stat.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            count += 1
            if count > 65536 or path.is_symlink():
                raise ForgeError(
                    "Device allocation probe exceeds entry budget",
                    code="resource_limit",
                )
            device = str(stat.st_dev)
            total[device] = total.get(device, 0) + stat.st_blocks * 512
            if path.is_dir():
                with os.scandir(path) as entries:
                    for entry in entries:
                        if len(pending) + count >= 65536:
                            raise ForgeError(
                                "Device allocation queue exceeds budget",
                                code="resource_limit",
                            )
                        pending.append((Path(entry.path), depth + 1))
    return total


def _target(path):
    path = Path(path).absolute()
    no_symlinks(path)
    suffix = []
    while not path.exists():
        suffix.insert(0, path.name)
        path = path.parent
        if len(suffix) > 24:
            raise ForgeError("Output ancestor budget exceeded")
    stat = path.stat()
    # Include existing parents so aliases still conflict when one target was
    # absent at admission and another already exists below a bind alias.
    anchors = []
    relative = list(suffix)
    ancestor = path
    for _ in range(32):
        info = ancestor.stat()
        anchors.append([info.st_dev, info.st_ino, "/".join(relative)])
        if ancestor.parent == ancestor:
            break
        relative.insert(0, ancestor.name)
        ancestor = ancestor.parent
    else:
        raise ForgeError("Output ancestor budget exceeded")
    return dict(
        path=str(path.joinpath(*suffix)),
        device=stat.st_dev,
        ancestor=[stat.st_dev, stat.st_ino],
        suffix=suffix,
        anchors=anchors,
    )


def _overlap(a, b):
    pa, pb = Path(a["path"]), Path(b["path"])
    if pa == pb or pa in pb.parents or pb in pa.parents:
        return True
    # Also handles bind aliases sharing the same existing ancestor inode.
    if a["ancestor"] == b["ancestor"]:
        sa, sb = a["suffix"], b["suffix"]
        return sa[: len(sb)] == sb or sb[: len(sa)] == sa
    left = {(d, i): s for d, i, s in a.get("anchors", [])}
    for device, inode, suffix in b.get("anchors", []):
        other = left.get((device, inode))
        if other is not None and (
            not other
            or not suffix
            or other == suffix
            or other.startswith(suffix + "/")
            or suffix.startswith(other + "/")
        ):
            return True
    return False


def _owner_finished(owner):
    from demiflow.workbench.runs import manager as runs
    from demiflow.resources.process_observation import unit_stopped

    try:
        target = runs.directory(owner["root"], owner["run_id"])
        path = target / "run.sqlite"
        if not path.is_file() or path.stat().st_size > 64 * MiB:
            return False
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        try:
            row = db.execute(
                "SELECT document FROM attempts WHERE attempt=?", (owner["attempt"],)
            ).fetchone()
            if not row or len(row[0]) > 65536:
                return False
            state = json.loads(row[0])
        finally:
            db.close()
        return (
            state["status"] in runs.TERMINAL
            and state.get("execution_stopped") is True
            and state.get("cleanup") == "complete"
            and runs.execution_stopped(target, state)
        )
    except (OSError, ValueError, KeyError, sqlite3.Error):
        return False  # Missing/relocated evidence is not a free slot.


def reserve(path, config, root, manifest, attempt):
    outputs = [_target(o["uri"]) for o in manifest["outputs"].values()]
    if len(outputs) > 32:
        raise ForgeError("Output lease budget exceeded")
    limits = manifest["limits"]
    from demiflow.workbench.runs.manager import directory
    from demiflow.workbench.runs.storage import object_locations
    target = directory(root, manifest["run_id"])
    object_paths = object_locations(manifest=manifest, target=target)
    object_targets = [_target(p) for p in object_paths]
    own = dict(
        root=str(root),
        run_id=manifest["run_id"],
        attempt=attempt,
        rss_bytes=limits["max_rss_bytes"] + CONTROL_RSS,
        cpu_threads=5,
        outputs=outputs,
        object_locations=object_paths,
    )
    device = _target(target)["device"]
    devices = {
        str(device): limits["max_scratch_bytes"]
        + limits["max_evidence_bytes"]
        + MANAGEMENT_RESERVE
    }
    # The native shared scratch ceiling includes owned object namespaces.
    # Reserve its full bound on each distinct device before any worker starts.
    for d in {o["device"] for o in object_targets} - {device}:
        devices[str(d)] = limits["max_scratch_bytes"]
    # Output budget is aggregate, conservatively reserve it on every distinct
    # output device. Never pretend per-query TMPDIR leases cover other runs.
    for d in {o["device"] for o in outputs}:
        devices[str(d)] = devices.get(str(d), 0) + limits["max_output_bytes"]
    own["devices"] = devices
    from demiflow.workbench.host_partition import load
    load(path, config, root=root, devices=devices, admit=True)
    live, free = [], []
    for number in range(16):
        slot = path / f"slot-{number}.json"
        owner = read_json(slot, maximum=128 * 1024) if slot.exists() else None
        if owner and not _owner_finished(owner):
            if (owner["root"], owner["run_id"], owner["attempt"]) == (
                str(root),
                manifest["run_id"],
                attempt,
            ):
                raise ForgeError(
                    "This execution reservation requires reconciliation",
                    code="conflict",
                )
            live.append(owner)
        else:
            free.append(number)
    if len(live) >= config["max_active"]:
        raise ForgeError("Deployment active capacity is full; no queue", code="busy")
    if (
        sum(o["rss_bytes"] for o in live) + own["rss_bytes"] + host_partition.PREPARATION_RSS
        > config["max_rss_bytes"]
        or sum(o["cpu_threads"] for o in live) + 5 + 2 > config["cpu_threads"]
    ):
        raise ForgeError(
            "Deployment CPU or RSS reservation exceeds capacity", code="resource_limit"
        )
    for other in live:
        if any(_overlap(a, b) for a in outputs for b in other["outputs"]):
            raise ForgeError(
                "Another managed run owns an overlapping output", code="conflict"
            )
    deadline = time.monotonic() + 5
    allocated = {
        id(o): _allocated(o, deadline=deadline) if "allocated_baseline" in o else {}
        for o in live
    }
    own["allocated_baseline"] = _allocated(own, deadline=deadline)
    own["allocated_credit"] = _allocated(own, deadline=deadline, include_outputs=False)
    for d, amount in devices.items():
        reserved = sum(o["devices"].get(d, 0) for o in live)
        if amount + reserved > config["device_bytes"]:
            raise ForgeError(
                "Deployment device reservation exceeds capacity", code="resource_limit"
            )
        candidate = (
            target
            if int(d) == device
            else next(o["path"] for o in [*outputs, *object_targets] if o["device"] == int(d))
        )
        candidate = Path(candidate)
        while not candidate.exists():
            candidate = candidate.parent
        remaining = sum(
            max(
                0,
                o["devices"].get(d, 0)
                - o.get("allocated_credit", {}).get(d, 0)
                - max(
                    0,
                    allocated[id(o)].get(d, 0)
                    - o.get("allocated_baseline", {}).get(d, 0),
                ),
            )
            for o in live
        )
        if (
            shutil.disk_usage(candidate).free
            < max(0, amount - own["allocated_credit"].get(d, 0)) + remaining
        ):
            raise ForgeError(
                "Insufficient device space for managed reservations",
                code="resource_limit",
            )
    number = free[0]
    if len(json.dumps(own).encode()) > 128 * 1024:
        raise ForgeError("Deployment lease exceeds byte budget", code="resource_limit")
    atomic_write_json(path / f"slot-{number}.json", own)
    return dict(path=str(path), slot=number)


def reserved(root, run_id, attempt):
    path = host_partition.location()
    for number in range(16):
        slot = path / f"slot-{number}.json"
        if slot.exists():
            owner = read_json(slot, maximum=128 * 1024)
            if (owner["root"], owner["run_id"], owner["attempt"]) == (
                str(root),
                run_id,
                attempt,
            ):
                return True
    return False


def abandon(path, slot, *, run_id, attempt):
    """Only before spawning any execution; caller still holds domain lock."""
    target = path / f"slot-{slot}.json"
    owner = read_json(target, maximum=128 * 1024)
    if (owner["run_id"], owner["attempt"]) != (run_id, attempt):
        raise ForgeError("Deployment owner changed")
    target.unlink()
    fsync_directory(path)


_HISTORY_ENTRIES = 65536
_HISTORY_RUNS = 4096
_HISTORY_DEPTH = 24
_HISTORY_SECONDS = 30
_HISTORY_CPU_SECONDS = 5
_HISTORY_SLOW_SECONDS = 5
_HISTORY_WORKERS = 4
_HISTORY_BUFFER_BYTES = 128 * MiB
_HISTORY_SKIP = frozenset({"work", "evidence", "checkpoints"})
_HISTORY_ACTIVE = frozenset({"starting", "running", "stopping"})
_HISTORY_STATES = _HISTORY_ACTIVE | {None, "prepared", "succeeded", "failed", "cancelled", "interrupted"}


class _HistoryCPUExceeded(ForgeError):
    pass


class _HistoryCPUExit:
    """A transient pool thread records its final clock during TLS teardown."""
    def __init__(self, budget, ident):
        self.budget, self.ident = budget, ident

    def __del__(self):
        try:
            self.budget.finish_worker(self.ident)
        except BaseException:
            self.budget.unavailable = True


class _HistoryCPU:
    """At most this caller and four new pool threads; never process CPU."""
    def __init__(self, started):
        self._lock = threading.Lock()
        self._main = threading.get_ident()
        self._clocks = {self._main: started}
        self._workers = set()
        self._finished = set()
        self._local = threading.local()
        self.used_ns = 0
        self.unavailable = False

    def _sample(self, ident, now):
        previous = self._clocks[ident]
        if now < previous:
            self.unavailable = True
        else:
            self.used_ns += now - previous
        self._clocks[ident] = now

    def start_worker(self):
        ident = threading.get_ident()
        with self._lock:
            if ident in self._clocks or len(self._workers) >= _HISTORY_WORKERS:
                raise ForgeError('History CPU participants are unavailable', code='resource_limit')
            self._workers.add(ident)
            # A newly created pool thread belongs entirely to this scan.
            self._clocks[ident] = 0
        self._local.exit = _HistoryCPUExit(self, ident)
        self.tick()

    def tick(self):
        ident, now = threading.get_ident(), time.thread_time_ns()
        with self._lock:
            if ident not in self._clocks or ident in self._finished:
                self.unavailable = True
            else:
                self._sample(ident, now)
            used = self.used_ns
        if self.unavailable:
            raise ForgeError('History CPU coverage is unavailable', code='resource_limit')
        if used > _HISTORY_CPU_SECONDS * 10**9:
            raise self.exceeded()

    def finish_worker(self, ident):
        if threading.get_ident() != ident:
            self.unavailable = True
            return
        with self._lock:
            self._sample(ident, time.thread_time_ns())
            self._finished.add(ident)

    def finish_main(self):
        with self._lock:
            self._sample(self._main, time.thread_time_ns())
            self._finished.add(self._main)

    def exceeded(self):
        return _HistoryCPUExceeded(
            f'History CPU budget exceeded; used {self.used_ns / 10**9:.6f}s', code='resource_limit')

    def require_complete(self):
        if self.unavailable or self._finished != set(self._clocks):
            raise ForgeError('History final CPU coverage is unavailable', code='resource_limit')
        if self.used_ns > _HISTORY_CPU_SECONDS * 10**9:
            raise self.exceeded()


class _HistoryScan:
    """One admission, no retained state; directory FDs grow only with depth."""

    def __init__(self, *, cpu=None, deadline=None):
        self.cpu = cpu or _HistoryCPU(time.thread_time_ns())
        self.operation_deadline = operation_budget.current_deadline()
        self.started = time.monotonic()
        limit = self.started + _HISTORY_SECONDS if deadline is None else deadline
        self.deadline = min(limit, self.operation_deadline) if self.operation_deadline is not None else limit
        self.slow = False
        self.remaining = _HISTORY_ENTRIES
        self._counter = threading.Lock()
        self.metadata = threading.Lock()
        self.stopped = threading.Event()
        self.failure = None
        self.buffer_bytes = 0

    def check(self, depth=0):
        if self.stopped.is_set():
            raise ForgeError("History traversal stopped after another failure", code="resource_limit")
        self.cpu.tick()
        if depth > _HISTORY_DEPTH:
            raise ForgeError("History traversal exceeds depth budget", code="resource_limit")
        now = time.monotonic()
        self.slow = self.slow or now - self.started >= _HISTORY_SLOW_SECONDS
        if now >= self.deadline:
            if self.operation_deadline is not None and now >= self.operation_deadline:
                raise ForgeError('Control operation deadline exceeded during history', code='resource_limit')
            raise ForgeError("History traversal exceeds wait budget", code="resource_limit")

    def take(self, depth):
        self.check(depth)
        with self._counter:
            if self.remaining <= 0:
                raise ForgeError("History traversal exceeds entry budget", code="resource_limit")
            self.remaining -= 1

    def fail(self, error):
        with self._counter:
            if self.failure is None:
                self.failure = error
            self.stopped.set()

    def buffer_reserve(self, amount):
        with self._counter:
            self.check()
            if self.buffer_bytes + amount > _HISTORY_BUFFER_BYTES:
                raise ForgeError("History entry buffer exceeds management budget", code="resource_limit")
            self.buffer_bytes += amount

    def buffer_release(self, amount):
        with self._counter:
            self.buffer_bytes -= amount

    @contextmanager
    def entries(self, fd, depth):
        # Prepay list header/capacity, pair+cost objects, Unicode/name/path,
        # UTF-8 caches and a conservative 1 KiB per future stat result graph.
        # The 32 B/slot reservation remains until the list itself is freed.
        values = []
        self.buffer_reserve(1024)
        entry = None
        try:
            with os.scandir(fd) as children:
                for entry in children:
                    self.take(depth)
                    cost = (sys.getsizeof(entry) + sys.getsizeof(entry.name) +
                            sys.getsizeof(entry.path) + 1024 + 512 + 128)
                    self.buffer_reserve(cost + 32)
                    try:
                        values.append((entry, cost))
                    except BaseException:
                        self.buffer_release(cost + 32)
                        raise
                    if sys.getsizeof(values) > 1024 + 32 * len(values):
                        raise ForgeError("Unknown history list allocation", code="resource_limit")
            # Closing the iterator before DFS leaves one directory FD/level.
            entry = None
            yield values
        finally:
            entry = None
            remaining = 1024 + 32 * len(values) + sum(item[1] for item in values if item is not None)
            values.clear()
            self.buffer_release(remaining)

    @contextmanager
    def workers(self):
        # Linux affinity is per calling thread. The transient workers inherit
        # this same one-CPU mask; other service threads are never changed.
        original = os.sched_getaffinity(0)
        if not original:
            raise ForgeError("Historical CPU reservation is unavailable", code="resource_limit")
        cpu = {min(original)}
        pool = None
        try:
            os.sched_setaffinity(0, cpu)
            pool = ThreadPoolExecutor(max_workers=_HISTORY_WORKERS, thread_name_prefix="forge-history",
                                      initializer=self.cpu.start_worker)
            yield pool
        finally:
            try:
                if pool is not None:
                    # Never return/release caller locks with background scans.
                    pool.shutdown(wait=True, cancel_futures=True)
                    os.sched_yield()
            finally:
                os.sched_setaffinity(0, original)

    @staticmethod
    def identity(info):
        return info.st_dev, info.st_ino

    @staticmethod
    def changed(before, after):
        # Some filesystems can expose equal timestamps for adjacent mutations.
        # Retained directory size/link changes are still evidence of mutation.
        return (before.st_mtime_ns, before.st_ctime_ns, before.st_size, before.st_nlink) != (
            after.st_mtime_ns, after.st_ctime_ns, after.st_size, after.st_nlink)

    @contextmanager
    def run_set(self, parent):
        """Bounded names/inodes only; timestamps need not resolve each mutation."""
        identities = {}
        reserved = 0
        try:
            self.buffer_reserve(1024)
            reserved = 1024
            with os.scandir(parent) as entries:
                for count, entry in enumerate(entries):
                    self.check()
                    if count >= _HISTORY_RUNS or entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                        raise ForgeError("History traversal exceeds run budget or has invalid entry", code="resource_limit")
                    # Prepay the key, identity tuple/integers and dict capacity.
                    cost = sys.getsizeof(entry.name) + 512
                    self.buffer_reserve(cost)
                    reserved += cost
                    identities[entry.name] = self.identity(entry.stat(follow_symlinks=False))
            yield
            count = 0
            with os.scandir(parent) as entries:
                for entry in entries:
                    self.check()
                    count += 1
                    if (count > _HISTORY_RUNS or entry.is_symlink() or
                            not entry.is_dir(follow_symlinks=False) or
                            identities.get(entry.name) != self.identity(entry.stat(follow_symlinks=False))):
                        raise ForgeError("Historical run set changed during admission", code="busy")
            if count != len(identities):
                raise ForgeError("Historical run set changed during admission", code="busy")
        finally:
            identities.clear()
            self.buffer_release(reserved)

    def _absolute(self, path):
        path = Path(path).absolute()
        if ".." in path.parts or len(os.fsencode(path)) > 4096:
            raise ForgeError("Historical root path exceeds budget")
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for name in path.parts[1:]:
                self.check()
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            return fd
        except BaseException:
            os.close(fd)
            raise

    def linked(self, parent, name, info):
        self.check()
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if self.identity(current) != self.identity(info) or stat.S_IFMT(current.st_mode) != stat.S_IFMT(info.st_mode):
            raise ForgeError("Historical path identity changed", code="busy")
        return current

    @contextmanager
    def directory(self, path, *, parent=None, missing=False, expected=None):
        self.check()
        try:
            fd = (self._absolute(path) if parent is None else os.open(
                path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent))
        except FileNotFoundError:
            if not missing:
                raise
            yield None
            # Missing is only allowed for .forge/runs in an empty workspace.
            try:
                os.stat(path, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return
            raise ForgeError("Historical directory appeared during admission", code="busy")
        try:
            info = os.fstat(fd)
            if expected is not None and self.identity(info) != self.identity(expected):
                raise ForgeError("Historical directory identity changed", code="busy")
            yield fd
            self.check()
            if parent is None:
                verify = self._absolute(path)
                try:
                    if self.identity(os.fstat(verify)) != self.identity(info):
                        raise ForgeError("Historical root identity changed", code="busy")
                finally:
                    os.close(verify)
            else:
                self.linked(parent, path, info)
        finally:
            os.close(fd)

    def bytes(self, fd, *, depth=0, run_root=False):
        self.check(depth)
        before = os.fstat(fd)
        total = 0
        with self.entries(fd, depth) as entries:
            for number in range(len(entries)):
                entry, cost = entries[number]
                try:
                    self.check(depth)
                    if entry.is_symlink():
                        raise ForgeError("History traversal contains symlink", code="resource_limit")
                    if run_root and depth == 0 and entry.name in _HISTORY_SKIP:
                        info = entry.stat(follow_symlinks=False)
                        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                            raise ForgeError("Unexpected historical file type")
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        self.check(depth + 1)
                        expected = entry.stat(follow_symlinks=False)
                        with self.directory(entry.name, parent=fd, expected=expected) as child:
                            total += self.bytes(child, depth=depth + 1)
                    else:
                        info = entry.stat(follow_symlinks=False)
                        if not stat.S_ISREG(info.st_mode):
                            raise ForgeError("Unexpected historical file type")
                        total += info.st_size
                    self.check(depth)
                finally:
                    entries[number] = None
                    entry = None
                    self.buffer_release(cost)
        after = os.fstat(fd)
        if self.changed(before, after):
            raise ForgeError("Historical directory changed during traversal", code="busy")
        self.check(depth)
        return total

    @contextmanager
    def journal(self, fd):
        self.check()
        try:
            descriptor = os.open("run.sqlite", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        except FileNotFoundError:
            yield None
            try:
                os.stat("run.sqlite", dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            raise ForgeError("Historical journal appeared during admission", code="busy")
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * MiB:
                raise ForgeError("Historical journal type/size exceeds budget", code="resource_limit")
            self.linked(fd, "run.sqlite", info)
            # Linux /proc anchor follows the already pinned run directory.
            # The storage reader remains mode=ro; no schema initialization.
            journal = Path("/proc/self/fd") / str(fd)
            yield journal
            current = self.linked(fd, "run.sqlite", info)
            if current.st_size > 64 * MiB:
                raise ForgeError("Historical journal exceeds budget", code="resource_limit")
        finally:
            os.close(descriptor)

    def snapshot(self, journal, run_id):
        from demiflow.workbench.runs.storage import storage_snapshot

        self.check()
        value = (storage_snapshot(journal) if journal is not None else
                 dict(state=None, manifest=None, identity=None, object_roots=[],
                      manifest_digest=None, identity_digest=None))
        self.check()
        state, manifest = value["state"], value["manifest"]
        if state is not None and ("status" not in state or
                                  not (state["status"] is None or type(state["status"]) is str) or
                                  state["status"] not in _HISTORY_STATES):
            raise ForgeError("Unknown historical execution state", code="resource_limit")
        if manifest is not None and "run_id" in manifest and manifest["run_id"] != run_id:
            raise ForgeError("Historical run identity differs", code="resource_limit")
        return value

    def run(self, parent, name, expected, target, admitting):
        """One independent reader/FD stack; only scalar billing leaves the job."""
        from demiflow.workbench.runs.storage import history_objects

        before = after = roots = None
        try:
            self.check()
            with self.directory(name, parent=parent, expected=expected) as run:
                with self.journal(run) as journal:
                    with self.metadata:
                        before = self.snapshot(journal, name)
                        roots = history_objects(before, target)
                    size = self.bytes(run, run_root=True)
                    for path in roots:
                        with self.directory(path) as owned:
                            size += self.bytes(owned)
                    with self.metadata:
                        after = self.snapshot(journal, name)
                        if any(before[k] != after[k] for k in ("manifest", "identity", "object_roots", "manifest_digest", "identity_digest")):
                            raise ForgeError("Historical storage ownership changed", code="busy")
                        if (before["state"] or {}).get("attempt") != (after["state"] or {}).get("attempt"):
                            raise ForgeError("Historical execution attempt changed", code="busy")
                        active = name == admitting or any(
                            (value["state"] or {}).get("status") in _HISTORY_ACTIVE
                            for value in (before, after))
                        # Release large metadata before another decoder enters.
                        before = after = roots = None
            return max(size, MANAGEMENT_RESERVE) if active else size
        except BaseException as exc:
            self.fail(exc)
            raise
        finally:
            before = after = roots = None

    def total(self, parent, root, admitting, capacity):
        """Stream at most four in-flight jobs; no full run or result list."""
        total = 0
        pending, done = set(), set()

        def collect(completed):
            nonlocal total
            for job in completed:
                total += job.result()
                capacity(total)
            completed.clear()

        try:
            with self.workers() as pool:
                try:
                    with os.scandir(parent) as entries:
                        for count, entry in enumerate(entries):
                            self.check()
                            if count >= _HISTORY_RUNS or entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                                raise ForgeError("History traversal exceeds run budget or has invalid entry", code="resource_limit")
                            expected = entry.stat(follow_symlinks=False)
                            target = Path(root).absolute() / ".forge/runs" / entry.name
                            with self._counter:
                                self.check()
                                pending.add(pool.submit(self.run, parent, entry.name, expected, target, admitting))
                            if len(pending) == _HISTORY_WORKERS:
                                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                                collect(done)
                        while pending:
                            done, pending = wait(pending, return_when=FIRST_COMPLETED)
                            collect(done)
                except BaseException as exc:
                    self.fail(exc)
                    for job in pending:
                        job.cancel()
                    raise
        except BaseException as exc:
            # The executor has joined every worker at this point. Drop worker
            # tracebacks (which may retain metadata) before returning an error.
            error = self.failure or exc
            for job in pending | done:
                if not job.cancelled():
                    failure = job.exception()
                    if failure is not None:
                        failure.__traceback__ = None
            self.failure = None
            raise error.with_traceback(None)
        return total


def history_admission(root, *, reserve=MANAGEMENT_RESERVE, ceiling=None, admitting=None):
    """Recount actual retained paths and reserve active/admitting growth.

    Caller holds the workspace control lock. Prepare never holds execution
    capacity; start independently rechecks. Short native SQL snapshots release
    transactions before filesystem traversal. There is no retained usage cache
    and these checks do not provide an atomic filesystem snapshot.
    """
    cpu = _HistoryCPU(time.thread_time_ns())
    entered = time.monotonic()
    total = 0
    cap = ceiling or host_partition.DEFAULTS["history_bytes"]
    error = None

    def capacity(value):
        if value + reserve > cap:
            raise ForgeError("Run history capacity is full; explicit retention required", code="resource_limit")

    try:
        with operation_budget.scope() as parent_deadline:
            scan = _HistoryScan(cpu=cpu, deadline=min(entered + _HISTORY_SECONDS, parent_deadline))
            scan.started = entered
            scan.check()
            domain = host_partition.location()
            if (domain / "host-budget.json").exists():
                from demiflow.workbench.host_partition import load
                load(domain, host_partition.policy(domain), root=root, admit=True)
            scan.check()
            with scan.directory(Path(root).absolute()) as workspace:
                with scan.directory(".forge", parent=workspace, missing=True) as forge:
                    if forge is not None:
                        with scan.directory("runs", parent=forge, missing=True) as parent:
                            if parent is not None:
                                parent_before = os.fstat(parent)
                                with scan.run_set(parent):
                                    total = scan.total(parent, root, admitting, capacity)
                                if scan.changed(parent_before, os.fstat(parent)):
                                    raise ForgeError("Historical run set changed during admission", code="busy")
            scan.check()
            capacity(total)
    except BaseException as exc:
        error = exc
    finally:
        # All worker TLS final clocks have been recorded after executor join.
        # Retain their totals, then include this caller's final cleanup CPU.
        cpu.finish_main()
    if error is not None:
        if isinstance(error, _HistoryCPUExceeded) or cpu.used_ns > _HISTORY_CPU_SECONDS * 10**9:
            raise cpu.exceeded() from None
        if isinstance(error, ForgeError):
            raise error
        if isinstance(error, (OSError, ValueError, TypeError, KeyError, AttributeError,
                              MemoryError, sqlite3.Error, RuntimeError)):
            raise ForgeError("Historical resource coverage is unavailable: " + str(error)[:512], code="resource_limit") from error
        raise error
    cpu.require_complete()
    return total
