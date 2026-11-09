"""Generic managed process unit: persistent identity first, controlled spawn,
birth-identity supervision, and exit-evidence cleanup.

No application roles, no command-string ownership. The unit lock handshake is
the same inherited-fd mechanism used by existing platform supervisors: the
parent locks unit.lock, the child inherits the locked descriptor for its whole
lifetime, and a free lock with matching file identity proves the unit stopped.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from .capacity import ENVELOPE_ENV, write_envelope
from .contracts import ResourceError, document, identifier
from .process_observation import (
    UNIT_ENV, _pidfd, _read, _signal_fd, descendants, require_pidfds,
    signal_owned, unit_stopped,
)

OWNER_ENV = "DEMIFLOW_RESOURCE_OWNER_PID"
OWNER_START_ENV = "DEMIFLOW_RESOURCE_OWNER_START"
BOOTSTRAP_DEADLINE_ENV = "DEMIFLOW_RESOURCE_BOOTSTRAP_DEADLINE_MS"
MAX_BOOTSTRAP_S = 300


def birth(pid):
    try:
        fields = _read(f"/proc/{pid}/stat").decode().rsplit(")", 1)[1].split()
        return fields[19], fields[0]
    except (FileNotFoundError, ProcessLookupError):
        return None


def birth_start(pid):
    value = birth(pid)
    return value[0] if value is not None else None


def _live(pid, start):
    value = birth(pid)
    return value is not None and value[0] == start and value[1] != "Z"


def process_identity(pid):
    start = birth_start(pid)
    if start is None:
        raise ResourceError("OUTCOME_UNKNOWN", "cannot read process birth identity")
    return {"pid": int(pid), "start": start}


def _owner_alive(pid, start):
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return _live(pid, start)


def bootstrap_guard(ready_path, *, poll_s=0.05):
    """Child-side fixed-term bootstrap: an ownerless orphan must exit.

    Readiness belongs to the worker protocol; this only bounds the window in
    which a bootstrap may linger after its owner disappeared.
    """
    owner = os.environ.get(OWNER_ENV)
    if owner is None:
        return
    pid = int(owner)
    start = os.environ.get(OWNER_START_ENV)
    deadline = int(os.environ.get(BOOTSTRAP_DEADLINE_ENV, "0")) / 1000.0
    while not Path(ready_path).exists():
        if not _owner_alive(pid, start) and time.time() >= deadline:
            raise SystemExit(75)
        time.sleep(poll_s)


def _signal_birth(pid, start, signum):
    try:
        fd = _pidfd(pid)
    except (FileNotFoundError, ProcessLookupError, OSError):
        return
    try:
        if _live(pid, start):
            _signal_fd(fd, signum)
    finally:
        os.close(fd)


def _save(path, value):
    raw = document(value)
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class ManagedProcessUnit:
    """One discoverable persistent record per allocation under a unit root."""

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def directory(self, scope_id, allocation_id):
        identifier(scope_id)
        identifier(allocation_id)
        return self.root / scope_id / allocation_id

    def _read_record(self, path):
        try:
            with open(path, "rb") as stream:
                raw = stream.read(8193)
        except FileNotFoundError:
            return None
        if len(raw) > 8192:
            raise ResourceError("INVALID_SPEC", "process unit record exceeds byte budget")
        return json.loads(raw)

    def prepare(self, scope_id, allocation_id, spec_hash):
        """Durable allocation identity before any spawn: recovery can find it."""
        directory = self.directory(scope_id, allocation_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "identity.json"
        record = self._read_record(path)
        value = {"scope_id": scope_id, "allocation_id": allocation_id,
                 "spec_hash": spec_hash, "created_ms": int(time.time() * 1000)}
        if record is None:
            _save(path, value)
            return directory
        if record["spec_hash"] != spec_hash:
            raise ResourceError("INVALID_SPEC", "persistent allocation spec differs", allocation_id=allocation_id)
        return directory

    def spawn(self, scope_id, allocation_id, spec_hash, argv, *, env=None,
              envelope=None, bootstrap_timeout_s=30, log_path=None,
              extra_env=None, extra_pass_fds=()):
        if type(argv) not in (tuple, list) or not argv or any(type(a) is not str for a in argv):
            raise ResourceError("INVALID_SPEC", "argv must be a nonempty string list")
        if not 0 < bootstrap_timeout_s <= MAX_BOOTSTRAP_S:
            raise ResourceError("INVALID_SPEC", "bootstrap timeout exceeds budget")
        if type(extra_pass_fds) not in (tuple, list) or any(type(fd) is not int for fd in extra_pass_fds):
            raise ResourceError("INVALID_SPEC", "extra_pass_fds must be integer descriptors")
        if extra_env is not None and (type(extra_env) is not dict
                                     or any(type(k) is not str or type(v) is not str for k, v in extra_env.items())):
            raise ResourceError("INVALID_SPEC", "extra_env must be a string map")
        directory = self.prepare(scope_id, allocation_id, spec_hash)
        existing = self._read_record(directory / "process.json")
        if existing is not None:
            # A recorded spawn is never blindly repeated; the provider inspects
            # the exact birth identity and either adopts it or retires it.
            raise ResourceError("OUTCOME_UNKNOWN", "allocation already has a spawn record; inspect first",
                                allocation_id=allocation_id)
        lock_path = directory / "unit.lock"
        unit_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        process = None
        try:
            info = os.fstat(unit_fd)
            try:
                fcntl.flock(unit_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ResourceError("OUTCOME_UNKNOWN", "unit lock is still owned; discover before creating",
                                    allocation_id=allocation_id) from None
            os.fsync(unit_fd)
            child_env = dict(os.environ if env is None else env)
            child_env[UNIT_ENV] = str(unit_fd)
            child_env[OWNER_ENV] = str(os.getpid())
            child_env[OWNER_START_ENV] = birth_start(os.getpid())
            child_env[BOOTSTRAP_DEADLINE_ENV] = str(int((time.time() + bootstrap_timeout_s) * 1000))
            if envelope is not None:
                envelope_path = directory / "envelope.json"
                write_envelope(envelope_path, envelope)
                child_env[ENVELOPE_ENV] = str(envelope_path)
            if extra_env:
                child_env.update(extra_env)
            stdout = subprocess.DEVNULL
            if log_path is not None:
                stdout = open(log_path, "ab")
            try:
                process = subprocess.Popen(
                    list(argv), env=child_env, stdin=subprocess.DEVNULL,
                    stdout=stdout, stderr=subprocess.STDOUT,
                    start_new_session=True, pass_fds=(unit_fd, *extra_pass_fds),
                )
            finally:
                if log_path is not None:
                    stdout.close()
            start = birth_start(process.pid)
            if start is None:
                raise ResourceError("OUTCOME_UNKNOWN", "spawned process vanished before identity binding",
                                    allocation_id=allocation_id)
            identity = {"pid": process.pid, "start": start,
                        "unit": {"device": info.st_dev, "inode": info.st_ino}}
            _save(directory / "process.json", {
                "identity": identity, "spec_hash": spec_hash,
                "argv_digest": hashlib.sha256(document(list(argv))).hexdigest(),
                "spawned_ms": int(time.time() * 1000),
            })
            return {"identity": identity, "directory": str(directory), "process": process}
        except BaseException:
            if process is not None and process.poll() is None:
                start = birth_start(process.pid)
                if start is not None:
                    _signal_birth(process.pid, start, signal.SIGKILL)
                process.wait(timeout=5)
            raise
        finally:
            os.close(unit_fd)

    def inspect(self, scope_id, allocation_id, *, process=None):
        if process is not None:
            process.poll()
        directory = self.directory(scope_id, allocation_id)
        record = self._read_record(directory / "process.json")
        if record is None:
            return {"phase": "REQUESTED", "observation": "FRESH", "identity": None,
                    "evidence_ref": str(directory / "identity.json")}
        identity = record["identity"]
        pid, start = identity["pid"], identity["start"]
        live = _live(pid, start)
        stopped = unit_stopped(directory / "unit.lock", identity["unit"])
        if live and not stopped:
            return {"phase": "RUNNING", "observation": "FRESH", "identity": identity,
                    "evidence_ref": str(directory / "process.json")}
        if not live and stopped:
            return {"phase": "TERMINATED", "observation": "FRESH", "identity": identity,
                    "evidence_ref": str(directory / "process.json")}
        # PID reused, lock busy, or unreadable proc: unknown is not terminal.
        return {"phase": "RUNNING", "observation": "STALE_UNKNOWN", "identity": identity,
                "evidence_ref": str(directory / "process.json")}

    def stop(self, scope_id, allocation_id, *, terminate_deadline_ms, process=None):
        directory = self.directory(scope_id, allocation_id)
        record = self._read_record(directory / "process.json")
        if record is None:
            raise ResourceError("INVALID_SPEC", "stop needs a recorded spawn", allocation_id=allocation_id)
        identity = record["identity"]
        pid, start = identity["pid"], identity["start"]
        deadline = time.monotonic() + max(0, terminate_deadline_ms) / 1000.0
        if _live(pid, start):
            _signal_birth(pid, start, signal.SIGTERM)
            signal_owned(pid, signal.SIGTERM)
            while time.monotonic() < deadline and _live(pid, start):
                if process is not None:
                    process.poll()
                time.sleep(0.02)
            if _live(pid, start):
                _signal_birth(pid, start, signal.SIGKILL)
                signal_owned(pid, signal.SIGKILL)
                while time.monotonic() < deadline and _live(pid, start):
                    if process is not None:
                        process.poll()
                    time.sleep(0.02)
        if process is not None:
            # Reap our own child after signalling; a zombie with a matching birth
            # identity is not a live host, but leaving it unreaped leaks a PID.
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        stopped = unit_stopped(directory / "unit.lock", identity["unit"])
        gone = not _live(pid, start)
        descendants_empty = not descendants(pid) if gone else False
        confirmed = stopped and gone and descendants_empty
        evidence = {
            "unit_stopped": stopped, "birth_identity_gone": gone,
            "descendants_empty": descendants_empty,
            "checked_ms": int(time.time() * 1000),
        }
        _save(directory / "cleanup.json", evidence)
        return {
            "confirmed": confirmed,
            "evidence_ref": str(directory / "cleanup.json"),
            "identity": identity,
        }