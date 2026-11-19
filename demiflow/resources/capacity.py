"""Local deployment-domain capacity: atomic reserve, bind physical identity,
release only on exit evidence. Unknown cleanup keeps charging.

This is physical admission for one machine domain, not the RunJournal
cumulative cost ledger. Ray placement is charged by Ray Core, never here.
"""
import fcntl
import hashlib
from contextvars import ContextVar
from contextlib import contextmanager
import json
import os
import stat
from pathlib import Path
import time

from .contracts import ResourceError, document, identifier, spec_hash

_thread_tracker = ContextVar("demiflow_owned_threads", default=None)


def track_owned_thread(thread):
    tracker = _thread_tracker.get()
    if tracker is not None:
        tracker(thread)


_thread_envelope = ContextVar("demiflow_allocation_envelope", default=None)


@contextmanager
def allocation_envelope(value, *, track_thread=None):
    """An admitted same-process host slice, without changing process env."""
    if type(value) is not dict or set(value) != _ENVELOPE_KEYS:
        raise ResourceError("INVALID_SPEC", "thread envelope has an unsupported shape")
    document(value, 4096)
    token = _thread_envelope.set(dict(value))
    tracker_token = _thread_tracker.set(track_thread)
    try:
        yield
    finally:
        _thread_envelope.reset(token)
        _thread_tracker.reset(tracker_token)


ENVELOPE_ENV = "DEMIFLOW_RESOURCE_ENVELOPE"
MAX_ACTIVE_RECORDS = 1024
_ENVELOPE_KEYS = frozenset({"allocation_id", "spec_hash", "cpu_millis", "memory_bytes", "resource_directory"})


def _save_atomic(path, value):
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


def write_envelope(path, envelope):
    """Parent-owned slice handed to a managed worker before it spawns."""
    if type(envelope) is not dict or set(envelope) != _ENVELOPE_KEYS:
        raise ResourceError("INVALID_SPEC", "resource envelope key set is unsupported")
    identifier(envelope["allocation_id"])
    if type(envelope["spec_hash"]) is not str or len(envelope["spec_hash"]) != 64:
        raise ResourceError("INVALID_SPEC", "envelope spec_hash must be sha256 hex")
    for name in ("cpu_millis", "memory_bytes"):
        if type(envelope[name]) is not int or envelope[name] <= 0:
            raise ResourceError("INVALID_SPEC", "envelope " + name + " must be positive")
    if type(envelope["resource_directory"]) is not str or not envelope["resource_directory"]:
        raise ResourceError("INVALID_SPEC", "envelope needs a private resource_directory")
    _save_atomic(Path(path), envelope)


def inherited_envelope():
    """Read the parent allocation slice inside a managed worker.

    Absent means an independent consumer (root reservation path). Present but
    malformed fails closed: a managed worker must not silently fall back to
    whole-machine defaults and double-charge the deployment domain.
    """
    current = _thread_envelope.get()
    if current is not None:
        return dict(current)
    value = os.environ.get(ENVELOPE_ENV)
    if value is None:
        return None
    with open(value, "rb") as stream:
        raw = stream.read(4097)
    if len(raw) > 4096:
        raise ValueError("Resource envelope exceeds byte budget")
    envelope = json.loads(raw)
    if type(envelope) is not dict or set(envelope) != _ENVELOPE_KEYS:
        raise ValueError("Invalid resource envelope key set")
    identifier(envelope["allocation_id"])
    for name in ("cpu_millis", "memory_bytes"):
        if type(envelope[name]) is not int or envelope[name] <= 0:
            raise ValueError("Invalid resource envelope " + name)
    if type(envelope["resource_directory"]) is not str or not envelope["resource_directory"]:
        raise ValueError("Invalid resource envelope directory")
    return envelope


class LocalCapacityPool:
    """One normalized root, one file lock, bounded atomic per-allocation records."""

    def __init__(self, root, *, capacity, deadline=None):
        self.root = Path(root).resolve()
        if type(capacity) is not dict or not {"cpu_millis", "memory_bytes"} <= set(capacity) or set(capacity) - {
                "cpu_millis", "memory_bytes", "scratch_bytes"}:
            raise ResourceError("INVALID_SPEC", "pool capacity needs cpu_millis/memory_bytes")
        if any(type(v) is not int or v <= 0 for v in capacity.values()):
            raise ResourceError("INVALID_SPEC", "pool capacity values must be positive ints")
        self.capacity = dict(capacity)
        if deadline is not None:
            import math
            if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= 0:
                raise ResourceError('INVALID_SPEC', 'capacity deadline must be a finite monotonic time')
        self._deadline = deadline
        self.root.mkdir(parents=True, exist_ok=True)
        self._records = self.root / "allocations"
        self._records.mkdir(exist_ok=True)
        self._released = self.root / "released"

    def _lock(self):
        path = self.root / "capacity.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            if self._deadline is None:
                fcntl.flock(fd, fcntl.LOCK_EX)
            else:
                while True:
                    remaining = self._deadline - time.monotonic()
                    if remaining <= 0:
                        raise ResourceError('CLEANUP_UNCONFIRMED', 'capacity cleanup lock deadline expired')
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        time.sleep(min(.01, remaining))
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _released_path(self, allocation_id):
        # Exact lookup, never a scan of unbounded historical allocations.
        bucket = hashlib.sha256(allocation_id.encode("utf-8")).hexdigest()[:2]
        return self._released / bucket / (allocation_id + ".json")

    @staticmethod
    def _read_file(path, allocation_id):
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ResourceError("INVALID_SPEC", "capacity record is not a regular file")
            try:
                raw = stream.read(8193)
            except OSError as exc:
                raise ResourceError("INVALID_SPEC", "capacity record cannot be read") from exc
        if len(raw) > 8192:
            raise ResourceError("INVALID_SPEC", "capacity record exceeds byte budget")
        record = json.loads(raw)
        if type(record) is not dict or record.get("allocation_id") != allocation_id:
            raise ResourceError("INVALID_SPEC", "capacity record identity differs")
        return record, raw

    def _read(self, allocation_id):
        current = self._read_file(self._records / (allocation_id + ".json"), allocation_id)
        archive_path = self._released_path(allocation_id)
        if self._released.is_symlink() or archive_path.parent.is_symlink():
            raise ResourceError("INVALID_SPEC", "capacity archive directory is a symlink")
        archived = self._read_file(archive_path, allocation_id)
        if archived is not None:
            if archived[0].get("state") != "RELEASED":
                raise ResourceError("INVALID_SPEC", "archived capacity record is not released")
            if current is not None and current[1] != archived[1]:
                raise ResourceError("INVALID_SPEC", "capacity history conflicts with active directory")
        selected = current if current is not None else archived
        return None if selected is None else selected[0]

    @staticmethod
    def _sync_directory(path):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _archive_released(self, allocation_id):
        """Same lock/record authority; interrupted moves remain retryable.

        Link before unlink preserves the original inode and byte-for-byte exit
        evidence. A crash can leave both names, but cannot lose the only durable
        name. Differing duplicates fail closed instead of overwriting history.
        """
        source = self._records / (allocation_id + ".json")
        current = self._read_file(source, allocation_id)
        if current is None or current[0].get("state") != "RELEASED":
            return
        destination = self._released_path(allocation_id)
        self._released.mkdir(exist_ok=True)
        self._sync_directory(self.root)
        self._sync_directory(self._released)
        destination.parent.mkdir(exist_ok=True)
        self._sync_directory(self._released)
        self._sync_directory(destination.parent)
        try:
            os.link(source, destination, follow_symlinks=False)
        except FileExistsError:
            archived = self._read_file(destination, allocation_id)
            if archived is None or current[1] != archived[1]:
                raise ResourceError("INVALID_SPEC", "capacity archive conflicts with original record")
        self._sync_directory(destination.parent)
        source.unlink()
        self._sync_directory(self._records)

    def _active(self):
        # At most two bounded pages per call, independent of total history.
        # Never admit from an incomplete active scan. Large legacy directories
        # make bounded progress and let the normal capacity retry try again.
        for attempt in range(2):
            records, released, overflow = [], [], False
            with os.scandir(self._records) as entries:
                for count, entry in enumerate(entries):
                    if count >= MAX_ACTIVE_RECORDS:
                        overflow = True
                        break
                    if not entry.name.endswith(".json"):
                        continue
                    allocation_id = entry.name[:-5]
                    identifier(allocation_id)
                    record = self._read(allocation_id)
                    if record is None:
                        continue
                    if record["state"] == "RELEASED":
                        released.append(allocation_id)
                    else:
                        records.append(record)
            if not overflow:
                return records
            if not released:
                raise ResourceError("QUOTA_EXCEEDED", "capacity record count exceeds active scan budget")
            for allocation_id in released:
                self._archive_released(allocation_id)
        raise ResourceError("CAPACITY_PENDING", "capacity history compaction made progress; retry bounded scan",
                            retriable=True, retry_after_ms=1)

    def _charged(self, records):
        charged = {
            "cpu_millis": sum(r["effective"]["cpu_millis"] for r in records),
            "memory_bytes": sum(r["effective"]["memory_bytes"] for r in records),
        }
        # Scratch joins the charge only when the domain declares it or a record
        # carries it; scratch-free domains keep the exact legacy shape.
        if "scratch_bytes" in self.capacity or any("scratch_bytes" in r["effective"] for r in records):
            charged["scratch_bytes"] = sum(r["effective"].get("scratch_bytes", 0) for r in records)
        return charged

    def _view(self, record):
        return {
            "allocation_id": record["allocation_id"], "spec_hash": record["spec_hash"],
            "state": record["state"], "effective": dict(record["effective"]),
            "identity": record.get("identity"),
        }

    def reserve(self, allocation_id, spec_hash_value, effective_profile, deadline_ms):
        identifier(allocation_id)
        if type(spec_hash_value) is not str or len(spec_hash_value) != 64:
            raise ResourceError("INVALID_SPEC", "spec_hash must be sha256 hex")
        if type(effective_profile) is not dict or not {"cpu_millis", "memory_bytes"} <= set(effective_profile):
            raise ResourceError("INVALID_SPEC", "effective profile needs cpu_millis/memory_bytes")
        if type(deadline_ms) is not int or deadline_ms <= 0:
            raise ResourceError("INVALID_SPEC", "deadline_ms must be positive")
        fd = self._lock()
        try:
            existing = self._read(allocation_id)
            if existing is not None:
                if existing["spec_hash"] != spec_hash_value:
                    raise ResourceError("INVALID_SPEC", "allocation spec is immutable", allocation_id=allocation_id)
                if existing["state"] == "RELEASED":
                    raise ResourceError("INVALID_SPEC", "released allocation cannot reserve again", allocation_id=allocation_id)
                return self._view(existing)
            active = self._active()
            if len(active) >= MAX_ACTIVE_RECORDS:
                raise ResourceError("QUOTA_EXCEEDED", "active capacity records exceed budget")
            charged = self._charged(active)
            for name in ("cpu_millis", "memory_bytes"):
                if charged[name] + effective_profile[name] > self.capacity[name]:
                    raise ResourceError(
                        "CAPACITY_PENDING", "deployment domain has no free " + name,
                        allocation_id=allocation_id, retriable=True, retry_after_ms=1000,
                    )
            if "scratch_bytes" in effective_profile:
                if "scratch_bytes" not in self.capacity:
                    raise ResourceError(
                        "UNSATISFIABLE_CONSTRAINT", "deployment domain declares no scratch capacity",
                        allocation_id=allocation_id)
                if charged.get("scratch_bytes", 0) + effective_profile["scratch_bytes"] > self.capacity["scratch_bytes"]:
                    raise ResourceError(
                        "CAPACITY_PENDING", "deployment domain has no free scratch_bytes",
                        allocation_id=allocation_id, retriable=True, retry_after_ms=1000,
                    )
            record = {
                "allocation_id": allocation_id, "spec_hash": spec_hash_value,
                "effective": {k: effective_profile[k] for k in ("cpu_millis", "memory_bytes")},
                "state": "RESERVED", "identity": None,
                "created_ms": int(time.time() * 1000), "deadline_ms": deadline_ms,
            }
            if "scratch_bytes" in effective_profile:
                record["effective"]["scratch_bytes"] = effective_profile["scratch_bytes"]
            _save_atomic(self._records / (allocation_id + ".json"), record)
            return self._view(record)
        finally:
            os.close(fd)

    def cancel_reservation(self, allocation_id):
        """Withdraw a never-bound reservation (no host ever existed).

        This is not an exit release: the record must be RESERVED with no physical
        identity, so no process can still be running. Deleting it keeps the
        allocation id reusable, which a release tombstone would forbid.
        """
        identifier(allocation_id)
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            if record is None:
                raise ResourceError("INVALID_SPEC", "cancel needs a known reservation", allocation_id=allocation_id)
            if record["state"] != "RESERVED" or record["identity"] is not None:
                raise ResourceError("INVALID_SPEC", "only an unbound reservation can be cancelled",
                                    allocation_id=allocation_id)
            os.remove(self._records / (allocation_id + ".json"))
            return {"allocation_id": allocation_id, "state": "CANCELLED"}
        finally:
            os.close(fd)

    def bind_identity(self, allocation_id, owned_process_identity):
        identifier(allocation_id)
        if type(owned_process_identity) is not dict or not {"pid", "start", "unit"} <= set(owned_process_identity):
            raise ResourceError("INVALID_SPEC", "process identity needs pid/start/unit")
        if type(owned_process_identity["pid"]) is not int or owned_process_identity["pid"] <= 0:
            raise ResourceError("INVALID_SPEC", "identity pid must be positive")
        document(owned_process_identity, 4096)
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            if record is None or record["state"] == "RELEASED":
                raise ResourceError("INVALID_SPEC", "bind requires an active reservation", allocation_id=allocation_id)
            if record["identity"] is not None and record["identity"] != owned_process_identity:
                raise ResourceError("INVALID_SPEC", "allocation already bound to another physical identity",
                                    allocation_id=allocation_id)
            record["identity"] = dict(owned_process_identity)
            record["state"] = "BOUND"
            _save_atomic(self._records / (allocation_id + ".json"), record)
            return self._view(record)
        finally:
            os.close(fd)

    def release(self, allocation_id, cleanup_evidence):
        """Exit, kill request, or a vanished PID alone never releases capacity."""
        identifier(allocation_id)
        if type(cleanup_evidence) is not dict or set(cleanup_evidence) != {"confirmed", "evidence_ref", "identity"}:
            raise ResourceError("INVALID_SPEC", "cleanup evidence key set is unsupported")
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            if record is None:
                raise ResourceError("INVALID_SPEC", "release needs a known reservation", allocation_id=allocation_id)
            if record["state"] == "RELEASED":
                if cleanup_evidence["confirmed"] and record.get("release_evidence") == cleanup_evidence["evidence_ref"]:
                    return {"allocation_id": allocation_id, "state": "RELEASED", "idempotent": True}
                raise ResourceError("CLEANUP_UNCONFIRMED", "released allocation lacks matching evidence",
                                    allocation_id=allocation_id)
            if not cleanup_evidence["confirmed"] or not cleanup_evidence["evidence_ref"]:
                raise ResourceError("CLEANUP_UNCONFIRMED", "exit evidence is not confirmed; capacity stays charged",
                                    allocation_id=allocation_id, retriable=True)
            if record["identity"] is not None and cleanup_evidence["identity"] != record["identity"]:
                raise ResourceError("CLEANUP_UNCONFIRMED", "cleanup evidence belongs to another physical identity",
                                    allocation_id=allocation_id)
            record["state"] = "RELEASED"
            record["release_evidence"] = cleanup_evidence["evidence_ref"]
            record["released_ms"] = int(time.time() * 1000)
            _save_atomic(self._records / (allocation_id + ".json"), record)
            return {"allocation_id": allocation_id, "state": "RELEASED", "idempotent": False}
        finally:
            os.close(fd)

    def snapshot(self):
        fd = self._lock()
        try:
            active = self._active()
            return {"charged": self._charged(active), "capacity": dict(self.capacity),
                    "active": [self._view(r) for r in active]}
        finally:
            os.close(fd)

    def reservation_view(self, allocation_id):
        identifier(allocation_id)
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            return None if record is None else self._view(record)
        finally:
            os.close(fd)
