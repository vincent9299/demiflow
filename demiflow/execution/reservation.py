"""Engine-side call admission: four-dimension pre-call reservation (U4/S2).

The gate reserves input bytes / inflight slots / output bytes / effect units
BEFORE a paid API/GPU call; an unreserved call must not happen. This is capacity
accounting, not a second effect authority: durable effect facts stay in the
existing journals (LLM PromptJournal et al.), whose reserve/lookup semantics the
effect dimension composes with (known-completed reuse consumes no new budget;
request-without-response is unknown and stays charged). Unconfirmed reservations
keep charging across controller restarts; recovery overlap is re-reserved
explicitly (idempotent for the same call, conservatively double-charged for a
different call attempting the same effect).
"""
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time

from .recovery_contract import RecoveryContractError, document, identifier

MAX_ACTIVE_RECORDS = 1024
_RECORD_BYTES = 8192
_DIMENSIONS = ("input_bytes", "inflight_slots", "output_bytes", "effect_units")


class ReservationError(RecoveryContractError):
    """A call cannot be admitted or released under the recorded reservation."""

    def __init__(self, code, message, *, call_id=None, retriable=False):
        super().__init__(message)
        self.code = code
        self.call_id = call_id
        self.retriable = retriable


@dataclass(frozen=True)
class CallBudget:
    input_bytes: int
    inflight_slots: int
    output_bytes: int
    effect_units: int

    def __post_init__(self):
        for name in _DIMENSIONS:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ReservationError("INVALID_SPEC", "budget " + name + " must be a positive integer")


def usable_budget(total, *, reserved=None):
    """Hold back control-plane / downstream-commit / lease capacity off the top
    (v1 §07 deadlock_prevention, role-minimum composition); data calls reserve
    only against the remainder so saturation never starves progress/commit/lease.
    ``total`` names exactly the four dimensions; ``reserved`` is a partial holdback
    (zeros elsewhere). A holdback that leaves no usable capacity is infeasible and
    rejected before any call, mirroring plan_activation's control_reserve."""
    if type(total) is not dict or set(total) != set(_DIMENSIONS):
        raise ReservationError("INVALID_SPEC", "total budget must name exactly the four dimensions")
    reserved = {} if reserved is None else reserved
    if type(reserved) is not dict or set(reserved) - set(_DIMENSIONS):
        raise ReservationError("INVALID_SPEC", "reserved holdback names unknown dimensions")
    usable = {}
    for name in _DIMENSIONS:
        amount, holdback = total[name], reserved.get(name, 0)
        if type(amount) is not int or amount <= 0 or type(holdback) is not int or holdback < 0:
            raise ReservationError("INVALID_SPEC", "budget positive / holdback nonnegative integers required")
        if holdback >= amount:
            raise ReservationError("INVALID_SPEC", "holdback leaves no usable " + name)
        usable[name] = amount - holdback
    return CallBudget(**usable)


def _save_atomic(path, value):
    raw = document(value, _RECORD_BYTES)
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


class ReserveGate:
    """One normalized root, one file lock, bounded atomic per-call records.

    A reservation is withdrawable only by a confirmed outcome (completed or
    cancelled). An unknown effect (request sent, no durable response) is NOT
    releasable: it stays charged and surfaces via ``outstanding`` so recovery
    marks it UNKNOWN fail-closed instead of blind-retrying.
    """

    def __init__(self, root, *, budget):
        if type(budget) is not CallBudget:
            raise ReservationError("INVALID_SPEC", "gate needs a CallBudget")
        self.root = Path(root).resolve()
        self.budget = budget
        self.root.mkdir(parents=True, exist_ok=True)
        self._records = self.root / "reservations"
        self._records.mkdir(exist_ok=True)

    def _lock(self):
        path = self.root / "reservations.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _read(self, call_id):
        path = self._records / (call_id + ".json")
        try:
            with open(path, "rb") as stream:
                raw = stream.read(_RECORD_BYTES + 1)
        except FileNotFoundError:
            return None
        if len(raw) > _RECORD_BYTES:
            raise ReservationError("INVALID_SPEC", "reservation record exceeds byte budget", call_id=call_id)
        record = json.loads(raw)
        if type(record) is not dict or record.get("call_id") != call_id:
            raise ReservationError("INVALID_SPEC", "reservation record identity differs", call_id=call_id)
        return record

    def _active(self):
        records = []
        with os.scandir(self._records) as entries:
            for count, entry in enumerate(entries):
                if count >= MAX_ACTIVE_RECORDS + 1:
                    raise ReservationError("QUOTA_EXCEEDED", "reservation record count exceeds budget")
                if not entry.name.endswith(".json") or entry.name.endswith(".tmp"):
                    continue
                record = self._read(entry.name[:-5])
                if record is not None and record["state"] != "RELEASED":
                    records.append(record)
        return records

    def _charged(self, records):
        charged = {"input_bytes": 0, "inflight_slots": 0, "output_bytes": 0, "effect_units": 0}
        for record in records:
            spec = record["spec"]
            charged["input_bytes"] += spec["input_bytes"]
            charged["inflight_slots"] += 1
            charged["output_bytes"] += spec["output_bytes"]
            charged["effect_units"] += spec["effect_units"]
        return charged

    def _view(self, record):
        return {"call_id": record["call_id"], "state": record["state"],
                "spec": dict(record["spec"]), "spec_hash": record["spec_hash"]}

    def reserve(self, call_id, *, input_bytes, output_bytes, effect_units=1):
        """Reserve all four dimensions or none; the call may proceed only after
        this returns. ``output_bytes`` is the declared maximum output residency:
        an undeclared/unbounded output is a precheck defect, rejected here, never
        discovered after the call with nowhere to save."""
        identifier(call_id)
        spec = {"input_bytes": input_bytes, "output_bytes": output_bytes, "effect_units": effect_units}
        for name in ("input_bytes", "effect_units"):
            if type(spec[name]) is not int or spec[name] <= 0:
                raise ReservationError("INVALID_SPEC", name + " must be a positive integer", call_id=call_id)
        if type(output_bytes) is not int or output_bytes <= 0:
            raise ReservationError("INVALID_SPEC", "output_bytes must declare a positive maximum (bounded chunk/spill or declared ceiling)",
                                   call_id=call_id)
        spec_hash = hashlib.sha256(document(spec)).hexdigest()
        fd = self._lock()
        try:
            existing = self._read(call_id)
            if existing is not None:
                if existing["spec_hash"] != spec_hash:
                    raise ReservationError("INVALID_SPEC", "reservation spec is immutable", call_id=call_id)
                if existing["state"] == "RELEASED":
                    raise ReservationError("INVALID_SPEC", "released call cannot reserve again", call_id=call_id)
                return self._view(existing)
            active = self._active()
            if len(active) >= MAX_ACTIVE_RECORDS:
                raise ReservationError("QUOTA_EXCEEDED", "active reservations exceed budget", call_id=call_id)
            charged = self._charged(active)
            requested = {"input_bytes": input_bytes, "inflight_slots": 1,
                         "output_bytes": output_bytes, "effect_units": effect_units}
            for name in _DIMENSIONS:
                budget = getattr(self.budget, name)
                if charged[name] + requested[name] > budget:
                    raise ReservationError("BUDGET_EXCEEDED", "no free reservation budget: " + name,
                                           call_id=call_id, retriable=True)
            record = {"call_id": call_id, "spec": spec, "spec_hash": spec_hash,
                      "state": "RESERVED", "outcome": None,
                      "created_ms": int(time.time() * 1000)}
            _save_atomic(self._records / (call_id + ".json"), record)
            return self._view(record)
        finally:
            os.close(fd)

    def release(self, call_id, *, outcome):
        """Only a confirmed outcome releases capacity. ``unknown`` keeps the
        reservation charged (fail-closed) for recovery to surface explicitly."""
        identifier(call_id)
        if outcome not in ("completed", "cancelled", "unknown"):
            raise ReservationError("INVALID_SPEC", "outcome must be completed/cancelled/unknown", call_id=call_id)
        fd = self._lock()
        try:
            record = self._read(call_id)
            if record is None:
                raise ReservationError("INVALID_SPEC", "release needs a known reservation", call_id=call_id)
            if record["state"] == "RELEASED":
                if record["outcome"] == outcome:
                    return {"call_id": call_id, "state": "RELEASED", "idempotent": True}
                raise ReservationError("INVALID_SPEC", "released reservation outcome differs", call_id=call_id)
            if outcome == "unknown":
                raise ReservationError("UNKNOWN_EFFECT", "unknown effect stays charged; recovery must surface it, not blind-retry",
                                       call_id=call_id)
            record["state"] = "RELEASED"
            record["outcome"] = outcome
            record["released_ms"] = int(time.time() * 1000)
            _save_atomic(self._records / (call_id + ".json"), record)
            return {"call_id": call_id, "state": "RELEASED", "idempotent": False}
        finally:
            os.close(fd)

    def outstanding(self):
        """Non-released reservations: the restart reconciliation surface. A fresh
        gate charges all of these automatically; recovery re-reserves overlap
        explicitly (same call idempotent, different call double-charged)."""
        fd = self._lock()
        try:
            return [self._view(record) for record in self._active()]
        finally:
            os.close(fd)

    def snapshot(self):
        fd = self._lock()
        try:
            active = self._active()
            return {"charged": self._charged(active),
                    "budget": {name: getattr(self.budget, name) for name in _DIMENSIONS},
                    "active": [self._view(record) for record in active]}
        finally:
            os.close(fd)


# Legacy prefetch constants (U3). None budget keeps them exactly; an admitted
# inflight-slot budget tightens each bound to what the admission can hold.
LEGACY_PREFETCH = {"append_slots": 64, "source_batch_size": 256, "queue_depth": 4096, "max_pending": 1024}


def derive_prefetch(inflight_slots=None, *, max_pending_floor=1):
    """Derive prefetch bounds from the admitted inflight budget.

    ``None`` (default/unified-seam-off path) returns the exact legacy constants,
    so the old default path is behavior-identical. A positive admitted slot
    budget caps every bound at that budget (never above the legacy ceiling).
    ``max_pending_floor`` is the InputGate protocol minimum: the alignment buffer
    must hold a full credit grant of envelopes (worst case one row each = credit
    rows). When the floor exceeds the admitted ceiling the layout is infeasible
    and rejected here (fail-closed); the floor never expands the admitted budget.
    The caller must jointly solve credit first (resolve_prefetch_credit), which
    tightens default credit to the grant or rejects an unsatisfiable explicit one."""
    if inflight_slots is None:
        return dict(LEGACY_PREFETCH)
    if type(inflight_slots) is not int or inflight_slots <= 0:
        raise ReservationError("INVALID_SPEC", "admitted inflight slots must be a positive integer")
    if type(max_pending_floor) is not int or max_pending_floor < 1:
        raise ReservationError("INVALID_SPEC", "max_pending floor must be a positive integer")
    bounds = {name: min(legacy, inflight_slots) for name, legacy in LEGACY_PREFETCH.items()}
    if max_pending_floor > bounds["max_pending"]:
        raise ReservationError("INVALID_SPEC",
                               "max_pending floor exceeds the admitted prefetch ceiling; jointly solve credit before seal")
    return bounds


def resolve_prefetch_credit(inflight_slots, *, inflight_bytes, credit_budget, credit_explicit):
    """Jointly solve channel credit against the admitted slot/byte grant before seal.

    Pure and deterministic: recovery re-invokes with the original sealed inputs and
    compares the result against the sealed credit; it never re-solves a different
    layout or dynamically changes credit. The slot grant (``inflight_slots``) and
    byte grant (``inflight_bytes``) are checked together against the actual channel
    ``credit_budget`` ({rows, bytes}); credit is not an extra grant. Default credit
    may tighten to one consistent smaller grant shared by seal/router/InputGate;
    explicit credit the grant cannot honor is rejected before any source read. The
    credit floor never expands the admitted budget and no second grant is requested.
    ``inflight_slots=None`` is the original unbound path: legacy prefetch, credit
    unchanged."""
    if type(credit_budget) is not dict or set(credit_budget) != {"rows", "bytes"}:
        raise ReservationError("INVALID_SPEC", "credit_budget must name exactly rows and bytes")
    rows, byte_credit = credit_budget["rows"], credit_budget["bytes"]
    if type(rows) is not int or rows < 1 or type(byte_credit) is not int or byte_credit < 1:
        raise ReservationError("INVALID_SPEC", "credit rows and bytes must be positive integers")
    if type(credit_explicit) is not bool:
        raise ReservationError("INVALID_SPEC", "credit_explicit must be a bool")
    if inflight_slots is None:
        return {"prefetch": derive_prefetch(None), "credit_budget": {"rows": rows, "bytes": byte_credit}}
    if type(inflight_slots) is not int or inflight_slots <= 0:
        raise ReservationError("INVALID_SPEC", "admitted inflight slots must be a positive integer")
    if type(inflight_bytes) is not int or inflight_bytes <= 0:
        raise ReservationError("INVALID_SPEC", "admitted inflight bytes must be a positive integer")
    ceiling = min(inflight_slots, LEGACY_PREFETCH["max_pending"])
    if credit_explicit:
        if rows > ceiling:
            raise ReservationError("BUDGET_EXCEEDED",
                                   "explicit credit rows exceed the admitted prefetch ceiling; cannot expand grant",
                                   retriable=False)
        if byte_credit > inflight_bytes:
            raise ReservationError("BUDGET_EXCEEDED",
                                   "explicit credit bytes exceed the admitted inflight byte grant",
                                   retriable=False)
        solved_rows, solved_bytes = rows, byte_credit
    else:
        solved_rows, solved_bytes = min(rows, ceiling), min(byte_credit, inflight_bytes)
    prefetch = derive_prefetch(inflight_slots, max_pending_floor=solved_rows)
    return {"prefetch": prefetch, "credit_budget": {"rows": solved_rows, "bytes": solved_bytes}}