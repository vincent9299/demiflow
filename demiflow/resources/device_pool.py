"""Logical accelerator device pool: independent ledger, confirmed-exit accounting.

The inventory is declarative (device_id/type/memory_bytes/optional max_residents).
CPU/memory stay in LocalCapacityPool; this ledger owns device count and device
memory coexistence only, so CPU and accelerator pools reserve independently.
Non-RELEASED records charge their devices: queueing, unconfirmed exit and
replacement overlap all count; STALE_UNKNOWN never releases. A model whose
declared device memory fits no single device is rejected before startup.

This is logical admission/accounting, not physical device isolation: real
CUDA_VISIBLE_DEVICES/UUID binding evidence is explicitly out of scope.
"""
import fcntl
import json
import os
from pathlib import Path
import time

from .capacity import _save_atomic
from .contracts import ResourceError, accelerator_budget_key, identifier

MAX_DEVICES = 64
MAX_ACTIVE_RECORDS = 1024
_PLACEMENT_KEYS = frozenset({"device_ids"})


def _positive_int(value, name):
    if type(value) is not int or value <= 0:
        raise ResourceError("INVALID_SPEC", name + " must be a positive int")
    return value


def validate_inventory(inventory):
    """Validate a bounded declaration without opening or allocating resources."""
    if type(inventory) is not dict or set(inventory) != {"devices"}:
        raise ResourceError("INVALID_SPEC", "device inventory needs a devices list")
    devices = inventory["devices"]
    if type(devices) is not list or not 1 <= len(devices) <= MAX_DEVICES:
        raise ResourceError("INVALID_SPEC", "device inventory must be a bounded nonempty list")
    seen = set()
    normalized = []
    for device in devices:
        if type(device) is not dict or not {"device_id", "type", "memory_bytes"} <= set(device) \
                or set(device) - {"device_id", "type", "memory_bytes", "max_residents"}:
            raise ResourceError("INVALID_SPEC", "device entry key set is unsupported")
        identifier(device["device_id"])
        identifier(device["type"])
        _positive_int(device["memory_bytes"], "device.memory_bytes")
        if device["device_id"] in seen:
            raise ResourceError("INVALID_SPEC", "duplicate device_id in inventory")
        seen.add(device["device_id"])
        entry = {"device_id": device["device_id"], "type": device["type"],
                 "memory_bytes": device["memory_bytes"]}
        if "max_residents" in device:
            entry["max_residents"] = _positive_int(device["max_residents"], "device.max_residents")
        normalized.append(entry)
    return normalized


# Native callers retain their historical import with the same implementation.
_validate_inventory = validate_inventory


def _validate_accelerators(accelerators):
    if type(accelerators) is not list or len(accelerators) > 8:
        raise ResourceError("INVALID_SPEC", "accelerators must be a bounded list")
    entries = []
    for item in accelerators:
        if type(item) is not dict or set(item) != {"type", "count"}:
            raise ResourceError("INVALID_SPEC", "accelerators entry needs type/count")
        identifier(item["type"])
        _positive_int(item["count"], "accelerators.count")
        entries.append((item["type"], item["count"]))
    if len({t for t, _ in entries}) != len(entries):
        raise ResourceError("INVALID_SPEC", "duplicate accelerator type in request")
    return entries


def _placement_ids(value, name):
    if type(value) is not dict or set(value) - _PLACEMENT_KEYS or "device_ids" not in value:
        raise ResourceError("INVALID_SPEC", name + " supports device_ids only")
    ids = value["device_ids"]
    if type(ids) is not list or not 1 <= len(ids) <= MAX_DEVICES:
        raise ResourceError("INVALID_SPEC", name + ".device_ids must be a bounded nonempty list")
    for device_id in ids:
        identifier(device_id)
    if len(set(ids)) != len(ids):
        raise ResourceError("INVALID_SPEC", name + ".device_ids must be unique")
    return list(ids)


class DevicePool:
    """One normalized root, one file lock, bounded atomic per-allocation records."""

    def __init__(self, root, *, inventory):
        self.root = Path(root).resolve()
        self.inventory = validate_inventory(inventory)
        self._devices = {device["device_id"]: device for device in self.inventory}
        self.root.mkdir(parents=True, exist_ok=True)
        self._records = self.root / "allocations"
        self._records.mkdir(exist_ok=True)

    def _lock(self):
        path = self.root / "devices.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _read(self, allocation_id):
        path = self._records / (allocation_id + ".json")
        try:
            with open(path, "rb") as stream:
                raw = stream.read(8193)
        except FileNotFoundError:
            return None
        if len(raw) > 8192:
            raise ResourceError("INVALID_SPEC", "device record exceeds byte budget")
        record = json.loads(raw)
        if type(record) is not dict or record.get("allocation_id") != allocation_id:
            raise ResourceError("INVALID_SPEC", "device record identity differs")
        return record

    def _active(self):
        records = []
        with os.scandir(self._records) as entries:
            for count, entry in enumerate(entries):
                if count >= MAX_ACTIVE_RECORDS + 1:
                    raise ResourceError("QUOTA_EXCEEDED", "device record count exceeds budget")
                if not entry.name.endswith(".json") or entry.name.endswith(".tmp"):
                    continue
                record = self._read(entry.name[:-5])
                if record is not None and record["state"] != "RELEASED":
                    records.append(record)
        return records

    def _usage(self, records):
        charged_memory = {device_id: 0 for device_id in self._devices}
        residents = {device_id: 0 for device_id in self._devices}
        for record in records:
            for entry in record["devices"]:
                charged_memory[entry["device_id"]] += entry["memory_bytes"]
                residents[entry["device_id"]] += 1
        return charged_memory, residents

    def _memory_need(self, device, device_memory_bytes):
        # Undeclared model memory is conservatively exclusive: the whole device.
        return device["memory_bytes"] if device_memory_bytes is None else device_memory_bytes

    def _candidates(self, device_type, device_memory_bytes, pinned=None):
        candidates = []
        for device_id in sorted(self._devices):
            device = self._devices[device_id]
            if device["type"] != device_type:
                continue
            if pinned is not None and device_id not in pinned:
                continue
            if self._memory_need(device, device_memory_bytes) > device["memory_bytes"]:
                continue
            candidates.append(device_id)
        return candidates

    def evaluate(self, accelerators, *, device_memory_bytes=None, placement=None):
        """Static feasibility preflight: reject the impossible before any reservation.

        Returns the effective pinning and preferred-placement warnings. Dynamic
        freeness is deliberately not consulted here: a busy-but-feasible request
        is CAPACITY_PENDING at reserve time, never a preflight failure.
        """
        entries = _validate_accelerators(accelerators)
        if device_memory_bytes is not None:
            _positive_int(device_memory_bytes, "device_memory_bytes")
        placement = placement or {}
        if type(placement) is not dict or set(placement) - {"required", "preferred"}:
            raise ResourceError("INVALID_SPEC", "placement supports required/preferred only")
        warnings = []
        required = None
        preferred = None
        if "required" in placement:
            if not entries:
                raise ResourceError("UNSATISFIABLE_CONSTRAINT", "required placement without accelerator demand")
            required = _placement_ids(placement["required"], "placement.required")
            unknown = [d for d in required if d not in self._devices]
            if unknown:
                raise ResourceError("UNSATISFIABLE_CONSTRAINT",
                                    "required devices absent from inventory: " + ",".join(unknown))
        if "preferred" in placement:
            try:
                preferred = _placement_ids(placement["preferred"], "placement.preferred")
            except ResourceError as exc:
                warnings.append({"code": "PREFERRED_UNSATISFIABLE", "reason": exc.reason})
                preferred = None
            if preferred is not None:
                unknown = [d for d in preferred if d not in self._devices]
                if unknown:
                    warnings.append({"code": "PREFERRED_UNSATISFIABLE",
                                     "reason": "preferred devices absent from inventory: " + ",".join(unknown)})
                    preferred = None
        for device_type, count in entries:
            candidates = self._candidates(device_type, device_memory_bytes, pinned=required)
            if len(candidates) < count:
                raise ResourceError(
                    "UNSATISFIABLE_CONSTRAINT",
                    "inventory cannot host " + str(count) + "x " + device_type
                    + (" under required placement" if required is not None else "")
                    + ": " + str(len(candidates)) + " feasible devices")
            if preferred is not None:
                if len(self._candidates(device_type, device_memory_bytes, pinned=preferred)) < count:
                    warnings.append({"code": "PREFERRED_UNSATISFIABLE",
                                     "reason": "preferred devices cannot host " + str(count) + "x " + device_type})
        pin = required if required is not None else preferred
        return {"warnings": warnings, "device_ids": pin, "pin_required": required is not None}

    def _view(self, record):
        return {"allocation_id": record["allocation_id"], "spec_hash": record["spec_hash"],
                "state": record["state"], "devices": [dict(d) for d in record["devices"]]}

    def reserve(self, allocation_id, spec_hash_value, accelerators, *, deadline_ms,
                device_memory_bytes=None, device_ids=None, pin_required=False):
        """Atomically assign concrete devices or report CAPACITY_PENDING.

        All-or-nothing per allocation across its accelerator entries; a partial
        assignment is never recorded. Existing non-RELEASED records keep charging
        (replacement overlap double-counts until the old exit is confirmed).
        """
        identifier(allocation_id)
        if type(spec_hash_value) is not str or len(spec_hash_value) != 64:
            raise ResourceError("INVALID_SPEC", "spec_hash must be sha256 hex")
        entries = _validate_accelerators(accelerators)
        if not entries:
            raise ResourceError("INVALID_SPEC", "device reserve needs accelerator demand")
        if device_memory_bytes is not None:
            _positive_int(device_memory_bytes, "device_memory_bytes")
        if type(deadline_ms) is not int or deadline_ms <= 0:
            raise ResourceError("INVALID_SPEC", "deadline_ms must be positive")
        if device_ids is not None:
            device_ids = _placement_ids({"device_ids": device_ids}, "device_ids")
        fd = self._lock()
        try:
            existing = self._read(allocation_id)
            if existing is not None:
                if existing["spec_hash"] != spec_hash_value:
                    raise ResourceError("INVALID_SPEC", "allocation spec is immutable", allocation_id=allocation_id)
                if existing["state"] == "RELEASED":
                    raise ResourceError("INVALID_SPEC", "released allocation cannot reserve again",
                                        allocation_id=allocation_id)
                return self._view(existing)
            active = self._active()
            if len(active) >= MAX_ACTIVE_RECORDS:
                raise ResourceError("QUOTA_EXCEEDED", "active device records exceed budget")
            charged_memory, residents = self._usage(active)
            assigned = []
            taken_memory = dict(charged_memory)
            taken_residents = dict(residents)
            for device_type, count in entries:
                candidates = self._candidates(device_type, device_memory_bytes,
                                              pinned=device_ids if pin_required else None)
                if pin_required and len(candidates) < count:
                    raise ResourceError("UNSATISFIABLE_CONSTRAINT",
                                        "required devices cannot host the request", allocation_id=allocation_id)
                # Preferred ids are tried first but never block feasible fallback.
                ordered = sorted(candidates, key=lambda d: (d not in (device_ids or ()), d))
                chosen = []
                for device_id in ordered:
                    device = self._devices[device_id]
                    need = self._memory_need(device, device_memory_bytes)
                    if taken_memory[device_id] + need > device["memory_bytes"]:
                        continue
                    if "max_residents" in device and taken_residents[device_id] + 1 > device["max_residents"]:
                        continue
                    chosen.append({"device_id": device_id, "type": device_type, "memory_bytes": need})
                    taken_memory[device_id] += need
                    taken_residents[device_id] += 1
                    if len(chosen) == count:
                        break
                if len(chosen) < count:
                    raise ResourceError(
                        "CAPACITY_PENDING", "no free " + device_type + " devices for the request",
                        allocation_id=allocation_id, retriable=True, retry_after_ms=1000)
                assigned.extend(chosen)
            record = {"allocation_id": allocation_id, "spec_hash": spec_hash_value,
                      "state": "RESERVED", "devices": assigned,
                      "created_ms": int(time.time() * 1000), "deadline_ms": deadline_ms}
            _save_atomic(self._records / (allocation_id + ".json"), record)
            return self._view(record)
        finally:
            os.close(fd)

    def reservation_view(self, allocation_id):
        """Current non-released reservation for the allocation, or None."""
        identifier(allocation_id)
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            if record is None or record["state"] == "RELEASED":
                return None
            return self._view(record)
        finally:
            os.close(fd)

    def released_device_seconds(self, allocation_id, spec_hash, device_type):
        """Return reserved-device seconds only after the original verified release."""
        identifier(allocation_id)
        fd = self._lock()
        try:
            record = self._read(allocation_id)
            if record is None or record['state'] != 'RELEASED':
                return None
            if record['spec_hash'] != spec_hash:
                raise ResourceError("ARTIFACT_MISMATCH", "original device allocation spec changed")
            start, end = record.get('created_ms'), record.get('released_ms')
            if type(start) is not int or type(end) is not int or end < start:
                return None
            count = sum(device['type'] == device_type for device in record['devices'])
            return count * (end - start) / 1000
        finally:
            os.close(fd)

    def release(self, allocation_id, cleanup_evidence):
        """Device capacity releases only on confirmed exit evidence, never on intent."""
        identifier(allocation_id)
        if type(cleanup_evidence) is not dict or set(cleanup_evidence) != {"confirmed", "evidence_ref", "identity"}:
            raise ResourceError("INVALID_SPEC", "cleanup evidence key set is unsupported")
        if cleanup_evidence["identity"] is not None:
            raise ResourceError("INVALID_SPEC", "device records carry no process identity")
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
                raise ResourceError("CLEANUP_UNCONFIRMED", "exit evidence is not confirmed; devices stay charged",
                                    allocation_id=allocation_id, retriable=True)
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
            charged_memory, residents = self._usage(active)
            charged = {}
            for record in active:
                for entry in record["devices"]:
                    key = accelerator_budget_key(entry["type"])
                    charged[key] = charged.get(key, 0) + 1
            return {"charged": charged,
                    "devices": [{"device_id": device_id, "type": self._devices[device_id]["type"],
                                 "memory_bytes": self._devices[device_id]["memory_bytes"],
                                 "charged_memory_bytes": charged_memory[device_id],
                                 "residents": residents[device_id]}
                                for device_id in sorted(self._devices)],
                    "active": [self._view(r) for r in active]}
        finally:
            os.close(fd)