"""Neutral resource contracts: value types, narrow ports, stable errors.

Standard library only. This package must never import execution, planning,
forge, or business modules; Ray is loaded only inside resources/ray.py.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Protocol

RESOURCE_PROTOCOL_VERSION = {"major": 1, "minor": 1}

_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
MAX_DOCUMENT_BYTES = 65536

ERROR_CODES = frozenset({
    "INVALID_SPEC", "UNSUPPORTED_CAPABILITY", "RESOURCE_QUANTIZATION_EXCEEDS_BUDGET",
    "IDEMPOTENCY_CONFLICT", "REVISION_CONFLICT", "FENCED", "SCOPE_CLOSED",
    "EXPIRED_OPERATION", "PERMISSION_DENIED", "QUOTA_EXCEEDED", "CAPACITY_PENDING",
    "UNSATISFIABLE_CONSTRAINT", "PROVISION_DEADLINE_EXCEEDED", "ARTIFACT_MISMATCH",
    "LAUNCH_FAILED", "PROVIDER_UNAVAILABLE", "OUTCOME_UNKNOWN", "RESYNC_REQUIRED",
    "CLEANUP_UNCONFIRMED",
})
SCOPE_STATES = frozenset({"OPEN", "CLOSING", "CLOSED"})
HOST_PHASES = frozenset({
    "REQUESTED", "PENDING", "RESERVED", "STARTING", "RUNNING",
    "TERMINATING", "TERMINATED", "FAILED", "LOST",
})
TERMINAL_HOST_PHASES = frozenset({"TERMINATED", "FAILED", "LOST"})
FRESHNESS = frozenset({"FRESH", "STALE_UNKNOWN"})
CLEANUP_STATES = frozenset({"NONE", "REQUESTED", "CONFIRMED", "UNCONFIRMED"})
RELEASE_MODES = frozenset({"DRAINED", "ABANDON"})

_PROFILE_RESOURCES = frozenset({"cpu_millis", "memory_bytes", "scratch_bytes"})
_BUDGET_KEYS = frozenset({"cpu_millis", "memory_bytes", "max_workers", "max_creates_per_minute"})
# minor 1.1 additive family: per-device-type authorization ceilings, e.g.
# accelerator_gpu. Documents without these keys remain valid minor-1.0 input.
ACCELERATOR_BUDGET_PREFIX = "accelerator_"


def accelerator_budget_key(device_type):
    return ACCELERATOR_BUDGET_PREFIX + identifier(device_type)


def _validate_budget_key(key):
    if key in _BUDGET_KEYS:
        return key
    if key.startswith(ACCELERATOR_BUDGET_PREFIX):
        identifier(key[len(ACCELERATOR_BUDGET_PREFIX):])
        return key
    raise ResourceError("INVALID_SPEC", "budget has unsupported key: " + str(key))


class ResourceError(Exception):
    """A resource/control fact, never a business success or paid-effect ruling."""

    def __init__(self, code, reason, *, scope_id=None, allocation_id=None,
                 operation_id=None, retriable=False, retry_after_ms=None,
                 observed_revision=None, evidence_ref=None):
        if code not in ERROR_CODES:
            raise ValueError("Unknown resource error code: " + str(code))
        super().__init__(reason)
        self.code = code
        self.reason = str(reason)[:1024]
        self.scope_id = scope_id
        self.allocation_id = allocation_id
        self.operation_id = operation_id
        self.retriable = bool(retriable)
        self.retry_after_ms = retry_after_ms
        self.observed_revision = observed_revision
        self.evidence_ref = evidence_ref

    def to_dict(self):
        return {
            "code": self.code, "scope_id": self.scope_id,
            "allocation_id": self.allocation_id, "operation_id": self.operation_id,
            "reason": self.reason, "retriable": self.retriable,
            "retry_after_ms": self.retry_after_ms,
            "observed_revision": self.observed_revision,
            "evidence_ref": self.evidence_ref,
        }


def identifier(value):
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise ResourceError("INVALID_SPEC", "Invalid resource identity")
    return value


def document(value, maximum=MAX_DOCUMENT_BYTES):
    """Canonical strict JSON within a structural and byte budget."""
    pending, nodes = [(value, 0)], 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 8192 or depth > 16:
            raise ResourceError("INVALID_SPEC", "Resource metadata exceeds structural budget")
        if type(item) is dict:
            if len(item) > 256 or any(type(k) is not str for k in item):
                raise ResourceError("INVALID_SPEC", "Invalid resource metadata keys")
            pending.extend((v, depth + 1) for v in item.values())
        elif type(item) in (list, tuple):
            if len(item) > 256:
                raise ResourceError("INVALID_SPEC", "Resource metadata collection exceeds budget")
            pending.extend((v, depth + 1) for v in item)
        elif type(item) is float:
            if not math.isfinite(item):
                raise ResourceError("INVALID_SPEC", "Nonfinite resource metadata")
        elif type(item) not in (str, int, bool, type(None)):
            raise ResourceError("INVALID_SPEC", "Resource metadata must be strict JSON")
        elif type(item) is str and len(item) > maximum:
            raise ResourceError("INVALID_SPEC", "Resource metadata exceeds byte budget")
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False,
                     allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(raw) > maximum:
        raise ResourceError("INVALID_SPEC", "Resource metadata exceeds byte budget")
    return raw


def spec_hash(value):
    return hashlib.sha256(document(value)).hexdigest()


def negotiate_version(local, remote):
    for value in (local, remote):
        if type(value) is not dict or set(value) != {"major", "minor"}:
            raise ResourceError("INVALID_SPEC", "protocol_version needs major/minor")
        if any(type(v) is not int or v < 0 for v in value.values()):
            raise ResourceError("INVALID_SPEC", "protocol_version components must be nonnegative ints")
    if local["major"] != remote["major"]:
        raise ResourceError("UNSUPPORTED_CAPABILITY", "Resource protocol major version differs")
    return {"major": local["major"], "minor": min(local["minor"], remote["minor"])}


def _positive_int(value, name):
    if type(value) is not int or value <= 0:
        raise ResourceError("INVALID_SPEC", name + " must be a positive int")
    return value


def _validate_requests(requests, name):
    if type(requests) is not dict or not {"cpu_millis", "memory_bytes"} <= set(requests):
        raise ResourceError("INVALID_SPEC", name + " needs cpu_millis and memory_bytes")
    extra = set(requests) - _PROFILE_RESOURCES - {"accelerators", "custom"}
    if extra:
        raise ResourceError("INVALID_SPEC", name + " has unsupported resources: " + str(sorted(extra)))
    for key in _PROFILE_RESOURCES & set(requests):
        _positive_int(requests[key], name + "." + key)
    accelerators = requests.get("accelerators", [])
    if type(accelerators) is not list or len(accelerators) > 8:
        raise ResourceError("INVALID_SPEC", name + ".accelerators must be a bounded list")
    for item in accelerators:
        if type(item) is not dict or set(item) != {"type", "count"}:
            raise ResourceError("INVALID_SPEC", name + ".accelerators entry needs type/count")
        identifier(item["type"])
        _positive_int(item["count"], name + ".accelerators.count")
    custom = requests.get("custom", {})
    if type(custom) is not dict or len(custom) > 16:
        raise ResourceError("INVALID_SPEC", name + ".custom must be a bounded map")
    for key, item in custom.items():
        identifier(key)
        if type(item) is not dict or set(item) != {"amount", "unit"}:
            raise ResourceError("INVALID_SPEC", name + ".custom entry needs amount/unit")
        _positive_int(item["amount"], name + ".custom.amount")
        identifier(item["unit"])
    return requests


def validate_budget(budget):
    if type(budget) is not dict or not {"cpu_millis", "memory_bytes"} <= set(budget):
        raise ResourceError("INVALID_SPEC", "budget needs cpu_millis/memory_bytes within a closed key set")
    for key in set(budget):
        _validate_budget_key(key)
        _positive_int(budget[key], "budget." + key)
    return dict(budget)


@dataclass(frozen=True)
class ResourceProfile:
    requests: dict
    limits: object = None
    requirements: object = None

    def __post_init__(self):
        _validate_requests(self.requests, "profile.requests")
        if self.limits is not None:
            _validate_requests(self.limits, "profile.limits")
        if self.requirements is not None:
            document(self.requirements, 4096)

    def to_dict(self):
        value = {"requests": dict(self.requests)}
        if self.limits is not None:
            value["limits"] = dict(self.limits)
        if self.requirements is not None:
            value["requirements"] = dict(self.requirements)
        return value


@dataclass(frozen=True)
class WorkerLaunchSpec:
    artifact_refs: dict
    entrypoint: str
    argv: tuple
    bootstrap_ref: str
    access_refs: dict
    runtime_protocol_version: dict

    def __post_init__(self):
        if type(self.artifact_refs) is not dict or not 1 <= len(self.artifact_refs) <= 8:
            raise ResourceError("INVALID_SPEC", "artifact_refs must be a bounded nonempty map")
        for name, ref in self.artifact_refs.items():
            identifier(name)
            if type(ref) is not dict or set(ref) != {"digest", "location"}:
                raise ResourceError("INVALID_SPEC", "artifact ref needs digest/location")
            if type(ref["digest"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", ref["digest"]):
                raise ResourceError("INVALID_SPEC", "artifact digest must be sha256 hex")
            if type(ref["location"]) is not str or not 1 <= len(ref["location"]) <= 1024:
                raise ResourceError("INVALID_SPEC", "artifact location must be a bounded string")
        if type(self.entrypoint) is not str or not 1 <= len(self.entrypoint) <= 256:
            raise ResourceError("INVALID_SPEC", "entrypoint must be a bounded string")
        if type(self.argv) not in (tuple, list) or len(self.argv) > 64 or any(
                type(item) is not str or len(item) > 4096 for item in self.argv):
            raise ResourceError("INVALID_SPEC", "argv must be bounded strings")
        identifier(self.bootstrap_ref)
        if type(self.access_refs) is not dict or len(self.access_refs) > 16 or any(
                type(k) is not str or type(v) is not str or len(v) > 1024
                for k, v in self.access_refs.items()):
            raise ResourceError("INVALID_SPEC", "access_refs must be bounded string references")
        negotiate_version(RESOURCE_PROTOCOL_VERSION, self.runtime_protocol_version)

    def to_dict(self):
        return {
            "artifact_refs": {k: dict(v) for k, v in self.artifact_refs.items()},
            "entrypoint": self.entrypoint, "argv": list(self.argv),
            "bootstrap_ref": self.bootstrap_ref,
            "access_refs": dict(self.access_refs),
            "runtime_protocol_version": dict(self.runtime_protocol_version),
        }


@dataclass(frozen=True)
class WorkerRequest:
    worker_id: str
    allocation_id: str
    profile: ResourceProfile
    launch: WorkerLaunchSpec
    provision_deadline_ms: int
    placement: object = None
    reservation_group: object = None

    def __post_init__(self):
        identifier(self.worker_id)
        identifier(self.allocation_id)
        if type(self.provision_deadline_ms) is not int or self.provision_deadline_ms <= 0:
            raise ResourceError("INVALID_SPEC", "provision_deadline_ms must be a positive int")
        if self.placement is not None:
            if type(self.placement) is not dict or set(self.placement) - {"required", "preferred"}:
                raise ResourceError("INVALID_SPEC", "placement supports required/preferred only")
            document(self.placement, 4096)
        if self.reservation_group is not None:
            identifier(self.reservation_group)

    def to_dict(self):
        value = {
            "worker_id": self.worker_id, "allocation_id": self.allocation_id,
            "profile": self.profile.to_dict(), "launch": self.launch.to_dict(),
            "provision_deadline_ms": self.provision_deadline_ms,
        }
        if self.placement is not None:
            value["placement"] = dict(self.placement)
        if self.reservation_group is not None:
            value["reservation_group"] = self.reservation_group
        return value


@dataclass(frozen=True)
class ReleaseSpec:
    allocation_id: str
    mode: str
    engine_receipt_ref: str
    terminate_deadline_ms: int
    reason: str

    def __post_init__(self):
        identifier(self.allocation_id)
        if self.mode not in RELEASE_MODES:
            raise ResourceError("INVALID_SPEC", "release mode must be DRAINED or ABANDON")
        identifier(self.engine_receipt_ref)
        if type(self.terminate_deadline_ms) is not int or self.terminate_deadline_ms <= 0:
            raise ResourceError("INVALID_SPEC", "terminate_deadline_ms must be a positive int")
        if type(self.reason) is not str or not 1 <= len(self.reason) <= 256:
            raise ResourceError("INVALID_SPEC", "release reason must be a bounded string")

    def to_dict(self):
        return {
            "allocation_id": self.allocation_id, "mode": self.mode,
            "engine_receipt_ref": self.engine_receipt_ref,
            "terminate_deadline_ms": self.terminate_deadline_ms, "reason": self.reason,
        }


@dataclass(frozen=True)
class MutationContext:
    protocol_version: dict
    scope_id: str
    controller_epoch: int
    authority_token: str
    operation_id: str
    expected_revision: int

    def __post_init__(self):
        negotiate_version(RESOURCE_PROTOCOL_VERSION, self.protocol_version)
        identifier(self.scope_id)
        if type(self.controller_epoch) is not int or self.controller_epoch < 0:
            raise ResourceError("INVALID_SPEC", "controller_epoch must be a nonnegative int")
        if type(self.authority_token) is not str or not 1 <= len(self.authority_token) <= 128:
            raise ResourceError("INVALID_SPEC", "authority_token must be a bounded opaque reference")
        identifier(self.operation_id)
        if type(self.expected_revision) is not int or self.expected_revision < 0:
            raise ResourceError("INVALID_SPEC", "expected_revision must be a nonnegative int")

    def to_dict(self):
        return {
            "protocol_version": dict(self.protocol_version), "scope_id": self.scope_id,
            "controller_epoch": self.controller_epoch, "authority_token": self.authority_token,
            "operation_id": self.operation_id, "expected_revision": self.expected_revision,
        }


@dataclass(frozen=True)
class ResourceScope:
    scope_id: str
    run_id: str
    provider_binding: dict
    authority_ref: str
    budget: dict

    def __post_init__(self):
        identifier(self.scope_id)
        identifier(self.run_id)
        if type(self.provider_binding) is not dict or not 1 <= len(self.provider_binding) <= 16:
            raise ResourceError("INVALID_SPEC", "provider_binding must be a bounded map")
        document(self.provider_binding, 4096)
        identifier(self.authority_ref)
        validate_budget(self.budget)

    def to_dict(self):
        return {
            "scope_id": self.scope_id, "run_id": self.run_id,
            "provider_binding": dict(self.provider_binding),
            "authority_ref": self.authority_ref, "budget": dict(self.budget),
        }


@dataclass(frozen=True)
class ResourcePlan:
    scope_id: str
    desired_workers: tuple
    retirements: tuple
    budgets: object = None

    def __post_init__(self):
        identifier(self.scope_id)
        if any(type(item) is not WorkerRequest for item in self.desired_workers):
            raise ResourceError("INVALID_SPEC", "desired_workers must be WorkerRequest values")
        if any(type(item) is not ReleaseSpec for item in self.retirements):
            raise ResourceError("INVALID_SPEC", "retirements must be ReleaseSpec values")
        workers = [item.allocation_id for item in self.desired_workers]
        if len(set(workers)) != len(workers):
            raise ResourceError("INVALID_SPEC", "duplicate allocation in desired_workers")
        retired = [item.allocation_id for item in self.retirements]
        if len(set(retired)) != len(retired) or set(retired) & set(workers):
            raise ResourceError("INVALID_SPEC", "retirements must be unique and absent from desired")
        if self.budgets is not None:
            validate_budget(self.budgets)

    def to_dict(self):
        value = {
            "scope_id": self.scope_id,
            "desired_workers": [item.to_dict() for item in self.desired_workers],
            "retirements": [item.to_dict() for item in self.retirements],
        }
        if self.budgets is not None:
            value["budgets"] = dict(self.budgets)
        return value


def _profile(value):
    if type(value) is not dict or set(value) - {"requests", "limits", "requirements"} or "requests" not in value:
        raise ResourceError("INVALID_SPEC", "profile needs requests within a closed key set")
    return ResourceProfile(value["requests"], value.get("limits"), value.get("requirements"))


def _launch(value):
    if type(value) is not dict or set(value) != {
            "artifact_refs", "entrypoint", "argv", "bootstrap_ref",
            "access_refs", "runtime_protocol_version"}:
        raise ResourceError("INVALID_SPEC", "launch spec key set is unsupported")
    return WorkerLaunchSpec(
        value["artifact_refs"], value["entrypoint"], tuple(value["argv"]),
        value["bootstrap_ref"], value["access_refs"], value["runtime_protocol_version"],
    )


def parse_plan(value, *, max_workers=64, max_retirements=64):
    """Accept the protocol v1 plan shape (inline or shared profile/launch refs)."""
    if type(value) is not dict or set(value) - {
            "scope_id", "profiles", "launches", "desired_workers",
            "retirements", "reservation_groups", "budgets", "note"}:
        raise ResourceError("INVALID_SPEC", "plan key set is unsupported")
    if "scope_id" not in value or "desired_workers" not in value:
        raise ResourceError("INVALID_SPEC", "plan needs scope_id and desired_workers")
    profiles = value.get("profiles", {})
    launches = value.get("launches", {})
    if type(profiles) is not dict or type(launches) is not dict or len(profiles) > 64 or len(launches) > 64:
        raise ResourceError("INVALID_SPEC", "plan templates must be bounded maps")
    workers = value["desired_workers"]
    if type(workers) is not list or len(workers) > max_workers:
        raise ResourceError("INVALID_SPEC", "desired_workers exceeds the plan bound")
    parsed = []
    for item in workers:
        if type(item) is not dict or not {"worker_id", "allocation_id"} <= set(item) or set(item) - {
                "worker_id", "allocation_id", "profile", "profile_ref", "launch", "launch_ref",
                "placement", "provision_deadline_ms", "reservation_group"}:
            raise ResourceError("INVALID_SPEC", "worker request key set is unsupported")
        profile = item.get("profile")
        if profile is None:
            ref = item.get("profile_ref")
            if ref not in profiles:
                raise ResourceError("INVALID_SPEC", "unknown profile_ref: " + str(ref))
            profile = profiles[ref]
        launch = item.get("launch")
        if launch is None:
            ref = item.get("launch_ref")
            if ref not in launches:
                raise ResourceError("INVALID_SPEC", "unknown launch_ref: " + str(ref))
            launch = launches[ref]
        deadline = item.get("provision_deadline_ms")
        if deadline is None:
            raise ResourceError("INVALID_SPEC", "worker request needs provision_deadline_ms")
        parsed.append(WorkerRequest(
            item["worker_id"], item["allocation_id"], _profile(profile), _launch(launch),
            deadline, item.get("placement"), item.get("reservation_group"),
        ))
    retirements = value.get("retirements", [])
    if type(retirements) is not list or len(retirements) > max_retirements:
        raise ResourceError("INVALID_SPEC", "retirements exceeds the plan bound")
    releases = []
    for item in retirements:
        if type(item) is not dict or set(item) != {
                "allocation_id", "mode", "engine_receipt_ref", "terminate_deadline_ms", "reason"}:
            raise ResourceError("INVALID_SPEC", "release spec key set is unsupported")
        releases.append(ReleaseSpec(**item))
    groups = value.get("reservation_groups")
    if groups is not None and (type(groups) is not list or len(groups) > 16):
        raise ResourceError("INVALID_SPEC", "reservation_groups must be a bounded list")
    return ResourcePlan(value["scope_id"], tuple(parsed), tuple(releases), value.get("budgets"))


_OBSERVATION_KEYS = frozenset({
    "allocation_id", "provider_uid", "boot_id", "phase", "observation",
    "observed_at_ms", "cleanup", "effective_resources", "conditions",
    "evidence_ref", "placement", "ownership", "observation_revision",
})


def validate_observation(value):
    if type(value) is not dict or set(value) - _OBSERVATION_KEYS or not {
            "allocation_id", "phase", "observation", "observed_at_ms",
            "cleanup", "observation_revision"} <= set(value):
        raise ResourceError("INVALID_SPEC", "allocation observation key set is unsupported")
    identifier(value["allocation_id"])
    if value["phase"] not in HOST_PHASES:
        raise ResourceError("INVALID_SPEC", "unknown host phase")
    if value["observation"] not in FRESHNESS:
        raise ResourceError("INVALID_SPEC", "observation must be FRESH or STALE_UNKNOWN")
    if value["cleanup"] not in CLEANUP_STATES:
        raise ResourceError("INVALID_SPEC", "unknown cleanup state")
    if type(value["observed_at_ms"]) is not int or value["observed_at_ms"] < 0:
        raise ResourceError("INVALID_SPEC", "observed_at_ms must be a nonnegative int")
    if type(value["observation_revision"]) is not int or value["observation_revision"] < 0:
        raise ResourceError("INVALID_SPEC", "observation_revision must be a nonnegative int")
    for name in ("provider_uid", "boot_id", "evidence_ref"):
        item = value.get(name)
        if item is not None and (type(item) is not str or len(item) > 256):
            raise ResourceError("INVALID_SPEC", name + " must be a bounded string or null")
    if value["cleanup"] == "CONFIRMED" and not value.get("evidence_ref"):
        raise ResourceError("INVALID_SPEC", "confirmed cleanup requires exit evidence")
    effective = value.get("effective_resources")
    if effective is not None:
        _validate_requests(effective, "effective_resources")
    for name in ("conditions", "placement", "ownership"):
        item = value.get(name)
        if item is not None:
            document(item, 4096)
    document(value)
    return value


class ResourceProvider(Protocol):
    def describe_and_normalize(self, binding, requirements, spec): ...

    def ensure_present(self, allocation_id, immutable_spec): ...

    def inspect(self, scope_id, page_token): ...

    def ensure_absent(self, exact_identity, terminate_deadline_ms): ...


class ResourceState(Protocol):
    def accept_plan(self, context, validated_plan): ...

    def begin_close(self, context, close_spec): ...

    def read_snapshot(self, scope_id, page_token): ...

    def claim_operations(self, authority, now_ms, limit): ...

    def record_observations(self, authority, observations): ...

    def read_events(self, scope_id, cursor, limit): ...