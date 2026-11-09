"""LocalResourceProvider: finite local capacity plus managed process units.

Discovery precedes creation: an existing recorded spawn is adopted or retired,
never blindly duplicated. Capacity is released only on confirmed exit evidence.
"""
import os
from pathlib import Path
import sys
import time

from .contracts import (
    HOST_PHASES, ResourceError, document, identifier, spec_hash, validate_observation,
)
from .process_observation import require_pidfds

PAGE_SIZE = 64


def _now_ms():
    return int(time.time() * 1000)


def _provider_uid(identity):
    if identity is None:
        return None
    return "local-" + spec_hash(identity)[:32]


class LocalResourceProvider:
    def __init__(self, pool, unit, *, control=None):
        self.pool = pool
        self.unit = unit
        # Optional provider-specific control connector (Local socketpair in S4,
        # Ray actor methods in S5). Duck-typed so resources/ stays neutral.
        self._control = control
        # The spawning controller is the parent and must reap its own children;
        # a zombie with a matching birth identity is not a live host.
        self._spawned = {}

    def describe_and_normalize(self, binding, requirements, spec):
        if not sys.platform.startswith("linux"):
            raise ResourceError("UNSUPPORTED_CAPABILITY", "Local managed units require Linux")
        try:
            require_pidfds()
        except (OSError, RuntimeError) as exc:
            raise ResourceError("UNSUPPORTED_CAPABILITY", "pidfd exit evidence unavailable: " + str(exc)) from exc
        profile = spec["profile"]
        requests = profile["requests"]
        if requests.get("accelerators") or requests.get("custom"):
            raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider admits CPU/memory only")
        if profile.get("limits"):
            raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot enforce hard physical limits")
        if profile.get("requirements") or (requirements or {}):
            raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot verify hardware requirements")
        placement = spec.get("placement") or {}
        if placement.get("required"):
            raise ResourceError("UNSATISFIABLE_CONSTRAINT", "Local provider has no placement constraints")
        effective = {"cpu_millis": requests["cpu_millis"], "memory_bytes": requests["memory_bytes"]}
        if "scratch_bytes" in requests:
            effective["scratch_bytes"] = requests["scratch_bytes"]
        return {"effective": effective, "spec": spec, "spec_hash": spec_hash(spec)}

    def _observation(self, scope_id, allocation_id, phase, freshness, identity=None,
                     cleanup="NONE", effective=None, conditions=None, evidence_ref=None):
        value = {
            "allocation_id": allocation_id, "provider_uid": _provider_uid(identity),
            "boot_id": None, "phase": phase, "observation": freshness,
            "observed_at_ms": _now_ms(), "observation_revision": _now_ms(),
            "cleanup": cleanup, "effective_resources": effective,
            "conditions": conditions or [], "evidence_ref": evidence_ref,
            "placement": None,
            "ownership": {"scope_owned": [allocation_id], "shared": []},
        }
        return validate_observation(value)

    def ensure_present(self, allocation_id, immutable_spec):
        identifier(allocation_id)
        scope_id = immutable_spec["scope_id"]
        normalized = self.describe_and_normalize(
            immutable_spec.get("binding", {}), immutable_spec.get("requirements"),
            immutable_spec["request"],
        )
        if normalized["spec_hash"] != immutable_spec["spec_hash"]:
            raise ResourceError("ARTIFACT_MISMATCH", "immutable spec hash differs", allocation_id=allocation_id)
        effective = normalized["effective"]
        found = self.unit.inspect(scope_id, allocation_id, process=self._spawned.get(allocation_id))
        if found["phase"] == "RUNNING" and found["observation"] == "FRESH":
            return self._observation(scope_id, allocation_id, "RUNNING", "FRESH",
                                     identity=found["identity"], effective=effective,
                                     evidence_ref=found["evidence_ref"])
        if found["phase"] == "TERMINATED":
            return self._observation(scope_id, allocation_id, "TERMINATED", "FRESH",
                                     identity=found["identity"], effective=effective,
                                     conditions=["exited_before_present"], evidence_ref=found["evidence_ref"])
        if found["observation"] == "STALE_UNKNOWN":
            return self._observation(scope_id, allocation_id, found["phase"], "STALE_UNKNOWN",
                                     identity=found["identity"], effective=effective,
                                     conditions=["identity_or_lock_unverified"], evidence_ref=found["evidence_ref"])
        try:
            self.pool.reserve(allocation_id, immutable_spec["spec_hash"], effective,
                              immutable_spec["request"]["provision_deadline_ms"])
        except ResourceError as exc:
            if exc.code == "CAPACITY_PENDING":
                return self._observation(scope_id, allocation_id, "PENDING", "FRESH",
                                         effective=effective, conditions=["capacity_pending"])
            raise
        request = immutable_spec["request"]
        envelope = {
            "allocation_id": allocation_id, "spec_hash": immutable_spec["spec_hash"],
            "cpu_millis": effective["cpu_millis"], "memory_bytes": effective["memory_bytes"],
            "resource_directory": str(self.unit.directory(scope_id, allocation_id) / "native"),
        }
        payload = {}
        if self._control is not None:
            try:
                payload = self._control.spawn_payload(allocation_id)
            except Exception as exc:
                self._control.abort(allocation_id)
                self.pool.release(allocation_id, {
                    "confirmed": True, "identity": None,
                    "evidence_ref": "control-failed:" + allocation_id,
                })
                return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                         effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        try:
            spawned = self.unit.spawn(
                scope_id, allocation_id, immutable_spec["spec_hash"],
                request["launch"]["argv"], envelope=envelope,
                log_path=self.unit.directory(scope_id, allocation_id) / "host.log",
                extra_env=payload.get("extra_env"), extra_pass_fds=payload.get("extra_pass_fds", ()),
            )
        except ResourceError as exc:
            if self._control is not None:
                self._control.abort(allocation_id)
            if exc.code == "OUTCOME_UNKNOWN":
                return self._observation(scope_id, allocation_id, "STARTING", "STALE_UNKNOWN",
                                         effective=effective, conditions=["spawn_outcome_unknown"])
            self.pool.release(allocation_id, {
                "confirmed": True, "identity": None,
                "evidence_ref": "launch-failed:" + allocation_id + ":" + exc.code,
            })
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=[exc.code, exc.reason])
        except OSError as exc:
            if self._control is not None:
                self._control.abort(allocation_id)
            self.pool.release(allocation_id, {
                "confirmed": True, "identity": None,
                "evidence_ref": "launch-failed:" + allocation_id + ":OSError",
            })
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        if self._control is not None:
            self._control.spawned(allocation_id)
        identity = spawned["identity"]
        self._spawned[allocation_id] = spawned["process"]
        self.pool.bind_identity(allocation_id, identity)
        return self._observation(scope_id, allocation_id, "RUNNING", "FRESH",
                                 identity=identity, effective=effective,
                                 evidence_ref=spawned["directory"] + "/process.json")

    def inspect(self, scope_id, page_token):
        identifier(scope_id)
        root = self.unit.root / scope_id
        names = []
        if root.is_dir():
            with os.scandir(root) as entries:
                for count, entry in enumerate(entries):
                    if count >= 4096:
                        raise ResourceError("INVALID_SPEC", "scope unit inventory exceeds budget")
                    if entry.is_dir(follow_symlinks=False):
                        names.append(entry.name)
        names.sort()
        start = 0
        if page_token is not None:
            if type(page_token) is not str or page_token not in names:
                raise ResourceError("RESYNC_REQUIRED", "inventory page token expired")
            start = names.index(page_token)
        page = names[start:start + PAGE_SIZE]
        allocations = []
        for allocation_id in page:
            found = self.unit.inspect(scope_id, allocation_id, process=self._spawned.get(allocation_id))
            allocations.append(self._observation(
                scope_id, allocation_id, found["phase"], found["observation"],
                identity=found["identity"], evidence_ref=found["evidence_ref"],
            ))
        next_token = page[-1] if len(page) == PAGE_SIZE and start + PAGE_SIZE < len(names) else None
        return {"allocations": allocations, "next_page_token": next_token, "sampled_at_ms": _now_ms()}

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        scope_id = exact_identity["scope_id"]
        allocation_id = exact_identity["allocation_id"]
        identifier(scope_id)
        identifier(allocation_id)
        process = self._spawned.get(allocation_id)
        found = self.unit.inspect(scope_id, allocation_id, process=process)
        if found["phase"] == "REQUESTED":
            self.pool.release(allocation_id, {
                "confirmed": True, "identity": None,
                "evidence_ref": "never-spawned:" + allocation_id,
            })
            return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                    "evidence_ref": "never-spawned:" + allocation_id, "observed_at_ms": _now_ms()}
        evidence = self.unit.stop(scope_id, allocation_id, terminate_deadline_ms=terminate_deadline_ms,
                                  process=process)
        if evidence["confirmed"]:
            self.pool.release(allocation_id, evidence)
            self._spawned.pop(allocation_id, None)
            return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                    "evidence_ref": evidence["evidence_ref"], "observed_at_ms": _now_ms()}
        return {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                "evidence_ref": evidence["evidence_ref"], "observed_at_ms": _now_ms()}