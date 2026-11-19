"""Shared ResourceManager: five protocol operations plus bounded reconcile.

Standard library and neutral contracts only. The state port is injected; the
reconcile step is driven by the existing engine coordination loop, never a
resident daemon. Provider I/O happens outside any journal transaction.
"""
from dataclasses import dataclass
import math
import time

from .contracts import (
    RESOURCE_PROTOCOL_VERSION, ResourceError, ResourcePlan, MutationContext,
    document, identifier, parse_plan, spec_hash, validate_observation,
)


@dataclass(frozen=True)
class ManagerBounds:
    max_desired_workers: int = 64
    max_retirements: int = 64
    max_claim: int = 16
    page_size: int = 64
    event_window: int = 1024
    claim_lease_ms: int = 30000
    operation_retention_ms: int = 3600000
    watch_poll_s: float = 0.05

    def __post_init__(self):
        for name in ("max_desired_workers", "max_retirements", "max_claim",
                     "page_size", "event_window"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 65536:
                raise ResourceError("INVALID_SPEC", name + " must be a bounded positive int")
        for name in ("claim_lease_ms", "operation_retention_ms"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 2**63 - 1:
                raise ResourceError("INVALID_SPEC", name + " must be a positive int duration")
        if type(self.watch_poll_s) not in (int, float) or not math.isfinite(self.watch_poll_s) or not 0 < self.watch_poll_s <= 5:
            raise ResourceError("INVALID_SPEC", "watch_poll_s must be finite in (0, 5]")
        if self.max_claim > self.max_desired_workers + self.max_retirements:
            raise ResourceError("INVALID_SPEC", "max_claim exceeds the plan operation bound")


_CLOSE_KEYS = frozenset({"mode", "engine_receipt_ref", "terminate_deadline_ms", "reason"})


def _failed_observation(allocation_id, spec, exc):
    return validate_observation({
        "allocation_id": allocation_id, "provider_uid": None, "boot_id": None,
        "phase": "FAILED", "observation": "FRESH",
        "observed_at_ms": int(time.time() * 1000), "observation_revision": 0,
        "cleanup": "NONE", "effective_resources": spec.get("effective"),
        "conditions": [exc.code, exc.reason], "evidence_ref": exc.evidence_ref,
        "placement": None, "ownership": {"scope_owned": [allocation_id], "shared": []},
    })


def _validate_close_spec(value):
    if type(value) is not dict or set(value) != _CLOSE_KEYS:
        raise ResourceError("INVALID_SPEC", "close_spec key set is unsupported")
    if value["mode"] not in ("DRAINED", "ABANDON"):
        raise ResourceError("INVALID_SPEC", "close mode must be DRAINED or ABANDON")
    identifier(value["engine_receipt_ref"])
    if type(value["terminate_deadline_ms"]) is not int or value["terminate_deadline_ms"] <= 0:
        raise ResourceError("INVALID_SPEC", "close terminate_deadline_ms must be positive")
    if type(value["reason"]) is not str or not 1 <= len(value["reason"]) <= 256:
        raise ResourceError("INVALID_SPEC", "close reason must be a bounded string")
    return dict(value)


class ResourceManager:
    def __init__(self, provider, state, *, bounds=None):
        self.provider = provider
        self.state = state
        self.bounds = bounds or ManagerBounds()
        self._last_revision = 0

    def describe(self, binding, requirements):
        """Preflight only: prove normalization through the provider entry, no reservation."""
        probe = {"profile": {"requests": {"cpu_millis": 1, "memory_bytes": 1}}}
        normalized = self.provider.describe_and_normalize(binding, requirements, probe)
        document(binding, 4096)
        return {
            "protocol_version": dict(RESOURCE_PROTOCOL_VERSION),
            "binding": binding,
            "probe": {"requests": dict(probe["profile"]["requests"]),
                      "effective": normalized["effective"]},
            "bounds": {
                "max_desired_workers": self.bounds.max_desired_workers,
                "max_retirements": self.bounds.max_retirements,
                "page_size": self.bounds.page_size,
                "event_window": self.bounds.event_window,
                "operation_retention_ms": self.bounds.operation_retention_ms,
            },
            "evidence_policy": "confirmed-exit-required",
        }

    def _parse(self, plan):
        if type(plan) is ResourcePlan:
            parsed = plan
        else:
            parsed = parse_plan(plan, max_workers=self.bounds.max_desired_workers,
                                max_retirements=self.bounds.max_retirements)
        if len(parsed.desired_workers) > self.bounds.max_desired_workers:
            raise ResourceError("INVALID_SPEC", "desired_workers exceeds manager bound")
        if len(parsed.retirements) > self.bounds.max_retirements:
            raise ResourceError("INVALID_SPEC", "retirements exceeds manager bound")
        return parsed

    def apply(self, context, plan):
        if getattr(self.state, 'cleanup_only', False):
            raise ResourceError('PERMISSION_DENIED', 'cleanup cannot admit a resource plan')
        if type(context) is not MutationContext:
            raise ResourceError("INVALID_SPEC", "apply needs a MutationContext")
        parsed = self._parse(plan)
        if parsed.scope_id != context.scope_id:
            raise ResourceError("INVALID_SPEC", "plan scope differs from context", scope_id=context.scope_id)
        workers = {}
        for worker in parsed.desired_workers:
            request = worker.to_dict()
            normalized = self.provider.describe_and_normalize(
                None, worker.profile.requirements, request)
            workers[worker.allocation_id] = {
                "request": request, "spec_hash": normalized["spec_hash"],
                "effective": normalized["effective"],
                "requirements": worker.profile.requirements,
            }
            if normalized.get("warnings"):
                workers[worker.allocation_id]["warnings"] = normalized["warnings"]
        validated = {"plan": parsed.to_dict(), "workers": workers,
                     "payload_hash": spec_hash(parsed.to_dict())}
        return self.state.accept_plan(context, validated)

    def get(self, scope_id, page_token=None):
        identifier(scope_id)
        return self.state.read_snapshot(scope_id, page_token)

    def watch(self, scope_id, cursor, limit, timeout):
        identifier(scope_id)
        if type(cursor) is not int or cursor < 0:
            raise ResourceError("INVALID_SPEC", "watch cursor must be a nonnegative int")
        if type(limit) is not int or not 1 <= limit <= self.bounds.event_window:
            raise ResourceError("INVALID_SPEC", "watch limit exceeds the event window")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 <= timeout <= 30:
            raise ResourceError("INVALID_SPEC", "watch timeout must be in [0, 30] seconds")
        deadline = time.monotonic() + timeout
        while True:
            batch = self.state.read_events(scope_id, cursor, limit)
            if batch["events"] or time.monotonic() >= deadline:
                return batch
            time.sleep(min(self.bounds.watch_poll_s, max(0.0, deadline - time.monotonic())))

    def close(self, context, close_spec):
        if type(context) is not MutationContext:
            raise ResourceError("INVALID_SPEC", "close needs a MutationContext")
        return self.state.begin_close(context, _validate_close_spec(close_spec))

    def _revision(self, entry):
        now = int(time.time() * 1000)
        value = max(now, self._last_revision + 1, entry.get("observation_revision") or 0)
        self._last_revision = value
        return value

    def _stale(self, allocation_id, conditions, evidence_ref=None):
        entry = {"allocation_id": allocation_id, "observation": "STALE_UNKNOWN",
                 "observed_at_ms": int(time.time() * 1000),
                 "conditions": conditions}
        if evidence_ref is not None:
            entry["evidence_ref"] = evidence_ref
        entry["observation_revision"] = self._revision(entry)
        return entry

    def _reconcile_operation(self, operation):
        if getattr(self.state, 'cleanup_only', False) and (
                operation['kind'] != 'ensure_absent' or operation['scope_id'] != self.state.cleanup_scope):
            raise ResourceError('PERMISSION_DENIED', 'cleanup cannot dispatch another scope or ensure_present')
        allocation_id = operation["allocation_id"]
        spec = operation["document"]
        try:
            if operation["kind"] == "ensure_present":
                entry = dict(self.provider.ensure_present(allocation_id, spec))
                entry["observation_revision"] = self._revision(entry)
                return entry
            identity = {"scope_id": operation["scope_id"], "allocation_id": allocation_id,
                        "provider_uid": spec.get("provider_uid"), "boot_id": spec.get("boot_id")}
            result = self.provider.ensure_absent(identity, spec["terminate_deadline_ms"])
            entry = {"allocation_id": allocation_id, "cleanup": result["cleanup"],
                     "observed_at_ms": result["observed_at_ms"],
                     "evidence_ref": result.get("evidence_ref")}
            entry["observation_revision"] = self._revision(entry)
            return entry
        except ResourceError as exc:
            if operation["kind"] == "ensure_absent":
                entry = {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                         "observed_at_ms": int(time.time() * 1000),
                         "conditions": [exc.code, exc.reason],
                         "evidence_ref": exc.evidence_ref}
                entry["observation_revision"] = self._revision(entry)
                return entry
            if exc.code in ("ARTIFACT_MISMATCH", "INVALID_SPEC", "UNSUPPORTED_CAPABILITY",
                            "UNSATISFIABLE_CONSTRAINT", "LAUNCH_FAILED"):
                entry = _failed_observation(allocation_id, spec, exc)
                entry["observation_revision"] = self._revision(entry)
                return entry
            return self._stale(allocation_id, [exc.code, exc.reason], exc.evidence_ref)
        except Exception as exc:
            if operation["kind"] == "ensure_absent":
                # Cleanup I/O returned an error, rather than disappearing with
                # its controller. Keep exit unknown and resources charged, but
                # let the same intent retry under the state's bounded backoff.
                entry = {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                         "observed_at_ms": int(time.time() * 1000),
                         "conditions": ["PROVIDER_UNAVAILABLE", str(exc)[:256]]}
                entry["observation_revision"] = self._revision(entry)
                return entry
            return self._stale(allocation_id, ["PROVIDER_UNAVAILABLE", str(exc)[:256]])

    def reconcile_once(self, authority, deadline, limit):
        """One bounded step for the existing engine loop; no journal transaction
        is held across provider I/O."""
        if type(limit) is not int or limit <= 0:
            raise ResourceError("INVALID_SPEC", "reconcile limit must be positive")
        now_ms = int(time.time() * 1000)
        batch = self.state.claim_operations(authority, now_ms, min(limit, self.bounds.max_claim))
        observations = []
        for operation in batch:
            if time.monotonic() >= deadline:
                break
            if getattr(self.state, 'cleanup_only', False):
                operation = {**operation, 'document': {**operation['document'],
                    'terminate_deadline_ms': min(operation['document']['terminate_deadline_ms'],
                        max(1, int((deadline - time.monotonic()) * 1000)))}}
            observations.append(self._reconcile_operation(operation))
        cursor = None
        if observations:
            cursor = self.state.record_observations(authority, observations)
        if getattr(self.state, 'cleanup_only', False):
            for entry in observations:
                conditions = entry.get('conditions') or ()
                if conditions and conditions[0] in {'ARTIFACT_MISMATCH', 'INVALID_SPEC', 'UNSUPPORTED_CAPABILITY'}:
                    # Keep the original UNCONFIRMED observation, then surface
                    # an unrecoverable binding error instead of hiding it in a
                    # generic timeout after repeated provider calls.
                    raise ResourceError(conditions[0], conditions[1], allocation_id=entry['allocation_id'])
        return {"claimed": len(batch), "processed": len(observations), "cursor": cursor}
