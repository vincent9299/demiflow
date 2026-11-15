"""RayResourceProvider: Ray Core named-actor hosting (U2/S5, A design §08).

This is the only module inside ``resources/`` that imports Ray (Core only). The
neutral contracts/manager/capacity/process_unit/local modules never load Ray.
Ray Core places and reserves the actor's CPU/memory; the manager only checks the
scope authorization ceiling, so there is no LocalCapacityPool double-charge
(A design §06 single_charge).

The actor host class and the commit transport (actor methods driver->worker plus
a bounded outbox pull worker->driver) live on the execution side and are injected
as ``control`` so this package stays neutral. The provider owns the actor
lifecycle: a named actor with ``max_restarts=0``/``max_task_retries=0`` (host
replacement is a new allocation, never an automatic business replay), explicit
spec_hash/boot verification when ``get_if_exists`` returns an existing instance,
and confirmed death evidence before cleanup is reported (``ray.kill`` called is
not itself confirmation).
"""
import time

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from .contracts import (
    ResourceError, identifier, spec_hash, validate_observation,
)


def _now_ms():
    return int(time.time() * 1000)


class RayResourceProvider:
    def __init__(self, control, *, provision_timeout_s=30, device_pool=None):
        # Duck-typed execution-side connector: host_class()/launch_spec()/adopt()
        # /close(). Keeping it injected means resources/ never imports execution.
        self._control = control
        self._provision_timeout_s = provision_timeout_s
        # Without a device ledger the provider keeps the exact CPU/memory-only
        # rejection defaults; a configured ledger additively admits logical GPUs
        # (Ray Core num_gpus placement + ledger memory-coexistence accounting).
        self._device_pool = device_pool
        self._actors = {}

    def describe_and_normalize(self, binding, requirements, spec):
        if not ray.is_initialized():
            raise ResourceError("PROVIDER_UNAVAILABLE", "Ray Core driver is not initialized")
        profile = spec["profile"]
        requests = profile["requests"]
        placement = spec.get("placement") or {}
        device_plan = {"warnings": [], "device_ids": None, "pin_required": False}
        device_memory_bytes = None
        if self._device_pool is None:
            if requests.get("accelerators") or requests.get("custom"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore first batch admits CPU/memory only")
            if profile.get("limits"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore provider does not enforce hard physical limits")
            if profile.get("requirements") or (requirements or {}):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore first batch cannot verify hardware requirements")
            if placement.get("required"):
                raise ResourceError("UNSATISFIABLE_CONSTRAINT", "RayCore first batch has no placement constraints")
        else:
            if requests.get("custom"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore provider cannot verify custom resources")
            if profile.get("limits"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore provider does not enforce hard physical limits")
            declared = dict(profile.get("requirements") or {})
            for key, value in (requirements or {}).items():
                if key in declared and declared[key] != value:
                    raise ResourceError("INVALID_SPEC", "conflicting hardware requirements: " + key)
                declared[key] = value
            if set(declared) - {"accelerator_memory_bytes"}:
                raise ResourceError("UNSUPPORTED_CAPABILITY", "RayCore provider cannot verify hardware requirements")
            if "accelerator_memory_bytes" in declared:
                device_memory_bytes = declared["accelerator_memory_bytes"]
                if type(device_memory_bytes) is not int or device_memory_bytes <= 0:
                    raise ResourceError("INVALID_SPEC", "accelerator_memory_bytes must be a positive int")
                if not requests.get("accelerators"):
                    raise ResourceError("INVALID_SPEC", "accelerator_memory_bytes needs accelerator demand")
            for item in requests.get("accelerators", []):
                if item["type"] != "gpu":
                    raise ResourceError("UNSUPPORTED_CAPABILITY",
                                        "RayCore provider maps accelerator type gpu only")
            if requests.get("accelerators") or placement:
                device_plan = self._device_pool.evaluate(
                    requests.get("accelerators", []), device_memory_bytes=device_memory_bytes,
                    placement=placement)
        effective = {"cpu_millis": requests["cpu_millis"], "memory_bytes": requests["memory_bytes"]}
        if "scratch_bytes" in requests:
            effective["scratch_bytes"] = requests["scratch_bytes"]
        if self._device_pool is not None and requests.get("accelerators"):
            effective["accelerators"] = [dict(item) for item in requests["accelerators"]]
        normalized = {"effective": effective, "spec": spec, "spec_hash": spec_hash(spec)}
        if self._device_pool is not None:
            normalized["warnings"] = device_plan["warnings"]
            normalized["device_plan"] = device_plan
            normalized["device_memory_bytes"] = device_memory_bytes
        return normalized

    def _observation(self, scope_id, allocation_id, phase, freshness, *, boot_id=None,
                     cleanup="NONE", effective=None, conditions=None, evidence_ref=None,
                     provider_uid=None, placement=None):
        value = {
            "allocation_id": allocation_id, "provider_uid": provider_uid,
            "boot_id": boot_id, "phase": phase, "observation": freshness,
            "observed_at_ms": _now_ms(), "observation_revision": _now_ms(),
            "cleanup": cleanup, "effective_resources": effective,
            "conditions": conditions or [], "evidence_ref": evidence_ref,
            "placement": placement,
            "ownership": {"scope_owned": [allocation_id], "shared": []},
        }
        return validate_observation(value)

    def _release_devices(self, allocation_id, evidence_ref):
        if self._device_pool is not None and self._device_pool.reservation_view(allocation_id) is not None:
            self._device_pool.release(allocation_id, {
                "confirmed": True, "identity": None, "evidence_ref": evidence_ref,
            })

    def _device_placement(self, allocation_id):
        if self._device_pool is None:
            return None
        view = self._device_pool.reservation_view(allocation_id)
        if view is None:
            return None
        return {"device_ids": [d["device_id"] for d in view["devices"]]}

    def _actor_name(self, allocation_id):
        return "demiflow-worker-" + allocation_id

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
        expected_hash = immutable_spec["spec_hash"]
        existing = self._actors.get(allocation_id)
        if existing is not None:
            actor, identity, _ = existing
            try:
                live = ray.get(actor.identity.remote(), timeout=self._provision_timeout_s)
            except RayActorError:
                self._actors.pop(allocation_id, None)
                return self._observation(scope_id, allocation_id, "LOST", "FRESH",
                                         effective=effective, conditions=["actor_died_before_present"],
                                         evidence_ref="ray-actor-dead:" + self._actor_name(allocation_id))
            if live["spec_hash"] == expected_hash and live["boot_id"] == identity["boot_id"]:
                return self._observation(scope_id, allocation_id, "RUNNING", "FRESH",
                                         boot_id=live["boot_id"], effective=effective,
                                         provider_uid="ray-" + expected_hash[:32],
                                         evidence_ref="ray-actor:" + self._actor_name(allocation_id))
            ray.kill(actor, no_restart=True)
            self._actors.pop(allocation_id, None)
            raise ResourceError("ARTIFACT_MISMATCH", "named actor spec_hash/boot differs",
                                allocation_id=allocation_id)
        launch_spec = self._control.launch_spec(allocation_id)
        boot_id = launch_spec["boot_id"]
        num_cpus = max(0.001, effective["cpu_millis"] / 1000.0)
        num_gpus = 0
        if self._device_pool is not None and effective.get("accelerators"):
            try:
                self._device_pool.reserve(
                    allocation_id, expected_hash, effective["accelerators"],
                    deadline_ms=immutable_spec["request"]["provision_deadline_ms"],
                    device_memory_bytes=normalized.get("device_memory_bytes"),
                    device_ids=normalized["device_plan"]["device_ids"],
                    pin_required=normalized["device_plan"]["pin_required"])
            except ResourceError as exc:
                if exc.code == "CAPACITY_PENDING":
                    return self._observation(scope_id, allocation_id, "PENDING", "FRESH",
                                             effective=effective, conditions=["capacity_pending"])
                raise
            num_gpus = sum(item["count"] for item in effective["accelerators"])
        name = self._actor_name(allocation_id)
        host = self._control.host_class()
        placement_options = (self._control.placement_options(allocation_id)
                             if hasattr(self._control, 'placement_options') else {})
        try:
            actor = host.options(
                name=name, get_if_exists=True, num_cpus=num_cpus,
                memory=effective["memory_bytes"], num_gpus=num_gpus, **placement_options,
            ).remote(launch_spec, expected_hash, boot_id)
        except Exception as exc:
            self._release_devices(allocation_id, "launch-failed:" + allocation_id)
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        try:
            identity = ray.get(actor.identity.remote(), timeout=self._provision_timeout_s)
        except GetTimeoutError:
            # Ray Core could not place the actor within the provision deadline
            # (e.g. requested CPUs exceed the cluster): pending, not running.
            # The device reservation stays charged (an actor handle existed);
            # retry re-reserves idempotently, ensure_absent releases on evidence.
            ray.kill(actor, no_restart=True)
            return self._observation(scope_id, allocation_id, "PENDING", "FRESH",
                                     effective=effective, conditions=["capacity_pending"],
                                     evidence_ref="ray-actor-pending:" + name)
        except RayActorError as exc:
            self._control.abort(allocation_id)
            self._release_devices(allocation_id, "launch-failed:" + allocation_id)
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        except Exception as exc:
            ray.kill(actor, no_restart=True)
            self._release_devices(allocation_id, "launch-failed:" + allocation_id)
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        # get_if_exists may return a pre-existing instance without re-running the
        # constructor; verify its birth identity before adopting it (A design §08).
        if identity["spec_hash"] != expected_hash or identity["boot_id"] != boot_id:
            ray.kill(actor, no_restart=True)
            self._control.abort(allocation_id)
            raise ResourceError("ARTIFACT_MISMATCH", "named actor spec_hash/boot differs",
                                allocation_id=allocation_id)
        self._actors[allocation_id] = (actor, identity, scope_id)
        self._control.adopt(allocation_id, actor)
        return self._observation(scope_id, allocation_id, "RUNNING", "FRESH",
                                 boot_id=boot_id, effective=effective,
                                 provider_uid="ray-" + expected_hash[:32],
                                 evidence_ref="ray-actor:" + name,
                                 placement=self._device_placement(allocation_id))

    def inspect(self, scope_id, page_token):
        identifier(scope_id)
        if page_token is not None:
            raise ResourceError("RESYNC_REQUIRED", "RayCore inventory is single-page")
        allocations = []
        for allocation_id, (actor, identity, owner_scope) in sorted(self._actors.items()):
            if owner_scope != scope_id:
                continue
            try:
                ray.get(actor.identity.remote(), timeout=self._provision_timeout_s)
                phase, freshness = "RUNNING", "FRESH"
            except RayActorError:
                phase, freshness = "LOST", "FRESH"
            except GetTimeoutError:
                phase, freshness = "RUNNING", "STALE_UNKNOWN"
            allocations.append(self._observation(
                scope_id, allocation_id, phase, freshness, boot_id=identity["boot_id"],
                provider_uid="ray-" + identity["spec_hash"][:32],
                evidence_ref="ray-actor:" + self._actor_name(allocation_id)))
        return {"allocations": allocations, "next_page_token": None, "sampled_at_ms": _now_ms()}

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        scope_id = exact_identity["scope_id"]
        allocation_id = exact_identity["allocation_id"]
        identifier(scope_id)
        identifier(allocation_id)
        entry = self._actors.get(allocation_id)
        name = self._actor_name(allocation_id)
        if entry is None:
            self._release_devices(allocation_id, "never-created:" + allocation_id)
            return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                    "evidence_ref": "never-created:" + allocation_id, "observed_at_ms": _now_ms()}
        actor, identity, _ = entry
        ray.kill(actor, no_restart=True)
        # Kill is asynchronous. A queued identity request may still finish while
        # the actor is exiting; keep observing within this release's deadline.
        # Returning immediately on that success strands the native cleanup intent
        # until its claim lease expires (the same length as the caller deadline).
        deadline = time.monotonic() + terminate_deadline_ms / 1000.0
        while time.monotonic() < deadline:
            try:
                ray.get(actor.identity.remote(),
                        timeout=max(0.001, deadline - time.monotonic()))
            except RayActorError:
                self._actors.pop(allocation_id, None)
                self._control.close(allocation_id)
                self._release_devices(allocation_id, "ray-actor-dead:" + name)
                return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                        "evidence_ref": "ray-actor-dead:" + name, "observed_at_ms": _now_ms()}
            except GetTimeoutError:
                break
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        return {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                "evidence_ref": "ray-actor-unconfirmed:" + name, "observed_at_ms": _now_ms()}
