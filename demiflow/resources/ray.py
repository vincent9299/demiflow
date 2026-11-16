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
    def __init__(self, control, *, provision_timeout_s=30, device_pool=None, allocation_reader=None):
        # Duck-typed execution-side connector: host_class()/launch_spec()/adopt()
        # /close(). Keeping it injected means resources/ never imports execution.
        self._control = control
        self._provision_timeout_s = provision_timeout_s
        # Without a device ledger the provider keeps the exact CPU/memory-only
        # rejection defaults; a configured ledger additively admits logical GPUs
        # (Ray Core num_gpus placement + ledger memory-coexistence accounting).
        self._device_pool = device_pool
        self._actors = {}
        self._validated = set()
        self._owned = set()
        self._never_created = set()
        self._allocation_reader = allocation_reader

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

    def device_host_capacity(self, host_id):
        reader = getattr(self._control, 'device_host_capacity', None)
        return 0 if reader is None else reader(host_id)

    def device_host_feasible(self, host_id, profile):
        reader = getattr(self._control, 'device_host_feasible', None)
        return True if reader is None else reader(host_id, profile)

    def _actor_placement(self, allocation_id, actor, expected):
        value = self._device_placement(allocation_id) or {}
        value.update(actor_id=actor._actor_id.hex(),
                     ray_namespace=ray.get_runtime_context().namespace,
                     ray_cluster_id=ray._private.worker.global_worker.node.cluster_id.hex(),
                     expected_spec_hash=expected['spec_hash'], expected_boot_id=expected['boot_id'])
        binding = expected.get('accelerator_binding') or {}
        actual_uuids = [d['uuid'] for d in binding.get('devices', ())]
        if actual_uuids and actual_uuids != value.get('device_ids'):
            value['physical_gpu_uuids'] = actual_uuids
        return value

    def _finish_present(self, allocation_id, immutable_spec, normalized, actor, expected):
        scope_id = immutable_spec['scope_id']
        effective = normalized['effective']
        placement = self._actor_placement(allocation_id, actor, expected)
        try:
            live = ray.get(actor.identity.remote(), timeout=self._provision_timeout_s)
        except GetTimeoutError:
            # The original handle and its physical allocation remain owned.
            # A later retry verifies this same shell; cleanup must prove death.
            return self._observation(scope_id, allocation_id, 'PENDING', 'FRESH',
                effective=effective, conditions=['identity_pending', 'capacity_pending'], placement=placement,
                evidence_ref='ray-actor-pending:' + self._actor_name(allocation_id))
        except RayActorError as exc:
            self._actors.pop(allocation_id, None)
            self._validated.discard(allocation_id)
            self._owned.discard(allocation_id)
            self._control.abort(allocation_id)
            self._release_devices(allocation_id, 'ray-actor-dead:' + self._actor_name(allocation_id))
            return self._observation(scope_id, allocation_id, 'FAILED', 'FRESH',
                effective=effective, conditions=['LAUNCH_FAILED', str(exc)[:256]],
                placement=placement)
        if live.get('spec_hash') != expected['spec_hash'] or live.get('boot_id') != expected['boot_id']:
            # Do not kill a different actor merely because its name collided.
            raise ResourceError('ARTIFACT_MISMATCH', 'named actor spec_hash/boot differs',
                                allocation_id=allocation_id)
        self._owned.add(allocation_id)
        verifier = getattr(self._control, 'validate_device_binding', None)
        if allocation_id not in self._validated and verifier is not None:
            try:
                actual_ids = verifier(allocation_id, live,
                    accelerators=effective.get('accelerators', []),
                    device_memory_bytes=normalized.get('device_memory_bytes'))
            except Exception as exc:
                raise ResourceError('ARTIFACT_MISMATCH', 'Worker device binding rejected: ' + str(exc),
                                    allocation_id=allocation_id) from exc
            if effective.get('accelerators'):
                plan = normalized['device_plan']
                if plan['pin_required'] and not set(actual_ids) <= set(plan['device_ids']):
                    raise ResourceError('UNSATISFIABLE_CONSTRAINT',
                        'Ray did not assign the explicitly required devices', allocation_id=allocation_id)
                try:
                    self._device_pool.reserve(allocation_id, expected['spec_hash'], effective['accelerators'],
                        deadline_ms=immutable_spec['request']['provision_deadline_ms'],
                        device_memory_bytes=normalized.get('device_memory_bytes'),
                        device_ids=list(actual_ids), pin_required=True)
                except ResourceError as exc:
                    if exc.code != 'CAPACITY_PENDING':
                        raise
                    return self._observation(scope_id, allocation_id, 'PENDING', 'FRESH',
                        effective=effective, conditions=['device_admission_pending'], placement=placement,
                        evidence_ref='ray-actor-pending:' + self._actor_name(allocation_id))
        elif allocation_id in self._validated and live.get('accelerator_binding') != expected.get('accelerator_binding'):
            raise ResourceError('ARTIFACT_MISMATCH', 'Adopted worker device identity changed',
                                allocation_id=allocation_id)
        self._actors[allocation_id] = (actor, live, scope_id)
        if allocation_id not in self._validated:
            self._control.adopt(allocation_id, actor)
            self._validated.add(allocation_id)
        return self._observation(scope_id, allocation_id, 'RUNNING', 'FRESH',
            boot_id=live['boot_id'], effective=effective,
            provider_uid='ray-' + expected['spec_hash'][:32],
            evidence_ref='ray-actor:' + self._actor_name(allocation_id),
            placement=self._actor_placement(allocation_id, actor, live))

    def ensure_present(self, allocation_id, immutable_spec):
        identifier(allocation_id)
        scope_id = immutable_spec['scope_id']
        normalized = self.describe_and_normalize(
            immutable_spec.get('binding', {}), immutable_spec.get('requirements'), immutable_spec['request'])
        if normalized['spec_hash'] != immutable_spec['spec_hash']:
            raise ResourceError('ARTIFACT_MISMATCH', 'immutable spec hash differs', allocation_id=allocation_id)
        existing = self._actors.get(allocation_id)
        if existing is not None:
            actor, expected, owner = existing
            if owner != scope_id or expected['spec_hash'] != normalized['spec_hash']:
                raise ResourceError('ARTIFACT_MISMATCH', 'Owned actor allocation changed', allocation_id=allocation_id)
            return self._finish_present(allocation_id, immutable_spec, normalized, actor, expected)
        effective = normalized['effective']
        strict_binding = hasattr(self._control, 'validate_device_binding')
        if not strict_binding and self._device_pool is not None and effective.get('accelerators'):
            # Legacy resource-only logical hosts retain their original protocol.
            # Every unified Dataset connector uses verified physical binding.
            try:
                self._device_pool.reserve(allocation_id, normalized['spec_hash'], effective['accelerators'],
                    deadline_ms=immutable_spec['request']['provision_deadline_ms'],
                    device_memory_bytes=normalized.get('device_memory_bytes'),
                    device_ids=normalized['device_plan']['device_ids'],
                    pin_required=normalized['device_plan']['pin_required'])
            except ResourceError as exc:
                if exc.code != 'CAPACITY_PENDING':
                    raise
                self._never_created.add(allocation_id)
                return self._observation(scope_id, allocation_id, 'PENDING', 'FRESH',
                                         effective=effective, conditions=['capacity_pending'])
        launch = self._control.launch_spec(allocation_id)
        expected = {'spec_hash': normalized['spec_hash'], 'boot_id': launch['boot_id']}
        options = self._control.placement_options(allocation_id) if hasattr(self._control, 'placement_options') else {}
        self._never_created.discard(allocation_id)
        # Remote creation can have an unknown outcome. Only an actual handle or
        # original Ray death evidence permits subsequent confirmed cleanup.
        try:
            actor = ray.get_actor(self._actor_name(allocation_id))
        except ValueError:
            actor = self._control.host_class().options(
                name=self._actor_name(allocation_id), get_if_exists=False,
                num_cpus=max(.001, effective['cpu_millis']/1000), memory=effective['memory_bytes'],
                num_gpus=sum(x['count'] for x in effective.get('accelerators', [])), **options,
            ).remote(launch, expected['spec_hash'], expected['boot_id'])
            # This newly created handle cannot refer to a pre-existing name.
            # It authorizes cleanup of a pending shell, never business activation.
            self._owned.add(allocation_id)
        self._actors[allocation_id] = (actor, expected, scope_id)
        return self._finish_present(allocation_id, immutable_spec, normalized, actor, expected)

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
                phase, freshness = ("RUNNING" if allocation_id in self._validated else "PENDING"), "FRESH"
            except RayActorError:
                phase, freshness = "LOST", "FRESH"
            except GetTimeoutError:
                phase, freshness = "RUNNING", "STALE_UNKNOWN"
            allocations.append(self._observation(
                scope_id, allocation_id, phase, freshness, boot_id=identity["boot_id"],
                provider_uid="ray-" + identity["spec_hash"][:32],
                evidence_ref="ray-actor:" + self._actor_name(allocation_id),
                placement=self._actor_placement(allocation_id, actor, identity)))
        return {"allocations": allocations, "next_page_token": None, "sampled_at_ms": _now_ms()}

    def _unconfirmed(self, allocation_id):
        return {'allocation_id': allocation_id, 'cleanup': 'UNCONFIRMED',
                'evidence_ref': 'ray-actor-unconfirmed:' + self._actor_name(allocation_id),
                'observed_at_ms': _now_ms()}

    def _confirmed_dead(self, allocation_id):
        self._actors.pop(allocation_id, None)
        self._validated.discard(allocation_id)
        self._owned.discard(allocation_id)
        self._control.close(allocation_id)
        proof = 'ray-actor-dead:' + self._actor_name(allocation_id)
        self._release_devices(allocation_id, proof)
        return {'allocation_id': allocation_id, 'cleanup': 'CONFIRMED',
                'evidence_ref': proof, 'observed_at_ms': _now_ms()}

    def _recover_actor(self, exact_identity, deadline):
        allocation_id = exact_identity['allocation_id']
        scope_id = exact_identity['scope_id']
        record = None if self._allocation_reader is None else self._allocation_reader(scope_id, allocation_id)
        observation = {} if record is None else record['observation']
        placement = observation.get('placement') or {}
        cluster = ray._private.worker.global_worker.node.cluster_id.hex()
        if placement.get('ray_cluster_id') not in (None, cluster):
            return None, False
        expected_hash = None if record is None else record['spec_hash']
        device = None if self._device_pool is None else self._device_pool.reservation_view(allocation_id)
        if expected_hash is None and device is not None:
            expected_hash = device['spec_hash']
        expected_boot = exact_identity.get('boot_id') or observation.get('boot_id') or placement.get('expected_boot_id')
        if expected_hash is None or expected_boot is None:
            return None, False
        if (placement.get('expected_spec_hash') not in (None, expected_hash)
                or placement.get('expected_boot_id') not in (None, expected_boot)):
            return None, False
        actor_id = placement.get('actor_id')
        if actor_id is not None:
            info = ray._private.state.actors(actor_id)
            if not info or info.get('Name') != self._actor_name(allocation_id):
                return None, False
            if info.get('State') == 'DEAD':
                return None, True  # Exact original actor in the same Ray cluster.
        try:
            actor = ray.get_actor(self._actor_name(allocation_id),
                                  namespace=placement.get('ray_namespace', ray.get_runtime_context().namespace))
        except ValueError:
            return None, False  # Absence of a name is not original exit evidence.
        if actor_id is not None and actor._actor_id.hex() != actor_id:
            return None, False
        live = ray.get(actor.identity.remote(), timeout=max(.001, deadline - time.monotonic()))
        if live.get('spec_hash') != expected_hash or live.get('boot_id') != expected_boot:
            return None, False
        entry = (actor, live, scope_id)
        self._actors[allocation_id] = entry
        self._owned.add(allocation_id)  # Ownership only; never business-binding admission.
        return entry, False

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        scope_id = exact_identity['scope_id']
        allocation_id = exact_identity['allocation_id']
        identifier(scope_id); identifier(allocation_id)
        deadline = time.monotonic() + terminate_deadline_ms / 1000
        entry = self._actors.get(allocation_id)
        if entry is None:
            if allocation_id in self._never_created:
                self._release_devices(allocation_id, 'never-created:' + allocation_id)
                return {'allocation_id': allocation_id, 'cleanup': 'CONFIRMED',
                        'evidence_ref': 'never-created:' + allocation_id, 'observed_at_ms': _now_ms()}
            try:
                entry, dead = self._recover_actor(exact_identity, deadline)
            except (GetTimeoutError, RayActorError, ValueError):
                return self._unconfirmed(allocation_id)
            if dead:
                return self._confirmed_dead(allocation_id)
            if entry is None:
                return self._unconfirmed(allocation_id)
        actor, expected, owner = entry
        if owner != scope_id or exact_identity.get('boot_id') not in (None, expected['boot_id']):
            return self._unconfirmed(allocation_id)
        if allocation_id not in self._owned:
            try:
                live = ray.get(actor.identity.remote(), timeout=max(.001, deadline - time.monotonic()))
            except RayActorError:
                return self._confirmed_dead(allocation_id)
            except GetTimeoutError:
                return self._unconfirmed(allocation_id)
            if live.get('spec_hash') != expected['spec_hash'] or live.get('boot_id') != expected['boot_id']:
                return self._unconfirmed(allocation_id)
        ray.kill(actor, no_restart=True)
        while time.monotonic() < deadline:
            try:
                ray.get(actor.identity.remote(), timeout=max(.001, deadline - time.monotonic()))
            except RayActorError:
                return self._confirmed_dead(allocation_id)
            except GetTimeoutError:
                break
            time.sleep(min(.01, max(0, deadline - time.monotonic())))
        return self._unconfirmed(allocation_id)
