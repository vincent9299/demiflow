"""LocalResourceProvider: finite local capacity plus managed process units.

Discovery precedes creation: an existing recorded spawn is adopted or retired,
never blindly duplicated. Capacity is released only on confirmed exit evidence.
"""
import os
from pathlib import Path
import sys
import time

from .capacity import OBSERVED_EXIT, observed_exit_reference
from .contracts import (
    HOST_PHASES, ResourceError, document, identifier, spec_hash, validate_observation,
)
from .process_observation import require_pidfds, unit_stopped, visibility
from .process_unit import _live, process_identity

PAGE_SIZE = 64
# One process looks for physically exited units at most this often while it waits.
OBSERVE_INTERVAL_S = 1.0


def _now_ms():
    return int(time.time() * 1000)


def _unit_state(record, here):
    """How the unit behind a live capacity record looks from this process.

    'gone'     its creating driver and the unit itself have physically exited: the
               conditions of ledger-observed release in the cleanup contract;
    'running'  its creating driver is alive and remains responsible for it;
    'orphaned' that driver has exited but the unit's exit cannot be established
               here: thread units, device work, a held, missing or replaced lock;
    'unknown'  no origin, or one this process cannot observe.
    """
    origin, identity = record.get("origin"), record.get("identity")
    try:
        if (origin["boot_id"], origin["pid_namespace"]) != (here["boot_id"], here["pid_namespace"]):
            return "unknown"
        owner = origin["owner"]
        if _live(owner["pid"], owner["start"]):
            return "running"
        if identity is None or origin["accelerators"] or type(identity["unit"]) is not dict:
            return "orphaned"
        if _live(identity["pid"], identity["start"]):
            return "orphaned"
        lock = Path(origin["directory"]) / "unit.lock"
    except (KeyError, TypeError):
        return "unknown"
    try:
        # Every process still doing this unit's work holds the lock it inherited.
        return "gone" if unit_stopped(lock, identity["unit"]) else "orphaned"
    except OSError:
        return "orphaned"


def _provider_uid(identity):
    if identity is None:
        return None
    return "local-" + spec_hash(identity)[:32]


class LocalResourceProvider:
    def __init__(self, pool, unit, *, control=None, device_pool=None):
        self.pool = pool
        self.unit = unit
        # Optional provider-specific control connector (Local socketpair in S4,
        # Ray actor methods in S5). Duck-typed so resources/ stays neutral.
        self._control = control
        # Without a device ledger the provider keeps the exact CPU/memory-only
        # rejection defaults; a configured ledger additively admits accelerators.
        self._device_pool = device_pool
        # The spawning controller is the parent and must reap its own children;
        # a zombie with a matching birth identity is not a live host.
        self._spawned = {}
        self._observe_after = 0.0
        self._holders = None

    def device_authority(self):
        if self._device_pool is None:
            return None
        return self._device_pool.authority()

    def released_device_seconds(self, allocation_id, spec_hash, device_type):
        if self._device_pool is None:
            return None
        return self._device_pool.released_device_seconds(allocation_id, spec_hash, device_type)

    def _reserve(self, allocation_id, spec_hash_value, effective, deadline_ms, directory):
        """Reserve for a unit that will live in `directory`, recording who creates it.

        When the domain is full, units whose physical exit this process can prove
        are released first; the request is then tried once more.
        """
        origin = {"owner": process_identity(os.getpid()), **visibility(), "directory": str(directory),
                  "accelerators": bool(effective.get("accelerators"))}
        try:
            return self.pool.reserve(allocation_id, spec_hash_value, effective, deadline_ms, origin=origin)
        except ResourceError as exc:
            if exc.code not in ("CAPACITY_PENDING", "QUOTA_EXCEEDED") or not self._release_gone():
                raise
        return self.pool.reserve(allocation_id, spec_hash_value, effective, deadline_ms, origin=origin)

    def _release_gone(self):
        now = time.monotonic()
        if now < self._observe_after:
            return 0
        self._observe_after = now + OBSERVE_INTERVAL_S
        here = visibility()
        released, held = 0, {"running": [0, 0], "orphaned": [0, 0], "unknown": [0, 0]}
        for record in self.pool.live_records():
            state = _unit_state(record, here)
            if state == "gone":
                try:
                    self.pool.release(record["allocation_id"], {
                        "confirmed": True, "identity": record["identity"],
                        "evidence_ref": observed_exit_reference(self.pool.root, record)})
                    released += 1
                except ResourceError:
                    # Its owner released it first, under the ledger lock, with its
                    # own evidence. That release stands and nothing is refunded twice.
                    pass
                continue
            held[state][0] += 1
            held[state][1] += record["effective"]["cpu_millis"]
        # For a caller that keeps waiting: who still holds the domain, as record
        # count and CPU millis per state of the unit's creating driver.
        self._holders = "capacity_holders " + " ".join(
            name + "=" + str(count) + "/" + str(millis) + "m" for name, (count, millis) in held.items())
        return released

    def _capacity_pending(self, scope_id, allocation_id, effective):
        conditions = ["capacity_pending"] + ([self._holders] if self._holders else [])
        return self._observation(scope_id, allocation_id, "PENDING", "FRESH",
                                 effective=effective, conditions=conditions)

    def describe_and_normalize(self, binding, requirements, spec):
        if not sys.platform.startswith("linux"):
            raise ResourceError("UNSUPPORTED_CAPABILITY", "Local managed units require Linux")
        try:
            require_pidfds()
        except (OSError, RuntimeError) as exc:
            raise ResourceError("UNSUPPORTED_CAPABILITY", "pidfd exit evidence unavailable: " + str(exc)) from exc
        profile = spec["profile"]
        requests = profile["requests"]
        placement = spec.get("placement") or {}
        device_plan = {"warnings": [], "device_ids": None, "pin_required": False}
        device_memory_bytes = None
        if self._device_pool is None:
            if requests.get("accelerators") or requests.get("custom"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider admits CPU/memory only")
            if profile.get("limits"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot enforce hard physical limits")
            if profile.get("requirements") or (requirements or {}):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot verify hardware requirements")
            if placement.get("required"):
                raise ResourceError("UNSATISFIABLE_CONSTRAINT", "Local provider has no placement constraints")
        else:
            if requests.get("custom"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot verify custom resources")
            if profile.get("limits"):
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot enforce hard physical limits")
            declared = dict(profile.get("requirements") or {})
            for key, value in (requirements or {}).items():
                if key in declared and declared[key] != value:
                    raise ResourceError("INVALID_SPEC", "conflicting hardware requirements: " + key)
                declared[key] = value
            if set(declared) - {"accelerator_memory_bytes"}:
                raise ResourceError("UNSUPPORTED_CAPABILITY", "Local provider cannot verify hardware requirements")
            if "accelerator_memory_bytes" in declared:
                device_memory_bytes = declared["accelerator_memory_bytes"]
                if type(device_memory_bytes) is not int or device_memory_bytes <= 0:
                    raise ResourceError("INVALID_SPEC", "accelerator_memory_bytes must be a positive int")
                if not requests.get("accelerators"):
                    raise ResourceError("INVALID_SPEC", "accelerator_memory_bytes needs accelerator demand")
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

    def _observation(self, scope_id, allocation_id, phase, freshness, identity=None,
                     cleanup="NONE", effective=None, conditions=None, evidence_ref=None,
                     placement=None):
        value = {
            "allocation_id": allocation_id, "provider_uid": _provider_uid(identity),
            "boot_id": None, "phase": phase, "observation": freshness,
            "observed_at_ms": _now_ms(), "observation_revision": _now_ms(),
            "cleanup": cleanup, "effective_resources": effective,
            "conditions": conditions or [], "evidence_ref": evidence_ref,
            "placement": placement,
            "ownership": {"scope_owned": [allocation_id], "shared": []},
        }
        return validate_observation(value)

    def _release_devices(self, allocation_id, evidence_ref):
        prior = self._device_pool.reservation_view(allocation_id) if self._device_pool is not None else None
        if prior is not None and prior['state'] != 'RELEASED':
            self._device_pool.release(allocation_id, {
                "confirmed": True, "identity": None, "evidence_ref": evidence_ref,
            })

    def resolve_evidence(self, scope_id, allocation_id, reference):
        if type(reference) is str and reference.startswith(OBSERVED_EXIT):
            return self._observed_exit(allocation_id, reference)
        return self.unit.resolve_evidence(scope_id, allocation_id, reference)

    def _accept_observed_exit(self, scope_id, allocation_id):
        """Confirm cleanup from the ledger's own release of this unit, if there is one.

        The ledger released it on its proof of physical exit. That stands for the
        unit this run spawned only when the identities agree; the unit lock is not
        consulted again and may be gone by now.
        """
        observed = self._observed_exit(allocation_id)
        if observed is None:
            return None
        spawned = self.unit.resolve_evidence(
            scope_id, allocation_id, self.unit.evidence_ref(scope_id, allocation_id, "process.json"))
        if spawned is None or spawned["identity"] != observed["identity"]:
            raise ResourceError("ARTIFACT_MISMATCH", "ledger-observed release names another physical unit",
                                allocation_id=allocation_id)
        self._spawned.pop(allocation_id, None)
        self._close_confirmed_control(allocation_id)
        return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                "evidence_ref": observed["release_evidence"], "observed_at_ms": _now_ms()}

    def _observed_exit(self, allocation_id, reference=None):
        """The ledger's own release record of this allocation, verified, or None.

        A released state alone proves nothing here: the reference must be the one
        this ledger root derives from the record it actually stores.
        """
        record = self.pool.record(allocation_id)
        if record is None or record["state"] != "RELEASED" or not str(
                record.get("release_evidence")).startswith(OBSERVED_EXIT):
            if reference is not None:
                raise ResourceError("ARTIFACT_MISMATCH", "no ledger-observed release for this allocation",
                                    allocation_id=allocation_id)
            return None
        if record["release_evidence"] != observed_exit_reference(self.pool.root, record) or (
                reference is not None and reference != record["release_evidence"]):
            raise ResourceError("ARTIFACT_MISMATCH", "ledger-observed release does not match its record",
                                allocation_id=allocation_id)
        return record

    def _device_placement(self, allocation_id):
        if self._device_pool is None:
            return None
        view = self._device_pool.reservation_view(allocation_id)
        if view is None:
            return None
        return {"device_ids": [d["device_id"] for d in view["devices"]]}

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
                                     evidence_ref=found["evidence_ref"],
                                     placement=self._device_placement(allocation_id))
        if found["phase"] == "TERMINATED":
            return self._observation(scope_id, allocation_id, "TERMINATED", "FRESH",
                                     identity=found["identity"], effective=effective,
                                     conditions=["exited_before_present"], evidence_ref=found["evidence_ref"],
                                     placement=self._device_placement(allocation_id))
        if found["observation"] == "STALE_UNKNOWN":
            return self._observation(scope_id, allocation_id, found["phase"], "STALE_UNKNOWN",
                                     identity=found["identity"], effective=effective,
                                     conditions=["identity_or_lock_unverified"], evidence_ref=found["evidence_ref"],
                                     placement=self._device_placement(allocation_id))
        try:
            self._reserve(allocation_id, immutable_spec["spec_hash"], effective,
                          immutable_spec["request"]["provision_deadline_ms"],
                          self.unit.directory(scope_id, allocation_id))
        except ResourceError as exc:
            if exc.code == "CAPACITY_PENDING":
                return self._capacity_pending(scope_id, allocation_id, effective)
            raise
        if self._device_pool is not None and effective.get("accelerators"):
            # Independent device pool: all-or-nothing per allocation. A device
            # shortfall withdraws the never-spawned CPU reservation rather than
            # holding it (hold-and-wait across pools is the deadlock the engine
            # forbids); withdrawal keeps the allocation id retryable.
            try:
                self._device_pool.reserve(
                    allocation_id, immutable_spec["spec_hash"], effective["accelerators"],
                    deadline_ms=immutable_spec["request"]["provision_deadline_ms"],
                    device_memory_bytes=normalized.get("device_memory_bytes"),
                    device_ids=normalized["device_plan"]["device_ids"],
                    pin_required=normalized["device_plan"]["pin_required"])
            except ResourceError as exc:
                self.pool.cancel_reservation(allocation_id)
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
                self._release_devices(allocation_id, "control-failed:" + allocation_id)
                return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                         effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        # The device authority's actual reservation determines child visibility.
        # This is CUDA runtime binding, not a claim of kernel/driver isolation.
        if self._device_pool is not None:
            placement = self._device_placement(allocation_id)
            visible = ','.join((placement or {}).get('device_ids', ()))
            payload['extra_env'] = {**(payload.get('extra_env') or {}),
                                    'CUDA_VISIBLE_DEVICES': visible}
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
                # Unknown spawn outcome keeps both pools charged until confirmed.
                return self._observation(scope_id, allocation_id, "STARTING", "STALE_UNKNOWN",
                                         effective=effective, conditions=["spawn_outcome_unknown"],
                                         placement=self._device_placement(allocation_id))
            self.pool.release(allocation_id, {
                "confirmed": True, "identity": None,
                "evidence_ref": "launch-failed:" + allocation_id + ":" + exc.code,
            })
            self._release_devices(allocation_id, "launch-failed:" + allocation_id + ":" + exc.code)
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=[exc.code, exc.reason])
        except OSError as exc:
            if self._control is not None:
                self._control.abort(allocation_id)
            self.pool.release(allocation_id, {
                "confirmed": True, "identity": None,
                "evidence_ref": "launch-failed:" + allocation_id + ":OSError",
            })
            self._release_devices(allocation_id, "launch-failed:" + allocation_id + ":OSError")
            return self._observation(scope_id, allocation_id, "FAILED", "FRESH",
                                     effective=effective, conditions=["LAUNCH_FAILED", str(exc)[:256]])
        if self._control is not None:
            self._control.spawned(allocation_id)
        identity = spawned["identity"]
        self._spawned[allocation_id] = spawned["process"]
        self.pool.bind_identity(allocation_id, identity)
        return self._observation(scope_id, allocation_id, "RUNNING", "FRESH",
                                 identity=identity, effective=effective,
                                 evidence_ref=self.unit.evidence_ref(scope_id, allocation_id, 'process.json'),
                                 placement=self._device_placement(allocation_id))

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

    def _close_confirmed_control(self, allocation_id):
        if self._control is None:
            return
        try:
            self._control.close(allocation_id)
        except Exception:
            # The physical exit proof and original business error remain true
            # even if closing a driver-side descriptor fails independently.
            import logging
            logging.getLogger(__name__).warning('Confirmed Local unit control socket close failed: %s',
                allocation_id, exc_info=True)

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        scope_id = exact_identity["scope_id"]
        allocation_id = exact_identity["allocation_id"]
        identifier(scope_id)
        identifier(allocation_id)
        process = self._spawned.get(allocation_id)
        accepted = self._accept_observed_exit(scope_id, allocation_id)
        if accepted is not None:
            return accepted
        found = self.unit.inspect(scope_id, allocation_id, process=process)
        if found["phase"] == "REQUESTED":
            # A capacity-pending request may never have reserved anything.
            # Unit absence proves no host existed; only an existing unbound
            # reservation needs release. A bound record still fails closed.
            prior = self.pool.reservation_view(allocation_id)
            if prior is not None and prior['state'] != 'RELEASED':
                self.pool.release(allocation_id, {
                    "confirmed": True, "identity": None,
                    "evidence_ref": "never-spawned:" + allocation_id,
                })
            self._release_devices(allocation_id, "never-spawned:" + allocation_id)
            self._close_confirmed_control(allocation_id)
            return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                    "evidence_ref": "never-spawned:" + allocation_id, "observed_at_ms": _now_ms()}
        evidence = self.unit.stop(scope_id, allocation_id, terminate_deadline_ms=terminate_deadline_ms,
                                  process=process)
        if evidence["confirmed"]:
            prior = self.pool.reservation_view(allocation_id)
            if prior is None or prior['state'] != 'RELEASED':
                try:
                    self.pool.release(allocation_id, evidence)
                except ResourceError:
                    # Another run's observation released the unit between the two
                    # ledger calls. Its release stands if it names this very unit.
                    accepted = self._accept_observed_exit(scope_id, allocation_id)
                    if accepted is None:
                        raise
                    return accepted
            self._release_devices(allocation_id, evidence["evidence_ref"])
            self._spawned.pop(allocation_id, None)
            self._close_confirmed_control(allocation_id)
            return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                    "evidence_ref": evidence["evidence_ref"], "observed_at_ms": _now_ms()}
        return {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                "evidence_ref": evidence["evidence_ref"], "observed_at_ms": _now_ms()}
