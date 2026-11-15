"""Worker-side lifecycle command executor (U2/S3).

Consumes the frozen E2 contracts (K3 worker messages, K1 manifest, envelope
fencing) without redefining them. The driver owns a WorkerProtocolModel per
worker for receipts/idempotency/fence; this runtime performs the actual side
effects: K1 recheck, read-only state restore, granted-fence authority attach at
Activate, the business-input gate, and drain/cancel. The data path (source-in,
run_stream drive, CommitCut/CommitReceipt wire) is injected and wired in S4; the
worker never builds a second commit authority.
"""
import tempfile
import time
import uuid
from pathlib import Path

from ..contracts.artifact import verify_manifest
from ..contracts.envelope import (
    WORKER_PROTOCOL_VERSION, OperationReceipts, check_boot, check_controller_epoch,
    check_grant, require_capabilities,
)
from ..contracts.errors import ProtocolError
from ..contracts.worker import apply_assignment, next_worker_state, validate_message
from ..recovery_contract import RecoveryContractError
from ..run_journal import RunJournal
from ..state_artifacts import StateArtifacts
from .authority import WorkerAuthority


class _ReadContext:
    """Read-only directory/limits view for state restore before Activate."""

    def __init__(self, directory, limits):
        self.directory, self.limits = directory, limits


def _is_dataflow_executor(executor):
    return executor is not None and executor.__class__.__name__ == "DataflowWorkerExecutor"


def _decode_v3_assignment(config):
    from ..contracts.dataflow import Bounds, WorkerAssignment
    from ..contracts.dataflow_codec import decode_wire
    assignment = config["assignment"]
    if isinstance(assignment, WorkerAssignment):
        return assignment
    bounds = config["bounds"]
    if isinstance(bounds, dict):
        bounds = Bounds(**bounds)
    raw = assignment["wire"]
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return decode_wire(raw, type_name="WorkerAssignment", bounds=bounds)


def _build_v3_executor(config, *, control):
    """Assemble the unified v3 executor from a sealed launch payload (H027/B001/B002)."""
    from ..dataflow_services import make_runtime_services
    from .dataflow_execution import DataflowWorkerExecutor
    assignment = _decode_v3_assignment(config)
    transport = config.get("transport")
    if transport is None:
        transport = control
    cache_value = config.get("cache")
    owned_cache = None
    if cache_value is None:
        owned_cache = tempfile.mkdtemp(prefix="demiflow-worker-cache-")
        cache_value = owned_cache
    cache = Path(cache_value)
    # Every assignment owns its private code cache, including a configured
    # run-local parent. No worker removes another worker's files.
    owned_cache = Path(owned_cache) if owned_cache is not None else cache / assignment.lease.allocation_id
    try:
        services = make_runtime_services(assignment, transport=transport, cache=cache)
    except BaseException:
        import shutil
        shutil.rmtree(owned_cache, ignore_errors=True)
        raise
    mailbox = config.get("mailbox")
    if mailbox is None:
        from ..dataflow_hosts import ControlMailbox
        mailbox = ControlMailbox(bounds=services.bounds, check_stop=services.check_stop)
    executor = DataflowWorkerExecutor(services=services, mailbox=mailbox)
    executor._owned_services_cache = Path(owned_cache) if owned_cache is not None else None
    executor.load(assignment)
    return executor


class WorkerRuntime:
    def __init__(self, launch_spec, *, control=None, manifest_loader=None, executor=None):
        self.directory = Path(launch_spec["directory"]).absolute()
        self.run_id = launch_spec["run_id"]
        self.scope_id = launch_spec["scope_id"]
        self.allocation_id = launch_spec["allocation_id"]
        self.boot_id = launch_spec["boot_id"]
        self.controller_epoch = launch_spec["controller_epoch"]
        self.capabilities = list(launch_spec.get("capabilities", ()))
        self.grant = launch_spec.get("grant")
        self.state = "REGISTERED"
        self.receipts = OperationReceipts()
        self.plan_revision = 0
        self.assignment = None
        self.authority = None
        self.progress = None
        self._control = control
        self._manifest_loader = manifest_loader
        self._executor = executor
        self._restored = {}
        self._admit_open = False
        self._draining = False
        self._v3_config = launch_spec.get("dataflow_v3")
        self._v3 = self._v3_config is not None or _is_dataflow_executor(executor)
        self._v3_executor = executor if _is_dataflow_executor(executor) else None
        if self._v3:
            # v3 workers never open the driver RunJournal database; limits and
            # the code pin come from the sealed launch payload and are mandatory
            # (no silent empty default — finite-resource premise I08 / code-pin).
            from ..contracts.dataflow_errors import DataflowError
            config = self._v3_config or {}
            limits = config.get("limits")
            if type(limits) is not dict or not limits:
                raise DataflowError("INVALID_CONTRACT", "v3 launch payload missing limits")
            self._limits = dict(limits)
            digest = launch_spec.get("artifact_digest_observed") or config.get("code_pin")
            if not digest:
                raise DataflowError("CODE_MISMATCH", "v3 launch payload missing code pin observation")
            self._artifact_digest_observed = digest
        else:
            config = RunJournal(self.directory).recovery_identity()
            if config is None or config["run_id"] != self.run_id:
                raise RecoveryContractError("Worker launch run identity differs")
            self._limits = config["limits"]
            self._artifact_digest_observed = launch_spec.get(
                "artifact_digest_observed", config["platform_revision"])

    # ---- worker->driver events (K3 envelopes; driver model validates) ----
    def _envelope(self, kind, payload):
        return {
            "protocol_version": WORKER_PROTOCOL_VERSION,
            "kind": kind, "scope_id": self.scope_id, "run_id": self.run_id,
            "operation_id": uuid.uuid4().hex, "controller_epoch": self.controller_epoch,
            "authority_ref": self.grant["grant_id"] if self.grant else self.boot_id,
            "allocation_id": self.allocation_id, "boot_id": self.boot_id,
            "payload": payload,
        }

    def _emit(self, kind, payload):
        envelope = self._envelope(kind, payload)
        if self._control is not None:
            self._control(envelope)
        return envelope

    def register(self):
        return self._emit("Register", {
            "boot_id": self.boot_id, "allocation_id": self.allocation_id,
            "scope_id": self.scope_id, "capabilities": self.capabilities,
            "artifact_digest_observed": self._artifact_digest_observed})

    def ready(self, assignment_id):
        return self._emit("Ready", {"boot_id": self.boot_id, "assignment_id": assignment_id})

    def heartbeat(self):
        return self._emit("Heartbeat", {"boot_id": self.boot_id,
                                        "observed_at_ms": int(time.time() * 1000)})

    # ---- driver->worker commands ----
    def handle(self, envelope, *, now_ms):
        validate_message(envelope)
        if envelope["scope_id"] != self.scope_id or envelope["run_id"] != self.run_id:
            raise ProtocolError("FENCED", "message belongs to another scope/run")
        check_controller_epoch(self.controller_epoch, envelope["controller_epoch"])
        kind, payload = envelope["kind"], envelope["payload"]
        check_boot(self.boot_id, envelope["boot_id"])
        return self.receipts.apply(envelope["operation_id"], payload,
                                   lambda: self._execute(kind, payload, now_ms))

    def _execute(self, kind, payload, now_ms):
        if self._v3:
            return self._execute_v3(kind, payload, now_ms)
        if kind == "VerifyArtifactAndCapabilities":
            self._verify(payload["artifact_manifest_ref"])
            require_capabilities(self.capabilities, payload["required_capabilities"])
            self.state = next_worker_state(self.state, "VERIFIED")
            return {"kind": "Verified", "state": self.state}
        if kind == "Restore":
            self.state = next_worker_state(self.state, "RESTORING")
            checksums = self._restore(payload["state_refs"])
            self.state = next_worker_state(self.state, "READY")
            return {"kind": "Restored", "state": self.state, "state_checksums": checksums}
        if kind == "Activate":
            target = next_worker_state(self.state, "ACTIVATED")
            self._activate(payload, now_ms)
            self.state = target
            self._admit_open = True
            return {"kind": "Activated", "state": self.state}
        if kind == "Assign":
            self.plan_revision = apply_assignment(self.plan_revision, payload["assignment"])
            self.assignment = payload["assignment"]
            if self._executor is not None:
                self._executor.start(self.authority, payload["assignment"])
            return {"kind": "Assigned", "plan_revision": self.plan_revision}
        if kind == "CheckpointRequest":
            # The driver-initiated cut converges to the same E1 barrier during
            # execution; the durable cut itself flows out as a CommitCut.
            if self._executor is not None:
                self._executor.checkpoint(payload["cut"])
            return {"kind": "CheckpointAck", "state_refs": [], "inflight_desc": [],
                    "channel_cuts": []}
        if kind in ("Quiesce", "Drain"):
            self._drain(payload)
            if self.state == "ACTIVATED":
                self.state = next_worker_state(self.state, "DRAINING")
            return {"kind": "DrainProof", "state": self.state, "release_hint": "DRAINED"}
        if kind == "Cancel":
            self._cancel(payload)
            if self.state != "RETIRED":
                self.state = next_worker_state(self.state, "RETIRED")
            return {"kind": "Cancelled", "state": self.state}
        raise ProtocolError("INVALID_SPEC", "unhandled worker command: " + str(kind))

    # ---- v3 unified worker path (B011-B018, contract C07) ----
    def _v3_ensure_executor(self):
        if self._v3_executor is None:
            self._v3_executor = _build_v3_executor(self._v3_config or {}, control=self._control)
        return self._v3_executor

    def _execute_v3(self, kind, payload, now_ms):
        if kind == "VerifyArtifactAndCapabilities":
            self._verify(payload["artifact_manifest_ref"])
            require_capabilities(self.capabilities, payload["required_capabilities"])
            self.state = next_worker_state(self.state, "VERIFIED")
            return {"kind": "Verified", "state": self.state}
        if kind == "Restore":
            self.state = next_worker_state(self.state, "RESTORING")
            checksums = self._restore(payload["state_refs"])
            self.state = next_worker_state(self.state, "READY")
            return {"kind": "Restored", "state": self.state, "state_checksums": checksums}
        if kind == "Activate":
            target = next_worker_state(self.state, "ACTIVATED")
            self._activate(payload, now_ms)
            self.state = target
            self._admit_open = True
            return {"kind": "Activated", "state": self.state}
        if kind == "Assign":
            self.plan_revision = apply_assignment(self.plan_revision, payload["assignment"])
            self.assignment = payload["assignment"]
            self._v3_ensure_executor().run()
            return {"kind": "Assigned", "plan_revision": self.plan_revision}
        if kind == "CheckpointRequest":
            executor = self._v3_ensure_executor()
            executor.request_cut(payload["cut"])
            return {"kind": "CheckpointAck", "state_refs": [], "inflight_desc": [],
                    "channel_cuts": []}
        if kind in ("Quiesce", "Drain"):
            self._drain(payload)
            if self.state == "ACTIVATED":
                self.state = next_worker_state(self.state, "DRAINING")
            return {"kind": "DrainProof", "state": self.state, "release_hint": "DRAINED"}
        if kind == "Cancel":
            self._cancel(payload)
            if self.state != "RETIRED":
                self.state = next_worker_state(self.state, "RETIRED")
            return {"kind": "Cancelled", "state": self.state}
        raise ProtocolError("INVALID_SPEC", "unhandled worker command: " + str(kind))

    def _verify(self, ref):
        if self._v3:
            from ..contracts.dataflow_errors import DataflowError
            assignment = _decode_v3_assignment(self._v3_config)
            if ref.get("sha256") != assignment.sealed.code_pin_ref.digest:
                raise DataflowError("CODE_MISMATCH", "Control manifest differs from sealed assignment")
            # Loads and hashes the original sealed refs, and verifies the installed
            # platform and user programs. This path never opens RunJournal.
            self._v3_ensure_executor()
            return
        # Fail-closed for v2 and v3 alike: VerifyArtifactAndCapabilities runs
        # before Assign/load, so nothing has been verified yet at this point.
        if self._manifest_loader is None:
            raise ProtocolError("INVALID_SPEC", "manifest loader is not wired")
        verify_manifest(self._manifest_loader(ref), ref)

    def _restore(self, state_refs):
        if self._v3:
            executor = self._v3_ensure_executor()
            image = (self._v3_config or {}).get("recovery_image")
            if (self._v3_config or {}).get("recovery_image_wire") is not None:
                from ..contracts.dataflow_codec import decode_wire
                image = decode_wire(self._v3_config["recovery_image_wire"].encode(),
                    type_name="RecoveryImage", bounds=executor._services.bounds)
            executor.restore(image)
            return [ref["sha256"] for ref in state_refs]
        artifacts = StateArtifacts(_ReadContext(self.directory, self._limits))
        checksums = []
        for ref in state_refs:
            self._restored[ref["state_id"]] = artifacts.read(ref)
            checksums.append(ref["sha256"])
        return checksums

    def _activate(self, payload, now_ms):
        if self.grant is None:
            raise ProtocolError("GRANT_REVOKED", "no grant delivered for activation")
        if payload["grant_ref"] != self.grant["grant_id"]:
            raise ProtocolError("GRANT_REVOKED", "activation grant_ref differs from delivered grant")
        check_grant(self.grant, controller_epoch=self.controller_epoch,
                    allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
        if self._v3:
            # v3 attaches no RunJournal authority; the executor starts against
            # the granted fence through its worker journal port only.
            executor = self._v3_ensure_executor()
            executor.start()
            return
        # Attaching verifies journal runtime_owner==(attempt, granted fence); a
        # driver restart that replaced the fence fails here (ExecutionConflict).
        # Bind only after a successful enter so a rejected attach leaves no
        # dangling authority and cannot poison a later activation.
        authority = WorkerAuthority(self.directory, self.grant)
        authority.__enter__()
        self.authority = authority

    def _drain(self, payload):
        self._admit_open = False
        self._draining = True
        if self._v3:
            if self._v3_executor is not None:
                self._v3_executor.cancel(payload.get("reason", "drain"),
                                         deadline_ms=int(payload.get("deadline_ms", 1000)))
            return
        # The final barrier and astop/aclose run inside run_stream (E1) on the
        # execution thread; drain joins it and surfaces any chain error.
        if self._executor is not None:
            self._executor.drain(payload)

    def _cancel(self, payload):
        self._admit_open = False
        if self._v3:
            if self._v3_executor is not None:
                self._v3_executor.cancel(payload.get("reason", "cancel"),
                                         deadline_ms=int(payload.get("deadline_ms", 1000)))
            return
        if self._executor is not None:
            self._executor.cancel(payload)
        elif self.authority is not None:
            self.authority.cancel_local()
        # In-flight converges under the existing cancel_grace; unconfirmed
        # effects are never declared successful.

    def admit_business_input(self, *, now_ms):
        if self.state != "ACTIVATED" or not self._admit_open:
            raise ProtocolError("FENCED", "business input requires activation")
        check_grant(self.grant, controller_epoch=self.controller_epoch,
                    allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
        return True

    def close(self):
        self._admit_open = False
        if self._v3:
            if self._v3_executor is not None:
                self._v3_executor.close()
                self._v3_executor = None
            return
        if self.authority is not None:
            self.authority.__exit__(None, None, None)
            self.authority = None