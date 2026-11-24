"""K3 WorkerAssignment + WorkerProtocol messages (§8).

WorkerLaunchSpec/WorkerRequest/ResourcePlan/ReleaseSpec/MutationContext remain
owned by resource protocol v1 (module A); messages here reference their field
names and semantics without redefining them. The worker state machine names
are inherited verbatim; no parallel state machine is introduced.
"""
from demiflow.execution.contracts.recovery_contract import document, identifier
from .envelope import (
    WORKER_PROTOCOL_VERSION, OperationReceipts, check_boot, check_controller_epoch,
    check_grant, validate_envelope,
)
from .errors import ProtocolError

ASSIGNMENT_SCHEMA = "demiflow.worker_assignment.v1"

WORKER_STATES = ("REGISTERED", "VERIFIED", "RESTORING", "READY", "ACTIVATED", "DRAINING", "RETIRED")
_TRANSITIONS = {
    "REGISTERED": {"VERIFIED", "RETIRED"},
    "VERIFIED": {"RESTORING", "READY", "RETIRED"},
    "RESTORING": {"READY", "RETIRED"},
    "READY": {"ACTIVATED", "RETIRED"},
    "ACTIVATED": {"DRAINING", "RETIRED"},
    "DRAINING": {"RETIRED"},
    "RETIRED": set(),
}

MESSAGE_KINDS = frozenset({
    "Register", "VerifyArtifactAndCapabilities", "Restore", "Ready", "Activate",
    "Assign", "CheckpointRequest", "Quiesce", "Drain", "Cancel", "Heartbeat",
})
_COORD_COMMANDS = frozenset({
    "VerifyArtifactAndCapabilities", "Restore", "Activate", "Assign",
    "CheckpointRequest", "Quiesce", "Drain", "Cancel",
})

_ASSIGNMENT_KEYS = frozenset({
    "schema_version", "assignment_id", "operation_id", "scope_id",
    "allocation_id", "boot_id", "worker_id", "controller_epoch", "grant_ref",
    "plan_revision", "expected_plan_revision", "artifact_manifest_ref",
    "stages", "partitions", "channels", "restore_point", "route_epoch",
    "topology_revision", "ready_deadline_ms",
})


def next_worker_state(current, target):
    if current not in _TRANSITIONS or target not in WORKER_STATES:
        raise ProtocolError("INVALID_SPEC", "unknown worker state")
    if target not in _TRANSITIONS[current]:
        raise ProtocolError("INVALID_SPEC",
                            "worker transition %s -> %s is not allowed" % (current, target))
    return target


def _manifest_ref(value):
    if type(value) is not dict or set(value) != {"manifest_id", "sha256"}:
        raise ProtocolError("INVALID_SPEC", "artifact_manifest_ref needs manifest_id/sha256")
    identifier(value["manifest_id"])
    identifier(value["sha256"])


def _state_refs(value):
    if type(value) is not list:
        raise ProtocolError("INVALID_SPEC", "state_refs must be a list")
    for ref in value:
        if type(ref) is not dict or set(ref) != {"state_id", "path", "bytes", "sha256"}:
            raise ProtocolError("INVALID_SPEC", "state ref key set is unsupported")
        identifier(ref["state_id"])
        identifier(ref["sha256"])
        if type(ref["bytes"]) is not int or ref["bytes"] < 0 or type(ref["path"]) is not str:
            raise ProtocolError("INVALID_SPEC", "state ref bytes/path are invalid")


def validate_assignment(value):
    if type(value) is not dict or set(value) != _ASSIGNMENT_KEYS:
        raise ProtocolError("INVALID_SPEC", "assignment key set is unsupported")
    if value["schema_version"] != ASSIGNMENT_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "unsupported assignment schema: " + str(value["schema_version"]))
    for name in ("assignment_id", "operation_id", "scope_id", "allocation_id",
                 "boot_id", "worker_id", "grant_ref"):
        identifier(value[name])
    for name in ("controller_epoch", "plan_revision", "expected_plan_revision",
                 "route_epoch", "topology_revision"):
        if type(value[name]) is not int or value[name] < 0:
            raise ProtocolError("INVALID_SPEC", name + " must be a nonnegative int")
    if value["plan_revision"] < value["expected_plan_revision"]:
        raise ProtocolError("ASSIGNMENT_PLAN_CONFLICT", "assignment plan revision regresses")
    _manifest_ref(value["artifact_manifest_ref"])
    for name in ("stages", "partitions"):
        if type(value[name]) is not list or any(type(item) is not str for item in value[name]):
            raise ProtocolError("INVALID_SPEC", name + " must be a list of ids")
        for item in value[name]:
            identifier(item)
    channels = value["channels"]
    if type(channels) is not dict or set(channels) != {"inputs", "outputs"}:
        raise ProtocolError("INVALID_SPEC", "assignment channels key set is unsupported")
    if type(channels["inputs"]) is not list or type(channels["outputs"]) is not list:
        raise ProtocolError("INVALID_SPEC", "assignment channels must be lists")
    for item in channels["inputs"]:
        if type(item) is not dict or not {"channel_id"} <= set(item) or set(item) - {"channel_id", "restore_cut"}:
            raise ProtocolError("INVALID_SPEC", "input channel declaration is unsupported")
        identifier(item["channel_id"])
        if "restore_cut" in item and (type(item["restore_cut"]) is not int or item["restore_cut"] < 0):
            raise ProtocolError("INVALID_SPEC", "restore_cut must be a nonnegative int")
    for item in channels["outputs"]:
        identifier(item)
    restore = value["restore_point"]
    if restore is not None:
        if type(restore) is not dict or set(restore) != {"checkpoint_id"}:
            raise ProtocolError("INVALID_SPEC", "restore_point needs checkpoint_id only")
        identifier(restore["checkpoint_id"])
    if type(value["ready_deadline_ms"]) is not int or value["ready_deadline_ms"] <= 0:
        raise ProtocolError("INVALID_SPEC", "ready_deadline_ms must be positive")
    document(value)
    return value


def apply_assignment(current_plan_revision, assignment):
    """CAS: assignments built on a stale plan revision never apply."""
    validate_assignment(assignment)
    if current_plan_revision != assignment["expected_plan_revision"]:
        raise ProtocolError("ASSIGNMENT_PLAN_CONFLICT",
                            "expected plan revision %d, current %d" % (
                                assignment["expected_plan_revision"], current_plan_revision))
    return assignment["plan_revision"]


_PAYLOAD_KEYS = {
    "Register": {"boot_id", "allocation_id", "scope_id", "capabilities", "artifact_digest_observed"},
    "VerifyArtifactAndCapabilities": {"artifact_manifest_ref", "required_capabilities"},
    "Restore": {"assignment_ref", "restore_point", "state_refs"},
    "Ready": {"boot_id", "assignment_id"},
    "Activate": {"grant_ref", "controller_epoch", "operation_id"},
    "Assign": {"assignment"},
    "CheckpointRequest": {"checkpoint_id", "epoch", "cut", "topology_revision", "deadline_ms"},
    "Quiesce": {"deadline_ms", "reason"},
    "Drain": {"deadline_ms", "reason"},
    "Cancel": {"operation_id", "reason"},
    "Heartbeat": {"boot_id", "observed_at_ms"},
}


def validate_payload(kind, payload):
    if kind not in _PAYLOAD_KEYS:
        raise ProtocolError("INVALID_SPEC", "unknown message kind: " + str(kind))
    if type(payload) is not dict or set(payload) != _PAYLOAD_KEYS[kind]:
        raise ProtocolError("INVALID_SPEC", kind + " payload key set is unsupported")
    if kind == "Register":
        for name in ("boot_id", "allocation_id", "scope_id"):
            identifier(payload[name])
        caps = payload["capabilities"]
        if type(caps) is not list or any(type(item) is not str for item in caps):
            raise ProtocolError("INVALID_SPEC", "capabilities must be strings")
        identifier(payload["artifact_digest_observed"])
    elif kind == "VerifyArtifactAndCapabilities":
        _manifest_ref(payload["artifact_manifest_ref"])
        caps = payload["required_capabilities"]
        if type(caps) is not list or any(type(item) is not str for item in caps):
            raise ProtocolError("INVALID_SPEC", "required_capabilities must be strings")
    elif kind == "Restore":
        identifier(payload["assignment_ref"])
        restore = payload["restore_point"]
        if restore is not None and (type(restore) is not dict or set(restore) != {"checkpoint_id"}):
            raise ProtocolError("INVALID_SPEC", "restore_point needs checkpoint_id only")
        if restore is not None:
            identifier(restore["checkpoint_id"])
        _state_refs(payload["state_refs"])
    elif kind == "Ready":
        identifier(payload["boot_id"])
        identifier(payload["assignment_id"])
    elif kind == "Activate":
        identifier(payload["grant_ref"])
        identifier(payload["operation_id"])
        if type(payload["controller_epoch"]) is not int or payload["controller_epoch"] < 0:
            raise ProtocolError("INVALID_SPEC", "controller_epoch must be nonnegative")
    elif kind == "Assign":
        validate_assignment(payload["assignment"])
    elif kind == "CheckpointRequest":
        identifier(payload["checkpoint_id"])
        for name in ("epoch", "topology_revision", "deadline_ms"):
            if type(payload[name]) is not int or payload[name] < 0:
                raise ProtocolError("INVALID_SPEC", name + " must be a nonnegative int")
        cut = payload["cut"]
        if type(cut) is not dict or not {"producer_sequence_bound"} <= set(cut) \
                or set(cut) - {"producer_sequence_bound", "source_progress_ref"}:
            raise ProtocolError("INVALID_SPEC", "checkpoint cut is unsupported")
        if type(cut["producer_sequence_bound"]) is not int or cut["producer_sequence_bound"] < 0:
            raise ProtocolError("INVALID_SPEC", "producer_sequence_bound must be nonnegative")
    elif kind in ("Quiesce", "Drain"):
        if type(payload["deadline_ms"]) is not int or payload["deadline_ms"] <= 0:
            raise ProtocolError("INVALID_SPEC", "deadline_ms must be positive")
        if type(payload["reason"]) is not str:
            raise ProtocolError("INVALID_SPEC", "reason must be a string")
    elif kind == "Cancel":
        identifier(payload["operation_id"])
        if type(payload["reason"]) is not str:
            raise ProtocolError("INVALID_SPEC", "reason must be a string")
    elif kind == "Heartbeat":
        identifier(payload["boot_id"])
        if type(payload["observed_at_ms"]) is not int:
            raise ProtocolError("INVALID_SPEC", "observed_at_ms must be an int")
    document(payload)
    return payload


def validate_message(envelope):
    if type(envelope) is not dict:
        raise ProtocolError("INVALID_SPEC", "envelope must be an object")
    validate_envelope(
        envelope, kinds=MESSAGE_KINDS,
        require_allocation=envelope.get("kind") in _COORD_COMMANDS,
        require_boot=True,
    )
    validate_payload(envelope["kind"], envelope["payload"])
    return envelope


class WorkerProtocolModel:
    """Pure conformance model: one worker boot, no transport or scheduling.

    Commands are fenced by controller epoch/boot/grant, deduplicated by
    operation_id, and gated by the inherited state machine. Business input is
    admitted only after activate with a live grant (resource v1 activation rule).
    """

    def __init__(self, *, scope_id, run_id, allocation_id, boot_id,
                 controller_epoch, grant, plan_revision=0, capabilities=()):
        self.scope_id, self.run_id = scope_id, run_id
        self.allocation_id, self.boot_id = allocation_id, boot_id
        self.controller_epoch = controller_epoch
        self.grant = grant
        self.plan_revision = plan_revision
        self.capabilities = list(capabilities)
        self.state = "REGISTERED"
        self.receipts = OperationReceipts()
        self.assignment = None

    def handle(self, envelope, *, now_ms):
        validate_message(envelope)
        if envelope["scope_id"] != self.scope_id or envelope["run_id"] != self.run_id:
            raise ProtocolError("FENCED", "message belongs to another scope/run")
        check_controller_epoch(self.controller_epoch, envelope["controller_epoch"])
        kind, payload = envelope["kind"], envelope["payload"]
        if kind in _COORD_COMMANDS or kind in ("Ready", "Heartbeat"):
            check_boot(self.boot_id, envelope["boot_id"])
        if kind in ("Register", "Ready", "Heartbeat"):
            check_boot(self.boot_id, payload["boot_id"])
        if kind == "Register":
            if payload["boot_id"] != self.boot_id or payload["allocation_id"] != self.allocation_id:
                raise ProtocolError("BOOT_FENCED", "registration boot/allocation mismatch")

        def execute():
            return self._apply(kind, payload, now_ms)

        return self.receipts.apply(envelope["operation_id"], payload, execute)

    def _apply(self, kind, payload, now_ms):
        if kind == "Register":
            return {"kind": "Registered", "state": self.state}
        if kind == "VerifyArtifactAndCapabilities":
            from .envelope import require_capabilities
            require_capabilities(self.capabilities, payload["required_capabilities"])
            self.state = next_worker_state(self.state, "VERIFIED")
            return {"kind": "Verified", "state": self.state}
        if kind == "Restore":
            self.state = next_worker_state(self.state, "RESTORING")
            self.state = next_worker_state(self.state, "READY")
            return {"kind": "Restored", "state": self.state, "state_checksums": []}
        if kind == "Ready":
            if self.state == "VERIFIED":
                self.state = next_worker_state(self.state, "READY")
            return {"kind": "ReadyAck", "state": self.state}
        if kind == "Activate":
            check_grant(self.grant, controller_epoch=self.controller_epoch,
                        allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
            self.state = next_worker_state(self.state, "ACTIVATED")
            return {"kind": "Activated", "state": self.state}
        if kind == "Assign":
            self.plan_revision = apply_assignment(self.plan_revision, payload["assignment"])
            self.assignment = payload["assignment"]
            return {"kind": "Assigned", "plan_revision": self.plan_revision}
        if kind == "CheckpointRequest":
            return {"kind": "CheckpointAck", "state_refs": [], "inflight_desc": [],
                    "channel_cuts": []}
        if kind in ("Quiesce", "Drain"):
            if self.state == "ACTIVATED":
                self.state = next_worker_state(self.state, "DRAINING")
            return {"kind": "DrainProof", "state": self.state, "release_hint": "DRAINED"}
        if kind == "Cancel":
            if self.state != "RETIRED":
                self.state = next_worker_state(self.state, "RETIRED")
            return {"kind": "Cancelled", "state": self.state}
        if kind == "Heartbeat":
            # Liveness only: never extends control leases or admits input.
            return {"kind": "HeartbeatAck", "state": self.state}
        raise ProtocolError("INVALID_SPEC", "unhandled kind")

    def admit_business_input(self, *, now_ms):
        if self.state != "ACTIVATED":
            raise ProtocolError("FENCED", "business input requires activation")
        check_grant(self.grant, controller_epoch=self.controller_epoch,
                    allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
        return True