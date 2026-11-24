"""Common protocol envelope, version negotiation and message-level rules (§4).

Pure functions and bounded ledgers only; no transport, no scheduling. Receipt,
revision and fence semantics are inherited from resource protocol v1 §03; the
resource-side apply/close implementation remains owned by module A.
"""
from demiflow.execution.contracts.recovery_contract import document, identifier, identity
from .errors import ProtocolError

WORKER_PROTOCOL_VERSION = {"major": 1, "minor": 0}
EXCHANGE_PROTOCOL_VERSION = {"major": 1, "minor": 0}

_ENVELOPE_REQUIRED = frozenset({
    "protocol_version", "kind", "scope_id", "run_id", "operation_id",
    "controller_epoch", "authority_ref", "payload",
})
_ENVELOPE_OPTIONAL = frozenset({"allocation_id", "boot_id", "deadline_ms"})

_GRANT_KEYS = frozenset({
    "grant_id", "run_id", "attempt", "fence", "scope_id", "allocation_id",
    "boot_id", "controller_epoch", "expires_at_ms",
})

EVENT_STATES = frozenset({
    "allocating", "ready", "active", "draining", "retired", "cancelled",
    "failed", "unknown",
})
_TERMINAL_STATES = frozenset({"retired", "cancelled", "failed"})
_EVENT_KEYS = frozenset({
    "event_id", "subject_id", "observation_revision", "state", "detail",
})


def protocol_version(value):
    if type(value) is not dict or set(value) != {"major", "minor"}:
        raise ProtocolError("INVALID_SPEC", "protocol_version needs major/minor")
    for name in ("major", "minor"):
        item = value[name]
        if type(item) is not int or item < 0:
            raise ProtocolError("INVALID_SPEC", "protocol_version " + name + " must be a nonnegative int")
    return dict(value)


def negotiate_version(local, remote):
    """Major mismatch rejects; minor extensions negotiate to the lower minor."""
    local, remote = protocol_version(local), protocol_version(remote)
    if local["major"] != remote["major"]:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "major versions differ: %d vs %d" % (local["major"], remote["major"]))
    return {"major": local["major"], "minor": min(local["minor"], remote["minor"])}


def require_capabilities(declared, required):
    """Required capabilities missing at startup reject; never silently drop."""
    if type(declared) not in (list, tuple, set, frozenset) or any(type(c) is not str for c in declared):
        raise ProtocolError("INVALID_SPEC", "declared capabilities must be strings")
    if type(required) not in (list, tuple, set, frozenset) or any(type(c) is not str for c in required):
        raise ProtocolError("INVALID_SPEC", "required capabilities must be strings")
    missing = sorted(set(required) - set(declared))
    if missing:
        raise ProtocolError("CAPABILITY_REQUIRED_MISSING", "missing: " + ",".join(missing))
    return sorted(set(required))


def validate_envelope(value, *, kinds, require_allocation=False, require_boot=False):
    """Closed envelope shared by worker and exchange control messages."""
    if type(value) is not dict:
        raise ProtocolError("INVALID_SPEC", "envelope must be an object")
    keys = set(value)
    if not _ENVELOPE_REQUIRED <= keys or keys - _ENVELOPE_REQUIRED - _ENVELOPE_OPTIONAL:
        raise ProtocolError("INVALID_SPEC", "envelope key set is unsupported")
    protocol_version(value["protocol_version"])
    if value["kind"] not in kinds:
        raise ProtocolError("INVALID_SPEC", "unknown message kind: " + str(value["kind"]))
    for name in ("scope_id", "run_id", "operation_id", "authority_ref"):
        identifier(value[name])
    epoch = value["controller_epoch"]
    if type(epoch) is not int or epoch < 0:
        raise ProtocolError("INVALID_SPEC", "controller_epoch must be a nonnegative int")
    for name in ("allocation_id", "boot_id"):
        if name in value:
            identifier(value[name])
    if require_allocation and "allocation_id" not in value:
        raise ProtocolError("INVALID_SPEC", "message requires allocation_id")
    if require_boot and "boot_id" not in value:
        raise ProtocolError("INVALID_SPEC", "message requires boot_id")
    if "deadline_ms" in value:
        deadline = value["deadline_ms"]
        if type(deadline) is not int or deadline <= 0:
            raise ProtocolError("INVALID_SPEC", "deadline_ms must be a positive int")
    if type(value["payload"]) is not dict:
        raise ProtocolError("INVALID_SPEC", "payload must be an object")
    document(value)
    return value


class OperationReceipts:
    """Bounded message-level idempotency ledger (resource v1 §03 rules).

    Same operation_id + same payload returns the original receipt without
    re-execution; same id with a different payload is IDEMPOTENCY_CONFLICT.
    """

    def __init__(self, maximum=4096):
        if type(maximum) is not int or maximum < 1:
            raise ValueError("receipt ledger maximum must be positive")
        self.maximum = maximum
        self._entries = {}

    def apply(self, operation_id, payload, execute):
        identifier(operation_id)
        payload_id = identity(payload)
        entry = self._entries.get(operation_id)
        if entry is not None:
            if entry["payload_id"] != payload_id:
                raise ProtocolError("IDEMPOTENCY_CONFLICT",
                                    "operation_id reused with a different payload")
            return entry["receipt"]
        if len(self._entries) >= self.maximum:
            raise ProtocolError("EXPIRED_OPERATION", "receipt ledger is full")
        receipt = execute()
        document(receipt)
        self._entries[operation_id] = {"payload_id": payload_id, "receipt": receipt}
        return receipt


def check_controller_epoch(current, observed):
    if type(observed) is not int or observed < current:
        raise ProtocolError("FENCED", "controller_epoch %r is older than %r" % (observed, current))


def check_boot(current_boot, observed_boot):
    """Boots are per-launch random identities; any stale boot is fenced."""
    if observed_boot != current_boot:
        raise ProtocolError("BOOT_FENCED", "boot_id does not match the current launch")


def validate_grant(value):
    """Native execution grant reference (§12); derived by the current owner."""
    if type(value) is not dict or set(value) != _GRANT_KEYS:
        raise ProtocolError("INVALID_SPEC", "grant key set is unsupported")
    for name in ("grant_id", "run_id", "fence", "scope_id", "allocation_id", "boot_id"):
        identifier(value[name])
    if type(value["attempt"]) is not int or value["attempt"] < 1:
        raise ProtocolError("INVALID_SPEC", "grant attempt must be a positive int")
    if type(value["controller_epoch"]) is not int or value["controller_epoch"] < 0:
        raise ProtocolError("INVALID_SPEC", "grant controller_epoch must be nonnegative")
    if type(value["expires_at_ms"]) is not int:
        raise ProtocolError("INVALID_SPEC", "grant expires_at_ms must be an int")
    document(value)
    return value


def check_grant(grant, *, controller_epoch, allocation_id, boot_id, now_ms):
    """A stale/foreign grant cannot publish results or admit business input."""
    validate_grant(grant)
    if (grant["controller_epoch"] != controller_epoch
            or grant["allocation_id"] != allocation_id or grant["boot_id"] != boot_id):
        raise ProtocolError("GRANT_REVOKED", "grant belongs to another epoch/allocation/boot")
    if now_ms >= grant["expires_at_ms"]:
        raise ProtocolError("GRANT_REVOKED", "grant has expired")


class EventOrdering:
    """Late/duplicate event rules: dedup, monotonic revisions, tombstones.

    Terminal states never regress; a cancel tombstone suppresses later
    allocation events for the same subject (cancel-before-allocation).
    Heartbeat loss maps to UNKNOWN and never triggers a retry by itself.
    """

    def __init__(self, maximum=4096):
        self.maximum = maximum
        self._events = set()
        self._subjects = {}

    def observe(self, event):
        if type(event) is not dict or not {"event_id", "subject_id", "observation_revision", "state"} <= set(event) \
                or set(event) - _EVENT_KEYS:
            raise ProtocolError("INVALID_SPEC", "event key set is unsupported")
        identifier(event["event_id"])
        identifier(event["subject_id"])
        revision = event["observation_revision"]
        if type(revision) is not int or revision < 0:
            raise ProtocolError("INVALID_SPEC", "observation_revision must be nonnegative")
        if event["state"] not in EVENT_STATES:
            raise ProtocolError("INVALID_SPEC", "unknown event state")
        if "detail" in event:
            document(event["detail"])
        subject = event["subject_id"]
        if event["event_id"] in self._events:
            current = self._subjects.get(subject, {})
            return {"status": "duplicate", "state": current.get("state")}
        if len(self._events) >= self.maximum or (
                subject not in self._subjects and len(self._subjects) >= self.maximum):
            raise ProtocolError("EXPIRED_OPERATION", "event ledger is full")
        current = self._subjects.get(subject)
        if current is not None and current.get("tombstone"):
            self._events.add(event["event_id"])
            return {"status": "suppressed", "state": current["state"]}
        if current is not None:
            if revision < current["revision"]:
                return {"status": "stale", "state": current["state"]}
            if revision == current["revision"] and event["state"] != current["state"]:
                return {"status": "stale", "state": current["state"]}
            if current["state"] in _TERMINAL_STATES and event["state"] not in _TERMINAL_STATES:
                return {"status": "stale", "state": current["state"]}
        self._events.add(event["event_id"])
        self._subjects[subject] = {
            "state": event["state"], "revision": max(revision, current["revision"] if current else 0),
            "tombstone": event["state"] == "cancelled",
        }
        return {"status": "applied", "state": event["state"]}

    def heartbeat_expired(self, subject_id):
        """Loss of liveness is UNKNOWN, not failure; no paid-call retry implied."""
        identifier(subject_id)
        current = self._subjects.get(subject_id)
        if current is None:
            self._subjects[subject_id] = {"state": "unknown", "revision": 0, "tombstone": False}
        elif current["state"] not in _TERMINAL_STATES and not current["tombstone"]:
            current["state"] = "unknown"
        return {"state": self._subjects[subject_id]["state"], "retry": False}