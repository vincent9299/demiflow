"""Error vocabulary for the unified execution protocol (U1).

Resource-domain codes are owned by resource protocol v1 (future module A,
``demiflow/resources/contracts.py``); they are referenced here as vocabulary
only. Engine-domain codes describe protocol/control facts, never business
success or whether a paid effect may be retried.
"""
from ..recovery_contract import RecoveryContractError

ENGINE_ERROR_CODES = frozenset({
    "PROTOCOL_VERSION_UNSUPPORTED",
    "CAPABILITY_REQUIRED_MISSING",
    "ASSIGNMENT_PLAN_CONFLICT",
    "BOOT_FENCED",
    "GRANT_REVOKED",
    "SEQUENCE_ORDER_VIOLATION",
    "BARRIER_OVERTAKE",
    "CHECKPOINT_PARTICIPANT_MISSING",
    "CHECKPOINT_SCHEMA_INCOMPATIBLE",
    "ARTIFACT_SEALED",
    "REF_READ_DOMAIN_MISMATCH",
    "STATE_ADAPTER_MISSING",
})

INHERITED_RESOURCE_CODES = frozenset({
    "INVALID_SPEC", "UNSUPPORTED_CAPABILITY", "FENCED", "SCOPE_CLOSED",
    "EXPIRED_OPERATION", "ARTIFACT_MISMATCH", "IDEMPOTENCY_CONFLICT",
    "REVISION_CONFLICT", "PERMISSION_DENIED", "CAPACITY_PENDING",
    "QUOTA_EXCEEDED", "UNSATISFIABLE_CONSTRAINT", "PROVISION_DEADLINE_EXCEEDED",
    "LAUNCH_FAILED", "PROVIDER_UNAVAILABLE", "OUTCOME_UNKNOWN",
    "CLEANUP_UNCONFIRMED", "RESYNC_REQUIRED",
    "RESOURCE_QUANTIZATION_EXCEEDS_BUDGET",
})


class ProtocolError(RecoveryContractError):
    """A protocol/control fact violated; carries a stable machine code."""

    def __init__(self, code, reason, *, retriable=False):
        if code not in ENGINE_ERROR_CODES and code not in INHERITED_RESOURCE_CODES:
            raise ValueError("Unknown protocol error code: " + str(code))
        self.code = code
        self.reason = reason
        self.retriable = retriable
        super().__init__(code + ": " + reason)

    def to_dict(self):
        return {"code": self.code, "reason": self.reason, "retriable": self.retriable}