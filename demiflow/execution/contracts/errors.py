"""Error vocabulary for the unified execution protocol (U1).

Resource-domain codes are owned by resource protocol v1 (future module A,
``demiflow/resources/contracts.py``); they are referenced here as vocabulary
only. Engine-domain codes describe protocol/control facts, never business
success or whether a paid effect may be retried.
"""

MAX_DETAIL = 512
_REASONS = frozenset({
    "missing_plan", "missing_bindings", "unsafe_artifact", "artifact_too_large",
    "corrupt_artifact", "identity_mismatch", "invalid_contract",
    "unsupported_layout", "invalid_assignment",
})
from demiflow.execution.contracts.recovery_contract import RecoveryContractError

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


class ReservationError(RecoveryContractError):
    """A call cannot be admitted or released under the recorded reservation."""

    def __init__(self, code, message, *, call_id=None, retriable=False):
        super().__init__(message)
        self.code = code
        self.call_id = call_id
        self.retriable = retriable


class PlanContractError(RecoveryContractError):
    """A sealed input cannot be interpreted or an assignment contradicts it.

    Consumers branch on ``.reason`` only; the message string is not a contract.
    """

    def __init__(self, reason, detail=""):
        if reason not in _REASONS:
            raise ValueError("Unknown plan contract reason: " + str(reason))
        self.reason = reason
        self.detail = str(detail)[:MAX_DETAIL]
        super().__init__(self.detail)

def require_plan_contract(condition, reason, detail):
    if not condition:
        raise PlanContractError(reason, detail)


class PublicationMismatch(RecoveryContractError):
    """A prepared native publication contradicts its sealed output identity."""

class PreparedPublicationUnresolved(RecoveryContractError):
    """Native sink inspection could not conclude a prepared publication."""


class _CapacityPending(Exception):
    """No intent was written: a live response can release admission capacity."""


class ExecutionBudgetExceeded(RecoveryContractError):
    """A valid reservation exceeded its named durable ceiling."""

    def __init__(self, budget, message=None):
        self.budget = budget
        super().__init__(message or 'Execution budget exhausted: ' + budget)

class UnprovenGC(Exception):
    pass
