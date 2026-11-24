"""Control receipts implementation."""
from __future__ import annotations

from demiflow.execution.contracts.recovery_contract import identifier, identity, document
from demiflow.execution.contracts.errors import ProtocolError


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
