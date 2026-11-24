"""Operator state implementation."""
from __future__ import annotations

import json
from demiflow.execution.contracts.recovery_contract import document, RecoveryContractError
from demiflow.data.runtime_contract import check_size, RuntimeContractError
from demiflow.execution.compiler_support.operator_policy import AUTO_STATE_SCHEMA, _auto_json_eligible, _json_fields


def _state_budget(max_bytes, occurrence):
    return f"limit_bytes={max_bytes}" + (f", occurrence_id={occurrence}" if occurrence is not None else "")

def _encode_auto_state(fields, max_bytes, occurrence):
    budget = _state_budget(max_bytes, occurrence)
    try:
        check_size(fields, max_bytes)
    except RuntimeContractError as error:
        raise RecoveryContractError(f"auto_state rejected before encoding ({budget}): {error}") from error
    try:
        return document(fields, max_bytes)
    except RecoveryContractError as error:
        raise RecoveryContractError(f"auto_state encoding failed ({budget}): {error}") from error

def snapshot_auto_state(owner, *, max_bytes, occurrence=None):
    """Bounded, detached strict-JSON copy of a plain instance's fields."""
    if type(max_bytes) is not int or max_bytes < 1:
        raise RecoveryContractError("auto_state max_bytes must be a positive integer")
    eligible, reason = _auto_json_eligible(owner)
    if not eligible:
        raise RecoveryContractError(f"auto_json snapshot rejected ({_state_budget(max_bytes, occurrence)}): {reason}")
    fields = vars(owner)
    raw = _encode_auto_state(fields, max_bytes, occurrence)
    return {"schema": AUTO_STATE_SCHEMA, "fields": json.loads(raw)}

def restore_auto_state(owner, state, *, max_bytes, occurrence=None):
    """Validate schema/budget/fields fully, then atomically replace instance fields."""
    if type(max_bytes) is not int or max_bytes < 1:
        raise RecoveryContractError("auto_state max_bytes must be a positive integer")
    if type(state) is not dict or set(state) != {"schema", "fields"}:
        raise RecoveryContractError("invalid auto_state document")
    if state["schema"] != AUTO_STATE_SCHEMA:
        raise RecoveryContractError("unsupported auto_state schema: " + str(state["schema"]))
    fields = state["fields"]
    if type(fields) is not dict:
        raise RecoveryContractError("auto_state fields must be a dict")
    for key in fields:
        if type(key) is not str or (key.startswith("__") and key.endswith("__")):
            raise RecoveryContractError("auto_state refuses prototype/dunder field injection")
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        raise RecoveryContractError(f"auto_state restore rejected ({_state_budget(max_bytes, occurrence)}): {reason}")
    eligible, reason = _auto_json_eligible(owner)
    if not eligible:
        raise RecoveryContractError(f"auto_state restore rejected ({_state_budget(max_bytes, occurrence)}): {reason}")
    raw = _encode_auto_state(fields, max_bytes, occurrence)
    detached = json.loads(raw)
    # Owner is untouched until every check above has passed; the swap preserves
    # exactly the restored field set with no shared mutable aliases.
    current = vars(owner)
    current.clear()
    current.update(detached)
