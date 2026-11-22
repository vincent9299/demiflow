"""C04 unified operator state policy: classification and bounded auto_json.

One policy function shared by caller precheck, runtime registration, semantic
identity, OperatorSpec compilation and trait derivation, so no consumer guesses
state ownership independently. ``auto_json`` is the platform's adaptation for
plain ``__dict__`` data instances, not a second journal: the data still flows
through OperatorStates -> StateArtifacts -> the same commit. A hookless instance
is never treated as stateless; a class is a factory blueprint and is never
constructed here.
"""
from __future__ import annotations

import inspect
import json
import math
import types
from dataclasses import dataclass
from typing import Literal

from .recovery_contract import document, RecoveryContractError, MAX_DOCUMENT_BYTES
from ..data.runtime_contract import check_size, RuntimeContractError

AUTO_STATE_SCHEMA = "demiflow.auto_state.v1"
_OWNERSHIP = ("none", "partitionable", "shared_colocated")
_PHASES = ("declaration", "instance")
# Built-in object-protocol descriptors are not user state; only genuine custom
# data descriptors, properties and slots force a native adapter.
_PROTOCOL_DESCRIPTORS = (
    types.WrapperDescriptorType, types.MethodWrapperType, types.MethodDescriptorType,
    types.BuiltinFunctionType, types.BuiltinMethodType, types.GetSetDescriptorType,
)


@dataclass(frozen=True)
class StatePolicy:
    mode: Literal["function", "factory", "hooks", "auto_json", "unsupported"]
    ownership: Literal["none", "partitionable", "shared_colocated"]
    adapter_present: bool
    reason: str | None


def _resolved_ownership(mode, ownership):
    # A function carries no cross-row business state. Anything else whose caller
    # has not declared ownership defaults to shared_colocated: unknown is never
    # silently downgraded to stateless.
    if mode == "function":
        return "none"
    return "shared_colocated" if ownership == "none" else ownership


def _json_fields(value, maximum):
    """Bounded, type-only strict-JSON admission. Executes no user code.

    Tuples/sets/bytes/custom objects are rejected: auto_json must round-trip
    losslessly through JSON, which cannot preserve those types.
    """
    pending = [(value, 0)]
    nodes = 0
    size = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 65536 or depth > 32:
            return False, "auto_json state exceeds structural budget"
        kind = type(item)
        if kind is dict:
            if len(item) > 16384 or any(type(k) is not str for k in item):
                return False, "auto_json state keys must be bounded strings"
            pending.extend((v, depth + 1) for v in item.values())
        elif kind is list:
            if len(item) > 16384:
                return False, "auto_json state collection exceeds budget"
            pending.extend((v, depth + 1) for v in item)
        elif kind is float:
            if not math.isfinite(item):
                return False, "auto_json state must be finite"
        elif kind is str:
            size += len(item)
            if size > maximum:
                return False, "auto_json state exceeds byte budget"
        elif kind not in (int, bool, type(None)):
            return False, "auto_json state must be strict JSON, not " + kind.__name__
    return True, None


def _auto_json_eligible(owner):
    """Decide plain-data adaptability from type structure, without probing code."""
    cls = type(owner)
    if cls.__getattribute__ is not object.__getattribute__:
        return False, "custom __getattribute__ requires a native state adapter"
    if not any(isinstance(vars(k).get("__dict__"), types.GetSetDescriptorType)
               for k in cls.__mro__):
        return False, "auto_json requires a plain __dict__ instance; slots are unsupported"
    for klass in cls.__mro__:
        for name, attr in vars(klass).items():
            if name in ("__dict__", "__weakref__"):
                continue
            if isinstance(attr, property):
                return False, "property descriptors require a native state adapter"
            if isinstance(attr, types.MemberDescriptorType):
                return False, "slots require a native state adapter"
            if isinstance(attr, _PROTOCOL_DESCRIPTORS) or inspect.isfunction(attr) \
                    or isinstance(attr, (classmethod, staticmethod)):
                continue
            attr_type = type(attr)
            if hasattr(attr_type, "__set__") or hasattr(attr_type, "__delete__"):
                return False, "custom data descriptors require a native state adapter"
    return _json_fields(vars(owner), MAX_DOCUMENT_BYTES)


def classify_operator_state(target, *, phase, ownership="none"):
    """Return the single C04 StatePolicy for a function, factory or instance."""
    if phase not in _PHASES:
        raise RecoveryContractError("unknown state classification phase: " + str(phase))
    if ownership not in _OWNERSHIP:
        raise RecoveryContractError("unknown state ownership: " + str(ownership))
    if inspect.isfunction(target) or inspect.isbuiltin(target):
        return StatePolicy("function", "none", False, None)
    if inspect.isclass(target):
        # Factory blueprint: the platform's deferred adaptation is available, but
        # the real instance is still verified after construction. Never built here.
        return StatePolicy("factory", _resolved_ownership("factory", ownership), True, None)
    owner = target.__self__ if inspect.ismethod(target) else target
    snapshot = callable(getattr(owner, "snapshot_state", None))
    restore = callable(getattr(owner, "restore_state", None))
    if snapshot and restore:
        return StatePolicy("hooks", _resolved_ownership("hooks", ownership), True, None)
    if snapshot != restore:
        return StatePolicy("unsupported", ownership, False,
                           "half-declared state adapter: exactly one of snapshot_state/restore_state")
    eligible, reason = _auto_json_eligible(owner)
    if not eligible:
        return StatePolicy("unsupported", ownership, False, reason)
    if ownership == "partitionable":
        return StatePolicy("unsupported", ownership, False,
                           "auto_json state is shared_colocated; partitionable needs a native adapter")
    return StatePolicy("auto_json", _resolved_ownership("auto_json", ownership), True, None)


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