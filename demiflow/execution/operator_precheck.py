"""Caller-side K2 state-adapter precheck for the unified compile path (U4/S3).

The frozen ``compile_operator_spec`` owns the durable-protocol precheck; this
entry applies the same state-ownership rule at streaming/graph startup without
requiring a durable artifact bundle, so in-run pickle callables (lambdas) stay
valid. It reuses the frozen adapter probe and never edits the contract: a
stateful operator (ownership != none) lacking snapshot_state/restore_state, or a
callable instance carrying state without an adapter, fails STATE_ADAPTER_MISSING
before any worker is provisioned.
"""
import inspect

from .contracts.errors import ProtocolError
from .contracts.operator import _adapter_present


def precheck_operator_state(callable_spec, traits):
    target = callable_spec.target
    ownership = traits.get("state_ownership", "none")
    adapter = _adapter_present(target)
    if ownership != "none" and not adapter:
        raise ProtocolError("STATE_ADAPTER_MISSING",
                            "stateful operator lacks snapshot_state/restore_state")
    if ownership == "none" and not inspect.isfunction(target) and not inspect.isclass(target):
        owner = target.__self__ if inspect.ismethod(target) else target
        if getattr(owner, "__dict__", {}) and not adapter:
            raise ProtocolError("STATE_ADAPTER_MISSING",
                                "callable instance carries state without a state adapter")
    return adapter


def precheck_plan_state(plan):
    from ..planning.traits import unified_operation_traits
    for op in plan.operations:
        callable_spec = getattr(op, "callable", None)
        if callable_spec is None:
            continue
        traits = unified_operation_traits(op)
        precheck_operator_state(callable_spec, {
            "state_ownership": getattr(traits, "state_ownership", "none")})