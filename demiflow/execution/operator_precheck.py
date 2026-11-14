"""Caller-side K2 state-adapter precheck for the unified compile path (U4/S3).

The frozen ``compile_operator_spec`` owns the durable-protocol precheck; this
entry applies the same C04 state policy at streaming/graph startup without
requiring a durable artifact bundle, so in-run pickle callables (lambdas) stay
valid. It mirrors compile: the frozen adapter probe plus the single
``classify_operator_state`` policy for platform auto_json adaptation. A class
factory is deferred (the real instance is verified after construction); a
stateful instance lacking a usable adapter fails STATE_ADAPTER_MISSING before
any worker is provisioned. An empty callable instance carries no cross-row
state and stays admissible under ownership none, exactly as compile admits it.
"""
import inspect

from .contracts.errors import ProtocolError
from .contracts.operator import _adapter_present
from .operator_policy import classify_operator_state


def precheck_operator_state(callable_spec, traits):
    target = callable_spec.target
    ownership = traits.get("state_ownership", "none")
    adapter = _adapter_present(target)
    if not adapter and not inspect.isfunction(target) and not inspect.isclass(target):
        # C04: a hookless plain-data instance is platform-adaptable (auto_json)
        # only under shared_colocated, and only at same-owner concurrency 1.
        policy = classify_operator_state(target, phase="instance", ownership=ownership)
        if policy.mode == "auto_json" and ownership == "shared_colocated":
            if traits.get("concurrency", 1) != 1:
                raise ProtocolError("STATE_ADAPTER_MISSING",
                                    "auto_json state requires concurrency 1; declare a native adapter for higher concurrency")
            adapter = True
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
        options = getattr(op, "stream_options", None)
        precheck_operator_state(callable_spec, {
            "state_ownership": getattr(traits, "state_ownership", "none"),
            "concurrency": getattr(options, "concurrency", 1) if options is not None else getattr(op, 'concurrency', 1)})
