"""Exhaustive operator trait derivation without backend concepts."""
from __future__ import annotations
from dataclasses import replace
from ..data import plan as ops
from ..data import sources
from .model import OperatorTraits

def source_traits(source):
    if isinstance(source, sources.MaterializedSource): return OperatorTraits("source","driver","resident","ephemeral_worker",False)
    if isinstance(source, sources.SourcePlan): return OperatorTraits("source","ephemeral","transient","ephemeral_worker")
    raise TypeError(f"unknown source plan: {type(source).__name__}")

def operation_traits(op):
    if isinstance(op, ops.VectorSearchOp): return OperatorTraits("transform","reusable","resident","reusable_worker")
    if isinstance(op, (ops.OperatorLLMMapOp, ops.EmbeddingMapOp)): return OperatorTraits("network_io","reusable","resident","network_io_worker")
    if isinstance(op,(ops.MapOp,ops.FlatMapOp,ops.MapBatchesOp,ops.BoundMapOp,ops.FilterOp,ops.AddColumnOp)):
        return _callable_traits(op.callable)
    if isinstance(op,(ops.SortOp,ops.RepartitionOp,ops.RandomShuffleOp)): return OperatorTraits("global","ephemeral","barrier","ephemeral_worker")
    if isinstance(op,ops.LogicalOp): return OperatorTraits("transform","ephemeral","transient","ephemeral_worker")
    raise TypeError(f"unknown logical operation: {type(op).__name__}")


def partition_operation_traits(op):
    """Local partition callbacks are constructed per task, without resident actors."""
    traits=operation_traits(op)
    if traits.category=='transform' and traits.worker_model=='reusable' and getattr(op,'native_options',None) is None:
        return replace(traits,worker_model='ephemeral',lifecycle='transient',policy_class='ephemeral_worker')
    return traits

def terminal_traits(category="action"):
    if category == "action":
        return OperatorTraits(category,"driver","transient","ephemeral_worker",False)
    return OperatorTraits(category,"ephemeral","transient","ephemeral_worker")

def requires_native_ray(operation):
    return isinstance(operation,(ops.SortOp,ops.RepartitionOp,ops.RandomShuffleOp,ops.RandomizeBlockOrderOp))


def _callable_traits(callable_spec):
    """Derive execution shape from the user callable, never from a backend wrapper."""
    if callable_spec.is_class:
        return OperatorTraits("transform", "reusable", "resident", "reusable_worker")
    return OperatorTraits("transform", "ephemeral", "transient", "ephemeral_worker")


def unified_operation_traits(op):
    """Legacy traits plus protocol-level facts; legacy results stay unchanged.

    Only built-in operators derive facts here; business requirements arrive
    through the existing runtime_contract/NativeOptions paths, not a new base
    class. Worker counts, chunk sizes and attempts never enter these facts.
    """
    base = operation_traits(op)
    updates = {}
    if isinstance(op, (ops.SortOp, ops.RepartitionOp, ops.RandomShuffleOp, ops.RandomizeBlockOrderOp)):
        updates["input_mode"] = "finite_scope"
    if isinstance(op, ops.SortOp):
        updates["ordering"] = "requires_global_order"
    if isinstance(op, ops.FlatMapOp):
        updates["cardinality"] = "many"
    if isinstance(op, ops.FilterOp):
        updates["cardinality"] = "zero_or_one"
    if isinstance(op, (ops.OperatorLLMMapOp, ops.EmbeddingMapOp, ops.SearchWebOp)):
        updates["effect_kind"] = "external_paid"
    if isinstance(op, ops.SaveLanceOp):
        updates["effect_kind"] = "external_unknown"
    if isinstance(op, (ops.BatchMapOp, ops.MapBatchesOp)) or getattr(op, "stream_options", None) is not None:
        updates["batching"] = "batch"
    callable_spec = getattr(op, "callable", None)
    if callable_spec is not None:
        # Instance and class ownership share the single C04 policy. A class is a
        # factory (deferred shared_colocated); an instance with a native adapter
        # keeps that adapter instead of dropping it for not being a class. A
        # hookless instance keeps the caller's declaration so the precheck still
        # enforces the stateless-declaration boundary; its plain-data state is
        # captured by auto_json at registration, not by upgrading sealed ownership.
        from ..execution.operator_policy import classify_operator_state
        target = callable_spec.target
        if callable_spec.is_class:
            policy = classify_operator_state(target, phase="declaration")
            if policy.mode == "factory":
                updates["state_ownership"] = "shared_colocated"
        else:
            policy = classify_operator_state(target, phase="instance")
            if policy.mode == "hooks":
                updates["state_ownership"] = "shared_colocated"
    return replace(base, **updates) if updates else base


def unified_source_traits(source):
    base = source_traits(source)
    if isinstance(source, sources.MaterializedSource):
        return replace(base, input_mode="finite_scope")
    return base


def unified_terminal_traits(category="action"):
    return terminal_traits(category)
