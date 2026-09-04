"""Exhaustive operator trait derivation without backend concepts."""
from __future__ import annotations
from ..data import plan as ops
from ..data import sources
from .model import OperatorTraits

def source_traits(source):
    if isinstance(source, sources.MaterializedSource): return OperatorTraits("source","driver","resident","ephemeral_worker",False)
    if isinstance(source, sources.SourcePlan): return OperatorTraits("source","ephemeral","transient","ephemeral_worker")
    raise TypeError(f"unknown source plan: {type(source).__name__}")

def operation_traits(op):
    if isinstance(op, ops.OperatorLLMMapOp): return OperatorTraits("network_io","reusable","resident","network_io_worker")
    if isinstance(op,(ops.MapOp,ops.FlatMapOp,ops.MapBatchesOp,ops.BoundMapOp,ops.FilterOp,ops.AddColumnOp)):
        return _callable_traits(op.callable)
    if isinstance(op,(ops.SortOp,ops.RepartitionOp,ops.RandomShuffleOp)): return OperatorTraits("global","ephemeral","barrier","ephemeral_worker")
    if isinstance(op,ops.LogicalOp): return OperatorTraits("transform","ephemeral","transient","ephemeral_worker")
    raise TypeError(f"unknown logical operation: {type(op).__name__}")

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
