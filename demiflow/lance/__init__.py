"""Lazy public facade for Demiflow's built-in Lance I/O."""
from __future__ import annotations

from .model import (
    LanceInspection, LanceQuerySpec, LanceScanSpec, LanceVectorSearchSpec,
    LanceWriteReceipt, LanceWriteSpec,
)


def inspect_lance(*args, **kwargs):
    from .storage import inspect_lance as implementation
    return implementation(*args, **kwargs)


def iter_lance_batches(*args, **kwargs):
    from .read import iter_lance_batches as implementation
    return implementation(*args, **kwargs)


def plan_lance_scan_partitions(*args, **kwargs):
    from .read import plan_lance_scan_partitions as implementation
    return implementation(*args, **kwargs)


def iter_lance_partition_batches(*args, **kwargs):
    from .read import iter_lance_partition_batches as implementation
    return implementation(*args, **kwargs)


__all__ = [
    "LanceInspection", "LanceQuerySpec", "LanceScanSpec",
    "LanceVectorSearchSpec", "LanceWriteReceipt", "LanceWriteSpec",
    "inspect_lance", "iter_lance_batches", "iter_lance_partition_batches",
    "plan_lance_scan_partitions",
]
