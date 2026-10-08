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


def checkpoint_lance(*args, **kwargs):
    from .checkpoint import checkpoint_lance as implementation
    return implementation(*args, **kwargs)


def find_committed_append(*args, **kwargs):
    from .reconcile import find_committed_append as implementation
    return implementation(*args, **kwargs)


def reconcile_lance_append(*args, **kwargs):
    from .reconcile import reconcile_lance_append as implementation
    return implementation(*args, **kwargs)


def rowhash_v1(*args, **kwargs):
    from .digest import rowhash_v1 as implementation
    return implementation(*args, **kwargs)


def table_digest_v1(*args, **kwargs):
    from .digest import table_digest_v1 as implementation
    return implementation(*args, **kwargs)


def aggregate_digest_v1(*args, **kwargs):
    from .digest import aggregate_digest_v1 as implementation
    return implementation(*args, **kwargs)


def write_registered_table(*args, **kwargs):
    from .registry import write_registered_table as implementation
    return implementation(*args, **kwargs)


def add_lance_columns(*args, **kwargs):
    from .mutate import add_lance_columns as implementation
    return implementation(*args, **kwargs)


def ensure_lance_vector_index(*args, **kwargs):
    from .index import ensure_lance_vector_index as implementation
    return implementation(*args, **kwargs)


def vector_index_config(*args, **kwargs):
    from .index import vector_index_config as implementation
    return implementation(*args, **kwargs)


# 类型直接导入（这些模块顶层仅依赖 stdlib；类不能经函数包装转出，
# 否则 isinstance / from_dict / 类型标注都会失效）。
from .refs import DatasetRef  # noqa: E402
from .registry import (  # noqa: E402
    Catalog, CatalogConflict, ReleaseConflict, ReleaseRegistry,
)


__all__ = [
    'add_lance_columns',
    'ensure_lance_vector_index', 'vector_index_config',
    "Catalog", "CatalogConflict",
    "LanceInspection", "LanceQuerySpec", "LanceScanSpec",
    "LanceVectorSearchSpec", "LanceWriteReceipt", "LanceWriteSpec",
    "aggregate_digest_v1", "checkpoint_lance", "DatasetRef",
    "find_committed_append", "inspect_lance", "iter_lance_batches",
    "iter_lance_partition_batches", "plan_lance_scan_partitions",
    "reconcile_lance_append", "ReleaseConflict", "ReleaseRegistry",
    "rowhash_v1", "table_digest_v1",
    "write_registered_table",
]
