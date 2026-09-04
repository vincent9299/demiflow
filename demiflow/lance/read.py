"""Exact Lance scan, vector search, and fragment-partitioned reads."""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Iterator

import pyarrow as pa

from ..errors import InvalidLanceRequest, LanceExecutionError
from .model import (
    LanceQuerySpec, LanceScanSpec, LanceVectorSearchSpec, _LanceScanPartition,
)
from .storage import open_lance_dataset, schema_hash, storage_fingerprint

_MAX_SCAN_TASKS = 64


def constrain_lance_query(
    query: LanceQuerySpec, row_limit: int | None,
) -> LanceQuerySpec:
    if row_limit is None:
        return query
    if isinstance(row_limit, bool) or not isinstance(row_limit, int) or row_limit <= 0:
        raise InvalidLanceRequest("Lance source row limit must be positive")
    if isinstance(query, LanceScanSpec):
        return replace(
            query,
            limit=row_limit if query.limit is None else min(query.limit, row_limit),
        )
    return replace(query, top_k=min(query.top_k, row_limit))


def iter_lance_batches(
    query: LanceQuerySpec, *, batch_size: int = 256,
) -> Iterator[pa.RecordBatch]:
    _positive_batch_size(batch_size)
    if isinstance(query, LanceScanSpec):
        yield from _iter_scan(query, batch_size=batch_size)
        return
    if isinstance(query, LanceVectorSearchSpec):
        yield from _iter_vector(query, batch_size=batch_size)
        return
    raise InvalidLanceRequest(f"unsupported Lance query: {type(query).__name__}")


def plan_lance_scan_partitions(
    query: LanceScanSpec, *, target_partitions: int,
) -> tuple[_LanceScanPartition, ...]:
    if not isinstance(query, LanceScanSpec):
        raise InvalidLanceRequest("Lance partition planning requires a scan")
    if query.limit is not None:
        raise InvalidLanceRequest("partitioned Lance scan does not support a global limit")
    if (
        isinstance(target_partitions, bool)
        or not isinstance(target_partitions, int)
        or target_partitions <= 0
    ):
        raise InvalidLanceRequest("target_partitions must be positive")
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    version = _dataset_version(dataset)
    exact = replace(query, version=version)
    fragments = tuple(dataset.get_fragments())
    inventory_hash = fragment_inventory_hash(fragments)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError("Lance returned a non-Arrow schema")
    count = min(len(fragments) or 1, target_partitions, _MAX_SCAN_TASKS)
    assignments: list[list[tuple[int, int]]] = [[] for _ in range(count)]
    loads = [0] * count
    weighted = sorted(
        ((_fragment_weight(fragment), int(fragment.fragment_id)) for fragment in fragments),
        key=lambda item: (-item[0], item[1]),
    )
    for weight, fragment_id in weighted:
        index = min(range(count), key=lambda value: (loads[value], value))
        assignments[index].append((weight, fragment_id))
        loads[index] += weight
    fingerprint = storage_fingerprint(exact.uri, exact.storage_options)
    digest = schema_hash(schema)
    return tuple(
        _LanceScanPartition(
            query=exact, schema=schema, schema_hash=digest,
            partition_index=index, partition_count=count,
            fragment_ids=tuple(sorted(fragment_id for _, fragment_id in assignment)),
            source_inventory_hash=inventory_hash,
            storage_fingerprint=fingerprint,
        )
        for index, assignment in enumerate(assignments)
    )


def iter_lance_partition_batches(
    partition: _LanceScanPartition, *, batch_size: int = 256,
) -> Iterator[pa.RecordBatch]:
    _positive_batch_size(batch_size)
    fingerprint = storage_fingerprint(
        partition.query.uri, partition.query.storage_options,
    )
    if partition.storage_fingerprint != fingerprint:
        raise InvalidLanceRequest("Lance storage changed after scan planning")
    dataset = open_lance_dataset(
        partition.query.uri, partition.query.version,
        partition.query.storage_options,
    )
    if schema_hash(dataset.schema) != partition.schema_hash:
        raise InvalidLanceRequest("Lance partition schema changed")
    fragments = tuple(dataset.get_fragments())
    if fragment_inventory_hash(fragments) != partition.source_inventory_hash:
        raise InvalidLanceRequest("Lance fragment inventory changed")
    by_id = {int(fragment.fragment_id): fragment for fragment in fragments}
    if len(by_id) != len(fragments):
        raise InvalidLanceRequest("Lance fragment inventory contains duplicate IDs")
    try:
        selected = tuple(by_id[value] for value in partition.fragment_ids)
    except KeyError as exc:
        raise InvalidLanceRequest("Lance partition references a missing fragment") from exc
    yield from _iter_scan(
        partition.query, batch_size=batch_size, fragments=selected,
        expected_schema=partition.schema,
    )


def fragment_inventory_hash(fragments) -> str:
    value = []
    for fragment in fragments:
        metadata = fragment.metadata
        files = sorted(
            (str(item.path), int(getattr(item, "file_size_bytes", 0) or 0))
            for item in metadata.files
        )
        value.append((int(fragment.fragment_id), int(metadata.physical_rows), files))
    raw = json.dumps(sorted(value), separators=(",", ":"), ensure_ascii=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _iter_scan(
    query: LanceScanSpec, *, batch_size: int, fragments=None,
    expected_schema: pa.Schema | None = None,
) -> Iterator[pa.RecordBatch]:
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError("Lance returned a non-Arrow schema")
    if expected_schema is not None and not schema.equals(expected_schema, check_metadata=False):
        raise InvalidLanceRequest("Lance exact-version schema differs from partition plan")
    _validate_columns(schema, query.columns)
    scanner = dataset.scanner(
        columns=list(query.columns) or None, filter=query.filter,
        limit=query.limit, batch_size=batch_size, fragments=fragments,
    )
    yield from _validated_batches(scanner.to_batches(), scanner.projected_schema)


def _iter_vector(
    query: LanceVectorSearchSpec, *, batch_size: int,
) -> Iterator[pa.RecordBatch]:
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError("Lance returned a non-Arrow schema")
    if query.vector_column not in schema.names:
        raise InvalidLanceRequest(f"Lance vector column not found: {query.vector_column!r}")
    field = schema.field(query.vector_column)
    if (
        not pa.types.is_fixed_size_list(field.type)
        or not pa.types.is_floating(field.type.value_type)
    ):
        raise InvalidLanceRequest("Lance vector column must be a fixed-size floating list")
    if field.type.list_size != len(query.vector):
        raise InvalidLanceRequest("Lance query vector dimension differs from column")
    if "_distance" in schema.names:
        raise InvalidLanceRequest("Lance source schema reserves '_distance'")
    _validate_columns(schema, query.columns)
    nearest = {
        "column": query.vector_column,
        "q": pa.array(query.vector, type=field.type.value_type),
        "k": query.top_k,
    }
    if query.metric is not None:
        nearest["metric"] = query.metric
    scanner = dataset.scanner(
        columns=list(query.columns) if query.columns else list(schema.names),
        filter=query.filter, nearest=nearest, prefilter=query.filter is not None,
        batch_size=batch_size,
    )
    yield from _validated_batches(scanner.to_batches(), scanner.projected_schema)


def _validated_batches(batches, expected_schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        if not isinstance(batch, pa.RecordBatch):
            raise LanceExecutionError("Lance yielded a non-RecordBatch value")
        if not batch.schema.equals(expected_schema, check_metadata=False):
            raise LanceExecutionError("Lance batch schema differs from projected schema")
        yield batch


def _validate_columns(schema: pa.Schema, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns) - set(schema.names))
    if missing:
        raise InvalidLanceRequest(f"Lance columns not found: {missing}")


def _dataset_version(dataset) -> int:
    value = getattr(dataset, "version", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LanceExecutionError("Lance returned an invalid dataset version")
    return value


def _positive_batch_size(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidLanceRequest("batch_size must be positive")


def _fragment_weight(fragment) -> int:
    sizes = [
        int(getattr(item, "file_size_bytes", 0) or 0)
        for item in fragment.metadata.files
    ]
    total = sum(value for value in sizes if value > 0)
    return total or max(1, int(fragment.metadata.physical_rows))


__all__ = [
    "constrain_lance_query", "fragment_inventory_hash", "iter_lance_batches",
    "iter_lance_partition_batches", "plan_lance_scan_partitions",
]
