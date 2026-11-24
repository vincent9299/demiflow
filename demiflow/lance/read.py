"""Exact Lance scan, vector search, and fragment-partitioned reads."""
from __future__ import annotations

from contextlib import closing

import math

import pyarrow.compute as pc
from demiflow.lance.arrow_batches import ARROW_BATCH_BYTES

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


def lance_scan_schema(query: LanceScanSpec) -> pa.Schema:
    """Read the declared scan schema without sampling a row, including empty tables."""
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError('Lance returned a non-Arrow schema')
    _validate_columns(schema, query.columns)
    return dataset.scanner(columns=dict(query.projection) if query.projection else (list(query.columns) or None),
                           filter=query.filter, limit=query.limit).projected_schema


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
    query: LanceScanSpec, *, batch_size: int, fragments=None, dataset=None,
    expected_schema: pa.Schema | None = None, offset: int = 0, batch_bytes: int | None = None,
) -> Iterator[pa.RecordBatch]:
    if dataset is None:
        dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError("Lance returned a non-Arrow schema")
    if expected_schema is not None and not schema.equals(expected_schema, check_metadata=False):
        raise InvalidLanceRequest("Lance exact-version schema differs from partition plan")
    _validate_columns(schema, query.columns)
    scanner = dataset.scanner(
        columns=dict(query.projection) if query.projection else (list(query.columns) or None), filter=query.filter,
        limit=None if query.limit is None else max(0,query.limit-offset), batch_size=query.batch_size or batch_size, fragments=fragments,
        **({'offset': offset} if offset else {}),
        **({'scan_in_order': True} if offset or batch_bytes is not None else {}),
        **({'batch_size_bytes': batch_bytes, 'io_buffer_size': batch_bytes} if batch_bytes is not None else {}),
        **{name: getattr(query, name) for name in ('batch_readahead', 'fragment_readahead')
           if getattr(query, name) is not None},
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


def payload_schema(schema, references):
    active = [ref for ref in references if ref['field'] in schema.names]
    fields = {ref['field'] for ref in active}
    return pa.schema([pa.field(f.name, pa.large_binary(), nullable=f.nullable)
                      if f.name in fields else f for f in schema], metadata=schema.metadata), active

def hydrate_payloads(batches, references, *, max_row_bytes, stats):
    """Bound lookups before allocation using lengths saved with each envelope.

    Offset/length arrays are query-batch bounded. Combined decoded envelope
    bytes per lookup batch are <= 8 MiB, or one finite oversized row. Repeated
    offsets share one native lookup within each admitted part; output expansion
    still obeys the full row-byte cost above. Native buffers and Lance decoding
    still require RSS headroom.
    """
    import lance
    snapshots = {ref['field']: lance.dataset(**ref['source']) for ref in references}
    for batch in batches:
        sizes = [batch.column(ref['size_field']).to_pylist() for ref in references]
        start, total = 0, 0
        ranges = []
        for i in range(batch.num_rows):
            values = [column[i] for column in sizes]
            if any(value is not None and (not isinstance(value, int) or value < 0) for value in values):
                raise ValueError('Invalid private payload length')
            cost = sum(value or 0 for value in values) + batch.slice(i, 1).nbytes
            if cost > max_row_bytes:
                raise MemoryError(f'Opaque joined row exceeds max_row_bytes={max_row_bytes}')
            if i > start and total + cost > min(ARROW_BATCH_BYTES, max_row_bytes):
                ranges.append((start, i))
                start, total = i, 0
            total += cost
        if start < batch.num_rows:
            ranges.append((start, batch.num_rows))
        for start, end in ranges:
            part = batch.slice(start, end - start)
            for ref in references:
                field = ref['field']
                offsets = part.column(field).to_pylist()
                lengths = part.column(ref['size_field']).to_pylist()
                indices, positions, expected = [], [], []
                unique = {}
                for offset, length in zip(offsets, lengths):
                    if offset is None:
                        if length is not None:
                            raise ValueError('Unmatched payload has a length')
                        positions.append(None)
                    else:
                        if length is None:
                            raise ValueError('Matched payload has no length')
                        position = unique.get(offset)
                        if position is None:
                            position = len(indices)
                            unique[offset] = position
                            indices.append(offset)
                            expected.append(length)
                        elif expected[position] != length:
                            raise RuntimeError('Pinned opaque payload lookup changed size or lost rows')
                        positions.append(position)
                if indices:
                    lookup = snapshots[field].take(indices, columns=[field])
                    if lookup.num_rows != len(indices) or pc.binary_length(lookup[field]).to_pylist() != expected:
                        raise RuntimeError('Pinned opaque payload lookup changed size or lost rows')
                    stats['max_lookup_bytes'] = max(stats['max_lookup_bytes'], lookup.nbytes)
                    column = lookup[field]
                    values = column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()
                    # Gather once into an Array, also compacting any borrowed
                    # source slice. Taking a ChunkedArray then combining it
                    # would copy the expanded payload a second time.
                    values = pc.take(values, pa.array(positions, type=pa.int32()))
                else:
                    values = pa.nulls(part.num_rows, type=pa.large_binary())
                part = part.set_column(part.schema.get_field_index(field), field, values)
            stats['rows'] += part.num_rows
            stats['batches'] += 1
            stats['max_batch_bytes'] = max(stats['max_batch_bytes'], part.nbytes)
            yield part


def scalar_equal(name, value):
    # Lance treats double quotes as string literals; identifiers use backticks.
    identifier = '`' + name.replace('`', '``') + '`'
    if value is None:
        return identifier + ' IS NULL'
    if isinstance(value, str):
        literal = "'" + value.replace("'", "''") + "'"
    elif type(value) is bool:
        literal = 'TRUE' if value else 'FALSE'
    elif type(value) is int:
        literal = str(value)
    elif type(value) is float and math.isfinite(value):
        literal = repr(value)
    else:
        raise ValueError('Lance equality key requires a finite scalar')
    return identifier + ' = ' + literal


def open_lookup_source(source, on, columns, max_matches):
    import lance
    table = lance.dataset(**source)
    names = table.schema.names
    if on not in names:
        raise ValueError('Lookup key absent from source')
    # An indexed lookup already fetches only matching rows. Keep its native
    # plan, and leave multi-match or unusual projections to that scanner:
    # many separate payload takes are slower than one native multi-row read.
    direct_projection = columns is None or (
        isinstance(columns, (list, tuple)) and bool(columns)
        and all(isinstance(c, str) and c in names for c in columns)
        and len(set(columns)) == len(columns))
    key_scan = False
    if max_matches == 1 and direct_projection:
        key_scan = not any(on in index.field_names for index in table.describe_indices())
    return table, key_scan


def lookup_batches(table, on, key, columns, max_matches, *, key_scan):
    """Read the original indexed scan or bounded key-only row-ID scan."""
    if not key_scan:
        return table.scanner(columns=columns, filter=scalar_equal(on, key),
            limit=max_matches+1, batch_size=1, batch_readahead=1,
            fragment_readahead=1).to_batches()
    def unindexed():
        predicate = scalar_equal(on, key)
        # batch_size=1 also splits an unindexed filter's *input* into individual
        # rows. Scan only keys/row IDs in finite native batches instead. The
        # result holds at most two uint64 IDs; no payload or full key index stays
        # resident. Row IDs belong to this same pinned dataset, not row offsets.
        batches = table.scanner(columns=[], filter=predicate, with_row_id=True,
            limit=max_matches+1, batch_size=8192, batch_size_bytes=64*1024,
            io_buffer_size=1024**2, batch_readahead=1, fragment_readahead=1).to_batches()
        with closing(batches):
            for batch in batches:
                for row_id in batch.column('_rowid').to_pylist():
                    # Keep the original one-row Arrow/Python payload checks.
                    # Taking all matches together could allocate max_matches
                    # oversized payloads before any size check can reject them.
                    payload = table._take_rows([row_id], columns=columns)
                    if payload.num_rows != 1:
                        raise RuntimeError('Pinned Lance lookup did not return its matching row')
                    yield payload

    return unindexed()
