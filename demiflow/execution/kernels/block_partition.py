"""Bounded logical block assignment over a completed native input spool.

This is output layout, not a second scheduler. A single admitted kernel may
produce N logical blocks without claiming N concurrent resource allocations.
"""
from dataclasses import replace
import hashlib

from ..contracts.dataflow import BlockPartition
from .dataflow import _err, _occurrence, _materialize_row, _group_key


def block_count(spec, *, rows, bounds):
    count = spec.configuration.get('num_blocks')
    if count is None:
        target = spec.configuration.get('target_num_rows_per_block')
        count = max(1, (rows + target - 1) // target) if target is not None else 1
    if type(count) is not int or not 1 <= count <= bounds.max_partitions:
        raise _err('BUDGET_EXCEEDED', 'logical block count exceeds max_partitions')
    return count


def partition_records(records, *, spec, rows, services):
    """Keep no-shuffle order; key shuffle colocates equal keys in real blocks."""
    count = block_count(spec, rows=rows, bounds=services.bounds)
    occurrence = _occurrence(spec)
    target = spec.configuration.get('target_num_rows_per_block')
    keys = tuple(spec.configuration.get('keys') or ())
    shuffle = spec.configuration.get('shuffle', False)
    if (keys or spec.configuration.get('sort')) and not shuffle:
        raise _err('INVALID_CONTRACT', 'repartition keys/sort require shuffle=True')
    if spec.configuration.get('sort') and not keys:
        raise _err('INVALID_CONTRACT', 'sorted repartition requires keys')
    for ordinal, record in enumerate(records):
        services.check_stop()
        if keys:
            key = _group_key(_materialize_row(record, services), keys)
            index = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % count
        elif target is not None:
            index = ordinal // target
        else:
            # Balanced contiguous blocks: first remainder blocks have one extra.
            size, remainder = divmod(rows, count)
            prefix = (size + 1) * remainder
            index = (ordinal // (size + 1) if ordinal < prefix
                     else remainder + (ordinal - prefix) // size)
        yield replace(record, block_partition=BlockPartition(occurrence, index, count))


def preserve_block(records, block):
    for record in records:
        yield replace(record, block_partition=block)


def block_identity(record):
    from .dataflow import _canonical
    tag = record.block_partition
    if tag is not None:
        return _canonical(['logical', tag.occurrence_id, tag.index])
    block = record.source_position.get('split_id') or record.source_position.get('partition')
    return _canonical(['source', block]) if block is not None else record.record_id


def shuffled_block_layout(records, *, spec, services):
    """Bounded block metadata only; record payloads stay in their native spool."""
    from .dataflow import _canonical, _seed, _shuffle_key_bytes
    blocks = set()
    layouts = set()
    for record in records:
        services.check_stop()
        tag = record.block_partition
        if tag is not None and (tag.occurrence_id, tag.count) not in layouts:
            if tag.count > services.bounds.max_partitions:
                raise _err('BUDGET_EXCEEDED', 'logical block count exceeds bound')
            layouts.add((tag.occurrence_id, tag.count))
            blocks.update(_canonical(['logical', tag.occurrence_id, i]) for i in range(tag.count))
        else:
            blocks.add(block_identity(record))
        if len(blocks) > services.bounds.max_partitions:
            raise _err('BUDGET_EXCEEDED', 'randomized block metadata exceeds bound')
    seed, occurrence = _seed(spec, services), _occurrence(spec)
    ordered = sorted(blocks, key=lambda key: (_shuffle_key_bytes(key, seed=seed, occurrence_id=occurrence), key))
    return {key: i for i, key in enumerate(ordered)}


def reordered_records(records, *, layout, spec):
    occurrence = _occurrence(spec)
    for record in records:
        yield replace(record, block_partition=BlockPartition(occurrence, layout[block_identity(record)], len(layout)))
