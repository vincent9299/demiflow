"""Bounded logical block assignment over a completed native input spool.

This is output layout, not a second scheduler. A single admitted kernel may
produce N logical blocks without claiming N concurrent resource allocations.
"""
from demiflow.execution.contracts.dataflow import clone_record

from ..contracts.dataflow import BlockPartition, Record
from demiflow.execution.kernels.dataflow import _err, _occurrence
from demiflow.execution.storage.streams import materialize_row as _materialize_row
from demiflow.operators.relational import group_key
from demiflow.operators.block_partition import block_count, block_index, collect_block_identities, shuffled_layout


def partition_records(records, *, spec, rows, services):
    """Keep no-shuffle order; key shuffle colocates equal keys in real blocks."""
    count = block_count(spec.configuration, rows=rows, max_partitions=services.bounds.max_partitions)
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
        key = (group_key(_materialize_row(record, store=services.store, bounds=services.bounds,
                 assignment=getattr(services, 'assignment', None)), keys) if keys else None)
        index = block_index(key=key, ordinal=ordinal, rows=rows, count=count, target=target)
        yield clone_record(record, block_partition=BlockPartition(occurrence, index, count))


def preserve_block(records, block):
    for record in records:
        # A Record is already immutable and validated. Keeping an unchanged
        # sidecar needs no second DTO/provenance traversal per emitted row.
        yield (record if type(record) is Record and (record.block_partition is block
               or type(block) is BlockPartition and record.block_partition == block)
               else clone_record(record, block_partition=block))


def block_identity(record):
    from demiflow.execution.contracts.record_codec import canonical_record_json as _canonical
    tag = record.block_partition
    if tag is not None:
        return _canonical(['logical', tag.occurrence_id, tag.index])
    block = record.source_position.get('split_id') or record.source_position.get('partition')
    return _canonical(['source', block]) if block is not None else record.record_id


def shuffled_block_layout(records, *, spec, services):
    """Bind actual block identities; computation owns the bounded layout."""
    from demiflow.execution.kernels.dataflow import _seed
    def entries():
        for record in records:
            services.check_stop()
            tag = record.block_partition
            yield (None, block_identity(record)) if tag is None else ((tag.occurrence_id, tag.index, tag.count), None)
    blocks = collect_block_identities(entries(), max_partitions=services.bounds.max_partitions)
    seed, occurrence = _seed(spec, services), _occurrence(spec)
    return shuffled_layout(blocks, seed=seed, occurrence_id=occurrence)


def reordered_records(records, *, layout, spec):
    occurrence = _occurrence(spec)
    for record in records:
        yield clone_record(record, block_partition=BlockPartition(occurrence, layout[block_identity(record)], len(layout)))
