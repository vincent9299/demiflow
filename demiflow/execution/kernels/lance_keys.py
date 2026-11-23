"""Key-only anti join, scan-order reconstruction, then pinned payload hydration."""
from dataclasses import replace
from demiflow.execution.contracts.dataflow import clone_record

from ..contracts.dataflow_errors import DataflowError
from ..record_schema import RecordSchemas
from .dataflow import _materialize_row, _output_record, _hash_id, _OutputBudget
from .ordering import build_sort_runs, merge_sort_runs


def _sql_survivors(left, right, keys, services):
    from ...operators.relational_sql import exclusion_query
    from ..contracts.relational_region import KEY_SQL_SUFFIX, SQL_CHILD_RSS, selection_wire_bytes
    from .dataflow import _group_key

    if not any(s.stage_id.endswith(KEY_SQL_SUFFIX) for s in services.assignment.stages):
        return None
    if services.query_region_offsets is None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'sealed key exclusion requires its SQL host')
    # Both finite inputs are closed and replayable. Send only canonical UTF-8 keys
    # to SQL, then read the original left port in order for survivor provenance.
    # No source, callback or payload lookup is repeated by this second scan.
    import pyarrow as pa

    window = selection_wire_bytes(SQL_CHILD_RSS)
    tables = {}
    retained = 4096  # request header and query
    for name, port in (('lkeys', left), ('rkeys', right)):
        scan = port.within_offset_budget(window // 8)
        if scan is None:
            return None
        chunks, pending = [], []
        try:
            for record in scan:
                services.check_stop()
                row = _materialize_row(record, services)
                key = None if any(row[f] is None for f in keys) else _group_key(row, keys)
                retained += 5 + (len(key.encode('utf-8')) if key is not None else 0)
                if not pending:
                    retained += 512  # per-buffer and serialization overhead
                if retained > window:
                    return None
                pending.append(key)
                if len(pending) == 8192:
                    chunks.append(pa.array(pending, type=pa.string()))
                    pending.clear()
            if pending:
                chunks.append(pa.array(pending, type=pa.string()))
        finally:
            scan.close()
        tables[name] = pa.chunked_array(chunks, type=pa.string())
    rows = len(tables['lkeys'])
    # Equality of these complete typed tokens is exactly string equality.
    # Keep the original SQL string-join template without changing the current
    # engine's key codec or converting a business value to another type.
    sql, key_functions = exclusion_query(pa.string())
    selection = services.query_region_offsets(sql, tables=tables, max_pairs=rows,
        rss_bytes=SQL_CHILD_RSS, check_stop=services.check_stop, selection=True,
        key_functions=key_functions)
    if not any(selection):
        return iter(())
    return (record for i, record in enumerate(left) if selection[i // 8] & (1 << (i % 8)))


def exclude_keys(left, right, *, spec, services):
    from .join_spool import join_pairs
    from ..lance_key_source import PinnedKeyPayload
    source = PinnedKeyPayload(spec, services)
    schema_ref = RecordSchemas(services).put(source.schema)
    keys = tuple(spec.configuration['on'])
    survivors = _sql_survivors(left, right, keys, services)
    if survivors is None:
        pairs = join_pairs(left, right, left_keys=keys, right_keys=keys, how='anti', services=services)
        survivors = (record for record, _ in pairs)
    def ids():
        for record in survivors:
            pos = record.source_position
            source.validate_position(pos)
            # The sort relation has only a row ID and two integers. Body fields
            # never enter either the hash relation or the ordering scratch.
            order = source.scan_order(pos)
            yield _output_record(record.record_id, pos, {'ordinal':order, 'row_id':pos['row_id']},
                                 record.sequence, services)
    from ..contracts.dataflow_codec import _thaw_json
    config = _thaw_json(spec.configuration)
    config.update(keys=['ordinal'], descending=False)
    ordering = replace(spec, kind='SortOp', configuration=config)
    runs = build_sort_runs(ids(), spec=ordering, services=services)
    ordered = merge_sort_runs(runs, spec=ordering, services=services)
    bulk = bool(spec.configuration.get('bulk_key_hydration'))
    batch_rows = source.hydration_rows(bulk=bulk)
    pending, sequence, budget = [], 0, _OutputBudget(spec, services, streaming=True)
    def batches():
        for record in ordered:
            pending.append(record)
            if len(pending) == batch_rows:
                yield pending
                pending.clear()
        if pending:
            yield pending

    for batch in batches():
        for original, row in zip(batch, source.read_rows(
                [r.source_position['row_id'] for r in batch], bulk=bulk), strict=True):
            out = _output_record(_hash_id([spec.configuration['occurrence_id'],original.record_id]),
                                 original.source_position, row, sequence, services)
            budget.charge(out)
            sequence += 1
            yield clone_record(out, schema_ref=schema_ref)
