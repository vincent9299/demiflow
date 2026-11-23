"""Key-only anti join, scan-order reconstruction, then pinned payload hydration."""
from dataclasses import replace
from ..dataflow_records import clone_record

from ..contracts.dataflow import thaw_record_value
from ..contracts.dataflow_errors import DataflowError
from ..seal.plan import load_sealed_dataflow
from ..record_schema import RecordSchemas
from .dataflow import _materialize_row, _output_record, _hash_id, _OutputBudget
from .ordering import build_sort_runs, merge_sort_runs


def _sql_survivors(left, right, keys, services):
    from ...operators.relational import EXCLUDE_OFFSETS_SQL
    from ..contracts.relational_region import KEY_SQL_SUFFIX, SQL_CHILD_RSS, selection_wire_bytes
    from .dataflow import _group_key

    if not any(s.stage_id.endswith(KEY_SQL_SUFFIX) for s in services.assignment.stages):
        return None
    if services.query_region_offsets is None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'sealed key exclusion requires its SQL host')
    # Both finite inputs are closed and replayable. Send only canonical keys
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
                key = None if any(row[f] is None for f in keys) else _group_key(row, keys).encode('utf-8')
                retained += 5 + (len(key) if key is not None else 0)
                if not pending:
                    retained += 512  # per-buffer and serialization overhead
                if retained > window:
                    return None
                pending.append(key)
                if len(pending) == 8192:
                    chunks.append(pa.array(pending, type=pa.binary()))
                    pending.clear()
            if pending:
                chunks.append(pa.array(pending, type=pa.binary()))
        finally:
            scan.close()
        tables[name] = pa.chunked_array(chunks, type=pa.binary())
    rows = len(tables['lkeys'])
    selection = services.query_region_offsets(EXCLUDE_OFFSETS_SQL, tables=tables, max_pairs=rows,
        rss_bytes=SQL_CHILD_RSS, check_stop=services.check_stop, selection=True)
    if not any(selection):
        return iter(())
    return (record for i, record in enumerate(left) if selection[i // 8] & (1 << (i % 8)))


def _hydration_rows(schema, services, requested):
    """Choose a bulk read before allocation. Variable-width values retain the sealed row ceiling."""
    from ..dataflow_record_frames import resident_limit

    bounds = services.bounds
    width = 0
    for field in schema:
        try:
            width += max(1, (field.type.bit_width + 7) // 8) + 1
        except (ValueError, AttributeError):
            # Variable-width or nested buffers cannot be bounded by the schema.
            width = bounds.max_row_bytes
            break
    python_row = 256 + 128 * len(schema) + 8 * width
    return max(1, min(requested, bounds.max_batch_bytes // max(1, width),
                      resident_limit(bounds, services) // (2 * python_row)))


def exclude_keys(left, right, *, spec, services):
    from .join_spool import join_pairs
    from ...lance.storage import open_lance_dataset
    source_id = spec.configuration['lance_key_source']
    _, physical, _ = load_sealed_dataflow(services.assignment.sealed,
        action_id=services.assignment.action_id, store=services.store, bounds=services.bounds)
    snapshot = next((s for s in physical.sources if s.source_id == source_id), None)
    if snapshot is None or snapshot.kind != 'lance':
        raise DataflowError('INCOMPATIBLE_HISTORY', 'exclude_keys requires its sealed Lance snapshot')
    query = thaw_record_value(snapshot.immutable_identity['query'])
    version = snapshot.immutable_identity['version']
    dataset = open_lance_dataset(query['uri'], version, query.get('storage_options'))
    columns = tuple(spec.configuration.get('payload_columns') or dataset.schema.names)
    import pyarrow as pa
    schema = pa.schema([dataset.schema.field(c) for c in columns], metadata=dataset.schema.metadata)
    schemas = RecordSchemas(services)
    schema_ref = schemas.put(schema)
    fragment_order = {int(f.fragment_id): i for i, f in enumerate(dataset.get_fragments())}
    keys = tuple(spec.configuration['on'])
    survivors = _sql_survivors(left, right, keys, services)
    if survivors is None:
        pairs = join_pairs(left, right, left_keys=keys, right_keys=keys, how='anti', services=services)
        survivors = (record for record, _ in pairs)
    def ids():
        for record in survivors:
            pos = record.source_position
            if pos.get('version') != version or type(pos.get('row_id')) is not int:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Lance anti join lost pinned row identity')
            # The sort relation has only a row ID and two integers. Body fields
            # never enter either the hash relation or the ordering scratch.
            order = (fragment_order.get(pos['fragment_id'], 0), pos['row_offset'])
            yield _output_record(record.record_id, pos, {'ordinal':order, 'row_id':pos['row_id']},
                                 record.sequence, services)
    from ..contracts.dataflow_codec import _thaw_json
    config = _thaw_json(spec.configuration)
    config.update(keys=['ordinal'], descending=False)
    ordering = replace(spec, kind='SortOp', configuration=config)
    runs = build_sort_runs(ids(), spec=ordering, services=services)
    ordered = merge_sort_runs(runs, spec=ordering, services=services)
    bulk = bool(spec.configuration.get('bulk_key_hydration'))
    if bulk:
        batch_rows = _hydration_rows(schema, services, min(query.get('batch_size') or 1024, 8192))
    else:
        batch_rows = max(1, min(256, services.bounds.max_batch_bytes // services.bounds.max_row_bytes))
    from ..dataflow_native_limits import limit_value
    batch_rows = limit_value(services, 'batch_rows', batch_rows)
    pending, sequence, budget = [], 0, _OutputBudget(spec, services, streaming=True)
    def hydrate(batch):
        services.check_stop()
        table = dataset._take_rows([int(r.source_position['row_id']) for r in batch], columns=list(columns))
        if table.num_rows != len(batch):
            raise DataflowError('SOURCE_CHANGED', 'Pinned Lance row IDs did not resolve one-for-one')
        if table.nbytes > services.bounds.max_batch_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds batch budget')
        for index, original in enumerate(batch):
            row = table.slice(index, 1)
            if row.nbytes > services.bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds row budget')
            if not bulk:
                yield original, row.to_pylist()[0]
        if bulk:
            yield from zip(batch, table.to_pylist(), strict=True)
    def batches():
        for record in ordered:
            pending.append(record)
            if len(pending) == batch_rows:
                yield pending
                pending.clear()
        if pending:
            yield pending

    for batch in batches():
        for original, row in hydrate(batch):
            out = _output_record(_hash_id([spec.configuration['occurrence_id'],original.record_id]),
                                 original.source_position, row, sequence, services)
            budget.charge(out)
            sequence += 1
            yield clone_record(out, schema_ref=schema_ref)
