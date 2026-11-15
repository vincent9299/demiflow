"""Key-only anti join, scan-order reconstruction, then pinned payload hydration."""
from dataclasses import replace
from ..dataflow_records import clone_record

from ..contracts.dataflow import thaw_record_value
from ..contracts.dataflow_errors import DataflowError
from ..dataflow_seal import load_sealed_dataflow
from ..record_schema import RecordSchemas
from .dataflow import _materialize_row, _output_record, _hash_id, _OutputBudget
from .ordering import build_sort_runs, merge_sort_runs


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
    pairs = join_pairs(left, right, left_keys=keys, right_keys=keys, how='anti', services=services)
    def ids():
        for record, _ in pairs:
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
            yield original, row.to_pylist()[0]
    for record in ordered:
        pending.append(record)
        if len(pending) < batch_rows:
            continue
        for original, row in hydrate(pending):
            out = _output_record(_hash_id([spec.configuration['occurrence_id'],original.record_id]),
                                 original.source_position, row, sequence, services)
            out = clone_record(out, schema_ref=schema_ref)
            budget.charge(out); sequence += 1
            yield out
        pending = []
    if pending:
        for original, row in hydrate(pending):
            out = _output_record(_hash_id([spec.configuration['occurrence_id'],original.record_id]),
                                 original.source_position, row, sequence, services)
            out = clone_record(out, schema_ref=schema_ref)
            budget.charge(out); sequence += 1
            yield out
