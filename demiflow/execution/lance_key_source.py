"""Internal key-only Lance projection used by the public exclude_keys operator."""
from dataclasses import dataclass, replace
from ..data.sources import LanceSource
from .contracts.dataflow_errors import DataflowError


@dataclass(frozen=True)
class LanceKeySource(LanceSource):
    """The query columns are keys; payload columns remain on the consumer node."""


def key_rows(reader):
    """Read actual Lance row IDs from the original frozen version, never offsets."""
    from ..lance.storage import open_lance_dataset
    query = replace(reader._plan.query, version=reader._version)
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    bounds = reader._services.bounds
    batch_rows = max(1, min(query.batch_size or 256,
        bounds.max_batch_bytes // bounds.max_row_bytes))
    fragments = {int(f.fragment_id): f for f in dataset.get_fragments()}
    scopes = [(0, None)] if reader._global else [(f, [fragments[f]]) for f in reader._fragment_ids]
    for fragment_id, selected in scopes:
        scanner = dataset.scanner(columns=list(query.columns), filter=query.filter,
            limit=query.limit, fragments=selected, with_row_id=True, scan_in_order=True,
            batch_size=batch_rows, batch_readahead=1, fragment_readahead=1)
        offset = 0
        for batch in scanner.to_batches():
            reader._services.check_stop()
            if batch.nbytes > bounds.max_batch_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Lance key batch exceeds max_batch_bytes')
            reader._schema_ref = reader._schemas.put(batch.schema.remove(batch.schema.get_field_index('_rowid')))
            for index in range(batch.num_rows):
                part = batch.slice(index, 1)
                if part.nbytes > bounds.max_row_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'Lance key row exceeds max_row_bytes')
                yield fragment_id, offset, part.to_pylist()[0]
                offset += 1


def _hydration_rows(schema, services, requested):
    """Choose a bulk read before allocation. Variable-width values retain the sealed row ceiling."""
    from .dataflow_record_frames import resident_limit

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


class PinnedKeyPayload:
    """Read surviving row IDs from the exact source version sealed for this partition."""
    def __init__(self, spec, services):
        import pyarrow as pa
        from ..lance.storage import open_lance_dataset
        from .contracts.dataflow import thaw_record_value
        from .seal.plan import load_sealed_dataflow

        self.services = services
        _, physical, _ = load_sealed_dataflow(services.assignment.sealed,
            action_id=services.assignment.action_id, store=services.store, bounds=services.bounds)
        source_id = spec.configuration['lance_key_source']
        snapshot = next((s for s in physical.sources if s.source_id == source_id), None)
        if snapshot is None or snapshot.kind != 'lance':
            raise DataflowError('INCOMPATIBLE_HISTORY', 'exclude_keys requires its sealed Lance snapshot')
        query = thaw_record_value(snapshot.immutable_identity['query'])
        self.version = snapshot.immutable_identity['version']
        self.dataset = open_lance_dataset(query['uri'], self.version, query.get('storage_options'))
        self.columns = list(spec.configuration.get('payload_columns') or self.dataset.schema.names)
        self.schema = pa.schema([self.dataset.schema.field(c) for c in self.columns], metadata=self.dataset.schema.metadata)
        self.fragment_order = {int(f.fragment_id): i for i, f in enumerate(self.dataset.get_fragments())}
        self.requested = min(query.get('batch_size') or 1024, 8192)

    def validate_position(self, pos):
        if pos.get('version') != self.version or type(pos.get('row_id')) is not int:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Lance anti join lost pinned row identity')

    def scan_order(self, pos):
        return self.fragment_order.get(pos['fragment_id'], 0), pos['row_offset']

    def hydration_rows(self, *, bulk):
        from .dataflow_native_limits import limit_value
        bounds = self.services.bounds
        rows = _hydration_rows(self.schema, self.services, self.requested) if bulk else (
            max(1, min(256, bounds.max_batch_bytes // bounds.max_row_bytes)))
        return limit_value(self.services, 'batch_rows', rows)

    def read_rows(self, row_ids, *, bulk):
        self.services.check_stop()
        table = self.dataset._take_rows(row_ids, columns=self.columns)
        if table.num_rows != len(row_ids):
            raise DataflowError('SOURCE_CHANGED', 'Pinned Lance row IDs did not resolve one-for-one')
        if table.nbytes > self.services.bounds.max_batch_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds batch budget')
        for index in range(table.num_rows):
            row = table.slice(index, 1)
            if row.nbytes > self.services.bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds row budget')
            if not bulk:
                yield row.to_pylist()[0]
        if bulk:
            yield from table.to_pylist()
