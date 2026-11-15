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
