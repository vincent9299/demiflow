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


class PinnedKeyScan:
    """Project one fixed-width key from the original split, with its real row IDs on the left."""
    @classmethod
    def open(cls, reader, field, *, positions):
        import pyarrow as pa
        from ..lance.model import LanceScanSpec
        from ..lance.storage import open_lance_dataset
        from demiflow.execution.scheduler.native_limits import limit_value
        q = reader._plan.query
        if (type(q) is not LanceScanSpec or reader._global or q.filter or q.limit or q.projection
                or reader._replay_pending or reader._offset.get('arrow_batch')
                or q.columns and field not in q.columns):
            return None
        ds = open_lance_dataset(q.uri, reader._version, q.storage_options)
        if field not in ds.schema.names or not pa.types.is_integer(ds.schema.field(field).type):
            return None
        fragments = {int(f.fragment_id): f for f in ds.get_fragments()}
        if any(f not in fragments for f in reader._fragment_ids):
            raise DataflowError('SOURCE_CHANGED', 'a pinned key scan lost an assigned fragment')
        bounds = reader._services.bounds
        width = (ds.schema.field(field).type.bit_width // 8 + 1) + 8 * positions
        if width > bounds.max_row_bytes:
            return None
        rows = min(q.batch_size or 1024, bounds.max_batch_bytes // width)
        rows = limit_value(reader._services, 'batch_rows', rows)
        if rows < 1:
            return None
        return cls(reader, ds, [fragments[f] for f in reader._fragment_ids], field, positions, rows)

    def __init__(self, reader, dataset, fragments, field, positions, rows):
        self.reader, self.dataset, self.fragments = reader, dataset, fragments
        self.field, self.positions, self.batch_rows = field, positions, rows
        self.dtype = dataset.schema.field(field).type
        self.counts = [f.count_rows() for f in fragments]
        self.count = sum(self.counts)
        self.chunk_bound = sum((n+rows-1)//rows for n in self.counts)
        self._scan = self._batches()
        self._pending = None
        self._replay = reader._next_sequence

    def _batches(self):
        for fragment in self.fragments:
            offset = 0
            scan = self.dataset.scanner(columns=[self.field], fragments=[fragment], scan_in_order=True,
                with_row_id=self.positions, batch_size=self.batch_rows, strict_batch_size=True,
                io_buffer_size=self.reader._services.bounds.max_batch_bytes, batch_readahead=1, fragment_readahead=1)
            for batch in scan.to_batches():
                if not batch.num_rows:
                    continue
                if batch.nbytes > self.reader._services.bounds.max_batch_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'decoded key projection exceeds its batch window')
                yield int(fragment.fragment_id), offset, batch
                offset += batch.num_rows

    def close(self):
        self._scan.close()
        self._pending = self._admitted = None

    def advance(self, max_rows, max_bytes):
        """Return a new native buffer once; only admitted rows advance the durable cursor."""
        self.reader._services.check_stop()
        new = None
        if self._pending is None:
            item = next(self._scan, None)
            if item is None:
                if self._replay or self.reader._next_sequence != self.count:
                    raise DataflowError('SOURCE_CHANGED', 'pinned key count differs from the source cursor')
                self.reader._eoi = True
                return None
            frag, start, batch = item
            self._pending = (frag, start, batch, 0)
            new = item
        frag, start, batch, consumed = self._pending
        if self._replay:
            n = min(self._replay, batch.num_rows - consumed)
            self._replay -= n
            if not self._replay and (frag != self.reader._fragment_id or
                                     start + consumed + n != self.reader._row_offset):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'fixed key replay differs from the sealed cursor')
            charged = size = 0
        else:
            n = min(batch.num_rows - consumed, max_rows)
            while n and batch.slice(consumed, n).nbytes > max_bytes:
                n //= 2
            if not n:
                raise DataflowError('BUDGET_EXCEEDED', 'a projected key row exceeds its credit')
            charged, size = n, batch.slice(consumed, n).nbytes
            self.reader._next_sequence += n
            self.reader._fragment_id, self.reader._row_offset = frag, start + consumed + n
            self.reader._offset = {'version': self.reader._version, 'fragment_id': frag,
                                   'row_offset': self.reader._row_offset}
        consumed += n
        self._pending = (frag, start, batch, consumed) if consumed < batch.num_rows else None
        self._admitted = ((frag, start + consumed - n, batch.slice(consumed - n, n)) if charged else None)
        return new, charged, size

    def probe_payload_is_bounded(self):
        """Skipping negative rows must not hide a source row validation failure."""
        import json
        import pyarrow as pa
        fields = self.reader._plan.query.columns or self.dataset.schema.names
        encoded = 64
        for name in fields:
            dtype = self.dataset.schema.field(name).type
            if pa.types.is_integer(dtype) or pa.types.is_boolean(dtype):
                width = 48
            elif pa.types.is_fixed_size_binary(dtype):
                width = 32 + 4 * ((dtype.byte_width + 2) // 3)
            else:
                # Variable output or nonfinite floats need the ordinary row codec.
                return False
            encoded += len(json.dumps(name, ensure_ascii=True).encode()) + width
            if encoded > self.reader._services.bounds.max_row_bytes:
                return False
        from .dataflow_record_frames import resident_limit
        # Conservatively include the row dict, typed codec tree, source/frozen
        # copies and per-field containers in the existing stage grant.
        return 8 * encoded + 2048 * (1 + len(fields)) <= resident_limit(
            self.reader._services.bounds, self.reader._services)

    def next_probe(self, rows, size):
        """Read original keys/row IDs with the cursor advanced by exactly this slice."""
        step = self.advance(rows, size)
        if step is None:
            return None
        if self._admitted is None:
            return (), 0, 0
        return self._admitted, step[1], step[2]

    def matched_records(self, fragment, offset, projection, matches):
        """Restore original row, ID, schema and sequence only for probe positives."""
        from .contracts.dataflow import Record
        from .recovery_contract import identity
        from .arrow_payload import fallback_row
        import pyarrow.compute as pc
        reader = self.reader
        query = reader._plan.query
        columns = list(query.columns or self.dataset.schema.names)
        schema = self.dataset.schema
        requested = query.batch_size or 1024
        maximum = _hydration_rows([schema.field(c) for c in columns], reader._services, requested)
        sequence = reader._next_sequence - projection.num_rows
        for start in range(0, len(matches), maximum):
            indices = matches.slice(start, maximum)
            table = _take_pinned(self.dataset, columns, pc.take(projection.column('_rowid'), indices),
                                 reader._services)
            reader._schema_ref = reader._schemas.put(table.schema)
            ordinal = 0
            for batch in table.to_batches():
                for i in range(batch.num_rows):
                    reader._services.check_stop()
                    local = indices[ordinal].as_py()
                    pos = {'version': reader._version, 'fragment_id': fragment, 'row_offset': offset + local}
                    row = fallback_row(batch, i)
                    encoded = reader.measured_row_size(row)
                    yield Record(identity([reader._snapshot.source_id,reader._split.split_id,pos]), pos,
                        None,row,encoded,sequence+local,reader._schema_ref)
                    ordinal += 1


def _take_pinned(dataset, columns, row_ids, services):
    services.check_stop()
    table = dataset._take_rows(row_ids, columns=columns)
    if table.num_rows != len(row_ids):
        raise DataflowError('SOURCE_CHANGED', 'Pinned Lance row IDs did not resolve one-for-one')
    if table.nbytes > services.bounds.max_batch_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds batch budget')
    return table


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
        from demiflow.execution.scheduler.native_limits import limit_value
        bounds = self.services.bounds
        rows = _hydration_rows(self.schema, self.services, self.requested) if bulk else (
            max(1, min(256, bounds.max_batch_bytes // bounds.max_row_bytes)))
        return limit_value(self.services, 'batch_rows', rows)

    def supports_batch_output(self):
        import pyarrow as pa
        return all(pa.types.is_integer(f.type) or pa.types.is_boolean(f.type)
                   or pa.types.is_string(f.type) or pa.types.is_binary(f.type)
                   or pa.types.is_fixed_size_binary(f.type) for f in self.schema)

    def read_table(self, row_ids):
        return _take_pinned(self.dataset, self.columns, row_ids, self.services)

    def read_rows(self, row_ids, *, bulk):
        table = self.read_table(row_ids)
        for index in range(table.num_rows):
            row = table.slice(index, 1)
            if row.nbytes > self.services.bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Lance payload hydration exceeds row budget')
            if not bulk:
                yield row.to_pylist()[0]
        if bulk:
            yield from table.to_pylist()
