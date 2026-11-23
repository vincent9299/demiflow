"""A bounded terminal IPC leaf containing original values and their pinned row positions."""
import json

from ..contracts.dataflow import Record
from ..contracts.dataflow_errors import DataflowError
from ..recovery_contract import document, identity

_POSITION_FIELDS = ('fragment_id', 'row_offset', 'row_id')


def pack_result(batch, positions, *, source_id, split_id, version, occurrence, sequence, bounds):
    import pyarrow as pa
    if (positions.num_rows != batch.num_rows or tuple(positions.schema.names) != _POSITION_FIELDS
            or any(not pa.types.is_integer(col.type) or col.null_count for col in positions.columns)):
        raise DataflowError('INVALID_CONTRACT', 'terminal Arrow rows need one pinned source position per row')
    lineage = {'source': source_id, 'split': split_id, 'version': version, 'occurrence': occurrence,
               'sequence': sequence}
    fields = [pa.field('value', pa.struct(list(batch.schema)), metadata=batch.schema.metadata),
              pa.field('position', pa.struct(list(positions.schema)))]
    schema = pa.schema(fields, metadata={b'demiflow.result.positions.v1': document(lineage, bounds.max_control_bytes)})
    return pa.RecordBatch.from_arrays([batch.to_struct_array(), positions.to_struct_array()], schema=schema)


def validate_row_sizes(batch, maximum):
    """Prove existing typed-row limits in bulk. Ambiguous rows use the original exact codec."""
    from demiflow.execution.contracts.record_codec import _value_upper
    from demiflow.execution.contracts.record_codec import _encode_payload
    names = sum(8 + len(json.dumps(name, ensure_ascii=False).encode()) for name in batch.schema.names)
    def check(start, count):
        part = batch.slice(start, count)
        upper = (32 + names) * count
        for col in part.columns:
            amount = _value_upper(col)
            if amount is None:
                break
            upper += amount
        else:
            if upper <= maximum:
                return
        if count == 1:
            _encode_payload(part.to_pylist()[0], max_bytes=maximum)
        else:
            half = count // 2
            check(start, half)
            check(start + half, count - half)
    if batch.num_rows:
        check(0, batch.num_rows)


class PublishedArrowRows:
    """One authenticated result batch; Record consumers can reconstruct original provenance."""
    def __init__(self, ref, *, rows, first, schema_ref, schemas):
        import pyarrow as pa
        from demiflow.execution.contracts.record_codec import _uncompressed_ipc
        from demiflow.execution.storage.segments import ARROW_STREAM_MEDIA
        bounds = schemas.bounds
        if (ref.media_type != ARROW_STREAM_MEDIA or type(rows) is not int or not 0 < rows <= bounds.max_output_rows
                or not 0 < ref.size_bytes <= bounds.max_batch_bytes
                or 6 * ref.size_bytes + 65536 > bounds.max_resident_bytes):
            raise DataflowError('BUDGET_EXCEEDED', 'terminal Arrow result exceeds original read window')
        schema = schemas.read(schema_ref)
        raw = b''.join(schemas.store.read(ref, max_bytes=ref.size_bytes))
        buf = pa.py_buffer(raw)
        _uncompressed_ipc(buf, bounds)
        with pa.ipc.open_stream(buf, options=pa.ipc.IpcReadOptions(ensure_alignment=pa.ipc.Alignment.DataTypeSpecific)) as reader:
            result = reader.read_next_batch()
        result.validate(full=True)
        if (schema is None or result.num_rows != rows or result.schema.names != ['value', 'position']
                or not pa.types.is_struct(result.column(0).type) or not pa.types.is_struct(result.column(1).type)
                or result.column(0).null_count or result.column(1).null_count):
            raise DataflowError('CORRUPT_ARTIFACT', 'terminal Arrow batch shape differs from its publication')
        value = pa.RecordBatch.from_struct_array(result.column(0))
        if not value.schema.equals(schema) or result.schema.field(0).metadata != schema.metadata:
            raise DataflowError('CORRUPT_ARTIFACT', 'terminal Arrow batch differs from its declared schema')
        self.batch = value.replace_schema_metadata(schema.metadata)
        self.positions = pa.RecordBatch.from_struct_array(result.column(1))
        if (tuple(self.positions.schema.names) != _POSITION_FIELDS
                or any(not pa.types.is_integer(col.type) or col.null_count for col in self.positions.columns)):
            raise DataflowError('CORRUPT_ARTIFACT', 'invalid terminal source positions')
        try:
            self.lineage = json.loads((result.schema.metadata or {})[b'demiflow.result.positions.v1'])
            if (set(self.lineage) != {'source','split','version','occurrence','sequence'}
                    or type(self.lineage['sequence']) is not int or self.lineage['sequence'] != first
                    or type(self.lineage['version']) is not int
                    or any(not isinstance(self.lineage[k], str) or not self.lineage[k]
                           for k in ('source','split','occurrence'))):
                raise ValueError('invalid lineage')
        except (KeyError, TypeError, ValueError) as exc:
            raise DataflowError('CORRUPT_ARTIFACT', 'invalid terminal Arrow lineage') from exc
        self.schema_ref = schema_ref
        self.bounds = bounds
        self.index = 0
        self.bytes = result.get_total_buffer_size() + ref.size_bytes

    def record(self):
        from demiflow.execution.contracts.record_codec import _encode_payload
        i, lineage = self.index, self.lineage
        value = self.batch.slice(i, 1).to_pylist()[0]
        position = self.positions.slice(i, 1).to_pylist()[0]
        position['version'] = lineage['version']
        source_record = identity([lineage['source'], lineage['split'], position])
        return Record(identity([lineage['occurrence'], source_record]), position, None, value,
                      len(_encode_payload(value, max_bytes=self.bounds.max_row_bytes)), lineage['sequence'] + i,
                      self.schema_ref)

    def values(self):
        # The fast path admits primitive Arrow columns only. Charge values, containers,
        # and the authenticated IPC batch before retaining converted Python rows.
        start = self.index
        while start < self.batch.num_rows:
            count = min(256, self.batch.num_rows - start)
            available = self.bounds.max_resident_bytes - self.bytes
            while True:
                batch = self.batch.slice(start, count)
                workspace = 8 * batch.nbytes + count * (256 + 128 * len(batch.schema))
                if workspace <= available:
                    break
                if count == 1:
                    raise DataflowError('BUDGET_EXCEEDED', 'terminal Python row exceeds read workspace')
                count //= 2
            rows = batch.to_pylist()
            yield from rows
            del rows
            start += count
