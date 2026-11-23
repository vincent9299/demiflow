"""Column records and worker callables referenced by sealed native payloads."""
from __future__ import annotations

from dataclasses import dataclass


def finish_typed_batch(batch, *, columns, encoded_operations, schema):
    """Partition-local callbacks, one instance per configured input task batch."""
    import pyarrow as pa
    import cloudpickle
    from .local_tasks import _apply
    operations = cloudpickle.loads(encoded_operations)
    rows = ({c.name: row[c.value] for c in columns if row[c.present]} for row in batch.to_pylist())
    return pa.RecordBatch.from_pylist(list(_apply(rows, operations)), schema=schema)


@dataclass
class Column:
    name: str
    value: str
    present: str
    dtype: object
    optional: bool = False


def _cast_sink_batch(batch, *, schema_bytes, max_bytes):
    """An ordinary native worker operator; retain both arrays before casting."""
    import base64
    import pyarrow as pa
    from .arrow_batch_sink import _cast_allocation
    from .contracts.dataflow_errors import DataflowError
    from ..resources.capacity import inherited_envelope
    schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(schema_bytes, validate=True)))
    envelope = inherited_envelope()
    if envelope is None:
        raise DataflowError('INVALID_CONTRACT', 'native schema conversion requires an admitted worker')
    maximum = min(max_bytes, envelope['memory_bytes'])
    if batch.get_total_buffer_size() + _cast_allocation(batch, schema) > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'native schema input and cast output exceed the byte ceiling')
    return batch.cast(schema, safe=True)
