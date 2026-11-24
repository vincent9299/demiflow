"""Batch conversion implementation."""
from __future__ import annotations

import io
from demiflow.execution.contracts.record_codec import is_lance_blob_type, writer_metadata_bytes as _writer_metadata, writer_type_bytes as _writer_type


def _first_incompatible_row(values, arrow_type):
    import pyarrow as pa
    for index, value in enumerate(values):
        try:
            pa.array([value], type=arrow_type)
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            return index, type(value).__name__
    return None, None


def _column_schema_error(field, values, exc):
    row_index, actual_type = _first_incompatible_row(values, field.type)
    got = f', got {actual_type}' if actual_type is not None else ''
    loc = f' at row {row_index}' if row_index is not None else ''
    return ValueError(
        f"Lance schema mismatch on column '{field.name}': "
        f"expected {field.type}{got}{loc} ({exc})"
    )


def table_from_pylist(rows, schema):
    """Build an Arrow table from row dicts; name the failing column and types on mismatch."""
    import pyarrow as pa
    try:
        return pa.Table.from_pylist(rows, schema=schema)
    except (pa.ArrowTypeError, pa.ArrowInvalid) as exc:
        if schema is not None and rows:
            for field in schema:
                values = [row.get(field.name) for row in rows]
                try:
                    pa.array(values, type=field.type)
                except (pa.ArrowTypeError, pa.ArrowInvalid) as col_exc:
                    raise _column_schema_error(field, values, col_exc) from None
        raise ValueError(f'Lance schema mismatch: {exc}') from None


def native_arrow_table(rows, schema):
    """Construct already-admitted native columns with original blob semantics."""
    import pyarrow as pa
    if schema is None or not any(is_lance_blob_type(f.type) for f in schema):
        return table_from_pylist(rows, schema)
    import lance
    from lance.blob import Blob
    arrays = []
    for field in schema:
        values = [row.get(field.name) for row in rows]
        if is_lance_blob_type(field.type):
            values = [Blob(**value) if type(value) is dict else value for value in values]
            array = lance.blob_array(values)
            if array.type != field.type:
                # Historical two-field BlobType storage remains its own type.
                storage = pa.StructArray.from_arrays(
                    [array.storage.field(f.name) for f in field.type.storage_type],
                    fields=list(field.type.storage_type), mask=array.is_null())
                array = pa.ExtensionArray.from_storage(field.type, storage)
        else:
            try:
                array = pa.array(values, type=field.type)
            except (pa.ArrowTypeError, pa.ArrowInvalid) as col_exc:
                raise _column_schema_error(field, values, col_exc) from None
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)

def native_writer_workspace(schema, bounds):
    """Bound the actual schema plus the original Python file write buffer.

    This replaces a 64-KiB *estimate*, not an allocated writer buffer. Payload,
    Arrow builder and two writer copies remain separately admitted as before.
    """
    return (io.DEFAULT_BUFFER_SIZE + 2048 + _writer_metadata(schema.metadata)
        + sum(4*len(field.name) + _writer_metadata(field.metadata)
              + _writer_type(field.type, bounds) for field in schema))


def _cast_sink_batch(batch, *, schema_bytes, max_bytes):
    """An ordinary native worker operator; retain both arrays before casting."""
    import base64
    import pyarrow as pa
    from demiflow.execution.contracts.record_codec import cast_allocation as _cast_allocation
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.resources.capacity import inherited_envelope
    schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(schema_bytes, validate=True)))
    envelope = inherited_envelope()
    if envelope is None:
        raise DataflowError('INVALID_CONTRACT', 'native schema conversion requires an admitted worker')
    maximum = min(max_bytes, envelope['memory_bytes'])
    if batch.get_total_buffer_size() + _cast_allocation(batch, schema) > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'native schema input and cast output exceed the byte ceiling')
    return batch.cast(schema, safe=True)
