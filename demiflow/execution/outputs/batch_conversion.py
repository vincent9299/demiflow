"""Batch conversion implementation."""
from __future__ import annotations

import io
from demiflow.execution.contracts.record_codec import is_lance_blob_type, writer_metadata_bytes as _writer_metadata, writer_type_bytes as _writer_type


def native_arrow_table(rows, schema):
    """Construct already-admitted native columns with original blob semantics."""
    import pyarrow as pa
    if schema is None or not any(is_lance_blob_type(f.type) for f in schema):
        return pa.Table.from_pylist(rows, schema=schema)
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
            array = pa.array(values, type=field.type)
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
