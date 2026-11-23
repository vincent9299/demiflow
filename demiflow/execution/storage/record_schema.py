"""Bounded Arrow type metadata carried beside, never inside, business rows.

One reader/kernel owns at most its latest input and output schema. Immutable
schema bytes use the original artifact store and its original byte authority.
No schema inference is used when an upstream typed batch declared its schema.
"""
from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE, encode_schema
import threading

from demiflow.execution.contracts.dataflow_errors import DataflowError


class RecordSchemas:
    def __init__(self, services=None, *, store=None, bounds=None):
        self.store = store if services is None else services.store
        self.bounds = bounds if services is None else services.bounds
        self.maximum = min(65536, self.bounds.max_control_bytes, self.bounds.max_batch_bytes)
        self._input = (None, None)
        self._output = (None, None)
        self._lock = threading.RLock()

    def put(self, schema):
        if schema is None:
            return None
        with self._lock:
            previous, ref = self._output
            if previous is not None and schema.equals(previous, check_metadata=True):
                return ref
            raw = encode_schema(schema, bounds=self.bounds)
            ref = self.store.put(iter((raw,)), max_bytes=self.maximum, media_type=SCHEMA_MEDIA_TYPE)
            self._output = (schema, ref)
            return ref

    def read(self, ref):
        if ref is None:
            return None
        with self._lock:
            if ref == self._input[0]:
                return self._input[1]
            if ref.media_type != SCHEMA_MEDIA_TYPE or ref.size_bytes > self.maximum:
                raise DataflowError('CORRUPT_ARTIFACT', 'Arrow schema ref exceeds its encoding/bounds')
            import pyarrow as pa
            raw = b''.join(self.store.read(ref, max_bytes=self.maximum))
            try:
                schema = pa.ipc.read_schema(pa.BufferReader(raw))
            except (pa.ArrowException, ValueError) as exc:
                raise DataflowError('CORRUPT_ARTIFACT', 'Arrow schema bytes are invalid') from exc
            self._input = (ref, schema)
            return schema

    def project(self, ref, *, columns=None, names=None):
        schema = self.read(ref)
        if schema is None:
            return None
        import pyarrow as pa
        fields = [schema.field(name) for name in columns] if columns is not None else list(schema)
        if names is not None:
            fields = [field.with_name(name) for field, name in zip(fields, names)]
        return self.put(pa.schema(fields, metadata=schema.metadata))
