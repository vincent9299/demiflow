"""Durable Lance adapter around the existing native writer and its CAS marker."""
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import uuid

from ..data.datasink import Datasink
from ..errors import LanceResourceNotFound, LanceWriteConflict, LanceWriteError
from ..lance.model import LanceWriteReceipt
from ..lance.storage import inspect_lance, open_lance_dataset, schema_hash
from .block_sink import storage_usage
from .recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity
from .state_artifacts import atomic_bytes


class LanceSink(Datasink):
    def __init__(self, action, spec, *, native_merge=None):
        self.action, self.run, self.spec = action, action.context, spec
        self.sink_id = "lance"
        self.native_merge = native_merge
        self.root = self.run.directory / "materializations" / action.action_id / "lance"

    def write(self, blocks, context):
        raise RecoveryContractError("Lance sink requires the native commit protocol")

    def prepare(self, blocks, context):
        import pyarrow as pa
        try:
            current = inspect_lance(self.spec.uri, None, self.spec.storage_options)
            base = current.resolved_version
        except LanceResourceNotFound:
            base = None
        if self.spec.mode == "create" and base is not None:
            raise LanceWriteConflict("Create target already exists")
        if self.spec.expected_version is not None and base != self.spec.expected_version:
            raise LanceWriteConflict("Lance CAS baseline changed")
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / (uuid.uuid4().hex + ".arrow")
        self.run.allocate_artifact(path)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        writer = None
        rows = logical_bytes = 0
        schema = self.spec.schema
        try:
            with path.open("xb") as stream:
                from .payload_limits import arrow_batches
                for block in arrow_batches(blocks, self.run.limits):
                    self.run.check_stop()
                    if not isinstance(block, (pa.Table, pa.RecordBatch)):
                        raise RecoveryContractError("Lance staging requires native Arrow batches")
                    rows += block.num_rows
                    logical_bytes += block.nbytes
                    if rows > self.run.limits["max_output_rows"] or logical_bytes > self.run.limits["max_output_bytes"]:
                        raise RecoveryContractError("Lance output exceeds row/byte budget")
                    # IPC framing/alignment plus retained buffers; native allocations
                    # remain covered by the driver isolation, not by this estimate.
                    self.run.reserve_artifact_bytes(path, stream.tell() + 2 * block.get_total_buffer_size() + 65536)
                    schema = schema or block.schema
                    block_schema = self.spec.schema or block.schema
                    if block.schema != block_schema:
                        block = block.cast(block_schema)
                    writer = pa.ipc.new_stream(stream, block_schema)
                    if isinstance(block, pa.RecordBatch):
                        writer.write_batch(block)
                    else:
                        writer.write_table(block)
                    writer.close()
                    stream.flush()
                    self.run.reserve_artifact_bytes(path, stream.tell())
                if writer is None:
                    if schema is None and self.spec.mode == 'merge' and base is not None:
                        schema = current.schema
                    if schema is None:
                        from ..errors import InvalidLanceRequest
                        raise InvalidLanceRequest("Empty Lance output requires an explicit schema")
                    writer = pa.ipc.new_stream(stream, schema)
                    writer.close()
                stream.flush()
                os.fsync(stream.fileno())
                self.run.reserve_artifact_bytes(path, stream.tell())
        except BaseException as exc:
            path.unlink(missing_ok=True)
            from ..errors import InvalidLanceRequest
            # The native writer consumes the first nonempty input before its
            # write-error boundary. Preserve failures before that point.
            if rows and isinstance(exc, (RuntimeError,)) and not isinstance(exc, (RecoveryContractError, InvalidLanceRequest, LanceWriteConflict)):
                from demiflow._compat.error_transport import error_from_exception
                raise LanceWriteError(LanceWriteReceipt(request_hash=self.spec.content_hash, uri=self.spec.uri,
                    expected_version=self.spec.expected_version, committed_version=None, input_rows=rows,
                    written_rows=None, schema_hash=schema_hash(schema) if schema is not None else None,
                    status='indeterminate', error=error_from_exception(exc))) from exc
            raise
        self.run.reserve('output_rows', rows, ceiling=self.run.limits['max_output_rows'],
            reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                     'component': 'output_rows'}))
        self.run.reserve_output('row_bytes', logical_bytes,
            reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                     'component': 'row_bytes'}))
        descriptor = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for raw in iter(lambda: stream.read(1024**2), b""):
                digest.update(raw)
        return dict(path=str(path.relative_to(self.run.directory)), bytes=path.stat().st_size,
                    sha256=digest.hexdigest(), rows=rows, schema_hash=schema_hash(schema), base_version=base)

    def _path(self, prepared):
        relative = Path(prepared["path"])
        path = self.run.directory / relative
        if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(self.root):
            raise RecoveryContractError("Lance staging reference escapes its owner")
        if any(p.is_symlink() for p in (path, *path.parents)) or path.stat().st_size != prepared["bytes"]:
            raise RecoveryContractError("Lance staging size differs")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for raw in iter(lambda: stream.read(1024**2), b""):
                digest.update(raw)
        if digest.hexdigest() != prepared["sha256"]:
            raise RecoveryContractError("Lance staging checksum differs")
        return path

    def _receipt(self, native, context):
        native = replace(native, request_hash=self.spec.content_hash,
                         expected_version=self.spec.expected_version)
        return dict(commit_id=context.commit_id, sink_id=self.sink_id,
                    status="committed", data=native.to_dict())

    def _marker(self, context):
        return self.root / (context.commit_id + ".receipt.json")

    def inspect(self, prepared, context):
        import json
        self._path(prepared)
        marker = self._marker(context)
        if marker.exists():
            if marker.is_symlink() or marker.stat().st_size > 65536:
                raise RecoveryContractError("Invalid native Lance receipt")
            receipt = json.loads(marker.read_bytes())
            native = LanceWriteReceipt.from_dict(receipt["data"])
            if (native.uri != self.spec.uri or native.request_hash != self.spec.content_hash
                    or native.expected_version != self.spec.expected_version
                    or native.input_rows != prepared['rows']):
                raise RecoveryContractError('Native Lance receipt identity differs')
            dataset = open_lance_dataset(self.spec.uri, native.committed_version, self.spec.storage_options)
            if schema_hash(dataset.schema) != native.schema_hash:
                raise RecoveryContractError("Committed Lance schema differs")
            if self.spec.mode != "merge":
                transaction = dataset.read_transaction(native.committed_version)
                actual = (transaction.transaction_properties or {}).get("__lance_commit_message")
                if actual != "demiflow-lance:" + context.commit_id:
                    raise RecoveryContractError("Native Lance publication identity differs")
            return receipt
        base = prepared["base_version"]
        expected = 1 if base is None else base + 1
        try:
            dataset = open_lance_dataset(self.spec.uri, None, self.spec.storage_options)
        except LanceResourceNotFound:
            if base is None:
                return None
            raise UnresolvedCommit("Previously bound Lance target disappeared")
        if dataset.version == base:
            return None
        try:
            committed = open_lance_dataset(self.spec.uri, expected, self.spec.storage_options)
        except LanceResourceNotFound as exc:
            raise UnresolvedCommit("Cannot establish the exact Lance successor") from exc
        transaction = committed.read_transaction(expected)
        actual = (transaction.transaction_properties or {}).get("__lance_commit_message")
        if actual != "demiflow-lance:" + context.commit_id:
            raise UnresolvedCommit("Lance successor does not prove this publication")
        if schema_hash(committed.schema) != prepared["schema_hash"]:
            raise RecoveryContractError("Lance successor schema differs")
        native = LanceWriteReceipt(request_hash=self.spec.content_hash, uri=self.spec.uri,
            expected_version=self.spec.expected_version, committed_version=expected,
            input_rows=prepared["rows"], written_rows=prepared["rows"],
            schema_hash=prepared["schema_hash"], status="committed")
        return self._receipt(native, context)

    def commit(self, prepared, context):
        import pyarrow as pa
        from ..lance.write import write_lance
        path = self._path(prepared)
        effective = self.spec
        if self.spec.mode in {"append", "overwrite"}:
            effective = replace(self.spec, mode="create" if prepared["base_version"] is None else self.spec.mode,
                                expected_version=prepared["base_version"])
        merged = None
        if self.spec.mode == 'merge':
            effective = replace(self.spec, expected_version=prepared['base_version'])
            if self.native_merge is not None:
                merged = self.native_merge(effective)
        if merged is not None:
            receipt = merged
        elif (self.spec.mode == 'append' and self.spec.expected_version is None
                and prepared['base_version'] is not None and prepared['rows'] == 0):
            # Preserve default empty-append semantics using a native empty CAS
            # transaction. Explicit compare-and-append still rejects empty input.
            from ..lance.storage import require_lance, require_compatible_schema
            native = require_lance()
            dataset = open_lance_dataset(self.spec.uri, prepared['base_version'], self.spec.storage_options)
            require_compatible_schema(dataset.schema, self.spec.schema)
            committed = native.LanceDataset.commit(self.spec.uri, native.LanceOperation.Append([]),
                read_version=prepared['base_version'], max_retries=0,
                storage_options=dict(self.spec.storage_options) or None,
                commit_message='demiflow-lance:' + context.commit_id)
            receipt = LanceWriteReceipt(request_hash=self.spec.content_hash, uri=self.spec.uri,
                expected_version=None, committed_version=committed.version, input_rows=0, written_rows=0,
                schema_hash=prepared['schema_hash'], status='committed')
        else:
            with path.open("rb") as stream:
                def original_batches():
                    while stream.tell() < prepared['bytes']:
                        reader = pa.ipc.open_stream(stream)
                        yield from reader
                receipt = write_lance(effective, original_batches(), limits=self.run.limits, check_stop=self.run.check_stop,
                                      operation_id=context.commit_id if effective.mode != "merge" else None)
        if receipt.status != "committed":
            raise LanceWriteError(receipt)
        result = self._receipt(receipt, context)
        raw = document(result, 65536)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id, size=len(raw))
        atomic_bytes(self._marker(context), raw)
        return result

    def abort(self, prepared, context):
        if self.inspect(prepared, context) is None:
            self._path(prepared).unlink(missing_ok=True)


def spec_from_identity(value):
    from ..lance.model import LanceWriteSpec
    import pyarrow as pa
    if value['type'] != 'demiflow.lance.model.LanceWriteSpec':
        raise RecoveryContractError('Expected native Lance write declaration')
    fields = dict(value['fields'])
    schema = fields.get('schema')
    if schema is not None:
        fields['schema'] = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(schema['arrow_schema'])))
    return LanceWriteSpec(**fields)
