"""Durable Lance adapter around the existing native writer and its CAS marker."""

from demiflow.execution.contracts.dataflow_errors import DataflowError

from demiflow.execution.contracts.errors import PublicationMismatch
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import uuid

from demiflow.data.datasink import Datasink
from demiflow.errors import LanceResourceNotFound, LanceWriteConflict, LanceWriteError
from demiflow.lance.model import LanceWriteReceipt
from demiflow.lance.storage import inspect_lance, open_lance_dataset, schema_hash
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity
from demiflow.execution.storage.state_artifacts import atomic_bytes
from demiflow.execution.run_journal import ExecutionBudgetExceeded


class LanceSink(Datasink):
    def __init__(self, action, spec, *, exact_merge_source=False):
        self.action, self.run, self.spec = action, action.context, spec
        self.sink_id = "lance"
        self.exact_merge_source = exact_merge_source
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
        append_schema = current.schema if self.spec.mode == 'append' and base is not None else None
        if append_schema is not None and self.spec.schema is not None:
            from demiflow.lance.storage import require_compatible_schema
            require_compatible_schema(self.spec.schema, append_schema)
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / (uuid.uuid4().hex + ".arrow")
        self.run.allocate_artifact(path)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        writer = None
        rows = logical_bytes = 0
        schema = self.spec.schema
        try:
            with path.open("xb") as stream:
                from demiflow.lance.arrow_batches import arrow_batches
                for block in arrow_batches(blocks, self.run.limits):
                    self.run.check_stop()
                    if not isinstance(block, (pa.Table, pa.RecordBatch)):
                        raise RecoveryContractError("Lance staging requires native Arrow batches")
                    rows += block.num_rows
                    logical_bytes += block.nbytes
                    if rows > self.run.limits["max_output_rows"]:
                        raise ExecutionBudgetExceeded("max_output_rows", "Lance output exceeds row budget")
                    if logical_bytes > self.run.limits["max_output_bytes"]:
                        raise ExecutionBudgetExceeded("max_output_bytes", "Lance output exceeds byte budget")
                    # IPC framing/alignment plus retained buffers; native allocations
                    # remain covered by the driver isolation, not by this estimate.
                    self.run.reserve_artifact_bytes(path, stream.tell() + 2 * block.get_total_buffer_size() + 65536)
                    schema = schema or block.schema
                    block_schema = self.spec.schema or block.schema
                    if block.schema != block_schema:
                        if set(block.schema.names) == set(block_schema.names):
                            # Column order is supplied by the declared schema;
                            # row dictionaries may have canonicalized key order.
                            block = block.select(block_schema.names)
                        retained = block.get_total_buffer_size()
                        from demiflow.execution.contracts.record_codec import cast_allocation as _cast_allocation
                        allocation = _cast_allocation(
                            pa.Table.from_batches([block]) if isinstance(block, pa.RecordBatch) else block, block_schema)
                        if retained + allocation > self.run.limits['max_batch_bytes']:
                            raise RecoveryContractError('Lance cast coexistence exceeds batch budget before allocation')
                        block = block.cast(block_schema)
                        if retained + block.get_total_buffer_size() > self.run.limits['max_batch_bytes']:
                            raise RecoveryContractError('Lance cast coexistence exceeds batch budget')
                    if append_schema is not None:
                        from demiflow.lance.storage import require_compatible_schema
                        require_compatible_schema(block.schema, append_schema)
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
                        from demiflow.errors import InvalidLanceRequest
                        raise InvalidLanceRequest("Empty Lance output requires an explicit schema")
                    writer = pa.ipc.new_stream(stream, schema)
                    writer.close()
                stream.flush()
                os.fsync(stream.fileno())
                self.run.reserve_artifact_bytes(path, stream.tell())
        except BaseException as exc:
            path.unlink(missing_ok=True)
            from demiflow.errors import InvalidLanceRequest
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

    def prepare_publication(self, blocks, context, publication):
        """A pinned merge already owns durable, authenticated terminal segments."""
        if self.spec.mode != 'merge' or self.spec.schema is not None:
            raise RecoveryContractError('Only an uncast merge can consume its upstream publication')
        target = inspect_lance(self.spec.uri, None, self.spec.storage_options)
        if self.spec.expected_version is not None and target.resolved_version != self.spec.expected_version:
            raise LanceWriteConflict('Lance CAS baseline changed')
        rows = size = 0
        schema = None
        for block in blocks:
            self.run.check_stop()
            rows += block.num_rows
            size += block.nbytes
            if rows > self.run.limits['max_output_rows']:
                raise ExecutionBudgetExceeded('max_output_rows', 'Merge publication exceeds the row budget')
            if size > self.run.limits['max_output_bytes']:
                raise ExecutionBudgetExceeded('max_output_bytes', 'Merge publication exceeds the byte budget')
            if schema is not None and schema != block.schema:
                raise RecoveryContractError('Merge publication schema changed')
            schema = block.schema
        self.run.reserve('output_rows', rows, ceiling=self.run.limits['max_output_rows'],
            reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                     'component': 'output_rows'}))
        self.run.reserve_output('row_bytes', size,
            reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                     'component': 'row_bytes'}))
        self.root.mkdir(parents=True, exist_ok=True)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return dict(publication=publication, rows=rows, bytes=size,
                    schema_hash=schema_hash(schema if schema is not None else target.schema),
                    base_version=target.resolved_version)

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
        if 'publication' not in prepared:
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

    def commit(self, prepared, context, *, blocks=None, batch_rows=None):
        import pyarrow as pa
        from demiflow.lance.write import write_lance
        if ('publication' in prepared) != (blocks is not None):
            raise RecoveryContractError('Prepared input differs from the native merge publication')
        path = self._path(prepared) if blocks is None else None
        effective = self.spec
        if self.spec.mode in {"append", "overwrite"}:
            effective = replace(self.spec, mode="create" if prepared["base_version"] is None else self.spec.mode,
                                expected_version=prepared["base_version"])
        if self.spec.mode == 'merge':
            effective = replace(self.spec, expected_version=prepared['base_version'])
        if blocks is not None:
            limits = dict(self.run.limits, batch_rows=batch_rows)
            receipt = write_lance(effective, blocks, limits=limits, check_stop=self.run.check_stop,
                                  _source_row_count=prepared['rows'])
        elif (self.spec.mode == 'append' and self.spec.expected_version is None
                and prepared['base_version'] is not None and prepared['rows'] == 0):
            # Preserve default empty-append semantics using a native empty CAS
            # transaction. Explicit compare-and-append still rejects empty input.
            from demiflow.lance.storage import require_lance, require_compatible_schema
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
                    operation_id=context.commit_id if effective.mode != "merge" else None,
                    _source_row_count=(prepared['rows'] if effective.mode == 'merge' and self.exact_merge_source else None))
        if receipt.status != "committed":
            raise LanceWriteError(receipt)
        result = self._receipt(receipt, context)
        raw = document(result, 65536)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id, size=len(raw))
        atomic_bytes(self._marker(context), raw)
        return result

    def abort(self, prepared, context):
        if self.inspect(prepared, context) is None and 'publication' not in prepared:
            self._path(prepared).unlink(missing_ok=True)


def spec_from_identity(value):
    from demiflow.lance.model import LanceWriteSpec
    import pyarrow as pa
    if value['type'] != 'demiflow.lance.model.LanceWriteSpec':
        raise RecoveryContractError('Expected native Lance write declaration')
    fields = dict(value['fields'])
    schema = fields.get('schema')
    if schema is not None:
        fields['schema'] = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(schema['arrow_schema'])))
    return LanceWriteSpec(**fields)


def _validate_metadata(actual, version):
    if (type(actual) is not dict or type(actual.get('version')) is not int or actual['version'] != version
            or type(actual.get('schema_hash')) is not str or not actual['schema_hash']
            or type(actual.get('schema_identity')) is not str or not actual['schema_identity']
            or type(actual.get('row_count')) is not int or actual['row_count'] < 0):
        raise PublicationMismatch('Invalid fixed-version metadata response')

def verify_target_write_receipt(parsed, declaration, intent, *, expected_uri,
                                require_current_head, deadline, metadata_reader, check_deadline):
    """Verify this target's original receipt; the caller owns historical selection."""
    from demiflow.lance.preview import snapshot_metadata
    if declaration is not None:
        if declaration.get('kind') != 'write_table':
            raise PublicationMismatch('Output is not an original native write action')
        request_hash = spec_from_identity(declaration['sink']).content_hash
        if intent is not None and intent.get('request_hash') != request_hash:
            raise PublicationMismatch('Write observation differs from original declaration')
    elif intent is not None:
        request_hash = intent['request_hash']
    else:
        raise PublicationMismatch('Write receipt has no original request identity')
    if parsed.uri != expected_uri or parsed.request_hash != request_hash:
        raise PublicationMismatch('Write receipt identity differs')
    check_deadline(deadline)
    actual = (metadata_reader or snapshot_metadata)(expected_uri, parsed.committed_version)
    _validate_metadata(actual, parsed.committed_version)
    check_deadline(deadline)
    if actual['schema_hash'] != parsed.schema_hash or actual['row_count'] != parsed.written_rows:
        raise PublicationMismatch('Committed table differs from receipt')
    if require_current_head:
        if inspect_lance(expected_uri).resolved_version != parsed.committed_version:
            raise PublicationMismatch('Output head differs from owned version')
        if intent is None:
            raise PublicationMismatch('Committed write has no original intent')
        dataset = open_lance_dataset(expected_uri, parsed.committed_version, ())
        transaction = dataset.read_transaction(parsed.committed_version)
        if transaction.transaction_properties.get('__lance_commit_message') != 'demiflow-lance:' + intent['operation_id']:
            raise PublicationMismatch('Output operation marker differs')
    return actual


def verify_target_stage(*, expected_uri, version, expected_schema_identity,
                        require_current_head, deadline, metadata_reader, check_deadline):
    """Verify the fixed target version selected by the historical reader."""
    from demiflow.lance.preview import snapshot_metadata
    check_deadline(deadline)
    actual = (metadata_reader or snapshot_metadata)(expected_uri, version)
    _validate_metadata(actual, version)
    check_deadline(deadline)
    if actual['schema_identity'] != expected_schema_identity:
        raise PublicationMismatch('Committed stage schema differs')
    if require_current_head and inspect_lance(expected_uri).resolved_version != version:
        raise PublicationMismatch('Stage head differs from owned version')
    return actual


def inspect_prepared_write(native_action, spec, prepared, *, make_context):
    """Recreate the original target/composite, then inspect with the original context."""
    sink = LanceSink(native_action, spec)
    composite = type(prepared) is dict and {'objects', 'rows'} <= set(prepared)
    if composite:
        from demiflow.execution.outputs.object_sink import ObjectRowsSink
        sink = ObjectRowsSink.for_inspection(native_action, sink, prepared)
    receipt = sink.inspect(prepared, make_context())
    return receipt


class _BatchSink:
    """Native prepared table publication, or verified reference-only receipt."""
    sink_id='lance'
    def __init__(self, scope, declaration):
        self.scope,self.declaration=scope,declaration
        spec=declaration['spec']
        if spec is not None:
            from demiflow.execution.outputs.lance_sink import spec_from_identity
            self.native=LanceSink(scope,spec_from_identity(spec))
            self._marker=self.native._marker  # Original coordinator releases its atomic marker staging grant.
        else:self.native=None

    def prepare(self, blocks, context):
        return {'native':self.native.prepare(blocks,context) if self.native else None,
                'reference':self.declaration['reference'],'request_key':self.declaration['request_key']}

    def inspect(self, prepared, context):
        if prepared['request_key']!=self.declaration['request_key']:
            raise DataflowError('CORRUPT_ARTIFACT','Stream prepared request differs')
        if prepared['reference'] != self.declaration['reference']:
            raise DataflowError('CORRUPT_ARTIFACT','Stream prepared reference differs from its declaration')
        if (prepared['native'] is None) != (self.native is None):
            raise DataflowError('CORRUPT_ARTIFACT','Stream prepared native sink differs from its declaration')
        if self.native:
            receipt=self.native.inspect(prepared['native'],context)
            if receipt is None:return None
            version=receipt['data']['committed_version']
            reference={'uri':self.native.spec.uri,'version':version}
            native=receipt['data']
        else:
            reference=prepared['reference'];native=None
            if reference is not None:
                from demiflow.lance.storage import open_lance_dataset
                from demiflow.lance.schema import schema_identity
                table=open_lance_dataset(reference['uri'],reference['version'],None)
                if schema_identity(table.schema)!=self.declaration['schema_identity']:
                    raise DataflowError('CORRUPT_ARTIFACT','Stream saved reference schema differs')
        return {'commit_id':context.commit_id,'sink_id':'lance','status':'committed',
                'data':{'reference':reference,'native':native,'request_key':self.declaration['request_key']}}

    def commit(self, prepared, context):
        if self.native:self.native.commit(prepared['native'],context)
        return self.inspect(prepared,context)

    def abort(self, prepared, context):
        if self.native:self.native.abort(prepared['native'],context)


def create_target_absent_binding(run_id, action_id, declaration, intent):
    """Derive a create admission identity from native facts, without storage IO."""
    from demiflow.errors import DemiflowError
    from demiflow.execution.outputs.lance_sink import spec_from_identity
    if (type(declaration) is not dict or declaration.get('kind') != 'write_table'
            or type(intent) is not dict or intent.get('kind') != 'write_table'):
        return None
    try:
        spec = spec_from_identity(declaration['sink'])
        if (spec.mode != 'create' or spec.expected_version is not None
                or intent.get('spec') != spec.to_dict()
                or intent.get('request_hash') != spec.content_hash
                or intent.get('operation_id') != identity([run_id, action_id, 1])):
            return None
    except (KeyError, TypeError, ValueError, DemiflowError, RecoveryContractError):
        return None
    return dict(kind='create-target-absent', action_id=action_id,
                operation_id=intent['operation_id'], request_hash=spec.content_hash,
                uri=spec.uri, mode='create')
