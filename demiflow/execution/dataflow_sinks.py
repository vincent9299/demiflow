"""Durable sink assembly and bounded staging for the unified dataflow runtime (D).

Worker-side ``prepare_sink_partition`` only stages owned, content-addressed data
and a small reconcilable manifest; it holds no global publication right. The
driver-side ``make_sink_adapter`` assembles a native four-method durable sink and
``combine_prepared_sinks`` folds per-partition manifests into one bounded combined
manifest without pulling row data to the driver. Publication itself flows through
the single native ``CommitCoordinator`` (D004); ``ack_source_claims`` only verifies
the native finalize fact and never issues a second ACK (C08/C16).
"""
from __future__ import annotations

import json
from collections.abc import Mapping

from ..data.datasink import Datasink, WriteContext
from .contracts.dataflow import (
    ArtifactRef, DriverServices, KernelSpec, Record, RecoveryImage, RuntimeServices,
)
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import (
    RecoveryContractError, UnresolvedCommit, document, identifier, identity,
)
from .run_context import RunAction
from .state_artifacts import atomic_bytes

RECORDS_MEDIA = "application/x-demiflow-records-v3"
MANIFEST_MEDIA = "application/x-demiflow-sink-manifest-v3"
PUBLICATION_MEDIA = "application/x-demiflow-sink-publication-v3"
_MANIFEST_SCHEMA = "demiflow.dataflow_sink_manifest.v3"
_COMBINED_SCHEMA = "demiflow.dataflow_sink_combined.v3"
_PUBLICATION_SCHEMA = "demiflow.dataflow_sink_publication.v3"
_DURABLE_METHODS = ("prepare", "commit", "inspect", "abort")


def _artifact_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes,
            "store_id": ref.store_id, "key": ref.key, "media_type": ref.media_type}


def _artifact_from_json(value) -> ArtifactRef:
    if type(value) is not dict or set(value) != {
            "digest", "size_bytes", "store_id", "key", "media_type"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid artifact reference in sink manifest")
    return ArtifactRef(digest=value["digest"], size_bytes=value["size_bytes"],
                       store_id=value["store_id"], key=value["key"],
                       media_type=value["media_type"])


def _validate_ref(ref, *, what: str) -> None:
    if not isinstance(ref, ArtifactRef):
        raise DataflowError("INVALID_CONTRACT", what + " must be a durable ArtifactRef")
    if (type(ref.digest) is not str or len(ref.digest) != 64
            or any(c not in "0123456789abcdef" for c in ref.digest)):
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid digest")
    if type(ref.key) is not str or not ref.key or ".." in ref.key.split("/"):
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid store key")
    if type(ref.size_bytes) is not int or ref.size_bytes < 0:
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid size")


def _require_durable(sink) -> None:
    """A legacy write callback is never proof of a recoverable four-method sink."""
    if not isinstance(sink, Datasink):
        raise DataflowError("INVALID_CONTRACT",
                            "Sink declaration is not a native Datasink; a write callback cannot commit durably")
    for method in _DURABLE_METHODS:
        impl = getattr(type(sink), method, None)
        if not callable(impl) or impl is getattr(Datasink, method):
            raise DataflowError("INVALID_CONTRACT",
                                "Sink lacks native durable " + method + "; legacy write is not recoverable")


def _read_json(store, ref: ArtifactRef, *, max_bytes: int, media_type: str):
    _validate_ref(ref, what="sink artifact")
    if ref.media_type != media_type:
        raise DataflowError("INVALID_CONTRACT", "Sink artifact media type differs")
    raw = b"".join(store.read(ref, max_bytes=max_bytes))
    if len(raw) > max_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "Sink artifact exceeds read budget")
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "Sink artifact is not valid JSON") from exc


# -- D008 -----------------------------------------------------------------
def make_sink_adapter(spec: KernelSpec, *, services: DriverServices) -> Datasink:
    if not isinstance(spec, KernelSpec):
        raise DataflowError("INVALID_CONTRACT", "make_sink_adapter requires a KernelSpec")
    config = spec.configuration
    if not isinstance(config, Mapping):
        raise DataflowError("INVALID_CONTRACT", "Sink kernel configuration must be a mapping")
    candidate = config.get("sink", config.get("datasink"))
    if candidate is not None:
        # A real native/custom Datasink keeps its own recover/commit capability.
        _require_durable(candidate)
        return candidate
    sink_id = config.get("sink_id") or spec.kind
    if type(sink_id) is not str:
        raise DataflowError("INVALID_CONTRACT", "Sink id must be a string")
    identifier(sink_id)
    from .contracts.dataflow import thaw_record_value
    pure = thaw_record_value({k: v for k, v in config.items() if k not in {"sink", "datasink"}})
    try:
        document(pure)
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT",
                            "Sink configuration carries a live handle; declare a durable migration: " + str(exc)) from exc
    return _DataflowArtifactSink(sink_id=sink_id, store=services.store,
                                 action=services.action, bounds=services.bounds,
                                 configuration=pure)


class _DataflowArtifactSink(Datasink):
    """The default v3 durable terminal sink: confirmed output is a store artifact.

    Publication state uses the native run-directory receipt marker pattern (a
    deterministic ``commit_id`` file), so inspect can distinguish prepared from
    committed without a second ledger. Data bytes live in the shared store; the
    marker only proves this commit published the staged publication descriptor.
    """

    def __init__(self, *, sink_id, store, action, bounds, configuration):
        self._sink_id = sink_id
        self._store = store
        self._action = action
        self._bounds = bounds
        self._configuration = configuration
        self._root = action.context.directory / "materializations" / "dataflow-sinks" / sink_id

    def recovery_identity(self):
        return {"format": _PUBLICATION_SCHEMA, "sink_id": self._sink_id,
                "configuration": self._configuration}

    def _marker(self, context: WriteContext):
        return self._root / ".receipts" / (identifier(context.commit_id) + ".json")

    def _publication_bytes(self, manifest, context):
        return document({"schema": _PUBLICATION_SCHEMA, "sink_id": self._sink_id,
                         "commit_id": context.commit_id, "manifest": manifest},
                        self._bounds.max_control_bytes)

    def prepare(self, blocks, context: WriteContext):
        if type(blocks) is not dict or blocks.get("schema") != _COMBINED_SCHEMA:
            raise DataflowError("INVALID_CONTRACT", "Sink prepare requires a combined manifest")
        if blocks.get("sink_id") != self._sink_id:
            raise DataflowError("INVALID_CONTRACT", "Combined manifest sink id differs")
        raw = self._publication_bytes(blocks, context)
        pub_ref = self._store.put(iter([raw]), max_bytes=self._bounds.max_control_bytes,
                                  media_type=PUBLICATION_MEDIA)
        for partition in blocks["partitions"]:
            data_ref = _artifact_from_json(partition["data_ref"])
            from .dataflow_segments import tree_refs
            for ref in tree_refs(self._store, data_ref, self._bounds):
                # Authenticate the entire prefix before publication, including
                # old leaves. The original GC traces this publication's typed
                # children, so each cut needs only its publication owner pin.
                if not self._store.inspect(ref):
                    raise DataflowError('UNKNOWN_EFFECT', 'Cannot pin an absent artifact')
        self._store.retain(pub_ref, owner=context.commit_id)
        self._action.context.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return {"ref": _artifact_json(pub_ref), "sink_id": self._sink_id,
                "rows": blocks["total_rows"], "bytes": blocks["total_bytes"],
                "manifest": blocks}

    def _expected_receipt(self, prepared, context):
        # Native validate_receipt requires exactly {commit_id, sink_id, status, data}.
        return {"commit_id": context.commit_id, "sink_id": self._sink_id,
                "status": "committed",
                "data": {"ref": prepared["ref"], "rows": prepared["rows"],
                         "bytes": prepared["bytes"]}}

    def inspect(self, prepared, context: WriteContext):
        if type(prepared) is not dict or "ref" not in prepared:
            raise DataflowError("CORRUPT_ARTIFACT", "Invalid prepared sink descriptor")
        pub_ref = _artifact_from_json(prepared["ref"])
        if not self._store.inspect(pub_ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Prepared sink publication artifact disappeared")
        marker = self._marker(context)
        try:
            if any(p.is_symlink() for p in (marker, *marker.parents)):
                raise UnresolvedCommit("Sink receipt path contains a symlink")
            raw = marker.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise UnresolvedCommit("Sink receipt is inaccessible") from exc
        try:
            value = json.loads(raw)
        except ValueError as exc:
            raise UnresolvedCommit("Sink receipt is corrupt") from exc
        if value != self._expected_receipt(prepared, context):
            raise UnresolvedCommit("Sink receipt identity differs")
        return value

    def commit(self, prepared, context: WriteContext):
        existing = self.inspect(prepared, context)
        if existing is not None:
            return existing
        receipt = self._expected_receipt(prepared, context)
        atomic_bytes(self._marker(context), document(receipt))
        return receipt

    def abort(self, prepared, context: WriteContext):
        if type(prepared) is not dict or "ref" not in prepared:
            return
        if self.inspect(prepared, context) is not None:
            return  # never undo committed data
        pub_ref = _artifact_from_json(prepared["ref"])
        self._store.release(pub_ref, owner=context.commit_id)
        for partition in prepared.get("manifest", {}).get("partitions", ()):
            from .dataflow_segments import tree_refs
            for ref in tree_refs(self._store, _artifact_from_json(partition["data_ref"]), self._bounds):
                self._store.release(ref, owner=context.commit_id)


# -- D009 -----------------------------------------------------------------
def _partition_id(spec: KernelSpec, services: RuntimeServices) -> str:
    lease = services.assignment.lease
    configured = spec.configuration.get("partition_id")
    if configured is not None:
        if configured not in lease.partition_ids:
            raise DataflowError("FENCED", "Sink partition is not in the worker lease")
        return configured
    if len(lease.partition_ids) == 1:
        return lease.partition_ids[0]
    raise DataflowError("INVALID_CONTRACT", "Sink partition id is ambiguous for this lease")


def _encode_record(record: Record, bounds, *, services=None, retained=0) -> bytes:
    from .dataflow_record_frames import encode_frame
    # Preserve the original no-newline helper contract and exact JSON bytes.
    return encode_frame(record, bounds, services=services, retained=retained)[:-1]


def _manifest_non_mapping_values(manifest):
    flag = manifest.get('non_mapping_values', False)
    if type(flag) is not bool:
        raise DataflowError('CORRUPT_ARTIFACT', 'Sink non_mapping_values must be a boolean')
    if flag and (not manifest['rows'] or manifest['columns'] is not None
                 or manifest.get('schema_ref') is not None):
        raise DataflowError('CORRUPT_ARTIFACT', 'Sink non_mapping_values contradicts its table metadata')
    return flag


def prepare_sink_partition(records, *, spec: KernelSpec, services: RuntimeServices) -> ArtifactRef:
    if not isinstance(spec, KernelSpec):
        raise DataflowError("INVALID_CONTRACT", "prepare_sink_partition requires a KernelSpec")
    bounds = services.bounds
    store = services.store
    partition_id = _partition_id(spec, services)
    sink_id = spec.configuration.get("sink_id") or spec.kind
    identifier(sink_id)
    rows, total, columns = 0, 0, None
    non_mapping_values = False
    chunks = []
    for record in records:
        services.check_stop()
        if not isinstance(record, Record):
            raise DataflowError("INVALID_CONTRACT", "Sink staging requires Record values")
        encoded = _encode_record(record, bounds, services=services, retained=total + 64 * len(chunks))
        rows += 1
        total += len(encoded) + 1
        if rows > bounds.max_output_rows:
            raise DataflowError("BUDGET_EXCEEDED", "Sink partition row count exceeds budget")
        if total > bounds.max_batch_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "Sink partition byte count exceeds budget")
        if record.inline is not None and not isinstance(record.inline, Mapping):
            non_mapping_values = True
            columns = None
        elif not non_mapping_values and columns is None and isinstance(record.inline, Mapping):
            columns = sorted(record.inline)
        chunks.append(encoded + b"\n")
    data_ref = store.put(iter(chunks), max_bytes=bounds.max_batch_bytes, media_type=RECORDS_MEDIA)
    manifest = {"schema": _MANIFEST_SCHEMA, "sink_id": sink_id, "partition_id": partition_id,
                "rows": rows, "bytes": total, "columns": columns,
                "data_ref": _artifact_json(data_ref)}
    if non_mapping_values:
        manifest['non_mapping_values'] = True
    raw = document(manifest, bounds.max_control_bytes)
    return store.put(iter([raw]), max_bytes=bounds.max_control_bytes, media_type=MANIFEST_MEDIA)


# -- D010 -----------------------------------------------------------------
def combine_prepared_sinks(refs, *, sink_id: str, services: DriverServices):
    identifier(sink_id)
    bounds = services.bounds
    store = services.store
    if len(refs) > bounds.max_partitions:
        raise DataflowError("BUDGET_EXCEEDED", "Prepared sink partition count exceeds budget")
    partitions, seen = {}, set()
    total_rows, total_bytes, columns = 0, 0, None
    from .record_schema import RecordSchemas
    schemas = RecordSchemas(services)
    schema_ref, declared_schema = None, None
    non_mapping_values = column_drift = schema_drift = False
    for ref in refs:
        manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=MANIFEST_MEDIA)
        if type(manifest) is not dict or manifest.get("schema") != _MANIFEST_SCHEMA:
            raise DataflowError("CORRUPT_ARTIFACT", "Invalid prepared sink manifest")
        if manifest.get("sink_id") != sink_id:
            raise DataflowError("INVALID_CONTRACT", "Prepared manifest belongs to a different sink")
        non_mapping_values |= _manifest_non_mapping_values(manifest)
        partition_id = manifest["partition_id"]
        if partition_id in seen:
            raise DataflowError("INVALID_CONTRACT",
                                "Duplicate prepared partition; two workers submitted one partition")
        seen.add(partition_id)
        if manifest.get('schema_ref') is not None:
            current_ref = _artifact_from_json(manifest['schema_ref'])
            current = schemas.read(current_ref)
            if declared_schema is not None and not declared_schema.equals(current, check_metadata=True):
                schema_drift = True
            schema_ref, declared_schema = current_ref, current
        manifest_columns = manifest["columns"]
        if manifest_columns is not None:
            if columns is None:
                columns = manifest_columns
            elif manifest_columns != columns:
                column_drift = True
        entries = manifest.get('partitions', [manifest])
        if not isinstance(entries, list) or len(entries) + len(partitions) > bounds.max_partitions:
            raise DataflowError('BUDGET_EXCEEDED', 'logical sink partition count exceeds bound')
        if sum(p['rows'] for p in entries) != manifest['rows'] or sum(p['bytes'] for p in entries) != manifest['bytes']:
            raise DataflowError('CORRUPT_ARTIFACT', 'logical sink partition totals disagree')
        for part in entries:
            pid = part['partition_id']
            if pid in partitions:
                raise DataflowError('INVALID_CONTRACT', 'duplicate logical sink partition')
            _artifact_from_json(part['data_ref'])
            entry = {'partition_id': pid, 'rows': part['rows'], 'bytes': part['bytes'],
                     'data_ref': part['data_ref'], 'scalar': manifest.get('scalar')}
            if 'block_partition' in part:
                from .contracts.dataflow import BlockPartition
                tag = BlockPartition(**part['block_partition'])
                if tag.count > bounds.max_partitions:
                    raise DataflowError('BUDGET_EXCEEDED', 'logical partition count exceeds bound')
                entry['block_partition'] = dict(part['block_partition'])
            partitions[pid] = entry
        total_rows += manifest["rows"]
        total_bytes += manifest["bytes"]
    if non_mapping_values:
        columns = schema_ref = None
    elif schema_drift:
        raise DataflowError('SOURCE_CHANGED', 'Prepared partition Arrow schema drifted')
    elif column_drift:
        raise DataflowError('SOURCE_CHANGED', 'Prepared partition schema drifted')
    def order(part):
        tag = part.get('block_partition')
        return (tag['occurrence_id'], tag['index']) if tag else (part['partition_id'], -1)
    combined = {"schema": _COMBINED_SCHEMA, "sink_id": sink_id,
                "partitions": sorted(partitions.values(), key=order),
                "total_rows": total_rows, "total_bytes": total_bytes, "columns": columns}
    if schema_ref is not None:
        combined['schema_ref'] = _artifact_json(schema_ref)
    document(combined, bounds.max_control_bytes)
    return combined


# -- D011 -----------------------------------------------------------------
def ack_source_claims(image: RecoveryImage, *, action: RunAction) -> None:
    """Verify the native finalize fact is acked; never issue a second ACK (C16).

    The atomic queue settlement belongs to the native C16 service. This only
    reconciles the image against the single native commit fact and advances the
    confirmed frontier read-only, so a lost response retried here is idempotent.
    """
    if not isinstance(image, RecoveryImage):
        raise DataflowError("INVALID_CONTRACT", "ack_source_claims requires a RecoveryImage")
    from .dataflow_checkpoint import source_vector_position
    journal = action.context.journal
    fact = journal.execution_commit(image.commit_id)
    if fact is None or fact["action_id"] != action.action_id:
        raise DataflowError("INVALID_CONTRACT", "Recovery image is not a commit of this action")
    if fact["status"] != "acked":
        raise DataflowError("UNKNOWN_EFFECT", "Native finalize fact is not acked; cannot advance frontier")
    for sink_id in image.confirmed_sinks:
        if sink_id not in fact["receipts"]:
            raise DataflowError("UNKNOWN_EFFECT", "Confirmed sink has no native receipt; not ACKed")
    position = source_vector_position(image, action_id=action.action_id)
    if fact["intent"]["position"] != position.to_dict():
        raise DataflowError("SOURCE_CHANGED", "Confirmed source frontier differs from the committed position")

class NativeTerminalSink(_DataflowArtifactSink):
    """Bind a published result manifest and a native destination to one commit.

    Staging streams bounded Arrow batches from worker-produced data. Native
    prepare/inspect/commit/abort retain all destination CAS and recovery rules.
    """
    def __init__(self, *, native, batch_rows=256, native_limits=None, admit_publication=None, **kwargs):
        super().__init__(**kwargs)
        self.native = native
        self._batch_rows = batch_rows
        self._native_limits = dict(native_limits or {})
        self._admit_publication = admit_publication
        from .dataflow_publication_budget import workspace_bytes, phased
        self._publication_workspace = workspace_bytes(self._configuration)
        self._publication_metadata = phased(self._configuration)
        if self._publication_workspace:
            from dataclasses import replace
            self._bounds = replace(self._bounds,
                max_resident_bytes=min(self._bounds.max_resident_bytes, self._publication_workspace))

    def _native_scope(self):
        from .payload_limits import payload_scope
        limits = dict(self._native_limits, memory_bytes=self._bounds.max_resident_bytes,
                      max_row_bytes=self._bounds.max_row_bytes,
                      max_batch_bytes=self._bounds.max_batch_bytes, batch_rows=self._batch_rows)
        return payload_scope(self._action.context, None, owner_limits=limits)

    def _blocks(self, manifest):
        import pyarrow as pa
        from .exchange.dataflow_bridge import _decode_payload
        from .dataflow_record_frames import (decode_frame, decode_document, line_limit, frame_stream,
            read_chunk_bytes, admit_workspace, metadata_bytes, read_document, raw_workspace,
            _typed_shape, _json_shape)
        from .contracts.dataflow import thaw_record_value
        from .record_schema import RecordSchemas
        from .arrow_payload import ArrowBatchCache, is_arrow_record
        from ..data.runtime_contract import check_size
        schemas = RecordSchemas(store=self._store, bounds=self._bounds)
        arrow = ArrowBatchCache(self._store, self._bounds)
        pending = stream = None
        batch, size = [], 0
        batch_resident = check_size(batch, self._bounds.max_resident_bytes)
        schema_ref, schema = None, None
        control_bytes = metadata_bytes(manifest) if self._publication_workspace else 0
        index_bytes = held_record_bytes = schema_bytes = last_output_bytes = 0

        def cached_bytes():
            if arrow.batch is None:
                return 0
            return arrow.ref.size_bytes + arrow.batch.get_total_buffer_size() + metadata_bytes(arrow.header)

        def held():
            return (control_bytes + index_bytes + batch_resident + held_record_bytes
                    + cached_bytes() + schema_bytes + last_output_bytes)

        def call(function, *args, _frame_retained=0, **kwargs):
            retained = held() if self._publication_workspace else size
            return function(*args, retained=retained + _frame_retained, **kwargs)

        def index_call(function, *args, _frame_retained=0, **kwargs):
            return call(function, *args, _frame_retained=_frame_retained +
                        (stream.buffered_bytes if stream is not None else 0), **kwargs)

        def retain_index(value):
            nonlocal index_bytes
            index_bytes = value

        def table():
            from .batch_conversion import admit_batch_conversion, native_arrow_table, native_writer_workspace
            writer_window = []
            reserved = (held() - batch_resident + stream.buffered_bytes + (0 if self._publication_metadata else 65536)
                        if self._publication_workspace else 0)
            admit_batch_conversion(batch, 'pyarrow', schema=schema, bounds=self._bounds,
                reserved_bytes=reserved, resident_workspace=bool(self._publication_workspace),
                writer_copies=2 if self._publication_workspace else 0,
                writer_metadata=self._publication_metadata,
                observe_writer_workspace=writer_window.append if self._publication_metadata else None)
            result = native_arrow_table(batch, schema)
            if self._publication_workspace:
                if size > self._bounds.max_batch_bytes or result.nbytes > self._bounds.max_batch_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'native sink payload exceeds max_batch_bytes')
                overhead = writer_window[0] if self._publication_metadata else 65536
                if self._publication_metadata and native_writer_workspace(result.schema, self._bounds) > overhead:
                    raise DataflowError('BUDGET_EXCEEDED', 'native writer schema exceeds its constructor admission')
                call(admit_workspace, 3 * result.get_total_buffer_size() + overhead,
                     self._bounds, _frame_retained=stream.buffered_bytes)
            elif size + result.nbytes > self._bounds.max_batch_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'native sink conversion coexistence exceeds max_batch_bytes')
            return result

        def arrow_table(value):
            if self._publication_workspace:
                # The cache already owns the retained parent. Writer copies
                # and the caller's previous yielded batch share this grant.
                from .batch_conversion import native_writer_workspace
                overhead = native_writer_workspace(value.schema, self._bounds) if self._publication_metadata else 65536
                if self._publication_metadata:
                    overhead += sum(64*len(chunk.buffers()) for column in value.columns for chunk in column.chunks)
                call(admit_workspace, 2 * value.get_total_buffer_size() + overhead,
                     self._bounds, _frame_retained=stream.buffered_bytes)
                if value.nbytes > self._bounds.max_batch_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'native Arrow payload exceeds max_batch_bytes')
            return value

        try:
            for partition in manifest['partitions']:
                ref = _artifact_from_json(partition['data_ref'])
                from .dataflow_segments import block_chunks
                stream = frame_stream(block_chunks(self._store, ref, self._bounds,
                    chunk_bytes=read_chunk_bytes(self._bounds),
                    frame_call=index_call if self._publication_workspace else None,
                    on_retained=retain_index if self._publication_workspace else None), self._bounds,
                    check_workspace=(lambda amount: call(admit_workspace, amount, self._bounds))
                                    if self._publication_workspace else None)
                try:
                    while True:
                        line = stream.readline(line_limit(self._bounds))
                        if not line:
                            break
                        raw, item = call(decode_frame, line, self._bounds,
                                         _frame_retained=stream.buffered_bytes)
                        del line
                        record = call(decode_document, raw, record_id=item['record_id'],
                            sequence=item['sequence'], bounds=self._bounds,
                            _frame_retained=stream.buffered_bytes)
                        del raw, item
                        held_record_bytes = (_typed_shape(record.inline, self._bounds.max_row_bytes)[1]
                            if record.inline is not None else 0)
                        held_record_bytes += (_json_shape(record.source_position, self._bounds.max_control_bytes)[1]
                            + metadata_bytes((record.payload_ref, record.schema_ref, record.record_id)) + 256)
                        if is_arrow_record(record):
                            if batch:
                                output = table()
                                yield output
                                last_output_bytes = output.get_total_buffer_size()
                                del output
                                batch, size = [], 0
                                batch_resident = check_size(batch, self._bounds.max_resident_bytes)
                            if pending is not None and (record.payload_ref != pending[0]
                                    or record.source_position['row_offset'] != pending[4]
                                    or pending[3] >= self._batch_rows
                                    or pending[5] + record.encoded_bytes > self._bounds.max_batch_bytes // 2):
                                output = arrow_table(pa.Table.from_batches([pending[1].slice(pending[2], pending[3])]))
                                yield output
                                last_output_bytes = output.get_total_buffer_size()
                                del output
                                pending = None
                            if record.payload_ref != arrow.ref:
                                arrow.close()
                                if self._publication_workspace:
                                    # IPC is uncompressed and at most the original
                                    # Arrow window; admit raw/header/object peaks
                                    # before the cache reads or opens the batch.
                                    call(admit_workspace, 12 * record.payload_ref.size_bytes + 16384,
                                         self._bounds, _frame_retained=stream.buffered_bytes)
                            native_batch, index = arrow.locate(record)
                            if pending is None:
                                pending = [record.payload_ref, native_batch, index, 0, record.source_position['row_offset'], 0]
                            pending[3] += 1
                            pending[4] += 1
                            pending[5] += record.encoded_bytes
                            del native_batch, index, record
                            held_record_bytes = 0
                            continue
                        if pending is not None:
                            output = arrow_table(pa.Table.from_batches([pending[1].slice(pending[2], pending[3])]))
                            yield output
                            last_output_bytes = output.get_total_buffer_size()
                            del output
                            pending = None
                            arrow.close()
                        # Flush the previous window before constructing the
                        # next mutable row; its record already has an admitted
                        # immutable owner included in held_record_bytes.
                        if batch and (record.schema_ref != schema_ref or len(batch) >= self._batch_rows
                                or size + record.encoded_bytes > self._bounds.max_batch_bytes // 2):
                            output = table()
                            yield output
                            last_output_bytes = output.get_total_buffer_size()
                            del output
                            batch, size = [], 0
                            batch_resident = check_size(batch, self._bounds.max_resident_bytes)
                        if record.inline is None:
                            raw = call(read_document, record.payload_ref, self._store, self._bounds,
                                       _maximum=self._bounds.max_row_bytes,
                                       _frame_retained=stream.buffered_bytes)
                            call(admit_workspace, raw_workspace(raw), self._bounds,
                                 _frame_retained=stream.buffered_bytes)
                            row = _decode_payload(raw, max_bytes=self._bounds.max_row_bytes)
                            del raw
                        else:
                            call(admit_workspace, 2 * held_record_bytes, self._bounds,
                                 _frame_retained=stream.buffered_bytes)
                            row = thaw_record_value(record.inline)
                        schema_ref = record.schema_ref
                        if schema_ref is not None and schemas._input[0] != schema_ref:
                            call(admit_workspace, 8 * schema_ref.size_bytes + 512, self._bounds,
                                 _frame_retained=stream.buffered_bytes)
                            schema_bytes = 8 * schema_ref.size_bytes + 512
                        schema = schemas.read(schema_ref)
                        declared = getattr(getattr(self.native, 'spec', None), 'schema', None)
                        if schema is None and declared is not None:
                            extra = set(row) - set(declared.names)
                            if extra:
                                raise ValueError('Lance output has undeclared fields: ' + ', '.join(sorted(extra)))
                            schema = declared
                        row_bytes = check_size(row, self._bounds.max_resident_bytes)
                        call(admit_workspace, row_bytes + 64, self._bounds,
                             _frame_retained=stream.buffered_bytes)
                        batch.append(row)
                        size += record.encoded_bytes
                        batch_resident = check_size(batch, self._bounds.max_resident_bytes)
                        del record, row
                        held_record_bytes = 0
                finally:
                    stream.close()
            if batch:
                yield table()
            if pending is not None:
                yield arrow_table(pa.Table.from_batches([pending[1].slice(pending[2], pending[3])]))
        finally:
            arrow.close()

    def prepare(self, manifest, context):
        result = super().prepare(manifest, context)
        # An intermediate cut commits the durable result prefix, while the
        # external destination publishes exactly once when all inputs finish.
        with self._native_scope():
            if manifest.get('final', True) and self._admit_publication is not None:
                self._admit_publication()
            result['native'] = (self.native.prepare(self._blocks(manifest), context)
                                if manifest.get('final', True) else None)
        return result

    def inspect(self, prepared, context):
        artifact = super().inspect(prepared, context)
        if prepared['native'] is None:
            return artifact
        native = self.native.inspect(prepared['native'], context)
        if native is None and artifact is not None:
            # The exact original rows receipt is written only after native
            # commit returned. It cannot prove that external publication;
            # neither may recovery replay that ambiguous invocation.
            raise UnresolvedCommit('Sink publication returned without matching native inspect evidence')
        if native is None or artifact is None:
            return None
        return {**artifact, 'data': {**artifact['data'], 'native_receipt': native}}

    def commit(self, prepared, context):
        if prepared['native'] is None:
            return super().commit(prepared, context)
        native = self.native.inspect(prepared['native'], context)
        if native is None:
            with self._native_scope():
                native = self.native.commit(prepared['native'], context)
        artifact = super().commit(prepared, context)
        return {**artifact, 'data': {**artifact['data'], 'native_receipt': native}}

    def abort(self, prepared, context):
        if prepared['native'] is not None:
            self.native.abort(prepared['native'], context)
        super().abort(prepared, context)


def make_native_terminal_sink(spec, *, request, services, publication_reserved_bytes=None, admit_publication=None):
    from .dataflow_publication_budget import workspace_bytes
    workspace = workspace_bytes(spec.configuration)
    if workspace:
        control = max(1, min(services.bounds.max_control_bytes, services.bounds.max_resident_bytes // 8))
        if (type(publication_reserved_bytes) is not int
                or workspace + control > publication_reserved_bytes
                or publication_reserved_bytes > services.bounds.max_resident_bytes):
            raise DataflowError('INVALID_CONTRACT', 'native publication workspace lacks sealed residency reservation')
    if request.kind == 'write_lance':
        from .lance_sink import LanceSink
        from ..lance.model import LanceWriteSpec
        declaration = next((p.value for p in request.programs if p.name == 'write_lance'), None)
        if not isinstance(declaration, LanceWriteSpec):
            raise DataflowError('INVALID_CONTRACT', 'write_lance requires its sealed LanceWriteSpec')
        if declaration.expected_version is not None and not services.action.replaying:
            from ..lance.storage import inspect_lance
            from ..errors import LanceWriteConflict
            current = inspect_lance(declaration.uri, None, declaration.storage_options)
            if current.resolved_version != declaration.expected_version:
                raise LanceWriteConflict('Lance CAS baseline changed before source execution')
        # Historical final ACKs are inspected at their original committed
        # version. A later destination head must not reject that replay.
        # Native prepare and commit still enforce the actual publication CAS.
        from ..data.sources import LanceSource
        from ..lance.model import LanceScanSpec
        # The sealed request identifies an exact scan; its already staged
        # output supplies the count. Never rescan source data during commit.
        # Unknown/transformed/filtered streams retain the existing auto mode.
        exact_merge_source = (type(request.source) is LanceSource
            and isinstance(request.source.query, LanceScanSpec)
            and not request.source.query.filter and not request.source.query.projection
            and not request.plan.operations)
        native = LanceSink(services.action, declaration, exact_merge_source=exact_merge_source)
        if declaration.mode == 'create':
            from ..lance.storage import inspect_lance
            from ..errors import LanceResourceNotFound, LanceWriteConflict
            action = services.action
            # Before any model call, retain the same absent-target admission
            # fact used by workbench recovery. Actual prepare still checks CAS.
            if not action.replaying:
                try:
                    inspect_lance(declaration.uri, None, declaration.storage_options)
                except LanceResourceNotFound:
                    from .run_context import create_target_absent_binding
                    view = action.context.journal.execution_view(detail='summary', action_ids=[action.action_id])
                    intent = next((e['value'] for e in view['events'] if e['kind'] == 'intent'), None)
                    if intent is None and action.record['declaration']['kind'] == 'write_table':
                        from .recovery_contract import identity
                        intent = {'kind': 'write_table', 'spec': declaration.to_dict(),
                                  'request_hash': declaration.content_hash,
                                  'operation_id': identity([action.context.ref.run_id, action.action_id, 1])}
                        action.context.journal.action_event(action.action_id, 'intent', intent)
                    binding = create_target_absent_binding(action.context.ref.run_id,
                        action.action_id, action.record['declaration'], intent)
                    if binding is not None:
                        action.context.journal.bind_source(action.context.fence,
                            action.action_id + '-create-target-absent', binding)
                else:
                    raise LanceWriteConflict('Create target already exists')
    elif request.kind == 'write_file':
        from .file_sink import FileSink
        native = FileSink(services.action, request.arguments)
    elif request.kind == 'write_datasink':
        native = request.sink
        from .queue_sinks import QueueResultSink
        if isinstance(native, QueueResultSink):
            # Each task is already atomically settled by the original queue
            # authority before the terminal can publish it. This cut only owns
            # the Dataset result manifest; never submit those tasks twice.
            return _DataflowArtifactSink(sink_id=request.kind, store=services.store,
                action=services.action, bounds=services.bounds, configuration=dict(spec.configuration))
        _require_durable(native)
    else:
        raise DataflowError('INVALID_CONTRACT', 'unknown native terminal adapter')
    batch_rows = min(256, (services.policy.native_limits or {}).get("batch_rows", 256),
                     services.action.context.limits['batch_rows'])
    return NativeTerminalSink(native=native, batch_rows=batch_rows, native_limits=services.policy.native_limits,
        admit_publication=admit_publication,
        sink_id=request.kind, store=services.store,
        action=services.action, bounds=services.bounds, configuration=dict(spec.configuration))


def inspect_native_write_receipt(journal, action_id):
    """Project a v3 native publication without starting code or changing history."""
    from types import SimpleNamespace
    from .commit import CommitCoordinator
    from .lance_sink import LanceSink, spec_from_identity
    from .sealed_plans import _v3_read_store
    from .dataflow_seal import load_dataflow
    binding = journal.recovery_identity()
    action = journal.execution_action(action_id)
    facts = journal.execution_commits(action_id)
    if binding is None or action is None or not facts:
        return None
    fact = facts[-1]
    if fact['prepared'] is None or fact['intent']['sinks'] != ['write_lance']:
        return None
    seal = journal.source_binding(action_id + '-dataflow-sealed')
    if seal is None:
        return None
    store, bounds = _v3_read_store(journal.directory, store_id=seal['physical_ref']['store_id'])
    graph, physical, _ = load_dataflow(journal.directory, action_id, store=store, bounds=bounds)
    context = SimpleNamespace(directory=journal.directory, limits=binding['limits'],
        journal=journal, ref=SimpleNamespace(run_id=binding['run_id']),
        attempt=journal.execution_status()['attempt'], fence='observation-only')
    native_action = RunAction(context, action_id, action['declaration'])
    native = LanceSink(native_action, spec_from_identity(action['declaration']['sink']))
    sink = NativeTerminalSink(native=native, sink_id='write_lance', store=store,
        action=native_action, bounds=physical.bounds, configuration={})
    prepared = fact['prepared']['sinks']['write_lance']
    composite = type(prepared) is dict and {'objects', 'rows'} <= set(prepared)
    if composite:
        from .object_sink import ObjectRowsSink
        sink = ObjectRowsSink.for_inspection(native_action, sink, prepared)
    receipt = sink.inspect(prepared, CommitCoordinator(native_action)._context(fact['commit_id']))
    if receipt is None:
        return None
    data = receipt['data']['rows'] if composite else receipt['data']
    native_receipt = data.get('native_receipt')
    return native_receipt['data'] if native_receipt is not None else None
