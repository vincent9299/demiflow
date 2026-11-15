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
    if type(config) is not dict:
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
    pure = {k: v for k, v in config.items() if k not in {"sink", "datasink"}}
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
            self._store.retain(data_ref, owner=context.commit_id)
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
            self._store.release(_artifact_from_json(partition["data_ref"]), owner=context.commit_id)


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


def _encode_record(record: Record, bounds) -> bytes:
    if record.inline is not None and record.payload_ref is not None:
        raise DataflowError("INVALID_CONTRACT", "Record carries both inline and payload_ref")
    if record.payload_ref is not None:
        _validate_ref(record.payload_ref, what="record payload")
    body = {"record_id": record.record_id, "source_position": dict(record.source_position),
            "sequence": record.sequence,
            "inline": record.inline,
            "payload_ref": _artifact_json(record.payload_ref) if record.payload_ref is not None else None}
    raw = document(body, bounds.max_row_bytes)
    if record.encoded_bytes != len(raw):
        raise DataflowError("SOURCE_CHANGED", "Record encoded byte count drifted")
    return raw


def prepare_sink_partition(records, *, spec: KernelSpec, services: RuntimeServices) -> ArtifactRef:
    if not isinstance(spec, KernelSpec):
        raise DataflowError("INVALID_CONTRACT", "prepare_sink_partition requires a KernelSpec")
    bounds = services.bounds
    store = services.store
    partition_id = _partition_id(spec, services)
    sink_id = spec.configuration.get("sink_id") or spec.kind
    identifier(sink_id)
    rows, total, columns = 0, 0, None
    chunks = []
    for record in records:
        services.check_stop()
        if not isinstance(record, Record):
            raise DataflowError("INVALID_CONTRACT", "Sink staging requires Record values")
        encoded = _encode_record(record, bounds)
        rows += 1
        total += len(encoded) + 1
        if rows > bounds.max_output_rows:
            raise DataflowError("BUDGET_EXCEEDED", "Sink partition row count exceeds budget")
        if total > bounds.max_batch_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "Sink partition byte count exceeds budget")
        if columns is None and record.inline is not None:
            columns = sorted(record.inline)
        chunks.append(encoded + b"\n")
    data_ref = store.put(iter(chunks), max_bytes=bounds.max_batch_bytes, media_type=RECORDS_MEDIA)
    manifest = {"schema": _MANIFEST_SCHEMA, "sink_id": sink_id, "partition_id": partition_id,
                "rows": rows, "bytes": total, "columns": columns,
                "data_ref": _artifact_json(data_ref)}
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
    for ref in refs:
        manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=MANIFEST_MEDIA)
        if type(manifest) is not dict or manifest.get("schema") != _MANIFEST_SCHEMA:
            raise DataflowError("CORRUPT_ARTIFACT", "Invalid prepared sink manifest")
        if manifest.get("sink_id") != sink_id:
            raise DataflowError("INVALID_CONTRACT", "Prepared manifest belongs to a different sink")
        partition_id = manifest["partition_id"]
        if partition_id in seen:
            raise DataflowError("INVALID_CONTRACT",
                                "Duplicate prepared partition; two workers submitted one partition")
        seen.add(partition_id)
        manifest_columns = manifest["columns"]
        if manifest_columns is not None:
            if columns is None:
                columns = manifest_columns
            elif manifest_columns != columns:
                raise DataflowError("SOURCE_CHANGED", "Prepared partition schema drifted")
        _artifact_from_json(manifest["data_ref"])
        total_rows += manifest["rows"]
        total_bytes += manifest["bytes"]
        partitions[partition_id] = {"partition_id": partition_id, "rows": manifest["rows"],
                                    "bytes": manifest["bytes"], "data_ref": manifest["data_ref"]}
    combined = {"schema": _COMBINED_SCHEMA, "sink_id": sink_id,
                "partitions": [partitions[k] for k in sorted(partitions)],
                "total_rows": total_rows, "total_bytes": total_bytes, "columns": columns}
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