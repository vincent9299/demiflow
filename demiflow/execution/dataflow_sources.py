"""Incremental source snapshots, splits and reader assembly (lane S, C04).

Freeze source version/membership without copying the whole source into the
driver, plan bounded splits with stable cursors, and assemble the frozen reader
registry. One-shot sources persist before downstream effects; re-readable
sources keep a stable seek cursor. Identity formulas follow IDENTITY.md §7.
"""
from __future__ import annotations

import hashlib
import json
import pickle
from pathlib import Path
from typing import Any, Mapping

from ..data.datasource import Datasource
from ..data.sources import (
    DatasourceSource,
    FileSource,
    ItemsSource,
    IterableSource,
    LanceSource,
    MaterializedSource,
    RangeSource,
    SourcePlan,
    SqlSource,
)
from ..lance.model import LanceScanSpec
from .contracts.dataflow import (
    ArtifactRef,
    Bounds,
    Record,
    RuntimeServices,
    SourceCursor,
    SourceReader,
    SourceSnapshot,
    SourceSplit,
)
from .contracts.dataflow_errors import DataflowError
from .contracts.dataflow import thaw_record_value
from .recovery_contract import document, identity

_RECORD_MEDIA_TYPE = "application/vnd.demiflow.record.v3"
_MANIFEST_MEDIA_TYPE = "application/vnd.demiflow.source-manifest.v3"
_REPLAY_KINDS = ("seek", "durable_spool", "lease_queue", "attempt_only")
_SOURCE_KINDS = {"ItemsSource": "items", "RangeSource": "range", "FileSource": "file",
                 "SqlSource": "sql", "LanceSource": "lance", "MaterializedSource": "materialized",
                 "IterableSource": "iterable", "task": "datasource", "history": "queue_history"}


def _canonical_kind(kind):
    # Only new snapshots emit semantic kinds. Existing stored snapshots remain
    # byte-for-byte unchanged and are interpreted through the explicit map.
    return _SOURCE_KINDS.get(kind, kind)



def _content_digest(value: Any) -> str:
    from .literal_source import items_fingerprint
    return items_fingerprint(value)[0]


def _json_digest(value: Any) -> str:
    from .contracts.dataflow import thaw_record_value
    return hashlib.sha256(document(thaw_record_value(value))).hexdigest()


def _options_digest(options: Mapping[str, Any]) -> str:
    if any(type(value).__module__ == 'pyarrow._csv'
           and type(value).__name__ in {'ReadOptions', 'ParseOptions', 'ConvertOptions'}
           for value in options.values()):
        # These native declarations were unsupported by the v3 compiler.
        # Reuse their complete semantic projection; stdlib pickle cannot carry
        # a local/partial callback's module captures. Old JSON/SQL option bytes
        # continue through the original branch below without any migration.
        from .recovery_identity import describe
        return _json_digest(describe(dict(options)))
    return _content_digest(dict(options))


def _factory_identity(factory: Any) -> dict[str, Any]:
    """Bounded code identity for a callable; never pickle a live closure."""
    name = getattr(factory, "__qualname__", None) or getattr(factory, "__name__", None) or type(factory).__name__
    code = getattr(factory, "__code__", None)
    if code is None:
        return {"callable": name}
    payload = {"name": name, "bytecode": code.co_code.hex(),
               "consts": [repr(item) for item in code.co_consts], "names": list(code.co_names)}
    return {"callable": name, "code_digest": hashlib.sha256(document(payload)).hexdigest()}


def _source_facts(source: SourcePlan) -> dict[str, Any]:
    """Canonical, Json-safe semantic facts used for identity and matching."""
    if isinstance(source, ItemsSource):
        from .literal_source import items_fingerprint
        return {"kind": "ItemsSource", "count": len(source.items),
                "items_digest": items_fingerprint(source.items)[0]}
    if isinstance(source, RangeSource):
        return {"kind": "RangeSource", "count": int(source.count)}
    if isinstance(source, FileSource):
        return {"kind": "FileSource", "format": source.format,
                "paths": list(source.paths), "options_digest": _options_digest(source.options)}
    if isinstance(source, SqlSource):
        return {"kind": "SqlSource", "sql": source.sql,
                "options_digest": _options_digest(source.options)}
    if isinstance(source, LanceSource):
        return {"kind": "LanceSource", "query": source.query.to_dict()}
    if isinstance(source, MaterializedSource):
        # Arrow/pandas/NumPy constructors carry a bounded handle, but their
        # optional SourcePlan row-count hint may be absent. Absence is not EOF.
        count = source.row_count
        if count is None:
            count = getattr(source.handle, 'row_count', None)
        if type(count) is not int or count < 0:
            raise DataflowError('INVALID_CONTRACT', 'Materialized source needs a finite row count')
        return {"kind": "MaterializedSource", "row_count": count,
                "handle_digest": (_json_digest(source.handle.recovery_reference)
                                  if getattr(source.handle, "recovery_reference", None) is not None
                                  else _content_digest(source.handle))}
    if isinstance(source, IterableSource):
        return {"kind": "IterableSource", "factory": _factory_identity(source.factory)}
    if isinstance(source, DatasourceSource):
        return {"kind": "DatasourceSource",
                "datasource_digest": _content_digest(source.datasource.snapshot_identity())}
    raise DataflowError("INVALID_CONTRACT", f"unsupported source plan: {type(source).__name__}")


def _source_ordinal(source: SourcePlan, action: Any) -> int:
    """Stable source position in the action declaration's ordered sources.

    The occurrence index is assigned by the compiler's stable traversal; the
    declaration preserves that order so the snapshot can bind the same index.
    """
    declaration = getattr(action, "declaration", None)
    facts = _source_facts(source)
    if isinstance(declaration, Mapping):
        sources = declaration.get("sources")
        if isinstance(sources, (list, tuple)):
            for index, entry in enumerate(sources):
                if isinstance(entry, Mapping) and dict(entry) == facts:
                    return index
    return 0


def _file_entry(path: str) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.is_file():
        raise DataflowError("INVALID_CONTRACT", f"source file is missing: {path}")
    digest = hashlib.sha256()
    size = 0
    with resolved.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    stat = resolved.stat()
    return {"path": str(resolved), "size": size, "sha256": digest.hexdigest(),
            "mtime_ns": stat.st_mtime_ns, "inode": stat.st_ino}


def _put_manifest(store: Any, payload: Any, bounds: Bounds) -> ArtifactRef:
    raw = document(payload)
    if len(raw) > bounds.max_control_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "source manifest exceeds control byte budget")
    return store.put(iter((raw,)), max_bytes=len(raw), media_type=_MANIFEST_MEDIA_TYPE)


def _lance_version(source: LanceSource) -> int:
    from ..lance.read import _dataset_version
    from ..lance.storage import open_lance_dataset

    query = source.query
    dataset = open_lance_dataset(query.uri, query.version, query.storage_options)
    return _dataset_version(dataset)


def _datasource_kind(datasource: Datasource) -> str:
    from .legacy_queue import LegacyQueueHistoryDatasource
    from .queue_history import QueueHistoryDatasource
    from .queue_source import QueueDatasource

    if isinstance(datasource, QueueDatasource):
        return "queue"
    if isinstance(datasource, (QueueHistoryDatasource, LegacyQueueHistoryDatasource)):
        return "history"
    return "task"


def snapshot_source(
    source: SourcePlan, *, action: Any, store: Any, bounds: Bounds,
    source_id: str | None = None,
) -> SourceSnapshot:
    """Freeze version/membership with bounded metadata; never list the whole source."""
    action_id = getattr(action, "action_id", None)
    if not isinstance(action_id, str) or not action_id:
        raise DataflowError("INVALID_CONTRACT", "snapshot requires a bound action_id")
    ordinal = _source_ordinal(source, action)
    source_id = source_id or identity(["dataflow-occurrence-v3", action_id, ordinal])
    facts = _source_facts(source)
    manifest_ref: ArtifactRef | None = None

    if isinstance(source, ItemsSource):
        kind, replay, finite = "ItemsSource", "seek", True
        immutable = facts
        from .literal_source import literal_reference, stage_literal
        reference = literal_reference(source, bounds)
        if reference is not None:
            manifest_ref = stage_literal(source, reference, source_id=source_id,
                                         store=store, bounds=bounds)
    elif isinstance(source, RangeSource):
        if isinstance(source.count, bool) or not isinstance(source.count, int) or source.count < 0:
            raise DataflowError("INVALID_CONTRACT", "range count must be a nonnegative integer")
        kind, replay, finite = "RangeSource", "seek", True
        immutable = facts
    elif isinstance(source, FileSource):
        kind, replay, finite = "FileSource", "seek", True
        entries = [_file_entry(path) for path in source.paths]
        manifest_ref = _put_manifest(store, {"files": entries}, bounds)
        immutable = {"kind": "FileSource", "format": source.format,
                     "file_count": len(entries), "manifest_digest": manifest_ref.digest,
                     "options_digest": _options_digest(source.options)}
    elif isinstance(source, SqlSource):
        kind, finite = "SqlSource", True
        snapshot_id = source.options.get("snapshot_id")
        keyset = source.options.get("keyset")
        if isinstance(snapshot_id, str) and snapshot_id and keyset:
            replay = "seek"
        else:
            replay = "durable_spool"
        immutable = {"kind": "SqlSource", "sql": source.sql,
                     "snapshot_id": snapshot_id if isinstance(snapshot_id, str) else None,
                     "keyset": list(keyset) if isinstance(keyset, (list, tuple)) else None,
                     "options_digest": _options_digest(source.options)}
    elif isinstance(source, LanceSource):
        kind, replay, finite = "LanceSource", "seek", True
        version = _lance_version(source)
        immutable = {"kind": "LanceSource", "version": version, "query": source.query.to_dict()}
        if isinstance(source.query, LanceScanSpec):
            from dataclasses import replace
            from ..lance.read import lance_scan_schema
            from .dataflow_schema import encode, KEY
            immutable[KEY] = encode(lance_scan_schema(replace(source.query, version=version)), bounds=bounds)
    elif isinstance(source, MaterializedSource):
        kind, replay, finite = "MaterializedSource", "seek", True
        immutable = facts
        from .literal_source import literal_reference, stage_literal
        reference = literal_reference(source, bounds)
        if reference is not None:
            manifest_ref = stage_literal(source, reference, source_id=source_id,
                                         store=store, bounds=bounds)
    elif isinstance(source, IterableSource):
        kind, replay, finite = "IterableSource", "attempt_only", False
        immutable = facts
    elif isinstance(source, DatasourceSource):
        kind = _datasource_kind(source.datasource)
        snapshot_identity = source.datasource.snapshot_identity()
        if kind == "queue":
            replay, finite = "lease_queue", False
            immutable = {"kind": "queue", "declaration": source.datasource.declaration()}
        elif kind == "history":
            replay, finite = "seek", True
            immutable = {"kind": "history", "declaration": snapshot_identity}
        elif snapshot_identity is None:
            replay, finite = "attempt_only", False
            immutable = {"kind": "task", "declaration": None}
        else:
            replay, finite = "seek", True
            immutable = {"kind": "task", "declaration": snapshot_identity}
    else:
        raise DataflowError("INVALID_CONTRACT", f"unsupported source plan: {type(source).__name__}")

    return SourceSnapshot(
        source_id=source_id, kind=_canonical_kind(kind), immutable_identity=immutable,
        finite=finite, replay=replay, manifest_ref=manifest_ref,
    )


def _split(source_id: str, locator: Mapping[str, Any], start_cursor: Mapping[str, Any],
           *, estimated_bytes: int | None, required_domain: str) -> SourceSplit:
    locator = dict(locator)
    return SourceSplit(
        split_id=identity([source_id, locator]), source_id=source_id, locator=locator,
        start_cursor=dict(start_cursor), estimated_bytes=estimated_bytes,
        required_domain=required_domain,
    )


def _partition_ranges(count: int, target_partitions: int) -> list[tuple[int, int]]:
    if count <= 0:
        return [(0, 0)]
    parts = max(1, min(target_partitions, count))
    step = (count + parts - 1) // parts
    return [(start, min(start + step, count)) for start in range(0, count, step)]


def plan_source_splits(
    snapshot: SourceSnapshot, source: SourcePlan, *, target_partitions: int, bounds: Bounds,
) -> tuple[SourceSplit, ...]:
    """Build bounded splits with stable start cursors; reads stay on the worker."""
    if isinstance(target_partitions, bool) or not isinstance(target_partitions, int) or target_partitions <= 0:
        raise DataflowError("INVALID_CONTRACT", "target_partitions must be a positive integer")
    if target_partitions > bounds.max_partitions:
        raise DataflowError("BUDGET_EXCEEDED", "target_partitions exceeds partition budget")
    source_id = snapshot.source_id
    kind = _canonical_kind(snapshot.kind)
    domain = "local"

    if kind in ("items", "materialized"):
        count = snapshot.immutable_identity.get("count")
        if count is None:
            count = snapshot.immutable_identity.get("row_count")
        if type(count) is not int or count < 0:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Sealed materialized source has no finite row count')
        splits = []
        for index, (start, end) in enumerate(_partition_ranges(int(count), target_partitions)):
            splits.append(_split(source_id, {"partition": index, "start": start, "end": end},
                                 {"index": start}, estimated_bytes=None, required_domain=domain))
        return tuple(splits)
    if kind == "range":
        count = int(snapshot.immutable_identity["count"])
        splits = []
        for index, (start, end) in enumerate(_partition_ranges(count, target_partitions)):
            splits.append(_split(source_id, {"partition": index, "start": start, "end": end},
                                 {"next": start}, estimated_bytes=None, required_domain=domain))
        return tuple(splits)
    if kind == "file":
        paths = list(source.paths) if isinstance(source, FileSource) else []
        if not paths:
            return (_split(source_id, {"file_index": 0, "empty": True},
                           {"file_index": 0, "record_index": 0},
                           estimated_bytes=0, required_domain=domain),)
        return tuple(
            _split(source_id, {"file_index": index}, {"file_index": index, "record_index": 0},
                   estimated_bytes=None, required_domain=domain)
            for index in range(len(paths))
        )
    if kind == "lance":
        if isinstance(source, LanceSource) and isinstance(source.query, LanceScanSpec) and source.query.limit is None:
            from ..lance.read import plan_lance_scan_partitions

            version = int(snapshot.immutable_identity["version"])
            from dataclasses import replace

            exact = replace(source.query, version=version)
            partitions = plan_lance_scan_partitions(exact, target_partitions=target_partitions)
            splits = []
            for partition in partitions:
                locator = {"partition_index": partition.partition_index,
                           "fragment_ids": list(partition.fragment_ids), "version": version}
                first = partition.fragment_ids[0] if partition.fragment_ids else 0
                splits.append(_split(source_id, locator,
                                     {"version": version, "fragment_id": first, "row_offset": 0},
                                     estimated_bytes=None, required_domain=domain))
            return tuple(splits)
        return (_split(source_id, {"partition_index": 0, "global_query": True},
                       {"version": int(snapshot.immutable_identity["version"]),
                        "fragment_id": 0, "row_offset": 0},
                       estimated_bytes=None, required_domain=domain),)
    if kind == "sql":
        immutable = snapshot.immutable_identity
        return (_split(source_id, {"partition": "0"},
                       {"snapshot_id": immutable.get("snapshot_id"), "partition": "0", "last_key": None},
                       estimated_bytes=None, required_domain=domain),)
    if kind in ("datasource", "queue_history"):
        if isinstance(source, DatasourceSource):
            tasks = source.datasource.get_read_tasks(target_partitions)
        else:
            tasks = []
        if not tasks:
            return (_split(source_id, {"task_index": 0, "empty": True}, {"index": 0},
                           estimated_bytes=None, required_domain=domain),)
        return tuple(
            _split(source_id, {"task_index": index}, {"index": 0},
                   estimated_bytes=None, required_domain=domain)
            for index in range(len(tasks))
        )
    if kind == "iterable":
        return (_split(source_id, {"partition": 0}, {"index": 0},
                       estimated_bytes=None, required_domain=domain),)
    if kind == "queue":
        declaration = snapshot.immutable_identity.get("declaration", {})
        binding = {
            "source_id": source_id, "mode": "sink",
            "contract_digest": _json_digest(declaration), "generation": None,
            "pool": declaration.get("pool"), "source_occurrence": source_id,
            "fetch_occurrence": source_id, "final_occurrence": None,
        }
        return (_split(source_id, {"binding": binding}, {"claim_ref": "", "record_index": 0},
                       estimated_bytes=None, required_domain="driver"),)
    raise DataflowError("INVALID_CONTRACT", f"unknown source kind: {kind}")


def verify_source_snapshot(snapshot: SourceSnapshot, *, store: Any, bounds: Bounds) -> None:
    """Validate fixed version/membership/config digest and readable domain before recovery."""
    if not isinstance(snapshot.source_id, str) or len(snapshot.source_id) != 64:
        raise DataflowError("INVALID_CONTRACT", "snapshot source_id is not a sha256 hex identity")
    if snapshot.replay not in _REPLAY_KINDS:
        raise DataflowError("INVALID_CONTRACT", f"unknown replay mode: {snapshot.replay}")
    if not isinstance(snapshot.immutable_identity, Mapping):
        raise DataflowError("INVALID_CONTRACT", "snapshot immutable_identity must be a mapping")
    kind = _canonical_kind(snapshot.kind)
    if kind == 'producer':
        from dataclasses import replace
        from .contracts.dataflow_codec import _thaw_json
        original = replace(snapshot, kind=snapshot.immutable_identity['original_kind'],
                           immutable_identity=_thaw_json(snapshot.immutable_identity['original_identity']))
        return verify_source_snapshot(original, store=store, bounds=bounds)
    if kind not in ("items", "range", "file", "sql", "lance",
                    "materialized", "iterable", "datasource", "queue_history", "queue"):
        raise DataflowError("INVALID_CONTRACT", f"unknown source kind: {kind}")

    if kind in ('items', 'materialized') and snapshot.manifest_ref is not None:
        # Literal data is authenticated lazily by the reader under the worker's
        # shared codec grant, before any row or user effect is exposed.
        from .literal_source import MEDIA
        if snapshot.manifest_ref.media_type != MEDIA:
            raise DataflowError('CORRUPT_ARTIFACT', 'literal source manifest media differs')
        return
    if snapshot.manifest_ref is not None:
        if not store.inspect(snapshot.manifest_ref):
            raise DataflowError("SOURCE_CHANGED", "source manifest artifact is unavailable")
        raw = b"".join(store.read(snapshot.manifest_ref, max_bytes=bounds.max_control_bytes))
        if hashlib.sha256(raw).hexdigest() != snapshot.manifest_ref.digest:
            raise DataflowError("CORRUPT_ARTIFACT", "source manifest digest mismatch")
        manifest = json.loads(raw.decode("utf-8"))
        if kind == "file":
            expected = snapshot.immutable_identity.get("manifest_digest")
            if expected != snapshot.manifest_ref.digest:
                raise DataflowError("SOURCE_CHANGED", "source manifest binding changed")
            for entry in manifest.get("files", []):
                current = _file_entry(entry["path"])
                if current["sha256"] != entry["sha256"] or current["size"] != entry["size"]:
                    raise DataflowError("SOURCE_CHANGED", f"source file changed: {entry['path']}")
    if kind == "lance":
        from ..lance.storage import open_lance_dataset

        query = snapshot.immutable_identity.get("query", {})
        version = snapshot.immutable_identity.get("version")
        try:
            open_lance_dataset(query.get("uri"), version,
                               tuple(query.get("storage_options", {}).items()))
        except Exception as exc:
            raise DataflowError("SOURCE_CHANGED", f"Lance fixed version is unreadable: {exc}") from exc


def spool_record(
    record: Record, *, split_id: str, sequence: int, store: Any, bounds: Bounds,
) -> ArtifactRef:
    """Persist a one-shot record before publication; content-addressed and idempotent."""
    if record.payload_ref is not None:
        return record.payload_ref
    if record.inline is None:
        raise DataflowError("INVALID_CONTRACT", "spool record has neither inline payload nor ref")
    from .exchange.dataflow_bridge import _encode_payload
    raw = _encode_payload(record.inline, max_bytes=bounds.max_row_bytes)
    if len(raw) > bounds.max_row_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "spool record exceeds row byte budget")
    return store.put(iter((raw,)), max_bytes=len(raw), media_type=_RECORD_MEDIA_TYPE)


def _resolve_reader(reader_symbol: str) -> type:
    from . import source_readers

    prefix = "demiflow.execution.source_readers:"
    if not reader_symbol.startswith(prefix):
        raise DataflowError("INVALID_CONTRACT", f"reader symbol outside source_readers: {reader_symbol}")
    name = reader_symbol[len(prefix):]
    reader = getattr(source_readers, name, None)
    if reader is None or not isinstance(reader, type):
        raise DataflowError("INVALID_CONTRACT", f"unknown reader symbol: {reader_symbol}")
    return reader


def open_source_reader(
    snapshot: SourceSnapshot, split: SourceSplit, *, cursor: SourceCursor | None,
    services: RuntimeServices,
) -> SourceReader:
    """Assemble the frozen reader from the single source registry, verified by domain."""
    from ..planning.dataflow_registry import source_registry

    verify_source_snapshot(snapshot, store=services.store, bounds=services.bounds)
    if split.source_id != snapshot.source_id:
        raise DataflowError("INVALID_CONTRACT", "split belongs to another source")
    if split.split_id != identity([snapshot.source_id, thaw_record_value(split.locator)]):
        raise DataflowError("INVALID_CONTRACT", "split_id does not match its locator")
    registry = source_registry()
    # Snapshot versions preserve their original class-name kind. The registry
    # names reader capabilities; translate only this closed vocabulary.
    reader_kind = _canonical_kind(snapshot.kind)
    definition = registry.get(reader_kind)
    if definition is None:
        raise DataflowError("INVALID_CONTRACT", f"no registered reader for kind: {snapshot.kind}")
    reader_class = _resolve_reader(definition.reader_symbol)
    reader = reader_class(snapshot, services=services)
    reader.open(split, cursor)
    return reader


__all__ = [
    "snapshot_source", "plan_source_splits", "open_source_reader",
    "verify_source_snapshot", "spool_record",
]
