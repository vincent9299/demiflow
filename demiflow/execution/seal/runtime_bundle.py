"""Runtime bundle implementation."""
from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import document, identifier
from demiflow.execution.contracts.dataflow_codec import artifact_reference_fields as _ref_dict
if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import ArtifactRef, ArtifactStore, Bounds, SealedExecution
BUNDLE_SCHEMA = "demiflow.runtime_bundle.v1"
_MAX_ENTRY_BYTES = 1 << 24


def _no_symlink(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink():
            raise DataflowError("INVALID_CONTRACT", "cache path contains a symlink: " + str(item))

def _read_bytes(store: "ArtifactStore", ref: "ArtifactRef", bounds: "Bounds") -> bytes:
    return b"".join(store.read(ref, max_bytes=bounds.max_spill_bytes))

def stage_runtime_bundle(sealed: "SealedExecution", *, store: "ArtifactStore", bounds: "Bounds") -> "ArtifactRef":
    from demiflow.execution.contracts.dataflow import ArtifactRef
    modules: dict[str, str] = {}
    entries: list[tuple[str, bytes]] = []
    programs = []
    for ref in sealed.programs:
        raw = _read_bytes(store, ref, bounds)
        if len(raw) > _MAX_ENTRY_BYTES:
            raise DataflowError("BUDGET_EXCEEDED", "program module exceeds the entry budget")
        try:
            name = json.loads(raw).get("name") or ref.key
        except (ValueError, AttributeError):
            name = ref.key
        if type(name) is not str or not name:
            raise DataflowError("INVALID_CONTRACT", "program module name is invalid")
        if name in modules and modules[name] != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "module name bound to two different code digests")
        modules[name] = ref.digest
        entries.append(("modules/" + ref.digest, raw))
        programs.append({"name": name, "digest": ref.digest, "ref": _ref_dict(ref)})
    manifest = {
        "schema": BUNDLE_SCHEMA,
        "platform_ref": _ref_dict(sealed.code_pin_ref),
        "deps_digest": sealed.code_pin_ref.digest,
        "logical_ref": _ref_dict(sealed.logical_ref),
        "physical_ref": _ref_dict(sealed.physical_ref),
        "code_pin_ref": _ref_dict(sealed.code_pin_ref),
        "identity_bridge_ref": _ref_dict(sealed.identity_bridge_ref),
        "constants_ref": _ref_dict(sealed.constants_ref),
        "source_refs": [_ref_dict(ref) for ref in sealed.source_refs],
        "programs": programs,
        "modules": modules,
    }
    manifest_raw = document(manifest)
    if len(manifest_raw) > bounds.max_control_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "runtime bundle manifest exceeds the control budget")
    buffer = io.BytesIO()
    total = len(manifest_raw)
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", manifest_raw)
        for name, raw in entries:
            total += len(raw)
            if total > bounds.max_spill_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "runtime bundle exceeds the spill budget")
            archive.writestr(name, raw)
    payload = buffer.getvalue()
    return store.put(iter([payload]), max_bytes=len(payload), media_type="application/zip")

def fetch_runtime_bundle(ref: "ArtifactRef", *, cache: Path, store: "ArtifactStore", bounds: "Bounds") -> Path:
    identifier(ref.digest)
    cache = Path(cache)
    if not cache.is_absolute():
        raise DataflowError("INVALID_CONTRACT", "bundle cache must be absolute")
    _no_symlink(cache)
    cache.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix=".bundle-", dir=cache))
    download = staging_root / "bundle.zip"
    try:
        digest = hashlib.sha256()
        size = 0
        with open(download, "wb") as stream:
            for chunk in store.read(ref, max_bytes=bounds.max_spill_bytes):
                size += len(chunk)
                if size > ref.size_bytes or size > bounds.max_spill_bytes:
                    raise DataflowError("BUDGET_EXCEEDED", "bundle exceeds its declared size")
                digest.update(chunk)
                stream.write(chunk)
        if size != ref.size_bytes or digest.hexdigest() != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "bundle bytes do not match its reference")
        extracted = staging_root / "tree"
        extracted.mkdir()
        with zipfile.ZipFile(download) as archive:
            members = archive.infolist()
            if len(members) > bounds.max_nodes:
                raise DataflowError("BUDGET_EXCEEDED", "bundle entry count exceeds budget")
            unpacked = 0
            for member in members:
                name = member.filename
                if name.startswith("/") or ".." in Path(name).parts:
                    raise DataflowError("INVALID_CONTRACT", "bundle entry escapes its root")
                if member.external_attr >> 16 & 0o170000 == 0o120000:
                    raise DataflowError("INVALID_CONTRACT", "bundle entry is a symlink")
                if member.file_size > _MAX_ENTRY_BYTES:
                    raise DataflowError("BUDGET_EXCEEDED", "bundle entry exceeds the size budget")
                unpacked += member.file_size
                if unpacked > bounds.max_spill_bytes:
                    raise DataflowError("BUDGET_EXCEEDED", "bundle unpacked size exceeds budget")
                target = extracted / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, open(target, "wb") as sink:
                    sink.write(source.read())
        target_dir = cache / ref.digest
        _no_symlink(target_dir)
        if target_dir.exists():
            if not (target_dir / "manifest.json").exists():
                raise DataflowError("INVALID_CONTRACT", "cache target is not a published bundle")
        else:
            os.replace(extracted, target_dir)
        return target_dir
    finally:
        for item in staging_root.glob("**/*"):
            if item.is_file():
                item.unlink(missing_ok=True)
        for item in sorted(staging_root.glob("**/*"), reverse=True):
            if item.is_dir():
                item.rmdir()
        staging_root.rmdir()
