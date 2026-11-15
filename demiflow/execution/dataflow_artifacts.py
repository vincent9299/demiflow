"""Runtime bundle staging/fetch and cross-host read-domain proof (H011-H014, C10).

A runtime bundle is a bounded ZIP carrying a manifest that binds platform,
dependency, business-module and by-value-program identities by digest. It never
embeds credentials or absolute workspace paths. Fetch verifies every entry
against traversal/symlink/size attacks before an atomic publish into the worker
cache. The durable artifact ref is always the recovery credential; a Ray object
locator is only a short-term transfer hint.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from .contracts.dataflow import DataLocation, DomainProof
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document, identifier, identity

if TYPE_CHECKING:
    from .contracts.dataflow import ArtifactRef, ArtifactStore, Bounds, HostDescriptor, SealedExecution

BUNDLE_SCHEMA = "demiflow.runtime_bundle.v1"
_MAX_ENTRY_BYTES = 1 << 24


def _no_symlink(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink():
            raise DataflowError("INVALID_CONTRACT", "cache path contains a symlink: " + str(item))


def _read_bytes(store: "ArtifactStore", ref: "ArtifactRef", bounds: "Bounds") -> bytes:
    return b"".join(store.read(ref, max_bytes=bounds.max_spill_bytes))


def stage_runtime_bundle(sealed: "SealedExecution", *, store: "ArtifactStore", bounds: "Bounds") -> "ArtifactRef":
    from .contracts.dataflow import ArtifactRef
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


def verify_host_domain(host: "HostDescriptor", *, store: "ArtifactStore",
                       challenge: "ArtifactRef", bounds: "Bounds") -> DomainProof:
    identifier(host.host_id)
    if store.store_id not in host.store_domains:
        raise DataflowError("INVALID_CONTRACT", "host cannot reach the store domain")
    if host.provider == 'ray_core':
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        if not host.host_id.startswith('ray-'):
            raise DataflowError('INVALID_CONTRACT', 'Ray host lacks an actual node identity')
        task = ray.remote(num_cpus=0, max_retries=0)(_probe_read_domain).options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(host.host_id[4:], soft=False))
        future = task.remote(str(store.root), store.store_id, _ref_dict(challenge),
                             bounds.max_control_bytes)
        try:
            proof = ray.get(future, timeout=bounds.timeout_ms / 1000)
        except Exception as exc:
            ray.cancel(future, force=True)
            raise DataflowError('RESOURCE_INFEASIBLE',
                'Ray host cannot prove access to the sealed runtime and durable store') from exc
        if proof['node_id'] != host.host_id[4:] or proof['runtime_revision'] != host.runtime_revision:
            raise DataflowError('CODE_MISMATCH', 'Remote host runtime/node differs from its declaration')
        if proof['digest'] != challenge.digest:
            raise DataflowError('CORRUPT_ARTIFACT', 'Remote read-domain challenge differs')
    raw = _read_bytes(store, challenge, bounds)
    if hashlib.sha256(raw).hexdigest() != challenge.digest:
        raise DataflowError("INVALID_CONTRACT", "challenge bytes differ from the declared digest")
    if not store.inspect(challenge):
        raise DataflowError("UNKNOWN_EFFECT", "challenge is not durably present")
    namespace_digest = identity({"store_id": store.store_id, "host_id": host.host_id,
                                 "domains": list(host.store_domains)})
    return DomainProof(host_id=host.host_id, store_id=store.store_id,
                       challenge_digest=challenge.digest, namespace_digest=namespace_digest, durable=True)


def _probe_read_domain(root, store_id, challenge, max_bytes):
    """Executed on the specified Ray node without creating a run or journal."""
    import ray
    from .recovery_identity import platform_identity
    from .dataflow_store import _no_symlink
    if challenge['key'] != challenge['digest'] or challenge['size_bytes'] > max_bytes:
        raise ValueError('Unsupported read-domain challenge')
    identifier(store_id)
    identifier(challenge['digest'])
    path = Path(root) / store_id / 'objects' / challenge['key']
    _no_symlink(path)
    with path.open('rb') as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) != challenge['size_bytes']:
        raise ValueError('Remote challenge size differs')
    return {'node_id': ray.get_runtime_context().get_node_id(),
            'runtime_revision': platform_identity(), 'digest': hashlib.sha256(raw).hexdigest()}


def resolve_data_location(ref: "ArtifactRef", *, host: "HostDescriptor", store: "ArtifactStore") -> DataLocation:
    identifier(ref.digest)
    if store.store_id not in host.store_domains:
        raise DataflowError("INVALID_CONTRACT", "host cannot reach the artifact domain")
    temporary_locator = None
    # No ray.put occurred. A digest-shaped fake ObjectRef is not a location.
    if not store.inspect(ref):
        raise DataflowError("UNKNOWN_EFFECT", "durable artifact is not readable on this host")
    return DataLocation(domain=store.store_id, temporary_locator=temporary_locator, durable_ref=ref)


def _ref_dict(ref: "ArtifactRef") -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}
