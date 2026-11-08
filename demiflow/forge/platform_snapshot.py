"""Platform content identities and read-only, scope-preserving comparison."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import tempfile

from .io import no_symlinks
from .platform import LIMITS, bounded_json, content_digest, discover_catalog, limits_with, module_scope
from .platform_environment import environment_inventory, platform_declaration, read_bounded
from .schema import ForgeError
from .standards import engineering_standard


FORMAT = "demiflow_forge_platform_snapshot_v5"
_SECTIONS = ("standard", "catalog", "platform", "environment")
_CACHES = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}


def _relative_path(value):
    if not isinstance(value, str):
        return False
    path = PurePosixPath(value)
    return bool(path.parts) and not path.is_absolute() and ".." not in path.parts and str(path) == value and "\\" not in value


def _file_state(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def source_inventory(root, limits):
    """Bound traversal, open descriptors and read buffers before consuming files."""
    no_symlinks(root)
    records, pending = [], [(root, 0)]
    entries, total = 0, 0
    while pending:
        directory, depth = pending.pop()
        if depth > limits["depth"]:
            raise ForgeError("Platform source exceeds directory depth budget")
        before_directory = directory.stat()
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                if entries > limits["entries"]:
                    raise ForgeError("Platform source exceeds entry budget")
                if child.is_symlink():
                    raise ForgeError("Platform source contains a symlink")
                if child.name in _CACHES or child.name.endswith((".pyc", ".pyo")):
                    continue
                path = Path(child.path)
                info = child.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    pending.append((path, depth + 1))
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise ForgeError("Platform source contains a special file")
                if info.st_size > limits["file_bytes"] or total + info.st_size > limits["source_bytes"]:
                    raise ForgeError("Platform source exceeds file/total byte budget")
                digest, count = hashlib.sha256(), 0
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as source:
                    opened = os.fstat(source.fileno())
                    if not stat.S_ISREG(opened.st_mode) or _file_state(info) != _file_state(opened):
                        raise ForgeError("source_changed: file replaced before reading")
                    while True:
                        allowance = min(1024**2, limits["file_bytes"] - count + 1,
                                        limits["source_bytes"] - total - count + 1)
                        chunk = source.read(allowance)
                        if not chunk:
                            break
                        count += len(chunk)
                        if count > limits["file_bytes"] or total + count > limits["source_bytes"]:
                            raise ForgeError("Platform source grew beyond byte budget")
                        digest.update(chunk)
                    after = os.fstat(source.fileno())
                if count != info.st_size or _file_state(info) != _file_state(after) or _file_state(after) != _file_state(path.lstat()):
                    raise ForgeError("source_changed: file changed while hashing")
                total += count
                records.append({"path": path.relative_to(root).as_posix(), "size": count, "sha256": digest.hexdigest()})
        if _file_state(before_directory) != _file_state(directory.stat()):
            raise ForgeError("source_changed: directory changed while scanning")
    return {"files": sorted(records, key=lambda item: item["path"]), "bytes": total}


def _seal(value):
    value["digests"] = {name: content_digest(value[name]) for name in _SECTIONS}
    value["digests"]["snapshot"] = content_digest({
        "format": value["format"], "scope": value["scope"], "limits": value["limits"],
        "digests": dict(value["digests"]),
    })
    return value


def build_snapshot(modules=None, *, extras=(), limits=None, version=5):
    if type(version) is not int or version not in (1, 2, 3, 4, 5):
        raise ForgeError("Unsupported snapshot version")
    limits = limits_with(limits)
    modules = module_scope(modules)
    root = Path(__file__).resolve().parents[1]
    source = source_inventory(root, limits)
    declaration = platform_declaration(root, limits)
    environment = environment_inventory(declaration, limits, extras=extras)
    standard = engineering_standard(version)
    discovery_version = min(version, 3)
    catalog = discover_catalog(modules, limits=limits, version=discovery_version)
    if source != source_inventory(root, limits) or declaration != platform_declaration(root, limits):
        raise ForgeError("source_changed: platform changed during inspection")
    if environment != environment_inventory(declaration, limits, extras=extras):
        raise ForgeError("environment_changed: metadata changed during inspection")
    result = _seal({
        "format": f"demiflow_forge_platform_snapshot_v{version}",
        "scope": {"discovery_version": discovery_version, "modules": list(modules), "extras": environment["extras"],
                  **({'standard_version': version} if version >= 4 else {})},
        "standard": standard, "catalog": catalog, "platform": source,
        "environment": environment, "limits": limits,
        "provenance": {"created_at": datetime.now(timezone.utc).isoformat()},
    })
    bounded_json(result, limits["snapshot_bytes"])
    return result


def _diagnostics(value):
    return [*value["catalog"]["diagnostics"], *value["environment"]["diagnostics"]]


def _complete(value):
    return value["catalog"]["inspection_complete"] and not value["environment"]["diagnostics"]


def write_snapshot(path, value):
    """Publish a complete new file atomically; an existing file is never replaced."""
    if not _complete(value):
        raise ForgeError("Cannot publish an incomplete platform snapshot")
    raw = bounded_json(value, value["limits"]["snapshot_bytes"])
    path = Path(path).absolute()
    no_symlinks(path)
    if path.resolve().is_relative_to(Path(__file__).resolve().parents[1]):
        raise ForgeError("Snapshot output must be outside the platform package")
    if path.exists():
        raise ForgeError("Snapshot output already exists")
    # Parent creation is explicit: do not walk or create an arbitrary tree.
    descriptor, temporary = tempfile.mkstemp(prefix=".forge-platform-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        no_symlinks(path)
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_snapshot(path):
    try:
        value = json.loads(read_bounded(Path(path), LIMITS["snapshot_bytes"]))
        if not isinstance(value, dict) or set(value) != {
            "format", "scope", *_SECTIONS, "limits", "digests", "provenance"
        } or value["format"] not in {"demiflow_forge_platform_snapshot_v1", "demiflow_forge_platform_snapshot_v2", "demiflow_forge_platform_snapshot_v3", "demiflow_forge_platform_snapshot_v4", FORMAT}:
            raise ForgeError("Invalid platform snapshot format")
        limits = value["limits"]
        if not isinstance(limits, dict) or set(limits) != set(LIMITS) or limits_with(limits) != limits:
            raise ForgeError("Invalid snapshot limits")
        scope = value["scope"]
        modern = value['format'] in {'demiflow_forge_platform_snapshot_v4', FORMAT}
        if set(scope) != {"discovery_version", "modules", "extras"} | ({'standard_version'} if modern else set()) or type(scope["discovery_version"]) is not int or scope["discovery_version"] not in (1, 2, 3):
            raise ForgeError("Invalid discovery scope")
        version = scope["discovery_version"]
        if modern and (version != 3 or scope['standard_version'] != int(value['format'].rsplit('v', 1)[1])) or not modern and value["format"] != f"demiflow_forge_platform_snapshot_v{version}":
            raise ForgeError("Snapshot and discovery versions differ")
        if list(module_scope(scope["modules"])) != scope["modules"] or not isinstance(scope["extras"], list) or len(scope["extras"]) > 32 or any(type(x) is not str for x in scope["extras"]) or sorted(set(scope["extras"])) != scope["extras"]:
            raise ForgeError("Invalid discovery scope")
        catalog = value["catalog"]
        if catalog["format"] != f"demiflow_forge_platform_catalog_v{version}" or catalog["modules"] != scope["modules"] or type(catalog["inspection_complete"]) is not bool:
            raise ForgeError("Invalid snapshot catalogue")
        objects = catalog["objects"]
        if not isinstance(objects, list) or len(objects) > limits["objects"]:
            raise ForgeError("Invalid snapshot objects")
        ids = set()
        fields = {"id", "kind", "origin", "defined_name", "member_owner", "signature", "documentation", "native_semantics", "diagnostics"}
        for item in objects:
            if set(item) != fields or not isinstance(item["id"], str) or item["id"] in ids or item["id"].split(":")[0] not in scope["modules"] or ":" not in item["id"]:
                raise ForgeError("Invalid or duplicate snapshot interface")
            if any(not name.isidentifier() or name.startswith("_") for name in item["id"].split(":", 1)[1].split(".")):
                raise ForgeError("Invalid public interface path")
            ids.add(item["id"])
            if not isinstance(item["origin"], dict) or not isinstance(item["signature"], dict) or not isinstance(item["documentation"], dict):
                raise ForgeError("Invalid snapshot interface facts")
            origin = item["origin"]
            if origin.get("kind") not in {"platform", "dependency", "stdlib", "builtin"} or not isinstance(origin.get("module"), str) or (origin["kind"] != "builtin" and not _relative_path(origin.get("path"))):
                raise ForgeError("Invalid snapshot interface origin")
            signature = item["signature"]
            if signature.get("status") not in {"available", "unavailable", "not_applicable"} or (signature["status"] == "available" and (not isinstance(signature.get("text"), str) or not isinstance(signature.get("parameters"), list))):
                raise ForgeError("Invalid snapshot signature")
            bounded_json(signature, limits["signature_bytes"])
            doc = item["documentation"]
            if doc.get("status") not in {"available", "missing"} or not isinstance(doc.get("text"), str) or len(doc["text"].encode("utf-8")) > limits["doc_bytes"]:
                raise ForgeError("Invalid snapshot documentation")
            if (version == 1 and item["native_semantics"] != {"status": "unknown"}) or not isinstance(item["diagnostics"], list):
                raise ForgeError("Invalid snapshot semantic facts")
            if version >= 2:
                from ..data.flow_contract import validate_semantics
                validate_semantics(item["native_semantics"], signature.get("parameters", []), historical=version == 2)
        files = value["platform"]["files"]
        if not isinstance(files, list) or len(files) > limits["entries"]:
            raise ForgeError("Invalid platform file inventory")
        paths, total = set(), 0
        for item in files:
            path = PurePosixPath(item["path"])
            if set(item) != {"path", "size", "sha256"} or not _relative_path(item["path"]) or str(path) in paths:
                raise ForgeError("Invalid platform file path")
            if type(item["size"]) is not int or not 0 <= item["size"] <= limits["file_bytes"] or not isinstance(item["sha256"], str) or len(item["sha256"]) != 64 or any(c not in "0123456789abcdef" for c in item["sha256"]):
                raise ForgeError("Invalid platform file identity")
            paths.add(str(path))
            total += item["size"]
        if total != value["platform"]["bytes"] or total > limits["source_bytes"]:
            raise ForgeError("Invalid platform source size")
        if value["environment"]["extras"] != scope["extras"] or value["environment"]["dependency_assurance"] != "metadata_only" or not isinstance(value["environment"]["diagnostics"], list) or not isinstance(catalog["diagnostics"], list):
            raise ForgeError("Invalid environment inventory")
        if not isinstance(value["provenance"], dict):
            raise ForgeError("Invalid snapshot provenance")
        expected = value["digests"]
        sealed = _seal(dict(value))
        if sealed["digests"] != expected:
            raise ForgeError("Platform snapshot digest mismatch")
        bounded_json(value, limits["snapshot_bytes"])
        return value
    except (KeyError, TypeError, ValueError, RecursionError, OSError, ForgeError) as exc:
        raise ForgeError(f"Invalid platform snapshot: {exc}") from exc


def compare_snapshots(previous, current):
    changes = []
    old = {item["id"]: item for item in previous["catalog"]["objects"]}
    new = {item["id"]: item for item in current["catalog"]["objects"]}
    for identifier in sorted(old.keys() | new.keys()):
        if identifier not in old or identifier not in new:
            changes.append({"kind": "interface_added" if identifier in new else "interface_removed", "id": identifier})
            continue
        for field, kind in (("signature", "signature_changed"), ("documentation", "documentation_changed"),
                            ("origin", "origin_changed"), ("member_owner", "owner_changed"),
                            ("defined_name", "definition_changed"), ("kind", "kind_changed"),
                            ("native_semantics", "semantics_changed"), ("diagnostics", "diagnostics_changed")):
            if old[identifier][field] != new[identifier][field]:
                changes.append({"kind": kind, "id": identifier})
    old_files = {item["path"]: item for item in previous["platform"]["files"]}
    new_files = {item["path"]: item for item in current["platform"]["files"]}
    for path in sorted(old_files.keys() | new_files.keys()):
        if old_files.get(path) != new_files.get(path):
            action = "added" if path not in old_files else "removed" if path not in new_files else "changed"
            changes.append({"kind": "platform_file_" + action, "path": path})
    for section in ("standard", "environment"):
        if previous["digests"][section] != current["digests"][section]:
            changes.append({"kind": section + "_changed"})
    if previous["catalog"]["inspection_complete"] != current["catalog"]["inspection_complete"] or previous["catalog"]["diagnostics"] != current["catalog"]["diagnostics"]:
        changes.append({"kind": "inspection_changed"})
    if previous["scope"] != current["scope"] or previous["limits"] != current["limits"]:
        changes.append({"kind": "inspection_configuration_changed"})
    status = "current" if previous["digests"]["snapshot"] == current["digests"]["snapshot"] else "stale"
    if not _complete(previous) or not _complete(current):
        status = "incomplete"
    return {"status": status, "previous_digest": previous["digests"]["snapshot"],
            "current_digest": current["digests"]["snapshot"], "scope": previous["scope"],
            "dependency_assurance": "metadata_only", "changes": changes,
            "diagnostics": _diagnostics(current), "previous_diagnostics": _diagnostics(previous)}


def check_snapshot(path):
    try:
        previous = load_snapshot(path)
    except ForgeError as exc:
        return {"status": "invalid", "changes": [], "diagnostics": [{"code": "invalid_snapshot", "detail": str(exc)}]}
    try:
        current = build_snapshot(previous["scope"]["modules"], extras=previous["scope"]["extras"], limits=previous["limits"], version=previous['scope'].get('standard_version', previous['scope']['discovery_version']))
        return compare_snapshots(previous, current)
    except (ForgeError, OSError, ValueError, RuntimeError, TypeError, ImportError) as exc:
        return {"status": "incomplete", "changes": [], "diagnostics": [{"code": "inspection_incomplete", "detail": str(exc)}]}
