"""Bounded distribution metadata inventory, not an installed-file attestation."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from email.parser import BytesParser
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import sys
import stat
from urllib.parse import urlsplit, urlunsplit

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from .platform import bounded_json, content_digest
from .schema import ForgeError


@contextmanager
def _regular_file(path, maximum):
    from .io import directory_handle, no_symlinks
    path = no_symlinks(Path(path))
    with directory_handle(path.parent) as parent:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ForgeError("Metadata must be a regular file")
            if maximum is not None and before.st_size > maximum:
                raise ForgeError(f"Metadata exceeds byte budget: {path.name}", code='resource_limit')
            yield source, before.st_size
            after = os.fstat(source.fileno())
        current = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        def identity(info):
            return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
        if identity(before) != identity(after) or identity(after) != identity(current):
            raise ForgeError("Metadata changed while reading", code='conflict')


def regular_file_chunks(path, maximum=None):
    """Yield a stable regular file through anchored descriptors."""
    with _regular_file(path, maximum) as (source, remaining):
        while remaining:
            chunk = source.read(min(remaining, 65536))
            if not chunk:
                raise ForgeError("Metadata changed while reading", code='conflict')
            remaining -= len(chunk)
            yield chunk
        if source.read(1):
            raise ForgeError("Metadata changed while reading", code='conflict')


def read_bounded(path, maximum):
    with _regular_file(path, maximum) as (source, size):
        raw = source.read(size + 1)
        if len(raw) > maximum:
            raise ForgeError(f"Metadata exceeds byte budget: {Path(path).name}", code='resource_limit')
        if len(raw) != size:
            raise ForgeError("Metadata changed while reading", code='conflict')
    return raw


def platform_declaration(root, limits):
    """Source metadata is used only when it belongs to this package root."""
    project = root.parent / "pyproject.toml"
    if project.exists():
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib
        raw = read_bounded(project, limits["metadata_file_bytes"])
        payload = tomllib.loads(raw.decode("utf-8"))
        values = payload.get("project", {})
        if canonicalize_name(values.get("name", "")) != "demiflow":
            raise ForgeError("Source package and project metadata do not match")
        if not isinstance(values.get("version"), str) or not isinstance(values.get("dependencies", []), list) or not isinstance(values.get("optional-dependencies", {}), dict):
            raise ForgeError("Invalid platform project metadata")
        return {
            "mode": "source", "name": "demiflow", "version": values["version"],
            "requires": values.get("dependencies", []),
            "extras": values.get("optional-dependencies", {}),
            "project_sha256": hashlib.sha256(raw).hexdigest(),
        }
    dist = metadata.distribution("demiflow")
    if Path(dist.locate_file("demiflow/__init__.py")).resolve() != root / "__init__.py":
        raise ForgeError("Installed package and distribution metadata do not match")
    headers = BytesParser().parsebytes(read_bounded(_metadata_directory(dist) / "METADATA", limits["metadata_file_bytes"]))
    if canonicalize_name(headers.get("Name", "")) != "demiflow" or not headers["Version"]:
        raise ForgeError("Invalid platform distribution identity")
    return {"mode": "installed", "name": "demiflow", "version": headers["Version"],
            "metadata_root": str(_metadata_directory(dist).absolute()),
            "requires": headers.get_all("Requires-Dist", []),
            "extras": {name: [] for name in headers.get_all("Provides-Extra", [])}}


def _metadata_directory(dist):
    # PathDistribution is the stdlib filesystem metadata provider. Refuse
    # other providers rather than use read_text(), which has no size bound.
    path = getattr(dist, "_path", None)
    if not isinstance(path, Path) or not path.is_dir():
        raise ForgeError("Distribution metadata is not a local directory")
    return path


def _direct_url(raw):
    value = json.loads(raw)
    parsed = urlsplit(value.get("url", ""))
    # Only the digest is persisted. Keep remote source distinctions (including
    # ports and queries) without disclosing URL credentials or query contents.
    host = parsed.hostname or ""
    if ":" in host:
        host = "[" + host + "]"
    if parsed.port:
        host += ":" + str(parsed.port)
    origin = "local" if parsed.scheme == "file" else urlunsplit((
        parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
    vcs = value.get("vcs_info", {})
    archive = value.get("archive_info", {})
    return {"origin": origin, "vcs": vcs.get("vcs"), "commit_id": vcs.get("commit_id"),
            "hashes": archive.get("hashes", {}), "hash": archive.get("hash"),
            "editable": value.get("dir_info", {}).get("editable", False)}


def environment_inventory(declaration, limits, *, extras=(), distribution=metadata.distribution):
    extras = tuple(sorted(set(extras)))
    if len(extras) > 32 or set(extras) - set(declaration["extras"]):
        raise ForgeError("Unknown platform extra")
    edge_limit = limits["distributions"] * 64
    if len(declaration["requires"]) > edge_limit:
        raise ForgeError("Platform requirements exceed inventory budget")
    roots = list(declaration["requires"])
    if declaration["mode"] == "source":
        for extra in extras:
            if not isinstance(declaration["extras"][extra], list) or len(roots) + len(declaration["extras"][extra]) > edge_limit:
                raise ForgeError("Platform extra requirements exceed inventory budget")
            roots.extend(declaration["extras"][extra])
    if len(roots) > limits["distributions"] * 64:
        raise ForgeError("Platform requirements exceed inventory budget")
    pending = [(value, extras) for value in roots]
    records, requirements, activated, diagnostics = {}, {}, {}, []
    bytes_read = 0
    environment = default_environment()

    def file_identity(path, name):
        nonlocal bytes_read
        if not path.exists():
            return None
        raw = read_bounded(path, min(limits["metadata_file_bytes"], limits["metadata_bytes"] - bytes_read))
        bytes_read += len(raw)
        if bytes_read > limits["metadata_bytes"]:
            raise ForgeError("Dependency metadata exceeds total byte budget")
        if name == "direct_url.json":
            return content_digest(_direct_url(raw))
        return hashlib.sha256(raw).hexdigest()

    processed = 0
    while pending:
        text, parent_extras = pending.pop()
        processed += 1
        if processed > limits["distributions"] * 64:
            raise ForgeError("Dependency edges exceed inventory budget")
        requirement = Requirement(text)
        if requirement.marker and not any(requirement.marker.evaluate({**environment, "extra": extra})
                                          for extra in ("", *parent_extras)):
            continue
        name = canonicalize_name(requirement.name)
        requested = set(requirement.extras)
        if name not in records:
            if len(records) >= limits["distributions"]:
                raise ForgeError("Dependency count exceeds inventory budget")
            try:
                dist = distribution(name)
            except metadata.PackageNotFoundError:
                records[name] = {"name": name, "status": "missing", "version": None}
                diagnostics.append({"code": "dependency_missing", "name": name})
                continue
            directory = _metadata_directory(dist)
            fingerprints = {}
            for filename in ("METADATA", "PKG-INFO", "RECORD", "direct_url.json"):
                identity = file_identity(directory / filename, filename)
                if identity:
                    fingerprints[filename] = identity
            metadata_path = directory / ("METADATA" if (directory / "METADATA").exists() else "PKG-INFO")
            headers = BytesParser().parsebytes(read_bounded(metadata_path, limits["metadata_file_bytes"]))
            if canonicalize_name(headers.get("Name", "")) != name or not headers["Version"]:
                raise ForgeError("Invalid dependency distribution identity")
            records[name] = {"name": name, "status": "installed", "version": headers["Version"],
                             "metadata": fingerprints}
            requires = headers.get_all("Requires-Dist", [])
            if len(requires) > 512:
                raise ForgeError("Distribution requirements exceed budget")
            requirements[name] = requires
        record = records[name]
        if record["status"] == "missing":
            continue
        if requirement.url:
            diagnostics.append({"code": "direct_requirement_not_attested", "name": name})
        if requirement.specifier and not requirement.specifier.contains(record["version"], prereleases=True):
            diagnostics.append({"code": "dependency_version_mismatch", "name": name,
                                "expected": str(requirement.specifier), "actual": record["version"]})
        if name not in activated or not requested <= activated[name]:
            active = activated.setdefault(name, set())
            active.update(requested)
            pending.extend((value, tuple(sorted(active))) for value in requirements.get(name, []))
            if len(pending) > limits["distributions"] * 64:
                raise ForgeError("Dependency queue exceeds inventory budget")
    if declaration["mode"] == "installed":
        # The sealed package runs outside its installation directory. Bind the
        # original distribution explicitly instead of resolving a different
        # demiflow installation from the worker's sys.path. Its metadata is
        # still hashed and compared; this does not waive environment checking.
        dist = (metadata.PathDistribution(Path(declaration['metadata_root']))
                if 'metadata_root' in declaration else distribution("demiflow"))
        directory = _metadata_directory(dist)
        own = {name: file_identity(directory / name, name) for name in ("METADATA", "RECORD", "direct_url.json")}
    else:
        own = {"pyproject.toml": declaration["project_sha256"]}
    result = {
        "python": {"implementation": platform.python_implementation(), "version": platform.python_version(),
                   "cache_tag": sys.implementation.cache_tag, "system": platform.system(), "machine": platform.machine()},
        "platform_distribution": {"mode": declaration["mode"], "name": declaration["name"],
                                  "version": declaration["version"], "metadata": own},
        "extras": list(extras), "dependency_assurance": "metadata_only",
        "dependencies": sorted(records.values(), key=lambda item: item["name"]),
        "diagnostics": sorted({json.dumps(item, sort_keys=True): item for item in diagnostics}.values(), key=lambda item: (item["name"], item["code"])),
    }
    bounded_json(result, limits["metadata_bytes"])
    return result
