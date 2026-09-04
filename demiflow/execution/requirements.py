"""Frozen Candidate and trusted platform requirement declarations."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Mapping

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.tags import sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version

REQUIREMENTS_PATH = "pipeline/requirements.txt"
MAX_REQUIREMENTS_BYTES = 64 * 1024
PLATFORM_WHEEL_BUNDLE_DIRECTORY = "platform-wheels"
_HASH_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class FrozenRequirements:
    path: Path | None
    bundle_relative_path: str
    text: str
    sha256: str

    @property
    def nonempty(self) -> bool:
        return bool(self.text.strip())


@dataclass(frozen=True)
class ResolvedWheel:
    name: str
    version: str
    filename: str
    sha256: str

    def __post_init__(self) -> None:
        if canonicalize_name(self.name) != self.name or not self.name:
            raise ValueError("resolved wheel name must be canonical")
        if not self.version:
            raise ValueError("resolved wheel version is required")
        if Path(self.filename).name != self.filename or not self.filename.endswith(".whl"):
            raise ValueError("resolved wheel filename is invalid")
        if len(self.sha256) != 64 or any(character not in "0123456789abcdef" for character in self.sha256):
            raise ValueError("resolved wheel sha256 is invalid")
        try:
            distribution, version, _build, _tags = parse_wheel_filename(self.filename)
        except Exception as exc:
            raise ValueError("resolved wheel filename is invalid") from exc
        if canonicalize_name(str(distribution)) != self.name or str(version) != self.version:
            raise ValueError("resolved wheel filename does not match its identity")

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "version": self.version,
            "filename": self.filename,
            "sha256": self.sha256,
        }

    @classmethod
    def from_mapping(cls, value) -> "ResolvedWheel":
        if not isinstance(value, Mapping) or set(value) != {
            "name", "version", "filename", "sha256",
        }:
            raise ValueError("resolved wheel fields are invalid")
        return cls(
            str(value["name"]), str(value["version"]),
            str(value["filename"]), str(value["sha256"]),
        )


def read_candidate_requirements(root: str | Path) -> FrozenRequirements:
    base = Path(root).resolve()
    path = base / REQUIREMENTS_PATH
    if not path.exists():
        return FrozenRequirements(None, "", "", "")
    resolved, raw, text = _read_requirements_file(path, owner=base)
    return FrozenRequirements(
        resolved, REQUIREMENTS_PATH, text, hashlib.sha256(raw).hexdigest(),
    )


def read_platform_requirements(path: str | Path) -> FrozenRequirements:
    resolved, raw, text = _read_requirements_file(path)
    _validate_platform_pins(text)
    return FrozenRequirements(
        resolved, resolved.name, text, hashlib.sha256(raw).hexdigest(),
    )


def platform_requirement_names(text: str) -> frozenset[str]:
    return frozenset(
        canonicalize_name(requirement.name)
        for _number, _raw, requirement in _requirement_lines(text)
    )


def resolve_platform_wheels(
    platform: FrozenRequirements, *, directory: str | Path | None,
    packages: tuple[str, ...],
) -> tuple[ResolvedWheel, ...]:
    """Resolve explicitly selected exact platform requirements to local wheels."""
    if tuple(sorted(set(packages))) != packages or any(
        canonicalize_name(name) != name or not name for name in packages
    ):
        raise ValueError("platform wheel packages must be sorted unique canonical names")
    if not packages:
        if directory is not None:
            raise ValueError("platform wheel directory requires selected packages")
        return ()
    if directory is None:
        raise ValueError("selected platform wheel packages require a directory")
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("platform wheel directory is unavailable")
    root = root.resolve()
    environment = default_environment()
    declared: dict[str, Requirement] = {
        canonicalize_name(requirement.name): requirement
        for _number, _raw, requirement in _requirement_lines(platform.text)
    }
    compatible_tags = set(sys_tags())
    parsed: list[tuple[Path, str, Version, frozenset]] = []
    for path in sorted(root.iterdir()):
        if path.is_symlink() or not path.is_file() or path.suffix != ".whl":
            continue
        try:
            distribution, version, _build, tags = parse_wheel_filename(path.name)
        except Exception:
            continue
        parsed.append((path, canonicalize_name(str(distribution)), version, tags))
    selected = []
    for name in packages:
        requirement = declared.get(name)
        if requirement is None:
            raise ValueError(f"platform wheel package is not declared: {name}")
        if requirement.marker is not None and not requirement.marker.evaluate(environment):
            raise ValueError(f"platform wheel package marker is inactive: {name}")
        exact = _exact_requirement_version(requirement)
        matches = [
            path for path, distribution, version, tags in parsed
            if distribution == name and version == exact
            and bool(set(tags).intersection(compatible_tags))
        ]
        if len(matches) != 1:
            raise ValueError(
                f"platform wheel package requires exactly one compatible exact wheel: {name}"
            )
        path = matches[0]
        selected.append(ResolvedWheel(name, str(exact), path.name, file_sha256(path)))
    return tuple(selected)


def platform_wheel_paths(
    wheels: tuple[ResolvedWheel, ...], directory: str | Path | None,
) -> tuple[Path, ...]:
    """Verify and locate frozen platform wheel artifacts."""
    if not wheels:
        if directory is not None:
            raise ValueError("platform wheel directory exists without frozen wheels")
        return ()
    if directory is None:
        raise ValueError("frozen platform wheels require a directory")
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("platform wheel directory is unavailable")
    root = root.resolve()
    compatible_tags = set(sys_tags())
    paths = []
    for wheel in wheels:
        source = root / wheel.filename
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"platform wheel is unavailable: {wheel.filename}")
        path = source.resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"platform wheel path escapes directory: {wheel.filename}") from exc
        distribution, version, _build, tags = parse_wheel_filename(path.name)
        if (
            canonicalize_name(str(distribution)) != wheel.name
            or str(version) != wheel.version
            or not set(tags).intersection(compatible_tags)
            or file_sha256(path) != wheel.sha256
        ):
            raise ValueError(f"platform wheel identity mismatch: {wheel.filename}")
        paths.append(path)
    return tuple(paths)


def render_effective_requirements(
    platform: FrozenRequirements,
    candidate: FrozenRequirements,
    *,
    platform_wheel_references: Mapping[str, str] | None = None,
    candidate_wheel_references: tuple[str, ...] | None = None,
) -> str:
    """Render one mixed-source requirements file without duplicate distributions.

    ``candidate_wheel_references=None`` preserves Candidate requirement text.
    A tuple means the Candidate declaration has already been frozen to those
    wheel references, so its original specifier lines are not emitted.
    """
    references = dict(platform_wheel_references or {})
    if any(canonicalize_name(name) != name or not reference for name, reference in references.items()):
        raise ValueError("platform wheel references are invalid")
    consumed = set()
    output = []
    for raw in platform.text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            output.append(raw)
            continue
        requirement = Requirement(stripped)
        name = canonicalize_name(requirement.name)
        reference = references.get(name)
        if reference is None:
            output.append(raw)
        else:
            output.append(_direct_reference(requirement, reference))
        consumed.add(name)
    if consumed != set(references):
        missing = sorted(set(references) - consumed)
        raise ValueError(f"platform wheel references are not declared: {missing}")
    if candidate_wheel_references is None:
        output.extend(candidate.text.splitlines())
    else:
        output.extend(candidate_wheel_references)
    return "\n".join(output).rstrip("\n") + "\n"


def write_effective_requirements(text: str, destination: str | Path) -> Path:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@dataclass(frozen=True)
class PythonEnvironmentResolution:
    platform_requirements_sha256: str
    candidate_requirements_sha256: str
    wheelhouse_sha256: str
    wheels: tuple[ResolvedWheel, ...]
    schema_version: str = "demiflow_python_environment_resolution_v1"

    @property
    def resolution_digest(self):
        import json
        return hashlib.sha256(json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()).hexdigest()

    def to_dict(self):
        return {
            "schema_version": self.schema_version,
            "platform_requirements_sha256": self.platform_requirements_sha256,
            "candidate_requirements_sha256": self.candidate_requirements_sha256,
            "wheelhouse_sha256": self.wheelhouse_sha256,
            "wheels": [wheel.to_dict() for wheel in self.wheels],
        }

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, Mapping) or set(value) != {
            "schema_version", "platform_requirements_sha256",
            "candidate_requirements_sha256", "wheelhouse_sha256", "wheels",
        } or value["schema_version"] != "demiflow_python_environment_resolution_v1":
            raise ValueError("unsupported Python environment resolution")
        wheels = tuple(ResolvedWheel.from_mapping(wheel) for wheel in value["wheels"])
        return cls(
            str(value["platform_requirements_sha256"]),
            str(value["candidate_requirements_sha256"]),
            str(value["wheelhouse_sha256"]), wheels,
        )


def resolve_python_environment(
    platform: FrozenRequirements, candidate: FrozenRequirements, *,
    wheelhouse: str | Path | None = None,
) -> PythonEnvironmentResolution:
    root = Path(wheelhouse).resolve() if wheelhouse else None
    available = {}
    compatible_tags = set(sys_tags())
    wheelhouse_hash = ""
    if root is not None and root.is_dir():
        digest = hashlib.sha256()
        for path in sorted(root.iterdir()):
            if not path.is_file() or path.is_symlink():
                continue
            content_hash = file_sha256(path)
            digest.update(path.name.encode() + b"\0" + bytes.fromhex(content_hash))
            if path.suffix != ".whl":
                continue
            try:
                distribution, version, _build, tags = parse_wheel_filename(path.name)
            except Exception:
                continue
            if not set(tags).intersection(compatible_tags):
                continue
            item = ResolvedWheel(
                canonicalize_name(str(distribution)), str(version), path.name, content_hash,
            )
            available.setdefault(item.name, []).append((version, item))
        wheelhouse_hash = digest.hexdigest()
    selected = []
    seen = set()
    environment = default_environment()
    for number, raw in enumerate(candidate.text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-") or "\\" in line:
            raise ValueError(f"Candidate requirements line {number} uses an unsupported source form")
        try:
            requirement = Requirement(line)
        except InvalidRequirement as exc:
            raise ValueError(f"Candidate requirements line {number} is invalid") from exc
        if requirement.url is not None:
            raise ValueError(f"Candidate requirements line {number} uses an unsupported direct URL")
        if requirement.marker is not None and not requirement.marker.evaluate(environment):
            continue
        name = canonicalize_name(requirement.name)
        if name in seen:
            raise ValueError(f"Candidate requirements duplicate package {name}")
        seen.add(name)
        matches = [
            (version, item) for version, item in available.get(name, ())
            if version in requirement.specifier
        ]
        if not matches:
            raise ValueError(f"Candidate dependency is unavailable in trusted wheelhouse: {name}")
        matches.sort(key=lambda pair: pair[0], reverse=True)
        selected.append(matches[0][1])
    return PythonEnvironmentResolution(
        platform.sha256, candidate.sha256, wheelhouse_hash,
        tuple(sorted(selected, key=lambda item: item.name)),
    )


def verify_python_environment_resolution(
    value, platform: FrozenRequirements, candidate: FrozenRequirements, *,
    wheelhouse: str | Path | None = None,
) -> PythonEnvironmentResolution:
    result = value if isinstance(value, PythonEnvironmentResolution) else PythonEnvironmentResolution.from_mapping(dict(value))
    if result.platform_requirements_sha256 != platform.sha256 or result.candidate_requirements_sha256 != candidate.sha256:
        raise ValueError("Python environment resolution requirements mismatch")
    names = [item.name for item in result.wheels]
    if names != sorted(set(names)):
        raise ValueError("Python environment resolution wheel set is invalid")
    if wheelhouse is not None:
        current = resolve_python_environment(platform, candidate, wheelhouse=wheelhouse)
        if current.wheelhouse_sha256 != result.wheelhouse_sha256:
            raise ValueError("Python environment resolution wheelhouse mismatch")
        if tuple(item.to_dict() for item in current.wheels) != tuple(item.to_dict() for item in result.wheels):
            raise ValueError("Python environment resolution selected wheels mismatch")
    return result


def resolved_wheel_paths(resolution, wheelhouse: str | Path) -> tuple[Path, ...]:
    value = resolution if isinstance(resolution, PythonEnvironmentResolution) else PythonEnvironmentResolution.from_mapping(dict(resolution))
    root = Path(wheelhouse).resolve()
    paths = []
    for item in value.wheels:
        source = root / item.filename
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"resolved Candidate wheel is unavailable: {item.filename}")
        path = source.resolve()
        path.relative_to(root)
        if file_sha256(path) != item.sha256:
            raise ValueError(f"resolved Candidate wheel is unavailable: {item.filename}")
        paths.append(path)
    return tuple(paths)


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _read_requirements_file(
    path: str | Path, *, owner: Path | None = None,
) -> tuple[Path, bytes, str]:
    target = Path(path)
    if target.is_symlink() or not target.is_file():
        raise ValueError("requirements must be a regular non-symlink file")
    resolved = target.resolve()
    if owner is not None:
        resolved.relative_to(owner)
    raw = resolved.read_bytes()
    if len(raw) > MAX_REQUIREMENTS_BYTES:
        raise ValueError(f"requirements exceed {MAX_REQUIREMENTS_BYTES} bytes")
    if b"\0" in raw:
        raise ValueError("requirements contain a NUL byte")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("requirements must be UTF-8") from exc
    return resolved, raw, text


def _validate_platform_pins(text: str) -> None:
    names = set()
    count = 0
    for number, raw, requirement in _requirement_lines(text):
        count += 1
        line = raw.strip()
        if line.startswith("-") or "\\" in line:
            raise ValueError(f"platform requirements line {number} must be a self-contained pin")
        if requirement.url is not None:
            raise ValueError(f"platform requirements line {number} cannot use a direct URL")
        specifiers = list(requirement.specifier)
        if len(specifiers) != 1 or specifiers[0].operator != "==" or "*" in specifiers[0].version:
            raise ValueError(f"platform requirements line {number} must use one exact == pin")
        name = canonicalize_name(requirement.name)
        if name in names:
            raise ValueError(f"platform requirements line {number} duplicates package {name}")
        names.add(name)
    if not count:
        raise ValueError("platform requirements must contain at least one exact pin")


def _requirement_lines(text: str):
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            requirement = Requirement(line)
        except InvalidRequirement as exc:
            raise ValueError(f"requirements line {number} is invalid") from exc
        yield number, raw, requirement


def _exact_requirement_version(requirement: Requirement) -> Version:
    specifiers = list(requirement.specifier)
    if len(specifiers) != 1 or specifiers[0].operator != "==":
        raise ValueError(f"platform requirement is not an exact pin: {requirement.name}")
    return Version(specifiers[0].version)


def _direct_reference(requirement: Requirement, reference: str) -> str:
    extras = ""
    if requirement.extras:
        extras = "[" + ",".join(sorted(requirement.extras)) + "]"
    marker = f" ; {requirement.marker}" if requirement.marker is not None else ""
    return f"{requirement.name}{extras} @ {reference}{marker}"


__all__ = [
    "FrozenRequirements", "PythonEnvironmentResolution", "ResolvedWheel",
    "MAX_REQUIREMENTS_BYTES", "PLATFORM_WHEEL_BUNDLE_DIRECTORY",
    "REQUIREMENTS_PATH", "file_sha256", "platform_requirement_names",
    "platform_wheel_paths", "read_candidate_requirements",
    "read_platform_requirements", "render_effective_requirements",
    "resolve_platform_wheels", "resolve_python_environment",
    "resolved_wheel_paths", "verify_python_environment_resolution",
    "write_effective_requirements", "CANDIDATE_ENVIRONMENT_LOCK_PATH",
    "read_candidate_environment_lock",
]

CANDIDATE_ENVIRONMENT_LOCK_PATH = "pipeline/environment-lock.json"


def read_candidate_environment_lock(bundle_root, platform, requirements, *, wheelhouse=None):
    """Read and verify the frozen Candidate-owned Python environment lock."""
    path = Path(bundle_root) / CANDIDATE_ENVIRONMENT_LOCK_PATH
    if path.is_symlink() or not path.is_file():
        raise ValueError("Candidate environment lock is unavailable")
    value = json.loads(path.read_text(encoding="utf-8"))
    return verify_python_environment_resolution(
        value, platform, requirements, wheelhouse=wheelhouse,
    )
