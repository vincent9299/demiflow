"""Minimal contracts for running one immutable Pipeline bundle."""
from __future__ import annotations

import hashlib
import json
import platform
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Protocol

if TYPE_CHECKING:
    from .requirements import ResolvedWheel

from ..pipeline import PIPELINE_PACKAGE_PATH, discover_pipeline_definition
from .package_loader import RUNTIME_PACKAGE_PATH

PIPELINE_RUNTIME_ABI = "demiflow-pipeline-v12"


@dataclass(frozen=True)
class PlatformRuntimeIdentity:
    distribution: str
    version: str
    wheel_filename: str
    wheel_sha256: str
    platform_requirements_filename: str
    platform_requirements_sha256: str
    platform_preflight_imports: tuple[str, ...]
    platform_wheels: tuple["ResolvedWheel", ...]
    pipeline_runtime_abi: str
    environment_policy: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "distribution": self.distribution,
            "version": self.version,
            "wheel_filename": self.wheel_filename,
            "wheel_sha256": self.wheel_sha256,
            "platform_requirements_filename": self.platform_requirements_filename,
            "platform_requirements_sha256": self.platform_requirements_sha256,
            "platform_preflight_imports": list(self.platform_preflight_imports),
            "platform_wheels": [item.to_dict() for item in self.platform_wheels],
            "pipeline_runtime_abi": self.pipeline_runtime_abi,
            "environment_policy": self.environment_policy,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PlatformRuntimeIdentity":
        fields = {
            "distribution", "version", "wheel_filename", "wheel_sha256",
            "platform_requirements_filename", "platform_requirements_sha256",
            "platform_preflight_imports", "pipeline_runtime_abi",
            "platform_wheels", "environment_policy",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("Platform runtime identity fields are unsupported")
        imports = value["platform_preflight_imports"]
        if not isinstance(imports, list) or not imports or any(
            not isinstance(item, str) for item in imports
        ):
            raise ValueError("Platform runtime preflight imports are invalid")
        from .requirements import ResolvedWheel
        raw_wheels = value["platform_wheels"]
        if not isinstance(raw_wheels, list):
            raise ValueError("Platform runtime wheels are invalid")
        wheels = tuple(ResolvedWheel.from_mapping(item) for item in raw_wheels)
        result = cls(
            distribution=str(value["distribution"]),
            version=str(value["version"]),
            wheel_filename=str(value["wheel_filename"]),
            wheel_sha256=str(value["wheel_sha256"]),
            platform_requirements_filename=str(
                value["platform_requirements_filename"]
            ),
            platform_requirements_sha256=str(
                value["platform_requirements_sha256"]
            ),
            platform_preflight_imports=tuple(imports),
            platform_wheels=wheels,
            pipeline_runtime_abi=str(value["pipeline_runtime_abi"]),
            environment_policy=str(value["environment_policy"]),
        )
        if (
            result.distribution != "demiurge"
            or result.pipeline_runtime_abi != PIPELINE_RUNTIME_ABI
            or not result.version
            or not result.wheel_filename.endswith(".whl")
            or not result.platform_requirements_filename.endswith(".txt")
            or re.fullmatch(r"[0-9a-f]{64}", result.wheel_sha256) is None
            or re.fullmatch(
                r"[0-9a-f]{64}", result.platform_requirements_sha256
            ) is None
            or len(set(result.platform_preflight_imports))
            != len(result.platform_preflight_imports)
            or any(
                re.fullmatch(
                    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*",
                    item,
                ) is None
                for item in result.platform_preflight_imports
            )
            or tuple(item.name for item in result.platform_wheels)
            != tuple(sorted({item.name for item in result.platform_wheels}))
            or result.environment_policy != "demiflow-python-env-v4"
        ):
            raise ValueError("Platform runtime identity is invalid")
        return result


@dataclass(frozen=True)
class PipelineExecutionEnvironment:
    runtime: PlatformRuntimeIdentity
    runtime_wheel: Path
    platform_requirements: Path
    local_environment_cache: Path
    dependency_setup_timeout_seconds: int
    planning_policy: Any
    ray_job_enabled: bool = True
    candidate_wheelhouse: Path | None = None
    platform_wheel_directory: Path | None = None

    def verify_artifacts(self) -> None:
        values = (
            (
                self.runtime_wheel,
                self.runtime.wheel_filename,
                self.runtime.wheel_sha256,
                "runtime wheel",
            ),
            (
                self.platform_requirements,
                self.runtime.platform_requirements_filename,
                self.runtime.platform_requirements_sha256,
                "platform requirements",
            ),
        )
        for configured, filename, expected_sha256, label in values:
            if configured.is_symlink():
                raise ValueError(f"Configured {label} is unavailable")
            path = configured.resolve()
            if not path.is_file() or path.name != filename:
                raise ValueError(f"Configured {label} is unavailable")
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256:
                raise ValueError(f"Configured {label} digest mismatch")
        from .requirements import platform_wheel_paths
        platform_wheel_paths(
            self.runtime.platform_wheels, self.platform_wheel_directory,
        )

    def declaration_id(self, requirements_sha256: str) -> str:
        payload = {
            "environment_policy": self.runtime.environment_policy,
            "runtime": self.runtime.to_dict(),
            "requirements_sha256": str(requirements_sha256 or ""),
            "python_cache_tag": str(sys.implementation.cache_tag or ""),
            "platform": sys.platform,
            "machine": platform.machine(),
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PipelineBundleRef:
    root: str
    content_digest: str
    runtime_abi: str = PIPELINE_RUNTIME_ABI

    @classmethod
    def load(cls, root: str | Path) -> "PipelineBundleRef":
        bundle_root = Path(root).expanduser().resolve()
        discover_pipeline_definition(bundle_root)
        return cls(
            root=str(bundle_root),
            content_digest=pipeline_bundle_digest(bundle_root),
        )

    def verify(self) -> Path:
        if self.runtime_abi != PIPELINE_RUNTIME_ABI:
            raise ValueError(
                f"unsupported Pipeline runtime ABI: {self.runtime_abi}"
            )
        root = Path(self.root).resolve()
        if pipeline_bundle_digest(root) != self.content_digest:
            raise ValueError("Pipeline bundle content digest mismatch")
        discover_pipeline_definition(root)
        return root

    @property
    def module_key(self) -> str:
        value = self.content_digest.removeprefix("sha256:")
        if not value or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError(
                "Pipeline bundle digest is not a canonical sha256 value"
            )
        return "p_" + value[:24]


@dataclass(frozen=True)
class PipelineRunRequest:
    run_id: str
    bundle: PipelineBundleRef
    target: Any

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("Pipeline run_id is required")
        from .target import PipelineExecutionTarget
        if not isinstance(self.target, PipelineExecutionTarget):
            raise TypeError("PipelineRunRequest requires PipelineExecutionTarget")


@dataclass(frozen=True)
class PipelineRunResult:
    run_id: str
    bundle_digest: str
    entrypoint: str
    status: str
    backend_type: str
    driver_id: str
    log_tail: tuple[str, ...] = ()
    logs_truncated: bool = False


@dataclass(frozen=True)
class PipelineRunReadiness:
    ready: bool
    bundle_digest: str
    entrypoint: str
    backend_type: str
    configured_timeout_seconds: int
    requirements_declared: bool
    requirements_sha256: str
    platform_requirements_sha256: str
    platform_preflight_imports: tuple[str, ...]
    environment_declaration_id: str
    environment_cache_status: str
    required_environment_names: tuple[str, ...]
    missing_environment_names: tuple[str, ...]
    issues: tuple[str, ...]


@dataclass(frozen=True)
class PipelineRunObservation:
    kind: str
    phase: str
    elapsed_ms: int
    event: str = ""
    action: str = ""
    action_phase: str = ""
    rows: int | None = None
    batches: int | None = None
    duration_ms: int | None = None
    backend_status: str = ""
    logs_truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in self.__dict__.items() if value not in ("", None)}


PipelineRunObserver = Callable[[PipelineRunObservation], None]


class PipelineBackend(Protocol):
    TYPE: str

    def run(
        self,
        request: PipelineRunRequest,
        *,
        timeout_seconds: int | None = None,
        cancellation: Any = None,
        observer: PipelineRunObserver | None = None,
    ) -> PipelineRunResult: ...


def pipeline_bundle_digest(root: str | Path) -> str:
    bundle_root = Path(root).expanduser().resolve()
    roots = [bundle_root / PIPELINE_PACKAGE_PATH]
    runtime = bundle_root / RUNTIME_PACKAGE_PATH
    if runtime.exists():
        if not runtime.is_dir():
            raise ValueError(f"Pipeline bundle runtime must be a directory: {runtime}")
        roots.append(runtime)
    digest = hashlib.sha256()
    paths = []
    for directory in roots:
        if directory.is_symlink():
            raise ValueError(f"Pipeline bundle directory symlinks are forbidden: {directory}")
        if not directory.is_dir():
            raise ValueError(f"Pipeline bundle directory is missing: {directory}")
        paths.extend(path for path in directory.rglob("*") if path.is_file())
    for path in sorted(set(paths)):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink():
            raise ValueError(f"Pipeline bundle symlinks are forbidden: {path}")
        relative = str(path.relative_to(bundle_root)).replace("\\", "/")
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return "sha256:" + digest.hexdigest()
