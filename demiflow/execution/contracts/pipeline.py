"""Minimal contracts for running one immutable Pipeline bundle."""
from __future__ import annotations

from packaging.utils import canonicalize_name, parse_wheel_filename

import hashlib
import json
import platform
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Protocol


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
class PipelineRunRequest:
    run_id: str
    bundle: object
    target: Any

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("Pipeline run_id is required")
        from demiflow.execution.contracts.target import PipelineExecutionTarget
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


NATIVE_SUBMISSION_FORMAT = 'demiflow.managed_native_submission.v1'
