"""Strict trusted Demiflow execution configuration."""
from __future__ import annotations

import ast
from email.parser import Parser
import hashlib
from pathlib import Path
import re
from typing import Any, Mapping
import zipfile

from packaging.requirements import Requirement
from packaging.tags import sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename

from .execution.contracts import (
    PIPELINE_RUNTIME_ABI, PipelineExecutionEnvironment, PlatformRuntimeIdentity,
)
from .execution.requirements import (
    platform_requirement_names, read_platform_requirements,
    resolve_platform_wheels,
)
from .planning.policy import parse_platform_planning_policy


def parse_demiflow_config(
    value: Mapping[str, Any], *, component_path: str | Path,
) -> PipelineExecutionEnvironment:
    if not isinstance(value, Mapping):
        raise TypeError("demiflow config must be a mapping")
    if set(value) != {"schema_version", "execution", "planning", "connections"}:
        raise ValueError("demiflow config fields are incomplete or unsupported")
    if value.get("schema_version") != "demiflow_config_v5":
        raise ValueError("demiflow config requires schema_version: demiflow_config_v5")
    raw = value.get("execution")
    if not isinstance(raw, Mapping) or set(raw) != {
        "runtime_wheel", "platform_requirements", "platform_preflight_imports",
        "platform_wheels",
        "candidate_wheelhouse", "local_environment_cache", "dependency_setup_timeout_seconds",
    }:
        raise ValueError("demiflow execution config fields are incomplete or unsupported")
    owner = Path(component_path).resolve()
    wheel = _safe_path(owner, raw["runtime_wheel"], must_exist=True)
    platform_requirements = _safe_path(
        owner, raw["platform_requirements"], must_exist=True,
    )
    candidate_wheelhouse = _safe_path(owner, raw["candidate_wheelhouse"], must_exist=False)
    platform_wheel_directory, platform_wheel_packages = _platform_wheels(
        owner, raw["platform_wheels"],
    )
    cache = _safe_path(owner, raw["local_environment_cache"], must_exist=False)
    timeout = int(raw["dependency_setup_timeout_seconds"])
    if timeout <= 0:
        raise ValueError("dependency_setup_timeout_seconds must be positive")
    imports = _preflight_imports(raw["platform_preflight_imports"])
    identity = inspect_runtime_artifacts(
        wheel, platform_requirements, imports,
        platform_wheel_directory=platform_wheel_directory,
        platform_wheel_packages=platform_wheel_packages,
    )
    connections=value.get("connections")
    if not isinstance(connections,Mapping) or set(connections)!={"ray_job"}:
        raise ValueError("demiflow connections requires only ray_job")
    ray_job=connections["ray_job"]
    if not isinstance(ray_job,Mapping) or set(ray_job)!={"enabled"} or not isinstance(ray_job["enabled"],bool):
        raise ValueError("demiflow connections.ray_job requires boolean enabled")
    return PipelineExecutionEnvironment(
        runtime=identity,
        runtime_wheel=wheel,
        platform_requirements=platform_requirements,
        local_environment_cache=cache,
        dependency_setup_timeout_seconds=timeout,
        planning_policy=parse_platform_planning_policy(value.get("planning")),
        ray_job_enabled=ray_job["enabled"],
        candidate_wheelhouse=candidate_wheelhouse,
        platform_wheel_directory=platform_wheel_directory,
    )


def inspect_runtime_artifacts(
    wheel_path: str | Path,
    requirements_path: str | Path,
    preflight_imports: tuple[str, ...],
    *,
    platform_wheel_directory: str | Path | None = None,
    platform_wheel_packages: tuple[str, ...] = (),
) -> PlatformRuntimeIdentity:
    wheel_source = Path(wheel_path)
    if wheel_source.is_symlink() or not wheel_source.is_file():
        raise ValueError("runtime_wheel must be a regular .whl file")
    wheel = wheel_source.resolve()
    if wheel.suffix != ".whl":
        raise ValueError("runtime_wheel must be a regular .whl file")
    distribution, version, build, tags = parse_wheel_filename(wheel.name)
    if canonicalize_name(distribution) != "demiurge":
        raise ValueError("runtime_wheel distribution must be demiurge")
    if build:
        raise ValueError("configured runtime_wheel cannot contain a build tag")
    if not set(tags).intersection(sys_tags()):
        raise ValueError("runtime_wheel is incompatible with the current Python platform")
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        required = {
            "demiurge/demiflow/execution/driver.py",
            "demiurge/demiflow/execution/contracts.py",
        }
        if not required <= names:
            raise ValueError("runtime_wheel does not contain the Demiflow Driver contract")
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise ValueError("runtime_wheel requires exactly one METADATA file")
        metadata = Parser().parsestr(archive.read(metadata_names[0]).decode("utf-8"))
        if canonicalize_name(metadata.get("Name", "")) != "demiurge":
            raise ValueError("runtime_wheel METADATA name mismatch")
        if metadata.get("Version") != str(version):
            raise ValueError("runtime_wheel METADATA version mismatch")
        contracts = archive.read("demiurge/demiflow/execution/contracts.py").decode("utf-8")
        driver = archive.read("demiurge/demiflow/execution/driver.py").decode("utf-8")
        if _string_assignment(contracts, "PIPELINE_RUNTIME_ABI") != PIPELINE_RUNTIME_ABI:
            raise ValueError("runtime_wheel Pipeline ABI mismatch")
        driver_schema = _driver_schema(driver)
        if driver_schema != "demiflow_pipeline_driver_v9":
            raise ValueError("runtime_wheel Driver request schema mismatch")
        requirements = read_platform_requirements(requirements_path)
        platform_wheels = resolve_platform_wheels(
            requirements, directory=platform_wheel_directory,
            packages=platform_wheel_packages,
        )
        required_names = {
            canonicalize_name(Requirement(item).name)
            for item in metadata.get_all("Requires-Dist", [])
            if Requirement(item).marker is None
        }
        missing = sorted(required_names - platform_requirement_names(requirements.text))
        if missing:
            raise ValueError(
                "platform requirements do not cover runtime wheel dependencies: "
                + ", ".join(missing)
            )
        return PlatformRuntimeIdentity(
            distribution="demiurge",
            version=str(version),
            wheel_filename=wheel.name,
            wheel_sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
            platform_requirements_filename=requirements.bundle_relative_path,
            platform_requirements_sha256=requirements.sha256,
            platform_preflight_imports=preflight_imports,
            platform_wheels=platform_wheels,
            pipeline_runtime_abi=PIPELINE_RUNTIME_ABI,
            environment_policy="demiflow-python-env-v4",
        )


def _platform_wheels(owner: Path, value: Any) -> tuple[Path | None, tuple[str, ...]]:
    if not isinstance(value, Mapping) or set(value) != {"directory", "packages"}:
        raise ValueError("platform_wheels requires only directory and packages")
    packages = value["packages"]
    if not isinstance(packages, list) or any(not isinstance(item, str) for item in packages):
        raise ValueError("platform_wheels.packages must be a list of canonical names")
    canonical = tuple(canonicalize_name(item) for item in packages)
    if tuple(packages) != canonical or canonical != tuple(sorted(set(canonical))):
        raise ValueError("platform_wheels.packages must be sorted unique canonical names")
    raw_directory = value["directory"]
    if not canonical:
        if raw_directory is not None:
            raise ValueError("platform_wheels.directory must be null when packages is empty")
        return None, ()
    if raw_directory is None:
        raise ValueError("platform_wheels.directory is required when packages is non-empty")
    return _safe_directory(owner, raw_directory), canonical


def _preflight_imports(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("platform_preflight_imports must be a non-empty list")
    result = tuple(value)
    if any(
        not isinstance(item, str)
        or re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", item,
        ) is None
        for item in result
    ) or len(set(result)) != len(result):
        raise ValueError("platform_preflight_imports are invalid or duplicated")
    return result


def _safe_path(owner: Path, raw: Any, *, must_exist: bool) -> Path:
    relative = Path(str(raw or ""))
    if not relative.parts or relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ValueError("Demiflow execution paths must be safe relative paths")
    current = owner.parent
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Demiflow execution paths cannot traverse symlinks")
    result = current.resolve()
    result.relative_to(owner.parent)
    if must_exist and not result.is_file():
        raise ValueError(f"Demiflow execution file not found: {result}")
    return result


def _safe_directory(owner: Path, raw: Any) -> Path:
    relative = Path(str(raw or ""))
    if not relative.parts or relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ValueError("Demiflow execution paths must be safe relative paths")
    current = owner.parent
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Demiflow execution paths cannot traverse symlinks")
    result = current.resolve()
    result.relative_to(owner.parent)
    if not result.is_dir():
        raise ValueError(f"Demiflow execution directory not found: {result}")
    return result


def _string_assignment(source: str, name: str) -> str:
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return node.value.value
    return ""


def _driver_schema(source: str) -> str:
    values = set(re.findall(r'demiflow_pipeline_driver_v\d+', source))
    return next(iter(values)) if len(values) == 1 else ""


__all__ = ["inspect_runtime_artifacts", "parse_demiflow_config"]
