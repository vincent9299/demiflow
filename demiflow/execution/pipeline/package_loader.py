"""Load Python packages from an immutable Demiflow Pipeline bundle."""
from __future__ import annotations

from demiflow.execution.contracts.pipeline import PipelineBundleRef, PIPELINE_RUNTIME_ABI
from demiflow.pipeline import discover_pipeline_definition

import hashlib
import sys
import types
from dataclasses import dataclass
from pathlib import Path

from demiflow.pipeline import PIPELINE_PACKAGE_PATH

RUNTIME_PACKAGE_PATH = "runtime"


@dataclass(frozen=True)
class BundlePackages:
    namespace: str
    runtime_package: str
    pipeline_package: str


def load_bundle_packages(
    bundle_root: str | Path, *, namespace: str | None = None,
) -> BundlePackages:
    root = Path(bundle_root).expanduser().resolve()
    pipeline_dir = root / PIPELINE_PACKAGE_PATH
    runtime_dir = root / RUNTIME_PACKAGE_PATH
    digest = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:16]
    namespace = namespace or f"_demiurge_bundles.bundle_{digest}"
    parent = namespace.split(".", 1)[0]
    if parent not in sys.modules:
        module = types.ModuleType(parent)
        module.__path__ = [] # type: ignore[attr-defined]
        sys.modules[parent] = module
    if namespace not in sys.modules:
        module = types.ModuleType(namespace)
        module.__path__ = [str(root)] # type: ignore[attr-defined]
        module.__package__ = namespace
        sys.modules[namespace] = module

    runtime_package = f"{namespace}.runtime"
    pipeline_package = f"{namespace}.pipeline"
    _register_package(pipeline_package, pipeline_dir)
    return BundlePackages(namespace, runtime_package, pipeline_package)


def _register_package(name: str, directory: Path) -> None:
    init = directory / "__init__.py"
    if not init.is_file():
        raise ValueError(f"Pipeline bundle package requires __init__.py: {init}")
    existing = sys.modules.get(name)
    if (
        existing is not None
        and Path(getattr(existing, "__file__", "")).resolve() == init.resolve()
    ):
        return
    module = types.ModuleType(name)
    module.__file__ = str(init)
    module.__path__ = [str(directory)] # type: ignore[attr-defined]
    module.__package__ = name
    sys.modules[name] = module


__all__ = ["BundlePackages", "RUNTIME_PACKAGE_PATH", "load_bundle_packages"]


def load_bundle_ref(root: str | Path) -> "PipelineBundleRef":
    bundle_root = Path(root).expanduser().resolve()
    discover_pipeline_definition(bundle_root)
    return PipelineBundleRef(
        root=str(bundle_root),
        content_digest=pipeline_bundle_digest(bundle_root),
    )


def verify_bundle(bundle) -> Path:
    if bundle.runtime_abi != PIPELINE_RUNTIME_ABI:
        raise ValueError(
            f"unsupported Pipeline runtime ABI: {bundle.runtime_abi}"
        )
    root = Path(bundle.root).resolve()
    if pipeline_bundle_digest(root) != bundle.content_digest:
        raise ValueError("Pipeline bundle content digest mismatch")
    discover_pipeline_definition(root)
    return root


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
