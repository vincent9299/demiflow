"""Load Python packages from an immutable Demiflow Pipeline bundle."""
from __future__ import annotations

import hashlib
import sys
import types
from dataclasses import dataclass
from pathlib import Path

from ..pipeline import PIPELINE_PACKAGE_PATH

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
