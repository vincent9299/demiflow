"""Versioned engine protocol contracts.

The legacy pipeline bundle contract lives in ``pipeline.py`` and is re-exported
here so existing ``demiflow.execution.contracts`` imports keep working. Unified
protocol objects (K1-K6) live in the sibling modules; resource-management
objects remain owned by the future ``demiflow.resources`` contracts (module A)
and are only referenced here, never redefined.
"""
from importlib import import_module as _import_module


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(_import_module('.pipeline', __name__), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "PIPELINE_RUNTIME_ABI",
    "PlatformRuntimeIdentity",
    "PipelineBackend",
    "PipelineRunObservation",
    "PipelineRunObserver",
    "PipelineRunReadiness",
    "PipelineRunRequest",
    "PipelineRunResult",
]

_EXPORTS = {name: (".pipeline", name) for name in __all__}
