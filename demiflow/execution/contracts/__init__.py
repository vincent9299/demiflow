"""Versioned engine protocol contracts.

The legacy pipeline bundle contract lives in ``pipeline.py`` and is re-exported
here so existing ``demiflow.execution.contracts`` imports keep working. Unified
protocol objects (K1-K6) live in the sibling modules; resource-management
objects remain owned by the future ``demiflow.resources`` contracts (module A)
and are only referenced here, never redefined.
"""
from .pipeline import (
    PIPELINE_RUNTIME_ABI,
    PlatformRuntimeIdentity,
    PipelineBackend,
    PipelineBundleRef,
    PipelineExecutionEnvironment,
    PipelineRunObservation,
    PipelineRunObserver,
    PipelineRunReadiness,
    PipelineRunRequest,
    PipelineRunResult,
    pipeline_bundle_digest,
)

__all__ = [
    "PIPELINE_RUNTIME_ABI",
    "PlatformRuntimeIdentity",
    "PipelineBackend",
    "PipelineBundleRef",
    "PipelineExecutionEnvironment",
    "PipelineRunObservation",
    "PipelineRunObserver",
    "PipelineRunReadiness",
    "PipelineRunRequest",
    "PipelineRunResult",
    "pipeline_bundle_digest",
]