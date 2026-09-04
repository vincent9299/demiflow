from .contracts import (
    PipelineBackend, PipelineBundleRef, PipelineExecutionEnvironment,
    PipelineRunObservation, PipelineRunObserver, PipelineRunReadiness, PipelineRunRequest, PipelineRunResult, PlatformRuntimeIdentity,
)
from .readiness import inspect_pipeline_run_readiness
from .target import PipelineExecutionTarget
from .package_loader import BundlePackages, RUNTIME_PACKAGE_PATH, load_bundle_packages


def backend_for(bundle, execution_environment, target):
    del bundle
    if not isinstance(target, PipelineExecutionTarget):
        target = PipelineExecutionTarget.from_mapping(target)
    if target.backend == "local":
        from .local import LocalPipelineBackend
        return LocalPipelineBackend(execution_environment)
    if target.backend == "ray":
        if not execution_environment.ray_job_enabled:
            raise ValueError("Ray Job connection capability is disabled")
        from .ray import RayPipelineBackend
        return RayPipelineBackend(
            job_api_address=target.job_api_address,
            namespace=target.namespace,
            execution_environment=execution_environment,
        )
    raise ValueError(f"unsupported Pipeline backend: {target.backend!r}")


__all__ = [
    "BundlePackages", "RUNTIME_PACKAGE_PATH", "load_bundle_packages",
    "PipelineBackend", "PipelineBundleRef", "PipelineExecutionEnvironment",
    "PipelineRunObservation", "PipelineRunObserver", "PipelineRunReadiness", "PipelineRunRequest", "PipelineRunResult", "PipelineExecutionTarget", "PlatformRuntimeIdentity", "inspect_pipeline_run_readiness", "backend_for",
]


def prepare_candidate_environment(execution_environment, bundle_root, *, deadline_at, cancellation=None, resolution=None):
    """Prepare the exact local Candidate Python environment for trusted callers."""
    from .requirements import read_candidate_requirements
    from .python_environment import prepare_local_environment
    return prepare_local_environment(
        execution_environment, read_candidate_requirements(bundle_root),
        deadline_at=deadline_at, cancellation=cancellation,
        resolution=resolution,
    )

__all__.append("prepare_candidate_environment")
