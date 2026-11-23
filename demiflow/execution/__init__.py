"""Execution facade with original exports resolved only on demand."""
from importlib import import_module as _import_module

_EXPORTS = {
    "PipelineBundleRef": (".pipeline.package_loader", "PipelineBundleRef"),
    "PipelineExecutionEnvironment": (".hosts.python_environment", "PipelineExecutionEnvironment"),
    **{name: (".contracts", name) for name in (
        "PipelineBackend",
        "PipelineRunObservation", "PipelineRunObserver", "PipelineRunReadiness",
        "PipelineRunRequest", "PipelineRunResult", "PlatformRuntimeIdentity",
    )},
    "inspect_pipeline_run_readiness": ('demiflow.execution.pipeline.readiness', "inspect_pipeline_run_readiness"),
    "PipelineExecutionTarget": ('demiflow.execution.contracts.target', "PipelineExecutionTarget"),
    **{name: ('demiflow.execution.pipeline.package_loader', name) for name in (
        "BundlePackages", "RUNTIME_PACKAGE_PATH", "load_bundle_packages",
    )},
    **{name: (".run_context", name) for name in ("RunContext", "current_run")},
    **{name: (".recovery_contract", name) for name in (
        "RunRef", "SourcePosition", "RecoveryContractError", "UnresolvedCommit",
    )},
}


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = _EXPORTS[name]
    value = getattr(_import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


def backend_for(bundle, execution_environment, target):
    from demiflow.execution.contracts.target import PipelineExecutionTarget
    del bundle
    if not isinstance(target, PipelineExecutionTarget):
        target = PipelineExecutionTarget.from_mapping(target)
    if target.backend == "local":
        from demiflow.execution.hosts.local import LocalPipelineBackend
        return LocalPipelineBackend(execution_environment)
    if target.backend == "ray":
        if not execution_environment.ray_job_enabled:
            raise ValueError("Ray Job connection capability is disabled")
        from demiflow.execution.hosts.ray import RayPipelineBackend
        return RayPipelineBackend(
            job_api_address=target.job_api_address,
            namespace=target.namespace,
            execution_environment=execution_environment,
        )
    raise ValueError(f"unsupported Pipeline backend: {target.backend!r}")


__all__ = [
    "RunContext", "RunRef", "current_run", "SourcePosition", "RecoveryContractError", "UnresolvedCommit",
    "BundlePackages", "RUNTIME_PACKAGE_PATH", "load_bundle_packages",
    "PipelineBackend", "PipelineBundleRef", "PipelineExecutionEnvironment",
    "PipelineRunObservation", "PipelineRunObserver", "PipelineRunReadiness", "PipelineRunRequest", "PipelineRunResult", "PipelineExecutionTarget", "PlatformRuntimeIdentity", "inspect_pipeline_run_readiness", "backend_for",
]


def prepare_candidate_environment(execution_environment, bundle_root, *, deadline_at, cancellation=None, resolution=None):
    """Prepare the exact local Candidate Python environment for trusted callers."""
    from .requirements import read_candidate_requirements
    from demiflow.execution.hosts.python_environment import prepare_local_environment
    return prepare_local_environment(
        execution_environment, read_candidate_requirements(bundle_root),
        deadline_at=deadline_at, cancellation=cancellation,
        resolution=resolution,
    )

__all__.append("prepare_candidate_environment")
