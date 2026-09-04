"""Pure, side-effect-free readiness inspection for one Pipeline bundle."""
from __future__ import annotations
import importlib.util
from typing import Mapping
from .contracts import PipelineBundleRef, PipelineExecutionEnvironment, PipelineRunReadiness
from .target import PipelineExecutionTarget
from .environment import missing_required_environment_names, prompt_environment_names
from .requirements import read_candidate_requirements
from .python_environment import inspect_local_environment_cache, uses_embedded_current_runtime
from ..pipeline import discover_pipeline_definition

def inspect_pipeline_run_readiness(bundle: PipelineBundleRef, execution_environment: PipelineExecutionEnvironment, target: PipelineExecutionTarget, *, environment: Mapping[str, str] | None = None) -> PipelineRunReadiness:
    root = bundle.verify()
    definition = discover_pipeline_definition(root)
    requirements = read_candidate_requirements(root)
    issues=[]
    try: execution_environment.verify_artifacts()
    except (OSError, ValueError): issues.append("platform_runtime_unavailable")
    required=prompt_environment_names(root)
    missing=missing_required_environment_names(required, environment)
    if missing: issues.append("required_environment_missing")
    cache="not_applicable"
    if target.backend == "local":
        if requirements.nonempty:
            # The cache key includes the frozen Candidate wheel resolution,
            # which zero-IO readiness does not receive or recompute.
            cache = "not_inspected"
        else:
            cache = (
                "ready"
                if uses_embedded_current_runtime(execution_environment)
                else inspect_local_environment_cache(execution_environment, requirements)
            )
        if cache == "invalid": issues.append("local_environment_cache_invalid")
    else:
        if not execution_environment.ray_job_enabled: issues.append("ray_connection_disabled")
        try: ray_available=importlib.util.find_spec("ray.job_submission") is not None
        except (ImportError,ModuleNotFoundError,AttributeError): ray_available=False
        if not ray_available: issues.append("ray_client_unavailable")
    return PipelineRunReadiness(
        not issues, bundle.content_digest, definition.entrypoint, target.backend,
        target.timeout_seconds, requirements.nonempty, requirements.sha256,
        execution_environment.runtime.platform_requirements_sha256,
        execution_environment.runtime.platform_preflight_imports,
        execution_environment.declaration_id(requirements.sha256), cache,
        tuple(required), missing, tuple(issues),
    )

__all__=["inspect_pipeline_run_readiness"]
