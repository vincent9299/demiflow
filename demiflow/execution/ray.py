"""Ray Jobs status-only PipelineBackend."""

from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path

from ..errors import (
    PipelineRunCancelled,
    PipelineRunFailure,
    PipelineRunIndeterminate,
    PipelineRunTimeout,
)
from ..operator_llm import load_referenced_prompt_packs
from ..pipeline import discover_pipeline_definition
from .contracts import (
    PipelineBundleRef,
    PipelineRunObservation,
    PipelineRunRequest,
    PipelineRunResult,
)
from .driver import PipelineDriverRequest
from .requirements import (
    PLATFORM_WHEEL_BUNDLE_DIRECTORY,
    FrozenRequirements,
    file_sha256,
    platform_wheel_paths,
    read_candidate_requirements,
    read_platform_requirements,
    render_effective_requirements,
    resolved_wheel_paths,
    write_effective_requirements,
)
from .observation import emit_observation, parse_pipeline_log_observation
from .environment import (
    pipeline_environment_names,
    prompt_environment_names,
    select_pipeline_environment,
)
from ..planning.policy import BUILT_IN_RULE_VERSION

_MAX_LOG_BYTES = 1_000_000
_RAY_WORKING_DIR_REFERENCE = "file://${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}"


class RayPipelineBackend:
    TYPE = "ray"

    def __init__(
        self, *, job_api_address: str, namespace: str = "", execution_environment=None
    ):
        if execution_environment is None:
            raise ValueError("RayPipelineBackend requires execution_environment")
        self.execution_environment = execution_environment
        self.job_api_address = str(job_api_address or "").strip()
        self.namespace = str(namespace or "").strip()
        if not self.job_api_address:
            raise ValueError("RayPipelineBackend requires job_api_address")

    def run(
        self,
        request: PipelineRunRequest,
        *,
        timeout_seconds=None,
        cancellation=None,
        observer=None,
    ) -> PipelineRunResult:
        started = time.monotonic()
        try:
            result = self._run_impl(
                request,
                timeout_seconds=timeout_seconds,
                cancellation=cancellation,
                observer=observer,
            )
        except PipelineRunCancelled:
            status = "CANCELLED"
            raise
        except PipelineRunTimeout:
            status = "TIMED_OUT"
            raise
        except Exception:
            status = "FAILED"
            raise
        else:
            status = result.status
            return result
        finally:
            emit_observation(
                observer,
                PipelineRunObservation(
                    "terminal",
                    "terminal",
                    round((time.monotonic() - started) * 1000),
                    backend_status=status if "status" in locals() else "FAILED",
                    logs_truncated=(
                        result.logs_truncated if "result" in locals() else False
                    ),
                ),
            )

    def _run_impl(
        self,
        request: PipelineRunRequest,
        *,
        timeout_seconds=None,
        cancellation=None,
        observer=None,
    ) -> PipelineRunResult:
        started = time.monotonic()
        emit_observation(
            observer, PipelineRunObservation("phase", "preparing_environment", 0)
        )
        request.bundle.verify()
        definition = discover_pipeline_definition(Path(request.bundle.root))
        execution = definition.execution
        if request.target.backend != self.TYPE:
            raise ValueError("PipelineExecutionTarget is not Ray")
        if (
            self.job_api_address != request.target.job_api_address
            or self.namespace != request.target.namespace
        ):
            raise ValueError("Ray backend does not match PipelineExecutionTarget")
        if execution.backend_affinity and execution.backend_affinity != self.TYPE:
            raise ValueError("Pipeline native affinity is incompatible with Ray target")
        operator_environment = select_pipeline_environment(
            pipeline_environment_names(request.bundle.root),
            required_names=prompt_environment_names(request.bundle.root),
        )
        try:
            from ray.job_submission import JobStatus, JobSubmissionClient
        except ModuleNotFoundError as exc:
            raise RuntimeError("Ray execution requires ray[default,data]") from exc

        bundle_dir = Path(tempfile.mkdtemp(prefix="demiflow-ray-job-"))
        submission_id = f"demiflow-{request.run_id}"
        try:
            driver = _prepare_bundle(
                request,
                bundle_dir,
                self.namespace,
                self.execution_environment.runtime.platform_preflight_imports,
                self.execution_environment.planning_policy,
            )
            (bundle_dir / "driver.json").write_text(
                json.dumps(driver.to_dict()),
                encoding="utf-8",
            )
            requirements = read_candidate_requirements(bundle_dir / driver.bundle_root)
            from .requirements import read_candidate_environment_lock

            lock_path = (
                bundle_dir / driver.bundle_root / "pipeline/environment-lock.json"
            )
            resolution = (
                read_candidate_environment_lock(
                    bundle_dir / driver.bundle_root,
                    read_platform_requirements(
                        self.execution_environment.platform_requirements
                    ),
                    requirements,
                    wheelhouse=self.execution_environment.candidate_wheelhouse,
                )
                if requirements.nonempty or lock_path.is_file()
                else None
            )
            self.execution_environment.verify_artifacts()
            platform_requirements = read_platform_requirements(
                self.execution_environment.platform_requirements
            )
            platform_references = _materialize_ray_platform_wheels(
                self.execution_environment,
                bundle_dir,
            )
            candidate_references = None
            if requirements.nonempty:
                if resolution is None:
                    raise ValueError(
                        "Candidate requirements require a frozen environment resolution"
                    )
                wheel_dir = bundle_dir / "candidate-wheels"
                wheel_dir.mkdir()
                references = []
                for wheel, source in zip(
                    resolution.wheels,
                    resolved_wheel_paths(
                        resolution,
                        self.execution_environment.candidate_wheelhouse,
                    ),
                ):
                    target = wheel_dir / wheel.sha256 / source.name
                    target.parent.mkdir(exist_ok=True)
                    shutil.copy2(source, target)
                    if file_sha256(target) != wheel.sha256:
                        raise ValueError(
                            f"copied Candidate wheel digest mismatch: {wheel.filename}"
                        )
                    references.append(
                        f"{_RAY_WORKING_DIR_REFERENCE}/candidate-wheels/"
                        f"{wheel.sha256}/{target.name}"
                    )
                candidate_references = tuple(references)
            effective_requirements = write_effective_requirements(
                render_effective_requirements(
                    platform_requirements,
                    requirements,
                    platform_wheel_references=platform_references,
                    candidate_wheel_references=candidate_references,
                ),
                bundle_dir / "effective-requirements.txt",
            )
            runtime_wheel = _materialize_ray_runtime_wheel(
                self.execution_environment,
                bundle_dir,
            )
            client = JobSubmissionClient(self.job_api_address)
            existing = next(
                (
                    job.status
                    for job in client.list_jobs()
                    if job.submission_id == submission_id
                ),
                None,
            )
            driver_environment = dict(operator_environment)
            driver_environment.setdefault("DEMIURGE_LOG_LEVEL", "INFO")
            driver_environment.setdefault("DEMIURGE_LOG_FORMAT", "json")
            runtime_env = {
                "working_dir": str(bundle_dir),
                "py_modules": [str(runtime_wheel)],
                "env_vars": driver_environment,
                "config": {
                    "setup_timeout_seconds": self.execution_environment.dependency_setup_timeout_seconds
                },
            }
            runtime_env["pip"] = str(effective_requirements)
            emit_observation(
                observer,
                PipelineRunObservation(
                    "phase",
                    "submitting_driver",
                    round((time.monotonic() - started) * 1000),
                ),
            )
            if existing is None:
                client.submit_job(
                    entrypoint=(
                        "python -m demiflow.execution.driver "
                        "--request driver.json"
                    ),
                    submission_id=submission_id,
                    runtime_env=runtime_env,
                    metadata={
                        "demiflow_run_id": request.run_id,
                        "bundle_digest": request.bundle.content_digest,
                        "platform_runtime_sha256": self.execution_environment.runtime.wheel_sha256,
                        "platform_requirements_sha256": self.execution_environment.runtime.platform_requirements_sha256,
                        "requirements_sha256": requirements.sha256,
                        "environment_declaration_id": self.execution_environment.declaration_id(
                            resolution.resolution_digest
                            if resolution is not None
                            else requirements.sha256
                        ),
                    },
                )
            emit_observation(
                observer,
                PipelineRunObservation(
                    "phase",
                    "running_driver",
                    round((time.monotonic() - started) * 1000),
                ),
            )
            status = _wait_for_job(
                client,
                submission_id,
                (
                    min(request.target.timeout_seconds, int(timeout_seconds))
                    if timeout_seconds is not None
                    else request.target.timeout_seconds
                ),
                cancellation,
                JobStatus,
                observer=observer,
                started=started,
            )
            logs, truncated = _bounded_logs(client.get_job_logs(submission_id))
            if status != JobStatus.SUCCEEDED and not logs:
                detail = _job_failure_detail(client, submission_id)
                if detail:
                    logs, detail_truncated = _bounded_logs(detail)
                    truncated = truncated or detail_truncated
            for line in logs:
                value = parse_pipeline_log_observation(
                    line,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                )
                if value is not None:
                    emit_observation(observer, value)
            if status == JobStatus.STOPPED:
                raise PipelineRunCancelled(
                    f"Ray Pipeline Job {status}: " + "\n".join(logs)
                )
            if status != JobStatus.SUCCEEDED:
                raise PipelineRunFailure(
                    f"Ray Pipeline Job {status}: " + "\n".join(logs)
                )
            entrypoint = definition.entrypoint
            return PipelineRunResult(
                run_id=request.run_id,
                bundle_digest=request.bundle.content_digest,
                entrypoint=entrypoint,
                status=_status_value(status),
                backend_type=self.TYPE,
                driver_id=submission_id,
                log_tail=logs,
                logs_truncated=truncated,
            )
        finally:
            shutil.rmtree(bundle_dir, ignore_errors=True)


def _materialize_ray_runtime_wheel(execution_environment, job_root: Path) -> Path:
    source = execution_environment.runtime_wheel
    digest = execution_environment.runtime.wheel_sha256
    prefix, python_tag, abi_tag, platform_tag = source.name[:-4].rsplit("-", 3)
    target = job_root / (
        f"{prefix}-1{digest[:12]}-{python_tag}-{abi_tag}-{platform_tag}.whl"
    )
    shutil.copyfile(source, target)
    if file_sha256(target) != digest:
        raise ValueError("copied Demiurge runtime wheel digest mismatch")
    return target


def _materialize_ray_platform_wheels(
    execution_environment, job_root: Path
) -> dict[str, str]:
    wheels = execution_environment.runtime.platform_wheels
    sources = platform_wheel_paths(
        wheels,
        execution_environment.platform_wheel_directory,
    )
    if not wheels:
        return {}
    directory = job_root / PLATFORM_WHEEL_BUNDLE_DIRECTORY
    directory.mkdir()
    references = {}
    for wheel, source in zip(wheels, sources):
        target = directory / wheel.sha256 / wheel.filename
        target.parent.mkdir(exist_ok=True)
        shutil.copy2(source, target)
        if file_sha256(target) != wheel.sha256:
            raise ValueError(f"copied platform wheel digest mismatch: {wheel.filename}")
        references[wheel.name] = (
            f"{_RAY_WORKING_DIR_REFERENCE}/"
            f"{PLATFORM_WHEEL_BUNDLE_DIRECTORY}/{wheel.sha256}/{wheel.filename}"
        )
    return references


def _prepare_bundle(
    request, bundle_dir, namespace, platform_preflight_imports, planning_policy
):
    package = bundle_dir / "_demiurge_pipelines"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    bundle_root = package / request.bundle.module_key
    shutil.copytree(request.bundle.root, bundle_root)
    relative_root = str(bundle_root.relative_to(bundle_dir))
    bundled = PipelineBundleRef(
        relative_root,
        request.bundle.content_digest,
        request.bundle.runtime_abi,
    )
    return PipelineDriverRequest(
        run_id=request.run_id,
        bundle=bundled,
        bundle_root=relative_root,
        backend="ray",
        namespace=namespace,
        bundle_namespace=f"_demiurge_pipelines.{request.bundle.module_key}",
        platform_preflight_imports=tuple(platform_preflight_imports),
        planning_policy=planning_policy.to_dict(),
        planning_policy_digest=planning_policy.digest,
        planning_rule_version=BUILT_IN_RULE_VERSION,
    )


def _wait_for_job(
    client,
    submission_id,
    timeout_seconds,
    cancellation,
    status_type,
    *,
    observer=None,
    started=None,
    heartbeat_seconds=30.0,
):
    deadline = time.monotonic() + timeout_seconds
    terminal = {
        status_type.SUCCEEDED,
        status_type.FAILED,
        status_type.STOPPED,
    }
    last_heartbeat = time.monotonic()
    while True:
        if bool(getattr(cancellation, "requested", False)):
            emit_observation(
                observer,
                PipelineRunObservation(
                    "phase",
                    "stopping",
                    round((time.monotonic() - (started or time.monotonic())) * 1000),
                ),
            )
            _stop_and_confirm(client, submission_id, status_type)
            raise PipelineRunCancelled(
                str(getattr(cancellation, "reason", "cancelled"))
            )
        if time.monotonic() >= deadline:
            emit_observation(
                observer,
                PipelineRunObservation(
                    "phase",
                    "stopping",
                    round((time.monotonic() - (started or time.monotonic())) * 1000),
                ),
            )
            _stop_and_confirm(client, submission_id, status_type)
            raise PipelineRunTimeout("Ray Pipeline Job timed out")
        status = client.get_job_status(submission_id)
        now = time.monotonic()
        if now - last_heartbeat >= heartbeat_seconds:
            emit_observation(
                observer,
                PipelineRunObservation(
                    "heartbeat",
                    "running_driver",
                    round((now - (started or now)) * 1000),
                    backend_status=_status_value(status),
                ),
            )
            last_heartbeat = now
        if status in terminal:
            return status
        time.sleep(0.5)


def _stop_and_confirm(client, submission_id, status_type, *, timeout_seconds=30.0):
    try:
        client.stop_job(submission_id)
    except Exception as exc:
        raise PipelineRunIndeterminate(
            f"Ray stop request failed: {exc}",
            platform_execution_id=submission_id,
        ) from exc
    deadline = time.monotonic() + max(1.0, float(timeout_seconds))
    terminal = {status_type.SUCCEEDED, status_type.FAILED, status_type.STOPPED}
    while time.monotonic() < deadline:
        try:
            status = client.get_job_status(submission_id)
        except Exception as exc:
            raise PipelineRunIndeterminate(
                f"Ray terminal status query failed: {exc}",
                platform_execution_id=submission_id,
            ) from exc
        if status in terminal:
            return status
        time.sleep(0.5)
    raise PipelineRunIndeterminate(
        "Ray stop outcome remains unknown",
        platform_execution_id=submission_id,
    )


def _job_failure_detail(client, submission_id) -> str:
    try:
        info = client.get_job_info(submission_id)
    except Exception:
        return ""
    message = getattr(info, "message", "")
    return str(message or "")


def _bounded_logs(value: str) -> tuple[tuple[str, ...], bool]:
    raw = str(value or "").encode("utf-8", errors="replace")
    truncated = len(raw) > _MAX_LOG_BYTES
    if truncated:
        raw = raw[-_MAX_LOG_BYTES:]
    return tuple(raw.decode("utf-8", errors="replace").splitlines()), truncated


def _status_value(status) -> str:
    return str(getattr(status, "value", status))
