"""Local isolated-process PipelineBackend."""
from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Mapping

from ..errors import PipelineRunCancelled, PipelineRunFailure, PipelineRunTimeout
from ..pipeline import discover_pipeline_definition
from .environment import pipeline_environment_names,prompt_environment_names,select_pipeline_environment
from .contracts import PipelineRunObservation, PipelineRunRequest, PipelineRunResult
from .driver import PipelineDriverRequest
from demiflow._compat.error_transport import make_error, validate_error
from .observation import emit_observation, parse_pipeline_log_observation
from ..planning.policy import BUILT_IN_RULE_VERSION


class LocalPipelineBackend:
    TYPE = "local"

    def __init__(self, execution_environment):
        self.execution_environment = execution_environment

    def run(
        self, request: PipelineRunRequest, *, timeout_seconds=None,
        cancellation=None, observer=None,
    ) -> PipelineRunResult:
        started = time.monotonic()
        try:
            result = self._run_impl(
                request, timeout_seconds=timeout_seconds,
                cancellation=cancellation, observer=observer,
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
            emit_observation(observer, PipelineRunObservation(
                "terminal", "terminal", round((time.monotonic()-started)*1000),
                backend_status=status if "status" in locals() else "FAILED",
                logs_truncated=(result.logs_truncated if "result" in locals() else False),
            ))

    def _run_impl(
        self, request: PipelineRunRequest, *, timeout_seconds=None,
        cancellation=None, observer=None,
    ) -> PipelineRunResult:
        started = time.monotonic()
        request.bundle.verify()
        definition = discover_pipeline_definition(Path(request.bundle.root))
        execution = definition.execution
        if request.target.backend != self.TYPE:
            raise ValueError("PipelineExecutionTarget is not local")
        if execution.backend_affinity and execution.backend_affinity != self.TYPE:
            raise ValueError("Pipeline native affinity is incompatible with local target")
        operator_environment = select_pipeline_environment(
            pipeline_environment_names(request.bundle.root),
            required_names=prompt_environment_names(request.bundle.root),
        )
        from .requirements import read_candidate_requirements
        from .python_environment import prepare_local_environment
        requirements = read_candidate_requirements(request.bundle.root)
        from .requirements import read_platform_requirements, read_candidate_environment_lock
        platform = read_platform_requirements(self.execution_environment.platform_requirements)
        lock_path = Path(request.bundle.root) / "pipeline/environment-lock.json"
        resolution = (
            read_candidate_environment_lock(
                request.bundle.root, platform, requirements,
                wheelhouse=self.execution_environment.candidate_wheelhouse,
            )
            if requirements.nonempty or lock_path.is_file()
            else None
        )
        effective_timeout = min(request.target.timeout_seconds, int(timeout_seconds)) if timeout_seconds is not None else request.target.timeout_seconds
        deadline_at = time.monotonic() + effective_timeout
        setup_deadline = min(
            deadline_at,
            time.monotonic() + self.execution_environment.dependency_setup_timeout_seconds,
        )
        emit_observation(observer, PipelineRunObservation("phase", "preparing_environment", 0))
        python = prepare_local_environment(
            self.execution_environment, requirements,
            deadline_at=setup_deadline, cancellation=cancellation,
            resolution=resolution,
        )
        driver = PipelineDriverRequest(
            run_id=request.run_id,
            bundle=request.bundle,
            bundle_root=request.bundle.root,
            backend=self.TYPE,
            platform_preflight_imports=(
                self.execution_environment.runtime.platform_preflight_imports
            ),
            planning_policy=self.execution_environment.planning_policy.to_dict(),
            planning_policy_digest=self.execution_environment.planning_policy.digest,
            planning_rule_version=BUILT_IN_RULE_VERSION,
        )
        remaining = math.ceil(deadline_at - time.monotonic())
        if remaining <= 0:
            raise PipelineRunTimeout("pipeline run timed out during environment setup")
        process_result = _run_isolated(
            driver.to_dict(),
            timeout_seconds=remaining,
            cancellation=cancellation,
            operator_environment=operator_environment,
            python_executable=str(python), observer=observer,
        )
        entrypoint = definition.entrypoint
        return PipelineRunResult(
            run_id=request.run_id,
            bundle_digest=request.bundle.content_digest,
            entrypoint=entrypoint,
            status="SUCCEEDED",
            backend_type=self.TYPE,
            driver_id=process_result["driver_id"],
            log_tail=process_result["log_tail"],
            logs_truncated=process_result["logs_truncated"],
        )


def _run_isolated(
    request: Mapping[str, Any], *, timeout_seconds: int,
    cancellation: Any = None, operator_environment: Mapping[str, str] | None = None,
    termination_grace_seconds: int = 2,
    output_limit_bytes: int = 1_000_000,
    python_executable: str | None = None, observer=None,
) -> dict[str, Any]:
    payload = json.dumps(dict(request), ensure_ascii=False, default=str).encode("utf-8")
    command = (python_executable or sys.executable, "-m", "demiflow.execution.driver")
    started = time.monotonic()
    with tempfile.TemporaryFile() as stdout_file:
        inherited_names = (
            "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL",
            "DEMIURGE_LOG_LEVEL", "DEMIURGE_LOG_FORMAT", "DEMIURGE_LOG_JSON",
        )
        child_environment = {
            name: os.environ[name] for name in inherited_names
            if name in os.environ
        }
        child_environment.update(dict(operator_environment or {}))
        child_environment.setdefault("DEMIURGE_LOG_LEVEL", "INFO")
        child_environment.setdefault("DEMIURGE_LOG_FORMAT", "json")
        bundle_root = Path(str(request["bundle_root"])).resolve()
        emit_observation(observer, PipelineRunObservation(
            "phase", "submitting_driver", round((time.monotonic()-started)*1000),
        ))
        process = subprocess.Popen(
            command, cwd=bundle_root,
            stdin=subprocess.PIPE, stdout=stdout_file,
            stderr=subprocess.PIPE, start_new_session=True, env=child_environment,
        )
        stderr_chunks: list[bytes] = []
        stderr_size = [0]
        thread = threading.Thread(
            target=_forward_stderr,
            args=(process.stderr, stderr_chunks, stderr_size, output_limit_bytes, observer, started),
            daemon=True,
        )
        thread.start()
        emit_observation(observer, PipelineRunObservation(
            "phase", "running_driver", round((time.monotonic()-started)*1000),
        ))
        assert process.stdin is not None
        process.stdin.write(payload)
        process.stdin.close()
        deadline = started + max(1, int(timeout_seconds))
        while True:
            if bool(getattr(cancellation, "requested", False)):
                emit_observation(observer, PipelineRunObservation(
                    "phase", "stopping", round((time.monotonic()-started)*1000),
                ))
                _terminate(process, termination_grace_seconds)
                raise PipelineRunCancelled(
                    str(getattr(cancellation, "reason", "cancelled"))
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                emit_observation(observer, PipelineRunObservation(
                    "phase", "stopping", round((time.monotonic()-started)*1000),
                ))
                _terminate(process, termination_grace_seconds)
                raise PipelineRunTimeout("pipeline run timed out")
            try:
                process.wait(timeout=min(remaining, 0.25))
                break
            except subprocess.TimeoutExpired:
                continue
        thread.join(timeout=2)
        stdout_file.seek(0)
        stdout = stdout_file.read(output_limit_bytes + 1)
        stderr = b"".join(stderr_chunks)[-output_limit_bytes:]
        if process.returncode != 0:
            raise PipelineRunFailure(_fallback_error(stderr or stdout))
        if len(stdout) > output_limit_bytes:
            raise PipelineRunFailure("pipeline process output exceeded limit")
        return {
            "driver_id": f"pid:{process.pid}",
            "log_tail": tuple(
                stderr.decode("utf-8", errors="replace").splitlines()
            ),
            "logs_truncated": stderr_size[0] > output_limit_bytes,
        }




def _fallback_error(data: bytes):
    text = data.decode("utf-8", errors="replace")
    for line in reversed(text.splitlines()):
        try:
            value = json.loads(line)
            return validate_error(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return make_error(
        module="demiflow.execution.local",
        type_name="ProcessFailure", message=text,
    )

def _terminate(process, grace):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=max(1, int(grace)))
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
    process.wait()


def _forward_stderr(stream, chunks, size, limit, observer=None, started=None):
    if stream is None:
        return
    for line in iter(stream.readline, b""):
        try:
            sys.stderr.buffer.write(line)
            sys.stderr.buffer.flush()
        except Exception:
            pass
        if started is not None:
            value = parse_pipeline_log_observation(
                line, elapsed_ms=round((time.monotonic()-started)*1000),
            )
            if value is not None:
                emit_observation(observer, value)
        chunks.append(line)
        size[0] += len(line)
        while chunks and size[0] > limit: size[0] -= len(chunks.pop(0))
