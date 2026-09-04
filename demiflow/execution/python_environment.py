"""Trusted preparation of Candidate Python dependency environments."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from ..errors import PipelineRunCancelled, PipelineRunTimeout

from .requirements import (
    FrozenRequirements,
    platform_wheel_paths,
    read_platform_requirements,
    render_effective_requirements,
    resolved_wheel_paths,
    write_effective_requirements,
)


def inspect_local_environment_cache(
    execution_environment, requirements: FrozenRequirements
) -> str:
    environment_id = execution_environment.declaration_id(requirements.sha256)
    target = execution_environment.local_environment_cache.resolve() / environment_id
    python = target / "venv" / "bin" / "python"
    marker = target / "environment.json"
    if not marker.is_file() or not python.is_file():
        return "missing"
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return "invalid"
    return (
        "ready"
        if value == _marker(execution_environment, requirements, environment_id)
        else "invalid"
    )


def prepare_local_environment(
    execution_environment,
    requirements: FrozenRequirements,
    *,
    deadline_at: float,
    cancellation=None,
    resolution=None,
) -> Path:
    execution_environment.verify_artifacts()
    if (
        not requirements.nonempty
        and resolution is None
        and uses_embedded_current_runtime(execution_environment)
    ):
        return Path(sys.executable).resolve()
    resolution_digest = str(getattr(resolution, "resolution_digest", "") or "")
    environment_id = execution_environment.declaration_id(
        resolution_digest or requirements.sha256
    )
    cache = execution_environment.local_environment_cache.resolve()
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / environment_id
    lock_path = cache / f"{environment_id}.lock"
    with lock_path.open("a+") as lock:
        _acquire_lock(lock, deadline_at, cancellation)
    python = target / "venv" / "bin" / "python"
    marker = target / "environment.json"
    expected = _marker(
        execution_environment, requirements, environment_id, resolution_digest
    )
    if marker.is_file() and python.is_file():
        if json.loads(marker.read_text(encoding="utf-8")) != expected:
            raise ValueError("Cached Pipeline environment identity mismatch")
        return python
    if target.exists():
        if marker.exists():
            raise ValueError("Cached Pipeline environment is incomplete")
        shutil.rmtree(target)
    target.mkdir(mode=0o700)
    try:
        venv = target / "venv"
        _run(
            [sys.executable, "-m", "virtualenv", "--no-download", str(venv)],
            deadline_at,
            cancellation,
            environment_mode="control_runtime",
        )
        python = venv / "bin" / "python"
        platform = read_platform_requirements(
            execution_environment.platform_requirements,
        )
        platform_paths = platform_wheel_paths(
            execution_environment.runtime.platform_wheels,
            execution_environment.platform_wheel_directory,
        )
        platform_references = {
            wheel.name: path.as_uri()
            for wheel, path in zip(
                execution_environment.runtime.platform_wheels,
                platform_paths,
            )
        }
        candidate_references = None
        if requirements.nonempty:
            if resolution is None:
                raise ValueError(
                    "Candidate requirements require a frozen environment resolution"
                )
            candidate_references = tuple(
                path.as_uri()
                for path in resolved_wheel_paths(
                    resolution,
                    execution_environment.candidate_wheelhouse,
                )
            )
        effective = write_effective_requirements(
            render_effective_requirements(
                platform,
                requirements,
                platform_wheel_references=platform_references,
                candidate_wheel_references=candidate_references,
            ),
            target / "effective-requirements.txt",
        )
        command = [
            str(python),
            "-m",
            "pip",
            "install",
            "-r",
            str(effective),
        ]
        _run(command, deadline_at, cancellation)
        _run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--force-reinstall",
                str(execution_environment.runtime_wheel),
            ],
            deadline_at,
            cancellation,
        )
        _atomic_marker(
            marker,
            json.dumps(expected, sort_keys=True, separators=(",", ":")),
        )
        return python
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise


def uses_embedded_current_runtime(execution_environment) -> bool:
    prefix = Path(sys.prefix).resolve()
    values = (
        Path(execution_environment.runtime_wheel).resolve(),
        Path(execution_environment.platform_requirements).resolve(),
    )
    try:
        for value in values:
            value.relative_to(prefix)
    except ValueError:
        return False
    return all(value.is_file() and not value.is_symlink() for value in values)


def _atomic_marker(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _marker(environment, requirements, environment_id, resolution_digest=""):
    return {
        "schema_version": "demiflow_python_environment_v4",
        "environment_id": environment_id,
        "platform_runtime": environment.runtime.to_dict(),
        "requirements_path": requirements.bundle_relative_path,
        "requirements_sha256": requirements.sha256,
        "environment_resolution_digest": resolution_digest,
    }


def _acquire_lock(handle, deadline_at, cancellation):
    while True:
        if bool(getattr(cancellation, "requested", False)):
            raise PipelineRunCancelled(
                str(getattr(cancellation, "reason", "cancelled"))
            )
        if time.monotonic() >= deadline_at:
            raise PipelineRunTimeout("Pipeline dependency environment lock timed out")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            time.sleep(0.1)


def _run(
    command,
    deadline_at,
    cancellation,
    cwd=None,
    output_limit=64_000,
    environment_mode="isolated",
):
    with (
        tempfile.TemporaryFile() as stdout_file,
        tempfile.TemporaryFile() as stderr_file,
    ):
        environment = dict(os.environ)
        if environment_mode == "isolated":
            environment.pop("PYTHONPATH", None)
            environment.pop("PYTHONHOME", None)
        environment["PYTHONNOUSERSITE"] = "1"
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=stdout_file,
            stderr=stderr_file,
            start_new_session=True,
            env=environment,
        )
        try:
            while True:
                if bool(getattr(cancellation, "requested", False)):
                    _terminate(process)
                    raise PipelineRunCancelled(
                        str(getattr(cancellation, "reason", "cancelled"))
                    )
                remaining = deadline_at - time.monotonic()
                if remaining <= 0:
                    _terminate(process)
                    raise PipelineRunTimeout(
                        "Pipeline dependency environment setup timed out"
                    )
                try:
                    process.wait(timeout=min(0.25, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
            stdout, stdout_truncated = _read_bounded(stdout_file, output_limit)
            stderr, stderr_truncated = _read_bounded(stderr_file, output_limit)
            if process.returncode != 0:
                detail = (stderr or stdout).decode(errors="replace")
                truncation = (
                    " [output truncated]"
                    if stderr_truncated or stdout_truncated
                    else ""
                )
                raise RuntimeError(
                    f"Dependency command failed with exit code {process.returncode}{truncation}: {detail}"
                )
        finally:
            if process.poll() is None:
                _terminate(process)


def _read_bounded(handle, limit):
    handle.seek(0, os.SEEK_END)
    size = handle.tell()
    handle.seek(max(0, size - limit))
    return handle.read(limit), size > limit


def _terminate(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=2)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


__all__ = ["inspect_local_environment_cache", "prepare_local_environment", "uses_embedded_current_runtime"]
