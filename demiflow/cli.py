"""Standalone CLI for inspecting and running immutable Demiflow bundles."""

from __future__ import annotations
import json
import sys
import uuid
from pathlib import Path
import click
import yaml
from .config import parse_demiflow_config
from .errors import (
    PipelineRunCancelled,
    PipelineRunFailure,
    PipelineRunIndeterminate,
    PipelineRunTimeout,
)
from .execution import (
    PipelineExecutionTarget,
    backend_for,
    inspect_pipeline_run_readiness,
)
from .execution.contracts import PipelineBundleRef, PipelineRunRequest


def _environment(config_path):
    from dataclasses import replace
    import tempfile

    path = (
        Path(config_path).expanduser().resolve()
        if str(config_path or "")
        else Path(sys.prefix).resolve() / "share" / "demiflow" / "demiflow.yaml"
    )
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    value = parse_demiflow_config(raw, component_path=path)
    cache = Path(tempfile.gettempdir()).resolve() / "demiflow-python-envs"
    return replace(value, local_environment_cache=cache)


def _target(backend, timeout_seconds, ray_job_api_address, ray_namespace):
    return PipelineExecutionTarget.from_mapping(
        {
            "backend": backend,
            "timeout_seconds": timeout_seconds or 7200,
            **(
                {
                    "ray": {
                        "job_api_address": ray_job_api_address,
                        "namespace": ray_namespace,
                    }
                }
                if backend == "ray"
                else {}
            ),
        }
    )


@click.group()
def main():
    """Inspect and run immutable Demiflow Pipeline bundles."""


@main.command("inspect-readiness")
@click.option(
    "--root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=str),
)
@click.option(
    "--config",
    default="",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
)
@click.option("--timeout-seconds", type=click.IntRange(1), default=None)
@click.option("--backend", type=click.Choice(("local", "ray")), required=True)
@click.option("--ray-job-api-address", default="")
@click.option("--ray-namespace", default="")
def inspect_readiness(
    root,
    config,
    timeout_seconds,
    backend,
    ray_job_api_address,
    ray_namespace,
):
    target = _target(
        backend,
        timeout_seconds,
        ray_job_api_address,
        ray_namespace,
    )
    result = inspect_pipeline_run_readiness(
        PipelineBundleRef.load(root),
        _environment(config),
        target,
    )
    click.echo(
        json.dumps(
            {"schema_version": "demiflow_cli_readiness_v1", **result.__dict__},
            ensure_ascii=False,
            indent=2,
        )
    )
    if not result.ready:
        raise SystemExit(1)


@main.command("run")
@click.option(
    "--root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=str),
)
@click.option(
    "--config",
    default="",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
)
@click.option("--run-id", default="")
@click.option("--timeout-seconds", type=click.IntRange(1), default=None)
@click.option("--backend", type=click.Choice(("local", "ray")), required=True)
@click.option("--ray-job-api-address", default="")
@click.option("--ray-namespace", default="")
@click.option(
    "--monitor-format",
    type=click.Choice(("text", "jsonl")),
    default="text",
    show_default=True,
)
def run(
    root,
    config,
    run_id,
    timeout_seconds,
    backend,
    ray_job_api_address,
    ray_namespace,
    monitor_format,
):
    bundle = PipelineBundleRef.load(root)
    environment = _environment(config)
    target = _target(
        backend,
        timeout_seconds,
        ray_job_api_address,
        ray_namespace,
    )
    identifier = run_id or f"pipeline-{uuid.uuid4().hex[:16]}"

    def observe(value):
        item = value.to_dict()
        if monitor_format == "jsonl":
            click.echo(
                json.dumps(item, ensure_ascii=False, separators=(",", ":")), err=True
            )
        else:
            detail = " ".join(
                f"{key}={item[key]}"
                for key in (
                    "backend_status",
                    "action",
                    "action_phase",
                    "rows",
                    "batches",
                )
                if key in item
            )
            click.echo(
                f"[{value.elapsed_ms/1000:09.3f}] {value.kind.upper()} {value.phase.upper()} {detail}".rstrip(),
                err=True,
            )

    try:
        result = backend_for(bundle, environment, target).run(
            PipelineRunRequest(identifier, bundle, target),
            timeout_seconds=timeout_seconds,
            observer=observe,
        )
    except PipelineRunTimeout as exc:
        _error(exc)
        raise SystemExit(124)
    except PipelineRunCancelled as exc:
        _error(exc)
        raise SystemExit(130)
    except PipelineRunIndeterminate as exc:
        _error(exc)
        raise SystemExit(75)
    except PipelineRunFailure as exc:
        _error(exc)
        raise SystemExit(1)
    click.echo(
        json.dumps(
            {
                "schema_version": "demiflow_cli_run_result_v1",
                "run_id": result.run_id,
                "status": result.status,
                "backend": result.backend_type,
                "driver_id": result.driver_id,
                "bundle_digest": result.bundle_digest,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _error(exc):
    click.echo(
        json.dumps(getattr(exc, "error", {"message": str(exc)}), ensure_ascii=False),
        err=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
