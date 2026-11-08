"""Agent-neutral Forge commands for the standalone demiflow CLI."""
from __future__ import annotations

import json
from pathlib import Path

import click

from .tools import invoke, describe as describe_tools
from .contract import load_contract
from .io import read_json
from .lifecycle import freeze_project, review_project, status
from .manifest import build_manifest, write_manifest
from .runner import dry_run, run_project
from .scaffold import new_project
from .schema import ForgeError
from .verify import verify_project


_JSON_OPTION = click.option("--json", "json_output", is_flag=True, help="Emit machine-readable JSON")
_PROJECT_ARGUMENT = click.argument("project", type=click.Path(file_okay=False, path_type=Path))


def register(main: click.Group) -> None:
    @main.group()
    def forge():
        """Develop, verify, review, freeze, and run contracted Pipelines."""

    from .development.click_entry import register as register_development
    register_development(forge)

    @forge.command("schema")
    @click.argument("kind", type=click.Choice(("request",)))
    def schema(kind):
        _call(lambda: read_json(Path(__file__).resolve().parent / "contracts" / f"{kind}-v1.schema.json"), True)

    @forge.command("rules")
    @_JSON_OPTION
    def rules(json_output):
        _call(load_contract, json_output)

    @forge.command("new")
    @click.argument("request", type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @click.option("--out", required=True, type=click.Path(file_okay=False, path_type=Path))
    @_JSON_OPTION
    def new(request, out, json_output):
        _call(lambda: new_project(request, out), json_output)

    @forge.command("context")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def context(project, json_output):
        _call(lambda: read_json(project / "contract.lock.json"), json_output)

    @forge.command("facts")
    @_PROJECT_ARGUMENT
    @click.option('--task')
    @_JSON_OPTION
    def facts(project, task, json_output):
        _call(lambda: invoke('forge.context', project, {'task': task} if task is not None else {}), json_output)

    @forge.command("tools")
    def tools():
        _call(describe_tools, True)

    @forge.command("describe")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def describe(project, json_output):
        _call(lambda: build_manifest(project), json_output)

    @forge.command("manifest")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def manifest(project, json_output):
        _call(lambda: write_manifest(project), json_output)

    @forge.command("check")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def check(project, json_output):
        _call(lambda: invoke('forge.check', project), json_output)

    @forge.command("dry-run")
    @_PROJECT_ARGUMENT
    @click.option("--config", required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @_JSON_OPTION
    def dry_run_command(project, config, json_output):
        _call(lambda: dry_run(project, config), json_output)

    @forge.command("verify")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def verify(project, json_output):
        _call(lambda: verify_project(project), json_output)

    @forge.command("status")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def status_command(project, json_output):
        _call(lambda: invoke('forge.status', project), json_output)

    @forge.command("review")
    @_PROJECT_ARGUMENT
    @click.option("--file", "review_file", required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @_JSON_OPTION
    def review(project, review_file, json_output):
        _call(lambda: review_project(project, review_file), json_output)

    @forge.command("freeze")
    @_PROJECT_ARGUMENT
    @_JSON_OPTION
    def freeze(project, json_output):
        _call(lambda: freeze_project(project), json_output)

    @forge.command("run")
    @_PROJECT_ARGUMENT
    @click.option("--config", required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @_JSON_OPTION
    def run(project, config, json_output):
        _call(lambda: run_project(project, config), json_output)

    from .platform_cli import register as register_platform
    register_platform(forge)


def _call(callback, json_output: bool) -> None:
    try:
        value = callback()
    except (ForgeError, OSError, TypeError, ValueError, RuntimeError) as exc:
        click.echo(json.dumps({
            "schema_version": "demiflow_forge_error_v1",
            "error": str(exc),
        }, ensure_ascii=False), err=True)
        raise SystemExit(1) from exc
    click.echo(json.dumps(value, ensure_ascii=False, indent=2 if json_output else None, default=str))
    if isinstance(value, dict) and (value.get("passed") is False or value.get("status") == "failed"):
        raise SystemExit(1)


__all__ = ["register"]
