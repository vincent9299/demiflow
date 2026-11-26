"""Click CLI commands for multi-pipeline workflows under ``demiflow workbench workflow``."""
from __future__ import annotations

import json
from pathlib import Path

import click

from demiflow.forge.platform_cli import output
from demiflow.forge.schema import ForgeError
from demiflow.workbench.workflows import manager as wf


def _parse_kv_pairs(pairs: tuple[str, ...], *, label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in pairs:
        if '=' not in item:
            raise ForgeError(f'{label} must use KEY=VALUE format: {item!r}', code='invalid_request')
        key, val = item.split('=', 1)
        key = key.strip()
        if not key or not val:
            raise ForgeError(f'{label} must have non-empty KEY and VALUE: {item!r}', code='invalid_request')
        result[key] = val
    return result


def _parse_inputs(raw_json: str | None) -> dict | None:
    if raw_json is None:
        return None
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ForgeError(f'Invalid --inputs JSON: {exc}', code='invalid_request') from exc
    if type(parsed) is not dict:
        raise ForgeError('--inputs must be a JSON object', code='invalid_request')
    return parsed


def register(workspace: click.Group) -> None:
    root_arg = click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    pretty_option = click.option('--json', 'pretty', is_flag=True)
    registry_option = click.option('--registry', type=click.Path(path_type=Path, dir_okay=False),
                                   help='Optional Workbench registry.json to resolve cross-project aliases.')
    project_option = click.option('--project', 'projects', multiple=True,
                                  help='Bind an external project alias as ALIAS=/abs/path.')

    @workspace.group('workflow')
    def workflow():
        """Validate, run, monitor, cancel, and resume multi-pipeline DAG workflows."""

    @workflow.command('list')
    @root_arg
    @registry_option
    @project_option
    @pretty_option
    def list_cmd(root, registry, projects, pretty):
        output(lambda: wf.list_workflows(
            root,
            registry_path=registry,
            project_map=_parse_kv_pairs(projects, label='--project') if projects else None,
        ), pretty)

    @workflow.command('validate')
    @root_arg
    @click.argument('path')
    @registry_option
    @project_option
    @pretty_option
    def validate_cmd(root, path, registry, projects, pretty):
        output(lambda: wf.inspect_workflow(
            root,
            path,
            registry_path=registry,
            project_map=_parse_kv_pairs(projects, label='--project') if projects else None,
        ), pretty)

    @workflow.command('run')
    @root_arg
    @click.argument('path')
    @click.option('--inputs', 'inputs_json', help='Workflow top-level inputs as JSON object.')
    @click.option('--reuse', 'reuses', multiple=True, help='Reuse a succeeded child run: NODE_ID=RUN_ID.')
    @registry_option
    @project_option
    @pretty_option
    def run_cmd(root, path, inputs_json, reuses, registry, projects, pretty):
        output(lambda: wf.start_workflow(
            root,
            path,
            inputs=_parse_inputs(inputs_json),
            reuse_runs=_parse_kv_pairs(reuses, label='--reuse') if reuses else None,
            registry_path=registry,
            project_map=_parse_kv_pairs(projects, label='--project') if projects else None,
        ), pretty)

    @workflow.command('runs')
    @root_arg
    @click.option('--workflow', 'workflow_path', help='Filter by workflows/<name>.json path.')
    @click.option('--offset', default=0, type=click.IntRange(min=0))
    @click.option('--limit', default=20, type=click.IntRange(1, 100))
    @pretty_option
    def runs_cmd(root, workflow_path, offset, limit, pretty):
        output(lambda: wf.list_workflow_runs(root, workflow_path=workflow_path, offset=offset, limit=limit), pretty)

    @workflow.command('status')
    @root_arg
    @click.argument('wf_run_id')
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty_option
    def status_cmd(root, wf_run_id, attempt, pretty):
        output(lambda: wf.show_workflow_run(root, wf_run_id, attempt=attempt), pretty)

    @workflow.command('cancel')
    @root_arg
    @click.argument('wf_run_id')
    @click.option('--attempt', 'expected_attempt', type=click.IntRange(1, 32))
    @pretty_option
    def cancel_cmd(root, wf_run_id, expected_attempt, pretty):
        output(lambda: wf.cancel_workflow_run(root, wf_run_id, expected_attempt=expected_attempt), pretty)

    @workflow.command('reconcile')
    @root_arg
    @click.argument('wf_run_id')
    @pretty_option
    def reconcile_cmd(root, wf_run_id, pretty):
        output(lambda: wf.reconcile_workflow_run(root, wf_run_id), pretty)

    @workflow.command('resume')
    @root_arg
    @click.argument('wf_run_id')
    @click.option('--from-node', help='Force rerun starting from this node and its downstream.')
    @click.option('--reuse', 'reuses', multiple=True, help='Reuse a succeeded child run: NODE_ID=RUN_ID.')
    @registry_option
    @project_option
    @pretty_option
    def resume_cmd(root, wf_run_id, from_node, reuses, registry, projects, pretty):
        output(lambda: wf.resume_workflow_run(
            root,
            wf_run_id,
            from_node=from_node,
            reuse_runs=_parse_kv_pairs(reuses, label='--reuse') if reuses else None,
            registry_path=registry,
            project_map=_parse_kv_pairs(projects, label='--project') if projects else None,
        ), pretty)
