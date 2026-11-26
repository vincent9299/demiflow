"""Workbench run commands composing Forge's single-project run CLI with Workbench product views."""
import json
from pathlib import Path

import click

from demiflow.forge.runs import manager as runs
from demiflow.forge.runs.cli import emit, register as register_forge_runs
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs.authority import local_authority


def register(workspace):
    root_arg = click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    id_arg = click.argument('run_id')
    pretty = click.option('--json', 'pretty', is_flag=True)

    group = register_forge_runs(workspace, authority_factory=local_authority)

    @group.command('attach')
    @root_arg
    @id_arg
    @click.argument('output')
    @click.argument('alias')
    @click.option('--attempt', type=click.IntRange(1, 32))
    @click.option('--legacy-attempt', is_flag=True)
    @click.option('--expected-result', required=True)
    @pretty
    def attach(root, run_id, output, alias, attempt, legacy_attempt, expected_result, pretty):
        if (attempt is None) == (not legacy_attempt):
            raise click.UsageError('Select exactly one of --attempt or --legacy-attempt')
        emit(lambda: project_ops.attach_result(root, run_id, output, alias,
             attempt=attempt, expected_result=expected_result), pretty)

    @group.command('compare')
    @root_arg
    @click.argument('left')
    @click.argument('right')
    @pretty
    def compare(root, left, right, pretty):
        emit(lambda: runs.compare(root, left, right), pretty)

    @group.command('evidence')
    @root_arg
    @id_arg
    @click.option('--reference', required=True, help='JSON native reference from a result case.')
    @pretty
    def evidence(root, run_id, reference, pretty):
        emit(lambda: runs.evidence(root, run_id, json.loads(reference)), pretty)

    @group.command('snapshot')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @click.option('--revision')
    @pretty
    def snapshot(root, run_id, attempt, revision, pretty):
        from demiflow.workbench.runs.view import snapshot
        emit(lambda: snapshot(root, run_id, attempt=attempt, revision=revision), pretty)

    @group.command('events')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @click.option('--cursor')
    @pretty
    def events(root, run_id, attempt, cursor, pretty):
        from demiflow.workbench.runs.view import events
        emit(lambda: events(root, run_id, attempt=attempt, cursor=cursor), pretty)

    @group.command('metrics')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @click.option('--points', type=click.IntRange(1, 600), default=600)
    @click.option('--resolution', type=click.Choice(['raw', 'minute']), default='raw')
    @pretty
    def metrics(root, run_id, attempt, points, resolution, pretty):
        from demiflow.workbench.runs.view import metrics
        emit(lambda: metrics(root, run_id, attempt=attempt, points=points, resolution=resolution), pretty)
