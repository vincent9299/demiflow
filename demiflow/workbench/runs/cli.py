"""Workspace run commands; the historical forge run command is unchanged."""
import json
from pathlib import Path
import time
import click
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs import operations as run_operations


def register(workspace):
    root_arg = click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    id_arg = click.argument('run_id')
    pretty = click.option('--json', 'pretty', is_flag=True)

    def emit(callback, pretty):
        try:
            result = callback()
        except (ValueError, TypeError, OSError, RuntimeError) as exc:
            result = {'status': 'rejected', 'error': str(exc), 'code': getattr(exc, 'code', 'operation_failed')}
        click.echo(json.dumps(result, ensure_ascii=False, indent=2 if pretty else None))
        if result.get('status') in {'failed', 'interrupted', 'rejected', 'cancelled'}:
            raise click.exceptions.Exit(1)

    @workspace.group('run')
    def group():
        """Prepare, execute and inspect local experiments with native Dataset."""

    @group.command('context')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def context(root, run_id, attempt, pretty):
        from .context import query
        emit(lambda: query(root, run_id, attempt=attempt), pretty)

    @group.command('tools')
    def tools():
        from .tools import describe
        click.echo(json.dumps(describe(), ensure_ascii=False, indent=2))

    @group.command('invoke')
    @root_arg
    @click.argument('name')
    @click.option('--arguments', required=True, help='Closed JSON input from run tools.')
    @pretty
    def invoke(root, name, arguments, pretty):
        from .tools import invoke as call
        emit(lambda: call(name, root, json.loads(arguments)), pretty)

    @group.command('prepare')
    @root_arg
    @click.argument('entry')
    @click.option('--params', required=True)
    @click.option('--exact-output', is_flag=True, help='Use the configured absent targets instead of new run paths.')
    @click.option('--outputs', default='{}', help='JSON map of output table identities.')
    @click.option('--inputs', default='{}', help='JSON map of input argument to table alias or run:ID:OUTPUT.')
    @click.option('--limits', default='{}', help='JSON resource limits, only tighter than the workspace policy.')
    @click.option('--metrics/--no-metrics',default=True,help='Optional process telemetry; protection/accounting stay enabled.')
    @click.option('--request-id', default=None, help='Persistent prepare identity for safe retry after disconnection.')
    @pretty
    def prepare(root, entry, params, exact_output, outputs, inputs, limits, request_id, metrics, pretty):
        emit(lambda: run_operations.perform('prepare', root, entry=entry, params=params, outputs=json.loads(outputs),
             inputs=json.loads(inputs), new_outputs=not exact_output, limits=json.loads(limits), request_id=request_id,telemetry=metrics), pretty)

    @group.command('start')
    @root_arg
    @id_arg
    @click.option('--expected', required=True)
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @click.option('--wait', 'wait_for_run', is_flag=True)
    @pretty
    def start(root, run_id, expected, expected_attempt, wait_for_run, pretty):
        def perform():
            result = run_operations.perform('start', root, run_id=run_id, expected=expected,
                                **({'expected_attempt': expected_attempt} if expected_attempt is not None else {}))
            selected_attempt = result.get('attempt')
            deadline = time.monotonic() + result['limits']['timeout_s'] + 60
            while wait_for_run and result['status'] in runs.ACTIVE and time.monotonic() < deadline:
                time.sleep(0.5)
                result = run_operations.perform('show', root, run_id=run_id, attempt=selected_attempt)
                if result.get('supervisor_alive') is False:
                    break
            return result
        emit(perform, pretty)

    @group.command('list')
    @root_arg
    @click.option('--offset', type=click.IntRange(min=0), default=0)
    @click.option('--limit', type=click.IntRange(1, 100), default=20)
    @click.option('--cursor', default=None)
    @pretty
    def listing(root, offset, limit, cursor, pretty):
        emit(lambda: run_operations.perform('list', root, offset=offset, limit=limit, cursor=cursor), pretty)

    @group.command('resume')
    @root_arg
    @id_arg
    @click.option('--expected', required=True)
    @click.option('--expected-attempt', type=click.IntRange(1, 31), required=True)
    @pretty
    def resume(root, run_id, expected, expected_attempt, pretty):
        emit(lambda: run_operations.perform('resume', root, run_id=run_id, expected=expected, expected_attempt=expected_attempt), pretty)

    @group.command('show')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def show(root, run_id, attempt, pretty):
        emit(lambda: run_operations.perform('show', root, run_id=run_id, include_report=True, attempt=attempt), pretty)

    @group.command('cancel')
    @root_arg
    @id_arg
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @pretty
    @click.option('--compact',is_flag=True,help='Return the bounded runtime snapshot after the stop receipt.')
    def cancel(root, run_id, expected_attempt, compact, pretty):
        emit(lambda: run_operations.perform('cancel', root, run_id=run_id, compact=compact,
             **({'expected_attempt': expected_attempt} if expected_attempt is not None else {})), pretty)

    @group.command('reconcile')
    @root_arg
    @id_arg
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @pretty
    def reconcile(root, run_id, expected_attempt, pretty):
        emit(lambda: runs.reconcile(root, run_id,
             **({'expected_attempt': expected_attempt} if expected_attempt is not None else {})), pretty)

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

    @group.command('logs')
    @root_arg
    @id_arg
    @click.option('--offset', type=click.IntRange(min=0), default=0)
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def logs(root, run_id, offset, attempt, pretty):
        emit(lambda: run_operations.perform('logs', root, run_id=run_id, offset=offset, attempt=attempt), pretty)

    @group.command('evidence')
    @root_arg
    @id_arg
    @click.option('--reference', required=True, help='JSON native reference from a result case.')
    @pretty
    def evidence(root, run_id, reference, pretty):
        emit(lambda: runs.evidence(root, run_id, json.loads(reference)), pretty)

    @group.command('request-id')
    def new_request_id():
        from demiflow.workbench.runs.requests import request_id
        click.echo(request_id())

    @group.command('request')
    @root_arg
    @click.argument('request_id')
    @click.option('--reconcile', is_flag=True)
    @pretty
    def request(root,request_id,reconcile,pretty):
        from demiflow.workbench.runs import requests as run_requests
        emit(lambda:(run_requests.reconcile if reconcile else run_requests.read)(root,request_id),pretty)

    @group.command('snapshot')
    @root_arg
    @id_arg
    @click.option('--attempt',type=click.IntRange(1,32))
    @click.option('--revision')
    @pretty
    def snapshot(root,run_id,attempt,revision,pretty):
        from demiflow.workbench.runs.view import snapshot
        emit(lambda:snapshot(root,run_id,attempt=attempt,revision=revision),pretty)

    @group.command('events')
    @root_arg
    @id_arg
    @click.option('--attempt',type=click.IntRange(1,32))
    @click.option('--cursor')
    @pretty
    def events(root,run_id,attempt,cursor,pretty):
        from demiflow.workbench.runs.view import events
        emit(lambda:events(root,run_id,attempt=attempt,cursor=cursor),pretty)

    @group.command('metrics')
    @root_arg
    @id_arg
    @click.option('--attempt',type=click.IntRange(1,32))
    @click.option('--points',type=click.IntRange(1,600),default=600)
    @click.option('--resolution',type=click.Choice(['raw','minute']),default='raw')
    @pretty
    def metrics(root,run_id,attempt,points,resolution,pretty):
        from demiflow.workbench.runs.view import metrics
        emit(lambda:metrics(root,run_id,attempt=attempt,points=points,resolution=resolution),pretty)

    @group.command('admission')
    @root_arg
    @click.argument('entry')
    @click.option('--params',required=True)
    @pretty
    def admission(root,entry,params,pretty):
        from demiflow.workbench.runs.admission import describe
        emit(lambda:describe(root,entry,params=params),pretty)
