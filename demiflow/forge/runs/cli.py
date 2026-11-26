"""Shared Click CLI commands for single-project Forge managed runs."""
from __future__ import annotations

import json
from pathlib import Path
import time

import click

from demiflow.forge.runs import manager as runs
from demiflow.forge.runs import operations as run_operations
from demiflow.forge.runs.authority import LocalAuthority


def emit(callback, pretty):
    try:
        result = callback()
    except (ValueError, TypeError, OSError, RuntimeError) as exc:
        result = {'status': 'rejected', 'error': str(exc), 'code': getattr(exc, 'code', 'operation_failed')}
    click.echo(json.dumps(result, ensure_ascii=False, indent=2 if pretty else None))
    if result.get('status') in {'failed', 'interrupted', 'rejected', 'cancelled'}:
        raise click.exceptions.Exit(1)


def _local_authority(root: Path, python: Path | None, read_roots: tuple[Path, ...] = ()):
    py = str(python) if python is not None else None
    rr = tuple(str(Path(p).resolve()) for p in read_roots) if read_roots else None
    return LocalAuthority(str(Path(root).absolute()), python=py, read_roots=rr)


def register(parent_group, *, group_name='run', authority_factory=None):
    make_authority = authority_factory or _local_authority
    root_arg = click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    id_arg = click.argument('run_id')
    pretty = click.option('--json', 'pretty', is_flag=True)

    @parent_group.group(group_name)
    def group():
        """Prepare, execute, stop, resume and inspect single-project managed runs."""

    @group.command('context')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def context(root, run_id, attempt, pretty):
        from demiflow.forge.runs.context import query
        emit(lambda: query(root, run_id, attempt=attempt), pretty)

    @group.command('tools')
    def tools():
        from demiflow.forge.bridge.run_tools import describe
        click.echo(json.dumps(describe(), ensure_ascii=False, indent=2))

    @group.command('invoke')
    @root_arg
    @click.argument('name')
    @click.option('--arguments', required=True, help='Closed JSON input from run tools.')
    @click.option('--python', 'python_path', type=click.Path(path_type=Path, dir_okay=False), default=None,
                  help='Explicit project Python executable for projects declaring dependencies.')
    @pretty
    def invoke(root, name, arguments, python_path, pretty):
        from demiflow.forge.bridge.run_tools import invoke as call
        auth = make_authority(root, python_path)
        emit(lambda: call(name, root, json.loads(arguments), authority=auth, perform=run_operations.perform), pretty)

    @group.command('prepare')
    @root_arg
    @click.argument('entry')
    @click.option('--params', required=True)
    @click.option('--python', 'python_path', type=click.Path(path_type=Path, dir_okay=False), default=None,
                  help='Explicit project Python executable when pyproject.toml declares dependencies.')
    @click.option('--read-root', 'read_roots', type=click.Path(path_type=Path, file_okay=False), multiple=True,
                  help='Authorized external read root(s) for local_ref packages.')
    @click.option('--exact-output', is_flag=True, help='Use the configured absent targets instead of new run paths.')
    @click.option('--outputs', default='{}', help='JSON map of output table identities.')
    @click.option('--inputs', default='{}', help='JSON map of input argument to table alias or run:ID:OUTPUT.')
    @click.option('--limits', default='{}', help='JSON resource limits, only tighter than the workspace policy.')
    @click.option('--metrics/--no-metrics', default=True, help='Optional process telemetry; protection/accounting stay enabled.')
    @click.option('--request-id', default=None, help='Persistent prepare identity for safe retry after disconnection.')
    @pretty
    def prepare(root, entry, params, python_path, read_roots, exact_output, outputs, inputs, limits, request_id, metrics, pretty):
        auth = make_authority(root, python_path, read_roots)
        emit(lambda: run_operations.perform(
            'prepare', root, authority=auth, entry=entry, params=params, outputs=json.loads(outputs),
            inputs=json.loads(inputs), new_outputs=not exact_output, limits=json.loads(limits),
            request_id=request_id, telemetry=metrics,
        ), pretty)

    @group.command('start')
    @root_arg
    @id_arg
    @click.option('--expected', required=True)
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @click.option('--python', 'python_path', type=click.Path(path_type=Path, dir_okay=False), default=None)
    @click.option('--read-root', 'read_roots', type=click.Path(path_type=Path, file_okay=False), multiple=True)
    @click.option('--wait', 'wait_for_run', is_flag=True)
    @pretty
    def start(root, run_id, expected, expected_attempt, python_path, read_roots, wait_for_run, pretty):
        auth = make_authority(root, python_path, read_roots)

        def perform():
            result = run_operations.perform(
                'start', root, authority=auth, run_id=run_id, expected=expected,
                **({'expected_attempt': expected_attempt} if expected_attempt is not None else {}),
            )
            selected_attempt = result.get('attempt')
            deadline = time.monotonic() + result['limits']['timeout_s'] + 60
            while wait_for_run and result['status'] in runs.ACTIVE and time.monotonic() < deadline:
                time.sleep(0.5)
                result = run_operations.perform('show', root, authority=auth, run_id=run_id, attempt=selected_attempt)
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
    @click.option('--wait', 'wait_for_run', is_flag=True)
    @pretty
    def resume(root, run_id, expected, expected_attempt, wait_for_run, pretty):
        auth = make_authority(root, None)

        def perform():
            result = run_operations.perform(
                'resume', root, authority=auth, run_id=run_id, expected=expected, expected_attempt=expected_attempt,
            )
            selected_attempt = result.get('attempt')
            deadline = time.monotonic() + result['limits']['timeout_s'] + 60
            while wait_for_run and result['status'] in runs.ACTIVE and time.monotonic() < deadline:
                time.sleep(0.5)
                result = run_operations.perform('show', root, run_id=run_id, attempt=selected_attempt)
                if result.get('supervisor_alive') is False:
                    break
            return result

        emit(perform, pretty)

    @group.command('show')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def show(root, run_id, attempt, pretty):
        emit(lambda: run_operations.perform('show', root, run_id=run_id, include_report=True, attempt=attempt), pretty)

    @group.command('status')
    @root_arg
    @id_arg
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def status(root, run_id, attempt, pretty):
        emit(lambda: run_operations.perform('show', root, run_id=run_id, include_report=False, attempt=attempt), pretty)

    def _cancel_cmd(root, run_id, expected_attempt, compact, pretty):
        emit(lambda: run_operations.perform(
            'cancel', root, run_id=run_id, compact=compact,
            **({'expected_attempt': expected_attempt} if expected_attempt is not None else {}),
        ), pretty)

    @group.command('cancel')
    @root_arg
    @id_arg
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @click.option('--compact', is_flag=True, help='Return the bounded runtime snapshot after the stop receipt.')
    @pretty
    def cancel(root, run_id, expected_attempt, compact, pretty):
        _cancel_cmd(root, run_id, expected_attempt, compact, pretty)

    @group.command('stop')
    @root_arg
    @id_arg
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @click.option('--compact', is_flag=True, help='Return the bounded runtime snapshot after the stop receipt.')
    @pretty
    def stop(root, run_id, expected_attempt, compact, pretty):
        _cancel_cmd(root, run_id, expected_attempt, compact, pretty)

    @group.command('reconcile')
    @root_arg
    @id_arg
    @click.option('--expected-attempt', type=click.IntRange(1, 32))
    @pretty
    def reconcile(root, run_id, expected_attempt, pretty):
        emit(lambda: runs.reconcile(
            root, run_id,
            **({'expected_attempt': expected_attempt} if expected_attempt is not None else {}),
        ), pretty)

    @group.command('logs')
    @root_arg
    @id_arg
    @click.option('--offset', type=click.IntRange(min=0), default=0)
    @click.option('--attempt', type=click.IntRange(1, 32))
    @pretty
    def logs(root, run_id, offset, attempt, pretty):
        emit(lambda: run_operations.perform('logs', root, run_id=run_id, offset=offset, attempt=attempt), pretty)

    @group.command('request-id')
    def new_request_id():
        from demiflow.forge.runs.requests import request_id
        click.echo(request_id())

    @group.command('request')
    @root_arg
    @click.argument('request_id')
    @click.option('--reconcile', is_flag=True)
    @pretty
    def request(root, request_id, reconcile, pretty):
        from demiflow.forge.runs import requests as run_requests
        emit(lambda: (run_requests.reconcile if reconcile else run_requests.read)(root, request_id), pretty)

    @group.command('admission')
    @root_arg
    @click.argument('entry')
    @click.option('--params', required=True)
    @pretty
    def admission(root, entry, params, pretty):
        from demiflow.forge.runs.admission import describe
        emit(lambda: describe(root, entry, params=params), pretty)

    @group.command('rebuild-index')
    @root_arg
    @pretty
    def rebuild_index(root, pretty):
        emit(lambda: runs.rebuild_index(root), pretty)

    return group
