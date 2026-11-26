"""Author and inspect a DemiFlow project using the installed SDK."""
import json
from pathlib import Path

import click

from .platform_cli import register as register_platform
from .schema import ForgeError


class _ForgeCommands(click.Group):
    """Keep --json machine-readable when Click rejects a command before dispatch."""
    def parse_args(self, ctx, args):
        if '--json' in args:
            ctx.meta['forge_json_args'] = tuple(args)
        return super().parse_args(ctx, args)

    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except click.UsageError as error:
            args = ctx.meta.get('forge_json_args')
            if args is None:
                raise
            root = next((arg.removeprefix('--root=') for arg in args if arg.startswith('--root=')), None)
            if '--root' in args:
                values = args[args.index('--root') + 1:]
                root = values[0] if values and not values[0].startswith('--') else None
            click.echo(json.dumps(dict(status='failed', root=str(Path(root).absolute()) if root else None,
                diagnostics=[dict(code='invalid_request', detail=error.format_message())]), ensure_ascii=False))
            ctx.exit(2)


def _output(root, as_json, operation, render=None):
    try:
        result = operation()
        status = result['status']
        code = {'failed': 1, 'invalid': 1, 'incomplete': 3}.get(status, 0)
    except (OSError, ValueError) as error:
        kind = getattr(error, 'code', 'operation_failed')
        code = {'invalid_request': 2, 'not_found': 2, 'conflict': 1, 'busy': 3,
                'resource_limit': 3, 'inputs_changed': 3, 'inspection_incomplete': 3, 'operation_rejected': 1}.get(kind, 2)
        result = dict(status='incomplete' if code == 3 else 'failed', root=str(root.absolute()),
                      diagnostics=[{'code': kind, 'detail': str(error)}])
    if as_json or render is None or code and not result.get('project'):
        click.echo(json.dumps(result, ensure_ascii=False, indent=None if as_json else 2))
    else:
        click.echo(render(result))
    if code:
        raise click.exceptions.Exit(code)


def register(main: click.Group) -> None:
    @main.group(cls=_ForgeCommands)
    def forge():
        """Develop a project and inspect the current SDK."""

    register_platform(forge)
    root_option = click.option('--root', required=True, type=click.Path(path_type=Path, file_okay=False))
    json_option = click.option('--json', 'as_json', is_flag=True, help='Return one JSON document.')

    @forge.command()
    @root_option
    @click.option('--path', 'paths', multiple=True, help='Project-relative target path.')
    @click.option('--topic', help='Progressive-disclosure topic: sdk, rules, files, flow, operator, prompt.')
    @click.option('--api', help='Full public symbol ID in the installed SDK catalogue.')
    @click.option('--source', 'sources', multiple=True, help='Include a specific project document.')
    @json_option
    def context(root, paths, topic, api, sources, as_json):
        """Read coding rules, SDK signatures and facts about this project."""
        from .context import build_context, render_context
        _output(root, as_json, lambda: build_context(root, paths=paths, topic=topic, api=api,
                                                     sources=sources), render_context)

    @forge.command()
    @root_option
    @click.option('--name')
    @click.option('--existing', is_flag=True, help='Initialize a directory that already contains source.')
    @click.option('--extra', 'extras', multiple=True, help='Include a declared optional dependency group.')
    @json_option
    def init(root, name, existing, extras, as_json):
        """Create the project declaration and its SDK snapshot."""
        from .project import initialize_project
        _output(root, as_json, lambda: initialize_project(root, name=name, existing=existing,
                                                          extras=extras or None))

    @forge.group()
    def add():
        """Create a standard source file or register a completed flow."""

    @add.command()
    @click.argument('name', required=False)
    @root_option
    @click.option('--register', 'register_entry', is_flag=True)
    @click.option('--entry')
    @click.option('--params')
    @json_option
    def flow(name, root, register_entry, entry, params, as_json):
        """Create a flow draft, or use --register --entry to publish its binding."""
        from .project import create_role_file, register_flow
        def perform():
            if register_entry and entry and name is None:
                return register_flow(root, entry, params=params)
            if name and not register_entry and entry is None and params is None:
                return create_role_file(root, 'flow', name)
            raise ForgeError('Choose a module name or --register --entry FILE:FUNCTION [--params FILE].',
                             code='invalid_request')
        _output(root, as_json, perform)

    @forge.command()
    @root_option
    @click.option('--goal', required=True, help='Original business goal for this change.')
    @click.option('--entry', help='Review a draft or registered FILE:FUNCTION.')
    @click.option('--params', help='Project-relative parameters of the selected entry.')
    @click.option('--path', 'paths', multiple=True, help='Project-relative source to review.')
    @click.option('--base', help='Compare current project files to this local Git revision.')
    @click.option('--evidence', multiple=True, help='Include a project-relative test or evidence file.')
    @click.option('--timeout', type=click.IntRange(1, 900), default=180, show_default=True,
                  help='Time budget in seconds for the complete independent model review.')
    @json_option
    def review(root, goal, entry, params, paths, base, evidence, timeout, as_json):
        """Start an independent, read-only review of a pipeline change."""
        from .review import render_review, review_project
        from .review_deepseek import run_reviewer
        from time import monotonic
        deadline = monotonic() + timeout

        def reviewer(prompt):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ForgeError('DeepSeek 评审已超时。', code='inspection_incomplete')
            return run_reviewer(prompt, timeout=remaining)

        _output(root, as_json, lambda: review_project(
            root, goal=goal, entry=entry, params=params, paths=paths, base=base, evidence=evidence,
            reviewer=reviewer),
            render_review)

    @add.command()
    @click.argument('name')
    @root_option
    @click.option('--actor', is_flag=True, help='Create a callable class.')
    @json_option
    def operator(name, root, actor, as_json):
        """Create a reusable code operator."""
        from .project import create_role_file
        _output(root, as_json, lambda: create_role_file(root, 'operator', name, actor=actor))

    @add.command()
    @click.argument('name')
    @root_option
    @click.option('--from', 'source', required=True, help='Project-relative config file, or - for stdin.')
    @json_option
    def prompt(name, root, source, as_json):
        """Validate and create a native Prompt or Agent definition."""
        from .project import create_role_file
        from .project_io import relative_path
        from .platform_environment import read_bounded
        def perform():
            maximum = 1024**2
            if source == '-':
                content = click.get_binary_stream('stdin').read(maximum + 1)
                if len(content) > maximum:
                    raise ForgeError('Prompt input exceeds 1 MiB.', code='resource_limit')
            else:
                content = read_bounded(relative_path(root, source), maximum)
            return create_role_file(root, 'prompt', name, content=content)
        _output(root, as_json, perform)

    @add.command()
    @root_option
    @click.option('--register', 'register_flag', is_flag=True)
    @click.option('--path', 'workflow_path', required=True, help='Project-relative workflows/<name>.json path.')
    @json_option
    def workflow(root, register_flag, workflow_path, as_json):
        """Validate and register a declarative workflows/*.json file in workspace.json."""
        from .project import register_workflow
        def perform():
            if not register_flag:
                raise ForgeError('Pass --register --path workflows/<name>.json to register a workflow.',
                                 code='invalid_request')
            return register_workflow(root, workflow_path)
        _output(root, as_json, perform)

    @forge.command()
    @root_option
    @click.option('--path', 'paths', multiple=True, help='Check a changed or deleted project-relative path.')
    @click.option('--entry', help='Check one existing flow, including a draft.')
    @click.option('--params', help='Check the selected parameter binding.')
    @json_option
    def check(root, paths, entry, params, as_json):
        """Check native flow wiring and project layout without executing user code."""
        from .pipeline_check import check_project
        def perform():
            if paths and entry or params and not entry:
                raise ForgeError('Use --path or --entry [--params].', code='invalid_request')
            return check_project(root, paths=paths, entry=entry, params=params)
        _output(root, as_json, perform)
