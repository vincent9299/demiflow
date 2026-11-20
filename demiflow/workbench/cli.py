"""Workspace commands share the same check and case APIs as the local UI."""

import json
from pathlib import Path

import click

from demiflow.forge.platform_cli import output
from demiflow.workbench.projects import workspace as api


def register(main):
    root_arg = click.argument("root", type=click.Path(path_type=Path, file_okay=False))
    pretty_option = click.option("--json", "pretty", is_flag=True)

    @main.group("workbench")
    def workspace():
        """Organize projects, tables and checked native flows."""

    from demiflow.workbench.projects.registry import register as register_registry
    register_registry(workspace)

    @workspace.command("init")
    @root_arg
    @click.option("--name", default="workspace")
    @click.option('--existing', is_flag=True, help='Initialize controls in a nonempty project without replacing files.')
    @click.option('--extra', 'extras', multiple=True, help='Bind a declared platform dependency extra.')
    @pretty_option
    def initialize(root, name, existing, extras, pretty):
        output(lambda: api.init_workspace(root, name=name, existing=existing, extras=extras), pretty)

    @workspace.command("location")
    @root_arg
    @click.argument("name")
    @click.argument(
        "path", type=click.Path(path_type=Path, exists=True, file_okay=False)
    )
    @pretty_option
    @click.option('--writable', is_flag=True, help='Explicitly permit writes to this owned location.')
    @click.option('--write-mode', 'write_modes', multiple=True, type=click.Choice(['create', 'overwrite', 'checkpoint']))
    def location(root, name, path, pretty, writable, write_modes):
        output(lambda: api.add_location(root, name, path, writable=writable, write_modes=list(write_modes) if write_modes else None), pretty)

    @workspace.command("context")
    @root_arg
    @pretty_option
    def context(root, pretty):
        output(lambda: api.workspace_context(root), pretty)

    @workspace.command('discover')
    @root_arg
    @pretty_option
    def discover(root, pretty):
        """Read supported original declarations without importing project code."""
        output(lambda: api.discover_project(root), pretty)

    @workspace.command('document')
    @root_arg
    @click.argument('path')
    @click.option('--expected', required=True)
    @pretty_option
    def document(root, path, expected, pretty):
        output(lambda: api.project_document(root, path, expected=expected), pretty)

    @workspace.command('data-preview')
    @root_arg
    @click.argument('identity')
    @click.option('--expected', required=True, help='Discovery digest returned by discover.')
    @click.option('--version', type=click.IntRange(min=1))
    @click.option('--binding', help='Metadata binding; when supplied, read a bounded page at --version.')
    @click.option('--column', 'columns', multiple=True)
    @click.option('--offset', default=0, type=click.IntRange(min=0))
    @click.option('--limit', default=20, type=click.IntRange(1, 100))
    @click.option('--row-id', type=click.IntRange(min=0))
    @pretty_option
    def data_preview(root, identity, expected, version, binding, columns, offset, limit, row_id, pretty):
        def perform():
            if binding is None:
                if columns or offset or limit != 20 or row_id is not None:
                    raise ValueError('Read metadata first, then supply its version and binding for a page.')
                return api.project_table(root, identity, expected=expected, version=version)
            if version is None:
                raise ValueError('A page requires the exact observed version.')
            return api.project_sample(root, identity, expected=expected, version=version, binding=binding,
                                      columns=list(columns) or None, offset=offset, limit=limit, row_id=row_id)
        output(perform, pretty)

    @workspace.command('check')
    @root_arg
    def project_check(root):
        """Check all declared entries against the unified platform standard."""
        output(lambda: api.check_project(root), True)

    @workspace.command('check-status')
    @root_arg
    def project_status(root):
        output(lambda: api.project_check_status(root), True)

    @workspace.group('audit')
    def audit():
        """Observe checked content and explicitly manage author change constraints."""

    @audit.command('status')
    @root_arg
    def audit_status(root):
        output(lambda: api.audit_status(root), True)

    @audit.command('check')
    @root_arg
    @click.option('--kind', required=True, type=click.Choice(['scope', 'report']))
    @click.option('--digest')
    @click.option('--expected-policy', required=True)
    def audit_check(root, kind, digest, expected_policy):
        output(lambda: api.audit_check(root, kind=kind, report_digest=digest, expected_policy=expected_policy), True)

    @audit.command('enable')
    @root_arg
    @click.option('--expected', required=True)
    def audit_enable(root, expected):
        output(lambda: api.enable_audit(root, expected=expected), True)

    @audit.command('bind')
    @root_arg
    @click.argument('digest')
    @click.option('--expected', required=True)
    def audit_bind(root, digest, expected):
        output(lambda: api.bind_audit_report(root, digest, expected=expected), True)

    @audit.command('unbind')
    @root_arg
    @click.argument('digest')
    @click.option('--expected', required=True)
    def audit_unbind(root, digest, expected):
        output(lambda: api.unbind_audit_report(root, digest, expected=expected), True)

    @audit.group('scope')
    def scope():
        """One active change constraint; no task baseline is moved implicitly."""

    @scope.command('begin')
    @root_arg
    @click.option('--declaration', required=True, type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--expected', required=True)
    def scope_begin(root, declaration, expected):
        def perform():
            value = api.read_control(declaration, 65536)
            if type(value) is not dict or 'writes' not in value or set(value) - {'writes', 'watch_roots', 'limits'}:
                raise ValueError('Scope declaration needs writes and optional watch_roots/limits')
            return api.begin_change_scope(root, expected=expected, **value)
        output(perform, True)

    @scope.command('import')
    @root_arg
    @click.argument('baseline', type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--writes', required=True, type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--expected', required=True)
    def scope_import(root, baseline, writes, expected):
        output(lambda: api.import_change_scope(root, baseline, writes=api.read_control(writes, 65536), expected=expected), True)

    @scope.command('amend')
    @root_arg
    @click.option('--writes', required=True, type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--expected', required=True)
    @click.option('--reason', required=True)
    def scope_amend(root, writes, expected, reason):
        output(lambda: api.amend_change_scope(root, writes=api.read_control(writes, 65536), expected=expected, reason=reason), True)

    @scope.command('end')
    @root_arg
    @click.option('--expected', required=True)
    @click.option('--reason', required=True)
    def scope_end(root, expected, reason):
        output(lambda: api.end_change_scope(root, expected=expected, reason=reason), True)

    @workspace.command('changes')
    @root_arg
    @pretty_option
    def changes(root, pretty):
        output(lambda: api.workspace_changes(root), pretty)

    @workspace.command('source')
    @root_arg
    @click.argument('path')
    @click.option('--expected-sha256')
    @pretty_option
    def source(root, path, expected_sha256, pretty):
        output(lambda: api.source_text(root, path, expected_sha256=expected_sha256), pretty)

    @workspace.command('parameters')
    @root_arg
    @click.argument('path')
    @pretty_option
    def parameters(root, path, pretty):
        output(lambda: api.read_parameters(root, path), pretty)

    @workspace.command('budgets')
    @root_arg
    @click.option('--limits', required=True, help='Explicit workspace budget policy as JSON.')
    @click.option('--expected', required=True, help='Current workspace control digest.')
    @pretty_option
    def budgets(root, limits, expected, pretty):
        output(lambda: api.set_budgets(root, json.loads(limits), expected=expected), pretty)

    @workspace.command('index')
    @root_arg
    @click.option('--rebuild', is_flag=True, help='Explicitly rebuild run/report listings from original records.')
    @click.option('--synchronize', is_flag=True, help='Resolve bounded pending index publications.')
    @pretty_option
    def directory_index(root, rebuild, synchronize, pretty):
        from demiflow.workbench.projects import index
        def perform():
            if rebuild and synchronize:
                raise ValueError('Select either rebuild or synchronize')
            from demiflow.workbench import directory_operations
            return directory_operations.rebuild(root) if rebuild else directory_operations.synchronize(root) if synchronize else index.status(root)
        output(perform, pretty)

    @workspace.command("bind-platform")
    @root_arg
    @click.option('--extra', 'extras', multiple=True, help='Replace the bound extras; omitted preserves the current set.')
    @click.option(
        "--expected", required=True, help="Digest of the previous platform binding."
    )
    @pretty_option
    def bind_platform(root, expected, extras, pretty):
        output(lambda: api.bind_platform(root, expected=expected, extras=extras or None), pretty)

    @workspace.command("view")
    @root_arg
    @click.option("--port", type=click.IntRange(0, 65535), default=0)
    @click.option("--enable-runs", is_flag=True, help="Enable explicit local experiment operations.")
    @click.option('--enable-config-write', is_flag=True, help='Deployment ceiling for graph parameter saves.')
    @click.option('--workspace-config-write', is_flag=True, help='Explicit parameter-write grant for this single workspace.')
    @click.option('--token-file', type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--control-only', is_flag=True)
    @click.option('--analysis-config', type=click.Path(exists=True, dir_okay=False, path_type=Path), help='Explicit reviewed production analysis adapter; disabled by default')
    @click.option('--notebook-python', type=click.Path(exists=True, dir_okay=False, path_type=Path), help='Python environment with native Jupyter Notebook 7 installed.')
    def view(root, port, enable_runs, control_only, token_file, enable_config_write, workspace_config_write, analysis_config, notebook_python):
        from demiflow.workbench.api import serve

        serve(root, port=port, enable_runs=enable_runs, control_only=control_only, token_file=token_file,
              enable_config_write=enable_config_write,workspace_config_write=workspace_config_write,
              analysis_config=analysis_config,notebook_python=notebook_python)

    from demiflow.workbench.runs.cli import register as register_runs
    register_runs(workspace)

    @workspace.group("table")
    def table():
        """Attach fixed table versions and browse their real rows."""

    def attach_options(function):
        for decorator in (
            pretty_option,
            click.option("--schema-version"),
            click.option("--schema-name"),
            click.option("--dataset-id"),
            click.option("--location-id", default="local"),
            click.option("--head", is_flag=True),
            click.option("--version", type=click.IntRange(1)),
            click.option("--uri", "relative_uri", required=True),
            click.argument("alias"),
            root_arg,
        ):
            function = decorator(function)
        return function

    def attach_action(
        root,
        alias,
        relative_uri,
        version,
        head,
        location_id,
        dataset_id,
        schema_name,
        schema_version,
        pretty,
        expected=None,
    ):
        if (version is None) == (not head):
            raise click.UsageError("Select exactly one of --version or --head")
        output(
            lambda: api.attach_table(
                root,
                alias,
                relative_uri=relative_uri,
                version=version,
                location_id=location_id,
                dataset_id=dataset_id,
                schema_name=schema_name,
                schema_version=schema_version,
                expected=expected,
            ),
            pretty,
        )

    table.command("attach")(attach_options(attach_action))

    @table.command("replace")
    @attach_options
    @click.option(
        "--expected", required=True, help="Digest of the current alias reference."
    )
    def replace(**kwargs):
        attach_action(**kwargs)

    @table.command("list")
    @root_arg
    @click.option("--offset", default=0, type=click.IntRange(0))
    @click.option("--limit", default=50, type=click.IntRange(1, 200))
    @pretty_option
    def list_tables(root, offset, limit, pretty):
        def perform():
            context = api.workspace_context(root)
            tables = context["tables"]
            return {
                "status": "listed",
                "total": len(tables),
                "offset": offset,
                "tables": tables[offset : offset + limit],
            }

        output(perform, pretty)

    @table.command("show")
    @root_arg
    @click.argument("alias")
    @click.option("--expected-binding", help="Require the previously observed preview identity.")
    @pretty_option
    def show_table(root, alias, expected_binding, pretty):
        output(lambda: api.describe_table(root, alias, expected_binding=expected_binding), pretty)

    @table.command('view')
    @root_arg
    @click.argument('alias')
    @click.option('--declaration', type=click.Path(path_type=Path, dir_okay=False), required=True)
    @click.option('--expected', required=True, help='Current complete table control digest.')
    @pretty_option
    def table_view(root, alias, declaration, expected, pretty):
        def perform():
            value = api.read_control(declaration, 16384)
            if set(value) != {'display', 'provenance'}:
                raise ValueError('Table view declaration requires display and provenance')
            return api.set_table_view(root, alias, **value, expected=expected)
        output(perform, pretty)

    @table.command("sample")
    @root_arg
    @click.argument("alias")
    @click.option("--column", "columns", multiple=True)
    @click.option("--offset", default=0, type=click.IntRange(0))
    @click.option("--limit", default=20, type=click.IntRange(1, 100))
    @click.option("--row-id", type=click.IntRange(0))
    @click.option("--expected-binding", help="Require the previously observed preview identity.")
    @pretty_option
    def sample(root, alias, columns, offset, limit, row_id, expected_binding, pretty):
        output(
            lambda: api.sample_table(
                root,
                alias,
                columns=list(columns) or None,
                offset=offset,
                limit=limit,
                row_id=row_id,
                expected_binding=expected_binding,
            ),
            pretty,
        )

    @table.command("detach")
    @root_arg
    @click.argument("alias")
    @click.option("--expected", required=True)
    @pretty_option
    def detach(root, alias, expected, pretty):
        output(lambda: api.detach_table(root, alias, expected=expected), pretty)

    @table.command('media')
    @root_arg
    @click.argument('alias')
    @click.option('--row-id', type=click.IntRange(0), required=True)
    @click.option('--field')
    @click.option('--field-path', help='JSON field path from the declared view.')
    @click.option('--expected-binding', required=True)
    @pretty_option
    def media(root, alias, row_id, field, field_path, expected_binding, pretty):
        output(lambda: api.table_media(root, alias, row_id=row_id, field=field,
            field_path=json.loads(field_path) if field_path is not None else None,
            expected_binding=expected_binding), pretty)

    @table.command('evidence')
    @root_arg
    @click.argument('alias')
    @click.option('--row-id', type=click.IntRange(0), required=True)
    @click.option('--field-path', required=True)
    @click.option('--expected-binding', required=True)
    @pretty_option
    def evidence(root, alias, row_id, field_path, expected_binding, pretty):
        output(lambda: api.table_evidence(root, alias, row_id=row_id,
            field_path=json.loads(field_path), expected_binding=expected_binding), pretty)

    @workspace.group("flow")
    def flow():
        """Inspect source wiring; this stage does not execute the flow."""

    @flow.command('register')
    @root_arg
    @click.argument('entry')
    @click.option('--params', required=True)
    @pretty_option
    def register_flow(root, entry, params, pretty):
        output(lambda: api.register_flow(root, entry, params=params), pretty)

    @flow.command('unregister')
    @root_arg
    @click.argument('entry')
    @click.option('--expected', required=True)
    @pretty_option
    def unregister_flow(root, entry, expected, pretty):
        output(lambda: api.unregister_flow(root, entry, expected=expected), pretty)

    @flow.command("inspect")
    @root_arg
    @click.argument("entry")
    @click.option("--params", required=True, help="Project-relative parameter JSON.")
    @click.option('--track', is_flag=True, help='Explicitly pin the new checked report for observation.')
    @click.option('--expected-workspace')
    @pretty_option
    def inspect(root, entry, params, track, expected_workspace, pretty):
        def perform():
            if track != (expected_workspace is not None):
                raise ValueError('--track requires --expected-workspace, and vice versa')
            report = api.inspect_flow(root, entry, params=params)
            if not track or report.get('status') != 'checked':
                return report
            try:
                binding = api.bind_audit_report(root, report['digest'], expected=expected_workspace)
            except ValueError as exc:
                return dict(status='report_saved_binding_failed', report=report,
                            error=dict(code=getattr(exc, 'code', 'operation_rejected'), message=str(exc)))
            return dict(status='checked_and_tracked', report=report, binding=binding)
        output(perform, pretty)

    @flow.command("view")
    @root_arg
    @click.argument("digest")
    @click.option("--out", type=click.Path(path_type=Path, dir_okay=False))
    @pretty_option
    def view_flow(root, digest, out, pretty):
        def perform():
            if out:
                from demiflow.workbench.api import export_report

                return export_report(root, digest, out)
            return api.report_status(root, digest)

        output(perform, pretty)

    @flow.command("diff")
    @root_arg
    @click.argument("left")
    @click.argument("right")
    @pretty_option
    def diff(root, left, right, pretty):
        output(lambda: api.diff_reports(root, left, right), pretty)
