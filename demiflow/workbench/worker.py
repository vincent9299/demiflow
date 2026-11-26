"""One fixed, resource-limited protocol for inspection and read-only case I/O."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
import sys

if __name__ == '__main__':
    platform_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(platform_root))
    if sys.argv[1:] == ['--confined-project']:
        from importlib.util import find_spec
        spec = find_spec('demiflow')
        if spec is None or not spec.origin or Path(spec.origin).resolve() != platform_root / 'demiflow/__init__.py':
            raise RuntimeError('Project Python loaded DemiFlow outside the trusted platform source')

from demiflow.forge import operation_budget

FORMAT = "demiflow_forge_workspace_response_v1"


@operation_budget.controlled
def operation(name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
    from demiflow.forge.platform_worker import capture
    from demiflow.forge.platform import limits_with
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.host_partition import PREPARATION_RSS
    from demiflow.workbench.host_partition import location
    from demiflow.workbench.host_partition import READONLY_RSS

    from demiflow.workbench.reader import current
    from demiflow.workbench.reader import ALLOWED
    from demiflow.workbench.reader import in_native_dispatch
    request={"operation":name,"root":str(Path(root).absolute()),"arguments":arguments}
    root = Path(request['root'])
    inspection_reads = ()
    if _authority is not None:
        request["bootstrap"] = _authority
    if name in {'plan_preview', 'run_prepare'}:
        from demiflow.workbench.python_sources import deployment_modules
        _source_modules = deployment_modules(Path(request['root']))
    if name == 'plan_preview':
        from demiflow.workbench.python_sources import project_dependencies
        _dependencies = project_dependencies(root, authority=_authority)
    if name in {'plan_preview', 'native_submission_inspect'} and _dependencies is not None:
        request.pop('bootstrap', None)
        request['dependencies'] = _dependencies
        inspection_reads, request['table_inputs'] = _inspection_inputs(root, request)
    if _source_modules is not None:
        request['source_modules'] = _source_modules
    settings=limits_with({"catalog_bytes":9*1024**2,**({"timeout_seconds":20} if name in {"analysis_export","analysis_media_export","analysis_media_sources","analysis_source_read","table_profile"} else {})})
    context=current()
    if name == 'metadata' and in_native_dispatch() and not _pass_fds and _authority is None:
        # Publication verification can request fixed-version metadata while
        # serving a table read. It already runs under the reader's time/RSS/IO
        # ceilings; reuse the fixed dispatcher, never create a nested worker.
        # All other operations retain their original isolation and admission.
        value=dispatch(request)
    elif context is not None and _dependencies is None:
        reader,consumer_closed=context
        reusable=not _pass_fds and not os.environ.get('DEMIFLOW_FORGE_PREPARATION_FD') and name in ALLOWED
        if reusable:
            value=reader.call(request,timeout=min(20,settings['timeout_seconds']),consumer_closed=consumer_closed)
        else:
            reader.retire_idle()
            value=None
    else:value=None
    if value is None:
        from functools import partial
        business = name in {'plan_preview', 'native_submission_inspect'} and _dependencies is not None
        python = sys.executable
        if business:
            from demiflow.forge.dependencies import verify_interpreter
            python = verify_interpreter(_dependencies['environment']['interpreter'])
        argv = [python, "-I", "-B", str(Path(__file__).resolve())]
        launcher = None
        if business:
            argv.append('--confined-project')
            launcher = partial(_launch_project_inspection, root, _dependencies, _source_modules,
                               inspection_reads, settings['timeout_seconds'])
        value=capture(argv,request,settings,response_formats={FORMAT},input_limit=1024**2,
            pass_fds=_pass_fds,inherit_authority=not business,launcher=launcher,
            tree_rss_limit=PREPARATION_RSS if business or _pass_fds or name in {"analysis_media_sources","analysis_source_read","table_profile"} else (READONLY_RSS if (location() / 'host-budget.json').exists() else None))
    if not value["ok"]:
        code = value.get('code', 'operation_failed')
        if code == 'forbidden':
            raise PermissionError(value['error'])
        if code not in ForgeError.CODES:
            raise ForgeError('Invalid workspace error code', code='operation_failed')
        raise ForgeError(value["error"], code=code)
    return value["value"]


def _launch_project_inspection(root, locked, source_modules, inspection_reads, seconds, command, *, stdin, cwd, env, **_):
    """Apply OS bounds before the fixed project Python inspection starts."""
    from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox
    from demiflow.workbench.host_partition import PREPARATION_RSS

    if sys.platform != 'linux':
        raise ValueError('Confined project inspection requires the Linux sandbox backend')
    home = Path(cwd) / 'home'
    home.mkdir(mode=0o700)
    interpreter = locked['environment']['interpreter']
    from demiflow.forge.dependencies import module_files
    files = set(inspection_reads)
    source_files = {Path(row['path']) for row in module_files(root, locked).values()}
    source_files.update(map(Path, (source_modules or {}).values()))
    traversal = {root, *(path.parent for path in source_files)}
    files.update(source_files)
    runtime = {Path(__file__).resolve().parents[2], Path(interpreter['prefix']),
               Path(interpreter['base_prefix']), Path(interpreter['binary'])}
    runtime.update(path for path in map(Path, ('/usr', '/lib', '/lib64')) if path.exists())
    if any(root.is_relative_to(path) for path in runtime):
        raise ValueError('Project Python runtime root must not cover the project directory')
    spec = SandboxSpec(read_paths=tuple(map(str, sorted(files | {Path(cwd)}))), traversal_paths=tuple(map(str, sorted(traversal))),
                       write_paths=(), runtime_paths=tuple(sorted(map(str, runtime))),
                       protected_paths=(), denied_paths=(), private_home=str(home),
                       role='verifier', seconds=seconds, rss_bytes=PREPARATION_RSS)
    return launch_sandbox(command, spec=spec, env={**env, 'HOME': str(home), 'TMPDIR': str(home)},
                          cwd=cwd, stdin=stdin)


def _inspection_inputs(root, request):
    """Grant project source and exact input tables while keeping controls private."""
    from demiflow.forge.platform_environment import read_bounded
    from demiflow.forge.project_bindings import extract_arguments, is_direct_table_ref, resolve_table_uri
    from demiflow.forge.project_rules import ROLE_DIRS
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.table_bindings import make_resolver

    _, workspace = ws.load_workspace_control(root)
    supplied = request['arguments']
    from demiflow.forge.project import STATE_PATH
    paths = {root / 'workspace.json', root / STATE_PATH, ws.relative_path(root, workspace['platform']['path'])}
    parameters = supplied.get('parameters', {})
    if request['operation'] == 'plan_preview' and supplied.get('params') is not None:
        parameter_file = ws.validate_parameter_path(root, supplied['params'])
        paths.add(parameter_file)
        from demiflow.workbench.graph.preview import parameter_document
        parameters = parameter_document(read_bounded(parameter_file, 65536))
        if parameters.get('format') is None and set(parameters) <= {'arguments', 'run_directory'}:
            if isinstance(parameters.get('arguments'), dict):
                # This historical preview format passes raw arguments to the Flow.
                parameters = {}
    fixed_inputs = {}
    allowed = set(ROLE_DIRS) | {'tables', 'configs', 'parameters', 'lib', 'src', 'data'}
    with os.scandir(root) as entries:
        for index, item in enumerate(entries):
            if index >= 1024:
                raise ForgeError('Project directory exceeds the inspection path budget', code='resource_limit')
            if item.name in allowed and item.is_dir(follow_symlinks=False) or (
                    not item.name.startswith('.') and item.name.endswith('.py') and item.is_file(follow_symlinks=False)):
                paths.add(Path(item.path))
    resolver = make_resolver(ws.table_binding, runs.result_table)
    values, _ = extract_arguments(parameters)
    pending = [(values, 0)]
    seen = 0
    while pending:
        operation_budget.check()
        item, depth = pending.pop()
        seen += 1
        if seen > 4096 or depth > 16:
            raise ForgeError('Project inspection path scope exceeds its budget', code='resource_limit')
        if isinstance(item, dict):
            if is_direct_table_ref(item):
                target, _ = resolve_table_uri(root, item['uri'], item.get('version'))
                paths.add(target)
            elif item.get('kind') == 'table' and set(item) == {'kind', 'alias'}:
                alias = item['alias']
                if isinstance(alias, str) and alias in fixed_inputs:
                    continue
                control, ref, target = resolver(root, workspace, alias)
                paths.add(target)
                if alias.startswith('run:'):
                    fixed_inputs[alias] = dict(control=control, ref=ref.to_dict(), path=str(target))
            elif item.get('kind') == 'location' and set(item) == {'kind', 'location_id'}:
                location = workspace['locations'].get(item['location_id'])
                if location is not None and location.get('writable') is False:
                    target = ws.scoped_data_location(root, ws.location_root(root, location))
                    if target is not None:
                        paths.add(target)
            else:
                pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    return {path for path in paths if path.exists()}, fixed_inputs


def prepare_native(*, analysis_read, confined=False):
    if analysis_read:
        # Trusted export is a new read-only operation; keep the existing
        # inspection/preview paths' thread settings unchanged. Set these before
        # importing native libraries in this child, never in the Web process.
        import os
        import resource
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        for name in ("TOKIO_WORKER_THREADS","RAYON_NUM_THREADS","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS",
                     "MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","LANCE_CPU_THREADS"):
            os.environ[name]="1"
    # Load trusted native libraries before measuring their fixed virtual address
    # footprint. Candidate source directories are never put on sys.path.
    import pyarrow as pa
    import lance
    import resource
    import os

    pa.set_cpu_count(1 if analysis_read else 2)
    pa.set_io_thread_count(1 if analysis_read else 2)
    if confined:
        # The launcher accounts real RSS outside the sandbox. Lance still
        # requires a finite virtual memory limit for opening fixed tables.
        ceiling = min([4 * 1024**3, *(limit for limit in resource.getrlimit(resource.RLIMIT_AS)
                                       if limit != resource.RLIM_INFINITY)])
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
        return
    with open("/proc/self/statm") as source:
        baseline = int(source.read(128).split()[0]) * os.sysconf("SC_PAGE_SIZE")
    ceiling = min(4 * 1024**3, baseline + 384 * 1024**2)
    # Recovery can launch metadata inspection from an already limited worker.
    # exec preserves rlimits; respect both inherited bounds rather than trying
    # to raise the parent's hard limit when native-library footprints differ.
    for inherited in resource.getrlimit(resource.RLIMIT_AS):
        if inherited != resource.RLIM_INFINITY:
            ceiling = min(ceiling, inherited)
    if ceiling - baseline < 128 * 1024**2:
        raise MemoryError("Insufficient worker allocation headroom")
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))


def dispatch(request):
    from demiflow.forge.schema import ForgeError
    from demiflow.errors import ExecutionConflict
    root = Path(request["root"])
    args = request["arguments"]
    op = request["operation"]
    from demiflow.forge.io import no_symlinks

    no_symlinks(root)
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.table_bindings import make_resolver
    from demiflow.workbench.python_sources import dependency_scope
    table_resolver = make_resolver(ws.table_binding, runs.result_table)
    if 'dependencies' in request:
        from demiflow.lance.refs import DatasetRef

        def table_resolver(root, control, selector):
            if isinstance(selector, str) and selector.startswith('run:'):
                fact = request['table_inputs'].get(selector)
                if fact is None:
                    raise ForgeError('Missing admitted run output reference', code='conflict')
                return fact['control'], DatasetRef.from_dict(fact['ref']), Path(fact['path'])
            return ws.table_binding(root, control, selector)
    try:
        from demiflow.errors import DemiflowError
        from demiflow.lance import preview

        if op == "describe":
            value = preview.describe_table(root, args["version"])
        elif op == "analysis_export":
            from demiflow.workbench.analysis.source_export import export
            if set(args) != {"source_locator", "source_snapshot", "output_fd"}:
                raise ForgeError("Invalid fixed source export arguments",code="invalid_request")
            value = export(root, table_resolver=table_resolver, **args)
        elif op=="analysis_media_export":
            from demiflow.workbench.analysis.media_source_export import export
            if set(args)!={"source_locator","source_snapshot","case_key","field","output_fd"}:
                raise ForgeError("Invalid fixed declared media export arguments",code="invalid_request")
            value=export(root,table_resolver=table_resolver, **args)
        elif op=="analysis_media_sources":
            from demiflow.workbench.analysis.media_source_export import resolve_fields
            if set(args)!={"source_locator","source_snapshot","case_key","fields"}:
                raise ForgeError("Invalid fixed declared media proof arguments",code="invalid_request")
            value=resolve_fields(root,table_resolver=table_resolver, **args)
        elif op == "analysis_source_read":
            from demiflow.workbench.analysis.source_query import read
            if set(args)!={"source_locator","source_snapshot","query"}:
                raise ForgeError("Invalid fixed source sample/profile arguments",code="invalid_request")
            value=read(root,table_resolver=table_resolver, **args)
        elif op=="table_profile":
            from demiflow.workbench.analysis.catalog_profile import profile
            from demiflow.workbench.analysis import contracts
            contracts.fields(args,{"kind","columns"},{"alias","expected_binding","identity","expected","version","binding","version_binding"},label="catalog profile")
            value=profile(root,table_resolver=table_resolver, **args)
        elif op == 'metadata':
            value = preview.snapshot_metadata(root, args['version'])
        elif op == "catalog":
            ref = preview.catalog_reference(root, **args)
            value = ref.to_dict() if ref else None
        elif op in {'project_check', 'project_status'}:
            from demiflow.workbench.projects import checks as project_check
            if args:
                raise ForgeError('Project checks accept no rule overrides', code='invalid_request')
            with dependency_scope(root, authority=request.get('bootstrap')):
                value = (project_check.check if op == 'project_check' else project_check.status)(root, table_resolver=table_resolver)
        elif op == 'project_table':
            from demiflow.workbench.projects.discovery import table
            from demiflow.workbench.projects.workspace import load_workspace
            root, _, _ = load_workspace(root)
            value = table(root, **args)
        elif op == "inspect":
            from demiflow.workbench.projects.workspace import _inspect_flow

            with dependency_scope(root, authority=request.get('bootstrap')):
                value = _inspect_flow(root, table_resolver=table_resolver, **args)
        elif op == 'plan_preview':
            from demiflow.workbench.graph.preview import preview as compile_preview
            value = compile_preview(root, table_resolver=table_resolver, source_modules=request.get('source_modules'), dependencies=request.get('dependencies'), **args)
        elif op == 'native_submission_inspect':
            from demiflow.workbench.runs.native_submission import inspect_submission
            value = inspect_submission(root, table_resolver=table_resolver, source_modules=request.get('source_modules'), dependencies=request.get('dependencies'), **args)
        elif op == 'workbench_source':
            from demiflow.workbench.graph.model import _source_tree
            value=_source_tree(root,**args)
        elif op == 'workbench_entry_arguments':
            from demiflow.workbench.graph.parameters import _entry_arguments
            value=_entry_arguments(root,**args)
        elif op == 'run_prepare_reconcile':
            from demiflow.workbench.runs.requests import _reconcile
            value=_reconcile(root,**args)
        elif op == 'run_admission':
            from demiflow.workbench.runs.admission import _describe
            value=_describe(root,table_resolver=table_resolver, **args)
        elif op in {"run_prepare", "run_preflight", "run_resume"}:
            from demiflow.workbench.runs import manager as run_manager
            from demiflow.workbench.runs.authority import from_bootstrap
            from demiflow.workbench.runs.operations import worker_arguments
            authority = from_bootstrap(request.get('bootstrap'), root)
            handler = {'run_prepare': run_manager._prepare, 'run_preflight': run_manager._preflight,
                       'run_resume': run_manager._resume}[op]
            trusted = {'source_modules': request.get('source_modules')} if op == 'run_prepare' else {}
            value = handler(root, **worker_arguments(op, args), authority=authority, **trusted)
        elif op == 'run_reconcile':
            from demiflow.workbench.runs import manager as runs
            value = runs._reconcile(root, **args)
        elif op == "status":
            from demiflow.workbench.projects.workspace import _report_status

            with dependency_scope(root, authority=request.get('bootstrap')):
                value = _report_status(root, table_resolver=table_resolver, verify_dependencies=True, **args)
        elif op == 'audit_capture':
            from demiflow.workbench.projects.workspace import _capture_change_baseline
            value = _capture_change_baseline(root, **args)
        elif op == 'audit_scope':
            from demiflow.workbench.projects.workspace import _check_change_scope
            if args:
                raise ValueError('Unknown scope check arguments')
            value = _check_change_scope(root)
        elif op == 'audit_check':
            from demiflow.workbench.projects.workspace import _audit_check
            with dependency_scope(root, authority=request.get('bootstrap')):
                value = _audit_check(root, table_resolver=table_resolver, **args)
        elif op == 'audit_hint':
            from demiflow.workbench.projects.workspace import _audit_hint
            value = _audit_hint(root, **args)
        elif op in {"table", "sample", "media", 'table_evidence', "table_versions"}:
            import re
            from demiflow.workbench.projects.workspace import load_workspace
            from demiflow.workbench.projects.workspace import preview_binding
            from demiflow.workbench.projects.workspace import location_root
            from demiflow.workbench.projects.workspace import relative_path
            from demiflow.forge.platform import content_digest

            expected = args.pop("expected_binding", None)
            if expected is not None and (type(expected) is not str or not re.fullmatch(r"[0-9a-f]{64}", expected)):
                raise ForgeError("Invalid preview binding digest", code='invalid_request')
            root, workspace, _ = load_workspace(root)
            alias = args.pop("alias")
            control, ref, uri = table_resolver(root, workspace, alias)
            binding = preview_binding(root, control, uri)
            if expected is not None and expected != binding["digest"]:
                raise ForgeError("Preview binding changed; reopen the table explicitly", code='conflict')
            if op != "table_versions":
                version = args.pop("version", None)
                version_binding = args.pop("version_binding", None)
                if version is not None or version_binding is not None:
                    if version is None or version_binding is None:
                        raise ForgeError("Historical version and version binding must be provided together", code='invalid_request')
                    if type(version) is not int or version < 1:
                        raise ForgeError("Version must be a fixed positive integer", code='invalid_request')
                    if type(version_binding) is not str or not re.fullmatch(r"[0-9a-f]{64}", version_binding):
                        raise ForgeError("Invalid version binding digest", code='invalid_request')
                    if expected is None:
                        raise ForgeError("Historical reads require the current alias binding", code='invalid_request')
                    from demiflow.workbench.analysis.catalog_versions import resolve_version
                    resolution = resolve_version(root, alias=alias, expected_binding=expected,
                                                 version=version, expected_version_binding=version_binding, table_resolver=table_resolver)
                    wire = resolution.to_wire()
                    control, ref, binding = wire["reference"], resolution.ref, wire["binding"]
            if op == "table_versions":
                version = args.pop("version", None)
                if args:
                    raise ValueError("Unknown table version arguments")
                if version is not None and (type(version) is not int or version < 1):
                    raise ForgeError("Version must be a fixed positive integer", code='invalid_request')
                if isinstance(alias, str) and alias.startswith("run:"):
                    # A run product is pinned to one sealed output version.
                    if version is not None:
                        raise ForgeError("Run outputs have one sealed version and cannot select another version", code='invalid_request')
                    value = {"status": "versions", "versions": [], "truncated": False,
                             "total_registered": 0, "reason": "run_output_sealed_version"}
                else:
                    from demiflow.lance.registry import Catalog
                    base = location_root(root, workspace["locations"][control["location_id"]])
                    registered = [item for item in Catalog(base).versions(ref.dataset_id)
                                  if item.store_id == ref.store_id and item.relative_uri == ref.relative_uri]
                    total = len(registered)
                    value = {"status": "versions", "versions": [item.to_dict() for item in registered[:128]],
                             "truncated": total > 128, "total_registered": total}
                    if not total:
                        value["reason"] = "no_registered_versions"
                    if version is not None:
                        if not any(item.lance_version == version for item in registered):
                            raise ForgeError("Version is not a registered version of this table", code='invalid_request')
                        from demiflow.workbench.analysis.catalog_versions import resolve_version
                        resolution = resolve_version(root, alias=alias,
                                                     expected_binding=expected if expected is not None else binding["digest"],
                                                     version=version, table_resolver=table_resolver)
                        value["selected"] = resolution.to_wire()
            elif op == "table":
                if args:
                    raise ValueError("Unknown table description arguments")
                metadata = preview.describe_table(uri, ref.lance_version)
                # Preserve native column order across sorted JSON object keys.
                value = {"status": "described", "reference": control,
                         "digest": content_digest(control),
                         "columns": list(metadata["fields"]), "metadata": metadata}
            elif op == "sample":
                if args.get('columns') is None and control.get('display', {}).get('fields'):
                    args['columns'] = list(dict.fromkeys(f['path'][0] for f in control['display']['fields']))
                value = preview.sample_table(uri, ref.to_dict(), **args)
            elif 'field_path' in args:
                from demiflow.workbench import catalog as table_view
                if set(args) != {'field_path', 'row_id'}:
                    raise ValueError('Invalid declared resource arguments')
                path = table_view.field_path(args['field_path'])
                kind = 'image' if op == 'media' else 'evidence'
                field = table_view.select_field(control.get('display', {}), path, kind)
                columns = list(dict.fromkeys([path[0]] + ([field['sha256_path'][0]] if kind == 'image' else [])))
                rows = preview.sample_table(uri, ref.to_dict(), columns=columns, row_id=args['row_id'])['rows']
                if len(rows) != 1:
                    raise ValueError('Resource requires exactly one fixed Case')
                base = location_root(root, workspace['locations'][field['location_id']])
                value = (table_view.image if kind == 'image' else table_view.evidence)(rows[0], path, field, base)
            else:
                if op != 'media' or set(args) != {"field", "row_id"} or type(args["field"]) is not str:
                    raise ValueError("Invalid media arguments")
                rows = preview.sample_table(
                    uri, ref.to_dict(), columns=[args["field"]], row_id=args["row_id"]
                )["rows"]
                if len(rows) != 1 or type(rows[0].get(args["field"])) is not str:
                    raise ValueError("Image field must reference one local image")
                base = location_root(root, workspace["locations"][control["location_id"]])
                path = relative_path(base, rows[0][args["field"]])
                value = preview.image_preview(path)
            value["binding"] = binding
        else:
            raise ValueError("Unknown workspace operation")
        result = {"format": FORMAT, "ok": True, "value": value}
    except (
        ValueError,
        TypeError,
        KeyError,
        OSError,
        RuntimeError,
        MemoryError,
        RecursionError,
        ImportError,
        sqlite3.Error,
        DemiflowError,
    ) as exc:
        result = {
            "format": FORMAT,
            "ok": False,
            "error": f"{type(exc).__name__}: {str(exc)[:2048]}",
            "code": ('forbidden' if isinstance(exc, PermissionError) else
                     'conflict' if isinstance(exc, ExecutionConflict) else
                     exc.code if isinstance(exc, ForgeError) else
                     'not_found' if isinstance(exc, FileNotFoundError) else
                     'resource_limit' if isinstance(exc, MemoryError) else 'operation_failed'),
        }
    return result


def main():
    confined = sys.argv[1:] == ['--confined-project']
    from demiflow.forge.platform_worker import bind_parent
    if not confined:
        bind_parent()
    from demiflow.forge.platform import bounded_json
    if sys.argv[1:]==["--reader"]:
        from demiflow.workbench.reader import serve
        serve(prepare_native,dispatch)
        return
    raw=sys.stdin.buffer.read(1024**2+1)
    if len(raw)>1024**2:raise ValueError("Workspace request exceeds budget")
    request=json.loads(raw)
    required = {"operation", "root", "arguments"}
    if (type(request) is not dict or not required <= request.keys()
            or request.keys() - required - {"bootstrap", "source_modules", "dependencies", "table_inputs"}
            or "source_modules" in request and (
                request["operation"] not in {"plan_preview", "run_prepare", "native_submission_inspect"}
                or type(request["source_modules"]) is not dict)):
        raise ValueError("Invalid workspace request")
    if ('dependencies' in request) != confined or ('table_inputs' in request) != confined:
        raise ValueError("Project dependency inspection requires the confined worker path")
    if confined:
        if request['operation'] not in {'plan_preview', 'native_submission_inspect'} or 'bootstrap' in request:
            raise ValueError('Only fixed business inspection may use the project worker')
        from demiflow.forge.dependencies import current_environment
        locked = request['dependencies']
        if current_environment(locked, check_conflicts=True) != locked['environment']:
            raise ValueError('Project Python changed after dependency approval')
    prepare_native(analysis_read=request["operation"] in {"analysis_export","analysis_media_export","analysis_media_sources","analysis_source_read","table_profile"}, confined=confined)
    result=dispatch(request)
    sys.stdout.buffer.write(bounded_json(result,9*1024**2))


if __name__=="__main__":main()
