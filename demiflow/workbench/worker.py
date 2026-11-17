"""One fixed, resource-limited protocol for inspection and read-only case I/O."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
import sys

FORMAT = "demiflow_forge_workspace_response_v1"


def operation(name, root, *, _pass_fds=(), _authority=None, **arguments):
    from demiflow.forge.platform_worker import capture
    from demiflow.forge.platform import limits_with
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.host_partition import PREPARATION_RSS
    from demiflow.workbench.host_partition import location
    from demiflow.workbench.host_partition import READONLY_RSS

    from demiflow.workbench.reader import current
    from demiflow.workbench.reader import ALLOWED
    request={"operation":name,"root":str(Path(root).absolute()),"arguments":arguments}
    if _authority is not None:
        request["bootstrap"] = _authority
    settings=limits_with({"catalog_bytes":9*1024**2,**({"timeout_seconds":20} if name in {"analysis_export","analysis_media_export","analysis_media_sources","analysis_source_read","table_profile"} else {})})
    context=current()
    if context is not None:
        reader,consumer_closed=context
        import os
        reusable=not _pass_fds and not os.environ.get('DEMIFLOW_FORGE_PREPARATION_FD') and name in ALLOWED
        if reusable:
            value=reader.call(request,timeout=min(20,settings['timeout_seconds']),consumer_closed=consumer_closed)
        else:
            reader.retire_idle()
            value=None
    else:value=None
    if value is None:
        value=capture([sys.executable,"-I","-B",str(Path(__file__).resolve())],request,settings,
            response_formats={FORMAT},input_limit=1024**2,pass_fds=_pass_fds,
            tree_rss_limit=PREPARATION_RSS if _pass_fds or name in {"analysis_media_sources","analysis_source_read","table_profile"} else (READONLY_RSS if (location() / 'host-budget.json').exists() else None))
    if not value["ok"]:
        code = value.get('code', 'operation_failed')
        if code == 'forbidden':
            raise PermissionError(value['error'])
        if code not in ForgeError.CODES:
            raise ForgeError('Invalid workspace error code', code='operation_failed')
        raise ForgeError(value["error"], code=code)
    return value["value"]


def prepare_native(*, analysis_read):
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
    try:
        from demiflow.errors import DemiflowError
        from demiflow.lance import preview

        if op == "describe":
            value = preview.describe_table(root, args["version"])
        elif op == "analysis_export":
            from demiflow.workbench.analysis.source_export import export
            if set(args) != {"source_locator", "source_snapshot", "output_fd"}:
                raise ForgeError("Invalid fixed source export arguments",code="invalid_request")
            value = export(root, **args)
        elif op=="analysis_media_export":
            from demiflow.workbench.analysis.media_source_export import export
            if set(args)!={"source_locator","source_snapshot","case_key","field","output_fd"}:
                raise ForgeError("Invalid fixed declared media export arguments",code="invalid_request")
            value=export(root,**args)
        elif op=="analysis_media_sources":
            from demiflow.workbench.analysis.media_source_export import resolve_fields
            if set(args)!={"source_locator","source_snapshot","case_key","fields"}:
                raise ForgeError("Invalid fixed declared media proof arguments",code="invalid_request")
            value=resolve_fields(root,**args)
        elif op == "analysis_source_read":
            from demiflow.workbench.analysis.source_query import read
            if set(args)!={"source_locator","source_snapshot","query"}:
                raise ForgeError("Invalid fixed source sample/profile arguments",code="invalid_request")
            value=read(root,**args)
        elif op=="table_profile":
            from demiflow.workbench.analysis.catalog_profile import profile
            from demiflow.workbench.analysis import contracts
            contracts.fields(args,{"kind","columns"},{"alias","expected_binding","identity","expected","version","binding","version_binding"},label="catalog profile")
            value=profile(root,**args)
        elif op == 'metadata':
            value = preview.snapshot_metadata(root, args['version'])
        elif op == "catalog":
            ref = preview.catalog_reference(root, **args)
            value = ref.to_dict() if ref else None
        elif op in {'project_check', 'project_status'}:
            from demiflow.workbench.projects import checks as project_check
            if args:
                raise ForgeError('Project checks accept no rule overrides', code='invalid_request')
            value = (project_check.check if op == 'project_check' else project_check.status)(root)
        elif op == 'project_table':
            from demiflow.workbench.projects.discovery import table
            from demiflow.workbench.projects.workspace import load_workspace
            root, _, _ = load_workspace(root)
            value = table(root, **args)
        elif op == "inspect":
            from demiflow.workbench.projects.workspace import _inspect_flow

            value = _inspect_flow(root, **args)
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
            value=_describe(root,**args)
        elif op in {"run_prepare", "run_preflight", "run_resume"}:
            from demiflow.workbench.runs import manager as run_manager
            from demiflow.workbench.runs.authority import from_bootstrap
            from demiflow.workbench.runs.operations import worker_arguments
            authority = from_bootstrap(request.get('bootstrap'), root)
            handler = {'run_prepare': run_manager._prepare, 'run_preflight': run_manager._preflight,
                       'run_resume': run_manager._resume}[op]
            value = handler(root, **worker_arguments(op, args), authority=authority)
        elif op == 'run_reconcile':
            from demiflow.workbench.runs import manager as runs
            value = runs._reconcile(root, **args)
        elif op == "status":
            from demiflow.workbench.projects.workspace import _report_status

            value = _report_status(root, **args)
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
            value = _audit_check(root, **args)
        elif op == 'audit_hint':
            from demiflow.workbench.projects.workspace import _audit_hint
            value = _audit_hint(root, **args)
        elif op in {"table", "sample", "media", 'table_evidence', "table_versions"}:
            import re
            from demiflow.workbench.projects.workspace import load_workspace
            from demiflow.workbench.projects.workspace import table_binding
            from demiflow.workbench.projects.workspace import preview_binding
            from demiflow.workbench.projects.workspace import location_root
            from demiflow.workbench.projects.workspace import relative_path
            from demiflow.forge.platform import content_digest

            expected = args.pop("expected_binding", None)
            if expected is not None and (type(expected) is not str or not re.fullmatch(r"[0-9a-f]{64}", expected)):
                raise ForgeError("Invalid preview binding digest", code='invalid_request')
            root, workspace, _ = load_workspace(root)
            alias = args.pop("alias")
            control, ref, uri = table_binding(root, workspace, alias)
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
                                                 version=version, expected_version_binding=version_binding)
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
                                                     version=version)
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
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from demiflow.forge.platform_worker import bind_parent
    bind_parent()
    from demiflow.forge.platform import bounded_json
    if sys.argv[1:]==["--reader"]:
        from demiflow.workbench.reader import serve
        serve(prepare_native,dispatch)
        return
    raw=sys.stdin.buffer.read(1024**2+1)
    if len(raw)>1024**2:raise ValueError("Workspace request exceeds budget")
    request=json.loads(raw)
    if type(request) is not dict or set(request) not in ({"operation","root","arguments"}, {"operation","root","arguments","bootstrap"}):raise ValueError("Invalid workspace request")
    prepare_native(analysis_read=request["operation"] in {"analysis_export","analysis_media_export","analysis_media_sources","analysis_source_read","table_profile"})
    result=dispatch(request)
    sys.stdout.buffer.write(bounded_json(result,9*1024**2))


if __name__=="__main__":main()
