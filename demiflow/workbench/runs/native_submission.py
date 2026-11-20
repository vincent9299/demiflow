"""Native flow admission/sealing through P4; no execution or commit authority."""
import hashlib
import importlib
import inspect
import os
from pathlib import Path
import sys

from demiflow.forge.platform import content_digest, bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import workspace as ws

FORMAT = 'demiflow_forge_native_inspection_v1'


def inspect_submission(root, *, table_resolver, entry, parameters, limits, source_modules=None):
    """Fixed disposable worker: original code construction is read-only."""
    from contextlib import redirect_stdout
    from demiflow.execution.host.inspection import capture_managed_pipeline, native_submission_view
    from demiflow.workbench.graph.preview import _Discard
    from demiflow.workbench.projects.python_sources import PythonSources
    from demiflow.workbench.projects.shared_sources import SharedSources
    from demiflow.workbench.graph.preview_guard import enter_readonly
    from demiflow.workbench.projects.discovery import discover
    root, control, platform = ws.load_workspace(root)
    project = discover(root, control)
    if not any(item['entry'] == entry and item['status'] == 'recognized' for item in project['entries']):
        raise ForgeError('Native entry must be declared by this project')
    target, symbol = ws.validate_entry(root, entry)
    shared = SharedSources(root, {} if source_modules is None else source_modules)
    sources = PythonSources(root, shared_imports=shared.importer)
    module = sources.entry_module(target)
    arguments, tables, targets, _ = ws.resolve_arguments(root, control, parameters, table_resolver=table_resolver)
    if targets:
        raise ForgeError('Native admission cannot mix implicit and explicit output grants')
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(root)]
    sys.meta_path.insert(0, sources)
    sys.meta_path.insert(0, shared.importer)
    os.chdir(root)
    reads = set()
    tracking = [False]
    def opened(event, args):
        if tracking[0] and event == 'open' and isinstance(args[0], (str, bytes)):
            path = Path(os.fsdecode(args[0])).absolute()
            # Native libraries own their files; only project Python/config
            # closure is copied. Fixed external inputs remain in place.
            if path.is_relative_to(root):
                if len(reads) >= 512 and path not in reads:
                    raise ForgeError('Native source resource closure exceeds budget')
                reads.add(path)
    sys.addaudithook(opened)
    enter_readonly()
    tracking[0] = True
    try:
        with redirect_stdout(_Discard()):
            function = getattr(importlib.import_module(module), symbol)
            if not inspect.isfunction(function) or Path(inspect.getfile(function)) != target:
                raise ForgeError('Native entry resolved outside its declared source')
            inspect.signature(function).bind(**arguments)
            from demiflow.workbench.graph.plan_callsite import PlanCallsites, pipeline_material
            from demiflow.workbench.graph.plan_details import build_plan_details
            with PlanCallsites(root, sources.digests) as callsites:
                captured = capture_managed_pipeline(function, arguments, limits=limits, on_compiled=callsites.bind_compilation)
            shared_binding = shared.binding()
            details = build_plan_details(captured.compiled, sources=sources.digests, root=root,
                                         callsites=callsites, bounds=captured.bounds, shared_sources=shared_binding)
            pipeline = pipeline_material(root, target, sources.digests)
    except ModuleNotFoundError as exc:
        raise sources.missing_import(exc) from exc
    finally:
        tracking[0] = False
    sources.verify()
    resources = {}
    for path in sorted(reads):
        name = path.relative_to(root).as_posix()
        if name in sources.digests or not path.is_file():
            continue
        ws.no_symlinks(path)
        raw = read_bounded(path, 16 * 1024**2)
        resources[name] = dict(kind='file', path=name, sha256=hashlib.sha256(raw).hexdigest(), max_bytes=16 * 1024**2)
    view = native_submission_view(captured)
    for index, path in enumerate(captured.submission['object_roots']):
        resources['objects-' + str(index)] = dict(kind='objects', root=path, access='write')
    result = dict(format=FORMAT, status='compiled', entry=entry,
        **{k: view[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        native_contract=captured.submission, native_dag=view['dag'], native_details=details,
        pipeline_detail=pipeline,
        observation_instances=view['observations'], diagnostics=[],
        coverage=dict(execution='not_run', scope='first_action'),
        binding=dict(entry=entry, source_root=str(root),
            source_entry=target.relative_to(root).as_posix() + ':' + symbol,
            sources=sources.digests, resources=resources, arguments=arguments,
            parameters=parameters, tables=tables, targets={},
            workspace_digest=content_digest(control), platform_digest=content_digest(platform)))
    if sources.package:
        result['binding'].update(source_package='.'.join(sources.package), entry_module=module)
    if shared_binding:
        result['binding']['shared_sources'] = shared_binding
    result['digest'] = content_digest(result)
    bounded_json(result, 8 * 1024**2)
    return result


def prepare(root, entry, *, parameters, limits, metadata, source_modules=None):
    from demiflow.workbench.worker import operation
    report = operation('native_submission_inspect', root, entry=entry, parameters=parameters,
                       limits=limits, _source_modules=source_modules)
    outputs = {}
    for index, output in enumerate(report['native_contract']['outputs']):
        uri = ws.no_symlinks(Path(output['uri']))
        if uri.exists():
            raise ForgeError('Create target already exists: ' + str(uri))
        # Every output uses its actual compiled target, never guessed argument
        # names or rewritten business parameters. History stores that exact URI.
        name = output.get('stage', 'result')
        if name in outputs:
            name = name + '_' + str(index)
        ws.identifier(name)
        info = dict(dataset_id=name, schema_name=name, schema_version='1')
        supplied = metadata.get(name, {})
        if type(supplied) is not dict or supplied.keys() - info.keys():
            raise ForgeError('Invalid native output metadata')
        info.update(supplied)
        if any(type(v) is not str or not v or len(v) > 128 for v in info.values()):
            raise ForgeError('Invalid native output metadata')
        outputs[name] = {**info, **output, 'action_id': output['node_id'],
                         'relative_uri': uri.name, 'location_id': 'native_' + str(index),
                         'location_root': str(uri.parent),
                         'allocation_root': str(uri if uri.parent.exists() else uri.parent)}
        if 'stage' in output:
            outputs[name]['publication'] = 'native_stage_v3'
        report['binding']['targets'][str(uri)] = dict(mode='create', expected_version=None)
    if metadata.keys() - outputs.keys():
        raise ForgeError('Unknown native output metadata')
    if sum(o.get('max_index_bytes', 0) for o in outputs.values()) > limits['max_scratch_bytes']:
        raise ForgeError('Native stage index reservation exceeds scratch budget')
    report.pop('digest')
    report['digest'] = content_digest(report)
    return report, outputs


def verify_basis(root, manifest, report):
    from demiflow.forge import audit
    from demiflow.workbench.projects.shared_sources import MAX_MODULE_BYTES, bound_files
    root, workspace, platform = ws.load_workspace(root)
    audit.require_scope(ws._check_change_scope(root), expected_scope=manifest.get('change_scope'))
    binding = report['binding']
    original = manifest['workspace']
    if content_digest(original) != binding['workspace_digest']:
        raise ForgeError('Native original project basis digest differs', code='conflict')
    if {k: v for k, v in workspace.items() if k != 'platform'} != {
            k: v for k, v in original.items() if k != 'platform'}:
        raise ForgeError('Native prepared project basis changed', code='conflict')
    base = Path(binding['source_root'])
    for name, digest in binding['sources'].items():
        if hashlib.sha256(read_bounded(ws.relative_path(base, name), 4 * 1024**2)).hexdigest() != digest:
            raise ForgeError('Native prepared source changed', code='conflict')
    for name, origin, _, digest in bound_files(binding):
        if hashlib.sha256(read_bounded(ws.no_symlinks(origin), MAX_MODULE_BYTES)).hexdigest() != digest:
            raise ForgeError('Native prepared shared source changed: ' + name, code='conflict')
    for resource in binding['resources'].values():
        if resource['kind'] in {'config', 'file'} and hashlib.sha256(read_bounded(
                ws.relative_path(base, resource['path']), resource['max_bytes'])).hexdigest() != resource['sha256']:
            raise ForgeError('Native prepared resource changed', code='conflict')
    if manifest['parameter_path'] and ws.read_control(root / manifest['parameter_path']) != manifest['original_parameters']:
        raise ForgeError('Native prepared parameters changed', code='conflict')
