"""Standalone bounded single-project run inspector subprocess owned by Forge.

Handles single-project run inspection, preparation, preflight, resume, reconcile,
and fixed-version Lance metadata reads with ZERO Workbench imports. Optional hosts
(such as Workbench) may register a host operation delegate to route through a warm
reader pool or enrich compiled plans with UI presentation materials.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

if __name__ == '__main__':
    platform_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(platform_root))
    if sys.argv[1:] == ['--confined-project']:
        from importlib.util import find_spec
        spec = find_spec('demiflow')
        if spec is None or not spec.origin or Path(spec.origin).resolve() != platform_root / 'demiflow/__init__.py':
            raise RuntimeError('Project Python loaded DemiFlow outside the trusted platform source')

from demiflow.forge import operation_budget

FORMAT = 'demiflow_forge_workspace_response_v1'

_HOST_OPERATION = None
_MATERIAL_OBSERVER = None


def register_host_operation(fn):
    """Allow an optional host (Workbench) to route run operations through its worker/reader."""
    global _HOST_OPERATION
    _HOST_OPERATION = fn


def register_material_observer(fn):
    """Allow an optional host (Workbench) to supply UI callsite/node-details materials."""
    global _MATERIAL_OBSERVER
    _MATERIAL_OBSERVER = fn


def material_observer():
    return _MATERIAL_OBSERVER


@operation_budget.controlled
def operation(name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
    if _HOST_OPERATION is not None:
        return _HOST_OPERATION(
            name, root, _pass_fds=_pass_fds, _authority=_authority,
            _source_modules=_source_modules, _dependencies=_dependencies, **arguments,
        )
    return run_operation(
        name, root, _pass_fds=_pass_fds, _authority=_authority,
        _source_modules=_source_modules, _dependencies=_dependencies, **arguments,
    )


@operation_budget.controlled
def run_operation(name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
    from demiflow.forge.host_partition import PREPARATION_RSS, READONLY_RSS, location
    from demiflow.forge.platform import limits_with
    from demiflow.forge.platform_worker import capture
    from demiflow.forge.schema import ForgeError

    request = {'operation': name, 'root': str(Path(root).absolute()), 'arguments': arguments}
    root_path = Path(request['root'])
    inspection_reads = ()
    if _authority is not None:
        request['bootstrap'] = _authority
    if name == 'run_prepare':
        from demiflow.forge.runtime import deployment_modules
        _source_modules = deployment_modules(root_path)
    if name == 'native_submission_inspect' and _dependencies is not None:
        request.pop('bootstrap', None)
        request['dependencies'] = _dependencies
        inspection_reads, request['table_inputs'] = _inspection_inputs(root_path, request)
    if _source_modules is not None:
        request['source_modules'] = _source_modules
    settings = limits_with({'catalog_bytes': 9 * 1024**2})
    business = name == 'native_submission_inspect' and _dependencies is not None
    python = sys.executable
    if business:
        from demiflow.forge.runtime import launch_confined_python, verify_interpreter
        interpreter = _dependencies['environment']['interpreter']
        python = verify_interpreter(interpreter)
    argv = [python, '-I', '-B', str(Path(__file__).resolve())]
    launcher = None
    if business:
        argv.append('--confined-project')
        launcher = lambda command, *, stdin, cwd, env, **_: launch_confined_python(
            root_path, command, interpreter=interpreter, dependencies=_dependencies,
            source_modules=_source_modules, read_paths=inspection_reads,
            seconds=settings['timeout_seconds'], rss_bytes=PREPARATION_RSS,
            cwd=cwd, env=env, stdin=stdin,
        )
    value = capture(
        argv, request, settings, response_formats={FORMAT}, input_limit=1024**2,
        pass_fds=_pass_fds, inherit_authority=not business, launcher=launcher,
        tree_rss_limit=PREPARATION_RSS if business or _pass_fds else (READONLY_RSS if (location() / 'host-budget.json').exists() else None),
    )
    if not value['ok']:
        code = value.get('code', 'operation_failed')
        if code == 'forbidden':
            raise PermissionError(value['error'])
        if code not in ForgeError.CODES:
            raise ForgeError('Invalid workspace error code', code='operation_failed')
        raise ForgeError(value['error'], code=code)
    return value['value']


def _inspection_inputs(root: Path, request: dict):
    """Grant project source and exact input tables while keeping controls private."""
    from demiflow.forge import project_workspace as ws
    from demiflow.forge.project import STATE_PATH
    from demiflow.forge.project_bindings import extract_arguments, is_direct_table_ref, resolve_table_uri
    from demiflow.forge.project_rules import ROLE_DIRS
    from demiflow.forge.runs import manager as runs
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.table_bindings import make_resolver

    _, workspace = ws.load_workspace_control(root)
    supplied = request['arguments']
    paths = {root / 'workspace.json', root / STATE_PATH, ws.relative_path(root, workspace['platform']['path'])}
    parameters = supplied.get('parameters', {})
    fixed_inputs = {}
    allowed = set(ROLE_DIRS) | {'tables', 'configs', 'parameters', 'lib', 'src', 'data'}
    with os.scandir(root) as entries:
        for index_i, item in enumerate(entries):
            if index_i >= 1024:
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
                control, _, target = resolver(root, workspace, alias)
                paths.add(target)
                if alias.startswith('run:'):
                    fixed_inputs[alias] = dict(control=control, path=str(target))
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


def _fixed_table_resolver(table_inputs):
    from demiflow.forge import project_workspace as ws
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.table_bindings import make_resolver

    def fixed_run_table(root, run_id, output, *, attempt=None):
        alias = f'run:{run_id}:{output}' + ('' if attempt is None else f':{attempt}')
        item = table_inputs.get(alias)
        if item is None:
            raise ForgeError('Fixed run table input was not admitted for project inspection', code='operation_rejected')
        return item['control'], ws.dataset_ref(item['control']['reference']), Path(item['path'])

    return make_resolver(ws.table_binding, fixed_run_table)


def _prepare_native(*, confined=False):
    import resource

    os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
    os.environ.setdefault('OMP_NUM_THREADS', '1')
    os.environ.setdefault('MKL_NUM_THREADS', '1')
    os.environ.setdefault('NUMEXPR_NUM_THREADS', '1')
    os.environ.setdefault('RAYON_NUM_THREADS', '2')
    os.environ.setdefault('TOKIO_WORKER_THREADS', '2')
    os.environ.setdefault('LANCE_CPU_THREADS', '2')
    os.environ.setdefault('CUDA_VISIBLE_DEVICES', '')
    import pyarrow as pa
    import lance  # noqa: F401 - preload native libraries before measuring baseline RSS/VSZ

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    if confined:
        ceiling = min([4 * 1024**3, *(limit for limit in resource.getrlimit(resource.RLIMIT_AS)
                                      if limit != resource.RLIM_INFINITY)])
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
        return
    with open('/proc/self/statm') as source:
        baseline = int(source.read(128).split()[0]) * os.sysconf('SC_PAGE_SIZE')
    ceiling = min(4 * 1024**3, baseline + 384 * 1024**2)
    for inherited in resource.getrlimit(resource.RLIMIT_AS):
        if inherited != resource.RLIM_INFINITY:
            ceiling = min(ceiling, inherited)
    if ceiling - baseline < 128 * 1024**2:
        raise MemoryError('Insufficient worker allocation headroom')
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))


def dispatch(request):
    from demiflow.errors import DemiflowError, ExecutionConflict
    from demiflow.forge import project_workspace as ws
    from demiflow.forge.runs import manager as run_manager
    from demiflow.forge.runs.authority import from_bootstrap
    from demiflow.forge.runs.operations import worker_arguments
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.table_bindings import make_resolver

    op = request.get('operation')
    root = request.get('root')
    args = request.get('arguments', {})
    if type(args) is not dict or type(root) is not str:
        return {'format': FORMAT, 'ok': False, 'error': 'Invalid workspace request', 'code': 'invalid_request'}
    table_resolver = (_fixed_table_resolver(request['table_inputs']) if 'table_inputs' in request
                      else make_resolver(ws.table_binding, run_manager.result_table))
    try:
        if op == 'metadata':
            from demiflow.lance import preview
            value = preview.snapshot_metadata(root, args['version'])
        elif op == 'native_submission_inspect':
            from demiflow.forge.runs.native_submission import inspect_submission
            value = inspect_submission(
                root, table_resolver=table_resolver,
                source_modules=request.get('source_modules'),
                dependencies=request.get('dependencies'),
                **args,
            )
        elif op == 'run_prepare_reconcile':
            from demiflow.forge.runs.requests import reconcile_in_worker
            value = reconcile_in_worker(root, **args)
        elif op == 'run_admission':
            from demiflow.forge.runs.admission import describe_in_worker
            value = describe_in_worker(root, table_resolver=table_resolver, **args)
        elif op in {'run_prepare', 'run_preflight', 'run_resume'}:
            authority = from_bootstrap(request.get('bootstrap'), root)
            handler = {'run_prepare': run_manager._prepare, 'run_preflight': run_manager._preflight,
                       'run_resume': run_manager._resume}[op]
            trusted = {'source_modules': request.get('source_modules')} if op == 'run_prepare' else {}
            value = handler(root, **worker_arguments(op, args), authority=authority, **trusted)
        elif op == 'run_reconcile':
            value = run_manager.reconcile_in_worker(root, **args)
        else:
            raise ForgeError('Unsupported Forge run inspector operation: ' + str(op), code='invalid_request')
        return {'format': FORMAT, 'ok': True, 'value': value}
    except PermissionError as exc:
        return {'format': FORMAT, 'ok': False, 'error': str(exc), 'code': 'forbidden'}
    except (ForgeError, DemiflowError, ExecutionConflict) as exc:
        code = getattr(exc, 'code', None)
        if code not in ForgeError.CODES:
            code = 'conflict' if isinstance(exc, ExecutionConflict) else 'operation_failed'
        return {'format': FORMAT, 'ok': False, 'error': str(exc), 'code': code}
    except (ValueError, TypeError, OSError, RuntimeError, KeyError) as exc:
        return {'format': FORMAT, 'ok': False, 'error': f'{type(exc).__name__}: {exc}', 'code': 'operation_failed'}


def main():
    confined = sys.argv[1:] == ['--confined-project']
    if sys.argv[1:] and not confined:
        raise ValueError('Unsupported Forge run inspector arguments')
    from demiflow.forge.platform import bounded_json
    from demiflow.forge.platform_worker import bind_parent
    if not confined:
        bind_parent()
    raw = sys.stdin.buffer.read(1024**2 + 1)
    if len(raw) > 1024**2:
        raise ValueError('Workspace request exceeds budget')
    request = json.loads(raw)
    required = {'operation', 'root', 'arguments'}
    if (type(request) is not dict or not required <= request.keys()
            or request.keys() - required - {'bootstrap', 'source_modules', 'dependencies', 'table_inputs'}):
        raise ValueError('Invalid workspace request')
    if ('dependencies' in request) != confined or ('table_inputs' in request) != confined:
        raise ValueError('Project dependency inspection requires the confined worker path')
    if confined:
        if request['operation'] != 'native_submission_inspect' or 'bootstrap' in request:
            raise ValueError('Only fixed business inspection may use the project worker')
        from demiflow.forge.runtime import current_environment
        locked = request['dependencies']
        if current_environment(locked, check_conflicts=True, strict=True) != locked['environment']:
            raise ValueError('Project Python changed after dependency verification')
    _prepare_native(confined=confined)
    result = dispatch(request)
    sys.stdout.buffer.write(bounded_json(result, 9 * 1024**2))


if __name__ == '__main__':
    main()
