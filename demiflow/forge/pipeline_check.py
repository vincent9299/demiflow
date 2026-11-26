"""Read-only authoring checks against native SDK contracts and project files."""
from __future__ import annotations

import ast
import copy
from hashlib import sha256
from pathlib import Path
import time

from demiflow.errors import DemiflowError
from . import operation_budget
from .api_reference import build_reference, sdk_identity
from .definition_exports import EXPORT_FIELDS, exports
from .flow_check import FlowChecker
from .operator_check import OperatorChecker
from .platform import content_digest
from .platform_environment import read_bounded
from .platform_worker import describe_fixed_table
from .project import discover_project_sources, inspect_prompt_source, load_project, validate_entry, validate_parameter_path
from .project_bindings import resolve_arguments, resolve_flow_resources, table_binding, table_identity
from .project_io import decode_control, relative_path
from .project_rules import (
    ROLE_DIRS,
    RULES_ID,
    check_layout,
    check_project_hygiene,
    check_source_import_boundaries,
    diagnostic,
)
from .schema import ForgeError
from .source import Definition, InspectionError, SOURCE_LIMITS, SourceTree


MAX_VARIANTS = 64
CHECK_SECONDS = 180
MAX_SOURCE_BYTES = 16 * 1024**2


def _tree_seconds(deadline):
    return max(1, min(SOURCE_LIMITS['seconds'], int(deadline - time.monotonic())))


_SUPPRESSED_INCOMPLETE_RULES = frozenset({'OP-06', 'OP-03'})


def _keep_diagnostic(item):
    if not isinstance(item, dict):
        return False
    if item.get('status') == 'incomplete' and item.get('rule') in _SUPPRESSED_INCOMPLETE_RULES:
        return False
    return True


def result(diagnostics, inspected, *, applicable=True):
    diagnostics = [d for d in diagnostics if _keep_diagnostic(d)]
    invalid = any(d.get('status') == 'invalid' for d in diagnostics)
    status = ('failed' if invalid else 'incomplete' if diagnostics else
              'passed' if applicable else 'not_applicable')
    return dict(status=status, inspected_count=len(inspected), diagnostic_count=len(diagnostics),
                diagnostics=[{k: (v[:256] if isinstance(v, str) else v)
                              for k, v in d.items() if k in {'rule', 'path', 'line', 'detail'}}
                             for d in diagnostics[:16]])


def _record(sources, path, value):
    if sources.setdefault(path, value) != value:
        raise ForgeError('Source changed between inspections: ' + path, code='inputs_changed')


def inspect_variant(root, control, entry, parameters, catalog, *, table_resolver, limits=None,
                    exclude=(), table_describer=None):
    """Produce the sole native report used by both authoring and local run preparation."""
    args, tables, targets, controls = resolve_arguments(
        root, control, parameters, table_resolver=table_resolver, table_describer=table_describer)
    def resources(tree, references, bindings):
        return resolve_flow_resources(root, tree, references, bindings, control=control)
    report = FlowChecker(root, catalog, tables, targets, limits=limits, exclude=exclude,
                         resource_resolver=resources).inspect(entry, args)
    report['binding'].update(controls=controls, parameters=parameters)
    return report


def _layout(report):
    binding = report['binding']
    path = report['entry'].partition(':')[0]
    issues = []
    if Path(path).parts[0] != 'flows':
        issues.append(diagnostic('DF-02', path, 'Flow 入口应位于 flows/。'))
    for other in sorted({n['source'] for n in report['nodes']
                         if n['source'] != path and n['source'].startswith('flows/')}):
        issues.append(diagnostic('DF-03', other, 'Flow 之间通过共享算子复用逻辑。'))
    operators, prompts = set(), set()
    for node in report['nodes']:
        callbacks = list(node.get('callbacks', {}).values())
        if node.get('operator'):
            callbacks.append(node['operator'])
        for callback in callbacks:
            module = callback['module'].replace('.', '/')
            filename = next((p for p in (module + '.py', module + '/__init__.py')
                             if p in binding['sources']), None)
            if filename:
                operators.add((filename, callback['line']))
            else:
                issues.append(diagnostic('DF-01', module, '无法确认算子定义位置。', status='incomplete'))
        if node['api'].rsplit('.', 1)[-1] in {'map_prompt', 'map_prompt_async', 'agentmap_async'}:
            prompts.update((resource['path'], None) for resource in node['source_resources'])
    inspected, violations = check_layout(changed=(path,), operators=sorted(operators), prompts=sorted(prompts))
    return inspected | {path}, issues + violations


def _visible(path, exclude):
    value = Path(path)
    return not any(value.is_relative_to(parent) for parent in exclude)


def _declared_exports(root, paths, sources, layout, deadline, max_bytes):
    """Validate published symbols even when no native flow consumes the module."""
    declared = {}
    for path in paths:
        file = Path(path)
        field = EXPORT_FIELDS.get(file.parts[0])
        if not field or file.suffix != '.py':
            continue
        operation_budget.check(deadline)
        try:
            raw = read_bounded(relative_path(root, path), min(512 * 1024, max_bytes))
            max_bytes -= len(raw)
            _record(sources, path, sha256(raw).hexdigest())
            names = exports(ast.parse(raw, filename=path), field)
            if names is not None:
                declared[path] = names
        except FileNotFoundError:
            continue
        except ForgeError as exc:
            if exc.code in {'conflict', 'inputs_changed', 'resource_limit'}:
                raise
            layout.append(diagnostic('DF-02', path, str(exc)))
        except (SyntaxError, ValueError, RecursionError, OSError) as exc:
            layout.append(diagnostic('DF-02', path, str(exc), line=getattr(exc, 'lineno', None)))
    return declared


def _candidates(root, view, paths, entry, params, sources, native, layout):
    """Select explicit variants, or conservatively resolve all possible shared consumers."""
    entries = (view['control'] or {}).get('entries', {})
    if entry is not None:
        validate_entry(root, entry)
        return [(entry, p, False) for p in ([params] if params else entries.get(entry, [None]))], set()
    if not paths:
        return [(e, p, False) for e, bindings in entries.items() for p in bindings], set()
    declared = {e.partition(':')[0] for e in entries}
    parameters = {p for bindings in entries.values() for p in bindings}
    relevant = set()
    direct = []
    drafts = set()
    for path in paths:
        file = Path(path)
        if (path in declared or path in parameters or file.parts[0] in ROLE_DIRS
                or path == 'workspace.json' or file.parts[0] == 'tables'
                or (file.parts[0] not in {'tests', 'docs'} and file.suffix.lower() != '.md')):
            relevant.add(path)
        physical = relative_path(root, path)
        if path not in relevant:
            continue
        if not physical.exists():
            if path in declared or path in parameters:
                layout.append(diagnostic('DF-02', path, '登记的入口或参数文件已删除。'))
            continue
        if path in declared or file.parts[0] == 'data_tools' or file.suffix != '.py' or file.name == '__init__.py':
            continue
        # A misplaced terminal pipeline is detectable from its wiring, never from its name.
        raw = read_bounded(physical, 128 * 1024)
        _record(sources, path, sha256(raw).hexdigest())
        try:
            tree = ast.parse(raw, filename=path)
        except (SyntaxError, ValueError) as exc:
            native.append(diagnostic('FL-01', path, '源码语法错误。', line=getattr(exc, 'lineno', None)))
            continue
        public = [n.name for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith('_')]
        is_flow = file.parts[0] == 'flows'
        if len(public) != 1:
            if is_flow:
                layout.append(diagnostic('DF-02', path, '新 Flow 需要一个明确的公开入口。'))
                native.append(diagnostic('FL-01', path, '缺少唯一的 Flow 入口。', status='incomplete'))
            continue
        if is_flow:
            drafts.add(path)
        direct.append((path + ':' + public[0], None, not is_flow))

    shared = any(Path(p).parts[0] not in {'data_tools', 'workflows'} for p in relevant - declared - parameters - drafts)
    variants = [(e, p, False) for e, bound in entries.items() for p in bound
                if shared or e.partition(':')[0] in relevant or p in relevant]
    return variants + direct, relevant


def _binding_files(root, binding, sources, table_files):
    files = dict(binding['sources'])
    for resource in binding['resources'].values():
        if 'sha256' in resource:
            _record(files, resource['path'], resource['sha256'])
    for name, digest in files.items():
        _record(sources, name, digest)
    for name, semantic in binding['controls'].items():
        raw = read_bounded(relative_path(root, name), 16384)
        if content_digest(table_identity(decode_control(raw))) != semantic:
            raise ForgeError('Pinned table changed during inspection', code='inputs_changed')
        _record(table_files, name, sha256(raw).hexdigest())
    return files.keys() | binding['controls'].keys()


def _check_agent_tools(root, references, *, deadline, exclude, sources):
    """Verify project-owned tool definitions and their statically supported Python closure."""
    from demiflow.data.callable_contract import bind_parameters
    tree = SourceTree(root, {'seconds': _tree_seconds(deadline)}, exclude=exclude)
    checker = OperatorChecker(tree)
    try:
        for reference in references:
            entry, kind = reference['entry'], reference['kind']
            module, symbol = entry.split(':')
            # A declaration cannot re-export another module's callable.
            item = tree.load(module).get(symbol)
            if not isinstance(item, Definition) or item.module != module:
                raise InspectionError('OP-01', '工具无法定位到声明中的本地定义：' + entry,
                                      path=module.replace('.', '/') + '.py', status='incomplete')
            if (kind == 'actor') != isinstance(item.node, ast.ClassDef):
                raise InspectionError('OP-02', '工具形态与声明不符：' + entry, node=item.node,
                                      path=module.replace('.', '/') + '.py')
            description = checker.describe(item)
            expected = reference.get('execution')
            if expected and expected != ('async' if description['async'] else 'thread'):
                raise InspectionError('OP-02', '工具执行方式与声明不符：' + entry, node=item.node,
                                      path=module.replace('.', '/') + '.py')
            try:
                if kind in {'function', 'actor'}:
                    for names in ('required_arguments', 'possible_arguments'):
                        bind_parameters(description['parameters'], [], dict.fromkeys(reference[names]))
                else:
                    count = 2 if kind == 'validate_resources' else 3
                    bind_parameters(description['parameters'], [None]*count, {})
            except TypeError as exc:
                raise InspectionError('OP-02', f'工具声明与签名不匹配：{entry} · {exc}',
                                      node=item.node, path=module.replace('.', '/') + '.py') from exc
            checker.check(description, constructor_kwargs=reference.get('init'))
        tree.unchanged()
    finally:
        for path, digest in tree.files.items():
            _record(sources, path, digest)


def _definition(root, path, deadline, exclude, sources, published):
    """A detached definition proves its own syntax and shape, not a call-site binding."""
    relative = Path(path)
    if relative.parts[0] == 'prompts' and relative.suffix in {'.yaml', '.yml', '.json'}:
        raw = read_bounded(relative_path(root, path), 1024**2)
        _record(sources, path, sha256(raw).hexdigest())
        format_name, names, tools = inspect_prompt_source(raw.decode('utf-8'))
        if tools:
            _check_agent_tools(root, tools, deadline=deadline, exclude=exclude, sources=sources)
        return dict(path=path, format=format_name, symbols=list(names), scope='definition')
    if relative.parts[0] == 'configs' and relative.suffix in {'.json', '.yaml', '.yml'}:
        raw = read_bounded(relative_path(root, path), 1024**2)
        _record(sources, path, sha256(raw).hexdigest())
        if relative.suffix == '.json':
            decode_control(raw)
        return dict(path=path, format=relative.suffix.lstrip('.'), symbols=[], scope='definition')
    if relative.parts[0] == 'notebooks' and relative.suffix == '.ipynb':
        raw = read_bounded(relative_path(root, path), 4 * 1024**2)
        _record(sources, path, sha256(raw).hexdigest())
        decode_control(raw)
        return dict(path=path, format='ipynb', symbols=[], scope='definition')
    if relative.parts[0] == 'data_tools':
        if published:
            return dict(path=path, symbols=list(published), roles=published, scope='definition')
        return None
    if relative.parts[0] == 'operators' and relative.suffix == '.py' and relative.name != '__init__.py':
        module = path[:-3].replace('/', '.')
        tree = SourceTree(root, {'seconds': _tree_seconds(deadline)}, exclude=exclude)
        try:
            namespace = tree.load(module)
            checker = OperatorChecker(tree)
            names = []
            for name, item in namespace.items():
                if published is not None and name not in published:
                    continue
                if name.startswith('_') or not isinstance(item, Definition) or item.module != module:
                    continue
                if isinstance(item.node, ast.ClassDef) and not any(
                        isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)) and member.name == '__call__'
                        for member in item.node.body):
                    continue
                checker.describe(item)
                names.append(name)
            tree.unchanged()
        finally:
            for filename, value in tree.files.items():
                _record(sources, filename, value)
        if not names and published != {}:
            raise InspectionError('OP-01', '没有可独立确认的公开算子定义。', path=path, status='incomplete')
        return dict(path=path, symbols=names, scope='definition')
    return None


def check_project(root, *, paths=(), entry=None, params=None, reference=None, table_resolver=None,
                  table_describer=describe_fixed_table,
                  exclude=(), publish=None, max_variants=MAX_VARIANTS, seconds=CHECK_SECONDS,
                  max_bytes=MAX_SOURCE_BYTES, include_hygiene=False):
    """Check without importing candidate code. A product may publish each exact inspected report."""
    if paths and entry is not None or params is not None and entry is None:
        raise ForgeError('Choose paths or one entry and its parameters', code='invalid_request')
    root = Path(root).absolute()
    paths = tuple(dict.fromkeys(paths))
    if len(paths) > 256:
        raise ForgeError('Changed path count exceeds check budget', code='resource_limit')
    for name in (*paths, *((entry.partition(':')[0],) if entry else ()), *((params,) if params else ())):
        if not _visible(name, exclude):
            raise ForgeError('Requested input is outside the visible scope', code='operation_rejected')
        relative_path(root, name)
    if any(type(n) is not int or not 1 <= n <= cap for n, cap in (
            (max_variants, 128), (seconds, 600), (max_bytes, 64*1024**2))):
        raise ForgeError('Invalid project inspection limits', code='invalid_request')
    with operation_budget.scope(
            deadline=time.monotonic() + seconds,
            max_seconds=max(operation_budget.OPERATION_SECONDS, seconds)) as deadline:
        reference = reference or build_reference()
        if content_digest(reference['catalog']) != reference['catalog_id']:
            raise ForgeError('SDK catalogue differs from its supplied identity', code='inputs_changed')
        view = discover_project_sources(root, exclude=exclude, include_roles=entry is None,
                                        role_names=ROLE_DIRS if paths else ('workflows',))
        describer = table_describer
        if table_describer is not None:
            table_cache = {}
            def describer(uri, version):
                raw = str(uri.absolute()) if isinstance(uri, Path) else (
                    str(Path(uri).absolute()) if '://' not in str(uri) else str(uri)
                )
                key = (raw, version)
                if key not in table_cache:
                    table_cache[key] = table_describer(uri, version)
                return copy.deepcopy(table_cache[key])
        return _check(root, view, paths, entry, params, reference, table_resolver, describer,
                      exclude, deadline, publish, max_variants, max_bytes, include_hygiene=include_hygiene)


def _check(root, view, paths, entry, params, reference, table_resolver, table_describer, exclude, deadline, publish, max_variants, max_bytes, *, include_hygiene=False):
    native, layout, checked, rows, definitions = [], [], [], [], []
    sources, parameter_digests, table_files = {}, {}, {}
    layout_paths = set()
    checked_workflows = set()
    control = view['control']
    manifest = 'workspace.json'
    manifest_hash = None
    if control is not None:
        raw = read_bounded(relative_path(root, manifest), 1024**2)
        if load_project(root)[1] != control:
            raise ForgeError('Project declaration changed during discovery', code='inputs_changed')
        manifest_hash = sha256(raw).hexdigest()
    elif not paths and not entry:
        native.append(diagnostic('DF-01', manifest, '完整项目检查需要项目声明。', status='incomplete'))
    control = control or {'entries': {}, 'locations': {}}
    scope = dict(kind='entry' if entry else 'paths' if paths else 'project', requested=entry or list(paths))
    local_resolver = table_resolver or table_binding
    def resolve(root, current, alias):
        if isinstance(alias, str) and not alias.startswith('run:'):
            name = 'tables/' + alias + '.json'
            if not _visible(name, exclude):
                raise ForgeError('Table is outside the visible scope')
        if isinstance(alias, str) and alias.startswith('run:') and table_resolver is None:
            raise ForgeError('Run-backed table requires an explicit receipt resolver', code='not_found')
        return local_resolver(root, current, alias)

    variants, relevant = _candidates(root, view, paths, entry, params, sources, native, layout)
    if not paths and entry is None:
        relevant.update(view['roles']['workflows'])
    declarations = _declared_exports(root, sorted(relevant), sources, layout, deadline, max_bytes)
    visible = [item for item in variants if _visible(item[0].partition(':')[0], exclude)
               and (item[1] is None or _visible(item[1], exclude))]
    if len(visible) != len(variants):
        native.append(diagnostic('FL-10', '', '部分受影响的入口或参数超出允许读取的范围。', status='incomplete'))
        layout.append(diagnostic('DF-01', '', '部分受影响入口的目录角色未核实。', status='incomplete'))
    variants = visible
    layout_paths.update(p for p in relevant if Path(p).parts[0] in ROLE_DIRS)
    visited_sources = set()
    agent_configs = set()
    for index, (candidate, param, hypothetical) in enumerate(variants):
        seconds = int(deadline - time.monotonic())
        if index >= max_variants or seconds < 1:
            native.append(diagnostic('FL-10', '', f'{len(variants)-index} 个入口或参数变体超出检查预算。', status='incomplete'))
            layout.append(diagnostic('DF-01', '', '未覆盖全部入口的文件角色。', status='incomplete'))
            break
        path = candidate.partition(':')[0]
        try:
            parameters = {}
            if param is not None:
                raw = read_bounded(validate_parameter_path(root, param), 65536)
                _record(parameter_digests, param, sha256(raw).hexdigest())
                parameters = decode_control(raw)
            report = inspect_variant(root, control, candidate, parameters, reference['catalog'],
                                     table_resolver=resolve, limits={'seconds': min(SOURCE_LIMITS['seconds'], seconds)},
                                     exclude=exclude, table_describer=table_describer)
            binding = report['binding']
            dependencies = _binding_files(root, binding, sources, table_files)
            # Shared changes require probing potential consumers. An unrelated complete
            # closure is evidence of independence, not another flow under review.
            if (paths and path not in relevant and param not in relevant
                    and 'workspace.json' not in relevant and dependencies.isdisjoint(relevant)):
                kept_unrelated_diags = [d for d in report['diagnostics'] if _keep_diagnostic(d)]
                if report['status'] != 'checked' and (report['status'] != 'incomplete' or kept_unrelated_diags):
                    native.append(diagnostic('FL-10', path, '入口依赖未能完整解析，与变更的关系待核实。',
                                             status='incomplete'))
                continue
            if hypothetical and not any(n['role'] in {'action','query'} or n['kind'] == 'materialize'
                                        for n in report['nodes']):
                continue
            visited_sources.update(dependencies)
            for node in report['nodes']:
                if node['api'].rsplit('.', 1)[-1] == 'agentmap_async':
                    agent_configs.update(r['path'] for r in node['source_resources'] if r['kind'] == 'config')
            checked.append(candidate)
            native.extend({**d, 'entry': candidate, 'params': param} for d in report['diagnostics'])
            inspected, issues = _layout(report)
            layout_paths.update(inspected)
            layout.extend({**d, 'entry': candidate, 'params': param} for d in issues)
            report['binding']['parameter_path'] = param
            kept_entry_diags = [d for d in report['diagnostics'] if _keep_diagnostic(d)]
            entry_status = (
                'invalid' if any(d.get('status') == 'invalid' for d in kept_entry_diags)
                else 'incomplete' if kept_entry_diags
                else ('checked' if report['status'] in ('checked', 'incomplete') else report['status'])
            )
            row = dict(entry=candidate, params=param, status=entry_status)
            if publish is not None:
                publish(report)
            rows.append(row)
        except (ValueError, OSError, KeyError, TypeError, RuntimeError, RecursionError, DemiflowError) as exc:
            if isinstance(exc, ForgeError) and exc.code in {'conflict', 'inputs_changed'}:
                raise ForgeError(str(exc), code='inputs_changed') from exc
            invalid = isinstance(exc, ForgeError) and exc.code == 'invalid_request'
            native.append(diagnostic('FL-03' if invalid else 'FL-10', path, str(exc),
                                     status='invalid' if invalid else 'incomplete'))
            layout.append(diagnostic('DF-01', path, '入口和配置未完整核实。', status='incomplete'))
            rows.append(dict(entry=candidate, params=param, status='incomplete'))

    for path in sorted(agent_configs | (relevant - visited_sources)):
        if not (root / path).exists():
            if Path(path).parts[0] == 'data_tools':
                continue
            native.append(diagnostic('FL-01', path, '变更的文件已删除，无法验证其定义。', status='incomplete'))
            continue
        try:
            operation_budget.check(deadline)
            if Path(path).parts[0] == 'workflows':
                from .workflow import MAX_WORKFLOW_BYTES, validate_workflow
                raw_wf = read_bounded(relative_path(root, path), MAX_WORKFLOW_BYTES)
                _record(sources, path, sha256(raw_wf).hexdigest())
                layout_paths.add(path)
                if Path(path).suffix != '.json':
                    layout.append(diagnostic('DF-02', path, 'workflows/ 目录仅允许存放声明式 JSON 编排文件（workflows/*.json）。'))
                else:
                    wf_spec = validate_workflow(root, path, control=view['control'])
                    layout.extend(wf_spec['diagnostics'])
                    definitions.append(dict(
                        path=path,
                        format=wf_spec.get('format') or 'demiflow_workflow_v1',
                        symbols=[n['id'] for n in wf_spec.get('nodes') or ()],
                        scope='definition', unverified_projects=wf_spec['unverified_projects'],
                    ))
                checked_workflows.add(path)
                continue
            definition = _definition(root, path, deadline, exclude, sources, declarations.get(path))
            if definition:
                definitions.append(definition)
        except InspectionError as exc:
            native.append(exc.diagnostic)
        except (ValueError, OSError, RecursionError) as exc:
            invalid = not isinstance(exc, (OSError, RecursionError)) and getattr(exc,'code',None) != 'resource_limit'
            native.append(diagnostic('DF-05' if Path(path).parts[0] == 'prompts' else 'OP-01',
                                     path, str(exc), status='invalid' if invalid else 'incomplete'))
    pipeline_paths = [p for p in relevant if Path(p).parts[0] != 'data_tools']
    if pipeline_paths and not checked and not definitions and not native:
        native.append(diagnostic('FL-10', '', '未定位到可核对的原生流程或独立定义。', status='incomplete'))
    # Hygiene applies to every declared project, including new ones without registered entries.
    if (scope['kind'] == 'project' or include_hygiene) and view['control'] is not None:
        hygiene_paths, hygiene_issues = check_project_hygiene(
            root, control, checked_sources=sources, parameter_digests=parameter_digests,
            exclude=exclude, structural_only=(scope['kind'] != 'project'), checked_workflows=checked_workflows)
        layout_paths.update(hygiene_paths)
        layout.extend(hygiene_issues)
    elif sources:
        boundary_paths, boundary_issues = check_source_import_boundaries(root, sources)
        layout_paths.update(boundary_paths)
        layout.extend(boundary_issues)
    basis = dict(sdk_revision=reference['sdk_revision'], catalog_id=reference['catalog_id'], rules_id=RULES_ID,
                 manifest_sha256=manifest_hash, sources=sources, parameter_digests=parameter_digests, controls=table_files)
    verify_check_inputs(root, basis, deadline=deadline, max_bytes=max_bytes)
    if sdk_identity(reference['catalog']) != reference['sdk_revision']:
        raise ForgeError('SDK changed during inspection', code='inputs_changed')
    native = [d for d in native if _keep_diagnostic(d)]
    flow = result(native, checked, applicable=bool(checked))
    roles = result(layout, layout_paths, applicable=bool(layout_paths))
    statuses = {flow['status'], roles['status']}
    status = next(x for x in ('failed','incomplete','passed','not_applicable') if x in statuses)
    diagnostics = list(native) + list(layout)
    unverified = [d for d in diagnostics if d.get('status') == 'incomplete']
    data = dict(status=status, scope=scope, root=str(root), entries=rows, definitions=definitions,
                native_flow=flow, project_layout=roles, rules_id=RULES_ID, catalog_id=reference['catalog_id'],
                source_digest=content_digest(sources), source_count=len(sources), basis=basis,
                diagnostics=diagnostics[:256], diagnostic_count=len(diagnostics),
                coverage=dict(checked=len(checked), unverified=unverified[:64], requested_variants=len(variants),
                              external_projects_pending=sorted({alias for d in definitions
                                                                for alias in d.get('unverified_projects', ())})))
    data['check_digest'] = content_digest(data)
    return data


def verify_check_inputs(root, basis, *, deadline=None, max_bytes=MAX_SOURCE_BYTES):
    """Compare exact project bytes at inspection completion and before registration."""
    root = Path(root)
    expected = {}
    for group in ('sources', 'parameter_digests', 'controls'):
        for path, value in basis[group].items():
            _record(expected, path, value)
    if basis['manifest_sha256'] is not None:
        _record(expected, 'workspace.json', basis['manifest_sha256'])
    elif (root / 'workspace.json').exists():
        raise ForgeError('Project initialized during inspection', code='inputs_changed')
    remaining = max_bytes
    for path, digest in expected.items():
        operation_budget.check(deadline)
        try:
            raw = read_bounded(relative_path(root, path), remaining)
        except FileNotFoundError as exc:
            raise ForgeError('Input disappeared during inspection: ' + path, code='inputs_changed') from exc
        remaining -= len(raw)
        if sha256(raw).hexdigest() != digest:
            raise ForgeError('Input changed during inspection: ' + path, code='inputs_changed')
