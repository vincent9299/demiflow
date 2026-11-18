"""Read-only interpretation of published project declarations.

No candidate imports, directory-name roles, shell commands or data migration.
This is a view of evidence, not an executable registry or a certification.
"""
from __future__ import annotations

import ast
import hashlib
import json
import math
from pathlib import Path
from pathlib import PurePosixPath
import re
from urllib.parse import unquote
from urllib.parse import urlsplit

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_forge_project_discovery_v1'
STANDARDS = {f'1.0.0-candidate.{i}' for i in range(1, 11)}
LEGACY_STANDARDS = {f'1.0.0-candidate.{i}' for i in range(1, 7)}
PARAMETERS = {'demiflow_forge_flow_parameters_v1', 'demiflow_forge_flow_parameters_v2'}
MAX_FILES, MAX_BYTES, MAX_ITEMS = 128, 4 * 1024**2, 256


def _json(raw):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ForgeError('Duplicate declaration field: ' + key[:128])
            result[key] = value
        return result
    def invalid(value):
        raise ForgeError('Nonfinite declaration number: ' + value)
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            return invalid(value)
        return result
    return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid, parse_float=number)


def discover(root, control=None):
    from demiflow.workbench.projects import workspace as ws
    root = ws.no_symlinks(Path(root).absolute()).resolve(strict=True)
    if control is None:
        control = ws.read_control(root / 'workspace.json')
    evidence, diagnostics, entries, configurations, tables, runs = {}, [], [], [], {}, []
    operator_paths, prompt_paths, total = {}, {}, 0
    entry_sources = {}
    declarations = []

    def issue(code, path, detail, *, cause='unknown', scope='project_discovery'):
        row = dict(code=code, path=str(path), detail=str(detail)[:1024],cause=cause,scope=scope,related_nodes=[])
        if row not in diagnostics and len(diagnostics) < 64:
            diagnostics.append(row)

    def read(path, maximum=1024**2):
        nonlocal total
        path = ws.no_symlinks(Path(path))
        if len(evidence) >= MAX_FILES or total >= MAX_BYTES:
            raise ForgeError('Project declaration read budget exhausted', code='resource_limit')
        raw = read_bounded(path, min(maximum, MAX_BYTES - total))
        total += len(raw)
        digest = hashlib.sha256(raw).hexdigest()
        if str(path) in evidence and evidence[str(path)] != digest:
            raise ForgeError('Project declaration changed during discovery', code='conflict')
        evidence[str(path)] = digest
        return raw

    def local(path):
        target = Path(path)
        if not target.is_absolute():
            target = root / target
        if '..' in target.parts or not target.is_relative_to(root):
            raise ForgeError('Reference is outside the registered project', code='operation_rejected')
        relative = target.relative_to(root)
        if any(part.startswith('.') for part in relative.parts):
            raise ForgeError('Private paths are not project content', code='operation_rejected')
        return ws.relative_path(root, relative.as_posix())

    def entry_source(target, relative):
        # Reuse compact declaration facts within this discovery only. Keeping
        # raw bytes or ASTs for every module would retain their full payloads.
        def stamp():
            info = ws.no_symlinks(target).stat()
            return (info.st_dev, info.st_ino, info.st_size,
                    info.st_mtime_ns, info.st_ctime_ns)

        before = stamp()
        if target in entry_sources:
            previous, facts = entry_sources[target]
            if before != previous:
                raise ForgeError('Entry source changed during discovery', code='conflict')
            return facts
        raw = read(target)
        tree = ast.parse(raw, filename=relative)
        if sum(1 for _ in ast.walk(tree)) > 50000:
            raise ForgeError('Entry AST exceeds discovery budget', code='resource_limit')
        functions, requirements, config_count = {}, {}, 0
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                count, line = functions.get(node.name, (0, node.lineno))
                functions[node.name] = (count + 1, line)
                positional = [*node.args.posonlyargs, *node.args.args]
                requirements[node.name] = [p.arg for p in positional[:len(positional)-len(node.args.defaults)]]
                requirements[node.name].extend(p.arg for p, default in zip(node.args.kwonlyargs, node.args.kw_defaults)
                                               if default is None)
            if isinstance(node, ast.FunctionDef) and node.name == 'config':
                config_count += 1
        facts = dict(sha256=hashlib.sha256(raw).hexdigest(),
                     functions=functions, requirements=requirements, configurations=config_count)
        if stamp() != before:
            raise ForgeError('Entry source changed during discovery', code='conflict')
        entry_sources[target] = (before, facts)
        return facts

    def add_entry(path, symbol, protocol, origin, standard=None, params=()):
        target = local(path)
        if target.suffix != '.py' or type(symbol) is not str or not symbol.isidentifier() or symbol.startswith('_'):
            raise ForgeError('Invalid Python entry declaration')
        relative = target.relative_to(root).as_posix()
        item = dict(entry=relative + ':' + symbol, path=relative, symbol=symbol,
                    protocol=protocol, standard=standard, evidence=origin,
                    parameters=list(params), status='recognized')
        if any(e['entry'] == item['entry'] and e['protocol'] != protocol for e in entries):
            item['status'] = 'conflict'
            issue('conflicting_entry', relative, '同一入口被声明为不同协议；未自动选择。')
        try:
            source = entry_source(target, relative)
            item['sha256'] = source['sha256']
            count, line = source['functions'].get(symbol, (0, None))
            if count != 1:
                item['status'] = 'invalid'
                issue('entry_missing', relative, '声明的顶层入口函数不存在或不唯一。')
            else:
                item['line'] = line
                item['required_arguments'] = source['requirements'][symbol]
            if protocol == 'config_run_pipeline':
                item['configuration_entry'] = relative + ':config'
                if source['configurations'] != 1:
                    item['status'] = 'invalid'
                    issue('configuration_entry_missing', relative, '旧协议要求的顶层 config 函数不存在或不唯一。')
        except (ValueError, OSError, SyntaxError, RecursionError) as exc:
            item['status'] = 'invalid'
            issue('entry_unreadable', relative, exc)
        existing = next((e for e in entries if e['entry'] == item['entry'] and e['protocol'] == protocol), None)
        if existing is None:
            entries.append(item)
        else:
            existing.setdefault('also_declared_by', []).append(origin)
            if standard:
                existing['standard'] = standard

    def add_operator(path, origin, kind='code'):
        target = local(path)
        relative = target.relative_to(root).as_posix()
        extensions = {'.py'} if kind == 'code' else {'.yaml', '.yml', '.json'}
        state = 'available' if target.is_dir() or target.is_file() and target.suffix in extensions else 'missing'
        (operator_paths if kind == 'code' else prompt_paths)[relative] = dict(path=relative, evidence=origin, status=state)
        if state == 'missing':
            issue('operator_missing', relative, '声明的算子文件或目录不存在。')

    for entry, params in control.get('entries', {}).items():
        try:
            path, symbol = entry.split(':')
            add_entry(path, symbol, 'native_flow', 'workspace.json', params=params)
        except (ValueError, OSError) as exc:
            issue('invalid_native_entry', entry, exc)

    # The public JSON declaration may live at a repository root. Only the exact
    # registered owner is selected; sibling configurations/commands are never run.
    declaration = None
    for depth, base in enumerate((root, *root.parents)):
        if depth >= 32:
            issue('discovery_limit', '', '父目录查找达到 32 层上限。')
            break
        candidate = base / '.demiforge/project.json'
        if candidate.exists() or candidate.is_symlink():
            declaration = candidate
            break
        if (base / '.git').exists():
            break
    if declaration is not None:
        try:
            raw = read(declaration)
            value = _json(raw)
            if type(value) is not dict or value.get('schema') not in {'demiforge-project-v1', 'demiforge-project-v2', 'demiforge-project-v3'}:
                raise ForgeError('Unsupported project declaration schema')
            multi = value['schema'] == 'demiforge-project-v3'
            records = value.get('projects' if multi else 'pipelines')
            if type(records) is not dict or len(records) > MAX_ITEMS:
                raise ForgeError('Invalid or oversized pipeline declarations')
            base = declaration.parent.parent
            matches = []
            for name, record in records.items():
                if type(record) is not dict or type(record.get('path')) is not str:
                    raise ForgeError('Invalid pipeline owner declaration')
                relative = PurePosixPath(record['path'])
                if (not record['path'] or str(relative) != record['path'] or relative.is_absolute()
                        or '..' in relative.parts or '\\' in record['path']):
                    issue('invalid_owner', declaration, '项目归属路径无效：' + name)
                    continue
                # Select lexically before inspecting any filesystem path. A
                # sibling's unavailable or symlinked files do not hide this owner.
                owner = base.joinpath(*relative.parts)
                if owner == root:
                    matches.append((name, record))
            if len(matches) > 1:
                issue('conflicting_owner', declaration, '多个登记声明拥有同一目录；未自动选择入口。')
            elif not matches:
                issue('owner_not_declared', declaration, '未找到与当前目录准确匹配的项目归属声明。')
            else:
                name, record = matches[0]
                standard = record.get('standard', value.get('standard'))
                declarations.append(dict(path=str(declaration), pipeline=name, standard=standard,
                                         schema=value['schema']))
                if standard not in STANDARDS:
                    issue('unsupported_standard', declaration, f'不支持的原项目规范版本：{standard}')
                else:
                    entry = record.get('entry')
                    origin = str(declaration) + ('#projects.' if multi else '#pipelines.') + name
                    if multi:
                        if standard != '1.0.0-candidate.10' or type(record.get('flows')) is not dict or not 1 <= len(record['flows']) <= 64:
                            raise ForgeError('Invalid multi-flow declaration')
                        for flow_id, flow in record['flows'].items():
                            if (type(flow_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', flow_id)
                                    or type(flow) is not dict or set(flow) != {'path', 'symbol'}):
                                raise ForgeError('Invalid flow declaration')
                            target = ws.relative_path(base, flow['path'])
                            add_entry(target, flow['symbol'], 'native_flow', origin, standard)
                            found = next(e for e in entries if e['entry'] == target.relative_to(root).as_posix() + ':' + flow['symbol'])
                            found['flow_id'] = flow_id
                        for role, kind in (('operators', 'code'), ('prompts', 'prompt')):
                            paths = record.get(role)
                            if type(paths) is not list or len(paths) > 64:
                                raise ForgeError('Invalid definition paths')
                            for path in paths:
                                add_operator(ws.relative_path(base, path), origin, kind)
                    elif standard in LEGACY_STANDARDS and type(entry) is str and '/' not in entry and entry.endswith('.py'):
                        add_entry(entry, 'run_pipeline', 'config_run_pipeline', origin, standard)
                        # This role is part of the supported legacy standard,
                        # not a rule imposed on arbitrary registered projects.
                        add_operator('operators', origin)
                    elif standard not in LEGACY_STANDARDS and type(entry) is dict and set(entry) == {'path', 'symbol'}:
                        add_entry(ws.relative_path(base, entry['path']), entry['symbol'], 'native_flow', origin, standard)
                        operators = record.get('operators', [])
                        if type(operators) is not list or len(operators) > 64:
                            raise ForgeError('Invalid operator declarations')
                        for operator in operators:
                            add_operator(ws.relative_path(base, operator), origin)
                    else:
                        issue('unsupported_entry', declaration, '入口声明与指定规范版本的协议不一致。')
        except (ValueError, OSError, TypeError, RecursionError) as exc:
            issue('declaration_unreadable', declaration, exc)
    if not entries:
        issue('entry_not_declared', '', '未识别到受支持的入口声明；不会根据函数名猜测并执行代码。')

    def add_table(uri, version, origin, *, fixed=False):
        if type(uri) is not str or len(uri) > 4096:
            return
        parsed = urlsplit(uri)
        if not unquote(parsed.path).endswith('.lance'):
            return
        if len(tables) >= MAX_ITEMS:
            raise ForgeError('Project data-reference budget exhausted', code='resource_limit')
        if fixed and version is None or version is not None and (type(version) is not int or not 1 <= version < 2**64):
            raise ForgeError('Fixed Lance references require a positive integer version')
        if parsed.scheme not in {'', 'file'} or parsed.netloc:
            state, path = 'outside_project', uri
        else:
            try:
                target = local(unquote(parsed.path))
                if target.suffix != '.lance':
                    return
                path = target.relative_to(root).as_posix()
                state = 'available' if target.is_dir() else 'missing'
            except (ValueError, OSError):
                state, path = 'outside_project', uri
        key = 'd' + content_digest(dict(path=path, version=version))[:24]
        if key not in tables:
            tables[key] = dict(id=key, path=path, version=version, status=state, evidence=[])
        if origin not in tables[key]['evidence']:
            tables[key]['evidence'].append(origin)
        if state != 'available':
            detail = ('引用位于项目之外，未授予读取权限。' if state == 'outside_project' else
                      '文档引用未解析为项目内存在的表；可能是示例、模板或不完整路径，不据此判断历史数据丢失。' if version is None else
                      '固定数据引用的路径不存在。')
            issue('table_' + state, path, detail)

    configs = {}
    for paths in control.get('entries', {}).values():
        for path in paths:
            if path in configs:
                continue
            if len(configs) >= 64:
                issue('configuration_limit', 'workspace.json', '识别只读取前 64 份配置；其余配置未计为已识别。')
                break
            configs[path] = 'workspace.json'
    readme = root / 'README.md'
    if readme.exists():
        try:
            text = read(readme, ws.README_BYTES).decode('utf-8')
            # Documentation links are evidence of a reference, never executable
            # parameter bindings or an inferred successful historical run.
            for match in re.finditer(r'\[[^\]\n]{0,256}\]\(([^)\s]{1,1024})\)', text):
                parsed = urlsplit(match.group(1))
                if parsed.scheme or parsed.netloc:
                    continue
                path = unquote(parsed.path)
                if Path(path).suffix in {'.json', '.yaml', '.yml'}:
                    try:
                        relative = local(path).relative_to(root).as_posix()
                        if len(configs) >= 64 and relative not in configs:
                            raise ForgeError('Configuration reference budget exceeded')
                        configs.setdefault(relative, 'README.md:' + str(text.count('\n', 0, match.start()) + 1))
                    except (ValueError, OSError) as exc:
                        issue('configuration_reference', path, exc)
            for match in re.finditer(r'`([^`\n]{1,1024})`', text):
                declared = match.group(1)
                origin = 'README.md:' + str(text.count('\n', 0, match.start()) + 1)
                if declared.endswith('.lance') and not any(c in declared for c in (' ', '*', '<', '>')):
                    add_table(declared, None, origin)
                elif any(declared.endswith(ext) for ext in ('.sqlite', '.jsonl', '.log')) and '/' in declared and len(runs) < 64:
                    runs.append(dict(path=declared, status='unverified', evidence=origin,
                        detail='原文档中的运行产物引用；未找到受支持的运行状态声明，不能推断执行成功或恢复能力。'))
        except (ValueError, OSError, RecursionError) as exc:
            issue('documentation_unreadable', 'README.md', exc)

    for path, origin in sorted(configs.items()):
        row = dict(path=path, evidence=origin, status='referenced', format=None)
        try:
            raw = read(local(path), 65536)
            row['sha256'] = hashlib.sha256(raw).hexdigest()
            if Path(path).suffix == '.json':
                data = _json(raw)
                row['format'] = data.get('format') if type(data) is dict else None
                if row['format'] is not None and (type(row['format']) is not str or len(row['format']) > 128):
                    row['format'] = None
                    issue('configuration_format_unknown', path, '未识别为版本化参数协议；仅展示原配置和明确的数据引用，不校验其业务语义。')
                row['status'] = 'native_parameters' if row['format'] in PARAMETERS else 'original_configuration'
                row['plan_preview_parameters'] = (type(data) is dict and type(data.get('arguments')) is dict
                    and (set(data) <= {'arguments', 'run_directory'} or
                         row['format'] in PARAMETERS and set(data) == {'format', 'arguments'}))
                pending, visits = [(data, '', 0)], 0
                while pending:
                    obj, pointer, depth = pending.pop();visits += 1
                    if visits > 4096 or depth > 16:
                        raise ForgeError('Configuration structure budget exceeded')
                    if type(obj) is dict:
                        if set(obj) == {'uri', 'version'}:
                            add_table(obj['uri'], obj['version'], path + '#' + pointer, fixed=True)
                        elif len(obj) <= 256:
                            pending.extend((v, pointer + '/' + str(k), depth + 1) for k, v in obj.items())
                        else:
                            raise ForgeError('Configuration member budget exceeded')
                    elif type(obj) is list:
                        if len(obj) > 256:
                            raise ForgeError('Configuration item budget exceeded')
                        pending.extend((v, pointer + '/' + str(k), depth + 1) for k, v in enumerate(obj))
        except (ValueError, OSError, RecursionError, TypeError) as exc:
            row['status'] = 'unreadable';issue('configuration_unreadable', path, exc)
        configurations.append(row)

    for entry in entries:
        if entry['protocol'] == 'config_run_pipeline':
            issue('unsupported_flow_protocol', entry['path'], '已识别原 config → run_pipeline 入口；当前原生 Flow 契约检查与执行尚不支持此协议。保留原代码，改造需用户确认。',cause='unsupported',scope='entry')
        elif not entry['parameters']:
            issue('parameters_unbound', entry['path'], '已识别入口，但缺少入口与原生参数配置的明确绑定；文档引用不自动成为执行配置。',cause='source',scope='entry')
    result = dict(format=FORMAT, status='partial' if diagnostics else 'recognized',
        declarations=declarations, entries=entries, operators=[operator_paths[p] for p in sorted(operator_paths)],
        prompts=[prompt_paths[p] for p in sorted(prompt_paths)], configurations=configurations,
        tables=list(tables.values()), runs=runs, diagnostics=diagnostics,
        evidence=[dict(path=p, sha256=h) for p, h in sorted(evidence.items())],
        coverage=dict(candidate_code_executed=False, complete=False,
                      historical_runs='only_explicit_references', data='declared_references_only',
                      entry_checks='python_syntax_and_declared_symbol_only',
                      operator_checks='declared_path_existence_only'))
    result['digest'] = content_digest(result)
    bounded_json(result, 1024**2)
    return result


def document(root, path, *, expected):
    from demiflow.workbench.projects import workspace as ws
    value = discover(root)
    row = next((c for c in value['configurations'] if c['path'] == path), None)
    if row is None or row.get('sha256') != expected:
        raise ForgeError('Configuration evidence changed; refresh before reading', code='conflict')
    raw = read_bounded(ws.relative_path(root, path), 65536)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ForgeError('Configuration changed during reading', code='conflict')
    return dict(path=path, sha256=expected, text=raw.decode('utf-8'))


def table(root, identity, *, expected, version=None, binding=None, sample=None):
    """Worker-only preview of an evidenced local table; never persists a binding."""
    from demiflow.workbench.projects import workspace as ws
    from demiflow.lance import preview
    from demiflow.lance.refs import DatasetRef
    root = ws.no_symlinks(Path(root).absolute()).resolve(strict=True)
    if version is not None and (type(version) is not int or not 1 <= version < 2**64):
        raise ForgeError('Invalid table version', code='invalid_request')
    value = discover(root)
    if value['digest'] != expected:
        raise ForgeError('Project declarations changed; refresh before preview', code='conflict')
    row = next((t for t in value['tables'] if t['id'] == identity), None)
    if row is None or row['status'] != 'available':
        raise ForgeError('Table is not an accessible project declaration', code='operation_rejected')
    if row['version'] is not None and version not in {None, row['version']}:
        raise ForgeError('Version differs from the declared fixed reference', code='conflict')
    uri = ws.relative_path(root, row['path'])
    description = preview.describe_table(uri, row['version'] if version is None else version)
    observed = dict(project=str(root), id=identity, path=row['path'], version=description['version'],
                    row_count=description['row_count'], schema_hash=description['schema_hash'], discovery=expected)
    digest = content_digest(observed)
    if sample is not None:
        if binding != digest or version is None:
            raise ForgeError('Preview requires the exact observed table version and binding', code='conflict')
        ref = DatasetRef('project-preview', row['path'], description['version'], 'observed', '1',
                         description['schema_hash'], description['row_count'], store_id='project')
        return {**preview.sample_table(uri, ref.to_dict(), **sample), 'binding': digest}
    return {**description, 'binding': digest, 'id': identity, 'path': row['path'], 'evidence': row['evidence']}
