"""Read-only interpretation of published project declarations.

No candidate imports, directory-name roles, shell commands or data migration.
This is a view of evidence, not an executable registry or a certification.
"""
from __future__ import annotations

import ast
import hashlib
import json
import math
import os
from pathlib import Path
import re
from urllib.parse import unquote
from urllib.parse import urlsplit

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_forge_project_discovery_v1'
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
        root, control = ws.load_workspace_control(root)
    evidence, diagnostics, entries, configurations, tables, runs = {}, [], [], [], {}, []
    operator_paths, prompt_paths, total = {}, {}, 0
    entry_sources = {}

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
        functions, requirements = {}, {}
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                count, line = functions.get(node.name, (0, node.lineno))
                functions[node.name] = (count + 1, line)
                positional = [*node.args.posonlyargs, *node.args.args]
                requirements[node.name] = [p.arg for p in positional[:len(positional)-len(node.args.defaults)]]
                requirements[node.name].extend(p.arg for p, default in zip(node.args.kwonlyargs, node.args.kw_defaults)
                                               if default is None)
        facts = dict(sha256=hashlib.sha256(raw).hexdigest(),
                     functions=functions, requirements=requirements)
        if stamp() != before:
            raise ForgeError('Entry source changed during discovery', code='conflict')
        entry_sources[target] = (before, facts)
        return facts

    def add_entry(path, symbol, params):
        target = local(path)
        if target.suffix != '.py' or type(symbol) is not str or not symbol.isidentifier() or symbol.startswith('_'):
            raise ForgeError('Invalid Python entry declaration')
        relative = target.relative_to(root).as_posix()
        item = dict(entry=relative + ':' + symbol, path=relative, symbol=symbol,
                    evidence='workspace.json', parameters=list(params), status='recognized')
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
        except (ValueError, OSError, SyntaxError, RecursionError) as exc:
            item['status'] = 'invalid'
            issue('entry_unreadable', relative, exc)
        entries.append(item)

    for entry, params in control.get('entries', {}).items():
        try:
            path, symbol = entry.split(':')
            add_entry(path, symbol, params)
        except (ValueError, OSError) as exc:
            issue('invalid_native_entry', entry, exc)

    from demiflow.forge.project_rules import ROLE_DIRS
    for role, paths in (('operators', operator_paths), ('prompts', prompt_paths)):
        if role in ROLE_DIRS:
            try:
                target = local(role)
                if target.is_dir():
                    paths[role] = dict(path=role, evidence='project_directory', status='available')
            except (ValueError, OSError) as exc:
                issue('definition_directory_unreadable', role, exc)

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

    result = dict(format=FORMAT, status='partial' if diagnostics else 'recognized',
        entries=entries, operators=[operator_paths[p] for p in sorted(operator_paths)],
        prompts=[prompt_paths[p] for p in sorted(prompt_paths)], configurations=configurations,
        tables=list(tables.values()), runs=runs, diagnostics=diagnostics,
        evidence=[dict(path=p, sha256=h) for p, h in sorted(evidence.items())],
        coverage=dict(candidate_code_executed=False, complete=False,
                      historical_runs='only_explicit_references', data='declared_references_only',
                      entry_checks='python_syntax_and_declared_symbol_only',
                      operator_checks='role_directory_existence_only'))
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
    if identity.startswith('l'):
        from demiflow.workbench.notebooks.dictionary import resolve
        row, uri = resolve(root, identity, expected)
    else:
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
