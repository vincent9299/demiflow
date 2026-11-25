"""Read declared reusable definitions and their real Git history; never import them."""
from __future__ import annotations

import ast
import hashlib
import json
import keyword
import os
from pathlib import Path
import re
import subprocess
import yaml

from demiflow.forge.definition_exports import DATA_TOOL_EXPORT, OPERATOR_EXPORT, data_tool_semantics, exports
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from . import workspace as ws
from .discovery import discover
from ..operator_references import direct_references

from ..definition_contracts import signature, code_definition, prompt_definition

MAX_SOURCE = 512 * 1024



def _safe_tree(raw):
    tree = ast.parse(raw)
    if sum(1 for _ in ast.walk(tree)) > 50000:
        raise ForgeError('Definition AST exceeds budget', code='resource_limit')
    return tree


def _items(path, raw, kind):
    text = raw.decode('utf-8')
    if kind in ('code', 'data_tool'):
        tree = _safe_tree(text)
        public = exports(tree, DATA_TOOL_EXPORT if kind == 'data_tool' else OPERATOR_EXPORT)
        semantics = data_tool_semantics(tree) if kind == 'data_tool' else {}
        lines = text.splitlines(keepends=True)
        module_summary = (ast.get_docstring(tree) or '').partition('\n\n')[0][:512]
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) or node.name.startswith('_'):
                continue
            if kind == 'data_tool' and public is not None and node.name not in public:
                continue
            start = min([node.lineno, *(d.lineno for d in node.decorator_list)])
            body = ''.join(lines[start - 1:node.end_lineno])
            defn = code_definition(node, body, kind=kind)
            meta = semantics.get(node.name, {})
            semantic_fields = {}
            if kind == 'data_tool':
                semantic_fields = {
                    'tool_kind': (public[node.name] if public is not None else 'tool'),
                    'title': meta.get('title') or node.name,
                    'summary': meta.get('summary') or (defn.get('description') or '').partition('\n')[0].strip()[:200] or module_summary[:200],
                    'when_to_use': meta.get('when_to_use') or '',
                }
            yield dict(defn, label=node.name,
                       export_status=('legacy' if public is None else 'declared' if node.name in public else 'internal'),
                       definition_type=('class' if isinstance(node, ast.ClassDef) else
                                        'async_function' if isinstance(node, ast.AsyncFunctionDef) else 'function'),
                       **semantic_fields,
                       module_summary=module_summary, line=start, end_line=node.end_lineno)
    else:
        value = yaml.safe_load(text)
        # Reject recursion/alias expansion before serialization or traversal.
        pending = [(value, 0)]; count = 0
        while pending:
            obj, depth = pending.pop(); count += 1
            if count > 8192 or depth > 20:
                raise ForgeError('Prompt structure exceeds budget', code='resource_limit')
            if type(obj) is dict:
                pending.extend((v, depth + 1) for v in obj.values())
            elif type(obj) is list:
                pending.extend((v, depth + 1) for v in obj)
        if type(value) is not dict:
            raise ForgeError('Expected a native Prompt or Agent document', code='invalid_request')
        agent = value.get('schema_version') == 'demiflow_agent_v2'
        if agent:
            from demiflow.forge.project import inspect_prompt_source
            _, names, _ = inspect_prompt_source(text)
            tasks = value['tasks']
        elif value.get('schema_version') == 'demiflow_prompt_pack_v2' and type(value.get('prompts')) is dict:
            tasks = value['prompts']
            names = tasks.keys()
        else:
            raise ForgeError('Expected a native versioned Prompt pack or Agent config', code='invalid_request')
        for name in names:
            item = tasks[name]
            if type(name) is not str or type(item) is not dict:
                raise ForgeError('Invalid named Prompt definition', code='invalid_request')
            body = yaml.safe_dump(item, allow_unicode=True, sort_keys=False)
            # Agent tasks share the document model; the source excerpt stays scoped to the selected task.
            projected = {**item, 'model': value['model']} if agent else item
            yield dict(prompt_definition(name, projected), label=name, source=body, line=None, end_line=None,
                       template_sha256=hashlib.sha256(item['template'].encode()).hexdigest() if type(item.get('template')) is str else None)


def _files(root, declarations, kind):
    count = 0; result = set()
    suffixes = {'.py'} if kind in ('code', 'data_tool') else {'.yaml', '.yml', '.json'}
    for declaration in declarations:
        path = ws.relative_path(root, declaration['path'])
        candidates = [path] if path.is_file() else ()
        if path.is_dir():
            def walk():
                nonlocal count
                for directory, directories, files in os.walk(path, followlinks=False):
                    count += len(directories) + len(files)
                    if count > 1024:
                        raise ForgeError('Definition directory exceeds budget', code='resource_limit')
                    directories[:] = sorted(d for d in directories if not d.startswith('.') and d != '__pycache__')
                    for d in directories: ws.no_symlinks(Path(directory) / d)
                    for name in sorted(files): yield Path(directory) / name
            candidates = walk()
        for candidate in candidates:
            if candidate.suffix in suffixes and not candidate.name.startswith('.'):
                ws.no_symlinks(candidate)
                result.add(candidate)
                if len(result) > 128:
                    raise ForgeError('Definition file count exceeds budget', code='resource_limit')
    return sorted(result)


PLATFORM_ROOT = Path(__file__).resolve().parents[3]


def catalog(root, *, data_tools=False, include_platform=False):
    root, control, snapshot = ws.load_workspace(root)
    items, diagnostics, sources, total = [], [], {}, 0
    if data_tools:
        groups = [(root, 'data_tool', [{'path': 'data_tools'}])]
        if include_platform:
            groups.append((PLATFORM_ROOT, 'data_tool', [{'path': 'demiflow/data_tools'}]))
    else:
        facts = discover(root, control)
        groups = [(root, 'code', facts['operators']), (root, 'prompt', facts['prompts'])]
    source_files = {}
    for base, kind, paths in groups:
        for path in _files(base, paths, kind):
            raw = read_bounded(path, MAX_SOURCE); total += len(raw)
            if total > 4 * 1024**2:
                raise ForgeError('Definition source bytes exceeded', code='resource_limit')
            relative = path.relative_to(base).as_posix()
            source_files[relative] = path
            sources[relative] = hashlib.sha256(raw).hexdigest()
            try:
                for item in _items(relative, raw, kind):
                    item.update(id=content_digest([relative, kind, item['symbol']]), path=relative,
                                sha256=content_digest(item['source']), file_sha256=sources[relative])
                    if data_tools:
                        item.update(origin='platform' if relative.startswith('demiflow/data_tools/') else 'project',
                                    module=_module_name(relative))
                    item.pop('source')
                    item.pop('module_summary', None)
                    # Large Prompt bodies belong to the selected detail, never the index.
                    for field in ('template', 'messages', 'model', 'output_schema', 'input_variables', 'defaults'): item.pop(field, None)
                    items.append(item)
                    if len(items) > 1024:
                        raise ForgeError('Definition count exceeds budget', code='resource_limit')
            except (SyntaxError, UnicodeError, ValueError, yaml.YAMLError) as exc:
                if isinstance(exc, ForgeError) and exc.code == 'resource_limit': raise
                diagnostics.append(dict(path=relative, detail=str(exc)[:256]))
    for path, digest in sources.items():
        if hashlib.sha256(read_bounded(source_files[path], MAX_SOURCE)).hexdigest() != digest:
            raise ForgeError('Definitions changed during reading', code='conflict')
    result = dict(format='demiflow.project_data_tools.v1' if data_tools else 'demiflow.project_definitions.v1',
                  definitions=items, diagnostics=diagnostics, sources=sources)
    if not data_tools:
        uses = direct_references(root, facts['entries'], items, snapshot)
        for item in items:
            if item['kind'] == 'code':
                item['flow_references'] = uses.get(item['id'], [])
        result.update(discovery=facts['digest'], usage_coverage='direct_registered_flow_callsites')
    result['revision'] = content_digest(result)
    bounded_json(result, 1024**2)
    return result


def _git(root, *args):
    # Fixed commands and bounded objects only. No hooks, shell or candidate code.
    try:
        value = subprocess.run(['git', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
                                '-C', str(root), *args], capture_output=True, timeout=4,
                               env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0', 'GIT_TERMINAL_PROMPT': '0', 'GIT_PAGER': 'cat'})
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ForgeError('Version history temporarily unavailable', code='busy') from exc
    if value.returncode:
        return None
    return value.stdout


def history(root, *, id):
    root = Path(root)
    item = next((i for i in catalog(root)['definitions'] if i['id'] == id), None)
    if item is None: raise ForgeError('Unknown declared definition', code='not_found')
    prefix = _git(root, 'rev-parse', '--show-prefix')
    if prefix is None:
        return dict(versions=[], available=False, reason='项目未保存 Git 版本历史；当前内容仍可阅读。')
    path = prefix.decode().strip() + item['path']
    raw = _git(root, 'log', '-16', '--format=%H%x09%ct', '--', ':(top)' + path) or b''
    versions = []
    for line in raw.decode().splitlines():
        revision, timestamp = line.split('\t')
        if re.fullmatch('[0-9a-f]{40,64}', revision):
            versions.append(dict(revision=revision, timestamp=int(timestamp)))
    return dict(versions=versions, available=True, scope='当前文件最近 16 次提交；版本内容按所选定义读取。')



def _module_name(path):
    parts = Path(path).with_suffix('').parts
    if parts[-1] == '__init__':
        parts = parts[:-1]
    if all(part.isidentifier() and not keyword.iskeyword(part) for part in parts):
        return '.'.join(parts)
    return None


def write_data_tool(root, *, path, source):
    """Safely write a project data_tools/*.py module with mandatory semantic declarations."""
    from demiflow.forge.io import no_symlinks

    if type(path) is not str or not path.strip() or len(path) > 128:
        raise ForgeError('工具文件名格式不正确。', code='invalid_request')
    if type(source) is not str or not source.strip():
        raise ForgeError('工具源码不能为空。', code='invalid_request')
    raw_bytes = source.encode('utf-8')
    if len(raw_bytes) > 65536:
        raise ForgeError('工具源码超过 64 KB 上限。', code='invalid_request')

    cleaned = path.strip().replace('\\', '/')
    if cleaned.startswith('data_tools/'):
        cleaned = cleaned[len('data_tools/'):]
    if '/' in cleaned or '..' in cleaned:
        raise ForgeError('仅允许写入项目 data_tools/ 根目录下的 .py 文件，不允许子目录或越界路径：' + path, code='invalid_request')
    stem = cleaned[:-3] if cleaned.endswith('.py') else cleaned
    if not stem.isidentifier() or keyword.iskeyword(stem) or stem.startswith('_'):
        raise ForgeError('工具模块名必须是合法的公开 Python 标识符（例如 pool_review.py）：' + path, code='invalid_request')

    try:
        tree = _safe_tree(source)
        public = exports(tree, DATA_TOOL_EXPORT)
        semantics = data_tool_semantics(tree)
    except (SyntaxError, ValueError) as exc:
        raise ForgeError(f'工具源码校验失败：{exc}', code='invalid_request') from exc

    top_defs = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not node.name.startswith('_')
    }
    target_names = sorted(public) if public is not None else sorted(top_defs)
    if not target_names:
        raise ForgeError(
            '数据工具模块必须至少定义一个公开函数或类（非 _ 开头），并为每个导出项写清首行 docstring 语义描述。',
            code='invalid_request',
        )
    exported = []
    for name in target_names:
        meta = semantics.get(name) or {}
        node = top_defs.get(name)
        doc_first = ((ast.get_docstring(node) or '').strip().splitlines()[0].strip()[:200]
                     if node is not None and (ast.get_docstring(node) or '').strip() else '')
        summary = (meta.get('summary') or doc_first).strip()
        if not summary:
            raise ForgeError(
                f'数据工具 {name} 缺少语义描述：请为 {name} 编写非空 docstring（首行用一句话说明用途，与 Forge 算子规范一致），或在可选 __demiflow_data_tools__ 中提供 summary。',
                code='invalid_request',
            )
        tool_kind = meta.get('kind') or (public[name] if public is not None and name in public else 'tool')
        exported.append({
            'symbol': name,
            'kind': tool_kind,
            'title': meta.get('title') or name,
            'summary': summary,
            'when_to_use': meta.get('when_to_use') or '',
        })

    root = Path(root)
    tools_dir = root / 'data_tools'
    no_symlinks(tools_dir)
    tools_dir.mkdir(mode=0o755, exist_ok=True)
    if not tools_dir.is_dir():
        raise ForgeError('项目 data_tools 路径不是目录。', code='conflict')
    target = tools_dir / f'{stem}.py'
    no_symlinks(target)
    tmp = tools_dir / f'.{stem}.{os.getpid()}.tmp'
    try:
        tmp.write_bytes(raw_bytes)
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
    return {
        'status': 'written',
        'path': f'data_tools/{stem}.py',
        'module': f'data_tools.{stem}',
        'tools': exported,
    }


def _notebook_entry(root, item, definition, module):
    path = Path(item['path'])
    node = _safe_tree(definition['source']).body[0]
    if isinstance(node, ast.ClassDef):
        init = next((n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == '__init__'), None)
        if not init and node.bases:
            return None
        parameters = signature(init) if init else []
    else:
        parameters = signature(node)
    if any(p['positional_only'] for p in parameters):
        return None
    preset_path = ws.relative_path(root, path.with_suffix('.presets.json').as_posix())
    entry = dict(module=module, symbol=item['symbol'], parameters=parameters,
                 asynchronous=isinstance(node, ast.AsyncFunctionDef), presets={})
    if not preset_path.exists():
        return entry
    try:
        by_symbol = json.loads(read_bounded(preset_path, 64 * 1024))
        if type(by_symbol) is not dict:
            raise ForgeError('工具预设文件需要按入口名称组织。', code='invalid_request')
        presets = by_symbol.get(item['symbol'], {})
        allowed = {p['name'] for p in parameters}
        if (type(presets) is not dict or len(presets) > 32 or
                any(not label.strip() or len(label) > 96 or type(args) is not dict
                    or not set(args) <= allowed for label, args in presets.items())):
            raise ForgeError('工具预设的名称或参数与所选入口不匹配。', code='invalid_request')
        bounded_json(presets, 64 * 1024)
        entry['presets'] = presets
    except (ValueError, UnicodeError, OSError, RecursionError) as exc:
        entry['preset_error'] = f'{preset_path.name} · {str(exc)[:200]}'
    return entry


def read(root, *, id, revision='current', data_tools=False, include_platform=False):
    root = Path(root)
    index = catalog(root, data_tools=data_tools, include_platform=include_platform)
    item = next((i for i in index['definitions'] if i['id'] == id), None)
    if item is None: raise ForgeError('Unknown declared definition', code='not_found')
    if item.get('origin') == 'platform':
        root = PLATFORM_ROOT
    if revision == 'current':
        raw = read_bounded(ws.relative_path(root, item['path']), MAX_SOURCE)
        if hashlib.sha256(raw).hexdigest() != item['file_sha256']:
            raise ForgeError('Definition changed; reload', code='conflict')
    else:
        if not isinstance(revision, str) or not re.fullmatch('[0-9a-f]{40,64}', revision):
            raise ForgeError('Invalid saved revision', code='invalid_request')
        if revision not in {v['revision'] for v in history(root, id=id)['versions']}:
            raise ForgeError('Revision is outside the displayed history', code='not_found')
        prefix = (_git(root, 'rev-parse', '--show-prefix') or b'').decode().strip()
        address = revision + ':' + prefix + item['path']
        size = _git(root, 'cat-file', '-s', address)
        if size is None: raise ForgeError('Source unavailable in this revision', code='not_found')
        if int(size) > MAX_SOURCE: raise ForgeError('Historical source exceeds budget', code='resource_limit')
        raw = _git(root, 'show', address)
        if raw is None: raise ForgeError('Historical source unavailable', code='not_found')
    definition = next((d for d in _items(item['path'], raw, item['kind']) if d['symbol'] == item['symbol']), None)
    if definition is None:
        raise ForgeError('所选版本中尚无此定义。', code='not_found')
    result = dict(definition, id=id, path=item['path'], revision=revision, historical=revision != 'current',
                  sha256=content_digest(definition['source']), file_sha256=hashlib.sha256(raw).hexdigest())
    if data_tools:
        result.update(module=item['module'], origin=item['origin'])
        if result['module'] and definition['tool_kind'] in ('view', 'annotation'):
            result['notebook_entry'] = _notebook_entry(root, item, definition, result['module'])
    bounded_json(result, 1024**2)
    return result
