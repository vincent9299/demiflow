"""Read declared reusable definitions and their real Git history; never import them."""
from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path
import re
import subprocess
import yaml

from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from . import workspace as ws
from .discovery import discover

from ..definition_contracts import signature, code_definition, prompt_definition

MAX_SOURCE = 512 * 1024



def _safe_tree(raw):
    tree = ast.parse(raw)
    if sum(1 for _ in ast.walk(tree)) > 50000:
        raise ForgeError('Definition AST exceeds budget', code='resource_limit')
    return tree


def _items(path, raw, kind):
    text = raw.decode('utf-8')
    if kind == 'code':
        tree = _safe_tree(text)
        lines = text.splitlines(keepends=True)
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) or node.name.startswith('_'):
                continue
            start = min([node.lineno, *(d.lineno for d in node.decorator_list)])
            body = ''.join(lines[start - 1:node.end_lineno])
            yield dict(code_definition(node, body), label=node.name,
                       line=start, end_line=node.end_lineno)
    else:
        import yaml
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
        if type(value) is not dict or value.get('schema_version') != 'demiflow_prompt_pack_v2' or type(value.get('prompts')) is not dict:
            raise ForgeError('Expected a native versioned Prompt pack', code='invalid_request')
        for name, item in value['prompts'].items():
            if type(name) is not str or type(item) is not dict:
                raise ForgeError('Invalid named Prompt definition', code='invalid_request')
            body = yaml.safe_dump(item, allow_unicode=True, sort_keys=False)
            yield dict(prompt_definition(name, item), label=name, source=body, line=None, end_line=None,
                       template_sha256=hashlib.sha256(item['template'].encode()).hexdigest() if type(item.get('template')) is str else None)


def _files(root, declarations, kind):
    count = 0; result = set()
    suffixes = {'.py'} if kind == 'code' else {'.yaml', '.yml', '.json'}
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


def catalog(root):
    root, control, _ = ws.load_workspace(root)
    facts = discover(root, control)
    items, diagnostics, sources, total = [], [], {}, 0
    for role, kind in (('operators', 'code'), ('prompts', 'prompt')):
        for path in _files(root, facts.get(role, []), kind):
            raw = read_bounded(path, MAX_SOURCE); total += len(raw)
            if total > 4 * 1024**2:
                raise ForgeError('Definition source bytes exceeded', code='resource_limit')
            relative = path.relative_to(root).as_posix()
            sources[relative] = hashlib.sha256(raw).hexdigest()
            try:
                for item in _items(relative, raw, kind):
                    item.update(id=content_digest([relative, kind, item['symbol']]), path=relative,
                                sha256=content_digest(item['source']), file_sha256=sources[relative])
                    item.pop('source')
                    # Large Prompt bodies belong to the selected detail, never the index.
                    for field in ('template', 'messages', 'model', 'output_schema', 'input_variables', 'defaults'): item.pop(field, None)
                    items.append(item)
                    if len(items) > 1024:
                        raise ForgeError('Definition count exceeds budget', code='resource_limit')
            except (SyntaxError, UnicodeError, ValueError, yaml.YAMLError) as exc:
                if isinstance(exc, ForgeError) and exc.code == 'resource_limit': raise
                diagnostics.append(dict(path=relative, detail=str(exc)[:256]))
    for path, digest in sources.items():
        if hashlib.sha256(read_bounded(ws.relative_path(root, path), MAX_SOURCE)).hexdigest() != digest:
            raise ForgeError('Definitions changed during reading', code='conflict')
    source_prefix = ''
    for declaration in facts['declarations']:
        path = Path(declaration['path'])
        if path.name == 'project.json' and path.parent.name == '.demiforge' and root.is_relative_to(path.parent.parent):
            source_prefix = root.relative_to(path.parent.parent).as_posix()
            if source_prefix == '.': source_prefix = ''
            break
    result = dict(format='demiflow.project_definitions.v1', source_prefix=source_prefix, definitions=items, diagnostics=diagnostics,
                  discovery=facts['digest'], sources=sources, usage_coverage='declared_sources_only')
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


def read(root, *, id, revision='current'):
    root = Path(root)
    index = catalog(root)
    item = next((i for i in index['definitions'] if i['id'] == id), None)
    if item is None: raise ForgeError('Unknown declared definition', code='not_found')
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
    bounded_json(result, 1024**2)
    return result
