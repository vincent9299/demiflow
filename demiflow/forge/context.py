"""Read authoring guidance, installed SDK facts and the selected project's sources."""
from __future__ import annotations

import ast
import json
import os
from hashlib import sha256
from pathlib import Path

from .api_reference import api_markdown, build_reference, select_apis, sdk_identity
from .io import no_symlinks
from .platform import content_digest
from .platform_environment import read_bounded
from .project_io import relative_path
from .project_rules import GUIDE_DIGEST, GUIDE_RESOURCE, ROLE_DIRS, RULES_ID, TOPICS, guidance
from .schema import ForgeError, canonical

CONTEXT_BYTES = 64 * 1024
SOURCE_LIMIT = 32
FILE_BYTES = 256 * 1024


def _sample(items, budget):
    selected = []
    for item in items:
        budget -= len(canonical(item)) + 1
        if budget < 0 or len(selected) == 64:
            break
        selected.append(item)
    return {'total': len(items), 'items': selected}


def _hidden(name, excluded):
    parts = Path(name).parts
    return (any(part.startswith('.') or part == '__pycache__' for part in parts)
            or any(name == item or name.startswith(item + '/') for item in excluded))


def _direct_references(tree, symbol, filename, target_paths):
    """Return source evidence for direct lexical references in one registered function."""
    function = next((node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                     and node.name == symbol), None)
    if function is None:
        return []
    names = {node.id for node in ast.walk(function) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    literals = {node.value for node in ast.walk(function)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    imported = set()
    for node in [*tree.body, *ast.walk(function)]:
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names
                            if (alias.asname or alias.name.split('.')[0]) in names)
        elif isinstance(node, ast.ImportFrom):
            prefix = list(Path(filename).parent.parts)
            if node.level > len(prefix):
                continue
            prefix = prefix[:len(prefix) - node.level + 1] if node.level else []
            module = '.'.join([*prefix, *((node.module or '').split('.') if node.module else [])])
            for alias in node.names:
                if (alias.asname or alias.name) in names:
                    imported.add(module)
                    imported.add('.'.join(filter(None, (module, alias.name))))
    result = []
    for name in target_paths:
        module = Path(name).with_suffix('').as_posix().replace('/', '.')
        if name in literals or (name.endswith('.py') and module in imported):
            result.append(name)
    return result


def _targets(root, entries, paths, coverage, diagnostics):
    selected = {entry for entry, configs in entries.items()
                if entry.partition(':')[0] in paths or any(name in configs for name in paths)}
    resources = [name for name in paths if name.split('/')[0] in ('operators', 'prompts')]
    consumers = []
    if resources:
        modules = {}
        for entry in entries:
            filename, _, symbol = entry.partition(':')
            modules.setdefault(filename, []).append((entry, symbol))
        remaining = 2 * 1024**2
        for index, (filename, symbols) in enumerate(modules.items()):
            if index >= SOURCE_LIMIT or remaining <= 0:
                message = '入口模块引用扫描达到上限'
                diagnostics.append(dict(code='resource_limit', path=filename, detail=message))
                coverage['unverified'].append(message)
                break
            try:
                raw = read_bounded(relative_path(root, filename), min(FILE_BYTES, remaining))
                remaining -= len(raw)
                tree = ast.parse(raw, filename)
            except (OSError, ValueError, SyntaxError, RecursionError) as exc:
                message = '无法读取入口模块的静态引用：' + filename
                diagnostics.append(dict(code=getattr(exc, 'code', 'source_unavailable'), path=filename,
                                        detail=message + ' · ' + str(exc)))
                coverage['unverified'].append(message)
                continue
            digest = sha256(raw).hexdigest()
            for entry, symbol in symbols:
                for name in _direct_references(tree, symbol, filename, resources):
                    consumers.append(dict(path=name, entry=entry, evidence='direct_source_reference',
                                          source_sha256=digest))
                    selected.add(entry)
        coverage['unverified'].append('间接引用和动态调用需结合目标源码及入口检查核实')
    return dict(paths=list(paths), entries=sorted(selected),
                parameter_paths=sorted({p for e in selected for p in entries[e]}), known_consumers=consumers)


def _project_sources(root, requested, excluded, coverage, diagnostics):
    selected = list(requested)
    if 'README.md' not in selected and not _hidden('README.md', excluded):
        readme = no_symlinks(root / 'README.md')
        if readme.is_file() and readme.stat().st_size <= 8192:
            selected.append('README.md')
    rows = []
    for name in selected:
        try:
            raw = read_bounded(relative_path(root, name), FILE_BYTES)
            text = raw.decode('utf8')
            excerpt = raw[:2048].decode('utf8', 'ignore')
            rows.append(dict(origin='project', path=name, sha256=sha256(raw).hexdigest(),
                             excerpt=excerpt, range=[0, len(excerpt.encode())]))
            coverage['checked'].append(name)
            if len(excerpt) < len(text):
                coverage['unverified'].append('正文仅节选：' + name)
        except (OSError, ValueError) as exc:
            coverage['unverified'].append(name)
            if name in requested:
                diagnostics.append(dict(code='source_unavailable', path=name, detail=str(exc)))
    return rows


def _runtime_environment(root):
    setting = os.environ.get('DEMIFLOW_FORGE_RUNTIME_CONFIG')
    if not setting:
        return None
    path = Path(setting)
    if not path.is_absolute() or '..' in path.parts or path.is_relative_to(root):
        return None
    try:
        raw = read_bounded(no_symlinks(path), 65536)
        data = json.loads(raw)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get('root') and Path(data['root']).resolve() != root.resolve():
        return None
    env = {}
    if isinstance(data.get('execution_mode'), str):
        env['execution_mode'] = data['execution_mode']
    py = data.get('python')
    env['python'] = py if isinstance(py, str) and py else None
    writes = data.get('writes')
    if isinstance(writes, list):
        env['writes'] = [str(item) for item in writes[:64]]
        if len(writes) > 64:
            env['writes_total'] = len(writes)
    if isinstance(data.get('shared_sources'), list):
        env['shared_sources'] = [str(item) for item in data['shared_sources'][:32]]
    if isinstance(data.get('input_access'), bool):
        env['input_access'] = data['input_access']
    return env


def build_context(root, *, paths=(), topic=None, api=None, sources=(), reference=None, exclude=()):
    from .project import discover_project_sources

    root = Path(root).absolute()
    if '..' in root.parts:
        raise ForgeError('Project root must be normalized', code='invalid_request')
    no_symlinks(root)
    if (root.exists() and not root.is_dir()) or (not root.exists() and not root.parent.is_dir()):
        raise ForgeError('Select a project directory or an existing parent', code='invalid_request')
    if len(paths) + len(sources) > SOURCE_LIMIT - 3:
        raise ForgeError('Selected paths exceed context budget', code='resource_limit')
    paths, sources, excluded = tuple(dict.fromkeys(paths)), tuple(dict.fromkeys(sources)), tuple(exclude)
    for name in (*paths, *sources):
        if not isinstance(name, str):
            raise ForgeError('Select a project-relative text path', code='invalid_request')
        if _hidden(name, excluded):
            raise ForgeError('Selected project path is protected', code='operation_rejected')
        relative_path(root, name)
    chosen = {topic} if topic is not None else set()
    roles = dict(zip(ROLE_DIRS, TOPICS))
    chosen.update(roles[name.split('/')[0]] for name in paths if name.split('/')[0] in roles)
    rules, notes = guidance(topics=chosen)
    coverage = dict(checked=[], unverified=[])
    diagnostics = []
    result = dict(status='ready', root=str(root), diagnostics=diagnostics, coverage=coverage,
                  rules=rules, rules_id=RULES_ID, guidance=notes, topics=list(TOPICS),
                  sdk=dict(sdk_revision=None, catalog_id=None, apis=[]))
    try:
        if reference is None:
            reference = build_reference()
        elif (content_digest(reference['catalog']) != reference['catalog_id']
              or sdk_identity(reference['catalog']) != reference['sdk_revision']):
            raise ForgeError('Cached SDK identity differs from loaded sources', code='conflict')
        result['sdk'] = dict(sdk_revision=reference['sdk_revision'], catalog_id=reference['catalog_id'],
                             apis=select_apis(reference, api=api))
    except ForgeError as exc:
        if exc.code == 'invalid_request':
            raise
        diagnostics.append(dict(code=exc.code, detail=str(exc)))
        coverage['unverified'].append('SDK')

    result['sources'] = [dict(origin='forge', path=GUIDE_RESOURCE, sha256=GUIDE_DIGEST)]
    try:
        view = discover_project_sources(root, exclude=excluded)
    except ForgeError as exc:
        if exc.code != 'resource_limit':
            raise
        diagnostics.append(dict(code=exc.code, detail=str(exc)))
        coverage['unverified'].append('项目目录枚举未完成')
        unknown = dict(total=None, items=[])
        result.update(project=dict(state='unknown', name=root.name), registered_flows=unknown,
                      role_files={role: dict(unknown) for role in ROLE_DIRS},
                      targets=dict(paths=list(paths), entries=[], parameter_paths=[], known_consumers=[]))
    else:
        control = view['control']
        project = dict(state='uninitialized', name=root.name)
        if control is not None:
            project = dict(state='initialized', name=control['name'], id=control.get('id'),
                           platform_digest=control.get('platform', {}).get('digest'))
            if isinstance(control.get('locations'), dict) and control['locations']:
                project['locations'] = {
                    str(k): (dict(root=str(v.get('root', '')), writable=bool(v.get('writable', False)))
                             if isinstance(v, dict) else str(v))
                    for k, v in sorted(control['locations'].items())[:32]
                }
            raw = read_bounded(root / 'workspace.json', 1024**2)
            if json.loads(raw) != control:
                raise ForgeError('Project declaration changed during discovery', code='conflict')
            result['sources'].append(dict(origin='project', path='workspace.json',
                                          sha256=sha256(raw).hexdigest()))
            coverage['checked'].append('workspace.json')
        entries = {key: [p for p in configs if not _hidden(p, excluded)]
                   for key, configs in sorted(view['entries'].items()) if not _hidden(key.split(':')[0], excluded)}
        result.update(project=project,
                      registered_flows=_sample([dict(entry=key, parameters=values[:4], parameter_count=len(values))
                                                for key, values in entries.items()], 6 * 1024),
                      role_files={role: _sample([p for p in files if not _hidden(p, excluded)], 2 * 1024)
                                  for role, files in view['roles'].items()},
                      targets=_targets(root, entries, paths, coverage, diagnostics))
    requested = tuple(dict.fromkeys([*sources, *(name for name in paths
                                                   if name != 'workspace.json' and (root / name).is_file())]))
    result['sources'].extend(_project_sources(root, requested, excluded, coverage, diagnostics))
    env_cfg = _runtime_environment(root)
    if env_cfg is not None:
        result['environment'] = env_cfg
    if diagnostics:
        result['status'] = 'incomplete'
    if len(canonical(result)) > CONTEXT_BYTES:
        raise ForgeError('Selected context exceeds 64 KiB; narrow the target', code='resource_limit')
    return result


def render_context(result):
    rows = ['## Forge 开发规范', *(f"- {r['id']} · {r['text']}（{r['source']}）" for r in result['rules'])]
    rows.extend(f"\n{n['text']}（{n['source']}）" for n in result['guidance'])
    rows.append('强制条款版本 ' + result['rules_id'])
    sdk = result['sdk']
    rows.extend(['\n' + api_markdown(sdk['apis']),
                 f"SDK 版本依据 {sdk['sdk_revision']} · API 目录 {sdk['catalog_id']}", '\n## 当前项目',
                 f"{result['project']['name']} · {result['root']} · {result['project']['state']}"])
    locations = result['project'].get('locations')
    if locations:
        loc_parts = [
            f"{k}={v['root']}({'可写' if v.get('writable') else '只读'})" if isinstance(v, dict) else f'{k}={v}'
            for k, v in locations.items()
        ]
        rows.append('项目自定义目录（locations）：' + '、'.join(loc_parts))
    env_cfg = result.get('environment')
    if isinstance(env_cfg, dict):
        rows.append('\n## 项目运行与沙箱配置')
        if env_cfg.get('execution_mode'):
            rows.append('执行模式：' + ('受限开发沙箱（读写限于本项目目录及 $TMPDIR）'
                                     if env_cfg['execution_mode'] == 'restricted' else env_cfg['execution_mode']))
        if env_cfg.get('python'):
            rows.append('业务 Python 环境（只读）：' + env_cfg['python'] + '（已加入 PATH，可直接运行 python / pytest，已自动接通只读 demiflow SDK 与 pytest 沙箱参数）')
        else:
            rows.append('业务 Python 环境：未配置（工作台不默认继承平台环境；请勿在宿主机搜索 Python/pytest，如需运行测试请提示用户在项目设置中配置业务 Python）')
        if env_cfg.get('writes'):
            extra = f"（共 {env_cfg['writes_total']} 项）" if env_cfg.get('writes_total') else ''
            rows.append('授权可写范围' + extra + '：' + '、'.join(env_cfg['writes'][:20]))
        if env_cfg.get('shared_sources'):
            rows.append('已声明只读共享模块根包：' + '、'.join(env_cfg['shared_sources']))
    flows = result['registered_flows']
    rows.append(f"登记入口（共 {flows['total'] if flows['total'] is not None else '未知'} 个）")
    for flow in flows['items']:
        rows.append(f"- {flow['entry']} · {', '.join(flow['parameters'])}（参数文件 {flow['parameter_count']} 个）")
    for role, files in result['role_files'].items():
        rows.append(f"- {role} 文件（共 {files['total'] if files['total'] is not None else '未知'} 个） " +
                    '、'.join(files['items']))
    if result['targets']['paths']:
        rows.append('本次目标与已知直接引用：' + canonical(result['targets']).decode())
    for source in result['sources']:
        rows.append(f"来源 {source['origin']} · {source['path']} · sha256 {source['sha256']}")
        if source.get('excerpt'):
            rows.append(f"\n### {source['path']}［字节 {source['range'][0]}–{source['range'][1]}］\n{source['excerpt']}")
    rows.extend('待核对：' + item for item in result['coverage']['unverified'])
    rows.extend(d['detail'] for d in result['diagnostics'])
    rows.append('可按主题查阅：' + '、'.join(result['topics']))
    text = '\n'.join(rows)
    if len(text.encode()) > CONTEXT_BYTES:
        raise ForgeError('Rendered context exceeds budget', code='resource_limit')
    return text
