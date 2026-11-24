"""Packaged authoring rules and independent source-layout diagnostics."""
import ast
from hashlib import sha256
import json
from pathlib import Path
import re

from .platform import content_digest
from .platform_environment import read_bounded
from .schema import ForgeError


ROLE_DIRS = ('flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks')
TOPICS = ('flow', 'operator', 'prompt')
GUIDE_RESOURCE = 'standards/pipeline-authoring.md'
_IGNORED_NAMES = frozenset({
    '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache',
    '.ipynb_checkpoints', '.DS_Store',
})
_ROOT_CONFIG_SUFFIXES = frozenset({'.json', '.yaml', '.yml'})
_OUTPUT_ARG_KEYS = frozenset({
    'output_directory', 'output_dir', 'output_path', 'output_root',
    'output', 'target_dir', 'target', 'destination', 'destinations',
})
_NOTEBOOK_UI_MODULES = frozenset({'IPython', 'ipywidgets'})
_LOCAL_CODE_PACKAGES = frozenset({'flows', 'operators', 'data_tools', 'tests'})


def _read_guide():
    path = Path(__file__).parent / GUIDE_RESOURCE
    raw = read_bounded(path, 64 * 1024)
    sections = re.split(r'^## (\w+)\s*$', raw.decode(), flags=re.MULTILINE)
    if tuple(sections[1::2]) != ('core', *TOPICS):
        raise ForgeError('Authoring guide has invalid topic headings')
    rules, notes = [], []
    for topic, body in zip(sections[1::2], sections[2::2]):
        items = re.split(r'^### ([^\n]+)$', body, flags=re.MULTILINE)
        for heading, text in zip(items[1::2], items[2::2]):
            source = f'{GUIDE_RESOURCE}#{topic}'
            if heading == '建议与示例':
                notes.append(dict(topic=topic, text=text.strip(), source=source))
            elif re.fullmatch(r'DF-\d{2}', heading):
                if any(rule['id'] == heading for rule in rules):
                    raise ForgeError('Duplicate authoring rule: ' + heading)
                rules.append(dict(topic=topic, id=heading, text=text.strip(), source=source))
            else:
                raise ForgeError('Unknown authoring guide heading: ' + heading)
    return rules, notes, sha256(raw).hexdigest()


_RULES, _NOTES, GUIDE_DIGEST = _read_guide()
RULES_ID = content_digest([(item['id'], item['text']) for item in _RULES])


def guidance(*, topics=()):
    selected = {'core', *topics}
    if selected.difference(('core', *TOPICS)):
        raise ForgeError('Unknown authoring topic', code='invalid_request')
    return ([dict(item) for item in _RULES if item['topic'] in selected],
            [dict(item) for item in _NOTES if item['topic'] in selected])


def diagnostic(rule, path, detail, *, line=None, status='invalid'):
    return dict(rule=rule, path=path, line=line, detail=detail, status=status)


def check_layout(*, changed, operators, prompts):
    """Check known roles, including actual operator/prompt use reported by the checker."""
    diagnostics, inspected = [], set()
    for path in changed:
        if Path(path).parts[0] in ROLE_DIRS:
            inspected.add(path)
    roles = {
        'operators': operators,
        'prompts': prompts,
    }
    for directory, paths in roles.items():
        for path, line in paths:
            inspected.add(path)
            if Path(path).parts[:1] != (directory,):
                diagnostics.append(diagnostic('DF-02', path,
                    f'此文件的角色要求放在 {directory}/。', line=line))
    return inspected, diagnostics


def _iter_tree_files(root: Path, rel_dir: str, *, exclude=(), max_files: int = 2048):
    """Yield sorted relative POSIX paths under root/rel_dir without descending into data/cache dirs."""
    base = root / rel_dir
    if not base.is_dir() or base.is_symlink():
        return []
    found = []
    stack = [base]
    while stack and len(found) < max_files:
        current = stack.pop()
        try:
            children = sorted(current.iterdir(), key=lambda p: p.name, reverse=True)
        except OSError:
            continue
        for child in children:
            name = child.name
            if name.startswith('.') or name in _IGNORED_NAMES:
                continue
            rel = child.relative_to(root).as_posix()
            if any(Path(rel).is_relative_to(parent) for parent in exclude):
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                if child.suffix == '.lance':
                    continue
                stack.append(child)
            elif child.is_file():
                found.append(rel)
                if len(found) >= max_files:
                    break
    return sorted(found)


def _has_declared_export(path: Path, field: str) -> bool:
    from .definition_exports import exports
    try:
        raw = read_bounded(path, 512 * 1024)
        names = exports(ast.parse(raw, filename=str(path)), field)
        return bool(names)
    except Exception:
        return False


def _imports_notebook_ui(path: Path) -> bool:
    try:
        raw = read_bounded(path, 512 * 1024)
        tree = ast.parse(raw, filename=str(path))
    except Exception:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.split('.', 1)[0] in _NOTEBOOK_UI_MODULES for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if node.module.split('.', 1)[0] in _NOTEBOOK_UI_MODULES:
                return True
    return False


def _project_import_prefixes(root: Path) -> list[tuple[str, ...]]:
    """Derive possible package import prefixes for this project (e.g. ('pipelines', '<name>') or ('<name>',))."""
    prefixes: list[tuple[str, ...]] = []
    name = root.name
    if name and name.isidentifier():
        prefixes.append((name,))
        parent = root.parent.name
        if parent and parent.isidentifier():
            prefixes.append((parent, name))
    return prefixes


def _module_exists(root: Path, rel_parts: tuple[str, ...]) -> bool:
    if not rel_parts:
        return True
    base = root.joinpath(*rel_parts)
    if base.with_suffix('.py').is_file():
        return True
    if base.is_dir():
        return True
    return False


def _dangling_local_imports(root: Path, path: Path) -> list[tuple[int, str]]:
    """Return [(lineno, module_spec)] for project-local imports whose target module does not exist."""
    try:
        raw = read_bounded(path, 512 * 1024)
        tree = ast.parse(raw, filename=str(path))
    except Exception:
        return []
    prefixes = _project_import_prefixes(root)
    missing: list[tuple[int, str]] = []
    seen: set[str] = set()

    def strip_project_prefix(parts: tuple[str, ...]) -> tuple[str, ...] | None:
        if not parts:
            return None
        if parts[0] in _LOCAL_CODE_PACKAGES:
            return parts
        for pref in prefixes:
            if len(parts) > len(pref) and parts[:len(pref)] == pref:
                return parts[len(pref):]
        return None

    def check_target(lineno: int, full_name: str, rel_parts: tuple[str, ...]):
        if not rel_parts:
            return
        if not _module_exists(root, rel_parts):
            if full_name not in seen:
                seen.add(full_name)
                missing.append((lineno, full_name))

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                parts = tuple(alias.name.split('.'))
                rel = strip_project_prefix(parts)
                if rel:
                    check_target(node.lineno, alias.name, rel)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            parts = tuple(node.module.split('.'))
            rel = strip_project_prefix(parts)
            if rel is None:
                # Also handle `from pipelines.<proj> import foo`
                for pref in prefixes:
                    if parts == pref:
                        for alias in node.names:
                            if alias.name == '*':
                                continue
                            check_target(node.lineno, f'{node.module}.{alias.name}', (alias.name,))
                continue
            if len(rel) == 1 and rel[0] in _LOCAL_CODE_PACKAGES:
                # e.g. `from pipelines.<proj>.operators import moved_mod`
                init_file = root / rel[0] / '__init__.py'
                init_text = ''
                if init_file.is_file():
                    try:
                        init_text = read_bounded(init_file, 65536).decode('utf-8', errors='ignore')
                    except Exception:
                        pass
                for alias in node.names:
                    if alias.name == '*':
                        continue
                    if not _module_exists(root, (*rel, alias.name)) and alias.name not in init_text:
                        check_target(node.lineno, f'{node.module}.{alias.name}', (*rel, alias.name))
            else:
                check_target(node.lineno, node.module, rel)
    return missing


def _points_to_runs(root: Path, value) -> bool:
    runs_root = (root / 'runs').resolve(strict=False)
    candidates = []
    if isinstance(value, str) and value.strip():
        candidates.append(value.strip())
    elif isinstance(value, dict):
        for k in ('uri', 'path', 'directory', 'dir'):
            if isinstance(value.get(k), str) and value[k].strip():
                candidates.append(value[k].strip())
    for raw in candidates:
        if raw == 'runs' or raw.startswith('runs/') or raw.startswith('./runs/'):
            return True
        p = Path(raw)
        if p.is_absolute():
            resolved = p.resolve(strict=False)
            if resolved == runs_root or resolved.is_relative_to(runs_root):
                return True
    return False


def _group_unreferenced_under(prefix: str, all_files: list[str], unreferenced: list[str]) -> list[str]:
    """Collapse a top-level subdirectory into one diagnostic path when all its files are unreferenced."""
    unref_set = set(unreferenced)
    by_sub: dict[str, list[str]] = {}
    top_level: list[str] = []
    for rel in all_files:
        parts = Path(rel).parts
        if len(parts) >= 3 and parts[0] == prefix:
            by_sub.setdefault(f'{prefix}/{parts[1]}', []).append(rel)
        elif rel in unref_set:
            top_level.append(rel)
    grouped = list(top_level)
    for sub, members in sorted(by_sub.items()):
        if all(m in unref_set for m in members):
            grouped.append(sub)
        else:
            grouped.extend(m for m in members if m in unref_set)
    return sorted(grouped)


def check_project_hygiene(root: Path, control: dict, *, checked_sources: dict, parameter_digests: dict,
                          exclude=()):
    """Audit full-project DF-02 structural cleanliness without mutating or inspecting business data."""
    root = Path(root).absolute()
    entries = (control or {}).get('entries') or {}
    if not entries:
        return set(), []
    registered_flows = {e.partition(':')[0] for e in entries}
    bound_params = {p for ps in entries.values() for p in ps}
    active_sources = set(checked_sources) | bound_params
    inspected: set[str] = set()
    by_category: dict[str, list[dict]] = {}

    def add(category: str, path: str, detail: str):
        inspected.add(path)
        by_category.setdefault(category, []).append(diagnostic('DF-02', path, detail))

    # Build bounded text corpus from active checked sources & bound parameters for reachability checks.
    corpus_chunks = []
    corpus_budget = 4 * 1024 * 1024
    for rel in sorted(active_sources):
        if corpus_budget <= 0:
            break
        fp = root / rel
        if not fp.is_file() or fp.is_symlink():
            continue
        try:
            raw = read_bounded(fp, min(512 * 1024, corpus_budget))
        except Exception:
            continue
        corpus_budget -= len(raw)
        corpus_chunks.append(raw.decode('utf-8', errors='ignore'))
    corpus = '\n'.join(corpus_chunks)

    # 1. Root-level clutter: no stray notebooks, configs, or scripts outside standard directories.
    try:
        root_items = sorted(root.iterdir(), key=lambda p: p.name)
    except OSError:
        root_items = []
    for item in root_items:
        name = item.name
        if name.startswith('.') or name in _IGNORED_NAMES or item.is_symlink():
            continue
        if any(Path(name).is_relative_to(parent) for parent in exclude):
            continue
        if item.is_file():
            suffix = item.suffix.lower()
            if suffix == '.ipynb':
                add('root_notebook', name, '根目录散落 Notebook 文件；按 DF-02 规范应迁入 notebooks/ 目录（保留原有输出）。')
            elif suffix in _ROOT_CONFIG_SUFFIXES and name != 'workspace.json':
                add('root_config', name, '根目录散落配置文件；按 DF-02 规范业务与部署配置应统一放入 configs/ 目录。')
            elif suffix == '.py' and name != '__init__.py' and name not in registered_flows and name not in checked_sources:
                add('root_script', name, '根目录散落 Python 脚本；请按职责迁入 flows/、operators/、data_tools/ 或清理废弃脚本。')

    # 2. Deprecated parameters/ directory: unbound legacy files or empty leftover directory.
    param_dir = root / 'parameters'
    if param_dir.is_dir() and not param_dir.is_symlink():
        param_files = _iter_tree_files(root, 'parameters', exclude=exclude)
        unbound_params = [p for p in param_files if p not in bound_params]
        for rel in unbound_params:
            add('legacy_parameters', rel, 'parameters/ 为历史兼容目录，不应残留未登记的旧参数文件；请将有效配置放在 configs/ 并清理废弃文件。')
        if not param_files and not any(p.startswith('parameters/') for p in bound_params):
            add('legacy_parameters', 'parameters', 'parameters/ 为已废弃的空兼容目录，请清理。')

    # 3. Orphan unregistered files in flows/.
    for rel in _iter_tree_files(root, 'flows', exclude=exclude):
        p = Path(rel)
        if p.name == '__init__.py' or p.suffix == '.pyc':
            continue
        if p.suffix == '.py':
            if rel not in registered_flows:
                add('orphan_flows', rel, 'flows/ 下存在未在 workspace.json 登记的孤儿文件；请登记为正式入口，共享逻辑迁入 operators/，废弃文件请清理。')
        else:
            add('orphan_flows', rel, 'flows/ 仅存放已登记的 Python Flow 入口文件；配置或辅助文件请迁入对应标准目录。')

    # 4. Unreferenced / non-operator / Notebook-UI files in operators/.
    for rel in _iter_tree_files(root, 'operators', exclude=exclude):
        p = Path(rel)
        if p.name == '__init__.py' or p.suffix == '.pyc':
            continue
        if p.suffix != '.py':
            add('orphan_operators', rel, 'operators/ 仅存放 Python 算子模块；查看器模板请随工具迁入 data_tools/，配置请迁入 configs/。')
            continue
        if _imports_notebook_ui(root / rel):
            add('notebook_tool_in_operators', rel, 'operators/ 模块导入了 Notebook 交互库（IPython/ipywidgets），属于 Notebook 查看/分析/标注工具；按 DF-02 规范请迁入 data_tools/ 并声明 __demiflow_data_tools__。')
            continue
        if rel not in checked_sources and not _has_declared_export(root / rel, '__demiflow_operators__'):
            add('orphan_operators', rel, 'operators/ 模块未被已登记 Flow 使用且未声明 __demiflow_operators__；若为 Notebook 查看/分析/标注工具请迁入 data_tools/ 并声明 __demiflow_data_tools__，若为废弃代码请清理。')

    # 5. data_tools/*.py must declare __demiflow_data_tools__.
    for rel in _iter_tree_files(root, 'data_tools', exclude=exclude):
        p = Path(rel)
        if p.suffix == '.py' and p.name != '__init__.py':
            if not _has_declared_export(root / rel, '__demiflow_data_tools__'):
                add('data_tools_export', rel, 'data_tools/ 模块需在顶层声明 __demiflow_data_tools__ 映射（如 {"review": "annotation"}）以便工作台「工具」面板发现。')

    # 5b. Dangling project-local imports in tests/*.py (catches broken tests after cleanup/moves).
    for rel in _iter_tree_files(root, 'tests', exclude=exclude):
        p = Path(rel)
        if p.suffix != '.py' or p.name == '__init__.py':
            continue
        dangling = _dangling_local_imports(root, root / rel)
        if dangling:
            first_line, first_mod = dangling[0]
            extra = f' 等 {len(dangling)} 处' if len(dangling) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('broken_test_imports', []).append(
                diagnostic('DF-02', rel,
                           f'测试文件引用了不存在的项目模块 {first_mod}{extra}；删除或迁移模块后请同步更新或清理对应测试。',
                           line=first_line)
            )

    # 6. Unreferenced orphan files in configs/.
    config_files = _iter_tree_files(root, 'configs', exclude=exclude)
    unref_configs = []
    for rel in config_files:
        sub_rel = rel[len('configs/'):]
        if rel in active_sources or rel in corpus or sub_rel in corpus:
            continue
        unref_configs.append(rel)
    for target in _group_unreferenced_under('configs', config_files, unref_configs):
        add('orphan_configs', target, 'configs/ 中存在未被已登记入口、参数或活跃代码引用的孤立配置；请绑定到入口或清理废弃实验配置。')

    # 7. Unreferenced obsolete files in prompts/.
    prompt_files = _iter_tree_files(root, 'prompts', exclude=exclude)
    unref_prompts = []
    for rel in prompt_files:
        sub_rel = rel[len('prompts/'):]
        if rel in active_sources or rel in corpus or sub_rel in corpus:
            continue
        unref_prompts.append(rel)
    for target in _group_unreferenced_under('prompts', prompt_files, unref_prompts):
        add('orphan_prompts', target, 'prompts/ 中存在未被任何活跃 Flow、算子或配置引用的废弃 Prompt；请清理以保持目录整洁。')

    # 8. Active configs/*.json output paths should target data/, not legacy runs/.
    for param_rel in sorted(bound_params):
        if not param_rel.startswith('configs/'):
            continue
        fp = root / param_rel
        if not fp.is_file() or fp.is_symlink():
            continue
        try:
            doc = json.loads(read_bounded(fp, 65536).decode('utf-8'))
        except Exception:
            continue
        args = doc.get('arguments') if isinstance(doc, dict) else None
        if not isinstance(args, dict):
            continue
        for key, value in args.items():
            if key in _OUTPUT_ARG_KEYS and _points_to_runs(root, value):
                add('legacy_output_dir', param_rel,
                    f'活跃参数 {key} 仍指向历史 runs/ 目录；按 DF-02 规范默认产物应输出到 data/ 目录（历史 runs/ 仅作只读保留）。')
                break

    # Interleave categories round-robin so truncated summaries still show every violation type.
    ordered: list[dict] = []
    queues = [list(items) for _, items in sorted(by_category.items())]
    while queues:
        next_queues = []
        for q in queues:
            ordered.append(q.pop(0))
            if q:
                next_queues.append(q)
        queues = next_queues
    return inspected, ordered
