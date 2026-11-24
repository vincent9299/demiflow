"""Packaged authoring rules and independent source-layout diagnostics."""
import ast
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import re
import sys

from .platform import content_digest
from .platform_environment import read_bounded
from .schema import ForgeError


ROLE_DIRS = ('flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks')
ALLOWED_ROOT_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks',
    'data', 'logs', 'docs', 'tests', 'runs',
})
TOPICS = ('flow', 'operator', 'prompt')
GUIDE_RESOURCE = 'standards/pipeline-authoring.md'
_IGNORED_NAMES = frozenset({
    '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache',
    '.ipynb_checkpoints', '.DS_Store',
})
_ROOT_CONFIG_SUFFIXES = frozenset({'.json', '.yaml', '.yml'})
_ARCHIVE_SUFFIXES = frozenset({'.tar', '.gz', '.tgz', '.bz2', '.xz', '.zip', '.7z', '.rar'})
_DOCS_DISALLOWED_SUFFIXES = frozenset({'.py', '.sh', '.yaml', '.yml'}) | _ARCHIVE_SUFFIXES
_DOCS_DISALLOWED_SUBDIRS = frozenset({
    'flows', 'operators', 'configs', 'parameters', 'prompts',
    'data_tools', 'scripts', 'root-scripts',
})
_OUTPUT_ARG_KEYS = frozenset({
    'output_directory', 'output_dir', 'output_path', 'output_root',
    'output', 'target_dir', 'target', 'destination', 'destinations',
})
_NOTEBOOK_UI_MODULES = frozenset({'IPython', 'ipywidgets'})
_LOCAL_CODE_PACKAGES = frozenset({'flows', 'operators', 'data_tools', 'tests'})
_PIPELINE_ROLE_SEGMENTS = frozenset({'flows', 'operators', 'data_tools'})
_INTERNAL_DEMIFLOW_SUBPACKAGES = frozenset({'workbench', 'forge', 'services'})
_SITE_DIR_MARKERS = ('site-packages', 'dist-packages', 'lib-dynload')


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


def _public_execution_exports() -> frozenset[str]:
    try:
        import demiflow.execution as _exec_mod
        return frozenset(getattr(_exec_mod, '__all__', ()))
    except Exception:
        return frozenset({'RunContext', 'RunRef', 'current_run'})


def _is_site_or_stdlib(top_pkg: str) -> bool:
    if top_pkg in sys.stdlib_module_names or top_pkg == 'demiflow':
        return True
    try:
        spec = importlib.util.find_spec(top_pkg)
    except (ImportError, ModuleNotFoundError, ValueError):
        return False
    if spec is None:
        return False
    if spec.origin in (None, 'built-in', 'frozen'):
        locs = list(spec.submodule_search_locations or ())
        if not locs:
            return spec.origin in ('built-in', 'frozen')
        candidates = [str(loc) for loc in locs]
    else:
        candidates = [str(spec.origin)]
    prefixes = tuple({str(Path(p).resolve()) for p in (sys.prefix, sys.base_prefix) if p})
    for raw in candidates:
        if any(marker in raw for marker in _SITE_DIR_MARKERS):
            return True
        try:
            resolved = str(Path(raw).resolve())
        except OSError:
            resolved = raw
        if any(resolved.startswith(pref + '/') for pref in prefixes):
            return True
    return False


def _is_local_to_project(root: Path, parts: tuple[str, ...], prefixes: list[tuple[str, ...]]) -> bool:
    if not parts:
        return True
    if parts[0] in _LOCAL_CODE_PACKAGES:
        return True
    for pref in prefixes:
        if len(parts) >= len(pref) and parts[:len(pref)] == pref:
            return True
    try:
        if (root / parts[0]).exists() or (root / f'{parts[0]}.py').is_file():
            return True
    except OSError:
        pass
    return False


def _is_cross_project_sibling(root: Path, parts: tuple[str, ...]) -> bool:
    """Detect imports reaching outside this project into a sibling business project (works inside Landlock)."""
    if not parts:
        return False
    top = parts[0]
    if _is_site_or_stdlib(top):
        return False
    # 1. Same-parent sibling project (e.g. `pipelines.other_proj...` inside `pipelines/this_proj`)
    parent_name = root.parent.name
    if parent_name and parent_name.isidentifier() and top == parent_name and len(parts) >= 2 and parts[1] != root.name:
        return True
    # 2. Multi-part import carrying Forge role-dir segment outside this project (works under Landlock)
    if len(parts) >= 3 and any(seg in _PIPELINE_ROLE_SEGMENTS for seg in parts[1:]):
        return True
    # 3. Readable ancestor sibling directory on disk (host check)
    for anc in (root.parent, root.parent.parent):
        if anc == anc.parent:
            break
        try:
            cand = anc / top
            if cand != root and cand not in root.parents and cand.is_dir():
                return True
        except OSError:
            continue
    return False


def _check_demiflow_module_public(mod_name: str, imported_names: tuple[str, ...] = ()) -> str | None:
    """Return a violation target string if a demiflow.* import targets a private/removed/internal module."""
    parts = tuple(mod_name.split('.'))
    if not parts or parts[0] != 'demiflow':
        return None
    if any(p.startswith('_') for p in parts[1:]):
        return mod_name
    if len(parts) >= 2 and parts[1] in _INTERNAL_DEMIFLOW_SUBPACKAGES:
        return mod_name
    if len(parts) >= 3 and parts[1] == 'execution':
        return mod_name
    if len(parts) == 2 and parts[1] == 'execution' and imported_names:
        public_exec = _public_execution_exports()
        for sym in imported_names:
            if sym != '*' and sym not in public_exec:
                return f'{mod_name}.{sym}'
    try:
        spec = importlib.util.find_spec(mod_name)
    except (ImportError, ModuleNotFoundError, ValueError):
        spec = None
    if spec is None:
        return mod_name
    return None


def _boundary_import_issues(root: Path, path: Path, *, check_cross_project: bool = True):
    """Return (sdk_issues, cross_project_issues) as [(lineno, target)] lists."""
    try:
        raw = read_bounded(path, 512 * 1024)
        tree = ast.parse(raw, filename=str(path))
    except Exception:
        return [], []
    prefixes = _project_import_prefixes(root)
    sdk_issues: list[tuple[int, str]] = []
    cross_issues: list[tuple[int, str]] = []
    seen_sdk: set[str] = set()
    seen_cross: set[str] = set()

    def record_sdk(lineno: int, bad: str):
        if bad not in seen_sdk:
            seen_sdk.add(bad)
            sdk_issues.append((lineno, bad))

    def record_cross(lineno: int, mod: str):
        if mod not in seen_cross:
            seen_cross.add(mod)
            cross_issues.append((lineno, mod))

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod = alias.name
                parts = tuple(mod.split('.'))
                if parts[0] == 'demiflow':
                    bad = _check_demiflow_module_public(mod)
                    if bad:
                        record_sdk(node.lineno, bad)
                elif check_cross_project and not _is_local_to_project(root, parts, prefixes):
                    if _is_cross_project_sibling(root, parts):
                        record_cross(node.lineno, mod)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mod = node.module
            parts = tuple(mod.split('.'))
            names = tuple(a.name for a in node.names)
            if parts[0] == 'demiflow':
                bad = _check_demiflow_module_public(mod, names)
                if bad:
                    record_sdk(node.lineno, bad)
                elif mod == 'demiflow':
                    for sym in names:
                        if sym in _INTERNAL_DEMIFLOW_SUBPACKAGES or sym.startswith('_'):
                            record_sdk(node.lineno, f'demiflow.{sym}')
            elif check_cross_project and not _is_local_to_project(root, parts, prefixes):
                if _is_cross_project_sibling(root, parts):
                    record_cross(node.lineno, mod)
    return sdk_issues, cross_issues


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
                          exclude=(), structural_only: bool = False):
    """Audit DF-02 structural & protocol cleanliness without mutating or inspecting business data.

    When ``structural_only=True`` (used by incremental post-turn hooks), skips corpus-dependent
    orphan checks on operators/configs/prompts so partial ``checked_sources`` never causes false
    positives, while still enforcing all directory, manifest, config protocol, SDK and secret rules.
    """
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

    corpus = ''
    if not structural_only:
        # Build bounded text corpus from active checked sources, bound parameters, and
        # declared operators/data_tools so prompts/configs used on alternate branches are
        # never falsely flagged as unused.
        corpus_sources = set(active_sources)
        for rel in _iter_tree_files(root, 'operators', exclude=exclude):
            if rel.endswith('.py') and Path(rel).name != '__init__.py':
                if rel in checked_sources or _has_declared_export(root / rel, '__demiflow_operators__'):
                    corpus_sources.add(rel)
        for rel in _iter_tree_files(root, 'data_tools', exclude=exclude):
            if rel.endswith('.py') and Path(rel).name != '__init__.py':
                corpus_sources.add(rel)
        corpus_chunks = []
        corpus_budget = 4 * 1024 * 1024
        for rel in sorted(corpus_sources):
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
            elif suffix in _ARCHIVE_SUFFIXES:
                add('root_archive', name, '根目录存在压缩归档包；废弃代码或旧配置请直接清理，不要在项目内保留源码/配置打包归档。')
        elif item.is_dir():
            if name == 'tables':
                add('legacy_tables', 'tables', 'tables/ 为历史兼容目录，输入表路径请直接写在 configs/*.json 中（如 {"uri": "data/in.lance", "version": 1}），迁移后请清理 tables/ 目录。')
            elif name != 'parameters' and name not in ALLOWED_ROOT_DIRS:
                add('root_dir', name, f'根目录存在非规范顶层目录 {name}/；按 DF-02 规范请按职责迁入标准目录（flows/、operators/、prompts/、configs/、data_tools/、notebooks/、data/、logs/、docs/、tests/）或清理废弃目录。')

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
            add('notebook_tool_in_operators', rel, 'operators/ 模块导入了 Notebook 交互库（IPython/ipywidgets），属于 Notebook 查看/分析/标注工具；按 DF-02 规范请迁入 data_tools/。')
            continue
        if not structural_only and rel not in checked_sources and not _has_declared_export(root / rel, '__demiflow_operators__'):
            add('orphan_operators', rel, 'operators/ 模块未被已登记 Flow 使用且未声明 __demiflow_operators__；若为 Notebook 查看/分析/标注工具请迁入 data_tools/，若为废弃代码请清理。')

    # 5. data_tools/*.py automatically exposes top-level public callables as 'tool';
    # explicit __demiflow_data_tools__ is optional (used for finer role tags).

    # 5b. Public DemiFlow SDK & project self-containment boundary across code dirs.
    for code_dir in ('flows', 'operators', 'data_tools', 'tests'):
        check_cross = code_dir != 'tests'
        for rel in _iter_tree_files(root, code_dir, exclude=exclude):
            p = Path(rel)
            if p.suffix != '.py' or p.name == '__init__.py':
                continue
            sdk_bad, cross_bad = _boundary_import_issues(root, root / rel, check_cross_project=check_cross)
            if sdk_bad:
                first_line, first_mod = sdk_bad[0]
                extra = f' 等 {len(sdk_bad)} 处' if len(sdk_bad) > 1 else ''
                inspected.add(rel)
                by_category.setdefault('internal_sdk_import', []).append(
                    diagnostic('DF-02', rel,
                               f'导入了非公开或已移除的 DemiFlow 内部模块 {first_mod}{extra}；业务项目仅允许依赖 DemiFlow 公开 SDK（运行 demiflow forge context --root . --topic sdk 查看当前公开接口）。',
                               line=first_line)
                )
            if cross_bad:
                first_line, first_mod = cross_bad[0]
                extra = f' 等 {len(cross_bad)} 处' if len(cross_bad) > 1 else ''
                inspected.add(rel)
                by_category.setdefault('cross_project_import', []).append(
                    diagnostic('DF-02', rel,
                               f'跨项目导入了外部业务源码 {first_mod}{extra}；按 Forge 独立项目规范，项目源码须自包含，仅依赖标准库、已安装三方包、DemiFlow 公开 SDK 及本项目自身模块，请将所需纯计算/辅助逻辑迁入本项目并解耦外部源码依赖。',
                               line=first_line)
                )

    # 5c. Dangling project-local imports in tests/*.py (catches broken tests after cleanup/moves).
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
    if not structural_only:
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
    if not structural_only:
        unref_prompts = []
        for rel in prompt_files:
            sub_rel = rel[len('prompts/'):]
            if rel in active_sources or rel in corpus or sub_rel in corpus:
                continue
            unref_prompts.append(rel)
        for target in _group_unreferenced_under('prompts', prompt_files, unref_prompts):
            add('orphan_prompts', target, 'prompts/ 中存在未被任何活跃 Flow、算子或配置引用的废弃 Prompt；请清理以保持目录整洁。')

    # 8. docs/ is for documentation and verification evidence, never a dumping ground or archive for obsolete code/configs/prompts.
    docs_files = _iter_tree_files(root, 'docs', exclude=exclude)
    docs_bad_targets: set[str] = set()
    for rel in docs_files:
        p = Path(rel)
        sub_parts = p.parts[1:]  # relative inside docs/
        dir_parts = sub_parts[:-1]
        is_bad = (
            p.suffix.lower() in _DOCS_DISALLOWED_SUFFIXES
            or any(part in _DOCS_DISALLOWED_SUBDIRS for part in dir_parts)
        )
        if is_bad:
            # Collapse offending archives/dumped subtrees to their top-level docs/<subdir> when nested.
            docs_bad_targets.add(f'docs/{sub_parts[0]}' if len(sub_parts) >= 2 else rel)
    docs_root = root / 'docs'
    if docs_root.is_dir() and not docs_root.is_symlink():
        for child in sorted(docs_root.iterdir(), key=lambda x: x.name):
            if child.is_dir() and not child.is_symlink() and not child.name.startswith('.'):
                if child.name in _DOCS_DISALLOWED_SUBDIRS:
                    docs_bad_targets.add(f'docs/{child.name}')
                else:
                    try:
                        for sub in child.iterdir():
                            if sub.is_dir() and not sub.is_symlink() and sub.name in _DOCS_DISALLOWED_SUBDIRS:
                                docs_bad_targets.add(f'docs/{child.name}')
                                break
                    except OSError:
                        pass
    for target in sorted(docs_bad_targets):
        add('code_in_docs', target,
            'docs/ 仅用于项目文档与验收证据，不应存放废弃代码、压缩归档包（.tar.gz/.zip）或迁入的旧 configs/parameters/prompts/flows/operators 目录；废弃的非数据文件请直接清理（使用 manage_project_layout remove_path），切勿打包或转存到 docs/。')

    # 9. Active configs/*.json output paths & simplified flat URI protocol.
    from .project_bindings import extract_arguments

    def _has_legacy_param_indirection(obj, depth=0):
        if depth > 16:
            return False
        if isinstance(obj, dict):
            kind = obj.get('kind')
            if kind == 'table' and 'alias' in obj:
                return True
            if kind == 'output' and ('location_id' in obj or 'relative_uri' in obj):
                return True
            if kind == 'location' and 'location_id' in obj:
                return True
            return any(_has_legacy_param_indirection(v, depth + 1) for v in obj.values())
        if isinstance(obj, list):
            return any(_has_legacy_param_indirection(v, depth + 1) for v in obj)
        return False

    for param_rel in sorted(bound_params):
        if not param_rel.startswith('configs/'):
            continue
        fp = root / param_rel
        if not fp.is_file() or fp.is_symlink():
            continue
        try:
            doc = json.loads(read_bounded(fp, 65536).decode('utf-8'))
            args, param_fmt = extract_arguments(doc)
        except Exception:
            continue
        if param_fmt != 'flat' or _has_legacy_param_indirection(args):
            add('legacy_param_format', param_rel,
                '参数文件仍使用历史包装格式（format/arguments）或表别名/location_id 拆分；按 DF-02 规范请改为扁平 JSON，输入表直接写 {"uri": "...", "version": 1}（version 可选），输出表直接写 {"kind": "output", "uri": "data/out.lance"}。')
        if not isinstance(args, dict):
            continue
        for key, value in args.items():
            if key in _OUTPUT_ARG_KEYS and _points_to_runs(root, value):
                add('legacy_output_dir', param_rel,
                    f'活跃参数 {key} 仍指向历史 runs/ 目录；按 DF-02 规范默认产物应输出到 data/ 目录（历史 runs/ 仅作只读保留）。')
                break

    # 9b. Legacy bloated workspace.json (internal Forge state should live in .forge/state.json).
    ws_file = root / 'workspace.json'
    if ws_file.is_file() and not ws_file.is_symlink():
        try:
            from .project import is_slim_manifest, _referenced_location_ids
            raw_ws = json.loads(read_bounded(ws_file, 1024 * 1024).decode('utf-8'))
            used_locs = _referenced_location_ids(root, control)
            extra_locs = set((raw_ws.get('locations') or {}).keys()) - used_locs if isinstance(raw_ws, dict) else set()
            if not is_slim_manifest(raw_ws) or extra_locs:
                add('legacy_manifest', 'workspace.json',
                    'workspace.json 仍包含平台内部状态（format/id/platform/audit）或冗余 locations；按 DF-02 规范请先完成参数直连 uri 迁移并清理 tables/，再调用 manage_project_layout(action="slim_manifest") 将内部态收进 .forge/state.json 并瘦身 workspace.json。')
        except Exception:
            pass

    # 10. Project-owned runtime env template (.env.example) & plaintext secret guard.
    from .project_env import (
        declared_prompt_env_names,
        find_hardcoded_secret_line,
        is_placeholder_or_empty,
        load_project_env,
    )
    declared_envs = declared_prompt_env_names(root, exclude=exclude)
    env_example = root / '.env.example'
    if declared_envs:
        if not env_example.exists() and not env_example.is_symlink():
            names_text = ', '.join(sorted(declared_envs))
            add('env_example_missing', '.env.example',
                f'项目 Prompt 声明了运行环境变量 {names_text}，但项目根目录缺少 .env.example 模板；请创建 .env.example（仅列变量名、值为空；沙箱内可使用 manage_project_layout sync_env_example）以便独立运行与工作台按规范加载 .env。')
        else:
            try:
                example_entries = load_project_env(root, filename='.env.example', allow_empty=True)
            except Exception:
                example_entries = {}
            missing_envs = sorted(declared_envs - set(example_entries))
            if missing_envs:
                add('env_example_incomplete', '.env.example',
                    f'.env.example 缺少项目 Prompt 声明的运行环境变量：{", ".join(missing_envs)}；请补充空值模板项（沙箱内可使用 manage_project_layout sync_env_example）。')
            non_empty_secrets = [k for k, v in example_entries.items() if not is_placeholder_or_empty(v)]
            if non_empty_secrets:
                add('hardcoded_secret', '.env.example',
                    f'.env.example 为入库模板文件，变量 {", ".join(sorted(non_empty_secrets))} 不应填写实际密钥值；请置空并仅在本地 .env（不入 Git）中填写。')
    elif env_example.is_file() and not env_example.is_symlink():
        try:
            example_entries = load_project_env(root, filename='.env.example', allow_empty=True)
        except Exception:
            example_entries = {}
        non_empty_secrets = [k for k, v in example_entries.items() if not is_placeholder_or_empty(v)]
        if non_empty_secrets:
            add('hardcoded_secret', '.env.example',
                f'.env.example 为入库模板文件，变量 {", ".join(sorted(non_empty_secrets))} 不应填写实际密钥值；请置空并仅在本地 .env（不入 Git）中填写。')

    for scan_rel in (*config_files, *prompt_files):
        fp = root / scan_rel
        if not fp.is_file() or fp.is_symlink():
            continue
        try:
            txt = read_bounded(fp, 256 * 1024).decode('utf-8', errors='ignore')
        except Exception:
            continue
        secret_line = find_hardcoded_secret_line(txt)
        if secret_line is not None:
            inspected.add(scan_rel)
            by_category.setdefault('hardcoded_secret', []).append(
                diagnostic('DF-02', scan_rel,
                           '检测到疑似硬编码明文密钥；严禁将密钥写入配置或 Prompt 文件，请改为 api_key_env 声明并在项目本地 .env 中配置。',
                           line=secret_line)
            )

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
