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


ROLE_DIRS = ('flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'workflows')
ALLOWED_ROOT_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'workflows',
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


_BOUNDARY_IMPORT_DIRS = frozenset(('flows', 'operators', 'data_tools', 'tests'))


def _boundary_import_diagnostics(root: Path, rel: str, *, check_cross_project: bool = True) -> tuple[dict | None, dict | None]:
    sdk_bad, cross_bad = _boundary_import_issues(root, root / rel, check_cross_project=check_cross_project)
    sdk_diag = cross_diag = None
    if sdk_bad:
        first_line, first_mod = sdk_bad[0]
        extra = f' 等 {len(sdk_bad)} 处' if len(sdk_bad) > 1 else ''
        sdk_diag = diagnostic(
            'DF-02', rel,
            f'导入了非公开或已移除的 DemiFlow 内部模块 {first_mod}{extra}；业务项目仅允许依赖 DemiFlow 公开 SDK（运行 demiflow forge context --root . --topic sdk 查看当前公开接口，或用 --api <模块>:<符号> 查看目标接口完整契约与返回值说明）。',
            line=first_line,
        )
    if cross_bad:
        first_line, first_mod = cross_bad[0]
        extra = f' 等 {len(cross_bad)} 处' if len(cross_bad) > 1 else ''
        cross_diag = diagnostic(
            'DF-02', rel,
            f'跨项目导入了外部业务源码 {first_mod}{extra}；按 Forge 独立项目规范，项目源码须自包含，仅依赖标准库、已安装三方包、DemiFlow 公开 SDK 及本项目自身模块，请将所需纯计算/辅助逻辑迁入本项目并解耦外部源码依赖。',
            line=first_line,
        )
    return sdk_diag, cross_diag


def check_source_import_boundaries(root: Path, rel_paths) -> tuple[set[str], list[dict]]:
    """Verify public-SDK and project self-containment import boundaries on specific traced source files."""
    inspected: set[str] = set()
    issues: list[dict] = []
    for rel in sorted(set(rel_paths)):
        p = Path(rel)
        if not p.parts or p.parts[0] not in _BOUNDARY_IMPORT_DIRS:
            continue
        if p.suffix != '.py' or p.name == '__init__.py':
            continue
        if not (root / rel).is_file():
            continue
        sdk_diag, cross_diag = _boundary_import_diagnostics(root, rel, check_cross_project=(p.parts[0] != 'tests'))
        if sdk_diag is not None:
            inspected.add(rel)
            issues.append(sdk_diag)
        if cross_diag is not None:
            inspected.add(rel)
            issues.append(cross_diag)
    return inspected, issues


def _operator_import_targets(root: Path, tree: ast.AST, *, in_operators: bool) -> list[tuple[int, str, bool]]:
    """Return [(lineno, op_stem, is_private)] for every import of an operators/<stem>.py module (including nested)."""
    prefixes = _project_import_prefixes(root)
    hits: list[tuple[int, str, bool]] = []
    seen: set[tuple[int, str]] = set()

    def record(lineno: int, stem: str):
        if not stem or stem == '*' or stem == '__init__':
            return
        key = (lineno, stem)
        if key not in seen:
            seen.add(key)
            hits.append((lineno, stem, stem.startswith('_')))

    def extract_after_operators(parts: tuple[str, ...]) -> tuple[str, ...] | None:
        if not parts:
            return None
        if parts[0] == 'operators':
            return parts[1:]
        for pref in prefixes:
            if len(parts) > len(pref) and parts[:len(pref)] == pref and parts[len(pref)] == 'operators':
                return parts[len(pref) + 1:]
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ''
            if node.level >= 1 and in_operators:
                if not mod:
                    for alias in node.names:
                        record(node.lineno, alias.name)
                else:
                    record(node.lineno, mod.split('.')[0])
            elif node.level == 0 and mod:
                after = extract_after_operators(tuple(mod.split('.')))
                if after is None:
                    continue
                if len(after) == 0:
                    for alias in node.names:
                        record(node.lineno, alias.name)
                else:
                    record(node.lineno, after[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                after = extract_after_operators(tuple(alias.name.split('.')))
                if after and len(after) >= 1:
                    record(node.lineno, after[0])
    return hits


def _thin_operator_wrapper_issues(root: Path, path: Path) -> list[tuple[int, str, str]]:
    """OP-O1: Detect zero-logic shim operators in operators/*.py that only forward or re-export another operator module."""
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    prefixes = _project_import_prefixes(root)

    def extract_after_operators(parts: tuple[str, ...]) -> tuple[str, ...] | None:
        if not parts:
            return None
        if parts[0] == 'operators':
            return parts[1:]
        for pref in prefixes:
            if len(parts) > len(pref) and parts[:len(pref)] == pref and parts[len(pref)] == 'operators':
                return parts[len(pref) + 1:]
        return None

    op_modules: set[str] = set()
    op_symbols: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ''
            if node.level >= 1:
                for alias in node.names:
                    if alias.name == '*':
                        continue
                    local = alias.asname or alias.name
                    if not mod:
                        op_modules.add(local)
                    else:
                        op_symbols[local] = f'{mod}.{alias.name}'
            elif mod:
                after = extract_after_operators(tuple(mod.split('.')))
                if after is None:
                    continue
                for alias in node.names:
                    if alias.name == '*':
                        continue
                    local = alias.asname or alias.name
                    if len(after) == 0:
                        op_modules.add(local)
                    else:
                        op_symbols[local] = f'{after[0]}.{alias.name}'
        elif isinstance(node, ast.Import):
            for alias in node.names:
                after = extract_after_operators(tuple(alias.name.split('.')))
                if after and len(after) >= 1 and alias.asname:
                    op_modules.add(alias.asname)
    if not op_modules and not op_symbols:
        return []

    def _forwarded_target(expr) -> str | None:
        if isinstance(expr, ast.Attribute) and isinstance(expr.value, ast.Name) and expr.value.id in op_modules:
            return f'{expr.value.id}.{expr.attr}'
        if isinstance(expr, ast.Name) and expr.id in op_symbols:
            return op_symbols[expr.id]
        return None

    issues: list[tuple[int, str, str]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_'):
            stmts = list(node.body)
            if (stmts and isinstance(stmts[0], ast.Expr)
                    and isinstance(stmts[0].value, ast.Constant)
                    and isinstance(stmts[0].value.value, str)):
                stmts = stmts[1:]
            if len(stmts) == 1 and isinstance(stmts[0], ast.Return) and isinstance(stmts[0].value, ast.Call):
                target = _forwarded_target(stmts[0].value.func)
                if target:
                    issues.append((node.lineno, node.name, target))
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            lhs, rhs = node.targets[0], node.value
            if isinstance(lhs, ast.Name):
                pairs = [(lhs, rhs)]
            elif (isinstance(lhs, (ast.Tuple, ast.List))
                  and isinstance(rhs, (ast.Tuple, ast.List))
                  and len(lhs.elts) == len(rhs.elts)):
                pairs = list(zip(lhs.elts, rhs.elts))
            else:
                pairs = []
            for target_node, val_node in pairs:
                if not isinstance(target_node, ast.Name):
                    continue
                name = target_node.id
                if name.startswith('_') or name.isupper():
                    continue
                target = _forwarded_target(val_node)
                if target:
                    issues.append((node.lineno, name, target))
    return issues


def _cross_operator_module_imports(root: Path, path: Path) -> list[tuple[int, str]]:
    """OP-L1: Detect public operator modules importing another public operator module (including nested imports)."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    return [
        (lineno, stem)
        for lineno, stem, is_priv in _operator_import_targets(root, tree, in_operators=True)
        if not is_priv and stem != path.stem
    ]


def _private_operator_cross_imports(root: Path, path: Path) -> list[tuple[int, str]]:
    """OP-L2: Private utility modules (operators/_*.py) must be pure leaf utilities without importing other operators/."""
    if not path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    return [
        (lineno, stem)
        for lineno, stem, _ in _operator_import_targets(root, tree, in_operators=True)
        if stem != path.stem
    ]


def _flow_private_operator_imports(root: Path, path: Path) -> list[tuple[int, str]]:
    """OP-L2: Flows may only reference public operator modules, never private operators/_*.py utilities."""
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    return [
        (lineno, stem)
        for lineno, stem, is_priv in _operator_import_targets(root, tree, in_operators=False)
        if is_priv
    ]


def _top_level_callables(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return set()
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }


def _declared_string_list(tree: ast.Module, var_name: str) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    if isinstance(node.value, (ast.List, ast.Tuple, ast.Set)):
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                names.add(elt.value)
    return names


def _private_operator_reexport_issues(
    root: Path, path: Path, private_callables_cache: dict[str, set[str]]
) -> list[tuple[int, str, str]]:
    """OP-O2: Detect public operator modules re-exporting functions/classes imported from operators/_*.py."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    prefixes = _project_import_prefixes(root)

    def extract_private_stem(node: ast.ImportFrom) -> str | None:
        mod = node.module or ''
        if node.level >= 1 and mod:
            head = mod.split('.')[0]
            return head if head.startswith('_') else None
        if node.level == 0 and mod:
            parts = tuple(mod.split('.'))
            after = None
            if parts[0] == 'operators':
                after = parts[1:]
            else:
                for pref in prefixes:
                    if len(parts) > len(pref) and parts[:len(pref)] == pref and parts[len(pref)] == 'operators':
                        after = parts[len(pref) + 1:]
                        break
            if after and len(after) >= 1 and after[0].startswith('_'):
                return after[0]
        return None

    imported_callables: list[tuple[int, str, str]] = []
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        stem = extract_private_stem(node)
        if not stem:
            continue
        if stem not in private_callables_cache:
            priv_file = root / 'operators' / f'{stem}.py'
            private_callables_cache[stem] = _top_level_callables(priv_file) if priv_file.is_file() else set()
        priv_callables = private_callables_cache[stem]
        for alias in node.names:
            if alias.name == '*':
                continue
            local = alias.asname or alias.name
            if alias.name in priv_callables and not local.startswith('_'):
                imported_callables.append((node.lineno, local, f'{stem}.{alias.name}'))

    if not imported_callables:
        return []

    exported_names = _declared_string_list(tree, '__demiflow_operators__') | _declared_string_list(tree, '__all__')
    used_internally: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        # Exclude __demiflow_operators__ / __all__ assignments from counting as internal use.
        if isinstance(node, ast.Assign) and all(
            isinstance(t, ast.Name) and t.id in ('__demiflow_operators__', '__all__')
            for t in node.targets
        ):
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                used_internally.add(child.id)

    issues: list[tuple[int, str, str]] = []
    for lineno, local, target in imported_callables:
        if local in exported_names or local not in used_internally:
            issues.append((lineno, local, target))
    return issues


def _missing_operator_description_issues(path: Path) -> list[tuple[int, str]]:
    """OP-S1: Detect public operator functions/classes in operators/*.py lacking a semantic first-line docstring."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    issues: list[tuple[int, str]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not node.name.startswith('_'):
            doc = ast.get_docstring(node)
            if not doc or not doc.strip():
                issues.append((node.lineno, node.name))
    return issues


_IMPLICIT_ROW_PARAMS = frozenset({'self', 'cls', 'row', 'rows', 'batch', 'item', 'items', 'record', 'records'})
_ARG_SECTION_HEADERS = frozenset({'args', 'arguments', 'params', 'parameters', '参数', '入参'})
_EXAMPLE_SECTION_HEADERS = frozenset({'example', 'examples', '示例', '样例'})
_OTHER_SECTION_HEADERS = frozenset({'returns', 'return', 'yields', 'raises', 'examples', 'example', 'notes', 'note', 'attributes', '返回', '异常', '示例', '样例', '说明'})
_EXAMPLE_INPUT_RE = re.compile(r'^(?:[-*•]\s+)?(?:输入|入|前|input|before)\s*[:：]\s*(.*)$', re.IGNORECASE)
_EXAMPLE_OUTPUT_RE = re.compile(r'^(?:[-*•]\s+)?(?:输出|出|后|output|after)\s*[:：]\s*(.*)$', re.IGNORECASE)
_EXAMPLE_ARROW_RE = re.compile(r'^(.+?)\s*(?:->|→|=>)\s*(.+)$')


def parse_docstring_example(doc: str | None) -> dict[str, str] | None:
    """Extract a minimal before/after example ({input, output}) from an Example:/示例: docstring section."""
    if not doc or not doc.strip():
        return None
    lines = doc.splitlines()
    in_example = False
    section_indent = 0
    block: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_example:
                block.append('')
            continue
        indent = len(line) - len(line.lstrip())
        header_match = re.match(r'^([A-Za-z\u4e00-\u9fff]+)\s*[:：]\s*$', stripped)
        if header_match:
            key = header_match.group(1).lower()
            if key in _EXAMPLE_SECTION_HEADERS:
                in_example = True
                section_indent = indent
                continue
            if key in _ARG_SECTION_HEADERS or key in _OTHER_SECTION_HEADERS:
                if in_example:
                    break
                continue
        if in_example:
            if indent <= section_indent and not stripped.startswith(('-', '*', '•')):
                break
            block.append(line)
    if not block:
        return None

    # 1. Explicit 输入/Input and 输出/Output markers (supports multi-line values).
    in_parts: list[str] = []
    out_parts: list[str] = []
    current: str | None = None
    for raw in block:
        stripped = raw.strip()
        if not stripped:
            continue
        m_in = _EXAMPLE_INPUT_RE.match(stripped)
        if m_in:
            current = 'in'
            if m_in.group(1).strip():
                in_parts.append(m_in.group(1).strip())
            continue
        m_out = _EXAMPLE_OUTPUT_RE.match(stripped)
        if m_out:
            current = 'out'
            if m_out.group(1).strip():
                out_parts.append(m_out.group(1).strip())
            continue
        if current == 'in':
            in_parts.append(stripped)
        elif current == 'out':
            out_parts.append(stripped)
    if in_parts and out_parts:
        return {'input': '\n'.join(in_parts), 'output': '\n'.join(out_parts)}

    # 2. Fallback: compact single-line `input -> output` or `input → output`.
    for raw in block:
        stripped = raw.strip().lstrip('-*• ').strip()
        if not stripped:
            continue
        m_arrow = _EXAMPLE_ARROW_RE.match(stripped)
        if m_arrow and m_arrow.group(1).strip() and m_arrow.group(2).strip():
            return {'input': m_arrow.group(1).strip(), 'output': m_arrow.group(2).strip()}
    return None


def _str_const(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value:
        return node.value
    return None


def extract_field_flow(node: ast.AST, *, max_fields: int = 24, include_analysis: bool = False) -> dict | None:
    """Passively extract read/written row field names from an operator function or callable Actor AST."""
    entry = node
    if isinstance(node, ast.ClassDef):
        entry = next((n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == '__call__'), None)
    if not isinstance(entry, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return None

    pos_args = [*entry.args.posonlyargs, *entry.args.args]
    if pos_args and pos_args[0].arg in {'self', 'cls'}:
        pos_args = pos_args[1:]
    if not pos_args:
        return None
    row_param = pos_args[0].arg
    row_vars = {row_param}
    out_vars: set[str] = set()
    passes_through = False
    has_unknown_write = False
    reads: list[str] = []
    writes: list[str] = []

    def add_read(key: str | None):
        if key and key not in reads and len(reads) < max_fields:
            reads.append(key)

    def add_write(key: str | None):
        if key and key not in writes and len(writes) < max_fields:
            writes.append(key)

    def is_row_expr(expr: ast.AST) -> bool:
        return isinstance(expr, ast.Name) and expr.id in row_vars

    def collect_dict_keys(expr: ast.AST):
        nonlocal passes_through, has_unknown_write
        if isinstance(expr, ast.Dict):
            for k, v in zip(expr.keys, expr.values):
                if k is None:
                    if is_row_expr(v):
                        passes_through = True
                    else:
                        has_unknown_write = True
                else:
                    k_str = _str_const(k)
                    if k_str:
                        add_write(k_str)
                    else:
                        has_unknown_write = True
        elif isinstance(expr, ast.DictComp):
            # Support `{k: row[k] for k in ('a', 'b')}` statically; mark dynamic schema comprehensions as unknown.
            if (len(expr.generators) == 1
                    and isinstance(expr.generators[0].target, ast.Name)
                    and isinstance(expr.key, ast.Name)
                    and expr.key.id == expr.generators[0].target.id
                    and isinstance(expr.generators[0].iter, (ast.Tuple, ast.List, ast.Set))
                    and all(_str_const(e) is not None for e in expr.generators[0].iter.elts)):
                for elt in expr.generators[0].iter.elts:
                    k_str = _str_const(elt)
                    add_write(k_str)
                    if any(isinstance(n, ast.Name) and n.id in row_vars for n in ast.walk(expr.value)):
                        add_read(k_str)
            else:
                has_unknown_write = True
                if any(isinstance(n, ast.Name) and n.id in row_vars for n in ast.walk(expr.value)):
                    passes_through = True
        elif isinstance(expr, (ast.List, ast.Tuple)):
            for elt in expr.elts:
                collect_dict_keys(elt)
        elif isinstance(expr, ast.Call):
            has_unknown_write = True

    # First pass: discover loop aliases of row/batch and output dict containers.
    for sub in ast.walk(entry):
        if isinstance(sub, (ast.For, ast.AsyncFor)):
            if isinstance(sub.target, ast.Name) and is_row_expr(sub.iter):
                row_vars.add(sub.target.id)
        elif isinstance(sub, ast.Assign) and len(sub.targets) == 1 and isinstance(sub.targets[0], ast.Name):
            tgt = sub.targets[0].id
            val = sub.value
            if isinstance(val, ast.Call):
                if isinstance(val.func, ast.Name) and val.func.id == 'dict' and val.args and is_row_expr(val.args[0]):
                    out_vars.add(tgt)
                    passes_through = True
                elif isinstance(val.func, ast.Attribute) and val.func.attr == 'copy' and is_row_expr(val.func.value):
                    out_vars.add(tgt)
                    passes_through = True
            elif isinstance(val, ast.Dict):
                if any(k is None and is_row_expr(v) for k, v in zip(val.keys, val.values)):
                    out_vars.add(tgt)
                    passes_through = True
        elif isinstance(sub, (ast.Return, ast.Yield)) and isinstance(sub.value, ast.Name):
            if sub.value.id in row_vars:
                passes_through = True
            else:
                out_vars.add(sub.value.id)

    # Second pass: collect read keys, written keys, and detect unknown external dict builders.
    for sub in ast.walk(entry):
        if isinstance(sub, ast.Subscript) and isinstance(sub.value, ast.Name):
            key = _str_const(sub.slice)
            if isinstance(sub.ctx, ast.Load) and sub.value.id in row_vars:
                add_read(key)
            elif isinstance(sub.ctx, ast.Store) and (sub.value.id in out_vars or sub.value.id in row_vars):
                if key:
                    add_write(key)
                else:
                    has_unknown_write = True
        elif isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and isinstance(sub.func.value, ast.Name):
            recv = sub.func.value.id
            attr = sub.func.attr
            if recv in row_vars and attr in {'get', 'pop'} and sub.args:
                add_read(_str_const(sub.args[0]))
            elif (recv in out_vars or recv in row_vars) and attr == 'update':
                if sub.args and isinstance(sub.args[0], ast.Dict):
                    collect_dict_keys(sub.args[0])
                elif sub.args:
                    has_unknown_write = True
                for kw in sub.keywords:
                    if kw.arg:
                        add_write(kw.arg)
            elif attr == 'append' and sub.args:
                if isinstance(sub.args[0], ast.Dict):
                    collect_dict_keys(sub.args[0])
                elif isinstance(sub.args[0], ast.Call):
                    has_unknown_write = True
        elif isinstance(sub, ast.Compare) and len(sub.ops) == 1 and isinstance(sub.ops[0], (ast.In, ast.NotIn)):
            if len(sub.comparators) == 1 and is_row_expr(sub.comparators[0]):
                add_read(_str_const(sub.left))
        elif isinstance(sub, (ast.Return, ast.Yield)) and sub.value is not None:
            collect_dict_keys(sub.value)
        elif isinstance(sub, ast.Assign) and len(sub.targets) == 1 and isinstance(sub.targets[0], ast.Name):
            tgt = sub.targets[0].id
            if tgt in out_vars:
                if isinstance(sub.value, (ast.Dict, ast.DictComp)):
                    collect_dict_keys(sub.value)
                elif isinstance(sub.value, ast.Call):
                    is_shallow_copy = (
                        (isinstance(sub.value.func, ast.Name) and sub.value.func.id == 'dict' and sub.value.args and is_row_expr(sub.value.args[0]))
                        or (isinstance(sub.value.func, ast.Attribute) and sub.value.func.attr == 'copy' and is_row_expr(sub.value.func.value))
                    )
                    if not is_shallow_copy:
                        has_unknown_write = True

    if not reads and not writes:
        return None
    result = dict(reads=reads, writes=writes, passes_through=passes_through)
    if include_analysis:
        result['has_unknown_write'] = has_unknown_write
    return result


def _extract_example_dict_keys(text: str) -> set[str] | None:
    """Extract top-level string keys if an example snippet represents a dict or list of dicts."""
    if not text or '{' not in text:
        return None
    # Find the first balanced `{...}` block (handles `row={...}` or `[{...}]`).
    start = text.find('{')
    depth = 0
    in_str: str | None = None
    escaped = False
    end = -1
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == in_str:
                in_str = None
            continue
        if ch in ('"', "'"):
            in_str = ch
        elif ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end = i
                break
    if end <= start:
        return None
    snippet = text[start:end + 1]
    # Remove trailing `, ...` or `...` placeholder inside dicts before parsing.
    cleaned = re.sub(r',\s*\.\.\.\s*(?=\})', '', snippet)
    cleaned = re.sub(r'\{\s*\.\.\.\s*\}', '{}', cleaned)
    try:
        parsed = ast.literal_eval(cleaned)
        if isinstance(parsed, dict) and all(isinstance(k, str) for k in parsed):
            return set(parsed.keys())
    except Exception:
        pass
    try:
        parsed_json = json.loads(cleaned)
        if isinstance(parsed_json, dict):
            return {k for k in parsed_json if isinstance(k, str)}
    except Exception:
        pass
    return None


def _stale_operator_example_docs(path: Path) -> list[tuple[int, str, str]]:
    """OP-E1 drift guard: detect Example:/示例: outputs that contradict AST-extracted written fields."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    issues: list[tuple[int, str, str]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_'):
            doc = ast.get_docstring(node) or ''
            example = parse_docstring_example(doc)
        elif isinstance(node, ast.ClassDef) and not node.name.startswith('_'):
            call_fn = next(
                (m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__call__'),
                None,
            )
            if call_fn is None:
                continue
            example = parse_docstring_example(ast.get_docstring(node) or '') or parse_docstring_example(ast.get_docstring(call_fn) or '')
        else:
            continue
        if not example:
            continue
        flow = extract_field_flow(node, include_analysis=True)
        if not flow or not flow['writes'] or flow.get('has_unknown_write'):
            continue
        out_keys = _extract_example_dict_keys(example['output'])
        if not out_keys:
            continue
        in_keys = _extract_example_dict_keys(example['input']) or set()
        writes = set(flow['writes'])
        reads = set(flow['reads'])
        allowed = writes | (reads | in_keys if flow['passes_through'] else set())
        ghost = sorted(out_keys - allowed)
        if ghost:
            g_text = ', '.join(ghost[:3]) + ('…' if len(ghost) > 3 else '')
            issues.append((node.lineno, node.name, f'示例输出包含代码未产出的过期字段 {g_text}'))
        elif not (out_keys & writes):
            w_text = ', '.join(flow['writes'][:3]) + ('…' if len(flow['writes']) > 3 else '')
            issues.append((node.lineno, node.name, f'示例输出字段与代码真实产出字段（{w_text}）完全脱节'))
        elif in_keys and reads and not (in_keys & reads):
            r_text = ', '.join(flow['reads'][:3]) + ('…' if len(flow['reads']) > 3 else '')
            issues.append((node.lineno, node.name, f'示例输入字段与代码真实读取字段（{r_text}）完全脱节'))
    return issues


def _stale_operator_param_docs(path: Path) -> list[tuple[int, str, list[str]]]:
    """OP-P1 drift guard: detect docstring Args:/参数: entries referencing removed/renamed parameters."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    def all_sig_names(args: ast.arguments) -> tuple[set[str], bool]:
        names = {a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
        if args.vararg:
            names.add(args.vararg.arg)
        if args.kwarg:
            names.add(args.kwarg.arg)
        return names, args.kwarg is not None

    issues: list[tuple[int, str, list[str]]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_'):
            doc = ast.get_docstring(node) or ''
            arg_docs = _parse_docstring_arg_docs(doc)
            if not arg_docs:
                continue
            sig_names, has_varkw = all_sig_names(node.args)
            if has_varkw:
                continue
            stale = [k for k in arg_docs if k not in sig_names]
            if stale:
                issues.append((node.lineno, node.name, stale))
        elif isinstance(node, ast.ClassDef) and not node.name.startswith('_'):
            init_fn = next((m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__init__'), None)
            call_fn = next((m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__call__'), None)
            if init_fn is None and call_fn is None:
                continue
            cls_doc = ast.get_docstring(node) or ''
            init_doc = ast.get_docstring(init_fn) if init_fn else ''
            arg_docs = {**_parse_docstring_arg_docs(cls_doc), **_parse_docstring_arg_docs(init_doc)}
            if not arg_docs:
                continue
            sig_names: set[str] = set()
            has_varkw = False
            for fn in (init_fn, call_fn):
                if fn is not None:
                    n, kw = all_sig_names(fn.args)
                    sig_names |= n
                    has_varkw = has_varkw or kw
            if has_varkw:
                continue
            stale = [k for k in arg_docs if k not in sig_names]
            if stale:
                issues.append((node.lineno, node.name, stale))
    return issues


def _missing_operator_example_docs(path: Path) -> list[tuple[int, str]]:
    """OP-E1: Detect public operator functions/callable Actors lacking a minimal input->output Example:/示例: section."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    issues: list[tuple[int, str]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_'):
            doc = ast.get_docstring(node)
            if not doc or not doc.strip():
                continue  # Already flagged by OP-S1 missing_operator_description
            if parse_docstring_example(doc) is None:
                issues.append((node.lineno, node.name))
        elif isinstance(node, ast.ClassDef) and not node.name.startswith('_'):
            call_fn = next(
                (m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__call__'),
                None,
            )
            if call_fn is None:
                continue  # Non-callable helper/dataclass
            cls_doc = ast.get_docstring(node) or ''
            call_doc = ast.get_docstring(call_fn) or ''
            if not cls_doc.strip() and not call_doc.strip():
                continue  # Already flagged by OP-S1
            if parse_docstring_example(cls_doc) is None and parse_docstring_example(call_doc) is None:
                issues.append((node.lineno, node.name))
    return issues


def _parse_docstring_arg_docs(doc: str | None) -> dict[str, str]:
    """Extract {param_name: description} from Google-style (Args:/参数:) or Sphinx (:param name:) docstrings."""
    if not doc or not doc.strip():
        return {}
    found: dict[str, str] = {}
    lines = doc.splitlines()
    # 1. Sphinx :param name: desc
    for line in lines:
        m = re.match(r'^\s*:param(?:\s+[^:]+)?\s+([A-Za-z_]\w*)\s*:\s*(.+?)\s*$', line)
        if m:
            found[m.group(1)] = m.group(2).strip()
    # 2. Google-style Args: / Parameters: / 参数:
    in_args = False
    section_indent = 0
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip())
        header_match = re.match(r'^([A-Za-z\u4e00-\u9fff]+)\s*[:：]\s*$', stripped)
        if header_match:
            key = header_match.group(1).lower()
            if key in _ARG_SECTION_HEADERS:
                in_args = True
                section_indent = indent
                continue
            if key in _OTHER_SECTION_HEADERS:
                in_args = False
                continue
        if in_args:
            if indent <= section_indent and not stripped.startswith(('-', '*', '•')):
                in_args = False
                continue
            item_match = re.match(r'^(?:[-*•]\s+)?\*{0,2}([A-Za-z_]\w*)(?:\s*\([^)]*\))?\s*[:：]\s*(.+?)\s*$', stripped)
            if item_match:
                found[item_match.group(1)] = item_match.group(2).strip()
    return found


def _missing_operator_param_docs(path: Path) -> list[tuple[int, str, list[str]]]:
    """OP-P1: Detect public operators with extra non-row parameters lacking Args:/参数: semantic descriptions."""
    if path.stem.startswith('_'):
        return []
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    def extra_param_names(args: ast.arguments, *, is_method: bool) -> list[str]:
        all_pos = [*args.posonlyargs, *args.args]
        if is_method and all_pos and all_pos[0].arg in ('self', 'cls'):
            all_pos = all_pos[1:]
        # For a plain row function, skip the first positional data-row argument.
        if not is_method and all_pos:
            all_pos = all_pos[1:]
        names = [a.arg for a in all_pos] + [a.arg for a in args.kwonlyargs]
        return [n for n in names if not n.startswith('_') and n not in _IMPLICIT_ROW_PARAMS]

    issues: list[tuple[int, str, list[str]]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_'):
            extra = extra_param_names(node.args, is_method=False)
            if not extra:
                continue
            doc = ast.get_docstring(node)
            if not doc or not doc.strip():
                continue  # Already flagged by OP-S1 missing_operator_description
            arg_docs = _parse_docstring_arg_docs(doc)
            missing = [n for n in extra if not arg_docs.get(n)]
            if missing:
                issues.append((node.lineno, node.name, missing))
        elif isinstance(node, ast.ClassDef) and not node.name.startswith('_'):
            init_fn = next(
                (m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__init__'),
                None,
            )
            if init_fn is None:
                continue
            extra = extra_param_names(init_fn.args, is_method=True)
            if not extra:
                continue
            cls_doc = ast.get_docstring(node) or ''
            init_doc = ast.get_docstring(init_fn) or ''
            if not cls_doc.strip() and not init_doc.strip():
                continue  # Already flagged by OP-S1
            arg_docs = {**_parse_docstring_arg_docs(cls_doc), **_parse_docstring_arg_docs(init_doc)}
            missing = [n for n in extra if not arg_docs.get(n)]
            if missing:
                issues.append((node.lineno, node.name, missing))
    return issues


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
                          exclude=(), structural_only: bool = False, packages_approved=False):
    """Audit DF-02 structural & protocol cleanliness without mutating or inspecting business data.

    When ``structural_only=True`` (used by incremental post-turn hooks), skips corpus-dependent
    orphan checks on operators/configs/prompts so partial ``checked_sources`` never causes false
    positives, while still enforcing all directory, manifest, config protocol, SDK and secret rules.
    """
    root = Path(root).absolute()
    entries = (control or {}).get('entries') or {}
    if not entries:
        # A project without registered entries is still audited for directory, manifest,
        # config protocol and secret rules; only corpus-dependent orphan checks need entries.
        structural_only = True
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
            if name != 'parameters' and name not in ALLOWED_ROOT_DIRS and not (name == 'packages' and packages_approved):
                add('root_dir', name, f'根目录存在非规范顶层目录 {name}/；按 DF-02 规范根目录仅允许标准目录（flows/、operators/、prompts/、configs/、data_tools/、notebooks/、workflows/、data/、logs/、docs/、tests/、runs/），请按职责归位或清理。')

    # 2. Deprecated parameters/ directory: unbound legacy files or empty leftover directory.
    param_dir = root / 'parameters'
    if param_dir.is_dir() and not param_dir.is_symlink():
        param_files = _iter_tree_files(root, 'parameters', exclude=exclude)
        unbound_params = [p for p in param_files if p not in bound_params]
        for rel in unbound_params:
            add('legacy_parameters', rel, '参数文件应统一放在 configs/ 目录；请清理未登记的 parameters/ 文件。')
        if not param_files and not any(p.startswith('parameters/') for p in bound_params):
            add('legacy_parameters', 'parameters', '根目录存在空目录 parameters/；参数文件应统一放在 configs/，请清理该目录。')

    # 3. Orphan unregistered files & private-operator import guard in flows/.
    for rel in _iter_tree_files(root, 'flows', exclude=exclude):
        p = Path(rel)
        if p.name == '__init__.py' or p.suffix == '.pyc':
            continue
        if p.suffix == '.py':
            if rel not in registered_flows:
                add('orphan_flows', rel, 'flows/ 下存在未在 workspace.json 登记的孤儿文件；请登记为正式入口，共享逻辑迁入 operators/，废弃文件请清理。')
            flow_priv = _flow_private_operator_imports(root, root / rel)
            if flow_priv:
                first_line, first_mod = flow_priv[0]
                extra = f' 等 {len(flow_priv)} 处' if len(flow_priv) > 1 else ''
                inspected.add(rel)
                by_category.setdefault('flow_imports_private_operator', []).append(
                    diagnostic('DF-02', rel,
                               f'Flow 直接导入了私有工具模块 operators/{first_mod}.py{extra}；按 DF-02/OP-L2 规范，Flow 仅允许引用公开业务算子模块，若该能力需作为流程节点编排请定义在公开算子模块中。',
                               line=first_line)
                )
        else:
            add('orphan_flows', rel, 'flows/ 仅存放已登记的 Python Flow 入口文件；配置或辅助文件请迁入对应标准目录。')

    # 4. Unreferenced / non-operator / Notebook-UI / MECE rich-operator checks in operators/.
    operator_files = _iter_tree_files(root, 'operators', exclude=exclude)
    public_operator_stems = {
        Path(rel).stem
        for rel in operator_files
        if Path(rel).suffix == '.py' and Path(rel).name != '__init__.py' and not Path(rel).stem.startswith('_')
    }
    private_callables_cache: dict[str, set[str]] = {}
    for rel in operator_files:
        p = Path(rel)
        if p.name == '__init__.py' or p.suffix == '.pyc':
            continue
        if p.suffix != '.py':
            add('orphan_operators', rel, 'operators/ 仅存放 Python 算子模块；查看器模板请随工具迁入 data_tools/，配置请迁入 configs/。')
            continue
        if _imports_notebook_ui(root / rel):
            add('notebook_tool_in_operators', rel, 'operators/ 模块导入了 Notebook 交互库（IPython/ipywidgets），属于 Notebook 查看/分析/标注工具；按 DF-02 规范请迁入 data_tools/。')
            continue
        # OP-L2: Private utility modules (operators/_*.py) — no 1:1 shadow mirrors and pure leaf utilities only.
        if p.stem.startswith('_'):
            shadow_pub = p.stem[1:]
            if shadow_pub in public_operator_stems:
                inspected.add(rel)
                by_category.setdefault('shadow_private_operator_module', []).append(
                    diagnostic('DF-02', rel,
                               f'私有模块 {rel} 与公开模块 operators/{shadow_pub}.py 同名（1:1 影子私仓）；按 DF-02/OP-L2 规范，禁止通过建同名私有镜像绕过模块边界，请按底层通用能力命名纯工具模块（如 _codec.py、_hash.py）并将业务算子保留在公开模块中。',
                               line=1)
                )
            priv_cross = _private_operator_cross_imports(root, root / rel)
            if priv_cross:
                first_line, first_mod = priv_cross[0]
                extra = f' 等 {len(priv_cross)} 处' if len(priv_cross) > 1 else ''
                inspected.add(rel)
                by_category.setdefault('private_operator_cross_import', []).append(
                    diagnostic('DF-02', rel,
                               f'私有工具模块 {rel} 导入了另一算子模块 {first_mod}{extra}；按 DF-02/OP-L2 规范，operators/_*.py 必须为无业务依赖的纯底层叶节点工具，禁止私有模块之间互相导入或依赖公开算子模块。',
                               line=first_line)
                )
        # OP-O1: Zero-logic forwarding shims or assignment re-exports.
        shims = _thin_operator_wrapper_issues(root, root / rel)
        if shims:
            first_line, first_name, first_target = shims[0]
            extra = f' 等 {len(shims)} 处' if len(shims) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('thin_operator_wrapper', []).append(
                diagnostic('DF-02', rel,
                           f'算子 {first_name} 仅为转调或重导出 {first_target} 的无逻辑垫片{extra}；按 DF-02/OP-O1 规范，请在 Flow 中直接引用目标算子并用 fn_kwargs 传参，或将逻辑内联为自包含算子，避免多层转包增加阅读负担。',
                           line=first_line)
            )
        # OP-L1: Public operator modules must not import each other.
        cross_ops = _cross_operator_module_imports(root, root / rel)
        if cross_ops:
            first_line, first_mod = cross_ops[0]
            extra = f' 等 {len(cross_ops)} 处' if len(cross_ops) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('cross_operator_module_import', []).append(
                diagnostic('DF-02', rel,
                           f'公开业务算子模块导入了另一公开算子模块 {first_mod}{extra}；按 DF-02/OP-L1 规范，公开业务算子模块之间禁止互相导入调用（含函数内延迟导入），请保持算子自包含充血，仅共享无业务协议依赖的纯底层叶节点工具 operators/_<util>.py。',
                           line=first_line)
            )
        # OP-O2: Public operator modules must not re-export callables imported from operators/_*.py.
        reexports = _private_operator_reexport_issues(root, root / rel, private_callables_cache)
        if reexports:
            first_line, first_name, first_target = reexports[0]
            extra = f' 等 {len(reexports)} 处' if len(reexports) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('private_operator_reexport', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子模块直接重导出了私有模块函数/类 {first_name}（来自 {first_target}）{extra}；按 DF-02/OP-O2 规范，公开算子必须直接定义在公开模块中，私有模块仅可作为模块内部调用的底层工具函数。',
                           line=first_line)
            )
        # OP-S1: Mandatory first-line semantic docstring on public operators.
        missing_docs = _missing_operator_description_issues(root / rel)
        if missing_docs:
            first_line, first_name = missing_docs[0]
            extra = f' 等 {len(missing_docs)} 处' if len(missing_docs) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('missing_operator_description', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子 {first_name} 缺少首行语义描述 docstring{extra}；按 DF-02/OP-S1 规范，每个公开代码算子（函数/Actor 类）须在首行 docstring 中写清业务语义，供工作台执行图节点直接展示。',
                           line=first_line)
            )
        # OP-P1: Mandatory Args:/参数: semantic descriptions + stale-param drift guard.
        missing_param_docs = _missing_operator_param_docs(root / rel)
        if missing_param_docs:
            first_line, first_name, first_params = missing_param_docs[0]
            p_list = ', '.join(first_params[:3]) + ('…' if len(first_params) > 3 else '')
            extra = f' 等 {len(missing_param_docs)} 个算子' if len(missing_param_docs) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('missing_operator_param_description', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子 {first_name} 的参数 {p_list} 缺少语义说明{extra}；按 DF-02/OP-P1 规范，非数据行参数须在 docstring 的 Args:/参数: 段逐个写清中文业务含义，供工作台接口面板直接展示。',
                           line=first_line)
            )
        stale_param_docs = _stale_operator_param_docs(root / rel)
        if stale_param_docs:
            first_line, first_name, first_stale = stale_param_docs[0]
            p_list = ', '.join(first_stale[:3]) + ('…' if len(first_stale) > 3 else '')
            extra = f' 等 {len(stale_param_docs)} 个算子' if len(stale_param_docs) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('stale_operator_param_description', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子 {first_name} 的 docstring Args:/参数: 段残留已不存在的参数 {p_list}{extra}；按 DF-02/OP-P1 规范，参数变更后须同步更新或删除过期参数说明，确保与函数签名一致。',
                           line=first_line)
            )
        # OP-E1: Mandatory minimal input -> output example + AST field-flow drift guard.
        missing_examples = _missing_operator_example_docs(root / rel)
        if missing_examples:
            first_line, first_name = missing_examples[0]
            extra = f' 等 {len(missing_examples)} 个算子' if len(missing_examples) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('missing_operator_example', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子 {first_name} 缺少输入输出变换示例{extra}；按 DF-02/OP-E1 规范，须在 docstring 的 Example:/示例: 段补充 1 条最小「输入: ...」「输出: ...」对比样例（仅列关键变化字段，支持行变换、展开列表或过滤布尔值），供工作台接口面板直观展示。',
                           line=first_line)
            )
        stale_examples = _stale_operator_example_docs(root / rel)
        if stale_examples:
            first_line, first_name, first_reason = stale_examples[0]
            extra = f' 等 {len(stale_examples)} 个算子' if len(stale_examples) > 1 else ''
            inspected.add(rel)
            by_category.setdefault('stale_operator_example', []).append(
                diagnostic('DF-02', rel,
                           f'公开算子 {first_name} 的变换示例与当前代码不一致（{first_reason}）{extra}；按 DF-02/OP-E1 规范，代码字段变更后须同步更新 docstring 的 Example:/示例: 段。',
                           line=first_line)
            )
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
            sdk_diag, cross_diag = _boundary_import_diagnostics(root, rel, check_cross_project=check_cross)
            if sdk_diag is not None:
                inspected.add(rel)
                by_category.setdefault('internal_sdk_import', []).append(sdk_diag)
            if cross_diag is not None:
                inspected.add(rel)
                by_category.setdefault('cross_project_import', []).append(cross_diag)

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
            'docs/ 仅用于项目文档与验收证据，不应存放源码目录、配置目录或压缩归档包（.tar.gz/.zip）；废弃非数据文件请直接清理（使用 manage_project_layout remove_path），切勿打包或转存到 docs/。')

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

    # Bound parameter files plus not-yet-registered configs/*.json drafts share one protocol check.
    config_json = {rel for rel in _iter_tree_files(root, 'configs', exclude=exclude) if rel.endswith('.json')}
    for param_rel in sorted(bound_params | config_json):
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
                '参数文件应直接采用扁平 JSON 键值：输入 Lance 表写 {"uri": "...", "version": 1}（version 可选），输出 Lance 表写 {"kind": "output", "uri": "data/out.lance"}。')
        if not isinstance(args, dict):
            continue
        for key, value in args.items():
            if key in _OUTPUT_ARG_KEYS and _points_to_runs(root, value):
                add('legacy_output_dir', param_rel,
                    f'活跃参数 {key} 指向 runs/ 目录；按 DF-02 规范运行产物应输出到 data/ 目录。')
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
                    'workspace.json 仅保留可读项目清单（name、entries 及可选 budgets、workflows），平台内部状态应保存在 .forge/state.json；请调用 manage_project_layout(action="slim_manifest") 规范化。')
        except Exception:
            pass

    # 9c. Declarative multi-pipeline workflows under workflows/*.json.
    workflow_files = _iter_tree_files(root, 'workflows', exclude=exclude)
    registered_workflows = list((control or {}).get('workflows') or ())
    if workflow_files or registered_workflows:
        from .workflow import validate_workflow
        checked_workflows: set[str] = set()
        for rel in workflow_files:
            if not rel.endswith('.json'):
                add('workflow_non_json', rel, 'workflows/ 目录仅允许存放声明式 JSON 编排文件（workflows/*.json）。')
                continue
            checked_workflows.add(rel)
            spec = validate_workflow(root, rel, control=control)
            inspected.add(rel)
            for item in spec['diagnostics']:
                by_category.setdefault('workflow_invalid', []).append(item)
        for rel in registered_workflows:
            if rel in checked_workflows:
                continue
            spec = validate_workflow(root, rel, control=control)
            inspected.add(rel)
            for item in spec['diagnostics']:
                by_category.setdefault('workflow_invalid', []).append(item)

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
