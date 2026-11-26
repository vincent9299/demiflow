"""Bounded coding guidance from the loaded native SDK catalogue."""
import ast
from hashlib import sha256
from pathlib import Path

from .platform import content_digest, discover_catalog
from .platform_environment import read_bounded
from .schema import ForgeError


MODULES = ('demiflow.data', 'demiflow.execution', 'demiflow.operator_llm', 'demiflow.objects', 'demiflow.agent')
REFERENCE_BYTES = 2 * 1024**2
SOURCE_EXCERPT_BYTES = 2048
SDK_ROOT = Path(__file__).resolve().parents[1]
DATASET = 'demiflow.data:Dataset.'
ESSENTIAL = ('demiflow.data:read_lance', 'demiflow.data:from_items', 'demiflow.data:range', *(
    DATASET + method for method in (
        'map', 'map_async', 'filter', 'select_columns', 'join', 'union',
        'map_prompt', 'map_prompt_async', 'agentmap_async',
        'materialize', 'take', 'count', 'write_lance',
    )
))


def _symbol_source_excerpt(origin, symbol_id, doc_text, *, max_bytes=SOURCE_EXCERPT_BYTES):
    """Return a bounded AST source definition for explicit --api queries when helpful.

    Attached when the docstring is empty/terse/auto-generated dataclass signature, or when the
    target symbol is a class (dataclass / contract definition), so callers can inspect fields
    and validation logic without scanning the SDK tree.
    """
    if origin.get('kind') != 'platform' or not origin.get('path'):
        return None, False
    path = SDK_ROOT / origin['path']
    try:
        if not path.resolve().is_relative_to(SDK_ROOT) or not path.is_file():
            return None, False
        raw = read_bounded(path, REFERENCE_BYTES).decode('utf-8', 'replace')
        tree = ast.parse(raw)
    except (OSError, SyntaxError, ValueError, ForgeError):
        return None, False
    sym = symbol_id.partition(':')[2]
    parts = [p for p in sym.split('.') if p]
    if not parts:
        return None, False
    target = None
    body = tree.body
    for idx, name in enumerate(parts):
        found = next(
            (n for n in body
             if isinstance(n, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name),
            None,
        )
        if found is None:
            return None, False
        target = found
        if idx < len(parts) - 1:
            if not isinstance(found, ast.ClassDef):
                return None, False
            body = found.body
    clean_doc = (doc_text or '').strip()
    leaf = parts[-1]
    terse_or_auto = (
        not clean_doc
        or len(clean_doc) < 80
        or clean_doc.startswith(leaf + '(')
    )
    if not terse_or_auto and not isinstance(target, ast.ClassDef):
        return None, False
    lines = raw.splitlines()
    start = min([target.lineno, *(d.lineno for d in getattr(target, 'decorator_list', ()))]) - 1
    end = getattr(target, 'end_lineno', target.lineno)
    snippet = '\n'.join(lines[max(0, start):end])
    encoded = snippet.encode('utf-8')
    truncated = len(encoded) > max_bytes
    if truncated:
        snippet = encoded[:max_bytes].decode('utf-8', 'ignore').rstrip()
    return snippet, truncated


def sdk_identity(catalog):
    """Bind the inspected public implementations and native semantic rules."""
    paths = {'data/flow_contract.py', 'data/callable_contract.py'}
    for module in catalog['modules']:
        relative = module.removeprefix('demiflow.').replace('.', '/')
        path = relative + '.py'
        paths.add(path if (SDK_ROOT / path).is_file() else relative + '/__init__.py')
    for entry in catalog['objects']:
        origin = entry['origin']
        if origin['kind'] == 'platform':
            paths.add(origin['path'])
    remaining = 16 * 1024**2
    sources = {}
    for relative in sorted(paths):
        path = SDK_ROOT / relative
        if not path.resolve().is_relative_to(SDK_ROOT):
            raise ForgeError('API source is outside the loaded SDK', code='conflict')
        raw = read_bounded(path, min(remaining, REFERENCE_BYTES))
        remaining -= len(raw)
        sources[relative] = sha256(raw).hexdigest()
    return content_digest(sources)


def build_reference():
    catalog = discover_catalog(MODULES, limits={'timeout_seconds': 30, 'catalog_bytes': REFERENCE_BYTES})
    if not catalog['inspection_complete']:
        raise ForgeError('Native SDK API catalogue is incomplete', code='resource_limit')
    return dict(catalog=catalog, catalog_id=content_digest(catalog), sdk_revision=sdk_identity(catalog))


def _first_doc_line(text, limit=120):
    for line in (text or '').splitlines():
        clean = line.strip()
        if clean:
            return clean[:limit]
    return ''


def _is_inherited_duplicate(item, mod, sym, by_id):
    if '.' not in sym:
        return False
    defined = item.get('defined_name')
    return bool(defined and defined != sym and f'{mod}:{defined}' in by_id)


def _format_api_row(symbol, item, *, detailed=False, doc_limit=720):
    origin = item['origin']
    source = str(SDK_ROOT / origin['path']) if origin['kind'] == 'platform' else origin['module']
    documentation = item['documentation']['text'].encode()
    excerpt = documentation[:doc_limit].decode('utf8', 'ignore')
    row = dict(id=symbol, signature=item['signature']['text'], source=source,
               documentation=excerpt, documentation_truncated=len(documentation) > doc_limit,
               native_semantics=(item['native_semantics'] if detailed else
                                 {k: v for k, v in item['native_semantics'].items() if k == 'role'}))
    if detailed:
        src_excerpt, src_trunc = _symbol_source_excerpt(origin, symbol, item['documentation']['text'])
        if src_excerpt:
            row['source_excerpt'] = src_excerpt
            row['source_excerpt_truncated'] = src_trunc
    return row


def select_apis(reference, api=None, *, extra_apis=(), maximum=20 * 1024):
    """Project complete signatures and bounded documentation from one public catalogue."""
    from .schema import canonical

    catalog = reference['catalog']
    if not catalog['inspection_complete']:
        raise ForgeError('SDK catalogue inspection incomplete', code='resource_limit')
    by_id = {item['id']: item for item in catalog['objects']}
    if api is not None:
        if api.partition(':')[0] not in catalog['modules']:
            raise ForgeError('SDK symbol outside inspected modules: ' + api, code='invalid_request')
        if api not in by_id:
            raise ForgeError('Unknown SDK symbol: ' + api, code='invalid_request')
    rows, seen, remaining = [], set(), maximum
    for symbol in ((api,) if api is not None else ESSENTIAL):
        item = by_id.get(symbol)
        if item is None or item['signature']['status'] != 'available':
            raise ForgeError('SDK signature unavailable: ' + symbol, code='resource_limit')
        row = _format_api_row(symbol, item, detailed=(api is not None),
                              doc_limit=(maximum // 2 if api is not None else 720))
        size = len(canonical(row)) + 1
        if size > remaining:
            raise ForgeError('Selected SDK signatures exceed context budget', code='resource_limit')
        remaining -= size
        seen.add(symbol)
        rows.append(row)
    if api is None and extra_apis:
        extra_doc_limit = min(max(maximum // 4, 2048), 2800)
        allowed_modules = set(catalog.get('modules', ()))
        for symbol in extra_apis:
            if symbol in seen or symbol.partition(':')[0] not in allowed_modules:
                continue
            item = by_id.get(symbol)
            if item is None or item['signature']['status'] != 'available':
                continue
            row = _format_api_row(symbol, item, detailed=False, doc_limit=extra_doc_limit)
            size = len(canonical(row)) + 1
            if size > remaining:
                continue
            remaining -= size
            seen.add(symbol)
            rows.append(row)
    return rows


def api_markdown(apis):
    from .schema import canonical

    rows = ['## 当前版本 DemiFlow 原生 API']
    for item in apis:
        facts = f"- {item['id']} {item['signature']}\n  来源：{item['source']}"
        if 'role' not in item['native_semantics']:
            facts += '\n  此方法的原生 flow 静态语义尚未覆盖。'
        else:
            facts += '\n  原生语义：' + canonical(item['native_semantics']).decode()
        if item['documentation']:
            facts += '\n  ' + item['documentation']
            if item['documentation_truncated']:
                facts += '（节选）'
        if item.get('source_excerpt'):
            suffix = '（源码节选）' if item.get('source_excerpt_truncated') else ''
            facts += f"\n  源码定义{suffix}：\n```python\n{item['source_excerpt']}\n```"
        rows.append(facts)
    return '\n\n'.join(rows)


_CORE_MEMBER_OWNERS = frozenset({'Dataset', 'RunContext', 'LocalObjectStore', 'ObjectRef'})
_PROJECT_SCAN_DIRS = ('flows', 'operators')
_PROJECT_SCAN_MAX_FILES = 64
_PROJECT_SCAN_MAX_BYTES = 512 * 1024


def module_symbol_index(reference):
    """Build a compact, live-discovered public symbol directory grouped by module."""
    catalog = reference['catalog']
    if not catalog.get('inspection_complete'):
        return {}
    essential = set(ESSENTIAL)
    by_id = {item['id']: item for item in catalog['objects']}
    grouped = {mod: [] for mod in MODULES if mod in catalog.get('modules', ())}
    for item in catalog['objects']:
        if item.get('origin', {}).get('kind') != 'platform':
            continue
        if item.get('signature', {}).get('status') != 'available':
            continue
        mod, _, sym = item['id'].partition(':')
        if mod not in grouped or not sym or item['id'] in essential:
            continue
        if '.' in sym:
            owner, _, member = sym.partition('.')
            if member.startswith('_') or _is_inherited_duplicate(item, mod, sym, by_id):
                continue
            has_flow_semantics = bool(item.get('native_semantics', {}).get('role'))
            if not has_flow_semantics and owner not in _CORE_MEMBER_OWNERS:
                continue
        elif sym.endswith('Error') or sym.endswith('Warning'):
            continue
        grouped[mod].append(sym)
    return {mod: sorted(set(syms)) for mod, syms in grouped.items() if syms}


def flow_api_summaries(reference, *, exclude_ids=(), maximum=8 * 1024):
    """Return bounded live 1-line summaries for all non-duplicate @flow_api public symbols."""
    from .schema import canonical

    catalog = reference['catalog']
    if not catalog.get('inspection_complete') or maximum <= 0:
        return []
    excluded = set(exclude_ids)
    by_id = {item['id']: item for item in catalog['objects']}
    allowed_modules = set(MODULES)
    rows, remaining = [], maximum
    for item in sorted(catalog['objects'], key=lambda entry: entry['id']):
        if item.get('origin', {}).get('kind') != 'platform':
            continue
        if item.get('signature', {}).get('status') != 'available':
            continue
        role = item.get('native_semantics', {}).get('role')
        if not role:
            continue
        sym_id = item['id']
        mod, _, sym = sym_id.partition(':')
        if mod not in allowed_modules or sym_id in excluded:
            continue
        if _is_inherited_duplicate(item, mod, sym, by_id):
            continue
        row = dict(id=sym_id, role=role, summary=_first_doc_line(item['documentation']['text'], limit=120))
        size = len(canonical(row)) + 1
        if size > remaining:
            break
        remaining -= size
        rows.append(row)
    return rows


def discover_project_sdk_symbols(root, reference, *, exclude=()):
    """Dynamically detect non-essential public SDK symbols referenced in a project's code."""
    catalog = reference.get('catalog') if isinstance(reference, dict) else None
    if not catalog or not catalog.get('inspection_complete'):
        return ()
    root = Path(root)
    if not root.is_dir():
        return ()
    essential = set(ESSENTIAL)
    allowed_modules = {mod for mod in MODULES if mod in catalog.get('modules', ())}
    by_id = {item['id']: item for item in catalog['objects']}
    valid_ids = set()
    method_candidates = {}
    for item in catalog['objects']:
        if item.get('origin', {}).get('kind') != 'platform':
            continue
        if item.get('signature', {}).get('status') != 'available':
            continue
        sym_id = item['id']
        mod, _, sym = sym_id.partition(':')
        if mod not in allowed_modules or not sym or _is_inherited_duplicate(item, mod, sym, by_id):
            continue
        valid_ids.add(sym_id)
        if '.' in sym and sym_id not in essential and item.get('native_semantics', {}).get('role'):
            method = sym.rsplit('.', 1)[1]
            if not method.startswith('_'):
                method_candidates.setdefault(method, set()).add(sym_id)
    unique_flow_methods = {
        method: next(iter(ids))
        for method, ids in method_candidates.items()
        if len(ids) == 1
    }

    def is_hidden(rel):
        parts = Path(rel).parts
        return (any(part.startswith('.') or part == '__pycache__' for part in parts)
                or any(rel == item or rel.startswith(item + '/') for item in exclude))

    found = set()
    scanned_files = 0
    remaining_bytes = _PROJECT_SCAN_MAX_BYTES
    for role_dir in _PROJECT_SCAN_DIRS:
        base = root / role_dir
        if not base.is_dir() or base.is_symlink():
            continue
        for path in sorted(base.rglob('*.py')):
            if scanned_files >= _PROJECT_SCAN_MAX_FILES or remaining_bytes <= 0:
                break
            if path.is_symlink() or not path.is_file():
                continue
            try:
                rel = path.relative_to(root).as_posix()
            except ValueError:
                continue
            if is_hidden(rel):
                continue
            scanned_files += 1
            try:
                raw = read_bounded(path, min(remaining_bytes, 128 * 1024))
                remaining_bytes -= len(raw)
                tree = ast.parse(raw.decode('utf-8', 'replace'), filename=rel)
            except (OSError, ValueError, SyntaxError, RecursionError, ForgeError):
                continue
            module_aliases = {}
            demiflow_root_aliases = set()
            uses_demiflow = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        name = alias.name
                        local = alias.asname or name.split('.')[0]
                        if name == 'demiflow' or name.startswith('demiflow.'):
                            uses_demiflow = True
                        if name == 'demiflow':
                            demiflow_root_aliases.add(local)
                        elif name in allowed_modules:
                            if alias.asname:
                                module_aliases[alias.asname] = name
                            else:
                                demiflow_root_aliases.add('demiflow')
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    mod = node.module
                    if mod == 'demiflow' or mod.startswith('demiflow.'):
                        uses_demiflow = True
                    if mod == 'demiflow':
                        for alias in node.names:
                            sub = f'demiflow.{alias.name}'
                            if sub in allowed_modules:
                                module_aliases[alias.asname or alias.name] = sub
                    elif mod in allowed_modules:
                        for alias in node.names:
                            cand = f'{mod}:{alias.name}'
                            if cand in valid_ids and cand not in essential:
                                found.add(cand)
            if not uses_demiflow:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    if isinstance(node.value, ast.Name) and node.value.id in module_aliases:
                        cand = f'{module_aliases[node.value.id]}:{node.attr}'
                        if cand in valid_ids and cand not in essential:
                            found.add(cand)
                    elif (isinstance(node.value, ast.Attribute)
                          and isinstance(node.value.value, ast.Name)
                          and node.value.value.id in demiflow_root_aliases):
                        mod = f'demiflow.{node.value.attr}'
                        cand = f'{mod}:{node.attr}'
                        if mod in allowed_modules and cand in valid_ids and cand not in essential:
                            found.add(cand)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    mapped = unique_flow_methods.get(node.func.attr)
                    if mapped is not None:
                        found.add(mapped)
    return tuple(sorted(found))


def render_reference(reference, *, maximum=20 * 1024):
    rows = select_apis(reference, maximum=maximum)
    return api_markdown(rows), [item['id'] for item in rows]
