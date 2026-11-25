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


def select_apis(reference, api=None, *, maximum=20 * 1024):
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
    rows, remaining = [], maximum
    for symbol in ((api,) if api is not None else ESSENTIAL):
        item = by_id.get(symbol)
        if item is None or item['signature']['status'] != 'available':
            raise ForgeError('SDK signature unavailable: ' + symbol, code='resource_limit')
        origin = item['origin']
        source = str(SDK_ROOT / origin['path']) if origin['kind'] == 'platform' else origin['module']
        documentation = item['documentation']['text'].encode()
        doc_limit = maximum // 2 if api is not None else 720
        excerpt = documentation[:doc_limit].decode('utf8', 'ignore')
        row = dict(id=symbol, signature=item['signature']['text'], source=source,
                   documentation=excerpt, documentation_truncated=len(documentation) > doc_limit,
                   native_semantics=(item['native_semantics'] if api is not None else
                                     {k: v for k, v in item['native_semantics'].items() if k == 'role'}))
        if api is not None:
            src_excerpt, src_trunc = _symbol_source_excerpt(origin, symbol, item['documentation']['text'])
            if src_excerpt:
                row['source_excerpt'] = src_excerpt
                row['source_excerpt_truncated'] = src_trunc
        size = len(canonical(row)) + 1
        if size > remaining:
            raise ForgeError('Selected SDK signatures exceed context budget', code='resource_limit')
        remaining -= size
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


def module_symbol_index(reference):
    """Build a compact, live-discovered public symbol directory grouped by module."""
    catalog = reference['catalog']
    if not catalog.get('inspection_complete'):
        return {}
    essential = set(ESSENTIAL)
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
            if owner not in {'Dataset', 'FitRef', 'RunContext', 'LocalObjectStore', 'ObjectRef'}:
                continue
            if member.startswith('_'):
                continue
        elif sym.endswith('Error') or sym.endswith('Warning'):
            continue
        grouped[mod].append(sym)
    return {mod: sorted(set(syms)) for mod, syms in grouped.items() if syms}


def render_reference(reference, *, maximum=20 * 1024):
    rows = select_apis(reference, maximum=maximum)
    return api_markdown(rows), [item['id'] for item in rows]
