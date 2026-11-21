"""Bounded coding guidance from the loaded native SDK catalogue."""
from hashlib import sha256
from pathlib import Path

from .platform import content_digest, discover_catalog
from .platform_environment import read_bounded
from .schema import ForgeError


MODULES = ('demiflow.data', 'demiflow.operator_llm', 'demiflow.objects', 'demiflow.agent')
REFERENCE_BYTES = 2 * 1024**2
SDK_ROOT = Path(__file__).resolve().parents[1]
DATASET = 'demiflow.data:Dataset.'
ESSENTIAL = ('demiflow.data:read_lance', 'demiflow.data:from_items', 'demiflow.data:range', *(
    DATASET + method for method in (
        'map', 'map_async', 'filter', 'select_columns', 'join', 'union',
        'map_prompt', 'map_prompt_async', 'agentmap_async',
        'materialize', 'take', 'count', 'write_lance',
    )
))


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
    catalog = discover_catalog(MODULES, limits={'timeout_seconds': 12, 'catalog_bytes': REFERENCE_BYTES})
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
        rows.append(facts)
    return '\n\n'.join(rows)


def render_reference(reference, *, maximum=20 * 1024):
    rows = select_apis(reference, maximum=maximum)
    return api_markdown(rows), [item['id'] for item in rows]
