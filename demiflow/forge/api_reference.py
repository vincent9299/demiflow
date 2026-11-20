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


def render_reference(reference, *, maximum=20 * 1024):
    catalog = reference['catalog']
    by_id = {item['id']: item for item in catalog['objects']}
    rows = ['## 当前版本 DemiFlow 原生 API',
            '用原生 Dataset API 编排读取、转换、模型调用和输出。'
            '业务行逻辑写成普通函数或 Actor，保持 I/O 和模型节点在 flow 中可见。',
            '下列签名来自当前 SDK，使用说明为文档节选。需要完整说明时读取所列 SDK 源文件。',
            "最小接线示例：`from_items([{'id': 1}]).materialize()`。"
            "在实际 flow 里替换为所需的原生读取、转换和受控输出。"]
    total = sum(len(row.encode()) + 2 for row in rows)
    if total > maximum:
        raise ForgeError('Context cannot fit native API guidance', code='resource_limit')
    included = []
    for symbol in ESSENTIAL:
        item = by_id.get(symbol)
        if item is None or item['signature']['status'] != 'available':
            raise ForgeError('Required native API unavailable: ' + symbol)
        # Select whole documentation lines. The signature itself is never clipped.
        doc, used = [], 0
        for line in item['documentation']['text'].splitlines():
            size = len(line.encode()) + 1
            if used + size > 720:
                break
            doc.append(line)
            used += size
        origin = item['origin']
        source = str(SDK_ROOT / origin['path']) if origin['kind'] == 'platform' else origin['module']
        line = f"- {symbol} {item['signature']['text']}\n  来源：{source}"
        if 'role' not in item['native_semantics']:
            line += '\n  此方法是公开 API，原生 flow 静态检查尚未覆盖。'
        if doc:
            line += '\n  ' + '\n  '.join(doc)
        size = len(line.encode()) + 2
        if total + size > maximum:
            raise ForgeError('Context cannot fit required native API signatures', code='resource_limit')
        included.append(symbol)
        rows.append(line)
        total += size
    return '\n\n'.join(rows), included
