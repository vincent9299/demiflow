"""API contracts read from the exact platform source, including sealed runs."""
import ast
import hashlib
from pathlib import Path

from demiflow.forge.platform_environment import read_bounded
from demiflow.workbench.definition_contracts import code_definition


def source_address(api):
    if ':' in api and not api.startswith('demiflow.data:'):
        return None
    name = api.split(':')[-1]
    parts = name.split('.')
    owner, method = parts if len(parts) == 2 else ('DataAPI', name)
    if owner not in {'DataAPI', 'Dataset', 'MaterializedDataset'} or not method.isidentifier() or method.startswith('_'):
        return None
    return ('data/api.py' if owner == 'DataAPI' else 'data/dataset.py', owner, method)


def from_source(api, raw):
    address = source_address(api)
    if address is None:
        return None
    path, owner, method = address
    tree = ast.parse(raw)
    if sum(1 for _ in ast.walk(tree)) > 100000:
        return None
    nodes = [n for cls in tree.body if isinstance(cls, ast.ClassDef) and cls.name == owner
             for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == method]
    if len(nodes) != 1:
        return None
    value = code_definition(nodes[0], '', kind='api', name=owner + '.' + method)
    value.update(path=path, file_sha256=hashlib.sha256(raw).hexdigest(),
                 schema_note='API 签名描述参数与返回类型；Dataset 行字段由数据源、算子契约及本次接线决定。未声明的类型保持未知。')
    return value


def current(api):
    address = source_address(api)
    if address is None:
        return None
    root = Path(__file__).resolve().parents[2]
    return from_source(api, read_bounded(root / address[0], 2 * 1024**2))


_CATALOG_CACHE = None


def catalog():
    """Return public DemiFlow DataAPI and Dataset standard API contracts from engine source."""
    global _CATALOG_CACHE
    if _CATALOG_CACHE is not None:
        return _CATALOG_CACHE
    root = Path(__file__).resolve().parents[2]
    items = []
    for rel, owner, group in (
        ('data/api.py', 'DataAPI', '数据入口 · data'),
        ('data/dataset.py', 'Dataset', '数据变换与输出 · Dataset'),
    ):
        raw = read_bounded(root / rel, 2 * 1024**2)
        tree = ast.parse(raw)
        digest = hashlib.sha256(raw).hexdigest()
        for cls in tree.body:
            if not isinstance(cls, ast.ClassDef) or cls.name != owner:
                continue
            for fn in cls.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) or fn.name.startswith('_'):
                    continue
                value = code_definition(fn, '', kind='api', name=f'{owner}.{fn.name}')
                value.update(
                    id=f'api:{owner}.{fn.name}',
                    owner=owner,
                    method=fn.name,
                    group=group,
                    display_name=f'data.{fn.name}' if owner == 'DataAPI' else f'Dataset.{fn.name}',
                    path=rel,
                    file_sha256=digest,
                )
                items.append(value)
    _CATALOG_CACHE = items
    return items
