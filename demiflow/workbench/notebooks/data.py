"""Read the name or path printed in the notebook's data dictionary."""
import os
from pathlib import Path

MAX_SAMPLE_LIMIT = 100


def _resolve_project_table(root, path, *, version):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.notebooks import dictionary
    from demiflow.workbench.projects import discovery
    declarations = discovery.discover(root)
    matching = [r for r in declarations['tables'] if r['path'] == path and r['status'] == 'available']
    matches = [r for r in dictionary.discover(root)['tables'] if r['path'] == path]
    if matching:
        if any(Path(item['uri']) != root / path for item in matches):
            raise ForgeError('数据字典中有多个同名路径，请使用唯一的表名。', code='conflict')
        row = next((r for r in matching if r['version'] == version),
                   next((r for r in matching if r['version'] is None), None))
        if row is None:
            raise ForgeError('数据字典中没有这张表的指定版本。', code='conflict')
        expected = declarations['digest']
    else:
        if len(matches) != 1:
            raise ForgeError('请使用数据字典中唯一的完整表路径。', code='invalid_request')
        row = matches[0]
        expected = row['expected']
    return row, expected


def describe(alias, *, version=None, binding=None, version_binding=None):
    """Return fixed-version metadata (version, row_count, fields) without scanning rows."""
    from demiflow.workbench import project_operations as ops
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    if isinstance(alias, str) and alias.endswith('.lance') and not alias.startswith('run:'):
        if version is None:
            raise ValueError('请指定数据字典中显示的表版本。')
        row, expected = _resolve_project_table(root, alias, version=version)
        info = ops.project_table(root, row['id'], expected=expected, version=version)
        return dict(version=info['version'], row_count=info['row_count'],
                    fields=info['fields'], binding=info['binding'])
    if binding is None:
        current = ops.describe_table(root, alias)
        binding = current['binding']['digest']
        if version is None or version == current['binding']['reference']['ref']['lance_version']:
            ref = current['binding']['reference']['ref']
            return dict(version=ref['lance_version'], row_count=ref['row_count'],
                        fields=current['metadata']['fields'], binding=binding)
        selected = ops.table_versions(root, alias, expected_binding=binding, version=version)['selected']
        version_binding = selected['binding']['digest']
    info = ops.describe_table(root, alias, expected_binding=binding, version=version, version_binding=version_binding)
    ref = info['binding']['reference']['ref']
    return dict(version=ref['lance_version'], row_count=ref['row_count'],
                fields=info['metadata']['fields'], binding=info['binding']['digest'])


def count(alias, *, version=None, binding=None, version_binding=None):
    """Return the exact row count of a fixed-version table from its manifest metadata."""
    return int(describe(alias, version=version, binding=binding, version_binding=version_binding)['row_count'])


def sample(alias, *, version=None, columns=None, offset=0, limit=20, binding=None, version_binding=None):
    """Sample the exact name or path shown in the data dictionary at a fixed version (limit <= 100)."""
    if type(limit) is not int or not 1 <= limit <= MAX_SAMPLE_LIMIT:
        raise ValueError(
            f'sample() 仅用于受限预览，单次 limit 必须在 1..{MAX_SAMPLE_LIMIT} 之间；'
            '统计总行数请使用 count() 或 describe()，读取更多行请分页使用 offset。'
        )
    if isinstance(alias, str) and alias.endswith('.lance') and not alias.startswith('run:'):
        if version is None:
            raise ValueError('请指定数据字典中显示的表版本。')
        return project_sample(alias, version=version, columns=columns, offset=offset, limit=limit)
    from demiflow.workbench import project_operations as ops
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    if binding is None:
        current = ops.describe_table(root, alias)['binding']
        binding = current['digest']
        if version == current['reference']['ref']['lance_version']:
            version = None
        elif version is not None:
            selected = ops.table_versions(root, alias, expected_binding=binding, version=version)['selected']
            version_binding = selected['binding']['digest']
    value = ops.sample_table(root, alias, expected_binding=binding, version=version,
                             version_binding=version_binding, columns=columns, offset=offset, limit=limit)
    import pandas as pd
    return pd.DataFrame(value['rows'], columns=value['columns'])


def project_sample(path, *, version, columns=None, offset=0, limit=20):
    """Sample a project table by the exact path shown in the data dictionary (limit <= 100)."""
    if type(limit) is not int or not 1 <= limit <= MAX_SAMPLE_LIMIT:
        raise ValueError(
            f'sample() 仅用于受限预览，单次 limit 必须在 1..{MAX_SAMPLE_LIMIT} 之间；'
            '统计总行数请使用 count() 或 describe()，读取更多行请分页使用 offset。'
        )
    from demiflow.workbench import project_operations as ops
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    row, expected = _resolve_project_table(root, path, version=version)
    info = ops.project_table(root, row['id'], expected=expected, version=version)
    value = ops.project_sample(root, row['id'], expected=expected, version=version, binding=info['binding'],
                               columns=columns, offset=offset, limit=limit)
    import pandas as pd
    return pd.DataFrame(value['rows'], columns=value['columns'])
