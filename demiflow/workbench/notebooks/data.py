"""Read the name or path printed in the notebook's data dictionary."""
import os
from pathlib import Path


def sample(alias, *, version=None, columns=None, offset=0, limit=20, binding=None, version_binding=None):
    """Sample the exact name or path shown in the data dictionary at a fixed version."""
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
    """Sample a project table by the exact path shown in the data dictionary."""
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench import project_operations as ops
    from demiflow.workbench.notebooks import dictionary
    from demiflow.workbench.projects import discovery
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    declarations = discovery.discover(root)
    matching = [r for r in declarations['tables'] if r['path'] == path and r['status'] == 'available']
    matches = [r for r in dictionary.discover(root)['tables'] if r['path'] == path]
    if matching:
        if any(Path(item['uri']) != root/path for item in matches):
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
    info = ops.project_table(root, row['id'], expected=expected, version=version)
    value = ops.project_sample(root, row['id'], expected=expected, version=version, binding=info['binding'],
                               columns=columns, offset=offset, limit=limit)
    import pandas as pd
    return pd.DataFrame(value['rows'], columns=value['columns'])
