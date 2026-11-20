"""Notebook-friendly DataFrames from the existing bounded, fixed table reader."""
import os
from pathlib import Path


def sample(alias, *, binding, version=None, version_binding=None, columns=None, offset=0, limit=20):
    """Read a fixed catalog sample; binding/version are supplied by the dictionary."""
    from demiflow.workbench.project_operations import sample_table
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    value = sample_table(root, alias, expected_binding=binding, version=version,
                         version_binding=version_binding, columns=columns, offset=offset, limit=limit)
    import pandas as pd
    return pd.DataFrame(value['rows'], columns=value['columns'])


def project_sample(identity, *, expected, version, binding, columns=None, offset=0, limit=20):
    """Read a project's declared table at the version selected in the dictionary."""
    from demiflow.workbench.project_operations import project_sample as read
    root = Path(os.environ['DEMIFLOW_NOTEBOOK_ROOT'])
    value = read(root, identity, expected=expected, version=version, binding=binding,
                 columns=columns, offset=offset, limit=limit)
    import pandas as pd
    return pd.DataFrame(value['rows'], columns=value['columns'])
