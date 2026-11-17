"""Read-only catalog profiling; does not depend on Notebook execution grants.

Invoked inside the existing bounded workspace worker, using the same source
projection, fixed-version verification and profile as the analysis callback.
"""
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.source_export import fixed_input
from demiflow.workbench.analysis.source_query import read


def profile(root, *, kind, columns, alias=None, expected_binding=None, identity=None, expected=None, version=None, binding=None, version_binding=None):
    if kind=="workspace_table":
        if any(value is not None for value in (identity,expected,binding)):
            c.reject("Catalog table profile has foreign locator fields")
        if (version is None) != (version_binding is None):
            c.reject("Catalog table profile version and version binding must be provided together")
        if version is not None:
            value=dict(kind=kind,alias=alias,binding=version_binding,columns=columns,version=version,alias_binding=expected_binding)
        else:
            value=dict(kind=kind,alias=alias,binding=expected_binding,columns=columns)
    elif kind=="project_table":
        if alias is not None or expected_binding is not None or version_binding is not None:
            c.reject("Project table profile has foreign locator fields")
        value=dict(kind=kind,id=identity,discovery=expected,version=version,binding=binding,columns=columns)
    else:c.reject("Unknown catalog profile source")
    _,_,source=fixed_input(root,value)
    result=read(root,source_locator=value,source_snapshot=source["digest"],query=dict(kind="profile",columns=columns))
    return dict(result,binding=value["binding"])
