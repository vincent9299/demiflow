"""Web-side authority for existing registered/recognized fixed project data.

All native metadata/Case reads reuse the existing bounded workspace worker.
No source code or user SQL is executed here. Callback requests cannot select
a URI, new root, row decoder or arbitrary filesystem path.
"""
from __future__ import annotations

from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.sources import locator


def _snapshot(metadata, value):
    columns = []
    fields = metadata["fields"]
    if type(fields) is not dict:
        c.reject("Fixed source schema metadata is unavailable", code="operation_rejected")
    for name in value["columns"]:
        parts=[name] if name in fields else name.split('.')
        if len(parts)>8 or any(not part for part in parts):c.reject("Source struct projection exceeds its path budget")
        current=fields;nullable=False
        for index,part in enumerate(parts):
            if part not in current:c.reject("Source projection differs from its fixed schema", code="conflict")
            info=current[part]["type"];nullable=nullable or info["nullable"]
            if index+1<len(parts):
                if info["kind"]!="record":c.reject("Source nested projection requires a struct field")
                current=info["fields"]
        physical = info.get("physical", info["kind"])
        # Logical names of the explicit Arrow bridge types; other physical
        # types remain explicit (Case may still read/render them). SQL export
        # must separately admit its supported native projection types.
        kind = (info["kind"] if info["kind"] in {"list","struct","map","dictionary"} else
            {"double": "float64", "string": "utf8"}.get(physical, physical))
        columns.append(dict(name=name, type=kind, nullable=nullable))
    return c.source_snapshot(dict(kind="fixed_lance", binding=value["binding"], schema=columns,
        rows=metadata["row_count"], case_key=dict(kind="row_id", field=None), retention="external"))


def resolve(root, value):
    value = locator(value)
    if value["kind"] == "workspace_table":
        if "version" in value:
            description = project_ops.describe_table(root, value["alias"], expected_binding=value["alias_binding"],
                                            version=value["version"], version_binding=value["binding"])
        else:
            description = project_ops.describe_table(root, value["alias"], expected_binding=value["binding"])
        if description["binding"]["digest"] != value["binding"]:
            c.reject("Registered source binding changed", code="conflict")
        metadata = description["metadata"]
    else:
        description = project_ops.project_table(root, value["id"], expected=value["discovery"], version=value["version"])
        if description["binding"] != value["binding"]:
            c.reject("Recognized source binding changed", code="conflict")
        metadata = description
    return dict(source=_snapshot(metadata, value), locator=value)


def verify(root, *, locator: dict, source, target):
    resolved = resolve(root, locator)
    if resolved["source"] != source:
        c.reject("Fixed source content/schema/row receipt differs", code="conflict")
    if target is None:
        return {"verified": True}
    Records._target(source, target)
    if target["variant"] is not None:
        c.reject("Separate variant identities need a registered row-value proof", code="operation_rejected")
    if source["case_key"]["kind"] != "row_id":
        c.reject("This external source has no registered unique-field Case proof", code="operation_rejected")
    arguments = dict(columns=[], row_id=int(target["case_key"]))
    if locator["kind"] == "workspace_table":
        if "version" in locator:
            result = project_ops.sample_table(root, locator["alias"], expected_binding=locator["alias_binding"],
                                     version=locator["version"], version_binding=locator["binding"], **arguments)
        else:
            result = project_ops.sample_table(root, locator["alias"], expected_binding=locator["binding"], **arguments)
    else:
        result = project_ops.project_sample(root, locator["id"], expected=locator["discovery"],
            version=locator["version"], binding=locator["binding"], **arguments)
    if len(result["rows"]) != 1 or result["rows"][0].get("_rowid") != target["case_key"]:
        c.reject("Case does not exist in its exact fixed source", code="conflict")
    return {"verified": True}
