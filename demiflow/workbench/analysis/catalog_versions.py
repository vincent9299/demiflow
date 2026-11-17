"""Read-only resolution of registered historical workspace table versions.

C4.1: the current alias preview binding guards identity; the Catalog supplies
the registered DatasetRef; a virtual control (only its ref replaced) feeds the
existing preview_binding identity algorithm. No control file is written, no
Catalog record is created, and native reads run only inside the workspace
worker. The resolution is an immutable internal value; ``to_wire`` produces the
plain JSON shape used by worker responses and source snapshots.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from demiflow.workbench.analysis import contracts as c


def _freeze(value):
    if type(value) is dict:
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value):
    if isinstance(value, MappingProxyType):
        return {key: _thaw(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class VersionResolution:
    alias_binding: object
    binding: object
    reference: object
    ref: object
    uri: Path
    metadata: object

    def to_wire(self):
        return {
            "version": self.ref.lance_version,
            "alias_binding": self.alias_binding["digest"],
            "binding": _thaw(self.binding),
            "reference": _thaw(self.reference),
            "metadata": _thaw(self.metadata),
        }


def resolve_version(root, *, alias, expected_binding, version, expected_version_binding=None):
    from demiflow.lance import preview
    from demiflow.lance.registry import Catalog
    from demiflow.workbench.projects import workspace as ws

    c.sha(expected_binding, "current alias binding")
    c.integer(version, "fixed table version", 2**63 - 1, 1)
    if expected_version_binding is not None:
        c.sha(expected_version_binding, "expected version binding")
    root, workspace, _ = ws.load_workspace(root)
    control, ref, uri = ws.table_binding(root, workspace, alias)
    alias_binding = ws.preview_binding(root, control, uri)
    if expected_binding != alias_binding["digest"]:
        c.reject("Registered alias binding changed; reopen the table explicitly", code="conflict")
    if isinstance(alias, str) and alias.startswith("run:"):
        # A run product is pinned to its one sealed output version.
        if version != ref.lance_version:
            c.reject("Run outputs only admit their sealed version", code="invalid_request")
        selected = ref
    else:
        base = ws.location_root(root, workspace["locations"][control["location_id"]])
        registered = [item for item in Catalog(base).versions(ref.dataset_id)
                      if item.store_id == ref.store_id and item.relative_uri == ref.relative_uri]
        selected = next((item for item in registered if item.lance_version == version), None)
        if selected is None:
            c.reject("Version is not a registered version of this table", code="invalid_request")
    metadata = preview.describe_table(uri, version)
    if (metadata["version"] != version or metadata["schema_hash"] != selected.schema_hash
            or metadata["row_count"] != selected.row_count):
        c.reject("Registered version differs from the actual table", code="conflict")
    reference = {**control, "ref": selected.to_dict()}
    binding = ws.preview_binding(root, reference, uri)
    if expected_version_binding is not None and expected_version_binding != binding["digest"]:
        c.reject("Historical version binding changed", code="conflict")
    # The alias must still name the same source after the historical read.
    _, workspace_after, _ = ws.load_workspace(root)
    control_after, ref_after, uri_after = ws.table_binding(root, workspace_after, alias)
    if control_after != control or ref_after.to_dict() != ref.to_dict() or uri_after != uri:
        c.reject("Table alias changed while its historical version was resolved", code="conflict")
    return VersionResolution(
        alias_binding=_freeze(alias_binding), binding=_freeze(binding),
        reference=_freeze(reference), ref=selected, uri=Path(uri), metadata=_freeze(metadata))