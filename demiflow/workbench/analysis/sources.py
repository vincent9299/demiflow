"""Fixed source authority: Web-verified external locators or pinned objects.

No public call registers a URI/manifest/verifier as proof. Source locators are
controller metadata, never mounts. A pinned object's immutable row-ID index
checks Case membership without importing Lance or Arrow in this controller.
"""
from __future__ import annotations

import json
from pathlib import Path

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.artifacts import _row
from demiflow.workbench.analysis.case_index import NAME
from demiflow.workbench.analysis.case_index import contains
from demiflow.workbench.analysis.operations import reference
from demiflow.workbench.analysis.records import _source


def locator(value):
    if type(value) is not dict:
        c.reject("A source needs a fixed table locator")
    kind = c.one_of(value.get("kind"), {"workspace_table", "project_table"}, "external source locator")
    if kind == "workspace_table":
        c.fields(value, {"kind", "alias", "binding", "columns"}, {"version", "alias_binding"}, label="workspace fixed source")
        c.text(value["alias"], "registered source alias", 128)
        if ("version" in value) != ("alias_binding" in value):
            c.reject("Historical source version and alias binding must be provided together")
        if "version" in value:
            c.integer(value["version"], "fixed source version", 2**63 - 1, 1)
            c.sha(value["alias_binding"], "source alias binding")
    else:
        c.fields(value, {"kind", "id", "discovery", "version", "binding", "columns"}, label="recognized project fixed source")
        c.identity(value["id"], "declared source ID"); c.sha(value["discovery"])
        c.integer(value["version"], "fixed Lance version", 2**63-1, 1)
    c.sha(value["binding"])
    columns = c.sequence(value["columns"], "explicit source projection", 32, minimum=1)
    for name in columns:
        c.text(name, "source projection field", 256)
    if len(set(columns)) != len(columns):
        c.reject("Repeated source projection field")
    c.encoded(value)
    return value


class Sources:
    def __init__(self, records, state_root):
        self.records, self.database, self.state_root = records, records.database, Path(state_root)

    def _remember(self, access, source, origin):
        data = c.encoded(origin)
        with self.database.transaction(payload_bytes=65536) as db:
            previous = db.execute("SELECT body FROM source_locators WHERE project=? AND digest=?", (access.project, source["digest"])).fetchone()
            if previous is not None and bytes(previous[0]) != data:
                c.reject("Source authority locator differs from the registered immutable source", code="conflict")
            if previous is None:
                db.execute("INSERT INTO source_locators VALUES (?,?,?)", (access.project, source["digest"], data))
            access.check()
        return source

    def register(self, authority, value):
        value = locator(value)
        access = authority.access(); access.check()
        resolved = authority.resolve_source(value)
        c.fields(resolved, {"source", "locator"}, label="trusted fixed source resolution")
        if resolved["locator"] != value or type(resolved["source"]) is not dict:
            c.reject("Trusted source resolution differs from the requested fixed locator", code="conflict")
        source = c.source_snapshot({key: item for key, item in resolved["source"].items() if key != "digest"})
        if (source != resolved["source"] or source["kind"] != "fixed_lance" or source["binding"] != value["binding"]
                or [item["name"] for item in source["schema"]] != value["columns"]):
            c.reject("Trusted source projection/binding differs", code="conflict")
        registered = self.records.register_source(access, {key: item for key, item in source.items() if key != "digest"},
            verify=lambda candidate, target=None: authority.verify_source(candidate, target, locator=value))
        return self._remember(access, registered, dict(kind="external", locator=value))

    def private_source(self, access, handle):
        access.check()
        with self.database.locked() as db:
            actual = reference(db, access.project, handle)
            row = _row(db, access.project, handle["id"])
            manifest = json.loads(row["manifest"])
            if actual["retention"] != "pinned" or NAME not in manifest["files"]:
                c.reject("Case requires explicit pin and a verified native Case index", code="operation_rejected")
            source = c.source_snapshot(dict(kind="pinned_object", binding=actual["digest"], schema=manifest["payload"]["schema"],
                rows=actual["rows"], case_key=dict(kind="row_id", field=None), retention="pinned"))
        origin = dict(kind="private", handle=handle)
        registered = self.records.register_source(access, {key: value for key, value in source.items() if key != "digest"},
            verify=lambda candidate, target=None: self._verify_private(access.project, candidate, origin, target))
        return self._remember(access, registered, origin)

    def _origin(self, project, digest):
        # This can be used inside a Records transaction: no recursive mutex or
        # new transaction. The dedicated controller is the sole DB thread.
        row = self.database.connection.execute("SELECT body FROM source_locators WHERE project=? AND digest=?", (project, digest)).fetchone()
        if row is None:
            c.reject("Fixed source authority locator is unavailable", code="conflict")
        return json.loads(row[0])

    def verify(self, authority, source, target=None):
        access = authority.access(); access.check()
        if target is not None and target.get("variant") is not None:
            from demiflow.workbench.analysis.case_groups import require_target
            # Existing verification callbacks can already hold the non-reentrant
            # DB lock. Resolve on that same controller connection, then verify
            # the physical row through the original authority with no variant.
            require_target(self.database.connection, access, source, target)
            target = {**target, "variant":None, "field":None}
        origin = self._origin(access.project, source["digest"])
        if origin["kind"] == "external":
            return authority.verify_source(source, target, locator=origin["locator"])
        return self._verify_private(access.project, source, origin, target)

    def _verify_private(self, project, source, origin, target):
        if source["kind"] != "pinned_object" or origin["kind"] != "private":
            return False
        db = self.database.connection
        actual = reference(db, project, origin["handle"])
        row = _row(db, project, actual["id"])
        manifest = json.loads(row["manifest"])
        expected = c.source_snapshot(dict(kind="pinned_object", binding=actual["digest"], schema=manifest["payload"]["schema"],
            rows=actual["rows"], case_key=dict(kind="row_id", field=None), retention="pinned"))
        if expected != source or NAME not in manifest["files"] or actual["retention"] != "pinned":
            return False
        if target is None:
            return True
        self.records._target(source, target)
        if target["variant"] is not None:
            # A row ID already identifies a physical variant row. Free-form
            # extra variant identities require a registered row-value proof;
            # an arbitrary label is never accepted as that proof.
            c.reject("This source has no registered separate variant-key proof", code="operation_rejected")
        return contains(self.state_root/"objects"/actual["id"]/"data.lance"/NAME,
            entry=manifest["files"][NAME], rows=source["rows"], schema_sha256=manifest["payload"]["schema_sha256"], row_id=target["case_key"])

    def media_sources(self,authority,source,case_key,fields):
        """Current trusted resource proof through the existing bounded Web reader."""
        from demiflow.workbench.analysis.registered_media import source_witness
        access=authority.access();access.check()
        origin=self._origin(access.project,source['digest'])
        if origin['kind']!='external':
            c.reject('This fixed source has no declared public resource-root authority',code='operation_rejected')
        c.sequence(fields,'fixed media fields',16,minimum=1)
        if any(field not in {item['name'] for item in source['schema']} for field in fields) or len(set(fields))!=len(fields):
            c.reject('Media field is outside the fixed source projection',code='conflict')
        value=authority.call('media-sources',dict(source_locator=origin['locator'],source_snapshot=source['digest'],case_key=case_key,fields=fields))
        c.fields(value,{'source_snapshot','case_key','fields'},label='trusted fixed media resource proof')
        c.sequence(value['fields'],'trusted current media field proofs',16,minimum=1)
        if value['source_snapshot']!=source['digest'] or value['case_key']!=case_key or len(value['fields'])!=len(fields):
            c.reject('Current media resource proof differs from its original source/Case',code='conflict')
        for field,item in zip(fields,value['fields']):
            c.fields(item,{'source','mime'},label='trusted field resource proof');source_witness(item['source'])
            if item['source']['source_snapshot']!=source['digest'] or item['source']['case_key']!=case_key or item['source']['field']!=field:
                c.reject('Current media proof belongs to another field',code='conflict')
            from demiflow.workbench.analysis.media_artifact_worker import MIMES
            c.one_of(item['mime'],MIMES,'declared resource MIME')
        c.encoded(value,maximum=40*1024);access.check();return value['fields']

    def verify_media(self,authority,source,target,witnesses):
        from demiflow.workbench.analysis.registered_media import source_witness
        c.sequence(witnesses,'annotation media resource witnesses',16,minimum=1)
        for item in witnesses:source_witness(item)
        actual=self.media_sources(authority,source,target['case_key'],[item['field'] for item in witnesses])
        return [item['source'] for item in actual]==witnesses

    def registered(self, access, digest):
        access.check()
        with self.database.locked() as db:
            source = _source(db, access.project, digest)
            origin = self._origin(access.project, digest)
            access.check()
            return source, origin
