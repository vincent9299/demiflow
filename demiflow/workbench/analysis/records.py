"""Transactional Notebook/view revisions and source-bound human evaluations.

The controller supplies Access and a trusted source verifier. Neither IDs nor
client-provided source manifests confer authorization. The verifier must check
the fixed source (and Case key for annotations) again before commit. Read-only
history remains readable when a source expires; writes never fall back to its
latest version. All mutations and their receipts commit in the same transaction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import hashlib
import json
import sqlite3
import uuid

from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.workbench.analysis import contracts as c


@dataclass(frozen=True)
class Access:
    project: str
    principal: str
    authorize: object

    def check(self, permission="read"):
        project_identity(self.project)
        c.identity(self.principal, "server principal")
        if not callable(self.authorize) or self.authorize(self.project, self.principal, permission) is not True:
            c.reject("Project membership or analysis capability is no longer granted", code="operation_rejected")


def _time():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.loads(value)


def _source(db, project, digest):
    c.sha(digest, "source snapshot")
    row = db.execute("SELECT body FROM source_snapshots WHERE project=? AND digest=?", (project, digest)).fetchone()
    if row is None:
        c.reject("Fixed source is not registered for this project", code="not_found")
    return _json(row[0])


def _verify(callback, source, target=None):
    if not callable(callback) or callback(source, target) is not True:
        c.reject("Fixed source or stable Case key is unavailable; no latest-version fallback", code="conflict")


def _document(db, project, kind, identifier, revision=None):
    c.identity(identifier, kind + " ID")
    if revision is None:
        row = db.execute("SELECT revision FROM documents WHERE project=? AND kind=? AND id=?",
                         (project, kind, identifier)).fetchone()
        if row is None:
            c.reject(kind + " is not found", code="not_found")
        revision = row[0]
    c.integer(revision, "document revision", 2**31 - 1, 1)
    row = db.execute("SELECT revision,digest,body,principal,time FROM document_revisions WHERE project=? AND kind=? AND id=? AND revision=?",
                     (project, kind, identifier, revision)).fetchone()
    if row is None:
        c.reject("Document revision is not found", code="not_found")
    return dict(id=identifier, revision=row["revision"], digest=row["digest"],
                document=_json(row["body"]), principal=row["principal"], time=row["time"])


class Records:
    def __init__(self, database):
        self.database = database

    def _mutate(self, access, permission, request_id, operation, payload, apply):
        access.check(permission)
        c.identity(request_id, "request ID")
        data = c.encoded(payload)
        payload_digest = hashlib.sha256(data).hexdigest()
        # A completed request is a read even when no new storage can be
        # admitted. Recheck under the write lock below for the new-request race.
        with self.database.locked() as db:
            previous = db.execute("SELECT operation,payload_digest,result FROM operation_receipts WHERE project=? AND principal=? AND request_id=?",
                                  (access.project, access.principal, request_id)).fetchone()
            if previous is not None:
                if previous["operation"] != operation or previous["payload_digest"] != payload_digest:
                    c.reject("Request ID already names a different operation or payload", code="conflict")
                access.check(permission)
                return {**self._receipt_scope(db, access, _json(previous["result"])), "replayed": True}
        # The normal per-file WAL limit remains active even when a large
        # document is reconstructed from a small patch inside this transaction.
        reserve = 1024*1024 if operation == "notebook.patch" else (128*1024 if operation == "case_view.save" and payload.get("descriptor", {}).get("version") == 2 else len(data))
        with self.database.transaction(payload_bytes=max(len(data), reserve)) as db:
            previous = db.execute("SELECT operation,payload_digest,result FROM operation_receipts WHERE project=? AND principal=? AND request_id=?",
                                  (access.project, access.principal, request_id)).fetchone()
            if previous is not None:
                if previous["operation"] != operation or previous["payload_digest"] != payload_digest:
                    c.reject("Request ID already names a different operation or payload", code="conflict")
                access.check(permission)
                return {**self._receipt_scope(db, access, _json(previous["result"])), "replayed": True}
            if db.execute("SELECT count(*) FROM operation_receipts").fetchone()[0] >= 16384:
                c.reject("Analysis receipt quota is full; export/archive before new writes", code="resource_limit")
            now = _time()
            result = apply(db, now)
            result = {**result, "request_id": request_id, "operation": operation, "time": now, "replayed": False}
            access.check(permission)
            db.execute("INSERT INTO operation_receipts VALUES (?,?,?,?,?,?,?)",
                       (access.project, access.principal, request_id, operation, payload_digest, c.encoded(result), now))
            return result

    def receipt(self, access, request_id):
        access.check()
        c.identity(request_id, "request ID")
        with self.database.locked() as db:
            row = db.execute("SELECT result FROM operation_receipts WHERE project=? AND principal=? AND request_id=?",
                             (access.project, access.principal, request_id)).fetchone()
            access.check()
            if row is None:
                c.reject("Operation receipt is not found; absence does not justify a new request ID", code="not_found")
            return self._receipt_scope(db, access, _json(row[0]))

    def register_source(self, access, manifest, *, verify):
        """Internal resolver call; no public route accepts this manifest as proof."""
        access.check()
        source = c.source_snapshot(manifest)
        _verify(verify, source)
        data = c.encoded(source)
        with self.database.transaction(payload_bytes=len(data)) as db:
            old = db.execute("SELECT body FROM source_snapshots WHERE project=? AND digest=?",
                             (access.project, source["digest"])).fetchone()
            if old is not None and bytes(old[0]) != data:
                c.reject("Source digest collision", code="conflict")
            if old is None:
                if db.execute("SELECT count(*) FROM source_snapshots").fetchone()[0] >= 8192:
                    c.reject("Source snapshot quota is full", code="resource_limit")
                db.execute("INSERT INTO source_snapshots VALUES (?,?,?)", (access.project, source["digest"], data))
            _verify(verify, source)
            access.check()
        return source

    def source(self, access, digest):
        access.check()
        with self.database.locked() as db:
            result = _source(db, access.project, digest)
            access.check()
            return result

    def create_notebook(self, access, *, request_id, document):
        document = c.notebook(document)

        def apply(db, now):
            if db.execute("SELECT count(*) FROM documents WHERE project=? AND kind='notebook'", (access.project,)).fetchone()[0] >= 32:
                c.reject("Project Notebook quota is full", code="resource_limit")
            return self._put_document(db, access, "notebook", "n" + uuid.uuid4().hex, 0, document, now)

        return self._mutate(access, "analysis_write", request_id, "notebook.create", document, apply)

    @staticmethod
    def _put_document(db, access, kind, identifier, expected, document, now):
        body = c.encoded(document, maximum=1024*1024 if kind == "notebook" else 65536, nodes=8192)
        new_digest = hashlib.sha256(body).hexdigest()
        revision = expected + 1
        if expected == 0:
            db.execute("INSERT INTO documents VALUES (?,?,?,?)", (access.project, kind, identifier, revision))
        else:
            updated = db.execute("UPDATE documents SET revision=? WHERE project=? AND kind=? AND id=? AND revision=?",
                                 (revision, access.project, kind, identifier, expected))
            if updated.rowcount != 1:
                c.reject("Document changed; preserve the draft and refresh", code="conflict")
        db.execute("INSERT INTO document_revisions VALUES (?,?,?,?,?,?,?,?)",
                   (access.project, kind, identifier, revision, new_digest, body, access.principal, now))
        return dict(id=identifier, kind=kind, revision=revision, digest=new_digest)

    def patch_notebook(self, access, *, request_id, notebook_id, expected_revision, patch):
        c.identity(notebook_id, "Notebook ID")
        c.integer(expected_revision, "expected revision", 2**31 - 2, 1)
        payload = dict(id=notebook_id, expected_revision=expected_revision, patch=patch)

        def apply(db, now):
            current = _document(db, access.project, "notebook", notebook_id)
            if current["revision"] != expected_revision:
                c.reject("Notebook changed; preserve the draft and refresh", code="conflict")
            document = c.notebook_patch(current["document"], patch)
            return self._put_document(db, access, "notebook", notebook_id, expected_revision, document, now)

        return self._mutate(access, "analysis_write", request_id, "notebook.patch", payload, apply)

    def notebook(self, access, notebook_id, *, revision=None, offset=0, limit=8):
        access.check()
        c.integer(offset, "cell offset", 64)
        c.integer(limit, "cell page limit", 16, 1)
        with self.database.locked() as db:
            result = _document(db, access.project, "notebook", notebook_id, revision)
            cells = result["document"].pop("cells")
            page = []
            size = 0
            for cell in cells[offset:offset + limit]:
                charge = c.encoded_size(cell) + 1
                if size + charge > 56*1024:
                    break
                page.append(cell); size += charge
            if not page and offset < len(cells):
                c.reject("Notebook cell cannot fit the control page byte budget", code="resource_limit")
            result["document"]["cells"] = page
            end = offset + len(page)
            result.update(total_cells=len(cells), next_offset=end if end < len(cells) else None)
            c.encoded(result)
            access.check()
            return result

    def list_documents(self, access, *, kind, after="", limit=20):
        access.check()
        c.one_of(kind, {"notebook", "case_view"}, "analysis document kind")
        if after:
            c.identity(after, "document cursor")
        c.integer(limit, "document page limit", 32, 1)
        with self.database.locked() as db:
            if kind=="notebook":
                # Extract one <=256B saved title per bounded 1MiB source doc
                # inside SQLite. Do not copy/decode32 full notebooks in this
                # controller merely to populate a name picker.
                rows=db.execute("""SELECT d.id,d.revision,json_extract(CAST(r.body AS TEXT),'$.title') AS title
                    FROM documents d JOIN document_revisions r ON r.project=d.project AND r.kind=d.kind AND r.id=d.id AND r.revision=d.revision
                    WHERE d.project=? AND d.kind=? AND d.id>? ORDER BY d.id LIMIT ?""",
                    (access.project,kind,after,limit+1)).fetchall()
            else:
                rows = db.execute("""SELECT d.id,d.revision,json_extract(CAST(r.body AS TEXT),'$.name') AS name
                    FROM documents d JOIN document_revisions r ON r.project=d.project AND r.kind=d.kind
                        AND r.id=d.id AND r.revision=d.revision
                    WHERE d.project=? AND d.kind=? AND d.id>? ORDER BY d.id LIMIT ?""",
                                  (access.project, kind, after, limit + 1)).fetchall()
            access.check()
            items=[dict(row) for row in rows[:limit]]
            if kind=='case_view':
                for item in items:
                    if item['name'] is None:item.pop('name')
            return dict(items=items, next_after=rows[limit - 1]["id"] if len(rows) > limit else None)

    def save_view(self, access, *, request_id, view_id, expected_revision, descriptor, verify):
        if view_id is not None:
            c.identity(view_id, "view ID")
        c.integer(expected_revision, "expected revision", 2**31 - 2)
        if (view_id is None) != (expected_revision == 0):
            c.reject("New views use no ID and expected revision 0")
        payload = dict(id=view_id, expected_revision=expected_revision, descriptor=descriptor)

        def apply(db, now):
            c.fields(descriptor, {"version", "source_snapshot", "case_key", "fields", "grouping", "filters", "sort", "layout", "questions"}, {"variant_key", "output_groups", "name"}, label="Case view")
            source = _source(db, access.project, descriptor["source_snapshot"])
            c.case_view(descriptor, source)
            _verify(verify, source)
            if view_id is None:
                if db.execute("SELECT count(*) FROM documents WHERE project=? AND kind='case_view'", (access.project,)).fetchone()[0] >= 32:
                    c.reject("Project saved view quota is full", code="resource_limit")
                identifier = "v" + uuid.uuid4().hex
            else:
                current = _document(db, access.project, "case_view", view_id)
                if current["revision"] != expected_revision:
                    c.reject("Saved view changed; preserve the draft", code="conflict")
                identifier = view_id
            for definition in descriptor["questions"]:
                self._definition(db, access.project, definition)
            result = self._put_document(db, access, "case_view", identifier, expected_revision, descriptor, now)
            from demiflow.workbench.analysis.case_groups import register as register_groups
            register_groups(self, db, access, descriptor, source,
                origin=dict(kind="saved_view", reference=identifier+"@"+str(result["revision"])), now=now)
            _verify(verify, source)
            return result

        return self._mutate(access, "analysis_write", request_id, "case_view.save", payload, apply)

    def case_group_bindings(self, access, **payload):
        from demiflow.workbench.analysis.case_groups import bindings
        return bindings(self, access, **payload)

    def _checked_target(self, db, access, source, target):
        binding = self._target(source, target)
        if target["variant"] is not None:
            from demiflow.workbench.analysis.case_groups import require_target
            require_target(db, access, source, target)
        return binding

    def _receipt_scope(self, db, access, result):
        target = result.get("target")
        if result.get("operation") == "annotations.save" and type(target) is dict and target.get("variant") is not None:
            source = _source(db, access.project, target.get("source"))
            self._checked_target(db, access, source, target)
        return result

    def view(self, access, view_id, *, revision=None):
        access.check()
        with self.database.locked() as db:
            result = _document(db, access.project, "case_view", view_id, revision)
            access.check()
            return result

    @staticmethod
    def _definition(db, project, definition):
        c.question(definition)
        body = c.encoded(definition, maximum=16384)
        digest = hashlib.sha256(body).hexdigest()
        old = db.execute("SELECT body FROM question_definitions WHERE project=? AND digest=?", (project, digest)).fetchone()
        if old is None:
            if db.execute("SELECT count(*) FROM question_definitions").fetchone()[0] >= 16384:
                c.reject("Question definition quota is full", code="resource_limit")
            db.execute("INSERT INTO question_definitions VALUES (?,?,?)", (project, digest, body))
        elif bytes(old[0]) != body:
            c.reject("Question definition digest collision", code="conflict")
        return digest

    @staticmethod
    def _target(source, target):
        c.fields(target, {"source", "case_key", "variant", "field"}, label="annotation target")
        if target["source"] != source["digest"]:
            c.reject("Annotation source differs", code="conflict")
        key = c.text(target["case_key"], "stable Case key", 256)
        if source["case_key"]["kind"] == "row_id":
            if not key.isascii() or not key.isdecimal() or (len(key) > 1 and key.startswith("0")) or len(key) > 20 or int(key) >= 2**64:
                c.reject("Lance row_id must be a canonical unsigned 64-bit decimal string")
        for name in ("variant", "field"):
            if target[name] is not None:
                c.text(target[name], "annotation " + name, 256)
        if target["field"] is not None and target["field"] not in {item["name"] for item in source["schema"]}:
            c.reject("Unknown annotation field")
        return (target["source"], key, target["variant"] or "", target["field"] or "")

    @staticmethod
    def _origin(db, access, origin, source, definitions):
        if origin["kind"] == "unsaved_view":
            c.identity(origin["reference"], "unsaved view draft ID")
            return
        if origin["kind"] == "notebook_output":
            from demiflow.workbench.analysis.presentations import require
            require(db,access,origin["reference"],source,definitions)
            return
        identifier, separator, revision = origin["reference"].partition("@")
        if not separator or not revision.isascii() or not revision.isdecimal() or len(revision) > 10 or str(int(revision)) != revision:
            c.reject("Saved view origin must identify view_id@revision")
        view = _document(db, access.project, "case_view", identifier, int(revision))["document"]
        if view["source_snapshot"] != source["digest"] or not definitions <= {c.digest(item) for item in view["questions"]}:
            c.reject("Question definition is not part of the stated saved view revision", code="conflict")

    @staticmethod
    def _media_basis(db,project,sequence):
        if sequence is None:return None
        row=db.execute("SELECT basis FROM annotation_media_basis WHERE project=? AND event_sequence=?",(project,sequence)).fetchone()
        return None if row is None else c.decode(bytes(row[0]),maximum=16*1024)

    def save_annotations(self, access, *, request_id, target, entries, origin, verify,verify_media=None):
        """Atomic question batch. Clearing/reverting appends, never deletes history."""
        payload = dict(target=target, entries=entries, origin=origin)
        c.encoded(payload)
        c.sequence(entries, "annotation entries", 16, minimum=1)
        c.fields(origin, {"kind", "reference"}, label="annotation definition origin")
        c.one_of(origin["kind"], {"unsaved_view", "saved_view", "notebook_output"}, "annotation definition origin")
        c.text(origin["reference"], "definition origin reference", 256)

        def apply(db, now):
            c.fields(target, {"source", "case_key", "variant", "field"}, label="annotation target")
            source = _source(db, access.project, target["source"])
            binding = self._checked_target(db, access, source, target)
            group_media_fields=None
            if target['variant'] is not None:
                from demiflow.workbench.analysis.case_groups import media_fields
                group_media_fields=media_fields(db,access,source,target,origin)
            _verify(verify, source, target)
            result = []
            definitions = set()
            media_sources={}
            for item in entries:
                c.fields(item, {"definition", "expected_revision", "action"}, {"value", "revision","media_basis"}, label="annotation entry")
                definition = self._definition(db, access.project, item["definition"])
                if definition in definitions:
                    c.reject("Repeated question definition in annotation batch")
                definitions.add(definition)
                c.integer(item["expected_revision"], "annotation expected revision", 100)
                params = (access.project, *binding, access.principal, definition)
                head = db.execute("SELECT revision,sequence FROM annotation_heads WHERE project=? AND source=? AND case_key=? AND variant=? AND field=? AND principal=? AND definition=?", params).fetchone()
                revision = head[0] if head is not None else 0
                if revision != item["expected_revision"]:
                    c.reject("Annotation changed; preserve the draft and refresh", code="conflict")
                if revision >= 100:
                    c.reject("Case/question revision quota is full; export/archive before new edits", code="resource_limit")
                action = item["action"]
                media_basis=self._media_basis(db,access.project,head["sequence"] if head else None)
                if action == "set" and set(item) in ({"definition", "expected_revision", "action", "value"},{"definition", "expected_revision", "action", "value","media_basis"}):
                    value = c.answer(item["definition"], item["value"])
                    state = "answered"
                    if "media_basis" in item:media_basis=item["media_basis"]
                elif action == "clear" and set(item) == {"definition", "expected_revision", "action"}:
                    value, state = None, "cleared"
                elif action == "revert" and set(item) == {"definition", "expected_revision", "action", "revision"}:
                    c.integer(item["revision"], "reverted revision", revision, 1)
                    old = db.execute("SELECT sequence,state,value FROM annotation_events WHERE project=? AND source=? AND case_key=? AND variant=? AND field=? AND principal=? AND definition=? AND revision=?", (*params, item["revision"])).fetchone()
                    if old is None:
                        c.reject("Annotation revision is unavailable", code="not_found")
                    state, value = old["state"], _json(old["value"])
                    media_basis=self._media_basis(db,access.project,old["sequence"])
                else:
                    c.reject("Annotation action fields are invalid")
                if media_basis is not None:
                    from demiflow.workbench.analysis.registered_media import require_basis
                    for field,witness in require_basis(db,access,source,target,media_basis).items():
                        if (action=='set' and 'media_basis' in item and group_media_fields is not None
                                and field not in group_media_fields):
                            c.reject('New group evidence is outside its registered members and shared materials',code='conflict')
                        if field in media_sources and media_sources[field]!=witness:c.reject("One annotation batch cannot use different resource bindings for the same field",code="conflict")
                        media_sources[field]=witness
                    if len(media_sources)>16:c.reject("Annotation batch media field budget exceeded",code="resource_limit")
                event = db.execute("INSERT INTO annotation_events (project,source,case_key,variant,field,principal,definition,revision,state,value,origin,time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                   (*params, revision + 1, state, c.encoded(value, maximum=16384), c.encoded(origin), now))
                sequence = event.lastrowid
                if media_basis is not None:db.execute("INSERT INTO annotation_media_basis VALUES (?,?,?)",(access.project,sequence,c.encoded(media_basis,maximum=16*1024)))
                db.execute("INSERT INTO annotation_heads VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(project,source,case_key,variant,field,principal,definition) DO UPDATE SET revision=excluded.revision,sequence=excluded.sequence",
                           (*params, revision + 1, sequence))
                result.append(dict(definition=definition, revision=revision + 1, sequence=sequence, state=state,
                    **({"media_basis":media_basis} if media_basis is not None else {})))
            self._origin(db, access, origin, source, definitions)
            if target["variant"] is not None:
                from demiflow.workbench.analysis.case_groups import require_target
                require_target(db, access, source, target, origin=origin)
            _verify(verify, source, target)
            if media_sources and (not callable(verify_media) or verify_media(source,target,list(media_sources.values())) is not True):
                c.reject("Fixed annotation media/resource authority is unavailable",code="operation_rejected")
            return dict(target=target, heads=result, annotation_watermark=max(item["sequence"] for item in result),
                        definition_origin=origin, view_saved=False)

        return self._mutate(access, "case_annotate", request_id, "annotations.save", payload, apply)

    def annotation_definitions(self, access, *, target, before=None, watermark=None, limit=8):
        """Discover saved answers/definitions without any surviving local view.

        Reuses immutable events and definitions. The cut and newest event per
        definition are chosen before the `before` predicate, so a definition
        cannot reappear on later pages. No new history or mutable ledger.
        """
        access.check();c.integer(limit,"definition history page",8,1)
        if before is not None:c.integer(before,"definition history cursor",2**63-1,1)
        with self.database.locked() as db:
            source=_source(db,access.project,target.get("source") if type(target) is dict else None)
            binding=self._checked_target(db,access,source,target)
            current=db.execute("SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?",(access.project,)).fetchone()[0]
            if watermark is None:watermark=current
            c.integer(watermark,"definition history watermark",current)
            # Existing schema/index, finite DB and query work; no migration or
            # sort over unbounded history. Refuse a costly old cut explicitly.
            remaining=2048
            def budget():
                nonlocal remaining
                remaining-=1
                return int(remaining<0)
            db.set_progress_handler(budget,1000)
            try:
                rows=db.execute("""WITH latest AS (
                    SELECT definition,max(sequence) AS sequence FROM annotation_events
                    WHERE project=? AND source=? AND case_key=? AND variant=? AND field=? AND principal=? AND sequence<=?
                    GROUP BY definition)
                    SELECT e.definition,e.sequence,e.revision,e.state,e.value,e.origin,e.time,q.body
                    FROM latest h JOIN annotation_events e ON e.sequence=h.sequence
                    JOIN question_definitions q ON q.project=e.project AND q.digest=e.definition
                    WHERE h.sequence<? ORDER BY h.sequence DESC LIMIT ?""",
                    (access.project,*binding,access.principal,watermark,before or 2**63-1,limit+1)).fetchall()
            except sqlite3.OperationalError:
                if remaining<0:c.reject("Definition history query exceeds its bounded SQL work policy; narrow the Case/field",code="resource_limit")
                raise
            finally:db.set_progress_handler(None,0)
            items=[];used=0
            for row in rows[:limit]:
                item=dict(definition_digest=row["definition"],definition=_json(row["body"]),
                    head={key:_json(row[key]) if key in {"value","origin"} else row[key] for key in ("sequence","revision","state","value","origin","time")})
                basis=self._media_basis(db,access.project,row["sequence"])
                if basis is not None:item["head"]["media_basis"]=basis
                charge=c.encoded_size(item)
                if used+charge>48*1024:break
                items.append(item);used+=charge
            access.check()
            result=dict(target=target,items=items,annotation_watermark=watermark,
                next_before=items[-1]["head"]["sequence"] if len(rows)>len(items) and items else None)
            c.encoded(result)
            return result

    def annotation_heads(self, access, *, target, definitions, watermark=None):
        """A small consistent page of the current principal's question heads.

        Unknown/new definitions have no saved head. Question bodies supplied
        by a local view remain separate until an explicit view/answer save.
        """
        access.check()
        c.sequence(definitions,"question head page",4,minimum=1)
        for definition in definitions:c.sha(definition,"question definition")
        if len(set(definitions))!=len(definitions):c.reject("Repeated question head")
        with self.database.locked() as db:
            source=_source(db,access.project,target.get("source") if type(target) is dict else None)
            binding=self._checked_target(db,access,source,target)
            current=db.execute("SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?",(access.project,)).fetchone()[0]
            if watermark is None:watermark=current
            c.integer(watermark,"annotation head watermark",current)
            items=[]
            for definition in definitions:
                row=db.execute("SELECT sequence,revision,state,value,origin,time FROM annotation_events WHERE project=? AND source=? AND case_key=? AND variant=? AND field=? AND principal=? AND definition=? AND sequence<=? ORDER BY sequence DESC LIMIT 1",
                    (access.project,*binding,access.principal,definition,watermark)).fetchone()
                head=None if row is None else {**dict(row),"value":_json(row["value"]),"origin":_json(row["origin"])}
                if head is not None:
                    basis=self._media_basis(db,access.project,row["sequence"])
                    if basis is not None:head["media_basis"]=basis
                items.append(dict(definition_digest=definition,head=head))
            access.check()
            result=dict(target=target,items=items,annotation_watermark=watermark)
            c.encoded(result)
            return result

    def annotations(self, access, *, target, definition, before=None, limit=20, watermark=None):
        access.check()
        c.sha(definition, "question definition")
        c.integer(limit, "annotation page limit", 64, 1)
        if before is not None:
            c.integer(before, "history cursor", 2**63 - 1, 1)
        if watermark is not None:
            c.integer(watermark, "annotation watermark", 2**63 - 1)
        with self.database.locked() as db:
            c.fields(target, {"source", "case_key", "variant", "field"}, label="annotation target")
            source = _source(db, access.project, target["source"])
            binding = self._checked_target(db, access, source, target)
            params = (access.project, *binding, access.principal, definition)
            question = db.execute("SELECT body FROM question_definitions WHERE project=? AND digest=?", (access.project, definition)).fetchone()
            if question is None:
                c.reject("Question definition is not found", code="not_found")
            if watermark is None:
                watermark = db.execute("SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?", (access.project,)).fetchone()[0]
            rows = db.execute("SELECT sequence,revision,state,value,origin,time FROM annotation_events WHERE project=? AND source=? AND case_key=? AND variant=? AND field=? AND principal=? AND definition=? AND sequence<=? AND sequence<? ORDER BY sequence DESC LIMIT ?",
                              (*params, watermark, before if before is not None else 2**63 - 1, limit + 1)).fetchall()
            items=[];used=0
            header=dict(target=target,definition=_json(question[0]),definition_digest=definition,annotation_watermark=watermark)
            maximum=56*1024-c.encoded_size(header)
            for row in rows[:limit]:
                item={**dict(row),"value":_json(row["value"]),"origin":_json(row["origin"])}
                basis=self._media_basis(db,access.project,row["sequence"])
                if basis is not None:item["media_basis"]=basis
                charge=c.encoded_size(item)+1
                if used+charge>maximum:
                    if not items:c.reject("One annotation history event exceeds its bounded page",code="resource_limit")
                    break
                items.append(item);used+=charge
            access.check()
            return dict(target=target, definition=_json(question[0]), definition_digest=definition,
                        items=items, next_before=items[-1]["sequence"] if len(rows)>len(items) and items else None,
                        annotation_watermark=watermark)
