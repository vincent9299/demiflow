"""Atomic admission for the restricted SQL/Arrow broker's one native role.

This ledger describes analysis transfers only; no pipeline run is created.
An operation and its artifact/input reservation commit with the same receipt.
Completion and owned-job cleanup are independent facts. No recovery path starts
or replays an admitted native query/materializer automatically.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import execution_facts
from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.artifacts import _row
from demiflow.workbench.analysis.oci import job_id
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.records import _source
from demiflow.workbench.analysis.records import _time
from demiflow.workbench.analysis.sessions import _execution
from demiflow.workbench.analysis.sessions import _session


@dataclass(frozen=True)
class Scope:
    access: object
    session_id: str
    generation: str
    execution_id: str

    def active(self, db):
        self.access.check("analysis_execute")
        session = _session(db, self.access.project, self.session_id, self.generation)
        execution = _execution(db, self.access.project, self.execution_id)
        if (session["principal"] != self.access.principal or session["state"] != "executing"
                or execution["session_id"] != self.session_id or execution["generation"] != self.generation
                or execution["state"] not in {"dispatched", "running"}):
            c.reject("Broker request is outside its live owning cell/generation", code="conflict")
        return session


@dataclass(frozen=True)
class BootstrapScope:
    access: object
    session_id: str
    generation: str
    execution_id: str

    def active(self, db):
        self.access.check("analysis_execute")
        session=_session(db,self.access.project,self.session_id,self.generation)
        if session["state"]!="admitted" or session["principal"]!=self.access.principal or self.execution_id!=self.session_id:
            c.reject("Fixed input preparation is outside its original session admission",code="conflict")
        return session


def reference(db, project, value):
    c.fields(value, {"id", "generation", "digest"}, label="private table reference")
    object_id(value["id"]); c.integer(value["generation"], "table generation", 2**31-1, 1); c.sha(value["digest"])
    row = _row(db, project, value["id"])
    if row["state"] != "ready" or row["generation"] != value["generation"] or row["manifest_digest"] != value["digest"]:
        c.reject("Table handle expired or its generation/digest differs", code="conflict")
    manifest = json.loads(row["manifest"])
    if manifest["payload"]["format"] != "lance":
        c.reject("This private object is not a table handle", code="operation_rejected")
    return Artifacts._public(row)["handle"]


def operation_payload(operation, value):
    c.encoded(value)
    if operation == "query":
        c.fields(value, {"sql", "sources"}, label="broker SQL")
        c.text(value["sql"], "SQL source", 16384)
        if type(value["sources"]) is not dict or len(value["sources"]) > 16:
            c.reject("SQL supports at most16 explicit source handles")
        for alias, item in value["sources"].items():
            c.sql_alias(alias)
            c.fields(item, {"id", "generation", "digest"}, label="SQL source reference")
            object_id(item["id"]); c.integer(item["generation"], "source generation", 2**31-1, 1); c.sha(item["digest"])
    elif operation == "publish":
        c.fields(value, {"schema_sha256", "stream_sha256", "encoded_bytes", "chunks"}, label="Arrow publish commitment")
        c.sha(value["schema_sha256"]); c.sha(value["stream_sha256"])
        c.integer(value["encoded_bytes"], "Arrow upload bytes", 64*1024*1024, 1)
        c.integer(value["chunks"], "Arrow upload chunks", 4096, 1)
    elif operation=="import":
        c.fields(value,{"source_snapshot"},label="trusted fixed source preparation")
        c.sha(value["source_snapshot"])
    elif operation in {"read", "pin"}:
        c.fields(value, {"handle"}, label="bounded table read")
        c.fields(value["handle"], {"id", "generation", "digest"}, label="table read reference")
        object_id(value["handle"]["id"]); c.integer(value["handle"]["generation"], "handle generation", 2**31-1, 1); c.sha(value["handle"]["digest"])
    else:
        c.reject("Unsupported native data-broker operation")
    return value


class Operations:
    def __init__(self, records):
        self.records, self.database = records, records.database

    def admit(self, scope, *, request_id, operation, payload):
        c.identity(request_id, "client broker request ID")
        operation_payload(operation, payload)
        request_key = "b" + c.digest([scope.access.project, scope.access.principal, scope.session_id, scope.generation, request_id])
        complete_payload = dict(session_id=scope.session_id, generation=scope.generation, execution_id=scope.execution_id,
                                operation=operation, payload=payload)

        def apply(db, now):
            session=scope.active(db)
            binding=None
            if operation=="import":
                if not isinstance(scope,BootstrapScope) or payload["source_snapshot"] not in json.loads(session["sources"]).values():
                    c.reject("Source import is not part of this explicit session admission",code="conflict")
                source=_source(db,scope.access.project,payload["source_snapshot"])
                if (source["kind"]!="fixed_lance" or source["rows"]>100000
                        or any(field["type"] not in {"bool","int64","float64","utf8","binary"} for field in source["schema"])):
                    c.reject("This projection is outside the explicit bounded Arrow import types/rows",code="operation_rejected")
                binding=source["digest"]
            elif isinstance(scope,BootstrapScope):
                c.reject("Session preparation cannot execute arbitrary data operations")
            if execution_facts.pending_operations(db, limit=1):
                c.reject("Native analysis slot is busy or its cleanup is unresolved", code="busy")
            if (db.execute("SELECT count(*) FROM analysis_operations").fetchone()[0] >= 2048
                    or db.execute("SELECT count(*) FROM analysis_operations WHERE project=?", (scope.access.project,)).fetchone()[0] >= 256):
                c.reject("Analysis operation history quota is full", code="resource_limit")
            if operation in {'import','publish'}:
                held=db.execute('SELECT coalesce(sum(held_input_bytes),0) FROM analysis_operations').fetchone()[0]
                held+=db.execute('SELECT coalesce(sum(held_input_bytes),0) FROM analysis_media_captures').fetchone()[0]
                if held+64*1024*1024>64*1024*1024:c.reject('Original analysis transfer reservation is busy',code='busy')
            total=db.execute('SELECT count(*) FROM analysis_operations').fetchone()[0]+db.execute('SELECT count(*) FROM analysis_media_captures').fetchone()[0]
            own=db.execute('SELECT count(*) FROM analysis_operations WHERE project=?',(scope.access.project,)).fetchone()[0]+db.execute('SELECT count(*) FROM analysis_media_captures WHERE project=?',(scope.access.project,)).fetchone()[0]
            if total>=2048 or own>=256:c.reject('Existing analysis operation history quota is full',code='resource_limit')
            parents = []
            from demiflow.workbench.analysis.dependencies import remember
            if operation == "query":
                for item in payload["sources"].values():
                    reference(db, scope.access.project, item)
                    remember(db,scope,handle=item)
                    if item not in parents:
                        parents.append(item)
            elif operation in {"read", "pin"}:
                reference(db, scope.access.project, payload["handle"])
                remember(db,scope,handle=payload["handle"])
                if operation == "pin":
                    parents.append(payload["handle"])
            reservation = None if operation == "read" else Artifacts._allocate(db, scope.access.project,
                execution_id=scope.execution_id, generation=1, retention="pinned" if operation == "pin" else "ephemeral", parents=parents, source_binding=binding)
            identifier = "q" + uuid.uuid4().hex
            input_id = "i" + uuid.uuid4().hex if operation in {"publish","import"} else None
            db.execute("INSERT INTO analysis_operations (project,id,session_id,generation,execution_id,client_request,operation,payload,state,reservation,input_id,held_input_bytes,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (scope.access.project, identifier, scope.session_id, scope.generation, scope.execution_id, request_id, operation,
                 c.encoded(payload), "admitted", None if reservation is None else c.encoded(reservation), input_id,
                 64*1024*1024 if input_id else 0, now, now))
            return dict(operation_id=identifier, state="admitted", execution_started=False, cleanup_proven=False)

        return self.records._mutate(scope.access, "analysis_write" if operation == "pin" else "analysis_execute", request_key, "broker."+operation, complete_payload, apply)

    def receipt(self, access, *, session_id, generation, client_request):
        c.identity(session_id); c.identity(generation); c.identity(client_request)
        access.check("analysis_execute")
        with self.database.locked() as db:
            session = _session(db, access.project, session_id, generation)
            if session["principal"] != access.principal:
                c.reject("Broker session owner differs", code="operation_rejected")
            row = db.execute("SELECT id,state,result,cleanup_proven,reason FROM analysis_operations WHERE project=? AND session_id=? AND generation=? AND client_request=?",
                (access.project, session_id, generation, client_request)).fetchone()
            if row is None:
                c.reject("Original broker receipt is unavailable; do not automatically execute with a new ID", code="not_found")
            access.check("analysis_execute")
            result = dict(operation_id=row["id"], state=row["state"], value=None if row["result"] is None else json.loads(row["result"]),
                          cleanup_proven=bool(row["cleanup_proven"]), reason=row["reason"])
            environment = execution_facts.generation(db, access.project, session_id, generation)
            facts = execution_facts.cleanup(db, access.project, 'native', row['id'], generation)
            if environment is not None:
                result['environment'] = dict(digest=environment['digest'], mode=environment['body']['mode'])
                result['cleanup_complete'] = bool(facts and facts['body']['state'] == 'complete') or (
                    environment['body']['mode'] == 'oci' and result['cleanup_proven'])
                if environment['body']['mode'] == 'trusted_local':
                    result['cleanup_proven'] = False
            if facts is not None:
                result.update(cleanup_receipt=facts['body'], cleanup_receipt_digest=facts['digest'])
            return result

    def system_get(self, identifier):
        c.identity(identifier, "owned analysis operation")
        with self.database.locked() as db:
            row = db.execute("SELECT * FROM analysis_operations WHERE id=?", (identifier,)).fetchone()
            if row is None:
                c.reject("Owned analysis operation is missing", code="not_found")
            result = dict(row)
            for key in ("payload", "reservation", "result", "transfer_metadata"):
                if result[key] is not None:
                    result[key] = json.loads(result[key])
            return result

    def system_import_metadata(self,scope,identifier,metadata):
        if not isinstance(scope,BootstrapScope):
            c.reject("Trusted source transfer requires its internal preparation scope")
        c.fields(metadata,{"source_snapshot","schema_sha256","stream_sha256","encoded_bytes","chunks","rows"},label="trusted source transfer receipt")
        c.sha(metadata["source_snapshot"]);c.sha(metadata["schema_sha256"]);c.sha(metadata["stream_sha256"])
        c.integer(metadata["encoded_bytes"],"source transfer bytes",64*1024*1024,1)
        c.integer(metadata["chunks"],"source transfer chunks",4096,1);c.integer(metadata["rows"],"source transfer rows",100000)
        with self.database.transaction(payload_bytes=4096) as db:
            scope.active(db)
            row=db.execute("SELECT * FROM analysis_operations WHERE project=? AND id=?",(scope.access.project,identifier)).fetchone()
            if row is None or row["operation"]!="import" or row["state"]!="admitted" or row["session_id"]!=scope.session_id or row["generation"]!=scope.generation:
                c.reject("Original source import admission changed",code="conflict")
            source=_source(db,scope.access.project,json.loads(row["payload"])["source_snapshot"])
            if source["digest"]!=metadata["source_snapshot"] or source["rows"]!=metadata["rows"]:
                c.reject("Export receipt belongs to a different fixed input",code="conflict")
            if row["transfer_metadata"] is not None and json.loads(row["transfer_metadata"])!=metadata:
                c.reject("Source transfer commitment cannot change",code="conflict")
            db.execute("UPDATE analysis_operations SET transfer_metadata=?,updated=? WHERE id=?",(c.encoded(metadata),_time(),identifier))
            scope.active(db)

    def system_attach(self, scope, identifier, runtime_job):
        c.identity(identifier); job_id(runtime_job)
        with self.database.transaction(payload_bytes=4096) as db:
            scope.active(db)
            updated = db.execute("UPDATE analysis_operations SET state='created',job_id=?,updated=? WHERE id=? AND project=? AND session_id=? AND generation=? AND execution_id=? AND state='admitted'",
                (runtime_job, _time(), identifier, scope.access.project, scope.session_id, scope.generation, scope.execution_id))
            if updated.rowcount != 1:
                c.reject("Owned native admission changed before attachment", code="conflict")
            scope.active(db)

    def system_finish(self, scope, identifier, value):
        c.encoded(value)
        with self.database.transaction(payload_bytes=65536) as db:
            scope.active(db)
            row = db.execute("SELECT * FROM analysis_operations WHERE id=? AND project=?", (identifier, scope.access.project)).fetchone()
            if row is None or row["execution_id"] != scope.execution_id or row["state"] != "created":
                c.reject("Native result belongs to another admission", code="conflict")
            if row["operation"] == "read":
                if value != {"read": True}:
                    c.reject("Malformed native read completion")
            else:
                reserved = json.loads(row["reservation"])
                if value.get("id") != reserved["id"]:
                    c.reject("Native result object differs from its reservation", code="conflict")
                actual = reference(db, scope.access.project, {key: value[key] for key in ("id", "generation", "digest")})
                if value != actual:
                    c.reject("Native handle differs from its committed manifest", code="conflict")
                if row["operation"] in {"publish","import"}:
                    metadata=json.loads(row["payload"] if row["operation"]=="publish" else row["transfer_metadata"])
                    published=json.loads(_row(db,scope.access.project,actual["id"])["manifest"])
                    if published["payload"]["schema_sha256"]!=metadata["schema_sha256"]:
                        c.reject("Materialized schema digest differs from its admitted Arrow stream",code="conflict")
                if row["operation"]=="import":
                    session=scope.active(db)
                    source=_source(db,scope.access.project,json.loads(row["payload"])["source_snapshot"])
                    if actual["rows"]!=source["rows"] or actual["schema"]!=source["schema"]:
                        c.reject("Materialized input differs from its fixed projection/schema/rows",code="conflict")
                    handle={key:actual[key] for key in ("id","generation","digest")}
                    for alias,digest in json.loads(session["sources"]).items():
                        if digest==source["digest"]:
                            db.execute("INSERT INTO analysis_session_inputs VALUES (?,?,?,?,?,?)",
                                (scope.access.project,scope.session_id,scope.generation,alias,digest,c.encoded(handle)))
            db.execute("UPDATE analysis_operations SET state='succeeded',result=?,updated=? WHERE id=?", (c.encoded(value), _time(), identifier))
            scope.active(db)

    def system_cleanup(self, identifier, *, reason, verify_cleanup):
        c.identity(identifier); c.text(reason, "native termination reason", 256)
        with self.database.transaction(payload_bytes=20*1024) as db:
            row = db.execute("SELECT * FROM analysis_operations WHERE id=?", (identifier,)).fetchone()
            if row is None:
                c.reject("Owned native operation not found", code="not_found")
            environment = execution_facts.generation(db, row['project'], row['session_id'], row['generation'])
            mode = environment['body']['mode'] if environment else 'oci'
            previous = execution_facts.cleanup(db, row['project'], 'native', identifier, row['generation'])
            outcome = verify_cleanup(row['job_id'], row['input_id']) if callable(verify_cleanup) else False
            cleaned = execution_facts.cleanup_complete(outcome, mode=mode, job_id=row['job_id'])
            facts = previous
            if type(outcome) is dict:
                body = outcome.get('cleanup_receipt', outcome)
                if previous is None or previous['body']['state'] != 'complete':
                    facts = execution_facts.save_cleanup(db, project=row['project'], owner_kind='native',
                        owner_id=identifier, generation_id=row['generation'], body=body, now=_time())
            if facts is not None and facts['body']['state'] == 'complete':
                cleaned = True
            if mode == 'oci' and row['cleanup_proven']:
                cleaned = True
            legacy = cleaned and mode == 'oci'
            state = row["state"] if row["state"] == "succeeded" else "unknown"
            db.execute("UPDATE analysis_operations SET state=?,reason=?,cleanup_proven=?,held_input_bytes=CASE WHEN ? THEN 0 ELSE held_input_bytes END,updated=? WHERE id=?",
                       (state, reason, int(legacy), int(cleaned), _time(), identifier))
            result = dict(operation_id=identifier, state=state, cleanup_proven=legacy, automatic_replay=False)
            if environment is not None or facts is not None:
                result['cleanup_complete'] = cleaned
            if facts is not None:
                result.update(cleanup_receipt=facts['body'], cleanup_receipt_digest=facts['digest'])
            return result
