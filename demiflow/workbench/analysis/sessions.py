"""Bounded Notebook sessions and source-bound execution admission.

These are analysis records, separate from pipeline runs/attempts. Admission,
receipts and queue entries commit together. Dispatch is recorded BEFORE code
is sent; recovery never resends an execution with an uncertain outcome. A
session holds the single kernel slot through stopping and unresolved cleanup.
"""
from __future__ import annotations

import hashlib
import json
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import execution_facts as facts
from demiflow.workbench.analysis.oci import job_id
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.records import _document
from demiflow.workbench.analysis.records import _source
from demiflow.workbench.analysis.records import _time
from demiflow.workbench.analysis.records import _verify


def _session(db, project, identifier, generation=None):
    c.identity(identifier, "Notebook session ID")
    row = db.execute("SELECT * FROM analysis_sessions WHERE project=? AND id=?", (project, identifier)).fetchone()
    if row is None:
        c.reject("Notebook session is not found", code="not_found")
    if generation is not None and row["generation"] != c.identity(generation, "expected kernel generation"):
        c.reject("Kernel generation differs; refresh the original session", code="conflict")
    return row


def _execution(db, project, identifier):
    c.identity(identifier, "Notebook execution ID")
    row = db.execute("SELECT * FROM analysis_executions WHERE project=? AND id=?", (project, identifier)).fetchone()
    if row is None:
        c.reject("Notebook execution is not found", code="not_found")
    return row


def _parent(value):
    c.fields(value, {"pid", "start", "boot"}, label="server-owned Web identity")
    c.integer(value["pid"], "Web PID", 2**31-1, 1)
    c.text(value["start"], "Web start tick", 32); c.text(value["boot"], "host boot ID", 64)
    return value


class Sessions:
    def __init__(self, records):
        self.records, self.database = records, records.database

    @staticmethod
    def _slot(db, *, excluding=None):
        row = db.execute("SELECT id FROM analysis_sessions WHERE state!='stopped' AND (? IS NULL OR id!=?) LIMIT 1", (excluding, excluding)).fetchone()
        if row is not None:
            c.reject("The kernel slot is busy or its cleanup is unresolved", code="busy")

    def _public(self, row):
        result = {key: row[key] for key in ("id", "notebook_id", "notebook_revision", "generation", "state", "created", "updated", "reason")}
        # stopped is reached only through system_lost's full cleanup witness;
        # a requested stop, empty cgroup or gateway exit never sets it alone.
        result = {**result, "sources": json.loads(row["sources"]), "cleanup_proven": row["state"] == "stopped"}
        db = self.database.connection
        environment = facts.generation(db, row['project'], row['id'], row['generation'])
        cleanup = facts.cleanup(db, row['project'], 'session', row['id'], row['generation'])
        if environment is not None:
            result['environment'] = environment
            result['cleanup_proven'] = result['cleanup_proven'] and environment['body']['mode'] == 'oci'
            result['cleanup_complete'] = row['state'] == 'stopped' and (
                cleanup is None and environment['body']['mode'] == 'oci' or cleanup is not None and facts.cleanup_complete(
                    cleanup['body'], mode=environment['body']['mode'], job_id=row['job_id']))
        if cleanup is not None:
            result['cleanup_receipt'] = cleanup['body']
            result['cleanup_receipt_digest'] = cleanup['digest']
        return result

    def _execution_public(self, row):
        result = {key: row[key] for key in ("id", "session_id", "generation", "cell_id", "notebook_revision", "source_sha256",
            "language", "result_name", "state", "output_id", "status", "reason", "created", "updated")}
        environment = facts.generation(self.database.connection, row['project'], row['session_id'], row['generation'])
        if environment is not None:
            # Pages carry a bounded reference, never repeat a16KiB body per cell.
            result['environment'] = dict(digest=environment['digest'], mode=environment['body']['mode'])
        return result

    def start(self, access, *, request_id, notebook_id, revision, sources, web_parent, verify, environment=None):
        c.identity(notebook_id, "Notebook ID"); c.integer(revision, "Notebook revision", 2**31-1, 1)
        if type(sources) is not dict or len(sources) > 16:
            c.reject("Session sources require at most16 explicit alias/snapshot bindings")
        for alias, digest in sources.items():
            c.sql_alias(alias); c.sha(digest, "fixed source snapshot")
        web_parent = _parent(web_parent)
        payload = dict(notebook_id=notebook_id, revision=revision, sources=sources, web_parent=web_parent)
        if environment is not None:
            facts.origin(environment)
            payload['environment_digest'] = c.digest(environment)

        def apply(db, now):
            self._slot(db)
            if (db.execute("SELECT count(*) FROM analysis_sessions").fetchone()[0] >= 128
                    or db.execute("SELECT count(*) FROM analysis_sessions WHERE project=?", (access.project,)).fetchone()[0] >= 32):
                c.reject("Notebook session history quota is full", code="resource_limit")
            _document(db, access.project, "notebook", notebook_id, revision)
            for digest in sources.values():
                _verify(verify, _source(db, access.project, digest))
            identifier, generation = "s" + uuid.uuid4().hex, "g" + uuid.uuid4().hex
            db.execute("INSERT INTO analysis_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (access.project, identifier, access.principal, notebook_id, revision, generation, "admitted", None,
                 c.encoded(sources), c.encoded(web_parent), now, now, None))
            if environment is not None:
                facts.bind_generation(db, project=access.project, session_id=identifier,
                    generation_id=generation, body=environment, now=now)
            return {**self._public(_session(db, access.project, identifier)), "execution_started": False}

        return self.records._mutate(access, "analysis_execute", request_id, "session.start", payload, apply)

    def submit(self, access, *, request_id, session_id, expected_generation, revision, cell_id, source_sha256):
        c.identity(session_id); c.identity(expected_generation); c.identity(cell_id)
        c.integer(revision, "fixed Notebook revision", 2**31-1, 1); c.sha(source_sha256, "submitted cell source")
        payload = dict(session_id=session_id, expected_generation=expected_generation, revision=revision,
                       cell_id=cell_id, source_sha256=source_sha256)

        def apply(db, now):
            session = _session(db, access.project, session_id, expected_generation)
            if session["state"] not in {"admitted", "starting", "idle", "executing"}:
                c.reject("This kernel generation is stopping or unavailable", code="conflict")
            if session["principal"] != access.principal:
                c.reject("Execution belongs to another server principal", code="operation_rejected")
            document = _document(db, access.project, "notebook", session["notebook_id"], revision)["document"]
            cell = next((item for item in document["cells"] if item["id"] == cell_id), None)
            if cell is None or hashlib.sha256(cell["source"].encode("utf-8")).hexdigest() != source_sha256:
                c.reject("Cell source differs from its saved Notebook revision", code="conflict")
            if cell["language"] not in {"sql", "python"}:
                c.reject("Markdown cells are rendered without execution", code="operation_rejected")
            if db.execute("SELECT count(*) FROM analysis_executions WHERE session_id=? AND generation=? AND state='queued'", (session_id, expected_generation)).fetchone()[0] >= 2:
                c.reject("This kernel already has two queued cells", code="busy")
            if (db.execute("SELECT count(*) FROM analysis_executions").fetchone()[0] >= 2048
                    or db.execute("SELECT count(*) FROM analysis_executions WHERE project=?", (access.project,)).fetchone()[0] >= 256):
                c.reject("Notebook execution history quota is full", code="resource_limit")
            identifier = "e" + uuid.uuid4().hex
            db.execute("INSERT INTO analysis_executions (project,id,session_id,generation,cell_id,notebook_revision,source_sha256,language,result_name,state,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (access.project, identifier, session_id, expected_generation, cell_id, revision, source_sha256,
                 cell["language"], cell.get("result_name"), "queued", now, now))
            return {**self._execution_public(_execution(db, access.project, identifier)), "execution_started": False}

        return self.records._mutate(access, "analysis_execute", request_id, "cell.submit", payload, apply)

    def control(self, access, *, request_id, session_id, expected_generation, action, environment=None):
        c.identity(session_id); c.identity(expected_generation)
        c.one_of(action, {"stop", "restart"}, "Notebook session action")
        payload = dict(session_id=session_id, expected_generation=expected_generation, action=action)
        if environment is not None:
            facts.origin(environment)
            payload['environment_digest'] = c.digest(environment)

        def apply(db, now):
            row = _session(db, access.project, session_id, expected_generation)
            if row["principal"] != access.principal:
                c.reject("Session belongs to another server principal", code="operation_rejected")
            if action == "restart":
                if row["state"] != "stopped":
                    c.reject("Stop and confirm original generation cleanup before restarting", code="conflict")
                prior = facts.generation(db, access.project, session_id, expected_generation)
                if prior is not None:
                    cleanup = facts.cleanup(db, access.project, 'session', session_id, expected_generation)
                    if (cleanup is None and prior['body']['mode'] != 'oci' or cleanup is not None and not facts.cleanup_complete(
                            cleanup['body'], mode=prior['body']['mode'], job_id=row['job_id'])):
                        c.reject('Original scoped generation cleanup is unresolved', code='conflict')
                    if environment is None:
                        c.reject('Restart requires fresh server-bound environment facts', code='conflict')
                self._slot(db, excluding=session_id)
                generation = "g" + uuid.uuid4().hex
                db.execute("UPDATE analysis_sessions SET generation=?,state='admitted',job_id=NULL,reason=NULL,updated=? WHERE project=? AND id=?",
                    (generation, now, access.project, session_id))
                if environment is not None:
                    facts.bind_generation(db, project=access.project, session_id=session_id,
                        generation_id=generation, body=environment, now=now)
            elif row["state"] != "stopped":
                db.execute("UPDATE analysis_sessions SET state='stop_requested',reason='user_stop',updated=? WHERE project=? AND id=?", (now, access.project, session_id))
                db.execute("UPDATE analysis_executions SET state='cancelled',status='cancelled',reason='generation_stopping',updated=? WHERE session_id=? AND generation=? AND state='queued'", (now, session_id, expected_generation))
            result = self._public(_session(db, access.project, session_id))
            return {**result, "cleanup_proven": action == "stop" and row["state"] == "stopped" and result['cleanup_proven'],
                    "automatic_replay": False}

        return self.records._mutate(access, "analysis_execute", request_id, "session.control", payload, apply)

    def cancel(self, access, *, request_id, execution_id, expected_generation):
        c.identity(execution_id); c.identity(expected_generation)
        payload = dict(execution_id=execution_id, expected_generation=expected_generation)

        def apply(db, now):
            row = _execution(db, access.project, execution_id)
            session = _session(db, access.project, row["session_id"], expected_generation)
            if row["generation"] != expected_generation or session["principal"] != access.principal:
                c.reject("Execution generation or owner differs", code="conflict")
            if row["state"] == "queued":
                db.execute("UPDATE analysis_executions SET state='cancelled',status='cancelled',updated=? WHERE id=?", (now, execution_id))
            elif row["state"] in {"dispatched", "running"}:
                db.execute("UPDATE analysis_sessions SET state='stop_requested',reason='cell_cancel',updated=? WHERE id=?", (now, session["id"]))
                db.execute("UPDATE analysis_executions SET state='cancelled',status='cancelled',reason='generation_stopping',updated=? WHERE session_id=? AND generation=? AND state='queued'", (now, session["id"], expected_generation))
            return {**self._execution_public(_execution(db, access.project, execution_id)),
                    "accepted": True, "generation_will_stop": row["state"] in {"dispatched", "running"}, "cleanup_proven": False}

        return self.records._mutate(access, "analysis_execute", request_id, "cell.cancel", payload, apply)

    def get(self, access, session_id):
        access.check()
        with self.database.locked() as db:
            result = self._public(_session(db, access.project, session_id))
            access.check()
            return result

    def notebook_sessions(self,access,notebook_id):
        """Explicit history lookup over the existing32-session project quota."""
        access.check();c.identity(notebook_id)
        with self.database.locked() as db:
            _document(db,access.project,'notebook',notebook_id,None)
            rows=db.execute('SELECT id,notebook_revision,generation,state,created,updated FROM analysis_sessions '
                'WHERE project=? AND notebook_id=? ORDER BY created DESC,id DESC LIMIT 33',
                (access.project,notebook_id)).fetchall()
            if len(rows)>32:c.reject('Notebook session history exceeds its original project quota',code='resource_limit')
            result=dict(notebook_id=notebook_id,items=[dict(row) for row in rows])
            c.encoded(result,maximum=56*1024);access.check();return result

    def generations(self,access,session_id):
        """Read immutable execution generations; selecting one never restarts."""
        access.check()
        with self.database.locked() as db:
            session=_session(db,access.project,session_id)
            rows=db.execute('SELECT generation,min(created) first_execution,count(*) execution_count '
                'FROM analysis_executions WHERE project=? AND session_id=? GROUP BY generation '
                'ORDER BY min(sequence) DESC LIMIT 257',(access.project,session_id)).fetchall()
            if len(rows)>256:c.reject('Generation history exceeds the original execution quota',code='resource_limit')
            items=[dict(row,current=row['generation']==session['generation'],
                state=session['state'] if row['generation']==session['generation'] else 'historical') for row in rows]
            if not any(item['current'] for item in items):
                items.insert(0,dict(generation=session['generation'],first_execution=None,execution_count=0,current=True,state=session['state']))
            result=dict(session_id=session_id,items=items)
            c.encoded(result,maximum=56*1024);access.check();return result

    def execution(self, access, execution_id):
        access.check()
        with self.database.locked() as db:
            result = self._execution_public(_execution(db, access.project, execution_id))
            access.check()
            return result

    def environment(self, access, execution_id):
        """Read original generation facts without inspecting its old paths."""
        access.check()
        with self.database.locked() as db:
            row=_execution(db,access.project,execution_id)
            value=facts.generation(db,access.project,row['session_id'],row['generation'])
            access.check()
            return dict(execution_id=execution_id,session_id=row['session_id'],
                generation=row['generation'],environment=value)

    def page(self, access, *, session_id, generation, after=0, limit=16, order="asc"):
        c.identity(generation); c.integer(after, "execution cursor", 2**63-1); c.integer(limit, "execution page", 32, 1)
        c.one_of(order,{"asc","desc"},"execution history order")
        access.check()
        with self.database.locked() as db:
            _session(db, access.project, session_id)
            descending=order=="desc"
            rows = db.execute("SELECT * FROM analysis_executions WHERE project=? AND session_id=? AND generation=? AND sequence"+
                ("<? ORDER BY sequence DESC" if descending else ">? ORDER BY sequence")+" LIMIT ?",
                (access.project, session_id, generation, (after or 2**63-1) if descending else after, limit+1)).fetchall()
            access.check()
            return dict(session_id=session_id, generation=generation,
                        items=[self._execution_public(row) for row in rows[:limit]],
                        next_cursor=rows[limit-1]["sequence"] if len(rows) > limit else None)

    def system_attach(self, access, session_id, generation, identifier):
        job_id(identifier); access.check("analysis_execute")
        with self.database.transaction(payload_bytes=4096) as db:
            row = _session(db, access.project, session_id, generation)
            if row["state"] != "admitted":
                c.reject("Original session admission changed", code="conflict")
            db.execute("UPDATE analysis_sessions SET state='starting',job_id=?,updated=? WHERE id=?", (identifier, _time(), session_id))
            access.check("analysis_execute")

    def system_ready(self, access, session_id, generation, identifier):
        access.check("analysis_execute")
        with self.database.transaction(payload_bytes=4096) as db:
            row = _session(db, access.project, session_id, generation)
            if row["state"] != "starting" or row["job_id"] != job_id(identifier):
                c.reject("Kernel readiness belongs to another admission", code="conflict")
            db.execute("UPDATE analysis_sessions SET state='idle',updated=? WHERE id=?", (_time(), session_id))
            access.check("analysis_execute")

    def dispatch(self, access, session_id, generation):
        """Persist at-most-once dispatch BEFORE returning the source command."""
        access.check("analysis_execute")
        with self.database.transaction(payload_bytes=4096) as db:
            session = _session(db, access.project, session_id, generation)
            if session["state"] != "idle":
                return None
            row = db.execute("SELECT * FROM analysis_executions WHERE session_id=? AND generation=? AND state='queued' ORDER BY sequence LIMIT 1", (session_id, generation)).fetchone()
            if row is None:
                return None
            document = _document(db, access.project, "notebook", session["notebook_id"], row["notebook_revision"])["document"]
            cell = next(item for item in document["cells"] if item["id"] == row["cell_id"])
            now = _time()
            db.execute("UPDATE analysis_executions SET state='dispatched',updated=? WHERE id=?", (now, row["id"]))
            db.execute("UPDATE analysis_sessions SET state='executing',updated=? WHERE id=?", (now, session_id))
            access.check("analysis_execute")
            return dict(operation="execute", execution_id=row["id"], generation=generation, cell_id=row["cell_id"],
                        source=cell["source"], source_sha256=row["source_sha256"], language=row["language"], result_name=row["result_name"])

    def system_event(self, access, *, execution_id, generation, event, output_id=None, status=None, generation_will_stop=False):
        c.identity(execution_id); c.identity(generation)
        c.one_of(event, {"executing", "finished"}, "kernel event")
        if type(generation_will_stop) is not bool:
            c.reject("Kernel stop observation must be boolean")
        access.check("analysis_execute")
        with self.database.transaction(payload_bytes=4096) as db:
            row = _execution(db, access.project, execution_id)
            session = _session(db, access.project, row["session_id"], generation)
            if row["generation"] != generation or row["state"] not in {"dispatched", "running"}:
                c.reject("Late or foreign execution event rejected", code="conflict")
            now = _time()
            if event == "executing":
                db.execute("UPDATE analysis_executions SET state='running',updated=? WHERE id=?", (now, execution_id))
            else:
                object_id(output_id)
                c.one_of(status, {"succeeded", "error", "cancelled", "timeout", "output_limit", "protocol_error", "kernel_dead"}, "execution status")
                output = db.execute("SELECT state,seed,manifest FROM analysis_objects WHERE project=? AND id=?", (access.project, output_id)).fetchone()
                if output is None or output["state"] != "ready":
                    c.reject("Execution output is not durably committed", code="conflict")
                seed, manifest = json.loads(output["seed"]), json.loads(output["manifest"])
                if seed["execution_id"] != execution_id or manifest["payload"].get("mime") != "application/vnd.demiflow.output+json":
                    c.reject("Output artifact belongs to a different execution/type", code="conflict")
                db.execute("UPDATE analysis_executions SET state='finished',output_id=?,status=?,updated=? WHERE id=?", (output_id, status, now, execution_id))
                phase = "cleanup_pending" if generation_will_stop or session["state"] == "stop_requested" else "idle"
                db.execute("UPDATE analysis_sessions SET state=?,updated=? WHERE id=?", (phase, now, session["id"]))
            access.check("analysis_execute")

    def system_lost(self, project, session_id, generation, *, reason, verify_cleanup=None):
        """Owned lifecycle after revocation/disconnect; never a public route.

        Only a trusted runtime cleanup witness may release the kernel slot.
        Missing output or an interrupted dispatch remains unknown, never success.
        """
        c.text(reason, "kernel termination reason", 256)
        with self.database.transaction(payload_bytes=4096) as db:
            row = _session(db, project, session_id, generation)
            environment = facts.generation(db, project, session_id, generation)
            mode = environment['body']['mode'] if environment is not None else 'oci'
            if row['state'] == 'stopped':
                stored = self._public(row)
                result = dict(session_id=session_id, generation=generation,
                    cleanup_proven=stored['cleanup_proven'], automatic_replay=False)
                for key in ('cleanup_complete', 'cleanup_receipt', 'cleanup_receipt_digest'):
                    if key in stored:
                        result[key] = stored[key]
                return result
            outcome = verify_cleanup(row['job_id']) if callable(verify_cleanup) else False
            cleaned = facts.cleanup_complete(outcome, mode=mode, job_id=row['job_id'])
            now = _time()
            proof = outcome.get('cleanup_receipt', outcome) if type(outcome) is dict else None
            if proof is not None:
                facts.save_cleanup(db, project=project, owner_kind='session', owner_id=session_id,
                    generation_id=generation, body=proof, now=now)
            db.execute("UPDATE analysis_executions SET state=CASE WHEN state='queued' THEN 'cancelled' ELSE 'unknown' END,status=CASE WHEN state='queued' THEN 'cancelled' ELSE NULL END,reason=?,updated=? WHERE session_id=? AND generation=? AND state IN ('queued','dispatched','running')", (reason, now, session_id, generation))
            db.execute("UPDATE analysis_sessions SET state=?,reason=?,updated=? WHERE id=?",
                ("stopped" if cleaned else "cleanup_pending", reason, now, session_id))
            result = dict(session_id=session_id, generation=generation, cleanup_proven=cleaned and mode == 'oci', automatic_replay=False)
            if environment is not None or proof is not None:
                result.update(cleanup_complete=cleaned, cleanup_receipt=proof)
            return result
