"""The same analysis DB owns reservations and immutable object visibility.

File ingress cannot create ready DB state. Crash/response-loss recovery uses
the original publish/object IDs and manifest; cancellation retains the charge
until the trusted helper proves owned IO and process cleanup. No run ledger is
read, copied or replaced here.
"""
from __future__ import annotations

import json
import uuid

from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.object_files import candidate_manifest

MIB = 1024*1024
RESERVED_BYTES = 128*MIB
RESERVED_ENTRIES = 128


def _row(db, project, identifier):
    object_id(identifier)
    value = db.execute("SELECT * FROM analysis_objects WHERE project=? AND id=?", (project, identifier)).fetchone()
    if value is None:
        c.reject("Private object is not registered for this project", code="not_found")
    return value


class Artifacts:
    def __init__(self, records):
        self.records = records
        self.database = records.database

    @staticmethod
    def _permission(seed):
        return "analysis_write" if seed["retention"] == "pinned" else "analysis_execute"

    def reserve(self, access, *, request_id, execution_id, generation, retention, parents, source_binding):
        c.identity(execution_id, "analysis execution ID")
        c.integer(generation, "output generation", 2**31-1, 1)
        c.one_of(retention, {"ephemeral", "pinned"}, "object retention")
        if source_binding is not None:
            c.sha(source_binding, "source binding")
        for parent in c.sequence(parents, "object parents", 16):
            c.fields(parent, {"id", "generation", "digest"}, label="parent object")
            object_id(parent["id"]); c.integer(parent["generation"], "parent generation", 2**31-1, 1); c.sha(parent["digest"])
        payload = dict(execution_id=execution_id, generation=generation, retention=retention,
                       parents=parents, source_binding=source_binding)
        permission = "analysis_write" if retention == "pinned" else "analysis_execute"

        def apply(db, now):
            return self._allocate(db, access.project, **payload)

        return self.records._mutate(access, permission, request_id, "artifact.reserve", payload, apply)

    @staticmethod
    def _allocate(db, project, *, execution_id, generation, retention, parents, source_binding):
        """Internal allocation in the caller's SAME admission/receipt transaction.

        Callers validate the closed fields before opening their transaction;
        this is not a public operation and never commits independently.
        """
        for parent in parents:
            previous = _row(db, project, parent["id"])
            if previous["state"] != "ready" or previous["generation"] != parent["generation"] or previous["manifest_digest"] != parent["digest"]:
                c.reject("Parent object binding expired or differs", code="conflict")
        totals = db.execute("SELECT coalesce(sum(held_bytes),0),coalesce(sum(held_entries),0) FROM analysis_objects").fetchone()
        held = db.execute("SELECT coalesce(sum(held_bytes),0) FROM analysis_objects WHERE project=?", (project,)).fetchone()[0]
        if totals[0] + RESERVED_BYTES > 2*1024**3 or totals[1] + RESERVED_ENTRIES > 4096 or held + RESERVED_BYTES > 256*MIB:
            c.reject("Private artifact reservation is full; cleanup/retention review required", code="resource_limit")
        identifier, publish = "o" + uuid.uuid4().hex, "p" + uuid.uuid4().hex
        seed = dict(project=project, object_id=identifier, generation=generation, retention=retention,
                    execution_id=execution_id, parents=parents, source_binding=source_binding)
        db.execute("INSERT INTO analysis_objects (project,id,publish_id,generation,execution_id,state,seed,held_bytes,held_entries) VALUES (?,?,?,?,?,?,?,?,?)",
                   (project, identifier, publish, generation, execution_id, "reserved", c.encoded(seed), RESERVED_BYTES, RESERVED_ENTRIES))
        return dict(id=identifier, publish_id=publish, state="reserved", generation=generation,
                    reserved_bytes=RESERVED_BYTES, reserved_entries=RESERVED_ENTRIES, ready=False)

    def receiving(self, access, identifier):
        access.check()
        with self.database.transaction(payload_bytes=65536) as db:
            row = _row(db, access.project, identifier)
            seed = json.loads(row["seed"])
            access.check(self._permission(seed))
            if row["state"] not in {"reserved", "receiving"}:
                c.reject("Object can no longer accept ingress", code="conflict")
            db.execute("UPDATE analysis_objects SET state='receiving' WHERE project=? AND id=?", (access.project, identifier))
            access.check(self._permission(seed))
            return dict(publish_id=row["publish_id"], seed=seed, reserved_bytes=row["held_bytes"], reserved_entries=row["held_entries"])

    def prepared(self, access, identifier, *, manifest, receipt):
        access.check()
        c.encoded(manifest); c.encoded(receipt)
        c.fields(receipt, {"publish_id", "manifest_digest", "encoded_bytes", "filesystem_entries", "charged_bytes", "prepared", "database_ready"}, label="trusted IO preparation receipt")
        c.integer(receipt["charged_bytes"], "physical object charge", RESERVED_BYTES, 1)
        c.integer(receipt["encoded_bytes"], "prepared encoded bytes", 64*MIB)
        c.integer(receipt["filesystem_entries"], "prepared filesystem entries", RESERVED_ENTRIES, 1)
        with self.database.transaction(payload_bytes=65536) as db:
            row = _row(db, access.project, identifier)
            seed = json.loads(row["seed"])
            access.check(self._permission(seed))
            # Reconstruct the sole canonical manifest from the controller seed;
            # producer-supplied authority/project/output IDs are never adopted.
            if type(manifest) is not dict or "payload" not in manifest or "files" not in manifest:
                c.reject("Native object manifest is incomplete")
            expected = candidate_manifest(seed, manifest["payload"], manifest["files"])
            digest = c.digest(expected)
            if (expected != manifest or receipt["publish_id"] != row["publish_id"] or receipt["manifest_digest"] != digest
                    or receipt["prepared"] is not True or receipt["database_ready"] is not False
                    or receipt["encoded_bytes"] != expected["encoded_bytes"] or receipt["filesystem_entries"] != expected["filesystem_entries"]):
                c.reject("IO preparation differs from the reserved object identity", code="conflict")
            if row["state"] == "prepared" and row["manifest_digest"] == digest:
                return dict(id=identifier, manifest_digest=digest, state="prepared", ready=False)
            if row["state"] != "receiving":
                c.reject("Object was cancelled or changed during ingress", code="conflict")
            db.execute("UPDATE analysis_objects SET state='prepared',manifest_digest=?,manifest=?,prepared=? WHERE project=? AND id=?",
                       (digest, c.encoded(expected), c.encoded(receipt), access.project, identifier))
            access.check(self._permission(seed))
            return dict(id=identifier, manifest_digest=digest, state="prepared", ready=False)

    def begin_publish(self, access, identifier, *, expected_digest):
        access.check()
        c.sha(expected_digest, "prepared object digest")
        with self.database.transaction(payload_bytes=4096) as db:
            row = _row(db, access.project, identifier)
            seed = json.loads(row["seed"])
            access.check(self._permission(seed))
            if row["state"] not in {"prepared", "publishing"} or row["manifest_digest"] != expected_digest:
                c.reject("Publication identity/state differs", code="conflict")
            db.execute("UPDATE analysis_objects SET state='publishing' WHERE project=? AND id=?", (access.project, identifier))
            access.check(self._permission(seed))
            return dict(publish_id=row["publish_id"], manifest_digest=expected_digest, state="publishing", ready=False)

    def commit_ready(self, access, identifier, *, expected_digest, verify_renamed):
        access.check()
        c.sha(expected_digest, "renamed object digest")
        with self.database.transaction(payload_bytes=4096) as db:
            row = _row(db, access.project, identifier)
            seed = json.loads(row["seed"])
            access.check(self._permission(seed))
            if row["state"] == "ready" and row["manifest_digest"] == expected_digest:
                return self._public(row)
            if row["state"] != "publishing" or row["manifest_digest"] != expected_digest:
                c.reject("Object was cancelled or changed before ready commit", code="conflict")
            if not callable(verify_renamed) or verify_renamed(identifier, expected_digest) is not True:
                c.reject("Owned object rename/fsync is not confirmed", code="conflict")
            receipt = json.loads(row["prepared"])
            access.check(self._permission(seed))
            db.execute("UPDATE analysis_objects SET state='ready',held_bytes=?,held_entries=? WHERE project=? AND id=?",
                       (receipt["charged_bytes"], receipt["filesystem_entries"], access.project, identifier))
            return self._public(_row(db, access.project, identifier))

    @staticmethod
    def _public(row):
        result = dict(id=row["id"], generation=row["generation"], state=row["state"], ready=row["state"] == "ready",
                      digest=row["manifest_digest"], held_bytes=row["held_bytes"], held_entries=row["held_entries"], reason=row["reason"])
        if result["ready"]:
            manifest = json.loads(row["manifest"])
            if manifest["payload"]["format"] == "lance":
                result["handle"] = dict(id=row["id"], generation=row["generation"], digest=row["manifest_digest"],
                    rows=manifest["payload"]["rows"], schema=manifest["payload"]["schema"], retention=manifest["retention"])
            else:
                result["artifact"] = dict(id=row["id"], generation=row["generation"], digest=row["manifest_digest"],
                    mime=manifest["payload"]["mime"], bytes=manifest["payload"]["bytes"], complete=manifest["payload"]["complete"])
                if manifest["payload"]["format"]=="media":
                    result["artifact"].update(descriptor=manifest["payload"]["descriptor"],source=manifest["payload"]["source"])
        return result

    def status(self, access, identifier):
        access.check()
        with self.database.locked() as db:
            value = self._public(_row(db, access.project, identifier))
            access.check()
            return value

    def system_cleanup_pending(self, project, identifier, *, reason):
        """Internal helper lifecycle, also usable after membership revocation.

        Not a user route. The caller must already hold this owned job lease.
        Ready objects are committed facts and never removed by job cancellation.
        """
        project_identity(project); c.text(reason, "owned cleanup reason", 256)
        with self.database.transaction(payload_bytes=4096) as db:
            row = _row(db, project, identifier)
            if row["state"] in {"ready", "cleanup_complete"}:
                return self._public(row)
            db.execute("UPDATE analysis_objects SET state='cleanup_pending',reason=? WHERE project=? AND id=?", (reason, project, identifier))
            return self._public(_row(db, project, identifier))

    def system_cleanup_complete(self, project, identifier, *, verify_cleanup):
        project_identity(project)
        with self.database.transaction(payload_bytes=4096) as db:
            row = _row(db, project, identifier)
            if row["state"] == "cleanup_complete":
                return self._public(row)
            if row["state"] != "cleanup_pending" or not callable(verify_cleanup) or verify_cleanup(row["publish_id"], identifier) is not True:
                c.reject("Owned process/mount/file cleanup is not proved; reservation retained", code="conflict")
            db.execute("UPDATE analysis_objects SET state='cleanup_complete',held_bytes=0,held_entries=0 WHERE project=? AND id=?", (project, identifier))
            return self._public(_row(db, project, identifier))
