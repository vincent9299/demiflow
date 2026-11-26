"""Test-only dedicated controller. No kernel, SQL data engine or user code."""
import json
import os
from pathlib import Path
import sys

from demiflow.workbench.analysis.database import Database
from demiflow.workbench.analysis.database import FileSizeGuard
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.sessions import Sessions
from demiflow.workbench.analysis.receivers import ObjectReceiver
from demiflow.workbench.analysis.operations import BootstrapScope
from demiflow.workbench.analysis.operations import Operations
from demiflow.workbench.analysis.operations import Scope
from demiflow.workbench.analysis.transfers import ExportCopy
from demiflow.workbench.analysis.transfers import Upload
from demiflow.workbench.analysis import contracts
from demiflow.forge.schema import ForgeError


def main():
    guard = FileSizeGuard(database_bytes=int(sys.argv[2]), wal_bytes=int(sys.argv[3])).install()
    database = Database(Path(sys.argv[1]), guard=guard)
    records = Records(database)
    artifacts = Artifacts(records)
    sessions = Sessions(records)
    receivers = {}
    operations = Operations(records)
    uploads = {}
    copies = {}
    metadata_tasks = {}
    permissions = {"read", "analysis_write", "analysis_execute", "case_annotate"}
    source_valid = True
    revoke_at_check = 0
    checks = 0

    def authorized(project, principal, permission):
        nonlocal checks
        checks += 1
        return permission in permissions and (not revoke_at_check or checks < revoke_at_check)

    def verify(source, target=None):
        return source_valid

    print(json.dumps({"ready": True}), flush=True)
    try:
        while True:
            line = sys.stdin.buffer.readline(65537)
            if not line:
                break
            if len(line) > 65536:
                raise RuntimeError("test command exceeds limit")
            command = json.loads(line)
            operation = command.pop("operation")
            controls = command.pop("controls", {})
            project = command.pop("project", "w" + "0"*32)
            principal = command.pop("principal", "owner")
            permissions = set(controls.get("permissions", ["read", "analysis_write", "analysis_execute", "case_annotate"]))
            source_valid = controls.get("source_valid", True)
            revoke_at_check = controls.get("revoke_at_check", 0)
            checks = 0
            access = Access(project, principal, authorized)
            try:
                if operation in {"metadata_begin", "metadata_reply"}:
                    # Test-only controlled authority responses. Real Unix
                    # exchange framing/parent fences have separate tests.
                    from demiflow.forge.process import process_identity
                    from demiflow.workbench.analysis import deferred_metadata
                    from demiflow.workbench.analysis.deployment import Deployment
                    from demiflow.workbench.analysis.ipc import Peer
                    from demiflow.workbench.analysis.metadata import Metadata
                    from demiflow.workbench.analysis.sources import Sources
                    class Exchange:
                        def __init__(self, authority, name, payload):
                            self.operation, self.payload = name, payload
                            self.done, self.value, self.closed = False, None, False
                        def advance(self):
                            assert not self.closed
                            return self.done
                        def close(self): self.closed = True
                    deferred_metadata.Exchange = Exchange
                    key = command.pop("key")
                    if operation == "metadata_begin":
                        assert len(metadata_tasks) < 4 and key not in metadata_tasks
                        deployment = Deployment(dict(site=dict(allowed_web_uids=[os.geteuid()])), None, "test_only")
                        peer = Peer(os.getpid(), os.geteuid(), os.getegid(), process_identity(os.getpid()))
                        environment = command.get('server_environment')
                        provider = None if environment is None else lambda authority, body=environment: body
                        metadata=Metadata(records, Sources(records, database.directory.parent), environment=provider)
                        if command.get("source_scope") is not None:
                            from demiflow.workbench.analysis.source_broker import SourceReads
                            metadata=SourceReads(records,metadata.sources,**command["source_scope"])
                        metadata_tasks[key] = deferred_metadata.Task(deployment, peer, project,
                            metadata,
                            command["name"], command["payload"], expected_principal=command.get("expected_principal"))
                    else:
                        task = metadata_tasks[key]
                        assert task.pending is not None
                        task.pending[1].value, task.pending[1].done = command["value"], True
                    task = metadata_tasks[key]
                    complete = task.step()
                    result = dict(done=complete, value=task.value, pending=None if task.pending is None else task.pending[1].operation,
                        payload=None if task.pending is None else task.pending[1].payload,
                        in_transaction=database.connection.in_transaction, locked=database._mutex.locked(),
                        documents=database.connection.execute("SELECT count(*) FROM documents").fetchone()[0],
                        receipts=database.connection.execute("SELECT count(*) FROM operation_receipts").fetchone()[0])
                elif operation == "metadata_dispose":
                    metadata_tasks.pop(command["key"]).close()
                    result=None
                elif operation == "remember_source":
                    # Test-only origin for staged authority/source checks.
                    # It is never exposed by the product metadata/HTTP API.
                    from demiflow.workbench.analysis.sources import Sources
                    from demiflow.workbench.analysis.sources import locator
                    value=records.source(access,command["digest"])
                    result=Sources(records,database.directory.parent)._remember(access,value,
                        dict(kind="external",locator=locator(command["locator"])))
                elif operation == "copy_create":
                    key = command.pop("key")
                    assert len(copies) < 4
                    copies[key] = ExportCopy(database, database.directory.parent, **command,
                        web_uid=os.geteuid(), helper_gid=os.getegid(),
                        authorize=lambda: authorized(access.project, access.principal, "analysis_execute"))
                    result = {"created": True}
                elif operation in {"copy_step", "copy_close"}:
                    copy = copies[command["key"]]
                    if operation == "copy_step":
                        result = {"complete": copy.step(), "chunks": copy.upload.chunks}
                    else:
                        copy.close(); result = {"closed": copy.fd is None and copy.upload.descriptor is None}
                elif operation == "backup":
                    result = database.backup()
                elif operation == "create_crash":
                    records.create_notebook(access, **command)
                    os._exit(71)  # committed receipt, response deliberately lost
                elif operation=="session_inputs":
                    access.check()
                    with database.locked() as db:
                        rows=db.execute("SELECT alias,source_snapshot,handle FROM analysis_session_inputs WHERE project=? AND session_id=? AND generation=? ORDER BY alias",(project,command["session_id"],command["generation"])).fetchall()
                        result=[dict(row)|{"handle":json.loads(row["handle"])} for row in rows]
                elif operation.startswith("ops_"):
                    name = operation.removeprefix("ops_")
                    if name in {"admit", "system_attach", "system_finish", "system_import_metadata"}:
                        constructor=BootstrapScope if command.pop("bootstrap",False) else Scope
                        scope = constructor(access, **command.pop("scope"))
                        if name == "admit":
                            command["operation"] = command.pop("native_operation")
                        result = getattr(operations, name)(scope, **command)
                    elif name == "receipt":
                        result = operations.receipt(access, **command)
                    else:
                        if name == "system_cleanup":
                            command["verify_cleanup"] = lambda job, input_id: (
                                controls['cleanup_result'] if 'cleanup_result' in controls else controls.get('cleanup_proven') is True
                                ) if input_id is None or not (database.directory.parent/'transfer/ingress'/input_id).exists() else False
                        result = getattr(operations, name)(**command)
                elif operation == "upload_create":
                    key = command.pop("key")
                    assert len(uploads) < 4
                    uploads[key] = Upload(database, database.directory.parent, **command,
                        authorize=lambda: authorized(access.project, access.principal, "analysis_execute"))
                    result = {"created": True}
                elif operation == "upload_consume":
                    upload = uploads[command.pop("key")]
                    if command.get("kind") == "arrow":
                        command["value"] = command["value"].encode("utf-8")
                    result = upload.consume(**command)
                elif operation == "object_guard":
                    with database.guard.object_write():
                        try:
                            with database.locked():
                                pass
                        except ForgeError:
                            result = {"sql_rejected": True}
                    database.guard.check()
                elif operation == "object_guard_locked":
                    rejected = []
                    for context in (database.locked(), database.transaction(payload_bytes=1024)):
                        with context:
                            try:
                                with database.guard.object_write():
                                    raise AssertionError("must reject before elevating file limit")
                            except ForgeError:
                                rejected.append(True)
                            database.guard.check()
                    result = {"locked_and_transaction_rejected": rejected == [True, True]}
                elif operation == "receiver_create":
                    key = command.pop("key")
                    assert len(receivers) < 4
                    # Fresh access closure still reads the test control flags
                    # on each subsequent frame/commit, including revocation.
                    receivers[key] = ObjectReceiver(artifacts, access, database.directory.parent, **command, on_ready=lambda ready: None)
                    result = {"created": True}
                elif operation == "receiver_consume":
                    receiver = receivers[command.pop("key")]
                    if command.get("kind") == "blob":
                        command["value"] = command["value"].encode("utf-8")
                    result = receiver.consume(**command)
                elif operation.startswith("session_"):
                    name = operation.removeprefix("session_")
                    if name == "start":
                        command["verify"] = verify
                    if name == "system_lost":
                        command["verify_cleanup"] = lambda identifier: controls['cleanup_result'] if 'cleanup_result' in controls else controls.get("cleanup_proven") is True
                        result = sessions.system_lost(project, **command)
                    else:
                        result = getattr(sessions, name)(access, **command)
                elif operation.startswith("artifact_"):
                    name = operation.removeprefix("artifact_")
                    if name == "commit_ready":
                        def verify_renamed(identifier,digest):
                            path=database.directory.parent/'objects'/identifier/'manifest.json'
                            return path.is_file() and contracts.digest(json.loads(path.read_bytes()))==digest
                        command['verify_renamed']=verify_renamed
                    if name == "system_cleanup_complete":
                        command['verify_cleanup']=lambda publish,identifier: (controls.get('cleanup_proven',False) is True
                            and not (database.directory.parent/'.staging'/publish).exists()
                            and not (database.directory.parent/'objects'/identifier).exists())
                    if name.startswith("system_"):
                        result=getattr(artifacts,name)(project,**command)
                    else:
                        result=getattr(artifacts,name)(access,**command)
                else:
                    if operation in {"register_source", "save_view", "save_annotations"}:
                        command["verify"] = verify
                    result = getattr(records, operation)(access, **command)
                print(json.dumps({"result": result}, ensure_ascii=False, allow_nan=False), flush=True)
            except ForgeError as exc:
                print(json.dumps({"error": str(exc), "code": exc.code}), flush=True)
    finally:
        for receiver in receivers.values():
            receiver.close()
        for upload in uploads.values():
            upload.close()
        for copy in copies.values():
            copy.close()
        for task in metadata_tasks.values():
            task.close()
        database.close()


if __name__ == "__main__":
    main()
