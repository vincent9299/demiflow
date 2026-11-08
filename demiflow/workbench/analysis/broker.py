"""Restricted per-kernel data operations composed with the native lifecycle.

Only handles registered for the live project/cell are resolved. Caller JSON
never supplies a host/guest path, runtime command, environment or provider.
Arrow/Lance decoding stays inside the fixed native worker. The controller
drives these small state machines with nonblocking, quota-bound Unix channels.
"""
from __future__ import annotations

import time

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.operations import BootstrapScope
from demiflow.workbench.analysis.operations import Scope
from demiflow.workbench.analysis.receivers import ObjectReceiver
from demiflow.workbench.analysis.transfers import ExportCopy
from demiflow.workbench.analysis.transfers import Upload
from demiflow.workbench.analysis.outbox import release_from_helper
from demiflow.workbench.analysis.deferred_metadata import Task
from demiflow.workbench.analysis.source_broker import SourceReads
from demiflow.workbench.analysis.source_broker import alias_binding
from demiflow.forge.schema import ForgeError


class Broker:
    def __init__(self, controller):
        self.controller = controller
        self.operations = controller.operations
        self.current = None
        self.source_reads = []

    def _scope(self, owner):
        if owner.get("active_execution") is None:
            c.reject("Data operation requires a live owning Notebook cell", code="conflict")
        return Scope(owner["authority"].access(), owner["session_id"], owner["generation"], owner["active_execution"])

    def request(self, owner, connection, request):
        c.fields(request, {"version", "operation", "request_id", "payload"}, label="restricted data request")
        if type(request["version"]) is not int or request["version"] != 1:
            c.reject("Unsupported data broker protocol")
        request_id = c.identity(request["request_id"])
        operation, payload = request["operation"], request["payload"]
        if operation in {"source-info", "source-sample", "case-source", "handle-info", "table",'bind-table'}:
            if owner.get("active_execution") is None or len(self.source_reads)>=4:
                c.reject("Source reads require a live cell and available bounded reader slot", code="busy")
            if any(item["connection"] is connection for item in self.source_reads):
                c.reject("Original source read is already pending", code="conflict")
            scope = SourceReads(self.controller.records, self.controller.sources, session_id=owner["session_id"],
                generation=owner["generation"], execution_id=owner["active_execution"],bindings=owner["bindings"])
            authority = owner["authority"]
            task = Task(authority.deployment, authority.peer, authority.project, scope, operation, payload,
                expected_principal=authority.principal)
            self.source_reads.append(dict(task=task, owner=owner, connection=connection, execution=owner["active_execution"]))
            connection["phase"]="source_pending"
            return
        scope = self._scope(owner)
        with self.controller.database.locked() as db:
            scope.active(db)
        if operation == "receipt":
            c.fields(payload, {"request_id"}, label="original data operation receipt")
            value = self.operations.receipt(scope.access, session_id=scope.session_id, generation=scope.generation,
                client_request=payload["request_id"])
            self.controller.reply(connection, value)
            return
        c.one_of(operation, {"query", "publish", "read", "pin"}, "restricted data operation")
        if operation == "query":
            c.fields(payload, {"sql", "sources"}, label="SQL request")
            if type(payload["sources"]) is not dict:
                c.reject("SQL source bindings must be explicit aliases")
            payload = dict(sql=payload["sql"], sources={**owner["bindings"], **payload["sources"]})
        admitted = self.operations.admit(scope, request_id=request_id, operation=operation, payload=payload)
        if admitted["replayed"]:
            receipt = self.operations.receipt(scope.access, session_id=scope.session_id, generation=scope.generation, client_request=request_id)
            if operation != "read" and receipt["state"] == "succeeded" and receipt["value"] is not None:
                self.controller.reply(connection, receipt["value"])
                return
            c.reject("Original data operation is " + receipt["state"] + "; inspect its receipt, no automatic replay", code="conflict")
        if self.current is not None:
            # The DB is authoritative; this disagreement is retained/recovered,
            # never used to create a second native worker.
            c.reject("Native in-memory owner disagrees with its admission ledger", code="conflict")
        row = self.operations.system_get(admitted["operation_id"])
        current = dict(row=row, scope=scope, owner=owner, connection=connection, job_id=None, upload=None,
            receiver=None, stream=None, stream_bytes=0, stream_chunks=0, read_started=False,
            result=None, stopped=False, created=time.monotonic(), committed_at=None)
        current["copy"]=None
        current["exchange"]=None
        self.current = current
        connection["phase"] = "upload" if operation == "publish" else "waiting"
        connection["operation_id"] = row["id"]
        try:
            if operation == "publish":
                config = self.config(current)
                current["upload"] = Upload(self.controller.database, self.controller.deployment.paths.state, config,
                    authorize=lambda: self.controller.authorized(owner))
                connection["channel"].queue("json", {"accepted": True})
            else:
                connection["phase"]="waiting"
        except BaseException:
            self.stop(current, reason="native_admission_failed")
            raise

    @staticmethod
    def config(current):
        row = current["row"]
        payload, operation = row["payload"], row["operation"]
        if operation=="import":
            metadata=row["transfer_metadata"]
            if metadata is None:c.reject("Fixed source transfer is not committed",code="conflict")
            return dict(operation="materialize",input_id=row["input_id"],publish_id=row["reservation"]["publish_id"],
                **{key:metadata[key] for key in ("stream_sha256","schema_sha256","encoded_bytes","chunks")})
        if operation == "publish":
            return dict(operation="materialize", input_id=row["input_id"], publish_id=row["reservation"]["publish_id"], **payload)
        if operation == "read":
            return dict(operation="read", object_id=payload["handle"]["id"], transfer_id=row["id"])
        if operation == "pin":
            return dict(operation="query", sql='SELECT * FROM "source"', sources=[dict(alias="source", object_id=payload["handle"]["id"])],
                        publish_id=row["reservation"]["publish_id"])
        return dict(operation="query", sql=payload["sql"],
            sources=[dict(alias=alias, object_id=handle["id"]) for alias, handle in payload["sources"].items()],
            publish_id=row["reservation"]["publish_id"])

    def prepare_source(self,owner,source,origin):
        if self.current is not None:
            if self.current["owner"] is owner:return
            c.reject("Native fixed source preparation is busy",code="busy")
        access=owner["authority"].access()
        scope=BootstrapScope(access,owner["session_id"],owner["generation"],owner["session_id"])
        request_id="import-"+c.digest([scope.session_id,scope.generation,source["digest"]])
        admitted=self.operations.admit(scope,request_id=request_id,operation="import",payload=dict(source_snapshot=source["digest"]))
        if admitted["replayed"]:
            c.reject("Original fixed input preparation is already admitted; reconcile it, no restart",code="conflict")
        row=self.operations.system_get(admitted["operation_id"])
        current=dict(row=row,scope=scope,owner=owner,connection=None,job_id=None,upload=None,copy=None,receiver=None,
            stream=None,stream_bytes=0,stream_chunks=0,read_started=False,result=None,stopped=False,
            created=time.monotonic(),committed_at=None,exchange=None)
        self.current=current
        try:
            current["exchange"]=owner["authority"].begin("export-source",dict(export_id=row["input_id"],
                source_locator=origin["locator"],source_snapshot=source["digest"]))
        except BaseException:
            self.stop(current,reason="fixed_source_preparation_failed")
            raise

    def upload_frame(self, connection, kind, value):
        current = self.current
        if current is None or connection is not current["connection"] or current["upload"] is None:
            c.reject("Arrow upload no longer belongs to its original operation", code="conflict")
        result = current["upload"].consume(kind, value)
        if result is not None:
            connection["phase"] = "waiting"

    def _start(self, current):
        owner, scope, row = current["owner"], current["scope"], current["row"]
        with self.controller.database.locked() as db:
            scope.active(db)
        created = self.controller.runtime.create(project=scope.access.project, execution_id=row["id"], role="native",
            config=self.config(current), web_parent=owner["authority"].peer.identity,
            owner_digest=owner["priority_digest"],origin_digest=self.controller.origin_digest(owner),
            workspace=self.controller.generation_workspace(owner),
            authorize=lambda: self.controller.authorized(owner))
        current["job_id"] = created["job_id"]
        self.operations.system_attach(scope, row["id"], created["job_id"])
        current["row"] = self.operations.system_get(row["id"])
        self.controller.listen_job(created["job_id"], "arrow" if row["operation"] == "read" else "result", current)
        self.controller.runtime.start(created["job_id"], authorize=lambda: self.controller.authorized(owner))

    def result_frame(self, connection, kind, value):
        current = self.current
        if current is None or connection["job_id"] != current["job_id"] or current["row"]["operation"] == "read":
            c.reject("Native result belongs to another operation", code="conflict")
        if current["receiver"] is None:
            def ready(value):
                handle = value["handle"]
                self.operations.system_finish(current["scope"], current["row"]["id"], handle)
                current["result"] = handle
                current["committed_at"] = time.monotonic()
            current["receiver"] = ObjectReceiver(self.controller.artifacts, current["scope"].access,
                self.controller.deployment.paths.state, current["row"]["reservation"],
                execution_id=current["scope"].execution_id, native=True, on_ready=ready)
        reply = current["receiver"].consume(kind, value)
        if reply is not None:
            connection["channel"].queue("json", reply)
            if reply.get("committed"):
                connection["close_after_write"] = True

    def arrow_frame(self, connection, kind, value):
        current = self.current
        if current is None or connection["job_id"] != current["job_id"] or current["row"]["operation"] != "read":
            c.reject("Native Arrow stream belongs to another operation", code="conflict")
        if not current["read_started"]:
            if kind != "json" or value != dict(operation="arrow-read", transfer_id=current["row"]["id"]):
                c.reject("Native Arrow stream header identity differs", code="conflict")
            current["stream"] = connection; current["read_started"] = True
            connection["channel"].queue("json", {"accepted": True})
            current["connection"]["channel"].queue("json", {"accepted": True})
            return
        if kind == "arrow":
            if current["stream_bytes"]+len(value) > 64*1024*1024 or current["stream_chunks"] >= 4096:
                c.reject("Native Arrow stream exceeds its transfer budget", code="resource_limit")
            current["stream_bytes"] += len(value); current["stream_chunks"] += 1
            current["connection"]["channel"].queue("arrow", value)
        elif kind == "end":
            if not current["stream_chunks"]:
                c.reject("Native Arrow stream omitted its schema", code="conflict")
            self.operations.system_finish(current["scope"], current["row"]["id"], {"read": True})
            current["result"] = {"read": True}
            current["committed_at"] = time.monotonic()
            connection["channel"].queue("json", {"read": True}); connection["close_after_write"] = True
            # The kernel's final end waits for actual native cleanup, so the
            # next explicit data request cannot race a still-held native slot.
        else:
            c.reject("Unexpected native Arrow stream frame")

    def read_enabled(self, connection):
        current = self.current
        if connection["kind"] == "arrow" and current is not None:
            destination = current["connection"]["channel"]
            return destination.outgoing_bytes + 1024*1024+5 <= destination.output_limit
        return True

    def disconnected(self, connection):
        for item in list(self.source_reads):
            if item["connection"] is connection:
                item["task"].close(); self.source_reads.remove(item)
        current = self.current
        if current is None:
            return
        if connection is current["connection"]:
            self.stop(current, reason="data_consumer_disconnected")
        elif (current["job_id"] is not None and connection["kind"] in {"result","arrow"}
                and connection["job_id"] == current["job_id"] and current["result"] is None):
            self.stop(current, reason="native_producer_disconnected_before_commit")

    def stop(self, current, *, reason):
        if current["stopped"]:
            return
        current["stopped"] = True
        if current["exchange"] is not None:
            current["exchange"].close(); current["exchange"] = None
        if current["upload"] is not None:
            current["upload"].close()
        if current["copy"] is not None:
            current["copy"].close()
        if current["receiver"] is not None:
            current["receiver"].close()
        if current["job_id"] is not None:
            self.controller.runtime.stop(current["job_id"], reason=reason)
        self.operations.system_cleanup(current["row"]["id"], reason=reason, verify_cleanup=None)

    def tick(self):
        for item in list(self.source_reads):
            task, owner, connection = item["task"], item["owner"], item["connection"]
            try:
                if (connection["channel"].closed or owner.get("active_execution")!=item["execution"]
                        or self.controller.session_phase(owner)!="executing"):
                    c.reject("Original source read cell/generation ended", code="conflict")
                if not task.step(): continue
                if task.operation=='bind-table':
                    alias=alias_binding(owner['bindings'],owner.get('source_aliases',()),task.payload)
                    actual={key:task.value[key] for key in ('id','generation','digest')}
                    if actual!=task.payload['handle']:c.reject('Validated alias handle differs',code='conflict')
                    owner['bindings'][alias]=actual
                self.controller.reply(connection, task.value)
            except (ForgeError, OSError, EOFError) as exc:
                if not connection["channel"].closed:
                    self.controller.reply(connection, dict(error=str(exc)[:1024], code=getattr(exc,"code","operation_failed")))
            else:
                task.close(); self.source_reads.remove(item); continue
            task.close(); self.source_reads.remove(item)
        current = self.current
        if current is None:
            return
        owner = current["owner"]
        if (not owner["authority"].peer.live() or time.monotonic()-current["created"] > 90
                or self.controller.session_phase(owner) in {"stop_requested", "cleanup_pending", "stopped"}):
            self.stop(current, reason="owning_generation_or_deadline_ended")
        if current["exchange"] is not None and not current["stopped"]:
            try:
                exchange = current["exchange"]
                if not exchange.advance():
                    return
                self.operations.system_import_metadata(current["scope"],current["row"]["id"],exchange.value)
                current["row"]=self.operations.system_get(current["row"]["id"])
                current["exchange"]=None
                current["copy"]=ExportCopy(self.controller.database,self.controller.deployment.paths.state,
                    self.controller.deployment.value["site"]["host_transfer_root"],self.config(current),
                    web_uid=owner["authority"].peer.uid,helper_gid=self.controller.deployment.paths.helper_gid,
                    authorize=lambda:self.controller.authorized(owner))
            except BaseException:
                self.stop(current,reason="fixed_source_export_failed")
                raise
        if current["copy"] is not None and not current["stopped"]:
            try:
                copier=current["copy"]
                if not copier.step():return
                copier.close();current["copy"]=None
                site=self.controller.deployment.value["site"]
                release_from_helper(site["host_transfer_root"],project=current["row"]["project"],
                    export_id=current["row"]["input_id"],source_snapshot=current["row"]["payload"]["source_snapshot"],
                    allowed_web_uids=site["allowed_web_uids"],helper_gid=site["helper_gid"],
                    witness=lambda:copier.fd is None and copier.upload.descriptor is None)
            except BaseException:
                self.stop(current,reason="fixed_source_copy_or_materializer_start_failed")
                raise
        identifier = current["job_id"]
        if identifier is None and not current["stopped"]:
            ready=(current["row"]["operation"] in {"query","pin","read"}
                or current["row"]["operation"]=="publish" and current["upload"] is not None and current["upload"].sealed
                or current["row"]["operation"]=="import" and current["exchange"] is None
                    and current["copy"] is None and current["row"]["transfer_metadata"] is not None)
            if ready and not self.controller.control_reply_pending():
                try:self._start(current)
                except BaseException:
                    self.stop(current,reason="native_start_failed");raise
                identifier=current["job_id"]
        if identifier is not None:
            lease = self.controller.runtime.leases[identifier]
            if lease["state"] == "cleanup_removing_cgroup":
                self.controller.clean_job(identifier)
            elif lease["state"] != "cleanup_complete":
                observation=self.controller.runtime.observe(identifier)
                if observation['state']=='unknown':
                    self.stop(current,reason='original_native_observation_unknown')
                    return
                if observation['state']=='alive':
                    if current["committed_at"] is not None and time.monotonic()-current["committed_at"] > 2:
                        self.stop(current, reason="native_did_not_exit_after_committed_result")
                    return
                self.controller.clean_job(identifier)
        elif not current["stopped"]:
            return
        self.controller.clean_operation(current["row"]["id"])
        connection = current["connection"]
        if connection is not None and not connection["channel"].closed:
            if current["result"] is None:
                self.controller.reply(connection, dict(error="Data operation ended without a confirmed result; inspect original request_id="+current["row"]["client_request"], code="operation_failed"))
            else:
                current["scope"].access.check("analysis_execute")
                if current["row"]["operation"] == "read":
                    connection["channel"].queue("end"); connection["close_after_write"] = True
                else:
                    self.controller.reply(connection, current["result"])
        self.current = None
