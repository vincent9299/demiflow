"""Single-thread external analysis controller: records, broker and owned jobs.

The host supervisor, administrator binding and independent watchdog are required
before constructing this object. No endpoint can submit an OCI bundle/command,
credential, filesystem root or arbitrary verifier. This is an analysis service,
not a second pipeline scheduler or run ledger.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import selectors
import time

from demiflow.resources.process_observation import unit_stopped
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import execution_facts
from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.deferred_metadata import Task as MetadataTask
from demiflow.workbench.analysis.broker import Broker
from demiflow.workbench.analysis.output_reads import OutputReads
from demiflow.workbench.analysis.media_captures import MediaCaptures
from demiflow.workbench.analysis.cleanup import incomplete_object
from demiflow.workbench.analysis.cleanup import remove_tree
from demiflow.workbench.analysis.database import Database
from demiflow.workbench.analysis.database import FileSizeGuard
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.trusted_binding import TrustedBinding
from demiflow.workbench.analysis.trusted_binding import admitted
from demiflow.workbench.analysis.execution_backend import OciBackend
from demiflow.workbench.analysis.execution_backend import capabilities
from demiflow.workbench.analysis.execution_backend import cleanup_receipt
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.ipc import Listener
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.metadata import Metadata
from demiflow.workbench.analysis.metadata import SPECS
from demiflow.workbench.analysis.operations import Operations
from demiflow.workbench.analysis.receivers import ObjectReceiver
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.workbench.analysis.sources import Sources
from demiflow.workbench.analysis.transfers import remove_input
from demiflow.workbench.analysis.outbox import release_from_helper
from demiflow.workbench.analysis.watchdog_service import WatchClient
from demiflow.workbench.analysis.priority_control import NAME as PRIORITY_OWNER
from demiflow.workbench.analysis.priority_control import load as load_priority_owner
from demiflow.workbench.analysis.priority_control import publish as publish_priority_owner


class Controller:
    def __init__(self, deployment):
        if not admitted(deployment):
            c.reject("Analysis controller requires its admitted external deployment")
        self.deployment = deployment
        self.poll = selectors.DefaultSelector()
        self.listeners, self.connections, self.sessions = {}, {}, {}
        self.database = self.runtime = self.public = None
        self.watchdog = None
        self._recovery = None
        self.running = False
        try:
            # Full rootfs/release/site validation occurs in Deployment.load,
            # before DB state or a public listener is created.
            trusted=isinstance(deployment,TrustedBinding)
            guard = FileSizeGuard(preserve_hard_limit=trusted).install()
            self.database = Database(deployment.paths.state/"database", guard=guard)
            self.records = Records(self.database)
            self.artifacts, self.operations = Artifacts(self.records), Operations(self.records)
            self.sources = Sources(self.records, deployment.paths.state)
            self.metadata = Metadata(self.records, self.sources, environment=self.execution_environment,
                reader_environment=self.execution_environment)
            if trusted:
                from demiflow.workbench.analysis.trusted_local import TrustedLocalBackend
                self.runtime=TrustedLocalBackend(deployment.local_site)
            else:
                self.watchdog = WatchClient(deployment)
                if not self.watchdog.status():
                    c.reject("Independent watchdog is not ready", code="operation_rejected")
                self.runtime = OciBackend(Runtime(deployment, register_watch=self.watchdog.register),self.watchdog)
            self.last_watchdog_check = time.monotonic()
            self.broker = Broker(self)
            self.output_reads=OutputReads(self)
            self.media_captures=MediaCaptures(self)
            self.metadata.media_available=lambda:self.media_captures.current is None and self._recovery is None
            self.metadata.execution_available=lambda:self._recovery is None
            self._recover()
            self.public = Listener(deployment.value["site"]["helper_socket_path"], public=True)
            self._listener(self.public, "control", None, None)
            self.running = True
        except BaseException:
            self.close()
            raise

    def _listener(self, listener, kind, identifier, owner):
        fd = listener.socket.fileno()
        value = dict(listener=listener, kind=kind, job_id=identifier, owner=owner)
        self.listeners[fd] = value
        self.poll.register(listener.socket, selectors.EVENT_READ, ("listener", fd))

    def listen_job(self, identifier, kind, owner):
        name = {"kernel": "kernel.sock", "data": "data.sock", "output": "output.sock", "result": "result.sock", "arrow": "arrow.sock", "output-read":"output-read.sock","case-read":"case-read.sock","media-probe":"media-probe.sock"}[kind]
        listener = Listener(self.runtime.root/identifier/"broker"/name)
        self._listener(listener, kind, identifier, owner)
        try:
            self.runtime.register_bridge(identifier,listener.path)
        except BaseException:
            fd=listener.socket.fileno();self.poll.unregister(listener.socket)
            self.listeners.pop(fd,None);listener.close();raise

    def _accept(self, listener):
        connection, _ = listener["listener"].socket.accept()
        kind, identifier = listener["kind"], listener["job_id"]
        limit = 6 if kind == "control" else 4 if kind == "data" else 1
        if len(self.connections) >= 16 or sum(item["kind"] == kind for item in self.connections.values()) >= limit:
            connection.close(); return
        try:
            allowed = set(self.deployment.value["site"]["allowed_web_uids"]) if kind == "control" else {os.geteuid()}
            peer = Peer.capture(connection, allowed_uids=allowed)
            if identifier is not None:
                if not self.runtime.accept_peer(identifier,peer,init_only=kind!='data'):
                    c.reject("Broker peer is outside its original owned job", code="operation_rejected")
            channel = Channel(connection, binary=kind in {"data", "output", "result", "arrow", "output-read","case-read","media-probe"})
            item = dict(channel=channel, peer=peer, kind=kind, job_id=identifier, owner=listener["owner"],
                phase="header", receiver=None, close_after_write=False, registered=True)
            fd = connection.fileno()
            self.connections[fd] = item
            self.poll.register(connection, selectors.EVENT_READ, ("connection", fd))
        except BaseException:
            connection.close()
            raise

    def reply(self, connection, value):
        connection["channel"].queue("json", value)
        connection["close_after_write"] = True

    def _close_connection(self, fd):
        item = self.connections.pop(fd, None)
        if item is None:
            return
        channel = item["channel"]
        if item["registered"]:
            self.poll.unregister(channel.connection)
        if item["receiver"] is not None:
            item["receiver"].close()
        if item.get("metadata_task") is not None:
            item["metadata_task"].close()
        channel.close()
        if item["kind"] == "kernel":
            owner = item["owner"]
            if owner["connection"] == fd:
                owner["connection"] = None
                owner["gateway_lost"] = True
        if hasattr(self, "broker"):
            self.broker.disconnected(item)
        if hasattr(self,"output_reads"):self.output_reads.disconnected(item)
        if hasattr(self,"media_captures"):self.media_captures.disconnected(item)

    def _close_bridges(self, identifier):
        for fd, item in list(self.connections.items()):
            if item["job_id"] == identifier:
                self._close_connection(fd)
        for fd, item in list(self.listeners.items()):
            if item["job_id"] == identifier:
                self.poll.unregister(item["listener"].socket)
                item["listener"].close(); del self.listeners[fd]
        return not any(item["job_id"] == identifier for item in self.connections.values())

    def _refresh_interest(self, fd, item):
        channel = item["channel"]
        readable = not item["close_after_write"] and self.broker.read_enabled(item)
        if item["kind"] == "data" and item["phase"] == "waiting":
            # Keep reading one frame to notice EOF or reject a pipelined second
            # command. The request's response still has the same owning scope.
            readable = True
        events = (selectors.EVENT_READ if readable else 0) | (selectors.EVENT_WRITE if channel.outgoing else 0)
        if not events:
            if item["registered"]:
                self.poll.unregister(channel.connection); item["registered"] = False
        elif item["registered"]:
            self.poll.modify(channel.connection, events, ("connection", fd))
        else:
            self.poll.register(channel.connection, events, ("connection", fd)); item["registered"] = True

    def authorized(self, owner):
        authority = owner["authority"]
        try:
            allowed=authority.peer.live() and authority.check(authority.project, authority.principal, "analysis_execute")
            if not allowed:
                logging.getLogger(__name__).warning('Notebook execute permission denied session=%s generation=%s',
                    owner['session_id'],owner['generation'])
            return allowed
        except (ForgeError, OSError) as exc:
            logging.getLogger(__name__).warning('Notebook execute permission unavailable session=%s generation=%s code=%s detail=%s',
                owner['session_id'],owner['generation'],getattr(exc,'code',type(exc).__name__),str(exc)[:1024],exc_info=True)
            return False

    def reader_environment(self):
        """Current fixed reader facts, separate from any old producer."""
        if isinstance(self.deployment,TrustedBinding):
            return execution_facts.origin(self.deployment.local_site.origin())
        # These are the admitted OCI worker's namespace identity/cwd. Its
        # helper release and profile remain fixed by the original Deployment.
        return execution_facts.origin(dict(format='demiflow_analysis_oci_origin_v1',mode='oci',
            deployment_id='oci-'+c.digest(self.deployment.value)[:32],
            helper_sha256=self.deployment.value['owner_release']['helper_sha256'],
            profile_sha256=self.deployment.value['profile_sha256'],uid=0,gid=0,cwd='/work',
            capabilities=capabilities('oci')))

    def execution_environment(self, authority):
        return self.deployment.environment(authority) if isinstance(self.deployment,TrustedBinding) else self.reader_environment()

    def origin_digest(self, owner):
        with self.database.locked() as db:
            value=execution_facts.generation(db,owner['authority'].project,owner['session_id'],owner['generation'])
        if value is None:
            if self.runtime.mode!='oci':
                c.reject('Trusted generation has no original environment facts',code='conflict')
            return c.digest(self.reader_environment())
        if value['body']['mode']!=self.runtime.mode:
            c.reject('Original generation belongs to a different execution mode',code='conflict')
        return value['digest']

    def generation_workspace(self, owner):
        with self.database.locked() as db:
            value=execution_facts.generation(db,owner['authority'].project,owner['session_id'],owner['generation'])
        if value is None:
            if self.runtime.mode!='oci':c.reject('Trusted generation workspace is missing',code='conflict')
            return None
        return Path(value['body']['cwd']) if value['body']['mode']=='trusted_local' else None

    def control_reply_pending(self):
        return any(item["kind"]=="control" and item["channel"].outgoing for item in self.connections.values())

    def session_phase(self, owner):
        with self.database.locked() as db:
            row = db.execute("SELECT state,generation FROM analysis_sessions WHERE project=? AND id=?", (owner["authority"].project, owner["session_id"])).fetchone()
            return "stopped" if row is None or row["generation"] != owner["generation"] else row["state"]

    def _control(self, connection, request):
        c.fields(request, {"version", "project", "operation", "payload"}, label="analysis helper request")
        if type(request["version"]) is not int or request["version"] != 1:
            c.reject("Unsupported analysis helper protocol")
        operation, payload = request["operation"], request["payload"]
        if operation == "status":
            if request["project"] is not None:
                c.reject("Helper deployment status has no project scope")
            c.fields(payload, set(), label="helper status")
            if self.runtime.mode=='trusted_local':
                from demiflow.workbench.process import process_identity
                origin=self.reader_environment()
                self.reply(connection,dict(format='demiflow_analysis_helper_status_v2',mode='production',
                    active=True,execution_mode='trusted_local',helper_identity=process_identity(os.getpid()),
                    code_digest=origin['code_digest'],python_digest=origin['python_digest'],
                    environment_digest=origin['environment_digest'],policy_digest=c.digest(origin['limits']),
                    capabilities=self.runtime.describe(),features=dict(case_media=True,annotation_media_basis=True,
                        case_output_groups=True,case_review_filter=True,notebook_history=True)))
                return
            self.reply(connection, dict(format="demiflow_analysis_helper_status_v1", mode=self.deployment.mode,
                active=self.deployment.mode == "production", helper_sha256=self.deployment.value["owner_release"]["helper_sha256"],
                profile_sha256=self.deployment.value["profile_sha256"],features=dict(case_media=True,annotation_media_basis=True,case_output_groups=True,case_review_filter=True,notebook_history=True)))
            return
        # Calibration is a separate explicitly owned command path, never a Web
        # grant to arbitrary Notebook code before production Review.
        calibration_id=None
        if self.deployment.mode != 'production':
            calibration_id='w'+self.deployment.value['owner_release']['helper_sha256'][:32]
            if request['project']!=calibration_id:
                c.reject('Calibration only accepts its fixed owned fixture project',code='operation_rejected')
        c.one_of(operation, SPECS, "analysis operation")
        if connection.get("metadata_task") is not None:
            c.reject("Original metadata request is already pending; no second request", code="conflict")
        connection["metadata_task"] = MetadataTask(self.deployment, connection["peer"], request["project"],
            self.metadata, operation, payload, expected_principal=None if self.deployment.mode == "production"
                else "calibration-"+calibration_id[1:])
        connection["phase"] = "metadata_pending"

    def _metadata_complete(self, connection, task):
        authority, result, operation = task.authority, task.value, task.operation
        if self.deployment.mode != 'production':
            calibration_id='w'+self.deployment.value['owner_release']['helper_sha256'][:32]
            if authority.principal!='calibration-'+calibration_id[1:]:
                c.reject('Calibration requires its explicitly owned fixture principal',code='operation_rejected')
        if operation=='read-cancel':
            if self.runtime.mode!='trusted_local':
                c.reject('Managed read cancellation requires its trusted backend',code='operation_rejected')
            current=self.output_reads.current
            if current is not None and current['id']==result['read_request_id']:
                original=current['authority']
                if (original.project!=authority.project or original.principal!=authority.principal
                        or original.peer.identity!=authority.peer.identity):
                    c.reject('Read cancellation belongs to another original owner',code='operation_rejected')
                self.output_reads.stop(current,'original_consumer_cancelled')
            self.reply(connection,dict(format='demiflow_analysis_managed_read_stop_v1',**result,
                accepted=True,execution_mode='trusted_local',cleanup_complete=False,cleanup_proven=False))
            return
        if (operation in {"output","output-original","output-media","artifact-media"}
                or operation=="case-page" and result.get("format")=="demiflow_private_case_read_v1"):
            self.output_reads.admit(connection,authority,operation,task.payload,result)
            return
        if operation=='media-capture' and result['status']=='pending' and not result.get('replayed'):
            self.media_captures.admit(connection,authority,result);return
        if operation in {"session-start", "session-control"} and result["state"] == "admitted" and not result["replayed"]:
            identifier = result["id"]
            owner = dict(authority=authority, session_id=identifier, generation=result["generation"], job_id=None,
                connection=None, bindings={}, active_execution=None, created=time.monotonic(), last_used=time.monotonic(),
                cancel_at=None, last_grant_check=0, gateway_lost=False, kernel_started_at=None, execution_started_at=None,
                priority_digest=None, priority_checked=0)
            self.sessions[identifier] = owner
            try:
                owner["priority_digest"] = publish_priority_owner(self.runtime.root, authority, identifier, result["generation"])
                bound = self.runtime.owner("bind-owner", owner["priority_digest"], record=load_priority_owner(self.runtime.root))
                if not bound["bound"] or bound["stop_requested"]:
                    c.reject("Independent generation stop fence is not ready", code="operation_rejected")
            except BaseException:
                self._stop_session(owner, "priority_owner_not_ready")
                raise
        self.reply(connection, result)

    def _tick_metadata(self):
        for item in list(self.connections.values()):
            task = item.get("metadata_task")
            if task is None or item["close_after_write"]:
                continue
            try:
                if task.step():
                    self._metadata_complete(item, task)
                    task.close(); item["metadata_task"] = None
            except (ForgeError, OSError, EOFError) as exc:
                task.close(); item["metadata_task"] = None
                self.reply(item, dict(error=str(exc)[:1024], code=getattr(exc, "code", "operation_rejected")))

    def _kernel(self, connection, kind, event):
        if kind != "json":
            c.reject("Kernel gateway only accepts bounded control messages")
        owner = connection["owner"]
        if connection["job_id"] != owner["job_id"]:
            c.reject("Kernel connection belongs to another generation", code="conflict")
        if owner["connection"] not in {None, connection["channel"].connection.fileno()}:
            c.reject("Original kernel gateway connection already exists", code="conflict")
        owner["connection"] = connection["channel"].connection.fileno()
        access = owner["authority"].access()
        if type(event) is not dict:
            c.reject("Malformed kernel gateway event")
        if event.get("event") == "ready":
            c.fields(event, {"event", "session_id", "generation", "cancel_mode", "idle_ttl_s", "lifetime_s"}, label="kernel readiness")
            if (event["session_id"] != owner["session_id"] or event["generation"] != owner["generation"]
                    or event["cancel_mode"] != "generation_stops" or event["idle_ttl_s"] != 600 or event["lifetime_s"] != 3600):
                c.reject("Kernel readiness differs from its fixed admission", code="conflict")
            self.metadata.sessions.system_ready(access, owner["session_id"], owner["generation"], owner["job_id"])
            return
        if event.get("execution_id") != owner["active_execution"] or event.get("generation") != owner["generation"]:
            c.reject("Kernel event belongs to another execution/generation", code="conflict")
        if event["event"] == "executing":
            c.fields(event, {"event", "execution_id", "generation"}, label="kernel execution start")
            self.metadata.sessions.system_event(access, execution_id=event["execution_id"], generation=event["generation"], event="executing")
        elif event["event"] == "finished":
            c.fields(event, {"event", "execution_id", "generation", "status", "output", "cleanup_proven", "generation_will_stop"}, label="kernel execution completion")
            c.fields(event["output"], {"committed", "output_id", "sha256"}, label="committed kernel output")
            if event["output"]["committed"] is not True or event["cleanup_proven"] is not False:
                c.reject("Kernel cannot assert host cleanup or uncommitted output")
            c.sha(event["output"]["sha256"])
            with self.database.locked() as db:
                output = db.execute("SELECT manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'", (access.project, event["output"]["output_id"])).fetchone()
                if output is None or json.loads(output[0])["payload"].get("sha256") != event["output"]["sha256"]:
                    c.reject("Kernel completion output byte receipt differs", code="conflict")
            self.metadata.sessions.system_event(access, execution_id=event["execution_id"], generation=event["generation"], event="finished",
                output_id=event["output"]["output_id"], status=event["status"], generation_will_stop=event["generation_will_stop"])
            owner["active_execution"] = None; owner["last_used"] = time.monotonic()
        elif event["event"] in {"interrupting", "rejected"}:
            # Cancellation is already durable in the session record. These
            # gateway observations never free a lease or finish an execution.
            c.encoded(event)
        else:
            c.reject("Unsupported kernel gateway event")

    def _output(self, connection, kind, value):
        owner = connection["owner"]
        execution_id = owner["active_execution"]
        if execution_id is None:
            c.reject("Notebook output has no current owning execution", code="conflict")
        if connection["receiver"] is None:
            request_id = "u" + hashlib.sha256((execution_id+"/notebook-output").encode()).hexdigest()
            reservation = self.artifacts.reserve(owner["authority"].access(), request_id=request_id, execution_id=execution_id,
                generation=1, retention="ephemeral", parents=[], source_binding=None)
            connection["receiver"] = ObjectReceiver(self.artifacts, owner["authority"].access(), self.deployment.paths.state,
                reservation, execution_id=execution_id, native=False, on_ready=lambda ready: None)
        reply = connection["receiver"].consume(kind, value)
        if reply is not None:
            connection["channel"].queue("json", reply)
            if reply.get("committed"):
                connection["close_after_write"] = True

    def _frame(self, connection, kind, value):
        peer = connection["peer"]
        if not peer.live():
            c.reject("Original analysis peer is gone", code="operation_rejected")
        if connection["kind"] == "control":
            if kind != "json":
                c.reject("Analysis control accepts JSON only")
            if connection["phase"] != "header":
                c.reject("Original control request is already pending", code="conflict")
            self._control(connection, value)
        elif connection["kind"] == "kernel":
            self._kernel(connection, kind, value)
        elif connection["kind"] == "output":
            self._output(connection, kind, value)
        elif connection["kind"] in {"output-read","case-read","media-probe"}:
            self.output_reads.frame(connection,kind,value)
        elif connection["kind"] == "result":
            self.broker.result_frame(connection, kind, value)
        elif connection["kind"] == "arrow":
            self.broker.arrow_frame(connection, kind, value)
        elif connection["phase"] == "upload":
            self.broker.upload_frame(connection, kind, value)
        elif connection["phase"] == "header" and kind == "json":
            self.broker.request(connection["owner"], connection, value)
        else:
            c.reject("Data operation is already in flight; no pipelined second request", code="conflict")

    def _prepare_session(self, owner):
        if self.broker.current is not None and self.broker.current["owner"] is owner:
            return
        authority = owner["authority"]
        preparation = owner.setdefault("preparation", dict(verified=set(), exchange=None, digest=None))
        if preparation["exchange"] is not None:
            exchange = preparation["exchange"]
            if not exchange.advance():
                return
            preparation["exchange"] = None
            if exchange.value != {"verified": True}:
                c.reject("Kernel fixed source is unavailable", code="conflict")
            preparation["verified"].add(preparation["digest"])
            preparation["digest"] = None
        with self.database.locked() as db:
            row = db.execute("SELECT sources FROM analysis_sessions WHERE project=? AND id=? AND generation=? AND state='admitted'",
                (authority.project, owner["session_id"], owner["generation"])).fetchone()
            if row is None:
                c.reject("Original kernel admission is unavailable", code="conflict")
            sources = json.loads(row["sources"])
            owner['source_aliases']=tuple(sources)
            inputs={row["alias"]:dict(source=row["source_snapshot"],handle=json.loads(row["handle"])) for row in db.execute(
                "SELECT alias,source_snapshot,handle FROM analysis_session_inputs WHERE session_id=? AND generation=?",(owner["session_id"],owner["generation"]))}
        for alias, digest in sources.items():
            source, origin = self.sources.registered(authority.access(), digest)
            if origin["kind"] == "external" and digest not in preparation["verified"]:
                preparation["digest"] = digest
                preparation["exchange"] = authority.begin("verify-source", dict(locator=origin["locator"], source=source, target=None))
                return
            if origin["kind"] == "private":
                if not self.sources._verify_private(authority.project, source, origin, None):
                    c.reject("Kernel pinned source is unavailable", code="conflict")
                owner["bindings"][alias] = origin["handle"]
            elif any(field["type"] not in {"bool","int64","float64","utf8","binary"} for field in source["schema"]):
                # Complex fields remain fixed registered sources accessible
                # through source(alias).sample()/case_view(source(alias)).
                # They are never cast to SQL strings or passed through the
                # five-type Arrow importer under a misleading table handle.
                continue
            elif alias in inputs:
                if inputs[alias]["source"]!=digest:
                    c.reject("Prepared session input source differs",code="conflict")
                owner["bindings"][alias]=inputs[alias]["handle"]
            else:
                self.broker.prepare_source(owner,source,origin)
                return
        created = self.runtime.create(project=authority.project, execution_id=owner["session_id"], role="kernel",
            config=dict(session_id=owner["session_id"], generation=owner["generation"]), web_parent=authority.peer.identity,
            owner_digest=owner["priority_digest"],origin_digest=self.origin_digest(owner),
            workspace=self.generation_workspace(owner),
            authorize=lambda: self.authorized(owner))
        owner["job_id"] = created["job_id"]
        for kind in ("kernel", "output", "data"):
            self.listen_job(created["job_id"], kind, owner)
        self.metadata.sessions.system_attach(authority.access(), owner["session_id"], owner["generation"], created["job_id"])
        self.runtime.start(created["job_id"], authorize=lambda: self.authorized(owner))
        owner["kernel_started_at"] = time.monotonic()

    def _stop_session(self, owner, reason):
        owner.setdefault('stop_reason',reason)
        preparation = owner.get("preparation")
        if preparation is not None and preparation["exchange"] is not None:
            preparation["exchange"].close(); preparation["exchange"] = None
        if self.broker.current is not None and self.broker.current["owner"] is owner:
            self.broker.stop(self.broker.current, reason=reason)
        identifier = owner["job_id"]
        if identifier is not None:
            self.runtime.stop(identifier, reason=reason)
        self.metadata.sessions.system_lost(owner["authority"].project, owner["session_id"], owner["generation"], reason=reason)

    def _tick_sessions(self):
        for identifier, owner in list(self.sessions.items()):
            phase = self.session_phase(owner)
            now = time.monotonic()
            if (phase not in {"cleanup_pending", "stopped"} and owner["priority_digest"] is not None
                    and now-owner["priority_checked"] > .5):
                status = self.runtime.owner("owner-status", owner["priority_digest"])
                owner["priority_checked"] = now
                if not status["bound"] or status["stop_requested"]:
                    self._stop_session(owner, "independent_generation_stop"); phase = "cleanup_pending"
            if now-owner["last_grant_check"] > 1:
                owner["last_grant_check"] = now
                if not self.authorized(owner):
                    self._stop_session(owner, "analysis_permission_revoked")
                    phase = "cleanup_pending"
            if owner["gateway_lost"] or now-owner["created"] > 3600:
                self._stop_session(owner, "kernel_gateway_or_lifetime_ended"); phase = "cleanup_pending"
            if phase == "admitted":
                if self.control_reply_pending():
                    continue  # send the durable admission receipt before preparation IO
                try:
                    self._prepare_session(owner)
                except (ForgeError, OSError) as exc:
                    logging.getLogger(__name__).warning('Notebook preparation failed session=%s generation=%s code=%s detail=%s',
                        identifier,owner['generation'],getattr(exc,'code',type(exc).__name__),str(exc)[:1024])
                    self._stop_session(owner, "kernel_preparation_"+getattr(exc, "code", type(exc).__name__))
                continue
            if (phase == "starting" and now-(owner["kernel_started_at"] or owner["created"]) > 20
                    or phase == "idle" and now-owner["last_used"] > 600
                    or phase == "executing" and now-(owner["execution_started_at"] or now) > 125):
                self._stop_session(owner, "kernel_startup_or_idle_timeout"); phase = "cleanup_pending"
            connection = self.connections.get(owner["connection"])
            if phase == "idle" and connection is not None:
                command = self.metadata.sessions.dispatch(owner["authority"].access(), identifier, owner["generation"])
                if command is not None:
                    owner["active_execution"] = command["execution_id"]
                    owner["execution_started_at"] = time.monotonic()
                    connection["channel"].queue("json", command)
            if phase == "stop_requested":
                if owner["cancel_at"] is None:
                    owner["cancel_at"] = now
                    if connection is not None:
                        command = dict(operation="cancel", execution_id=owner["active_execution"], generation=owner["generation"]) if owner["active_execution"] else dict(operation="stop", generation=owner["generation"])
                        connection["channel"].queue("json", command)
                if now-owner["cancel_at"] >= 2:
                    self._stop_session(owner, "requested_generation_stopped"); phase = "cleanup_pending"
            job = owner["job_id"]
            if job is not None:
                lease = self.runtime.leases[job]
                if lease["state"] == "cleanup_complete":
                    phase = "cleanup_pending"
                elif lease["state"] == "cleanup_removing_cgroup":
                    self.clean_job(job); phase = "cleanup_pending"
                else:
                    observation=self.runtime.observe(job)
                    if observation['state']=='unknown':
                        self._stop_session(owner,'original_job_observation_unknown');phase='cleanup_pending'
                    elif observation['state']=='exited':
                        self.clean_job(job)
                        phase = "cleanup_pending"
            if phase == "cleanup_pending":
                # Also find a create that failed after lease persistence but
                # before its ID was attached to the Notebook record.
                for job, lease in list(self.runtime.leases.items()):
                    if lease["execution_id"] == identifier:
                        if lease["state"] != "cleanup_complete":
                            self.runtime.stop(job, reason="original_generation_cleanup")
                        self.clean_job(job)
                self._clean_session_outputs(owner["authority"].project, identifier, owner["generation"])
                result = self.metadata.sessions.system_lost(owner["authority"].project, identifier, owner["generation"],
                    reason=owner.get('stop_reason','owned_generation_ended'), verify_cleanup=lambda original_job: self._scope_cleanup(
                        owner['authority'].project,'session',identifier,owner['generation'],original_job))
                if result.get('cleanup_complete',result['cleanup_proven']):
                    if owner["priority_digest"] is not None:
                        self._clear_priority_owner(owner["priority_digest"],owner)
                    del self.sessions[identifier]

    def _clear_priority_owner(self, digest, original=None):
        if original is None or not original.get('priority_unlinked',False):
            if c.digest(load_priority_owner(self.runtime.root)) != digest:
                c.reject("Original priority owner changed before cleanup", code="conflict")
            result = self.runtime.owner("clear-owner", digest)
            if result["bound"]:
                c.reject("Independent generation owner still has unresolved watches", code="busy")
            if c.digest(load_priority_owner(self.runtime.root)) != digest:
                c.reject("Priority owner changed during cleanup", code="conflict")
            (self.runtime.root/PRIORITY_OWNER).unlink()
            if original is not None:original['priority_unlinked']=True
        fd = os.open(self.runtime.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _runtime_removed(self, identifier):
        runtime=self.runtime.runtime
        lease = runtime.leases[identifier]
        group = lease["cgroup"]
        runtime.delegation(lease)
        if group is not None:
            present = runtime.leaf(lease)
            if present and runtime.groups.populated(identifier, group["cgroup_identity"]):
                return False
        elif not runtime.groups.absent(identifier):
            return False
        return unit_stopped(runtime.root/identifier/"startup.lock", lease["unit_identity"]) and not runtime._listed(lease)

    def _clean_private_io(self, identifier):
        lease = self.runtime.leases[identifier]
        witness = lambda: self._runtime_removed(identifier)
        owned = self.runtime.root/identifier
        if any(item["job_id"] == identifier for item in self.connections.values()):
            return False
        for name in ("broker", "job", "bundle"):
            path = owned/name
            if name == "broker" and path.exists():
                # Only sockets created by our listeners can have existed here;
                # Listener.close already removed the exact original inodes.
                if any(path.iterdir()):
                    c.reject("Owned broker has unresolved socket/files", code="conflict")
            remove_tree(path, maximum_entries=4, maximum_bytes=128*1024, witness=witness)
        return all(not (owned/name).exists() for name in ("broker", "job", "bundle")) and witness()

    def clean_job(self, identifier):
        result = self.runtime.cleanup(identifier, close_bridges=self._close_bridges,
            clean_private_io=self._clean_private_io if self.runtime.mode=='oci' else None)
        if not execution_facts.cleanup_complete(result,mode=self.runtime.mode,job_id=identifier):
            c.reject('Original owned job cleanup remains pending',code='busy')
        return result

    def _anchor_clean(self, anchor):
        if any(value["execution_id"] == anchor and value["state"] != "cleanup_complete" for value in self.runtime.leases.values()):
            return False
        # Used as a read-only witness INSIDE an existing Records transaction.
        # The sole controller thread must not recursively acquire its mutex.
        self.database.guard.check()
        return not execution_facts.pending_operations(self.database.connection,session_id=anchor,limit=1)

    def _scope_cleanup(self,project,kind,identifier,generation,job_id):
        # All process and filesystem cleanup happens before this process-free
        # witness is called from the existing records transaction.
        db=self.database.connection
        environment=(execution_facts.generation(db,project,identifier,generation)
            if kind=='session' else execution_facts.generation(db,project,
                db.execute('SELECT session_id FROM analysis_operations WHERE project=? AND id=?',
                    (project,identifier)).fetchone()[0],generation))
        mode=environment['body']['mode'] if environment else 'oci'
        if mode!=self.runtime.mode:return False
        complete=self._anchor_clean(identifier)
        proof=cleanup_receipt(job_id=job_id,mode=mode,state='complete' if complete else 'pending',
            processes=complete,descendants=complete,channels=complete,temporary=complete,
            reason='original_owner_cleanup' if complete else 'original_owner_cleanup_pending')
        proof['evidence_digest']=c.digest(dict(owner_kind=kind,owner_id=identifier,generation=generation,
            jobs=sorted(key for key,value in self.runtime.leases.items() if value['execution_id']==identifier),
            resources_clean=complete))
        return dict(cleanup_complete=complete,cleanup_proven=complete and mode=='oci',cleanup_receipt=proof)

    def _clean_session_outputs(self, project, session_id, generation):
        if not self._anchor_clean(session_id):
            c.reject("Original generation still has an owned process/transfer", code="busy")
        with self.database.locked() as db:
            rows = db.execute("SELECT a.id FROM analysis_objects a JOIN analysis_executions e ON a.execution_id=e.id WHERE a.project=? AND e.session_id=? AND e.generation=? AND a.state NOT IN ('ready','cleanup_complete') LIMIT 33",
                (project, session_id, generation)).fetchall()
        if len(rows) > 32:
            c.reject("Unready output count exceeds global artifact reservation bound", code="resource_limit")
        witness = lambda: self._anchor_clean(session_id)
        for row in rows:
            identifier = row["id"]
            self.artifacts.system_cleanup_pending(project, identifier, reason="original_generation_output_cleanup")
            incomplete_object(self.artifacts, self.deployment.paths.state, project, identifier, witness=witness)
            self.artifacts.system_cleanup_complete(project, identifier,
                verify_cleanup=lambda publish, obj: witness() and not (self.deployment.paths.state/".staging"/publish).exists()
                    and not (self.deployment.paths.state/"objects"/obj).exists())

    def clean_operation(self, identifier):
        row = self.operations.system_get(identifier)
        for job, lease in list(self.runtime.leases.items()):
            if lease["execution_id"] == identifier:
                if lease["state"] != "cleanup_complete":
                    self.runtime.stop(job, reason="original_native_operation_cleanup")
                self.clean_job(job)
        witness = lambda: self._anchor_clean(identifier)
        if not witness():
            c.reject("Owned native cleanup is unresolved", code="busy")
        if row["input_id"] is not None:
            remove_input(self.deployment.paths.state, row["input_id"], processes_and_mounts_clean=witness)
            if row["operation"]=="import":
                site=self.deployment.value["site"]
                release_from_helper(site["host_transfer_root"],project=row["project"],export_id=row["input_id"],
                    source_snapshot=row["payload"]["source_snapshot"],allowed_web_uids=site["allowed_web_uids"],
                    helper_gid=site["helper_gid"],witness=witness)
        if row["reservation"] is not None:
            object_id = row["reservation"]["id"]
            state = self.artifacts.system_cleanup_pending(row["project"], object_id, reason="owned_native_ended")
            if not state["ready"]:
                incomplete_object(self.artifacts, self.deployment.paths.state, row["project"], object_id, witness=witness)
                self.artifacts.system_cleanup_complete(row["project"], object_id,
                    verify_cleanup=lambda publish, obj: witness() and not (self.deployment.paths.state/".staging"/publish).exists()
                        and not (self.deployment.paths.state/"objects"/obj).exists())
        return self.operations.system_cleanup(identifier, reason="owned_native_cleanup_confirmed",
            verify_cleanup=lambda job, ingress: self._scope_cleanup(row['project'],'native',identifier,row['generation'],job)
                if witness() and (ingress is None or not (self.deployment.paths.state/'transfer/ingress'/ingress).exists()) else False)

    def _recover(self):
        # Capture only startup owners once. Later ticks never adopt an old
        # kernel or rescan/replay historical cell, SQL, export or process work.
        if len(self.runtime.leases)>32:c.reject('Original runtime owner count exceeds32',code='resource_limit')
        with self.database.locked() as db:
            operations = execution_facts.pending_operations(db,limit=2)
            sessions = db.execute("SELECT project,id,generation FROM analysis_sessions WHERE state!='stopped' LIMIT 2").fetchall()
        if len(operations) > 1 or len(sessions) > 1:
            c.reject("Analysis persisted owners exceed the fixed role admission", code="conflict")
        from demiflow.workbench.analysis import read_control
        read_path=self.runtime.root/read_control.NAME
        read_owner=read_control.load(self.runtime.root) if read_path.exists() else None
        priority=load_priority_owner(self.runtime.root) if (self.runtime.root/PRIORITY_OWNER).exists() else None
        self._recovery=dict(jobs={key:self._recovery_job_key(value) for key,value in self.runtime.leases.items()},
            operations=[row['id'] for row in operations],sessions=[dict(row) for row in sessions],
            media=self.media_captures.pending_ids() if hasattr(self,'media_captures') else [],
            read=None if read_owner is None else dict(owner=read_owner,digest=c.digest(read_owner),owner_unlinked=False),
            priority=None if priority is None else dict(digest=c.digest(priority),priority_unlinked=False),
            next_at=0.,delay=.1,cursor=0,notices=0)
        c.encoded(self._recovery,maximum=48*1024)
        for row in sessions:
            self.metadata.sessions.system_lost(row["project"], row["id"], row["generation"], reason="controller_recovery_no_replay",
                verify_cleanup=None)
        for identifier,lease in self.runtime.leases.items():
            if lease['state']!='cleanup_complete':
                try:self.runtime.stop(identifier,reason='controller_recovery_no_replay')
                except (ForgeError,OSError):pass # Retry only this original lease below.
        self._tick_recovery()

    @staticmethod
    def _recovery_job_key(lease):
        return c.digest({key:lease[key] for key in ('job_id','role','project','execution_id',
            'owner_digest','origin_digest','controller','init_identity') if key in lease})

    def _tick_recovery(self):
        pending=self._recovery
        if pending is None or time.monotonic()<pending['next_at']:return
        progressed=False
        def attempt(action):
            try:return action()
            except (ForgeError,OSError) as error:
                if pending['notices']<16:
                    pending['notices']+=1
                    logging.getLogger(__name__).warning('Original startup cleanup remains pending: %s',str(error)[:512])
                return False
        identifiers=list(pending['jobs'])
        if identifiers:
            start=pending['cursor']%len(identifiers)
            selected=(identifiers+identifiers)[start:start+min(4,len(identifiers))]
            pending['cursor']=(start+len(selected))%len(identifiers)
            for identifier in selected:
                def clean(identifier=identifier):
                    lease=self.runtime.leases.get(identifier)
                    if lease is None or self._recovery_job_key(lease)!=pending['jobs'][identifier]:
                        c.reject('Original recovery lease is unavailable or changed',code='conflict')
                    if lease['state']!='cleanup_complete':self.runtime.stop(identifier,reason='controller_recovery_no_replay')
                    self.clean_job(identifier)
                    return True
                if attempt(clean):pending['jobs'].pop(identifier);progressed=True
        if not pending['jobs'] and pending['read'] is not None and hasattr(self,'output_reads'):
            if attempt(lambda:self.output_reads.recover(pending['read'])):
                pending['read']=None;progressed=True
        for identifier in pending['operations'][:1]:
            result=attempt(lambda:self.clean_operation(identifier))
            if result and result.get('cleanup_complete',result.get('cleanup_proven',False)):
                pending['operations'].remove(identifier);progressed=True
        for original in pending['sessions'][:1]:
            def session_cleanup():
                self._clean_session_outputs(original['project'],original['id'],original['generation'])
                return self.metadata.sessions.system_lost(original['project'],original['id'],original['generation'],
                    reason='controller_recovery_no_replay',verify_cleanup=lambda job:self._scope_cleanup(
                        original['project'],'session',original['id'],original['generation'],job))
            result=attempt(session_cleanup)
            if result and result.get('cleanup_complete',result.get('cleanup_proven',False)):
                pending['sessions'].remove(original);progressed=True
        if pending['media']:
            identifier=pending['media'].pop(0)
            result=attempt(lambda:self.media_captures.recover(identifiers=[identifier]))
            if result is False or result:pending['media'].append(identifier)
            else:progressed=True
        if not any(pending[key] for key in ('jobs','operations','sessions','media','read')) and pending['priority'] is not None:
            def priority_cleanup():
                self._clear_priority_owner(pending['priority']['digest'],pending['priority']);return True
            if attempt(priority_cleanup):pending['priority']=None;progressed=True
        if not any(pending[key] for key in ('jobs','operations','sessions','media','read','priority')):
            self._recovery=None;return
        pending['delay']=.1 if progressed else min(5.,pending['delay']*2)
        pending['next_at']=time.monotonic()+pending['delay']

    def _authority_waitables(self):
        """Wake on existing bounded RPCs; do not advance or parse them here."""
        exchanges={}
        def add(exchange):
            channel=getattr(exchange,'channel',None)
            if channel is None or channel.closed or getattr(exchange,'done',False):return
            descriptor=channel.connection.fileno()
            if descriptor<0:return
            exchanges[descriptor]=(channel.connection,selectors.EVENT_WRITE if channel.outgoing or not exchange.connected else selectors.EVENT_READ)
            if len(exchanges)>16:c.reject('Original authority waitable budget exceeds16',code='resource_limit')
        def task(value):
            pending=getattr(value,'pending',None)
            if pending is not None:add(pending[1])
        for item in self.connections.values():task(item.get('metadata_task'))
        broker=getattr(self,'broker',None)
        for item in getattr(broker,'source_reads',[]):task(item['task'])
        if getattr(broker,'current',None):add(broker.current.get('exchange'))
        for owner in self.sessions.values():add((owner.get('preparation') or {}).get('exchange'))
        for name in ('output_reads','media_captures'):
            current=getattr(getattr(self,name,None),'current',None)
            if current:
                task(current.get('task'));add(current.get('exchange'));add(current.get('release_exchange'))
        return exchanges

    def _select(self):
        # Tasks previously advanced only after the idle100ms timer when their
        # reply socket was outside this selector. Many fresh checks multiplied
        # that fixed delay. Register only current owned descriptors for this
        # wait; the original task still validates every reply on its next turn.
        registered=[]
        try:
            for descriptor,(connection,events) in self._authority_waitables().items():
                try:self.poll.get_key(descriptor)
                except KeyError:
                    self.poll.register(connection,events,('authority-wake',descriptor));registered.append(descriptor)
            return self.poll.select(.1)
        finally:
            for descriptor in registered:self.poll.unregister(descriptor)

    def serve(self):
        while self.running:
            if self.watchdog is not None and time.monotonic()-self.last_watchdog_check > 2:
                # If the controller is itself blocked, the external supervisor
                # must handle watchdog death. This is additional fail-closed
                # detection, not a replacement for that host policy.
                if not self.watchdog.status():
                    c.reject("Independent watchdog health was lost", code="operation_failed")
                self.last_watchdog_check = time.monotonic()
            try:
                self._tick_recovery()
                self._tick_metadata()
                if hasattr(self,"output_reads"):self.output_reads.tick()
                if hasattr(self,"media_captures"):self.media_captures.tick()
                self.broker.tick()
                self._tick_sessions()
            except (ForgeError, OSError) as exc:
                # A failed lifecycle observation is not cleanup/free. Keep the
                # original intents; watchdog/supervisor retain hard ownership.
                now=time.monotonic()
                count=getattr(self,'_lifecycle_warnings',0)
                if count<32 and now-getattr(self,'_last_lifecycle_warning',0)>=5:
                    self._lifecycle_warnings=count+1;self._last_lifecycle_warning=now
                    logging.getLogger(__name__).warning('Analysis lifecycle remains pending code=%s detail=%s',
                        getattr(exc,'code',type(exc).__name__),str(exc)[:1024],exc_info=True)
            for fd, item in list(self.connections.items()):
                self._refresh_interest(fd, item)
            for key, events in self._select():
                kind, fd = key.data
                if kind=='authority-wake':continue
                try:
                    if kind == "listener":
                        self._accept(self.listeners[fd]); continue
                    item = self.connections.get(fd)
                    if item is None:
                        continue
                    if events & selectors.EVENT_WRITE:
                        item["channel"].flush()
                        if item["close_after_write"] and not item["channel"].outgoing:
                            self._close_connection(fd); continue
                    if events & selectors.EVENT_READ:
                        frame = item["channel"].receive()
                        if frame is not None:
                            self._frame(item, *frame)
                except (ForgeError, OSError, EOFError) as exc:
                    item = self.connections.get(fd) if kind != "listener" else None
                    if item is not None and isinstance(exc, ForgeError) and not item["channel"].closed:
                        try:
                            self.reply(item, dict(error=str(exc)[:1024], code=exc.code))
                        except ForgeError:
                            self._close_connection(fd)
                    elif item is not None:
                        self._close_connection(fd)
            for fd, item in list(self.connections.items()):
                channel = item["channel"]
                partial = bool(channel.header) or channel.payload is not None
                wall = 3605 if item["kind"] == "kernel" else 140
                idle = 5 if partial else 605 if item["kind"] == "kernel" else 65 if item["phase"] in {"metadata_pending","source_pending","output_pending","capture_pending"} else 140 if item["phase"] == "waiting" else 10
                if channel.expired(wall_seconds=wall, idle_seconds=idle):
                    self._close_connection(fd)

    def close(self):
        self.running = False
        if self.runtime is not None:
            for identifier, value in self.runtime.leases.items():
                if value["state"] != "cleanup_complete":
                    try:
                        self.runtime.stop(identifier, reason="controller_shutting_down")
                    except (ForgeError, OSError):
                        pass
        for fd in list(self.connections):
            self._close_connection(fd)
        for item in self.listeners.values():
            item["listener"].close()
        self.listeners.clear()
        self.poll.close()
        if self.runtime is not None:
            self.runtime.close()
        if self.database is not None:
            self.database.close()
        # Independent watchdog + outer supervisor own any unresolved cleanup.
