"""Authenticated Web-to-helper adapter and live project-authority callback.

No DB, kernel, SQL, Arrow decoder or user code runs in this Web adapter. Its
separately configured Unix endpoints/UIDs are deployment metadata. Analysis is
absent unless an explicit production config matches an admitted real helper.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import os
from pathlib import Path
import select
import socket
import stat
import struct
import threading
import time

from demiflow.forge.process import process_identity
from demiflow.workbench.projects import registry as console
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Listener
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.workbench.analysis.outbox import Outbox
from demiflow.workbench.analysis.trusted_binding import WEB_FORMAT as TRUSTED_FORMAT
from demiflow.workbench.analysis import execution_facts
import logging

FORMAT = "demiflow_analysis_web_config_v2"


def credentials(connection, expected_uid, *, expected_identity=None):
    # An outer host helper can be an ancestor of the Web PID namespace; Linux
    # may report pid0 to this receiver. UID must still be explicitly mapped to
    # its dedicated non-root identity. Neither UID65534 nor an arbitrary PID is
    # accepted as a substitute. The OUTER Authority binds the actual Web peer
    # to its auth socket with host-visible SO_PEERCRED + boot/start/PID checks.
    pid, uid, gid = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
    if expected_identity is not None:
        c.fields(expected_identity, {'pid','boot','start'}, label='fixed trusted helper identity')
        if uid != expected_uid or uid != os.geteuid() or process_identity(pid) != expected_identity:
            c.reject('Unix peer is not the original trusted helper process', code='operation_rejected')
    elif uid != expected_uid or uid in {0, 65534, os.geteuid()}:
        c.reject("Unix peer is not the configured dedicated analysis helper", code="operation_rejected")
    return dict(pid=pid, uid=uid, gid=gid)


def _trusted_config(value):
    c.fields(value, {'format','deployment_id','principal','helper_uid','helper_identity',
        'helper_socket','auth_socket','transfer_root','code_digest','python_digest',
        'environment_digest','policy_digest','service_capabilities'}, label='trusted Web binding')
    c.identity(value['deployment_id']);c.identity(value['principal'])
    if type(value['helper_uid']) is not int or value['helper_uid'] != os.geteuid():
        c.reject('Trusted helper must use this current container account')
    c.fields(value['helper_identity'], {'pid','boot','start'}, label='original trusted helper')
    c.integer(value['helper_identity']['pid'],'original helper PID',2**31-1,1)
    c.text(value['helper_identity']['boot'],'original helper boot',64)
    c.text(value['helper_identity']['start'],'original helper start',32)
    for name in ('code_digest','python_digest','environment_digest','policy_digest'):
        c.sha(value[name],name)
    from demiflow.forge.io import no_symlinks
    for name in ('helper_socket','auth_socket','transfer_root'):
        c.text(value[name],name,2048);location=Path(value[name])
        if not location.is_absolute() or '..' in location.parts or str(location)!=value[name]:
            c.reject('Trusted Web paths must be fixed normalized absolute paths')
        if name!='transfer_root' and len(os.fsencode(location))>107:
            c.reject('Trusted Web socket path exceeds its Unix bound')
        no_symlinks(location if name=='transfer_root' else location.parent)
    if value['helper_socket']==value['auth_socket']:
        c.reject('Trusted helper and Web authority sockets must differ')
    c.fields(value['service_capabilities'], {'analysis_execute','analysis_write','case_annotate'},
        label='trusted Web capability ceiling')
    if any(type(flag) is not bool for flag in value['service_capabilities'].values()):
        c.reject('Trusted Web capability ceilings must be explicit booleans')
    return value


def load_config(path):
    path = Path(path)
    raw = _read(path)
    info = path.stat()
    if info.st_uid not in {0, os.geteuid()} or stat.S_IMODE(info.st_mode) != 0o600:
        c.reject("Web analysis config must be private and owned by its Web/admin identity")
    value = c.decode(raw)
    if type(value) is dict and value.get('format')==TRUSTED_FORMAT:
        return _trusted_config(value)
    c.fields(value, {"format", "deployment_id", "helper_uid", "helper_socket", "priority_socket", "auth_socket", "transfer_root", "helper_sha256", "profile_sha256", "service_capabilities"}, label="Web analysis deployment")
    if value["format"] != FORMAT or value["profile_sha256"] != profile()[1]:
        c.reject("Web analysis requires the reviewed config/profile edition")
    c.identity(value["deployment_id"]); c.sha(value["helper_sha256"])
    c.integer(value["helper_uid"], "mapped dedicated helper UID", 2**32-2, 1)
    if value["helper_uid"] in {65534, os.geteuid()}:
        c.reject("Web/helper UIDs must be distinct and explicitly mapped")
    for name in ("helper_socket", "priority_socket", "auth_socket"):
        c.text(value[name], name, 2048)
        location = Path(value[name])
        if not location.is_absolute() or ".." in location.parts or str(location) != value[name] or len(os.fsencode(location)) > 107:
            c.reject("Web analysis sockets require normalized short fixed absolute paths")
    from demiflow.forge.io import no_symlinks
    c.text(value["transfer_root"],"fixed Web transfer mount",2048)
    transfer=Path(value["transfer_root"])
    if not transfer.is_absolute() or ".." in transfer.parts or str(transfer)!=value["transfer_root"]:
        c.reject("Transfer mount must be a normalized fixed absolute path")
    no_symlinks(transfer)
    c.fields(value["service_capabilities"], {"analysis_execute", "analysis_write", "case_annotate"}, label="explicit analysis service ceiling")
    if any(type(flag) is not bool for flag in value["service_capabilities"].values()):
        c.reject("Analysis service ceilings must be explicit booleans")
    return value


class WebService:
    def _authority_warning(self,message,error=None):
        count=getattr(self,'_authority_warnings',0)
        if count>=16:return
        self._authority_warnings=count+1
        logging.getLogger(__name__).warning('Analysis Web callback %s%s',message,
            '' if error is None else ': '+str(error)[:512])

    def __init__(self, server, config_path):
        self.server, self.config = server, load_config(config_path)
        self.identity = process_identity(os.getpid())
        if self.identity is None:
            c.reject("Web process identity is unavailable")
        self.principal = self.config.get('principal') or "owner-" + hashlib.sha256(self.config["deployment_id"].encode()).hexdigest()
        self.execution_mode='trusted_local' if self.config['format']==TRUSTED_FORMAT else 'oci'
        self.execution_capabilities=None
        self.outbox = Outbox(self.config["transfer_root"])
        self.closed = threading.Event()
        self.output_lock=threading.Lock()
        self.output_connections={}
        self.features={}
        # Preserve the two-worker ceiling. One lane is reserved for short
        # principal/grant/context callbacks, one for source/transfer work.
        self.slots = threading.BoundedSemaphore(1)
        self.authority_controls = threading.BoundedSemaphore(1)
        self.sources = threading.BoundedSemaphore(1)
        self.listener = Listener(self.config["auth_socket"], public=True)
        self.thread = threading.Thread(target=self._serve_authority, name="demiflow-analysis-authority", daemon=True)
        self.thread.start()
        try:
            self.assert_ready()
        except BaseException:
            self.close()
            raise

    def ceiling(self):
        return dict(self.config["service_capabilities"])

    def execution_profile(self):
        from demiflow.workbench.analysis.execution_backend import capabilities
        mode=self.execution_mode
        result=dict(mode=mode,capabilities=self.execution_capabilities or capabilities(mode))
        if mode=='trusted_local':
            result.update({key:self.config[key] for key in ('code_digest','python_digest',
                'environment_digest','policy_digest')})
        c.encoded(result,maximum=4096)
        return result

    def assert_ready(self):
        status = self.call(None, "status", {})
        if self.config.get('format')==TRUSTED_FORMAT:
            c.fields(status, {'format','mode','active','execution_mode','helper_identity',
                'code_digest','python_digest','environment_digest','policy_digest','capabilities'},
                {'features'},label='trusted helper status')
            if (status['format']!='demiflow_analysis_helper_status_v2' or status['mode']!='production'
                    or status['active'] is not True or status['execution_mode']!='trusted_local'
                    or any(status[name]!=self.config[name] for name in ('helper_identity','code_digest',
                        'python_digest','environment_digest','policy_digest'))):
                c.reject('Trusted helper process or fixed execution inputs differ',code='operation_rejected')
            self.execution_capabilities=execution_facts.capabilities(status['capabilities'])
            if self.execution_capabilities['mode']!='trusted_local':
                c.reject('Trusted helper capabilities use another execution mode',code='conflict')
        else:
            c.fields(status, {"format", "mode", "active", "helper_sha256", "profile_sha256"}, {"features"},label="analysis helper status")
            if (status["format"] != "demiflow_analysis_helper_status_v1" or status["mode"] != "production" or status["active"] is not True
                    or status["helper_sha256"] != self.config["helper_sha256"] or status["profile_sha256"] != self.config["profile_sha256"]):
                c.reject("Analysis helper is not the exact reviewed production release", code="operation_rejected")
        features=status.get('features',{})
        c.fields(features,set(),{'case_media','annotation_media_basis','case_output_groups','case_review_filter','notebook_history'},label='reviewed analysis helper features')
        if any(type(value) is not bool for value in features.values()):c.reject('Helper features must be explicit booleans')
        if features.get('annotation_media_basis') and not features.get('case_media'):c.reject('Media basis requires the connected fixed media chain')
        self.features=dict(features)
        return True

    def _credentials(self, connection):
        options={'expected_identity':self.config['helper_identity']} if 'helper_identity' in self.config else {}
        return credentials(connection,self.config['helper_uid'],**options)

    def call(self, project, operation, payload, *, _timeout=None):
        if self.closed.is_set():
            c.reject("Analysis adapter is closed", code="operation_rejected")
        if project is not None:
            console.identity(project)
        c.identity(operation, "analysis operation"); c.encoded(payload)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        # A metadata source read can use its bounded 20s workspace reader and
        # asynchronous fresh checks (Task has a 60s total deadline). Do not
        # turn that admitted read into an artificial unknown result at 10s.
        if _timeout is not None:c.integer(_timeout,'bounded helper deadline',65,1)
        connection.settimeout(_timeout if _timeout is not None else 2 if operation=="status" else 65)
        request_id = payload.get("request_id") if type(payload) is dict else None
        try:
            with closing(connection):
                connection.connect(self.config["helper_socket"])
                self._credentials(connection)
                frames = Frames(connection)
                frames.send("json", dict(version=1, project=project, operation=operation, payload=payload))
                value = frames.json()
                if type(value) is dict and "error" in value:
                    c.fields(value, {"error", "code"}, label="analysis helper failure")
                    c.text(value["error"], "helper error", 1024)
                    c.reject(value["error"], code=value["code"] if value["code"] in ForgeError.CODES else "operation_failed")
                return value
        except OSError:
            suffix = "; query original request_id="+request_id if request_id is not None else ""
            c.reject("Analysis helper reply is unavailable or unknown"+suffix, code="operation_failed")

    def stop_generation(self, project, *, request_id, session_id, generation):
        """Force-stop the original generation through the independent watcher.

        This acknowledgement never says the SQL execution record is stopped
        or files/mounts are gone. Its generation cannot be restarted in place.
        """
        if self.closed.is_set():
            c.reject("Analysis adapter is closed", code="operation_rejected")
        console.identity(project)
        payload = dict(request_id=c.identity(request_id), session_id=c.identity(session_id), generation=c.identity(generation))
        if self.config.get('format')==TRUSTED_FORMAT:
            value=self.call(project,'session-control',dict(request_id=request_id,session_id=session_id,
                expected_generation=generation,action='stop'),_timeout=7)
            if value.get('id')!=session_id or value.get('generation')!=generation:
                c.reject('Managed stop reply belongs to another generation',code='conflict')
            return dict(format='demiflow_analysis_managed_stop_v1',**payload,accepted=True,
                execution_mode='trusted_local',cleanup_complete=value.get('cleanup_complete',False),
                cleanup_proven=False,force=False,state=value['state'])
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); connection.settimeout(7)
        try:
            with closing(connection):
                connection.connect(self.config["priority_socket"])
                self._credentials(connection)
                frames = Frames(connection)
                frames.send("json", dict(version=1, project=project, operation="stop-generation", payload=payload))
                value = frames.json()
                if type(value) is dict and "error" in value:
                    c.fields(value, {"error", "code"}, label="independent stop failure")
                    c.reject(value["error"], code=value["code"] if value["code"] in ForgeError.CODES else "operation_failed")
                c.fields(value, {"format", "request_id", "session_id", "generation", "accepted", "cleanup_proven", "force", "helper_sha256", "profile_sha256"}, label="independent stop acknowledgement")
                if (value["format"] != "demiflow_analysis_priority_stop_v1" or any(value[key] != payload[key] for key in payload)
                        or value["accepted"] is not True or value["cleanup_proven"] is not False or value["force"] is not True
                        or value["helper_sha256"] != self.config["helper_sha256"] or value["profile_sha256"] != self.config["profile_sha256"]):
                    c.reject("Independent stop acknowledgement differs", code="conflict")
                return value
        except OSError:
            c.reject("Independent stop is unconfirmed; inspect this original generation", code="operation_failed")

    def read_output(self,project,operation,payload,*,consumer_closed=None,cancellation_tag=None):
        from demiflow.workbench.analysis.output_transport import exchange
        return exchange(self,project,operation,payload,consumer_closed=consumer_closed,cancellation_tag=cancellation_tag)

    def cancel_output(self,cancellation_tag):
        from demiflow.workbench.analysis.output_transport import stop
        with self.output_lock:
            records=[item for item in self.output_connections.values() if item["tag"]==cancellation_tag]
        for item in records:
            try:stop(self,item)
            except (OSError,ForgeError):pass # independent five-second owner remains a hard fence

    def media_current(self,project,reference,descriptor):
        if 'source_snapshot' in reference:
            value=self.call(project,'artifact-media-current',dict(reference=reference))
            return value==dict(verified=True,descriptor=descriptor)
        expected={key:reference[key] for key in ("execution_id","expected_output_id","expected_binding")}
        return self.call(project,"output-current",expected)=={"verified":True} and descriptor.get("sha256")==reference["expected_sha256"]

    def media_inspect(self,project,reference,*,consumer_closed=None):
        from demiflow.workbench.analysis.output_transport import OutputRange
        value=self.read_output(project,"artifact-media" if "source_snapshot" in reference else "output-media",
            dict(reference=reference,range="bytes=0-0") if "source_snapshot" in reference else dict(reference,range="bytes=0-0"),consumer_closed=consumer_closed)
        if not isinstance(value,OutputRange) or type(value.metadata.get("descriptor")) is not dict:c.reject("Media inspection returned no verified descriptor",code="operation_failed")
        return value.metadata["descriptor"]

    def _authority(self, request):
        c.fields(request, {"version", "challenge", "operation", "project", "web_parent", "principal", "payload"}, label="live analysis authority callback")
        if type(request["version"]) is not int or request["version"] != 1:
            c.reject("Unsupported analysis authority protocol")
        c.identity(request["challenge"]); console.identity(request["project"])
        c.fields(request["web_parent"], {"pid", "boot", "start"}, label="outer-visible original Web parent")
        # Host PID can differ from local Web PID under an inner PID namespace.
        # The outer caller already checks that the accepted auth connection is
        # the exact same original host PID. Boot/start must still match here.
        if (request["web_parent"]["boot"] != self.identity["boot"] or request["web_parent"]["start"] != self.identity["start"]
                or process_identity(os.getpid()) != self.identity):
            c.reject("Authority callback belongs to another Web lifetime", code="conflict")
        operation, payload = request["operation"], request["payload"]
        if operation == "whoami":
            c.fields(payload, set(), label="authority principal request")
            if request["principal"] is not None:
                c.reject("Principal discovery cannot adopt a caller principal")
            return dict(principal=self.principal)
        if request["principal"] != self.principal:
            c.reject("Analysis principal differs from this deployment", code="operation_rejected")
        if operation=="release-source":
            c.fields(payload,{"export_id","source_snapshot"},label="owned source export cleanup")
            return self.outbox.release(project=request["project"],**payload)
        inventory = self.server.inventory()
        root, _ = console.resolve_workspace(inventory, request["project"])
        if operation == "permission":
            c.fields(payload, {"permission"}, label="current analysis grant")
            permission = c.one_of(payload["permission"], {"read", "analysis_execute", "analysis_write", "case_annotate"}, "current grant")
            grants = self.server.effective_permissions(inventory["projects"][request["project"]])
            return dict(allowed=permission == "read" or grants[permission])
        if operation == 'project-context':
            c.fields(payload, set(), label='current registered project context')
            return dict(workspace=str(root))
        if operation not in {"resolve-source", "verify-source", "export-source", "source-query", "media-sources", "export-media"}:
            c.reject("Unsupported authority operation")
        # Reuse the Web's existing bounded native-reader lane so a callback
        # cannot add a parallel unaccounted metadata/Case process pool.
        if not self.sources.acquire(blocking=False):
            c.reject("Analysis source authority reader is busy", code="busy")
        # The one source callback worker may wait for the existing workspace
        # reader; background catalog refresh must not randomly fail a running
        # cell. This admits no second reader and never retries user code or a
        # dispatched operation. Short authority callbacks keep their own lane.
        acquired = self.server.workers.acquire(timeout=5)
        try:
            if not acquired:
                c.reject("Workspace metadata reader is busy", code="busy")
            from demiflow.workbench.analysis import web_sources
            if operation in {'media-sources','export-media'}:
                required={'source_locator','source_snapshot','case_key','fields'} if operation=='media-sources' else {'source_locator','source_snapshot','case_key','field','export_id'}
                c.fields(payload,required,label='current declared media request')
                def authorized_media():
                    try:
                        current=self.server.inventory();resolved,_=console.resolve_workspace(current,request['project'])
                        return resolved==root and (operation=='media-sources' or self.server.effective_permissions(current['projects'][request['project']])['analysis_write'])
                    except ForgeError:return False
                if not authorized_media():c.reject('Current media source/capture grant is absent',code='operation_rejected')
                if operation=='export-media':
                    return self.outbox.export_media(root,project=request['project'],authorize=authorized_media,**payload)
                from demiflow.workbench.worker import operation as worker
                result=worker('analysis_media_sources',root,**payload)
                if not authorized_media():c.reject('Current media resource permission changed before return',code='operation_rejected')
                c.encoded(result,maximum=40*1024);return result
            if operation=="source-query":
                c.fields(payload,{"source_locator","source_snapshot","query","purpose"},label="fixed source query")
                c.one_of(payload["purpose"],{"catalog","notebook"},"fixed source read purpose")
                def authorized():
                    current=self.server.inventory();resolved,_=console.resolve_workspace(current,request["project"])
                    return resolved==root and (payload["purpose"]=="catalog" or self.server.effective_permissions(current["projects"][request["project"]])["analysis_execute"])
                if not authorized():c.reject("Current source read grant is absent",code="operation_rejected")
                from demiflow.workbench.worker import operation as worker
                result=worker("analysis_source_read",root,**{key:payload[key] for key in ("source_locator","source_snapshot","query")})
                if not authorized():c.reject("Source read grant changed before response",code="operation_rejected")
                c.encoded(result,maximum=56*1024)
                return result
            if operation=="export-source":
                c.fields(payload,{"export_id","source_locator","source_snapshot"},label="fixed source export")
                def authorize():
                    try:
                        current=self.server.inventory()
                        resolved,_=console.resolve_workspace(current,request["project"])
                        return resolved==root and self.server.effective_permissions(current["projects"][request["project"]])["analysis_execute"]
                    except ForgeError:
                        return False
                return self.outbox.export(root,project=request["project"],authorize=authorize,**payload)
            if operation == "resolve-source":
                c.fields(payload, {"locator"}, label="fixed source discovery")
                return web_sources.resolve(root, payload["locator"])
            c.fields(payload, {"locator", "source", "target"}, label="fixed source verification")
            return web_sources.verify(root, **payload)
        finally:
            if acquired:
                self.server.workers.release()
            self.sources.release()

    def _serve_request(self, connection, request, gate):
        challenge = None
        try:
            with closing(connection):
                connection.setblocking(True); connection.settimeout(2)
                self._credentials(connection)
                frames = Frames(connection)
                if type(request) is dict and type(request.get("challenge")) is str:
                    challenge = c.identity(request["challenge"])
                try:
                    from demiflow.workbench.reader import using
                    def consumer_closed():
                        try:
                            if not select.select([connection],[],[],0)[0]:return False
                            return connection.recv(1,socket.MSG_PEEK|socket.MSG_DONTWAIT)==b''
                        except (BlockingIOError,TimeoutError):return False
                        except (OSError,ValueError):return True
                    with using(getattr(self.server,'native_reader',None),consumer_closed):
                        value = self._authority(request)
                    reply = dict(challenge=challenge, value=value, error=None, code=None)
                except ForgeError as exc:
                    reply = dict(challenge=challenge, value=None, error=str(exc)[:1024], code=exc.code)
                except OSError as exc:
                    # This callback already passed original-peer/challenge
                    # validation. An operation's I/O failure is not a socket
                    # disconnect: return one bounded failure on this exchange,
                    # leaving its outcome unconfirmed and never replaying it.
                    self._authority_warning('operation I/O failed',exc)
                    reply = dict(challenge=challenge, value=None,
                        error='Live Web authority operation I/O failed; outcome remains unconfirmed',
                        code='operation_failed')
                frames.send("json", reply)
        except (OSError, ForgeError) as error:
            self._authority_warning('transport or original peer refused',error)
        finally:
            gate.release()

    def _serve_authority(self):
        # Partial headers/payloads never occupy a blocking callback worker.
        # Eight bounded64KiB inputs, two original worker lanes, finite waits.
        pending={};fast={'whoami','permission','project-context'}
        try:
            while not self.closed.is_set():
                readers=[item['channel'].connection for item in pending.values() if item['request'] is None]
                if len(pending)<8:readers.append(self.listener.socket)
                try:readable=select.select(readers,[],[],.05)[0]
                except (OSError,ValueError):
                    if self.closed.is_set():return
                    raise
                if self.listener.socket in readable:
                    for _ in range(8-len(pending)):
                        try:connection,_=self.listener.socket.accept()
                        except BlockingIOError:break
                        except OSError:
                            if self.closed.is_set():return
                            raise
                        try:
                            self._credentials(connection)
                            channel=Channel(connection)
                            pending[connection.fileno()]=dict(channel=channel,request=None,deadline=time.monotonic()+2)
                        except (OSError,ForgeError) as error:
                            connection.close();self._authority_warning('original peer refused',error)
                for key,item in list(pending.items()):
                    channel=item['channel']
                    if item['request'] is not None or channel.connection not in readable:continue
                    try:
                        frame=channel.receive()
                        if frame is None:continue
                        kind,request=frame
                        if kind!='json':c.reject('Authority callback requires bounded JSON')
                        item['request']=request
                        operation=request.get('operation') if type(request) is dict else None
                        item['deadline']=time.monotonic()+(2 if operation in fast else 25)
                    except (OSError,EOFError,ForgeError) as error:
                        channel.close();pending.pop(key);self._authority_warning('request transport refused',error)
                # Complete fast control requests have a dedicated original
                # worker. A source export or a silent peer cannot starve them.
                for key,item in list(pending.items()):
                    if time.monotonic()>=item['deadline']:
                        item['channel'].close();pending.pop(key);self._authority_warning('request admission deadline');continue
                    request=item['request']
                    if request is None:continue
                    operation=request.get('operation') if type(request) is dict else None
                    gate=self.authority_controls if operation in fast else self.slots
                    if not gate.acquire(blocking=False):continue
                    connection=item['channel'].connection;pending.pop(key)
                    try:
                        threading.Thread(target=self._serve_request,args=(connection,request,gate),daemon=True,
                            name='demiflow-analysis-authority-request').start()
                    except RuntimeError as error:
                        connection.close();gate.release();self._authority_warning('worker admission refused',error)
        finally:
            for item in pending.values():item['channel'].close()

    def close(self):
        if not self.closed.is_set():
            self.closed.set(); self.listener.close(); self.thread.join(timeout=1)
            with self.output_lock:records=list(self.output_connections.values())
            for item in records:
                try:item["connection"].shutdown(socket.SHUT_RDWR)
                except OSError:pass
