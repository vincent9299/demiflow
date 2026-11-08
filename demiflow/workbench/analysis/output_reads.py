"""One historical-output read using the existing owned media role/lifecycle.

Read ownership is separate from every Notebook generation. This controller
only handles <=56KiB projections or one opaque <=1MiB range; the registered
worker parses the original document. No kernel, host codec or process fallback.
"""
import os
import logging
from pathlib import Path
import socket
import time
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deferred_metadata import Task
from demiflow.workbench.analysis.presentations import current as current_output
from demiflow.workbench.analysis.presentations import register as register_presentations
from demiflow.forge.schema import ForgeError


class ReturnMetadata:
    def __init__(self,records,witness,value):self.records,self.witness,self.value=records,witness,value

    def route(self,authority,operation,payload):
        c.fields(payload,set(),label="owned output return")
        access=authority.access();access.check()
        with self.records.database.locked() as db:current_output(db,access,self.witness)
        if self.value is not None:
            return register_presentations(self.records,access,witness=self.witness,projection=self.value)
        access.check();return dict(verified=True)


class ReturnCase:
    def __init__(self,controller,value):self.controller,self.value=controller,value

    def route(self,authority,operation,payload):
        from demiflow.workbench.analysis.case_pages import finish
        value=self.value;access=authority.access();access.check()
        source,origin=self.controller.sources.registered(access,value["witness"]["source"]["digest"])
        if (source!=value["witness"]["source"] or origin["kind"]!="private"
                or origin["handle"]!=value["witness"]["artifact"] or not self.controller.sources.verify(authority,source)):
            c.reject("Original private Case source changed before return",code="conflict")
        result=finish(self.controller.records,authority,source,value["case_context"],value["value"])
        if not self.controller.sources.verify(authority,source):c.reject("Case source ended before return",code="conflict")
        access.check();return result


class ReturnMedia:
    def __init__(self,controller,witness):self.controller,self.witness,self.records=controller,witness,controller.records

    def route(self,authority,operation,payload):
        from demiflow.workbench.analysis.case_media import registered_witness
        c.fields(payload,set(),label='owned fixed media return')
        current=registered_witness(self.records,self.controller.sources,authority,self.witness['reference'])
        if current!=self.witness:c.reject('Fixed media registration changed before return',code='conflict')
        return dict(verified=True)


class OutputReads:
    def __init__(self,controller):self.controller,self.current=controller,None

    def admit(self,connection,authority,operation,payload,witness,*,complete=None):
        if self.current is not None:c.reject("The independent read owner is busy or cleanup is unresolved",code="busy")
        c.encoded(witness,maximum=48*1024)
        case_context=None;role="media";bridge="output-read"
        if operation=='probe-media':
            if not callable(complete):c.reject('Media probe requires its original capture callback')
            from demiflow.workbench.analysis.media_artifact_worker import configuration
            config=configuration(witness);bridge='media-probe'
        elif operation=='artifact-media':
            from demiflow.workbench.analysis.media_artifact_worker import configuration
            config=configuration(dict(version=1,operation='artifact-media',artifact=witness['artifact'],range=payload['range']))
        elif operation=="case-page":
            c.fields(witness,{"format","config","context"},label="private Case admission")
            if witness["format"]!="demiflow_private_case_read_v1":c.reject("Private Case admission format differs")
            config=witness["config"];case_context=witness["context"];witness=config["witness"];role="native";bridge="case-read"
            from demiflow.workbench.analysis.case_pages import native_config
            native_config(config)
        elif operation=="output":config=dict(version=1,operation="output-page",witness=witness,offset=payload.get("offset",0),limit=payload.get("limit",8))
        elif operation=="output-original":config=dict(version=1,operation="output-original",witness=witness,range=payload["range"])
        elif operation=="output-media":config=dict(version=1,operation="output-media",witness=witness,range=payload["range"],output_index=payload["output_index"],mime=payload["mime"],sha256=payload["expected_sha256"])
        else:c.reject("Unknown fixed read operation")
        # Fixed worker schema validation is metadata only; no decoder imports.
        if role=="media" and operation not in {"probe-media","artifact-media"}:
            from demiflow.workbench.analysis.output_worker import request
            request(config)
        origin=getattr(authority,'reader_environment',None)
        if origin is not None:
            from demiflow.workbench.analysis.execution_facts import origin as validate_origin
            validate_origin(origin)
        self.current=dict(connection=connection,authority=authority,operation=operation,witness=witness,config=config,
            id="r"+uuid.uuid4().hex,owner=None,digest=None,job_id=None,created=time.monotonic(),
            value=None,metadata=None,blob=None,ended=False,stopped=False,error=None,task=None,clean=False,
            case_context=case_context,role=role,bridge=bridge,retired=False,owner_unlinked=False,complete=complete,
            reader_environment=origin,cleanup_receipt=None)
        connection["phase"]="output_pending"
        # Binary is output-only here: _frame still rejects any control input
        # after its initial closed JSON request. Queue remains <=2MiB.
        connection["channel"].binary=True;connection["channel"].output_limit=2*1024*1024+10
        connection["channel"].queue("json",dict(format="demiflow_output_read_admission_v1",accepted=True,read_request_id=self.current["id"]))

    def _authorize(self,value):
        authority=value["authority"]
        try:
            channel=value["connection"]["channel"]
            if channel.closed:return False
            try:
                if channel.connection.recv(1,socket.MSG_PEEK|socket.MSG_DONTWAIT)==b"":return False
            except BlockingIOError:pass
            return (authority.peer.live() and authority.check(authority.project,authority.principal,"read")
                and (value["operation"]!="probe-media" or authority.check(authority.project,authority.principal,"analysis_write")))
        except (ForgeError,OSError):return False

    def _start(self,value):
        from demiflow.workbench.analysis import read_control
        controller=self.controller;authority=value["authority"]
        if value['operation']=='probe-media':value['digest']=read_control.publish_probe(controller.runtime.root,authority,value['id'],value['config'])
        elif value['operation']=='artifact-media':value['digest']=read_control.publish_artifact(controller.runtime.root,authority,value['id'],value['config'])
        elif value['operation']=='case-page':value['digest']=read_control.publish_case(controller.runtime.root,authority,value['id'],value['config'])
        else:value['digest']=read_control.publish(controller.runtime.root,authority,value['id'],value['witness']['artifact'])
        value["owner"]=read_control.load(controller.runtime.root)
        answer=controller.runtime.read_owner("bind-read-owner",value["digest"],record=value["owner"])
        if not answer["bound"] or answer["stop_requested"]:c.reject("Independent read owner is not ready",code="operation_rejected")
        origin=value['reader_environment']
        if origin is None:
            if controller.runtime.mode!='oci':c.reject('Trusted reader has no current environment binding',code='conflict')
            origin=controller.reader_environment()
        created=controller.runtime.create(project=authority.project,execution_id=value["id"],role=value["role"],config=value["config"],
            web_parent=authority.peer.identity,owner_digest=value["digest"],read_owner=value["owner"],
            origin_digest=c.digest(origin),workspace=Path(origin['cwd']) if origin['mode']=='trusted_local' else None,
            authorize=lambda:self._authorize(value))
        value["job_id"]=created["job_id"]
        controller.listen_job(value["job_id"],value["bridge"],value)
        controller.runtime.start(value["job_id"],authorize=lambda:self._authorize(value))

    def frame(self,connection,kind,reply):
        value=self.current
        if value is None or connection["owner"] is not value or connection["job_id"]!=value["job_id"] or value["stopped"]:
            c.reject("Output read frame belongs to another original owner",code="conflict")
        if value["ended"]:c.reject("Output read already ended",code="conflict")
        if kind=="json":
            if value["value"] is not None or value["metadata"] is not None or value["error"] is not None:c.reject("Output read repeated its header")
            if value['operation']=='probe-media':
                self._probe_header(value,reply);return
            expected_operation='artifact-media' if value['operation']=='artifact-media' else value['bridge']
            artifact=value['witness']['artifact']
            expected_artifact={key:artifact[key] for key in ('id','generation','digest')} if value['operation']=='artifact-media' else artifact
            c.fields(reply,{"operation","artifact"},{"value","metadata","error"},label="owned output read header")
            if (reply["operation"]!=expected_operation or reply["artifact"]!=expected_artifact
                    or sum(key in reply for key in ("value","metadata","error"))!=1):
                c.reject("Output read header differs from the admitted artifact",code="conflict")
            if "error" in reply:
                failure=reply["error"];c.fields(failure,{"code","message"},{"reason"},label="fixed read worker error")
                c.one_of(failure["code"],ForgeError.CODES,"fixed read worker error code");c.text(failure["message"],"fixed read worker error",1024)
                value["error"]=dict(code=failure["code"],error=failure["message"])
                if "reason" in failure:value["error"]["reason"]=c.one_of(failure["reason"],{"media_unavailable","range_not_satisfiable"},"media failure reason")
                return
            if "value" in reply:
                if value["operation"] not in {"output","case-page"}:c.reject("Binary output read returned a JSON page",code="conflict")
                c.encoded(reply["value"],maximum=56*1024)
                if value["operation"]=="case-page":
                    if reply["value"].get("source_snapshot")!=value["witness"]["source"]["digest"] or reply["value"].get("kind")!="cases":c.reject("Private Case projection source differs",code="conflict")
                elif reply["value"].get("artifact")!=value["witness"]["artifact"] or reply["value"].get("provenance")!=value["witness"]["provenance"]:
                    c.reject("Output projection provenance differs",code="conflict")
                value["value"]=reply["value"]
            else:
                if value["operation"] in {"output","case-page"}:c.reject("JSON output read returned an opaque range",code="conflict")
                metadata=reply["metadata"]
                if value['operation']=='artifact-media':
                    self._media_metadata(value,metadata);value['metadata']=metadata;return
                c.fields(metadata,{"status","mime","start","end","total","attachment"},{"descriptor","sha256"},label="output range metadata")
                c.integer(metadata['status'],'output range status',206,200)
                if metadata['status'] not in {200,206}:c.reject('Unsupported output range status')
                c.integer(metadata["total"],"media total",64*1024*1024,1)
                c.integer(metadata["start"],"media range start",metadata["total"]-1)
                c.integer(metadata["end"],"media range end",metadata["total"]-1,metadata["start"])
                if metadata["end"]-metadata["start"]+1>1024*1024:c.reject("Output range exceeds1MiB",code="resource_limit")
                if value["operation"]=="output-original":
                    if metadata["mime"]!="application/json" or metadata.get("sha256")!=value["witness"]["artifact"]["sha256"] or metadata["total"]!=value["witness"]["artifact"]["bytes"] or metadata["attachment"] is not True:
                        c.reject("Original output range identity differs",code="conflict")
                elif (metadata["mime"]!=value["config"]["mime"] or metadata["attachment"] is not False
                        or type(metadata.get("descriptor")) is not dict or metadata["descriptor"].get("sha256")!=value["config"]["sha256"]
                        or metadata["descriptor"].get("artifact_id")!=value["witness"]["artifact"]["id"]
                        or metadata["descriptor"].get("output_index")!=value["config"]["output_index"]
                        or metadata["descriptor"].get("mime")!=metadata["mime"] or metadata["descriptor"].get("bytes")!=metadata["total"]):
                    c.reject("Media range descriptor differs from its fixed MIME/index",code="conflict")
                if value["operation"]=="output-media":
                    from demiflow.workbench.analysis.media import verified_metadata
                    verified_metadata(metadata["descriptor"],metadata["mime"].split('/')[0])
                from demiflow.workbench.analysis.media import byte_range
                expected=byte_range(value["config"]["range"],metadata["total"])
                if (metadata["start"],metadata["end"],metadata["status"])!=expected:c.reject("Output range differs from the request",code="conflict")
                value["metadata"]=metadata
        elif kind=="blob":
            if value["metadata"] is None or value["blob"] is not None or len(reply)!=value["metadata"]["end"]-value["metadata"]["start"]+1:
                c.reject("Output read opaque bytes differ from the admitted range",code="conflict")
            value["blob"]=reply
        elif kind=="end":
            if value["error"] is None and value["value"] is None and (value["metadata"] is None or value["blob"] is None):c.reject("Output read ended without its confirmed page/range")
            value["ended"]=True
            # Peer identity is checked on every frame. Short readers must not
            # exit while their valid END is still queued in the Unix socket.
            # Publication still waits for original job cleanup and fresh auth.
            connection["channel"].queue("json",dict(format="demiflow_analysis_read_end_v1",accepted=True))
        else:c.reject("Unexpected output read frame")

    @staticmethod
    def _probe_header(value,reply):
        if type(reply) is not dict:c.reject('Pending media probe header must be an object')
        config=value['config'];reservation=config['reservation'];seed=reservation['seed']
        base={'operation','publish_id','object_id','seed_digest'}
        c.fields(reply,base|({'error'} if 'error' in reply else {'sha256','bytes','mime','source_digest','descriptor','chunk_sha256'}),label='owned pending media probe')
        if (reply['operation']!='media-probe' or reply['publish_id']!=reservation['publish_id']
                or reply['object_id']!=seed['object_id'] or reply['seed_digest']!=reservation['seed_digest']):
            c.reject('Pending probe belongs to another original reservation',code='conflict')
        if 'error' in reply:
            failure=reply['error'];c.fields(failure,{'code','message'},label='pending media failure')
            c.one_of(failure['code'],ForgeError.CODES,'probe failure code');c.text(failure['message'],'probe failure',1024)
            value['error']=dict(code=failure['code'],error=failure['message']);return
        if any(reply[key]!=config['content'][key] for key in ('sha256','bytes','mime')) or reply['source_digest']!=c.digest(config['source']):
            c.reject('Pending probe content/source differs',code='conflict')
        from demiflow.workbench.analysis.registered_media import payload
        payload(dict(format='media',**config['content'],source=config['source'],complete=True,
            descriptor=reply['descriptor'],chunk_sha256=reply['chunk_sha256']),artifact_id=seed['object_id'])
        value['value']=reply

    @staticmethod
    def _media_metadata(value,metadata):
        c.fields(metadata,{'status','mime','start','end','total','sha256','source_digest','descriptor'},label='fixed media range metadata')
        artifact=value['config']['artifact']
        c.integer(metadata['status'],'fixed media range status',206,200)
        if metadata['status'] not in {200,206}:c.reject('Unsupported fixed media range status')
        c.integer(metadata['total'],'fixed media range total',64*1024*1024,1)
        c.integer(metadata['start'],'fixed media range start',metadata['total']-1)
        c.integer(metadata['end'],'fixed media range end',metadata['total']-1,metadata['start'])
        if (metadata['total']!=artifact['bytes'] or any(metadata[key]!=artifact[key] for key in ('mime','sha256','source_digest','descriptor'))):
            c.reject('Media range metadata differs from registered fixed content',code='conflict')
        from demiflow.workbench.analysis.media import byte_range
        expected=byte_range(value['config']['range'],artifact['bytes'])
        if (metadata['start'],metadata['end'],metadata['status'])!=expected:c.reject('Fixed media range differs from its request',code='conflict')

    def disconnected(self,connection):
        value=self.current
        if value is None:return
        if connection is value["connection"]:self.stop(value,"read_consumer_disconnected")
        elif connection.get("kind") in {"output-read","case-read","media-probe"} and connection.get("job_id")==value["job_id"] and not value["ended"]:
            self.stop(value,"read_worker_disconnected_before_end")

    def stop(self,value,reason):
        if not value["stopped"] and getattr(self,'_stop_notices',0)<16:
            self._stop_notices=getattr(self,'_stop_notices',0)+1
            logging.getLogger(__name__).warning('Owned read stopped id=%s operation=%s job=%s reason=%s',
                value['id'],value['operation'],value['job_id'],str(reason)[:128])
        value["stopped"]=True
        if value["task"] is not None:value["task"].close();value["task"]=None
        # create may have persisted a lease and failed before returning its ID.
        if value["job_id"] is None:
            matches=[key for key,lease in self.controller.runtime.leases.items() if lease["execution_id"]==value["id"]]
            if len(matches)>1:c.reject("Read admission has multiple original leases",code="conflict")
            if matches:value["job_id"]=matches[0]
        value["error"]=value["error"] or dict(error="Output read ended without a confirmed current result",code="operation_failed")
        failure=None
        if value["digest"] is not None and not value["clean"] and not value["retired"]:
            try:self.controller.runtime.read_owner("stop-read-owner",value["digest"])
            except (ForgeError,OSError) as error:failure=error
        if value["job_id"] is not None and not value["clean"] and not value["retired"]:
            try:self.controller.runtime.stop(value["job_id"],reason=reason)
            except (ForgeError,OSError) as error:failure=failure or error
        if failure:raise failure

    def _clear_owner(self,digest,value=None):
        from demiflow.workbench.analysis import read_control
        root=self.controller.runtime.root
        if value is None or not value["owner_unlinked"]:
            if c.digest(read_control.load(root))!=digest:c.reject("Original read owner changed before cleanup",code="conflict")
            answer=self.controller.runtime.read_owner("clear-read-owner",digest)
            if answer["bound"]:c.reject("Original media watch remains unresolved",code="busy")
            if c.digest(read_control.load(root))!=digest:c.reject("Read owner changed during cleanup",code="conflict")
            (root/read_control.NAME).unlink()
            if value is not None:value["owner_unlinked"]=True
        fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    def recover(self,original=None):
        # Called after original Runtime leases have been stopped/reconciled.
        # A stale read owner never starts another read or a kernel generation.
        if original is None:
            if not (self.controller.runtime.root/"media-read-owner.json").exists():return True
            from demiflow.workbench.analysis import read_control
            owner=read_control.load(self.controller.runtime.root)
            original=dict(owner=owner,digest=c.digest(owner),owner_unlinked=False)
        from demiflow.workbench.analysis import read_control
        digest=original['digest']
        if not original['owner_unlinked']:
            if c.digest(read_control.load(self.controller.runtime.root))!=digest:
                c.reject('Original recovery read owner changed',code='conflict')
            self.controller.runtime.read_owner("stop-read-owner",digest)
            if any(lease.get("owner_digest")==digest and lease["state"]!="cleanup_complete" for lease in self.controller.runtime.leases.values()):return False
            for identifier,lease in list(self.controller.runtime.leases.items()):
                if lease.get("owner_digest")==digest:
                    self.controller.clean_job(identifier)
                    self.controller.runtime.retire_read(identifier)
        self._clear_owner(digest,original)
        return True

    def tick(self):
        value=self.current
        if value is None:return
        controller=self.controller;connection=value["connection"]
        try:
            if connection["channel"].closed or not value["authority"].peer.live():self.stop(value,"read_parent_or_consumer_ended")
            if value["digest"] is None and not value["stopped"]:
                if controller.control_reply_pending():return
                self._start(value)
            if value["owner"] is not None and time.monotonic()>=value["owner"]["deadline"] and not value["clean"] and not value["retired"]:
                self.stop(value,"read_deadline_expired")
            if value["digest"] is not None and not value["clean"] and not value["retired"]:
                status=controller.runtime.read_owner("read-owner-status",value["digest"])
                if status["stop_requested"]:self.stop(value,"independent_read_stop")
            identifier=value["job_id"]
            if identifier is not None and not value["clean"] and not value["retired"]:
                lease=controller.runtime.leases[identifier]
                if lease["state"]!="cleanup_complete":
                    if lease['state']!='cleanup_removing_cgroup':
                        observation=controller.runtime.observe(identifier)
                        if observation['state']=='unknown':
                            self.stop(value,'original_read_observation_unknown');return
                        if observation['state']=='alive':return
                    # A completed process may leave several queued frames.
                    # Consume its end/error/EOF before bridge cleanup; absence
                    # of a result still terminates at the fixed owner deadline.
                    if not value["ended"] and not value["stopped"]:return
                    value['cleanup_receipt']=controller.clean_job(identifier)
                if controller.runtime.leases[identifier]["state"]!="cleanup_complete":return
            elif identifier is None and not value["stopped"]:return
            if not value["clean"]:
                if identifier is not None and not value["retired"]:
                    # A prior OCI cleanup can have physically completed while
                    # its independent watch ACK was still unavailable. Recheck
                    # the full backend receipt before retiring this owner.
                    value['cleanup_receipt']=controller.clean_job(identifier)
                    controller.runtime.retire_read(identifier);value["retired"]=True
                if value["digest"] is not None:self._clear_owner(value["digest"],value)
                value["clean"]=True
            if value["error"] is not None:
                if value['operation']=='probe-media':
                    self.current=None;value['complete'](None,value['error']);return
                if not connection["channel"].closed:controller.reply(connection,value["error"])
                self.current=None;return
            if not value["ended"]:
                self.stop(value,"read_worker_ended_without_complete_reply");return
            if value['operation']=='probe-media':
                self.current=None;value['complete'](value['value'],None);return
            if value["task"] is None:
                authority=value["authority"]
                value["task"]=Task(authority.deployment,authority.peer,authority.project,
                    ReturnCase(controller,value) if value["operation"]=="case-page" else ReturnMedia(controller,value["witness"]) if value["operation"]=="artifact-media" else ReturnMetadata(controller.records,value["witness"],value["value"]),
                    "output-return",{},expected_principal=authority.principal)
            if not value["task"].step():return
            if value["value"] is not None:controller.reply(connection,value["task"].value)
            else:
                connection["channel"].queue("json",dict(format="demiflow_output_range_v1",artifact=({key:value["witness"]["artifact"][key] for key in ("id","generation","digest")} if value["operation"]=="artifact-media" else value["witness"]["artifact"]),metadata=value["metadata"]))
                connection["channel"].queue("blob",value["blob"]);connection["channel"].queue("end");connection["close_after_write"]=True
            value["task"].close();self.current=None
        except (ForgeError,OSError,EOFError) as error:
            value["error"]=dict(error=str(error)[:1024],code=getattr(error,"code","operation_failed"))
            self.stop(value,"owned_output_read_failed")
