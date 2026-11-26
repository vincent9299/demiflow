"""Explicit fixed-field media capture within the existing analysis ledger.

Admission, original request identity, artifact reservation and transfer charge
commit together. The controller advances one bounded opaque copy and the same
independent probe owner; restart/unknown delivery never replays an export/job.
"""
import time
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import execution_facts
from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.artifacts import _row
from demiflow.workbench.analysis.records import _source
from demiflow.workbench.analysis.records import _time
from demiflow.workbench.analysis.deferred_metadata import Task
from demiflow.workbench.analysis.media_ingress import MediaIngress
from demiflow.workbench.analysis.outbox import release_from_helper
from demiflow.workbench.analysis.cleanup import incomplete_object
from demiflow.workbench.analysis.execution_backend import cleanup_receipt
from demiflow.forge.schema import ForgeError

MIB=1024*1024


def _get(db,identifier):
    c.identity(identifier,'original media capture')
    row=db.execute('SELECT * FROM analysis_media_captures WHERE capture_id=?',(identifier,)).fetchone()
    if row is None:c.reject('Original media capture is unavailable',code='not_found')
    value=dict(row)
    for key in ('body','export_receipt','result'):
        if value[key] is not None:value[key]=c.decode(bytes(value[key]))
    return value


def _environment(row):
    value=row['body'].get('reader_environment')
    if value is None:return None
    c.fields(value,{'digest','body'},label='original capture reader environment')
    execution_facts.origin(value['body'])
    if c.digest(value['body'])!=value['digest']:
        c.reject('Original capture reader environment changed',code='conflict')
    return value


def _cleaned(db,row):
    environment=_environment(row)
    facts=execution_facts.cleanup(db,row['project'],'media',row['capture_id'],row['capture_id'])
    if facts is not None:return facts['body']['state']=='complete'
    return bool(row['cleanup_proven']) and (environment is None or environment['body']['mode']=='oci')


class Captures:
    def __init__(self,records,sources):self.records,self.sources=records,sources

    def admit(self,authority,*,request_id,digest,case_key,field):
        c.identity(request_id);c.sha(digest);c.text(case_key,'fixed media Case',20);c.text(field,'fixed media field',256)
        access=authority.access();access.check('analysis_write')
        payload=dict(digest=digest,case_key=case_key,field=field)
        def apply(db,now):
            if not getattr(self,'available',lambda:True)():c.reject('Original media capture is busy',code='busy')
            source=_source(db,access.project,digest)
            resource=self.sources.media_sources(authority,source,case_key,[field])[0]
            origin=self.sources._origin(access.project,digest)
            held=db.execute('SELECT coalesce(sum(held_input_bytes),0) FROM analysis_operations').fetchone()[0]
            held+=db.execute('SELECT coalesce(sum(held_input_bytes),0) FROM analysis_media_captures').fetchone()[0]
            if held+64*MIB>64*MIB:c.reject('The original analysis transfer reservation is busy',code='busy')
            total=db.execute('SELECT count(*) FROM analysis_operations').fetchone()[0]+db.execute('SELECT count(*) FROM analysis_media_captures').fetchone()[0]
            own=db.execute('SELECT count(*) FROM analysis_operations WHERE project=?',(access.project,)).fetchone()[0]+db.execute('SELECT count(*) FROM analysis_media_captures WHERE project=?',(access.project,)).fetchone()[0]
            if total>=2048 or own>=256:c.reject('Existing analysis operation history quota is full',code='resource_limit')
            identifier='m'+uuid.uuid4().hex;export_id='i'+uuid.uuid4().hex
            reservation=Artifacts._allocate(db,access.project,execution_id=identifier,generation=1,retention='pinned',parents=[],source_binding=digest)
            body=dict(payload=payload,locator=origin['locator'],source=resource['source'],mime=resource['mime'],reservation=reservation,web_parent=authority.peer.identity)
            reader=getattr(authority,'reader_environment',None)
            if reader is not None:
                execution_facts.origin(reader)
                body['reader_environment']=dict(digest=c.digest(reader),body=reader)
            db.execute('INSERT INTO analysis_media_captures(project,principal,request_id,capture_id,request_digest,source_snapshot,case_key,field,resource_binding,object_id,state,body,export_id,held_input_bytes,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (access.project,access.principal,request_id,identifier,c.digest(payload),digest,case_key,field,resource['source']['resource_binding'],reservation['id'],'admitted',c.encoded(body),export_id,64*MIB,now,now))
            return dict(status='pending',capture_id=identifier)
        result=self.records._mutate(access,'analysis_write',request_id,'media.capture',payload,apply)
        if result['replayed']:return self.receipt(authority,result)
        return result

    def get(self,identifier):
        with self.records.database.locked() as db:return _get(db,identifier)

    def update(self,identifier,*,state=None,body=None,export_receipt=None,result=None,cleanup=None,reason=None,input_released=False):
        with self.records.database.transaction(payload_bytes=48*1024) as db:
            row=_get(db,identifier)
            if row['state']=='ready' and state not in {None,'ready'}:c.reject('Committed media capture cannot be replaced',code='conflict')
            values=dict(updated=_time())
            if body is not None and body.get('reader_environment')!=row['body'].get('reader_environment'):
                c.reject('Original capture reader environment is immutable',code='conflict')
            for key,value in (('state',state),('body',body),('export_receipt',export_receipt),('result',result),('reason',reason)):
                if value is not None:values[key]=c.encoded(value,maximum=40*1024) if key in {'body','export_receipt','result'} else value
            if cleanup is not None:
                environment=_environment(row);mode=environment['body']['mode'] if environment else 'oci'
                cleaned=execution_facts.cleanup_complete(cleanup,mode=mode,job_id=None)
                if type(cleanup) is dict:
                    execution_facts.save_cleanup(db,project=row['project'],owner_kind='media',owner_id=identifier,
                        generation_id=identifier,body=cleanup.get('cleanup_receipt',cleanup),now=_time())
                values['cleanup_proven']=int(cleaned and mode=='oci')
            if input_released:values['held_input_bytes']=0
            db.execute('UPDATE analysis_media_captures SET '+','.join(key+'=?' for key in values)+' WHERE capture_id=?',(*values.values(),identifier))
        return self.get(identifier)

    def ready_result(self,authority,row):
        from demiflow.workbench.analysis.case_media import registered_witness
        with self.records.database.locked() as db:
            artifact=_row(db,row['project'],row['object_id'])
            if artifact['state']!='ready' or not _cleaned(db,row):return None
            manifest=c.decode(bytes(artifact['manifest']));media=manifest['payload']
            if media.get('format')!='media' or media['source']!=row['body']['source'] or media['mime']!=row['body']['mime']:
                c.reject('Original capture ready object differs',code='conflict')
            ref=dict(source_snapshot=row['source_snapshot'],case_key=row['case_key'],field=row['field'],artifact_id=row['object_id'],
                artifact_generation=artifact['generation'],expected_binding=artifact['manifest_digest'],expected_sha256=media['sha256'],mime=media['mime'])
        value=registered_witness(self.records,self.sources,authority,ref)
        return dict(status='ready',reference=ref,descriptor=value['artifact']['descriptor'])

    def receipt(self,authority,receipt):
        access=authority.access();access.check()
        row=self.get(receipt['capture_id'])
        if row['project']!=access.project or row['principal']!=access.principal or row['request_id']!=receipt['request_id']:
            c.reject('Media receipt belongs to another original request',code='conflict')
        result=self.ready_result(authority,row)
        if result is None:
            with self.records.database.locked() as db:cleaned=_cleaned(db,row)
            result=(dict(status='unavailable',reason='capture_failed',message=row['reason'] or 'Original media capture ended without ready publication')
                if cleaned else dict(status='pending',message='Original media capture or cleanup remains unconfirmed'))
        access.check();return dict(receipt,**result)


class CurrentCapture:
    def __init__(self,actor,row):self.actor,self.row,self.records=actor,row,actor.controller.records

    def route(self,authority,operation,payload):
        c.fields(payload,set(),label='original capture source return')
        access=authority.access();access.check('analysis_write');row=self.actor.ledger.get(self.row['capture_id'])
        if row['project']!=access.project or row['principal']!=access.principal or row['request_digest']!=self.row['request_digest']:
            c.reject('Original capture authority changed',code='conflict')
        source,_=self.actor.controller.sources.registered(access,row['source_snapshot'])
        current=self.actor.controller.sources.media_sources(authority,source,row['case_key'],[row['field']])[0]
        if current['source']!=row['body']['source'] or current['mime']!=row['body']['mime']:
            c.reject('Original capture resource declaration changed',code='conflict')
        access.check('analysis_write');return dict(verified=True)


class CaptureReturn:
    def __init__(self,actor,row):self.actor,self.row,self.records=actor,row,actor.controller.records

    def route(self,authority,operation,payload):
        c.fields(payload,set(),label='original capture result')
        original=self.records.receipt(authority.access(),self.row['request_id'])
        return self.actor.ledger.receipt(authority,original)


class MediaCaptures:
    def __init__(self,controller):
        self.controller=controller;self.ledger=Captures(controller.records,controller.sources);self.current=None

    def _cleanup_proof(self,value):
        row=value['row'];environment=_environment(row)
        if environment is None:
            return True  # Original isolated-only records retain their meaning.
        mode=environment['body']['mode']
        if mode!=self.controller.runtime.mode or value['read_id'] is not None and not value['probe_clean']:
            return False
        proof=cleanup_receipt(
            job_id=None,mode=mode,state='complete',processes=True,descendants=True,channels=True,temporary=True,
            reason='original_capture_managed_resources_released')
        proof['evidence_digest']=c.digest(dict(capture_id=row['capture_id'],
            read_request_id=value['read_id'],probe_clean=value['probe_clean'],owned_transfer_released=True))
        return dict(cleanup_complete=True,cleanup_proven=mode=='oci',cleanup_receipt=proof)

    def admit(self,connection,authority,result):
        if self.current is not None:c.reject('Original media capture remains busy',code='busy')
        row=self.ledger.get(result['capture_id'])
        if row['state']!='admitted':c.reject('Original capture cannot automatically restart',code='conflict')
        self.current=dict(connection=connection,authority=authority,row=row,phase='export',exchange=None,ingress=None,task=None,
            stopped=False,error=None,probe_clean=False,read_id=None,release_exchange=None,release_ack=False,deadline=time.monotonic()+55)
        connection['phase']='capture_pending'

    def _fresh(self,value):
        if value['task'] is None:
            authority=value['authority']
            value['task']=Task(authority.deployment,authority.peer,authority.project,CurrentCapture(self,value['row']),
                'capture-current',{},expected_principal=authority.principal)
        if not value['task'].step():return False
        value['task'].close();value['task']=None;return True

    def _probe_done(self,value,reply,error):
        value['probe_clean']=True
        if error is not None or value['stopped']:
            value['error']=error or value['error'];value['phase']='cleanup';return
        body=dict(value['row']['body'],probe=reply)
        value['row']=self.ledger.update(value['row']['capture_id'],state='verified',body=body)
        value['phase']='release'

    def _release(self,value):
        row=value['row'];site=self.controller.deployment.value['site']
        authority=value.get('authority')
        if authority is not None and authority.peer.live() and not value.get('release_ack'):
            if value['release_exchange'] is None:
                value['release_exchange']=authority.begin('release-source',dict(export_id=row['export_id'],source_snapshot=row['source_snapshot']))
            if not value['release_exchange'].advance():return False
            result=value['release_exchange'].value;value['release_exchange'].close();value['release_exchange']=None
            if result!={'released':True}:c.reject('Original Web export retirement was not confirmed',code='conflict')
            value['release_ack']=True
        def clean():
            return (value['ingress'] is None or value['ingress'].source_fd is None and value['ingress'].output_fd is None) and (
                value['read_id'] is None or value['probe_clean'])
        release_from_helper(site['host_transfer_root'],project=row['project'],export_id=row['export_id'],source_snapshot=row['source_snapshot'],
            allowed_web_uids=site['allowed_web_uids'],helper_gid=site['helper_gid'],witness=clean)
        value['row']=self.ledger.update(row['capture_id'],input_released=True)
        return True

    def stop(self,value,reason):
        value['stopped']=True;value['error']=value['error'] or dict(error=reason,code='operation_failed')
        if value['exchange'] is not None:value['exchange'].close();value['exchange']=None
        if value['task'] is not None:value['task'].close();value['task']=None
        if value['ingress'] is not None:value['ingress'].close()
        pending=self.controller.output_reads.current
        if pending is not None and pending.get('id')==value['read_id']:
            self.controller.output_reads.stop(pending,reason)
        value['phase']='cleanup'

    def disconnected(self,connection):
        if self.current is not None and self.current['connection'] is connection:self.stop(self.current,'Original media capture consumer ended')

    def tick(self):
        value=self.current
        if value is None:return
        if time.monotonic()<value.get('retry_at',0):return
        row=value['row'];authority=value['authority'];controller=self.controller
        try:
            if not value['stopped'] and (value['connection']['channel'].closed or not authority.peer.live() or time.monotonic()>=value['deadline']):
                self.stop(value,'Original media capture deadline or parent ended; reconcile its original request')
            phase=value['phase']
            if phase=='export':
                if value['exchange'] is None:
                    body=row['body'];p=body['payload']
                    value['exchange']=authority.begin('export-media',dict(export_id=row['export_id'],source_locator=body['locator'],
                        source_snapshot=row['source_snapshot'],case_key=p['case_key'],field=p['field']))
                if not value['exchange'].advance():return
                receipt=value['exchange'].value;value['exchange'].close();value['exchange']=None
                c.fields(receipt,{'source','sha256','bytes','mime'},label='original media export receipt')
                if receipt['source']!=row['body']['source'] or receipt['mime']!=row['body']['mime'] or receipt['sha256']!=row['body']['source']['declared_sha256']:
                    c.reject('Media exporter differs from its admitted resource',code='conflict')
                value['row']=row=self.ledger.update(row['capture_id'],state='exported',export_receipt=receipt)
                reservation=controller.artifacts.receiving(authority.access(),row['object_id']);reservation['seed_digest']=c.digest(reservation['seed'])
                value['ingress']=MediaIngress(controller.database,controller.deployment.paths.state,controller.deployment.value['site']['host_transfer_root'],
                    export_id=row['export_id'],reservation=reservation,receipt=receipt,web_uid=authority.peer.uid,helper_gid=controller.deployment.value['site']['helper_gid'])
                value['phase']='copy';return
            if phase=='copy':
                if value['ingress'].step():value['phase']='preprobe'
                return
            if phase=='preprobe':
                if controller.output_reads.current is not None or not self._fresh(value):return
                controller.output_reads.admit(value['connection'],authority,'probe-media',{},value['ingress'].config,
                    complete=lambda reply,error:self._probe_done(value,reply,error))
                value['read_id']=controller.output_reads.current['id']
                value['row']=self.ledger.update(row['capture_id'],state='probing',body=dict(row['body'],read_request_id=value['read_id']))
                value['phase']='probe';return
            if phase=='probe':return
            if phase=='release':
                if not self._release(value):return
                value['phase']='publish';return
            if phase=='publish':
                if not value['probe_clean'] or not self._fresh(value):return
                row=value['row'];ingress=value['ingress'];access=authority.access();access.check('analysis_write')
                receipt=ingress.prepare_after_probe(row['body']['probe'])
                controller.artifacts.prepared(access,row['object_id'],manifest=ingress.manifest,receipt=receipt)
                controller.artifacts.begin_publish(access,row['object_id'],expected_digest=ingress.digest)
                def authorized(publish,digest):
                    access.check('analysis_write')
                    with controller.database.locked() as db:
                        artifact=_row(db,row['project'],row['object_id'])
                        return artifact['publish_id']==publish and artifact['manifest_digest']==digest and artifact['state']=='publishing'
                ingress.rename(authorize=authorized)
                from demiflow.workbench.analysis.object_files import verify_object
                controller.artifacts.commit_ready(access,row['object_id'],expected_digest=ingress.digest,
                    verify_renamed=lambda identifier,digest:verify_object(controller.deployment.paths.state,identifier,digest))
                value['row']=row=self.ledger.update(row['capture_id'],state='ready',cleanup=self._cleanup_proof(value),input_released=True)
                value['phase']='return';return
            if phase=='return':
                if value['task'] is None:
                    value['task']=Task(authority.deployment,authority.peer,authority.project,CaptureReturn(self,value['row']),
                        'capture-result',{},expected_principal=authority.principal)
                if not value['task'].step():return
                result=value['task'].value;value['task'].close();value['task']=None
                if not value['connection']['channel'].closed:controller.reply(value['connection'],result)
                self.current=None;return
            if phase=='cleanup':
                if value['read_id'] is not None and not value['probe_clean']:return
                if not self._release(value):return
                row=value['row']
                state=controller.artifacts.system_cleanup_pending(row['project'],row['object_id'],reason='original_media_capture_ended')
                if not state['ready']:
                    witness=lambda:value['read_id'] is None or value['probe_clean']
                    incomplete_object(controller.artifacts,controller.deployment.paths.state,row['project'],row['object_id'],witness=witness)
                    controller.artifacts.system_cleanup_complete(row['project'],row['object_id'],verify_cleanup=lambda publish,obj:
                        witness() and not (controller.deployment.paths.state/'.staging'/publish).exists() and not (controller.deployment.paths.state/'objects'/obj).exists())
                    value['row']=self.ledger.update(row['capture_id'],state='failed',cleanup=self._cleanup_proof(value),input_released=True,reason='Original capture ended without ready publication; no automatic replay')
                else:value['row']=self.ledger.update(row['capture_id'],cleanup=self._cleanup_proof(value),input_released=True)
                if not value['connection']['channel'].closed:controller.reply(value['connection'],dict(status='pending',message='Original capture ended; reconcile its original receipt',request_id=row['request_id']))
                self.current=None
        except (ForgeError,OSError,EOFError) as error:
            if value['stopped']:
                # Cleanup uncertainty retains the original actor/charge. No
                # tight recovery retries or fresh request/job are generated.
                if value.get('release_exchange') is not None:value['release_exchange'].close();value['release_exchange']=None
                value['retry_at']=time.monotonic()+1;return
            value['error']=dict(error=str(error)[:512],code=getattr(error,'code','operation_failed'))
            self.stop(value,'Original fixed media capture failed')

    def pending_ids(self):
        with self.controller.database.locked() as db:
            identifiers=[row[0] for row in db.execute("""SELECT m.capture_id FROM analysis_media_captures m
                WHERE m.cleanup_proven=0 AND NOT EXISTS (
                    SELECT 1 FROM analysis_cleanup_receipts f WHERE f.project=m.project
                    AND f.owner_kind='media' AND f.owner_id=m.capture_id AND f.generation=m.capture_id
                    AND json_extract(CAST(f.body AS TEXT),'$.state')='complete') LIMIT 17""")]
        if len(identifiers)>16:c.reject('Original media cleanup exceeds artifact reservation bound',code='resource_limit')
        return identifiers

    def recover(self,*,identifiers=None):
        identifiers=self.pending_ids() if identifiers is None else identifiers
        c.sequence(identifiers,'original media recovery',16)
        pending=set(identifiers)
        for identifier in identifiers:
            c.identity(identifier)
            row=self.ledger.get(identifier);read_id=row['body'].get('read_request_id')
            with self.controller.database.locked() as db:
                if _cleaned(db,row):pending.discard(identifier);continue
            if (self.controller.runtime.root/'media-read-owner.json').exists():continue
            if read_id is not None and any(lease['execution_id']==read_id for lease in self.controller.runtime.leases.values()):continue
            if row['export_receipt'] is None:
                from demiflow.forge.process import process_identity
                original=row['body'].get('web_parent')
                if original is None:continue
                try:
                    if process_identity(original['pid'])==original:continue
                except (OSError,ValueError,KeyError):pass
            value=dict(row=row,read_id=read_id,probe_clean=True,ingress=None)
            try:
                if not self._release(value):continue
                state=self.controller.artifacts.system_cleanup_pending(row['project'],row['object_id'],reason='original_capture_recovery_no_replay')
                if not state['ready']:
                    incomplete_object(self.controller.artifacts,self.controller.deployment.paths.state,row['project'],row['object_id'],witness=lambda:True)
                    self.controller.artifacts.system_cleanup_complete(row['project'],row['object_id'],verify_cleanup=lambda publish,obj:
                        not (self.controller.deployment.paths.state/'.staging'/publish).exists() and not (self.controller.deployment.paths.state/'objects'/obj).exists())
                self.ledger.update(identifier,state='ready' if state['ready'] else 'failed',cleanup=self._cleanup_proof(value),input_released=True,
                    reason='Original capture recovery reconciled owned cleanup; no job replay')
                pending.discard(identifier)
            except (ForgeError,OSError):continue
        return [identifier for identifier in identifiers if identifier in pending]
