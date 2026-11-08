"""Private composition driver: real Web worker/outbox/SQLite and probe bytes.

Runtime/cgroup/watchdog/Task scheduling are explicit doubles. Codec inspection
runs in a bounded child, never the Web/helper process. This is not OCI/B1.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from types import SimpleNamespace


def web(context):
    from demiflow.workbench.analysis.outbox import Outbox
    from demiflow.workbench.analysis import web_sources
    from demiflow.workbench.worker import operation
    from demiflow.workbench.projects import workspace as ws
    from demiflow.forge.schema import ForgeError
    root=Path(context['workspace']);box=Outbox(context['outbox'])
    for line in sys.stdin:
        request=json.loads(line);name=request['operation'];payload=request['payload']
        try:
            if name=='media-sources':value=operation('analysis_media_sources',root,**payload)
            elif name=='export-media':value=box.export_media(root,project=context['project'],authorize=lambda:True,**payload)
            elif name=='release-source':value=box.release(project=context['project'],**payload)
            elif name=='verify-source':value=web_sources.verify(root,**payload)
            elif name=='resolve-source':value=web_sources.resolve(root,payload['locator'])
            elif name=='source-query':value=operation('analysis_source_read',root,**{key:payload[key] for key in ('source_locator','source_snapshot','query')})
            elif name=='remove-declaration':
                old=ws.describe_table(root,'rows');value=ws.set_table_view(root,'rows',expected=old['digest'],display={},provenance={})
            else:raise ValueError(name)
            print(json.dumps(dict(value=value)),flush=True)
        except ForgeError as error:print(json.dumps(dict(error=str(error),code=error.code)),flush=True)


def worker(mode,config,root):
    import resource
    resource.setrlimit(resource.RLIMIT_CORE,(0,0));resource.setrlimit(resource.RLIMIT_CPU,(3,3))
    resource.setrlimit(resource.RLIMIT_AS,(512*1024*1024,512*1024*1024))
    from demiflow.workbench.analysis.media_artifact_worker import probe
    from demiflow.workbench.analysis.media_artifact_worker import ready_range
    if mode=='probe':print(json.dumps(probe(config,pending_root=root)),flush=True)
    else:
        value=ready_range(config,objects_root=root);value['data']=value['data'].hex();print(json.dumps(value),flush=True)


def main(context,scenario):
    from demiflow.workbench.process import process_identity
    from demiflow.workbench.analysis import contracts as c
    from demiflow.workbench.analysis import media_captures
    from demiflow.workbench.analysis import output_reads
    from demiflow.workbench.analysis.database import Database
    from demiflow.workbench.analysis.database import FileSizeGuard
    from demiflow.workbench.analysis.records import Records
    from demiflow.workbench.analysis.records import Access
    from demiflow.workbench.analysis.sources import Sources
    from demiflow.workbench.analysis.metadata import Metadata
    from demiflow.workbench.analysis.artifacts import Artifacts
    from demiflow.workbench.analysis.ipc import Channel
    from demiflow.forge.schema import ForgeError
    # Web starts before the helper's own SQLite FSIZE hard bound is installed.
    process=subprocess.Popen([sys.executable,'-B',__file__,'web',sys.argv[2]],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    database=None;local=remote=None
    try:
        guard=FileSizeGuard(database_bytes=16*1024*1024,wal_bytes=8*1024*1024).install()
        state=Path(context['state']);state.mkdir(mode=0o700)
        database=Database(state/'database',guard=guard);records=Records(database);sources=Sources(records,state)
        grants={'read','analysis_write','case_annotate'};log=[];replies=[];ranges=[]
        class Exchange:
            def __init__(self,authority,name,payload):self.authority,self.name,self.payload=authority,name,payload;self.value=None
            def advance(self):self.value=self.authority.call(self.name,self.payload);return True
            def close(self):pass
        class Authority:
            project=context['project'];principal='fixture-reader';deployment=None
            peer=SimpleNamespace(uid=os.geteuid(),identity=process_identity(process.pid),live=lambda:process_identity(process.pid) is not None)
            def access(self):return Access(self.project,self.principal,lambda p,u,grant:grant in grants)
            def call(self,name,payload):
                process.stdin.write(json.dumps(dict(operation=name,payload=payload))+'\n');process.stdin.flush()
                value=json.loads(process.stdout.readline())
                if 'error' in value:c.reject(value['error'],code=value['code'])
                return value['value']
            def begin(self,name,payload):return Exchange(self,name,payload)
            def resolve_source(self,locator):return self.call('resolve-source',dict(locator=locator))
            def verify_source(self,source,target=None,*,locator):return self.call('verify-source',dict(source=source,target=target,locator=locator))==dict(verified=True)
        authority=Authority();access=authority.access();source=context['source']
        records.register_source(access,{k:v for k,v in source.items() if k!='digest'},verify=lambda actual,target=None:authority.verify_source(actual,target,locator=context['locator']))
        sources._remember(access,source,dict(kind='external',locator=context['locator']))
        class ImmediateTask:
            def __init__(self,deployment,peer,project,metadata,name,payload,**kwargs):self.metadata,self.name,self.payload,self.value=metadata,name,payload,None
            def step(self):self.value=self.metadata.route(authority,self.name,self.payload);return True
            def close(self):pass
        media_captures.Task=output_reads.Task=ImmediateTask
        class Runtime:
            mode='oci'
            root=state/'runtime';leases={};groups=SimpleNamespace(populated=lambda *args:False)
            def group(self,lease):pass
            def observe(self,key):return dict(state='exited')
            def read_owner(self,*args,**kwargs):return dict(bound=True,stop_requested=False)
            def stop(self,key,**kwargs):log.append('stop')
            def retire_read(self,key):log.append('retire');assert self.leases[key]['state']=='cleanup_complete';del self.leases[key]
        Runtime.root.mkdir(mode=0o700)
        class Controller:
            def __init__(self):
                self.database,self.records,self.sources=database,records,sources;self.artifacts=Artifacts(records);self.runtime=Runtime()
                self.watchdog=SimpleNamespace(read_owner=lambda *args,**kwargs:dict(bound=True,stop_requested=False))
                self.deployment=SimpleNamespace(paths=SimpleNamespace(state=state),value=dict(site=dict(host_transfer_root=context['outbox'],allowed_web_uids=[os.geteuid()],helper_gid=os.getegid())))
                self.output_reads=output_reads.OutputReads(self);self.media_captures=media_captures.MediaCaptures(self)
                self.metadata=Metadata(records,sources);self.metadata.media_available=lambda:self.media_captures.current is None
            def reply(self,connection,value):replies.append(value);connection['channel'].queue('json',value);connection['close_after_write']=True
            def clean_job(self,key):log.append('cleanup');self.runtime.leases[key]['state']='cleanup_complete'
            def control_reply_pending(self):return False
        controller=Controller();reads=controller.output_reads;actor=controller.media_captures
        reads._clear_owner=lambda *args:log.append('clear-owner')
        def start_read(value):
            key='fixture-job';value.update(job_id=key,digest='a'*64,owner=dict(deadline=time.monotonic()+5))
            controller.runtime.leases[key]=dict(state='running',execution_id=value['id'],cgroup=dict(cgroup_identity={}))
            mode='probe' if value['operation']=='probe-media' else 'range'
            root=state/'.staging'/value['config']['reservation']['publish_id']/'payload' if mode=='probe' else state/'objects'
            if mode=='probe':
                assert not controller.artifacts.status(authority.access(),value['config']['reservation']['seed']['object_id'])['ready']
                log.append('probe')
            command=[sys.executable,'-B',__file__,mode,str(root)]
            run=subprocess.run(command,input=json.dumps(value['config']),capture_output=True,text=True,timeout=5)
            assert run.returncode==0,run.stderr
            result=json.loads(run.stdout)
            worker_local,worker_remote=socket.socketpair();worker_channel=Channel(worker_local)
            connection=dict(owner=value,job_id=key,kind=value['bridge'],channel=worker_channel)
            if mode=='probe':reads.frame(connection,'json',result)
            else:
                ranges.append(result)
                artifact={k:value['config']['artifact'][k] for k in ('id','generation','digest')}
                reads.frame(connection,'json',dict(operation='artifact-media',artifact=artifact,metadata=result['metadata']))
                reads.frame(connection,'blob',bytes.fromhex(result['data']))
            reads.frame(connection,'end',None)
            connection['channel'].outgoing.clear();connection['channel'].outgoing_bytes=0
            worker_channel.close();worker_remote.close()
        reads._start=start_read
        local,remote=socket.socketpair();connection=dict(channel=Channel(local),phase='header',close_after_write=False)
        if scenario=='rpc':
            print(json.dumps(dict(ready=True)),flush=True)
            for line in sys.stdin:
                command=json.loads(line);grants.clear();grants.update(name for name,allowed in command['permissions'].items() if allowed)
                try:
                    name=command['name'];payload=command['payload'];connection['close_after_write']=False
                    result=controller.metadata.route(authority,name,payload)
                    if name=='media-capture' and result['status']=='pending' and not result.get('replayed'):
                        actor.admit(connection,authority,result)
                        for _ in range(100):
                            actor.tick();reads.tick()
                            if actor.current is None:break
                        assert actor.current is None,(actor.current and actor.current['phase'],actor.current and actor.current['error'])
                        result=controller.metadata.route(authority,'receipt',dict(request_id=payload['request_id']))
                    elif name=='artifact-media':
                        reads.admit(connection,authority,name,payload,result)
                        for _ in range(8):
                            reads.tick()
                            if reads.current is None:break
                        assert reads.current is None
                        result=dict(artifact={key:result['artifact'][key] for key in ('id','generation','digest')},**ranges[-1],format='fixture-verified-range')
                    print(json.dumps(dict(result=result)),flush=True)
                    # This fixture forwards JSON through its parent IPC. The
                    # real shared Channel still receives bounded ACK/range
                    # frames; drain them so no projection survives a request.
                    connection['channel'].outgoing.clear();connection['channel'].outgoing_bytes=0
                except ForgeError as error:print(json.dumps(dict(error=str(error),code=error.code)),flush=True)
            return
        payload=dict(request_id='original-capture',digest=source['digest'],case_key='0',field='image_path')
        assert controller.metadata.route(authority,'case-media',{k:v for k,v in payload.items() if k!='request_id'})['status']=='capture_required'
        result=controller.metadata.route(authority,'media-capture',payload);actor.admit(connection,authority,result)
        revoked=False
        for turn in range(100):
            if scenario=='revoked' and actor.current and actor.current['phase']=='preprobe' and not revoked:
                grants.remove('analysis_write');revoked=True
            actor.tick();reads.tick()
            if actor.current is None:break
        assert actor.current is None,(actor.current and actor.current['phase'],actor.current and actor.current['error'])
        receipt=controller.metadata.route(authority,'receipt',dict(request_id=payload['request_id']))
        assert not list(Path(context['outbox']).iterdir())
        assert database.connection.execute('SELECT sum(held_input_bytes) FROM analysis_media_captures').fetchone()[0]==0
        if scenario=='revoked':
            assert revoked and receipt['status']=='unavailable' and 'probe' not in log
            assert database.connection.execute("SELECT count(*) FROM analysis_objects WHERE state='ready'").fetchone()[0]==0
            print(json.dumps(dict(ok=True,scenario=scenario,log=log,receipt_status=receipt['status'],source=source['digest'])),flush=True);return
        assert receipt['status']=='ready' and replies[-1]['status']=='ready',replies
        assert log.index('probe')<log.index('cleanup')<log.index('retire')<log.index('clear-owner')
        ref=receipt['reference'];lookup=controller.metadata.route(authority,'case-media',{k:v for k,v in payload.items() if k!='request_id'})
        assert lookup['reference']==ref and lookup['descriptor']==receipt['descriptor']
        replay=controller.metadata.route(authority,'media-capture',payload);assert replay['replayed'] and replay['reference']==ref
        grants.remove('analysis_write')
        witness=controller.metadata.route(authority,'artifact-media',dict(reference=ref,range='bytes=0-0'))
        reads.admit(connection,authority,'artifact-media',dict(reference=ref,range='bytes=0-0'),witness)
        for _ in range(8):
            reads.tick()
            if reads.current is None:break
        assert reads.current is None and not controller.runtime.leases
        definition=dict(id='quality',title='质量',type='choice',required=True,options=[dict(id='yes',label='可用'),dict(id='no',label='不可用')])
        target=dict(source=source['digest'],case_key='0',variant=None,field=None)
        entry=dict(definition=definition,expected_revision=0,action='set',value='yes',media_basis=[ref])
        answer=dict(request_id='answer-with-basis',target=target,entries=[entry],origin=dict(kind='unsaved_view',reference='owned-case-view'))
        saved=controller.metadata.route(authority,'annotations-save',answer)
        assert saved['heads'][0]['media_basis']==[ref]
        assert controller.metadata.route(authority,'receipt',dict(request_id=answer['request_id']))['heads']==saved['heads']
        assert controller.metadata.route(authority,'annotations-save',answer)['replayed']
        for changed in ({**ref,'artifact_generation':2},{**ref,'case_key':'1'},{**ref,'expected_sha256':'e'*64}):
            try:controller.metadata.route(authority,'artifact-media-current',dict(reference=changed))
            except ForgeError:pass
            else:raise AssertionError('foreign ready reference accepted')
        try:controller.metadata.route(authority,'annotations-save',dict(answer,request_id='cas-competitor'))
        except ForgeError as error:assert error.code=='conflict'
        else:raise AssertionError('stale answer CAS accepted')
        history=controller.metadata.route(authority,'annotations',dict(target=target,definition=c.digest(definition)))
        assert history['items'][0]['media_basis']==[ref]
        authority.call('remove-declaration',{})
        try:controller.metadata.route(authority,'annotations-save',dict(answer,request_id='resource-revoked',entries=[dict(entry,expected_revision=1)]))
        except ForgeError:pass
        else:raise AssertionError('revoked resource basis accepted')
        assert database.connection.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==1
        assert database.connection.execute('SELECT count(*) FROM analysis_sessions').fetchone()[0]==0
        assert database.connection.execute('SELECT count(*) FROM analysis_executions').fetchone()[0]==0
        database.close();database=Database(state/'database',guard=guard);reopened=Records(database)
        assert reopened.annotations(authority.access(),target=target,definition=c.digest(definition))['items'][0]['media_basis']==[ref]
        print(json.dumps(dict(ok=True,scenario=scenario,log=log,reference=ref,receipt_status=receipt['status'],history_reopened=True,no_notebook_created=True)),flush=True)
    finally:
        if database is not None:database.close()
        if local is not None:local.close();remote.close()
        process.stdin.close()
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        error=process.stderr.read();process.stdout.close();process.stderr.close()
        if process.returncode:raise AssertionError(error)


if __name__=='__main__':
    mode=sys.argv[1]
    if mode in {'probe','range'}:worker(mode,json.load(sys.stdin),sys.argv[2])
    else:
        context=json.loads(Path(sys.argv[2]).read_text())
        if mode=='web':web(context)
        else:main(context,mode)
