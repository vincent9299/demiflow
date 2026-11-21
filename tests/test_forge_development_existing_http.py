"""Actual loopback transport fixtures, never model/provider/business turns."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.conversations import http as routes
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.existing_http import ExistingHTTP
from demiflow.workbench.conversations.existing_http import ForwardedError
from demiflow.workbench.conversations.existing_http import FORMAT
from demiflow.workbench.conversations.existing_http import RESPONSE_BYTES
from demiflow.workbench.conversations.service import connect
from demiflow.workbench.conversations.service import Service
from demiflow.forge.schema import ForgeError

WORKSPACE = 'w'+'a'*32
CAPS = dict(format='demiflow_development_chat_v1', enabled=True, start=True, mode='trusted_local',
            providers=[dict(id='codex', model='fixture', available=True)], reason='Explicit transport fixture')
TOKEN = 'fixture-original-console-token'


@contextmanager
def peer(callback=None):
    calls=[];lock=threading.Lock();active=0;peak=0
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.0'
        def log_message(self,*args):pass
        def handle_request(self):
            nonlocal active,peak
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            call=dict(method=self.command,path=self.path,body=json.loads(raw) if raw else None,
                      bytes=len(raw),headers=dict(self.headers))
            with lock:calls.append(call);active+=1;peak=max(peak,active)
            try:
                if self.headers.get('X-Forge-Token')!=TOKEN:status,value=403,dict(code='unauthorized',error='Fixture authorization denied')
                elif callback is None:status,value=(200,CAPS) if self.path.endswith('dev-capabilities') else (202 if self.path.endswith('dev-turn-start') else 200,dict(id='fixture-result'))
                else:
                    reply=callback(self,call)
                    if reply is None:return
                    status,value=reply
                body=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()
                self.send_response(status);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.send_header('X-Forge-API-Contract','demiflow_forge_http_v1');self.end_headers();self.wfile.write(body)
            except (BrokenPipeError,ConnectionResetError):pass
            finally:
                with lock:active-=1
        do_GET=do_POST=handle_request
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02},daemon=True);thread.start()
    try:yield dict(origin='http://127.0.0.1:'+str(server.server_port),calls=calls,server=server,peak=lambda:peak)
    finally:server.shutdown();server.server_close();thread.join(2);assert not thread.is_alive()


def configuration(tmp_path, origin, *, mode='existing_grants', identity=None, read_seconds=2, write_seconds=2):
    root=tmp_path/'project';root.mkdir();(root/'code.py').write_text('# fixture only\n')
    source=tmp_path/'original-policy.json'
    value=dict(format='demiflow_development_deployment_v1',mode='trusted_local',state_root=str(tmp_path/'original-state'),
        projects={WORKSPACE:dict(root=str(root),kind='platform',phase='implementation',writes=['code.py'],
            watch=['code.py'],sources=[],non_goals=['No model/business turns'],authorization='Explicit private transport fixture')},
        providers={'codex':dict(executable=sys.executable,version='0.164.0',model='fixture')},
        host_budget=dict(memory_bytes=8*1024**3,run_memory_bytes=0,other_memory_bytes=1024**3,
            disk_bytes=4*1024**3,run_disk_bytes=0,other_disk_bytes=256*1024**2,device_path=str(tmp_path)),limits={'active':1})
    source.write_text(json.dumps(value));source.chmod(0o600)
    token=tmp_path/'original-console.token';token.write_text(TOKEN);token.chmod(0o600)
    config=dict(format=FORMAT,upstream=origin,upstream_identity=identity or process_identity(os.getpid()),
        source_deployment_config=dict(path=str(source),sha256=hashlib.sha256(source.read_bytes()).hexdigest()),
        token_file=str(token),projects={WORKSPACE:dict(root=str(root),identity=root_identity(root),mode=mode)},
        read_seconds=read_seconds,write_seconds=write_seconds)
    path=tmp_path/'bridge.json';path.write_text(json.dumps(config));path.chmod(0o600)
    return root,path,source,config


def invoke(client,root,name='dev-session',arguments=None,**kwargs):
    return client.forward(WORKSPACE,root,name,arguments or {'session_id':'fixture-session'},**kwargs)


def test_local_factory_keeps_original_single_owner_and_store(tmp_path):
    from test_forge_development_chat import configuration as local_configuration
    root,path,value=local_configuration(tmp_path)
    local=connect(path)
    try:
        assert isinstance(local,Service) and not local.remote
        assert local.capabilities('wfixture',root)['start']
        assert (Path(value['state_root'])/'service.lock').is_file()
        assert (Path(value['state_root'])/'chat.sqlite3').is_file()
        assert not local.workers
    finally:local.close()


def test_factory_and_existing_grants_preserve_every_public_route_ids_and_revision(tmp_path):
    with peer() as upstream:
        root,path,source,cfg=configuration(tmp_path,upstream['origin'])
        client=connect(path);assert isinstance(client,ExistingHTTP)
        try:
            assert client.capabilities(WORKSPACE,root)['start']
            reads={'dev-conversation':{},'dev-project-turns':{'offset':'0','limit':'2','kind':'timeline'},
                'dev-sessions':{'offset':'0','limit':'2'},'dev-session':{'session_id':'s'},
                'dev-context':{'session_id':'s','turn_id':'t'},'dev-events':{'session_id':'s','after_sequence':'0','wait_ms':'0'},
                'dev-output':{'session_id':'s','item_id':'i','after_sequence':'0'}}
            for name,args in reads.items():client.forward(WORKSPACE,root,name,args)
            writes={'dev-session-create':dict(request_id='original-create-id',provider_id='codex'),
                'dev-turn-start':dict(session_id='s',submission_id='original-send-id',expected_revision=7,text='fixture'),
                'dev-compact':dict(session_id='s',submission_id='original-compact-id',expected_revision=7),
                'dev-input':dict(session_id='s',turn_id='t',pending_request_id='p',expected_digest='d',answer={'q':'a'}),
                'dev-cancel':dict(session_id='s',turn_id='t',request_id='original-stop-id'),
                'dev-recover':dict(session_id='s',expected_revision=7)}
            for name,args in writes.items():client.forward(WORKSPACE,root,name,args,mutation=True)
            posts=[row for row in upstream['calls'] if row['method']=='POST']
            assert [row['body'] for row in posts]==list(writes.values())
            generation=dict(writes['dev-turn-start'],operation='notebook_cell',submission_id='original-cell-id')
            client.forward(WORKSPACE,root,'dev-turn-start',generation,mutation=True)
            assert upstream['calls'][-1]['body']==generation
            assert all(row['headers'].get('X-Forge-Token')==TOKEN for row in upstream['calls'])
            assert all('Cookie' not in row['headers'] and 'Authorization' not in row['headers'] for row in upstream['calls'])
            assert all(row['headers']['Origin']==upstream['origin'] for row in upstream['calls'])
            assert not (tmp_path/'original-state').exists(),'Bridge must not initialize any Store/lock/state'
        finally:client.close()
        assert client.diagnostics()['active_connections']==client.diagnostics()['monitor_threads']==0


@pytest.mark.parametrize('name',list(routes.POST_FIELDS))
def test_history_only_rejects_all_mutations_before_any_upstream_call(tmp_path,name):
    with peer() as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin'],mode='history_only')
        client=ExistingHTTP(path)
        try:
            required,_=routes.POST_FIELDS[name];args={key:'fixture' for key in required}
            with pytest.raises(ForwardedError) as error:client.forward(WORKSPACE,root,name,args,mutation=True)
            assert error.value.status==403 and error.value.payload['operation_started'] is False
            assert upstream['calls']==[]
            caps=client.capabilities(WORKSPACE,root);assert caps['enabled'] and not caps['start'] and caps['read_only']
            assert all(not p['available'] for p in caps['providers'])
        finally:client.close()


def test_demo_unauthorized_and_upstream_dynamic_denial_and_cancel_semantics(tmp_path):
    enabled=True;start=True
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,dict(CAPS,enabled=enabled,start=start)
        return 200,dict(state='stopping')
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        try:
            caps=client.capabilities('w'+'b'*32,root)
            assert not caps['enabled'] and caps['service_configured'] and not caps['project_authorized']
            assert upstream['calls']==[]
            assert client.capabilities(WORKSPACE,root)['start']
            enabled=False
            assert not client.capabilities(WORKSPACE,root)['enabled']
            with pytest.raises(ForwardedError):invoke(client,root,'dev-cancel',dict(session_id='s',turn_id='t',request_id='r'),mutation=True)
            assert not any(x['method']=='POST' for x in upstream['calls'])
            enabled=True;start=False
            assert invoke(client,root,'dev-cancel',dict(session_id='s',turn_id='t',request_id='r'),mutation=True)['state']=='stopping'
        finally:client.close()


def test_policy_process_root_drift_and_private_token_refuse_without_grant_or_adoption(tmp_path):
    with peer() as upstream:
        root,path,source,cfg=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        try:
            original=source.read_bytes();source.write_bytes(original+b' ')
            assert not client.capabilities(WORKSPACE,root)['enabled']
            source.write_bytes(original)
            client.identity=dict(client.identity,start='0')
            assert not client.capabilities(WORKSPACE,root)['enabled']
            client.identity=cfg['upstream_identity']
            moved=root.with_name('old-project');root.rename(moved);root.mkdir()
            with pytest.raises(ForgeError):invoke(client,root)
            root.rmdir();moved.rename(root)
            Path(cfg['token_file']).chmod(0o644)
            assert not client.capabilities(WORKSPACE,root)['enabled']
            assert upstream['calls']==[]
        finally:client.close()


def test_original_error_status_and_auth_namespace_are_preserved(tmp_path):
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        return 409,dict(code='conflict',error='Exact old CAS rejection',operation_started=False,extra='retained')
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        try:
            with pytest.raises(ForwardedError) as error:invoke(client,root)
            assert error.value.status==409 and error.value.payload==dict(code='conflict',error='Exact old CAS rejection',operation_started=False,extra='retained')
            Path(client.token_file).write_text('fixture-invalid-token')
            with pytest.raises(ForwardedError) as error:invoke(client,root)
            assert error.value.status==403 and error.value.payload['code']=='upstream_unauthorized'
        finally:client.close()


def test_write_ack_lost_is_unknown_ids_retained_and_no_automatic_retry(tmp_path):
    effects={};drop=True
    def reply(handler,call):
        nonlocal drop
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        key=call['body']['submission_id'];effects.setdefault(key,'same-original-turn')
        if drop:
            drop=False;handler.connection.shutdown(socket.SHUT_RDWR);return None
        return 202,dict(id=effects[key])
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        args=dict(session_id='s',submission_id='one-original-id',expected_revision=4,text='fixture')
        try:
            with pytest.raises(ForwardedError) as error:invoke(client,root,'dev-turn-start',args,mutation=True)
            assert error.value.status==502 and error.value.payload['dispatch']=='unknown'
            assert 'operation_started' not in error.value.payload
            assert len([x for x in upstream['calls'] if x['method']=='POST'])==1
            assert invoke(client,root,'dev-turn-start',args,mutation=True)['id']=='same-original-turn'
            assert len(effects)==1 and [x['body'] for x in upstream['calls'] if x['method']=='POST']==[args,args]
        finally:client.close()


def test_close_during_dispatched_write_retains_unknown_without_owner_cancel(tmp_path):
    entered=threading.Event();release=threading.Event();accepted=[];errors=[]
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        accepted.append(call['body']['submission_id']);entered.set();release.wait(2)
        return 202,dict(id='original-still-owned')
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        def call():
            try:invoke(client,root,'dev-turn-start',dict(session_id='s',submission_id='one-id',expected_revision=2,text='fixture'),mutation=True)
            except ForwardedError as error:errors.append(error)
        thread=threading.Thread(target=call);thread.start()
        try:
            assert entered.wait(2);client.close();thread.join(2)
            assert not thread.is_alive() and errors[0].payload['dispatch']=='unknown'
            assert accepted==['one-id']
            assert [x['path'].rsplit('/',1)[-1] for x in upstream['calls']]==['dev-capabilities','dev-turn-start']
        finally:release.set();thread.join(2);client.close()


def test_malformed_upstream_capabilities_do_not_authorize_a_mutation(tmp_path):
    with peer(lambda handler,call:(200,dict(enabled=True))) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        try:
            with pytest.raises(ForwardedError) as error:invoke(client,root,'dev-turn-start',dict(session_id='s',submission_id='id',expected_revision=0,text='fixture'),mutation=True)
            assert error.value.payload['operation_started'] is False
            assert len(upstream['calls'])==1 and upstream['calls'][0]['method']=='GET'
        finally:client.close()


@pytest.mark.parametrize('reason',['consumer','deadline','close','policy'])
def test_partial_header_and_detached_http10_body_cancel_release_transport_only(tmp_path,reason):
    entered=threading.Event();release=threading.Event();closed=threading.Event();errors=[]
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        header=b'HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 9\r\nX-Forge-API-Contract: demiflow_forge_http_v1\r\n'
        handler.connection.sendall(header+(b'\r\n{' if reason!='consumer' else b'X-Partial: '))
        entered.set();release.wait(3);return None
    with peer(reply) as upstream:
        root,path,source,_=configuration(tmp_path,upstream['origin'],read_seconds=.5 if reason=='deadline' else 2)
        client=ExistingHTTP(path)
        def call():
            try:invoke(client,root,consumer_closed=closed.is_set)
            except ForwardedError as error:errors.append(error)
        thread=threading.Thread(target=call);thread.start();assert entered.wait(2)
        started=time.monotonic()
        try:
            if reason=='consumer':closed.set()
            elif reason=='close':client.close()
            elif reason=='policy':source.write_bytes(source.read_bytes()+b' ')
            thread.join(2);assert not thread.is_alive() and len(errors)==1
            assert time.monotonic()-started<2 and client.diagnostics()['active_connections']==0
            assert all(x['method']=='GET' for x in upstream['calls'])
        finally:
            release.set();client.close();thread.join(2)


def test_two_slots_no_queue_and_bounded_single_monitor_shutdown(tmp_path):
    entered=threading.Barrier(3);release=threading.Event();errors=[]
    def reply(handler,call):entered.wait(2);release.wait(2);return 200,dict(id='read')
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        def call():
            try:invoke(client,root)
            except Exception as error:errors.append(error)
        threads=[threading.Thread(target=call) for _ in range(2)]
        try:
            for thread in threads:thread.start()
            entered.wait(2)
            with pytest.raises(ForwardedError) as error:invoke(client,root)
            assert error.value.status==503 and error.value.payload['operation_started'] is False
            assert len(upstream['calls'])==2 and client.diagnostics()['peak_connections']==2
        finally:
            release.set()
            for thread in threads:thread.join(2)
            client.close()
        assert not errors and client.diagnostics()['monitor_threads']==0
        assert upstream['peak']()<=2


def test_request_response_exact_bounds_and_no_silent_truncation(tmp_path):
    huge=None
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        return 200,huge if huge is not None else dict(id='input')
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        args=dict(session_id='s',turn_id='t',pending_request_id='p',expected_digest='d',answer='')
        args['answer']='x'*(65536-len(wire(args,65536)))
        try:
            assert len(wire(args,65536))==65536
            invoke(client,root,'dev-input',args,mutation=True)
            assert [x for x in upstream['calls'] if x['method']=='POST'][-1]['bytes']==65536
            args['answer']+='x';count=len(upstream['calls'])
            with pytest.raises(ForwardedError):invoke(client,root,'dev-input',args,mutation=True)
            assert len(upstream['calls'])==count
            huge=dict(chunks=[dict(text='')],next_sequence=1,last_sequence=1)
            huge['chunks'][0]['text']='x'*(RESPONSE_BYTES-len(wire(huge,RESPONSE_BYTES)))
            assert len(wire(huge,RESPONSE_BYTES))==RESPONSE_BYTES
            result=invoke(client,root,'dev-output',dict(session_id='s',item_id='i'))
            assert len(result['chunks'][0]['text'])==len(huge['chunks'][0]['text'])
            assert client.diagnostics()['peak_encoded_wire_buffer_bytes']<=RESPONSE_BYTES+131072
        finally:client.close()


@pytest.mark.parametrize('kind',['redirect','large','headers','json'])
def test_malformed_or_redirect_responses_do_not_escape_the_fixed_backend(tmp_path,kind):
    def reply(handler,call):
        if kind=='json':body=b'{"x":1,"x":2}'
        else:body=b'{}'
        handler.send_response(302 if kind=='redirect' else 200)
        handler.send_header('Content-Type','application/json');handler.send_header('X-Forge-API-Contract','demiflow_forge_http_v1')
        handler.send_header('Content-Length',str(RESPONSE_BYTES+1 if kind=='large' else len(body)))
        if kind=='redirect':handler.send_header('Location','http://example.invalid/not-followed')
        if kind=='headers':
            handler.send_header('X-First','x'*40000);handler.send_header('X-Second','x'*40000)
        handler.end_headers();handler.wfile.write(body);return None
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        try:
            with pytest.raises(ForwardedError):invoke(client,root)
            assert len(upstream['calls'])==1
        finally:client.close()


@pytest.mark.parametrize('origin',['https://127.0.0.1:1234','http://localhost:1234','http://127.0.0.1:1234/extra','http://user@127.0.0.1:1234','http://127.0.0.1:1234?x=1'])
def test_literal_origin_and_no_caller_project_or_path_injection(tmp_path,origin):
    root,path,_,cfg=configuration(tmp_path,origin)
    with pytest.raises(ForgeError):ExistingHTTP(path)


def test_actual_owned_upstream_process_exit_is_not_adopted(tmp_path):
    program=tmp_path/'fixture-peer.py'
    program.write_text("from http.server import HTTPServer,BaseHTTPRequestHandler\nimport json,time\n"
        "class H(BaseHTTPRequestHandler):\n def log_message(self,*a):pass\n def do_GET(self):\n"
        "  self.connection.sendall(b'HTTP/1.0 200 OK\\r\\nX-Partial: ');print('holding',flush=True);time.sleep(30)\n"
        "s=HTTPServer(('127.0.0.1',0),H);print(s.server_port,flush=True);s.serve_forever()\n")
    process=subprocess.Popen([sys.executable,'-I','-B',str(program)],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    client=None;thread=None;errors=[]
    try:
        with selectors.DefaultSelector() as poll:
            poll.register(process.stdout,selectors.EVENT_READ);assert poll.select(2)
            port=int(process.stdout.readline())
            root,path,_,_=configuration(tmp_path,'http://127.0.0.1:'+str(port),identity=process_identity(process.pid))
            client=ExistingHTTP(path)
            def call():
                try:invoke(client,root)
                except ForwardedError as error:errors.append(error)
            thread=threading.Thread(target=call);thread.start();assert poll.select(2)
            assert process.stdout.readline().strip()=='holding'
        original=process_identity(process.pid);process.terminate();process.wait(2)
        thread.join(2);assert not thread.is_alive() and errors and process_identity(original['pid'])!=original
        assert not client.capabilities(WORKSPACE,root)['enabled']
    finally:
        if process.poll() is None:process.kill();process.wait(2)
        process.stdout.close()
        if client is not None:client.close()
        if thread is not None:thread.join(2)


@pytest.mark.parametrize('cause',['deadline','policy'])
def test_nested_capability_and_write_share_one_deadline_and_midwrite_drift_is_unknown(tmp_path,cause):
    entered=threading.Event();release=threading.Event()
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):
            time.sleep(.3);return 200,CAPS
        entered.set();release.wait(2);return 202,dict(id='old-accepted')
    with peer(reply) as upstream:
        root,path,source,_=configuration(tmp_path,upstream['origin'],write_seconds=.6 if cause=='deadline' else 2)
        client=ExistingHTTP(path);args=dict(session_id='s',submission_id='one-id',expected_revision=3,text='fixture')
        errors=[]
        def call():
            try:invoke(client,root,'dev-turn-start',args,mutation=True)
            except ForwardedError as error:errors.append(error)
        started=time.monotonic();thread=threading.Thread(target=call);thread.start()
        try:
            assert entered.wait(2)
            if cause=='policy':source.write_bytes(source.read_bytes()+b' ')
            thread.join(1.5)
            assert not thread.is_alive() and errors[0].payload['dispatch']=='unknown'
            assert time.monotonic()-started<1.2
            assert len([x for x in upstream['calls'] if x['method']=='POST'])==1
        finally:release.set();thread.join(2);client.close()


def test_explicit_control_yields_only_own_read_transport_under_two_polls(tmp_path):
    entered=threading.Barrier(3);release=threading.Event();errors=[]
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        if call['method']=='POST':return 200,dict(state='stopping')
        entered.wait(2);release.wait(2);return 200,dict(events=[],next_sequence=0,last_sequence=0)
    with peer(reply) as upstream:
        root,path,_,_=configuration(tmp_path,upstream['origin']);client=ExistingHTTP(path)
        def read():
            try:invoke(client,root,'dev-events',{'session_id':'s','wait_ms':'1500'})
            except ForwardedError as error:errors.append(error)
        threads=[threading.Thread(target=read) for _ in range(2)]
        try:
            for thread in threads:thread.start()
            entered.wait(2);started=time.monotonic()
            assert invoke(client,root,'dev-cancel',dict(session_id='s',turn_id='t',request_id='one-stop'),mutation=True)['state']=='stopping'
            assert time.monotonic()-started<1
            assert client.diagnostics()['peak_connections']<=2
            assert [x['path'].rsplit('/',1)[-1] for x in upstream['calls'] if x['method']=='POST']==['dev-cancel']
        finally:
            release.set()
            for thread in threads:thread.join(2)
            client.close()
        assert errors and all(error.payload['operation_started'] is False for error in errors)


@contextmanager
def workbench(tmp_path,upstream,mode='existing_grants'):
    from demiflow.workbench.projects import registry as console
    from demiflow.workbench.api import WorkspaceServer
    root,path,source,cfg=configuration(tmp_path,upstream['origin'],mode=mode)
    registry=tmp_path/'registry.json';workspace=console.add(registry,root)['workspace_id']
    policy=json.loads(source.read_text());policy['projects'][workspace]=policy['projects'].pop(WORKSPACE)
    source.write_text(json.dumps(policy))
    cfg['projects'][workspace]=cfg['projects'].pop(WORKSPACE)
    cfg['source_deployment_config']['sha256']=hashlib.sha256(source.read_bytes()).hexdigest()
    path.write_text(json.dumps(cfg))
    server=WorkspaceServer(registry=registry,development_config=path)
    thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02},daemon=True);thread.start()
    try:yield server,workspace,root
    finally:server.shutdown();server.server_close();thread.join(2);assert not thread.is_alive()


def http_request(server,workspace,name,body=None,query='',**headers):
    request=urllib.request.Request(server.origin+'/api/workspaces/'+workspace+'/'+name+query,
        data=None if body is None else json.dumps(body).encode(),
        headers={'X-Forge-Token':server.token,'Content-Type':'application/json',**headers})
    try:response=urllib.request.urlopen(request,timeout=3)
    except urllib.error.HTTPError as error:response=error
    with response:return response.status,json.load(response)


def test_real_workbench_dispatches_public_context_and_errors_without_fake_store(tmp_path):
    def reply(handler,call):
        if call['path'].endswith('dev-capabilities'):return 200,CAPS
        if 'dev-context?' in call['path']:return 200,dict(context=dict(delivery='acknowledged',source='original-public-endpoint'))
        if call['method']=='POST':return 409,dict(code='conflict',error='Original exact CAS refusal',operation_started=False)
        return 200,dict(session=None)
    with peer(reply) as upstream,workbench(tmp_path,upstream) as (server,workspace,root):
        assert not hasattr(server.development,'store')
        assert http_request(server,workspace,'dev-capabilities')[1]['start']
        code,value=http_request(server,workspace,'dev-context',query='?session_id=s&turn_id=t',Cookie='never-forward',Authorization='never-forward')
        assert code==200 and value['context']['source']=='original-public-endpoint'
        code,value=http_request(server,workspace,'dev-turn-start',dict(session_id='s',submission_id='unchanged-id',expected_revision=9,text='fixture'))
        assert code==409 and value==dict(code='conflict',error='Original exact CAS refusal',operation_started=False)
        assert [x['body'] for x in upstream['calls'] if x['method']=='POST']==[dict(session_id='s',submission_id='unchanged-id',expected_revision=9,text='fixture')]
        assert all('Cookie' not in x['headers'] and 'Authorization' not in x['headers'] for x in upstream['calls'])
        assert all(x['headers'].get('X-Forge-Token')==TOKEN for x in upstream['calls'])
        assert not (tmp_path/'original-state').exists()


def test_real_downstream_disconnect_releases_existing_http_without_owner_control(tmp_path):
    entered=threading.Event();release=threading.Event()
    def reply(handler,call):
        handler.connection.sendall(b'HTTP/1.0 200 OK\r\nX-Incomplete: ')
        entered.set();release.wait(3);return None
    with peer(reply) as upstream,workbench(tmp_path,upstream) as (server,workspace,root):
        client=socket.create_connection(('127.0.0.1',server.server_port),timeout=2)
        client.sendall(('GET /api/workspaces/'+workspace+'/dev-events?session_id=s HTTP/1.1\r\nHost: 127.0.0.1:'+str(server.server_port)+'\r\nX-Forge-Token: '+server.token+'\r\n\r\n').encode())
        try:
            assert entered.wait(2);client.close();started=time.monotonic()
            while server.development.diagnostics()['active_connections'] and time.monotonic()-started<2:time.sleep(.02)
            assert server.development.diagnostics()['active_connections']==0
            assert all(x['method']=='GET' for x in upstream['calls'])
        finally:client.close();release.set()
