import json
from pathlib import Path
import sys
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

from demiflow.workbench.projects import registry as console
from demiflow.workbench.api import WorkspaceServer
from test_forge_development_chat import configuration


@contextmanager
def app(tmp_path, *, acp=False, guided=False):
    root,path,value=configuration(tmp_path)
    if guided:
        from demiflow.forge.project import initialize_project
        initialize_project(root, name='Guided project', existing=True)
        (root / 'flows').mkdir()
        (root / 'prompts').mkdir()
        value['projects']['wfixture'].update(kind='pipeline', writes=['flows', 'prompts'],
                                             watch=['flows', 'prompts'])
    registry=tmp_path/'registry.json';created=console.add(registry,root)
    workspace=created['workspace_id']
    value['projects'][workspace]=value['projects'].pop('wfixture');path.write_text(json.dumps(value))
    if acp:
        fixture = Path(__file__).with_name('devchat_fake_acp.py')
        executable = tmp_path / 'fake-acp'
        executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
        executable.chmod(0o700)
        value['providers'] = {'deepseek': dict(executable=str(executable), version='0.2.1-rc.2', model='fixture')}
        path.write_text(json.dumps(value))
    server=WorkspaceServer(registry=registry,development_config=path)
    if acp:
        original = server.development.capabilities
        def fixture_capabilities(*args):
            caps = original(*args)
            caps['providers'][0].update(available=True, reason='Explicit protocol fixture; no model')
            return caps
        server.development.capabilities = fixture_capabilities
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try: yield server,workspace,root
    finally: server.shutdown();server.server_close();thread.join()


def request(server,workspace,name,body=None,query='',token=None):
    req=urllib.request.Request(server.origin+'/api/workspaces/'+workspace+'/'+name+query,
        data=json.dumps(body).encode() if body is not None else None,
        headers={'X-Forge-Token':server.token if token is None else token,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=5) as response:return response.status,json.load(response)
    except urllib.error.HTTPError as exc:return exc.code,json.load(exc)


def legacy_history(server,workspace,root):
    """Persist the old ACP shape without a model or rewriting previous events."""
    from demiflow.workbench.conversations.common import root_identity
    service=server.development;store=service.store
    sid=service.create_project(workspace,root,'history-fixture','deepseek')['id']
    rows=[]
    for revision in range(2):
        name=('第一轮','第二轮')[revision]
        turn,_=store.submit(workspace,sid,'history-'+str(revision),revision,name+'要求',root_identity(root),service.policy_hash)
        events=[]
        events.append(store.event(turn['id'],'message.delta',dict(item_id='acp-interval:client-3:answer',text=name+'进展')))
        events.append(store.event(turn['id'],'item.updated',dict(id='same-tool',type='tool',title=name+'工具',status='completed')))
        events.append(store.event(turn['id'],'message.delta',dict(item_id='acp-interval:client-3:answer',text=name+'结果')))
        store.finish_provider(turn['id'],state='completed',cleanup='complete',result={})
        rows.append((turn,events))
    return sid,rows


def test_output_segment_preserves_turn_epoch_and_sequence_scope(tmp_path):
    with app(tmp_path,acp=True) as (server,w,root):
        sid,rows=legacy_history(server,w,root)
        turn,events=rows[-1];final=events[-1]
        before=server.development.store.events(w,sid)['events']
        scope=dict(session_id=sid,item_id=final['data']['item_id'],turn_id=turn['id'],provider_epoch=turn['epoch'],
                   from_sequence=final['sequence'],through_sequence=final['sequence'])
        def read(**changes):return request(server,w,'dev-output',query='?'+urlencode(dict(scope,**changes)))
        status,value=read();assert status==200,value
        assert [chunk['text'] for chunk in value['chunks']]==['第二轮结果']
        assert value['next_sequence']==value['last_sequence']==final['sequence']
        assert read(provider_epoch='another-epoch')[1]['chunks']==[]
        # Existing clients keep the old unscoped route contract.
        old=request(server,w,'dev-output',query='?'+urlencode(dict(session_id=sid,item_id=scope['item_id'])))
        assert len(old[1]['chunks'])==4
        for patch in (dict(from_sequence=-1),dict(from_sequence=final['sequence']+1),
                      dict(through_sequence=2**53+1),dict(after_sequence=-1)):
            assert read(**patch)[0]==400
        assert read(through_sequence=before[-1]['sequence']+1)[0]==409
        assert read(after_sequence=final['sequence']+1)[0]==409
        incomplete=dict(scope);incomplete.pop('through_sequence')
        assert request(server,w,'dev-output',query='?'+urlencode(incomplete))[0]==400
        other=server.development.create(w,root,'other-history','deepseek')['id']
        assert read(session_id=other)[0]==404
        assert server.development.store.events(w,sid)['events']==before


def test_authenticated_routes_and_stop_under_read_load(tmp_path):
    with app(tmp_path) as (server,w,root):
        # Exercise saturation at a small explicit budget, independently of the
        # production subscription capacity and without consuming the stop slot.
        server.development_readers = threading.BoundedSemaphore(2)
        assert request(server,w,'dev-capabilities',token='wrong')[0]==403
        assert request(server,w,'dev-capabilities')[1]['start']
        assert request(server,w,'dev-session-create',{'request_id':'c','provider_id':'codex','cwd':'/tmp'})[0]==400
        status,created=request(server,w,'dev-session-create',{'request_id':'c','provider_id':'codex'});assert status==200,created
        sid=created['id'];status,t=request(server,w,'dev-turn-start',{'session_id':sid,'submission_id':'s','expected_revision':0,'text':'hang'})
        assert status==202,t
        until=time.monotonic()+5
        while server.development.store.turn(t['id'])['state']!='running' and time.monotonic()<until:time.sleep(.05)
        other=server.development.create(w,root,'legacy-other','codex')['id']
        original_pipe=server.development.workers[t['id']][1]
        assert request(server,w,'dev-cancel',{'session_id':other,'turn_id':t['id'],'request_id':'wrong-target'})[0]==404
        time.sleep(.1)
        assert server.development.workers[t['id']][1]==original_pipe
        assert server.development.store.turn(t['id'])['stop_at'] is None
        assert server.development.store.turn(t['id'])['state']=='running'
        sequence=server.development.store.show(w,sid)['sequence']
        query='?session_id='+sid+'&after_sequence='+str(sequence)+'&wait_ms=1500'
        with ThreadPoolExecutor(max_workers=3) as pool:
            reads=[pool.submit(request,server,w,'dev-events',query=query) for _ in range(2)]
            time.sleep(.15)
            assert request(server,w,'dev-events',query=query)[0]==503
            started=time.monotonic();code,result=request(server,w,'dev-cancel',{'session_id':sid,'turn_id':t['id'],'request_id':'stop'})
            assert code==200 and result['state']=='stopping' and time.monotonic()-started<1
            assert all(f.result()[0]==200 for f in reads)
        assert request(server,w,'dev-events',query='?session_id='+sid+'&after_sequence=-1')[0]==400
        assert request(server,w,'dev-recover',{'session_id':sid,'expected_revision':99})[0]==409
