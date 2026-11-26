"""Fixed model channel protocol and shared-turn quotas, without real model calls."""
import json
from pathlib import Path
import shutil
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import model_broker as broker
from demiflow.workbench.conversations.deepseek import DeepSeek
from demiflow.workbench.conversations.policy import Limits


def options():
    return dict(provider='fixture', model='fixture-model', reasoningEffort='high',
        messages=[dict(id='m', role='user', source={'kind':'user'}, content=[dict(type='text', text='fixture')])], tools=[])


@pytest.fixture
def native(tmp_path, monkeypatch):
    node = shutil.which('node')
    if not node: pytest.skip('Node is needed for the native-service protocol fixture')
    plugin = Path(broker.__file__).with_name('deepseek_model_broker.cjs')
    driver = tmp_path/'llm.cjs'
    driver.write_text('''
const plugin=require(process.argv[2]);
plugin.apply({effect(){},llm:{
 async resolveModelInfo(provider,id){return {provider,id,name:id,defaultMaxTokens:131072,inputModalities:['text','image']};},
 async *stream(options){
  if(options.provider!=='fixture'||options.model!=='fixture-model'||options.maxTokens>65536)throw new Error('route');
  yield {type:'text-delta',index:0,text:'fixture response'};
  yield {type:'finish',reason:{kind:'stop'}};
 }
}}, {provider:'fixture',model:'fixture-model',reasoningEffort:'high'});
''')
    monkeypatch.setattr(broker, 'capabilities', lambda *_:dict(available=True))
    def launch(config, home, root):
        assert config['_model_broker'] is True
        return [node,str(driver),str(plugin)], dict(PATH='/usr/bin:/bin',HOME=str(home))
    monkeypatch.setattr(DeepSeek, 'launch', launch)
    deployment = SimpleNamespace(limits=Limits(), providers={'deepseek':dict(model='fixture-model',model_provider='fixture',effort='high',native_credentials='/not-exposed')})
    def start(epoch='first', turn_id='turn', quota_turn=None):
        home=tmp_path/'session/control'/('turn-'+epoch)/'model'
        return broker.ModelBroker(deployment,dict(id=turn_id,created=time.time(),
            forge_options=json.dumps(dict(quota_turn=quota_turn)) if quota_turn else '{}'),dict(provider='deepseek'),home)
    return start


def exchange(channel, method, value, token=None):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(5); client.connect(str(channel.socket_path))
        client.sendall(json.dumps(dict(token=token or channel.token,method=method,options=value)).encode()+b'\n')
        stream=client.makefile('rb'); rows=[]
        while True:
            raw=stream.readline()
            if not raw: return rows
            row=json.loads(raw); rows.append(row)
            if row.get('done') or row.get('error'): return rows


def test_native_metadata_and_stream_are_fixed_and_hide_credentials(native):
    channel=native()
    try:
        assert 'native_credentials' not in channel.child_config
        assert channel.child_config['_boundary']['token'] == channel.token
        rows=exchange(channel,'resolve',dict(provider='fixture',model='fixture-model'))
        assert rows[-1] == {'done':True}
        assert rows[0]['value']['defaultMaxTokens'] == 65536
        assert rows[0]['value']['inputModalities'] == ['text']
        assert channel.requests == 0
        rows=exchange(channel,'stream',options())
        assert rows[0]['value']['text'] == 'fixture response'
        assert rows[-1] == {'done':True}
        assert channel.requests == channel.quota.value['requests'] == 1
    finally: channel.close()
    assert channel.process.poll() is not None and not channel.socket_path.exists()


@pytest.mark.parametrize('mutation', [
    lambda v:v.update(provider='other'),
    lambda v:v.update(model='other'),
    lambda v:v.update(maxTokens=65537),
    lambda v:v.update(reasoningEffort='max'),
    lambda v:v.update(base_url='https://elsewhere.invalid'),
    lambda v:v['messages'][0].update(content=[dict(type='image',attachment={'path':'/protected'})]),
    lambda v:v['messages'][0].update(content=[dict(type='future-file',path='/protected')]),
])
def test_untrusted_options_do_not_reach_native_adapter(native, mutation):
    channel=native()
    try:
        value=options(); mutation(value)
        assert exchange(channel,'stream',value)[-1].get('error')
        assert channel.requests == 0
    finally: channel.close()


def test_wrong_token_cannot_dispatch_and_valid_next_request_still_works(native):
    channel=native()
    try:
        assert exchange(channel,'stream',options(),token='0'*64)[-1].get('error')
        assert channel.requests == 0
        assert exchange(channel,'stream',options())[-1] == {'done':True}
    finally: channel.close()


def test_repair_epoch_cannot_reset_root_turn_request_allowance(native, monkeypatch):
    monkeypatch.setattr(broker,'REQUESTS',1)
    first=native('one')
    try: assert exchange(first,'stream',options())[-1] == {'done':True}
    finally: first.close()
    second=native('two')
    try:
        assert exchange(second,'stream',options())[-1].get('error')
        assert second.quota.value['requests'] == 1
    finally: second.close()


def test_new_feedback_turn_and_warm_handoff_keep_original_quota(native, monkeypatch):
    monkeypatch.setattr(broker, 'REQUESTS', 1)
    first = native('author', turn_id='original')
    try:
        assert exchange(first, 'stream', options())[-1] == {'done': True}
    finally:
        first.close()
    second = native('feedback', turn_id='repair', quota_turn='original')
    try:
        assert exchange(second, 'stream', options())[-1].get('error')
        second.park_idle()
        second.begin_turn(dict(id='warm-repair', created=time.time(),
            forge_options=json.dumps(dict(quota_turn='original'))), Limits())
        assert exchange(second, 'stream', options())[-1].get('error')
        assert second.quota.value['requests'] == 1
    finally:
        second.close()


def test_quota_rejects_overlapping_owner_corruption_and_alias(tmp_path):
    path=tmp_path/'quota'; first=broker._Quota(path,'turn',65536)
    try:
        with pytest.raises(BlockingIOError): broker._Quota(path,'turn',65536)
        first.charge(requests=1,output=65536)
        with pytest.raises(ForgeError): first.charge(output=1)
    finally: first.close()
    path.write_text('{partial')
    with pytest.raises(ValueError): broker._Quota(path,'turn',65536)
    path.unlink(); target=tmp_path/'private';target.write_text('preserved');path.symlink_to(target)
    with pytest.raises(OSError): broker._Quota(path,'turn',65536)
    assert target.read_text()=='preserved'


def test_capabilities_do_not_infer_codex_app_auth(monkeypatch):
    assert not broker.capabilities('codex',{})['available']
    monkeypatch.setenv('FIXED_MODEL_TEST_KEY','fixture')
    config=dict(base_url='https://example.invalid/v1',credential_env='FIXED_MODEL_TEST_KEY')
    assert broker.capabilities('codex',config)['available']
    config['base_url']='http://example.invalid/v1'
    assert not broker.capabilities('codex',config)['available']


@pytest.mark.parametrize('update',[
    {'previous_response_id':'other-response'}, {'conversation':'other-session'}, {'prompt':{'id':'remote'}},
    {'input':[{'type':'item_reference','id':'remote'}]},
    {'input':[{'role':'user','content':[{'type':'input_file','file_id':'remote'}]}]},
    {'input':[{'role':'user','content':[{'type':'input_image','image_url':'https://example.invalid'}]}]},
    {'tools':[{'type':'web_search'}]}, {'tools':[{'type':'namespace','tools':[{'type':'computer_use_preview'}]}]},
    {'max_output_tokens':65537},
])
def test_responses_rejects_remote_account_content_and_hosted_tools(update):
    with pytest.raises(ForgeError):
        broker.validate_responses({'model':'fixture','input':'text',**update},{'model':'fixture'})


def test_responses_text_tool_history_remains_supported_with_fixed_output_cap():
    value=broker.validate_responses(dict(model='fixture',input=[
        {'role':'user','content':[{'type':'input_text','text':'fixture'}]},
        {'type':'function_call','call_id':'c','name':'shell','arguments':'{}'},
        {'type':'function_call_output','call_id':'c','output':'bounded fixture output'},
    ],tools=[{'type':'function','name':'shell','parameters':{'type':'object'}}]),{'model':'fixture'})
    assert value['max_output_tokens']==65536 and value['store'] is False


@pytest.mark.parametrize('phase',['headers','body'])
def test_http_cancel_drains_blocked_upstream_and_releases_quota(tmp_path,monkeypatch,phase):
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(5)
    entered=threading.Event();release=threading.Event();captured={}
    def upstream():
        try:
            conn,_=listener.accept()
            with conn:
                conn.settimeout(5);data=bytearray()
                while b'\r\n\r\n' not in data:data.extend(conn.recv(65536))
                header,body=bytes(data).split(b'\r\n\r\n',1)
                size=int(next(row.split(b':',1)[1] for row in header.split(b'\r\n') if row.lower().startswith(b'content-length:')))
                while len(body)<size:body+=conn.recv(65536)
                captured['body']=json.loads(body)
                if phase=='body':conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{')
                entered.set();release.wait(10)
        except OSError:pass
    server=threading.Thread(target=upstream);server.start()
    monkeypatch.setenv('FIXED_MODEL_TEST_KEY','fixture-private')
    config=dict(base_url='http://127.0.0.1:'+str(listener.getsockname()[1])+'/v1',credential_env='FIXED_MODEL_TEST_KEY',model='fixture')
    deployment=SimpleNamespace(limits=Limits(),providers={'codex':config})
    channel=broker.ModelBroker(deployment,dict(id='turn',created=time.time()),dict(provider='codex'),tmp_path/'session/control/turn-first/model')
    client=socket.socket(socket.AF_UNIX)
    try:
        client.connect(str(channel.socket_path));raw=json.dumps({'model':'fixture','input':'fixture'}).encode()
        client.sendall(('POST /v1/responses HTTP/1.1\r\nHost: fixture\r\nAuthorization: Bearer '+channel.token+'\r\nContent-Length: '+str(len(raw))+'\r\n\r\n').encode()+raw)
        assert entered.wait(6)
        helper=channel.http_process
        before=time.monotonic();channel.close()
        assert time.monotonic()-before<3
        assert not channel.thread.is_alive() and channel.quota.fd is None
        assert helper.poll() is not None
        assert captured['body']['max_output_tokens']==65536
        replacement=broker._Quota(channel.home.parent.parent/'turn.model-budget.json','turn',deployment.limits.output_bytes)
        try:assert replacement.value['requests']==1
        finally:replacement.close()
    finally:
        release.set();client.close();listener.close();server.join(6);channel.close()
