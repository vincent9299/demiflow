"""Native image contracts without a request-result journal or model service."""
import base64
import errno
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow import data
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.objects import ObjectRef
from demiflow.operator_llm.parser import parse_prompt_pack
from image_http_fixture import fixture_http,definition,picture,SlowConsumer
from test_managed_image_execution import native_dataset,run_limits


def settings(root):
    return {'object_store':str(root),'image_encoding':'preserve','io_workers':1,'prepare_workers':1,'response_workers':1,
        'limits':{'max_images':3,'max_image_bytes':4096,'max_text_bytes':2048,'max_pixels':64,
            'max_total_pixels':192,'max_request_bytes':8192,'max_response_bytes':8192}}


def node(root,server,rows,**changes):
    arguments=dict(config=parse_prompt_pack(json.dumps(definition(server['base']))),
        inputs={'instruction':'instruction','refs':'refs'},output='image',call_output='call',error_output='error',
        max_requests=2,options=settings(root))
    return data.from_items(rows).map_prompt_async('generate',**(arguments|changes))


def test_output_binding_and_when_preserve_native_rows(tmp_path,fixture_http):
    rows=[{'id':0,'instruction':'skip','refs':[]},{'id':1,'instruction':'draw {{ literal }}','refs':[]}]
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        result=node(tmp_path/'assets',fixture_http,rows,output=None,outputs={'image':'generated'},
            when=lambda value:value['id']==1).take(2)
    by_id={r['id']:r for r in result}
    assert by_id[0]==rows[0]
    assert ObjectRef(**by_id[1]['generated']).read(max_bytes=4096)==fixture_http['raw']
    assert len(fixture_http['requests'])==1


def test_three_interleaved_references_preserve_order_and_text(tmp_path,fixture_http):
    references=[picture(color=c) for c in ('red','green','blue')]
    template=''
    values={'instruction':'draw {{ literal }}'}
    for index,raw in enumerate(references):
        template+='Image '+str(index)+'{{ image'+str(index)+' | image }}{{ caption'+str(index)+' }}'
        values['image'+str(index)]=[raw];values['caption'+str(index)]='Caption '+str(index)
    template+='{% text_segment %}Generate {{ instruction }}'
    config=parse_prompt_pack(json.dumps(definition(fixture_http['base'],'chat_interleaved',template)))
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        row,=data.from_items([values]).map_prompt_async('generate',config=config,inputs=list(values),
            output='image',call_output='call',error_output='error',max_requests=1,
            options=settings(tmp_path/'assets')).take(1)
    assert row['error'] is None
    content=fixture_http['requests'][0]['body']['messages'][0]['content']
    assert [base64.b64decode(p['image_url']['url'].split(',')[1]) for p in content if p['type']=='image_url']==references
    assert content[-1]=={'type':'text','text':'Generate '+values['instruction']}
    for index,pos in enumerate([i for i,p in enumerate(content) if p['type']=='image_url']):
        assert content[pos+1]['text'].startswith(values['caption'+str(index)])


@pytest.mark.parametrize('change',[{'concurrency':9},{'concurrency':True},{'queue_depth':9},
    {'options':{'unknown':True}},{'options':{'sqlite_journal':{'path':'forbidden.sqlite'}}}])
def test_invalid_declaration_stops_before_http_or_object_write(tmp_path,fixture_http,change):
    with pytest.raises((TypeError,ValueError)):
        node(tmp_path/'assets',fixture_http,[],**change)
    assert not fixture_http['requests'] and not (tmp_path/'assets').exists()


def test_actual_object_admission_happens_before_http(tmp_path,fixture_http):
    with pytest.raises(RecoveryContractError,match='output_admission_bytes'):
        with RunContext.start(tmp_path/'run',limits=run_limits()|{'max_output_bytes':2048}):
            native_dataset(tmp_path/'assets',fixture_http,1).take(1)
    assert not fixture_http['requests'] and not (tmp_path/'assets').exists()


def test_url_download_has_no_model_credential(tmp_path,fixture_http,monkeypatch):
    monkeypatch.setenv('IMAGE_FIXTURE_KEY','fixture-only')
    fixture_http['mode']='url'
    config=definition(fixture_http['base'])
    config['prompts']['generate']['model']['api_key_env']='IMAGE_FIXTURE_KEY'
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        row,=node(tmp_path/'assets',fixture_http,[{'instruction':'paint','refs':[]}],
            config=parse_prompt_pack(json.dumps(config))).take(1)
    assert row['error'] is None and ObjectRef(**row['image']).read(max_bytes=4096)==fixture_http['raw']
    post,get=fixture_http['requests']
    assert post['method']=='POST' and post['authorization']=='Bearer fixture-only'
    assert get['method']=='GET' and get['authorization'] is None


def test_enospc_cleans_unprepared_object_and_returns_no_reference(tmp_path,fixture_http,monkeypatch):
    import demiflow.objects as objects
    original=objects.os.link
    def no_space(source,target,*args,**kwargs):
        if '.demiflow-objects' in str(target):raise OSError(errno.ENOSPC,'controlled object disk full')
        return original(source,target,*args,**kwargs)
    monkeypatch.setattr(objects.os,'link',no_space)
    logical=tmp_path/'assets'
    with pytest.raises(OSError,match='controlled object disk full'):
        with RunContext.start(tmp_path/'run',limits=run_limits()):
            native_dataset(logical,fixture_http,1).take(1)
    assert len(fixture_http['requests'])==1
    assert not [p for p in logical.rglob('*') if p.is_file() and (len(p.name)==64 or p.name.startswith('.pending'))]


def test_slow_consumer_applies_finite_queue_backpressure(tmp_path,fixture_http):
    entered,release=threading.Event(),threading.Event()
    def execute():
        with RunContext.start(tmp_path/'run',limits=run_limits()):
            return native_dataset(tmp_path/'assets',fixture_http,16).map_async(SlowConsumer(entered,release),
                concurrency=1,queue_depth=1).take(16)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(execute)
        try:
            assert entered.wait(5)
            time.sleep(.15)
            assert 1<=len(fixture_http['requests'])<=6
        finally:release.set()
        rows=future.result(timeout=50)
    assert len(rows)==len(fixture_http['requests'])==16
