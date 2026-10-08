"""Public Dataset contracts and regressions for the P1/P2 platform boundary."""
import asyncio
import json
from pathlib import Path
import httpx
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.collect.reading import select_blocks
from demiflow.collect.documents import canonical, parse_document


class FakeWeb:
    def __init__(self, reverse=False): self.calls=[]; self.closed=0; self.reverse=reverse
    async def search(self, query):
        self.calls.append(query)
        return {'status':'ok','reason':'','candidates':[{'url':'https://example.org/a','title':'A','snippet':''}],'attempts':[]}
    async def fetch(self, url):
        self.calls.append(url)
        await asyncio.sleep(.005 if url.endswith('/a') != self.reverse else 0)
        return {'url':url,'status':'ok','reason':'','document_ref':{'uri':'file:///'+url[-1], 'sha256':url[-1]*64},'attempts':[]}
    async def aclose(self): self.closed+=1
    def snapshot_metrics(self): return {'calls':len(self.calls)}


def collect(stream):
    rows=[]
    stats=stream.map(lambda row: rows.append(row) or row).run_stream()
    return rows,stats


def test_native_plan_is_lazy_and_resources_are_owned(tmp_path):
    from demiflow.data.plan import SearchWebOp, FetchDocumentsOp, SaveLanceOp
    web=FakeWeb(); uri=tmp_path/'output.lance'
    stream=(data.from_items([{'id':'a','requests':[{'request_id':'r','query':'seed','bindings':['x']}]}])
        .search_web(requests='requests',output='searched',session=web)
        .map(lambda row:{**row,'urls':[{'request_id':'r','bindings':['x'],'urls':['https://example.org/a']}]})
        .fetch_documents(requests='urls',output='fetched',session=web)
        .save_lance(uri,schema=pa.schema([('id',pa.string())]),key='id',stage='out',flush_interval=.01))
    assert not uri.exists() and not web.calls
    assert any(isinstance(op,SearchWebOp) for op in stream._plan.operations)
    assert any(isinstance(op,FetchDocumentsOp) for op in stream._plan.operations)
    assert isinstance(stream._plan.operations[-1],SaveLanceOp)
    rows,stats=collect(stream)
    assert len(rows)==1 and web.closed==1
    assert stats.outputs['out']['version']>=1
    assert stats.metrics['resources']['FakeWeb:0']['calls']==2


def test_fetch_caps_and_existing_document_binding_merge():
    web=FakeWeb()
    requests=[{'request_id':str(i),'bindings':['b'+str(i)],'urls':[f'https://example.org/{i}/{j}' for j in range(5)]} for i in range(2)]
    out,_=collect(data.from_items([{'requests':requests}]).fetch_documents(
        requests='requests',output='out',session=web,per_request=None,max_attempts=5,max_new_documents=2))
    assert len(web.calls)==2 and len(out[0]['out']['documents'])==2
    known={'url':'https://example.org/a','status':'ok','reason':'','document_ref':{'uri':'file:///a','sha256':'a'*64},
           'request_ids':['old'],'bindings':['b1']}
    web=FakeWeb()
    out,_=collect(data.from_items([{'known':[known],'requests':[{'request_id':'new','bindings':['b2'],'urls':[known['url']]}]}])
        .fetch_documents(requests='requests',output='out',known='known',session=web,max_attempts=0,max_new_documents=0))
    document=out[0]['out']['documents'][0]
    assert document['request_ids']==['old','new'] and document['bindings']==['b1','b2']
    assert not web.calls and out[0]['out']['touched_urls']==[known['url']]


def test_fetch_order_is_independent_of_completion():
    requests=[{'request_id':'q1','bindings':['c1'],'urls':['https://example.org/a','https://example.org/b']},
              {'request_id':'q2','bindings':['c2'],'urls':['https://example.org/b','https://example.org/a']}]
    outputs=[]
    for reverse in (False,True):
        rows,_=collect(data.from_items([{'requests':requests}]).fetch_documents(requests='requests',output='out',session=FakeWeb(reverse)))
        outputs.append(rows[0]['out']['documents'])
    assert outputs[0]==outputs[1]
    assert all(d['bindings']==['c1','c2'] and d['request_ids']==['q1','q2'] for d in outputs[0])


class Counter:
    def text(self,value): return len(value)


def document(text, index):
    ref={'uri':f'file:///{index}','sha256':str(index)*64}
    return ({'document_ref':ref,'url':'https://example.org/'+str(index),'eligible':True,'bindings':['a','b']},
            {'source':{'url':'https://example.org/'+str(index),'title':''},'blocks':[
                {'block_id':'b000000','position':0,'kind':'paragraph','headings':[],'text':text}]})


def test_global_read_priority_and_binding_union():
    docs=[document('Windows folders files explorer windows folders files explorer',1),
          document('Cerambycidae longhorn beetle antennae',2)]
    spec={'questions':[{'id':'a','text':'Cerambycidae longhorn beetle'},{'id':'b','text':'Cerambycidae antennae'}],
          'retained':[],'requests':[],'new_tokens':10000,'total_tokens':10000}
    full=select_blocks(spec,docs,Counter())
    assert full['materials'][0]['text'].startswith('Cerambycidae')
    assert full['selected'][0]['bindings']==['a','b']
    budget=len(canonical([full['materials'][0]]))
    out=select_blocks({**spec,'new_tokens':budget,'total_tokens':budget},docs,Counter())
    assert len(out['materials'])==1 and out['materials'][0]['text'].startswith('Cerambycidae')


def test_empty_snapshot_and_failure_cleanup(tmp_path):
    schema=pa.schema([('id',pa.string())]); uri=tmp_path/'empty.lance'
    stats=data.from_items([]).save_lance(uri,schema=schema,key='id',stage='empty').run_stream()
    assert stats.outputs['empty']['version']>=1
    web=FakeWeb(); seen=[]
    def fail(row): raise ValueError('failure after committed stage')
    stream=(data.from_items([{'id':'a','requests':[]}]).search_web(requests='requests',output='out',session=web)
        .save_lance(uri,schema=schema,key='id',stage='saved',flush_interval=.01).map(fail))
    with pytest.raises(ValueError,match='failure after'):
        stream.run_stream(on_drain=lambda stats:seen.append(stats.outputs))
    assert web.closed==1 and seen[0]['saved']['version']>0
    data.from_items([]).save_lance(uri,schema=schema,key='id',stage='retry').run_stream()


def test_duplicate_targets_rejected_before_mutation(tmp_path):
    schema=pa.schema([('id',pa.string())]);uri=tmp_path/'out.lance'
    stream=data.from_items([]).save_lance(uri,schema=schema,key='id',stage='a').save_lance(uri,schema=schema,key='id',stage='b')
    with pytest.raises(ValueError,match='Duplicate'): stream.run_stream()
    assert not uri.exists()


def test_search_adapter_handles_infobox_and_malformed():
    from demiflow.collect.searxng import normalize_response
    from demiflow.collect.web import normalized_url
    result=normalize_response({'results':[None], 'infoboxes':[{'infobox':'Ginkgo','engine':'wikipedia',
        'urls':[{'url':'https://en.wikipedia.org/wiki/Ginkgo','title':'Wikipedia'}]}]},normalized_url)
    assert result['status']=='ok' and result['candidates'][0]['result_kind']=='infobox_link'
    assert 'malformed' in result['reason']
    result=normalize_response({'results':[None]},normalized_url)
    assert result['status']=='invalid_search_response'


def test_profile_change_separates_search_cache(tmp_path):
    from demiflow.collect.web import WebClient
    calls=[]
    async def run(profile):
        web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',
                      search_url='https://search.example/search',search_engines=['wikipedia'],search_profile=profile)
        async def get(url,**kwargs):
            calls.append(kwargs['params'])
            return {'status':'ok','body':json.dumps({'results':[{'url':'https://example.org/'+profile}]}).encode(),'attempts':[]}
        web._get=get
        try: return await web.search('same')
        finally: await web.aclose()
    a=asyncio.run(run('a'));b=asyncio.run(run('b'));c=asyncio.run(run('b'))
    assert len(calls)==2 and a['candidates']!=b['candidates'] and b==c
    assert calls[0]['engines']=='wikipedia' and 'categories' not in calls[0]


def test_parser_upgrade_reuses_raw_download(tmp_path,monkeypatch):
    import demiflow.collect.web as module
    calls=[]
    async def run():
        web=module.WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://s.example')
        async def get(url,**kwargs):
            calls.append(url)
            return {'status':'ok','body':b'<main><p>Public text</p></main>','final_url':url,
                    'headers':{'content-type':'text/html'},'attempts':[]}
        web._get=get
        try: return await web.fetch('https://example.org')
        finally: await web.aclose()
    monkeypatch.setattr(module,'run_isolated',lambda function,*args,timeout_s=None,**kwargs:function(*args,**kwargs))
    first=asyncio.run(run());monkeypatch.setattr(module,'PARSER_VERSION','test-next-parser');second=asyncio.run(run())
    assert len(calls)==1 and first['raw_ref']==second['raw_ref'] and second['status']=='ok'


def test_public_main_survives_sidebar_login_and_table_rows():
    body=b'<aside><form><input type="password"></form></aside><main><p>Public body</p><table><tr><th>Name</th><th>Size</th></tr><tr><td>A</td><td>10</td></tr></table></main>'
    doc=parse_document(body,url='https://example.org',final_url='https://example.org',content_type='text/html')
    assert [b['text'] for b in doc['blocks']]==['Public body','Name | Size\nA | 10']


def test_article_layout_is_not_a_sidebar_and_metadata_is_not_body():
    from demiflow.collect.documents import DocumentError
    html = b'''<html><head><title>Metadata title</title></head><body class="ast-no-sidebar l-sidebar-right">
      <article><p>Small navigation card</p></article>
      <article class="has-sidebar sidebar-right"><div class="entry-content">
      <h2>Article section</h2><p>Substantive original body and its qualifying conditions.</p>
      <div class="sidebar"><p>Widget noise</p></div></div></article></body></html>'''
    doc = parse_document(html,url='https://example.org',final_url='https://example.org',content_type='text/html')
    assert doc['source']['title']=='Metadata title'
    assert [b['text'] for b in doc['blocks']]==['Article section','Substantive original body and its qualifying conditions.']
    with pytest.raises(DocumentError,match='empty_body'):
        parse_document(b'<html><head><title>Only a title</title></head><body></body></html>',
                       url='https://example.org',final_url='https://example.org',content_type='text/html')


def test_multilingual_documents_get_reading_coverage_and_retained_blocks_survive():
    a=document('母狮鬃毛通常情况',1); b=document('Exceptions to typical lion manes.',2)
    a[1]['blocks']=[{**a[1]['blocks'][0], 'position':i,'block_id':f'b{i:06d}'} for i in range(30)]
    spec={'questions':[{'id':'a','text':'母狮鬃毛通常情况'}], 'retained':[], 'requests':[],
          'new_tokens':2400,'total_tokens':2400}
    out=select_blocks(spec,[a,b],Counter())
    assert {m['document_ref']['sha256'] for m in out['materials']}=={'1'*64,'2'*64}
    second=select_blocks({**spec,'retained':out['selected'],'new_tokens':0},[a,b],Counter())
    assert second['materials']==out['materials']


def test_focused_reading_prioritizes_section_prose_without_clipping_or_headings():
    record, doc = document('Intro history of an object.', 1)
    texts = [('paragraph', [], 'Design history and dates.'),
             ('heading', ['Appearance'], 'Appearance'),
             ('paragraph', ['Appearance'], 'Two blue panels surround a circular opening; red panels are an allowed variant.'),
             ('paragraph', [], '{{navigation}}')]
    doc['blocks'] = [{'block_id': f'b{i:06d}', 'position': i, 'kind': kind,
                     'headings': headings, 'text': text} for i, (kind, headings, text) in enumerate(texts)]
    spec = {'questions': [{'id': 'a', 'text': 'design appearance'}], 'new_tokens': 10000, 'total_tokens': 10000,
            'selection': {'heading_weight': 3, 'include_neighbors': False,
                          'excluded_kinds': ['heading'], 'min_matches': 1, 'fallback_blocks': 1}}
    out = select_blocks(spec, [(record, doc)], Counter())
    assert out['materials'][0]['text'] == texts[2][2]
    assert {m['block_id'] for m in out['materials']} == {'b000000', 'b000002'}
    first_budget = len(canonical([out['materials'][0]]))
    limited = select_blocks({**spec, 'new_tokens': first_budget, 'total_tokens': first_budget}, [(record, doc)], Counter())
    assert limited['materials'] == out['materials'][:1]
    # Explicit locators remain readable even when automatic selection excludes headings.
    requested = {**spec, 'requests': [{'request_id': 'loc', 'document_ref': record['document_ref'],
                 'block_ids': ['b000001'], 'bindings': ['a']}]}
    explicit = select_blocks(requested, [(record, doc)], Counter())
    assert explicit['materials'][0]['block_id'] == 'b000001'
    retained = select_blocks({**spec, 'retained': explicit['selected'], 'new_tokens': 0}, [(record, doc)], Counter())
    assert retained['materials'] == explicit['materials']


def test_focused_reading_fallback_and_invalid_policy():
    pair = document('A short article without matching headings.', 1)
    spec = {'questions': [{'id': 'a', 'text': 'morphology'}], 'new_tokens': 10000, 'total_tokens': 10000,
            'selection': {'min_matches': 1, 'fallback_blocks': 1, 'include_neighbors': False}}
    assert select_blocks(spec, [pair], Counter())['materials'][0]['text'] == pair[1]['blocks'][0]['text']
    assert not select_blocks({**spec, 'selection': {'min_matches': 1}}, [pair], Counter())['materials']
    for invalid in ({'heading_weight': 0}, {'fallback_blocks': 100}, {'unknown': True}):
        with pytest.raises(ValueError):
            select_blocks({**spec, 'selection': invalid}, [pair], Counter())


def test_platform_boundary_has_no_business_imports():
    import ast
    root=Path(__file__).parents[1]/'demiflow'
    for path in [* (root/'collect').glob('*.py'),* (root/'services').rglob('*.py'),root/'execution/stream_resources.py']:
        tree=ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node,ast.ImportFrom):
                assert not (node.module or '').startswith(('preparation.','benchmark.','demiwtg.')),path


@pytest.mark.parametrize('stop_when_idle',[False,True])
def test_shared_service_leases_configuration_and_capacity(tmp_path,stop_when_idle):
    import socket
    import sys
    from demiflow.services.shared_http import SharedHTTPService
    from demiflow.services.manage import stop_service,status_service
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    configuration={'command':[sys.executable,'-m','http.server',str(port),'--bind','127.0.0.1'],
        'base_url':f'http://127.0.0.1:{port}', 'ready_path':'/', 'startup_timeout_s':5, 'shutdown_timeout_s':1}
    spec=SharedHTTPService(root=tmp_path,name='test',configuration=configuration,request_concurrency=1,stop_when_idle=stop_when_idle)
    async def run():
        a,b=spec.bind(),spec.bind()
        try:
            await asyncio.gather(a.ensure_ready(),b.ensure_ready())
            with pytest.raises(RuntimeError,match='in use'): stop_service(tmp_path,'test')
            active=0;peak=0
            async def request(owner):
                nonlocal active,peak
                async with owner.request_slot():
                    active+=1;peak=max(peak,active)
                    await asyncio.sleep(.02)
                    active-=1
            await asyncio.gather(request(a),request(b))
            assert peak==1
            mismatch=SharedHTTPService(root=tmp_path,name='test',configuration={**configuration,'env':{'PROFILE':'different'}},request_concurrency=1).bind()
            try:
                with pytest.raises(RuntimeError,match='differs'): await mismatch.ensure_ready()
            finally: await mismatch.aclose()
            capacity_mismatch=SharedHTTPService(root=tmp_path,name='test',configuration=configuration,request_concurrency=2).bind()
            try:
                with pytest.raises(ValueError,match='concurrency differs'): await capacity_mismatch.ensure_ready()
            finally: await capacity_mismatch.aclose()
            await a.aclose()
            with pytest.raises(RuntimeError,match='in use'): stop_service(tmp_path,'test')
            assert status_service(tmp_path,'test')['alive']
        finally:
            await a.aclose();await b.aclose()
        assert status_service(tmp_path,'test')['alive'] is not stop_when_idle
    try: asyncio.run(run())
    finally: stop_service(tmp_path,'test',timeout_s=5)
    assert not status_service(tmp_path,'test')['alive']


def test_error_categories_hide_internal_classes():
    from demiflow.operator_llm.errors import error_category
    assert error_category({'type':'PromptResponsePending'})=='pending_response'
    assert error_category({'type':'PromptResponseParseError','call':{'http_status':408}})=='provider_error'
    assert error_category({'type':'InputTokenBudgetExceeded'})=='input_budget'


def test_retained_material_cannot_silently_exceed_total_budget():
    pair=document('Text',1)
    spec={'questions':[{'id':'a','text':'Text'}],'retained':[],'requests':[],'new_tokens':2000,'total_tokens':2000}
    first=select_blocks(spec,[pair],Counter())
    result=select_blocks({**spec,'retained':first['selected'],'total_tokens':1},[pair],Counter())
    assert result['status']=='context_budget' and result['selected']==first['selected']
