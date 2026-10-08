"""Import fidelity and native Dataset reuse, including batch publication."""
from dataclasses import replace
import sqlite3
import pytest
from demiflow import data
from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.documents import read_document
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient

URL='https://en.wikipedia.org/wiki/Example'
LIMITS={'max_bytes':100000,'max_document_bytes':100000}

def material(text='A [[bird|visible bird]] {{convert|10|cm}}.',url=URL):
    return {'format':'wikitext_sections','source':{'url':url,'final_url':url,'title':'Example','retrieved_at':''},
            'sections':[{'title':'Appearance','text':text,'level':2}], 'source_id':'wiki:1','revision':'3'}

@pytest.fixture
def library(tmp_path):
    return DocumentLibrary(index_path=tmp_path/'index.sqlite',object_directory=tmp_path/'objects')

def collect(graph):
    rows=[]
    stats=graph.map(lambda r:rows.append(r) or r).run_stream()
    return rows,stats

def test_fidelity_unknown_time_and_raw_provenance(library):
    text='10<sup>2</sup> and H<sub>2</sub>O. [[bird|bird]]\n\n{{box|name=two\n\nparagraphs}} <math>x^2</math>'
    receipt=library.register(material(text),**LIMITS)
    doc=read_document(receipt['document_ref'])
    body='\n'.join(b['text'] for b in doc['blocks'])
    assert '10<sup>2</sup>' in body and 'H<sub>2</sub>O' in body
    assert '{{box|name=two\n\nparagraphs}}' in body and '<math>x^2</math>' in body
    assert '[[bird' not in body and 'bird' in body
    assert doc['parser']['template_expansion'].startswith('not_performed')
    assert receipt['retrieved_at']==''
    assert library.lookup(URL,**LIMITS)['document_ref']==receipt['document_ref']
    assert replace(library,max_age_s=999999999).lookup(URL,**LIMITS) is None

def test_batch_native_registration_and_fetch_no_http(library,tmp_path,monkeypatch):
    from demiflow.data.plan import RegisterDocumentsBatchOp
    rows=[{'request':material()}, {'request':{'format':'redirect','url':URL+'Alias','target_url':URL}},
          {'request':{'format':'redirect','url':URL+'Alias2','target_url':URL+'Alias'}},
          {'request':material('')}, {'request':None,'receipt':{'status':'skipped'}}]
    graph=data.from_items(rows).register_documents(request='request',output='receipt',library=library,
        batch_size=2,prepare_workers=2,concurrency=2,queue_depth=4,when=lambda r:r['request'] is not None)
    assert isinstance(graph._plan.operations[-1],RegisterDocumentsBatchOp)
    got,_=collect(graph)
    assert sorted(r['receipt']['status'] for r in got)==['invalid_document','ok','redirect_registered','redirect_registered','skipped']
    assert library.lookup(URL+'Alias2',**LIMITS)['final_url']==URL
    async def no_http(*args,**kwargs):raise AssertionError('local import made HTTP request')
    monkeypatch.setattr(WebClient,'_get',no_http)
    session=WebSession(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'unused',
                       document_library=library,search_url='https://search.example')
    got,stats=collect(data.from_items([{'requests':[{'request_id':'q','urls':[URL+'Alias2'],'bindings':['fact']}]}])
        .fetch_documents(requests='requests',output='out',session=session))
    assert got[0]['out']['documents'][0]['acquisition']['kind']=='shared_library'
    assert stats.metrics['resources']['WebSession:0']['fetch_attempts']==0
    db=sqlite3.connect(library.index_path)
    assert db.execute('SELECT count(*) FROM documents').fetchone()[0]==1
    db.close()

def test_redirect_missing_cycles_conflicts_and_bad_shape(library):
    library.register({'format':'redirect','url':URL,'target_url':URL+'1'},**LIMITS)
    assert library.lookup(URL,**LIMITS) is None
    library.register({'format':'redirect','url':URL+'1','target_url':URL},**LIMITS)
    assert library.lookup(URL,**LIMITS) is None
    conflict=library.register({'format':'redirect','url':URL,'target_url':URL+'2'},**LIMITS)
    assert conflict['status']=='invalid_document'
    rows,_=collect(data.from_items([{'request':{'format':'redirect'}}])
        .register_documents(request='request',output='out',library=library,batch_size=2))
    assert rows[0]['out']['status']=='invalid_document'


def test_concurrent_batches_publish_serially_and_actor_can_run_again(library,monkeypatch):
    import asyncio
    import threading
    import time
    from demiflow.collect.document_library import BatchRegisterDocuments
    original=DocumentLibrary.publish;original_db=DocumentLibrary._db
    active=0;peak=0;opens=0
    guard=threading.Lock()
    def opened(self):
        nonlocal opens
        opens+=1
        return original_db(self)
    def observed(self,prepared,**kwargs):
        nonlocal active,peak
        with guard:
            active+=1;peak=max(peak,active)
        try:
            time.sleep(.02)
            return original(self,prepared,**kwargs)
        finally:
            with guard:active-=1
    monkeypatch.setattr(DocumentLibrary,'publish',observed)
    monkeypatch.setattr(DocumentLibrary,'_db',opened)
    actor=BatchRegisterDocuments('request','receipt',library,None,10000,10000,prepare_workers=2)
    batches=[[{'request':{'format':'redirect','url':URL+str(i),'target_url':URL}}] for i in range(8)]
    async def run():
        try:
            return await asyncio.gather(*(actor(rows) for rows in batches))
        finally:await actor.aclose()
    for _ in range(2):
        result=asyncio.run(run())
        assert all(rows[0]['receipt']['status']=='redirect_registered' for rows in result)
    assert peak==1 and active==0 and opens==2
    with sqlite3.connect(library.index_path) as db:
        assert db.execute('SELECT count(*) FROM redirects').fetchone()[0]==8


def test_cancel_waits_for_index_commit_before_closing_publisher(library,monkeypatch):
    import asyncio
    import threading
    from demiflow.collect.document_library import BatchRegisterDocuments
    started=threading.Event();release=threading.Event();committed=threading.Event()
    original=DocumentLibrary.publish
    def delayed(self,prepared,**kwargs):
        started.set()
        assert release.wait(5)
        result=original(self,prepared,**kwargs)
        committed.set()
        return result
    monkeypatch.setattr(DocumentLibrary,'publish',delayed)
    actor=BatchRegisterDocuments('request','receipt',library,None,10000,10000,prepare_workers=1)
    async def run():
        task=asyncio.create_task(actor([{'request':{'format':'redirect','url':URL+'Cancel','target_url':URL}}]))
        assert await asyncio.to_thread(started.wait,5)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):await task
        assert committed.is_set()
        await actor.aclose()
        assert actor.publisher_pool is None and actor.index_connection is None
    asyncio.run(run())
    with sqlite3.connect(library.index_path) as db:
        assert db.execute('SELECT target_url FROM redirects WHERE url=?',(URL+'Cancel',)).fetchone()[0]==URL


def test_batch_pause_releases_sqlite_for_another_writer(library,monkeypatch):
    import asyncio
    import time
    from demiflow.collect.document_library import BatchRegisterDocuments
    original=DocumentLibrary.publish;committed=[]
    def observed(self,prepared,**kwargs):
        receipt=original(self,prepared,**kwargs)
        committed.append(time.monotonic())
        return receipt
    monkeypatch.setattr(DocumentLibrary,'publish',observed)
    actor=BatchRegisterDocuments('request','receipt',library,None,10000,10000,publish_pause_s=.15)
    async def run():
        tasks=[asyncio.create_task(actor([{'request':{'format':'redirect','url':URL+str(i),'target_url':URL}}])) for i in range(2)]
        try:
            while not committed:await asyncio.sleep(.005)
            with sqlite3.connect(library.index_path,timeout=.05) as db:
                db.execute('BEGIN IMMEDIATE')
                db.rollback()
            assert len(committed)==1
            result=await asyncio.gather(*tasks)
            assert all(r[0]['receipt']['status']=='redirect_registered' for r in result)
            assert committed[1]-committed[0]>=.14
        finally:await actor.aclose()
    asyncio.run(run())
