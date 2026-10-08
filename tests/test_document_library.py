"""Shared source lookup and run-journal boundaries through native Dataset APIs."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys
import threading
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.collect.contracts import DOCUMENT_RESULT
from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.documents import DocumentError, read_document, store_document
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient


URL = 'https://source.example/page'


@pytest.fixture
def library(tmp_path):
    return DocumentLibrary(index_path=tmp_path/'public/index.sqlite', object_directory=tmp_path/'public/objects')


@pytest.fixture
def fast_parser(monkeypatch):
    monkeypatch.setattr('demiflow.collect.web.run_isolated',
        lambda fn,*args,timeout_s=None,**kwargs:fn(*args,**kwargs))


class Body(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'<main><h1>Species</h1><p>A preserved source paragraph.</p></main>'


def client(tmp_path, library, run, calls, status=200):
    web=WebClient(cache_path=tmp_path/(run+'.sqlite'), object_directory=tmp_path/'unused',
                  document_library=library, search_url='https://search.example', host_interval_s=0, retries=0)
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(status,stream=Body(),headers={'content-type':'text/html'})
    web.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
    return web


def existing(tmp_path, text=b'Original content', date='2026-01-01T00:00:00+00:00'):
    return store_document(tmp_path/'original', text, url=URL, final_url=URL,
                          content_type='text/plain', retrieved_at=date)


def capture(stream):
    rows=[]
    stats=stream.map(lambda r:rows.append(r) or r).run_stream()
    return rows,stats


def test_lazy_declaration_and_strict_configuration(tmp_path, library):
    web=WebSession(cache_path=tmp_path/'run.sqlite', object_directory=tmp_path/'fallback',
                   search_url='https://search.example', document_library=library)
    stream=data.from_items([]).register_documents(request='request',output='out',library=library)
    assert not Path(library.index_path).parent.exists()
    from demiflow.data.plan import RegisterDocumentsOp
    assert isinstance(stream._plan.operations[-1], RegisterDocumentsOp)
    with pytest.raises(ValueError,match='separate'):
        WebSession(**{**web.options,'cache_path':library.index_path})
    for kw in ({'policy':'guess'},{'max_age_s':0},{'lock_timeout_s':float('inf')},{'parser_versions':[]}):
        with pytest.raises(ValueError):replace(library,**kw)


def test_cross_run_reuse_journal_freeze_and_refresh(tmp_path,library,fast_parser):
    calls=[]
    async def run(name, lib):
        web=client(tmp_path,lib,name,calls)
        try:
            result=await web.fetch(URL)
            return result,web.snapshot_metrics()
        finally:await web.aclose()
    first,a=asyncio.run(run('a',library))
    second,b=asyncio.run(run('b',library))
    assert first['document_ref']==second['document_ref'] and len(calls)==1
    assert second['attempts']==[] and second['acquisition']['kind']=='shared_library'
    assert a['fetch_attempts']==1 and b['fetch_attempts']==0 and b['library_hits']==1
    refreshed,c=asyncio.run(run('c',replace(library,policy='refresh')))
    assert len(calls)==2 and refreshed['document_ref']!=first['document_ref']
    repeated,d=asyncio.run(run('b',library))
    assert repeated==second and d['reused']==1 and d['library_hits']==0 and len(calls)==2


def test_enabling_library_can_register_a_run_saved_raw_download(tmp_path,library,fast_parser):
    calls=[]
    async def run(lib):
        web=client(tmp_path,lib,'existing-run',calls)
        try:return await web.fetch(URL)
        finally:await web.aclose()
    before=asyncio.run(run(None))
    after=asyncio.run(run(library))
    assert len(calls)==1 and after['status']=='ok'
    assert before['raw_ref']['sha256']==after['raw_ref']['sha256']
    assert after['raw_ref']['uri'].startswith(Path(library.object_directory).as_uri())
    assert after['document_ref']==library.lookup(URL,max_bytes=10000,max_document_bytes=10000)['document_ref']


@pytest.mark.parametrize('use_library', [False, True])
def test_invalid_parsed_page_is_a_receipt_and_other_pages_continue(tmp_path,library,fast_parser,use_library):
    calls=[]
    async def run():
        web=client(tmp_path,library if use_library else None,'malformed',calls)
        async def get(url):
            calls.append(url)
            # Empty table rows produce a whitespace-only block in parser v3.
            body=(b'<main><p>Article</p><table><tr></tr><tr></tr></table></main>'
                  if url.endswith('/bad') else b'<main><p>Valid article</p></main>')
            return {'status':'ok','body':body,'final_url':url,
                    'headers':{'content-type':'text/html'},
                    'attempts':[{'attempt':1,'status':'ok','http_status':200,'elapsed_s':0.1}]}
        web._get=get
        try:
            bad=await web.fetch(URL+'/bad')
            good=await web.fetch(URL+'/good')
            repeated=await web.fetch(URL+'/bad')
            return bad,good,repeated
        finally:await web.aclose()
    bad,good,repeated=asyncio.run(run())
    assert bad['status']=='parse_error' and bad['reason']=='invalid_document_blocks'
    assert bad['document_ref'] is None and bad['raw_ref']
    assert bad['attempts'][0]['http_status']==200
    assert repeated==bad and len(calls)==2
    assert good['status']=='ok' and read_document(good['document_ref'])['blocks'][0]['text']=='Valid article'
    if use_library:
        assert library.lookup(URL+'/bad',max_bytes=10000,max_document_bytes=10000) is None
        assert library.lookup(URL+'/good',max_bytes=10000,max_document_bytes=10000)['status']=='ok'


def test_native_registration_copies_objects_and_fetch_uses_them(tmp_path,library,monkeypatch):
    ref=existing(tmp_path)
    alias='https://source.example/alternate'
    schema=pa.schema([('id',pa.string()),('registered',DOCUMENT_RESULT)])
    stream=(data.from_items([{'id':'a','request':{'document_ref':ref,'aliases':[alias],
                                              'source_id':'page:42','revision':'17'}}])
        .register_documents(request='request',output='registered',library=library)
        .save_lance(tmp_path/'registered.lance',schema=schema,key='id',stage='registered'))
    rows,stats=capture(stream)
    receipt=rows[0]['registered']
    assert receipt['document_ref']['uri'].startswith(Path(library.object_directory).as_uri())
    assert receipt['acquisition']['revision']=='17' and receipt['acquisition']['origin']=='import'
    assert receipt['retrieved_at']=='2026-01-01T00:00:00+00:00'
    doc=read_document(ref)
    Path(urlsplit(ref['uri']).path).unlink()
    Path(urlsplit(doc['raw_ref']['uri']).path).unlink()
    async def no_network(*args,**kwargs):raise AssertionError('Shared hit made a network request')
    monkeypatch.setattr(WebClient,'_get',no_network)
    web=WebSession(cache_path=tmp_path/'new-run.sqlite',object_directory=tmp_path/'fallback',
                   document_library=library,search_url='https://search.example')
    rows,stats=capture(data.from_items([{'requests':[{'request_id':'q','urls':[alias],'bindings':['fact']}]}])
        .fetch_documents(requests='requests',output='out',session=web))
    result=rows[0]['out']['documents'][0]
    assert result['document_ref']==receipt['document_ref'] and result['bindings']==['fact']
    assert result['acquisition']['kind']=='shared_library' and result['acquisition']['origin']=='import'
    assert read_document(result['document_ref'])['blocks'][0]['text']=='Original content'
    assert stats.metrics['resources']['WebSession:0']['fetch_attempts']==0


def test_failures_are_local_successes_shared_and_unknown_not_retried(tmp_path,library,fast_parser):
    calls=[]
    async def run(name,status):
        web=client(tmp_path,library,name,calls,status)
        try:return await web.fetch(URL)
        finally:await web.aclose()
    assert asyncio.run(run('failed',500))['status']=='http_error'
    assert asyncio.run(run('succeeded',200))['status']=='ok' and len(calls)==2
    # A later public success never overwrites this run's exhausted receipt.
    assert asyncio.run(run('failed',200))['status']=='http_error' and len(calls)==2
    db=sqlite3.connect(tmp_path/'failed.sqlite')
    with db:
        db.execute("UPDATE cache SET value=?",(json.dumps({'status':'interrupted','reason':'reserved','attempts':[]}),))
    db.close()
    assert asyncio.run(run('failed',200))['status']=='interrupted' and len(calls)==2


def test_old_import_freshness_exact_aliases_and_parser_identity(tmp_path,library):
    limits={'max_bytes':1000,'max_document_bytes':10000}
    newer=library.register({'document_ref':existing(tmp_path,b'New','2026-02-01T00:00:00Z')},**limits)
    library.register({'document_ref':existing(tmp_path,b'Old','2020-01-01T00:00:00Z')},**limits)
    assert library.lookup(URL+'#section',**limits)['document_ref']==newer['document_ref']
    assert library.lookup(URL+'?variant=1',**limits) is None
    assert library.lookup(URL.upper(),**limits) is None  # path case is meaningful
    assert replace(library,max_age_s=.001).lookup(URL,**limits) is None
    assert replace(library,parser_versions=('another-parser',)).lookup(URL,**limits) is None
    repeated=library.register({'document_ref':existing(tmp_path,b'New','2026-02-01T00:00:00Z')},**limits)
    assert repeated==newer


@pytest.mark.parametrize('target',['raw_ref','document_ref'])
def test_corrupt_public_object_stops_without_hidden_download(tmp_path,library,fast_parser,target):
    calls=[]
    registered=library.register({'document_ref':existing(tmp_path)},max_bytes=1000,max_document_bytes=10000)
    Path(urlsplit(registered[target]['uri']).path).write_bytes(b'broken')
    async def run():
        web=client(tmp_path,library,'broken',calls)
        try:
            with pytest.raises(DocumentError):await web.fetch(URL)
        finally:await web.aclose()
    asyncio.run(run())
    assert not calls


def test_import_failure_no_index_and_limits_are_not_silently_relaxed(tmp_path,library):
    ref=existing(tmp_path)
    rows,_=capture(data.from_items([{'request':{'document_ref':ref}}])
        .register_documents(request='request',output='out',library=library,max_bytes=1))
    assert rows[0]['out']['status']=='invalid_document'
    assert not Path(library.index_path).exists()
    receipt=library.register({'document_ref':ref},max_bytes=1000,max_document_bytes=10000)
    with pytest.raises(DocumentError,match='too_large'):
        library.lookup(URL,max_bytes=1,max_document_bytes=10000)
    with pytest.raises(ValueError,match='different object_directory'):
        replace(library,object_directory=tmp_path/'other').lookup(URL,max_bytes=1000,max_document_bytes=10000)


def test_lock_timeout_and_cancellation_release(library):
    async def run():
        async with library.claim(URL):
            with pytest.raises(TimeoutError,match='lock_timeout'):
                async with replace(library,lock_timeout_s=.02).claim(URL):pass
            async def wait():
                async with library.claim(URL):pass
            task=asyncio.create_task(wait())
            await asyncio.sleep(.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
        async with library.claim(URL):pass
    asyncio.run(run())


def test_lock_timeout_is_known_failure_and_does_not_send_http(tmp_path,library):
    calls=[]
    async def run():
        web=client(tmp_path,replace(library,lock_timeout_s=.02),'busy',calls)
        try:
            async with library.claim(URL):
                result=await web.fetch(URL)
            assert result['status']=='library_busy' and result['attempts']==[]
            assert await web.fetch(URL)==result  # Keep this run's known outcome.
        finally:await web.aclose()
    asyncio.run(run())
    assert not calls


def test_lookup_during_reserved_import_writer_is_read_only(tmp_path, library):
    """SQLite permits readers during a reserved write; lookup must not write."""
    ref = existing(tmp_path)
    saved = library.register({'document_ref': ref}, max_bytes=1000, max_document_bytes=10000)
    writer = sqlite3.connect(library.index_path)
    try:
        writer.execute('BEGIN IMMEDIATE')
        writer.execute('INSERT INTO redirects VALUES (?,?,?)', ('https://pending.example/', URL, '{}'))
        reader = replace(library, lock_timeout_s=.03)
        result = reader.lookup(URL, max_bytes=1000, max_document_bytes=10000)
        assert result['document_ref'] == saved['document_ref']
        assert result['acquisition']['kind'] == 'shared_library'
        assert reader.lookup('https://missing.example/', max_bytes=1000, max_document_bytes=10000) is None
    finally:
        writer.rollback()
        writer.close()


def test_index_publish_waits_for_writer_and_honors_timeout(tmp_path, library):
    import time
    library._db().close()
    ready = threading.Event()

    def hold_writer():
        with sqlite3.connect(library.index_path) as db:
            db.execute('BEGIN IMMEDIATE')
            ready.set()
            time.sleep(.2)

    writer = threading.Thread(target=hold_writer)
    writer.start()
    assert ready.wait(2)
    item = library.prepare({'document_ref': existing(tmp_path)}, max_bytes=1000, max_document_bytes=10000)
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            replace(library, lock_timeout_s=.02).publish([item])
        assert time.monotonic()-started < 1
        assert replace(library, lock_timeout_s=2).publish([item])[0]['status'] == 'ok'
    finally:
        writer.join(3)
    assert not writer.is_alive()


def test_redirect_final_url_is_reused(tmp_path,library,fast_parser):
    calls=[]
    final=URL+'/canonical'
    async def run():
        first=client(tmp_path,library,'redirect',calls)
        def handle(request):
            calls.append(str(request.url))
            if str(request.url)==URL:
                return httpx.Response(302,stream=Body(),headers={'location':final})
            return httpx.Response(200,stream=Body(),headers={'content-type':'text/html'})
        await first.client.aclose()
        first.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:a=await first.fetch(URL)
        finally:await first.aclose()
        second=client(tmp_path,library,'direct',calls)
        try:b=await second.fetch(final)
        finally:await second.aclose()
        assert a['document_ref']==b['document_ref'] and b['url']==b['final_url']==final
    asyncio.run(run())
    assert calls==[URL,final]


def test_two_processes_download_same_url_once(tmp_path,library):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            import time
            requests.append(self.path)
            time.sleep(.15)
            self.send_response(200);self.send_header('Content-Type','text/plain');self.end_headers()
            self.wfile.write(b'Cross-process shared document')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    program='''
import asyncio,json,sys
from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.web import WebClient
async def main():
    web=WebClient(cache_path=sys.argv[1],object_directory=sys.argv[3],search_url='http://unused',
        document_library=DocumentLibrary(index_path=sys.argv[2],object_directory=sys.argv[3]))
    try:
        result=await web.fetch(sys.argv[4])
        print(json.dumps({'status':result['status'],'ref':result['document_ref'],'metrics':web.snapshot_metrics()}))
    finally:await web.aclose()
asyncio.run(main())
'''
    async def run():
        async def child(i):
            proc=await asyncio.create_subprocess_exec(sys.executable,'-c',program,str(tmp_path/f'p{i}.sqlite'),
                library.index_path,library.object_directory,f'http://127.0.0.1:{server.server_port}/doc',
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            stdout,stderr=await asyncio.wait_for(proc.communicate(),timeout=30)
            assert proc.returncode==0,stderr.decode()
            return json.loads(stdout)
        return await asyncio.gather(child(1),child(2))
    try:results=asyncio.run(run())
    finally:server.shutdown();server.server_close();thread.join()
    assert len(requests)==1 and results[0]['ref']==results[1]['ref']
    assert sum(r['metrics']['fetch_attempts'] for r in results)==1
    assert sum(r['metrics']['library_hits'] for r in results)==1
