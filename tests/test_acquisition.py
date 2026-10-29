import asyncio
import httpx
import threading
import pytest
from demiflow.collect.acquisition import Download, ParseDocument
from demiflow.collect.documents import read_document
from demiflow.collect.session import WebSession
from demiflow.objects import LocalObjectStore
from demiflow.execution.request_limits import RequestGate


@pytest.mark.slow
def test_raw_download_then_offline_parse_use_independent_stores(tmp_path, monkeypatch):
    calls = []
    original = httpx.AsyncClient
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'<html><title>Example</title><p>Actual body</p></html>'
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, headers={'content-type':'text/html'}, stream=Body())
    monkeypatch.setattr(httpx, 'AsyncClient', lambda **kw: original(**{**kw,'transport':httpx.MockTransport(handler)}))
    web = WebSession(cache_path=tmp_path/'journal.sqlite', object_directory=tmp_path/'unused', retries=0)
    raw_store, body_store = LocalObjectStore(tmp_path/'raw'), LocalObjectStore(tmp_path/'body')
    async def run():
        try:
            raw = await Download(session=web, store=raw_store)({'url':'https://example.org/a'})
        finally:
            await web.aclose()
        assert raw['raw_ref'] and 'document_ref' not in raw
        request = {k:raw[k] for k in ('url','final_url','content_type','retrieved_at','raw_ref')}
        return await ParseDocument(store=body_store)(request), raw
    parsed, raw = asyncio.run(run())
    assert len(calls) == 1 and parsed['status'] == 'ok'
    assert str(tmp_path/'raw') in raw['raw_ref']['uri']
    assert str(tmp_path/'body') in parsed['document_ref']['uri']
    doc = read_document(parsed['document_ref'])
    assert doc['raw_ref'] == raw['raw_ref'] and doc['source']['title'] == 'Example'
    assert not (tmp_path/'unused').exists()


@pytest.mark.slow
def test_parse_errors_are_receipts_and_shared_admission_is_bounded(tmp_path):
    raw_store = LocalObjectStore(tmp_path/'raw')
    request = dict(url='https://example.org/a',final_url='https://example.org/a',
        content_type='text/html',retrieved_at='',raw_ref=raw_store.put(b'<p>' + b'x'*200 + b'</p>').to_dict())
    gate = RequestGate(1)
    async def run():
        actors = [ParseDocument(store=LocalObjectStore(tmp_path/'body'),max_document_bytes=64,request_gate=gate)
                  for _ in range(2)]
        return await asyncio.gather(*(actor(request) for actor in actors))
    receipts = asyncio.run(run())
    assert all(r['status']=='parse_error' and r['document_ref'] is None and r['raw_ref']==request['raw_ref'] for r in receipts)
    assert gate.peak == 1 and gate.admitted == 2


def test_parse_cancellation_drains_and_preserves_cancellation_on_child_failure(tmp_path, monkeypatch):
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    def child(*args,**kwargs):
        started.set()
        assert release.wait(5)
        finished.set()
        raise TimeoutError('child failed while cancelled')
    monkeypatch.setattr('demiflow.collect.acquisition.run_isolated',child)
    actor = ParseDocument(store=LocalObjectStore(tmp_path/'objects'))
    async def run():
        task = asyncio.create_task(actor(dict(raw_ref={},url='',final_url='',content_type='',retrieved_at='')))
        assert await asyncio.to_thread(started.wait,5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError): await task
        assert finished.is_set()
    asyncio.run(run())
