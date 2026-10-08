import io
from pathlib import Path
import hashlib

import httpx
import pytest
from PIL import Image

from demiflow import data
from demiflow.collect.image_fetch import ImageFetchPolicy
from demiflow.collect.image_library import ImageLibrary
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient


def pixels(size=(8, 6)):
    out = io.BytesIO()
    Image.new('RGB', size, 'red').save(out, format='PNG')
    return out.getvalue()


@pytest.mark.parametrize('outcome',['ok','network_error','interrupted'])
def test_old_concurrency_receipts_survive_parallelism_and_transport_change(tmp_path,monkeypatch,outcome):
    import asyncio
    import sqlite3
    import json
    original_once=WebClient._once
    async def legacy_once(self,kind,identity,fn):
        if kind=='fetch_image':
            identity=list(identity);identity[5]={**identity[5],'concurrency':64}
        return await original_once(self,kind,identity,fn)
    monkeypatch.setattr(WebClient,'_once',legacy_once)
    async def request(self,url,**kwargs):
        if outcome=='interrupted':raise asyncio.CancelledError()
        value={'status':outcome,'reason':'fixture','attempts':[]}
        if outcome=='ok':value.update(body=pixels(),headers={'content-type':'image/png'},final_url=url)
        return value
    monkeypatch.setattr(WebClient,'_get',request)
    previous={'session_pool':{'factory':'test_image_fetch:unused_factory','size':1}}
    current={'pool':[{'name':'fixed','proxy':'http://fixed.example:3128'}],'reuse_completed':[previous]}
    def session(route,concurrency):
        return WebSession(cache_path=str(tmp_path/'journal.sqlite'),object_directory=str(tmp_path/'objects'),
            image_library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'index.sqlite')),
            image_policy=ImageFetchPolicy(concurrency=concurrency,reuse_completed_concurrency=[64]),
            fetch_proxy_routes={'*':route},retries=0,host_interval_s=0)
    async def run():
        old=session(previous,64)
        try:
            if outcome=='interrupted':
                with pytest.raises(asyncio.CancelledError):await old.fetch_image({'url':'https://image.example/a.png'})
                with sqlite3.connect(tmp_path/'journal.sqlite') as db:before=json.loads(db.execute('SELECT value FROM cache').fetchone()[0])
            else:before=await old.fetch_image({'url':'https://image.example/a.png'})
        finally:await old.aclose()
        monkeypatch.setattr(WebClient,'_once',original_once)
        async def forbidden(*args,**kwargs):raise AssertionError('Concurrency change renewed a request')
        monkeypatch.setattr(WebClient,'_get',forbidden)
        for concurrency in (128,16):
            new=session(current,concurrency)
            try:
                assert await new.fetch_image({'url':'https://image.example/a.png'})==before
                assert new.image_client.web.fetch_gate.concurrency==concurrency
            finally:await new.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('outcome',['ok','network_error','interrupted'])
def test_image_transport_migration_keeps_session_receipts_without_new_http(tmp_path,monkeypatch,outcome):
    import asyncio
    import json
    import sqlite3
    calls=[]
    async def fetch(self,url,**options):
        calls.append(url)
        if outcome=='interrupted':raise asyncio.CancelledError()
        result={'status':outcome,'reason':'fixture','attempts':[{'attempt':1,'status':outcome}]}
        if outcome=='ok':result.update(body=pixels(),headers={'content-type':'image/png'},final_url=url)
        return result
    monkeypatch.setattr(WebClient,'_get',fetch)
    previous={'session_pool':{'factory':'test_image_fetch:unused_factory','size':1}}
    migrated={'pool':[{'name':'explicit_ip','proxy':'http://new.example:3128'}],
              'reuse_completed':[previous]}
    def web(route):
        return WebSession(cache_path=str(tmp_path/'migration.sqlite'),object_directory=str(tmp_path/'objects'),
            image_library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'index.sqlite')),
            retries=0,host_interval_s=0,fetch_proxy_routes={'*':route})
    request={'url':'https://images.example/example.png'}
    async def run():
        old=web(previous)
        try:
            if outcome=='interrupted':
                with pytest.raises(asyncio.CancelledError):await old.fetch_image(request)
                with sqlite3.connect(tmp_path/'migration.sqlite') as db:
                    before=json.loads(db.execute('SELECT value FROM cache').fetchone()[0])
            else:before=await old.fetch_image(request)
        finally:await old.aclose()
        async def forbidden(*args,**kwargs):raise AssertionError('Migration purchased another HTTP attempt')
        monkeypatch.setattr(WebClient,'_get',forbidden)
        for _ in range(2):
            current=web(migrated)
            try:
                assert await current.fetch_image(request)==before
                assert current.image_client.web.metrics['reused_previous_proxy']==1
                assert not current.image_client.web.fetch_session_pools
            finally:await current.aclose()
        assert len(calls)==1
    asyncio.run(run())


def test_decode_capacity_is_separate_from_download_capacity(tmp_path, monkeypatch):
    import asyncio
    import threading
    import time
    from demiflow.collect import image_fetch
    calls=network(monkeypatch,pixels())
    lock=threading.Lock();active=0;peak=0;count=0
    def decode(*args,**kwargs):
        nonlocal active,peak,count
        with lock:active+=1;peak=max(peak,active);count+=1
        try:
            time.sleep(.03)
            return {'width':8,'height':6,'format':'PNG','content_type':'image/png','size_bytes':len(pixels())}
        finally:
            with lock:active-=1
    monkeypatch.setattr(image_fetch,'decode_file',decode)
    async def run():
        web=session(tmp_path,concurrency=8,decode_concurrency=2)
        try:
            return await asyncio.gather(*(web.fetch_image({'url':f'https://images-{i}.example/a'}) for i in range(8)))
        finally:await web.aclose()
    results=asyncio.run(run())
    assert all(r['status']=='ok' for r in results)
    assert len(calls)==count==8 and peak==2 and active==0
    with pytest.raises(ValueError):ImageFetchPolicy(decode_concurrency=0)


def test_decode_scheduling_change_keeps_completed_failure(tmp_path,monkeypatch):
    calls=network(monkeypatch,b'bad pixels')
    request={'request_id':'r','url':'https://images.example/invalid'}
    old=execute(session(tmp_path,decode_concurrency=1),[request])[0]['result']
    new=execute(session(tmp_path,decode_concurrency=8),[request])[0]['result']
    assert old==new and new['status']=='image_error' and len(calls)==1


def test_decode_budget_excludes_framework_imports(tmp_path, monkeypatch):
    # A framework-started worker maps >1 GiB here, even before any pixel decode.
    # A valid image must fit the 256 MiB decoder budget independently of BLAS.
    monkeypatch.setenv('OPENBLAS_NUM_THREADS', '64')
    calls = network(monkeypatch, pixels((1024, 1024)))
    result = execute(session(tmp_path, decode_memory_bytes=256 * 1024**2),
        [{'request_id': 'r', 'url': 'https://images.example/normal.png'}])[0]['result']
    assert result['status'] == 'ok', result
    assert (result['width'], result['height']) == (1024, 1024)
    assert len(calls) == 1


def test_decode_address_space_limit_still_rejects_pixel_allocation(tmp_path, monkeypatch):
    # Build a compressed 8192² RGBA raster with a row-size encoder allocation.
    # Its decoded pixels alone need 256 MiB, exceeding the worker's total limit.
    import struct
    import zlib
    def chunk(kind, body):
        return struct.pack('>I', len(body)) + kind + body + struct.pack('>I', zlib.crc32(kind + body))
    compressor = zlib.compressobj()
    encoded = bytearray()
    row = b'\0' * (1 + 8192 * 4)
    for _ in range(8192):
        encoded.extend(compressor.compress(row))
    encoded.extend(compressor.flush())
    payload = (b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>2I5B', 8192, 8192, 8, 6, 0, 0, 0))
               + chunk(b'IDAT', encoded) + chunk(b'IEND', b''))
    calls = network(monkeypatch, payload)
    result = execute(session(tmp_path, decode_memory_bytes=256 * 1024**2, max_pixels=8192**2),
        [{'request_id': 'r', 'url': 'https://images.example/wide.png'}])[0]['result']
    assert result['status'] == 'image_error' and result['image_ref'] is None
    assert result['reason'].startswith('MemoryError:'), result
    assert len(calls) == 1


def test_fetch_attempt_cap_preserves_prior_receipts_and_caps_new_downloads(tmp_path,monkeypatch):
    import asyncio
    calls=[]
    def handle(request):
        calls.append(str(request.url))
        raise httpx.ConnectError('fixture unavailable',request=request)
    def client(self,search=False):
        if self.client is None:self.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        return self.client
    monkeypatch.setattr(WebClient,'_client',client)
    def web(limit):
        return WebSession(cache_path=str(tmp_path/'cap.sqlite'),object_directory=str(tmp_path/'objects'),
            image_library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'index.sqlite')),
            retries=5,retry_delay_s=0,host_interval_s=0,fetch_attempt_limit=limit)
    async def run():
        old=web(None)
        try:previous=await old.fetch_image({'url':'https://images.example/old'})
        finally:await old.aclose()
        assert len(calls)==6
        capped=web(3)
        try:
            assert await capped.fetch_image({'url':'https://images.example/old'})==previous
            current=await capped.fetch_image({'url':'https://images.example/new'})
            assert len(current['attempts'])==3 and len(calls)==9
        finally:await capped.aclose()
        restored=web(None)
        try:assert await restored.fetch_image({'url':'https://images.example/new'})==current
        finally:await restored.aclose()
        assert len(calls)==9
    asyncio.run(run())
    for limit in (0,7,True,1.5):
        with pytest.raises(ValueError,match='fetch_attempt_limit'):web(limit)


def session(tmp_path, name='run', **policy):
    return WebSession(cache_path=str(tmp_path / (name + '.sqlite')),
        object_directory=str(tmp_path / 'objects'),
        image_library=ImageLibrary(str(tmp_path / 'objects'), str(tmp_path / 'index.sqlite')),
        image_policy=ImageFetchPolicy(**policy), retries=0, host_interval_s=0)


def execute(web, requests, **options):
    rows = []
    ds = data.from_items([{'id': 'a', 'requests': requests}]).fetch_images(
        requests='requests', output='fetched', session=web, **options)
    ds.map(lambda r: rows.append(r) or r).run_stream()
    return rows[0]['fetched']


def network(monkeypatch, body):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(body), headers={'content-type': 'application/octet-stream'})

    def client(self, search=False):
        if self.client is None:
            self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return self.client
    monkeypatch.setattr(WebClient, '_client', client)
    return calls


def test_lazy_typed_node_and_local_download_reuse(tmp_path, monkeypatch):
    from demiflow.data.plan import FetchImagesOp
    payload = pixels()
    calls = network(monkeypatch, payload)
    web = session(tmp_path)
    request = {'request_id': 'r', 'url': 'https://images.example/a.png', 'bindings': ['opaque']}
    plan = data.from_items([{'requests': [request]}]).fetch_images(requests='requests', output='fetched', session=web)
    assert isinstance(plan._plan.operations[-1], FetchImagesOp)
    assert not list(tmp_path.iterdir()) and not calls
    first = execute(web, [request])[0]
    assert first['bindings'] == ['opaque'] and first['result']['status'] == 'ok'
    result = first['result']
    assert result['content_type'] == 'image/png' and result['width'] == 8
    assert result['image_ref']['sha256'] == hashlib.sha256(payload).hexdigest()
    assert len(calls) == 1
    # Same journal replays original receipt; a different run uses shared storage.
    assert execute(session(tmp_path), [request])[0]['result'] == result
    second = execute(session(tmp_path, 'next'), [request])[0]['result']
    assert second['origin'] == 'local' and second['attempts'] == []
    assert second['image_ref'] == result['image_ref'] and len(calls) == 1
    by_sha = execute(session(tmp_path, 'sha'), [{'request_id': 'sha', 'sha256': result['image_ref']['sha256']}])[0]['result']
    assert by_sha['status'] == 'ok' and by_sha['origin'] == 'local' and len(calls) == 1


def test_same_url_requests_share_acquisition_and_preserve_order(tmp_path, monkeypatch):
    calls = network(monkeypatch, pixels())
    req = [{'request_id': str(i), 'url': 'https://images.example/a', 'bindings': [str(i)]} for i in range(3)]
    out = execute(session(tmp_path), req, request_concurrency=3)
    assert len(calls) == 1 and [r['request_id'] for r in out] == ['0', '1', '2']
    assert [r['bindings'] for r in out] == [['0'], ['1'], ['2']]


@pytest.mark.parametrize('payload,policy,status', [
    (b'<html>not an image</html>', {}, 'image_error'),
    (pixels((9, 9)), {'max_pixels': 16}, 'image_error'),
    (pixels(), {'max_bytes': 16}, 'content_error'),
])
def test_invalid_and_oversize_are_receipts_not_images(tmp_path, monkeypatch, payload, policy, status):
    calls = network(monkeypatch, payload)
    request = {'request_id': 'r', 'url': 'https://images.example/a'}
    result = execute(session(tmp_path, **policy), [request])[0]['result']
    assert result['status'] == status and result['image_ref'] is None
    assert len(calls) == 1
    assert execute(session(tmp_path, **policy), [request])[0]['result'] == result
    assert len(calls) == 1


def test_expected_sha_mismatch_and_missing_cached_file(tmp_path, monkeypatch):
    calls = network(monkeypatch, pixels())
    bad = execute(session(tmp_path, 'bad'), [{'request_id': 'r', 'url': 'https://images.example/a', 'sha256': 'a'*64}])[0]['result']
    assert bad['status'] == 'integrity_error'
    request = {'request_id': 'r', 'url': 'https://images.example/a'}
    good = execute(session(tmp_path, 'good'), [request])[0]['result']
    from demiflow.collect.image_library import local_path
    local_path(good['image_ref']['uri']).unlink()
    again = execute(session(tmp_path, 'good'), [request])[0]['result']
    assert again['status'] == 'integrity_error' and len(calls) == 2


def test_local_file_import_without_http(tmp_path, monkeypatch):
    calls = network(monkeypatch, b'should not fetch')
    path = tmp_path / 'original.png'
    payload = pixels()
    path.write_bytes(payload)
    sha = hashlib.sha256(payload).hexdigest()
    result = execute(session(tmp_path), [{'request_id': 'r', 'sha256': sha, 'image_uri': path.as_uri()}])[0]['result']
    assert result['status'] == 'ok' and result['origin'] == 'local' and not calls
    assert (tmp_path / 'objects' / sha[:2] / sha).read_bytes() == payload


def test_metadata_bounds_and_duplicate_ids_before_http(tmp_path, monkeypatch):
    calls = network(monkeypatch, pixels())
    req = {'request_id': 'r', 'url': 'https://images.example/a'}
    with pytest.raises(ValueError, match='unique'):
        execute(session(tmp_path, 'dupe'), [req, req])
    with pytest.raises(ValueError, match='positive'):
        execute(session(tmp_path, 'list'), [req], max_requests=0)
    with pytest.raises(ValueError, match='budget'):
        execute(session(tmp_path, 'bytes'), [req], max_request_bytes=8)
    assert not calls


def test_search_images_keep_distinct_images_on_same_page():
    from demiflow.collect.searxng import normalize_response
    from demiflow.collect.web import normalized_url
    response = normalize_response({'results': [
        {'url': 'https://example.org/page', 'img_src': 'https://example.org/a.png', 'thumbnail_src': 'https://example.org/a-small.png', 'engine': 'one'},
        {'url': 'https://example.org/page', 'img_src': 'https://example.org/b.png', 'engine': 'one'},
        {'url': 'https://example.org/page', 'img_src': 'https://example.org/a.png', 'engine': 'two'},
    ]}, normalized_url)
    assert len(response['candidates']) == 2
    assert response['candidates'][0]['engines'] == ['one', 'two']
    assert response['candidates'][0]['thumbnail_src'] == 'https://example.org/a-small.png'
    assert response['candidates'][1]['img_src'] == 'https://example.org/b.png'


def test_search_scheme_relative_image_urls_keep_original_rendition_and_dimensions():
    from demiflow.collect.searxng import normalize_response
    from demiflow.collect.web import normalized_url
    response = normalize_response({'results': [
        {'url': 'https://www.flickr.com/photos/example/1', 'category': 'images',
         'img_src': '//live.staticflickr.com/1/photo_k.jpg',
         'thumbnail_src': '//live.staticflickr.com/1/photo_n.jpg', 'resolution': '2048 x 1536'},
        {'url': 'https://example.org/page', 'category': 'images',
         'img_src': 'javascript:alert(1)', 'resolution': '0 x 100'},
        {'url': 'https://example.org/page2', 'category': 'images',
         'img_src': '//user:password@example.org/photo.jpg', 'resolution': 'unknown'},
    ]}, normalized_url)
    first, invalid, credentialed = response['candidates']
    assert first['img_src'] == 'https://live.staticflickr.com/1/photo_k.jpg'
    assert first['thumbnail_src'] == 'https://live.staticflickr.com/1/photo_n.jpg'
    assert (first['declared_width'], first['declared_height']) == (2048, 1536)
    assert invalid['img_src'] is None and 'declared_width' not in invalid
    assert credentialed['img_src'] is None and 'declared_width' not in credentialed


def test_two_run_journals_share_inflight_url(tmp_path,monkeypatch):
    import asyncio
    from demiflow.collect.image_fetch import ImageClient
    calls=network(monkeypatch,pixels())
    request={'request_id':'r','url':'https://images.example/shared'}
    async def run():
        a=session(tmp_path,'parallel-a');b=session(tmp_path,'parallel-b')
        try:
            return await asyncio.gather(a.fetch_image(request),b.fetch_image(request))
        finally:
            await a.aclose();await b.aclose()
    results=asyncio.run(run())
    assert len(calls)==1
    assert {r['origin'] for r in results}=={'local','download'}
    assert results[0]['image_ref']==results[1]['image_ref']


def test_frame_limit_and_corrupt_local_object(tmp_path,monkeypatch):
    frames=[Image.new('RGB',(8,6),c) for c in ('red','blue')]
    buf=io.BytesIO();frames[0].save(buf,format='GIF',save_all=True,append_images=frames[1:],duration=10)
    calls=network(monkeypatch,buf.getvalue())
    request={'request_id':'r','url':'https://images.example/animated.gif'}
    bad=execute(session(tmp_path,'frame'),[request])[0]['result']
    assert bad['status']=='image_error' and bad['image_ref'] is None
    good=execute(session(tmp_path,'frames',max_frames=2),[request])[0]['result']
    assert good['status']=='ok'
    from demiflow.collect.image_library import local_path
    local_path(good['image_ref']['uri']).write_bytes(b'corrupt')
    bad=execute(session(tmp_path,'after',max_frames=2),[request])[0]['result']
    assert bad['status']=='image_error' and len(calls)==2


def test_cancelled_lock_wait_does_not_release_other_owner(tmp_path):
    import asyncio
    library=session(tmp_path).image_library
    async def run():
        acquired=[]
        async def waiter():
            async with library.claim('same'):acquired.append(True)
        async with library.claim('same'):
            task=asyncio.create_task(waiter());await asyncio.sleep(.08);task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
            assert not acquired
        async with library.claim('same'):acquired.append(True)
        assert acquired==[True]
    asyncio.run(run())


def test_session_closes_images_when_search_cleanup_fails(tmp_path):
    import asyncio
    from types import SimpleNamespace
    closed=[]
    class Resource:
        def __init__(self,name):self.name=name
        async def aclose(self):
            closed.append(self.name)
            if self.name=='search':raise RuntimeError('close failed')
    web=session(tmp_path)
    web.native=Resource('search');web.client=Resource('documents')
    web.image_client=SimpleNamespace(web=Resource('images'));web.owner=Resource('owner')
    with pytest.raises(RuntimeError,match='close failed'):asyncio.run(web.aclose())
    assert closed==['search','documents','images','owner']
    assert web.image_client is None and web.native is None
