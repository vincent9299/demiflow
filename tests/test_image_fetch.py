"""Independent image decode/import/history and search candidate normalization."""
import asyncio
import io
from pathlib import Path
import pytest
from PIL import Image
from demiflow.objects import LocalObjectStore
from demiflow.collect.image_decode import decode_image,local_path
from demiflow.collect.image_library import ImageLibrary

POLICY=dict(max_bytes=1024**2,max_pixels=1024**2,max_frames=1,decode_memory_bytes=256*1024**2,decode_timeout_s=10)

def pixels(size=(8,6)):
    out=io.BytesIO();Image.new('RGB',size,'red').save(out,format='PNG');return out.getvalue()

def validate(tmp_path,payload,**policy):
    ref=LocalObjectStore(tmp_path/'objects').put(payload)
    return ref,decode_image(ref.uri,ref.sha256,{**POLICY,**policy})

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


def test_cancelled_lock_wait_does_not_release_other_owner(tmp_path):
    import asyncio
    library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'index.sqlite'))
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


def test_decode_budget_excludes_framework_imports(tmp_path,monkeypatch):
    monkeypatch.setenv('OPENBLAS_NUM_THREADS','64')
    _,metadata=validate(tmp_path,pixels((1024,1024)))
    assert (metadata['width'],metadata['height'])==(1024,1024)

def test_decode_address_space_limit_still_rejects_pixel_allocation(tmp_path):
    import struct,zlib
    def chunk(kind,body):
        return struct.pack('>I',len(body))+kind+body+struct.pack('>I',zlib.crc32(kind+body))
    compressor=zlib.compressobj();encoded=bytearray();row=b'\0'*(1+8192*4)
    for _ in range(8192):encoded.extend(compressor.compress(row))
    encoded.extend(compressor.flush())
    payload=(b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>2I5B',8192,8192,8,6,0,0,0))
             +chunk(b'IDAT',encoded)+chunk(b'IEND',b''))
    with pytest.raises(MemoryError):validate(tmp_path,payload,max_pixels=8192**2)

def test_frame_limit_and_corrupt_local_object(tmp_path):
    out=io.BytesIO();frames=[Image.new('RGB',(8,6),c) for c in ('red','blue')]
    frames[0].save(out,format='GIF',save_all=True,append_images=frames[1:],duration=10)
    with pytest.raises(ValueError,match='frame_limit'):validate(tmp_path,out.getvalue())
    ref,metadata=validate(tmp_path,out.getvalue(),max_frames=2)
    assert (metadata['width'],metadata['height'])==(8,6)
    local_path(ref.uri).write_bytes(b'corrupt')
    with pytest.raises(ValueError,match='sha256'):decode_image(ref.uri,ref.sha256,{**POLICY,'max_frames':2})

def test_explicit_local_import_can_be_read_from_historical_library(tmp_path):
    ref,metadata=validate(tmp_path,pixels())
    library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'images.sqlite'))
    receipt=dict(status='ok',url='https://example.org/original',final_url='https://example.org/final',
                 image_ref=ref.to_dict(),**metadata)
    library.publish(receipt)
    assert library.lookup(receipt['url'])==receipt and library.lookup(receipt['final_url'])==receipt
    historical=library.reference(ref.sha256)
    assert historical.read(max_bytes=1024**2)==pixels()
