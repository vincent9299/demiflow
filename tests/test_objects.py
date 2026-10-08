import hashlib
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

from demiflow.objects import LocalObjectStore, ObjectRef


def test_json_data_urls_store_bytes_once_and_restore_exact_request(tmp_path):
    from demiflow.objects import externalize_data_uris, restore_data_uris
    original = {'images': ['data:image/png;base64,YWJj', 'data:image/png;base64,YWJj'],
                'text': 'keep original text'}
    stored = externalize_data_uris(original, LocalObjectStore(tmp_path))
    assert stored['images'][0]['_demiflow_data_uri']['uri'].startswith('file:///')
    assert stored['images'][0] == stored['images'][1]
    assert restore_data_uris(stored) == original
    assert len([p for p in tmp_path.rglob('*') if p.is_file()]) == 1
    uri = stored['images'][0]['_demiflow_data_uri']['uri']
    Path(unquote(urlsplit(uri).path)).write_bytes(b'changed')
    with pytest.raises(ValueError, match='SHA256 mismatch'):
        restore_data_uris(stored)


def test_plain_file_is_readable_without_lance(tmp_path):
    payload = b'image, audio or any other bytes'
    ref = LocalObjectStore(tmp_path).put(payload)
    assert set(ref.to_dict()) == {'uri', 'sha256'}
    assert Path(unquote(urlsplit(ref.uri).path)).read_bytes() == payload
    assert ObjectRef(**ref.to_dict()).read() == payload
    assert ref.verify() == len(payload)


def test_read_rejects_oversized_objects_before_materializing_them(tmp_path, monkeypatch):
    ref = LocalObjectStore(tmp_path).put(b'12345')
    assert ref.read(max_bytes=5) == b'12345'
    with pytest.raises(ValueError, match='max_bytes'):
        ref.read(max_bytes=4)
    for limit in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            ref.read(max_bytes=limit)

    import demiflow.objects as objects
    sizes = []
    class Stream(io.BytesIO):
        def read(self, size=-1):
            sizes.append(size)
            return super().read(size)
    monkeypatch.setattr(objects, 'open_object', lambda uri: Stream(b'x' * 10000))
    with pytest.raises(ValueError, match='max_bytes'):
        ref.read(max_bytes=32)
    assert sizes == [33]


def test_concurrent_identical_objects_share_one_file(tmp_path):
    store = LocalObjectStore(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        refs = list(pool.map(store.put, [b'same content'] * 32))
    assert len({ref.uri for ref in refs}) == 1
    assert len([p for p in tmp_path.rglob('*') if p.is_file()]) == 1
    assert refs[0].read() == b'same content'


def test_corrupt_existing_object_is_not_overwritten(tmp_path):
    store = LocalObjectStore(tmp_path)
    ref = store.put(b'original')
    path = Path(unquote(urlsplit(ref.uri).path))
    path.write_bytes(b'corruption')
    with pytest.raises(ValueError, match='SHA256 mismatch'):
        ref.read()
    with pytest.raises(ValueError, match='SHA256 mismatch'):
        store.put(b'original')
    assert path.read_bytes() == b'corruption'
    assert not list(tmp_path.glob('.pending-*'))


def test_streaming_hash_mismatch_never_publishes(tmp_path):
    store = LocalObjectStore(tmp_path)
    with pytest.raises(ValueError, match='before publication'):
        store.put(b'wrong', sha256=hashlib.sha256(b'expected').hexdigest())
    assert not list(tmp_path.iterdir())


def test_stream_reads_are_bounded_and_handles_remain_callers(tmp_path):
    class Stream(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024
            return super().read(size)

    raw = b'x' * (3 * 1024 * 1024 + 7)
    stream = Stream(raw)
    store = LocalObjectStore(tmp_path.as_uri())
    ref = store.put_stream(stream, sha256=hashlib.sha256(raw).hexdigest())
    assert not stream.closed
    assert ref.verify() == len(raw)


def test_existing_bytes_verify_without_creating_temporary_file(tmp_path, monkeypatch):
    store = LocalObjectStore(tmp_path)
    ref = store.put(b'already durable')
    import demiflow.objects as objects
    def cannot_stage(*args, **kwargs):
        raise AssertionError('Existing verified object must not be rewritten')
    monkeypatch.setattr(objects.tempfile, 'mkstemp', cannot_stage)
    assert store.put(b'already durable') == ref
    Path(unquote(urlsplit(ref.uri).path)).write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='SHA256 mismatch'):
        store.put(b'already durable')


def test_known_hash_stream_mismatch_leaves_no_partial_object(tmp_path):
    store = LocalObjectStore(tmp_path)
    with pytest.raises(ValueError, match='before publication'):
        store.put_stream(io.BytesIO(b'wrong'), sha256=hashlib.sha256(b'expected').hexdigest())
    assert not [p for p in tmp_path.rglob('*') if p.is_file()]


@pytest.mark.parametrize('uri', ['relative/image.jpg', 'file:relative', 'file://elsewhere/image'])
def test_reference_requires_independently_resolvable_uri(uri):
    with pytest.raises(ValueError):
        ObjectRef(uri, 'a' * 64)


def test_local_writer_rejects_remote_uri_instead_of_making_local_directory():
    with pytest.raises(ValueError, match='local directory'):
        LocalObjectStore('s3://bucket/objects')
