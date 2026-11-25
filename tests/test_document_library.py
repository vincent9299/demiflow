"""Generic import/index/history integrity; transport is Dataset.fetch."""
import asyncio
from dataclasses import replace
from pathlib import Path
import sqlite3
import threading
from urllib.parse import urlsplit
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.collect.contracts import DOCUMENT_RESULT
from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.documents import DocumentError,read_document,store_document
from _row_capture import run_stream_rows as capture
URL='https://source.example/page'

@pytest.fixture
def library(tmp_path):
    return DocumentLibrary(index_path=tmp_path/'public/index.sqlite', object_directory=tmp_path/'public/objects')


def existing(tmp_path, text=b'Original content', date='2026-01-01T00:00:00+00:00'):
    return store_document(tmp_path/'original', text, url=URL, final_url=URL,
                          content_type='text/plain', retrieved_at=date)



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
    result=library.lookup(alias,max_bytes=1000,max_document_bytes=10000)
    assert result['document_ref']==receipt['document_ref']
    assert result['acquisition']['origin']=='import'
    assert read_document(result['document_ref'])['blocks'][0]['text']=='Original content'


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


def test_lazy_declaration_and_strict_configuration(tmp_path,library):
    graph=data.from_items([]).register_documents(request='request',output='out',library=library)
    from demiflow.data.plan import RegisterDocumentsOp
    assert isinstance(graph._plan.operations[-1],RegisterDocumentsOp)
    assert not Path(library.index_path).parent.exists()
    for kw in ({'policy':'guess'},{'max_age_s':0},{'lock_timeout_s':float('inf')},{'parser_versions':[]}):
        with pytest.raises(ValueError):replace(library,**kw)

@pytest.mark.parametrize('target',['raw_ref','document_ref'])
def test_corrupt_public_object_stops_without_hidden_download(tmp_path,library,target):
    saved=library.register({'document_ref':existing(tmp_path)},max_bytes=1000,max_document_bytes=10000)
    Path(urlsplit(saved[target]['uri']).path).write_bytes(b'broken')
    with pytest.raises(DocumentError):library.lookup(URL,max_bytes=1000,max_document_bytes=10000)
