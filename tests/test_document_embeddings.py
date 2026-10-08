"""Document spans actually reach native HTTP; replay and invalid inputs stay safe."""
import hashlib
import json
from copy import deepcopy

import pytest
from demiflow import data
from demiflow.collect.documents import store_document
from demiflow.embeddings.config import embedding_options
from demiflow.embeddings.documents import DocumentInputReader
from test_embeddings import server


def fixture_input(tmp_path):
    stored = store_document(tmp_path / 'objects', b'First paragraph.\n\nSecond paragraph.',
                            url='https://example.org/a', final_url='https://example.org/a',
                            content_type='text/plain')
    # store_document returns the normalized document ObjectRef and raw ObjectRef.
    ref = stored[0] if isinstance(stored, tuple) else stored
    from demiflow.collect.documents import read_document
    if hasattr(ref, 'to_dict'):
        ref = ref.to_dict()
    doc = read_document(ref)
    block = doc['blocks'][0]
    text = 'Title\n\n' + block['text'][0:5]
    return dict(document_ref=ref, prefix='Title\n\n',
                spans=[dict(block_id=block['block_id'], start=0, end=5)],
                text_sha256=hashlib.sha256(text.encode()).hexdigest()), text


def test_native_document_payload_and_replay(server, tmp_path):
    value, text = fixture_input(tmp_path)
    bad = deepcopy(value); bad['spans'][0]['end'] = 99999
    ds = data.from_items([{'id': 'ok', 'document_input': value},
                          {'id': 'bad', 'document_input': bad}]).document_embeddings(
        model=server['model'], batch_size=2, error_output='error', call_output='call',
        options={'sqlite_journal': {'path': str(tmp_path / 'journal.sqlite')}})
    rows = ds.materialize().take_all()
    assert server['bodies'][0]['input'] == [text]
    assert len(server['bodies']) == 1
    assert next(r for r in rows if r['id'] == 'bad')['embedding'] is None
    assert next(r for r in rows if r['id'] == 'ok')['call']['input_text_sha256'] == value['text_sha256']
    ds.materialize()
    assert len(server['bodies']) == 1
    assert ds._stages[-1]._document_reader is None


def test_hash_size_cache_and_unicode_bounds(tmp_path):
    value, text = fixture_input(tmp_path)
    reader = DocumentInputReader(embedding_options({'document_cache_bytes': 1}))
    assert reader(value)[0] == text and reader.bytes == 0 and not reader.cache
    reader = DocumentInputReader(embedding_options())
    reader(value)
    assert reader.bytes > 0
    reader.clear(); assert not reader.cache and reader.bytes == 0
    wrong = deepcopy(value); wrong['text_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='SHA256'): reader(wrong)
    wrong = deepcopy(value); wrong['document_ref']['sha256'] = '0' * 64
    with pytest.raises(ValueError): reader(wrong)
    with pytest.raises(ValueError, match='max_document_text_bytes'):
        DocumentInputReader(embedding_options({'max_document_text_bytes': 2}))(value)
    with pytest.raises(ValueError):
        DocumentInputReader(embedding_options({'max_document_bytes': 2}))(value)


def test_model_prefix_is_applied_after_source_hash_verification(tmp_path):
    value, text = fixture_input(tmp_path)
    reader = DocumentInputReader(embedding_options())
    actual, metadata = reader({**value, 'model_prefix': 'passage: '})
    assert actual == 'passage: ' + text
    assert metadata['input_text_sha256'] == hashlib.sha256(actual.encode()).hexdigest()


def test_text_import_preserves_unknown_time_and_raw_bytes(tmp_path):
    from demiflow.collect.document_formats import normalize_material
    from demiflow.collect.document_library import DocumentLibrary
    request = dict(format='text', text='中文正文。\n\nSecond paragraph.',
        source=dict(url='https://example.org/a',final_url='https://example.org/a',title='Article',retrieved_at=''))
    doc, raw = normalize_material(request, max_bytes=1024)
    assert raw == request['text'].encode() and doc['source']['retrieved_at'] == ''
    assert 'not original HTTP response' in doc['parser']['raw_format']
    with pytest.raises(ValueError): normalize_material(request,max_bytes=1)
    library = DocumentLibrary(index_path=tmp_path/'index.sqlite',object_directory=tmp_path/'objects')
    result = data.from_items([{'request':request}]).register_documents(
        request='request', output='registration', library=library).materialize().take(1)[0]['registration']
    assert result['status'] == 'ok'
    replay = library.lookup('https://example.org/a',max_bytes=1024,max_document_bytes=4096)
    assert replay['document_ref'] == result['document_ref']
