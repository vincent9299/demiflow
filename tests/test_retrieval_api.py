"""Actual native encoding/search, fixed vector space, and real image delivery."""
import asyncio
from dataclasses import asdict
import io
import json

import lance
import pyarrow as pa
import pytest
from PIL import Image

from demiflow.environment import OperatorEnvironment
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.operator_api import OperatorCallError
from demiflow.operator_media import OperatorImages
from demiflow.embeddings.model import canonical
from test_embeddings import server


@pytest.fixture
def retrieval(server, tmp_path):
    model = server['model']
    store = LocalObjectStore(tmp_path / 'objects')
    raw = io.BytesIO()
    Image.new('RGB', (8, 6), 'red').save(raw, 'PNG')
    image = store.put(raw.getvalue()).to_dict()
    uri = str(tmp_path / 'vectors.lance')
    schema = pa.schema([('sha256', pa.string()), ('image_uri', pa.string()),
        ('embedding', pa.list_(pa.float32(), 3))], metadata={b'embedding.contract': canonical(model.contract()).encode()})
    lance.write_dataset(pa.Table.from_pylist([{'sha256': image['sha256'], 'image_uri': image['uri'],
                                            'embedding': [.6, .8, 0.]}], schema=schema), uri)
    settings = {
        'map_embeddings': {'arguments': {'model': asdict(model), 'object_directory': str(store.directory)}},
        'search_vectors': {'arguments': {'uri': uri, 'version': 1, 'vector_column': 'embedding',
            'columns': ['sha256', 'image_uri'], 'encoder_id': model.fingerprint,
            'object_directory': str(store.directory), 'contract_metadata_key': 'embedding.contract',
            'max_top_k': 4, 'options': {'use_index': False}},
            'result_images': {'items_field': 'candidates', 'uri_field': 'image_uri', 'sha256_field': 'sha256'}}}
    env = OperatorEnvironment(runtime='codex', operators=('map_embeddings', 'search_vectors'), operator_settings=settings)
    return env, image


def test_native_encoding_search_and_actual_image(retrieval, server):
    env, image = retrieval
    async def run():
        scope = env.bind({})
        encoded = await scope.invoke({'method': 'map_embeddings', 'arguments': {'text': 'red object'}}, context=None)
        assert 'vector' not in encoded and encoded['dimensions'] == 3
        vector = json.loads(ObjectRef(**encoded['embedding_ref']).read(max_bytes=4096))
        assert vector['vector'] == pytest.approx([.6, .8, 0.])
        result = await scope.invoke({'method': 'search_vectors', 'arguments': {
            'query_ref': encoded['embedding_ref'], 'top_k': 1}}, context=None)
        assert result['candidates'][0]['sha256'] == image['sha256']
        receipts, images = await OperatorImages(env).render(result, env.image_output('search_vectors'))
        assert receipts[0]['image_id'] == 'sha256:' + image['sha256']
        assert receipts[0]['status'] == 'attached' and images[0].startswith('data:image/png;base64,')
        for args in ({'query_ref': encoded['embedding_ref'], 'top_k': 5},
                     {'query_ref': image}, {'query_ref': {**encoded['embedding_ref'], 'uri': image['uri']}}):
            with pytest.raises((OperatorCallError, ValueError)):
                await scope.invoke({'method': 'search_vectors', 'arguments': args}, context=None)
        return encoded
    asyncio.run(run())
    assert len(server['bodies']) == 1 and server['bodies'][0]['input'] == ['red object']


def test_same_dimensions_wrong_vector_space_is_rejected(retrieval):
    env, _ = retrieval
    async def run():
        scope = env.bind({})
        encoded = await scope.invoke({'method': 'map_embeddings', 'arguments': {'text': 'object'}}, context=None)
        settings = env.operator_settings['search_vectors']['arguments']
        store = LocalObjectStore(settings['object_directory'])
        value = json.loads(ObjectRef(**encoded['embedding_ref']).read(max_bytes=4096))
        value['encoder_id'] = '0' * 64
        bad = store.put(canonical(value).encode()).to_dict()
        with pytest.raises(OperatorCallError, match='encoder_id'):
            await scope.invoke({'method': 'search_vectors', 'arguments': {'query_ref': bad}}, context=None)
    asyncio.run(run())


def test_embedding_service_failure_is_a_native_error_result(retrieval, server):
    env, _ = retrieval
    server['status'] = 503
    result = asyncio.run(env.bind({}).invoke({'method': 'map_embeddings', 'arguments': {'text': 'object'}}, context=None))
    assert result['status'] == 'error' and result['code'] == 'embedding_execution_error'
    assert 'embedding_ref' not in result and len(server['bodies']) == 1


def test_fixed_table_contract_checked_even_with_matching_dimensions(retrieval):
    env, _ = retrieval
    async def run():
        encoded = await env.bind({}).invoke({'method': 'map_embeddings', 'arguments': {'text': 'object'}}, context=None)
        fixed = {**env.operator_settings['search_vectors']['arguments'], 'encoder_id': '0' * 64}
        store = LocalObjectStore(fixed['object_directory'])
        value = json.loads(ObjectRef(**encoded['embedding_ref']).read(max_bytes=4096))
        value['encoder_id'] = fixed['encoder_id']
        ref = store.put(canonical(value).encode()).to_dict()
        from demiflow.lance.search_api import search_vectors
        with pytest.raises(ValueError, match='table encoding contract'):
            search_vectors(ref, **fixed)
    asyncio.run(run())
