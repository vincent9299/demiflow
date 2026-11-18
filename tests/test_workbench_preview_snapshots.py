from copy import deepcopy

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph.preview_snapshots import PreviewSnapshots, validate_request


def specimen(digest='first', text='def actual(row): return row'):
    basis = dict(entry='flow.py:run', params=None, digest=digest, dag_id='dag')
    detail = dict(availability='available', reason=None, source_basis='compiled_snapshot',
                  text=text, path='operators.py', sha256='source-digest', start_line=1,
                  end_line=1, language='python', truncated=False)
    value = dict(basis=basis, dag=dict(nodes=[dict(logical_node_id='logical', occurrence_id='occurrence')]),
                 _node_details={'occurrence': {'implementation': detail}})
    request = dict(basis=dict(basis, kind='preview'), logical_node_id='logical',
                   occurrence_id='occurrence', kind='implementation')
    return value, request


def test_snapshot_is_project_and_node_scoped_and_cannot_be_mutated_by_reader():
    cache = PreviewSnapshots()
    value, request = specimen()
    assert '_node_details' not in cache.put('/project-one', value)
    first = cache.read('/project-one', **request)
    assert first['text'].startswith('def actual')
    first['text'] = 'changed'
    first['basis']['entry'] = 'different'
    assert cache.read('/project-one', **request)['text'].startswith('def actual')
    with pytest.raises(ForgeError, match='preview_expired'):
        cache.read('/project-two', **request)
    for field in ('logical_node_id', 'occurrence_id'):
        with pytest.raises(ForgeError, match='does not belong'):
            cache.read('/project-one', **dict(request, **{field: 'other'}))
    changed = deepcopy(request)
    changed['basis']['params'] = 'other.json'
    with pytest.raises(ForgeError, match='does not belong'):
        cache.read('/project-one', **changed)
    assert cache.read('/project-one', **dict(request, kind='prompt'))['availability'] == 'unavailable'


def test_eviction_and_expiry_request_reparse_without_current_source_fallback():
    now = [0]
    cache = PreviewSnapshots(max_entries=1, ttl=10, clock=lambda: now[0])
    first, request = specimen()
    cache.put('/root', first)
    second, other = specimen('second')
    cache.put('/root', second)
    with pytest.raises(ForgeError, match='preview_expired'):
        cache.read('/root', **request)
    now[0] = 10
    with pytest.raises(ForgeError, match='preview_expired'):
        cache.read('/root', **other)
    assert not cache._entries and cache._bytes == 0


@pytest.mark.parametrize('change', [dict(kind={}), dict(kind=[]), dict(kind='other'),
                                  dict(basis={}), dict(logical_node_id=False)])
def test_malformed_material_address_is_a_client_error(change):
    _, request = specimen()
    with pytest.raises(ForgeError) as caught:
        validate_request(dict(request, **change))
    assert caught.value.code == 'invalid_request'


def test_snapshot_byte_budget_rejects_without_retaining_large_material():
    cache = PreviewSnapshots(max_bytes=128)
    value, _ = specimen(text='x' * 256)
    with pytest.raises(ForgeError) as caught:
        cache.put('/root', value)
    assert caught.value.code == 'resource_limit'
    assert cache._bytes == 0 and not cache._entries


def test_pipeline_snapshot_obeys_same_project_digest_ttl_and_copy_boundary():
    now=[0];cache=PreviewSnapshots(ttl=10,clock=lambda:now[0])
    value, request=specimen()
    value['_pipeline_detail']=deepcopy(value['_node_details']['occurrence']['implementation'])
    public=cache.put('/one',value)
    assert '_pipeline_detail' not in public
    first=cache.read_pipeline('/one',basis=request['basis'])
    first['text']='tampered'
    assert cache.read_pipeline('/one',basis=request['basis'])['text'].startswith('def actual')
    with pytest.raises(ForgeError, match='preview_expired'):
        cache.read_pipeline('/two',basis=request['basis'])
    with pytest.raises(ForgeError, match='does not belong'):
        cache.read_pipeline('/one',basis=dict(request['basis'],dag_id='different'))
    now[0]=10
    with pytest.raises(ForgeError, match='preview_expired'):
        cache.read_pipeline('/one',basis=request['basis'])
