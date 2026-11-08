"""Real metadata SQLite/authority suspension; source-query reply is staged.

Tests cursor identity, query scope, annotation watermark and grant fencing.
Native scanner semantics have separate owned real-Lance fixture coverage.
"""
from copy import deepcopy

from demiflow.workbench.analysis import contracts as c
from test_forge_analysis_records import controller,source_manifest,view,save,question
from test_forge_analysis_deferred_metadata import origin,reply


def body(source):
    descriptor=view(source)
    return dict(digest=source['digest'],view=dict(id=None,revision=0,descriptor=descriptor),
        query=dict(kind='cases',columns=['input','output'],filters=None,grouping=[],sort=[],limit=1))


def sample(source,offset=0,*,scope_digest='e'*64):
    return dict(kind='cases',source_snapshot=source['digest'],scope_digest=scope_digest,rows=[dict(_rowid=str(offset),input='one',output='two')],
        columns=['input','output'],offset=offset,next_offset=1 if offset==0 else None,matching_in_scope=2,groups=[],other_groups=0,
        coverage=dict(kind='full_source',requested_scope='full',rows_scanned=2,total_rows=2,complete=True))


def begin(client,source,payload,key='page'):
    client.call('metadata_begin',key=key,name='case-page',payload=payload)
    value=reply(client,key,dict(principal='owner'))
    for _ in range(32):
        if value.get('code') or value['result']['done'] or value['result']['pending']=='source-query':return value
        assert not value['result']['in_transaction'] and not value['result']['locked']
        assert value['result']['pending']=='permission'
        value=reply(client,key,dict(allowed=True))
    raise AssertionError('bounded route did not reach its source query')


def finish(client,key,value):
    for _ in range(32):
        if value.get('code') or value['result']['done']:return value
        assert value['result']['pending']=='permission'
        value=reply(client,key,dict(allowed=True))
    raise AssertionError('bounded route did not complete')


def test_cursor_binds_source_view_full_query_and_scope_population(tmp_path):
    with controller(tmp_path/'db') as client:
        source=origin(client);payload=body(source)
        value=begin(client,source,payload)
        assert value['result']['payload']['query']['scope']=='full'
        first=finish(client,'page',reply(client,'page',sample(source)))['result']['value']
        client.call('metadata_dispose',key='page')
        cursor=first['next_cursor'];assert cursor['annotation_watermark']==0 and cursor['offset']==1
        for key,changed in [('scope',dict(payload,query={**payload['query'],'scope':'sample'})),
            ('fields',dict(payload,view={**payload['view'],'descriptor':{**payload['view']['descriptor'],'fields':[{**payload['view']['descriptor']['fields'][0],'format':'md'},payload['view']['descriptor']['fields'][1]]}}))]:
            result=begin(client,source,dict(changed,cursor=cursor),key=key)
            assert result['code']=='conflict'
        value=begin(client,source,dict(payload,cursor=cursor),key='next')
        assert value['result']['payload']['query']['offset']==1
        assert reply(client,'next',sample(source,1,scope_digest='f'*64))['code']=='conflict'
        value=begin(client,source,dict(payload,cursor=cursor),key='same')
        next_page=finish(client,'same',reply(client,'same',sample(source,1)))['result']['value']
        assert next_page['next_cursor'] is None and next_page['query_digest']==first['query_digest']


def test_saved_view_revision_and_annotation_cut_remain_explicit_between_pages(tmp_path):
    with controller(tmp_path/'db') as client:
        source=origin(client);payload=body(source)
        saved=client.call('save_view',request_id='view1',view_id=None,expected_revision=0,descriptor=payload['view']['descriptor'])['result']
        payload['view']=dict(id=saved['id'],revision=1,descriptor=payload['view']['descriptor'])
        value=begin(client,source,payload)
        first=finish(client,'page',reply(client,'page',sample(source)))['result']['value']
        assert save(client,source)['result']['annotation_watermark']==1
        updated=deepcopy(payload['view']['descriptor']);updated['questions'][0]['title']='new definition'
        client.call('save_view',request_id='view2',view_id=saved['id'],expected_revision=1,descriptor=updated)
        value=begin(client,source,dict(payload,cursor=first['next_cursor']),key='past')
        result=finish(client,'past',reply(client,'past',sample(source,1)))['result']['value']
        assert result['annotation_watermark']==0 and result['view_digest']==first['view_digest']
        changed=deepcopy(payload);changed['view']['revision']=2;changed['view']['descriptor']=updated;changed['cursor']=first['next_cursor']
        assert begin(client,source,changed,key='new-view')['code']=='conflict'


def test_heads_respect_immutable_definition_and_watermark_without_losing_current_revision(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=source_manifest())['result'];definition=c.digest(question())
        target=dict(source=source['digest'],case_key='18446744073709551615',variant=None,field=None)
        empty=client.call('annotation_heads',target=target,definitions=[definition])['result']
        assert empty['items'][0]['head'] is None and empty['annotation_watermark']==0
        save(client,source);save(client,source,request_id='new',revision=1,value='no')
        old=client.call('annotation_heads',target=target,definitions=[definition],watermark=1)['result']
        current=client.call('annotation_heads',target=target,definitions=[definition])['result']
        assert old['items'][0]['head']['value']=='yes' and old['items'][0]['head']['revision']==1
        assert current['items'][0]['head']['value']=='no' and current['items'][0]['head']['revision']==2
        assert client.call('annotation_heads',target=target,definitions=[definition],watermark=3)['code']=='invalid_request'


def test_review_filter_crosses_real_metadata_authority_without_lock_and_retains_issued_cut(tmp_path):
    with controller(tmp_path/'db') as client:
        source=origin(client);payload=body(source)
        target=dict(source=source['digest'],case_key='0',variant=None,field=None)
        client.call('save_annotations',request_id='one',target=target,
            entries=[dict(definition=question(),expected_revision=0,action='set',value='yes')],origin=dict(kind='unsaved_view',reference='draft'))
        payload['review']=dict(variant=None,field=None,definitions=[c.digest(question())],match='answered_any')
        pending=begin(client,source,payload)
        assert not pending['result']['in_transaction'] and not pending['result']['locked']
        selection=pending['result']['payload']['query']['annotation_rows']
        assert selection['keys']==['0'] and selection['mode']=='include'
        client.call('save_annotations',request_id='late',target={**target,'case_key':'1'},
            entries=[dict(definition=question(),expected_revision=0,action='set',value='no')],origin=dict(kind='unsaved_view',reference='draft'))
        supplied={**sample(source),'annotation_selection_digest':c.digest(selection)}
        completed=finish(client,'page',reply(client,'page',supplied))
        assert 'result' in completed,completed
        result=completed['result']['value']
        assert result['annotation_watermark']==result['review']['annotation_watermark']==1
        assert result['review']['filter']==payload['review']


def test_late_permission_denial_or_foreign_source_never_returns_a_case_page(tmp_path):
    with controller(tmp_path/'db') as client:
        source=origin(client);payload=body(source)
        value=begin(client,source,payload)
        pending=reply(client,'page',sample(source))
        assert pending['result']['pending']=='permission'
        assert reply(client,'page',dict(allowed=False))['code']=='operation_rejected'
        value=begin(client,source,payload,key='foreign')
        foreign={**sample(source),'source_snapshot':'f'*64}
        assert reply(client,'foreign',foreign)['code']=='conflict'
