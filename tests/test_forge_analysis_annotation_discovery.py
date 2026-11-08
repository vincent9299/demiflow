"""Persistent definition discovery with stable cuts and finite byte pages."""
from demiflow.workbench.analysis import contracts as c
from test_forge_analysis_records import controller,source_manifest,question,save


def target(source):return dict(source=source['digest'],case_key='18446744073709551615',variant=None,field=None)


def test_old_unsaved_view_answers_are_discoverable_after_all_local_descriptors_are_gone(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=source_manifest())['result']
        old=question();changed={**old,'title':'A different immutable question'}
        save(client,source,definition=old,request_id='old')
        save(client,source,definition=changed,request_id='changed',value='no')
        save(client,source,definition=old,request_id='old-edit',revision=1,value='no')
        first=client.call('annotation_definitions',target=target(source),limit=1)['result']
        assert first['items'][0]['definition']==old and first['items'][0]['head']['revision']==2
        assert first['annotation_watermark']==3 and first['next_before']==3
        later={**old,'title':'Saved after the fixed history cut'}
        save(client,source,definition=later,request_id='later')
        save(client,source,definition=changed,request_id='changed-later',revision=1,value='yes')
        second=client.call('annotation_definitions',target=target(source),limit=1,before=first['next_before'],watermark=3,
            controls=dict(source_valid=False))['result']
        assert second['items'][0]['definition']==changed and second['items'][0]['head']['value']=='no'
        assert second['items'][0]['head']['revision']==1 and second['next_before'] is None
        assert client.call('annotation_definitions',target=target(source),principal='different')['result']['items']==[]
        assert client.call('annotation_definitions',target=target(source),project='w'+'f'*32)['code']=='not_found'


def test_definition_pages_stop_before_byte_limit_without_truncating_questions(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=source_manifest())['result']
        definitions=[]
        for index in range(8):
            definition={**question(),'id':'wide'+str(index),'options':[dict(id='o'+str(i),label='界'*75) for i in range(32)]}
            definitions.append(definition)
            save(client,source,definition=definition,request_id='q'+str(index),value='o0')
        first=client.call('annotation_definitions',target=target(source),limit=8)['result']
        assert 1<=len(first['items'])<8 and first['next_before'] is not None
        assert len(c.encoded(first))<65536
        seen=[item['definition_digest'] for item in first['items']];cursor=first['next_before']
        while cursor is not None:
            page=client.call('annotation_definitions',target=target(source),limit=8,before=cursor,watermark=first['annotation_watermark'])['result']
            seen.extend(item['definition_digest'] for item in page['items']);cursor=page['next_before']
        assert len(seen)==len(set(seen))==8
        full=client.call('annotations',target=target(source),definition=c.digest(definitions[0]))['result']
        assert full['definition']==definitions[0]
