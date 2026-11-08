"""Registered group identity over actual SQLite/document/output proofs.

The dedicated record controller uses an explicit source-authority double.
Notebook output witnesses are synthetic registered artifacts, not OCI/B1.
The physical-source test reads a real private sealed Lance row-ID index.
"""
from contextlib import contextmanager
import copy
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import case_groups
from demiflow.workbench.analysis import dependencies
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis import presentations
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.sources import Sources
from demiflow.forge.schema import ForgeError
from test_forge_analysis_records import controller, question
from test_forge_outputs_media import OBJECT, PROJECT, registered_database, witness_and_raw


def manifest(binding='a'*64):
    names=['input','reference','base_image','base_text','base_score','check_image','check_text','check_score','extra']
    return dict(kind='fixed_lance',binding=binding,rows=2,retention='external',
        case_key=dict(kind='row_id',field=None),schema=[dict(name=name,type='utf8',nullable=True) for name in names])


def descriptor(source):
    fields=[]
    for name,role,fmt in [('input','input','dialogue'),('reference','reference','text'),
            ('base_image','output','image'),('base_text','output','md'),('base_score','evaluation','json'),
            ('check_image','output','image'),('check_text','output','md'),('check_score','evaluation','json'),
            ('extra','output','text')]:
        fields.append(dict(id=name,field=name,label=name,role=role,format=fmt))
    return dict(version=2,source_snapshot=source['digest'],case_key=source['case_key'],fields=fields,
        output_groups=[dict(id='base',label='Base',fields=['base_image','base_text','base_score']),
            dict(id='checkpoint',label='Checkpoint',fields=['check_image','check_text','check_score'])],
        grouping=[],filters=None,sort=[],layout='comparison',questions=[question()])


def basis(value,saved=None):
    return dict(id=None if saved is None else saved['id'],revision=0 if saved is None else saved['revision'],descriptor=value)


def target(source,*,variant=None,field=None):
    return dict(source=source['digest'],case_key='1',variant=variant,field=field)


def answer(client,actual,request,*,expected=0,value='yes',origin=None,action='set',revision=None,controls=None):
    entry=dict(definition=question(),expected_revision=expected,action=action)
    if action=='set':entry['value']=value
    if revision is not None:entry['revision']=revision
    return client.call('save_annotations',request_id=request,target=actual,entries=[entry],
        origin=origin or dict(kind='unsaved_view',reference='draft'),controls=controls or {})


def saved_view(client,value,request='view',saved=None):
    result=client.call('save_view',request_id=request,view_id=None if saved is None else saved['id'],
        expected_revision=0 if saved is None else saved['revision'],descriptor=value)
    assert 'result' in result,result
    return result['result']


def bound(client,source,value,saved=None):
    result=client.call('case_group_bindings',digest=source['digest'],case_key='1',view=basis(value,saved))
    assert 'result' in result,result
    return result['result']


def test_unsaved_groups_cannot_mint_answer_scope_and_saved_proof_is_read_only(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=manifest())['result'];value=descriptor(source)
        local=bound(client,source,value)
        assert local['view_digest']==c.digest(basis(value))
        assert all(not group['registered'] and group['target'] is None and group['variant'] is None for group in local['groups'])
        semantic=case_groups.definitions(value,source)[0][1];variant='cg'+c.digest(semantic)
        for index,actual in enumerate([target(source,variant=variant),target(source,variant='Base'),target(source,variant='cg'+'f'*64)]):
            assert 'error' in answer(client,actual,'unregistered'+str(index))
            assert 'error' in client.call('annotation_heads',target=actual,definitions=[c.digest(question())])
            assert 'error' in client.call('annotation_definitions',target=actual)
        saved=saved_view(client,value);result=bound(client,source,value,saved)
        assert len(result['groups'])==2 and all(group['registered'] for group in result['groups'])
        first=result['groups'][0]
        assert first['variant']==variant and first['target']==target(source,variant=variant)
        assert first['registration']==dict(kind='saved_view',reference=saved['id']+'@1',descriptor_sha256=c.digest(value))
        assert first['definition']['fields']==['base_image','base_score','base_text']
        assert not bound(client,source,value)['groups'][0]['target']['field']
        bad={**first['target'],'field':'base_text'}
        assert 'error' in answer(client,bad,'borrow-field')
        assert 'error' in client.call('case_group_bindings',digest=source['digest'],case_key='1',view=basis(value),variant=variant)
        assert 'error' in client.call('case_group_bindings',digest=source['digest'],case_key='01',variant=variant)
        # No arbitrary registration endpoint; exactly the view and its two facts.
        with sqlite3.connect(tmp_path/'db/records.sqlite') as db:
            assert db.execute('SELECT count(*) FROM documents').fetchone()[0]==3
            assert db.execute('SELECT count(*) FROM operation_receipts').fetchone()[0]==1
            assert db.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==0


def test_readable_view_name_lists_and_reopens_without_changing_group_or_old_answers(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=manifest())['result']
        value=descriptor(source);value['name']='模型输出比较 · 第一版'
        saved=saved_view(client,value)
        group=bound(client,source,value,saved)['groups'][0]
        answer(client,group['target'],'original-answer',origin=dict(kind='saved_view',reference=saved['id']+'@1'))
        listed=client.call('list_documents',kind='case_view')['result']['items']
        assert listed==[dict(id=saved['id'],revision=1,name=value['name'])]
        renamed={**value,'name':'模型输出比较 · 新名称'}
        updated=saved_view(client,renamed,'renamed',saved)
        assert bound(client,source,renamed,updated)['groups'][0]['variant']==group['variant']
        assert client.call('view',view_id=saved['id'],revision=1)['result']['document']['name']==value['name']
        assert client.call('view',view_id=saved['id'])['result']['document']['name']==renamed['name']
        history=client.call('annotations',target=group['target'],definition=c.digest(question()))['result']
        assert history['items'][0]['origin']['reference']==saved['id']+'@1'
        for bad in ['x'*257,'多行\n名称','']:
            assert 'error' in client.call('save_view',request_id='invalid-'+str(len(bad)),view_id=saved['id'],
                expected_revision=2,descriptor={**renamed,'name':bad})


def test_case_field_two_groups_have_separate_cas_heads_history_receipts_and_principals(tmp_path):
    directory=tmp_path/'db'
    with controller(directory) as client:
        source=client.call('register_source',manifest=manifest())['result'];value=descriptor(source)
        saved=saved_view(client,value);groups=bound(client,source,value,saved)['groups']
        targets=[target(source),target(source,field='base_text'),groups[0]['target'],groups[1]['target']]
        origin=dict(kind='saved_view',reference=saved['id']+'@1')
        for index,actual in enumerate(targets):
            result=answer(client,actual,'answer'+str(index),value='yes' if index%2 else 'no',origin=origin)['result']
            assert result['target']==actual and result['heads'][0]['revision']==1
        assert answer(client,targets[2],'race',value='no')['code']=='conflict'
        assert client.call('receipt',request_id='race')['code']=='not_found'
        answer(client,targets[2],'clear',expected=1,action='clear')
        answer(client,targets[2],'revert',expected=2,action='revert',revision=1)
        assert client.call('receipt',request_id='answer2')['result']['target']==targets[2]
        assert answer(client,targets[2],'answer2',value='no',origin=origin)['result']['replayed']
        for index,actual in enumerate(targets):
            heads=client.call('annotation_heads',target=actual,definitions=[c.digest(question())])['result']
            assert heads['items'][0]['head']['revision']==(3 if index==2 else 1)
            assert heads['items'][0]['head']['value']==('yes' if index%2 else 'no')
        assert client.call('annotation_heads',target=targets[2],definitions=[c.digest(question())],principal='other')['result']['items'][0]['head'] is None
        assert client.call('receipt',request_id='answer2',principal='other')['code']=='not_found'
        assert 'error' in client.call('annotation_heads',target=targets[2],definitions=[c.digest(question())],project='w'+'1'*32)
        assert answer(client,targets[2],'gone',expected=3,controls=dict(source_valid=False))['code']=='conflict'
        history=client.call('annotations',target=targets[2],definition=c.digest(question()),controls=dict(source_valid=False))['result']
        assert [item['state'] for item in history['items']]==['answered','cleared','answered']
        assert client.call('annotation_heads',target=targets[2],definitions=[c.digest(question())],controls=dict(permissions=[]))['code']=='operation_rejected'
        assert answer(client,targets[2],'revoked',expected=3,controls=dict(revoke_at_check=2))['code']=='operation_rejected'
    with controller(directory) as client:
        assert client.call('receipt',request_id='answer2')['result']['target']==targets[2]
        assert client.call('annotations',target=targets[2],definition=c.digest(question()))['result']['items'][0]['revision']==3


def test_group_renaming_keeps_original_proof_membership_and_source_changes_never_reuse_answers(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=manifest())['result'];value=descriptor(source)
        first=saved_view(client,value);old=bound(client,source,value,first)['groups'][0]
        answer(client,old['target'],'old-answer')
        rename=copy.deepcopy(value);rename['output_groups'][0]['label']='Renamed'
        rename['output_groups'][0]['fields'].reverse();rename['fields'].reverse()
        for field in rename['fields']:
            if field['id']=='base_text':field.update(id='answer_new',label='Answer',format='text')
        rename['output_groups'][0]['fields']=['answer_new' if name=='base_text' else name for name in rename['output_groups'][0]['fields']]
        second=saved_view(client,rename,'rename',first);same=bound(client,source,rename,second)['groups'][0]
        assert same['variant']==old['variant'] and same['label']=='Renamed' and same['registration']==old['registration']
        change=copy.deepcopy(rename);change['output_groups'][0]['fields'].remove('base_score')
        third=saved_view(client,change,'membership',second);new=bound(client,source,change,third)['groups'][0]
        assert new['variant']!=old['variant']
        assert client.call('annotation_heads',target=new['target'],definitions=[c.digest(question())])['result']['items'][0]['head'] is None
        # An unrelated newer view is not a legitimate immutable origin for old group answers.
        assert answer(client,old['target'],'wrong-origin',expected=1,origin=dict(kind='saved_view',reference=third['id']+'@3'))['code']=='conflict'
        history=client.call('case_group_bindings',digest=source['digest'],case_key='1',variant=old['variant'])['result']
        assert history['groups'][0]['label']=='Base' and history['groups'][0]['definition']==old['definition']
        assert client.call('annotations',target=old['target'],definition=c.digest(question()))['result']['items'][0]['value']=='yes'
        other=client.call('register_source',manifest=manifest('b'*64))['result'];other_view=descriptor(other)
        assert not bound(client,other,other_view)['groups'][0]['registered']
        assert 'error' in client.call('case_group_bindings',digest=other['digest'],case_key='1',variant=old['variant'])
        other_saved=saved_view(client,other_view,'other-view');other_group=bound(client,other,other_view,other_saved)['groups'][0]
        assert other_group['variant']!=old['variant']
        assert client.call('annotation_heads',target=other_group['target'],definitions=[c.digest(question())])['result']['items'][0]['head'] is None
        with sqlite3.connect(tmp_path/'db/records.sqlite') as db:
            assert db.execute("SELECT count(*) FROM documents WHERE kind='case_output_group'").fetchone()[0]==5
            assert db.execute("SELECT max(revision) FROM documents WHERE kind='case_output_group'").fetchone()[0]==1


@pytest.mark.parametrize('change', ['v1','bool','empty','duplicate','input','evaluation-only','unknown','anchor','variant'])
def test_group_descriptor_rejects_ambiguous_or_invalid_members(change):
    source=c.source_snapshot(manifest());value=descriptor(source)
    if change=='v1':value['version']=1
    elif change=='bool':value['version']=True
    elif change=='empty':value['output_groups']=[]
    elif change=='duplicate':value['output_groups'][1]['fields'].append('base_text')
    elif change=='input':value['output_groups'][0]['fields'].append('input')
    elif change=='evaluation-only':value['output_groups'][0]['fields']=['base_score']
    elif change=='unknown':value['output_groups'][0]['fields']=['missing']
    elif change=='anchor':value['output_groups'][0]['target_field']='base_text'
    elif change=='variant':value['variant_key']='base_text'
    with pytest.raises(ForgeError):c.case_view(value,source)


def test_group_registration_and_view_receipt_rollback_together_on_late_revoke(tmp_path):
    with controller(tmp_path/'db') as client:
        source=client.call('register_source',manifest=manifest())['result'];value=descriptor(source)
        denied=client.call('save_view',request_id='denied',view_id=None,expected_revision=0,descriptor=value,controls=dict(revoke_at_check=2))
        assert denied['code']=='operation_rejected'
        assert client.call('receipt',request_id='denied')['code']=='not_found'
        assert not bound(client,source,value)['groups'][0]['registered']
        assert client.call('list_documents',kind='case_view')['result']['items']==[]


@contextmanager
def notebook_store():
    source=c.source_snapshot(manifest());value=descriptor(source)
    offered,raw=witness_and_raw([dict(output_type='display_data',metadata={},data={presentations.CASE_MIME:value})])
    records,digest=registered_database(raw,offered);db=records.database
    @contextmanager
    def transaction(**kwargs):
        db.connection.execute('BEGIN IMMEDIATE')
        try:yield db.connection;db.connection.commit()
        except BaseException:db.connection.rollback();raise
    db.transaction=transaction
    access=Access(PROJECT,'owner',lambda *args:True)
    records.register_source(access,manifest(),verify=lambda *args:True)
    db.connection.execute('UPDATE analysis_sessions SET sources=?',(c.encoded(dict(data=source['digest'])),));db.connection.commit()
    witness=output_reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,expected_binding=digest,dependency_resolver=dependencies.resolve)
    try:yield records,access,source,value,witness,raw
    finally:db.connection.close()


def test_admitted_notebook_representation_registers_groups_and_historical_proof_survives_unavailable_output():
    with notebook_store() as (records,access,source,value,witness,raw):
        page=output_reader.project_document(raw,witness)
        assert len(page['items'][0]['representations'][0]['value']['output_groups'])==2
        assert not records.case_group_bindings(access,digest=source['digest'],case_key='1',view=basis(value))['groups'][0]['registered']
        forged=copy.deepcopy(page);forged['items'][0]['representations'][0]['value']['output_groups'][0]['fields'].pop()
        with pytest.raises(ForgeError):presentations.register(records,access,witness=witness,projection=forged)
        result=presentations.register(records,access,witness=witness,projection=page)
        assert presentations.register(records,access,witness=witness,projection=page)==result
        origin=result['items'][0]['representations'][0]['origin']
        group=records.case_group_bindings(access,digest=source['digest'],case_key='1',view=basis(value))['groups'][0]
        assert group['registration']['kind']=='notebook_output' and group['registration']['reference']==origin['reference']
        saved=records.save_annotations(access,request_id='notebook-group',target=group['target'],
            entries=[dict(definition=question(),expected_revision=0,action='set',value='yes')],origin=origin,verify=lambda *args:True)
        assert saved['target']==group['target']
        db=records.database.connection;db.execute("UPDATE analysis_objects SET state='unavailable'");db.commit()
        assert records.case_group_bindings(access,digest=source['digest'],case_key='1',variant=group['variant'])['groups'][0]['definition']==group['definition']
        assert records.annotations(access,target=group['target'],definition=c.digest(question()))['items'][0]['origin']==origin
        assert db.execute("SELECT count(*) FROM documents WHERE kind='case_output_group'").fetchone()[0]==2


def test_registered_group_uses_actual_private_physical_row_index_without_reentrant_lock(tmp_path):
    from demiflow.workbench.analysis import case_pages
    from demiflow.workbench.analysis import case_query
    from test_forge_analysis_private_case_runtime import sealed
    config,path=sealed(tmp_path,v2=True);source=config['witness']['source'];project=config['witness']['project']
    offered,raw=witness_and_raw();records,_=registered_database(raw,offered);db=records.database
    @contextmanager
    def transaction(**kwargs):
        db.connection.execute('BEGIN IMMEDIATE')
        try:yield db.connection;db.connection.commit()
        except BaseException:db.connection.rollback();raise
    db.transaction=transaction
    access=Access(project,'owner',lambda *args:True);sources=Sources(records,tmp_path/'state')
    authority=SimpleNamespace(access=lambda:access)
    artifact=c.decode((path/'manifest.json').read_bytes())
    seed={key:artifact[key] for key in ('project','object_id','generation','retention','execution_id','parents','source_binding')}
    db.connection.execute('INSERT INTO analysis_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (project,path.name,'p'+'c'*32,1,seed['execution_id'],'ready',c.encoded(seed),artifact['payload']['logical_bytes'],4,
            source['binding'],c.encoded(artifact),None,None));db.connection.commit()
    handle=dict(id=path.name,generation=1,digest=source['binding'])
    registered=sources.private_source(access,handle);assert registered==source
    value=dict(version=2,source_snapshot=source['digest'],case_key=source['case_key'],
        fields=[dict(id=name,field=name,label=name,role='output',format='text') for name in ['input','score']],
        output_groups=[dict(id='result',label='Result',fields=['input','score'])],grouping=[],filters=None,sort=[],layout='card',questions=[question()])
    try:
        saved=records.save_view(access,request_id='native-view',view_id=None,expected_revision=0,descriptor=value,
            verify=lambda source,target=None:sources.verify(authority,source,target))
        group=records.case_group_bindings(access,digest=source['digest'],case_key='1',view=basis(value,saved))['groups'][0]
        one=records.save_annotations(access,request_id='physical-row',target=group['target'],entries=[dict(definition=question(),expected_revision=0,action='set',value='yes')],
            origin=dict(kind='saved_view',reference=saved['id']+'@1'),verify=lambda source,target=None:sources.verify(authority,source,target))
        assert one['heads'][0]['revision']==1
        assert sources.verify(authority,source,{**group['target'],'case_key':'18446744073709551615'}) is False
        assert records.annotations(access,target=group['target'],definition=c.digest(question()))['items'][0]['value']=='yes'
        for key in ['0','2']:
            records.save_annotations(access,request_id='whole-'+key,target=dict(source=source['digest'],case_key=key,variant=None,field=None),
                entries=[dict(definition=question(),expected_revision=0,action='set',value='yes')],origin=dict(kind='unsaved_view',reference='draft'),
                verify=lambda source,target=None:sources.verify(authority,source,target))
        shown=copy.deepcopy(value);shown.update(grouping=['input'],sort=[dict(field='score',direction='asc')])
        query=dict(kind='cases',columns=['input','score'],filters=None,grouping=shown['grouping'],sort=shown['sort'],scope='full',limit=1)
        payload=dict(digest=source['digest'],view=basis(shown),query=query,
            review=dict(variant=None,field=None,definitions=[c.digest(question())],match='answered_any'))
        context=case_pages.prepare(records,authority,source,payload)
        first=case_pages.finish(records,authority,source,context,case_query.read_private(source,path,context['query']))
        assert [row['_rowid'] for row in first['rows']]==['2'] and first['matching_in_scope']==2
        assert {item['values']['input'] for item in first['groups']}=={'one','three'}
        assert first['coverage']['rows_scanned']==3 and first['coverage']['complete']
        cut=first['annotation_watermark'];assert cut==3
        records.save_annotations(access,request_id='late-whole',target=dict(source=source['digest'],case_key='1',variant=None,field=None),
            entries=[dict(definition=question(),expected_revision=0,action='set',value='yes')],origin=dict(kind='unsaved_view',reference='draft'),
            verify=lambda source,target=None:sources.verify(authority,source,target))
        next_context=case_pages.prepare(records,authority,source,{**payload,'cursor':first['next_cursor']})
        second=case_pages.finish(records,authority,source,next_context,case_query.read_private(source,path,next_context['query']))
        assert [row['_rowid'] for row in second['rows']]==['0'] and second['annotation_watermark']==cut and second['matching_in_scope']==2
        fresh_context=case_pages.prepare(records,authority,source,payload)
        fresh=case_pages.finish(records,authority,source,fresh_context,case_query.read_private(source,path,fresh_context['query']))
        assert [row['_rowid'] for row in fresh['rows']]==['1'] and fresh['matching_in_scope']==3
        assert fresh['review']['annotation_watermark']==fresh['annotation_watermark']==4
    finally:db.connection.close()
