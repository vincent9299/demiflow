"""Actual SQLite event/CAS/receipt/basis composition; explicit source grant fake.

Media manifest uses a real private PNG probe, not a capture/runtime/B1 claim.
"""
from contextlib import contextmanager
import copy

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.records import Access
from demiflow.forge.schema import ForgeError
from test_forge_analysis_presentations import store
from test_forge_analysis_registered_media import fixture
from test_forge_analysis_records import question


@contextmanager
def records_with_media():
    with store() as (records,access,witness,raw,source,descriptor):
        data,seed,payload,files=fixture();seed.update(project=access.project,source_binding=source['digest'])
        payload['source'].update(source_snapshot=source['digest'],case_key='1',field='input')
        manifest=candidate_manifest(seed,payload,files);digest=c.digest(manifest)
        db=records.database.connection
        db.execute('INSERT INTO analysis_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (access.project,seed['object_id'],'p'+'a'*32,1,'explicit-capture-request','ready',c.encoded(seed),len(data),4,digest,c.encoded(manifest),None,None));db.commit()
        ref=dict(source_snapshot=source['digest'],case_key='1',field='input',artifact_id=seed['object_id'],artifact_generation=1,
            expected_binding=digest,expected_sha256=payload['sha256'],mime=payload['mime'])
        target=dict(source=source['digest'],case_key='1',variant=None,field=None)
        yield records,access,target,ref


MISSING=object()


def save(records,access,target,ref,*,request='answer',expected=0,action='set',basis=MISSING,definition=None,value='yes',revision=None,allowed=True):
    item=dict(definition=definition or question(),expected_revision=expected,action=action)
    if action=='set':item['value']=value
    if basis is not MISSING:item['media_basis']=basis
    if revision is not None:item['revision']=revision
    return records.save_annotations(access,request_id=request,target=target,entries=[item],origin=dict(kind='unsaved_view',reference='draft'),
        verify=lambda *args:True,verify_media=lambda source,actual,witnesses:allowed and all(w['resource_binding']=='c'*64 for w in witnesses))


def test_basis_persists_with_answer_head_receipt_history_clear_and_revert_without_write_grant():
    with records_with_media() as (records,access,target,ref):
        limited=Access(access.project,access.principal,lambda project,principal,permission:permission in {'read','case_annotate'})
        one=save(records,limited,target,ref,basis=[ref])
        assert one['heads'][0]['media_basis']==[ref]
        replay=save(records,limited,target,ref,basis=[ref]);assert replay['replayed'] and replay['heads']==one['heads']
        assert records.receipt(limited,'answer')['heads']==one['heads']
        # An older client omitting the optional field preserves the server's
        # known context; only an explicit [] can deliberately clear evidence.
        save(records,limited,target,ref,request='edit',expected=1,value='no')
        save(records,limited,target,ref,request='clear',expected=2,action='clear')
        save(records,limited,target,ref,request='restore',expected=3,action='revert',revision=1)
        heads=records.annotation_heads(limited,target=target,definitions=[c.digest(question())])
        assert heads['items'][0]['head']['media_basis']==[ref] and heads['items'][0]['head']['value']=='yes'
        history=records.annotations(limited,target=target,definition=c.digest(question()))
        assert len(history['items'])==4 and all(item['media_basis']==[ref] for item in history['items'])
        discovered=records.annotation_definitions(limited,target=target)
        assert discovered['items'][0]['head']['media_basis']==[ref]
        save(records,limited,target,ref,request='explicit-none',expected=4,basis=[])
        assert records.annotation_heads(limited,target=target,definitions=[c.digest(question())])['items'][0]['head']['media_basis']==[]


def test_forged_changed_foreign_or_revoked_media_never_creates_answer_or_receipt():
    with records_with_media() as (records,access,target,ref):
        for index,(key,value) in enumerate([('source_snapshot','f'*64),('case_key','2'),('field','absent'),('artifact_generation',2),
                ('expected_binding','f'*64),('expected_sha256','e'*64),('mime','image/jpeg')]):
            with pytest.raises(ForgeError):save(records,access,target,ref,request='bad'+str(index),basis=[{**ref,key:value}])
        with pytest.raises(ForgeError):save(records,access,target,ref,request='duplicate',basis=[ref,ref])
        with pytest.raises(ForgeError):save(records,access,target,ref,request='revoked-root',basis=[ref],allowed=False)
        assert records.database.connection.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==0
        with pytest.raises(ForgeError):records.receipt(access,'revoked-root')
        save(records,access,target,ref,basis=[ref])
        with pytest.raises(ForgeError):save(records,access,target,ref,request='race',basis=[ref])
        with pytest.raises(ForgeError):save(records,access,target,ref,basis=[])
        with pytest.raises(ForgeError):save(records,access,target,ref,request='forged-clear',expected=1,action='clear',basis=[])
        with pytest.raises(ForgeError):save(records,access,target,ref,request='clear-after-revoke',expected=1,action='clear',allowed=False)
        assert records.database.connection.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==1


def test_missing_old_basis_is_not_fabricated_and_wide_history_has_a_byte_bound():
    with records_with_media() as (records,access,target,ref):
        old=save(records,access,target,ref,request='text-only')
        assert 'media_basis' not in old['heads'][0]
        assert 'media_basis' not in records.annotations(access,target=target,definition=c.digest(question()))['items'][0]
        definition=dict(id='note',title='说明',type='text',required=False)
        for index in range(12):save(records,access,target,ref,request='wide'+str(index),expected=index,definition=definition,value='界'*2000,basis=[ref])
        page=records.annotations(access,target=target,definition=c.digest(definition),limit=20)
        assert len(c.encoded(page))<=56*1024 and page['next_before'] is not None
        older=records.annotations(access,target=target,definition=c.digest(definition),before=page['next_before'],watermark=page['annotation_watermark'])
        assert page['items'][-1]['sequence']>older['items'][0]['sequence']
        assert len(page['items'])+len(older['items'])==12


def test_ready_row_cannot_borrow_a_canonical_manifest_for_another_object_generation():
    with records_with_media() as (records,access,target,ref):
        db=records.database.connection
        row=db.execute('SELECT manifest FROM analysis_objects WHERE project=? AND id=?',(access.project,ref['artifact_id'])).fetchone()
        changed=c.decode(bytes(row[0]));changed['generation']=2;digest=c.digest(changed)
        db.execute('UPDATE analysis_objects SET manifest=?,manifest_digest=? WHERE project=? AND id=?',
            (c.encoded(changed),digest,access.project,ref['artifact_id']));db.commit()
        with pytest.raises(ForgeError):save(records,access,target,ref,basis=[dict(ref,expected_binding=digest)])
        assert db.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==0


def test_registered_group_basis_uses_shared_material_and_own_members_without_borrowing_another_group():
    with records_with_media() as (records,access,target,ref):
        source=records.source(access,target['source'])
        value=dict(version=2,source_snapshot=source['digest'],case_key=source['case_key'],
            fields=[dict(id=name,field=name,label=name,role=role,format=fmt) for name,role,fmt in [('input','input','image'),('output','output','text')]],
            output_groups=[dict(id='result',label='Result',fields=['output'])],grouping=[],filters=None,sort=[],layout='card',questions=[question()])
        one=records.save_view(access,request_id='view-one',view_id=None,expected_revision=0,descriptor=value,verify=lambda *args:True)
        view=dict(id=one['id'],revision=1,descriptor=value)
        result=records.case_group_bindings(access,digest=source['digest'],case_key='1',view=view)['groups'][0]
        def group_save(actual,request,expected,revision,action='set',basis=None):
            entry=dict(definition=question(),expected_revision=expected,action=action)
            if action=='set':entry.update(value='yes',media_basis=[ref] if basis is None else basis)
            return records.save_annotations(access,request_id=request,target=actual,entries=[entry],
                origin=dict(kind='saved_view',reference=one['id']+'@'+str(revision)),verify=lambda *args:True,verify_media=lambda *args:True)
        original=group_save(result['target'],'shared',0,1)
        changed=copy.deepcopy(value);changed['fields'][0]['role']='output';changed['output_groups'].append(dict(id='other',label='Other',fields=['input']))
        two=records.save_view(access,request_id='view-two',view_id=one['id'],expected_revision=1,descriptor=changed,verify=lambda *args:True)
        groups=records.case_group_bindings(access,digest=source['digest'],case_key='1',view=dict(id=two['id'],revision=2,descriptor=changed))['groups']
        assert groups[0]['variant']==result['variant']
        with pytest.raises(ForgeError):group_save(result['target'],'borrowed',1,2)
        assert group_save(groups[1]['target'],'own',0,2)['heads'][0]['media_basis']==[ref]
        # Clear keeps its original proven media context instead of rewriting it
        # to the current view's interpretation of a formerly shared component.
        cleared=group_save(result['target'],'clear-original',1,2,action='clear')
        assert cleared['heads'][0]['media_basis']==original['heads'][0]['media_basis']==[ref]
        assert records.annotations(access,target=result['target'],definition=c.digest(question()))['items'][0]['state']=='cleared'
