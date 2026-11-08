"""Server annotation predicates on actual SQLite; native composition in groups.

The record fixture explicitly doubles live source grants, never B1 execution.
"""
import copy
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import case_pages
from demiflow.workbench.analysis import case_review
from demiflow.workbench.analysis.records import Access
from demiflow.forge.schema import ForgeError
from test_forge_analysis_case_groups import notebook_store,basis,target
from test_forge_analysis_records import question


def put(records,access,source,key,request,*,field=None,variant=None,definition=None,value='yes',expected=0,action='set',revision=None):
    item=dict(definition=definition or question(),expected_revision=expected,action=action)
    if action=='set':item['value']=value
    if revision is not None:item['revision']=revision
    return records.save_annotations(access,request_id=request,target=dict(source=source['digest'],case_key=key,variant=variant,field=field),entries=[item],
        origin=dict(kind='unsaved_view',reference='draft'),verify=lambda *args:True)


def select(records,access,source,value,match='answered_any',*,field=None,variant=None,definitions=None,option=None,watermark=None):
    review=dict(variant=variant,field=field,definitions=definitions or [c.digest(question())],match=match)
    if option is not None:review['option']=option
    return case_review.selection(records,access,source,review,value,watermark=watermark)


def test_saved_predicates_isolate_target_definition_principal_and_snapshot_clear_revert():
    with notebook_store() as (records,access,source,value,*_):
        extra=dict(id='note',title='Note',type='text',required=False);value=copy.deepcopy(value);value['questions'].append(extra)
        saved=records.save_view(access,request_id='view',view_id=None,expected_revision=0,descriptor=value,verify=lambda *args:True)
        group=records.case_group_bindings(access,digest=source['digest'],case_key='0',view=basis(value,saved))['groups'][0]
        put(records,access,source,'0','case')
        put(records,access,source,'1','field',field='base_text')
        put(records,access,source,'1','group',variant=group['variant'])
        other=Access(access.project,'other',lambda *args:True);put(records,other,source,'1','other')
        rows,report=select(records,access,source,value);cut=report['annotation_watermark']
        assert rows['keys']==['0'] and rows['mode']=='include'
        assert select(records,access,source,value,'unanswered')[0]['keys']==['0']
        assert select(records,access,source,value,'unanswered')[0]['mode']=='exclude'
        assert select(records,access,source,value,field='base_text')[0]['keys']==['1']
        assert select(records,access,source,value,variant=group['variant'])[0]['keys']==['1']
        assert select(records,other,source,value)[0]['keys']==['1']
        definitions=[c.digest(question()),c.digest(extra)]
        assert select(records,access,source,value,'answered_all',definitions=definitions)[0]['keys']==[]
        put(records,access,source,'0','note',definition=extra,value='checked')
        assert select(records,access,source,value,'answered_all',definitions=definitions)[0]['keys']==['0']
        put(records,access,source,'0','clear',expected=1,action='clear')
        assert select(records,access,source,value)[0]['keys']==[]
        assert select(records,access,source,value,watermark=cut)[0]['keys']==['0']
        put(records,access,source,'0','revert',expected=2,action='revert',revision=1)
        assert select(records,access,source,value)[0]['keys']==['0']
        denied=Access(access.project,access.principal,lambda *args:False)
        with pytest.raises(ForgeError):select(records,denied,source,value)
        with pytest.raises(ForgeError):select(records,access,source,value,variant='Base')
        with pytest.raises(ForgeError):select(records,access,source,value,watermark=2**63-1)


def test_options_are_explicit_exact_choice_or_multi_definitions_and_never_label_inferred():
    with notebook_store() as (records,access,source,value,*_):
        multi=dict(id='tags',title='Review tags',type='multi',required=False,options=[dict(id='review',label='待复核'),dict(id='accepted',label='Accepted')])
        value=copy.deepcopy(value);value['questions'].append(multi)
        put(records,access,source,'0','yes');put(records,access,source,'1','no',value='no')
        put(records,access,source,'1','multi',definition=multi,value=['review'])
        exact=dict(definition=c.digest(question()),value='yes')
        assert select(records,access,source,value,'option',option=exact)[0]['keys']==['0']
        option=dict(definition=c.digest(multi),value='review')
        assert select(records,access,source,value,'option',definitions=[c.digest(multi)],option=option)[0]['keys']==['1']
        for bad in [dict(definition=c.digest(multi),value='待复核'),dict(definition='f'*64,value='review')]:
            with pytest.raises(ForgeError):select(records,access,source,value,'option',definitions=[c.digest(multi)],option=bad)
        with pytest.raises(ForgeError):select(records,access,source,value,option=exact)
        with pytest.raises(ForgeError):select(records,access,source,value,definitions=[c.digest(question())]*2)
        with pytest.raises(ForgeError):select(records,access,source,value,definitions=['e'*64])


def page_payload(source,value,review):
    return dict(digest=source['digest'],view=basis(value),review=review,
        query=dict(kind='cases',columns=[field['field'] for field in value['fields']],filters=None,grouping=[],sort=[],scope='full',limit=1))


def test_cursor_captures_original_cut_and_principal_and_reader_must_prove_selection():
    with notebook_store() as (records,access,source,value,*_):
        review=dict(variant=None,field=None,definitions=[c.digest(question())],match='answered_any')
        payload=page_payload(source,value,review);authority=SimpleNamespace(access=lambda:access)
        put(records,access,source,'0','one');context=case_pages.prepare(records,authority,source,payload)
        selection=context['query']['annotation_rows'];cut=selection['annotation_watermark']
        put(records,access,source,'1','late')
        result=dict(kind='cases',source_snapshot=source['digest'],columns=payload['query']['columns'],rows=[],offset=0,next_offset=1,
            matching_in_scope=1,scope_digest='f'*64,coverage=dict(requested_scope='full',complete=True),annotation_selection_digest=c.digest(selection))
        first=case_pages.finish(records,authority,source,context,result)
        assert first['annotation_watermark']==cut==1 and first['review']['annotation_watermark']==1
        next_context=case_pages.prepare(records,authority,source,{**payload,'cursor':first['next_cursor']})
        assert next_context['query']['annotation_rows']['keys']==['0']
        assert case_pages.prepare(records,authority,source,payload)['query']['annotation_rows']['keys']==['0','1']
        with pytest.raises(ForgeError):case_pages.finish(records,authority,source,context,{**result,'annotation_selection_digest':'e'*64})
        foreign=SimpleNamespace(access=lambda:Access(access.project,'other',lambda *args:True))
        with pytest.raises(ForgeError):case_pages.prepare(records,foreign,source,{**payload,'cursor':first['next_cursor']})
        with pytest.raises(ForgeError):case_pages.prepare(records,authority,source,{**payload,'review':{**review,'match':'unanswered'},'cursor':first['next_cursor']})
        with pytest.raises(ForgeError):case_pages.prepare(records,authority,source,{**payload,'query':{**payload['query'],'annotation_rows':selection}})
        with pytest.raises(ForgeError):case_review.validate({**review,'principal':'other'},source,value)


def test_selection_enforces_unique_canonical_uint64_keys_and_selection_bounds():
    good=dict(format=case_review.FORMAT,mode='include',keys=['0','18446744073709551615'],predicate_digest='a'*64,annotation_watermark=0)
    assert case_review.rows_specification(good)==good
    for keys in [['01'],['18446744073709551616'],['1','1'],['10','2'],[1],['1']*1001]:
        with pytest.raises(ForgeError):case_review.rows_specification({**good,'keys':keys})


def test_more_than_1000_saved_matching_keys_refuses_without_silently_truncating(monkeypatch):
    with notebook_store() as (records,access,source,value,*_):
        put(records,access,source,'0','seed')
        db=records.database.connection
        db.execute('''INSERT INTO annotation_events(project,source,case_key,variant,field,principal,definition,revision,state,value,origin,time)
            SELECT project,source,'1','','',principal,definition,1,state,value,origin,time FROM annotation_events WHERE case_key='0' ''')
        # Controlled metadata fixture: actual event ledger, no per-case writes or hidden native scan.
        template=db.execute("SELECT project,source,variant,field,principal,definition,revision,state,value,origin,time FROM annotation_events WHERE case_key='0'").fetchone()
        db.executemany('INSERT INTO annotation_events(project,source,variant,field,principal,definition,revision,state,value,origin,time,case_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            [(*template,str(index)) for index in range(2,1001)]);db.commit()
        with pytest.raises(ForgeError) as error:select(records,access,source,value)
        assert error.value.code=='resource_limit'
        assert db.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==1001
        monkeypatch.setattr(case_review,'SQL_TICKS',1)
        with pytest.raises(ForgeError,match='SQL work') as work_error:select(records,access,source,value)
        assert work_error.value.code=='resource_limit'
        assert db.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==1001
