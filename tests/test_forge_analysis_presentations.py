"""Real SQLite metadata composition, private synthetic output/handle manifests.

This does not execute Jupyter/OCI, read Lance bytes, or claim B1 isolation.
"""
from contextlib import contextmanager
import copy
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import dependencies
from demiflow.workbench.analysis import presentations
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.operations import Scope
from demiflow.forge.schema import ForgeError
from test_forge_analysis_records import source_manifest,view,question
from test_forge_outputs_media import PROJECT,OBJECT,witness_and_raw,registered_database


@contextmanager
def store():
    source=c.source_snapshot(source_manifest());descriptor=view(source)
    offered,raw=witness_and_raw([dict(output_type='display_data',metadata={},
        data={presentations.CASE_MIME:descriptor})])
    records,digest=registered_database(raw,offered);db=records.database
    @contextmanager
    def transaction(**kwargs):
        db.connection.execute('BEGIN IMMEDIATE')
        try:yield db.connection;db.connection.commit()
        except BaseException:db.connection.rollback();raise
    db.transaction=transaction
    access=Access(PROJECT,'owner',lambda *args:True)
    records.register_source(access,{key:value for key,value in source.items() if key!='digest'},verify=lambda *args:True)
    db.connection.execute('UPDATE analysis_sessions SET sources=?',(c.encoded(dict(data=source['digest'])),));db.connection.commit()
    witness=output_reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,
        expected_binding=digest,dependency_resolver=dependencies.resolve)
    try:yield records,access,witness,raw,source,descriptor
    finally:db.connection.close()


def save(records,access,source,descriptor,origin,request='answer'):
    return records.save_annotations(access,request_id=request,
        target=dict(source=source['digest'],case_key='1',variant=None,field=None),
        entries=[dict(definition=descriptor['questions'][0],expected_revision=0,action='set',value='yes')],
        origin=origin,verify=lambda *args:True)


def test_trusted_output_registration_enables_exact_annotation_origin_and_history():
    with store() as (records,access,witness,raw,source,descriptor):
        page=output_reader.project_document(raw,witness)
        offered=page['items'][0]['representations'][0]['definition_witness']
        assert offered['registered'] is False
        with pytest.raises(ForgeError):save(records,access,source,descriptor,offered['origin'])
        registered=presentations.register(records,access,witness=witness,projection=page)
        proof=registered['items'][0]['representations'][0]['definition_witness']
        assert proof['registered'] and proof['origin']==offered['origin']
        assert page['items'][0]['representations'][0]['definition_witness']['registered'] is False
        assert presentations.register(records,access,witness=witness,projection=page)==registered
        result=save(records,access,source,descriptor,proof['origin'])
        reopened=Records(records.database)
        history=reopened.annotations(access,target=result['target'],definition=c.digest(question()))
        assert history['items'][0]['origin']==proof['origin']
        assert history['items'][0]['value']=='yes'
        assert records.database.connection.execute('SELECT count(*) FROM analysis_output_presentations').fetchone()[0]==1
        assert witness['provenance']['kernel_generation']=='generation'


def test_changed_definition_source_or_forged_witness_cannot_claim_notebook_origin():
    with store() as (records,access,witness,raw,source,descriptor):
        page=output_reader.project_document(raw,witness)
        registered=presentations.register(records,access,witness=witness,projection=page)
        origin=registered['items'][0]['representations'][0]['definition_witness']['origin']
        changed=copy.deepcopy(descriptor);changed['questions'][0]['title']='different'
        with pytest.raises(ForgeError):save(records,access,source,changed,origin)
        forged=copy.deepcopy(page);forged['items'][0]['representations'][0]['value']=changed
        with pytest.raises(ForgeError):presentations.register(records,access,witness=witness,projection=forged)
        foreign=Access('w'+'9'*32,'owner',lambda *args:True)
        with pytest.raises(ForgeError):presentations.register(records,foreign,witness=witness,projection=page)
        bad=copy.deepcopy(witness);bad['artifact']['generation']=True
        with pytest.raises(ForgeError):presentations.register(records,access,witness=bad,projection={**page,'artifact':bad['artifact']})
        other_manifest={**source_manifest(),'binding':'b'*64}
        other=records.register_source(access,other_manifest,verify=lambda *args:True)
        with pytest.raises(ForgeError):save(records,access,other,descriptor,origin)
        assert records.database.connection.execute('SELECT count(*) FROM annotation_events').fetchone()[0]==0


def add_handle(db,index,*,project=PROJECT,execution='older-cell'):
    identifier='o'+f'{index:032x}';schema=source_manifest()['schema']
    seed=dict(project=project,object_id=identifier,generation=1,retention='pinned',execution_id=execution,parents=[],source_binding=None)
    manifest=candidate_manifest(seed,dict(format='lance',version=1,rows=2,schema=schema,
        schema_sha256='a'*64,logical_bytes=12,complete=True),{'_versions/1.manifest':dict(bytes=1,sha256='b'*64)})
    digest=c.digest(manifest)
    db.execute('INSERT INTO analysis_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (project,identifier,'p'+f'{index:032x}',1,execution,'ready',c.encoded(seed),1,4,digest,c.encoded(manifest),None,None))
    return dict(id=identifier,generation=1,digest=digest)


def test_only_broker_registered_prior_cell_handles_enter_the_exact_output_witness():
    with store() as (records,access,witness,raw,source,descriptor):
        db=records.database.connection
        first=add_handle(db,100);unrelated=add_handle(db,101)
        # Real active session/execution state, then exact live-scope record.
        db.execute("UPDATE analysis_sessions SET generation='generation',state='executing'")
        db.execute("UPDATE analysis_executions SET state='running'");db.commit()
        scope=Scope(access,'session','generation','execution')
        dependencies.record(records,scope,handle=first)
        dependencies.record(records,scope,handle=first)
        db.execute("UPDATE analysis_executions SET state='finished'");db.commit()
        actual=output_reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,dependency_resolver=dependencies.resolve)
        assert first['id'] in actual['table_handles'] and unrelated['id'] not in actual['table_handles']
        with pytest.raises(ForgeError):dependencies.record(records,scope,handle=unrelated)
        assert db.execute('SELECT count(*) FROM analysis_execution_dependencies').fetchone()[0]==1


def test_dependency_distinct_handle_limit_is_checked_before_admission():
    with store() as (records,access,*unused):
        db=records.database.connection;scope=SimpleNamespace(access=access,execution_id='execution')
        handles=[add_handle(db,200+index) for index in range(17)]
        for handle in handles[:16]:dependencies.remember(db,scope,handle=handle)
        with pytest.raises(ForgeError) as error:dependencies.remember(db,scope,handle=handles[16])
        assert error.value.code=='resource_limit'
        assert db.execute('SELECT count(*) FROM analysis_execution_dependencies').fetchone()[0]==16
        db.rollback()
