"""Keep historical output registration protected after retiring its UI."""
from contextlib import contextmanager
import pytest
from test_forge_outputs_media import PROJECT, OBJECT, witness_and_raw
from demiflow.workbench.analysis import outputs, output_reader

@pytest.fixture
def registered_output(request):
    """Historical registered-output authority, independent of the retired UI."""
    from types import SimpleNamespace
    from demiflow.workbench.analysis import contracts as c
    from demiflow.workbench.analysis.records import Access
    from test_forge_outputs_media import registered_database
    from demiflow.workbench.analysis import presentations as backend
    numeric=getattr(request,'param',None)=='numeric'
    source=c.source_snapshot(dict(kind='fixed_lance',binding='b'*64,schema=[dict(name='number' if numeric else 'text',type='float64' if numeric else 'utf8',nullable=True)],rows=1,case_key=dict(kind='row_id',field=None),retention='external'))
    descriptor=dict(version=1,source_snapshot=source['digest'],case_key=source['case_key'],fields=[dict(id='f',field='number' if numeric else 'text',label='Material',role='input',format='json' if numeric else 'text')],grouping=[],filters=dict(op='range',field='number',value=dict(lower=1e-6,upper=1.0)) if numeric else None,sort=[],layout='card',questions=[dict(id='quality',title='质量',type='rating',required=False,minimum=1,maximum=5) if numeric else dict(id='quality',title='质量',type='text',required=False)])
    witness,raw=witness_and_raw([dict(output_type='display_data',data={outputs.CASE_MIME:descriptor,'text/plain':'Case output'},metadata={})])
    records,digest=registered_database(raw,witness);db=records.database.connection
    db.execute('INSERT INTO source_snapshots VALUES (?,?,?)',(PROJECT,source['digest'],c.encoded(source)))
    db.execute('UPDATE analysis_sessions SET sources=? WHERE id=?',(c.encoded({'data':source['digest']}),'session'));db.commit()
    @contextmanager
    def transaction(*,payload_bytes):
        assert payload_bytes<=128*1024
        db.execute('BEGIN')
        try:yield db
        except BaseException:db.rollback();raise
        else:db.commit()
    records.database.transaction=transaction
    access=Access(PROJECT,'owner',lambda *args:True)
    witness=output_reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,expected_binding=digest)
    page=output_reader.project_document(raw,witness)
    registered=backend.register(records,access,witness=witness,projection=page)
    rep=next(item for item in registered['items'][0]['representations'] if item['format']=='case')
    assert rep['definition_witness']['registered'] is True and rep['origin']==rep['definition_witness']['origin']
    stored=db.execute('SELECT descriptor,binding FROM analysis_output_presentations WHERE project=? AND origin_id=?',(PROJECT,rep['origin']['reference'])).fetchone()
    assert c.decode(bytes(stored['descriptor']))==descriptor
    backend.require(db,access,rep['origin']['reference'],source,{c.digest(question) for question in descriptor['questions']})
    try:yield SimpleNamespace(records=records,db=db,backend=backend,access=access,witness=witness,source=source,descriptor=descriptor,page=page,registered=registered,rep=rep)
    finally:db.close()


@pytest.mark.parametrize('kind',['descriptor','source','offered_origin','output_binding'])
def test_origin_real_registry_rejects_forged_descriptor_or_witness_without_committing(registered_output,kind):
    import copy
    from demiflow.forge.schema import ForgeError
    value=copy.deepcopy(registered_output.page)
    rep=next(rep for rep in value['items'][0]['representations'] if rep['format']=='case')
    if kind=='descriptor':rep['value']['questions'][0]['title']='伪造定义'
    if kind=='source':rep['value']['source_snapshot']='f'*64
    if kind=='offered_origin':rep['definition_witness']['origin']['reference']='no'+'f'*64
    if kind=='output_binding':value['artifact']['digest']='f'*64
    with pytest.raises(ForgeError):registered_output.backend.register(registered_output.records,registered_output.access,witness=registered_output.witness,projection=value)
    assert registered_output.db.execute('SELECT count(*) FROM analysis_output_presentations').fetchone()[0]==1

