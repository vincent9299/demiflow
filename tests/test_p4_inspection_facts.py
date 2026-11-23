"""Publication projections, explicit verification and preparation proofs."""
import json
import sqlite3
from contextlib import closing

import pytest

from demiflow.execution.journal.run_reader import read_run, RunReadError
from demiflow.execution.journal.publication_facts import verify_write_receipt
from demiflow.execution.contracts.errors import PublicationMismatch
from demiflow.lance.model import LanceWriteReceipt
from test_forge_history_admission import run_fixture, database


def receipt(uri):
    return LanceWriteReceipt(request_hash='request', uri=str(uri), expected_version=None,
        committed_version=1, input_rows=2, written_rows=2, schema_hash='schema', status='committed').to_dict()


def test_projection_retains_prior_commit_but_current_failure_and_diagnostics(tmp_path):
    target, journal = run_fixture(tmp_path, status='failed')
    raw = receipt(tmp_path/'result.lance')
    journal.action_event('action-1','intent',{'request_hash':'request'})
    journal.action_event('action-1','receipt',raw)
    journal.action_event('action-1','result',{'ref':{'lance_version':1},'location_id':'local'})
    state = journal.execution_status()
    journal.begin_attempt(expected_attempt=1, expected_revision=state['revision'])
    from demiflow.execution.run_journal import RunJournal
    second = RunJournal(target, attempt=2)
    second.action_event('action-1','intent',{'request_hash':'next'})
    second.action_event('action-1','failed',{'message':'current failure'})
    with read_run(target) as reader:
        p = reader.output_projection(2,'action-1',diagnostics=True)
        assert p['committed'] and p['retained_from_attempt'] == 1
        assert p['failed'] and p['error'] == {'message':'current failure'}
        assert p['receipt'] == raw and p['result_ref'] == {'lance_version':1}
        assert p['evidence']['attempt'] == 1
    # An incomplete new receipt prevents fallback to a prior committed receipt.
    incomplete = dict(raw, status='indeterminate', written_rows=None,
                      error={'type':'failed','message':'uncertain'})
    second.action_event('action-1','receipt',incomplete)
    with read_run(target) as reader:
        p = reader.output_projection(2,'action-1')
        assert not p['committed'] and p['retained_from_attempt'] is None


def test_output_history_is_complete_or_refused(tmp_path):
    target, _ = run_fixture(tmp_path)
    # Recreate only this test table without uniqueness constraints to exercise
    # corrupt historical databases, rather than trust the writer constraint.
    with closing(sqlite3.connect(target/'run.sqlite')) as db:
        db.execute('DROP TABLE action_events')
        db.execute('CREATE TABLE action_events(attempt,sequence,action_id,kind,document)')
        kinds=('intent','receipt','recovered','stage_commit','stage_recovered','result','recovered_result','failed')
        for a in range(1,33):
            for k in kinds:
                db.execute('INSERT INTO action_events VALUES (?,?,?,?,?)',(a,a*8+kinds.index(k),'action-1',k,'{}'))
        db.commit()
    with read_run(target) as reader:
        assert len(reader._events(attempt=32,action_id='action-1',kinds=kinds,through=True)) == 256
    database(target,"INSERT INTO action_events VALUES (32,999,'action-1','failed','{}')")
    with read_run(target) as reader, pytest.raises(RunReadError,match='budget'):
        reader._events(attempt=32,action_id='action-1',kinds=kinds,through=True)
    database(target,'DELETE FROM action_events WHERE sequence=999')
    database(target,"UPDATE action_events SET attempt=0 WHERE sequence=8")
    with read_run(target) as reader, pytest.raises(RunReadError,match='Ambiguous'):
        reader._events(attempt=32,action_id='action-1',kinds=kinds,through=True)


def test_explicit_metadata_callback_and_recording_are_separate_from_projection(tmp_path,monkeypatch):
    target, journal = run_fixture(tmp_path)
    uri=tmp_path/'result.lance'; raw=receipt(uri)
    journal.action_event('action-1','intent',{'request_hash':'request'})
    calls=[]
    def metadata(uri,version):
        calls.append((uri,version))
        return dict(version=1,schema_hash='schema',schema_identity='identity',row_count=2)
    monkeypatch.setattr('demiflow.execution.journal.publication_facts.inspect_write_receipt',lambda *a:raw)
    before=(target/'run.sqlite').read_bytes()
    with read_run(target) as reader:
        assert reader.output_projection(1,'action-1')['committed'] is False
    assert calls == []
    result=verify_write_receipt(target,'action-1',expected_uri=str(uri),recover=True,metadata_reader=metadata)
    assert calls == [(str(uri),1)]
    assert result['recording'] == dict(action_id='action-1',attempt=1,kind='recovered',value=raw)
    assert result['version']==1 and result['row_count']==2 and 'receipt' not in result
    assert (target/'run.sqlite').read_bytes()==before
    with pytest.raises(PublicationMismatch,match='differs'):
        verify_write_receipt(target,'action-1',expected_uri=str(uri),recover=True,
                             metadata_reader=lambda *a:dict(version=1,schema_hash='schema',schema_identity='identity',row_count=3))
    with pytest.raises(PublicationMismatch,match='metadata'):
        verify_write_receipt(target,'action-1',expected_uri=str(uri),recover=True,
                             metadata_reader=lambda *a:dict(version=1,schema_hash='schema',schema_identity='identity',row_count=True))


def test_preparation_keeps_empty_manifest_distinct_and_formed_state_precedence(tmp_path):
    from demiflow.workbench.runs.requests import _incomplete_seal
    from demiflow.forge.schema import ForgeError
    path=tmp_path/'run.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE manifest(document)')
        db.execute("INSERT INTO manifest VALUES ('{}')")
    with read_run(tmp_path) as reader:
        p=reader.preparation_evidence()
        assert p['manifest_present'] and p['prepare_request_id'] is None
    with pytest.raises(ForgeError,match='ownership'):
        _incomplete_seal(tmp_path,'req','run',tmp_path)
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE execution_state(document)')
        db.execute('INSERT INTO execution_state VALUES (?)',(json.dumps({'status':'prepared'}),))
        db.execute("UPDATE manifest SET document='invalid'")
    with read_run(tmp_path) as reader:
        assert reader.preparation_evidence()['status']=='not_proven'


def test_attempt_state_preserves_character_budget_and_rejects_bad_proof(tmp_path):
    target,_=run_fixture(tmp_path)
    value={'status':'succeeded','note':'中'*40000}
    database(target,'UPDATE attempts SET document=? WHERE attempt=1',(json.dumps(value,ensure_ascii=False),))
    with read_run(target) as reader:
        assert reader.attempt_state(1)==value
    database(target,"UPDATE attempts SET document='invalid' WHERE attempt=1")
    with read_run(target) as reader:
        assert reader.attempt_state(1) is None


def test_legacy_outputs_keep_implicit_attempt_and_reject_explicit_selector(tmp_path):
    target,journal=run_fixture(tmp_path,attempts=False)
    uri=tmp_path/'result.lance';raw=receipt(uri)
    database(target,'UPDATE manifest SET document=?',(json.dumps({'outputs':{'o':{'action_id':'a'}}}),))
    journal.action_event('a','intent',{'request_hash':'request'})
    journal.action_event('a','receipt',raw)
    journal.action_event('a','value',['legacy',1])
    with read_run(target) as reader:
        for detail in ('full','summary'):
            view=reader.execution_view(detail=detail)
            p=view['output_projections']['a']
            assert p['committed'] and p['current_attempt'] is None and p['receipt']==raw
            assert view['actions']['a']['value']==['legacy',1]
        assert reader.output_projection(1,'a')['committed']
        with pytest.raises(RunReadError,match='no attempt'):
            reader.execution_view(attempt=1)
    result=verify_write_receipt(target,'a',expected_uri=str(uri),
        metadata_reader=lambda *a:dict(version=1,schema_hash='schema',schema_identity='identity',row_count=2))
    assert result['version']==1 and result['current_attempt'] is None
    with pytest.raises(RunReadError,match='no attempt'):
        verify_write_receipt(target,'a',expected_uri=str(uri),attempt=1)


def test_old_absence_does_not_supply_a_new_attempt_intent(tmp_path):
    from test_p4_native_maintenance import native,create_action
    from test_p4_attempt_reservations import resume
    from demiflow.execution.outputs.lance_sink import create_target_absent_binding
    from demiflow.execution.journal.run_reader import action_recovery_facts
    journal=native(tmp_path)
    create_action(journal,tmp_path/'missing.lance')
    with read_run(tmp_path) as reader:
        current,_,_=reader._output_selection(1,'a')
    configuration=journal.recovery_identity()
    binding=create_target_absent_binding(configuration['run_id'],'a',journal.execution_action('a')['declaration'],current['intent'])
    journal.claim_execution(configuration,'fixture')
    journal.bind_source('fixture','a-create-target-absent',binding)
    journal.release_execution('fixture')
    resume(journal)
    facts=action_recovery_facts(tmp_path,'a',attempt=2)
    assert facts['absence_recorded'] and not facts['absence_eligible']


def test_metadata_callback_does_not_hold_a_read_transaction_or_write_lease(tmp_path,monkeypatch):
    target,journal=run_fixture(tmp_path)
    uri=tmp_path/'result.lance'
    journal.action_event('a','intent',{'request_hash':'request'})
    journal.action_event('a','receipt',receipt(uri))
    def metadata(uri,version):
        with closing(sqlite3.connect(target/'run.sqlite',timeout=0)) as db:
            db.execute('BEGIN EXCLUSIVE');db.rollback()
        return dict(version=1,schema_hash='schema',schema_identity='identity',row_count=2)
    assert verify_write_receipt(target,'a',expected_uri=str(uri),metadata_reader=metadata)['version']==1
    monkeypatch.setattr('demiflow.lance.preview.snapshot_metadata',metadata)
    assert verify_write_receipt(target,'a',expected_uri=str(uri))['version']==1


@pytest.mark.parametrize('deadline',[float('nan'),float('inf'),True,'later'])
def test_nonfinite_or_invalid_deadlines_are_rejected(tmp_path,deadline):
    from demiflow.execution.journal.run_reader import action_recovery_facts
    from demiflow.execution.journal.sealed_plan_facts import read_action_plan
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.journal.run_reader import observe_storage
    for read in (lambda:action_recovery_facts(tmp_path,'a',deadline=deadline),
                 lambda:read_action_plan(tmp_path,'a',max_bytes=1024,deadline=deadline),
                 lambda:observe_storage(tmp_path,deadline=deadline).__enter__()):
        with pytest.raises(RecoveryContractError):
            read()


def test_readonly_v3_carrier_budget_precedes_reads_and_deadline_follows_inspect(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from demiflow.execution.seal import plan as seal
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    def ref(size=1):
        return dict(digest='a'*64,size_bytes=size,store_id='store',key='a'*64,media_type='application/json')
    facts=dict(schema=seal.SEAL_SCHEMA,run_id='b'*32,action_id='a',dag_id='graph',plan_id='physical',
               logical_ref=ref(),physical_ref=ref(),programs=[ref(11)],code_pin_ref=ref(),
               identity_bridge_ref=ref(),source_refs=[ref()],constants_ref=ref(),
               plan_revision=0,graph_digest='graph',physical_digest='physical')
    (tmp_path/'run.sqlite').touch()
    reader=SimpleNamespace(path=tmp_path/'run.sqlite',source_binding=lambda _:facts)
    reads=[];inspections=[]
    def read(*args,**kwargs):
        reads.append(True);return b'x'
    monkeypatch.setattr(seal,'_read_bytes',read)
    monkeypatch.setattr(seal,'decode_wire',lambda raw,*,type_name,bounds: (
        SimpleNamespace(action_id='a',dag_id='graph') if type_name=='DataflowGraph'
        else SimpleNamespace(plan_id='physical',dag_id='graph')))
    monkeypatch.setattr(seal,'compute_graph_digest',lambda _: 'graph')
    monkeypatch.setattr(seal,'compute_physical_digest',lambda _: 'physical')
    store=SimpleNamespace(inspect=lambda ref: inspections.append(ref) or True)
    bounds=SimpleNamespace(max_control_bytes=4096)
    with pytest.raises(DataflowError,match='carrier exceeds'):
        seal.load_dataflow(tmp_path,'a',store=store,bounds=bounds,_reader=reader,_max_carrier_bytes=10)
    assert not reads and not inspections
    now=[1.0]
    monkeypatch.setattr('time.monotonic',lambda:now[0])
    def slow(ref):
        inspections.append(ref);now[0]=3.0;return True
    store.inspect=slow
    with pytest.raises(DataflowError,match='deadline'):
        seal.load_dataflow(tmp_path,'a',store=store,bounds=bounds,_reader=reader,
                           _max_carrier_bytes=20,_deadline=2.0)
    assert len(inspections)==1
    # Without the private read policy, the established SDK path is unchanged.
    monkeypatch.setattr(seal,'RunJournal',lambda directory:reader)
    store.inspect=lambda ref:True
    graph,physical,bundle=seal.load_dataflow(tmp_path,'a',store=store,bounds=bounds)
    assert graph.dag_id=='graph' and physical.plan_id=='physical' and len(bundle.programs)==1
