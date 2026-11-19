"""Real checked workspace SaveLance: native ACKs, fixed results and CAS."""
import json
import sqlite3
import time
from pathlib import Path
import lance
import pyarrow as pa
import pytest
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.execution.run_journal import RunJournal
from test_forge_runs import base, root, execute

BODY = '''from demiflow import data
def run(*, source, stage, target):
    schema = data.schema({'fields': [{'name': 'case_id', 'type': 'string'}]})
    rows = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    rows.save_lance(stage['uri'], schema=schema, key='case_id', stage='cases', mode='overwrite',
        max_batch=1, queue_depth=1, max_row_bytes=4096, max_index_bytes=65536).write_lance(
        target['uri'], mode='create', schema=schema, return_receipt=True)
'''


def prepare(root, *, body=BODY, stage=None):
    (root/'flows/saved.py').write_text(body)
    parameters = {'format':'demiflow_forge_flow_parameters_v2','arguments':{
        'source':{'kind':'table','alias':'candidates'},
        'stage': stage or {'kind':'output','location_id':'local','relative_uri':'stage.lance','mode':'create','expected_version':None},
        'target':{'kind':'output','location_id':'local','relative_uri':'final.lance','mode':'create','expected_version':None}}}
    (root/'flows/saved.json').write_text(json.dumps(parameters))
    ws.register_flow(root,'flows/saved.py:run',params='flows/saved.json')
    return runs.prepare(root,'flows/saved.py:run',params='flows/saved.json')


def test_native_save_prepare_execute_and_fixed_result(root):
    prepared=prepare(root)
    assert not any(Path(o['uri']).exists() for o in prepared['outputs'])
    result=execute(root,prepared)
    assert result['status']=='succeeded',result
    by={o['name']:o for o in result['outputs']}
    stage=by['stage'];commit=stage['commit']
    assert stage['publication']=='native_stage_v3' and stage['result']['ref']['row_count']==3
    assert commit['report_node_id']==stage['action_id'] and commit['parent_action_id']!=stage['action_id']
    original=RunJournal(runs.directory(root,result['run_id'])).execution_commit(commit['commit_id'])
    assert original['status']=='acked' and original['action_id']==commit['scope_id']
    assert original['receipts']['lance']['data']['reference']==commit['reference']
    assert [row['case_id'] for row in ws.sample_table(root,'run:'+result['run_id']+':stage')['rows']]==['c01','c02','c03']
    assert by['target']['result']['ref']['row_count']==3
    journal=RunJournal(runs.directory(root,result['run_id']))
    with journal._connection() as db:
        declarations=[json.loads(row[0]) for row in db.execute('SELECT declaration FROM runtime_commit_scopes')]
    first=next(d for d in declarations if d.get('schema')=='demiflow.stream_lance.v3' and d['request']['ordinal']==0)
    from demiflow.execution.lance_sink import spec_from_identity
    assert spec_from_identity(first['spec']).mode=='create'  # native absence-CAS, not unchecked overwrite
    from demiflow.execution.sealed_plans import load_sealed_plan
    bundle=load_sealed_plan(runs.directory(root,result['run_id']),commit['parent_action_id'])
    assert bundle['physical'].policy.max_active_workers==4
    assert bundle['physical'].bounds.max_resident_bytes <= prepared['limits']['memory_bytes']
    assert bundle['physical'].bounds.max_spill_bytes <= prepared['limits']['max_scratch_bytes']
    versions=lance.dataset(stage['uri']).versions()
    assert len(versions)==3
    from demiflow.execution.host.inspection import verify_published_output
    from demiflow.execution.recovery_contract import RecoveryContractError
    def verify_recorded():
        return verify_published_output(journal.directory, stage['action_id'],
            expected_uri=stage['uri'], expected_schema_identity=stage['schema_identity'],
            publication='native_stage_v3', report_digest=result['report_digest'],
            metadata_reader=runs._output_metadata)
    assert result['state']['cleanup']=='complete'
    with sqlite3.connect(journal.path) as db:
        db.execute("UPDATE runtime_commits SET status='prepared' WHERE commit_id=?",(commit['commit_id'],))
    try:
        with pytest.raises(RecoveryContractError,match='ACK'):
            verify_recorded()
    finally:
        with sqlite3.connect(journal.path) as db:
            db.execute("UPDATE runtime_commits SET status='acked' WHERE commit_id=?",(commit['commit_id'],))
    for changed in ({'scope_id':'wrong'},{'report_node_id':'wrong'},{'parent_action_id':'wrong'},
                    {'reference':{'uri':stage['uri'],'version':1}}):
        with sqlite3.connect(journal.path) as db:
            originals=db.execute("SELECT attempt,kind,document FROM action_events WHERE action_id=? AND kind IN ('stage_commit','stage_recovered')",(stage['action_id'],)).fetchall()
            db.execute("UPDATE action_events SET document=? WHERE action_id=? AND kind IN ('stage_commit','stage_recovered')",(json.dumps({**commit,**changed}),stage['action_id']))
        try:
            with pytest.raises(RecoveryContractError):
                verify_recorded()
        finally:
            with sqlite3.connect(journal.path) as db:
                for attempt,kind,document in originals:
                    db.execute('UPDATE action_events SET document=? WHERE action_id=? AND attempt=? AND kind=?',(document,stage['action_id'],attempt,kind))


def test_stage_stays_visible_after_later_failure_and_reconcile_without_events(root):
    body=BODY.replace(".write_lance(\n        target['uri'], mode='create', schema=schema, return_receipt=True)",
        ".run_stream()\n    data.require(False, 'later failure')\n    rows.write_lance(target['uri'], mode='create', schema=schema, return_receipt=True)")
    prepared=prepare(root,body=body)
    result=execute(root,prepared)
    assert result['status']=='failed',result
    stage=next(o for o in result['outputs'] if o['name']=='stage')
    assert stage['result']['ref']['row_count']==3
    target=runs.directory(root,result['run_id'])
    # UI projection loss must recover only from original ACKed child scopes.
    with sqlite3.connect(target/'run.sqlite') as db:
        db.execute('DELETE FROM action_events WHERE action_id=?',(stage['action_id'],))
    assert runs.register_results(root,result['run_id'],recover=True)==[]
    restored=next(o for o in runs.show(root,result['run_id'])['outputs'] if o['name']=='stage')
    assert restored['result']['ref']==stage['result']['ref']
    assert len(lance.dataset(stage['uri']).versions())==3


@pytest.mark.parametrize('drift',[False,True])
def test_fixed_overwrite_permission_preserves_original_version_and_rejects_drift(root,drift):
    existing=root/'data/saved-existing.lance'
    schema=pa.schema([('case_id',pa.string())])
    lance.write_dataset(pa.table({'case_id':['original']},schema=schema),str(existing))
    ws.add_location(root,'saved',root/'data',writable=True,write_modes=['overwrite'])
    prepared=prepare(root,stage={'kind':'output','location_id':'saved','relative_uri':existing.name,
                                 'mode':'overwrite','expected_version':1})
    if drift:
        lance.write_dataset(pa.table({'case_id':['foreign']},schema=schema),str(existing),mode='append')
        with pytest.raises(ValueError,match='CAS|changed'):
            runs.start(root,prepared['run_id'],expected=prepared['digest'])
        assert lance.dataset(str(existing)).version==2
        assert lance.dataset(str(existing)).to_table().column('case_id').to_pylist()==['original','foreign']
    else:
        result=execute(root,prepared)
        assert result['status']=='succeeded',result
        stage=next(o for o in result['outputs'] if o['name']=='stage')
        assert stage['commit']['reference']['version']==4
        assert lance.dataset(str(existing),version=1).to_table().column('case_id').to_pylist()==['original']
        assert lance.dataset(str(existing),version=4).to_table().column('case_id').to_pylist()==['c01','c02','c03']


def test_real_managed_worker_interruption_resumes_without_reoverwrite(root):
    import os
    import signal
    from test_forge_runs import wait
    prepared=prepare(root)
    target=runs.directory(root,prepared['run_id'])
    runs.start(root,prepared['run_id'],expected=prepared['digest'])
    journal=RunJournal(target)
    deadline=time.monotonic()+30
    killed=False
    original_ids=set()
    while time.monotonic()<deadline:
        with journal._connection() as db:
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone()
            native=db.execute("SELECT c.commit_id FROM runtime_commit_scopes s JOIN runtime_commits c ON c.action_id=s.scope_id WHERE c.status='acked' AND json_extract(s.declaration,'$.schema')='demiflow.stream_lance.v3'").fetchall() if exists else []
        # All original microbatches are ACKed; stop during the bounded product
        # projection before terminal publication. This is an actual owned PID.
        if len(native)>=4:
            state=journal.execution_status()
            worker=state.get('worker')
            if worker and runs.process_matches(worker):
                original_ids={row[0] for row in native}
                os.kill(worker['pid'],signal.SIGKILL)
                killed=True
                break
        if journal.execution_status()['status'] in runs.TERMINAL:
            break
        time.sleep(.01)
    assert killed,'worker finished before native-ACK interruption seam'
    stopped=wait(root,prepared['run_id'])
    assert stopped['status']=='interrupted',stopped
    reconciled=runs.reconcile(root,prepared['run_id'])
    assert reconciled['reconciliation_errors']==[],reconciled
    stage=next(o for o in reconciled['outputs'] if o['name']=='stage')
    assert stage['result']['ref']['row_count']==3
    before=len(lance.dataset(stage['uri']).versions())
    resumed=runs.resume(root,prepared['run_id'],expected=prepared['digest'],expected_attempt=1)
    final=execute(root,resumed)
    assert final['status']=='succeeded' and final['attempt']==2,final
    assert len(lance.dataset(stage['uri']).versions())==before==3
    assert all(journal.execution_commit(key)['status']=='acked' for key in original_ids)
    assert all(o['result']['ref']['row_count']==3 for o in final['outputs'])


def test_failed_second_microbatch_keeps_first_original_ack_visible(root):
    (root/'operators/saved.py').write_text('''from typing import TypedDict
class Row(TypedDict):
    case_id: str | None
def select(row: Row) -> bool:
    if row['case_id'] == 'c02':
        raise ValueError('second microbatch rejected')
    return True
''')
    body=BODY.replace('from demiflow import data','from demiflow import data\nfrom operators.saved import select').replace("mode='overwrite',", "mode='overwrite', when=select,")
    prepared=prepare(root,body=body)
    result=execute(root,prepared)
    assert result['status']=='failed',result
    by={o['name']:o for o in result['outputs']}
    assert by['stage']['result']['ref']['row_count']==1
    assert by['stage']['commit']['reference']['version']==1
    assert by['target']['result'] is None
    assert [r['case_id'] for r in ws.sample_table(root,'run:'+result['run_id']+':stage')['rows']]==['c01']
    original=RunJournal(runs.directory(root,result['run_id'])).execution_commit(by['stage']['commit']['commit_id'])
    assert original['status']=='acked'
    assert len(lance.dataset(by['stage']['uri']).versions())==1


def test_single_worker_native_adapter_recovery_preserves_microbatch_identity(tmp_path):
    import os
    import subprocess
    import sys
    from test_runtime_v3_stream_lance import CHILD, SCHEMA
    root=tmp_path
    target=root/'output.lance'
    lance.write_dataset(pa.Table.from_pylist([{'id':'old','value':-1}],schema=SCHEMA),str(target))
    child=CHILD.replace('data.local_execution():','data.local_execution(workers=1):')
    child=child.replace("own=db.execute('SELECT declaration FROM runtime_commit_scopes", "exists=db.execute(\"SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'\").fetchone()\n                if not exists:return\n                own=db.execute('SELECT declaration FROM runtime_commit_scopes")
    worktree=Path(__file__).resolve().parents[1]
    env=dict(os.environ,PYTHONPATH=str(worktree)+':'+str(worktree/'tests'),PYTHONDONTWRITEBYTECODE='1')
    first=subprocess.run([sys.executable,'-B','-c',child,str(root),'after_sink_receipt','crash'],
        cwd=worktree,env=env,capture_output=True,text=True,timeout=90)
    assert first.returncode==77,first.stderr
    before=json.loads((root/'boundary.json').read_text())
    second=subprocess.run([sys.executable,'-B','-c',child,str(root),'after_sink_receipt','resume'],
        cwd=worktree,env=env,capture_output=True,text=True,timeout=90)
    assert second.returncode==0,second.stderr
    result=json.loads((root/'result.json').read_text())
    assert result['attempt']==2 and result['outputs']['saved']['version']==3
    assert any(f['commit_id']==before['commit_id'] and f['status']=='acked' for f in result['commits'])
    assert result['budgets']['output_rows']==2
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'a','value':1},{'id':'b','value':2}]
    from demiflow.execution.sealed_plans import load_sealed_plan
    journal=RunJournal(root/'run')
    with journal._connection() as db:
        action=db.execute('SELECT action_id FROM runtime_actions').fetchone()[0]
    physical=load_sealed_plan(root/'run',action)['physical']
    assert physical.policy.max_active_workers==1
    assert any(e.mode=='durable_incremental' for e in physical.exchanges)


def test_replayed_old_ack_never_regresses_stage_projection():
    from demiflow.execution.dataflow_stream_lance import NativeStreamLanceService
    service=object.__new__(NativeStreamLanceService)
    service._last_acked={};service._acked_ordinals={}
    service._remember_ack('n','latest','c9',9)
    service._remember_ack('n','replayed','c1',1)
    assert service._last_acked=={'n':('latest','c9')}
    assert len(service._last_acked)==len(service._acked_ordinals)==1


def test_old_checkpoint_fixed_result_adapter_remains_readable(root):
    """Read real legacy facts without executing retired checkpoint APIs."""
    from demiflow.lance.refs import DatasetRef
    from demiflow.lance.storage import schema_hash
    from demiflow.lance.schema import schema_identity
    schema=pa.schema([('case_id',pa.string())])
    target=root/'data/legacy-stage.lance'
    table=pa.Table.from_pylist([{'case_id':'historical'}],schema=schema)
    lance.write_dataset(table,str(target))
    ref=DatasetRef('stage',target.name,1,'stage','1',schema_hash(schema),1,store_id='local')
    run_id='r'+'0'*24
    action_id='legacy-checkpoint'
    output={'action_id':action_id,'mode':'checkpoint','checkpoint_node':'original-checkpoint',
            'schema_identity':schema_identity(schema),'uri':str(target),'relative_uri':target.name,
            'location_id':'local','dataset_id':'stage','schema_name':'stage','schema_version':'1'}
    manifest={'format':'demiflow_forge_run_manifest_v2','run_id':run_id,'root':str(root.absolute()),
              'entry':'flows/historical.py:run','created_at':0.0,'parameter_path':None,
              'limits':{},'report_digest':'0'*64,'outputs':{'stage':output}}
    manifest['digest']=runs.content_digest(manifest)
    journal=RunJournal(runs.directory(root,run_id))
    journal.initialize(manifest,attempts=False)
    journal.transition({None},'succeeded')
    journal.action_event(action_id,'stage_commit',{
        'checkpoint_node':'original-checkpoint','schema_identity':schema_identity(schema),
        'reference':{'uri':str(target),'version':1}})
    journal.action_event(action_id,'result',{'ref':ref.to_dict(),'location_id':'local'})
    _,actual,path=runs.result_table(root,run_id,'stage')
    assert actual==ref and path==target
    assert lance.dataset(path,version=actual.lance_version).to_table().to_pylist()==[{'case_id':'historical'}]
    lance.write_dataset(pa.Table.from_pylist([{'case_id':'new'}],schema=schema),str(target),mode='append')
    assert lance.dataset(str(target)).version==2
    _,actual,path=runs.result_table(root,run_id,'stage')
    assert actual==ref and path==target
    assert lance.dataset(path,version=actual.lance_version).to_table().to_pylist()==[{'case_id':'historical'}]


@pytest.mark.parametrize('foreign',[False,True])
def test_managed_published_before_receipt_admits_only_original_native_recovery(root,tmp_path,monkeypatch,foreign):
    """Fault shim preserves the real managed supervisor, worker, unit and journal."""
    import subprocess
    from test_forge_runs import wait
    prepared=prepare(root)
    helper=tmp_path/'native_fault_worker.py'
    helper.write_text('''import sys,os,json,subprocess
from pathlib import Path
mode,root,run_id=sys.argv[1:4]
target=Path(root)/'.forge/runs'/run_id
sys.path.insert(0,str(target/'runtime'))
from demiflow.workbench.runs import worker
if mode=='supervise':
    original=subprocess.Popen
    def launch(command,*args,**kwargs):
        if len(command)>4 and command[4]=='execute':
            command=list(command);command[3]=__file__
        return original(command,*args,**kwargs)
    subprocess.Popen=launch
    worker.supervise(Path(root),run_id,int(sys.argv[4]),int(sys.argv[5]))
else:
    from demiflow.execution import commit
    from demiflow.execution.run_journal import RunJournal
    def fault(name,commit_id):
        if name!='after_sink_commit_before_receipt':return
        journal=RunJournal(target)
        fact=journal.execution_commit(commit_id)
        with journal._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():return
            row=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(fact['action_id'],)).fetchone()
        if not row or json.loads(row[0]).get('schema')!='demiflow.stream_lance.v3':return
        (target/'native-fault.json').write_text(json.dumps({'commit_id':commit_id,'scope_id':fact['action_id']}))
        os._exit(77)
    commit.fault_point=fault
    raise SystemExit(worker.execute(Path(root),run_id,int(sys.argv[4])))
''')
    original=subprocess.Popen
    def launch(command,*args,**kwargs):
        if isinstance(command,list) and len(command)>4 and command[4]=='supervise':
            command=list(command);command[3]=str(helper)
        return original(command,*args,**kwargs)
    with monkeypatch.context() as injected:
        injected.setattr(subprocess,'Popen',launch)
        runs.start(root,prepared['run_id'],expected=prepared['digest'])
    stopped=wait(root,prepared['run_id'])
    assert stopped['status']=='interrupted',stopped
    target=runs.directory(root,prepared['run_id'])
    fault=json.loads((target/'native-fault.json').read_text())
    journal=RunJournal(target)
    assert journal.execution_commit(fault['commit_id'])['status']=='prepared'
    assert journal.execution_commit(fault['commit_id'])['receipts']=={}
    stage=next(o for o in stopped['outputs'] if o['name']=='stage')
    assert stage['result'] is None and lance.dataset(stage['uri']).version==1
    if not foreign:
        from demiflow.execution.dataflow_stream_lance import _BatchSink
        from demiflow.execution.recovery_contract import UnresolvedCommit
        def unknown_inspection(*args, **kwargs):
            raise UnresolvedCommit('native inspection is indeterminate')
        # Real prepared scope, fixed table, bridge and managed control path;
        # inject only the sink-inspection uncertainty at the original boundary.
        with monkeypatch.context() as uncertain:
            uncertain.setattr(_BatchSink, 'inspect', unknown_inspection)
            _, value = runs._load(root, prepared['run_id'])
            with pytest.raises(ValueError, match='unresolved|indeterminate'):
                runs.validate_output_targets(value)
        assert journal.execution_commit(fault['commit_id'])['status']=='prepared'
        assert journal.execution_commit(fault['commit_id'])['receipts']=={}
    if foreign:
        lance.write_dataset(pa.table({'case_id':['foreign']}),stage['uri'],mode='append')
    reconciled=runs.reconcile(root,prepared['run_id'])
    assert reconciled['reconciliation_errors']==[],reconciled
    assert next(o for o in reconciled['outputs'] if o['name']=='stage')['result'] is None
    # Reconciliation/admission cannot turn inspection into a forged ACK.
    assert journal.execution_commit(fault['commit_id'])['status']=='prepared'
    if foreign:
        with pytest.raises(ValueError,match='outside|changed|unresolved'):
            runs.resume(root,prepared['run_id'],expected=prepared['digest'],expected_attempt=1)
        assert lance.dataset(stage['uri']).version==2
        assert journal.execution_commit(fault['commit_id'])['status']=='prepared'
    else:
        resumed=runs.resume(root,prepared['run_id'],expected=prepared['digest'],expected_attempt=1)
        assert journal.execution_commit(fault['commit_id'])['status']=='prepared'
        result=execute(root,resumed)
        assert result['status']=='succeeded' and result['attempt']==2,result
        assert journal.execution_commit(fault['commit_id'])['status']=='acked'
        assert lance.dataset(stage['uri']).version==3
        assert lance.dataset(stage['uri']).to_table().column('case_id').to_pylist()==['c01','c02','c03']
