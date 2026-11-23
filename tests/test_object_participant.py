"""Executable P3-Ia contract: real PNG bytes and the unchanged native coordinator.

These tests validate the storage participant and shared native budget/commit
contract; HTTP graph coverage lives in test_managed_image_execution.py.
"""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import demiflow
from demiflow import data
from demiflow.data.datasink import WriteContext
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.outputs.object_sink import ObjectRowsSink
from demiflow.execution.recovery_contract import RecoveryContractError, SourcePosition, RunRef, UnresolvedCommit
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.operator_llm.parser import parse_prompt_pack
import lance
import pytest
from PIL import Image

from image_http_fixture import fixture_http, json_definition

ROOT = Path(demiflow.__file__).resolve().parent.parent


@pytest.fixture(scope='session', autouse=True)
def bound_candidate():
    assert Path(demiflow.__file__).resolve() == ROOT / 'demiflow/__init__.py'
    yield
    assert all(Path(m.__file__).resolve().is_relative_to(ROOT)
        for name, m in tuple(sys.modules.items()) if name.startswith('demiflow') and getattr(m,'__file__',None))


def png():
    with Image.new('RGB', (4, 4), 'red') as image, io.BytesIO() as out:
        image.save(out, format='PNG')
        return out.getvalue()


def budgets(path):
    with sqlite3.connect(path / 'run.sqlite') as db:
        return dict(db.execute('SELECT name,consumed FROM runtime_budgets'))


def context(run, action, commit_id):
    return WriteContext(run_id=run.ref.run_id, action_id=action.action_id,
                        commit_id=commit_id, attempt=run.attempt, fence=run.fence)


def commit_fixture(run, *, copies=1):
    with run.action({'kind':'materialize','fixture':'object-rows-v1'}) as action:
        sink = ObjectRowsSink(action, max_object_bytes=4096)
        def produce():
            for i in range(copies):
                run.reserve('requests')
                yield {'id':i, 'image':sink.objects.stage(png())}
        fact = CommitCoordinator(action).commit(1, outputs={'output':(sink, produce())},
            position=SourcePosition('fixed-fixture',copies))
        rows = list(sink.rows.iter_rows(fact['receipts']['output']['data']['rows']))
        action.complete({'rows':len(rows)})
        return sink, fact, rows


def test_png_rows_objects_and_control_metadata_use_distinct_accounting(tmp_path):
    with RunContext.start(tmp_path/'run') as run:
        sink, fact, rows = commit_fixture(run, copies=2)
        assert len(rows) == 2 and rows[0]['image'] == rows[1]['image']
        assert ObjectRef(**rows[0]['image']).read(max_bytes=4096) == png()
        assert fact['status'] == 'acked'
        assert len(fact['prepared']['sinks']['output']['objects']['objects']) == 1
        values = budgets(run.directory)
        assert values['requests'] == 2  # equal bytes do not suppress producer calls
        assert values['object_bytes'] == len(png())
        assert values['row_bytes'] > 0 and values['object_metadata_bytes'] > 0
        assert values['output_bytes'] == values['object_bytes'] + values['row_bytes']
        with sqlite3.connect(run.directory/'run.sqlite') as db:
            names = [r[0] for r in db.execute('SELECT path FROM runtime_artifacts')]
            assert any(n.endswith(hashlib.sha256(png()).hexdigest()) for n in names)
    assert [p.name for p in tmp_path.rglob('*.sqlite')] == ['run.sqlite']


def test_component_and_aggregate_admission_is_atomic_and_idempotent(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'max_output_bytes':100}) as run:
        run.reserve_output('object_bytes',70,reservation_id='blob')
        with pytest.raises(RecoveryContractError,match='output_bytes'):
            run.reserve_output('row_bytes',31,reservation_id='rows')
        assert budgets(run.directory) == {'object_bytes':70,'output_bytes':70,'output_admission_bytes':70}
        run.reserve_output('row_bytes',30,reservation_id='rows')
        run.reserve_output('object_bytes',70,reservation_id='blob')
        assert budgets(run.directory) == {'object_bytes':70,'row_bytes':30,'output_bytes':100,'output_admission_bytes':100}


def test_reusing_reservation_with_different_components_cannot_bypass_aggregate(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'max_output_bytes':100}) as run:
        run.reserve_output('object_bytes',60,reservation_id='same')
        with pytest.raises(RecoveryContractError,match='output_bytes'):
            run.reserve_output('row_bytes',60,reservation_id='same')
        assert budgets(run.directory) == {'object_bytes':60,'output_bytes':60,'output_admission_bytes':60}


@pytest.mark.parametrize('change,match', [
    ({'max_output_bytes':16},'object_bytes'), ({'max_scratch_bytes':16},'scratch'),
])
def test_real_small_budget_rejects_before_blob_creation(tmp_path, change, match):
    with pytest.raises(RecoveryContractError,match=match):
        with RunContext.start(tmp_path/'run',limits=change) as run:
            with run.action({'kind':'materialize','fixture':'small-budget'}) as action:
                sink=ObjectRowsSink(action,max_object_bytes=4096)
                sink.objects.stage(png())
    assert not list((tmp_path/'run/materializations').rglob(hashlib.sha256(png()).hexdigest()))


def test_unprepared_cancellation_cleans_owned_blob_but_preserves_foreign_file(tmp_path):
    with pytest.raises(KeyboardInterrupt):
        with RunContext.start(tmp_path/'run') as run:
            with run.action({'kind':'materialize','fixture':'cancel'}) as action:
                sink=ObjectRowsSink(action,max_object_bytes=4096)
                reference=sink.objects.stage(png())
                foreign=sink.objects.root/'foreign-data'
                foreign.write_bytes(b'foreign')
                raise KeyboardInterrupt()
    assert not Path(reference['uri'].removeprefix('file://')).exists()
    assert foreign.read_bytes()==b'foreign'


def test_foreign_hash_is_not_adopted(tmp_path):
    with pytest.raises(RecoveryContractError,match='not owned'):
        with RunContext.start(tmp_path/'run') as run:
            with run.action({'kind':'materialize','fixture':'foreign'}) as action:
                sink=ObjectRowsSink(action,max_object_bytes=4096)
                target=sink.objects.root/hashlib.sha256(png()).hexdigest()[:2]/hashlib.sha256(png()).hexdigest()
                target.parent.mkdir(parents=True)
                target.write_bytes(png())
                sink.objects.stage(png())
    assert target.read_bytes()==png()


def test_unprepared_same_hash_does_not_delete_earlier_committed_object(tmp_path):
    with pytest.raises(RuntimeError,match='later'):
        with RunContext.start(tmp_path/'run') as run:
            sink,fact,rows=commit_fixture(run)
            sink.objects.reset()
            assert sink.objects.stage(png())==rows[0]['image']
            # A registered pending temporary is reclaimable; the original
            # prepared object path remains protected by the same native journal.
            raise RuntimeError('later')
    assert ObjectRef(**rows[0]['image']).read(max_bytes=4096)==png()


def test_invalid_object_path_and_checksum_fail_before_publication(tmp_path):
    with pytest.raises(RecoveryContractError,match='escapes'):
        with RunContext.start(tmp_path/'run') as run:
            with run.action({'kind':'materialize','fixture':'bad-ref'}) as action:
                sink=ObjectRowsSink(action,max_object_bytes=4096)
                sink.objects.stage(png())
                write_context=context(run,action,'commit-fixture')
                prepared=sink.objects.prepare(None,write_context)
                changed=copy.deepcopy(prepared)
                changed['objects'][0]['path']='../foreign'
                sink.objects.inspect(changed,write_context)


def test_unavailable_receipt_never_means_definitely_uncommitted(tmp_path,monkeypatch):
    with RunContext.start(tmp_path/'run') as run:
        sink,fact,rows=commit_fixture(run)
        write_context=WriteContext(run_id=run.ref.run_id, action_id='action-1',commit_id=fact['commit_id'],
                                   attempt=run.attempt,fence=run.fence)
        marker=sink.objects._marker(write_context)
        original=Path.open
        def denied(path,*args,**kwargs):
            if path==marker: raise PermissionError('controlled unavailable receipt')
            return original(path,*args,**kwargs)
        monkeypatch.setattr(Path,'open',denied)
        with pytest.raises(UnresolvedCommit,match='inaccessible'):
            sink.inspect(fact['prepared']['sinks']['output'],write_context)


def test_existing_predeclared_temporary_is_preserved(tmp_path):
    raw=png(); sha=hashlib.sha256(raw).hexdigest()
    store=LocalObjectStore(tmp_path/'objects')
    temporary=store.directory/sha[:2]/'.pending-foreign'
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b'foreign')
    with pytest.raises(FileExistsError):
        store.put(raw,temporary=temporary,max_bytes=4096)
    assert temporary.read_bytes()==b'foreign'


def test_bounded_object_stream_stops_before_writing_oversize_chunk(tmp_path):
    store=LocalObjectStore(tmp_path/'objects')
    with pytest.raises(ValueError,match='max_bytes'):
        store.put_stream(io.BytesIO(b'x'*128),max_bytes=32)
    assert not [p for p in store.directory.rglob('*') if p.is_file()]


def test_changed_shared_code_keeps_json_and_native_lance_budget_consistent(tmp_path, fixture_http, monkeypatch):
    monkeypatch.setenv('SYNTHETIC_JSON_KEY','fixture-only')
    with RunContext.start(tmp_path/'run',limits={'max_output_bytes':1024**2,
            'memory_bytes':160*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144}) as run:
        sink,fact,rows=commit_fixture(run)
        result=data.from_items([{'instruction':'x'}]).map_prompt_async('answer',
            config=parse_prompt_pack(json.dumps(json_definition(fixture_http['base']))),
            inputs={'instruction':'instruction'},output='answer',call_output='call',error_output='error',
            max_requests=1,options={'trust_env':False,'timeout_s':2,
                'max_request_bytes':8192,'max_response_bytes':8192,'max_event_bytes':8192}).materialize()
        result.select_columns(['answer']).write_lance(str(tmp_path/'json.lance'),mode='create')
        values=budgets(run.directory)
        assert values['output_bytes']==values['object_bytes']+values['row_bytes']
    assert lance.dataset(str(tmp_path/'json.lance')).to_table().to_pylist()==[{'answer':'ok'}]
    assert len(fixture_http['requests'])==1


@pytest.mark.parametrize('window', ['after_prepared','object_publication_before_return',
    'objects_committed_before_rows','rows_committed_before_return','after_outer_receipt','after_acked'])
def test_process_loss_recovers_objects_and_rows_without_rerunning_producer(tmp_path,window):
    (tmp_path/'fixture.png').write_bytes(png())
    script=Path(__file__).with_name('artifact_fault_child.py')
    env={**os.environ,'PYTHONPATH':str(ROOT),'PYTHONDONTWRITEBYTECODE':'1'}
    crashed=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),window],cwd=tmp_path,
        env=env,capture_output=True,text=True,timeout=30)
    assert crashed.returncode==73,crashed.stdout+crashed.stderr
    resumed=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'resume'],cwd=tmp_path,
        env=env,capture_output=True,text=True,timeout=30)
    assert resumed.returncode==0,resumed.stdout+resumed.stderr
    assert crashed.stdout.count('PRODUCER')==1 and 'PRODUCER' not in resumed.stdout
    rows=json.loads((tmp_path/'observed_rows.json').read_text())
    assert ObjectRef(**rows[0]['image']).read(max_bytes=4096)==png()
    values=budgets(tmp_path/'run')
    assert values['requests']==1 and values['object_bytes']==len(png())
    assert values['output_bytes']==values['row_bytes']+values['object_bytes']


@pytest.mark.parametrize('damage',['pixels','missing_blob','missing_object_receipt'])
def test_damaged_confirmed_objects_block_resume_without_regeneration(tmp_path,damage):
    (tmp_path/'fixture.png').write_bytes(png())
    script=Path(__file__).with_name('artifact_fault_child.py')
    env={**os.environ,'PYTHONPATH':str(ROOT),'PYTHONDONTWRITEBYTECODE':'1'}
    crashed=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'after_acked'],cwd=tmp_path,
        env=env,capture_output=True,text=True,timeout=30)
    assert crashed.returncode==73,crashed.stdout+crashed.stderr
    sha=hashlib.sha256(png()).hexdigest()
    blob=next((tmp_path/'run').rglob(sha))
    if damage=='pixels':
        blob.write_bytes(b'x'*len(png()))  # same length, wrong bytes
    elif damage=='missing_blob':
        blob.unlink()
    else:
        next((tmp_path/'run/objects').rglob('.receipts/*.json')).unlink()
    resumed=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'resume'],cwd=tmp_path,
        env=env,capture_output=True,text=True,timeout=30)
    assert resumed.returncode!=0 and 'PRODUCER' not in resumed.stdout
    assert budgets(tmp_path/'run')['requests']==1
