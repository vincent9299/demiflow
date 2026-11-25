"""Real Dataset writes and original commit recovery against a SQLite destination."""
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext, current_run
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import UnresolvedCommit
from demiflow.kv import KVDatasink, SQLiteKV
from demiflow.execution.contracts.dataflow_errors import DataflowError

WORKTREE = Path(__file__).resolve().parents[1]
CHILD = r'''
import json, os, sys
from pathlib import Path
import demiflow
from demiflow import data
from demiflow.kv import SQLiteKV
from demiflow.execution.checkpoint import commit
from demiflow.execution.pipeline.run_context import current_run
from runtime_v3_recovery_scenarios import _scope
root=Path(sys.argv[1]); boundary=sys.argv[2]; mode=sys.argv[3]
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
def observe(row):
    with (root/'source').open('a') as stream: stream.write('row\n')
    return row
if mode=='crash':
    def at(name,commit_id):
        staged=current_run().directory/'materializations/action-1'/('kv-'+commit_id+'.bin')
        if name==boundary and staged.exists():
            with (root/'boundary').open('w') as stream:
                json.dump({'name':name,'commit_id':commit_id},stream);stream.flush();os.fsync(stream.fileno())
            os._exit(79)
    commit.fault_point=at
store=SQLiteKV(root/'index.sqlite','test',missing='empty')
with _scope(root/'run') as run, data.local_execution(workers=1):
    sink=store.datasink(batch_rows=1,batch_bytes=32)
    data.from_items([{'key':'a','value':1},{'key':'b','value':2}]).map(observe).write_datasink(sink)
    (root/'result.json').write_text(json.dumps({'attempt':run.attempt,'rows':sink.receipt.written_rows,
        'commits':run.journal.execution_commits('action-1')}))
'''


def _child(root, boundary, mode):
    env = dict(os.environ, PYTHONPATH=str(WORKTREE)+':'+str(WORKTREE/'tests'), PYTHONDONTWRITEBYTECODE='1')
    return subprocess.run([sys.executable, '-B', '-c', CHILD, str(root), boundary, mode],
        cwd=WORKTREE, env=env, capture_output=True, text=True, timeout=90)


def _markers(store):
    with sqlite3.connect(store.path) as db:
        return db.execute('SELECT namespace,run_id,action_id,commit_id,prepared_digest,receipt FROM kv_sink_commits').fetchall()


@pytest.mark.parametrize('empty', [False, True])
def test_empty_or_typed_duplicate_rows_keep_original_kv_encoding(tmp_path, empty):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    history = SQLiteKV(store.path, 'other')
    history.put_many([('中文', {'version': 'historical'})])
    rows = [] if empty else [
        {'key':'中文','value':{'text':'first','number':1.,'items':[1,None]}},
        {'key':'中文','value':{'text':'你好','number':-0.,'items':[2,None]}},
        {'key':'null','value':None}]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1):
        sink = store.datasink(batch_rows=1)
        data.from_items(rows).write_datasink(sink)
        assert sink.receipt.written_rows == len(rows)
    assert len(_markers(store)) == 1
    assert history.get('中文')['value'] == {'version':'historical'}
    if not empty:
        value = store.get('中文')['value']
        assert value['text'] == '你好' and value['items'] == [2,None]
        assert math.copysign(1, value['number']) == -1
        assert store.get('null') == {'hit':True,'value':None}


@pytest.mark.parametrize('boundary', ['after_prepare_before_record', 'after_prepared', 'after_sink_commit_before_receipt'])
def test_dataset_process_crash_recovers_original_sqlite_commit(tmp_path, boundary):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test', missing='empty')
    store.put_many([('history', {'unchanged': True})])
    first = _child(tmp_path, boundary, 'crash')
    assert first.returncode == 79, first.stderr
    assert json.loads((tmp_path/'boundary').read_text())['name'] == boundary
    assert store.get('history')['value'] == {'unchanged': True}
    published = boundary == 'after_sink_commit_before_receipt'
    assert store.get('a') == {'hit': published, 'value': 1 if published else None}
    original_markers = _markers(store)
    assert len(original_markers) == int(published)
    if published:
        # A later legitimate destination update must not be overwritten by replay.
        store.put_many([('a', 99)])
    second = _child(tmp_path, boundary, 'resume')
    assert second.returncode == 0, second.stderr
    result = json.loads((tmp_path/'result.json').read_text())
    assert result['attempt'] == 2 and result['rows'] == 2
    assert all(c['status'] == 'acked' for c in result['commits'])
    assert len(_markers(store)) == 1
    if published:
        assert _markers(store) == original_markers
    assert store.get('a')['value'] == (99 if published else 1)
    assert store.get('b')['value'] == 2
    assert store.get('history')['value'] == {'unchanged': True}
    # A prepare that was never recorded leaves nothing to adopt: the source is read again.
    reads = 4 if boundary == 'after_prepare_before_record' else 2
    assert (tmp_path/'source').read_text().splitlines() == ['row'] * reads
    with RunJournal(tmp_path/'run')._connection() as db:
        assert not db.execute("SELECT path FROM runtime_artifacts WHERE path LIKE 'scratch/kv-%'").fetchall()


@pytest.mark.parametrize('damage', ['missing', 'replaced', 'receipt'])
def test_unknown_published_destination_is_never_recreated_or_replayed(tmp_path, damage):
    first = _child(tmp_path, 'after_sink_commit_before_receipt', 'crash')
    assert first.returncode == 79, first.stderr
    store = SQLiteKV(tmp_path/'index.sqlite', 'test', missing='empty')
    journal = RunJournal(tmp_path/'run')
    original = journal.execution_commits('action-1')
    if damage in {'missing', 'replaced'}:
        Path(store.path).rename(tmp_path/'original.sqlite')
        if damage == 'replaced':
            store.put_many([('foreign', 'retain')])
    else:
        with sqlite3.connect(store.path) as db:
            db.execute("UPDATE kv_sink_commits SET prepared_digest=?", ('0'*64,))
    second = _child(tmp_path, 'after_sink_commit_before_receipt', 'resume')
    assert second.returncode != 0 and ('missing or was replaced' in second.stderr or 'receipt differs' in second.stderr), second.stderr
    assert journal.execution_commits('action-1') == original
    assert (tmp_path/'source').read_text().splitlines() == ['row', 'row']
    if damage == 'missing':
        assert not Path(store.path).exists()
    if damage == 'replaced':
        assert store.get('foreign')['value'] == 'retain' and not store.get('a')['hit']


def test_sqlite_duplicate_commit_and_abort_do_not_reapply_or_delete_publication(tmp_path, monkeypatch):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    commit = KVDatasink.commit
    def twice(self, prepared, context):
        result = commit(self, prepared, context)
        self.store.put_many([('a', 99)])
        assert commit(self, prepared, context) == result
        self.abort(prepared, context)
        assert self.inspect(prepared, context) == result
        return result
    monkeypatch.setattr(KVDatasink, 'commit', twice)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=1, worker_mode='thread'):
        sink = store.datasink()
        data.from_items([{'key': 'a', 'value': 1}]).write_datasink(sink)
        assert sink.receipt.written_rows == 1
        assert all(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
    assert store.get('a')['value'] == 99 and len(_markers(store)) == 1


def test_sqlite_capacity_failure_rolls_back_whole_native_commit(tmp_path):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test', max_entries=1)
    store.put_many([('a', {'original': 1})])
    with pytest.raises(ValueError, match='capacity'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=1):
            data.from_items([{'key':'a','value':2}, {'key':'b','value':3}]).write_datasink(store.datasink(batch_rows=1))
    assert store.get('a')['value'] == {'original': 1} and not store.get('b')['hit']
    assert _markers(store) == []
    assert not any(c['status']=='acked' and c['prepared']['sinks']['write_datasink'].get('native') is not None
                   for c in run.journal.execution_commits('action-1'))


def test_prepare_inspection_locked_target_and_unpublished_abort(tmp_path, monkeypatch):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test', timeout_s=.05)
    prepare = KVDatasink.prepare
    def checked(self, blocks, context):
        prepared = prepare(self, blocks, context)
        assert self.inspect(prepared, context) is None
        assert not self.store.get('a')['hit']
        with sqlite3.connect(self.store.path) as lock:
            lock.execute('BEGIN EXCLUSIVE')
            with pytest.raises(UnresolvedCommit, match='cannot be inspected conclusively'):
                self.inspect(prepared, context)
        self.abort(prepared, context)
        assert not (current_run().directory/prepared['path']).exists()
        assert not self.store.get('a')['hit']
        raise ValueError('abort test finished without publication')
    monkeypatch.setattr(KVDatasink, 'prepare', checked)
    with pytest.raises(ValueError, match='abort test finished'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, worker_mode='thread'):
            data.from_items([{'key':'a','value':1}]).write_datasink(store.datasink())
    assert _markers(store) == []


def test_corrupt_prepared_payload_rolls_back_prior_upserts(tmp_path, monkeypatch):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    store.put_many([('a', 50)])
    prepare = KVDatasink.prepare
    def corrupt(self, blocks, context):
        prepared = prepare(self, blocks, context)
        path = current_run().directory/prepared['path']
        raw = path.read_bytes()
        path.write_bytes(raw[:-1]+bytes([raw[-1]^1]))
        return prepared
    monkeypatch.setattr(KVDatasink, 'prepare', corrupt)
    with pytest.raises(DataflowError, match='checksum'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, worker_mode='thread'):
            data.from_items([{'key':'a','value':1}, {'key':'b','value':2}]).write_datasink(store.datasink())
    assert store.get('a')['value'] == 50 and not store.get('b')['hit']
    assert _markers(store) == []


def test_sqlite_scratch_uses_aggregate_original_run_admission(tmp_path, monkeypatch):
    from demiflow.execution.contracts.errors import ExecutionBudgetExceeded
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    prepare = KVDatasink.prepare
    observed = []
    def compete(self, blocks, context):
        run = current_run()
        other = run.directory/'scratch/another-producer.reservation'
        used = run.journal.storage_usage()
        run.allocate_artifact(other, size=run.limits['max_scratch_bytes']-used-65536, atomic=False)
        try:
            with pytest.raises(ExecutionBudgetExceeded, match='scratch admission'):
                prepare(self, blocks, context)
            observed.append(True)
            assert not Path(self.store.path).exists()
            raise ValueError('original aggregate admission rejected SQLite allocation')
        finally:
            run.release_temporaries([other])
    monkeypatch.setattr(KVDatasink, 'prepare', compete)
    with pytest.raises(ValueError, match='original aggregate admission'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, worker_mode='thread'):
            data.from_items([{'key':'a','value':1}]).write_datasink(store.datasink())
    assert observed == [True] and not Path(store.path).exists()
