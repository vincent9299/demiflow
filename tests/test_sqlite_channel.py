"""Explicit v1 archive retention and rejection of in-place queue conversion."""
import hashlib
import sqlite3
from pathlib import Path

import pytest
from demiflow import data
from demiflow.queue import QueueChannel,QueueConfig,ProducerSourceSpec,QueueResultSink
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.queue_contract import HistoryReadLimits,HistoryReadLimit


def archive(tmp_path):
    path=tmp_path/'legacy.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript('CREATE TABLE tasks (task_id TEXT PRIMARY KEY,payload_json TEXT,pool TEXT); CREATE TABLE completions (task_id TEXT PRIMARY KEY,result_json TEXT);')
        db.execute("INSERT INTO tasks VALUES ('one','{\"value\":1}','p')")
        db.execute("INSERT INTO completions VALUES ('one','{\"old_status\":\"complete\"}')")
    return path,hashlib.sha256(path.read_bytes()).hexdigest()


def test_v1_archive_is_read_only_and_not_certified_as_v2(tmp_path):
    path,digest=archive(tmp_path)
    for results in (False,True):
        rows=data.read_legacy_queue_records(str(path),sha256=digest,pool='p',through=1,results=results).take_all()
        assert rows[0]['task_id']=='one' and rows[0]['verification']['status']=='legacy_not_certified'
        assert ('old_status' in rows[0]['value']) is results
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    assert not Path(str(path)+'-wal').exists()
    assert not Path(str(path)+'-shm').exists()


def test_v1_rejected_before_any_journal_or_schema_mutation(tmp_path):
    path,digest=archive(tmp_path)
    channel=QueueChannel(QueueConfig(backend='sqlite',queue_id='new',path=str(path)))
    with pytest.raises(RecoveryContractError,match='cannot be migrated'):
        channel.open('p',producer_id='producer',source=ProducerSourceSpec(kind='manifest',source_id='fixed',uri='memory:fixed',version=1))
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    assert not Path(str(path)+'-wal').exists()


def test_v1_checksum_and_finite_inspection_bound(tmp_path):
    path,digest=archive(tmp_path)
    with pytest.raises(RecoveryContractError,match='SHA256'):
        data.read_legacy_queue_records(str(path),sha256='0'*64,pool='p',through=1).take_all()
    with pytest.raises(HistoryReadLimit,match='hash budget'):
        data.read_legacy_queue_records(str(path),sha256=digest,pool='p',through=1,limits=HistoryReadLimits(max_hash_bytes=0)).take_all()


def test_retired_combination_and_queue_runner_apis_are_absent():
    from demiflow.data.dataset import Dataset
    from demiflow.collect.session import WebSession
    from demiflow.collect.web import WebClient
    assert not hasattr(Dataset,'fetch_images') and not hasattr(Dataset,'fetch_documents')
    assert not hasattr(WebSession,'download') and not hasattr(WebClient,'download')
    assert not hasattr(WebSession,'fetch') and not hasattr(WebSession,'fetch_image') and not hasattr(WebClient,'fetch')
    import importlib.util
    for module in ('demiflow.collect.cosqueue','demiflow.collect.queue_runner','demiflow.execution.sqlite_channel','demiflow.collect.sqlite_queue','demiflow.collect.image_fetch','demiflow.collect.fetch_routes','demiflow.collect.fetch_sessions','demiflow.collect.embedded_worker'):
        assert importlib.util.find_spec(module) is None


def test_v1_history_cursor_resumes_only_the_same_archive(tmp_path):
    path,_=archive(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO tasks VALUES ('two','{\"value\":2}','p')")
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    rows=data.read_legacy_queue_records(str(path),sha256=digest,pool='p',through=2,
        limits=HistoryReadLimits(max_rows=1)).iter_rows()
    assert next(rows)['task_id']=='one'
    with pytest.raises(HistoryReadLimit) as caught:next(rows)
    remainder=data.read_legacy_queue_records(str(path),sha256=digest,pool='p',through=2,
        after=caught.value.next_cursor).take_all()
    assert [row['task_id'] for row in remainder]==['two']
    with pytest.raises(ValueError,match='differs'):
        data.read_legacy_queue_records(str(path),sha256=digest,pool='other',through=2,
            after=caught.value.next_cursor)
