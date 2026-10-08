import hashlib
import json
import sqlite3
import pytest
import pyarrow as pa
from demiflow import data
from demiflow.collect.contracts import DOCUMENT_RESULT


def snapshot(tmp_path):
    path=tmp_path/'closed.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE cache (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        for key,value in [('ok',dict(url='https://example.org',status='ok',document_ref={'uri':'/object','sha256':'a'*64})),
                          ('failed',dict(url='https://example.org/f',status='timeout',document_ref=None)),
                          ('search',dict(status='ok',candidates=[]))]:
            db.execute('INSERT INTO cache VALUES (?,?)',(key,json.dumps(value)))
    return path,hashlib.sha256(path.read_bytes()).hexdigest()


def test_read_document_receipts_preserves_failed_and_is_read_only(tmp_path):
    path,sha=snapshot(tmp_path)
    rows=data.read_document_receipts(str(path),sha256=sha).take(10)
    assert [r['receipt']['status'] for r in rows]==['ok','timeout']
    assert rows[1]['receipt']['document_ref'] is None
    assert hashlib.sha256(path.read_bytes()).hexdigest()==sha
    output=tmp_path/'receipts.lance'
    data.read_document_receipts(str(path),sha256=sha).write_lance(str(output),mode='create',
        schema=pa.schema([('receipt_id',pa.string()),('receipt',DOCUMENT_RESULT)]))
    assert data.read_lance(str(output),version=1).count()==2


def test_receipt_source_checks_pin_and_budgets_before_decode(tmp_path):
    path,sha=snapshot(tmp_path)
    with pytest.raises(ValueError,match='SHA256'):
        data.read_document_receipts(str(path),sha256='b'*64).count()
    with pytest.raises(ValueError,match='max_journal_bytes'):
        data.read_document_receipts(str(path),sha256=sha,max_journal_bytes=10).count()
    with pytest.raises(ValueError,match='max_receipt_bytes'):
        data.read_document_receipts(str(path),sha256=sha,max_receipt_bytes=100).count()
    with pytest.raises(ValueError,match='max_rows'):
        data.read_document_receipts(str(path),sha256=sha,max_rows=1).count()
    path.with_name(path.name+'-wal').touch()
    with pytest.raises(ValueError,match='live journal'):
        data.read_document_receipts(str(path),sha256=sha).count()
