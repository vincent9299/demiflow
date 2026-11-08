"""Actual known-revision SQLite migration/backups preserve immutable rows."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from contextlib import closing

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import schema_migrations
from demiflow.workbench.analysis.database import _SCHEMA
from demiflow.workbench.analysis.database import _SCHEMA_V2
from demiflow.workbench.analysis.database import _SCHEMA_V3
from test_forge_analysis_records import document,question,source_manifest

PROJECT='w'+'0'*32


def old_database(directory,version):
    directory.mkdir(mode=0o700);path=directory/'records.sqlite'
    source=c.source_snapshot(source_manifest());book=document();definition=question();origin=dict(kind='unsaved_view',reference='old-draft')
    with sqlite3.connect(path) as db:
        db.executescript(schema_migrations.blueprint(version,_SCHEMA_V2,_SCHEMA,_SCHEMA_V3))
        db.execute('INSERT INTO analysis_schema VALUES (?)',(schema_migrations.LEGACY[version],))
        db.execute('INSERT INTO source_snapshots VALUES (?,?,?)',(PROJECT,source['digest'],c.encoded(source)))
        db.execute('INSERT INTO documents VALUES (?,?,?,?)',(PROJECT,'notebook','old-book',1))
        db.execute('INSERT INTO document_revisions VALUES (?,?,?,?,?,?,?,?)',(PROJECT,'notebook','old-book',1,c.digest(book),c.encoded(book),'owner','old-time'))
        db.execute('INSERT INTO question_definitions VALUES (?,?,?)',(PROJECT,c.digest(definition),c.encoded(definition)))
        db.execute('INSERT INTO annotation_events(project,source,case_key,variant,field,principal,definition,revision,state,value,origin,time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (PROJECT,source['digest'],'1','','','owner',c.digest(definition),1,'answered',c.encoded('yes'),c.encoded(origin),'old-time'))
        db.execute('INSERT INTO annotation_heads VALUES (?,?,?,?,?,?,?,?,?)',(PROJECT,source['digest'],'1','','','owner',c.digest(definition),1,1))
        db.execute('INSERT INTO operation_receipts VALUES (?,?,?,?,?,?,?)',(PROJECT,'owner','old-request','legacy','a'*64,c.encoded(dict(old_result='preserved')),'old-time'))
    path.chmod(0o600)
    db.close()
    return path


def snapshot(path):
    with closing(sqlite3.connect(path)) as db:
        return {table:db.execute('SELECT * FROM '+table).fetchall() for table in ('source_snapshots','documents','document_revisions','question_definitions','annotation_events','annotation_heads','operation_receipts')}


def start(directory):
    root=Path(__file__).resolve().parents[1]
    value=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('_forge_analysis_schema_driver.py')),str(directory)],
        cwd=root,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(root)),capture_output=True,text=True,timeout=20)
    assert value.returncode==0,value.stderr
    return json.loads(value.stdout)


@pytest.mark.parametrize('version',[1,2,3])
def test_known_additive_upgrade_keeps_notebook_event_origin_and_receipt_exactly(version,tmp_path):
    directory=tmp_path/'database';path=old_database(directory,version);before=snapshot(path)
    assert start(directory)==dict(ok=True,version=4,media_basis=0,captures=0,environments=0,cleanup_receipts=0)
    assert snapshot(path)==before and snapshot(directory/'backup.sqlite')==before
    with closing(sqlite3.connect(directory/'backup.sqlite')) as db:assert db.execute('PRAGMA user_version').fetchone()[0]==version
    backups={name.name:hashlib.sha256(name.read_bytes()).hexdigest() for name in directory.glob('backup*')}
    assert start(directory)['ok'] and snapshot(path)==before
    assert {name.name:hashlib.sha256(name.read_bytes()).hexdigest() for name in directory.glob('backup*')}==backups


def test_unknown_revision_or_spoofed_schema_remains_read_only(tmp_path):
    for name,sql in [('unknown',"UPDATE analysis_schema SET revision_sha256='unknown'"),('extra','CREATE TABLE undocumented(value TEXT)')]:
        directory=tmp_path/name;path=old_database(directory,2)
        with sqlite3.connect(path) as db:db.execute(sql)
        before=path.read_bytes();rows=snapshot(path)
        db.close()
        assert not start(directory)['ok']
        assert path.read_bytes()==before and snapshot(path)==rows and not list(directory.glob('backup*'))


def test_full_backup_slots_refuse_upgrade_without_changing_history(tmp_path):
    directory=tmp_path/'database';path=old_database(directory,2);before=snapshot(path)
    for name in ('backup.sqlite','backup.new.sqlite'):
        target=directory/name;target.write_bytes(b'pre-existing-backup');target.chmod(0o600)
    result=start(directory)
    assert result['ok'] is False and result['code']=='resource_limit'
    with closing(sqlite3.connect(path)) as db:assert db.execute('PRAGMA user_version').fetchone()[0]==2
    assert snapshot(path)==before
    assert (directory/'backup.sqlite').read_bytes()==b'pre-existing-backup' and (directory/'backup.new.sqlite').read_bytes()==b'pre-existing-backup'
