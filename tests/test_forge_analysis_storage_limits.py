"""Real small OS file/SQLite limits; not an OCI, volume or deployment proof."""
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]


def probe(tmp_path, script):
    prefix = """
import json, pathlib, resource, sqlite3, sys
from demiflow.workbench.analysis.database import Database, FileSizeGuard
from demiflow.forge.schema import ForgeError
directory=pathlib.Path(sys.argv[1])
"""
    result = subprocess.run([sys.executable, "-B", "-c", prefix + script, str(tmp_path/"db")],
        env=dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1"), cwd=ROOT,
        capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr.decode()
    return json.loads(result.stdout)


def test_wal_os_file_limit_rejects_commit_without_publishing_partial_records(tmp_path):
    result = probe(tmp_path, """
guard=FileSizeGuard(database_bytes=512*1024,wal_bytes=64*1024).install()
database=Database(directory,guard=guard)
assert resource.getrlimit(resource.RLIMIT_FSIZE)==(64*1024,512*1024)
# Deliberately exercise the OS last guard below the normal preflight budget.
# This is trusted test SQL, not an exposed statement or user-code endpoint.
with database.locked() as db:
    db.execute('BEGIN IMMEDIATE')
    db.execute('INSERT INTO question_definitions VALUES (?,?,?)',('p','x',b'x'*(96*1024)))
    try:
        db.execute('COMMIT')
        raise AssertionError('WAL file limit did not reject commit')
    except sqlite3.Error as error:
        failure=error.sqlite_errorcode
        if db.in_transaction: db.execute('ROLLBACK')
    size=(directory/'records.sqlite-wal').stat().st_size
    assert size<=64*1024
    assert db.execute('SELECT count(*) FROM question_definitions').fetchone()[0]==0
    assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
database.close()
print(json.dumps(dict(sqlite_errorcode=failure,wal_bytes=size,partial_rows=0,guard='RLIMIT_FSIZE')))
""")
    assert result["wal_bytes"] <= 65536 and result["partial_rows"] == 0


def test_main_database_limit_refuses_new_transaction_and_preserves_prior_commits(tmp_path):
    result = probe(tmp_path, """
guard=FileSizeGuard(database_bytes=512*1024,wal_bytes=256*1024).install()
database=Database(directory,guard=guard)
count=0
try:
    for index in range(64):
        with database.transaction(payload_bytes=32*1024) as db:
            db.execute('INSERT INTO question_definitions VALUES (?,?,?)',('p',str(index),b'x'*(32*1024)))
        count+=1
    raise AssertionError('DB limit did not reject transaction')
except ForgeError as error:
    assert error.code=='resource_limit', (error.code,str(error))
with database.locked() as db:
    assert db.execute('SELECT count(*) FROM question_definitions').fetchone()[0]==count
    assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
database.close()
assert (directory/'records.sqlite').stat().st_size<=512*1024
print(json.dumps(dict(committed=count,database_bytes=(directory/'records.sqlite').stat().st_size)))
""")
    assert 0 < result["committed"] < 64 and result["database_bytes"] <= 512*1024


def test_second_controller_cannot_take_live_writer_lock(tmp_path):
    result = probe(tmp_path, """
guard=FileSizeGuard(database_bytes=1024*1024,wal_bytes=512*1024).install()
database=Database(directory,guard=guard)
try:
    second_guard=FileSizeGuard(database_bytes=1024*1024,wal_bytes=512*1024).install()
    Database(directory,guard=second_guard)
    raise AssertionError('second writer accepted')
except BlockingIOError:
    pass
database.close()
print(json.dumps(dict(second_writer='refused')))
""")
    assert result["second_writer"] == "refused"


@pytest.mark.parametrize('inherited', [(-1, -1), (4*1024*1024, 8*1024*1024), (512*1024, 768*1024)])
def test_trusted_guard_preserves_account_limits_in_all_scopes_and_child(tmp_path, inherited):
    result = probe(tmp_path, """
import os, subprocess
original="""+repr(inherited)+"""
resource.setrlimit(resource.RLIMIT_FSIZE, original)
guard=FileSizeGuard(database_bytes=1024*1024,wal_bytes=256*1024,preserve_hard_limit=True).install()
assert guard.inherited_limits==original
normal=(256*1024,original[1])
assert resource.getrlimit(resource.RLIMIT_FSIZE)==normal
database=Database(directory,guard=guard)
ceiling=min([1024*1024]+[value for value in original if value!=-1])
for scope in (guard.checkpoint,guard.object_write):
    try:
        with scope():
            assert resource.getrlimit(resource.RLIMIT_FSIZE)==(ceiling,original[1])
            try:guard.check()
            except ForgeError:pass
            else:raise AssertionError('DB guard accepted an elevated scope')
            raise RuntimeError('controlled IO failure')
    except RuntimeError:pass
    guard.check()
    assert resource.getrlimit(resource.RLIMIT_FSIZE)==normal
with database.transaction(payload_bytes=4096) as db:
    db.execute('INSERT INTO question_definitions VALUES (?,?,?)',('p','retained',b'value'))
with guard.object_write():
    (directory.parent/'opaque.bin').write_bytes(b'x'*(384*1024))
child_code='''import pathlib,resource,sys
limits=tuple(map(int,sys.argv[1:3]));resource.setrlimit(resource.RLIMIT_FSIZE,limits)
pathlib.Path(sys.argv[3]).write_bytes(b'x'*(384*1024))
print('child-restored')
'''
child=subprocess.run([sys.executable,'-B','-c',child_code,*map(str,guard.inherited_limits),str(directory.parent/'child.bin')],capture_output=True,text=True,timeout=5)
assert child.returncode==0,child.stderr
assert child.stdout.strip()=='child-restored'
assert resource.getrlimit(resource.RLIMIT_FSIZE)==normal
with database.locked() as db:
    assert db.execute('SELECT count(*) FROM question_definitions').fetchone()[0]==1
    assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
database.close();guard.check()
print(json.dumps(dict(inherited=original,parent=resource.getrlimit(resource.RLIMIT_FSIZE),child='restored',sqlite='ok')))
""")
    assert tuple(result['inherited']) == inherited
    assert tuple(result['parent']) == (256*1024, inherited[1])
    assert result['child'] == 'restored' and result['sqlite'] == 'ok'
