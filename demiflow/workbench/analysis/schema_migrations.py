"""Additive, exact-revision private SQLite upgrades before request admission.

Unknown schemas remain readonly. Existing event/document payloads are never
rewritten; one unused, already-budgeted backup slot preserves the old snapshot.
"""
import os
import sqlite3
import time
from contextlib import closing

from demiflow.workbench.analysis import contracts as c

LEGACY={1:"c4a523b5ff9c74faa2d995d3548fdbd03750d88eae978a1436ea5ffe2380718a",
        2:"6991b2cff43241057e52ef2d98a18ef6d2dab983d665b2202c374e628bba4c69",
        3:"4cb11a85db8e0393d80a73683e2623b5d0990360752cdbebf03af8421ba57d53"}


def dependencies(core):
    return core[core.index("CREATE TABLE analysis_execution_dependencies"):core.index("PRAGMA user_version=2;")]


def blueprint(version,core_v2,current,core_v3=None):
    if version==4:return current
    if version==3:
        if core_v3 is None:c.reject("Known v3 schema blueprint is required")
        return core_v3
    if version==2:return core_v2
    if version==1:return core_v2.replace(dependencies(core_v2),"").replace("user_version=2","user_version=1")
    c.reject("Unknown analysis database version; no migration attempted")


def _shape(connection):
    rows=connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name LIMIT 65").fetchall()
    if len(rows)>64 or sum(len(row[3] or "") for row in rows)>64*1024:c.reject("Analysis schema exceeds its closed metadata budget",code="resource_limit")
    return [tuple(row) for row in rows]


def validate(connection,version,*,core_v2,current,current_digest,core_v3=None):
    expected_digest=current_digest if version==4 else LEGACY.get(version)
    if expected_digest is None:c.reject("Unknown analysis database version; no migration attempted")
    try:rows=connection.execute("SELECT revision_sha256 FROM analysis_schema LIMIT 2").fetchall()
    except sqlite3.Error:c.reject("Analysis schema revision differs; no migration attempted")
    if [tuple(row) for row in rows]!=[(expected_digest,)]:c.reject("Analysis schema revision differs; no migration attempted")
    with closing(sqlite3.connect(':memory:')) as expected:
        expected.executescript(blueprint(version,core_v2,current,core_v3))
        if _shape(connection)!=_shape(expected):c.reject("Analysis physical schema differs from its registered revision; no migration attempted")


def _backup(connection,directory,maximum_bytes):
    target=next((directory/name for name in ('backup.sqlite','backup.new.sqlite') if not (directory/name).exists()),None)
    if target is None:c.reject("Reviewed schema migration needs one free reserved backup slot; existing backups were preserved",code="resource_limit")
    fd=os.open(target,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600);os.close(fd)
    started=time.monotonic()
    def progress(status,remaining,total):
        if total*4096>maximum_bytes or time.monotonic()-started>20:c.reject("Schema backup exceeds its admitted size/time; original schema retained",code="resource_limit")
    # Interrupted/unknown backups are retained, never silently overwritten.
    with closing(sqlite3.connect(target)) as output:connection.backup(output,pages=128,progress=progress,sleep=.01)
    fd=os.open(target,os.O_RDONLY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
    fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
    return target


def migrate(connection,directory,version,*,core_v2,additions,current,current_digest,maximum_bytes,core_v3=None,generation_additions=""):
    validate(connection,version,core_v2=core_v2,core_v3=core_v3,current=current,current_digest=current_digest)
    pages=connection.execute('PRAGMA page_count').fetchone()[0]
    if (pages+32)*4096>maximum_bytes:c.reject("Schema migration lacks bounded metadata page headroom",code="resource_limit")
    _backup(connection,directory,maximum_bytes)
    sql=(dependencies(core_v2) if version==1 else "")+(additions if version in {1,2} else "")+generation_additions
    try:
        connection.executescript('BEGIN IMMEDIATE;'+sql)
        if connection.execute('PRAGMA user_version').fetchone()[0]!=version:c.reject("Original schema changed during migration",code="conflict")
        changed=connection.execute('UPDATE analysis_schema SET revision_sha256=? WHERE revision_sha256=?',(current_digest,LEGACY[version])).rowcount
        if changed!=1:c.reject("Original schema revision changed during migration",code="conflict")
        connection.execute('PRAGMA user_version=4')
        validate(connection,4,core_v2=core_v2,core_v3=core_v3,current=current,current_digest=current_digest)
        connection.execute('COMMIT')
    except BaseException:
        if connection.in_transaction:connection.execute('ROLLBACK')
        raise
