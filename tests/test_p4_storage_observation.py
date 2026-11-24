"""Stable native namespace proof across SQLite opens and host traversal."""
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from demiflow.execution.journal.run_reader import observe_storage, RunReadError
from test_forge_history_admission import run_fixture


def test_normal_snapshot_and_borrowed_descriptor(tmp_path):
    target,_=run_fixture(tmp_path)
    fd=os.open(target,os.O_RDONLY|os.O_DIRECTORY)
    before=len(os.listdir('/proc/self/fd'))
    try:
        with observe_storage(directory_fd=fd) as observed:
            assert observed.exists
            assert observed.snapshot()['state']['status']=='prepared'
            assert observed.native_footprint()['relative_roots']==('state','materializations','scratch')
            assert observed.change_token
        assert os.fstat(fd) and len(os.listdir('/proc/self/fd'))<=before
    finally:
        os.close(fd)


@pytest.mark.parametrize('restore',[False,True])
def test_main_replacement_inside_sqlite_open_is_refused_even_aba(tmp_path,monkeypatch,restore):
    target,_=run_fixture(tmp_path)
    original=sqlite3.connect; done=[]
    def connect(*args,**kw):
        if not done:
            done.append(True)
            main=target/'run.sqlite'; saved=target/'saved.sqlite'
            main.rename(saved)
            shutil.copyfile(saved,main)
            if restore:
                main.unlink();saved.rename(main)
        return original(*args,**kw)
    before=len(os.listdir('/proc/self/fd'))
    with pytest.raises(RunReadError,match='identity changed'):
        with observe_storage(target) as observation:
            monkeypatch.setattr(sqlite3,'connect',connect)
            observation.snapshot()
    assert len(os.listdir('/proc/self/fd'))==before


def test_missing_appeared_removed_aba_and_ancestor_rename(tmp_path):
    target=tmp_path/'empty';target.mkdir()
    with pytest.raises(RunReadError,match='identity changed'):
        with observe_storage(target) as observation:
            assert not observation.exists
            (target/'run.sqlite').touch();(target/'run.sqlite').unlink()
    target,_=run_fixture(tmp_path)
    with pytest.raises(RunReadError,match='identity changed'):
        with observe_storage(target):
            parent=target.parent; renamed=parent.with_name('moved')
            parent.rename(renamed);renamed.rename(parent)


def test_wal_reads_committed_uncheckpointed_rows_without_rewriting_facts(tmp_path):
    target,_=run_fixture(tmp_path)
    writer=sqlite3.connect(target/'run.sqlite')
    try:
        assert writer.execute('PRAGMA journal_mode=WAL').fetchone()[0]=='wal'
        writer.execute("UPDATE execution_state SET document=json_set(document,'$.status','failed')")
        writer.commit()
        main=(target/'run.sqlite').read_bytes();wal=(target/'run.sqlite-wal').read_bytes()
        with observe_storage(target) as observation:
            assert observation.snapshot()['state']['status']=='failed'
        assert (target/'run.sqlite').read_bytes()==main
        assert (target/'run.sqlite-wal').read_bytes()==wal
        # SQLite's SHM read marks are intentionally not a byte-invariance claim.
    finally:
        writer.close()


def test_original_error_preserved_and_descriptors_closed(tmp_path):
    target,_=run_fixture(tmp_path)
    before=len(os.listdir('/proc/self/fd'))
    with pytest.raises(ValueError,match='body') as error:
        with observe_storage(target):
            (target/'run.sqlite').rename(target/'saved.sqlite')
            raise ValueError('body')
    assert any('Storage observation' in note for note in error.value.__notes__)
    assert len(os.listdir('/proc/self/fd'))==before


@pytest.mark.parametrize('name',['run.sqlite-wal','run.sqlite-journal','run.sqlite-shm'])
def test_coordination_sidecars_cannot_be_symlinks_or_special_files(tmp_path,name):
    target,_=run_fixture(tmp_path)
    external=tmp_path/'external';external.write_text('untouched')
    (target/name).symlink_to(external)
    with pytest.raises(RunReadError,match='regular'):
        with observe_storage(target):
            pass
    assert external.read_text()=='untouched'


def test_overflow_and_watch_setup_failure_refuse_and_close(tmp_path,monkeypatch):
    import struct
    from demiflow.execution.journal import run_reader
    target,_=run_fixture(tmp_path)
    original=os.read
    with pytest.raises(RunReadError,match='identity changed'):
        with observe_storage(target) as observation:
            descriptor=observation._watch_fd
            monkeypatch.setattr(os,'read',lambda fd,n: struct.pack('iIII',-1,0x4000,0,0) if fd==descriptor else original(fd,n))
            observation.snapshot()
    monkeypatch.setattr(os,'read',original)
    before=len(os.listdir('/proc/self/fd'))
    def fail(*args):
        raise RunReadError('injected watch unavailable')
    monkeypatch.setattr(run_reader._StorageObservation,'_watch',fail)
    with pytest.raises(RunReadError,match='unavailable'):
        with observe_storage(target):
            pass
    assert len(os.listdir('/proc/self/fd'))<=before


def test_valid_in_place_writer_and_narrow_footprint(tmp_path):
    target,_=run_fixture(tmp_path)
    with observe_storage(target) as observation:
        assert observation.snapshot()['state']['status']=='prepared'
        with sqlite3.connect(target/'run.sqlite') as db:
            db.execute("UPDATE execution_state SET document=json_set(document,'$.status','failed')")
        assert observation.snapshot()['state']['status']=='failed'
        with sqlite3.connect(target/'run.sqlite') as db:
            db.execute("UPDATE manifest SET document='broken'")
        assert observation.native_footprint()['object_roots']==()
