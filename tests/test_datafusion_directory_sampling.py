"""Temporary files can disappear during the resource guard's traversal."""
from pathlib import Path
import shutil

import pytest
from demiflow.execution import datafusion as engine


def test_retired_spill_directory_does_not_hide_surviving_files(tmp_path,monkeypatch):
    retiring=tmp_path/'retiring';retiring.mkdir();(retiring/'old').write_bytes(b'old')
    remaining=tmp_path/'remaining';remaining.mkdir();(remaining/'active').write_bytes(b'1234567')
    original=engine.os.scandir
    removed=[]
    def race(path):
        if not isinstance(path,int) and Path(path)==retiring:
            shutil.rmtree(retiring);removed.append(str(path))
        return original(path)
    monkeypatch.setattr(engine.os,'scandir',race)
    assert engine._directory_bytes(tmp_path)==7
    assert removed==[str(retiring)]


def test_io_failure_is_not_counted_as_empty(tmp_path,monkeypatch):
    def denied(path):raise PermissionError('cannot inspect disk usage')
    monkeypatch.setattr(engine.os,'scandir',denied)
    with pytest.raises(PermissionError):engine._directory_bytes(tmp_path)


def test_scan_limits_fail_instead_of_returning_partial_usage(tmp_path):
    (tmp_path/'a').write_bytes(b'1');(tmp_path/'b').write_bytes(b'22')
    with pytest.raises(RuntimeError,match='scan budget'):
        engine._directory_bytes(tmp_path,max_entries=1)
    sub=tmp_path/'nested';sub.mkdir();(sub/'file').write_bytes(b'3')
    with pytest.raises(RuntimeError,match='depth limit'):
        engine._directory_bytes(tmp_path,max_depth=1)
    with pytest.raises(RuntimeError,match='scan budget'):
        engine._directory_bytes(tmp_path,max_seconds=0)
    assert engine._directory_bytes(tmp_path)==4


def test_scan_does_not_follow_symlinks_outside_owned_tree(tmp_path):
    owned=tmp_path/'owned';owned.mkdir()
    (tmp_path/'external').write_bytes(b'123456789')
    (owned/'alias').symlink_to(tmp_path/'external')
    (owned/'cycle').symlink_to(owned,target_is_directory=True)
    (owned/'real').write_bytes(b'123')
    assert engine._directory_bytes(owned)==3
