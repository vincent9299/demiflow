"""Owned scratch cleanup may race quota observation; durable receipts stay strict."""
from pathlib import Path
import os

import pytest
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


def test_completed_decoder_file_disappears_before_storage_stat(tmp_path,monkeypatch):
    with RunContext.start(tmp_path/'run') as run:
        folder=run.directory/'scratch';folder.mkdir()
        transient=folder/'decoder-result.json'
        run.allocate_artifact(transient, size=8, atomic=False)
        transient.write_bytes(b'finished')
        original=Path.lstat;removed=[]
        def stat_after_cleanup(path,*a,**kw):
            if path==transient and not removed:
                transient.unlink();removed.append(True)
            return original(path,*a,**kw)
        monkeypatch.setattr(Path,'lstat',stat_after_cleanup)
        assert run.journal.storage_usage()==8
        run.release_temporaries([transient])
        assert run.journal.storage_usage()==0
        assert removed==[True]


def test_quota_observation_does_not_enumerate_completed_decoder_directories(tmp_path,monkeypatch):
    with RunContext.start(tmp_path/'run') as run:
        folder=run.directory/'scratch';folder.mkdir()
        transient=folder/'decoder-directory';transient.mkdir()
        transient.rmdir()
        def forbidden_scan(path):
            raise AssertionError('Quota observation scanned historical files')
        monkeypatch.setattr(Path,'iterdir',forbidden_scan)
        assert run.journal.storage_usage()==0


@pytest.mark.parametrize('kind',['symlink','fifo','permission'])
def test_owned_temporary_release_keeps_type_and_permission_failures(tmp_path,monkeypatch,kind):
    with RunContext.start(tmp_path/'run') as run:
        folder=run.directory/'scratch';folder.mkdir()
        path=folder/'entry'
        run.allocate_artifact(path, size=1, atomic=False)
        foreign=tmp_path/'foreign.bin';foreign.write_bytes(b'unchanged')
        if kind=='symlink':path.symlink_to(foreign)
        elif kind=='fifo':os.mkfifo(path)
        else:
            path.write_bytes(b'x')
            original=Path.lstat
            def reject(value,*a,**kw):
                if value==path:raise PermissionError('controlled read failure')
                return original(value,*a,**kw)
            monkeypatch.setattr(Path,'lstat',reject)
        with pytest.raises(PermissionError if kind=='permission' else RecoveryContractError):
            run.release_temporaries([path])
        assert foreign.read_bytes()==b'unchanged'
        monkeypatch.undo()
        path.unlink()
        run.release_temporaries([path])
