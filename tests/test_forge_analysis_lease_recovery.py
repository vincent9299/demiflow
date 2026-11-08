"""Real durable lease/startup-lock files; fake cgroup/runtime, not OCI proof."""
import os
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError


def fixture(tmp_path, *, absent=False):
    identifier="j"+"a"*32;owned=tmp_path/identifier;owned.mkdir()
    unit=owned/"startup.lock";unit.write_bytes(b"");unit.chmod(0o600);info=unit.stat()
    root=dict(device=1,inode=2);jobs=dict(device=1,inode=3);leaf=dict(device=1,inode=4)
    state=dict(absent=absent)
    def remove(identifier,original):
        assert original==leaf and not state["absent"];state["absent"]=True
    groups=SimpleNamespace(root_identity=root,jobs_identity=jobs,_root=lambda:None,
        absent=lambda _:state["absent"],populated=lambda *a:False,remove_empty=Mock(side_effect=remove))
    value=dict(format=leases.FORMAT,job_id=identifier,role="native",state="cleanup_pending",project="w"+"0"*32,
        execution_id="owned-operation",controller=process_identity(os.getpid()),web_parent=process_identity(os.getpid()),
        owner_digest="c"*64,
        delegation_identity=root,jobs_identity=jobs,cgroup=dict(job_id=identifier,role="native",cgroup_identity=leaf,
            delegation_identity=root,jobs_identity=jobs),unit_identity=dict(device=info.st_dev,inode=info.st_ino),
        init_identity=None,reason="native_ended",deadline=time.monotonic()+60)
    runtime=Runtime.__new__(Runtime);runtime.groups=groups;runtime.root=tmp_path;runtime.leases={};runtime.pidfds={}
    runtime._command=Mock(return_value=[])
    runtime._save(value)
    return runtime,identifier,state


def cleanup(runtime,identifier):
    # Process/runtime witnesses are synthetic above; the inherited unit lock
    # file and durable lease writes here are real. No cgroup/kernel is created.
    return runtime.cleanup(identifier,close_bridges=lambda _:True,
        clean_private_io=lambda _:not any((runtime.root/identifier/name).exists() for name in ("broker","job","bundle")))


def test_final_save_failure_after_cgroup_removal_recovers_only_from_prior_durable_stage(tmp_path,monkeypatch):
    runtime,identifier,state=fixture(tmp_path)
    save=runtime._save
    def lose_final(value):
        if value["state"]=="cleanup_complete":
            raise OSError("control write lost after cgroup removal")
        save(value)
    monkeypatch.setattr(runtime,"_save",lose_final)
    with pytest.raises(OSError,match="write lost"):
        cleanup(runtime,identifier)
    assert state["absent"]
    assert runtime.leases[identifier]["state"]=="cleanup_removing_cgroup"
    persisted=leases.load(tmp_path,identifier)
    assert persisted["state"]=="cleanup_removing_cgroup"
    monkeypatch.setattr(runtime,"_save",save)
    # A reconstructed index uses the same durable stage, never just absence.
    runtime.leases={identifier:persisted}
    assert cleanup(runtime,identifier)==dict(job_id=identifier,cleanup_proven=True)
    assert leases.load(tmp_path,identifier)["state"]=="cleanup_complete"
    runtime.groups.remove_empty.assert_called_once()


def test_unexplained_missing_cgroup_or_changed_delegation_never_claims_cleanup(tmp_path):
    runtime,identifier,_=fixture(tmp_path,absent=True)
    with pytest.raises(ForgeError,match="without its durable cleanup witness"):
        cleanup(runtime,identifier)
    runtime._command.assert_not_called()
    assert leases.load(tmp_path,identifier)["state"]=="cleanup_pending"
    runtime.leases[identifier]["jobs_identity"]={"device":1,"inode":999}
    with pytest.raises(ForgeError,match="another cgroup delegation"):
        cleanup(runtime,identifier)
    runtime.groups.remove_empty.assert_not_called()
