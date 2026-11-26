"""Runtime command/config closure only; no OCI runtime is launched."""
from pathlib import Path
from contextlib import contextmanager
import fcntl
import json
import os
import pytest

from demiflow.workbench.analysis.runtime import runtime_arguments
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError


def test_oci_commands_have_fixed_nonce_scope_and_no_arbitrary_exec_restore_or_hooks(tmp_path):
    identifier="j"+"a"*32
    args=runtime_arguments('/fixed/runc',tmp_path/'runc',tmp_path/'log','create',identifier,
        bundle_path=tmp_path/'bundle',pid_file=tmp_path/'init.pid')
    assert args == ['/fixed/runc','--root',str(tmp_path/'runc'),'--rootless','true','--log',str(tmp_path/'log'),
                    '--log-format','json','create','--bundle',str(tmp_path/'bundle'),'--pid-file',str(tmp_path/'init.pid'),identifier]
    assert runtime_arguments('/fixed/runc',tmp_path/'runc',tmp_path/'log','delete',identifier)[-3:]==['delete','--force',identifier]
    for operation in ('exec','run','restore','update','shell'):
        with pytest.raises(ForgeError):runtime_arguments('/fixed/runc',tmp_path,tmp_path/'log',operation,identifier)
    with pytest.raises(ForgeError):runtime_arguments('/fixed/runc',tmp_path,tmp_path/'log','start','../../foreign')
    with pytest.raises(ForgeError):runtime_arguments('/fixed/runc',tmp_path,tmp_path/'log','start',identifier,bundle_path=tmp_path)


def test_kernel_and_native_configs_do_not_accept_argv_env_or_host_paths():
    Runtime._config('kernel',dict(session_id='session-one',generation='generation-one'))
    for field in ('argv','env','rootfs','mounts'):
        with pytest.raises(ForgeError):
            Runtime._config('kernel',dict(session_id='session-one',generation='generation-one',**{field:'untrusted'}))
    with pytest.raises(ForgeError):Runtime._config('native',dict(operation='python',source='print(1)'))
    with pytest.raises(ForgeError):Runtime._config('root-shell',{})


def test_unadmitted_deployment_refuses_before_runtime_cgroup_or_filesystem_changes(tmp_path):
    with pytest.raises(ForgeError,match='admitted external deployment'):
        Runtime(None,register_watch=lambda _:True)
    assert not list(tmp_path.iterdir())


def test_owned_stop_still_targets_original_group_when_control_intent_cannot_be_saved():
    from types import SimpleNamespace
    from unittest.mock import Mock
    identifier="j"+"a"*32
    root=dict(device=1,inode=2); jobs=dict(device=1,inode=3); leaf=dict(device=1,inode=4)
    groups=SimpleNamespace(root_identity=root,jobs_identity=jobs,kill=Mock(),_root=lambda:None)
    runtime=Runtime.__new__(Runtime);runtime.groups=groups
    value=dict(job_id=identifier,role="native",state="started",reason=None,
        delegation_identity=root,jobs_identity=jobs,
        cgroup=dict(job_id=identifier,role="native",delegation_identity=root,jobs_identity=jobs,cgroup_identity=leaf))
    runtime.leases={identifier:value}
    runtime._save=Mock(side_effect=OSError("owned control volume full"))
    with pytest.raises(OSError,match="full"):
        runtime.stop(identifier,reason="user_stop")
    groups.kill.assert_called_once_with(identifier,leaf)
    assert runtime.leases[identifier]["state"]=="started"  # failed write cannot publish a new in-memory fact
    groups.kill.reset_mock();value["cgroup"]["jobs_identity"]={"device":1,"inode":99}
    with pytest.raises(ForgeError,match="another cgroup delegation"):
        runtime.stop(identifier,reason="user_stop")
    groups.kill.assert_not_called()


def test_controller_constructor_never_adopts_a_missing_external_binding(tmp_path):
    from demiflow.workbench.analysis.controller import Controller
    with pytest.raises(ForgeError,match="admitted external deployment"):
        Controller(None)
    assert not list(tmp_path.iterdir())


@contextmanager
def observed_local_unit(tmp_path):
    # Real status files/startup flock/process identity; the test's own PID is
    # an explicit ownership witness. No worker, admission or cleanup runs.
    from demiflow.workbench.analysis.trusted_local import TrustedLocalBackend
    from demiflow.forge.process import process_identity
    identifier='j'+'b'*32;owned=tmp_path/identifier;owned.mkdir()
    path=owned/'startup.lock';path.touch();descriptor=os.open(path,os.O_RDWR)
    fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB);info=os.fstat(descriptor)
    runtime=TrustedLocalBackend.__new__(TrustedLocalBackend)
    runtime.root=tmp_path;runtime.processes={};runtime.worker_identities={};runtime.stop_fences=set()
    runtime.pending_retirement=None;runtime.retiring_reads=set()
    parent=process_identity(os.getpid())
    runtime.leases={identifier:dict(state='started',init_identity=parent,context_sha256='a'*64,
        unit_identity=dict(device=info.st_dev,inode=info.st_ino))}
    try:yield runtime,identifier,owned,parent
    finally:os.close(descriptor)


def test_missing_hot_status_does_not_turn_a_live_original_unit_into_a_dead_process(tmp_path):
    with observed_local_unit(tmp_path) as (runtime,identifier,owned,parent):
        assert runtime.observe(identifier)==dict(state='alive',reason=None,status=None)
        # Missing status cannot prove an exit or free any capacity once the
        # original wrapper identity is unavailable.
        runtime.leases[identifier]['init_identity']={**parent,'start':'different-start'}
        assert runtime.observe(identifier)['state']=='unknown'
        assert runtime.leases[identifier]['state']=='started'


def test_only_validated_original_worker_survives_a_missing_status_window(tmp_path):
    from demiflow.workbench.analysis.ipc import Peer
    with observed_local_unit(tmp_path) as (runtime,identifier,owned,parent):
        peer=Peer(os.getpid(),os.geteuid(),os.getegid(),parent)
        assert runtime.accept_peer(identifier,peer,init_only=True) is False
        status=dict(job_id=identifier,context_sha256='a'*64,wrapper=parent,worker=parent,
            state='running',reason=None,peak_sampled_rss_bytes=0,descendants_exited=False)
        (owned/'status.json').write_text(json.dumps(status))
        assert runtime.accept_peer(identifier,peer,init_only=True) is True
        (owned/'status.json').unlink()
        assert runtime.accept_peer(identifier,peer,init_only=True) is True
        runtime.leases[identifier]['init_identity']={**parent,'start':'different-start'}
        assert runtime.accept_peer(identifier,peer,init_only=True) is False
        assert runtime.observe(identifier)['state']=='unknown'
