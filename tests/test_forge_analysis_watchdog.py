"""Real pidfd, synthetic cgroup ticks; not real cgroup/namespace cleanup proof."""
import os
import subprocess
import sys
import time

import pytest

from demiflow.forge.process import process_identity
from demiflow.workbench.analysis import owned_cgroup
from demiflow.workbench.analysis.watchdog import ParentWatch
from demiflow.workbench.analysis.watchdog import JobWatch
from demiflow.forge.schema import ForgeError


def test_live_parent_pidfd_observes_exit_and_old_identity_is_never_signalled():
    child=subprocess.Popen([sys.executable,"-B","-c","import sys;sys.stdin.buffer.read(1)"],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        identity=process_identity(child.pid);assert identity is not None
        watch=ParentWatch(identity)
        assert not watch.stopped()
        child.stdin.write(b"x");child.stdin.close()
        child.wait(timeout=5)
        assert watch.stopped();watch.close()
        stale=ParentWatch(identity)
        assert stale.stopped() and stale.fd is None
        stale.close()
    finally:
        if child.poll() is None:child.kill();child.wait(timeout=5)


class Group:
    root_identity={"device":1,"inode":2}
    jobs_identity={"device":1,"inode":3}

    def __init__(self):self.alive=True;self.kills=[]
    def populated(self,identifier,identity):return self.alive
    def kill(self,identifier,identity):self.kills.append((identifier,identity))


def lease():
    return dict(job_id="j"+"a"*32,role="native",cgroup_identity={"device":1,"inode":4},
                delegation_identity=Group.root_identity,jobs_identity=Group.jobs_identity)


def test_cancel_does_not_invent_stopped_or_namespace_cleanup_and_never_blocks_other_jobs():
    group=Group();parent=process_identity(os.getpid())
    watcher=JobWatch(group,lease(),controller=parent,web_parent=parent,deadline=time.monotonic()+5)
    try:
        initial=watcher.tick();assert initial["populated"] and not initial["cleanup_pending"]
        cancelled=watcher.tick(stop_requested=True)
        assert cancelled["cleanup_pending"] and not cancelled["process_observed_stopped"]
        assert len(group.kills)==1 and group.kills[0][0]==lease()["job_id"]
        group.alive=False
        stopped=watcher.tick()
        assert stopped["process_observed_stopped"] and not stopped["namespace_file_cleanup_proven"]
    finally:watcher.close()


def test_missing_parent_or_deadline_kills_only_owned_group_and_keeps_cleanup_pending():
    parent=process_identity(os.getpid());missing=dict(pid=2**30,start="1",boot=parent["boot"])
    group=Group();watcher=JobWatch(group,lease(),controller=missing,web_parent=parent,deadline=0)
    try:
        result=watcher.tick()
        assert result["cleanup_pending"] and group.kills and not result["process_observed_stopped"]
    finally:watcher.close()


def test_wrong_delegation_and_non_cgroup_paths_refuse_before_controls(tmp_path):
    with pytest.raises(ForgeError):owned_cgroup.Cgroups(tmp_path)
    assert not list(tmp_path.iterdir())
    parent=process_identity(os.getpid());wrong=lease();wrong["jobs_identity"]={"device":1,"inode":99}
    with pytest.raises(ForgeError,match="different delegation"):
        JobWatch(Group(),wrong,controller=parent,web_parent=parent,deadline=time.monotonic()+5)
