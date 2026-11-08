"""Real owner file/Unix output with fake grants/cgroups; not B1 stop evidence."""
import os
import socket
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import priority_control
from demiflow.workbench.analysis import watchdog_service
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.framing import Frames
from demiflow.forge.schema import ForgeError


def authority():
    return SimpleNamespace(project="w"+"1"*32,principal="owner",
        peer=Peer(os.getpid(),os.geteuid(),os.getegid(),process_identity(os.getpid())))


def fixture(tmp_path,monkeypatch):
    a=authority();digest=priority_control.publish(tmp_path,a,"session","generation")
    calls=[]
    class Exchange:
        def __init__(self,owner,operation,payload):
            calls.append((operation,payload));self.done=False;self.value=None;self.closed=False
        def advance(self):return self.done
        def close(self):self.closed=True
    monkeypatch.setattr(priority_control,"Exchange",Exchange)
    service=watchdog_service.WatchdogService.__new__(watchdog_service.WatchdogService)
    service.root=tmp_path;service.owner=priority_control.load(tmp_path);service.stop_requested=False
    service.last_cleared=None;service.profile_sha256="c"*64
    service.deployment=SimpleNamespace(value=dict(owner_release=dict(helper_sha256="b"*64)),
        paths=SimpleNamespace(delegated_cgroup=tmp_path))
    service.groups=SimpleNamespace(kill=Mock(),root_identity=dict(device=1,inode=10),jobs_identity=dict(device=1,inode=11),absent=Mock(return_value=True))
    service.poll=SimpleNamespace(modify=Mock())
    own=dict(format=leases.FORMAT,owner_digest=digest,job_id="j"+"a"*32,state="started",role="native",execution_id="operation",
        delegation_identity=service.groups.root_identity,jobs_identity=service.groups.jobs_identity,
        cgroup=dict(cgroup_identity=dict(device=1,inode=2),job_id="j"+"a"*32,role="native",
            delegation_identity=service.groups.root_identity,jobs_identity=service.groups.jobs_identity),
        unit_identity=dict(device=1,inode=30),init_identity=None,reason=None,deadline=time.monotonic()+60,
        controller=a.peer.identity,web_parent=a.peer.identity,project=a.project)
    watch=Mock()
    watch.tick.side_effect=lambda **kw: (_ for _ in ()).throw(AssertionError("fence must precede kill")) if kw.get("stop_requested") and not service.stop_requested else None
    service.watches={own["job_id"]:dict(lease=own,watch=watch)}
    left,right=socket.socketpair()
    item=dict(channel=Channel(left),peer=a.peer,kind="priority",stop=None,responded=False)
    service.connections={left.fileno():item}
    return service,item,right,calls,watch


def request(service,item):
    service._priority_request(item,dict(version=1,project=service.owner["project"],operation="stop-generation",
        payload=dict(request_id="stop",session_id="session",generation="generation")))


def finish_stage(service,item,value):
    item["stop"].exchange.value=value;item["stop"].exchange.done=True
    service._advance_stops()


def receive(item,right):
    right.settimeout(1)
    for _ in range(4):item["channel"].flush()
    return Frames(right).json()


def close(item,right):
    if item["stop"] is not None:item["stop"].close()
    item["channel"].close();right.close()


def test_current_principal_and_permission_then_stop_fences_new_jobs_without_db_or_file_write(tmp_path,monkeypatch):
    service,item,right,calls,watch=fixture(tmp_path,monkeypatch)
    before=(tmp_path/priority_control.NAME).read_bytes()
    try:
        request(service,item)
        service._advance_stops();watch.tick.assert_not_called()
        finish_stage(service,item,dict(principal="owner"));watch.tick.assert_not_called()
        assert calls==[("whoami",{}),("permission",dict(permission="analysis_execute"))]
        finish_stage(service,item,dict(allowed=True))
        assert service.stop_requested
        watch.tick.assert_called_once_with(stop_requested=True)
        value=receive(item,right)
        assert value["accepted"] and value["force"] and not value["cleanup_proven"]
        assert value["generation"]=="generation" and value["request_id"]=="stop"
        assert (tmp_path/priority_control.NAME).read_bytes()==before
        # A late runtime create/start acknowledgement cannot revive this owner.
        lease=next(iter(service.watches.values()))["lease"]
        with pytest.raises(ForgeError,match="stop fence"):service._watch(lease)
        service.groups.kill.assert_called_once_with(lease["job_id"],dict(device=1,inode=2))
    finally:close(item,right)


@pytest.mark.parametrize("first,value",[(True,dict(principal="other")),(False,dict(allowed=False))])
def test_changed_principal_or_revocation_never_stops_a_generation(tmp_path,monkeypatch,first,value):
    service,item,right,_,watch=fixture(tmp_path,monkeypatch)
    try:
        request(service,item)
        if not first:finish_stage(service,item,dict(principal="owner"))
        finish_stage(service,item,value)
        assert receive(item,right)["code"]=="operation_rejected"
        assert not service.stop_requested;watch.tick.assert_not_called()
    finally:close(item,right)


def test_late_grant_for_a_replaced_owner_cannot_target_its_successor(tmp_path,monkeypatch):
    service,item,right,_,watch=fixture(tmp_path,monkeypatch)
    try:
        request(service,item);finish_stage(service,item,dict(principal="owner"))
        service.owner={**service.owner,"generation":"other"}
        finish_stage(service,item,dict(allowed=True))
        assert receive(item,right)["code"]=="conflict"
        assert not service.stop_requested;watch.tick.assert_not_called()
    finally:close(item,right)


def test_owner_replacement_and_clear_require_original_scope_and_completed_watches(tmp_path,monkeypatch):
    service,item,right,_,_=fixture(tmp_path,monkeypatch)
    try:
        a=authority();digest=c.digest(service.owner)
        with pytest.raises(ForgeError,match="cleanup before replacement"):
            priority_control.publish(tmp_path,a,"session","new-generation")
        monkeypatch.setattr(watchdog_service,"read_control",lambda *args:str(os.getpid()))
        service._tick_watches=lambda:None
        with pytest.raises(ForgeError,match="unresolved"):
            service._owner_control(a.peer,"clear-owner",digest)
        service.watches.clear()
        assert service._owner_control(a.peer,"clear-owner",digest)==dict(owner_digest=digest,bound=False,stop_requested=False)
        # Clearing independent watches does not itself erase the controller's
        # owner proof or assert process/mount/file cleanup to a public client.
        assert priority_control.load(tmp_path)["generation"]=="generation"
    finally:close(item,right)


def test_live_stop_status_and_parent_deadline_ticks_do_not_read_the_state_volume(tmp_path,monkeypatch):
    service,item,right,_,watch=fixture(tmp_path,monkeypatch)
    def forbidden(*args,**kwargs):raise AssertionError("independent loop must not wait on persistent state IO")
    monkeypatch.setattr(watchdog_service,"load_lease",forbidden)
    monkeypatch.setattr(watchdog_service,"load_owner",forbidden)
    monkeypatch.setattr(watchdog_service,"profile",forbidden)
    monkeypatch.setattr(watchdog_service,"read_control",lambda *args:str(os.getpid()))
    try:
        service._tick_watches()
        assert service._owner_control(item["peer"],"owner-status",c.digest(service.owner))["bound"]
        request(service,item);finish_stage(service,item,dict(principal="owner"))
        finish_stage(service,item,dict(allowed=True))
        assert receive(item,right)["accepted"]
        service._tick_watches()
        assert service.stop_requested
    finally:close(item,right)


def test_explicit_watch_cleanup_requires_same_closed_lease_and_absent_original_leaf(tmp_path,monkeypatch):
    service,item,right,_,watch=fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(watchdog_service,"read_control",lambda *args:str(os.getpid()))
    try:
        original=next(iter(service.watches.values()))["lease"]
        completed={**original,"state":"cleanup_complete"}
        assert leases.validate(completed)==completed
        service.groups.absent.return_value=False
        with pytest.raises(ForgeError,match="absent leaf"):service._clean_watch(item["peer"],completed)
        service.groups.absent.return_value=True
        with pytest.raises(ForgeError,match="replace"):
            service._clean_watch(item["peer"],{**completed,"owner_digest":"f"*64})
        assert service.watches;watch.close.assert_not_called()
        service._clean_watch(item["peer"],completed)
        assert service.watches=={};watch.close.assert_called_once()
    finally:close(item,right)
