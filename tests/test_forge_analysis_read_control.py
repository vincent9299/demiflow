"""Independent read owner using real files/pidfd; synthetic cgroups/authority.

No kernel/OCI launch. Closed current-read control is distinct from historical
output provenance and the concurrent generation's project/permission scope.
"""
import copy
import os
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import priority_control
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis import watchdog_service
from demiflow.workbench.analysis.ipc import Peer
from demiflow.forge.io import atomic_write_json
from demiflow.forge.schema import ForgeError


class Groups:
    root_identity=dict(device=1,inode=2)
    jobs_identity=dict(device=1,inode=3)
    def __init__(self):self.alive=True;self.kills=[];self.missing=False
    def populated(self,*_):return self.alive
    def kill(self,identifier,owned):self.kills.append(identifier)
    def absent(self,identifier):return self.missing


def authority(project='w'+'2'*32):
    return SimpleNamespace(project=project,principal='reader',check=Mock(return_value=True),
        peer=Peer(os.getpid(),os.geteuid(),os.getegid(),process_identity(os.getpid())))


def artifact():return dict(id='o'+'a'*32,generation=1,digest='b'*64,sha256='c'*64,bytes=20)


@pytest.mark.parametrize('kind,denied',[('media-read','read'),('media-probe','read'),('media-probe','analysis_write')])
def test_real_authority_boolean_denial_never_publishes_owner(tmp_path,kind,denied):
    # Authority.check returns False on denial; it does not raise like Access.check.
    from demiflow.workbench.analysis.authority import Authority
    a=Authority.__new__(Authority);fixture=authority()
    a.project,a.principal,a.peer=fixture.project,fixture.principal,fixture.peer
    a.call=lambda operation,payload:dict(allowed=payload['permission']!=denied)
    target=artifact() if kind=='media-read' else dict(publish_id='p'+'d'*32,object_id='o'+'a'*32,
        seed_digest='b'*64,source_digest='c'*64,content_sha256='d'*64,config_digest='e'*64)
    with pytest.raises(ForgeError,match='permission is absent'):
        read_control._publish(tmp_path,a,'denied-owner',kind,target)
    assert not (tmp_path/read_control.NAME).exists()


def lease(owner,role='media',nonce='b'):
    group=dict(job_id='j'+nonce*32,role=role,cgroup_identity=dict(device=1,inode=4),
               delegation_identity=Groups.root_identity,jobs_identity=Groups.jobs_identity)
    media=role=='media'
    result=dict(format=leases.MEDIA_FORMAT if media else leases.FORMAT,job_id=group['job_id'],role=role,state='started',
        project=owner['project'],execution_id=owner['read_request_id'] if media else 'execution',
        controller=owner['controller'],web_parent=owner['web_parent'],owner_digest=c.digest(owner),
        delegation_identity=group['delegation_identity'],jobs_identity=group['jobs_identity'],cgroup=group,
        unit_identity=dict(device=1,inode=5),init_identity=None,reason=None,
        deadline=owner['deadline'] if media else time.monotonic()+30)
    if media:result['owner_kind']='media-read'
    return leases.validate(result)


def fixture(tmp_path,monkeypatch,*,generation=True,registered_artifact=None):
    a=authority();digest=read_control.publish(tmp_path,a,'read-one',artifact() if registered_artifact is None else registered_artifact);owner=read_control.load(tmp_path)
    a.check.assert_called_once_with(a.project,a.principal,'read')
    service=watchdog_service.WatchdogService.__new__(watchdog_service.WatchdogService)
    service.root=tmp_path;service.groups=Groups();service.watches={}
    service.owner=None;service.stop_requested=False;service.last_cleared=None
    service.read_owner=None;service.read_stop_requested=False;service.last_read_cleared=None
    service.read_monitor=None;service.read_next_check=0
    service.deployment=SimpleNamespace(paths=SimpleNamespace(delegated_cgroup=tmp_path))
    monkeypatch.setattr(watchdog_service,'read_control',lambda *_:str(os.getpid()))
    monitor=Mock();monitor.advance.return_value=True
    monkeypatch.setattr(read_control,'Authorization',Mock(return_value=monitor))
    if generation:
        gen_auth=authority('w'+'1'*32)
        priority_control.publish(tmp_path,gen_auth,'live-session','newer-kernel-generation')
        service.owner=priority_control.load(tmp_path)
        service._watch(lease(service.owner,'kernel','a'))
    assert service._read_owner_control(a.peer,'bind-read-owner',digest,record=owner)['bound']
    return service,a,owner,monitor


def close(service):
    for item in service.watches.values():item['watch'].close()
    if service.read_monitor is not None:service.read_monitor.close()


@pytest.mark.parametrize('generation',[False,True])
def test_history_read_owner_requires_no_kernel_and_can_coexist_with_other_project(tmp_path,monkeypatch,generation):
    service,a,owner,monitor=fixture(tmp_path,monkeypatch,generation=generation)
    try:
        before=service.owner
        item=lease(owner);service._watch(item)
        service._tick_watches()
        assert service.owner==before and not service.stop_requested
        assert len(service.watches)==(2 if generation else 1)
        assert not service.groups.kills
        assert read_control.Authorization.call_args.args[1]['project']==a.project
        # One global media slot, even across another project or read ID.
        with pytest.raises(ForgeError,match='slots'):
            service._watch({**item,'job_id':'j'+'c'*32,'cgroup':{**item['cgroup'],'job_id':'j'+'c'*32}})
    finally:close(service)


def test_independent_read_revocation_and_stop_cannot_touch_concurrent_kernel(tmp_path,monkeypatch):
    service,a,owner,monitor=fixture(tmp_path,monkeypatch)
    try:
        item=lease(owner);service._watch(item)
        original_generation=(tmp_path/priority_control.NAME).read_bytes()
        monitor.advance.side_effect=ForgeError('read revoked')
        service._tick_watches()
        assert service.read_stop_requested and not service.stop_requested
        assert set(service.groups.kills)=={item['job_id']}
        assert (tmp_path/priority_control.NAME).read_bytes()==original_generation
        with pytest.raises(ForgeError,match='stop fence'):service._watch(item)
        assert item['job_id'] in service.watches  # still occupies media slot
    finally:close(service)


def test_original_read_identity_and_kind_cannot_be_rebound_to_generation_or_foreign_project(tmp_path,monkeypatch):
    service,a,owner,_=fixture(tmp_path,monkeypatch)
    try:
        item=lease(owner)
        wrong={**item,'project':'w'+'3'*32}
        with pytest.raises(ForgeError,match='control owner'):service._watch(wrong)
        with pytest.raises(ForgeError,match='reconciliation'):
            leases.validate({key:value for key,value in {**item,'format':leases.FORMAT}.items() if key!='owner_kind'})
        with pytest.raises(ForgeError):leases.validate({**item,'role':'kernel'})
        with pytest.raises(ForgeError):read_control.validate(service.owner)
        with pytest.raises(ForgeError,match='cleanup'):
            read_control.publish(tmp_path,a,'different-read',artifact())
        assert service.groups.kills==[]
    finally:close(service)


def test_clear_requires_cleanup_watch_not_just_empty_population_and_preserves_generation(tmp_path,monkeypatch):
    service,a,owner,_=fixture(tmp_path,monkeypatch)
    try:
        item=lease(owner);service._watch(item);digest=c.digest(owner)
        assert service._read_owner_control(a.peer,'stop-read-owner',digest)['stop_requested']
        service.groups.alive=False
        with pytest.raises(ForgeError,match='unresolved'):
            service._read_owner_control(a.peer,'clear-read-owner',digest)
        completed={**item,'state':'cleanup_complete'}
        with pytest.raises(ForgeError,match='absent leaf'):
            service._clean_watch(a.peer,completed)
        service.groups.missing=True;service._clean_watch(a.peer,completed)
        result=service._read_owner_control(a.peer,'clear-read-owner',digest)
        assert result==dict(owner_digest=digest,bound=False,stop_requested=False)
        assert service.owner['generation']=='newer-kernel-generation'
        assert read_control.load(tmp_path)==owner  # controller still owns durable cleanup
    finally:close(service)


def test_recovery_fences_read_retains_slot_and_never_rechecks_to_resume(tmp_path,monkeypatch):
    service,a,owner,_=fixture(tmp_path,monkeypatch,generation=False)
    item=lease(owner);directory=tmp_path/item['job_id'];directory.mkdir()
    atomic_write_json(directory/'lease.json',item)
    service.read_owner=None;service._recover()
    try:
        assert service.read_stop_requested and item['job_id'] in service.watches
        service._tick_watches()
        assert service.groups.kills==[item['job_id']]
        read_control.Authorization.assert_not_called()
        assert leases.load(tmp_path,item['job_id'])['state']=='started'
    finally:close(service)


def test_idempotent_publication_does_not_extend_five_second_deadline(tmp_path):
    a=authority();first=read_control.publish(tmp_path,a,'read-one',artifact())
    original=read_control.load(tmp_path);second=read_control.publish(tmp_path,a,'read-one',artifact())
    assert first==second and read_control.load(tmp_path)['deadline']==original['deadline']


@pytest.mark.parametrize('generation', [True, False, 1.0, 0, -1, 2**31, '1', 'output-generation'])
def test_artifact_generation_keeps_existing_integer_contract(tmp_path,generation):
    with pytest.raises(ForgeError):
        read_control.publish(tmp_path,authority(),'read-one',{**artifact(),'generation':generation})
    assert not (tmp_path/read_control.NAME).exists()


def test_runtime_creates_media_with_exact_read_deadline_and_never_borrows_generation(tmp_path,monkeypatch):
    from demiflow.workbench.analysis.runtime import Runtime
    from demiflow.workbench.analysis.oci import FixedPaths
    service,a,owner,_=fixture(tmp_path,monkeypatch)
    runtime=Runtime.__new__(Runtime)
    runtime.root=tmp_path;runtime.leases={};runtime.pidfds={};runtime.stop_fences=set();runtime.groups=service.groups
    runtime._config=Mock()  # output worker merges from its lane; no parsing in this control test
    fixed=FixedPaths(tmp_path/'image',tmp_path/'state',tmp_path/'cg',23456,23456)
    runtime.deployment=SimpleNamespace(paths=fixed,check_site=lambda:None)
    runtime.groups._root=lambda:None
    runtime.groups.verify=lambda *args:None
    runtime.groups.create=lambda identifier,role:dict(job_id=identifier,role=role,cgroup_identity=dict(device=1,inode=6),
        delegation_identity=Groups.root_identity,jobs_identity=Groups.jobs_identity)
    def register(value):service._watch(value);return True
    runtime.register_watch=register
    def command(value,operation):
        assert operation=='create'
        (tmp_path/value['job_id']/'init.pid').write_text(str(os.getpid()))
    runtime._command=Mock(side_effect=command)
    config=dict(version=1,operation='output-page',witness=dict(artifact=owner['artifact'],
        provenance=dict(project=owner['project']),sources={}),offset=0,limit=8)
    try:
        for field,value in [('project','w'+'4'*32),('read_request_id','foreign-read'),('deadline',time.monotonic()+20)]:
            foreign={**owner,field:value}
            with pytest.raises(ForgeError):
                runtime.create(project=owner['project'],execution_id=owner['read_request_id'],role='media',config=config,
                    web_parent=owner['web_parent'],owner_digest=c.digest(foreign),read_owner=foreign,authorize=lambda:True)
        runtime._command.assert_not_called()
        created=runtime.create(project=owner['project'],execution_id=owner['read_request_id'],role='media',config=config,
            web_parent=owner['web_parent'],owner_digest=c.digest(owner),read_owner=owner,authorize=lambda:True)
        identifier=created['job_id'];saved=leases.load(tmp_path,identifier)
        assert saved['format']==leases.MEDIA_FORMAT and saved['deadline']==owner['deadline']
        assert saved['owner_digest']==c.digest(owner) and not created['execution_started']
        # Read permission loss before start kills only this job and retains slot.
        with pytest.raises(ForgeError):runtime.start(identifier,authorize=lambda:False)
        assert service.groups.kills==[identifier]
        assert leases.load(tmp_path,identifier)['state']=='cleanup_pending'
        assert service.owner['generation']=='newer-kernel-generation'
    finally:
        for fd in runtime.pidfds.values():os.close(fd)
        close(service)


def test_generation_stop_does_not_target_independent_read_owner(tmp_path,monkeypatch):
    service,a,owner,_=fixture(tmp_path,monkeypatch)
    try:
        item=lease(owner);service._watch(item)
        task=SimpleNamespace(advance=lambda:True,owner=dict(service.owner),
            payload=dict(request_id='stop-kernel',session_id='live-session',generation='newer-kernel-generation'))
        channel=Mock();service.connections={1:dict(stop=task,responded=False,channel=channel)}
        service.poll=Mock();service.profile_sha256='c'*64
        service.deployment.value=dict(owner_release=dict(helper_sha256='d'*64))
        service._advance_stops()
        assert service.stop_requested and not service.read_stop_requested
        assert service.groups.kills==['j'+'a'*32]
        assert item['job_id'] in service.watches
    finally:close(service)
