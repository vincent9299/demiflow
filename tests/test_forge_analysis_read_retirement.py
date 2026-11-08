"""Real Runtime/files/child-pidfd/watch lifecycle; cgroup/OCI/config seam doubles.

65 sequential range requests exercise the actual32-lease admission cap. This
does not claim a decoded64MiB video or real mounted/cgroup cleanup.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis import read_retirement
from demiflow.workbench.analysis.cleanup import _sync
from demiflow.workbench.analysis.oci import FixedPaths
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError
from test_forge_analysis_read_control import fixture, close, lease, artifact


def harness(tmp_path,monkeypatch):
    service,authority,owner,_=fixture(tmp_path,monkeypatch)
    groups=service.groups;groups.live={};groups.children={};groups.leaves=set()
    groups.populated=lambda identifier,*args:groups.live.get(identifier,False)
    groups.absent=lambda identifier:identifier not in groups.leaves
    groups._root=lambda:None
    groups.verify=lambda *args:None
    def create(identifier,role):
        groups.leaves.add(identifier);groups.live[identifier]=False
        return dict(job_id=identifier,role=role,cgroup_identity=dict(device=1,inode=6),
            delegation_identity=groups.root_identity,jobs_identity=groups.jobs_identity)
    groups.create=create
    def kill(identifier,*args):
        groups.kills.append(identifier)
        child=groups.children.get(identifier)
        if child is not None and child.poll() is None:child.terminate();child.wait(timeout=2)
        groups.live[identifier]=False
    groups.kill=kill
    groups.remove_empty=lambda identifier,*args:groups.leaves.remove(identifier)
    fixed=FixedPaths(tmp_path/'image',tmp_path/'state',tmp_path/'cg',23456,23456)
    def make():
        runtime=Runtime.__new__(Runtime)
        runtime.root=tmp_path;runtime.leases={};runtime.pidfds={};runtime.stop_fences=set();runtime.retiring_reads=set()
        runtime.groups=groups;runtime.deployment=SimpleNamespace(paths=fixed,check_site=lambda:None)
        runtime._config=lambda *args:None  # only the output-parser seam; real registered path tested separately
        def command(value,operation):
            identifier=value['job_id']
            if operation=='create':
                child=subprocess.Popen([sys.executable,'-S','-c','import signal;signal.pause()'],
                    stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                groups.children[identifier]=child;groups.live[identifier]=True
                (tmp_path/identifier/'init.pid').write_text(str(child.pid))
            elif operation=='list':return []
        runtime._command=command
        def watch(value):
            if value['state']=='cleanup_complete':service._clean_watch(authority.peer,value)
            else:service._watch(value)
            return True
        runtime.register_watch=watch
        runtime._recover_index()
        return runtime
    return make(),make,service,authority,owner


def request(runtime,owner,index,*,authorize=lambda:True):
    config=dict(version=1,operation='output-original',witness=dict(artifact=owner['artifact'],
        provenance=dict(project=owner['project']),sources={}),range=f'bytes={index%20}-{index%20}')
    return runtime.create(project=owner['project'],execution_id=owner['read_request_id'],role='media',config=config,
        web_parent=owner['web_parent'],owner_digest=c.digest(owner),read_owner=owner,authorize=authorize)['job_id']


def cleanup(runtime,identifier,*,private=True):
    return runtime.cleanup(identifier,close_bridges=lambda _:True,clean_private_io=lambda _:private)


def test_65_range_admissions_cleanup_retirement_and_restart_preserve_generation_history(tmp_path,monkeypatch,record_property):
    runtime,make,service,authority,owner=harness(tmp_path,monkeypatch)
    historical=lease(service.owner,'kernel','d');historical['state']='cleanup_complete'
    history=tmp_path/historical['job_id'];history.mkdir()
    runtime._save(historical);history_raw=(history/'lease.json').read_bytes()
    original_generation=(tmp_path/'priority-owner.json').read_bytes()
    identifiers=[]
    try:
        for index in range(65):
            if index:
                read_control.publish(tmp_path,authority,'read-'+str(index),artifact());owner=read_control.load(tmp_path)
                service._read_owner_control(authority.peer,'bind-read-owner',c.digest(owner),record=owner)
            identifier=request(runtime,owner,index);identifiers.append(identifier)
            if index==31:
                # Current read loss before start fences/kills; never frees slot.
                with pytest.raises(ForgeError):runtime.start(identifier,authorize=lambda:False)
            else:
                runtime.start(identifier,authorize=lambda:True);runtime.stop(identifier)
            if index==20:
                with pytest.raises(ForgeError):cleanup(runtime,identifier,private=False)
                with pytest.raises(ForgeError,match='proved-clean'):runtime.retire_read(identifier)
                with pytest.raises(ForgeError,match='busy'):request(runtime,owner,index)
                for descriptor in runtime.pidfds.values():os.close(descriptor)
                runtime=make()  # real persisted unresolved lease, no replay
                assert leases.load(tmp_path,identifier)['state']=='cleanup_pending'
            cleanup(runtime,identifier)
            with pytest.raises(ForgeError,match='busy'):request(runtime,owner,index)
            if index==40:
                original_save=read_retirement.save
                calls=[0]
                def fail_final(root,value):
                    calls[0]+=1
                    if calls[0]==2:raise OSError('injected terminal audit fsync failure')
                    return original_save(root,value)
                with monkeypatch.context() as local:
                    local.setattr(read_retirement,'save',fail_final)
                    with pytest.raises(OSError):runtime.retire_read(identifier)
                assert read_retirement.load(tmp_path)['pending']['lease']['job_id']==identifier
                assert not (tmp_path/identifier).exists() and not (tmp_path/read_retirement.STAGE).exists()
                runtime=make()
                with pytest.raises(ForgeError,match='busy'):request(runtime,owner,index)
            retired=runtime.retire_read(identifier)
            assert retired['retired'] and retired['cleanup_proven']
            assert identifier not in runtime.leases and identifier not in service.watches
            service._read_owner_control(authority.peer,'clear-read-owner',c.digest(owner))
            (tmp_path/read_control.NAME).unlink();_sync(tmp_path)
            assert len(runtime.leases)==1 and (history/'lease.json').read_bytes()==history_raw
        audit=read_retirement.load(tmp_path)
        assert audit['pending'] is None and audit['total']==65 and len(audit['recent'])==32
        assert (tmp_path/read_retirement.NAME).stat().st_size<=56*1024
        assert runtime.retire_read(identifiers[-1])['retired']
        assert read_retirement.load(tmp_path)['total']==65
        assert (tmp_path/'priority-owner.json').read_bytes()==original_generation
        with pytest.raises(ForgeError,match='Only proved-clean'):runtime.retire_read(historical['job_id'])
        record_property('sequential_range_lifecycles',65)
        record_property('persisted_audit_receipts',32)
        record_property('real_layers','Runtime/files/child-pidfd/watch registration; OCI/cgroups/config parser seam doubled')
    finally:
        for child in runtime.groups.children.values():
            if child.poll() is None:child.terminate();child.wait(timeout=2)
        for descriptor in runtime.pidfds.values():os.close(descriptor)
        close(service)


def test_retirement_rejects_unconfirmed_watch_and_replaced_stage(tmp_path,monkeypatch):
    runtime,make,service,authority,owner=harness(tmp_path,monkeypatch)
    try:
        identifier=request(runtime,owner,0);runtime.stop(identifier);cleanup(runtime,identifier)
        registered=runtime.register_watch;runtime.register_watch=lambda _:False
        with pytest.raises(ForgeError,match='unregister'):runtime.retire_read(identifier)
        assert identifier in runtime.leases and (tmp_path/identifier).exists()
        runtime.register_watch=registered
        with monkeypatch.context() as local:
            local.setattr(read_retirement,'remove_tree',lambda *a,**k: (_ for _ in ()).throw(OSError('injected delete failure')))
            with pytest.raises(OSError):runtime.retire_read(identifier)
        stage=tmp_path/read_retirement.STAGE
        stage.rename(tmp_path/'original-stage-held');stage.mkdir()
        with pytest.raises(ForgeError,match='directory changed'):runtime.retire_read(identifier)
        assert read_retirement.load(tmp_path)['pending'] is not None
        stage.rmdir();(tmp_path/'original-stage-held').rename(stage)
        assert runtime.retire_read(identifier)['retired']
    finally:
        for child in runtime.groups.children.values():
            if child.poll() is None:child.terminate();child.wait(timeout=2)
        for descriptor in runtime.pidfds.values():os.close(descriptor)
        close(service)
