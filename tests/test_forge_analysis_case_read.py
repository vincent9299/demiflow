"""Closed native Case purpose with actual Runtime config and owner/watch flow.

No actual Lance/OCI/cgroup run; shared read_private implementation merges from
integrator. Processes/pidfd/control files are real through the lifecycle fixture.
"""
import copy
import os
import time

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import case_worker
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis import read_retirement
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.workbench.analysis.oci import bundle
from demiflow.forge.schema import ForgeError
from test_forge_analysis_read_control import close
from test_forge_analysis_read_retirement import harness, cleanup


def config(project):
    source=c.source_snapshot(dict(kind='pinned_object',binding='b'*64,
        schema=[dict(name='x',type='int64',nullable=False)],rows=3,
        case_key=dict(kind='row_id',field=None),retention='pinned'))
    return dict(version=1,operation='case-page',witness=dict(project=project,
        artifact=dict(id='o'+'a'*32,generation=1,digest='b'*64),source=source),
        query=dict(kind='cases',columns=['x'],scope='full',offset=0,limit=2))


def case_harness(tmp_path,monkeypatch):
    runtime,make,service,authority,old=harness(tmp_path,monkeypatch)
    service._read_owner_control(authority.peer,'clear-read-owner',c.digest(old))
    (tmp_path/read_control.NAME).unlink()
    value=config(authority.project)
    read_control.publish_case(tmp_path,authority,'case-one',value)
    owner=read_control.load(tmp_path)
    service._read_owner_control(authority.peer,'bind-read-owner',c.digest(owner),record=owner)
    runtime._config=Runtime._config
    return runtime,service,authority,owner,value


def create(runtime,owner,value):
    return runtime.create(project=owner['project'],execution_id=owner['read_request_id'],role='native',config=value,
        web_parent=owner['web_parent'],owner_digest=c.digest(owner),read_owner=owner,authorize=lambda:True)['job_id']


def finish(runtime,service):
    for child in runtime.groups.children.values():
        if child.poll() is None:child.terminate();child.wait(timeout=2)
    for descriptor in runtime.pidfds.values():os.close(descriptor)
    close(service)


def test_case_read_has_native_budget_fixed_mount_and_no_kernel_owner_dependency(tmp_path,monkeypatch):
    runtime,service,authority,owner,value=case_harness(tmp_path,monkeypatch)
    # Prove no live Notebook/generation owner is necessary for this native read.
    original=service.owner;service.owner=None
    try:
        assert owner['kind']=='case-read' and 55<owner['deadline']-time.monotonic()<=60
        identifier=create(runtime,owner,value)
        stored=leases.load(tmp_path,identifier)
        assert stored['format']==leases.MEDIA_FORMAT and stored['owner_kind']=='case-read' and stored['role']=='native'
        spec=c.decode((tmp_path/identifier/'bundle/config.json').read_bytes())
        assert spec['process']['args'][-1]=='demiflow.workbench.analysis.case_worker'
        assert spec['linux']['resources']['memory']['limit']==1024**3
        mounts={item['destination']:item for item in spec['mounts']}
        assert '/objects/'+value['witness']['artifact']['id'] in mounts
        assert not any(name.startswith('/sources/') for name in mounts)
        assert '/var/lib/demiflow-analysis/native-slots' in mounts
        runtime.start(identifier,authorize=lambda:True)
        service._stop_read()
        assert runtime.groups.kills==[identifier] and not service.stop_requested
        runtime.stop(identifier);cleanup(runtime,identifier)
        assert runtime.retire_read(identifier)['retired']
        assert not read_retirement.charged(runtime,'native')
        service._read_owner_control(authority.peer,'clear-read-owner',c.digest(owner))
        assert service.owner is None
    finally:
        service.owner=original;finish(runtime,service)


def test_case_and_generation_native_slot_are_mutually_exclusive(tmp_path,monkeypatch):
    runtime,service,authority,owner,value=case_harness(tmp_path,monkeypatch)
    try:
        # A live native generation job occupies the existing single native role.
        native=runtime.create(project=service.owner['project'],execution_id='native-generation-read',role='native',
            config=dict(operation='read',object_id='o'+'c'*32,transfer_id='transfer-one'),
            web_parent=service.owner['web_parent'],owner_digest=c.digest(service.owner),authorize=lambda:True)['job_id']
        with pytest.raises(ForgeError,match='busy'):create(runtime,owner,value)
        runtime.stop(native);cleanup(runtime,native)
        assert runtime.register_watch(runtime.leases[native]) is True
        case=create(runtime,owner,value)
        with pytest.raises(ForgeError,match='busy'):
            runtime.create(project=service.owner['project'],execution_id='native-late',role='native',
                config=dict(operation='read',object_id='o'+'c'*32,transfer_id='transfer-two'),
                web_parent=service.owner['web_parent'],owner_digest=c.digest(service.owner),authorize=lambda:True)
        assert runtime.leases[native]['format']==leases.FORMAT and runtime.leases[native]['state']=='cleanup_complete'
        runtime.stop(case);cleanup(runtime,case);runtime.retire_read(case)
        assert native in runtime.leases  # generation history never retired
    finally:finish(runtime,service)


def test_case_owner_rejects_changed_query_object_project_and_generation_borrow(tmp_path,monkeypatch):
    runtime,service,authority,owner,value=case_harness(tmp_path,monkeypatch)
    try:
        variants=[]
        changed=copy.deepcopy(value);changed['query']['scope']='sample';variants.append(changed)
        changed=copy.deepcopy(value);changed['witness']['artifact']['id']='o'+'f'*32;variants.append(changed)
        changed=copy.deepcopy(value);changed['witness']['project']='w'+'f'*32;variants.append(changed)
        for changed in variants:
            with pytest.raises(ForgeError):create(runtime,owner,changed)
        with pytest.raises(ForgeError):
            runtime.create(project=owner['project'],execution_id=owner['read_request_id'],role='native',config=value,
                web_parent=owner['web_parent'],owner_digest=c.digest(service.owner),read_owner=service.owner,authorize=lambda:True)
        assert not runtime.groups.children
        other=copy.deepcopy(value);del other['query']['scope']
        with pytest.raises(ForgeError,match='explicit'):case_worker.configuration(other)
        other=copy.deepcopy(value);other['query']['kind']='case_page'
        with pytest.raises(ForgeError):case_worker.configuration(other)
        with pytest.raises(ForgeError):
            bundle(runtime.deployment.paths,identifier='j'+'f'*32,role='native',read_kind='case-read',
                source_ids=['o'+'a'*32],output_id='o'+'a'*32)
    finally:finish(runtime,service)


def test_case_entry_refuses_before_native_import_or_reader(monkeypatch):
    def refuse(role):
        assert role=='native'
        raise ForgeError('no admitted native job')
    monkeypatch.setattr(case_worker,'require_job',refuse)
    monkeypatch.setattr(case_worker,'execute',lambda value:pytest.fail('reader before sandbox'))
    with pytest.raises(ForgeError,match='admitted'):case_worker.main()


def test_case_error_reply_keeps_original_identity_without_host_path(monkeypatch):
    value=config('w'+'2'*32)
    monkeypatch.setattr(case_worker,'execute',lambda _:(_ for _ in ()).throw(OSError('/private/secret unreadable')))
    result=case_worker.response(value)
    assert result['artifact']==value['witness']['artifact'] and 'value' not in result
    assert result['error']['code']=='operation_failed' and '/private' not in str(result)
