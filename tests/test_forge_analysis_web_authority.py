"""Current registry/control identity with a test service ceiling; no native job."""
import json
import os
import socket
import struct
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from demiflow.forge.process import process_identity
from demiflow.workbench.projects import registry as console
from demiflow.workbench.analysis.web_service import WebService
from demiflow.workbench.analysis.web_service import credentials
from demiflow.forge.schema import ForgeError


def test_callback_rechecks_membership_grant_and_server_owned_principal(tmp_path):
    key="w"+"a"*32
    (tmp_path/"workspace.json").write_text(json.dumps(dict(id=key)))
    flags={name:False for name in console.PERMISSIONS|console.CAPABILITIES}
    inventory=dict(projects={key:dict(root=str(tmp_path),permissions=flags)})
    server=SimpleNamespace(inventory=lambda:inventory,effective_permissions=lambda item:dict(item["permissions"]))
    service=WebService.__new__(WebService);service.server=server
    service.identity=process_identity(os.getpid());service.principal="owner-stable"
    request=dict(version=1,challenge="challenge",operation="whoami",project=key,
        web_parent=service.identity,principal=None,payload={})
    assert service._authority(request)==dict(principal="owner-stable")
    request.update(operation="permission",principal="owner-stable",payload=dict(permission="analysis_execute"))
    assert service._authority(request)==dict(allowed=False)
    flags["analysis_execute"]=True
    assert service._authority(request)==dict(allowed=True)
    with pytest.raises(ForgeError,match="principal"):
        service._authority(request|dict(principal="from-browser"))
    del inventory["projects"][key]
    with pytest.raises(ForgeError,match="not registered"):
        service._authority(request)


def test_ancestor_pid_namespace_does_not_allow_unmapped_or_same_uid_helpers():
    # Synthetic SO_PEERCRED response checks the explicit mapping policy only;
    # real cross-namespace credentials remain a B1 calibration requirement.
    uid=os.geteuid()+1000
    class Socket:
        def __init__(self,value):self.value=value
        def getsockopt(self,*args):return struct.pack("3i",0,self.value,1)
    assert credentials(Socket(uid),uid)==dict(pid=0,uid=uid,gid=1)
    for actual,expected in ((65534,65534),(os.geteuid(),os.geteuid()),(uid,uid+1)):
        with pytest.raises(ForgeError):credentials(Socket(actual),expected)


def test_trusted_same_uid_peer_requires_exact_original_kernel_identity():
    # Actual Unix SO_PEERCRED, not a cross-UID/isolation or job cleanup proof.
    first,second=socket.socketpair()
    try:
        original=process_identity(os.getpid())
        assert credentials(first,os.geteuid(),expected_identity=original)['pid']==os.getpid()
        with pytest.raises(ForgeError):credentials(first,os.geteuid())
        for field,value in [('pid',original['pid']+1),('boot','different-boot'),('start','different-start')]:
            with pytest.raises(ForgeError):
                credentials(first,os.geteuid(),expected_identity={**original,field:value})
    finally:first.close();second.close()


def test_project_context_uses_current_registered_root_without_granting_execution(tmp_path):
    key='w'+'b'*32
    (tmp_path/'workspace.json').write_text(json.dumps(dict(id=key)))
    flags={name:False for name in console.PERMISSIONS|console.CAPABILITIES}
    inventory=dict(projects={key:dict(root=str(tmp_path),permissions=flags)})
    service=WebService.__new__(WebService)
    service.server=SimpleNamespace(inventory=lambda:inventory,effective_permissions=lambda item:item['permissions'])
    service.identity=process_identity(os.getpid());service.principal='fixed-owner'
    request=dict(version=1,challenge='context',operation='project-context',project=key,
        web_parent=service.identity,principal=service.principal,payload={})
    assert service._authority(request)==dict(workspace=str(tmp_path))
    assert not flags['analysis_execute']
    with pytest.raises(ForgeError):service._authority({**request,'payload':dict(workspace='/caller/root')})
    with pytest.raises(ForgeError):service._authority({**request,'principal':'caller'})
    del inventory['projects'][key]
    with pytest.raises(ForgeError):service._authority(request)


def test_real_permission_callback_progresses_with_silent_headers_and_occupied_source_lane(tmp_path,short_control_directory):
    # Real Unix socket, captured process identity, public framing and registry
    # permission callback. Only the long source-worker occupancy is injected;
    # there is no Controller, Jupyter, native task or execution admission here.
    from demiflow.workbench.analysis.authority import Authority
    from demiflow.workbench.analysis.ipc import Listener
    from demiflow.workbench.analysis.ipc import Peer
    key='w'+'c'*32
    (tmp_path/'workspace.json').write_text(json.dumps(dict(id=key)))
    flags={name:False for name in console.PERMISSIONS|console.CAPABILITIES};flags['analysis_execute']=True
    inventory=dict(projects={key:dict(root=str(tmp_path),permissions=flags)})
    service=WebService.__new__(WebService);service.identity=process_identity(os.getpid());service.principal='test-owner'
    service.config=dict(helper_uid=os.geteuid(),helper_identity=service.identity)
    service.server=SimpleNamespace(inventory=lambda:inventory,effective_permissions=lambda item:item['permissions'])
    service.closed=threading.Event();service.slots=threading.BoundedSemaphore(1)
    service.authority_controls=threading.BoundedSemaphore(1)
    service.listener=Listener(short_control_directory/'auth.sock',public=True)
    service.thread=threading.Thread(target=service._serve_authority,daemon=True);service.thread.start()
    authority=Authority.__new__(Authority)
    authority.project=key;authority.principal=service.principal
    authority.peer=Peer(os.getpid(),os.geteuid(),os.getegid(),service.identity)
    authority.deployment=SimpleNamespace(value=dict(site=dict(web_auth_socket_path=str(service.listener.path))))
    silent=[];assert service.slots.acquire(False)
    try:
        for _ in range(2):
            connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.connect(str(service.listener.path));silent.append(connection)
        started=time.monotonic()
        assert authority.check(key,service.principal,'analysis_execute') is True
        assert time.monotonic()-started<1.5
        flags['analysis_execute']=False
        assert authority.check(key,service.principal,'analysis_execute') is False
        assert service.authority_controls.acquire(timeout=1);service.authority_controls.release()
    finally:
        for connection in silent:connection.close()
        service.slots.release();service.closed.set();service.listener.close();service.thread.join(2)
        assert not service.thread.is_alive()


def test_source_callback_waits_for_the_original_workspace_slot_without_replaying(tmp_path,monkeypatch):
    from demiflow.workbench.analysis import web_sources
    key='w'+'d'*32
    (tmp_path/'workspace.json').write_text(json.dumps(dict(id=key)))
    flags={name:True for name in console.PERMISSIONS|console.CAPABILITIES}
    inventory=dict(projects={key:dict(root=str(tmp_path),permissions=flags)})
    service=WebService.__new__(WebService);service.identity=process_identity(os.getpid());service.principal='test-owner'
    slot=threading.BoundedSemaphore(1);service.sources=threading.BoundedSemaphore(1)
    service.server=SimpleNamespace(inventory=lambda:inventory,effective_permissions=lambda item:item['permissions'],workers=slot)
    started=threading.Event();finished=threading.Event();calls=[];results=[]
    monkeypatch.setattr(web_sources,'verify',lambda *a,**k:(calls.append((a,k)) or dict(verified=True)))
    request=dict(version=1,challenge='source',operation='verify-source',project=key,
        web_parent=service.identity,principal=service.principal,payload=dict(locator={},source={},target=None))
    def waiting():
        started.set()
        try:results.append(service._authority(request))
        finally:finished.set()
    assert slot.acquire(False);held=True
    thread=threading.Thread(target=waiting);thread.start()
    try:
        assert started.wait(1) and not finished.wait(.05) and not calls
        control={**request,'operation':'permission','payload':dict(permission='analysis_execute')}
        assert service._authority(control)==dict(allowed=True)
        slot.release();held=False;thread.join(2)
        assert not thread.is_alive() and results==[dict(verified=True)] and len(calls)==1
    finally:
        if held:slot.release()
        thread.join(6)


@pytest.fixture
def short_control_directory():
    with TemporaryDirectory(prefix='dfa-web-') as path:
        yield Path(path)


def test_trusted_status_and_managed_stop_keep_mode_and_false_legacy_proof(tmp_path,short_control_directory):
    from demiflow.workbench.analysis.web_service import load_config
    from demiflow.workbench.analysis.trusted_binding import WEB_FORMAT
    from demiflow.workbench.analysis.execution_backend import capabilities
    root=tmp_path/'transfer';root.mkdir()
    body=dict(format=WEB_FORMAT,deployment_id='local-test',principal='fixed-owner',
        helper_uid=os.geteuid(),helper_identity=process_identity(os.getpid()),
        helper_socket=str(short_control_directory/'helper.sock'),auth_socket=str(short_control_directory/'auth.sock'),
        transfer_root=str(root),code_digest='a'*64,python_digest='b'*64,
        environment_digest='c'*64,policy_digest='d'*64,
        service_capabilities=dict(analysis_execute=True,analysis_write=True,case_annotate=True))
    path=tmp_path/'web.json';path.write_text(json.dumps(body));path.chmod(0o600)
    assert load_config(path)==body
    service=WebService.__new__(WebService);service.config=body;service.closed=threading.Event()
    status=dict(format='demiflow_analysis_helper_status_v2',mode='production',active=True,
        execution_mode='trusted_local',**{key:body[key] for key in ['helper_identity','code_digest',
            'python_digest','environment_digest','policy_digest']},capabilities=capabilities('trusted_local'))
    service.call=lambda *args,**kwargs:status
    assert service.assert_ready()
    status['capabilities']['filesystem_isolation']=True
    with pytest.raises(ForgeError):service.assert_ready()
    status['capabilities']=capabilities('trusted_local');status['code_digest']='e'*64
    with pytest.raises(ForgeError):service.assert_ready()
    calls=[]
    def managed(project,operation,payload,**kwargs):
        calls.append((project,operation,payload,kwargs))
        return dict(id='session',generation='original',state='stop_requested',cleanup_complete=False)
    service.call=managed
    stopped=service.stop_generation('w'+'b'*32,request_id='stop',session_id='session',generation='original')
    assert stopped['accepted'] and stopped['execution_mode']=='trusted_local'
    assert not stopped['force'] and not stopped['cleanup_proven'] and not stopped['cleanup_complete']
    assert calls[0][1]=='session-control' and calls[0][2]['expected_generation']=='original'
    assert calls[0][3]['_timeout']==7
    # This is closed binding/status/route validation; no fake acknowledgement
    # is presented as an actual kernel/process stop.
