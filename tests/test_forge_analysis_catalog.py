"""Actual catalog/profile HTTP through existing owned fixture readers.

No analysis helper, kernel, external deployment, original business or model.
"""
from demiflow.forge.project import initialize_project
import json
import pytest
from urllib.parse import urlencode

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from test_forge_workspace import demo,isolated
from test_forge_project_discovery import original,business_bytes
from test_forge_public_api import request,service


def test_catalog_profile_is_read_only_fixed_and_available_without_analysis_environment(isolated):
    description=project_ops.describe_table(isolated,'candidates');binding=description['binding']['digest']
    query=dict(alias='candidates',expected_binding=binding,columns=json.dumps(['case_id','question']))
    with service(isolated) as app:
        code,value=request(app,'/api/table-profile?'+urlencode(query))
        assert code==200,value
        assert value['kind']=='profile' and value['binding']==binding and value['coverage']['complete']
        assert value['coverage']['rows_scanned']==description['metadata']['row_count']
        assert [item['name'] for item in value['fields']]==['case_id','question']
        assert request(app,'/api/capabilities')[1]['analysis_connected'] is False
        original_reader=app.native_reader.identity
        assert app.native_reader.creator_thread==app.native_reader.owner_ident
        assert request(app,'/api/table-profile?'+urlencode(query))[0]==200
        assert app.native_reader.identity==original_reader
        status,error=request(app,'/api/table-profile?'+urlencode({**query,'expected_binding':'f'*64}))
        assert status==409,(error,list(app.native_reader.observations))
        assert request(app,'/api/table-profile?'+urlencode({**query,'columns':json.dumps(['not-declared'])}))[0]==409
        assert request(app,'/api/table-profile?'+urlencode(dict(alias='candidates',columns='["case_id"]')))[0]==400
    assert not (isolated/'.forge/analysis').exists()


def test_original_declared_profile_keeps_fixed_version_and_does_not_edit_business_files(original):
    initialize_project(original,existing=True);before=business_bytes(original)
    discovery=ws.discover_project(original);item=next(row for row in discovery['tables'] if row['version']==1)
    metadata=project_ops.project_table(original,item['id'],expected=discovery['digest'])
    with service(original) as app:
        query=dict(id=item['id'],expected=discovery['digest'],version=1,binding=metadata['binding'],columns='["order","amount"]')
        code,value=request(app,'/api/project-table-profile?'+urlencode(query))
        assert code==200,value
        assert value['coverage']['complete'] and value['coverage']['rows_scanned']==2
        amount=next(field for field in value['fields'] if field['name']=='amount')
        assert amount['minimum']==3 and amount['maximum']==8
        status,error=request(app,'/api/project-table-profile?'+urlencode({**query,'binding':'f'*64}))
        assert status==409,(error,list(app.native_reader.observations))
    assert business_bytes(original)==before


def test_idle_housekeeping_and_real_foreground_http_share_the_original_reader_permit(isolated,monkeypatch):
    import threading
    import time
    description=project_ops.describe_table(isolated,'candidates')
    query=dict(alias='candidates',expected_binding=description['binding']['digest'],columns='["case_id"]')
    url='/api/table-profile?'+urlencode(query)
    entered=threading.Event();release=threading.Event();finished=threading.Event();answers=[]
    with service(isolated) as app:
        assert request(app,url)[0]==200
        original=app.native_reader._sample
        def sample():
            if threading.current_thread().name=='reader-maintenance-proof':
                entered.set();assert release.wait(3)
            return original()
        monkeypatch.setattr(app.native_reader,'_sample',sample)
        def maintenance():
            deadline=time.monotonic()+2
            while not entered.is_set() and time.monotonic()<deadline:
                app.service_actions();time.sleep(.005)
        def foreground():
            answers.append(request(app,url));finished.set()
        background=threading.Thread(target=maintenance,name='reader-maintenance-proof')
        foreground_thread=threading.Thread(target=foreground)
        try:
            background.start();assert entered.wait(2)
            foreground_thread.start()
            assert not finished.wait(.05) # It waits for the original read permit.
            release.set();foreground_thread.join(10);background.join(5)
            assert not foreground_thread.is_alive() and not background.is_alive()
            assert len(answers)==1 and answers[0][0]==200,answers
            assert answers[0][1]['coverage']['complete']
        finally:
            release.set();background.join(5)
            if foreground_thread.ident is not None:foreground_thread.join(10)


def test_native_cleanup_error_still_closes_the_other_owned_service_resources(isolated,monkeypatch):
    import socket
    import pytest
    from demiflow.workbench.api import WorkspaceServer
    from demiflow.forge.schema import ForgeError
    class SocketLease:
        def __init__(self):self.socket=socket.socket()
        def close(self):self.socket.close()
    app=WorkspaceServer(isolated)
    analysis,development=SocketLease(),SocketLease()
    app.analysis=analysis;app.development=development
    actual=app.native_reader.close
    def fail_after_owned_close():
        actual()
        raise ForgeError('Injected cleanup report after original reader retirement')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(app.native_reader,'close',fail_after_owned_close)
            with pytest.raises(ForgeError):app.server_close()
        assert analysis.socket.fileno()==development.socket.fileno()==-1
        assert app.native_reader.closed and app.native_reader.process is None
    finally:app.server_close()


@pytest.mark.parametrize('shutdown_owner',[False,True])
def test_deferred_owner_start_cancel_or_shutdown_never_launches_a_late_child(isolated,monkeypatch,shutdown_owner):
    import socket
    import threading
    import time
    from urllib.parse import urlsplit
    description=project_ops.describe_table(isolated,'candidates')
    query=dict(alias='candidates',expected_binding=description['binding']['digest'],columns='["case_id"]')
    endpoint='/api/table-profile?'+urlencode(query)
    allow=threading.Event();client=None
    with service(isolated) as app:
        original=app.native_reader.launch_pending
        def deferred():
            if allow.is_set():original()
        monkeypatch.setattr(app.native_reader,'launch_pending',deferred)
        location=urlsplit(app.origin)
        try:
            client=socket.create_connection((location.hostname,location.port),timeout=5)
            client.sendall(('GET '+endpoint+' HTTP/1.1\r\nHost: '+location.netloc+'\r\nX-Forge-Token: '+app.token+'\r\nConnection: close\r\n\r\n').encode())
            deadline=time.monotonic()+3
            while app.native_reader.launch is None and time.monotonic()<deadline:time.sleep(.01)
            assert app.native_reader.launch is not None and not app.native_reader.launch['started']
            if shutdown_owner:app.shutdown()
            else:client.close()
            deadline=time.monotonic()+3
            while app.native_reader.launch is not None and time.monotonic()<deadline:time.sleep(.01)
            assert app.native_reader.launch is None and app.native_reader.process is None
            allow.set()
            if not shutdown_owner:
                assert request(app,endpoint)[0]==200
                assert app.native_reader.creator_thread==app.native_reader.owner_ident
            else:assert app.native_reader.owner_ident is None
        finally:
            allow.set()
            if client is not None:client.close()
