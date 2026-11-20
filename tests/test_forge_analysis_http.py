"""Actual authenticated HTTP dispatch with an explicit fake helper adapter.

No deployment/cgroup/Jupyter/runtime is enabled by these tests. The adapter
only records closed metadata calls; real helper authority has separate tests.
"""
import threading
import json
import socket
import time
from urllib.parse import urlencode

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.analysis import web_service
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_public_api import request,service
from test_forge_workspace import demo,isolated


def test_consumer_probe_never_waits_for_positive_http_socket_timeout_or_consumes_bytes():
    from demiflow.workbench.api import Handler
    local,remote=socket.socketpair();local.settimeout(.5)
    handler=Handler.__new__(Handler);handler.connection=local
    try:
        started=time.monotonic()
        assert handler._consumer_closed() is False
        assert time.monotonic()-started<.1
        remote.sendall(b'x')
        assert handler._consumer_closed() is False
        assert local.recv(1)==b'x'
        remote.close()
        assert handler._consumer_closed() is True
        assert local.gettimeout()==.5
        local.close()
        assert handler._consumer_closed() is True
    finally:local.close();remote.close()


@pytest.fixture
def helper(monkeypatch):
    class Stub(web_service.WebService):
        def __init__(self,server,config):
            self.closed=threading.Event();self.calls=[];self.ready=True
        def ceiling(self):return dict(analysis_execute=True,analysis_write=True,case_annotate=True)
        def execution_profile(self):return None # Metadata-only double has no admitted runtime.
        def assert_ready(self):
            if not self.ready:raise ForgeError("test helper is unavailable",code="operation_rejected")
            return True
        def call(self,project,operation,payload):
            self.calls.append((project,operation,payload))
            return dict(project=project,operation=operation,payload=payload)
        def stop_generation(self,project,**payload):
            self.calls.append((project,"stop-generation",payload))
            return dict(**payload,accepted=True,cleanup_proven=False,force=True)
        def close(self):self.closed.set()
    monkeypatch.setattr(web_service,"WebService",Stub)
    return Stub


def registered(tmp_path,isolated):
    registry=tmp_path/"registry.json";key=console.add(registry,isolated)["workspace_id"]
    console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    return registry,key,"/api/workspaces/"+key+"/"


def grant(app,registry,key,**updates):
    value=console.load_registry(registry)
    capabilities={**console.capabilities(value["projects"][key]),**updates}
    return console.set_capabilities(registry,key,expected=content_digest(value),capabilities=capabilities,analysis_service=app.analysis)


def test_without_admitted_config_analysis_never_inherits_runs_or_starts_a_backend(isolated):
    with service(isolated,enable_runs=True) as app:
        assert request(app,"/api/capabilities")[1]["analysis_connected"] is False
        assert not any(request(app,"/api/context")[1]["capabilities"][name] for name in ("analysis_execute","analysis_write","case_annotate"))
        assert request(app,"/api/analysis-documents?kind=notebook")[0]==422
        assert request(app,"/api/analysis-notebook-create",dict(request_id="one",document=dict(title="one",cells=[])))[0]==422
    assert not (isolated/".forge/analysis").exists()


def test_closed_project_routes_separate_grants_and_independent_stop_capacity(tmp_path,isolated,helper):
    registry,key,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config="test-only-fake-config") as app:
        assert request(app,"/api/capabilities")[1]["analysis_connected"] is True
        assert request(app,prefix+"analysis-notebook-create",dict(request_id="one",document=dict(title="one",cells=[])))[0]==422
        grant(app,registry,key,analysis_write=True,analysis_execute=True)
        assert request(app,prefix+"analysis-notebook-create",dict(request_id="one",document=dict(title="one",cells=[])))[0]==200
        project,operation,payload=app.analysis.calls[-1]
        assert project==key and operation=="notebook-create" and payload["request_id"]=="one"
        calls=len(app.analysis.calls)
        assert request(app,prefix+"analysis-notebook-create",dict(request_id="one",document=dict(title="one",cells=[]),principal="browser"))[0]==400
        assert len(app.analysis.calls)==calls
        assert request(app,prefix+"analysis-notebook?notebook_id=n1&revision=2&offset=0&limit=3")[1]["payload"]==dict(notebook_id="n1",revision=2,offset=0,limit=3)
        assert request(app,prefix+"analysis-notebook?notebook_id=n1&revision=9999999999999999999")[0]==400
        assert app.analysis_requests.acquire(False) and app.analysis_requests.acquire(False)
        try:
            assert request(app,prefix+"analysis-documents?kind=notebook")[0]==503
            code,value=request(app,prefix+"analysis-stop-generation",dict(request_id="stop",session_id="s1",generation="g1"))
            assert code==200 and value["accepted"] and not value["cleanup_proven"]
            assert app.control_workers.acquire(False) # pipeline control lane stayed free
            app.control_workers.release()
        finally:app.analysis_requests.release();app.analysis_requests.release()
        grant(app,registry,key,analysis_execute=False)
        count=len(app.analysis.calls)
        assert request(app,prefix+"analysis-stop-generation",dict(request_id="stop",session_id="s1",generation="g1"))[0]==422
        assert len(app.analysis.calls)==count


def test_grant_addition_requires_live_helper_and_other_capability_edits_preserve_configured_grants(tmp_path,isolated,helper):
    registry,key,_=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config="test-only-fake-config",enable_config_write=True) as app:
        before=registry.read_bytes();app.analysis.ready=False
        with pytest.raises(ForgeError,match="unavailable"):grant(app,registry,key,analysis_execute=True)
        assert registry.read_bytes()==before
        app.analysis.ready=True
        grant(app,registry,key,analysis_execute=True,analysis_write=True)
    # A different deployment may preserve/revoke existing configured grants
    # while its effective analysis ceiling stays false. It cannot add one.
    with service(None,registry=registry,enable_config_write=True) as app:
        current=console.load_registry(registry);configured=console.capabilities(current["projects"][key])
        code,_=request(app,"/api/project-capabilities",dict(workspace_id=key,expected=content_digest(current),
            capabilities={**configured,"config_write":True}))
        assert code==200
        assert console.capabilities(console.load_registry(registry)["projects"][key])["analysis_execute"]
        current=console.load_registry(registry)
        code,_=request(app,"/api/project-capabilities",dict(workspace_id=key,expected=content_digest(current),
            capabilities={**console.capabilities(current["projects"][key]),"case_annotate":True}))
        assert code==403


def test_cli_exposes_only_explicit_opt_in_analysis_config():
    runner=CliRunner()
    for arguments in (["workbench","view","--help"],["workbench","console","--registry","/tmp/does-not-start-any-service.json","serve","--help"]):
        result=runner.invoke(main,arguments)
        assert result.exit_code==0,result.output
        assert "--analysis-config" in result.output


def test_profile_query_uses_common_typed_projection_and_read_bodies_do_not_grant_writes(tmp_path,isolated,helper):
    registry,key,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config='test-only-fake-config') as app:
        query=dict(digest='a'*64,columns=json.dumps(['case_id','question']))
        code,value=request(app,prefix+'analysis-source-profile?'+urlencode(query))
        assert code==200,value
        assert value['payload']['columns']==['case_id','question']
        code,value=request(app,prefix+'analysis-annotation-definitions',dict(target=dict(source='a'*64,case_key='5',variant=None,field=None),limit=8))
        assert code==200 and value['operation']=='annotation-definitions'
        assert request(app,prefix+'analysis-annotation-definitions',dict(target={},principal='caller'))[0]==400
        assert request(app,prefix+'analysis-annotations-save',dict(request_id='no-grant',target={},entries=[],origin={}))[0]==422
        assert request(app,prefix+'context')[1]['analysis_features']['annotation_definitions']


def test_registered_group_binding_is_closed_read_post_and_feature_requires_matched_helper(tmp_path,isolated,helper):
    registry,key,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config='test-only-fake-config') as app:
        payload=dict(digest='a'*64,case_key='1',variant='cg'+'b'*64)
        code,value=request(app,prefix+'analysis-case-group-bindings',payload)
        assert code==200 and value['operation']=='case-group-bindings' and value['payload']==payload
        assert request(app,prefix+'analysis-case-group-bindings',{**payload,'principal':'browser'})[0]==400
        assert request(app,prefix+'analysis-case-group-bindings',{**payload,'request_id':'arbitrary-write'})[0]==400
        assert request(app,prefix+'context')[1]['analysis_features']['case_output_groups'] is False
        assert request(app,prefix+'context')[1]['analysis_features']['case_review_filter'] is False
        app.analysis.features=dict(case_output_groups=True,case_review_filter=True,notebook_history=True)
        assert request(app,prefix+'context')[1]['analysis_features']['case_output_groups'] is True
        assert request(app,prefix+'context')[1]['analysis_features']['case_review_filter'] is True
        assert request(app,prefix+'context')[1]['analysis_features']['notebook_history'] is True
        app.analysis.features=dict(case_output_groups=1)
        assert request(app,prefix+'context')[1]['analysis_features']['case_output_groups'] is False


def test_product_context_routes_read_existing_project_records(isolated):
    from test_forge_runtime_review_fixes import control_fixture
    manifest, target, _ = control_fixture(isolated)
    before = (target / 'run.sqlite').read_bytes()
    with service(isolated) as app:
        assert request(app, '/api/forge-context')[0] == 404
        assert request(app, '/api/forge-tools')[0] == 404
        code, facts = request(app, '/api/run-context?run_id=' + manifest['run_id'])
        assert code == 200 and facts['facts'][0]['value']['state']['status'] == 'prepared'
        assert request(app, '/api/run-tools')[0] == 200
        assert request(app, '/api/run-context?run_id=' + manifest['run_id'] + '&principal=browser')[0] == 400
    assert (target / 'run.sqlite').read_bytes() == before
