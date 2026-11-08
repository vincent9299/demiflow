"""Real SQLite admission/recovery; no kernel or OCI execution is simulated as proof."""
import hashlib
import copy

from demiflow.workbench.analysis.execution_backend import capabilities
from demiflow.workbench.analysis.execution_backend import cleanup_receipt

from test_forge_analysis_records import controller, document


PARENT = dict(pid=123, start="42", boot="test-owned-host")
JOB = "j" + "a"*32


def trusted_origin(environment='c'):
    # Fixed test facts only; actual interpreter/package verification belongs
    # to the backend's real process tests, not this SQLite admission fixture.
    return dict(format='demiflow_analysis_local_origin_v1',mode='trusted_local',
        code_digest='a'*64,python_digest='b'*64,python='/fixed/env/bin/python',
        environment_digest=environment*64,uid=0,gid=0,cwd='/fixed/project',
        original_fsize=[-1,-1],limits=dict(cell_seconds=120,kernel_seconds=3600,
            native_seconds=60,media_seconds=5,scratch_bytes=256*1024**2,sample_rss_bytes=2*1024**3),
        capabilities=capabilities('trusted_local'))


def local_cleanup(job=JOB, mode='trusted_local'):
    proof=cleanup_receipt(job_id=job,mode=mode,state='complete',processes=True,
        descendants=True,channels=True,temporary=True,reason='test_owned_cleanup')
    return dict(cleanup_complete=True,cleanup_proven=mode=='oci',cleanup_receipt=proof)


def test_generation_environment_and_scoped_cleanup_survive_restart_without_relabeling_history(tmp_path):
    state=tmp_path/'state';original=trusted_origin()
    with controller(state) as client:
        book=client.call('create_notebook',request_id='create',document=document())['result']
        session=client.call('session_start',request_id='start',notebook_id=book['id'],revision=1,
            sources={},web_parent=PARENT,environment=original)['result']
        assert session['environment']['body']==original and session['cleanup_complete'] is False
        old_digest=session['environment']['digest'];execution=submit(client,book,session,'cell')['result']
        assert execution['environment']==dict(digest=old_digest,mode='trusted_local')
        ready(client,session);client.call('session_dispatch',session_id=session['id'],generation=session['generation'])
        # A boolean has only the old OCI meaning, even when it is true.
        lost=client.call('session_system_lost',session_id=session['id'],generation=session['generation'],
            reason='cancelled',controls=dict(cleanup_proven=True))['result']
        assert lost['cleanup_proven'] is False and lost['cleanup_complete'] is False
        assert client.call('session_control',request_id='early',session_id=session['id'],
            expected_generation=session['generation'],action='restart',environment=trusted_origin('d'))['code']=='conflict'
        wrong=client.call('session_system_lost',session_id=session['id'],generation=session['generation'],
            reason='wrong_mode',controls=dict(cleanup_result=local_cleanup(mode='oci')))
        assert wrong['code']=='conflict'
        clean=client.call('session_system_lost',session_id=session['id'],generation=session['generation'],
            reason='normal_stop',controls=dict(cleanup_result=local_cleanup()))['result']
        assert clean['cleanup_proven'] is False and clean['cleanup_complete'] is True
        assert clean['cleanup_receipt']['scope']=='managed_processes'
        stopped=client.call('session_get',session_id=session['id'])['result']
        assert stopped['state']=='stopped' and stopped['cleanup_complete'] and not stopped['cleanup_proven']
        assert stopped['cleanup_receipt']==clean['cleanup_receipt']
        late=client.call('session_system_lost',session_id=session['id'],generation=session['generation'],
            reason='late_unknown')['result']
        assert late['cleanup_complete'] and not late['cleanup_proven']
        assert client.call('session_control',request_id='missing-origin',session_id=session['id'],
            expected_generation=session['generation'],action='restart')['code']=='conflict'
        restarted=client.call('session_control',request_id='restart',session_id=session['id'],
            expected_generation=session['generation'],action='restart',environment=trusted_origin('d'))['result']
        assert restarted['generation']!=session['generation']
        assert restarted['environment']['body']==trusted_origin('d')
        assert restarted['environment']['digest']!=old_digest
        old=client.call('session_execution',execution_id=execution['id'])['result']
        assert old['state']=='unknown' and old['environment']['digest']==old_digest
        original_facts=client.call('session_environment',execution_id=execution['id'])['result']
        assert original_facts['generation']==session['generation']
        assert original_facts['environment']==dict(digest=old_digest,body=original)
        assert client.call('session_page',session_id=session['id'],generation=session['generation'])['result']['items'][0]['environment']['digest']==old_digest
    with controller(state) as client:
        # Reopening only reads saved facts; the fixture never starts a kernel.
        current=client.call('session_get',session_id=session['id'])['result']
        assert current['generation']==restarted['generation'] and current['environment']['body']==trusted_origin('d')
        assert client.call('session_execution',execution_id=execution['id'])['result']['environment']['digest']==old_digest


def test_environment_is_part_of_start_receipt_and_trusted_capabilities_cannot_claim_isolation(tmp_path):
    with controller(tmp_path/'state') as client:
        book=client.call('create_notebook',request_id='create',document=document())['result']
        request=dict(request_id='start',notebook_id=book['id'],revision=1,sources={},web_parent=PARENT)
        session=client.call('session_start',**request,environment=trusted_origin())['result']
        assert client.call('session_start',**request,environment=trusted_origin())['result']['id']==session['id']
        assert client.call('session_start',**request,environment=trusted_origin('d'))['code']=='conflict'
        forged=copy.deepcopy(trusted_origin());forged['capabilities']['hard_memory_quota']=True
        assert 'result' not in client.call('session_start',**{**request,'request_id':'forged'},environment=forged)
        assert client.call('session_get',session_id=session['id'])['result']['environment']['body']==trusted_origin()


def setup(client):
    notebook = client.call("create_notebook", request_id="create", document=document())["result"]
    session = client.call("session_start", request_id="start", notebook_id=notebook["id"], revision=1,
                         sources={}, web_parent=PARENT)["result"]
    return notebook, session


def submit(client, notebook, session, request_id):
    return client.call("session_submit", request_id=request_id, session_id=session["id"],
        expected_generation=session["generation"], revision=1, cell_id="first",
        source_sha256=hashlib.sha256(document()["cells"][0]["source"].encode()).hexdigest())


def ready(client, session):
    assert client.call("session_system_attach", session_id=session["id"], generation=session["generation"], identifier=JOB)["result"] is None
    assert client.call("session_system_ready", session_id=session["id"], generation=session["generation"], identifier=JOB)["result"] is None


def test_queue_receipts_source_binding_and_at_most_once_dispatch(tmp_path):
    with controller(tmp_path/"state") as client:
        notebook, session = setup(client)
        first = submit(client, notebook, session, "cell1")["result"]
        assert submit(client, notebook, session, "cell1")["result"]["id"] == first["id"]
        second = submit(client, notebook, session, "cell2")["result"]
        assert submit(client, notebook, session, "cell3")["code"] == "busy"
        wrong = client.call("session_submit", request_id="bad", session_id=session["id"],
            expected_generation=session["generation"], revision=1, cell_id="first", source_sha256="0"*64)
        assert wrong["code"] == "conflict"
        ready(client, session)
        command = client.call("session_dispatch", session_id=session["id"], generation=session["generation"])["result"]
        assert command["execution_id"] == first["id"] and command["source"] == document()["cells"][0]["source"]
        assert client.call("session_dispatch", session_id=session["id"], generation=session["generation"])["result"] is None
        third = submit(client, notebook, session, "cell3")["result"]
        cancelled = client.call("session_cancel", request_id="cancel-queued", execution_id=second["id"], expected_generation=session["generation"])["result"]
        assert cancelled["state"] == "cancelled" and cancelled["generation_will_stop"] is False
        assert client.call("session_execution", execution_id=third["id"])["result"]["state"] == "queued"


def test_uncertain_execution_survives_restart_and_holds_slot_until_cleanup(tmp_path):
    state = tmp_path/"state"
    with controller(state) as client:
        notebook, session = setup(client)
        execution = submit(client, notebook, session, "cell")["result"]
        ready(client, session)
        client.call("session_dispatch", session_id=session["id"], generation=session["generation"])
    with controller(state) as client:
        assert submit(client, notebook, session, "cell")["result"]["id"] == execution["id"]
        assert client.call("session_execution", execution_id=execution["id"])["result"]["state"] == "dispatched"
        assert client.call("session_dispatch", session_id=session["id"], generation=session["generation"])["result"] is None
        lost = client.call("session_system_lost", session_id=session["id"], generation=session["generation"], reason="controller_restarted")["result"]
        assert lost["cleanup_proven"] is False
        assert client.call("session_execution", execution_id=execution["id"])["result"]["state"] == "unknown"
        assert client.call("session_control", request_id="restart-early", session_id=session["id"], expected_generation=session["generation"], action="restart")["code"] == "conflict"
        assert client.call("session_start", request_id="second", notebook_id=notebook["id"], revision=1, sources={}, web_parent=PARENT)["code"] == "busy"
        # Test-only injected process-cleanup witness: this checks DB fencing,
        # not real namespace/cgroup/descendant or file cleanup.
        client.call("session_system_lost", session_id=session["id"], generation=session["generation"], reason="cleaned", controls={"cleanup_proven": True})
        new = client.call("session_control", request_id="restart", session_id=session["id"], expected_generation=session["generation"], action="restart")["result"]
        assert new["generation"] != session["generation"] and new["automatic_replay"] is False
        assert client.call("session_cancel", request_id="late", execution_id=execution["id"], expected_generation=session["generation"])["code"] == "conflict"
        old = client.call("session_page", session_id=session["id"], generation=session["generation"])["result"]
        assert old["items"][0]["id"] == execution["id"]


def test_revocation_rolls_back_dispatch_and_no_uncommitted_output_is_success(tmp_path):
    with controller(tmp_path/"state") as client:
        notebook, session = setup(client)
        execution = submit(client, notebook, session, "cell")["result"]
        ready(client, session)
        denied = client.call("session_dispatch", session_id=session["id"], generation=session["generation"], controls={"revoke_at_check": 2})
        assert denied["code"] == "operation_rejected"
        assert client.call("session_execution", execution_id=execution["id"])["result"]["state"] == "queued"
        client.call("session_dispatch", session_id=session["id"], generation=session["generation"])
        bad = client.call("session_system_event", execution_id=execution["id"], generation=session["generation"], event="finished", status="succeeded", output_id="o" + "b"*32)
        assert bad["code"] == "conflict"
        assert client.call("session_execution", execution_id=execution["id"])["result"]["state"] == "dispatched"
        cancelled = client.call("session_cancel", request_id="cancel-running", execution_id=execution["id"], expected_generation=session["generation"])["result"]
        assert cancelled["accepted"] and cancelled["generation_will_stop"] and not cancelled["cleanup_proven"]
        assert submit(client, notebook, session, "after-cancel")["code"] == "conflict"
