"""Real atomic analysis admission/replay/quota; injected runtime cleanup witness."""
from test_forge_analysis_records import controller
from test_forge_analysis_sessions import setup, submit, ready


def test_trusted_native_cleanup_releases_original_slot_without_claiming_oci_proof(tmp_path):
    from test_forge_analysis_records import document
    from test_forge_analysis_sessions import trusted_origin, local_cleanup, PARENT, JOB
    state = tmp_path/'db'
    with controller(state) as client:
        book = client.call('create_notebook', request_id='create', document=document())['result']
        origin = trusted_origin()
        origin['media_tool'] = dict(path='/fixed/bin/ffprobe', sha256='f'*64, version='fixed-test')
        session = client.call('session_start', request_id='start', notebook_id=book['id'],
            revision=1, sources={}, web_parent=PARENT, environment=origin)['result']
        execution = submit(client, book, session, 'cell')['result']
        ready(client, session)
        client.call('session_dispatch', session_id=session['id'], generation=session['generation'])
        scope = dict(session_id=session['id'], generation=session['generation'], execution_id=execution['id'])
        admitted = client.call('ops_admit', scope=scope, request_id='publish-one',
            native_operation='publish', payload=commitment())['result']
        identifier = admitted['operation_id']
        # Only metadata ownership/slot fencing is tested here. No native
        # process or file cleanup is simulated as real backend evidence.
        unproven = client.call('ops_system_cleanup', identifier=identifier, reason='legacy_boolean',
            controls=dict(cleanup_proven=True))['result']
        assert not unproven['cleanup_proven'] and not unproven['cleanup_complete']
        assert client.call('ops_admit', scope=scope, request_id='blocked',
            native_operation='publish', payload=commitment())['code'] == 'busy'
        assert client.call('ops_system_cleanup', identifier=identifier, reason='foreign_job',
            controls=dict(cleanup_result=local_cleanup(JOB)))['code'] == 'conflict'
        assert client.call('ops_system_cleanup', identifier=identifier, reason='foreign_mode',
            controls=dict(cleanup_result=local_cleanup(None, mode='oci')))['code'] == 'conflict'
        clean = client.call('ops_system_cleanup', identifier=identifier, reason='managed_cleanup',
            controls=dict(cleanup_result=local_cleanup(None)))['result']
        assert clean['cleanup_complete'] and not clean['cleanup_proven']
        assert clean['cleanup_receipt']['scope'] == 'managed_processes'
        row = client.call('ops_system_get', identifier=identifier)['result']
        assert row['held_input_bytes'] == 0 and row['cleanup_proven'] == 0
        assert client.call('ops_admit', scope=scope, request_id='publish-two',
            native_operation='publish', payload=commitment())['result']['execution_started'] is False
        late = client.call('ops_system_cleanup', identifier=identifier, reason='late_unknown')['result']
        assert late['cleanup_complete'] and late['cleanup_receipt_digest'] == clean['cleanup_receipt_digest']
        original_digest = session['environment']['digest']
    with controller(state) as client:
        receipt = client.call('ops_receipt', session_id=session['id'], generation=session['generation'],
            client_request='publish-one')['result']
        assert receipt['cleanup_complete'] and not receipt['cleanup_proven']
        assert receipt['environment'] == dict(digest=original_digest, mode='trusted_local')
        assert receipt['cleanup_receipt_digest'] == clean['cleanup_receipt_digest']


def active(client):
    notebook, session = setup(client)
    execution = submit(client, notebook, session, "cell")["result"]
    ready(client, session)
    client.call("session_dispatch", session_id=session["id"], generation=session["generation"])
    return dict(session_id=session["id"], generation=session["generation"], execution_id=execution["id"])


def commitment():
    return dict(schema_sha256="a"*64, stream_sha256="b"*64, encoded_bytes=128, chunks=1)


def test_atomic_reservation_replay_content_identity_and_cleanup_gate(tmp_path):
    with controller(tmp_path/"db") as client:
        scope = active(client)
        admission = client.call("ops_admit", scope=scope, request_id="publish-one", native_operation="publish", payload=commitment())["result"]
        replay = client.call("ops_admit", scope=scope, request_id="publish-one", native_operation="publish", payload=commitment())["result"]
        assert replay["replayed"] and replay["operation_id"] == admission["operation_id"]
        changed = commitment() | {"stream_sha256": "c"*64}
        assert client.call("ops_admit", scope=scope, request_id="publish-one", native_operation="publish", payload=changed)["code"] == "conflict"
        assert client.call("ops_admit", scope=scope, request_id="publish-two", native_operation="publish", payload=commitment())["code"] == "busy"
        row = client.call("ops_system_get", identifier=admission["operation_id"])["result"]
        assert row["held_input_bytes"] == 64*1024*1024 and row["reservation"]["reserved_bytes"] == 128*1024*1024
        assert row["job_id"] is None and not (tmp_path/"transfer").exists()
        pending = client.call("ops_system_cleanup", identifier=row["id"], reason="disconnected")["result"]
        assert not pending["cleanup_proven"]
        assert client.call("ops_admit", scope=scope, request_id="publish-two", native_operation="publish", payload=commitment())["code"] == "busy"
        clean = client.call("ops_system_cleanup", identifier=row["id"], reason="test_owned_cleanup", controls={"cleanup_proven": True})["result"]
        assert clean["state"] == "unknown" and clean["cleanup_proven"] and not clean["automatic_replay"]
        assert client.call("ops_admit", scope=scope, request_id="publish-two", native_operation="publish", payload=commitment())["result"]["execution_started"] is False
        receipt = client.call("ops_receipt", session_id=scope["session_id"], generation=scope["generation"], client_request="publish-one")["result"]
        assert receipt["value"] is None and receipt["state"] == "unknown"


def test_revoked_atomic_admission_leaves_no_operation_receipt_or_reservation(tmp_path):
    with controller(tmp_path/"db") as client:
        scope = active(client)
        rejected = client.call("ops_admit", scope=scope, request_id="publish", native_operation="publish", payload=commitment(), controls={"revoke_at_check": 3})
        assert rejected["code"] == "operation_rejected"
        assert client.call("ops_receipt", session_id=scope["session_id"], generation=scope["generation"], client_request="publish")["code"] == "not_found"
        accepted = client.call("ops_admit", scope=scope, request_id="publish", native_operation="publish", payload=commitment())["result"]
        assert not accepted["replayed"]
        wrong = dict(scope, generation="old-generation")
        assert client.call("ops_admit", scope=wrong, request_id="other", native_operation="publish", payload=commitment())["code"] == "conflict"


def test_unregistered_or_foreign_table_reference_never_enters_native_slot(tmp_path):
    with controller(tmp_path/"db") as client:
        scope = active(client)
        ref = dict(id="o"+"a"*32, generation=1, digest="b"*64)
        assert client.call("ops_admit", scope=scope, request_id="query", native_operation="query", payload=dict(sql="SELECT * FROM source", sources={"source": ref}))["code"] == "not_found"
        assert client.call("ops_admit", scope=scope, request_id="read", native_operation="read", payload=dict(handle=ref))["code"] == "not_found"
        assert client.call("ops_admit", scope=scope, request_id="pin", native_operation="pin", payload=dict(handle=ref), controls={"permissions": ["read", "analysis_execute"]})["code"] == "operation_rejected"
