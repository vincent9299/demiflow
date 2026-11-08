"""Real SQLite rollback/CAS with staged test authority, not production IO/SLA."""
from test_forge_analysis_records import controller,document,source_manifest,question,view,save
from demiflow.workbench.analysis import contracts as c


def reply(client, key, value):
    return client.call("metadata_reply", key=key, value=value)


def pending(value, name):
    result = value["result"]
    assert not result["done"] and result["pending"] == name
    assert not result["in_transaction"] and not result["locked"]
    return result


def test_only_server_provider_binds_generation_environment_across_deferred_checks(tmp_path):
    from test_forge_analysis_sessions import trusted_origin, local_cleanup
    with controller(tmp_path/'db') as client:
        book = client.call('create_notebook', request_id='book', document=document())['result']
        payload = dict(request_id='start', notebook_id=book['id'], revision=1, sources={})
        client.call('metadata_begin', key='start', name='session-start', payload=payload,
            server_environment=trusted_origin())
        value = reply(client, 'start', dict(principal='owner'))
        for _ in range(16):
            if value.get('result',{}).get('done'):
                break
            pending(value, 'permission')
            # Unrelated request controls cannot replace the server provider
            # captured for this original request while its checks suspend.
            value = client.call('metadata_reply', key='start', value=dict(allowed=True),
                controls=dict(server_environment=trusted_origin('d')))
        session = value['result']['value']
        assert session['environment']['body'] == trusted_origin()
        forged = dict(payload, request_id='forged', environment=trusted_origin('d'))
        client.call('metadata_begin', key='forged', name='session-start', payload=forged,
            server_environment=trusted_origin())
        assert reply(client, 'forged', dict(principal='owner'))['code'] == 'invalid_request'
        assert client.call('session_get', session_id=session['id'])['result']['environment']['body'] == trusted_origin()
        client.call('session_system_lost', session_id=session['id'], generation=session['generation'],
            reason='empty_admission_cleanup', controls=dict(cleanup_result=local_cleanup(None)))
        restart = dict(request_id='restart',session_id=session['id'],
            expected_generation=session['generation'],action='restart')
        client.call('metadata_begin',key='restart',name='session-control',payload=restart,
            server_environment=trusted_origin('d'))
        value = reply(client,'restart',dict(principal='owner'))
        for _ in range(16):
            if value.get('result',{}).get('done'):
                break
            pending(value,'permission')
            value = reply(client,'restart',dict(allowed=True))
        current = value['result']['value']
        assert current['generation'] != session['generation']
        assert current['environment']['body'] == trusted_origin('d')


def test_permission_wait_rolls_back_local_document_and_receipt_before_selector_turn(tmp_path):
    with controller(tmp_path/"db") as client:
        pending(client.call("metadata_begin", key="one", name="notebook-create",
            payload=dict(request_id="create", document=document())), "whoami")
        value = reply(client, "one", dict(principal="owner"))
        permissions = []
        for _ in range(12):
            result = pending(value, "permission")
            assert result["documents"] == result["receipts"] == 0
            permissions.append(result["payload"]["permission"])
            # Other metadata and opaque-write guard remain usable during the
            # precommit authority wait; no SQL transaction/mutex survives it.
            assert client.call("list_documents", kind="notebook")["result"]["items"] == []
            assert client.call("object_guard")["result"] == dict(sql_rejected=True)
            value = reply(client, "one", dict(allowed=True))
            if value["result"]["done"]:
                break
        assert value["result"]["done"] and permissions[-1] == "analysis_write"
        assert value["result"]["documents"] == value["result"]["receipts"] == 1
        receipt = client.call("receipt", request_id="create")["result"]
        assert receipt == value["result"]["value"]


def test_last_fresh_denial_cannot_commit_a_previously_prepared_write(tmp_path):
    with controller(tmp_path/"db") as client:
        client.call("metadata_begin", key="one", name="notebook-create",
            payload=dict(request_id="denied", document=document()))
        value = reply(client, "one", dict(principal="owner"))
        # Closed route: read at API entry, write admission, write precommit.
        for _ in range(2):
            pending(value, "permission")
            value = reply(client, "one", dict(allowed=True))
        assert pending(value, "permission")["payload"]["permission"] == "analysis_write"
        assert reply(client, "one", dict(allowed=False))["code"] == "operation_rejected"
        assert client.call("list_documents", kind="notebook")["result"]["items"] == []
        assert client.call("receipt", request_id="denied")["code"] == "not_found"


def test_calibration_principal_is_checked_before_any_metadata_route(tmp_path):
    with controller(tmp_path/"db") as client:
        client.call("metadata_begin", key="one", name="notebook-create", expected_principal="calibration-owned",
            payload=dict(request_id="denied", document=document()))
        assert reply(client, "one", dict(principal="other"))["code"] == "operation_rejected"
        assert client.call("list_documents", kind="notebook")["result"]["items"] == []


def test_concurrent_cas_edit_is_not_silently_rebased_while_authority_is_pending(tmp_path):
    with controller(tmp_path/"db") as client:
        original=client.call("create_notebook", request_id="original", document=document())["result"]
        payload=dict(request_id="pending-edit", notebook_id=original["id"], expected_revision=1, patch=dict(title="pending"))
        client.call("metadata_begin", key="one", name="notebook-patch", payload=payload)
        value=reply(client,"one",dict(principal="owner"))
        for _ in range(2):
            pending(value,"permission");value=reply(client,"one",dict(allowed=True))
        pending(value,"permission")
        assert client.call("patch_notebook",request_id="other-edit",notebook_id=original["id"],
            expected_revision=1,patch=dict(title="other"))["result"]["revision"]==2
        assert reply(client,"one",dict(allowed=True))["code"]=="conflict"
        assert client.call("notebook",notebook_id=original["id"])["result"]["document"]["title"]=="other"
        assert client.call("receipt",request_id="pending-edit")["code"]=="not_found"


def origin(client):
    source=client.call("register_source",manifest=source_manifest())["result"]
    client.call("remember_source",digest=source["digest"],locator=dict(kind="workspace_table",alias="fixed",binding=source["binding"],columns=["input","output"]))
    return source


def begin_annotation(client,source,*,definition=None,origin=None):
    payload=dict(request_id="pending-mark",target=dict(source=source["digest"],case_key="18446744073709551615",variant=None,field=None),
        entries=[dict(definition=definition or question(),expected_revision=0,action="set",value="yes")],
        origin=origin or dict(kind="unsaved_view",reference="draft"))
    client.call("metadata_begin",key="mark",name="annotations-save",payload=payload)
    return reply(client,"mark",dict(principal="owner")),payload


def advance_to_verifier(client,value,*,successes=0):
    seen=0
    for _ in range(32):
        result=value["result"];assert not result["in_transaction"] and not result["locked"]
        if result["pending"]=="verify-source":
            if seen==successes:return value
            seen+=1;value=reply(client,"mark",dict(verified=True))
        else:
            assert result["pending"]=="permission"
            value=reply(client,"mark",dict(allowed=True))
    raise AssertionError("verifier check bound exceeded")


def complete_mark(client,value):
    for _ in range(32):
        if value.get("code") or value.get("result",{}).get("done"):return value
        stage=value["result"]["pending"]
        value=reply(client,"mark",dict(verified=True) if stage=="verify-source" else dict(allowed=True))
    raise AssertionError("metadata check bound exceeded")


def test_last_case_or_source_unavailability_rolls_back_question_answer_and_receipt(tmp_path):
    with controller(tmp_path/"db") as client:
        source=origin(client);value,payload=begin_annotation(client,source)
        value=advance_to_verifier(client,value,successes=1)
        assert value["result"]["payload"]["target"]==payload["target"]
        assert reply(client,"mark",dict(verified=False))["code"]=="conflict"
        assert client.call("receipt",request_id="pending-mark")["code"]=="not_found"
        assert client.call("annotations",target=payload["target"],definition=c.digest(question()))["code"]=="not_found"


def test_annotation_cas_is_rechecked_after_wait_and_old_definition_origin_stays_immutable(tmp_path):
    with controller(tmp_path/"db") as client:
        source=origin(client);descriptor=view(source)
        saved=client.call("save_view",request_id="view-one",view_id=None,expected_revision=0,descriptor=descriptor)["result"]
        reference=saved["id"]+"@1"
        value,payload=begin_annotation(client,source,origin=dict(kind="saved_view",reference=reference))
        value=advance_to_verifier(client,value,successes=1)
        newer={**question(),"title":"New question definition"}
        changed={**descriptor,"questions":[newer]}
        assert client.call("save_view",request_id="view-two",view_id=saved["id"],expected_revision=1,descriptor=changed)["result"]["revision"]==2
        # Another definition has its own immutable answer head; changing the
        # current view must not rewrite the request's explicit v1 origin.
        assert save(client,source,request_id="new-question",definition=newer)["result"]["heads"][0]["revision"]==1
        result=complete_mark(client,reply(client,"mark",dict(verified=True)))
        assert result["result"]["done"] and result["result"]["value"]["definition_origin"]["reference"]==reference
        for definition in (question(),newer):
            history=client.call("annotations",target=payload["target"],definition=c.digest(definition))["result"]
            assert history["definition"]==definition and history["items"][0]["revision"]==1
        # Repeat on a fresh target/head: a concurrent answer to the same old
        # definition during remote wait conflicts, without a second receipt.
    with controller(tmp_path/"other/db") as client:
        source=origin(client);value,payload=begin_annotation(client,source)
        value=advance_to_verifier(client,value,successes=1)
        assert save(client,source,request_id="concurrent")["result"]["heads"][0]["revision"]==1
        assert reply(client,"mark",dict(verified=True))["code"]=="conflict"
        assert client.call("receipt",request_id="pending-mark")["code"]=="not_found"
