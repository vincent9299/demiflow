"""Real file+SQLite publication, revocation and incomplete reservation behavior."""
import hashlib

from test_forge_analysis_records import controller


def reservation(client, request_id="reserve"):
    return client.call("artifact_reserve", request_id=request_id, execution_id="execution", generation=1,
                       retention="ephemeral", parents=[], source_binding=None)["result"]


def receiver(client, item, key="one"):
    assert client.call("receiver_create", key=key, reservation=item, execution_id="execution", native=False)["result"] == {"created": True}


def header(data):
    return dict(operation="notebook-output", execution_id="execution", bytes=len(data), sha256=hashlib.sha256(data.encode()).hexdigest())


def frame(client, kind, value=None, key="one", **controls):
    return client.call("receiver_consume", key=key, kind=kind, value=value, **controls)


def test_output_only_visible_after_complete_bytes_rename_and_db_commit(tmp_path):
    with controller(tmp_path/"db") as client:
        item = reservation(client); receiver(client, item)
        data = '{"outputs":[{"text":"complete immutable output"}]}'
        assert frame(client, "json", header(data))["result"] == {"accepted": True}
        assert frame(client, "blob", data)["result"] is None
        assert client.call("artifact_status", identifier=item["id"])["result"]["ready"] is False
        reply = frame(client, "end")["result"]
        assert reply == dict(committed=True, output_id=item["id"], sha256=header(data)["sha256"])
        ready = client.call("artifact_status", identifier=item["id"])["result"]
        assert ready["ready"] and ready["artifact"]["bytes"] == len(data)
        path = tmp_path/"objects"/item["id"]/"payload/document.json"
        assert path.read_text() == data
        assert path.stat().st_mode & 0o777 == 0o400
        assert client.call("object_guard")["result"] == {"sql_rejected": True}
        assert client.call("object_guard_locked")["result"] == {"locked_and_transaction_rejected": True}


def test_bad_hash_and_revocation_never_publish_and_keep_reserved_charge(tmp_path):
    with controller(tmp_path/"db") as client:
        item = reservation(client); receiver(client, item)
        frame(client, "json", header("abc"))
        frame(client, "blob", "abd")
        assert frame(client, "end")["code"] == "conflict"
        state = client.call("artifact_status", identifier=item["id"])["result"]
        assert state["state"] == "receiving" and state["held_bytes"] == 128*1024*1024
        assert not (tmp_path/"objects"/item["id"]).exists()
    # A disconnected receiver never deletes its incomplete staging tree.
    assert (tmp_path/".staging"/item["publish_id"]).exists()
    other = tmp_path/"other"
    with controller(other/"db") as client:
        item = reservation(client); receiver(client, item)
        frame(client, "json", header("abc")); frame(client, "blob", "abc")
        assert frame(client, "end", controls={"permissions": ["read"]})["code"] == "operation_rejected"
        assert client.call("artifact_status", identifier=item["id"])["result"]["ready"] is False
        assert not (other/"objects"/item["id"]).exists()
