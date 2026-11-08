"""Real atomic fixed-input binding; fake native file payload/cleanup, not OCI."""
from test_forge_analysis_records import controller,document
from test_forge_analysis_sessions import PARENT,JOB
from test_forge_analysis_object_files import arguments
from test_forge_analysis_artifacts import stage


def setup(client):
    kwargs,_=arguments();description=kwargs["description"]
    source=client.call("register_source",manifest=dict(kind="fixed_lance",binding="a"*64,schema=description["schema"],
        rows=description["rows"],case_key=dict(kind="row_id",field=None),retention="external"))["result"]
    notebook=client.call("create_notebook",request_id="book",document=document())["result"]
    session=client.call("session_start",request_id="start",notebook_id=notebook["id"],revision=1,
        sources=dict(left=source["digest"],right=source["digest"]),web_parent=PARENT)["result"]
    scope=dict(session_id=session["id"],generation=session["generation"],execution_id=session["id"])
    return source,session,scope


def test_input_scope_cannot_run_sql_and_binding_waits_for_native_object_then_commits_all_aliases(tmp_path):
    state=tmp_path/"state";state.mkdir(mode=0o700)
    with controller(state/"db") as client:
        source,session,scope=setup(client)
        bad=client.call("ops_admit",bootstrap=True,scope=scope,request_id="bad",native_operation="query",payload=dict(sql="SELECT 1",sources={}))
        assert bad["code"]=="operation_rejected" or bad["code"]=="invalid_request"
        admission=client.call("ops_admit",bootstrap=True,scope=scope,request_id="source",native_operation="import",payload=dict(source_snapshot=source["digest"]))["result"]
        row=client.call("ops_system_get",identifier=admission["operation_id"])["result"]
        metadata=dict(source_snapshot=source["digest"],schema_sha256="c"*64,stream_sha256="d"*64,encoded_bytes=123,chunks=1,rows=2)
        assert client.call("ops_system_import_metadata",bootstrap=True,scope=scope,identifier=row["id"],metadata=metadata)["result"] is None
        assert client.call("session_inputs",session_id=session["id"],generation=session["generation"])["result"]==[]
        client.call("ops_system_attach",bootstrap=True,scope=scope,identifier=row["id"],runtime_job=JOB)
        receiver,proof=stage(client,state,row["reservation"]["id"])
        client.call("artifact_begin_publish",identifier=row["reservation"]["id"],expected_digest=proof["manifest_digest"])
        receiver.rename(authorize=lambda *_:True)
        handle=client.call("artifact_commit_ready",identifier=row["reservation"]["id"],expected_digest=proof["manifest_digest"])["result"]["handle"]
        assert client.call("ops_system_finish",bootstrap=True,scope=scope,identifier=row["id"],value=handle)["result"] is None
        bound=client.call("session_inputs",session_id=session["id"],generation=session["generation"])["result"]
        assert [item["alias"] for item in bound]==["left","right"]
        assert all(item["source_snapshot"]==source["digest"] and item["handle"]["id"]==handle["id"] for item in bound)
        assert receiver.manifest["source_binding"]==source["digest"]


def test_wrong_source_transfer_or_stopped_admission_never_binds_input(tmp_path):
    with controller(tmp_path/"db") as client:
        source,session,scope=setup(client)
        row=client.call("ops_admit",bootstrap=True,scope=scope,request_id="source",native_operation="import",payload=dict(source_snapshot=source["digest"]))["result"]
        metadata=dict(source_snapshot="f"*64,schema_sha256="c"*64,stream_sha256="d"*64,encoded_bytes=123,chunks=1,rows=2)
        assert client.call("ops_system_import_metadata",bootstrap=True,scope=scope,identifier=row["operation_id"],metadata=metadata)["code"]=="conflict"
        client.call("session_control",request_id="stop",session_id=session["id"],expected_generation=session["generation"],action="stop")
        metadata["source_snapshot"]=source["digest"]
        assert client.call("ops_system_import_metadata",bootstrap=True,scope=scope,identifier=row["operation_id"],metadata=metadata)["code"]=="conflict"
        assert client.call("session_inputs",session_id=session["id"],generation=session["generation"])["result"]==[]
