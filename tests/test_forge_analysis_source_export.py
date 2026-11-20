"""Real fixed Lance/Arrow export in existing bounded workspace_worker.

Owned synthetic demo only. This is not OCI, Jupyter, analysis materialization,
an externally bound persistent volume or B1 resource/permission acceptance.
"""
import json
import os

import pyarrow as pa
import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.analysis.arrow_bridge import decoded_batches
from demiflow.workbench.analysis.materialize import input_chunks
from demiflow.forge.schema import ForgeError
from demiflow.workbench.worker import operation
from test_forge_workspace import demo,isolated


def source(root):
    description=project_ops.describe_table(root,"candidates")
    locator=dict(kind="workspace_table",alias="candidates",binding=description["binding"]["digest"],columns=["case_id","question"])
    return locator,web_sources.resolve(root,locator)["source"]


def test_real_fixed_projected_export_roundtrip_retains_content_and_null_types(isolated,tmp_path):
    locator,snapshot=source(isolated)
    output=tmp_path/"source-input.bin"
    fd=os.open(output,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
    try:
        result=operation("analysis_export",isolated,source_locator=locator,source_snapshot=snapshot["digest"],output_fd=fd,_pass_fds=(fd,))
    finally:os.close(fd)
    config=dict(operation="materialize",input_id="i"+"a"*32,publish_id="owned-test",**{key:result[key] for key in ("stream_sha256","schema_sha256","encoded_bytes","chunks")})
    decoded=pa.Table.from_batches(decoded_batches(input_chunks(output,config)))
    assert decoded.num_rows==snapshot["rows"]==result["rows"]
    assert decoded.schema.names==["case_id","question"]
    original=project_ops.sample_table(isolated,"candidates",columns=locator["columns"],expected_binding=locator["binding"],limit=100)
    assert decoded.to_pylist()==[{key:row[key] for key in locator["columns"]} for row in original["rows"]]
    assert result["source_snapshot"]==snapshot["digest"] and output.stat().st_size==result["encoded_bytes"]


def test_alias_rebinding_or_invalid_fd_never_exports_a_newer_table(isolated,tmp_path):
    locator,snapshot=source(isolated)
    control=isolated/"tables/candidates.json";value=json.loads(control.read_bytes())
    value["ref"]["lance_version"]-=1;control.write_text(json.dumps(value))
    output=tmp_path/"refused.bin";fd=os.open(output,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with pytest.raises(ForgeError) as error:
            operation("analysis_export",isolated,source_locator=locator,source_snapshot=snapshot["digest"],output_fd=fd,_pass_fds=(fd,))
        assert error.value.code=="conflict" and output.stat().st_size==0
    finally:os.close(fd)
    with pytest.raises(ForgeError):
        operation("analysis_export",isolated,source_locator=locator,source_snapshot=snapshot["digest"],output_fd=1)
