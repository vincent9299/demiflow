"""Source read/profile budgets and fixed-version existing-reader integration.

One real owned demo read is not OCI/Jupyter/analysis SQL or B1 acceptance.
"""
from types import SimpleNamespace

import pyarrow as pa
import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.analysis import source_query
from demiflow.workbench.analysis import web_sources
from demiflow.forge.schema import ForgeError
from demiflow.workbench.worker import operation
from test_forge_workspace import demo,isolated


def source(rows=1001):
    return dict(rows=rows,schema=[dict(name="value",type="utf8",nullable=True)])


def test_projection_and_budget_policy_cannot_be_replaced_with_sql_or_paths():
    assert source_query.request(dict(kind="sample",columns=["value"],limit=2),source())["limit"]==2
    for query in (dict(kind="profile",columns=["value"],limit=100000),
        dict(kind="sample",columns=["value"],uri="/outside"),
        dict(kind="sample",columns=["other"]),dict(kind="sample",columns=["value"],limit=101)):
        with pytest.raises(ForgeError):source_query.request(query,source())


def test_profile_explicit_byte_coverage_and_nonfinite_null_complex_counts(monkeypatch):
    class Batch:
        nbytes=1024*1024;num_rows=1
        def get_total_buffer_size(self):return self.nbytes
        def to_pylist(self):return [dict(value=None if calls.pop(0)==0 else "label")]
    calls=list(range(10))
    class Reader:
        closed=False
        def __iter__(self):return iter([Batch() for _ in range(10)])
        def close(self):self.closed=True
    reader=Reader();settings=[]
    snapshot=SimpleNamespace(schema=pa.schema([pa.field("value",pa.string())]),scanner=lambda **kw:settings.append(kw) or SimpleNamespace(to_batches=lambda:reader))
    result=source_query._profile(snapshot,source(),["value"])
    assert result["coverage"]["rows_scanned"]==8 and not result["coverage"]["complete"]
    assert result["coverage"]["stopped_by"]=="bytes" and result["fields"][0]["null_count"]==1
    assert result["fields"][0]["enumeration"]==[dict(value="label",count=7)]
    assert reader.closed and settings[0]["batch_size"]==1 and settings[0]["limit"]==1000


def test_oversized_native_batch_is_rejected_before_python_expansion():
    class Batch:
        nbytes=1;num_rows=1
        def get_total_buffer_size(self):return 33*1024*1024
        def to_pylist(self):pytest.fail("must reject before expansion")
    class Reader:
        def __iter__(self):return iter([Batch()])
        def close(self):pass
    snapshot=SimpleNamespace(schema=pa.schema([pa.field("value",pa.string())]),scanner=lambda **kw:SimpleNamespace(to_batches=Reader))
    with pytest.raises(ForgeError) as error:source_query._profile(snapshot,source(),["value"])
    assert error.value.code=="resource_limit"


def test_real_fixed_source_sample_and_complete_small_table_profile(isolated):
    description=ws.describe_table(isolated,"candidates")
    locator=dict(kind="workspace_table",alias="candidates",binding=description["binding"]["digest"],columns=["case_id","question"])
    fixed=web_sources.resolve(isolated,locator)["source"]
    sample=operation("analysis_source_read",isolated,source_locator=locator,source_snapshot=fixed["digest"],
        query=dict(kind="sample",columns=["question"],offset=0,limit=2))
    assert len(sample["rows"])==2 and all(type(row["_rowid"]) is str for row in sample["rows"])
    assert sample["source_snapshot"]==fixed["digest"] and sample["columns"]==["question"]
    profile=operation("analysis_source_read",isolated,source_locator=locator,source_snapshot=fixed["digest"],
        query=dict(kind="profile",columns=["case_id"]))
    assert profile["coverage"]["rows_scanned"]==fixed["rows"] and profile["coverage"]["complete"]
    assert sum(item["count"] for item in profile["fields"][0]["enumeration"])+profile["fields"][0]["other_short_primitive_count"]==fixed["rows"]
    with pytest.raises(ForgeError) as error:
        operation("analysis_source_read",isolated,source_locator=locator,source_snapshot="f"*64,query=dict(kind="sample",columns=["question"]))
    assert error.value.code=="conflict"
