"""Source/version/Case proof contracts with fake bounded workspace readers."""
import pytest

from demiflow.workbench.analysis import web_sources
from demiflow.forge.schema import ForgeError


def locator():
    return dict(kind="workspace_table",alias="orders",binding="b"*64,columns=["input","output"])


def description():
    return dict(binding=dict(digest="b"*64),metadata=dict(row_count=2,fields={
        "input":dict(type=dict(kind="str",physical="string",nullable=False)),
        "output":dict(type=dict(kind="float",physical="double",nullable=True))}))


def test_exact_projection_and_fixed_case_membership_without_reading_payload_fields(tmp_path,monkeypatch):
    def describe(root,alias,*,expected_binding):
        assert root==tmp_path and alias=="orders" and expected_binding=="b"*64
        return description()
    monkeypatch.setattr(web_sources.ws,"describe_table",describe)
    resolved=web_sources.resolve(tmp_path,locator());source=resolved["source"]
    assert source["schema"]==[dict(name="input",type="utf8",nullable=False),dict(name="output",type="float64",nullable=True)]
    def sample(root,alias,**kwargs):
        assert kwargs==dict(expected_binding="b"*64,columns=[],row_id=2**64-1)
        return dict(rows=[dict(_rowid=str(2**64-1))])
    monkeypatch.setattr(web_sources.ws,"sample_table",sample)
    target=dict(source=source["digest"],case_key=str(2**64-1),variant=None,field="output")
    assert web_sources.verify(tmp_path,locator=locator(),source=source,target=target)=={"verified":True}
    monkeypatch.setattr(web_sources.ws,"sample_table",lambda *a,**kw:dict(rows=[]))
    with pytest.raises(ForgeError,match="does not exist"):
        web_sources.verify(tmp_path,locator=locator(),source=source,target=target)


def test_rebound_source_and_caller_uri_or_variant_label_are_not_proof(tmp_path,monkeypatch):
    monkeypatch.setattr(web_sources.ws,"describe_table",lambda *a,**kw:description())
    source=web_sources.resolve(tmp_path,locator())["source"]
    with pytest.raises(ForgeError):web_sources.resolve(tmp_path,locator()|{"uri":"/unregistered/data.lance"})
    bad=description();bad["metadata"]["row_count"]=3
    monkeypatch.setattr(web_sources.ws,"describe_table",lambda *a,**kw:bad)
    with pytest.raises(ForgeError,match="receipt differs"):
        web_sources.verify(tmp_path,locator=locator(),source=source,target=None)
    monkeypatch.setattr(web_sources.ws,"describe_table",lambda *a,**kw:description())
    target=dict(source=source["digest"],case_key="0",variant="invented",field=None)
    with pytest.raises(ForgeError,match="variant"):
        web_sources.verify(tmp_path,locator=locator(),source=source,target=target)


def test_struct_projection_preserves_parent_nullability_and_literal_column_precedence():
    metadata=dict(row_count=2,fields={
        'payload':dict(type=dict(kind='record',nullable=True,fields={'score':dict(type=dict(kind='float',physical='double',nullable=False))})),
        'payload.score':dict(type=dict(kind='str',physical='string',nullable=False))})
    value={**locator(),'columns':['payload.score']}
    assert web_sources._snapshot(metadata,value)['schema']==[dict(name='payload.score',type='utf8',nullable=False)]
    del metadata['fields']['payload.score']
    assert web_sources._snapshot(metadata,value)['schema']==[dict(name='payload.score',type='float64',nullable=True)]
    with pytest.raises(ForgeError):web_sources._snapshot(metadata,{**value,'columns':['payload.missing']})
