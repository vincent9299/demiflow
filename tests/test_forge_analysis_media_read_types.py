"""Closed native/Web reply types reject malformed frames before byte return."""
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis.output_reads import OutputReads
from demiflow.workbench.analysis.sources import Sources
from demiflow.forge.schema import ForgeError
from test_forge_analysis_presentations import store


@pytest.mark.parametrize('header',[None,[],False,'null',0])
def test_pending_probe_nonobject_is_a_closed_failure_without_touching_an_owner(header):
    with pytest.raises(ForgeError):OutputReads._probe_header({},header)


@pytest.mark.parametrize('field,value',[(key,value) for key in ('status','total','start','end') for value in (False,True,'0',0.0)])
def test_media_range_metadata_never_adopts_boolean_or_coerced_numbers(field,value):
    artifact=dict(bytes=7,mime='image/png',sha256='a'*64,source_digest='b'*64,descriptor={})
    owner=dict(config=dict(artifact=artifact,range='bytes=0-0'))
    header=dict(status=206,total=7,start=0,end=0,**{key:artifact[key] for key in ('mime','sha256','source_digest','descriptor')})
    OutputReads._media_metadata(owner,header)
    with pytest.raises(ForgeError):OutputReads._media_metadata(owner,dict(header,**{field:value}))


@pytest.mark.parametrize('fields',[None,False,{},'fields',[]])
def test_web_resource_proof_is_a_bounded_list_before_comparing_rows(fields):
    with store() as (records,access,witness,raw,source,descriptor):
        sources=Sources(records,'/unused-private-fixture')
        sources._remember(access,source,dict(kind='external',locator=dict(kind='workspace_table',alias='rows',binding=source['binding'],columns=['input','output'])))
        authority=SimpleNamespace(access=lambda:access,call=lambda *args:dict(source_snapshot=source['digest'],case_key='1',fields=fields))
        with pytest.raises(ForgeError):sources.media_sources(authority,source,'1',['input'])
