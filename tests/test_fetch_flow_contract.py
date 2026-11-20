"""Same native facts in read-only Forge inspection and actual raw acquisition."""
from pathlib import Path
import pytest
from demiflow import data
from demiflow.forge.platform import inspect_modules,LIMITS
from demiflow.forge.flow_check import FlowChecker
from demiflow.workbench.projects import workspace as ws
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.run_context import RunContext
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from test_collect_flow_contract import base,root
from test_operator_foundation_models import load
from test_raw_fetch import origin_server,BODY


def test_forge_inspection_and_actual_fetch_share_native_contract(root,monkeypatch):
    with origin_server() as (url,calls):
        source=root/'flows/acquire.py'
        source.write_text('from demiflow import data\nfrom demiflow.objects import LocalObjectStore\n'
            'from demiflow.collect.raw_contract import FetchPolicy,HttpSource\n'
            'def run():\n'
            f"    return (data.from_items([{{'request':{{'uri':{(url+'/file')!r}}}}}])\n"
            f"        .fetch(store=LocalObjectStore({str(root/'data')!r}),policy=FetchPolicy(max_object_bytes=131072,http=HttpSource(headers={{}},allow_private=True)),concurrency=2)\n"
            '        .materialize())\n')
        catalog=inspect_modules(['demiflow.data','demiflow.objects','demiflow.collect.raw_contract'],Path(data.__file__).resolve().parents[1],LIMITS)
        assert catalog['inspection_complete'],catalog['diagnostics']
        report=FlowChecker(root,catalog,{},[],resource_resolver=lambda tree,refs,bindings:
            ws.resolve_flow_resources(root,tree,refs,bindings)).inspect('flows/acquire.py:run',{})
        assert report['status']=='checked',report['diagnostics']
        assert not calls and not list((root/'data').iterdir())
        fetched=[n for n in report['nodes'] if n['kind']=='fetch']
        assert len(fetched)==1 and fetched[0]['analysis']['native']['contract']=='demiflow.collect.fetch.v1'
        flow=load(monkeypatch,'flows.acquire',source)
        context=CheckedExecution(nodes=report['nodes'],edges=report['edges'],actions=report['actions'],
            operators=report['operators'],source_root=root,limits=DEFAULT_LIMITS,record=lambda *a:None,check_stop=lambda:None)
        with RunContext.start(root/'native-run'):
            with context.bind_context():result=flow.run()
        row,=result.take_all()
        assert row['result']['status']=='ok' and row['result']['size_bytes']==len(BODY) and calls['/file']==1
