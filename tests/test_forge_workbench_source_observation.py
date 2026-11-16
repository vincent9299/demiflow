"""Source observation remains useful when strict compute admission is incomplete."""
import hashlib
import json

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.graph import model
from demiflow.workbench.graph import source_view
from test_forge_project_discovery import original, business_bytes
from test_forge_public_api import service
from test_forge_markdown_reading import browser
from test_forge_workbench_browser import page_for, pipeline


def author(root, *, calls=True):
    if calls=='cached':
        (root/'dispatch.py').write_text('from dependency import local_clean\n'
            'def config(**values):\n    return values\n'
            'def run_pipeline(cfg):\n    return local_clean(cfg)\n')
        (root/'dependency.py').write_text('import argparse\n'
            'raise RuntimeError("dependency must never be imported")\n'
            'def local_clean(value):\n    return value\n')
        return
    (root/'dispatch.py').write_text('import argparse\n'
        'raise RuntimeError("source observation must never import this file")\n'
        'def config(**values):\n    return values\n'
        'def local_clean(value):\n    return value\n'
        'def run_pipeline(cfg):\n'+('    return local_clean(cfg)\n' if calls else '    return cfg\n'))


def test_unadmitted_import_still_has_sha_bound_syntax_calls_without_compute_admission(original,tmp_path):
    author(original);before=business_bytes(original)
    console.add(tmp_path/'registry.json',original)
    value=model.get(original,basis_kind='source',entry='dispatch.py:run_pipeline')
    assert value['status']=='not_checked' and value['completeness']['semantic']=='unchecked'
    assert len(value['nodes'])==1
    assert all(edge['type']=='reference' for edge in value['edges'])
    assert value['nodes'][0]['api']=='local_clean'
    assert any(d['code']=='OP-06' and 'argparse' in d['detail'] for d in value['diagnostics'])
    source=source_view.get(original,graph_binding=value['binding'],node_id=value['nodes'][0]['id'],kind='implementation')
    assert source['path']=='dispatch.py' and 'local_clean(cfg)' in source['text']
    assert source['kind']=='callsite' and source['reason']=='source_callsite_only'
    # The current checker keeps its original strict admission result.
    checked=ws.check_project(original)
    assert checked['status']!='checked' and any(d.get('code')=='OP-06' for d in checked['diagnostics'])
    assert not ws.workspace_context(original)['flows'] and business_bytes(original)==before
    (original/'dispatch.py').write_text((original/'dispatch.py').read_text()+'\n# source changed\n')
    with pytest.raises(ValueError):
        source_view.get(original,graph_binding=value['binding'],node_id=value['nodes'][0]['id'],kind='implementation')


@pytest.mark.parametrize('calls',[True,False,'cached'])
def test_browser_unadmitted_source_and_empty_graph_finish_with_explicit_state(original,tmp_path,browser,calls):
    author(original,calls=calls);before=business_bytes(original)
    registry=tmp_path/'registry.json';console.add(registry,original)
    with service(None,registry=registry,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        posts=[];page.on('request',lambda r:posts.append(r.url) if r.method=='POST' else None)
        page.locator('.function-nav').get_by_role('button',name='管道',exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('源码观察图')
        expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy','true')
        if calls:
            expect(page.locator('.wb-node')).to_have_count(1)
            page.locator('.wb-node').first.click()
            expect(page.locator('.wb-source-body')).to_contain_text('local_clean(cfg)')
        else:
            expect(page.locator('.wb-inspector')).to_contain_text('argparse')
            expect(page.locator('.wb-source-body')).to_contain_text('没有可展示的调用节点')
            expect(page.locator('.wb-source-body')).not_to_contain_text('正在读取')
            assert page.locator('.wb-node').count()==0
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_disabled()
        page.get_by_role('button',name='检查问题',exact=True).click()
        expect(page.locator('.wb-check-issues')).to_contain_text('OP-06')
        assert not errors and not posts and business_bytes(original)==before
        evidence=tmp_path/'evidence'
        evidence.mkdir(exist_ok=True)
        page.screenshot(path=str(evidence/('source-observation-'+('cached-dependency' if calls=='cached' else 'calls' if calls else 'empty')+'.png')),full_page=True)


@pytest.mark.parametrize('text,limits,rule',[
    ('import argparse\ndef run():\n    return 1\n',{'file_nodes':4},'FL-10'),
    ('import argparse\ndef run(:\n    pass\n',{},'FL-01'),
    ('import argparse\ndef run(): pass\ndef run(): pass\n',{},'FL-02'),
])
def test_syntax_observation_preserves_resource_parse_and_ambiguous_entry_failures(tmp_path,text,limits,rule):
    from demiflow.forge.source import SourceTree,InspectionError
    (tmp_path/'entry.py').write_text(text)
    tree=SourceTree(tmp_path,limits);diagnostics=[]
    with pytest.raises(InspectionError) as caught:
        model._entry_definition(tree,'entry','entry.py','run',diagnostics)
    assert caught.value.diagnostic['code']==rule and not diagnostics


def test_local_dependency_failure_reuses_cached_entry_ast_within_cumulative_budget(tmp_path,monkeypatch):
    import ast
    from demiflow.forge.source import SourceTree,Definition
    entry='from dependency import clean\ndef run(value):\n'+''.join(
        '    item'+str(i)+' = clean(value)\n' for i in range(30))+'    return item29\n'
    dependency='import argparse\ndef clean(value):\n    return value\n'
    (tmp_path/'entry.py').write_text(entry);(tmp_path/'dependency.py').write_text(dependency)
    budget=sum(sum(1 for _ in ast.walk(ast.parse(text))) for text in [entry,dependency])
    parsed=[];parse=ast.parse
    def observed(*args,**kwargs):
        parsed.append(str(kwargs.get('filename','')));return parse(*args,**kwargs)
    monkeypatch.setattr(ast,'parse',observed)
    tree=SourceTree(tmp_path,{'ast_nodes':budget});diagnostics=[]
    namespace,definition=model._entry_definition(tree,'entry','entry.py','run',diagnostics)
    assert namespace=={} and definition is tree.modules['entry']['run']
    assert len(parsed)==2 and tree.nodes==budget
    retained=[v.node for module in tree.modules.values() for v in module.values() if isinstance(v,Definition)]+[definition.node]
    assert len({id(n) for node in retained for n in ast.walk(node)})<=budget
    assert any(d['code']=='OP-06' for d in diagnostics)
