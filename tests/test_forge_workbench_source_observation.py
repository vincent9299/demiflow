"""Source observation remains useful when strict compute admission is incomplete."""
import hashlib
import json

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.graph import model
from demiflow.workbench.graph import source_view
from test_forge_project_discovery import original, business_bytes, register_fixture_flow
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
    author(original);register_fixture_flow(original);before=business_bytes(original)
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
    checked=project_ops.check_project(original)
    assert checked['status']!='checked' and any(d.get('rule')=='OP-06' for d in checked['diagnostics'])
    assert business_bytes(original)==before
    (original/'dispatch.py').write_text((original/'dispatch.py').read_text()+'\n# source changed\n')
    with pytest.raises(ValueError):
        source_view.get(original,graph_binding=value['binding'],node_id=value['nodes'][0]['id'],kind='implementation')


@pytest.mark.parametrize('calls',[True,False,'cached'])
def test_browser_unadmitted_source_keeps_diagnostics_without_fake_execution_graph(original,tmp_path,browser,calls):
    author(original,calls=calls);register_fixture_flow(original);before=business_bytes(original)
    registry=tmp_path/'registry.json';console.add(registry,original)
    with service(None,registry=registry,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        posts=[];page.on('request',lambda r:posts.append(r.url) if r.method=='POST' else None)
        page.locator('.function-nav').get_by_role('button',name='管线',exact=True).click()
        # Syntax observations remain available to diagnostics, but the product
        # must never substitute them for a Dataset execution plan.
        expect(page.locator('.wb-graph-empty')).to_contain_text('尚无运行执行图', timeout=15000)
        expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy','true')
        expect(page.locator('.wb-node')).to_have_count(0)
        compile_plan = page.get_by_role('button', name='解析执行计划', exact=True)
        expect(compile_plan).to_be_enabled()
        with page.expect_response(lambda response: response.url.endswith('/plan-preview')) as pending:
            compile_plan.click()
        response = pending.value
        assert response.status != 200 or not response.json().get('available')
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-actions').get_by_role('button',name='运行',exact=True)).to_be_disabled()
        page.locator('.wb-more > summary').click()
        page.get_by_role('button',name='检查问题',exact=True).click()
        expect(page.locator('.wb-check-issues')).to_contain_text('OP-06')
        assert not errors and business_bytes(original)==before
        # Read-only native compilation may be attempted automatically; no
        # execution, writes, or synthetic static-to-runtime promotion is allowed.
        assert all(url.endswith('/plan-preview') for url in posts)
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
