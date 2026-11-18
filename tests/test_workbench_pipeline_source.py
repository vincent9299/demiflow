"""Actual compiler/HTTP provenance: no table-name matching or source fallback."""
import json
from urllib.parse import urlencode
import pytest

from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws, registry
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_markdown_reading import browser
from test_forge_public_api import service, request
from test_forge_web_redesign import opened
from d19_fixtures import owned_fixture


CODE = '''from demiflow import data
from pathlib import Path

def config(): return {}

def run_pipeline():
    # Two reads of the SAME table on the SAME line must remain distinct.
    table = str(Path(__file__).parent / 'records/orders.lance')
    first = data.read_lance(table, version=1, columns=['order']); second = data.read_lance(table, version=1, columns=['amount'])
    return first.union(second).take_all()
    raise AssertionError('AFTER_FIRST_ACTION_NOT_EXECUTED')
'''


def address(preview):
    return dict(kind='preview', **{k: preview['basis'][k] for k in ('entry', 'params', 'digest', 'dag_id')})


def test_read_callsite_and_entire_pipeline_share_immutable_preview(original):
    entry = original / 'dispatch.py'
    entry.write_text(CODE)
    ws.init_workspace(original, existing=True)
    before = business_bytes(original)
    with service(original) as app:
        status, preview = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=None))
        assert status == 200, preview
        assert '_pipeline_detail' not in preview and '_node_details' not in preview
        readers = [n for n in preview['dag']['nodes'] if n['op_kind'] == 'lance']
        assert len(readers) == 2
        texts = []
        for node in readers:
            assert node['source_views']['callsite']['available'], node['source_views']
            status, detail = request(app, '/api/plan-node-detail', dict(basis=address(preview),
                logical_node_id=node['logical_node_id'], occurrence_id=node['occurrence_id'], kind='callsite'))
            assert status == 200, detail
            assert detail['path'] == 'shipping/dispatch.py'
            assert detail['start_line'] == detail['end_line'] == 9
            texts.append(detail['text'])
            status, parameters=request(app, '/api/plan-node-detail', dict(basis=address(preview),
                logical_node_id=node['logical_node_id'], occurrence_id=node['occurrence_id'], kind='configuration'))
            assert status==200, parameters
            bound=json.loads(parameters['text'])
            assert bound['version']==1 and bound['columns'] in (['order'],['amount'])
            assert not {'occurrence_id','type','fields'} & set(bound)
            assert parameters['metadata']['display_title']=='读取参数'
            assert parameters['metadata']['engine_configuration']['occurrence_id']==node['occurrence_id']
        assert texts == ["data.read_lance(table, version=1, columns=['order'])",
                         "data.read_lance(table, version=1, columns=['amount'])"]
        status, full = request(app, '/api/plan-pipeline-source', dict(basis=address(preview)))
        assert status == 200 and full['text'] == CODE, full
        assert full['source_basis'] == 'compiled_snapshot' and not full['truncated']
        assert business_bytes(original) == before
        entry.write_text(CODE.replace('AFTER_FIRST_ACTION_NOT_EXECUTED', 'CHANGED'))
        assert request(app, '/api/plan-pipeline-source', dict(basis=address(preview))) == (200, full)
        bad = dict(address(preview), entry='unrelated.py:run')
        assert request(app, '/api/plan-pipeline-source', dict(basis=bad))[0] == 409
        assert request(app, '/api/plan-pipeline-source', dict(basis={}))[0] == 400
        status, newer = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=None))
        assert status == 200 and newer['basis']['digest'] != preview['basis']['digest']


def test_pipeline_ui_read_call_code_config_and_whole_source(original, tmp_path, browser):
    (original / 'dispatch.py').write_text(CODE)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'; registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('调用代码')
        expect(page.locator('.wb-source-body')).to_contain_text("columns=['order']")
        page.locator('.wb-source-tabs').get_by_role('button', name='配置', exact=True).click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('读取参数')
        expect(page.locator('.wb-parameter-explanation')).to_contain_text('不是另一份执行配置')
        page.locator('.wb-source-tabs').get_by_role('button', name='调用代码', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text("columns=['order']")
        page.get_by_role('button', name='Pipeline 源码', exact=True).click()
        expect(page.locator('.wb-pipeline-code')).to_be_visible()
        expect(page.locator('.wb-graph')).to_be_hidden()
        expect(page.locator('.wb-pipeline-text')).to_contain_text(CODE)
        assert page.locator('.wb-pipeline-text .reading-source-page').text_content()==CODE
        expect(page.locator('.wb-pipeline-basis')).to_contain_text('解析时快照')
        for width in (1440, 780, 390):
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.get_by_role('button', name='返回算子图', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4)
        expect(page.locator('.wb-source-body')).to_contain_text("columns=['order']")
        page.reload();expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        page.get_by_role('button', name='Pipeline 源码', exact=True).click()
        expect(page.locator('.wb-pipeline-text')).to_contain_text('AFTER_FIRST_ACTION_NOT_EXECUTED')
        page.get_by_role('button', name='返回算子图', exact=True).click()
        held=[]
        def delay(route):
            if not held:
                held.append((route, route.fetch()))
            else:
                route.continue_()
        page.route('**/plan-pipeline-source', delay)
        page.get_by_role('button', name='Pipeline 源码', exact=True).click()
        expect(page.locator('.wb-pipeline-code')).to_contain_text('正在读取')
        page.locator('.function-nav').get_by_role('button', name='Readme', exact=True).click()
        expect(page.locator('.workbench')).to_have_count(0)
        assert len(held)==1
        held[0][0].fulfill(response=held[0][1])
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        page.get_by_role('button', name='Pipeline 源码', exact=True).click()
        expect(page.locator('.wb-pipeline-text')).to_contain_text('AFTER_FIRST_ACTION_NOT_EXECUTED')
        assert not errors
        assert all(url.endswith(('/plan-preview','/plan-node-detail','/plan-pipeline-source')) for method,url in requests if method=='POST')
    assert business_bytes(original) == before


@pytest.mark.parametrize('owned_fixture', [{'execution_format':'v3'}], indirect=True)
def test_historical_pipeline_source_never_falls_back_to_edited_file(owned_fixture):
    fx=owned_fixture; run=fx.run_facts
    assert run['status']=='succeeded'
    status, graph=fx.api('/api/graph?'+urlencode(dict(basis_kind='run',run_id=run['run_id'],attempt=run['attempt'])))
    assert status==200, graph
    query='/api/pipeline-source?'+urlencode(dict(graph_binding=json.dumps(graph['binding'])))
    status, source=fx.api(query)
    assert status==200 and source['source_basis']=='sealed', source
    assert source['text']==(fx.root/source['path']).read_text()
    (fx.root/source['path']).write_text('# current file deliberately differs from the sealed run\n')
    assert fx.api(query)==(200,source)
