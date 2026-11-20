"""Actual compiler/HTTP provenance: no table-name matching or source fallback."""
import json
import hashlib
from pathlib import Path
from urllib.parse import urlencode
import pytest

from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws, registry
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes, register_fixture_flow
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
    register_fixture_flow(original)
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
            assert detail['path'] == 'dispatch.py'
            assert detail['start_line'] == detail['end_line'] == 9
            api = detail['metadata']['definition']
            assert api['symbol'] == 'DataAPI.read_lance'
            assert 'version: int | None=None' in api['signature']
            assert next(p for p in api['parameters'] if p['name']=='columns')['default'] is None
            assert api['returns'] == 'Dataset'
            texts.append(detail['text'])
            status, parameters=request(app, '/api/plan-node-detail', dict(basis=address(preview),
                logical_node_id=node['logical_node_id'], occurrence_id=node['occurrence_id'], kind='configuration'))
            assert status==200, parameters
            bound=json.loads(parameters['text'])
            assert bound['version']==1 and bound['columns'] in (['order'],['amount'])
            assert not {'occurrence_id','type','fields'} & set(bound)
            assert parameters['metadata']['display_title']=='API 入参'
            assert parameters['metadata']['engine_configuration']['occurrence_id']==node['occurrence_id']
            status, schema=request(app, '/api/plan-node-detail', dict(basis=address(preview),
                logical_node_id=node['logical_node_id'], occurrence_id=node['occurrence_id'], kind='schema'))
            assert status==200, schema
            assert schema['metadata']['output']['status']=='fields_only'
            assert [f['name'] for f in schema['metadata']['output']['fields']]==bound['columns']
            assert all(f['type'] is None for f in schema['metadata']['output']['fields'])

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
    register_fixture_flow(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'; registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('流程接线')
        expect(page.locator('.wb-source-body')).to_contain_text("columns=['order']")
        expect(page.get_by_text('输入输出与执行详情', exact=True)).to_have_count(0)
        expect(page.get_by_text('原文信息', exact=True)).to_have_count(0)
        expect(page.get_by_text('来源摘要', exact=True)).to_have_count(0)
        expect(page.locator('.wb-inspector')).to_be_hidden()
        expect(page.locator('.wb-source > .wb-source-tabs button:visible')).to_have_text(['Dataset API', '流程接线'])
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('流程接线')
        advanced = page.locator('.wb-input-advanced')
        expect(advanced).to_be_visible()
        expect(advanced).not_to_have_attribute('open', '')
        expect(page.locator('.wb-material-basis')).to_be_hidden()
        expect(page.locator('.wb-parameter-explanation')).to_be_hidden()
        expect(advanced.get_by_text('引擎内部描述（只读）', exact=True)).to_be_hidden()
        expect(advanced.get_by_text('来源摘要', exact=True)).to_have_count(0)
        table = page.locator('.wb-source-body > .wb-contract-table')
        expect(table).to_have_count(1)
        defaults = table.locator('tr').filter(has_text='API 默认值')
        assert defaults.count() > 0
        for row in defaults.all():
            expect(row).to_be_visible()
        advanced.locator(':scope > summary').click()
        expect(page.locator('.wb-material-basis')).to_be_visible()
        expect(advanced.get_by_text('引擎内部描述（只读）', exact=True)).to_be_visible()
        expect(advanced.get_by_text('来源摘要', exact=True)).to_have_count(0)
        expect(page.locator('.wb-parameter-explanation')).to_contain_text('外层配置字典')
        advanced.locator(':scope > summary').click()
        expect(page.locator('.wb-source-body')).to_contain_text('没有上游 Dataset 输入')
        expect(page.locator('.wb-source-body')).to_contain_text('输出行 Schema')
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-schema-section')).to_contain_text('order')
        expect(page.locator('.wb-schema-section')).to_contain_text('字段类型和可空性尚未取得')
        expect(page.locator('.wb-source-body')).to_contain_text('调用表达式 / 来源')

        page.locator('.wb-node').nth(2).click()  # union: two distinct Dataset inputs
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('上游数据流')
        expect(page.locator('.wb-schema-section')).to_have_count(3)
        expect(page.locator('.wb-material-basis')).to_be_hidden()
        for section, field in zip(page.locator('.wb-schema-section').all()[:2], ['order', 'amount']):
            expect(section.locator('table')).to_be_hidden()
            section.locator('summary').click()
            expect(section.locator('table')).to_be_visible()
            expect(section.locator('table')).to_contain_text(field)
        page.reload();expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True)).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('.wb-schema-section')).to_have_count(3)
        page.locator('.wb-node').first.click()

        page.locator('.wb-more > summary').click()
        page.get_by_role('button', name='整图配置', exact=True).click()
        expect(page.locator('.wb-inspector')).to_be_visible()
        expect(page.locator('.wb-inspector')).to_contain_text('整图配置')
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-inspector')).to_be_hidden()

        page.locator('.wb-source-tabs').get_by_role('button', name='Dataset API', exact=True).click()
        expect(page.locator('.definition-signature')).to_contain_text('DataAPI.read_lance')
        sdk_digest = hashlib.sha256((Path(__file__).parents[1] / 'demiflow/data/api.py').read_bytes()).hexdigest()
        expect(page.locator('.definition-meta')).to_contain_text(sdk_digest[:8])
        expect(page.locator('.wb-source-body')).to_contain_text('输入参数与默认值')
        expect(page.locator('.wb-source-body')).not_to_contain_text("columns=['order']")
        page.get_by_role('button', name='Pipeline 源码', exact=True).click()
        expect(page.locator('.wb-pipeline-code')).to_be_visible()
        expect(page.locator('.wb-graph')).to_be_hidden()
        expect(page.locator('.wb-pipeline-text')).to_contain_text(CODE)
        expect(page.get_by_text('原文信息', exact=True)).to_have_count(0)
        expect(page.get_by_text('来源摘要', exact=True)).to_have_count(0)
        assert page.locator('.wb-pipeline-text .reading-source-page').text_content()==CODE
        expect(page.locator('.wb-pipeline-basis')).to_contain_text('解析时快照')
        for width in (1440, 780, 390):
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.get_by_role('button', name='返回算子图', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4)
        expect(page.locator('.definition-signature')).to_contain_text('DataAPI.read_lance')
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
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
def test_historical_pipeline_source_never_falls_back_to_edited_file(owned_fixture, monkeypatch):
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
    from demiflow.workbench.graph import api_definition
    def forbidden(*args, **kwargs): raise AssertionError('Historical API must not use the current SDK')
    monkeypatch.setattr(api_definition, 'current', forbidden)
    node=graph['nodes'][0]
    status, contract=fx.api('/api/graph-source?'+urlencode(dict(graph_binding=json.dumps(graph['binding']),node_id=node['id'],kind='callsite')))
    assert status==200,contract
    assert contract['source_basis']=='sealed'
    assert contract['definition']['symbol']=='DataAPI.read_lance'
    assert 'version' in contract['definition']['signature']
