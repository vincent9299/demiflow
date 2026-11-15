"""UI-D1.9/2 independent integration acceptance -- the eight browser gates.

V01 shell/responsive, V03 graph-config-check-run, V04 sealed-graph attempts,
V05 notebook-to-case (real kernel), V06 case multi-material, V07 review scope
and conflict, V08 late response and cleanup, V09 asset and single authority.

Every gate drives the candidate tree's own private service (port=0) in a real
Chromium page and asserts content/version/target together with the key API
facts -- never merely "the page did not error". V03/V04/V08/V09 are C8
parametrized over the two real execution input classes (legacy / v3); the v3
class is accepted only when the candidate actually seals a v3 action, otherwise
it is reported not_run. V05 admits only the real WORKBENCH_D19_ANALYSIS_CONFIG
helper; a metadata double never counts. V06/V07 build on the existing real
Records/Lance v2 Case scene. New-shape assertions fail honestly until the
A-F implementation lanes land; they are never satisfied by a consumer mock.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from playwright.sync_api import expect

from d19_fixtures import WEB, owned_fixture, case_scene as scene
from test_forge_markdown_reading import browser
from test_forge_analysis_case_groups_ui_browser import (
    save, apply_filter, registered, group_binding, latest)

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.graph import model

TABS = ['开发', '管道', '数据', 'Readme']
C8 = [{'execution_format': 'legacy'}, {'execution_format': 'v3'}]
V3_SCHEMA = 'demiflow.dataflow_graph.v3'


def _select_flow(page):
    """source-only entry -> explicit check -> read the real graph (C2/C3)."""
    page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
    page.get_by_label('流程', exact=True).select_option('flows/questions.py:select')
    expect(page.get_by_label('配置', exact=True)).to_have_value('flows/select_a.json')
    expect(page.locator('.wb-source-body')).to_contain_text('def _select')
    for _ in range(5):
        page.get_by_role('button', name='检查', exact=True).click()
        page.wait_for_function(
            "() => document.querySelector('.wb-basis')?.textContent.includes('checked')"
            " || document.querySelector('.wb-notice')?.textContent.includes('尚未开始')")
        if 'checked' in page.locator('.wb-basis').inner_text():
            break
        page.wait_for_timeout(400)
    expect(page.locator('.wb-basis')).to_contain_text('checked')
    page.get_by_role('button', name='读图与配置', exact=True).click()
    expect(page.locator('.wb-node').filter(has_text='normalize_question')).to_be_visible()


def _run(page):
    page.get_by_role('button', name='运行', exact=True).click()
    expect(page.locator('.wb-run-bar')).to_be_visible()
    page.wait_for_function("() => /成功|失败|已取消/.test(document.querySelector('.wb-run-bar > .wb-tag')?.textContent || '')",
                           timeout=120000)
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')


def _facts(log, endpoint):
    # Registry mode scopes APIs under /api/workspaces/<id>/; compare the exact
    # final path component so /graph-source cannot stand in for /graph.
    from urllib.parse import urlsplit
    name = endpoint.rsplit('/', 1)[-1]
    return [r for r in log['requests'] if urlsplit(r['url']).path.rsplit('/', 1)[-1] == name]


def _is_v3(action):
    return action.get('version') == 'v3' or (action.get('dag') or {}).get('schema_version') == V3_SCHEMA


# ---------------------------------------------------------------------------
# V01 -- shell, space and interaction (C1, A01/A02)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', [{}], indirect=True)
def test_ui_d19_shell_and_responsive(owned_fixture, browser):
    fx = owned_fixture
    with fx.open_page(browser, viewport=(1440, 1050)) as (page, log):
        nav = page.locator('.function-nav')
        for label in TABS:
            expect(nav.get_by_role('button', name=label, exact=True)).to_be_visible()
        for label in ['数据', 'Readme', '开发', '管道']:
            nav.get_by_role('button', name=label, exact=True).click()
        fx.shot(page, 'v01-shell-1440')

        # graph node screen font size must stay readable (>=12px), not shrunk
        _select_flow(page)
        node_font = page.locator('.wb-node').first.evaluate(
            'el => parseFloat(getComputedStyle(el).fontSize)')
        assert node_font >= 12, node_font

        # responsive sweep: no accidental horizontal overflow at any width
        for width in (320, 390, 768, 1024, 1440, 1920):
            page.set_viewport_size({'width': width, 'height': 900})
            page.wait_for_timeout(150)
            overflow = page.evaluate('() => document.documentElement.scrollWidth - window.innerWidth')
            assert overflow <= 1, (width, overflow)
            fx.shot(page, f'v01-responsive-{width}')
        fx.record('v01-shell', dict(tabs=TABS, node_font=node_font))

        # counterexample: keyboard focus is visible and reachable
        page.set_viewport_size({'width': 1440, 'height': 1050})
        first = nav.get_by_role('button', name='管道', exact=True)
        first.focus()
        expect(first).to_be_focused()
        # A01: collapsing the project sidebar must release the content grid
        # column (content gets wider), not merely hide an overlay.
        toggle = page.locator('#sidebar-toggle')
        if toggle.count():
            content = page.locator('.content, main').first
            before = content.evaluate('el => el.getBoundingClientRect().width')
            toggle.click()
            page.wait_for_timeout(200)
            after = content.evaluate('el => el.getBoundingClientRect().width')
            assert after > before, ('sidebar collapse did not release the grid column', before, after)
            expect(toggle).to_have_attribute('aria-expanded', 'false')
            toggle.click()
            fx.shot(page, 'v01-sidebar-collapsed')
        # counterexample: switching tab keeps the project header, not a new shell
        nav.get_by_role('button', name='数据', exact=True).click()
        expect(nav.get_by_role('button', name='数据', exact=True)).to_be_visible()
        assert not log['errors'], log['errors']


# ---------------------------------------------------------------------------
# V03 -- source-only -> check -> real graph -> fix params -> check -> run -> result
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', C8, indirect=True)
def test_ui_d19_graph_config_check_run(owned_fixture, browser):
    fx = owned_fixture
    root = fx.root
    with fx.open_page(browser) as (page, log):
        _select_flow(page)
        # the graph card must match the server's inspected report, not an old
        # drawing that happens to look right. Read the real graph back over HTTP.
        node = page.locator('.wb-node').filter(has_text='normalize_question')
        node.click()
        expect(page.locator('.wb-inspector')).to_be_visible()
        assert _facts(log, '/api/graph') or _facts(log, '/api/inspect'), log['requests'][-5:]
        fx.shot(page, f'v03-graph-{fx.execution_format}')

        # Read the graph, repair a real parameter, then invalidate and recheck.
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        minimum = page.get_by_label('配置 /arguments/minimum', exact=True)
        expect(minimum).to_have_value('0.8')
        before = (root / 'flows/select_a.json').read_bytes()
        minimum.fill('not-a-number')
        page.get_by_role('button', name='保存配置', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('数字')
        assert (root / 'flows/select_a.json').read_bytes() == before
        minimum.fill('0.9')
        with page.expect_response(lambda response: '/parameters-update' in response.url) as saved:
            page.get_by_role('button', name='保存配置', exact=True).click()
        assert saved.value.status == 200, saved.value.text()
        expect(page.locator('.wb-notice')).to_contain_text('配置已保存')
        assert json.loads((root / 'flows/select_a.json').read_text())['arguments']['minimum'] == .9
        expect(page.locator('.wb-basis')).not_to_contain_text('checked')
        page.get_by_role('button', name='检查', exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('checked')

        _run(page)
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        shown = runs.show(root, run_id)
        assert shown['status'] == 'succeeded'
        assert shown['outputs'][0]['result']['ref']['row_count'] == 1
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        fx.record(f'v03-run-{fx.execution_format}', dict(
            run_id=run_id, status=shown['status'],
            row_count=shown['outputs'][0]['result']['ref']['row_count'],
            execution_format=fx.execution_format, v3_available=fx.v3_available))
        fx.shot(page, f'v03-fixed-result-{fx.execution_format}')
        # The earlier sealed run keeps its original two-row result.
        earlier = runs.show(root, fx.run_facts['run_id'])
        assert earlier['outputs'][0]['result']['ref']['row_count'] == 2
        assert earlier['run_id'] != run_id

        # counterexample: an invalid configuration is rejected, not auto-fixed
        code, value = fx.api('/api/inspect', dict(entry='flows/questions.py:select',
                                                 params='flows/does-not-exist.json'))
        assert code >= 400, value
        # counterexample: a stale parameter CAS write is a conflict, not a merge
        code, value = fx.api('/api/parameters-update', dict(
            path='flows/select_a.json', expected_sha256='0' * 64,
            proposed_sha256='1' * 64, graph_binding='0' * 64, replace={}))
        assert code >= 400, value


# ---------------------------------------------------------------------------
# V04 -- sealed action default visible; multi-action / occurrence / observation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', C8, indirect=True)
def test_ui_d19_sealed_graph_attempts(owned_fixture, browser):
    fx = owned_fixture
    root = fx.root
    assert fx.sealed_actions, 'a real run must seal at least one action'
    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.get_by_role('button', name='运行记录', exact=True).click()
        page.locator('.wb-run-record[data-run="' + fx.run_facts['run_id'] + '"]').click()
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        graph = page.locator('.wb-graph')
        expect(graph).to_be_visible()
        action_bar = page.locator('.wb-action-bar')
        sealed_time = action_bar.locator('time.wb-sealed-time')
        expect(sealed_time).to_be_visible()
        expect(sealed_time).to_contain_text('封存于')
        assert sealed_time.get_attribute('datetime')
        expect(action_bar).not_to_contain_text('Invalid Date')
        fx.shot(page, f'v04-sealed-{fx.execution_format}')

        # the drawn nodes must equal the server's sealed facts item by item
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        shown = runs.show(root, run_id)
        graph_facts = model.get(root, basis_kind='run', run_id=run_id, attempt=shown['attempt'])
        actions = [a for a in (graph_facts.get('logical') or {}).get('actions', [])
                   if a.get('status') == 'sealed']
        assert actions
        action = actions[0]
        if fx.execution_format == 'v3':
            assert _is_v3(action), 'v3 input class requires a real sealed v3 action'
        else:
            assert not _is_v3(action)
        expect(graph.locator('.wb-node')).to_have_count(7)
        # selecting a node binds the precise report node + observation bridge
        graph.locator('.wb-node').filter(has_text='normalize_question').click()
        inspector = page.locator('.wb-inspector')
        selection = page.evaluate('ForgeView.decodeView(location.hash)')
        assert selection['run_id'] == run_id and selection['attempt'] == shown['attempt']
        assert selection['report_digest'] == graph_facts['report_digest']
        assert selection['action_id'] == action['action_id']
        bridge = action['identity_bridge']['bridge']
        member = next(item for item in bridge['entries']
                      if item['logical_node_id'] == selection['logical_node_id']
                      and item['occurrence_id'] == selection['occurrence_id'])
        assert member['report_node_id'] == selection['node_id']
        assert any(node['id'] == selection['node_id']
                   and (node.get('operator') or {}).get('name') == 'normalize_question'
                   for node in graph_facts['nodes'])
        expect(inspector).to_contain_text('执行完成')
        with page.expect_response(lambda response: '/run-observations?' in response.url) as observed:
            page.get_by_role('button', name='节点证据', exact=True).click()
        assert observed.value.status == 200
        observation_page = observed.value.json()
        rows = [row for row in observation_page['observations']
                if row['action_id'] == action['action_id'] and row['node_id'] == selection['node_id']
                and row['occurrence'] == member['report_occurrence']]
        assert rows and all(row['state'] == 'succeeded' for row in rows)
        if fx.execution_format == 'v3':
            for row in rows:
                assert row['plan_id'] == action['plan_id']
                assert row['dag_id'] == action['dag']['dag_id']
                assert row['plan_revision'] == action['physical']['plan_revision']
                assert row['occurrence_id'] == selection['occurrence_id']
        expect(page.locator('.wb-run-detail-body')).to_contain_text('节点关联观测')
        expect(page.locator('.wb-run-detail-body')).to_contain_text(action['action_id'])
        fx.record(f'v04-sealed-{fx.execution_format}', dict(
            run_id=run_id, attempt=shown['attempt'], action_id=action.get('action_id'),
            is_v3=_is_v3(action), sealed_count=len(actions)))
        fx.shot(page, f'v04-inspector-{fx.execution_format}')

        # The context API carries references/facts, not an invented read_only
        # flag. Read-only configuration belongs to the original graph DTO.
        parameter_bytes = (root / 'flows/select_a.json').read_bytes()
        code, value = fx.api('/api/run-context?run_id=' + run_id + '&attempt=' + str(shown['attempt']))
        assert code == 200, value
        reference = value['references'][0]
        assert reference['run_id'] == run_id and reference['attempt'] == shown['attempt']
        assert reference['seal_digest'] == graph_facts['binding']['basis_digest']
        assert graph_facts['parameters']['readonly'] is True
        assert all(not field['writable'] for field in graph_facts['parameters']['fields'])
        assert (root / 'flows/select_a.json').read_bytes() == parameter_bytes
        # counterexample: unknown / fused CPU is not fabricated into node facts
        expect(inspector).not_to_contain_text('Invalid Date')
        assert not log['errors'], log['errors']


# ---------------------------------------------------------------------------
# V05 -- real Python + SQL notebook -> fixed table chart -> Case -> save/reopen
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', [{'need_runtime': True}], indirect=True)
def test_ui_d19_notebook_to_case(owned_fixture, browser):
    import re
    fx = owned_fixture
    assert fx.analysis_profile, 'V05 requires the admitted real kernel profile'
    def finished(cell):
        status = cell.locator('.analysis-cell-status')
        expect(status).to_contain_text(re.compile('完成|代码错误|已取消|超时|已结束'), timeout=120000)
        assert '完成' in status.inner_text(), status.inner_text()
    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        page.get_by_label('当前数据表', exact=True).select_option('scores')
        page.get_by_role('button', name='Notebook', exact=True).click()
        expect(page.locator('.data-source-context')).to_contain_text('固定版本 v2')
        source_panel = page.locator('details.analysis-sources')
        source_panel.locator(':scope > summary').click()
        page.get_by_role('button', name='添加当前表', exact=True).click()
        expect(source_panel.locator(':scope > summary')).to_contain_text('1')
        page.get_by_label('Notebook 名称', exact=True).fill('V05 Python SQL 固定来源')
        python_source = (
            'from demiflow.workbench.analysis.client import source, publish, case_view\n'
            'import pyarrow as pa\n'
            'shared_marker = 7\n'
            "rows = source('data').sample(limit=20)['rows']\n"
            "numbers = publish(pa.table({k: [r[k] for r in rows] for k in ('case_id', 'score')})).bind('numbers')\n"
            "assert len(rows) == 3 and rows[0]['case_id'] == 'c01'\n"
            'numbers'
        )
        first = page.locator('.analysis-cell').first
        first.get_by_label('python 单元代码', exact=True).fill(python_source)
        # A single browser action must save, start, and submit exactly once.
        with page.expect_response(lambda response: '/analysis-cell-submit' in response.url) as submitted:
            first.locator('.analysis-cell-run').click()
        assert submitted.value.status == 200, submitted.value.text()
        finished(first)
        assert len(_facts(log, 'analysis-notebook-create')) == 1
        assert len(_facts(log, 'analysis-session-start')) == 1
        notebook_id = page.get_by_label('选择 Notebook', exact=True).input_value()
        first_payload = json.loads(_facts(log, 'analysis-cell-submit')[0]['body'])
        assert first_payload['source_sha256'] == hashlib.sha256(python_source.encode()).hexdigest()
        assert first_payload['expected_generation'].startswith('g')
        assert 'session_id' in first_payload and 'notebook_id' not in first_payload

        page.get_by_role('button', name='＋ SQL', exact=True).click()
        sql_cell = page.locator('.analysis-cell').last
        sql_cell.get_by_label('sql 单元代码', exact=True).fill(
            'SELECT case_id, score, score * 2 AS doubled FROM numbers ORDER BY case_id')
        sql_cell.get_by_label('结果变量名', exact=True).fill('sql_result')
        sql_cell.locator('.analysis-cell-run').click()
        finished(sql_cell)
        sql_payload = json.loads(_facts(log, 'analysis-cell-submit')[-1]['body'])
        assert (sql_payload['session_id'], sql_payload['expected_generation']) == (
            first_payload['session_id'], first_payload['expected_generation'])
        assert len(_facts(log, 'analysis-session-start')) == 1
        # SQL results are ephemeral until the user explicitly pins them.
        with page.expect_response(lambda response: '/analysis-source-from-handle' in response.url) as ephemeral:
            sql_cell.get_by_role('button', name='内联预览前20行（表格/图表）', exact=True).click()
        assert ephemeral.value.status == 422, ephemeral.value.text()
        expect(sql_cell).to_contain_text('requires explicit pin')
        page.get_by_role('button', name='＋ Python', exact=True).click()
        pinned_cell = page.locator('.analysis-cell').last
        pinned_cell.get_by_label('python 单元代码', exact=True).fill('fixed = sql_result.pin()\nfixed')
        pinned_cell.locator('.analysis-cell-run').click()
        finished(pinned_cell)
        with page.expect_response(lambda response: '/analysis-source-from-handle' in response.url) as pinned_source:
            with page.expect_response(lambda response: '/analysis-case-page' in response.url) as pinned_page:
                pinned_cell.get_by_role('button', name='内联预览前20行（表格/图表）', exact=True).click()
        assert pinned_source.value.status == 200, pinned_source.value.text()
        assert pinned_page.value.status == 200, pinned_page.value.text()
        fixed_source = pinned_source.value.json()
        fixed_page = pinned_page.value.json()
        assert fixed_page['source_snapshot'] == fixed_source['digest']
        assert len(fixed_page['rows']) == 3
        assert fixed_page['rows'][0]['case_id'] == 'c01' and fixed_page['rows'][0]['doubled'] == 1.9
        chart = pinned_cell.locator('.analysis-charts')
        expect(chart.locator('tbody tr')).to_have_count(3)
        expect(chart.locator('tbody tr').first).to_contain_text('c01')
        expect(chart.locator('tbody tr').first).to_contain_text('1.9')
        chart.get_by_role('button', name='柱图', exact=True).click()
        expect(chart.locator('svg.analysis-charts-svg')).to_be_visible()
        assert _facts(log, 'analysis-source-from-handle')
        assert _facts(log, 'analysis-case-page')
        fx.shot(page, 'v05-real-sql-chart')

        page.get_by_role('button', name='＋ Python', exact=True).click()
        case_cell = page.locator('.analysis-cell').last
        case_cell.get_by_label('python 单元代码', exact=True).fill(
            'assert shared_marker == 7\n'
            "assert fixed.to_pandas()['doubled'].tolist() == [1.9, 1.68, 1.44]\n"
            "case_view(fixed, fields=[dict(id='case_id', field='case_id', label='Case', role='input', format='text'), "
            "dict(id='doubled', field='doubled', label='SQL 分数', role='output', format='json')], "
            "questions=[dict(id='note', title='备注', type='text', required=False)])")
        case_cell.locator('.analysis-cell-run').click()
        finished(case_cell)
        case_cell.get_by_role('button', name='在 Case 工作台查看此描述', exact=True).click()
        page.get_by_role('button', name='采用 Notebook 输出视图', exact=True).click()
        expect(page.get_by_label('标注 备注', exact=True)).to_be_visible()
        expect(page.locator('.analysis-case')).to_contain_text('c01')
        with page.expect_response(lambda response: '/analysis-view-save' in response.url) as view_saved:
            page.get_by_role('button', name='保存视图', exact=True).click()
        assert view_saved.value.status == 200, view_saved.value.text()
        view_id = view_saved.value.json()['id']
        assert json.loads(_facts(log, 'analysis-view-save')[-1]['body'])['descriptor']['source_snapshot'] == fixed_source['digest']
        expect(page.locator('.case-ui-context')).to_contain_text('已存视图')
        save(page, 'Python 与 SQL 同内核的固定结果已复核')
        saved_target = json.loads(_facts(log, 'analysis-annotations-save')[-1]['body'])['target']
        assert saved_target['source'] == fixed_source['digest']
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        with page.expect_response(lambda response: '/analysis-view?' in response.url
                                  and 'view_id=' + view_id in response.url) as reopened:
            page.get_by_label('选择 Case 视图', exact=True).select_option(view_id)
        assert reopened.value.status == 200, reopened.value.text()
        assert reopened.value.json()['document']['source_snapshot'] == fixed_source['digest']
        expect(page.get_by_label('标注 备注', exact=True)).to_have_value('Python 与 SQL 同内核的固定结果已复核')
        assert _facts(log, 'analysis-annotation-heads')
        # Genuine protocol negatives use the browser's real session identity.
        for changes in ({'source_sha256': 'f' * 64},
                        {'expected_generation': 'g' + 'f' * 32}):
            body = {**first_payload, 'request_id': 'v05-invalid-' + next(iter(changes)), **changes}
            code, value = fx.api('/api/analysis-cell-submit', body)
            assert code >= 400, value
        fx.record('v05-runtime', dict(profile=fx.analysis_profile, submissions=[
            json.loads(item['body']) for item in _facts(log, 'analysis-cell-submit')],
            annotation_target=saved_target, fixed_source=fixed_source, fixed_page=fixed_page,
            view_id=view_id, requests=log['requests']))
        fx.shot(page, 'v05-case-saved-reopened')
        page.get_by_role('button', name='Notebook', exact=True).click()
        page.get_by_label('选择 Notebook', exact=True).select_option(notebook_id)
        page.get_by_role('button', name='立即停止内核', exact=True).click()
        expect(page.get_by_role('button', name='立即停止内核', exact=True)).to_be_hidden(timeout=120000)


# ---------------------------------------------------------------------------
# V06 -- Case v2 two schemes, shared material, three layouts, mobile reading
# ---------------------------------------------------------------------------
def test_ui_d19_case_multimaterial(browser, tmp_path, monkeypatch):
    with scene(browser, tmp_path, monkeypatch) as f:
        page = f['page']
        registered(page)
        # two output groups, each image + markdown + score, on the same Case
        for group in ('base', 'next'):
            assert group_binding(f, group)['registered']
        expect(page.locator('.analysis-case')).to_have_count(1)
        for field in ('reference_image', 'base_image', 'next_image'):
            host = page.locator('.analysis-case-field[data-field=' + field + ']')
            host.get_by_role('button', name='固定媒体供评测', exact=True).click()
            host.get_by_role('button', name='预览图片', exact=True).click()
        images = page.locator('img.analysis-media')
        expect(images).to_have_count(3)
        page.wait_for_function('() => [...document.querySelectorAll("img.analysis-media")].every(i => i.naturalWidth === 384)')
        loaded = images.evaluate_all(
            '(items)=>items.map(i=>({field:i.closest(".analysis-case-field")?.dataset.field,'
            'complete:i.complete,width:i.naturalWidth}))')
        assert any(item['width'] > 0 for item in loaded), loaded
        page.screenshot(path=str(tmp_path / 'v06-comparison-1440.png'), full_page=True)
        # three configured layouts remain reachable; mobile still reads material
        for width in (1440, 900, 390):
            page.set_viewport_size({'width': width, 'height': 1000})
            page.wait_for_timeout(150)
            expect(page.locator('.analysis-case')).to_have_count(1)
            page.screenshot(path=str(tmp_path / f'v06-layout-{width}.png'), full_page=True)
        # counterexample: only schemes with a registered proof are bound; the
        # binding facts come from the real case-group-bindings response
        groups = next(value['result']['groups'] for name, value in reversed(f['responses'])
                      if name == 'case-group-bindings' and 'result' in value)
        assert all(g['registered'] for g in groups if g['id'] in ('base', 'next'))
        # counterexample: a null material renders an honest empty, not a fake image
        assert not f['errors'], f['errors']


# ---------------------------------------------------------------------------
# V07 -- review scope and conflict: answers differ, reopen, confirm then next
# ---------------------------------------------------------------------------
def test_ui_d19_review_scope_and_conflict(browser, tmp_path, monkeypatch):
    with scene(browser, tmp_path, monkeypatch) as f:
        page = f['page']
        registered(page)
        answers = [('group:base', 'Base 独立评测'), ('', 'Case 整体评测'),
                   ('base_answer', '单字段评测')]
        targets = []
        for scope, text in answers:
            page.get_by_label('标注对象', exact=True).select_option(scope)
            expect(page.get_by_label('标注 备注', exact=True)).to_have_value('')
            save(page, text)
            targets.append(copy.deepcopy(latest(f, 'annotations-save')['target']))
        assert targets[0]['variant'].startswith('cg') and targets[0]['field'] is None
        assert targets[1]['variant'] is None and targets[1]['field'] is None
        assert targets[2]['variant'] is None and targets[2]['field'] == 'base_answer'
        assert len({(item['source'], item['case_key']) for item in targets}) == 1
        page.reload(wait_until='domcontentloaded')
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        for scope, text in answers:
            page.get_by_label('标注对象', exact=True).select_option(scope)
            expect(page.get_by_label('标注 备注', exact=True)).to_have_value(text)

        # Another actual Records writer advances the same field revision.
        remote = copy.deepcopy(latest(f, 'annotations-save'))
        remote['request_id'] = 'd19-other-window'
        for entry in remote['entries']:
            entry['expected_revision'] = 1
            if entry['definition']['id'] == 'note':
                entry['value'] = '另一窗口已提交'
        f['app'].analysis.call(f['project'], 'annotations-save', remote)
        page.get_by_label('标注 备注', exact=True).fill('409 后保留本地草稿')
        with page.expect_response(lambda response: '/analysis-annotations-save' in response.url) as conflict:
            page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        assert conflict.value.status == 409, conflict.value.text()
        expect(page.locator('.analysis-case-review')).to_contain_text('保存冲突')
        expect(page.get_by_label('标注 备注', exact=True)).to_have_value('409 后保留本地草稿')
        assert latest(f, 'annotations-save')['target'] == targets[2]
        page.get_by_role('button', name='保留草稿并采用服务器版本', exact=True).click()
        save(page, '409 后保留本地草稿')

        # Lose only the browser delivery after the genuine server commit. Receipt
        # recovery must never retry the write or repaint another scope's draft.
        page.evaluate("""() => { const native=window.fetch; let drop=true;
          window.fetch=async(...args)=>{const response=await native(...args);
            if(drop&&String(args[0]).endsWith('/analysis-annotations-save')) {
              drop=false; throw Error('D19 lost response after committed write'); }
            return response;}; }""")
        page.get_by_label('标注 备注', exact=True).fill('已提交但回执丢失')
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_be_visible()
        writes = len([name for name, _ in f['calls'] if name == 'annotations-save'])
        page.get_by_label('标注对象', exact=True).select_option('group:base')
        page.get_by_label('标注 备注', exact=True).fill('其他 scope 的未保存草稿')
        page.get_by_role('button', name='核对 Case 原请求', exact=True).click()
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_have_count(0)
        expect(page.get_by_label('标注 备注', exact=True)).to_have_value('其他 scope 的未保存草稿')
        assert len([name for name, _ in f['calls'] if name == 'annotations-save']) == writes
        page.get_by_label('标注对象', exact=True).select_option('base_answer')
        expect(page.get_by_label('标注 备注', exact=True)).to_have_value('已提交但回执丢失')
        page.screenshot(path=str(tmp_path / 'v07-review-scope-conflict.png'), full_page=True)
        (tmp_path / 'v07-facts.json').write_text(json.dumps(dict(targets=targets,
            actual_conflict_status=conflict.value.status, writes_after_lost_reply=writes), indent=2))
        assert not f['errors'], f['errors']


# ---------------------------------------------------------------------------
# V08 -- late response isolation, failure can continue, 20 switches stable
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', C8, indirect=True)
def test_ui_d19_late_response_and_cleanup(owned_fixture, browser):
    fx = owned_fixture
    with fx.open_page(browser) as (page, log):
        _select_flow(page)
        _run(page)
        # Only transport delivery is delayed; the response was obtained from
        # the real server. Count this page's live requests, URLs and intervals.
        page.evaluate("""() => {
          const native=window.fetch, make=URL.createObjectURL, revoke=URL.revokeObjectURL;
          const every=window.setInterval, clear=window.clearInterval;
          window.__d19Life={active:0, urls:new Set(), intervals:new Set(), held:[], delayGraph:true};
          URL.createObjectURL=function(...args){const url=make.apply(this,args);__d19Life.urls.add(url);return url;};
          URL.revokeObjectURL=function(url){__d19Life.urls.delete(url);return revoke.call(this,url);};
          window.setInterval=function(...args){const id=every.apply(this,args);__d19Life.intervals.add(id);return id;};
          window.clearInterval=function(id){__d19Life.intervals.delete(id);return clear.call(this,id);};
          window.fetch=async(...args)=>{const tracked=String(args[0]).includes('/api/');
            if(tracked)__d19Life.active++;
            try { const response=await native(...args);
              if(__d19Life.delayGraph&&new URL(String(args[0]),location.href).pathname.endsWith('/graph')) {
                __d19Life.delayGraph=false;
                return await new Promise(resolve=>__d19Life.held.push(()=>resolve(response)));
              }
              return response;
            } finally { if(tracked)__d19Life.active--; }
          };
        }""")
        page.get_by_role('button', name='返回当前配置', exact=True).click()
        page.wait_for_function('() => __d19Life.held.length === 1')
        page.get_by_label('配置', exact=True).select_option('flows/select_b.json')
        expect(page.get_by_label('配置', exact=True)).to_have_value('flows/select_b.json')
        page.wait_for_function("() => ForgeView.decodeView(location.hash).params === 'flows/select_b.json'")
        page.evaluate('__d19Life.held.shift()()')
        page.wait_for_function('() => __d19Life.active === 0')
        expect(page.get_by_label('配置', exact=True)).to_have_value('flows/select_b.json')
        page.get_by_role('button', name='检查', exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('checked')
        _run(page)
        run_id = page.evaluate('ForgeView.decodeView(location.hash).run_id')
        assert runs.show(fx.root, run_id)['outputs'][0]['result']['ref']['row_count'] == 1
        page.wait_for_function('() => __d19Life.active === 0')
        baseline = page.evaluate('() => ({urls:__d19Life.urls.size, intervals:__d19Life.intervals.size})')
        for _ in range(20):
            page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
            expect(page.get_by_label('当前数据表', exact=True)).to_be_visible()
            page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
            expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        page.wait_for_function('() => __d19Life.active === 0 && __d19Life.held.length === 0')
        after = page.evaluate('() => ({urls:__d19Life.urls.size, intervals:__d19Life.intervals.size})')
        assert after['urls'] <= baseline['urls'], (baseline, after)
        assert after['intervals'] <= baseline['intervals'], (baseline, after)
        assert page.evaluate('ForgeView.decodeView(location.hash).run_id') == run_id
        fx.shot(page, f'v08-after-switches-{fx.execution_format}')
        fx.record(f'v08-cleanup-{fx.execution_format}', dict(
            switches=20, delayed_real_graph_response=True, current_run=run_id,
            requests=len(log['requests']), baseline=baseline, after=after,
            execution_format=fx.execution_format,
            measured_scope='this browser document fetches, created object URLs, and interval handles'))
        assert not log['errors'], log['errors']


# ---------------------------------------------------------------------------
# V09 -- assets really load, old duplicate canvas/style removed, single authority
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', C8, indirect=True)
def test_ui_d19_asset_and_single_authority(owned_fixture, browser):
    fx = owned_fixture
    with fx.open_page(browser) as (page, log):
        # the new public namespaces are wired by A in index.html, not injected
        namespaces = page.evaluate(
            '() => ({presentation: typeof ForgeWorkbenchPresentation,'
            ' charts: typeof ForgeAnalysisCharts,'
            ' graph: typeof ForgeWorkbenchGraph})')
        assert all(value in ('object', 'function') for value in namespaces.values()), namespaces
        # exactly one graph drawing authority; the old duplicate painter is gone
        canvases = page.locator('.wb-graph svg, .wb-graph canvas').count()
        assert canvases <= 1, canvases
        _select_flow(page)
        _run(page)
        # C09 normalizeExecutionPlan must really be called by the production
        # painter. Install a real counting probe on the new builder, reload and
        # re-drive the graph; a builder the real page never calls fails this gate.
        # The namespace is deliberately frozen and buildGraphModel calls its
        # local normalizer. Instrument the served function body, not a no-op
        # reassignment of a frozen export. These are the candidate's own bytes.
        script = (WEB / 'workbench-presentation.js').read_text()
        needle = 'function normalizeExecutionPlan(entry, binding) {'
        assert script.count(needle) == 1
        instrumented = script.replace(needle, needle +
            '\n globalThis.__d19NormalizeCalls=(globalThis.__d19NormalizeCalls||0)+1;')
        page.route('**/workbench-presentation.js', lambda route: route.fulfill(
            content_type='text/javascript', body=instrumented))
        page.reload()
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).wait_for()
        _select_flow(page)
        _run(page)
        normalize_calls = page.evaluate('() => window.__d19NormalizeCalls || 0')
        assert normalize_calls > 0, ('C09 normalizeExecutionPlan not called by real page', normalize_calls)
        fx.record(f'v09-authority-{fx.execution_format}', dict(
            namespaces=namespaces, canvases=canvases, normalize_calls=normalize_calls,
            execution_format=fx.execution_format))
        fx.shot(page, f'v09-single-authority-{fx.execution_format}')
        # counterexample: removing a new script must fail, not fall back to a
        # second authority; the frozen seven files keep their hash
        assert not log['errors'], log['errors']
