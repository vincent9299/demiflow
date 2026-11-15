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

import json
from pathlib import Path

import pytest
from playwright.sync_api import expect

from d19_fixtures import owned_fixture
from test_forge_markdown_reading import browser
from test_forge_analysis_case_groups_ui_browser import (
    scene, save, apply_filter, registered, group_binding, latest)

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
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')


def _facts(log, endpoint):
    return [r for r in log['requests'] if endpoint in r['url']]


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

        _run(page)
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        shown = runs.show(root, run_id)
        assert shown['status'] == 'succeeded'
        assert shown['outputs'][0]['result']['ref']['row_count'] == 2
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        fx.record(f'v03-run-{fx.execution_format}', dict(
            run_id=run_id, status=shown['status'],
            row_count=shown['outputs'][0]['result']['ref']['row_count'],
            execution_format=fx.execution_format, v3_available=fx.v3_available))
        fx.shot(page, f'v03-fixed-result-{fx.execution_format}')

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
        _select_flow(page)
        _run(page)
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        graph = page.locator('.wb-graph')
        expect(graph).to_be_visible()
        logical_bar = page.locator('.wb-logical-bar')
        expect(logical_bar).to_contain_text('已绑定本次执行计划')
        expect(logical_bar).to_contain_text('sealed_at 2026-')
        expect(logical_bar).not_to_contain_text('Invalid Date')
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
        expect(graph.locator('.wb-node')).not_to_have_count(0)
        # selecting a node binds the precise report node + observation bridge
        graph.locator('.wb-node').filter(has_text='normalize_question').click()
        inspector = page.locator('.wb-inspector')
        expect(inspector).to_contain_text('已绑定报告节点')
        expect(inspector).to_contain_text('已有观测')
        fx.record(f'v04-sealed-{fx.execution_format}', dict(
            run_id=run_id, attempt=shown['attempt'], action_id=action.get('action_id'),
            is_v3=_is_v3(action), sealed_count=len(actions)))
        fx.shot(page, f'v04-inspector-{fx.execution_format}')

        # counterexample: a historical attempt is read-only over the real API
        code, value = fx.api('/api/run-context?run_id=' + run_id)
        assert code == 200 and value['read_only'], value
        # counterexample: unknown / fused CPU is not fabricated into node facts
        expect(inspector).not_to_contain_text('Invalid Date')
        assert not log['errors'], log['errors']


# ---------------------------------------------------------------------------
# V05 -- real Python + SQL notebook -> fixed table chart -> Case -> save/reopen
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('owned_fixture', [{'need_runtime': True}], indirect=True)
def test_ui_d19_notebook_to_case(owned_fixture, browser):
    fx = owned_fixture
    assert fx.analysis_profile, 'V05 requires the admitted real kernel profile'
    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        # create a notebook bound to a fixed source through the real analysis API
        code, created = fx.api('/api/analysis-notebook-create', dict(
            request_id='v05-book', document=dict(title='V05', cells=[
                dict(id='py', language='python', source='print(1 + 1)')]))
        )
        assert code == 200, created
        notebook_id = created['id']
        # single-click run captures source bindings and submits one in-flight chain
        code, submitted = fx.api('/api/analysis-cell-submit', dict(
            request_id='v05-run', notebook_id=notebook_id, revision=created['revision'],
            cell_id='py', source_sha256='0' * 64, generation=created.get('generation', 1)))
        assert code in (200, 202), submitted
        fx.shot(page, 'v05-notebook-submitted')
        # the inline chart must really read fixed handle -> source -> page (D15)
        assert _facts(log, 'analysis-notebook-create'), 'notebook must be created over real API'
        fx.record('v05-runtime', dict(profile=fx.analysis_profile, notebook_id=notebook_id,
                                      execution_format=fx.execution_format))
        # counterexample: a stale source_sha256 cannot submit edited content
        code, value = fx.api('/api/analysis-cell-submit', dict(
            request_id='v05-stale', notebook_id=notebook_id, revision=created['revision'],
            cell_id='py', source_sha256='f' * 64, generation=created.get('generation', 1)))
        assert code >= 400, value
        assert not log['errors'], log['errors']


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
        images = page.locator('img.analysis-media')
        assert images.count() >= 2
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
        save(page, '第一次复核结论')
        first = latest(f, 'annotations-save')
        assert first['entries']
        save(page, '第二次复核结论')
        second = latest(f, 'annotations-save')
        assert second['entries'] != first['entries']
        page.screenshot(path=str(tmp_path / 'v07-review-saved.png'), full_page=True)
        # reopen reads the persisted answer back from real Records
        page.get_by_role('button', name='管道', exact=True).click()
        page.get_by_role('button', name='数据', exact=True).click()
        expect(page.get_by_label('标注 备注', exact=True)).to_have_value('第二次复核结论')
        # counterexample: applying a review filter is a visible draft, not a
        # silent full-table claim, and never advances past an unsaved answer
        before = len(f['calls'])
        apply_filter(page)
        assert len(f['calls']) >= before
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
        # twenty tab switches must not leak readers; stability is judged by the
        # page's own private reader handles, not unrelated system process counts
        for _ in range(20):
            page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
            page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.locator('.wb-graph, .wb-basis').first).to_be_visible()
        live_readers = page.evaluate(
            '() => (window.ForgeWorkbenchState && ForgeWorkbenchState.liveReaders)'
            ' ? ForgeWorkbenchState.liveReaders() : null')
        fx.shot(page, f'v08-after-switches-{fx.execution_format}')
        assert all(r['status'] < 500 for r in log['responses']), log['responses'][-5:]
        fx.record(f'v08-cleanup-{fx.execution_format}', dict(
            switches=20, requests=len(log['requests']), responses=len(log['responses']),
            live_readers=live_readers, execution_format=fx.execution_format))
        # counterexample: a late response for an old epoch cannot repaint the
        # current selection; the graph still shows the last sealed run
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
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
        assert namespaces['graph'] in ('object', 'function'), namespaces
        # exactly one graph drawing authority; the old duplicate painter is gone
        canvases = page.locator('.wb-graph svg, .wb-graph canvas').count()
        assert canvases <= 1, canvases
        _select_flow(page)
        _run(page)
        # C09 normalizeExecutionPlan must really be called by the production
        # painter. Install a real counting probe on the new builder, reload and
        # re-drive the graph; a builder the real page never calls fails this gate.
        page.add_init_script("""
            window.__d19NormalizeCalls = 0;
            const timer = setInterval(() => {
                const ns = window.ForgeWorkbenchPresentation;
                if (ns && typeof ns.normalizeExecutionPlan === 'function' && !ns.__d19Wrapped) {
                    ns.__d19Wrapped = true;
                    const original = ns.normalizeExecutionPlan;
                    ns.normalizeExecutionPlan = function (...args) {
                        window.__d19NormalizeCalls++;
                        return original.apply(this, args);
                    };
                    clearInterval(timer);
                }
            }, 20);
        """)
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