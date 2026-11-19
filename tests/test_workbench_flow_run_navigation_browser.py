"""Project directories reuse actual flow state and bounded run-page contracts.

The pagination test supplies named synthetic list rows around one real prepared
record. It tests browser paging, never represents those rows as executed jobs.
No pipeline rows or models are executed by these tests.
"""
import json
from urllib.parse import parse_qs, urlsplit

from playwright.sync_api import expect

from demiflow.forge.platform import content_digest
from demiflow.workbench.projects import workspace as ws
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_runs import base, root, prepared
from test_forge_workbench_browser import page_for, screenshot


FIRST = 'flows/questions.py:select'
SECOND = 'flows/navigation.py:run'


def two_flows(root):
    path = root / 'workspace.json'
    value = json.loads(path.read_text())
    expected = content_digest(value)
    value['entries'] = {FIRST: ['flows/select_a.json']}
    ws.write_control(path, value, expected=expected)
    (root / 'flows/navigation.py').write_text(
        'from demiflow import data\n'
        'def run(*, minimum=0.3):\n'
        '    return data.from_items([{ "minimum": minimum }]).take_all()\n')
    (root / 'flows/navigation.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1', 'arguments': {'minimum': 0.3}}))
    ws.register_flow(root, SECOND, params='flows/navigation.json')


def tab(page, name):
    page.locator('.function-nav [data-page="' + name + '"]').click()


def flow(page, entry):
    return page.locator('.wb-flow-record[data-entry="' + entry + '"]')


def test_pipeline_directory_preserves_drafts_late_selection_and_narrow_space(root, browser):
    two_flows(root)
    before = (root / 'flows/select_a.json').read_bytes()
    with service(root, enable_config_write=True, workspace_config_write=True) as app, page_for(browser, app) as (page, errors):
        writes = []
        page.on('request', lambda request: writes.append(request.url)
                if request.method == 'POST' else None)
        tab(page, 'flows')
        expect(page.locator('.wb-flow-record')).to_have_count(2)
        flow(page, FIRST).click()
        expect(flow(page, FIRST)).to_have_attribute('aria-current', 'page')
        expect(page.locator('.wb-selectors')).to_be_hidden()
        expect(page.locator('.wb-section-navigation:visible')).to_have_count(1)
        directory = page.locator('.wb-flow-navigation').bounding_box()
        canvas = page.locator('.wb-workspace-main').bounding_box()
        assert directory['x'] + directory['width'] < canvas['x']
        screenshot(page, 'project-pipeline-navigation-desktop')

        page.get_by_role('button', name='业务参数', exact=True).click()
        page.get_by_label('minimum', exact=True).fill('0.71')
        flow(page, SECOND).click()
        expect(flow(page, SECOND)).to_have_attribute('aria-current', 'page')
        page.get_by_role('button', name='业务参数', exact=True).click()
        expect(page.get_by_label('minimum', exact=True)).to_have_value('0.3')
        flow(page, FIRST).click()
        page.get_by_role('button', name='业务参数', exact=True).click()
        expect(page.get_by_label('minimum', exact=True)).to_have_value('0.71')
        expect(page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)).to_be_disabled()
        assert (root / 'flows/select_a.json').read_bytes() == before

        # Hold a genuine graph response after fetch resolves. The first request
        # can then finish after a new selection without replacing that selection.
        page.evaluate('''entry => {
          const native=fetch.bind(window);window.navigationHeld=false;
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/graph')&&url.searchParams.get('entry')===entry&&!navigationHeld){
              if(!response.ok)throw Error('Real graph request failed');
              navigationHeld=true;
              return new Promise(resolve=>window.releaseNavigationGraph=()=>resolve(response));
            }
            return response;
          };
        }''', SECOND)
        flow(page, SECOND).click()
        page.wait_for_function('() => window.navigationHeld')
        flow(page, FIRST).click()
        expect(flow(page, FIRST)).to_have_attribute('aria-current', 'page')
        page.evaluate('releaseNavigationGraph()')
        page.wait_for_function('entry=>ForgeView.decodeView(location.hash)?.entry===entry', arg=FIRST)
        page.get_by_role('button', name='业务参数', exact=True).click()
        expect(page.get_by_label('minimum', exact=True)).to_have_value('0.71')

        tab(page, 'operators')
        expect(page.locator('.op-tree')).to_be_visible()
        expect(page.locator('.wb-section-navigation:visible')).to_have_count(0)
        tab(page, 'monitor')
        expect(page.locator('.wb-section-navigation:visible')).to_have_count(1)
        expect(page.locator('.wb-history')).to_be_visible()
        expect(page.locator('.wb-flow-navigation')).to_be_hidden()
        tab(page, 'flows')
        expect(flow(page, FIRST)).to_have_attribute('aria-current', 'page')

        for width in (390, 320):
            page.set_viewport_size({'width': width, 'height': 920})
            expect(page.locator('.wb-flow-navigation')).to_be_hidden()
            toggle = page.locator('.wb-navigation-toggle')
            expect(toggle).to_have_attribute('aria-expanded', 'false')
            expect(page.locator('.wb-selectors')).to_be_hidden()
            container = page.locator('.wb-workspace-layout').bounding_box()
            content = page.locator('.wb-workspace-main').bounding_box()
            assert abs(container['width'] - content['width']) < 2
            toggle.click()
            expect(page.locator('.wb-flow-navigation')).to_be_visible()
            toggle.click()
            expect(page.locator('.wb-flow-navigation')).to_be_hidden()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        screenshot(page, 'project-pipeline-navigation-320')
        assert not any(url.endswith(('/flow-arguments-save', '/run-prepare', '/run-start')) for url in writes)
        assert not errors, errors


def test_run_directory_groups_current_page_and_restores_real_prepared_run(root, browser):
    two_flows(root)
    value = prepared(root)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        seen = []
        real = dict(run_id=value['run_id'], entry=FIRST, created_at=1700000000,
                    parameter_path='flows/select_a.json', status='prepared', state={})
        rows = [real] + [dict(run_id='navigation-fixture-' + str(i),
                    entry=FIRST if i % 2 else SECOND, created_at=1700000000 - i,
                    parameter_path=None, status='failed' if i % 3 == 0 else 'succeeded',
                    state={}) for i in range(1, 22)]
        def run_page(route):
            query = parse_qs(urlsplit(route.request.url).query)
            seen.append(query)
            assert query.get('limit') == ['20']
            old = query.get('cursor') == ['fixture-older']
            route.fulfill(status=200, content_type='application/json', body=json.dumps({
                'runs': rows[20:] if old else rows[:20], 'total': len(rows),
                'next_cursor': None if old else 'fixture-older', 'index': {'complete': True}}))
        page.route('**/runs?*', run_page)
        tab(page, 'monitor')
        expect(page.locator('.wb-run-record')).to_have_count(20)
        expect(page.locator('.wb-run-group')).to_have_count(2)
        expect(page.locator('.wb-history-heading')).to_contain_text('第 1 页 · 本页 20 条 · 全项目共 22 条')
        assert not any(query.get('cursor') for query in seen)
        for row in page.locator('.wb-run-record').all():
            assert row.locator('.wb-record-time').inner_text().startswith('2023-')
            assert row.locator('.wb-tag').inner_text()
        page.get_by_label('筛选本页运行状态', exact=True).select_option('failed')
        assert page.locator('.wb-run-record').count() == 6
        page.get_by_role('button', name='较早记录', exact=True).click()
        expect(page.locator('.wb-history-heading')).to_contain_text('第 2 页 · 本页 2 条')
        expect(page.locator('.wb-run-record')).to_have_count(1)
        expect(page.get_by_role('button', name='较早记录', exact=True)).to_be_disabled()
        page.get_by_label('筛选本页运行状态', exact=True).select_option('')
        expect(page.locator('.wb-run-record')).to_have_count(2)
        page.get_by_role('button', name='较新记录', exact=True).click()
        expect(page.locator('.wb-run-record')).to_have_count(20)
        page.locator('.wb-run-record[data-run="' + value['run_id'] + '"]').click()
        expect(page.locator('.wb-run-record.selected')).to_have_attribute('data-run', value['run_id'])
        expect(page.get_by_role('button', name='开始这次运行', exact=True)).to_be_enabled()
        page.wait_for_function('id=>ForgeView.decodeView(location.hash)?.run_id===id', arg=value['run_id'])
        page.reload()
        expect(page.locator('.wb-run-record.selected')).to_have_attribute('data-run', value['run_id'])
        page.locator('.wb-navigation-toggle').click()
        expect(page.locator('.wb-history')).to_be_hidden()
        page.get_by_role('button', name='运行记录', exact=True).click()
        expect(page.locator('.wb-history')).to_be_visible()
        page.set_viewport_size({'width': 390, 'height': 920})
        expect(page.locator('.wb-history')).to_be_hidden()
        expect(page.locator('.wb-navigation-toggle')).to_have_text('运行列表')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert len(rows) == 22 and all(query.get('limit') == ['20'] for query in seen)
        screenshot(page, 'project-run-navigation-390')
        assert not errors, errors
