"""Project directories reuse actual flow state and bounded run-page contracts.

The pagination test supplies named synthetic list rows around one real prepared
record. It tests browser paging, never represents those rows as executed jobs.
No pipeline rows or models are executed by these tests.
"""
from _forge_project_fixtures import declare_existing_flow
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
    declare_existing_flow(root, SECOND, params='flows/navigation.json')


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
        expect(page.get_by_role('searchbox',name='搜索管道')).to_be_hidden()
        page.get_by_role('button',name='搜索管道',exact=True).click()
        page.get_by_role('searchbox',name='搜索管道').fill(SECOND)
        expect(flow(page,FIRST)).to_be_hidden()
        expect(flow(page,SECOND)).to_be_visible()
        page.keyboard.press('Escape')
        flow(page, FIRST).click()
        expect(flow(page, FIRST)).to_have_attribute('aria-current', 'page')
        page.get_by_role('button',name='管道目录',exact=True).click()
        expect(page.locator('.wb-flow-navigation')).to_be_hidden()
        page.get_by_role('button',name='管道目录',exact=True).click()
        expect(flow(page, FIRST)).to_have_attribute('aria-current', 'page')
        expect(page.locator('.wb-selectors')).to_be_hidden()
        expect(page.locator('.wb-section-navigation:visible')).to_have_count(1)
        directory = page.locator('.wb-flow-navigation').bounding_box()
        canvas = page.locator('.wb-workspace-main').bounding_box()
        assert directory['x'] == 0 and directory['width'] == 240
        assert directory['x'] + directory['width'] == canvas['x']
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
            expect(page.locator('.wb-flow-navigation')).to_be_visible()
            expect(page.locator('.wb-navigation-toggle')).to_have_count(0)
            expect(page.locator('.wb-selectors')).to_be_hidden()
            expect(flow(page, FIRST)).to_be_visible()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        screenshot(page, 'project-pipeline-navigation-320')
        assert not any(url.endswith(('/flow-arguments-save', '/run-prepare', '/run-start')) for url in writes)
        assert not errors, errors


def test_pipeline_run_picker_latest_older_sparse_history_and_empty(root, browser):
    two_flows(root)
    older = prepared(root)
    latest = prepared(root)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        requests = []
        latest_row = dict(run_id=latest['run_id'], entry=FIRST,
                          created_at=1700000001, status='prepared')
        old_row = dict(latest_row, run_id=older['run_id'], created_at=1700000000)
        rows = [latest_row, old_row]
        def run_page(route):
            query = parse_qs(urlsplit(route.request.url).query)
            assert query.get('limit') == ['20']
            offset = int(query.get('cursor', ['0'])[0])
            requests.append(offset)
            route.fulfill(json={
                'runs': rows[offset:offset+20], 'total': len(rows),
                'next_cursor': str(offset+20) if offset+20<len(rows) else None,
                'index': {'complete': True}})
        page.route('**/runs?*', run_page)
        tab(page, 'monitor')
        nav = page.locator('.wb-history')
        first = nav.locator(f'[data-entry="{FIRST}"]')
        second = nav.locator(f'[data-entry="{SECOND}"]')
        picker = page.get_by_role('combobox', name='运行记录', exact=True)
        empty = page.locator('.wb-run-empty')
        first.click()
        expect(picker).to_have_value(latest['run_id'])
        expect(page.locator('.wb-run-bar')).to_contain_text(latest['run_id'][-10:])
        expect(picker.locator('option').first).to_contain_text('最新')
        expect(nav.locator('select,.wb-run-record')).to_have_count(0)
        expect(nav.locator('.wb-run-flow')).to_have_count(2)
        before_fold = len(requests)
        nav.get_by_role('button',name='运行目录').click()
        expect(nav).to_be_hidden()
        page.get_by_role('button',name='运行目录').click()
        expect(nav).to_be_visible()
        assert len(requests) == before_fold
        nav.get_by_role('button',name='搜索管道').click()
        nav.get_by_role('searchbox').fill(SECOND)
        expect(first).to_be_hidden()
        expect(second).to_be_visible()
        page.keyboard.press('Escape')
        picker.select_option(older['run_id'])
        expect(page.locator('.wb-run-bar')).to_contain_text(older['run_id'][-10:])
        page.reload()
        expect(picker).to_have_value(older['run_id'])
        expect(first).to_have_attribute('aria-current','page')
        screenshot(page,'run-picker-history')
        # An outgoing pipeline response must not replace the new pipeline.
        page.evaluate("""() => {
          const native=fetch.bind(window);
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/runs')&&!window.releaseRunList)
              return new Promise(resolve=>window.releaseRunList=()=>resolve(response));
            return response;
          };
        }""")
        second.click()
        page.wait_for_function('()=>!!window.releaseRunList')
        first.click()
        expect(picker).to_have_value(latest['run_id'])
        page.evaluate('releaseRunList()')
        expect(first).to_have_attribute('aria-current','page')
        second.click()
        expect(empty).to_contain_text('这条管道还没有运行记录')
        expect(picker).to_be_hidden()
        expect(page.locator('.wb-run-bar')).to_be_hidden()
        screenshot(page,'run-picker-empty')
        empty.get_by_role('button',name='查看管道').click()
        expect(flow(page, SECOND)).to_have_attribute('aria-current','page')
        tab(page, 'monitor')
        expect(second).to_have_attribute('aria-current','page')
        # The actual latest match is on page five of this project.
        rows[:] = [dict(latest_row, entry=SECOND, run_id='sparse-'+str(i))
                   for i in range(84)] + [latest_row,old_row]
        before = len(requests)
        first.click()
        expect(picker).to_have_value(latest['run_id'])
        assert requests[before:]==[0,20,40,60,80]
        for width in (1024,390,320):
            page.set_viewport_size({'width':width,'height':920})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        screenshot(page,'run-picker-mobile')
        # The selected run remains accessible when the bounded older window
        # moves past it. A fresh flow selection resets to the latest page.
        rows[:] = [latest_row]+[dict(old_row,run_id='deep-'+str(i),created_at=1699999999-i)
                               for i in range(241)]
        first.click()
        expect(picker.locator('option')).to_have_count(20)
        more = page.get_by_role('button',name='更早记录')
        for _ in range(12):
            more.click()
        expect(more).to_be_hidden()
        expect(picker.locator('option')).to_have_count(201)
        expect(picker.locator('option[value="deep-240"]')).to_have_count(1)
        expect(picker).to_have_value(latest['run_id'])
        first.click()
        expect(picker.locator('option')).to_have_count(20)
        expect(picker).to_have_value(latest['run_id'])
        assert not errors, errors
