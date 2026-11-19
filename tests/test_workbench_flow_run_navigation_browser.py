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
            expect(page.locator('.wb-flow-navigation')).to_be_visible()
            expect(page.locator('.wb-navigation-toggle')).to_have_count(0)
            expect(page.locator('.wb-selectors')).to_be_hidden()
            expect(flow(page, FIRST)).to_be_visible()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        screenshot(page, 'project-pipeline-navigation-320')
        assert not any(url.endswith(('/flow-arguments-save', '/run-prepare', '/run-start')) for url in writes)
        assert not errors, errors


def test_run_directory_simple_groups_show_more_and_identity(root, browser):
    two_flows(root)
    value = prepared(root)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        seen = []
        real = dict(run_id=value['run_id'], entry=FIRST, created_at=1700000000,
                    parameter_path='flows/select_a.json', status='prepared', state={})
        rows = [real] + [dict(run_id='navigation-fixture-' + str(i),
                    entry=FIRST if i % 2 else SECOND, created_at=1700000000 - i,
                    parameter_path=None, status='failed' if i % 3 == 0 else 'succeeded', state={})
                    for i in range(1, 42)]
        def run_page(route):
            query = parse_qs(urlsplit(route.request.url).query); seen.append(query)
            assert query.get('limit') == ['20']
            offset = int(query.get('cursor', ['fixture-offset-0'])[0].removeprefix('fixture-offset-'))
            route.fulfill(status=200, content_type='application/json', body=json.dumps({
                'runs': rows[offset:offset+20], 'total': len(rows),
                'next_cursor': 'fixture-offset-'+str(offset+20) if offset+20<len(rows) else None,
                'index': {'complete': True}}))
        page.route('**/runs?*', run_page)
        tab(page, 'monitor')
        nav = page.locator('.wb-history')
        expect(page.locator('.wb-run-group')).to_have_count(2)
        expect(page.locator('.wb-run-record')).to_have_count(10)
        expect(nav.locator('input,select,summary,details')).to_have_count(0)
        for text in ('管道与运行','最近运行','较新记录','较早记录','第 1 页'):
            expect(nav).not_to_contain_text(text)
        group = page.locator('.wb-run-group[data-entry="'+FIRST+'"]')
        group.locator('.wb-run-group-heading').click()
        expect(group.locator('.wb-run-record')).to_have_count(5)
        for count in (10,15,20,22):
            group.get_by_role('button',name='显示更多',exact=True).click()
            expect(group.locator('.wb-run-record')).to_have_count(count)
        expect(group.get_by_role('button',name='显示更多',exact=True)).to_have_count(0)
        assert len(set(group.locator('.wb-run-record').evaluate_all('(rows)=>rows.map(r=>r.dataset.run)'))) == 22
        row = group.locator('.wb-run-record').first
        assert row.bounding_box()['height'] <= 32
        assert group.locator('.wb-run-group-heading').bounding_box()['height'] <= 30
        assert group.locator('.wb-run-group-heading').evaluate('(e)=>getComputedStyle(e,"::before").content') in ('none','normal')
        screenshot(page, 'simple-run-sidebar-desktop')
        # The actual prepared run retains its controls and exact identity.
        row.click()
        expect(page.get_by_role('button',name='开始这次运行',exact=True)).to_be_enabled()
        expect(nav.locator('.wb-run-record.selected')).to_have_attribute('data-run',value['run_id'])
        page.reload()
        expect(nav.locator('.wb-run-record.selected')).to_have_attribute('data-run',value['run_id'])
        # Delay a real list response while selecting another pipeline.
        page.evaluate('''() => {
          const original=fetch.bind(window);window.runListHeld=false;
          window.fetch=async(...args)=>{
            const response=await original(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/runs')&&!runListHeld){runListHeld=true;return new Promise(resolve=>window.releaseRunList=()=>resolve(response));}
            return response;
          };
        }''')
        page.locator('.wb-run-group[data-entry="'+SECOND+'"] .wb-run-group-heading').click()
        page.wait_for_function('()=>window.runListHeld')
        group.locator('.wb-run-group-heading').click()
        expect(group.locator('.wb-run-record')).to_have_count(5)
        page.evaluate('releaseRunList()')
        expect(page.locator('.wb-run-record')).to_have_count(5)
        # Sparse history remains reachable without loading all project history.
        rows[:] = [dict(real, run_id='sparse-'+str(i), entry=SECOND) for i in range(84)] + [real]
        before = len(seen)
        group.locator('.wb-run-group-heading').click()
        expect(group.get_by_role('button',name='显示更多',exact=True)).to_be_visible()
        assert len(seen)-before == 3
        group.get_by_role('button',name='显示更多',exact=True).click()
        expect(group.locator('.wb-run-record')).to_have_count(1)
        expect(group.locator('.wb-run-record')).to_have_attribute('data-run',value['run_id'])
        expect(group.get_by_role('button',name='显示更多',exact=True)).to_have_count(0)
        for width in (1024,390,320):
            page.set_viewport_size({'width':width,'height':920})
            expect(nav).to_be_visible()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        screenshot(page, 'simple-run-sidebar-320')
        # Reaching the cache bound must still allow older history; refresh must
        # not evict unseen older records by reinserting the latest page.
        page.set_viewport_size({'width':1440,'height':1050})
        rows[:] = [dict(real, run_id='deep-'+str(i), created_at=1700000000-i)
                   for i in range(242)]
        group.locator('.wb-run-group-heading').click()
        expect(group.locator('.wb-run-record')).to_have_count(5)
        for count in range(10,201,5):
            group.get_by_role('button',name='显示更多',exact=True).click()
            expect(group.locator('.wb-run-record')).to_have_count(count)
        for final_id in ('deep-219','deep-239','deep-241'):
            group.get_by_role('button',name='显示更多',exact=True).click()
            expect(group.locator('.wb-run-record').last).to_have_attribute('data-run',final_id)
            expect(group.locator('.wb-run-record')).to_have_count(200)
        expect(group.get_by_role('button',name='显示更多',exact=True)).to_have_count(0)
        page.get_by_role('button',name='运行记录',exact=True).click()
        expect(group.locator('.wb-run-record').last).to_have_attribute('data-run','deep-241')
        expect(group.locator('.wb-run-record').first).to_have_attribute('data-run','deep-42')
        group.locator('.wb-run-group-heading').click()
        expect(group.locator('.wb-run-record')).to_have_count(5)
        expect(group.locator('.wb-run-record').first).to_have_attribute('data-run','deep-0')
        assert not errors, errors
