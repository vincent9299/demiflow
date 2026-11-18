"""Project tabs share the real compiled plan; presentation never starts a job."""
from playwright.sync_api import expect

from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_workbench_plan_node_details_ui import setup_materials


def tab(page, name):
    page.locator('.function-nav').get_by_role('button', name=name, exact=True).click()


def test_compact_tabs_keep_real_plan_selection_and_history_context(original, tmp_path, browser):
    setup_materials(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        assert page.locator('.function-nav button').all_text_contents() == [
            '开发', '算子', '管道', '运行监控', '数据', 'Readme']
        tab(page, '管道')
        expect(page.locator('.wb-node')).to_have_count(4)
        expect(page.locator('.wb-node-panel')).to_be_hidden()
        expect(page.locator('.wb-navigation')).to_have_count(0)
        assert page.get_by_role('combobox', name='入参文件', exact=True).count() == 1
        expect(page.get_by_role('combobox', name='解析配置路径')).to_be_hidden()
        initial = len([u for m, u in requests if m == 'POST' and u.endswith('/plan-preview')])
        page.locator('.wb-node').filter(has_text='联合出题').click()
        page.locator('.wb-source-tabs').get_by_role('button', name='自定义 Prompt', exact=True).click()
        expect(page.locator('.wb-node-panel')).to_be_visible()
        expect(page.locator('.wb-node-panel .wb-source-body')).to_contain_text('{{ payload | json }}')
        tab(page, '算子')
        expect(page.locator('.wb-operator-row[aria-pressed=true]')).to_contain_text('联合出题')
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('{{ payload | json }}')
        tab(page, '运行监控')
        expect(page.locator('.wb-history')).to_be_visible()
        expect(page.locator('.wb-body')).to_be_hidden()
        tab(page, '管道')
        expect(page.locator('.wb-node')).to_have_count(4)
        expect(page.locator('.wb-node[aria-pressed=true]')).to_contain_text('联合出题')
        assert len([u for m, u in requests if m == 'POST' and u.endswith('/plan-preview')]) == initial
        page.get_by_role('button', name='关闭详情', exact=True).click()
        expect(page.locator('.wb-node-panel')).to_be_hidden()
        page.locator('.wb-more > summary').click()
        page.keyboard.press('Escape')
        assert page.locator('.wb-more').get_attribute('open') is None
        page.locator('.wb-more > summary').click()
        page.locator('.wb-more').get_by_role('button', name='检查问题', exact=True).click()
        expect(page.locator('.wb-checks')).to_be_visible()
        page.get_by_role('button', name='收起检查', exact=True).click()
        expect(page.locator('.wb-checks')).to_be_hidden()
        tab(page, '算子')
        for _ in range(2):
            with page.expect_response(lambda r: r.url.endswith('/plan-preview')) as response:
                page.reload()
            assert response.value.status == 200
            expect(page.locator('.function-nav [data-page=operators]')).to_have_attribute('aria-current', 'page')
            expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('{{ payload | json }}')
        for width in (1440, 1024, 780, 390):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert all(u.endswith(('/plan-preview', '/plan-node-detail')) for m, u in requests if m == 'POST')
    assert business_bytes(original) == before


def test_late_real_compile_does_not_close_the_users_checks(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        expect(page.locator('.wb-node')).to_have_count(4)
        page.evaluate('''() => {
          const native=fetch.bind(window);
          window.fetch=async(...args)=>{
            const response=await native(...args);
            if(String(args[0]).endsWith('/plan-preview')){
              if(!response.ok)throw new Error('Real compiler did not succeed');
              return new Promise(resolve=>{window.releasePlan=()=>resolve(response);});
            }
            return response;
          };
        }''')
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        page.wait_for_function('() => typeof window.releasePlan === "function"')
        page.locator('.wb-more > summary').click()
        page.locator('.wb-more').get_by_role('button', name='检查问题', exact=True).click()
        expect(page.locator('.wb-checks')).to_be_visible()
        page.evaluate('() => window.releasePlan()')
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        expect(page.locator('.wb-checks')).to_be_visible()
        expect(page.locator('.wb-node')).to_have_count(4)


def test_project_change_can_leave_pending_plan_read(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    other = tmp_path / 'other'
    other.mkdir()
    (other / 'README.md').write_text('# Another project\nIndependent reading context.')
    other_id = registry.add(path, other)['workspace_id']
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        page.locator('#project-list').get_by_role('button', name='shipping', exact=True).click()
        tab(page, '管道')
        expect(page.locator('.wb-node')).to_have_count(4, timeout=65000)
        page.evaluate('''() => {
          const native=fetch.bind(window);
          window.fetch=async(...args)=>{
            const response=await native(...args);
            if(String(args[0]).endsWith('/plan-preview')){
              if(!response.ok)throw new Error('Real compiler did not succeed');
              return new Promise(resolve=>{window.releasePlan=()=>resolve(response);});
            }
            return response;
          };
        }''')
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        page.wait_for_function('() => typeof window.releasePlan === "function"')
        page.locator('#project-list [data-workspace="'+other_id+'"]').click()
        expect(page.get_by_role('heading', name='Another project', exact=True)).to_be_visible()
        page.evaluate('() => window.releasePlan()')
        expect(page.get_by_role('heading', name='Another project', exact=True)).to_be_visible()
        expect(page.locator('.workbench')).to_have_count(0)
        assert page.evaluate('ForgeView.decodeView(location.hash).workspace') == other_id
