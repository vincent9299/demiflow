"""The platform run entry works before integration and drives real owned runs."""
from demiflow.forge.project import initialize_project
import pytest
from playwright.sync_api import expect

from demiflow.forge.platform import content_digest
from demiflow.workbench.projects import registry
from demiflow.workbench.runs import manager as runs
from test_workbench_run_module import single_flow
from test_forge_runs import base, root
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened, artifact, navigate


def test_empty_project_shows_capability_without_grant_or_execution(tmp_path, browser):
    project = tmp_path / 'unintegrated-project'
    project.mkdir()
    (project / 'README.md').write_text('An existing project; integration comes later.')
    initialize_project(project, existing=True)
    original = (project / 'workspace.json').read_bytes()
    with service(project, enable_runs=False) as app, opened(browser, app) as (page, requests):
        navigate(page, '管道')
        expect(page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)).to_be_disabled()
        page.locator('.wb-more > summary').click()
        expect(page.get_by_role('button',name='检查',exact=True)).to_be_disabled()
        page.locator('.wb-more > summary').click()
        expect(page.locator('.wb-graph')).to_contain_text('未识别到受支持的入口声明')
        expect(page.locator('.wb-settings')).to_be_visible()
        expect(page.get_by_role('button',name='登记原生流程',exact=True)).to_be_disabled()
        navigate(page, '运行')
        expect(page.get_by_role('navigation', name='运行记录导航')).to_be_visible()
        expect(page.locator('.wb-run-record')).to_have_count(0)
        assert not page.locator('.wb-run-facts').is_visible()
        for width in (1440, 390, 320):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
            artifact(page, f'run-management-empty-{width}')
        assert not any(method != 'GET' for method, _ in requests)
    assert (project / 'workspace.json').read_bytes() == original
    assert not list((project / '.forge/runs').glob('*'))


@pytest.fixture(autouse=True)
def private_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


def hold_next_response(page, endpoint):
    """Deliver the next matching real HTTP response when released by the test."""
    page.evaluate("""endpoint => {
        const native = window.fetch.bind(window);
        window.releaseOutputRead = null;
        window.fetch = async (...args) => {
            if (!new URL(args[0], location.href).pathname.endsWith('/' + endpoint)) return native(...args);
            window.fetch = native;
            const response = await native(...args);
            await new Promise(resolve => {window.releaseOutputRead = resolve;});
            return response;
        };
    }""", endpoint)


def release_response(page):
    page.wait_for_function("typeof window.releaseOutputRead === 'function'")
    page.evaluate("() => window.releaseOutputRead()")


def test_management_runs_filters_and_opens_real_result(root,browser,tmp_path):
    # This business fixture also carries historical parameter variants. Select
    # the one current binding explicitly, as an author must in a real project.
    single_flow(root)
    projects = tmp_path / 'registry.json'
    project = registry.add(projects, root, allow_runs=True)['workspace_id']
    with service(None,registry=projects,enable_runs=True) as app,opened(browser,app) as (page,_):
        page.set_default_timeout(65000)
        navigate(page,'管道')
        page.locator('.wb-flow-record[data-entry="flows/questions.py:select"]').click()
        page.locator('.wb-actions').get_by_role('button', name='运行', exact=True).click()
        page.get_by_role('button', name='确认运行', exact=True).click()
        expect(page.locator('.wb-run-bar')).to_be_visible(timeout=65000)
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('已完成',timeout=65000)
        run_id=page.evaluate('ForgeView.decodeView(location.hash).run_id')
        value=runs.show(root,run_id)
        assert value['status']=='succeeded' and value['outputs'][0]['result']['ref']['row_count']==2
        navigate(page, '运行')
        group = page.locator('.wb-run-group[data-entry="flows/questions.py:select"]')
        group.locator('.wb-run-group-heading').click()
        record = group.locator(f'.wb-run-record[data-run="{run_id}"]')
        expect(record).to_be_visible()
        expect(record).to_have_attribute('data-status', 'succeeded')
        page.get_by_role('button', name='搜索运行', exact=True).click()
        search = page.get_by_role('searchbox', name='搜索运行')
        search.fill('不存在的运行')
        expect(record).to_be_hidden()
        search.fill(run_id)
        expect(record).to_be_visible()
        # Let the compact run arrive before its first real observation snapshot.
        hold_next_response(page, 'run-snapshot')
        with page.expect_response('**/run-snapshot?*') as snapshot:
            record.click()
        expect(record).to_have_attribute('aria-pressed', 'true')
        assert page.evaluate('ForgeView.decodeView(location.hash).run_id') == run_id
        expect(page.locator('.wb-run-bar')).to_contain_text(run_id[-10:])
        page.locator('.wb-monitor-tabs').get_by_role('button', name='输出结果', exact=True).click()
        page.get_by_role('button',name='查看固定结果',exact=True).click()
        preview = page.locator('.wb-result-preview')
        expect(preview).to_contain_text('c01', timeout=15000)
        expect(preview).to_contain_text('c02')
        original_panel = preview.element_handle()
        bar_title = page.locator('.wb-run-bar > :first-child').element_handle()
        assert snapshot.value.ok
        assert snapshot.value.json()['outputs'][0]['result_digest'] == value['outputs'][0]['result_digest']
        release_response(page)
        page.wait_for_function('title => !title.isConnected', arg=bar_title)
        assert original_panel.evaluate('panel => panel.isConnected')
        expect(preview).to_contain_text('c02')

        # Revoke an actual grant while a second fixed-version read is in flight.
        # Its delayed response must not repopulate the former, writable panel.
        read_result = page.get_by_role('button', name='查看固定结果', exact=True)
        read_result.click()
        expect(preview).to_have_count(0)
        hold_next_response(page, 'sample')
        with page.expect_response('**/sample?*') as sample:
            read_result.click()
        assert sample.value.ok and len(sample.value.json()['rows']) == 2
        registry.set_permissions(projects, project, expected=content_digest(registry.load_registry(projects)),
                                 run_submit=True, run_control=True, attach=False)
        page.get_by_role('button', name='刷新当前视图', exact=True).click()
        expect(page.get_by_role('button', name='保留为输入表', exact=True)).to_be_disabled()
        expect(preview).to_have_count(0)
        release_response(page)
        expect(preview).to_have_count(0)
