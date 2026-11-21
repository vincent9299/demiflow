"""The platform run entry works before integration and drives real owned runs."""
from demiflow.forge.project import initialize_project
import pytest
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
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
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_disabled()
        expect(page.get_by_role('button',name='检查',exact=True)).to_be_disabled()
        expect(page.locator('.wb-graph')).to_contain_text('尚无已声明入口')
        expect(page.locator('.wb-settings')).to_be_visible()
        expect(page.get_by_role('button',name='登记原生流程',exact=True)).to_be_disabled()
        page.get_by_role('button',name='运行记录',exact=True).click()
        expect(page.locator('.wb-history')).to_contain_text('当前页没有运行记录')
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


def test_management_runs_filters_and_opens_real_result(root,browser):
    with service(root,enable_runs=True) as app,opened(browser,app) as (page,_):
        page.set_default_timeout(65000)
        navigate(page,'管道')
        page.get_by_role('button',name='运行',exact=True).click()
        expect(page.locator('.wb-run-bar')).to_be_visible(timeout=65000)
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功',timeout=65000)
        run_id=page.evaluate('ForgeView.decodeView(location.hash).run_id')
        value=runs.show(root,run_id)
        assert value['status']=='succeeded' and value['outputs'][0]['result']['ref']['row_count']==2
        page.get_by_role('button',name='运行记录',exact=True).click()
        state=page.get_by_label('筛选本页运行状态',exact=True)
        state.select_option('failed')
        expect(page.locator('.wb-history')).to_contain_text('本页没有符合筛选条件的运行')
        assert page.locator('.wb-run-record').count()==0
        state.select_option('succeeded')
        page.locator('.wb-run-record[data-run="'+run_id+'"]').click()
        expect(page.locator('.wb-run-bar > strong')).to_have_text(run_id)
        page.get_by_role('button',name='输出结果',exact=True).click()
        page.get_by_role('button',name='查看固定结果',exact=True).click()
        page.get_by_text('c01',exact=True).first.wait_for()
