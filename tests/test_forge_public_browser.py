"""A browser is a public API client, independent of the development harness."""
import json
import subprocess
import sys
from urllib.parse import urlencode

import pytest

from playwright.sync_api import expect
from demiflow.forge.runs import manager as runs

from test_forge_panel_browser import panel, open_panel, demo, isolated
from test_forge_public_api import request


@pytest.mark.parametrize('initial_token', ['', 'expired-session-token'])
def test_authorization_link_recovers_in_same_document(panel, initial_token):
    root, app, page, errors = panel
    page.goto(app.origin + '/' + ('#token=' + initial_token if initial_token else ''))
    page.get_by_text('会话授权已失效，请使用当前服务的有效授权入口重新打开。', exact=True).wait_for()

    fragment = urlencode(dict(token=app.token, view=json.dumps(
        dict(v=1, workspace=str(root), kind='flows'))))
    page.evaluate('(fragment) => { location.hash = fragment; }', fragment)
    page.locator('.wb-node').first.wait_for()
    expect(page.locator('#workspace-name')).to_have_text('question-experiments')
    expect(page.locator('.error-box')).to_have_count(0)
    assert page.evaluate("sessionStorage.getItem('forge-token')") == app.token
    assert not page.evaluate("new URLSearchParams(location.hash.slice(1)).has('token')")

    page.reload()
    page.locator('.wb-node').first.wait_for()
    expect(page.locator('.error-box')).to_have_count(0)
    assert not errors


def test_non_json_failure_recovers_and_expired_session_stops_polling(panel):
    root, app, page, errors = panel
    open_panel(app, page)
    page.route('**/changes', lambda route: route.fulfill(status=503, content_type='text/html', body='busy'))
    page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    page.locator('#sync-status').filter(has_text='HTTP 503').wait_for()
    assert page.locator('#workspace-name').filter(has_text='question-experiments').count()
    page.unroute('**/changes')
    page.route('**/changes', lambda route: route.fulfill(status=503, content_type='application/json', body='null'))
    with page.expect_response('**/changes'):
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    page.locator('#sync-status').filter(has_text='HTTP 503').wait_for()
    page.unroute('**/changes')
    (root / 'README.md').write_text('Transport recovery retained the same workspace.')
    page.get_by_text('Transport recovery retained the same workspace.', exact=True).wait_for()
    calls = []
    page.on('request', lambda r: calls.append(r.url))
    app.token = 'b' * 43
    page.locator('#sync-status').filter(has_text='授权已失效').wait_for()
    count = len(calls)
    page.wait_for_timeout(3500)
    assert len(calls) == count
    open_panel(app, page)
    page.get_by_text('Transport recovery retained the same workspace.', exact=True).wait_for()
    assert not errors


def test_submission_waits_for_selected_graph_to_load(panel):
    root,app,page,errors=panel;app.enable_runs=True
    open_panel(app,page,dict(v=1,workspace=str(root),kind='flows'))
    held=[]
    def hold_selected(route):
        if 'params=flows%2Fselect_b.json' in route.request.url:held.append(route)
        else:route.continue_()
    page.route('**/graph?*',hold_selected)
    with page.expect_request(lambda r:'/graph?' in r.url and 'select_b.json' in r.url):
        page.get_by_label('配置',exact=True).select_option('flows/select_b.json')
    expect(page.get_by_role('button',name='运行',exact=True)).to_be_disabled()
    assert len(held)==1
    held[0].continue_();page.unroute('**/graph?*',hold_selected)
    expect(page.get_by_role('button',name='运行',exact=True)).to_be_enabled()
    with page.expect_response('**/run-prepare') as prepared:
        page.get_by_role('button',name='运行',exact=True).click()
    assert prepared.value.ok,prepared.value.text()
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功',timeout=90000)
    observed=runs.list_runs(root)
    assert observed['total']==1 and observed['runs'][0]['parameter_path']=='flows/select_b.json'
    assert not errors


def test_two_browsers_cli_and_http_share_run_and_fixed_result(panel):
    root, app, page, errors = panel
    app.enable_runs = True
    second_context = page.context.browser.new_context()
    second = second_context.new_page()
    second.set_default_timeout(90000)
    second.on('pageerror', lambda error: errors.append(str(error)))
    try:
        target = dict(v=1, workspace=str(root), kind='runs')
        open_panel(app, page, target)
        open_panel(app, second, target)
        cli = subprocess.run([sys.executable, '-B', '-m', 'demiflow.cli', 'workbench',
            'run', 'prepare', str(root), 'flows/questions.py:select', '--params', 'flows/select_a.json'],
            capture_output=True, text=True, timeout=90)
        assert cli.returncode == 0, cli.stdout + cli.stderr
        prepared = json.loads(cli.stdout)
        identity = prepared['run_id']
        second.locator('.wb-run-record[data-run=\"'+identity+'\"]').wait_for()
        page.locator('.wb-run-record[data-run=\"'+identity+'\"]').click()
        with page.expect_request('**/run-start') as submitted:
            page.get_by_role('button', name='开始这次运行', exact=True).click()
        assert submitted.value.post_data_json == dict(run_id=identity, expected=prepared['digest'], expected_attempt=1)
        second.locator('.wb-run-record[data-run=\"'+identity+'\"]').filter(has_text='成功').wait_for()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功',timeout=90000)
        page.get_by_role('button',name='输出结果',exact=True).click()
        code, observed = request(app, '/api/run?run_id=' + identity)
        assert code == 200 and observed['status'] == 'succeeded' and observed['attempt'] == 1
        page.get_by_label('结果别名 target').fill('shared_result')
        with page.expect_request('**/run-attach') as attached:
            page.get_by_role('button', name='保留为输入表', exact=True).click()
        assert attached.value.post_data_json['expected_result'] == observed['outputs'][0]['result_digest']
        assert attached.value.post_data_json['attempt'] == 1
        page.get_by_text('已保留固定版本引用：shared_result', exact=True).wait_for()
        code, table = request(app, '/api/table?alias=shared_result')
        assert code == 200 and table['reference']['ref'] == observed['outputs'][0]['result']['ref']
        second.get_by_role('button', name='数据', exact=True).click()
        second.get_by_role('button',name='返回当前表',exact=True).click()
        second.get_by_role('button',name='展开数据目录',exact=True).click()
        second.get_by_text('shared_result', exact=True).first.wait_for()
        assert runs.show(root, identity)['digest'] == prepared['digest']
        assert not errors
    finally:
        second_context.close()


def test_lost_prepare_response_is_not_automatically_replayed(panel):
    root, app, page, errors = panel
    app.enable_runs = True
    open_panel(app, page, dict(v=1, workspace=str(root), kind='flows'))
    submissions = []

    def lose_response(route):
        submissions.append(route.request.post_data_json)
        response = route.fetch(timeout=90000)
        assert response.status == 200
        route.abort('connectionreset')

    page.route('**/run-prepare', lose_response)
    page.get_by_role('button',name='运行',exact=True).click()
    page.locator('.error-box').filter(has_text='核对操作结果').wait_for()
    page.wait_for_timeout(3500)
    assert len(submissions) == 1
    listing = runs.list_runs(root)
    assert listing['total'] == 1 and listing['runs'][0]['status'] == 'prepared'
    assert not errors
