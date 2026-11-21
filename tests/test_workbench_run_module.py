"""Run workspace acceptance against the real service and execution journal."""
from _forge_project_fixtures import declare_existing_flow
import json
import time
from pathlib import Path

from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_runs import base, root, wait, custom
from test_forge_public_api import service
from test_forge_markdown_reading import browser
from test_forge_workbench_browser import page_for, screenshot
from test_forge_resume import stopped


def single_flow(root):
    path = root / 'workspace.json'
    value = json.loads(path.read_text())
    expected = content_digest(value)
    value['entries'] = {'flows/questions.py:select': ['flows/select_a.json']}
    ws.write_control(path, value, expected=expected)


def test_submit_history_snapshot_and_result(root, browser):
    import demiflow
    assert Path(demiflow.__file__).resolve().parents[1] == Path(__file__).resolve().parents[1]
    single_flow(root)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        start = page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)
        expect(start).to_be_enabled()
        start.click()
        expect(page.locator('.wb-run-confirmation')).to_contain_text('flows/select_a.json')
        assert runs.list_runs(root)['total'] == 0
        page.get_by_role('button', name='确认运行', exact=True).click()
        expect(page.locator('.wb-run-bar')).to_contain_text('已完成', timeout=90000)
        records = runs.list_runs(root)['runs']
        assert len(records) == 1
        result = runs.show(root, records[0]['run_id'], include_report=True)
        assert result['status'] == 'succeeded', result
        expect(page.get_by_label('运行记录', exact=True).locator('option')).to_have_count(1)
        expect(page.locator('.wb-history')).to_be_visible()
        page.get_by_role('button', name='本次参数与版本', exact=True).click()
        expect(page.get_by_label('封存业务参数')).to_contain_text('0.8')
        assert page.get_by_label('封存业务参数').inner_text() == json.dumps(
            result['original_parameters']['arguments'], ensure_ascii=False, indent=2)
        page.locator('.wb-monitor-tabs').get_by_role('button', name='执行图', exact=True).click()
        expect(page.locator('.wb-node').first).to_be_visible()
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-inspector')).not_to_be_visible()
        expect(page.locator('.wb-node-panel-tools')).not_to_be_visible()
        page.get_by_role('button', name='定义与接线', exact=True).click()
        expect(page.locator('.wb-source')).to_be_visible()
        page.get_by_role('button', name='日志', exact=True).click()
        expect(page.locator('.wb-run-detail')).to_be_visible()
        page.locator('.wb-monitor-tabs').get_by_role('button', name='输出结果', exact=True).click()
        expect(page.locator('.wb-output')).to_contain_text('2 行')
        screenshot(page, 'run-module-fixed-output')
        page.get_by_role('button', name='查看固定结果', exact=True).click()
        expect(page.locator('main')).to_contain_text('三角形')
        assert not errors, errors


def test_history_recovers_when_its_first_read_is_not_dispatched(root, browser):
    from test_forge_runs import prepared

    value = prepared(root)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        attempts = []

        def read_history(route):
            assert route.request.method == 'GET'
            if not attempts:
                # Exercise the real admission response before any history read begins.
                with app.workers:
                    response = route.fetch()
                assert response.status == 503
                assert response.json()['operation_started'] is False
                attempts.append(response.status)
                route.fulfill(response=response)
            else:
                attempts.append('retry')
                route.continue_()

        page.route('**/runs?*', read_history)
        page.locator('.function-nav').get_by_role('button', name='运行', exact=True).click()
        record = page.get_by_role('combobox',name='运行记录')
        expect(record).to_have_value(value['run_id'])
        assert attempts[:2] == [503, 'retry']
        assert runs.show(root, value['run_id'])['status'] == 'prepared'
        assert not errors


def test_stop_resume_and_historical_result_use_same_run(root, browser):
    import lance
    value = custom(root, '''
from operators.custom import slow
def run(*, source, target, other):
    d = data.read_lance(source['uri'], version=source['version'])
    first = d.write_lance(target['uri'], mode='create', return_receipt=True)
    second = d.map(slow, inputs={'text': 'question'}, output='waiting').write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''', operators='''def slow(text: str | None) -> str | None:
    total = 0
    for i in range(250000000):
        total = total + i
    return text
''', arguments={'source': {'kind': 'table', 'alias': 'candidates'},
               'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'first.lance'},
               'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'slow.lance'}},
        limits={'timeout_s': 180, 'cancel_grace_s': 1})
    run_id = value['run_id']
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        try:
            page.locator('.function-nav').get_by_role('button', name='运行', exact=True).click()
            expect(page.get_by_label('运行记录', exact=True)).to_have_value(run_id)
            page.get_by_role('button', name='开始这次运行', exact=True).click()
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                current = runs.show(root, run_id)
                if any(o['result'] for o in current['outputs']):
                    break
                assert current['status'] not in runs.TERMINAL, current
                page.wait_for_timeout(100)
            assert any(o['result'] for o in current['outputs'])
            page.get_by_role('button', name='停止', exact=True).click()
            page.get_by_role('button', name='确认停止', exact=True).click()
            expect(page.locator('.wb-run-bar')).to_contain_text('已停止', timeout=60000)
            previous = stopped(root, run_id)
            committed = next(o for o in previous['outputs'] if o['result'])
            expect(page.get_by_role('button', name='恢复运行', exact=True)).to_be_enabled(timeout=60000)
            screenshot(page, 'run-module-stopped-partial-result')
            page.get_by_role('button', name='恢复运行', exact=True).click()
            page.get_by_role('button', name='确认恢复', exact=True).click()
            expect(page.get_by_label('运行尝试', exact=True)).to_have_value('2', timeout=60000)
            expect(page.locator('.wb-run-bar')).to_contain_text('已完成', timeout=90000)
            resumed = runs.show(root, run_id, include_report=True)
            assert resumed['status'] == 'succeeded' and resumed['attempt'] == 2, resumed
            assert runs.list_runs(root)['total'] == 1
            assert lance.dataset(committed['uri']).version == committed['result']['ref']['lance_version']
            assert runs.show(root, run_id, attempt=1) == previous
            assert all(o['result']['ref']['row_count'] == 3 for o in resumed['outputs'])
            page.get_by_label('运行尝试', exact=True).select_option('1')
            expect(page.locator('.wb-run-bar')).to_contain_text('历史尝试 · 只读')
            # Selecting an old attempt must not replace the latest run summary.
            expect(page.get_by_label('运行记录', exact=True).locator('option')).to_contain_text('已完成')
            expect(page.get_by_label('运行记录', exact=True).locator('option')).not_to_contain_text('已停止')
            expect(page.locator('.wb-inspector')).not_to_be_visible()
            expect(page.locator('.wb-run-bar').get_by_role('button', name='停止', exact=True)).to_have_count(0)
            expect(page.get_by_role('button', name='恢复运行', exact=True)).to_have_count(0)
            for width in (1920, 1440, 1024, 390):
                page.set_viewport_size({'width': width, 'height': 1050})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), width
                screenshot(page, 'run-module-history-'+str(width))
            page.set_viewport_size({'width': 1440, 'height': 1050})
            page.locator('.wb-monitor-tabs').get_by_role('button', name='输出结果', exact=True).click()
            page.get_by_role('button', name='查看固定结果', exact=True).click()
            expect(page.locator('main')).to_contain_text('三角形')
            expect(page.locator('.wb-result-preview')).to_have_attribute('data-attempt', '1')
            page.get_by_role('button', name='在数据页打开', exact=True).click()
            assert page.evaluate('ForgeView.decodeView(location.hash).alias').endswith(':target:1')
            assert not errors, errors
        finally:
            current = runs.show(root, run_id)
            if current['status'] in runs.ACTIVE:
                runs.cancel(root, run_id, expected_attempt=current['attempt'])
                stopped(root, run_id)


def test_late_parameter_response_does_not_replace_selected_run(root, browser):
    from test_forge_runs import prepared, execute
    from test_forge_public_api import request
    single_flow(root)
    a = execute(root, prepared(root))
    path = root / 'flows/select_a.json'
    parameters = json.loads(path.read_text())
    expected = content_digest(parameters)
    parameters['arguments']['minimum'] = .9
    ws.write_control(path, parameters, expected=expected)
    b = execute(root, prepared(root))
    assert a['status'] == b['status'] == 'succeeded'
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        page.locator('.function-nav').get_by_role('button', name='运行', exact=True).click()
        page.get_by_label('运行记录',exact=True).select_option(a['run_id'])
        held = []
        page.route('**/run?*', lambda route: held.append(route) if 'report=1' in route.request.url
                   and a['run_id'] in route.request.url else route.continue_())
        page.get_by_role('button', name='本次参数与版本', exact=True).click()
        expect(page.locator('.wb-run-definition-panel')).to_contain_text('正在读取')
        deadline = time.monotonic()+10
        while not held and time.monotonic()<deadline:
            page.wait_for_timeout(50)
        assert held
        page.get_by_label('运行记录',exact=True).select_option(b['run_id'])
        page.get_by_role('button', name='本次参数与版本', exact=True).click()
        expect(page.get_by_label('封存业务参数')).to_contain_text('0.9')
        # Deliver the real old response late; never fabricate execution facts.
        status, response = request(app, '/api/run?run_id='+a['run_id']+'&attempt=1&report=1')
        assert status == 200
        for route in held:
            route.fulfill(status=200, content_type='application/json', body=json.dumps(response))
        expect(page.get_by_label('封存业务参数')).to_contain_text('0.9')
        expect(page.get_by_label('运行记录',exact=True)).to_have_value(b['run_id'])
        page.reload()
        expect(page.get_by_label('运行记录',exact=True)).to_have_value(b['run_id'])
        assert not errors, errors


def test_native_entry_without_create_schema_contract_does_not_create_run(root, browser):
    (root/'flows/native.py').write_text('''from demiflow import data
def run(*, directory):
    return data.from_items([{'value': 1}]).write_lance(directory + '/product.lance')
''')
    (root/'flows/native.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'directory': str(root/'never-created')}}))
    declare_existing_flow(root, 'flows/native.py:run', params='flows/native.json')
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.locator('.wb-flow-record[data-entry="flows/native.py:run"]').click()
        button = page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)
        expect(button).to_be_enabled()
        button.click()
        expect(page.locator('.wb-run-confirmation')).to_be_visible()
        page.get_by_role('button', name='确认运行', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('requires create')
        assert runs.list_runs(root)['total'] == 0
        assert not (root/'never-created').exists()
        assert not errors, errors
