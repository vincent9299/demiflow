"""Actual browser execution through a transparent loopback TCP forward."""
from _forge_project_fixtures import declare_existing_flow
import json
import os
from pathlib import Path
import select
import shutil
import socket
import socketserver
import threading

import pytest

from test_forge_runs import base, root, wait
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.api import WorkspaceServer


@pytest.fixture(autouse=True)
def private_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


def test_browser_prepare_run_reconnect_compare_and_continue(root, tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    expect = playwright.expect
    executable = Path(os.environ.get('FORGE_CHROMIUM', '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'))
    if not executable.is_file():
        pytest.skip('Set FORGE_CHROMIUM to an installed Chromium binary')
    if os.environ.get('FORGE_STAGE3_DEMO_ROOT'):
        destination = Path(os.environ['FORGE_STAGE3_DEMO_ROOT'])
        shutil.copytree(root, destination)
        root = destination
    (root / 'flows/continue_result.py').write_text('''from demiflow import data

def take_one(*, source, target):
    """从固定版本输入中取一个 case，另存为新的实验结果。"""
    return data.read_lance(source['uri'], version=source['version']).limit(1).write_lance(target['uri'], mode='create', return_receipt=True)
''')
    (root / 'flows/continue_result.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1', 'arguments': {
            'source': {'kind': 'table', 'alias': 'candidates'},
            'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'continued.lance'}}}))
    declare_existing_flow(root, 'flows/continue_result.py:take_one', params='flows/continue_result.json')
    # Each authored variant has its own entry and saved parameter binding.
    for variant in ('a', 'b'):
        document = json.loads((root / f'flows/select_{variant}.json').read_text())
        document['arguments'].pop('minimum')
        path = f'flows/config_{variant}.json'
        (root / path).write_text(json.dumps(document))
        declare_existing_flow(root, f'flows/questions.py:select_{variant}', params=path)
    output_dir = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path)))
    output_dir.mkdir(parents=True, exist_ok=True)
    server = WorkspaceServer(root, enable_runs=True)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            with socket.create_connection(server.server_address, timeout=60) as upstream:
                peers = {self.request: upstream, upstream: self.request}
                while True:
                    readable, _, _ = select.select(list(peers), [], [], 60)
                    if not readable:
                        return
                    for source in readable:
                        chunk = source.recv(65536)
                        if not chunk:
                            return
                        peers[source].sendall(chunk)

    class ForwardServer(socketserver.ThreadingTCPServer):
        daemon_threads = True
    forward = ForwardServer(('127.0.0.1', 0), Forward)
    forward_thread = threading.Thread(target=forward.serve_forever, daemon=True)
    forward_thread.start()
    origin = 'http://localhost:' + str(forward.server_address[1])
    errors = []
    evidence = {'workspace': str(root), 'browser_origin': origin, 'server_origin': server.origin}
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = browser.new_context(viewport={'width': 1600, 'height': 1050}, device_scale_factor=1)
            page = context.new_page()
            page.set_default_timeout(90000)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(origin + '/#token=' + server.token)
            page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
            def nav(label):
                page.get_by_role('navigation', name='项目功能').get_by_role('button', name=label, exact=True).click()
            def select_entry(entry):
                nav('管道')
                page.locator('.wb-flow-record[data-entry="' + entry + '"]').click()
                expect(page.locator('.wb-node').first).to_be_visible(timeout=65000)
            def submit():
                page.locator('.wb-actions').get_by_role('button', name='运行', exact=True).click()
                page.get_by_role('button', name='确认运行', exact=True).click()
            def run(variant, paused=False):
                select_entry('flows/questions.py:select_' + variant)
                held = []
                if paused:
                    page.route('**/run-start', lambda route: held.append(route))
                with page.expect_response('**/run-prepare', timeout=90000) as prepared:
                    submit()
                response = prepared.value
                assert response.ok, response.text()
                identity = response.json()['run_id']
                page.wait_for_function('id=>ForgeView.decodeView(location.hash)?.run_id===id', arg=identity)
                expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('待启动' if paused else '已完成',timeout=90000)
                return identity, held
            def pick(identity):
                nav('运行')
                entry=runs.show(root,identity)['entry']
                page.locator(f'.wb-run-flow[data-entry="{entry}"]').click()
                page.get_by_label('运行记录',exact=True).select_option(identity)
                expect(page.locator('.wb-run-bar')).to_contain_text(identity[-10:])
            def output():
                page.get_by_role('button',name='输出结果',exact=True).click()
            def get(endpoint, **params):
                response = context.request.get(origin + '/api/' + endpoint, params=params,
                                               headers={'X-Forge-Token': server.token})
                assert response.ok, response.status
                return response
            a, held = run('a', paused=True)
            assert runs.show(root,a)['status']=='prepared'
            assert not Path(runs.show(root,a)['outputs'][0]['uri']).exists()
            page.wait_for_function('() => ForgeView.decodeView(location.hash)?.kind==="run"')
            for _ in range(100):
                if held:break
                page.wait_for_timeout(20)
            assert held
            page.screenshot(path=str(output_dir/'01-prepared-run.png'),full_page=True)
            started=held[0].fetch(timeout=90000);assert started.ok,started.text()
            held[0].fulfill(response=started)
            context.close()
            context=browser.new_context(viewport={'width':1600,'height':1050})
            page=context.new_page();page.set_default_timeout(90000)
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto(origin+'/#token='+server.token)
            pick(a)
            expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('已完成',timeout=90000)
            output();page.get_by_role('button',name='查看固定结果',exact=True).click()
            expect(page.locator('.wb-result-preview tbody tr')).to_have_count(2)
            def sample(alias, case_id):
                binding = get('table', alias=alias).json()['binding']['digest']
                matched = get('sample', alias=alias, match=json.dumps({'field':'case_id','value':case_id}),
                              expected_binding=binding).json()
                assert matched['binding']['digest'] == binding
                return binding, matched['rows']
            binding, candidates = sample('candidates','c01')
            assert len(candidates) == 1
            image = get('media', alias='candidates', field='image_uri', row_id=candidates[0]['_rowid'],
                        expected_binding=binding)
            assert image.headers['x-forge-binding'] == binding
            assert image.headers['content-type'] == 'image/jpeg' and image.body().startswith(b'\xff\xd8')
            page.screenshot(path=str(output_dir/'02-result-and-input.png'),full_page=True)
            b, _ = run('b')
            page.evaluate("([left,right])=>{const {v,workspace}=ForgeView.decodeView(location.hash);location.hash=ForgeView.encodeView({v,workspace,kind:'run_compare',left,right})}",[a,b])
            page.get_by_role('heading',name='两次运行对比',exact=True).wait_for()
            assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='run_compare'
            page.reload();page.get_by_role('heading',name='两次运行对比',exact=True).wait_for()
            assert '2 行' in page.locator('.wb-results').inner_text() and '1 行' in page.locator('.wb-results').inner_text()
            page.screenshot(path=str(output_dir/'03-actual-ab-results.png'),full_page=True)
            pick(a);output();page.get_by_role('button',name='查看固定结果',exact=True).click()
            alias_b = 'run:' + b + ':target:1'
            assert [len(sample(alias_b, case)[1]) for case in ('c01','c02')] == [1,0]
            page.get_by_role('button',name='作为输入继续加工',exact=True).click()
            select_entry('flows/continue_result.py:take_one')
            page.locator('.wb-more > summary').click()
            page.get_by_role('button',name='整图配置',exact=True).click()
            page.get_by_text('本次运行设置',exact=True).click()
            page.get_by_label('本次输入 source',exact=True).select_option('run:'+a+':target:1')
            submit()
            expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('已完成',timeout=90000)
            page.wait_for_function('prior=>{const id=ForgeView.decodeView(location.hash)?.run_id;return id&&!prior.includes(id)}',arg=[a,b])
            c=page.evaluate('ForgeView.decodeView(location.hash).run_id')
            continued=runs.show(root,c,include_report=True)
            assert continued['report']['binding']['tables']['run:'+a+':target:1']['reference']['ref']==runs.show(root,a)['outputs'][0]['result']['ref']
            selected = page.locator('.wb-node').last
            selected.click()
            label = selected.get_attribute('aria-label')
            reference = page.evaluate('ForgeView.decodeView(location.hash)')
            assert reference['run_id'] == c
            assert all(reference.get(key) for key in ('node_id', 'action_id', 'occurrence_id', 'plan_id'))
            page.reload()
            expect(page.locator('.wb-node[aria-pressed="true"]')).to_have_attribute('aria-label', label)
            assert page.evaluate('ForgeView.decodeView(location.hash)') == reference
            output();page.get_by_label('结果别名 target',exact=True).fill('continued_case')
            page.get_by_role('button',name='保留为输入表',exact=True).click()
            page.get_by_text('已保留固定版本引用：continued_case',exact=True).wait_for()
            page.reload()
            retained = get('table', alias='continued_case').json()
            assert retained['binding']['reference']['ref'] == runs.show(root,c)['outputs'][0]['result']['ref']
            assert not errors,errors
            evidence.update(runs={'a':a,'b':b,'continued':c},browser_errors=errors,
                checks=['prepare has not executed while start delivery held','actual TCP forward',
                        'browser disconnect','native A/B results','fixed input and Case image',
                        'explicit business key/missing match','fixed result continuation','node URL reload','result alias'])
            browser.close()
        (output_dir / 'browser-evidence.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    finally:
        forward.shutdown()
        forward.server_close()
        server.shutdown()
        server.server_close()
        forward_thread.join(timeout=3)
        server_thread.join(timeout=3)


def test_prepared_view_observes_external_start_and_partial_cancel(root, tmp_path):
    from test_forge_run_faults import _slow
    playwright = pytest.importorskip('playwright.sync_api')
    expect = playwright.expect
    executable = Path(os.environ.get('FORGE_CHROMIUM', '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'))
    if not executable.is_file():
        pytest.skip('Set FORGE_CHROMIUM to an installed Chromium binary')
    value = _slow(root)
    server = WorkspaceServer(root, enable_runs=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage'])
            page = browser.new_page(viewport={'width': 1600, 'height': 1050})
            page.set_default_timeout(90000)
            page.goto(server.origin + '/#token=' + server.token)
            page.get_by_role('navigation',name='项目功能').get_by_role('button',name='运行',exact=True).click()
            page.locator(f'.wb-run-flow[data-entry="{value["entry"]}"]').click()
            expect(page.get_by_label('运行记录',exact=True)).to_have_value(value['run_id'])
            page.get_by_role('button',name='开始这次运行',exact=True).wait_for()
            runs.start(root,value['run_id'],expected=value['digest'])
            page.locator('.wb-partial').wait_for()
            page.get_by_role('button',name='输出结果',exact=True).click()
            alias=page.get_by_label('结果别名 target',exact=True);alias.fill('keep_partial')
            page.get_by_role('button',name='停止',exact=True).click()
            page.get_by_role('button', name='确认停止', exact=True).click()
            alias.fill('keep_after_cancel')
            expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('已停止',timeout=90000)
            assert page.get_by_label('结果别名 target',exact=True).input_value()=='keep_after_cancel'
            page.locator('.wb-monitor-tabs').get_by_role('button', name='执行图', exact=True).click()
            expect(page.locator('.wb-partial')).to_contain_text('部分输出已提交')
            assert sum(o['result'] is not None for o in runs.show(root,value['run_id'])['outputs'])==1
            output=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/'05-cancelled-partial-result.png'),full_page=True)
            browser.close()
    finally:
        if runs.show(root,value['run_id'])['status'] in runs.ACTIVE:
            runs.cancel(root,value['run_id']);wait(root,value['run_id'],timeout=25)
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
