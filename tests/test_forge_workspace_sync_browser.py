"""Real browser synchronization from external files/CLI; no host integration."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from test_forge_panel_browser import panel, open_panel, demo, isolated
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs import manager as runs


def test_saved_changes_refresh_overview_without_host_or_writes(panel):
    root, app, page, errors = panel
    page.add_init_script("""Object.defineProperty(document,'modelContext',{get(){throw Error('must not use host');}});
      Object.defineProperty(document,'oai',{get(){throw Error('must not use host');}});""")
    requests = []
    page.on('request', lambda r: requests.append((r.method, r.url)))
    open_panel(app, page)
    before = list((root / '.forge/inspections').iterdir())
    (root / 'README.md').write_text('# External edit\nSaved from the coding agent.')
    page.get_by_role('heading',name='External edit',exact=True).wait_for()
    page.get_by_text('Saved from the coding agent.',exact=True).wait_for()
    assert not page.get_by_role('button', name='与 Codex 讨论', exact=True).count()
    assert not page.get_by_role('button', name='选择此对象', exact=True).count()
    assert not page.locator('#host-capabilities').count()
    assert before == list((root / '.forge/inspections').iterdir())
    assert not (root / '.forge/runs').exists()
    assert not any(method == 'POST' for method, _ in requests)
    assert not errors




def test_external_source_change_marks_report_stale_and_preserves_draft(panel):
    root, app, page, errors = panel
    report = project_ops.inspect_flow(root, 'flows/questions.py:select', params='flows/select_a.json')['digest']
    assert project_ops.report_status(root, report)['status'] == 'checked'
    open_panel(app, page, dict(v=1, workspace=str(root), kind='inspection', report_digest=report))
    page.get_by_role('button',name='整图配置',exact=True).click()
    page.get_by_text('本次运行设置',exact=True).click()
    page.get_by_label('本次运行秒数上限',exact=True).fill('123')
    source=root/'operators/question_rules.py';source.write_text(source.read_text()+'\n# Saved outside the browser\n')
    page.locator('.wb-notice').filter(has_text='检查依据已变化').wait_for()
    page.locator('.wb-notice').filter(has_text='source_changed:operators/question_rules.py').wait_for()
    assert page.evaluate('ForgeView.decodeView(location.hash).report_digest')==report
    page.get_by_text('本次运行设置',exact=True).click()
    assert page.get_by_label('本次运行秒数上限',exact=True).input_value()=='123'
    page.get_by_role('button',name='重置本次运行设置',exact=True).click()
    assert page.get_by_label('本次运行秒数上限',exact=True).input_value()=='600'
    assert not errors


def test_hidden_pause_and_automatic_recovery_after_connection_loss(panel):
    root, app, page, errors = panel
    open_panel(app, page)
    checks = []
    page.on('request', lambda r: checks.append(r.url) if '/changes' in r.url else None)
    page.evaluate("Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_timeout(3500)
    assert not checks
    page.context.set_offline(True)
    page.evaluate("Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new Event('visibilitychange'))")
    page.locator('#sync-status.sync-error').wait_for()
    assert page.locator('#workspace-name').filter(has_text='question-experiments').count()
    (root / 'README.md').write_text('Restored connection sees saved changes.')
    page.context.set_offline(False)
    page.get_by_text('Restored connection sees saved changes.', exact=True).wait_for()
    assert not page.locator('#sync-status.sync-error').count()
    assert not errors


def test_external_inspection_appears_without_executing_from_panel(panel):
    root, app, page, errors = panel
    open_panel(app, page, dict(v=1, workspace=str(root), kind='flows'))
    source = root / 'operators/question_rules.py'
    source.write_text(source.read_text() + '\n# New experiment revision\n')
    result = subprocess.run([sys.executable, '-B', '-m', 'demiflow.cli', 'workbench', 'flow', 'inspect',
        str(root), 'flows/questions.py:select', '--params', 'flows/select_a.json', '--json'],
        capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr + result.stdout
    digest = json.loads(result.stdout)['digest']
    open_panel(app,page,dict(v=1,workspace=str(root),kind='inspection',report_digest=digest))
    pytest.importorskip('playwright.sync_api').expect(page.locator('.wb-node')).to_have_count(7)
    assert page.evaluate('ForgeView.decodeView(location.hash).report_digest')==digest
    assert not (root / '.forge/runs').exists()
    assert not errors


def test_slow_auto_refresh_cannot_navigate_back_or_accumulate_requests(panel):
    root, app, page, errors = panel
    open_panel(app, page)
    held = []
    page.route('**/api/workspaces/*/changes', lambda route: held.append(route))
    with page.expect_request('**/api/workspaces/*/changes'):
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    for _ in range(3):
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_timeout(3500)
    assert len(held) == 1
    page.get_by_role('button', name='管道', exact=True).click()
    page.locator('.wb-graph').wait_for()
    page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
    held[0].continue_()
    page.unroute('**/api/workspaces/*/changes')
    page.wait_for_timeout(3500)
    assert page.evaluate('ForgeView.decodeView(location.hash).kind') == 'flows'
    assert not page.locator('.error-box').count() and not errors




def test_manual_refresh_preserves_node_chosen_during_read(panel):
    root, app, page, errors = panel
    prepared = runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
    open_panel(app, page, dict(v=1, workspace=str(root), kind='run', run_id=prepared['run_id']))
    delayed = []
    def delay_report(route):
        if not delayed:delayed.append(route)
        else:route.continue_()
    page.route('**/api/workspaces/*/run-snapshot?*',delay_report)
    refresh = page.get_by_role('button', name='刷新当前视图', exact=True)
    with page.expect_request('**/run-snapshot?*'):
        refresh.click()
    node = page.locator('.wb-graph .wb-node').nth(1)
    chosen = node.get_attribute('data-node')
    node.click()
    delayed[0].continue_()
    pytest.importorskip('playwright.sync_api').expect(refresh).to_be_enabled(timeout=60000)
    assert page.locator('.wb-graph .wb-node.selected').get_attribute('data-node') == chosen
    assert page.evaluate('ForgeView.decodeView(location.hash).node_id') == chosen
    assert not page.locator('.error-box').count() and not errors


def test_external_cli_run_lifecycle_results_and_historical_position(panel, tmp_path):
    root, app, page, errors = panel
    open_panel(app, page, dict(v=1, workspace=str(root), kind='runs'))
    cli = [sys.executable, '-B', '-m', 'demiflow.cli', 'workbench', 'run']
    def command(*args):
        completed = subprocess.run(cli + list(map(str, args)) + ['--json'], capture_output=True, text=True, timeout=90)
        assert completed.returncode == 0, completed.stderr + completed.stdout
        return json.loads(completed.stdout)
    prepared = command('prepare', root, 'flows/questions.py:select', '--params', 'flows/select_a.json')
    run_id = prepared['run_id']
    page.locator('.wb-run-record[data-run=\"'+run_id+'\"]').wait_for()
    page.locator('.wb-run-record[data-run=\"'+run_id+'\"]').click()
    page.locator('.wb-run-bar > strong').filter(has_text=run_id).wait_for()
    # Starting through CLI, with no browser writes, must update the open run.
    command('start', root, run_id, '--expected', prepared['digest'])
    pytest.importorskip('playwright.sync_api').expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功',timeout=90000)
    page.get_by_role('button',name='输出结果',exact=True).click()
    page.get_by_text('固定版本 v1 · 2 行', exact=True).wait_for()
    output = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path)))
    output.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(output / 'sync-cli-run.png'), full_page=True)
    page.get_by_role('button', name='查看固定结果', exact=True).click()
    page.get_by_role('heading', name='单条 case · 0', exact=True).wait_for()
    page.screenshot(path=str(output / 'sync-fixed-result.png'), full_page=True)
    historical_url = page.url
    config = root / 'flows/select_b.json'
    # New experiment from external CLI appears beside the existing result.
    b = command('prepare', root, 'flows/questions.py:select', '--params', str(config.relative_to(root)))
    page.get_by_text('这页运行尚无已核验产物。', exact=True).wait_for(state='hidden')
    page.wait_for_timeout(3500)
    assert page.url == historical_url
    page.get_by_role('button', name='管道', exact=True).click()
    page.get_by_role('button',name='运行记录',exact=True).click()
    page.locator('.wb-run-record[data-run=\"'+b['run_id']+'\"]').wait_for()
    command('start', root, b['run_id'], '--expected', b['digest'])
    page.locator('.wb-run-record[data-run=\"'+b['run_id']+'\"]').filter(has_text='成功').wait_for(timeout=90000)
    assert not errors
