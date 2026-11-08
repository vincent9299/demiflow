"""Panel navigation and races against real HTTP and pinned Lance fixtures."""
import json
import os
from pathlib import Path
import threading
from urllib.parse import parse_qs, urlsplit

import pytest

from test_forge_workspace import demo, isolated
from demiflow.workbench.projects import workspace as api
from demiflow.workbench.api import WorkspaceServer


@pytest.fixture
def panel(isolated,tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))
    playwright = pytest.importorskip("playwright.sync_api")
    executable = Path(os.environ.get("FORGE_CHROMIUM", ""))
    if not executable.is_file():
        pytest.skip("Set FORGE_CHROMIUM to an installed Chromium binary")
    app = WorkspaceServer(isolated)
    thread = threading.Thread(target=app.serve_forever, daemon=True)
    thread.start()
    errors = []
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page(viewport={"width": 1500, "height": 1100})
            page.set_default_timeout(60000)
            page.on("pageerror", lambda error: errors.append(str(error)))
            yield isolated, app, page, errors
            browser.close()
    finally:
        app.shutdown(); app.server_close(); thread.join(timeout=3)


def open_panel(app, page, target=None):
    from urllib.parse import urlencode

    query = {"token": app.token}
    if target:
        query["view"] = json.dumps(target)
    # A new URL entry must not reuse readiness from the previous document.
    page.goto('about:blank')
    page.goto(app.origin + "/#" + urlencode(query))
    page.locator('#sync-status').filter(has_text='最近同步').or_(page.locator('main > .error-box')).first.wait_for()


def test_case_deep_link_reload_history_and_invalid_node(panel):
    root, app, page, errors = panel
    description = api.describe_table(root, "candidates")
    row = api.sample_table(root, "candidates", offset=1, limit=1)["rows"][0]
    reference = dict(v=1, workspace=str(root), kind="case", alias="candidates",
                     binding_digest=description["binding"]["digest"], row_id=row["_rowid"])
    open_panel(app, page, reference)
    page.get_by_role("heading", name="单条 case · " + row["_rowid"]).wait_for()
    assert app.token not in page.url
    page.reload()
    page.get_by_role("heading", name="单条 case · " + row["_rowid"]).wait_for()
    page.get_by_role("button", name="Readme", exact=True).click()
    page.locator("#workspace-name").filter(has_text="question-experiments").wait_for()
    page.go_back()
    page.get_by_role("heading", name="单条 case · " + row["_rowid"]).wait_for()
    page.go_forward()
    page.locator("#workspace-name").filter(has_text="question-experiments").wait_for()
    report = api.workspace_context(root)["reports"][0]["digest"]
    open_panel(app, page, dict(v=1, workspace=str(root), kind="inspection", report_digest=report, node_id="absent"))
    page.get_by_text("定位节点不属于本图", exact=True).first.wait_for()
    assert page.locator(".wb-node.selected").count() == 0
    page.get_by_role('button', name='刷新当前视图', exact=True).click()
    pytest.importorskip('playwright.sync_api').expect(
        page.get_by_role('button', name='刷新当前视图', exact=True)
    ).to_be_enabled(timeout=60000)
    page.get_by_text("定位节点不属于本图", exact=True).first.wait_for()
    assert page.locator(".wb-node.selected").count() == 0
    assert not errors, errors


def test_delayed_case_does_not_replace_the_last_selection(panel):
    _, app, page, errors = panel
    open_panel(app, page)
    page.get_by_role("button", name="数据", exact=True).click()
    page.get_by_role("heading", name="单条 case · 8589934592").wait_for()
    delayed = []

    def hold_second(route):
        query = parse_qs(urlsplit(route.request.url).query)
        if query.get("row_id") == ["8589934593"]:
            delayed.append(route)
        else:
            route.continue_()

    page.route("**/api/workspaces/*/sample?**", hold_second)
    rows = page.locator(".case-record-selector button")
    rows.nth(1).click()
    page.get_by_text('正在读取 Case…', exact=True).wait_for()
    rows.nth(2).click()
    page.get_by_role("heading", name="单条 case · 8589934594").wait_for()
    assert len(delayed) == 1
    with page.expect_response(lambda response: "row_id=8589934593" in response.url):
        delayed[0].continue_()
    page.wait_for_timeout(150)
    assert page.get_by_role("heading", name="单条 case · 8589934594").count() == 1
    assert page.evaluate("ForgeView.decodeView(location.hash).row_id") == "8589934594"
    assert not errors, errors


def test_media_rejects_alias_replacement_and_old_source_is_explicit(panel):
    root, app, page, errors = panel
    open_panel(app, page)
    page.get_by_role("button", name="数据", exact=True).click()
    page.get_by_role("heading", name="单条 case · 8589934592").wait_for()
    replacement = json.loads((root / "tables/comparison_fixture.json").read_text())
    replacement["alias"] = "candidates"
    (root / "tables/candidates.json").write_text(json.dumps(replacement))
    page.get_by_role('tab', name='image_uri', exact=True).first.click()
    with page.expect_response(lambda response: '/media?' in response.url) as rejected:
        page.get_by_role("button", name="查看本地图片", exact=True).first.click()
    assert rejected.value.status == 409 and rejected.value.json()['code'] == 'conflict'
    page.locator('.error-box').filter(has_text='Preview binding changed').wait_for()
    assert page.locator("img.case-image").count() == 0
    report=api.workspace_context(root)['reports'][0]['digest']
    open_panel(app,page,dict(v=1,workspace=str(root),kind='inspection',report_digest=report))
    page.locator('.wb-node').filter(has_text='normalize_question').click()
    page.locator('.wb-source-body').filter(has_text='def normalize_question').wait_for()
    source=root/'operators/question_rules.py'
    source.write_text(source.read_text()+"\n# another working tree revision\n")
    with page.expect_response(lambda response:'/graph-source?' in response.url) as rejected:
        page.get_by_role('button',name='实现',exact=True).click()
    assert rejected.value.status==409 and rejected.value.json()['code']=='conflict'
    page.locator('.wb-source-body .error-box').filter(has_text='Source changed since inspection').wait_for()
    assert not errors, errors


def test_selecting_case_during_initial_load_wins(panel):
    _, app, page, errors = panel
    open_panel(app, page)
    delayed = []

    def hold_initial(route):
        query = parse_qs(urlsplit(route.request.url).query)
        if query.get('row_id') == ['8589934592']:
            delayed.append(route)
        else:
            route.continue_()

    page.route('**/api/workspaces/*/sample?**', hold_initial)
    page.get_by_role('button', name='数据', exact=True).click()
    page.get_by_text('正在读取 Case…', exact=True).wait_for()
    page.locator('#case-browser table tbody tr').nth(1).click()
    page.get_by_role('heading', name='单条 case · 8589934593').wait_for()
    assert len(delayed) == 1
    delayed[0].continue_()
    page.wait_for_timeout(150)
    assert page.evaluate('ForgeView.decodeView(location.hash).row_id') == '8589934593'
    assert not errors, errors


def test_view_protocol_rejects_ambiguous_or_unbounded_targets(panel):
    root, app, page, errors = panel
    open_panel(app, page)
    result = page.evaluate("""root => {
      const base={v:1,workspace:root,kind:'case',alias:'candidates',binding_digest:'a'.repeat(64),row_id:'9007199254740993'};
      const valid=ForgeView.decodeView(ForgeView.encodeView(base));
      const invalid=[{...base,row_id:9007199254740993},{...base,row_id:'18446744073709551616'},
        {...base,v:2},{...base,token:'secret'},{...base,columns:Array(17).fill('x')},
        {...base,workspace:'relative'},{...base,kind:'execute'}];
      return {row:valid.row_id,rejected:invalid.map(v=>{try{ForgeView.validateView(v);return false;}catch{return true;}}),
        duplicateRejected:(()=>{try{ForgeView.decodeView(ForgeView.encodeView(base)+'&view=%7B%7D');return false;}catch{return true;}})()};
    }""", str(root))
    assert result["row"] == "9007199254740993" and all(result["rejected"])
    assert result['duplicateRejected']
    assert not errors, errors


def test_manual_refresh_joins_visibility_refresh(panel):
    root, app, page, errors = panel
    open_panel(app, page, dict(v=1, workspace=str(root), kind='runs'))
    reference=page.evaluate('ForgeView.decodeView(location.hash)')
    assert reference['kind']=='flows'
    pytest.importorskip('playwright.sync_api').expect(page.locator('.wb-history')).to_be_visible()
    held = []
    page.route('**/api/workspaces/*/changes', lambda route: held.append(route) if not held else route.continue_())
    with page.expect_request('**/api/workspaces/*/changes'):
        page.evaluate("() => {Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new Event('visibilitychange'));}")
    button = page.get_by_role('button', name='刷新当前视图', exact=True)
    button.click()
    assert len(held) == 1
    assert not page.locator('.error-box').count()
    held[0].continue_()
    pytest.importorskip('playwright.sync_api').expect(button).to_be_enabled(timeout=60000)
    page.locator('.wb-graph').wait_for()
    assert page.evaluate('ForgeView.decodeView(location.hash)') == reference
    pytest.importorskip('playwright.sync_api').expect(page.locator('.wb-history')).to_be_visible()
    assert not page.locator('.error-box').count() and not errors


def test_refresh_recovers_after_connection_failure(panel):
    root, app, page, errors = panel
    open_panel(app, page, dict(v=1, workspace=str(root), kind='runs'))
    button = page.get_by_role('button', name='刷新当前视图', exact=True)
    page.context.set_offline(True)
    button.click()
    page.get_by_text('无法连接工作区服务，请确认服务和连接可用后重试。', exact=True).wait_for()
    page.context.set_offline(False)
    button.click()
    pytest.importorskip('playwright.sync_api').expect(button).to_be_enabled(timeout=60000)
    page.locator('.wb-graph').wait_for()
    assert not page.locator('.error-box').count() and not errors
