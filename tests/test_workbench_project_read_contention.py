"""Project navigation survives transient read admission without replaying writes."""
import json
from urllib.parse import urlencode

from playwright.sync_api import expect

from test_forge_panel_browser import panel, demo, isolated


def test_project_open_recovers_after_real_read_lane_contention(panel):
    root, app, page, errors = panel
    requests = []
    page.on('request', lambda request: requests.append((request.method, request.url)))
    page.add_init_script('''
      window.projectLoadFailed = false;
      new MutationObserver(() => {
        if ([...document.querySelectorAll('.load-failure')].some(
            node => node.textContent.includes('项目读取失败')))
          window.projectLoadFailed = true;
      }).observe(document, {childList: true, subtree: true});
    ''')
    target = dict(v=1, workspace=str(root), kind='flows')
    url = app.origin + '/#' + urlencode(dict(token=app.token, view=json.dumps(target)))

    # Occupy the real server lane until its admission deadline rejects context.
    assert app.workers.acquire(timeout=5)
    try:
        with page.expect_response(lambda response: response.url.endswith('/context')) as refused:
            page.goto(url)
        assert refused.value.status == 503
        assert refused.value.json()['operation_started'] is False
    finally:
        app.workers.release()

    # No reload or retry click: the original project/route must finish opening.
    expect(page.locator('.wb-node').first).to_be_visible(timeout=10000)
    expect(page.locator('.load-failure')).to_have_count(0)
    assert not page.evaluate('window.projectLoadFailed'), 'Transient admission rejected the whole project'
    assert page.evaluate('ForgeView.decodeView(location.hash).kind') == 'flows'
    assert len([url for method, url in requests if url.endswith('/context')]) >= 2
    assert not [url for method, url in requests if method != 'GET']
    assert not errors
