"""Real host assembly and version-init regression checks in an owned /tmp domain.

The browser route faults below only hold a request or fail one transport read;
every successful table/version/sample response comes from the real HTTP service.
"""
import hashlib
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit
import urllib.error
import urllib.request

import pytest

from d19_fixtures import assert_tree_imports, owned_fixture
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workspace import demo

from demiflow.workbench.api import ASSETS


def test_index_resources_are_served_by_real_host(demo):
    assert_tree_imports()
    names = re.findall(r'(?:href|src)="(/[^\"]+)"', (ASSETS / 'index.html').read_text())
    assert '/workbench-presentation.js' in names and '/workbench-graph.css' in names
    with service(demo[0]) as app:
        for name in names:
            with urllib.request.urlopen(app.origin + name) as response:
                assert response.read() == (ASSETS / name[1:]).read_bytes(), name
                assert response.headers['X-Content-Type-Options'] == 'nosniff'
                assert 'unsafe-' not in response.headers['Content-Security-Policy']
                assert response.headers['Content-Type'].startswith(
                    'text/javascript' if name.endswith('.js') else 'text/css')
    with service(demo[0], api_only=True) as app:
        for name in ('/workbench-presentation.js', '/workbench-graph.css'):
            with pytest.raises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(app.origin + name)
            assert caught.value.code == 404


@pytest.mark.parametrize('owned_fixture', [{}], indirect=True)
def test_version_panel_waits_for_fixed_source_and_retries(owned_fixture, browser):
    from playwright.sync_api import expect

    fx = owned_fixture
    facts = fx.version_facts
    control = Path(facts['control_path'])
    before = hashlib.sha256(control.read_bytes()).hexdigest()
    held = []
    failed_reads = []

    def hold_initial_table(route):
        query = parse_qs(urlsplit(route.request.url).query)
        if query.get('alias') == [facts['alias']] and 'version' not in query and not held:
            held.append(route)
        else:
            route.continue_()

    def fail_first_version_read(route):
        if not failed_reads:
            failed_reads.append(route.request.url)
            route.abort('failed')
        else:
            route.continue_()

    with fx.open_page(browser) as (page, log):
        assert page.evaluate('typeof ForgeWorkbenchPresentation.buildGraphModel') == 'function'
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        page.locator('details.bound-versions').wait_for(state='visible')
        page.route('**/table?*', hold_initial_table)
        page.route('**/table-versions?*', fail_first_version_read)
        page.get_by_label('当前数据表', exact=True).select_option(facts['alias'])
        versions = page.locator('details.bound-versions')
        expect(versions).to_have_count(1)
        expect(versions).to_be_hidden()
        # Even a programmatic early toggle must not dereference the absent fixed
        # source or latch success. Releasing the real table read completes init.
        versions.evaluate('(element) => { element.open = true; }')
        page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
        assert held and not failed_reads
        assert not page.locator('.load-failure').count()
        page.unroute('**/table?*', hold_initial_table)
        held[0].continue_()
        expect(versions).to_be_visible()
        expect(versions.locator('.load-failure')).to_be_visible()
        assert len(failed_reads) == 1

        # Closing/reopening after failure retries; successful responses alone
        # latch the loaded state. No hard-coded short wait for native workers.
        versions.locator('summary').first.click()
        with page.expect_response(lambda response: '/table-versions?' in response.url
                                  and response.status == 200, timeout=65000):
            versions.locator('summary').first.click()
        historical = versions.locator('button.version-row[data-version="1"]')
        historical.wait_for()
        expect(versions.locator('.load-failure')).to_have_count(0)
        expect(page.locator('.data-source-context')).to_contain_text('3 行 · 6 字段')

        def historical_sample(response):
            parsed = urlsplit(response.url)
            query = parse_qs(parsed.query)
            return parsed.path.endswith('/sample') and query.get('version') == ['1'] and 'row_id' not in query

        with page.expect_response(historical_sample, timeout=65000) as sampled:
            historical.click()
        response = sampled.value
        assert response.status == 200
        sample = response.json()
        assert sample['binding']['digest'] == facts['versions'][1]['binding']
        assert sample['row_count'] == 2
        assert [row['case_id'] for row in sample['rows']] == ['v1-c01', 'v1-c02']
        expect(page.locator('.data-source-context')).to_contain_text('2 行 · 5 字段')
        expect(page.locator('.version-pinned')).to_contain_text('历史固定 v1')
        assert not [request for request in log['requests'] if request['method'] != 'GET']
        assert hashlib.sha256(control.read_bytes()).hexdigest() == before
        fx.record('host-version-init-and-switch', log)
        fx.shot(page, 'host-version-one-selected')
