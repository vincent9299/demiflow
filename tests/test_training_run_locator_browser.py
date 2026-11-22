"""A sealed training node can be reopened without a report-node mapping."""
from playwright.sync_api import expect

from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_runs import base, root
from test_forge_workbench_browser import page_for
from test_training_workbench_run import run_registered_training


def test_unmapped_training_node_retains_its_sealed_locator_on_reload(root, tmp_path, monkeypatch, browser):
    run_id = run_registered_training(root, tmp_path, monkeypatch)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        page.locator('.function-nav [data-page="monitor"]').click()
        page.locator('.wb-history [data-entry="flows/train.py:run"]').click()
        page.wait_for_function("""run => {
            const ref = ForgeView.decodeView(location.hash);
            return ref?.run_id === run && ref?.plan_format === 'v4' && !!ref.occurrence_id;
        }""", arg=run_id)
        view = page.evaluate('() => ForgeView.decodeView(location.hash)')
        assert view['attempt'] == 1 and view['action_id'] and view['logical_node_id']
        assert view['dag_id'] and view['plan_id'] and 'plan_revision' in view
        assert 'node_id' not in view and 'report_digest' not in view
        page.reload()
        expect(page.locator('.wb-node.selected')).to_have_count(1)
        assert page.evaluate('() => ForgeView.decodeView(location.hash)') == view
        expect(page.get_by_text('视图字段或版本不合法', exact=True)).to_have_count(0)
        assert not errors
