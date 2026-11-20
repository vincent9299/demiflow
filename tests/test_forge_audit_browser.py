"""The panel displays shared audit facts without taking over conversations."""
import json

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from test_forge_panel_browser import panel, open_panel, demo, isolated


def test_scope_violation_syncs_and_manual_verification_preserves_baseline(panel, tmp_path):
    root, app, page, errors = panel
    project_ops.begin_change_scope(root, writes=[], expected=content_digest(ws.load_workspace(root)[1]))
    policy = ws.audit_status(root)['policy_digest']
    before = ws.load_workspace(root)[1]['audit']['change_scope']
    project_ops.audit_check(root, kind='scope', expected_policy=policy)
    open_panel(app, page)
    page.locator('#workspace-status > summary').click()
    page.locator('#audit-panel summary').filter(has_text='范围内').wait_for()
    (root / 'operators/unexpected.py').write_text('# saved outside permitted range')
    project_ops.audit_check(root, kind='scope', expected_policy=policy)
    page.locator('#audit-panel summary').filter(has_text='存在越界修改').wait_for()
    assert page.locator('#workspace-status').get_attribute('data-state') == 'attention'
    assert page.locator('#sync-status .sync-caption').inner_text() == '检查需关注'
    page.locator('#audit-panel li').filter(has_text='operators/unexpected.py').wait_for()
    assert page.get_by_text('与 Codex 讨论', exact=True).count() == 0
    before_reports = ws.list_reports(root)['total']
    with page.expect_response('**/audit-check') as checked:
        page.get_by_role('button', name='核验修改范围', exact=True).click()
    assert checked.value.ok
    page.locator('#audit-panel summary').filter(has_text='存在越界修改').wait_for()
    page.get_by_role('button', name='核验修改范围', exact=True).wait_for(state='visible')
    assert ws.load_workspace(root)[1]['audit']['change_scope'] == before
    assert ws.list_reports(root)['total'] == before_reports
    page.screenshot(path=str(tmp_path / 'scope-violation.png'), full_page=True)
    assert not errors


def test_expired_audit_is_not_kept_green_without_new_revision(panel):
    root, app, page, errors = panel
    project_ops.begin_change_scope(root, writes=[], expected=content_digest(ws.load_workspace(root)[1]))
    policy = ws.audit_status(root)['policy_digest']
    project_ops.audit_check(root, kind='scope', expected_policy=policy, full_interval=1)
    open_panel(app, page)
    page.locator('#workspace-status > summary').click()
    page.locator('#audit-panel summary').filter(has_text='核验已超期').wait_for()
    assert page.locator('#workspace-status').get_attribute('data-state') == 'attention'
    assert not errors
