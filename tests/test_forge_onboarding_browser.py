"""Declared presentation and per-entry drafts through real workspace endpoints."""
from test_forge_panel_browser import panel, open_panel, demo, isolated
from demiflow.workbench.projects import workspace as ws


def test_each_registered_entry_keeps_its_own_unsent_inputs(panel):
    root, app, page, errors = panel
    (root / 'flows/alternate.py').write_text((root / 'flows/questions.py').read_text().replace('def select(', 'def other('))
    ws.register_flow(root, 'flows/alternate.py:other', params='flows/select_b.json')
    open_panel(app, page, dict(v=1, workspace=str(root), kind='flows', entry='flows/questions.py:select', params='flows/select_a.json'))
    page.get_by_role('button',name='整图配置',exact=True).click()
    page.get_by_text('本次运行设置', exact=True).click()
    page.get_by_label('本次输入 candidates', exact=True).wait_for()
    page.get_by_label('本次运行秒数上限').fill('123')
    page.get_by_label('流程',exact=True).select_option('flows/alternate.py:other')
    page.get_by_role('button',name='整图配置',exact=True).click()
    page.get_by_text('本次运行设置', exact=True).click()
    assert page.get_by_label('配置',exact=True).input_value() == 'flows/select_b.json'
    page.get_by_label('本次输入 candidates', exact=True).wait_for()
    page.get_by_label('本次运行秒数上限').fill('234')
    page.get_by_label('流程',exact=True).select_option('flows/questions.py:select')
    page.get_by_role('button',name='整图配置',exact=True).click()
    page.get_by_text('本次运行设置', exact=True).click()
    assert page.get_by_label('本次运行秒数上限').input_value() == '123'
    assert page.get_by_label('配置',exact=True).input_value() == 'flows/select_a.json'
    page.get_by_label('流程',exact=True).select_option('flows/alternate.py:other')
    page.get_by_role('button',name='整图配置',exact=True).click()
    page.get_by_text('本次运行设置', exact=True).click()
    assert page.get_by_label('本次运行秒数上限').input_value() == '234'
    assert not errors


def test_declared_case_text_and_historical_origin_are_visible(panel):
    root, app, page, errors = panel
    table = ws.describe_table(root, 'candidates')
    ws.set_table_view(root, 'candidates', expected=table['digest'],
        display={'title': '固定历史题目', 'fields': [{'path': ['question'], 'label': '题目正文', 'kind': 'text'}]},
        provenance={'kind': 'historical', 'note': '原结果只读接入，尚无新执行证据'})
    open_panel(app, page, dict(v=1, workspace=str(root), kind='table', alias='candidates', binding_digest=table['binding']['digest']))
    page.get_by_role('heading', name='固定历史题目', exact=True).wait_for()
    page.get_by_role('tab',name='题目正文',exact=True).wait_for()
    page.get_by_text('历史结果 · 原结果只读接入，尚无新执行证据', exact=True).wait_for()
    assert not errors


def test_historical_attempt_survives_node_navigation_and_reload(panel):
    from demiflow.workbench.runs import manager as runs
    root, app, page, errors = panel
    run = runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
    control = runs.journal(root, run['run_id'])
    assert control.transition({'prepared'}, 'interrupted')
    old = runs.show(root, run['run_id'])
    control.begin_attempt(expected_attempt=1, expected_revision=old['state']['revision'])
    open_panel(app, page, dict(v=1, workspace=str(root), kind='run', run_id=run['run_id'], attempt=1))
    page.get_by_text('历史尝试 · 只读', exact=True).wait_for()
    page.locator('.wb-graph .wb-node').last.click()
    reference = page.evaluate('ForgeView.decodeView(location.hash)')
    assert reference['attempt'] == 1 and reference['run_id'] == run['run_id']
    assert reference['node_id']
    page.reload()
    page.get_by_text('历史尝试 · 只读', exact=True).wait_for()
    assert page.get_by_label('运行尝试', exact=True).input_value() == '1'
    assert page.locator('.wb-graph .wb-node.selected').get_attribute('data-node') == reference['node_id']
    assert not errors
