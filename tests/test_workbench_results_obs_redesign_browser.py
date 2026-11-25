"""Real-browser verification of minimalist Results tab and clean Observation panel."""
from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_runs import base, root, wait, custom
from test_forge_public_api import service
from test_forge_markdown_reading import browser
from test_forge_workbench_browser import page_for


def test_results_tab_minimal_auto_preview_no_blank_rows_and_anomaly_highlight(root, browser):
    long_json = json.dumps({'checks': {'validity': 'pass', 'notes': 'x' * 1800}})
    value = custom(
        root,
        '''
from operators.custom import mark_error, make_long_json
def run(*, source, author, delivery):
    d = (
        data.read_lance(source['uri'], version=source['version'])
        .map(mark_error, inputs={'text': 'question'}, output='author_error')
        .map(make_long_json, inputs={'text': 'question'}, output='long_payload_json')
    )
    r1 = d.write_lance(author['uri'], mode='create', return_receipt=True)
    r2 = d.write_lance(delivery['uri'], mode='create', return_receipt=True)
    return r1, r2
''',
        operators=f'''
def mark_error(text: str | None) -> str | None:
    return '400 invalid temperature' if text and '三角' in text else None

def make_long_json(text: str | None) -> str:
    return {long_json!r}
''',
        arguments={
            'source': {'kind': 'table', 'alias': 'candidates'},
            'author': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'outputs__author.lance'},
            'delivery': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'outputs__delivery.lance'},
        },
        limits={'timeout_s': 120},
    )
    run_id = value['run_id']
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.locator('.wb-actions').get_by_role('button', name='运行记录', exact=True).click()
        expect(page.get_by_label('运行记录', exact=True)).to_have_value(run_id)
        page.get_by_role('button', name='开始这次运行', exact=True).click()
        expect(page.locator('.wb-run-bar')).to_contain_text('已完成', timeout=60000)

        # 1. Switch to 「输出结果」: auto-selects final output (delivery) & auto-loads sample table
        page.locator('.wb-monitor-tabs').get_by_role('button', name='输出结果', exact=True).click()
        expect(page.locator('.wb-results-card')).to_be_visible()
        expect(page.locator('.wb-out-pill.is-active')).to_contain_text('delivery')
        expect(page.locator('.wb-sample-table tbody tr').first).to_be_visible(timeout=15000)

        # 2. Removed clutter buttons must NOT be shown on the Results tab
        results = page.locator('.wb-results')
        for removed in ('查看固定结果', '作为输入继续加工', '保留为输入表', '固定结果引用'):
            expect(results.get_by_role('button', name=removed, exact=True)).to_have_count(0)
        expect(results.get_by_role('button', name='在数据页打开', exact=True)).to_be_visible()

        # 3. Single-line truncation prevents long JSON from blowing up row height
        row_height = page.locator('.wb-sample-table tbody tr').first.evaluate(
            'el => el.getBoundingClientRect().height'
        )
        assert row_height < 80, f'Expected compact single-line row height, got {row_height}px'

        # 4. Anomaly badge & red cell tag highlight non-empty author_error
        expect(results.locator('.wb-badge-err')).to_contain_text('样本含')
        expect(results.locator('.wb-cell-tag.is-err').first).to_contain_text('400 invalid temperature')

        # 5. Clicking a row opens right-hand drawer with formatted JSON and closes cleanly
        page.locator('.wb-sample-table tbody tr').first.click()
        drawer = page.locator('.wb-row-drawer')
        expect(drawer).to_be_visible()
        expect(drawer.locator('.wb-kv-json').first).to_contain_text('"validity": "pass"')
        drawer.get_by_label('关闭行详情', exact=True).click()
        expect(drawer).not_to_be_visible()

        # 6. Switching output pill switches directly to author table
        page.locator('.wb-out-pill', has_text='author').click()
        expect(page.locator('.wb-out-pill.is-active')).to_contain_text('author')
        expect(page.locator('.wb-sample-table tbody tr').first).to_be_visible(timeout=15000)

        # 7. Observation panel hides engineering jargon and '未测量'
        page.locator('.wb-monitor-tabs').get_by_role('button', name='执行图', exact=True).click()
        expect(page.locator('.wb-node').first).to_be_visible()
        page.locator('.wb-node').last.click()
        detail_body = page.locator('.wb-run-detail-body')
        expect(detail_body.locator('.wb-obs-kpis')).to_be_visible(timeout=15000)
        visible_text = detail_body.evaluate(
            'el => { const clone = el.cloneNode(true); clone.querySelectorAll("details:not([open])").forEach(d => d.remove()); return clone.innerText; }'
        )
        assert '未测量' not in visible_text, visible_text
        assert '节点观测范围' not in visible_text, visible_text
        assert '不是完整节点日志' not in visible_text, visible_text
        assert not errors, errors
