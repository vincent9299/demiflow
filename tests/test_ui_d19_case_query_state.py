"""Case query panel state over the actual client and native Records/Lance.

The representative fixture has a metadata transport helper, not a kernel. These
checks exercise real query responses and draft storage; no parser, query result,
or application state is replaced. Fast DOM actions share one browser task so a
pending native details toggle cannot hide a redraw race behind test timing.
"""
import hashlib
import json
import os
from pathlib import Path

import demiflow
from playwright.sync_api import expect

import test_forge_analysis_case_demo_browser as case_fixture
from test_forge_markdown_reading import browser


TREE = Path(__file__).resolve().parents[1]


def test_case_query_fast_edits_keep_panel_and_draft_until_explicit_apply(browser, tmp_path, monkeypatch):
    assert tmp_path.resolve().is_relative_to(Path('/tmp'))
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE)
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'domain'))
    out = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path)))
    assert out.resolve().is_relative_to(Path('/tmp'))
    out.mkdir(parents=True, exist_ok=True)
    with case_fixture.representative_case(browser, tmp_path, monkeypatch) as f:
        page = f['page']
        page.get_by_role('button', name='Case 2', exact=True).click()
        note = page.get_by_label('标注 备注', exact=True)
        note.fill('筛选编辑期间保留的 Case 2 草稿')
        controls = page.locator('details.case-ui-query-controls')
        assert controls.evaluate('(node) => node.open') is False
        fast = page.evaluate('''() => {
          const panel = () => document.querySelector('details.case-ui-query-controls');
          const original = panel(); original.querySelector('summary').click();
          const before = original.open;
          const field = original.querySelector('[aria-label="筛选字段"]');
          field.value = 'category'; field.dispatchEvent(new Event('change', {bubbles:true}));
          const afterField = panel();
          const relation = afterField.querySelector('[aria-label="筛选关系"]');
          relation.value = 'contains'; relation.dispatchEvent(new Event('change', {bubbles:true}));
          window.retiredQueryControls = [original, afterField];
          return {before, afterField:afterField.open, afterRelation:panel().open,
            oldDetached:!original.isConnected, fieldPanelDetached:!afterField.isConnected};
        }''')
        # No sleep, second summary click, or forced fill can conceal a collapse.
        assert fast == dict(before=True, afterField=True, afterRelation=True,
                            oldDetached=True, fieldPanelDetached=True), fast
        value = page.get_by_label('筛选值', exact=True)
        expect(value).to_be_visible()
        value.fill('陶瓷杯')
        expect(note).to_have_value('筛选编辑期间保留的 Case 2 草稿')

        # A retired panel can still deliver its queued toggle. Feed the opposite
        # state through the real DOM event handler, then force a normal redraw.
        retired = page.evaluate('''() => {
          const old = window.retiredQueryControls[0]; old.open = false;
          old.dispatchEvent(new Event('toggle'));
          return {connected:old.isConnected,
            currentOpen:document.querySelector('details.case-ui-query-controls').open};
        }''')
        assert retired == dict(connected=False, currentOpen=True)
        page.get_by_label('筛选关系', exact=True).select_option('eq')
        expect(value).to_be_visible()
        value.fill('陶瓷杯')
        expect(controls).to_have_attribute('open', '')
        expect(note).to_have_value('筛选编辑期间保留的 Case 2 草稿')

        page.get_by_role('button', name='应用筛选与分组', exact=True).click()
        expect(page.locator('.analysis-case-list button')).to_have_count(2)
        assert controls.evaluate('(node) => node.open') is False
        expect(value).not_to_be_visible()
        expect(page.locator('p.case-ui-current-key')).to_have_text('Case 2')
        expect(page.locator('.analysis-case-content')).to_contain_text('当前 Case 不在本次选样页')
        expect(note).to_have_value('筛选编辑期间保留的 Case 2 草稿')
        calls = [payload for name, payload in f['calls'] if name == 'case-page']
        assert calls
        sent_filter = calls[-1]['view']['descriptor']['filters']
        assert sent_filter == dict(op='eq', field='category', value='陶瓷杯')
        assert page.locator('.analysis-case-list button').evaluate_all(
            '(nodes) => nodes.map(node => node.dataset.caseKey)') == ['0', '1']
        assert not [payload for name, payload in f['calls'] if name == 'annotations-save']

        # Late open signals from both removed panels must not reopen the user's
        # explicitly closed current panel or erase the exact filter draft.
        closed = page.evaluate('''() => {
          for (const old of window.retiredQueryControls) {
            old.open = true; old.dispatchEvent(new Event('toggle'));
          }
          const current=document.querySelector('details.case-ui-query-controls');
          return {open:current.open,field:current.querySelector('[aria-label="筛选字段"]').value,
            relation:current.querySelector('[aria-label="筛选关系"]').value,
            value:current.querySelector('[aria-label="筛选值"]').value};
        }''')
        assert closed == dict(open=False, field='category', relation='eq', value='陶瓷杯')
        draft = page.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.source)',
                              dict(project=f['project'], source=f['source']['digest']))
        assert draft['current'] == '2'
        assert draft['descriptor']['filters'] == sent_filter
        assert any(answer.get('value') == '筛选编辑期间保留的 Case 2 草稿' for answer in draft['answers'])
        asset = case_fixture.OWN / 'analysis-case.js'
        (out / 'case-query-state.json').write_text(json.dumps(dict(
            asset_path=str(asset), asset_sha256=hashlib.sha256(asset.read_bytes()).hexdigest(),
            candidate_asset=asset.resolve() == TREE / 'demiflow/workbench/web/analysis-case.js',
            fast=fast, retired=retired, closed=closed, sent_filter=sent_filter,
            saved_draft=draft, no_annotation_writes=True), ensure_ascii=False, indent=2) + '\n')
        page.screenshot(path=str(out / 'case-query-state.png'), full_page=True)
        assert not f['errors'], f['errors']
