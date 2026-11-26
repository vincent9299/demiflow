"""Current run-result choices survive an unrelated project-context refresh."""
import json
import os
from pathlib import Path
from urllib.parse import parse_qs,urlsplit

from playwright.sync_api import expect

from demiflow.forge.runs import manager as runs

from test_forge_workspace import demo,isolated
from test_forge_runs import wait
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline


def test_catalog_refresh_retains_result_choices_without_resampling_current_case(isolated,browser,monkeypatch,tmp_path):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    ids=[]
    for config in ('a','b'):
        prepared=runs.prepare(isolated,'flows/questions.py:select',params='flows/select_'+config+'.json',new_outputs=True)
        runs.start(isolated,prepared['run_id'],expected=prepared['digest'])
        assert wait(isolated,prepared['run_id'])['status']=='succeeded'
        ids.append(prepared['run_id'])
    a,b=ids;left='run:'+a+':target';right='run:'+b+':target'
    with service(isolated) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='运行记录',exact=True).click()
        page.locator('.wb-run-record[data-run="'+a+'"]').click()
        expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(a)
        page.get_by_role('button',name='输出结果',exact=True).click()
        page.get_by_role('button',name='查看固定结果',exact=True).click()
        page.locator('.case-record-selector button').first.wait_for()
        page.locator('.case-comparison > summary').click()
        expect(page.get_by_label('对照表',exact=True).locator('option[value="'+right+'"]').first).to_have_count(1)
        page.locator('.case-comparison > summary').click()
        page.get_by_role('tab',name='image_uri',exact=True).first.click()
        page.get_by_role('button',name='查看本地图片',exact=True).first.click()
        image=page.locator('.case-image');image.wait_for()
        image_uri=image.get_attribute('src')
        position=page.url;reads=[];held=[]
        page.on('request',lambda r:reads.append(r.url) if '/sample?' in r.url or '/media?' in r.url else None)
        def hold_context(route):
            response=route.fetch(timeout=65000)
            assert response.ok,response.text()
            raw=response.json()
            assert right not in {table['alias'] for table in raw['tables']}
            held.append((route,response))
            page.evaluate('window.catalogRefreshHeld=true')
        page.route('**/context',hold_context)
        source=isolated/'operators/question_rules.py'
        source.write_text(source.read_text()+'\n# unrelated catalogue refresh\n')
        page.wait_for_function('() => window.catalogRefreshHeld === true',timeout=65000)
        assert page.url==position and image.get_attribute('src')==image_uri and reads==[]
        def selected_table(response):
            parts=urlsplit(response.url)
            return parts.path.endswith('/table') and parse_qs(parts.query).get('alias')==[left]
        with page.expect_response(selected_table,timeout=65000):
            assert len(held)==1
            route,response=held[0];route.fulfill(response=response)
        page.unroute('**/context',hold_context)
        assert page.url==position and image.get_attribute('src')==image_uri
        assert reads==[],reads
        row=page.locator('.case-record-selector button').nth(1);row_id=row.get_attribute('data-row-id');row.click()
        page.get_by_role('heading',name='单条 case · '+row_id,exact=True).wait_for()
        page.locator('.case-comparison > summary').click()
        options=page.get_by_label('对照表',exact=True).locator('option').evaluate_all('(rows)=>rows.map(row=>row.value)')
        out=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));out.mkdir(parents=True,exist_ok=True)
        (out/'result-catalog-refresh-observation.json').write_text(json.dumps(dict(runs=ids,selected=left,
            expected_comparison=right,comparison_options_after_refresh=options,current_case_not_resampled=True,
            url_unchanged=True,image_blob_unchanged=True,real_context_response_delayed=True),ensure_ascii=False,indent=2)+'\n')
        expect(page.get_by_label('对照表',exact=True).locator('option[value="'+right+'"]')).to_have_count(1,timeout=5000)
        page.get_by_label('对照表',exact=True).select_option(right)
        page.get_by_label('业务键',exact=True).select_option('case_id')
        page.get_by_role('button',name='查找对应 case',exact=True).click()
        page.get_by_text('未找到匹配记录',exact=True).wait_for()
        assert not errors,errors
