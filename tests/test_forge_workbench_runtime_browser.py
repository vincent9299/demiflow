"""Native multi-run/attempt workbench behavior, including delivery faults.

Fixtures use only owned local Lance tables and pure Python operators. Held
responses are real API responses; observation reservations below test protocol
reset behavior and are not reported as measured business execution.
"""
import json
import time
from urllib.parse import parse_qs,urlparse

import pytest
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from test_forge_workspace import demo,isolated
from test_forge_runs import base,root,wait
from test_forge_runtime_core_flow import unregistered
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline,screenshot


@pytest.fixture(autouse=True)
def private_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


def selected(page):
    return page.evaluate('ForgeView.decodeView(location.hash)')


def run_finished(page,status='成功'):
    expect(page.locator('.wb-run-bar')).to_be_visible()
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text(status)
    return selected(page)['run_id']


def pick_run(page,run_id):
    page.get_by_role('combobox',name='运行记录',exact=True).select_option(run_id)
    expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(run_id)
    expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy','true')
    expect(page.locator('.wb-run-record.selected')).to_have_attribute('data-run',run_id)
    expect(page.locator('.wb-run-record.selected')).to_have_attribute('aria-pressed','true')


def hold_next_log(page,run_id):
    page.evaluate('''id => {
      const native=window.fetch.bind(window);
      window.__heldLog=false;window.__releaseLog=null;
      window.fetch=async(...args)=>{
        const response=await native(...args),url=new URL(String(args[0]),location.href);
        if(!window.__heldLog&&url.pathname.endsWith('/run-logs')&&url.searchParams.get('run_id')===id){
          window.__heldLog=true;
          return new Promise((resolve,reject)=>{window.__releaseLog=fail=>fail?reject(new Error('owned late transport fault')):resolve(response);});
        }
        return response;
      };
    }''',run_id)


def test_native_multiple_runs_select_graph_logs_outputs_and_discard_late_delivery(isolated,browser):
    prepares=[]
    with service(isolated,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        page.on('request',lambda r:prepares.append(r.post_data_json) if r.method=='POST' and r.url.endswith('/run-prepare') else None)
        pipeline(page)
        page.get_by_role('button',name='运行',exact=True).click()
        first=run_finished(page)
        assert runs.show(isolated,first)['outputs'][0]['result']['ref']['row_count']==2
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        assert page.locator('.wb-node[data-state=success]').count()>0
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        page.get_by_role('button',name='节点证据',exact=True).click()
        expect(page.locator('.wb-run-detail-body')).to_contain_text(first+' / 尝试 1')
        expect(page.locator('.wb-run-detail-body')).to_contain_text('不是完整节点日志')
        page.get_by_role('button',name='输出结果',exact=True).click()
        expect(page.locator('.wb-results')).to_be_visible()
        expect(page.locator('.wb-body')).not_to_be_visible()
        expect(page.locator('.wb-results')).to_contain_text('2 行')
        page.get_by_role('button',name='返回当前配置',exact=True).click()
        page.get_by_label('配置',exact=True).select_option('flows/select_b.json')
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_enabled()
        page.get_by_role('button',name='运行',exact=True).click()
        second=run_finished(page)
        assert first!=second
        assert runs.show(isolated,second)['outputs'][0]['result']['ref']['row_count']==1
        page.get_by_role('button',name='运行记录',exact=True).click()
        expect(page.locator('.wb-run-record')).to_have_count(2)
        page.get_by_role('button',name='运行日志',exact=True).click()
        expect(page.locator('.wb-run-detail-body')).to_contain_text(second+' / 尝试 1')
        for fail in (False,True):
            hold_next_log(page,first)
            pick_run(page,first)
            page.wait_for_function('() => window.__heldLog&&window.__releaseLog!==null')
            pick_run(page,second)
            expect(page.locator('.wb-run-detail-body')).to_contain_text(second+' / 尝试 1')
            page.evaluate('fail=>window.__releaseLog(fail)',fail)
            page.wait_for_timeout(150)
            expect(page.locator('.wb-run-detail-body')).to_contain_text(second+' / 尝试 1')
            expect(page.locator('.wb-run-detail-body')).not_to_contain_text(first)
            expect(page.locator('.wb-notice')).not_to_contain_text('owned late transport fault')
        pick_run(page,first)
        page.get_by_role('button',name='控制事件',exact=True).click()
        expect(page.locator('.wb-run-detail-body')).to_contain_text(first+' / 尝试 1')
        expect(page.locator('.wb-run-detail-body')).to_contain_text('不代表完整节点日志')
        assert len(prepares)==2 and prepares[0]['request_id']!=prepares[1]['request_id']
        screenshot(page,'native-multiple-run-selection')
        assert not errors,errors


def test_native_stop_survives_current_code_removal_and_keeps_attempt_identity(unregistered,browser):
    ws.register_flow(unregistered,'flows/waiting.py:run',params='flows/waiting.json')
    author={p:(unregistered/p).read_bytes() for p in ('flows/waiting.py','flows/waiting.json','operators/waiting.py')}
    active=None
    with service(unregistered,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        try:
            pipeline(page)
            page.get_by_label('流程',exact=True).select_option('flows/waiting.py:run')
            expect(page.get_by_label('配置',exact=True)).to_have_value('flows/waiting.json')
            page.get_by_role('button',name='运行',exact=True).click()
            page.wait_for_function('() => ForgeView.decodeView(location.hash)?.run_id')
            expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(selected(page)['run_id'])
            active=selected(page)['run_id']
            expect(page.locator('.wb-partial')).to_contain_text('部分输出已提交')
            for path in author:(unregistered/path).unlink()
            page.get_by_role('button',name='停止',exact=True).click()
            run_finished(page,'已取消')
            expect(page.locator('.wb-run-bar')).to_contain_text('所属执行已停止')
            result=runs.show(unregistered,active)
            assert result['state']['execution_stopped'] and result['state']['cleanup']=='complete'
            assert sum(bool(o['result']) for o in result['outputs'])==1
            page.get_by_role('button',name='运行日志',exact=True).click()
            expect(page.locator('.wb-run-detail-body')).to_contain_text(active+' / 尝试 1')
            for path,raw in author.items():(unregistered/path).write_bytes(raw)
            page.get_by_role('button',name='恢复运行',exact=True).click()
            expect(page.get_by_label('运行尝试',exact=True)).to_have_value('1')
            expect(page.locator('.wb-notice')).to_contain_text('当前继续显示原尝试')
            page.get_by_label('运行尝试',exact=True).select_option('2')
            expect(page.get_by_label('运行尝试',exact=True)).to_have_value('2')
            expect(page.locator('.wb-partial')).to_contain_text('部分输出已提交')
            assert selected(page)['attempt']==2
            page.get_by_role('button',name='停止',exact=True).click()
            run_finished(page,'已取消')
            expect(page.locator('.wb-run-bar')).to_contain_text('所属执行已停止')
            page.get_by_role('button',name='输出结果',exact=True).click()
            expect(page.locator('.wb-results')).to_contain_text('保留自尝试 1')
            page.get_by_label('运行尝试',exact=True).select_option('1')
            expect(page.locator('.wb-run-bar')).to_contain_text('历史尝试 · 只读')
            expect(page.get_by_role('button',name='停止',exact=True)).to_have_count(0)
            page.get_by_role('button',name='运行日志',exact=True).click()
            expect(page.locator('.wb-run-detail-body')).to_contain_text(active+' / 尝试 1')
            assert selected(page)['attempt']==1
            page.reload()
            expect(page.locator('.wb-run-bar')).to_contain_text('历史尝试 · 只读')
            assert selected(page)['attempt']==1
            screenshot(page,'native-stopped-historical-attempt')
            assert not errors,errors
        finally:
            if active and runs.show(unregistered,active)['status'] in runs.ACTIVE:
                runs.cancel(unregistered,active)
                wait(unregistered,active,timeout=25)
            for path,raw in author.items():(unregistered/path).write_bytes(raw)


def test_browser_observation_version_change_resets_pages_without_mixing(isolated,browser):
    run=runs.prepare(isolated,'flows/questions.py:select',params='flows/select_a.json')
    node=next(n for n in run['report']['nodes'] if (n.get('operator') or {}).get('name')=='above_threshold')
    action=run['report']['actions'][0]
    journal=runs.journal(isolated,run['run_id'])
    journal.reserve_nodes([(action,node['id'],i) for i in range(70)])
    reset=[]
    with service(isolated,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='运行记录',exact=True).click()
        pick_run(page,run['run_id'])
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        page.get_by_role('button',name='节点证据',exact=True).click()
        expect(page.locator('.wb-observation')).to_have_count(32)
        def change_before_cursor_read(route):
            query=parse_qs(urlparse(route.request.url).query)
            if query.get('cursor') and not reset:
                journal.reserve_nodes([(action,node['id'],70)])
                reset.append(True)
            route.continue_()
        page.route('**/run-observations?*',change_before_cursor_read)
        page.get_by_role('button',name='后续节点观测',exact=True).click()
        expect(page.get_by_role('button',name='上一页观测',exact=True)).to_be_disabled()
        expect(page.locator('.wb-observation').first).to_contain_text('第 1 次')
        expect(page.locator('.wb-observation')).to_have_count(32)
        assert reset and not errors,errors
        screenshot(page,'native-observation-cursor-reset')


def test_run_seal_summary_and_history_time_match_sealed_facts(isolated,browser):
    (isolated/'flows/quiet.py').write_text('''from demiflow import data
def run(*, source, target):
    return data.read_lance(source['uri'], version=source['version']).write_lance(target['uri'], mode='create', return_receipt=True)
''')
    (isolated/'flows/quiet.json').write_text(json.dumps({
        'format':'demiflow_forge_flow_parameters_v1','arguments':{
            'source':{'kind':'table','alias':'candidates'},
            'target':{'kind':'output','location_id':'local','relative_uri':'quiet.lance'}}}))
    ws.register_flow(isolated,'flows/quiet.py:run',params='flows/quiet.json')
    with service(isolated,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_label('流程',exact=True).select_option('flows/quiet.py:run')
        expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy','true')
        page.get_by_role('button',name='检查',exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        page.get_by_role('button',name='读图与配置',exact=True).click()
        page.get_by_text('本次运行设置',exact=True).click()
        page.get_by_label('本次运行秒数上限').fill('77')
        page.get_by_role('button',name='运行',exact=True).click()
        run_id=run_finished(page,'成功')
        expect(page.locator('.wb-run-seal')).to_contain_text('封存输入 candidates v3')
        expect(page.locator('.wb-run-seal')).to_contain_text('预算 77 秒累计')
        page.get_by_role('button',name='返回当前配置',exact=True).click()
        page.get_by_text('本次运行设置',exact=True).click()
        page.get_by_label('本次运行秒数上限').fill('99')
        page.get_by_role('button',name='运行记录',exact=True).click()
        row=page.locator('.wb-run-record[data-run="'+run_id+'"]')
        created=runs.show(isolated,run_id)['created_at']
        expect(row).to_contain_text(time.strftime('%Y-%m-%d %H:%M',time.localtime(created)))
        row.click()
        expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(run_id)
        expect(page.locator('.wb-run-seal')).to_contain_text('预算 77 秒累计')
        expect(page.locator('.wb-run-seal')).not_to_contain_text('预算 99')
        assert not errors,errors
        screenshot(page,'run-seal-summary-history-time')
