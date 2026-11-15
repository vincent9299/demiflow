"""Real Chromium + dedicated real SQLite; source/API adapter is test-only.

The two synthetic source rows and authority verifier are explicit fakes. No
workspace service, B1 deployment, Notebook, OCI job, SQL or model is started.
"""
from contextlib import contextmanager
import json
from pathlib import Path
from urllib.parse import urlsplit,parse_qs

from playwright.sync_api import expect

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import outputs
from test_forge_analysis_records import controller
from test_forge_markdown_reading import browser

ASSETS=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
PROJECT='w'+'1'*32


@contextmanager
def case_page(browser,tmp_path):
    manifest=dict(kind='fixed_lance',binding='a'*64,retention='external',rows=2,
        schema=[dict(name=name,type=kind,nullable=True) for name,kind in [('input','utf8'),('output','utf8'),('dialogue','list'),('tags','list')]],
        case_key=dict(kind='row_id',field=None))
    rows=[dict(_rowid='17',input='问题 A',output='**回答 A**',dialogue=[dict(role='user',content='你好')],tags=['a','b']),
          dict(_rowid='18',input='问题 B',output='回答 B',dialogue=[],tags=['b'])]
    with controller(tmp_path/'database') as client:
        source=client.call('register_source',manifest=manifest)['result']
        context=browser.new_context(viewport=dict(width=1440,height=1000));page=context.new_page();page.set_default_timeout(15000)
        calls=[];errors=[];drop=dict(annotation=False)
        page.on('pageerror',lambda error:errors.append(str(error)))
        mapping={'documents':'list_documents','view':'view','source':'source','view-save':'save_view',
            'annotation-heads':'annotation_heads','annotations':'annotations','annotations-save':'save_annotations','receipt':'receipt'}

        def route(request):
            path=urlsplit(request.request.url).path
            if path=='/':
                return request.fulfill(content_type='text/html',body='<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/style.css"><link rel="stylesheet" href="/analysis.css"><link rel="stylesheet" href="/analysis-case.css"><main id="mount"></main>')
            if path in ('/style.css','/analysis.css','/analysis-case.css'):
                return request.fulfill(content_type='text/css',body=(ASSETS/path[1:]).read_text())
            name=path.removeprefix('/api/analysis-')
            payload=request.request.post_data_json if request.request.method=='POST' else {key:value[0] for key,value in parse_qs(urlsplit(request.request.url).query).items()}
            calls.append((name,payload))
            if name=='source-register':value=dict(result=source)
            elif name=='case-page':
                c.case_view(payload['view']['descriptor'],source)
                view_digest=c.digest(payload['view']);query_digest=c.digest(dict(source=source['digest'],view=view_digest,query=payload['query']))
                value=dict(result=dict(kind='cases',source_snapshot=source['digest'],rows=rows,columns=payload['query']['columns'],offset=0,next_offset=None,next_cursor=None,
                    view_digest=view_digest,query_digest=query_digest,scope_digest=c.digest(rows),matching_in_scope=2,groups=[],other_groups=0,annotation_watermark=0,coverage=dict(requested_scope=payload['query']['scope'],complete=True,rows_scanned=2,total_rows=2)))
            elif name=='field-preview':value=dict(result=outputs.field_preview(payload['format'],payload['value']))
            elif name in mapping:
                if name=='documents' and 'limit' in payload:payload['limit']=int(payload['limit'])
                if name=='annotations':
                    payload['target']=json.loads(payload['target'])
                    for key in ('before','limit','watermark'):
                        if key in payload:payload[key]=int(payload[key])
                value=client.call(mapping[name],**payload)
            else:raise AssertionError('Forbidden/non-test API: '+name)
            if name=='annotations-save' and drop['annotation']:
                drop['annotation']=False;return request.abort('failed')
            return request.fulfill(status=422 if 'code' in value else 200,content_type='application/json',body=json.dumps(value if 'code' in value else value['result'],ensure_ascii=False))
        page.route('**/*',route)
        try:
            page.goto('https://case-view-test.invalid/')
            for name in ('analysis-state.js','analysis-render.js','analysis-case.js'):page.add_script_tag(path=str(ASSETS/name))
            page.evaluate('''project=>{
              window.caseAPI=async(name,query={},body)=>{
                const response=await fetch('/api/'+name+(Object.keys(query).length?'?'+new URLSearchParams(query):''),{method:body?'POST':'GET',headers:{'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
                const value=await response.json();if(!response.ok){const error=Error(value.error);error.code=value.code;throw error;}return value;
              };
              window.mountCase=()=>window.caseView=ForgeCase.mount({parent:document.querySelector('#mount'),api:caseAPI,project,connected:true,
                permissions:{analysis_write:true,case_annotate:true},mountText:(parent,text)=>{const pre=document.createElement('pre');pre.textContent=text;parent.append(pre);},disposeReaders:()=>{}});
              mountCase();caseView.activate(true);caseView.setSource({label:'synthetic · v1',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});
            }''',PROJECT)
            expect(page.get_by_role('heading',name='Case 17',exact=True)).to_be_visible()
            expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible()
            yield page,client,source,calls,errors,drop
        except BaseException:
            page.screenshot(path=str(tmp_path/'case-browser-failure.png'),full_page=True)
            raise
        finally:context.close()


def test_three_regions_view_definition_changes_and_annotation_history_are_separate(browser,tmp_path):
    with case_page(browser,tmp_path) as (page,client,source,calls,errors,_):
        page.get_by_role('button',name='字段与评测配置',exact=True).click()
        page.get_by_label('字段标题 2',exact=True).fill('模型回答')
        page.get_by_label('预览格式 2',exact=True).select_option('md')
        page.get_by_label('Case 布局',exact=True).select_option('comparison')
        expect(page.get_by_role('heading',name='输出 · 模型回答',exact=True)).to_be_visible()
        page.get_by_role('button',name='保存视图',exact=True).click()
        expect(page.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
        descriptor=next(payload['descriptor'] for name,payload in calls if name=='view-save')
        target=dict(source=source['digest'],case_key='17',variant=None,field=None)
        old_definition=c.digest(descriptor['questions'][0])
        assert client.call('annotations',target=target,definition=old_definition)['result']['items']==[]
        page.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
        page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        page.get_by_role('button',name='配置评测',exact=True).click()
        page.get_by_label('评测题目 1',exact=True).fill('准确吗')
        page.get_by_role('button',name='应用评测定义',exact=True).click()
        expect(page.get_by_label('标注 准确吗',exact=True)).to_have_value('')
        assert client.call('annotations',target=target,definition=old_definition)['result']['items'][0]['value']=='yes'
        page.get_by_label('标注 准确吗',exact=True).select_option('no')
        page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        page.get_by_role('button',name='Case 18',exact=True).click()
        expect(page.get_by_label('标注 准确吗',exact=True)).to_have_value('')
        page.get_by_role('button',name='Case 17',exact=True).click()
        expect(page.get_by_label('标注 准确吗',exact=True)).to_have_value('no')
        page.get_by_text('标注历史与恢复',exact=True).click()
        page.get_by_role('button',name='清除已保存答案',exact=True).click()
        expect(page.get_by_label('标注 准确吗',exact=True)).to_have_value('')
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v2')
        page.get_by_text('标注历史与恢复',exact=True).click()
        page.get_by_role('button',name='读取历史',exact=True).click()
        page.get_by_role('button',name='恢复到 v1',exact=True).click()
        expect(page.get_by_label('标注 准确吗',exact=True)).to_have_value('no')
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v3')
        for width in (1440,390,320):
            page.set_viewport_size(dict(width=width,height=1000))
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            page.screenshot(path=str(tmp_path/('case-three-regions-'+str(width)+'.png')),full_page=True)
        assert not errors,errors
        assert not any(any(word in name for word in ('cell','session','execute')) for name,_ in calls)


def test_lost_annotation_response_reconciles_original_receipt_after_remount(browser,tmp_path):
    with case_page(browser,tmp_path) as (page,client,source,calls,errors,drop):
        page.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');drop['annotation']=True
        page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
        expect(page.get_by_role('button',name='核对 Case 原请求',exact=True)).to_be_visible()
        original=[payload for name,payload in calls if name=='annotations-save'];assert len(original)==1
        page.evaluate('''()=>{if(!caseView.dispose())throw Error('draft must be retained');document.querySelector('#mount').replaceChildren();mountCase();caseView.activate(true);caseView.setSource({label:'same fixed source',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});}''')
        expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
        page.get_by_role('button',name='核对 Case 原请求',exact=True).click()
        expect(page.get_by_role('button',name='核对 Case 原请求',exact=True)).to_have_count(0)
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        history=client.call('annotations',target=original[0]['target'],definition=c.digest(original[0]['entries'][0]['definition']))['result']
        assert len(history['items'])==1 and history['items'][0]['value']=='yes'
        assert len([payload for name,payload in calls if name=='annotations-save'])==1
        assert not errors,errors


def test_storage_denial_keeps_answer_and_refuses_dispatch_without_durable_identity(browser,tmp_path):
    with case_page(browser,tmp_path) as (page,client,source,calls,errors,_):
        page.evaluate('''()=>{Storage.prototype.setItem=function(){throw new DOMException('injected','QuotaExceededError');};}''')
        page.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
        page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
        expect(page.locator('.analysis-case')).to_contain_text('无法持久保存原请求身份')
        assert not [name for name,_ in calls if name=='annotations-save']
        page.evaluate('''()=>{if(!caseView.dispose())throw Error('bounded window draft should survive');document.querySelector('#mount').replaceChildren();mountCase();caseView.activate(true);caseView.setSource({label:'same',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});}''')
        expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
        page.get_by_text('历史与草稿',exact=True).click()
        with page.expect_download() as download:page.get_by_role('button',name='导出 Case 草稿',exact=True).click()
        path=tmp_path/'case-draft.json';download.value.save_as(path)
        exported=json.loads(path.read_bytes());assert any(answer['value']=='yes' for answer in exported['state']['answers'])
        assert page.evaluate('()=>{const event=new Event("beforeunload",{cancelable:true});dispatchEvent(event);return event.defaultPrevented;}')
        assert not errors,errors
