"""Case media consumer with real Chromium/renderer and explicit trusted fakes.

PNG/WAV are real generated fixture bytes. The lookup/capture/annotation adapters
below are synthetic consumer-contract fixtures, not deployed authorization,
probe/capture, or persistent media-evidence backend certification.
"""
from contextlib import contextmanager
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import wave
import zlib

from playwright.sync_api import expect

from test_forge_markdown_reading import browser

ASSETS = Path(__file__).resolve().parents[1] / 'demiflow/workbench/web'
RENDERER = Path(os.environ.get('FORGE_CASE_MEDIA_RENDERER_ROOT', str(ASSETS)))
PROJECT = 'w' + '1' * 32


def fixture_media():
    def chunk(kind, value):
        return struct.pack('>I', len(value)) + kind + value + struct.pack('>I', zlib.crc32(kind + value))
    png = b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', 3, 2, 8, 2, 0, 0, 0))
    png += chunk(b'IDAT', zlib.compress((b'\0' + b'\x30\x80\xc0' * 3) * 2)) + chunk(b'IEND', b'')
    stream = io.BytesIO()
    with wave.open(stream, 'wb') as output:
        output.setnchannels(1); output.setsampwidth(2); output.setframerate(8000)
        output.writeframes(b'\0\0' * 800)
    wav = stream.getvalue()
    return {name: dict(encoded=base64.b64encode(value).decode(), descriptor=dict(
        artifact_id='o' + letter * 32, sha256=hashlib.sha256(value).hexdigest(), mime=mime, bytes=len(value), **extra))
        for name, value, letter, mime, extra in [
            ('picture', png, '1', 'image/png', dict(width=3, height=2)),
            ('sound', wav, '2', 'audio/wav', dict(duration_s=.1, codec='pcm_s16le'))]}


@contextmanager
def media_case(browser, tmp_path, *, mode='ready', capability=False, write=True):
    context = browser.new_context(viewport=dict(width=1440, height=1100))
    page = context.new_page(); page.set_default_timeout(12000); errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    def route(request):
        name = request.request.url.rsplit('/', 1)[-1]
        if name in ('style.css', 'analysis.css', 'analysis-case.css'):
            return request.fulfill(content_type='text/css', body=(ASSETS / name).read_text())
        if request.request.url == 'https://case-media-test.invalid/':
            return request.fulfill(content_type='text/html', body='<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/style.css"><link rel="stylesheet" href="/analysis.css"><link rel="stylesheet" href="/analysis-case.css"><main id="mount"></main>')
        raise AssertionError('Unexpected external media/network request: ' + request.request.url)
    page.route('**/*', route)
    try:
        page.goto('https://case-media-test.invalid/')
        for name, root in [('analysis-state.js', ASSETS), ('analysis-output-media.js', RENDERER),
                           ('analysis-render.js', RENDERER), ('analysis-case-media.js', ASSETS), ('analysis-case.js', ASSETS)]:
            page.add_script_tag(path=str(root / name))
        page.evaluate(BOOT, dict(project=PROJECT, assets=fixture_media(), mode=mode, capability=capability, write=write))
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_be_visible()
        yield page, errors
    except BaseException:
        page.screenshot(path=str(tmp_path / 'case-media-failure.png'), full_page=True)
        raise
    finally:
        context.close()


BOOT = r'''async options => {
  const S=ForgeAnalysisState,clone=value=>value===undefined?undefined:S.clone(value),canonical=ForgeCaseMedia.canonical;
  const f=window.fixture={...options,calls:[],lookups:[],captures:[],resolves:[],releases:[],records:new Map(),receipts:new Map(),held:[],holdLookup:false,holdCapture:false,dropCapture:false,dropSave:false,generation:1,legacy:0,grants:{analysis_write:options.write,case_annotate:true}};
  const source=digest=>({digest,kind:'fixed_lance',binding:'a'.repeat(64),retention:'external',rows:2,schema:['picture','sound','input'].map(name=>({name,type:'utf8',nullable:true})),case_key:{kind:'row_id',field:null}});
  f.source=source('d'.repeat(64));f.other=source('e'.repeat(64));
  const schema=()=>({version:1,source_snapshot:f.source.digest,case_key:f.source.case_key,fields:[
    {id:'picture',field:'picture',label:'图片材料',role:'input',format:'image'},
    {id:'sound',field:'sound',label:'音频材料',role:'reference',format:'audio'},
    {id:'input',field:'input',label:'原文',role:'context',format:'text'}],grouping:[],filters:null,sort:[],layout:'card',
    questions:[{id:'quality',title:'结果是否符合预期',type:'choice',required:true,options:[{id:'yes',label:'符合'},{id:'no',label:'不符合'}]}]});
  const freshSource={id:'v'+'1'.repeat(32),view_id:null,revision:0,base:null,view_touched:false,current:null,offset:0,answers:[],descriptor:schema()};
  new S.Store().saveCaseDraft(f.project,f.source.digest,freshSource);
  const requestKey=r=>canonical({source:r.source_snapshot,case_key:r.case_key,field:r.field});
  const result=request=>{
    const asset=f.assets[request.field],descriptor=clone(asset.descriptor);
    return {status:'ready',reference:{source_snapshot:request.source_snapshot,case_key:request.case_key,field:request.field,
      artifact_id:descriptor.artifact_id,artifact_generation:f.generation,expected_binding:'c'.repeat(64),expected_sha256:descriptor.sha256,mime:descriptor.mime},descriptor,
      dispose:()=>f.releases.push(requestKey(request)+':'+f.generation)};
  };
  const defer=async(kind,value)=>new Promise(resolve=>f.held.push({kind,finish:()=>resolve(value)}));
  f.fieldMedia={
    lookup:async request=>{
      f.lookups.push({keys:Object.keys(request).sort(),project:request.project,source_snapshot:request.source_snapshot,case_key:request.case_key,field:request.field});
      let value=f.mode==='ready'?result(request):f.mode==='required'?{status:'capture_required'}:{status:'unavailable',reason:'resource_root_missing',message:'未声明授权媒体资源根；原字段值保留。'};
      if(f.holdLookup){f.holdLookup=false;return defer('lookup',value);}return value;
    },
    capture:async request=>{
      f.captures.push({keys:Object.keys(request).sort(),project:request.project,request_id:request.request_id,source_snapshot:request.source_snapshot,case_key:request.case_key,field:request.field,
        prepared:new S.Store().pending(f.project).some(i=>i.operation==='media-capture'&&i.payload.request_id===request.request_id)});
      const value=result(request);f.receipts.set(request.request_id,{kind:'media',request:clone({...request,signal:undefined})});
      if(f.holdCapture){f.holdCapture=false;return defer('capture',value);}
      if(f.dropCapture){f.dropCapture=false;throw Error('injected committed capture delivery loss');}return value;
    },
    receipt:async request=>{const stored=f.receipts.get(request.request_id);if(!stored)return {status:'pending'};return result(stored.request);}
  };
  f.resolveMedia=async request=>{
    f.resolves.push({kind:request.kind,reference:clone(request.reference),descriptor:clone(request.descriptor),source_snapshot:request.source_snapshot,case_key:request.case_key,field:request.field});
    const bytes=Uint8Array.from(atob(f.assets[request.field].encoded),c=>c.charCodeAt(0));
    return {descriptor:clone(request.descriptor),blob:new Blob([bytes],{type:request.descriptor.mime}),dispose:()=>f.releases.push('playback:'+requestKey(request))};
  };
  const recordKey=(target,digest)=>canonical({target,digest});
  f.api=async(name,query={},payload)=>{
    f.calls.push({name,query:clone(query),payload:clone(payload)});
    if(name==='analysis-source-register')return payload.locator.alias==='other'?clone(f.other):clone(f.source);
    if(name==='analysis-source')return clone(query.digest===f.other.digest?f.other:f.source);
    if(name==='analysis-documents')return {items:[]};
    if(name==='analysis-case-page')return {kind:'cases',source_snapshot:payload.digest,columns:payload.query.columns,rows:['17','18'].map(id=>({_rowid:id,picture:'/private/image-'+id+'.png',sound:'file:///private/sound-'+id+'.wav',input:'合成 Case '+id})),offset:0,next_cursor:null,next_offset:null,view_digest:'a'.repeat(64),query_digest:'b'.repeat(64),scope_digest:'c'.repeat(64),annotation_watermark:f.calls.length,groups:[],other_groups:0,matching_in_scope:2,coverage:{requested_scope:payload.query.scope,complete:true,total_rows:2,rows_scanned:2}};
    if(name==='analysis-annotation-heads')return {target:clone(payload.target),annotation_watermark:0,items:payload.definitions.map(digest=>({definition_digest:digest,head:clone(f.records.get(recordKey(payload.target,digest))?.at(-1)||null)}))};
    if(name==='analysis-annotations')return {items:clone([...(f.records.get(recordKey(JSON.parse(query.target),query.definition))||[])].reverse()),annotation_watermark:0,next_before:null};
    if(name==='analysis-receipt')return clone(f.receipts.get(query.request_id)?.value);
    if(name==='analysis-annotations-save'){
      const heads=[];
      for(const entry of payload.entries){const digest=await ForgeCase.hash(entry.definition),key=recordKey(payload.target,digest),events=f.records.get(key)||[],old=events.at(-1);
        if((old?.revision||0)!==entry.expected_revision){const error=Error('injected consumer CAS conflict');error.code='conflict';throw error;}
        const restored=entry.action==='revert'?events.find(i=>i.revision===entry.revision):null;
        const event={revision:events.length+1,state:entry.action==='clear'?'cleared':restored?.state||'answered',value:entry.action==='set'?clone(entry.value):entry.action==='clear'?null:clone(restored.value),origin:clone(payload.origin)};
        if(entry.action==='set'&&entry.media_basis!==undefined)event.media_basis=clone(entry.media_basis);
        if(entry.action!=='set'&&(restored||old)?.media_basis!==undefined)event.media_basis=clone((restored||old).media_basis);
        f.records.set(key,[...events,event]);heads.push({definition:digest,revision:event.revision,state:event.state,...(event.media_basis===undefined?{}:{media_basis:clone(event.media_basis)})});
      }
      const value={target:clone(payload.target),heads};f.receipts.set(payload.request_id,{kind:'annotation',value});
      if(f.dropSave){f.dropSave=false;throw Error('injected annotation delivery loss');}return clone(value);
    }
    throw Error('Forbidden synthetic consumer operation: '+name);
  };
  window.mountCase=()=>window.caseView=ForgeCase.mount({parent:document.querySelector('#mount'),project:f.project,api:f.api,connected:true,permissions:f.grants,
    features:{annotation_media_basis:f.capability},fieldMedia:f.fieldMedia,resolveMedia:f.resolveMedia,media:()=>{f.legacy++;},
    mountText:(parent,text)=>{const p=document.createElement('pre');p.textContent=text;parent.append(p);},disposeReaders:()=>{}});
  window.selectSource=alias=>caseView.setSource({label:'synthetic media adapter '+alias,locator:{kind:'workspace_table',alias,binding:'a'.repeat(64),columns:['picture','sound','input']}});
  window.remount=()=>{if(!caseView.dispose())throw Error('draft loss');document.querySelector('#mount').replaceChildren();mountCase();caseView.activate(true);selectSource('fixture');};
  window.flushHeld=kind=>{for(const held of f.held.filter(i=>i.kind===kind))held.finish();f.held=f.held.filter(i=>i.kind!==kind);};
  mountCase();caseView.activate(true);selectSource('fixture');
}'''


def draft(page):
    return page.evaluate('new ForgeAnalysisState.Store().caseDraft(fixture.project,fixture.source.digest)')


def test_ready_descriptors_use_shared_png_wav_renderer_and_release_on_case_format_leave(browser, tmp_path):
    with media_case(browser, tmp_path) as (page, errors):
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_be_visible()
        expect(page.get_by_role('button', name='加载音频', exact=True)).to_be_visible()
        assert page.evaluate('fixture.captures.length===0&&fixture.resolves.length===0')
        page.get_by_role('button', name='预览图片', exact=True).click()
        expect(page.locator('img.analysis-media')).to_be_visible()
        page.get_by_role('button', name='加载音频', exact=True).click()
        expect(page.locator('audio.analysis-media')).to_be_visible()
        assert page.evaluate('fixture.resolves.every(r=>Object.keys(r.reference).length===8&&r.reference.case_key==="17"&&r.descriptor.artifact_id===r.reference.artifact_id)')
        page.get_by_role('button', name='Case 18', exact=True).click()
        expect(page.locator('img.analysis-media,audio.analysis-media')).to_have_count(0)
        assert page.evaluate('fixture.releases.filter(i=>i.startsWith("playback:")).length===2')
        page.get_by_role('button', name='字段与评测配置', exact=True).click()
        page.get_by_label('预览格式 1', exact=True).select_option('text')
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_have_count(0)
        assert page.evaluate('fixture.captures.length===0&&fixture.legacy===0')
        page.evaluate('caseView.activate(false)')
        assert page.evaluate('ForgeAnalysisMedia.stats().allocations===0&&ForgeAnalysisMedia.stats().activePlayers===0&&ForgeAnalysisMedia.stats().encodedBytes===0')
        assert not errors, errors


def test_explicit_capture_permissions_original_receipt_and_independent_text_save(browser, tmp_path):
    with media_case(browser, tmp_path, mode='required', write=False) as (page, errors):
        pins=page.get_by_role('button', name='固定媒体供评测', exact=True)
        expect(pins).to_have_count(2);expect(pins.first).to_be_disabled()
        assert page.evaluate('fixture.captures.length===0')
        page.evaluate('fixture.grants.analysis_write=true;caseView.updatePermissions(fixture.grants);fixture.dropCapture=true')
        expect(pins.first).to_be_enabled();pins.first.click()
        expect(page.get_by_role('button', name='核对媒体原请求', exact=True)).to_be_visible()
        assert page.evaluate('fixture.captures.length===1&&fixture.captures[0].prepared')
        assert page.evaluate('JSON.stringify(fixture.captures[0].keys)===JSON.stringify(["case_key","field","project","request_id","signal","source_snapshot"])')
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('yes')
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        assert page.evaluate('fixture.calls.filter(i=>i.name==="analysis-annotations-save").length===1')
        page.evaluate('remount()')
        expect(page.get_by_role('button', name='核对媒体原请求', exact=True)).to_be_visible()
        page.get_by_role('button', name='核对媒体原请求', exact=True).click()
        expect(page.get_by_role('button', name='核对媒体原请求', exact=True)).to_have_count(0)
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_be_visible()
        assert page.evaluate('fixture.captures.length===1')
        assert not errors, errors


def test_missing_root_keeps_raw_values_and_late_lookup_capture_never_rebinds(browser, tmp_path):
    with media_case(browser, tmp_path, mode='unavailable') as (page, errors):
        expect(page.locator('.case-media-field').first).to_contain_text('未声明授权媒体资源根')
        page.get_by_text('原字段值', exact=True).first.click()
        expect(page.locator('.case-media-original').first).to_contain_text('/private/image-17.png')
        assert page.evaluate('fixture.legacy===0&&fixture.captures.length===0&&fixture.resolves.length===0')
        page.evaluate('fixture.mode="ready";fixture.holdLookup=true')
        page.get_by_role('button', name='重新核对媒体', exact=True).first.click()
        page.wait_for_function('fixture.held.some(i=>i.kind==="lookup")')
        page.get_by_role('button', name='Case 18', exact=True).click()
        page.evaluate('flushHeld("lookup")')
        expect(page.get_by_role('heading', name='Case 18', exact=True)).to_be_visible()
        assert page.evaluate('fixture.releases.some(i=>i.includes("17"))')
        page.evaluate('fixture.mode="required"')
        page.get_by_role('button', name='重新核对媒体', exact=True).first.click()
        page.evaluate('fixture.holdCapture=true')
        page.get_by_role('button', name='固定媒体供评测', exact=True).first.click()
        page.wait_for_function('fixture.held.some(i=>i.kind==="capture")')
        page.evaluate('selectSource("other")')
        page.wait_for_function('new ForgeAnalysisState.Store().caseDraft(fixture.project,fixture.other.digest)!==null')
        page.evaluate('flushHeld("capture")')
        expect(page.locator('.case-ui-context')).to_contain_text('来源 '+ 'e'*12)
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_have_count(0)
        assert page.evaluate('new ForgeAnalysisState.Store().pending(fixture.project).filter(i=>i.operation==="media-capture").length===0')
        assert not errors, errors


def test_answer_basis_freezes_on_first_edit_and_capability_blocks_only_media_drafts(browser, tmp_path):
    with media_case(browser, tmp_path) as (page, errors):
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_be_visible()
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('yes')
        before=draft(page)['answers'][0]['media_basis'];assert len(before)==2
        page.evaluate('fixture.generation=2')
        page.get_by_role('button', name='重新核对媒体', exact=True).first.click()
        expect(page.locator('.case-media-answer')).to_contain_text('此答案仍保留编辑时的依据')
        assert draft(page)['answers'][0]['media_basis']==before
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.locator('.analysis-case')).to_contain_text('含媒体依据的答案草稿已保留')
        assert page.evaluate('fixture.calls.every(i=>i.name!=="analysis-annotations-save")')
        page.evaluate('remount()')
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_have_value('yes')
        assert draft(page)['answers'][0]['media_basis']==before
        page.get_by_label('采用当前固定媒体作为本题依据 结果是否符合预期', exact=True).click()
        assert all(ref['artifact_generation']==2 for ref in draft(page)['answers'][0]['media_basis'])
        assert not errors, errors


def test_media_basis_save_receipt_history_clear_revert_preserve_exact_consumer_evidence(browser, tmp_path):
    with media_case(browser, tmp_path, capability=True) as (page, errors):
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('yes')
        before=draft(page)['answers'][0]['media_basis']
        page.evaluate('fixture.dropSave=true')
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_be_visible()
        page.evaluate('remount()')
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_have_value('yes')
        page.get_by_role('button', name='核对 Case 原请求', exact=True).click()
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_have_count(0)
        assert draft(page)['answers'][0]['media_basis']==before and not draft(page)['answers'][0]['dirty']
        page.get_by_text('标注历史与恢复', exact=True).click()
        page.get_by_role('button', name='清除已保存答案', exact=True).click()
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_have_value('')
        assert draft(page)['answers'][0]['media_basis']==before
        page.get_by_text('标注历史与恢复', exact=True).click()
        page.get_by_role('button', name='读取历史', exact=True).click()
        history=page.locator('.case-ui-history-entry')
        expect(history).to_have_count(2)
        expect(history.first).to_contain_text('固定媒体依据 · 2 个字段')
        page.get_by_role('button', name='恢复到 v1', exact=True).click()
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_have_value('yes')
        assert draft(page)['answers'][0]['media_basis']==before
        calls=page.evaluate('fixture.calls.filter(i=>i.name==="analysis-annotations-save").map(i=>i.payload)')
        assert len(calls)==3 and calls[0]['entries'][0]['media_basis']==before
        assert all('media_basis' not in call['entries'][0] for call in calls[1:])
        assert not errors, errors


def test_late_capture_keeps_dirty_empty_basis_until_explicit_adoption(browser, tmp_path):
    with media_case(browser, tmp_path, mode='required', capability=True) as (page, errors):
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('yes')
        assert draft(page)['answers'][0]['media_basis']==[]
        page.evaluate('fixture.holdCapture=true')
        page.get_by_role('button', name='固定媒体供评测', exact=True).first.click()
        page.wait_for_function('fixture.held.some(i=>i.kind==="capture")')
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('no')
        page.evaluate('flushHeld("capture")')
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_be_visible()
        expect(page.locator('.case-media-answer')).to_contain_text('此答案仍保留编辑时的依据')
        assert draft(page)['answers'][0]['media_basis']==[]
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        assert page.evaluate('fixture.calls.find(i=>i.name==="analysis-annotations-save").payload.entries[0].media_basis.length===0')
        page.get_by_label('采用当前固定媒体作为本题依据 结果是否符合预期', exact=True).click()
        assert len(draft(page)['answers'][0]['media_basis'])==1
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v2')
        assert draft(page)['answers'][0]['media_basis'][0]['field']=='picture'
        assert not errors, errors


def test_storage_denial_no_capture_and_malformed_ready_is_never_rendered(browser, tmp_path):
    with media_case(browser, tmp_path, mode='required') as (page, errors):
        page.evaluate('()=>{window.originalSet=Storage.prototype.setItem;Storage.prototype.setItem=function(){throw new DOMException("injected","QuotaExceededError");};}')
        page.get_by_role('button', name='固定媒体供评测', exact=True).first.click()
        expect(page.locator('.analysis-case')).to_contain_text('无法持久保存原请求身份')
        assert page.evaluate('fixture.captures.length===0&&new ForgeAnalysisState.Store().pending(fixture.project).length===0')
        page.evaluate('''()=>{
          Storage.prototype.setItem=originalSet;fixture.mode="ready";const lookup=fixture.fieldMedia.lookup;
          fixture.fieldMedia.lookup=async request=>{const value=await lookup(request);value.reference.case_key="999";return value;};
        }''')
        page.get_by_role('button', name='重新核对媒体', exact=True).first.click()
        expect(page.locator('.case-media-field').first).to_contain_text('媒体引用与当前固定来源、Case 或字段不一致')
        expect(page.get_by_role('button', name='预览图片', exact=True)).to_have_count(0)
        assert page.evaluate('fixture.resolves.length===0&&fixture.releases.length===1')
        assert not errors, errors


def test_inexact_media_receipt_preserves_draft_then_original_receipt_recovers(browser, tmp_path):
    with media_case(browser, tmp_path, capability=True) as (page, errors):
        page.get_by_label('标注 结果是否符合预期', exact=True).select_option('yes')
        before=draft(page)['answers'][0]['media_basis']
        page.evaluate('''()=>{
          const api=fixture.api;fixture.api=async(...args)=>{const result=await api(...args);
            if(args[0]==="analysis-annotations-save"){delete result.heads[0].media_basis;}return result;};
          remount();
        }''')
        expect(page.get_by_label('标注 结果是否符合预期', exact=True)).to_have_value('yes')
        page.get_by_role('button', name='保存本 Case 标注', exact=True).click()
        expect(page.locator('.analysis-case')).to_contain_text('保存收据未回传本次精确媒体依据')
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_be_visible()
        assert draft(page)['answers'][0]['dirty']
        assert draft(page)['answers'][0]['media_basis']==before
        page.get_by_role('button', name='核对 Case 原请求', exact=True).click()
        expect(page.get_by_role('button', name='核对 Case 原请求', exact=True)).to_have_count(0)
        assert not draft(page)['answers'][0]['dirty']
        assert draft(page)['answers'][0]['media_basis']==before
        for width in (1440, 390, 320):
            page.set_viewport_size(dict(width=width, height=1100))
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            page.screenshot(path=str(tmp_path / ('case-media-'+str(width)+'.png')), full_page=True)
        assert not errors, errors
