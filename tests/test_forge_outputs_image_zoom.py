"""Image zoom through the existing shared renderer, real PNG and Chromium.

The authenticated byte adapter is the explicit private fixture from the output
suite. Zoom must reuse its admitted image/source without another read or URL.
"""
from contextlib import contextmanager
import io
import json
import os
from pathlib import Path

import pytest
from playwright.sync_api import expect
from PIL import Image, ImageDraw

from demiflow.workbench.analysis import media
from test_forge_markdown_reading import browser
from test_forge_outputs_media import OBJECT
import test_forge_outputs_browser as previous

WEB = Path(os.environ.get('FORGE_ZOOM_WEB', str(previous.WEB)))


@pytest.fixture(autouse=True)
def selected_assets(monkeypatch):
    monkeypatch.setattr(previous, 'WEB', WEB)


def record(tmp_path, name, value):
    folder = Path(os.environ.get('FORGE_OUTPUT_EVIDENCE', tmp_path))
    (folder/(name+'.json')).write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')


def image_bytes(width, height):
    assert width*height <= 4*1024*1024 and max(width,height) <= 8192
    with Image.new('RGB', (width,height), '#f4f8ff') as image:
        draw=ImageDraw.Draw(image)
        for x in range(0,width,32):
            draw.line((x,0,x,height), fill='#87a9cf',width=1)
        for y in range(0,height,32):
            draw.line((0,y,width,y), fill='#87a9cf',width=1)
        draw.rectangle((width//4,height//4,width*3//4,height*3//4),fill='#269b85')
        if width>=384 and height>=288:
            draw.text((12,12),'Fixed PNG '+str(width)+' x '+str(height),fill='#1d365b')
        out=io.BytesIO();image.save(out,format='PNG');return out.getvalue()


@contextmanager
def image_card(browser, tmp_path, *, size=(384,288), width=390, card=280):
    with previous.page_for(browser) as (page,state,errors):
        page.set_viewport_size(dict(width=width,height=850))
        for name in ['analysis.css','analysis-case.css']:
            if (WEB/name).is_file():page.add_style_tag(path=str(WEB/name))
        page.add_style_tag(content='#mount{max-width:none}.zoom-fixture{min-width:0;width:100%}.zoom-fixture .analysis-case-field{max-width:100%;box-sizing:border-box}.zoom-fixture h3{font:600 14px sans-serif}')
        data=image_bytes(*size)
        material=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))/('material-%dx%d.png'%size)
        if material.exists():assert material.read_bytes()==data
        else:material.write_bytes(data)
        state['assets']['image']=data
        state['refs']['image']=media.probe_image(data,artifact_id=OBJECT,mime='image/png')
        previous.transport(page,state)
        page.evaluate('''({ref,card})=>{
          window.urls={created:0,revoked:0};window.resolves=0;
          const make=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
          URL.createObjectURL=value=>{urls.created++;return make(value);};URL.revokeObjectURL=value=>{urls.revoked++;return revoke(value);};
          const section=document.createElement('section');section.className='zoom-fixture analysis-case';
          const host=document.createElement('article');host.className='analysis-case-field';host.dataset.role='output';host.style.width=card+'px';
          const title=document.createElement('h3');title.textContent='固定来源图片 · '+ref.width+'×'+ref.height;host.append(title);section.append(host);document.getElementById('mount').append(section);
          window.zoomField=ForgeAnalysisRender.field({parent:host,format:'image',value:ref,resolveMedia:request=>{resolves++;return transport(request);}});
          fields.push(zoomField);
          window.zoomMetrics=()=>{
            const image=document.querySelector('img.analysis-media'),view=document.querySelector('.analysis-image-viewport'),rect=image?.getBoundingClientRect();
            return {width:rect?.width,height:rect?.height,naturalWidth:image?.naturalWidth,naturalHeight:image?.naturalHeight,
              viewWidth:view?.clientWidth,viewHeight:view?.clientHeight,scrollWidth:view?.scrollWidth,scrollHeight:view?.scrollHeight,
              scrollLeft:view?.scrollLeft,scrollTop:view?.scrollTop,src:image?.getAttribute('src'),
              resolves,urls:{...urls},media:ForgeAnalysisMedia.stats(),bodyWidth:document.documentElement.scrollWidth,windowWidth:innerWidth};
          };
        }''',dict(ref=state['refs']['image'],card=card))
        page.get_by_role('button',name='预览图片',exact=True).click()
        page.wait_for_function('document.querySelector("img.analysis-media")?.naturalWidth===%d'%size[0])
        page.evaluate('window.firstImage=document.querySelector("img.analysis-media")')
        try:yield page,state,errors
        finally:
            page.evaluate('clear()')
            assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
            assert page.evaluate('ForgeAnalysisMedia.stats().allocations')==0
            assert page.evaluate('ForgeAnalysisMedia.stats().pendingLoads')==0
            assert page.evaluate('urls.created===urls.revoked')
            assert page.locator('.analysis-image-viewport').count()==0
            assert not errors,errors


@pytest.mark.parametrize('size',[(384,288),(16,12),(1600,400),(384,1536),(16,8192)],ids=['small','tiny','wide','portrait','thin'])
@pytest.mark.parametrize('width,card',[(1440,960),(390,280)],ids=['wide-card','narrow-card'])
def test_image_zoom_visibly_increases_size_inside_bounded_card(browser,tmp_path,size,width,card):
    name='zoom-%dx%d-%d'%(*size,width)
    with image_card(browser,tmp_path,size=size,width=width,card=card) as (page,state,errors):
        before=page.evaluate('zoomMetrics()');reads=state['bytes_sent']
        previous.capture(page,tmp_path,name+'-fit.png')
        page.get_by_role('button',name='原位放大',exact=True).click()
        after=page.evaluate('zoomMetrics()')
        previous.capture(page,tmp_path,name+'-expanded.png')
        record(tmp_path,name,dict(before=before,expanded=after,reference=state['refs']['image']))
        # Assert the user's visible result. The original 384px behavior fails.
        assert after['width']>=before['width']*1.8
        assert after['height']>=before['height']*1.8
        assert max(after['width'],after['height'])<=4097
        assert after['width']*after['height']<=4*1024*1024+4096
        assert after['bodyWidth']<=after['windowWidth']
        assert after['src']==before['src'] and after['urls']==before['urls']
        assert after['media']==before['media']
        assert after['resolves']==1 and state['bytes_sent']==reads
        assert page.evaluate('document.querySelector("img.analysis-media")===firstImage')
        expect(page.get_by_role('button',name='缩回图片',exact=True)).to_have_attribute('aria-expanded','true')
        if after['width']>after['viewWidth']+1:assert after['scrollWidth']>after['viewWidth']
        page.get_by_role('button',name='缩回图片',exact=True).click()
        collapsed=page.evaluate('zoomMetrics()')
        assert abs(collapsed['width']-before['width'])<=1 and abs(collapsed['height']-before['height'])<=1
        assert collapsed['scrollLeft']==0 and collapsed['scrollTop']==0
        expect(page.get_by_role('button',name='原位放大',exact=True)).to_have_attribute('aria-expanded','false')
        record(tmp_path,name,dict(before=before,expanded=after,collapsed=collapsed,reference=state['refs']['image'],source_bytes_unchanged=state['bytes_sent']==reads))


def test_image_zoom_keyboard_resize_and_repeated_toggle_use_one_image(browser,tmp_path):
    with image_card(browser,tmp_path,width=1440,card=960) as (page,state,errors):
        zoom=page.get_by_role('button',name='原位放大',exact=True);zoom.focus();zoom.press('Space')
        region=page.get_by_role('region',name='放大图片，可使用方向键滚动，Esc 缩回',exact=True)
        expect(region).to_have_attribute('tabindex','0')
        assert page.get_by_role('button',name='缩回图片',exact=True).get_attribute('aria-controls')==region.get_attribute('id')
        page.set_viewport_size(dict(width=390,height=850))
        region.focus();region.press('ArrowRight')
        page.wait_for_function('document.querySelector(".analysis-image-viewport").scrollLeft>0')
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        region.press('Escape')
        expect(zoom).to_be_focused();expect(zoom).to_have_attribute('aria-expanded','false')
        page.evaluate('''()=>{for(let i=0;i<20;i++){document.querySelector('.analysis-image-zoom').click();}}''')
        result=page.evaluate('zoomMetrics()')
        assert result['resolves']==1 and result['urls']==dict(created=1,revoked=0)
        assert page.evaluate('document.querySelector("img.analysis-media")===firstImage')
        assert result['scrollLeft']==0 and result['scrollTop']==0
        record(tmp_path,'keyboard-resize-reuse',result)


def test_image_zoom_close_reopen_permission_and_detach_release_ownership(browser,tmp_path):
    with image_card(browser,tmp_path) as (page,state,errors):
        page.get_by_role('button',name='原位放大',exact=True).click()
        page.evaluate('window.oldZoom=document.querySelector(".analysis-image-zoom");window.oldRegion=document.querySelector(".analysis-image-viewport")')
        page.get_by_role('button',name='关闭预览',exact=True).click()
        assert page.locator('.analysis-image-viewport').count()==0
        assert page.evaluate('oldZoom.onclick===null&&oldRegion.onkeydown===null')
        state['allowed']=False;sent=state['bytes_sent']
        page.get_by_role('button',name='预览图片',exact=True).click()
        expect(page.get_by_role('status')).to_contain_text('准备不可用')
        assert page.locator('img.analysis-media').count()==0 and state['bytes_sent']==sent
        state['allowed']=True
        page.get_by_role('button',name='预览图片',exact=True).click()
        page.wait_for_function('document.querySelector("img.analysis-media")?.naturalWidth===384')
        expect(page.get_by_role('button',name='原位放大',exact=True)).to_have_attribute('aria-expanded','false')
        page.evaluate('oldZoom.click()')
        expect(page.get_by_role('button',name='原位放大',exact=True)).to_have_attribute('aria-expanded','false')
        page.get_by_role('button',name='原位放大',exact=True).click()
        page.evaluate('document.getElementById("mount").replaceChildren()')
        page.wait_for_function('ForgeAnalysisRender.stats().fields===0&&ForgeAnalysisMedia.stats().allocations===0')
        assert page.evaluate('urls')==dict(created=2,revoked=2)
        record(tmp_path,'close-reopen-detach',dict(urls=page.evaluate('urls'),stats=page.evaluate('ForgeAnalysisMedia.stats()'),resolves=page.evaluate('resolves')))


@pytest.mark.parametrize('scenario',[
    'test_review_cancelled_late_load_cannot_clear_reopened_preview_or_release_its_scope',
    'test_async_media_dispose_and_missing_codec_do_not_keep_budget',
    'test_review_cancelled_pending_loads_keep_a_bounded_charge_until_settled',
])
def test_zoom_keeps_existing_media_cancellation_and_budget_contract(browser,scenario):
    getattr(previous,scenario)(browser)
