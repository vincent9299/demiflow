exec(open('/tmp/demiflow-ui-alignment-20261114/capture.py').read().split('with sync_playwright() as pw:')[0])
from demiflow.workbench.analysis import web_service
with sync_playwright() as pw:
    b=pw.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
    with pytest.MonkeyPatch.context() as mp:
        # Metadata-only fixture has no admitted execution runtime. Fix its stale
        # test adapter, not the product implementation, and do not run cells.
        mp.setattr(web_service.WebService,'execution_profile',lambda self:None)
        case_root=OUT/'case2';case_root.mkdir()
        with representative_case(b,case_root,mp) as f:
            p=f['page'];p.set_viewport_size(dict(width=1440,height=1000));shot(p,'current-case')
            p.get_by_role('button',name='Notebook',exact=True).click();shot(p,'current-notebook')
            p.get_by_role('button',name='Case 分析',exact=True).click()
            p.set_viewport_size(dict(width=390,height=844));shot(p,'current-mobile')
    b.close()
(OUT/'case-capture.json').write_text(json.dumps(dict(note='Real frontend, Records and Lance; metadata helper double with execution_profile=None; no runtime or media-execution acceptance',views=evidence),ensure_ascii=False,indent=2))
print('DONE',len(evidence),flush=True)
