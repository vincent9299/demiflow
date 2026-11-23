"""Shared navigation layout and gestures against the real workbench shell."""
from playwright.sync_api import expect
from demiflow.workbench.projects import registry
from test_workbench_project_definitions import project, original, snapshot, cached_snapshot
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


def drag(page, handle, delta):
    box=handle.bounding_box()
    x,y=box['x']+box['width']/2,box['y']+min(80,box['height']/2)
    page.mouse.move(x,y);page.mouse.down();page.mouse.move(x+delta,y,steps=8);page.mouse.up()
    expect(page.locator('.side-navigation-drag-shield')).to_have_count(0)


def test_all_navigation_resize_ellipsis_and_retained_width(project,tmp_path,browser):
    for name in ('select','evaluate'):
        (project/f'flows/{name}.py').write_text('from demiflow import data\ndef run(*, limit: int = 3, label: str = "demo"):\n    return data.from_items([{"item": label}]).limit(limit).take_all()\n')
    long_name='generate_very_long_prompt_name_that_should_stay_on_one_line.yaml'
    (project/'prompts/review.yaml').rename(project/'prompts'/long_name)
    index=tmp_path/'registry.json';registry.add(index,project)
    with service(None,registry=index) as app,opened(browser,app) as (page,requests):
        page.set_viewport_size(dict(width=1440,height=900))
        for kind,rail,label in [('development','.development-session-sidebar','会话目录'),('flows','.wb-flow-navigation','管道目录'),('tables','.data-dictionary','数据字典目录'),('overview','.document-directory','文档目录')]:
            page.locator(f'.function-nav [data-page={kind}]').click()
            page.wait_for_function('(kind)=>{const v=JSON.parse(new URLSearchParams(location.hash.slice(1)).get("view")||"{}");return (v.panel||v.kind)===kind;}',arg=kind)
            sidebar=page.locator(rail);expect(sidebar).to_be_visible()
            separator=page.get_by_role('separator',name='调整'+label+'宽度',exact=True)
            expect(separator).to_be_visible()
            before=sidebar.bounding_box()['width'];drag(page,separator,85)
            expect(separator).to_have_attribute('aria-valuenow',str(round(before+85)))
            assert abs(sidebar.bounding_box()['width']-before-85)<2
            separator.press('ArrowLeft');expected=before+75
            assert abs(sidebar.bounding_box()['width']-expected)<2
            collapse=sidebar.locator('.side-navigation-actions button').last
            name=collapse.get_attribute('aria-label');collapse.click()
            expect(separator).to_be_hidden();expect(sidebar).to_be_hidden()
            page.get_by_role('button',name=name,exact=True).click()
            expect(separator).to_be_visible();assert abs(sidebar.bounding_box()['width']-expected)<2
            if kind=='development':
                sidebar.locator('.dev-sidebar-tabs').get_by_role('button',name='算子',exact=True).click()
                page.get_by_role('tab',name='Prompt 算子',exact=True).click()
                summary=sidebar.locator('.op-tree-file>summary').filter(has_text=long_name)
                facts=summary.locator('span').evaluate('(n)=>({scroll:n.scrollWidth,width:n.clientWidth,wrap:getComputedStyle(n).whiteSpace,align:getComputedStyle(n).textAlign,height:n.getBoundingClientRect().height})')
                assert facts['scroll']>facts['width'] and facts['wrap']=='nowrap' and facts['align']=='left' and facts['height']<30
                assert summary.get_attribute('title')=='prompts/'+long_name
                prompt_btn=sidebar.get_by_role('button',name='P review',exact=True)
                expect(prompt_btn).to_be_visible()
                prompt_btn.click()
                expect(page.locator('.dev-operator-pane')).to_be_visible()
                expect(page.locator('.dev-op-badge.is-prompt')).to_have_text('Prompt 算子')
                expect(page.locator('.dev-operator-pane')).to_contain_text('Prompt 模板')
                expect(page.locator('.dev-operator-pane')).to_contain_text('输入变量')
                summary.click();expect(prompt_btn).to_be_hidden()
                summary.click();expect(prompt_btn).to_be_visible()
            page.reload();expect(sidebar).to_be_visible(timeout=45000)
            assert abs(sidebar.bounding_box()['width']-expected)<2
            # Narrow viewports clamp stored widths instead of overflowing the page.
            for width in (390,320):
                page.set_viewport_size(dict(width=width,height=900))
                page.evaluate('() => new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
                if kind=='development' and not sidebar.is_visible():page.get_by_role('button',name='会话列表',exact=True).click()
                if kind=='flows' and width==390:
                    expect(sidebar).to_be_hidden()
                    expect(separator).to_be_hidden()
                    reopen=page.get_by_role('button',name='管道目录',exact=True)
                    expect(reopen).to_have_attribute('aria-expanded','false')
                    reopen.click()
                    expect(reopen).to_have_attribute('aria-expanded','true')
                expect(separator).to_be_visible();separator.press('End')
                expect(sidebar).to_be_visible()
                assert sidebar.bounding_box()['width']<=width-140+1,(kind,width)
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            page.set_viewport_size(dict(width=1440,height=900))
            separator.dblclick()
            assert abs(sidebar.bounding_box()['width']-240)<2
        page.get_by_role('button',name='设置',exact=True).click()
        dialog=page.get_by_role('dialog',name='项目设置')
        expect(dialog).to_be_visible()
        expect(dialog).to_contain_text(str(project))
        # The user removed the duplicate AI panel; project settings still work.
        # Model selection and Forge permissions remain covered by composer tests.
        expect(dialog.get_by_role('button',name='重新检查连接')).to_have_count(0)
        expect(dialog.get_by_text('在哪里配置连接？',exact=True)).to_have_count(0)
        expect(dialog.locator('input[type=password]')).to_have_count(0)
        dialog.get_by_role('button',name='关闭',exact=True).click()
        expect(dialog).to_have_count(0)
