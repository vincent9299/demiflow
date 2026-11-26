"""Real browser interactions for review outcomes, project isolation and save races."""
from playwright.sync_api import expect
from test_forge_chat_markdown_browser import chat_page


def mount(page, project='project-a'):
    page.evaluate('''project => {
      window.loopPanel?.dispose();document.querySelector('#loop-ui')?.remove();
      const make=tag=>document.createElement(tag),layout=make('section');layout.id='loop-ui';
      const files=make('div'),pane=make('div'),toggle=make('button');layout.append(files,pane,toggle);document.body.append(layout);
      window.loopReworks=[];window.loopRetries=0;window.loopSaves=[];window.loopSavePending=null;
      window.loopFile={text:'value = 1\\n',hash:'hash-one'};
      const api=async(name,query,body)=>{
        if(name==='project-changes')return {is_git:true,branch:'fixture',changes:[{path:'source.py',status:'M',sha256:loopFile.hash,added:1,removed:1}]};
        if(name==='project-file-diff')return {is_git:true,status:'M',changed:true,writable:true,original:'value = 0\\n',modified:loopFile.text,sha256:loopFile.hash};
        if(name==='project-files')return {entries:[],next_offset:null,root:'/fixture/'+project};
        if(name==='project-file-save'){
          loopSaves.push(body);
          return new Promise(resolve=>{window.loopSavePending=()=>{
            loopFile={text:body.text,hash:'hash-'+(loopSaves.length+1)};
            window.loopSavePending=null;resolve({sha256:loopFile.hash});
          };});
        }
        throw Error(name);
      };
      const iconButton=label=>{const button=make('button');button.textContent=label;button.setAttribute('aria-label',label);return button;};
      window.loopPanel=DevelopmentNavigation.filePanel({layout,filesHost:files,paneHost:pane,toggle,api,iconButton,
        project,disposeReaders(){},onRework:text=>loopReworks.push(text),onRequestReview:()=>loopRetries++});
      loopPanel.openFile('source.py','source');
    }''', project)
    page.locator('#loop-ui .cm-content').wait_for()


def test_editing_during_save_keeps_new_draft_and_next_save_uses_acknowledged_version(chat_page):
    page = chat_page; mount(page)
    editor = page.locator('#loop-ui .cm-content')
    editor.fill('value = 2\n')
    save = page.locator('#loop-ui .dev-action-save')
    save.click(); page.wait_for_function('!!loopSavePending')
    editor.fill('value = 3\n')
    page.evaluate('loopSavePending()')
    expect(editor).to_contain_text('value = 3')
    expect(save).to_be_enabled()
    save.click(); page.wait_for_function('!!loopSavePending')
    requests = page.evaluate('loopSaves')
    assert [r['text'] for r in requests] == ['value = 2\n', 'value = 3\n']
    assert [r['expected_sha256'] for r in requests] == ['hash-one', 'hash-2']
    page.evaluate('loopSavePending()')
    expect(save).to_be_disabled()


def test_incomplete_review_retries_and_more_than_twenty_blockers_remain_visible(chat_page):
    page = chat_page; mount(page)
    page.evaluate("loopPanel.recordChecks({review:'incomplete',reviewReset:true,reviewDetail:'模型超时，尚未形成结论'})")
    page.get_by_role('button', name='独立评审：未完成', exact=True).click()
    expect(page.locator('#loop-ui .dev-forge-rework')).to_have_count(0)
    page.get_by_role('button', name='重新评审', exact=True).click()
    assert page.evaluate('loopRetries') == 1
    page.evaluate("""() => {
      loopPanel.recordChecks({review:'invalid',reviewReset:true});
      for(let i=0;i<30;i++)loopPanel.recordChecks({reviewDetail:'需修正 · source.py:'+i+' · blocker-'+i});
    }""")
    expect(page.locator('#loop-ui .dev-forge-detail-list li')).to_have_count(30)
    page.get_by_role('button', name='带问题打回修改', exact=True).click()
    assert 'blocker-29' in page.evaluate('loopReworks[0]')


def test_approval_state_is_scoped_to_string_workspace_identifier(chat_page):
    page = chat_page; mount(page, 'project-a')
    page.locator('#loop-ui .dev-approve-all-btn').click()
    expect(page.locator('#loop-ui .dev-approve-all-btn')).to_be_hidden()
    mount(page, 'project-b')
    expect(page.locator('#loop-ui .dev-approve-all-btn')).to_be_visible()
    mount(page, 'project-a')
    expect(page.locator('#loop-ui .dev-approve-all-btn')).to_be_hidden()
