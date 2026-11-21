"""Real Notebook 7, HTTP/WebSocket gateway and native kernel acceptance."""
import json
import os
from pathlib import Path
import time
from urllib.parse import quote, urlsplit
import http.client

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws, registry
from demiflow.workbench import project_operations as project_ops
from test_forge_public_api import service
from test_forge_markdown_reading import browser

PYTHON = Path(os.environ.get('DEMIFLOW_TEST_NOTEBOOK_PYTHON',
    '/yzp/zhaozy/yangzepeng/0905/_demiflow/envs/native-jupyter-20261119/bin/python'))
pytestmark = pytest.mark.skipif(not PYTHON.exists(), reason='Native Notebook optional environment is not installed')


@pytest.fixture
def native_project(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'runtime'))
    root=tmp_path/'project';root.mkdir()
    (root/'README.md').write_text('# Native Notebook fixture\n')
    path=tmp_path/'projects.json'
    key=registry.add(path,root,name='Native Notebook')['workspace_id']
    value=json.loads(path.read_text());value['format']=registry.FORMAT_V3
    value['projects'][key].pop('allow_runs',None)
    value['projects'][key]['permissions']={**{k:False for k in registry.PERMISSIONS},
        **{k:False for k in registry.CAPABILITIES},'analysis_write':True,'analysis_execute':True}
    path.write_text(json.dumps(value))
    import lance, pyarrow as pa
    data=root/'data';data.mkdir();lance.write_dataset(pa.table({'name':['alpha','beta','gamma'],'score':[1,2,3]}),data/'samples.lance')
    (root/'tables').mkdir()
    ws.add_location(root,'local',data)
    project_ops.attach_table(root,'samples',relative_uri='samples.lance',version=1,
                    dataset_id='samples',schema_name='fixture',schema_version='1')
    return root,path,key


def call(app, method, path, body=None, *, cookie=None, origin=True, token=False):
    connection=http.client.HTTPConnection('127.0.0.1',app.server_port,timeout=60)
    headers={'Content-Type':'application/json'}
    if token:headers['X-Forge-Token']=app.token
    if cookie:headers['Cookie']=cookie
    if origin:headers['Origin']=app.origin if origin is True else origin
    try:
        connection.request(method,path,json.dumps(body).encode() if body is not None else None,headers)
        response=connection.getresponse();raw=response.read()
        return response.status,dict(response.getheaders()),json.loads(raw) if raw and 'json' in response.getheader('Content-Type','') else raw
    finally:connection.close()


def grants(path, key, **changes):
    value=json.loads(path.read_text());value['projects'][key]['permissions'].update(changes)
    path.write_text(json.dumps(value))


def test_native_gateway_permissions_files_and_owned_cleanup(native_project,tmp_path):
    import psutil
    root,path,key=native_project
    grants(path,key,analysis_execute=False)
    with service(None,registry=path,notebook_python=PYTHON) as app:
        base='/jupyter/'+key+'/'
        assert call(app,'GET',base+'api')[0]==403
        code,headers,opened=call(app,'POST','/api/workspaces/'+key+'/notebook-open',{},token=True)
        assert code==200
        cookie=headers['Set-Cookie'].split(';')[0]
        assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Strict' in headers['Set-Cookie']
        instance=app.notebooks.instances[key]
        assert instance.token not in json.dumps(opened) and app.token not in json.dumps(opened)
        assert call(app,'GET',base+'api',cookie=cookie)[0]==200
        assert call(app,'GET',base+'api',cookie=cookie,origin='http://other.invalid')[0]==403
        assert call(app,'POST',base+'api/kernels',{'name':'python3'},cookie=cookie,origin=False)[0]==403
        other=root.parent/'other';other.mkdir()
        otherkey=registry.add(path,other,name='Other project')['workspace_id']
        assert call(app,'GET','/jupyter/'+otherkey+'/api',cookie=cookie)[0]==403
        assert otherkey not in app.notebooks.instances
        (root/'notebooks/outside').symlink_to(tmp_path,target_is_directory=True)
        assert call(app,'GET',base+'api/contents/outside',cookie=cookie)[0]==403
        assert call(app,'GET',base+'files/outside/project/README.md',cookie=cookie)[0]==403
        (root/'notebooks/large.txt').write_bytes(b'x'*(16*1024**2+1))
        assert call(app,'GET',base+'files/large.txt',cookie=cookie)[0]==413
        assert call(app,'GET',base+'api/contents/%2e%2e/README.md',cookie=cookie)[0]==403
        assert call(app,'POST',base+'api/shutdown',{},cookie=cookie)[0]==403
        assert call(app,'POST',base+'api/kernels',{'name':'python3'},cookie=cookie)[0]==403
        grants(path,key,analysis_execute=True)
        status,_,kernel=call(app,'POST',base+'api/kernels',{'name':'python3'},cookie=cookie)
        assert status==201,kernel
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as pool:
            starts=list(pool.map(lambda _:call(app,'POST',base+'api/kernels',{'name':'python3'},cookie=cookie)[0],range(2)))
        assert sorted(starts)==[201,429]
        assert len(call(app,'GET',base+'api/kernels',cookie=cookie)[2])==2
        server_process=psutil.Process(instance.process.pid)
        descendants=server_process.children(recursive=True)
        assert descendants
        grants(path,key,analysis_write=False,analysis_execute=False)
        assert call(app,'PUT',base+'api/contents/rejected.ipynb',{},cookie=cookie)[0]==403
        # URL encoding must not downgrade execute requests to write requests.
        grants(path,key,analysis_write=True)
        assert call(app,'POST',base+'api/%6bernels',{'name':'python3'},cookie=cookie)[0]==403
        assert call(app,'POST',base+'api/%73essions',{},cookie=cookie)[0]==403
        deadline=time.monotonic()+25
        while instance.process.poll() is None and time.monotonic()<deadline:time.sleep(.1)
        assert instance.process.poll() is not None
        assert not (root/'notebooks/rejected.ipynb').exists()
    assert not server_process.is_running()
    assert all(not child.is_running() or child.status()==psutil.STATUS_ZOMBIE for child in descendants)


def test_dictionary_read_uses_fixed_binding(native_project,monkeypatch,tmp_path):
    from demiflow.workbench.notebooks.data import sample
    from demiflow.forge.schema import ForgeError
    root,path,key=native_project
    monkeypatch.setenv('DEMIFLOW_NOTEBOOK_ROOT',str(root))
    binding=project_ops.describe_table(root,'samples')['binding']['digest']
    assert sample('samples',binding=binding,columns=['name'],limit=2).name.tolist()==['alpha','beta']
    with pytest.raises(ForgeError):sample('samples',binding='f'*64,columns=['name'])


def test_native_edit_execute_save_reopen_and_dictionary(browser,native_project,tmp_path):
    root,path,key=native_project
    with service(None,registry=path,notebook_python=PYTHON) as app:
        context=browser.new_context(viewport={'width':1440,'height':900})
        page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(error.stack))
        network=[]
        page.on('requestfailed',lambda request:network.append(dict(path=urlsplit(request.url).path,error=request.failure)))
        page.on('response',lambda response:network.append(dict(path=urlsplit(response.url).path,status=response.status)) if response.status>=400 else None)
        try:
            view=quote(json.dumps(dict(v=2,workspace=key,kind='tables')))
            page.goto(app.origin+'/#token='+app.token+'&view='+view)
            frame=page.frame_locator('iframe.native-notebook-frame')
            expect(frame.locator('.jp-Notebook')).to_be_visible(timeout=60000)
            expect(page.get_by_role('searchbox',name='搜索数据字典')).to_be_hidden()
            page.get_by_role('button',name='搜索数据字典',exact=True).click()
            page.get_by_role('searchbox',name='搜索数据字典').fill('missing')
            expect(page.locator('.dictionary-table:visible')).to_have_count(0)
            page.keyboard.press('Escape')
            expect(page.locator('.dictionary-table:visible')).to_have_count(1)
            frame.locator('body').evaluate('async () => { const app=window.jupyterapp; await app.restored; const p=app.shell.currentWidget; await p.context.ready; await p.sessionContext.ready; }')
            editor=frame.locator('.jp-CodeCell .cm-content').first
            editor.fill('from pathlib import Path\nimport demiflow\n'
                + 'assert Path(demiflow.__file__).resolve().is_relative_to(Path(' + repr(str(Path(__file__).resolve().parents[1])) + '))\n'
                + 'answer = 40 + 2\nprint("native-ok", answer)')
            editor.press('Shift+Enter')
            expect(frame.locator('.jp-OutputArea-output')).to_contain_text('native-ok 42',timeout=30000)
            session=page.locator('iframe.native-notebook-frame').evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.id')
            from test_workbench_navigation_resize_browser import drag
            separator=page.get_by_role('separator',name='调整数据字典目录宽度')
            before_drag=page.locator('iframe.native-notebook-frame').bounding_box()['width']
            drag(page,separator,90)
            page.wait_for_function('width=>document.querySelector("iframe.native-notebook-frame").getBoundingClientRect().width<width-80',arg=before_drag)
            assert page.locator('iframe.native-notebook-frame').evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.id')==session
            expect(frame.locator('.jp-OutputArea-output')).to_contain_text('native-ok 42')
            width_before=page.locator('iframe.native-notebook-frame').bounding_box()['width']
            page.get_by_role('button',name='数据字典目录',exact=True).click()
            expect(page.locator('.data-dictionary')).to_be_hidden()
            page.wait_for_function('width=>document.querySelector("iframe.native-notebook-frame").getBoundingClientRect().width>width',arg=width_before)
            expect(frame.locator('.jp-OutputArea-output')).to_contain_text('native-ok 42')
            page.get_by_role('button',name='数据字典目录',exact=True).click()
            expect(page.locator('.data-dictionary')).to_be_visible()
            assert page.locator('iframe.native-notebook-frame').evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.id')==session
            page.screenshot(path=str(tmp_path/'native-notebook.png'))
            print('SCREENSHOT',tmp_path/'native-notebook.png')
            # Use the native document context to save, exactly as File -> Save.
            page.locator('iframe.native-notebook-frame').evaluate("f=>f.contentWindow.jupyterapp.commands.execute('docmanager:save')")
            page.wait_for_timeout(500)
            saved=json.loads((root/'notebooks/分析.ipynb').read_text())
            assert 'native-ok' in json.dumps(saved['cells'])
            page.get_by_text('samples',exact=True).first.click()
            insert=page.get_by_role('button',name='插入读取代码')
            expect(insert).to_be_visible(timeout=30000);insert.click()
            expect(frame.locator('.jp-CodeCell .cm-content').filter(has_text='from demiflow.workbench.notebooks.data')).to_be_visible()
            assert len(json.loads((root/'notebooks/分析.ipynb').read_text())['cells'][0]['outputs'])==1
            active=frame.locator('.jp-CodeCell.jp-mod-active .cm-content')
            active.press('Shift+Enter')
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='alpha')).to_be_visible(timeout=30000)
            page.locator('iframe.native-notebook-frame').evaluate("f=>f.contentWindow.jupyterapp.commands.execute('docmanager:save')")
            page.reload()
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='native-ok 42')).to_be_visible(timeout=60000)
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='alpha')).to_be_visible()
            assert not page.get_by_role('button',name='收起数据目录').count()
            assert not page.get_by_role('button',name='保存 Notebook',exact=True).count()
            assert not errors, "\n".join(errors)
        finally:
            (tmp_path/'browser-errors.json').write_text(json.dumps(errors,indent=2))
            (tmp_path/'network-errors.json').write_text(json.dumps(network,indent=2))
            page.screenshot(path=str(tmp_path/'last-page.png'))
            for f in page.frames:
                if '/jupyter/' in f.url:
                    (tmp_path/'frame.txt').write_text(f.locator('body').inner_text())
                    try:
                        facts=f.evaluate('''() => {
                          const app=window.jupyterapp;const panel=app?.shell?.currentWidget;
                          return {url:location.pathname, app:!!app, shell:Object.keys(app?.shell||{}),panel:panel?.constructor?.name,
                            source:panel?.content?.model?.toJSON?.(),session:panel?.sessionContext?.session?.kernel?.status};
                        }''')
                        (tmp_path/'frame.json').write_text(json.dumps(facts,ensure_ascii=False,indent=2))
                    except Exception: pass
            if app.notebooks:
                for instance in app.notebooks.instances.values():
                    text=(instance.state/'server.log').read_text()
                    (tmp_path/'jupyter.log').write_text(text.replace(instance.token,'REDACTED'))
            context.close()


def test_native_cold_loads_do_not_drop_script_chunks(browser,native_project,tmp_path):
    _,path,key=native_project
    facts=[]
    with service(None,registry=path,notebook_python=PYTHON) as app:
        try:
            for index in range(4):
                context=browser.new_context(viewport={'width':1440,'height':900})
                page=context.new_page();row=dict(index=index,network=[],errors=[]);facts.append(row)
                page.on('response',lambda response:row['network'].append(dict(path=urlsplit(response.url).path,status=response.status)) if response.status>=400 else None)
                page.on('pageerror',lambda error:row['errors'].append(str(error)))
                try:
                    view=quote(json.dumps(dict(v=2,workspace=key,kind='tables')))
                    page.goto(app.origin+'/#token='+app.token+'&view='+view)
                    frame=page.frame_locator('iframe.native-notebook-frame')
                    expect(frame.locator('.jp-Notebook')).to_be_visible(timeout=60000)
                    frame.locator('body').evaluate('async()=>{await window.jupyterapp.restored;await window.jupyterapp.shell.currentWidget.sessionContext.ready;}')
                    assert not row['errors'] and not row['network'],row
                finally:context.close()
        finally:(tmp_path/'cold-loads.json').write_text(json.dumps(facts,indent=2))


def test_native_interrupt_restart_drafts_project_switch_and_rename(browser,native_project,tmp_path):
    root,path,key=native_project
    other=tmp_path/'second';other.mkdir();(other/'README.md').write_text('# Second project')
    otherkey=registry.add(path,other,name='Second project')['workspace_id']
    grants(path,otherkey,analysis_write=True,analysis_execute=True)
    outside=tmp_path/'outside.txt';outside.write_text('separate input')
    draft={'v':1,'items':[{'project':key,'id':'old-draft','revision':1,'document':{
        'title':'Retained draft','cells':[{'id':'one','language':'python','source':'print("old draft")'},
        {'id':'two','language':'sql','source':'select * from samples'}]}}]}
    with service(None,registry=path,notebook_python=PYTHON) as app:
        context=browser.new_context(viewport={'width':1440,'height':900})
        page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.add_init_script('localStorage.setItem("demiflow-analysis-drafts-v1",'+json.dumps(json.dumps(draft))+')')
        try:
            view=quote(json.dumps(dict(v=2,workspace=key,kind='tables')))
            page.goto(app.origin+'/#token='+app.token+'&view='+view)
            selector='iframe.native-notebook-frame:not([hidden])'
            frame=page.frame_locator(selector)
            expect(frame.locator('.jp-Notebook')).to_be_visible(timeout=60000)
            ready='async f=>{const a=f.contentWindow.jupyterapp;await a.restored;await a.shell.currentWidget.context.ready;await a.shell.currentWidget.sessionContext.ready;}'
            page.locator(selector).evaluate(ready)
            migrated=list((root/'notebooks').glob('恢复草稿-*.ipynb'))
            assert len(migrated)==1 and json.loads(migrated[0].read_text())['cells'][1]['cell_type']=='raw'
            assert json.loads(page.evaluate('localStorage.getItem("demiflow-analysis-drafts-v1")'))==draft
            migration_bytes=migrated[0].read_bytes()
            editor=frame.locator('.jp-CodeCell .cm-content').first
            editor.fill('unsaved_note = "keep this draft"')
            kernel_id=page.locator(selector).evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.kernel.id')
            page.get_by_role('button',name='Readme',exact=True).click()
            expect(page.get_by_role('heading',name='Native Notebook fixture',exact=True)).to_be_visible()
            assert page.locator('iframe.native-notebook-frame').count()==1
            assert not page.locator(selector).count()
            page.get_by_role('button',name='数据',exact=True).click()
            expect(editor).to_contain_text('keep this draft',timeout=30000)
            assert page.locator(selector).evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.kernel.id')==kernel_id
            # The second project gets its own notebook, draft and kernel.
            page.locator('#project-switcher').click()
            page.locator('#project-list [data-workspace="'+otherkey+'"]').click()
            page.get_by_role('button',name='数据',exact=True).click()
            expect(frame.locator('.jp-Notebook')).to_be_visible(timeout=60000)
            page.locator(selector).evaluate(ready)
            assert 'keep this draft' not in frame.locator('.jp-Notebook').inner_text()
            assert not list((other/'notebooks').glob('恢复草稿-*.ipynb'))
            page.locator('#project-switcher').click()
            page.locator('#project-list [data-workspace="'+key+'"]').click()
            page.get_by_role('button',name='数据',exact=True).click()
            expect(editor).to_contain_text('keep this draft',timeout=30000)
            assert migrated[0].read_bytes()==migration_bytes
            editor.fill('import time\nprint("started", flush=True)\ntime.sleep(45)')
            editor.press('Shift+Enter')
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='started')).to_be_visible(timeout=30000)
            page.locator(selector).evaluate("f=>f.contentWindow.jupyterapp.commands.execute('notebook:interrupt-kernel')")
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='KeyboardInterrupt')).to_be_visible(timeout=15000)
            page.locator(selector).evaluate('f=>f.contentWindow.jupyterapp.shell.currentWidget.sessionContext.session.kernel.restart()')
            editor.fill('from pathlib import Path\nprint(Path('+repr(str(outside))+').read_text())\nprint("fresh kernel", "time" not in globals())')
            editor.press('Shift+Enter')
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='fresh kernel True')).to_be_visible(timeout=30000)
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='separate input')).to_be_visible()
            page.locator(selector).evaluate("async f=>{const p=f.contentWindow.jupyterapp.shell.currentWidget;await p.context.rename('验证.ipynb');await p.context.save();}")
            page.reload()
            expect(frame.locator('.jp-OutputArea-output').filter(has_text='fresh kernel True')).to_be_visible(timeout=60000)
            assert (root/'notebooks/验证.ipynb').is_file()
            for width in (1440,390):
                page.set_viewport_size({'width':width,'height':900})
                expect(page.get_by_role('complementary',name='数据字典')).to_be_visible()
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
                page.screenshot(path=str(tmp_path/('native-'+str(width)+'.png')))
            assert not errors,errors
        finally:
            context.close()


def test_legacy_saved_sources_copy_once_without_mutating_history(native_project):
    from types import SimpleNamespace
    from demiflow.workbench.notebooks.service import Notebooks
    root,path,key=native_project
    notebook={'title':'Saved history','cells':[{'id':'original','language':'python','source':'print(41)'}]}
    calls=[]
    def metadata(project,name,payload):
        calls.append((project,name,payload))
        if name=='documents':return {'items':[{'id':'history','revision':7}]}
        assert name=='notebook' and payload['revision']==7
        return {'document':notebook,'next_offset':None}
    directory=root/'notebooks';directory.mkdir()
    owner=SimpleNamespace(server=SimpleNamespace(analysis=SimpleNamespace(call=metadata)),_create=Notebooks._create)
    Notebooks._import_legacy(owner,key,directory,{'analysis_write':True})
    target=directory/'历史-history.ipynb'
    copy=json.loads(target.read_text())
    assert copy['metadata']['demiflow']['legacy']=={'id':'history','revision':7}
    assert copy['cells'][0]['outputs']==[] and copy['cells'][0]['source']=='print(41)'
    target.write_text('native user edit')
    Notebooks._import_legacy(owner,key,directory,{'analysis_write':True})
    assert target.read_text()=='native user edit'
    assert all(name in {'documents','notebook'} for _,name,_ in calls)
    assert notebook['cells'][0]['source']=='print(41)'


def test_idle_native_server_capacity_recycles_without_rebinding_roots(native_project,tmp_path):
    from demiflow.forge.schema import ForgeError
    root,path,key=native_project
    projects=[key]
    grants(path,key,analysis_execute=False)
    for index in range(4):
        directory=tmp_path/('project-'+str(index));directory.mkdir()
        identity=registry.add(path,directory,name='Project '+str(index))['workspace_id']
        grants(path,identity,analysis_write=True)
        projects.append(identity)
    with service(None,registry=path,notebook_python=PYTHON) as app:
        instances=[app.notebooks.get(identity) for identity in projects]
        assert len(app.notebooks.instances)==4
        assert instances[0].process.poll() is not None
        assert not instances[0].state.exists()
        # Retiring an idle process never permits an old project session to be
        # rebound to a new physical root behind a cached browser document.
        original=app.notebooks.authorize
        app.notebooks.authorize=lambda identity:(root.parent/'wrong',{'analysis_write':True,'analysis_execute':False}) if identity==key else original(identity)
        with pytest.raises(ForgeError,match='root changed'):app.notebooks.get(key)


@pytest.mark.parametrize('kind',['table','table_request'])
def test_dictionary_old_locator_does_not_silently_adopt_new_binding(browser,native_project,kind):
    from demiflow.forge.platform import content_digest
    root,path,key=native_project
    original=project_ops.describe_table(root,'samples')['binding']['digest']
    rebind_sample(root)
    assert project_ops.describe_table(root,'samples')['binding']['digest']!=original
    with service(None,registry=path) as app:
        context=browser.new_context();page=context.new_page();requests=[]
        page.on('request',lambda r:requests.append(r.url))
        try:
            view={'v':2,'workspace':key,'kind':kind,'alias':'samples',
                  'expected_binding' if kind=='table_request' else 'binding_digest':original}
            page.goto(app.origin+'/#token='+app.token+'&view='+quote(json.dumps(view)))
            expect(page.locator('.dictionary-table[open] .dictionary-fields')).to_contain_text('变化',timeout=30000)
            assert not page.get_by_role('button',name='插入读取代码').count()
            assert any('expected_binding='+original in url for url in requests)
            assert page.evaluate('ForgeView.decodeView(location.hash).kind')==kind
        finally:context.close()


def rebind_sample(root):
    import lance, pyarrow as pa
    from demiflow.forge.platform import content_digest
    control=ws.read_control(root/'tables/samples.json')
    lance.write_dataset(pa.table({'name':['replacement'],'score':[99]}),root/'data/replacement.lance')
    project_ops.attach_table(root,'samples',relative_uri='replacement.lance',version=1,dataset_id='replacement',
                    schema_name='fixture',schema_version='1',expected=content_digest(control))


def test_dictionary_resolved_request_stays_fixed_across_rebind_and_navigation(browser,native_project):
    root,path,key=native_project
    original=project_ops.describe_table(root,'samples')['binding']['digest']
    with service(None,registry=path) as app:
        context=browser.new_context();page=context.new_page()
        try:
            view={'v':2,'workspace':key,'kind':'table_request','alias':'samples'}
            page.goto(app.origin+'/#token='+app.token+'&view='+quote(json.dumps(view)))
            page.wait_for_function("digest=>ForgeView.decodeView(location.hash)?.binding_digest===digest",arg=original,timeout=30000)
            rebind_sample(root)
            expect(page.locator('.dictionary-table[open] .dictionary-fields')).to_contain_text('变化',timeout=30000)
            assert page.evaluate('ForgeView.decodeView(location.hash).binding_digest')==original
            page.get_by_role('button',name='Readme',exact=True).click()
            page.get_by_role('button',name='数据',exact=True).click()
            expect(page.locator('.dictionary-table[open] .dictionary-fields')).to_contain_text('变化',timeout=30000)
            assert page.evaluate('ForgeView.decodeView(location.hash).binding_digest')==original
            assert not page.get_by_role('button',name='插入读取代码').count()
        finally:context.close()


def test_reuse_annotation_tool_preset_in_native_notebook(browser,native_project):
    root,path,key=native_project
    folder=root/'data_tools'
    folder.mkdir()
    (folder/'ratings.py').write_text(
        '__demiflow_data_tools__={"review_images":"annotation","card":"annotation_component","count_accepted":"analysis"}\n'
        'def count_accepted(decisions):\n    """统计已接受样本数。"""\n    return decisions.count("通过")\n'
        'def card(row):\n    """展示一条候选。"""\n    return row\n'
        'def review_images(batch, labels, *, page_size=5):\n'
        '    """按批次和标签打开候选图审核。"""\n'
        '    import ipywidgets as widgets\n'
        '    from pathlib import Path\n'
        '    button=widgets.Button(description=labels[0])\n'
        '    status=widgets.Label(value=f"{batch}: {labels[0]} ({page_size})")\n'
        '    def mark(_):\n'
        '        Path("sample-review.txt").write_text(f"{batch}/sample-1 {labels[0]}")\n'
        '        status.value="已记录 sample-1"\n'
        '    button.on_click(mark)\n'
        '    return widgets.VBox([status,button])\n')
    (folder/'ratings.presets.json').write_text(json.dumps(
        {'review_images':{'候选图质检':{'batch':'batch-12','labels':['通过','退回'],'page_size':20}}},ensure_ascii=False))
    with service(None,registry=path,notebook_python=PYTHON) as app:
        page=browser.new_page(viewport={'width':1440,'height':950})
        try:
            view=quote(json.dumps(dict(v=2,workspace=key,kind='tables')))
            page.goto(app.origin+'/#token='+app.token+'&view='+view)
            frame=page.frame_locator('iframe.native-notebook-frame')
            expect(frame.locator('.jp-Notebook')).to_be_visible(timeout=60000)
            frame.locator('body').evaluate('async()=>{const a=window.jupyterapp;await a.restored;const p=a.shell.currentWidget;await p.context.ready;await p.sessionContext.ready;}')
            dataTabs=page.get_by_role('tablist',name='数据分类')
            expect(dataTabs.get_by_role('tab')).to_have_text(['数据字典','分析工具','标注工具'])
            expect(dataTabs.get_by_role('tab',name='数据字典')).to_have_attribute('aria-selected','true')
            kernel=frame.locator('body').evaluate('()=>window.jupyterapp.shell.currentWidget.sessionContext.session.kernel.id')
            frame.locator('.jp-CodeCell .cm-content').first.fill('original_draft = 1')
            page.get_by_role('tab',name='标注工具',exact=True).click()
            expect(page.get_by_role('button',name='card 组件',exact=True)).to_be_visible()
            folder=os.environ.get('FORGE_ACCEPTANCE_DIR')
            if folder:page.screenshot(path=str(Path(folder)/'annotation-library.png'))
            page.set_viewport_size({'width':390,'height':844})
            for tab in dataTabs.get_by_role('tab').all():expect(tab).to_be_in_viewport()
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            if folder:page.screenshot(path=str(Path(folder)/'annotation-library-mobile.png'))
            page.set_viewport_size({'width':1440,'height':950})
            page.get_by_role('button',name='查看 review_images 定义',exact=True).click()
            dialog=page.get_by_role('dialog',name='标注工具')
            dialog.get_by_label('工具预设').select_option(label='候选图质检')
            expect(dialog.get_by_label('batch',exact=True)).to_have_value('batch-12')
            dialog.get_by_label('batch',exact=True).fill('batch-13')
            dialog.get_by_text('另存参数预设').click()
            dialog.get_by_label('新预设名称').fill('下一批')
            dialog.get_by_role('button',name='保存到此浏览器').click()
            expect(dialog.get_by_role('status')).to_have_text('预设已保存。')
            dialog.get_by_role('button',name='插入 Notebook').click()
            expect(dialog).to_have_count(0)
            cells=frame.locator('.jp-CodeCell')
            expect(cells.first).to_contain_text('original_draft')
            expect(cells.nth(1)).to_contain_text('from data_tools.ratings import review_images')
            expect(cells.nth(1).locator('.jp-OutputArea-output')).to_have_count(0)
            cells.nth(1).get_by_role('button',name='运行卡片',exact=True).click()
            expect(cells.nth(1).locator('.jp-OutputArea-output')).to_contain_text('batch-13: 通过 (20)',timeout=30000)
            assert not (root/'sample-review.txt').exists()
            cells.nth(1).get_by_role('button',name='通过',exact=True).click()
            expect(cells.nth(1).locator('.jp-OutputArea-output')).to_contain_text('已记录 sample-1')
            assert (root/'sample-review.txt').read_text()=='batch-13/sample-1 通过'
            page.get_by_role('tab',name='标注工具',exact=True).click()
            page.get_by_role('button',name='查看 review_images 定义',exact=True).click()
            dialog.get_by_label('工具预设').select_option(label='下一批')
            expect(dialog.get_by_label('page_size',exact=True)).to_have_value('20')
            if folder:page.screenshot(path=str(Path(folder)/'annotation-tool-preset.png'))
            dialog.get_by_role('button',name='关闭').click()
            page.get_by_role('button',name='查看 card 定义',exact=True).click()
            component=page.get_by_role('dialog',name='标注组件')
            expect(component).to_contain_text('from data_tools.ratings import card')
            expect(component.locator('.tok-keyword').first).to_have_text('def')
            expect(component.get_by_role('button',name='插入 Notebook')).to_have_count(0)
            component.get_by_role('button',name='插入导入语句').click()
            expect(cells.last).to_contain_text('from data_tools.ratings import card')
            page.get_by_role('button',name='搜索标注工具').click()
            page.get_by_role('searchbox',name='搜索标注工具').fill('card')
            expect(page.get_by_role('button',name='review_images 工具',exact=True)).to_have_count(0)
            page.keyboard.press('Escape')
            expect(page.get_by_role('button',name='review_images 工具',exact=True)).to_be_visible()
            dataTabs.get_by_role('tab',name='标注工具').focus()
            page.keyboard.press('ArrowLeft')
            expect(dataTabs.get_by_role('tab',name='分析工具')).to_be_focused()
            expect(page.get_by_role('button',name='card 组件',exact=True)).to_have_count(0)
            page.get_by_role('button',name='查看 count_accepted 定义',exact=True).click()
            analysis=page.get_by_role('dialog',name='分析工具')
            expect(analysis).to_contain_text('统计已接受样本数')
            analysis.get_by_role('button',name='插入导入语句').click()
            editor=cells.last.locator('.cm-content')
            expect(editor).to_contain_text('from data_tools.ratings import count_accepted')
            editor.fill('from data_tools.ratings import count_accepted\ncount_accepted(["通过", "退回", "通过"])')
            cells.last.get_by_role('button',name='运行卡片',exact=True).click()
            expect(cells.last.locator('.jp-OutputArea-output')).to_have_text('2',timeout=30000)
            dataTabs.get_by_role('tab',name='数据字典').click()
            expect(page.locator('.dictionary-table[data-table]').first).to_be_visible()
            assert frame.locator('body').evaluate('()=>window.jupyterapp.shell.currentWidget.sessionContext.session.kernel.id')==kernel
            expect(cells.first).to_contain_text('original_draft')
        finally:
            page.close()


def test_card_assistant_generates_editable_notebook_widgets(browser,native_project,tmp_path,monkeypatch):
    import threading
    from test_forge_development_chat import configuration
    root,registry_path,project=native_project
    fixture=tmp_path/'provider'
    fixture.mkdir()
    _,config,value=configuration(fixture)
    grant=value['projects'].pop('wfixture')
    value['projects'][project]=dict(grant,root=str(root),sources=['README.md'],writes=[],watch=[])
    config.write_text(json.dumps(value))
    tools=root/'data_tools'
    tools.mkdir()
    (tools/'scores.py').write_text('__demiflow_data_tools__={"mean_score":"analysis"}\n'
        'def mean_score(values):\n    """返回分组均值。"""\n    return sum(values)/len(values)\n')
    source=('from data_tools.scores import mean_score\nimport ipywidgets as w\n'
            'group = w.Dropdown(options=["A", "B"], description="分组")\n'
            'result = w.Label(value=f"平均分 {mean_score(scores[group.value])}")\n'
            'def update(change):\n    result.value = f"平均分 {mean_score(scores[group.value])}"\n'
            'group.observe(update, names="value")\nw.VBox([group,result])')
    with service(None,registry=registry_path,notebook_python=PYTHON,development_config=config) as app:
        # Substitute the provider reply only. The real HTTP routes, turn admission,
        # durable events, source catalog and Jupyter kernel remain in the path.
        development=app.development
        capabilities=development.capabilities
        monkeypatch.setattr(development,'capabilities',lambda *args:dict(capabilities(*args),notebook_cells=True))
        submitted=[]
        editing,edited=threading.Event(),threading.Event()
        def respond(turn,session):
            assert turn['operation']=='notebook_cell'
            submitted.append(turn['text'])
            if len(submitted)==2:
                editing.set()
                assert edited.wait(15)
            store=development.store
            code=source if len(submitted)==1 else 'print("保留手动修改")'
            store.event(turn['id'],'message.completed',dict(item_id='answer',text='切换分组可以查看这批样本的平均分。\n```python\n'+code+'\n```'))
            store.update(turn['id'],state='completed',cleanup='complete',dispatch='acknowledged')
            return store.public_turn(store.turn(turn['id']))
        monkeypatch.setattr(development,'_launch_provider',respond)
        page=browser.new_page(viewport={'width':1440,'height':950})
        try:
            view=quote(json.dumps(dict(v=2,workspace=project,kind='tables')))
            page.goto(app.origin+'/#token='+app.token+'&view='+view)
            frame=page.frame_locator('iframe.native-notebook-frame')
            page.get_by_role('tab',name='分析工具',exact=True).click()
            page.get_by_role('button',name='卡片助手',exact=True).wait_for(timeout=60000)
            cells=frame.locator('.jp-CodeCell')
            cells.first.locator('.cm-content').fill('# 本批样本\nimport pandas as pd\n'
                'scores = {"A":[1,3], "B":[3,5]}\n'
                'pd.DataFrame({"样本":["A-01","A-02","B-01","B-02"], "评分":[1,3,3,5]})')
            cells.first.get_by_role('button',name='运行卡片').click()
            expect(cells.first.locator('.jp-OutputArea-output')).to_contain_text('B-02',timeout=30000)
            page.get_by_role('button',name='新建卡片',exact=True).click()
            page.get_by_role('button',name='mean_score 分析').click()
            pane=page.get_by_role('complementary',name='卡片助手')
            separator=page.get_by_role('separator',name='调整卡片助手宽度')
            before=pane.bounding_box()['width']
            separator.focus();page.keyboard.press('Shift+ArrowLeft')
            expect(separator).to_have_attribute('aria-valuenow',str(round(before+40)))
            pane.get_by_role('textbox',name='卡片要求').fill('按分组比较平均分')
            pane.get_by_role('button',name='发送要求').click()
            card=cells.nth(1)
            expect(card.locator('.cm-content')).to_contain_text('from data_tools.scores import mean_score',timeout=30000)
            assert all(term in submitted[0] for term in ('mean_score','samples','scores ='))
            expect(card.locator('.jp-OutputArea-output')).to_have_count(0)
            card.get_by_role('button',name='运行卡片').click()
            output=card.locator('.jp-OutputArea-output')
            expect(output.locator('select')).to_be_visible(timeout=30000)
            output.locator('select').select_option(label='B')
            expect(output).to_contain_text('平均分 4.0',timeout=10000)
            shots=os.environ.get('FORGE_ACCEPTANCE_DIR')
            if shots:page.screenshot(path=str(Path(shots)/'notebook-cards-assistant.png'))
            card.get_by_role('button',name='编辑代码').click()
            editor=card.locator('.cm-content')
            editor.fill('manual_draft = 1')
            pane.get_by_role('textbox',name='卡片要求').fill('打印修改结果')
            pane.get_by_role('button',name='发送要求').click()
            assert editing.wait(15)
            expect(editor).to_contain_text('manual_draft = 1')
            editor.fill('manual_draft = 2')
            edited.set()
            pane.get_by_role('button',name='作为新卡片插入').click()
            expect(cells).to_have_count(3)
            expect(card).to_contain_text('manual_draft = 2')
            expect(cells.last).to_contain_text('print("保留手动修改")')
            page.set_viewport_size({'width':390,'height':844})
            expect(pane).to_be_visible()
            pane.get_by_role('button',name='关闭助手').click()
            expect(cells.last).to_be_in_viewport()
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        finally:page.close()
