"""The dictionary, Python imports and the cell CLI share real notebook identities."""
import json
from pathlib import Path
import os
import subprocess
import sys
import threading
import time

import pytest

from demiflow.workbench.notebooks import dictionary
from demiflow.workbench.notebooks.cell_commands import CellCommands
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.projects import definitions, workspace
from test_workbench_native_notebook import native_project, browser


def test_discovered_project_and_external_lance_and_installed_modules(native_project, tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.workbench.project_operations import project_table, project_sample
    project, _, _ = native_project
    external = tmp_path / 'shared'
    external.mkdir()
    workspace.add_location(project, 'shared', external)
    for base in (project, external):
        lance.write_dataset(pa.table({'name': ['one']}), base / 'batch.lance')
    (project/'other').symlink_to(external, target_is_directory=True)
    (external/'.private.lance').mkdir()
    chosen = {row['path']:row for row in dictionary.discover(project)['tables']}
    assert set(chosen) == {'batch.lance', 'data/samples.lance', 'shared/batch.lance'}
    for key in ('batch.lance','shared/batch.lance'):
        item = chosen[key]
        info = project_table(project,item['id'],expected=item['expected'])
        result = project_sample(project,item['id'],expected=item['expected'],version=info['version'],
                                binding=info['binding'],columns=['name'])
        assert result['rows'][0]['name']=='one'
    vendor = project/'data_tools'/'reviews'
    vendor.mkdir(parents=True)
    (vendor/'__init__.py').write_text('__demiflow_data_tools__={"label_picker":"tool"}\n'
                                      'def label_picker():\n    return "third party"\n')
    index=definitions.catalog(project,data_tools=True,include_platform=True)
    items=[item for item in index['definitions'] if item['symbol']=='label_picker']
    assert {(i['origin'],i['module']) for i in items} == {
        ('project','data_tools.reviews'),('platform','demiflow.data_tools.annotation')}
    assert len({i['id'] for i in items})==2
    for item in items:
        detail=definitions.read(project,id=item['id'],data_tools=True,include_platform=True)
        assert detail['module']==item['module'] and detail['source'].startswith('def label_picker')


def test_cell_cli_uses_turn_channel_and_claims_once(tmp_path):
    from demiflow.workbench.conversations.execution import runtime_paths
    from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox
    root=tmp_path/'project'; root.mkdir()
    home=tmp_path/'home';home.mkdir(mode=0o700)
    store=Store(tmp_path/'state',Limits(control_reserve=4096),initialize=True)
    ident=root_identity(root)
    sid=store.create('w',ident,'codex','c','p')['id']
    turn,_=store.submit('w',sid,'s',0,'edit cell',ident,'p','notebook_cell_cli')
    bridge=CellCommands(store,turn,time.time()+50)
    try:
        scope=SandboxSpec(read_paths=(str(root),),write_paths=(),private_home=str(home),
            runtime_paths=(*runtime_paths({'executable':sys.executable}),str(bridge.bin)),
            protected_paths=(),denied_paths=(str(store.root),),role='readonly',seconds=45)
        proc=launch_sandbox([str(bridge.bin/'data'),'cell','edit'],spec=scope,
                            env={'PATH':'/usr/bin:/bin','HOME':str(home)},cwd=root,control_socket=bridge.path)
        proc.stdin.write(b'print(42)');proc.stdin.close()
        deadline=time.monotonic()+20
        while not store.pending(turn['id']) and time.monotonic()<deadline and proc.poll() is None:
            time.sleep(.05)
        pending=store.pending(turn['id'])
        if not pending:pytest.fail(f'CLI did not reach the turn, rc={proc.poll()} '+(proc.stderr.read().decode() if proc.poll() is not None else ''))
        q=pending[0];assert json.loads(q['request'])['arguments']=={'source':'print(42)'}
        args=('w',sid,turn['id'],q['id'],q['digest'])
        assert store.claim_cell_command(*args)=={'claimed':True}
        assert store.claim_cell_command(*args)=={'claimed':False}
        store.respond(*args,{'value':{'state':'edited'}})
        assert proc.wait(timeout=20)==0,proc.stderr.read().decode()
        assert json.loads(proc.stdout.read())=={'state':'edited'}
    finally:
        bridge.close()


def test_assistant_cli_edits_runs_and_reads_native_output(browser,native_project,tmp_path,monkeypatch):
    from urllib.parse import quote
    from playwright.sync_api import expect
    from test_forge_development_chat import configuration
    from test_forge_public_api import service
    from test_workbench_native_notebook import PYTHON
    if not PYTHON.is_file():
        pytest.skip('Native notebook environment unavailable')
    root,registry_path,project=native_project
    fixture=tmp_path/'provider';fixture.mkdir()
    _,conf,settings=configuration(fixture)
    grant=settings['projects'].pop('wfixture')
    settings['projects'][project]=dict(grant,root=str(root),sources=['README.md'],writes=[],watch=[])
    conf.write_text(json.dumps(settings))
    recorded=[]
    continue_reply=threading.Event()
    attempts=[]
    with service(None,registry=registry_path,notebook_python=PYTHON,development_config=conf) as app:
        dev=app.development
        caps=dev.capabilities
        monkeypatch.setattr(dev,'capabilities',lambda *args:dict(caps(*args),notebook_cells=True,notebook_cli=True))
        def launch(turn,session):
            attempts.append(turn['id'])
            if len(attempts)==3:
                dev.store.finish_provider(turn['id'],state='interrupted',cleanup='complete',
                                          result={'reason':'BrokenPipeError: [Errno 32] Broken pipe'})
                return dev.store.public_turn(dev.store.turn(turn['id']))
            def assistant():
                bridge=CellCommands(dev.store,turn,time.time()+100)
                try:
                    if len(attempts)==1:
                        dev.store.event(turn['id'],'message.delta',dict(item_id='step',channel='process',text='正在检查测试卡片'))
                        dev.store.event(turn['id'],'item.updated',dict(id='inspect',command='data cell show',status='running'))
                        assert continue_reply.wait(30)
                    def cli(*args,source=None,success=True):
                        result=subprocess.run([str(bridge.bin/'data'),*args],input=source,text=True,
                            env={**os.environ,'DEMIFLOW_CONTROL_ENDPOINT':str(bridge.path)},
                            capture_output=True,check=False,timeout=40)
                        assert (result.returncode==0)==success,(args,result.stdout,result.stderr)
                        return json.loads(result.stdout)
                    if not cli('cell','show')['allow_run']:
                        assert '运行许可' in cli('cell','run',success=False)
                    else:
                        tools=cli('catalog','list','tools')['items']
                        profile=next(i['name'] for i in tools if i['name'].endswith('.column_profile'))
                        tool=cli('catalog','show','tools',profile)
                        table=next(i for i in cli('catalog','list','dictionary')['items'] if i['name']=='samples')
                        fixed=cli('catalog','show','dictionary',table['name'])
                        assert 'binding=' not in fixed['read'] and 'version=1' in fixed['read']
                        assert dict(fixed['fields']).keys()=={'name','score'}
                        cmd=tool['import']+'\n'+fixed['read']+'\nprint("count=",column_profile(df).loc["score","present"])'
                        assert cli('cell','edit',source=cmd)['state']=='edited'
                        executed=cli('cell','run')
                        assert 'count= 3' in executed['output'],executed
                        assert executed['execution_count']>0
                        cli('cell','edit',source='raise ValueError("native error probe")')
                        assert 'native error probe' in cli('cell','run',success=False)
                        recorded.append(executed)
                    state='completed';message='已收到内核回执' if recorded else '本轮仅编辑'
                except Exception as exc:
                    state='failed';message=str(exc)
                    recorded.append({'error':message})
                finally:
                    bridge.close()
                dev.store.event(turn['id'],'message.completed',dict(item_id='reply',text=message))
                dev.store.update(turn['id'],state=state,cleanup='complete',dispatch='acknowledged')
            threading.Thread(target=assistant,daemon=True).start()
            return dev.store.public_turn(dev.store.turn(turn['id']))
        monkeypatch.setattr(dev,'_launch_provider',launch)
        page=browser.new_page(viewport={'width':1440,'height':950})
        try:
            view=quote(json.dumps(dict(v=2,workspace=project,kind='tables')))
            page.goto(app.origin+'/#token='+app.token+'&view='+view)
            page.frame_locator('iframe.native-notebook-frame').get_by_role('button',name='卡片助手',exact=True).first.click(timeout=60000)
            pane=page.get_by_role('complementary',name='卡片助手')
            auto_run=pane.get_by_role('checkbox',name='允许自动运行')
            expect(auto_run).to_be_checked()
            auto_run.uncheck()
            query=pane.get_by_role('textbox',name='卡片要求')
            query.fill('inspect');pane.get_by_role('button',name='发送要求').click()
            expect(pane.locator('.dfc-think .dfc-summary')).to_have_text('正在检查测试卡片',timeout=30000)
            expect(pane.locator('.dfc-tool .dfc-summary')).to_have_text('data cell show')
            expect(pane.locator('.dfc-answer')).to_have_count(0)
            expect(pane.locator('.dfc-bubble')).to_have_text('inspect')
            pane.locator('.dfc-tool > summary').click()
            pane.locator('.dfc-tool').get_by_role('button',name='View original').click()
            expect(pane.locator('.df-card-original pre')).to_contain_text('data cell show')
            pane.get_by_role('button',name='关闭原文').click()
            continue_reply.set()
            expect(pane).to_contain_text('本轮仅编辑',timeout=30000)
            auto_run.check()
            query.fill('检查评分，并验证运行错误');pane.get_by_role('button',name='发送要求').click()
            expect(pane).to_contain_text('已收到内核回执',timeout=95000)
            expect(auto_run).to_be_checked()
            frame=page.frame_locator('iframe.native-notebook-frame')
            expect(frame.locator('.jp-CodeCell').last).to_contain_text('native error probe')
            assert len(recorded)==1 and 'execution_count' in recorded[0],recorded
            auto_run.uncheck()
            query.fill('重试中断的连接');pane.get_by_role('button',name='发送要求').click()
            expect(pane).to_contain_text('模型连接已关闭，未收到完整回复。')
            assert len(attempts)==3
            pane.get_by_role('button',name='重新发送').click()
            expect(pane.locator('.dfc-answer').last).to_contain_text('已收到内核回执',timeout=30000)
            expect(pane.locator('.dfc-turn')).to_have_count(4)
            expect(query).to_be_enabled(timeout=30000)
            assert len(set(attempts))==4 and len(recorded)==1
            sid=dev.store.turn(attempts[0])['session']
            assert not dev.sessions(project,root)['sessions']
            # An old browser bookmark used to open the cell inside Development.
            view=quote(json.dumps(dict(v=2,workspace=project,kind='development_session',session_id=sid)))
            page.goto(app.origin+'/#view='+view)
            page.reload()
            expect(page.get_by_role('button',name='新会话')).to_be_enabled(timeout=30000)
            expect(page.locator('.development-session-list button')).to_have_count(0)
            expect(page.locator('.dfc-bubble')).to_have_count(0)
        finally:
            page.close()


def test_generated_readers_use_dictionary_paths_and_upgrade_only_matching_source(native_project, tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow.workbench.notebooks.data import sample
    from demiflow.workbench.notebooks.table_code import migrate_readers
    project, _, _ = native_project
    monkeypatch.setenv('DEMIFLOW_NOTEBOOK_ROOT',str(project))
    external = tmp_path/'shared'
    external.mkdir()
    workspace.add_location(project,'shared',external)
    for directory, value in [(project,11),(external,22)]:
        lance.write_dataset(pa.table({'n':[value]}),directory/'每月 样本.lance')
    visible = {row['path']:row for row in dictionary.discover(project)['tables']}
    assert sample('每月 样本.lance',version=1).n.tolist()==[11]
    assert sample('shared/每月 样本.lance',version=1).n.tolist()==[22]
    assert sample('samples',version=1,limit=1).name.tolist()==['alpha']
    old=visible['每月 样本.lance']
    directory=project/'notebooks';directory.mkdir()
    source=('from demiflow.workbench.notebooks.data import project_sample\n\n'
            f'df = project_sample("{old["id"]}", expected="{old["expected"]}", version=1, '
            f'binding="{"f"*64}", columns=["n"], limit=20)\ndf')
    output={'output_type':'stream','name':'stdout','text':['unchanged\n']}
    cells=[{'id':'first','cell_type':'code','source':source.splitlines(True),'execution_count':1,'outputs':[output]},
           {'id':'note','cell_type':'markdown','source':[source]}]
    doc=directory/'分析.ipynb'
    doc.write_text(json.dumps({'cells':cells,'metadata':{},'nbformat':4,'nbformat_minor':5}))
    migrate_readers(project,directory,[doc.name])
    migrated=json.loads(doc.read_bytes())['cells']
    code=''.join(migrated[0]['source'])
    assert '"每月 样本.lance", version=1' in code and old['id'] not in code and 'binding=' not in code
    exec(code,{'__name__':'__main__'})
    assert migrated[0]['outputs']==[output] and migrated[0]['execution_count']==1 and migrated[1]==cells[1]
    unchanged=doc.stat().st_mtime_ns
    migrate_readers(project,directory,[doc.name])
    assert doc.stat().st_mtime_ns==unchanged
