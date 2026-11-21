"""Browser acceptance for the standalone DeepSeek-style projection; no model."""
from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading

import pytest


WEB=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
CHROME='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'


@contextmanager
def presentation():
    playwright=pytest.importorskip('playwright.sync_api')
    if not Path(CHROME).is_file():pytest.skip('Installed Chromium unavailable')
    class Handler(SimpleHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            if self.path=='/fixture':
                page=b'''<!doctype html><html><meta charset="utf-8"><style>details{margin:12px 0}summary{color:#50647f}</style><link rel="stylesheet" href="/development-chat.css">
                <style>body{margin:0;background:white;padding:24px}#flow{max-width:748px;margin:0 auto;min-width:0}</style>
                <div id="flow"></div><script src="/markdown-it.min.js"></script><script src="/reading.js"></script>
                <script src="/development-chat.js"></script></html>'''
                self.send_response(200);self.send_header('Content-Type','text/html;charset=utf-8');self.send_header('Content-Length',str(len(page)));self.end_headers();self.wfile.write(page)
            else:super().do_GET()
    server=ThreadingHTTPServer(('127.0.0.1',0),partial(Handler,directory=str(WEB)))
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser=runtime.chromium.launch(executable_path=CHROME,headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
            page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
            page.add_init_script("Object.defineProperty(navigator,'clipboard',{value:{writeText:async text=>{window.copied=text;}}});")
            errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto('http://127.0.0.1:'+str(server.server_port)+'/fixture')
            page.evaluate("window.raw=[];window.chat=ForgeDevelopmentChat.create({flow:document.querySelector('#flow'),onRaw:item=>raw.push(structuredClone(item.sourceRef)),formatDevelopment:value=>'Checked state: '+value.state});")
            try:yield page,playwright.expect,errors
            finally:
                page.evaluate('chat.dispose()');browser.close()
    finally:server.shutdown();server.server_close();thread.join()


def event(sequence,kind,data,*,turn='first',epoch='epoch-1',time=None):
    return dict(session_id='session',turn_id=turn,provider_epoch=epoch,sequence=sequence,
                observed_at=1795060800+sequence if time is None else time,type=kind,data=data)


def push(page,events):
    page.evaluate('(events)=>{for(const event of events)chat.project(event);}',events)


def test_standard_turn_process_tool_rows_markdown_and_copy(tmp_path):
    final='已完成 **过滤算子**。\n\n1. 读取输入\n2. 保留有效记录\n\n```python\nfiltered = rows\n```\n\n[unsafe](javascript:alert(1))\n\n![hidden image](https://not-requested.invalid/image.png)\n\n<script>window.injected=true</script>'
    start=1795060800
    events=[
        event(1,'message.user',dict(text='开发一个过滤流程'),time=start),
        event(2,'message.delta',dict(item_id='thought',channel='process',text='**Inspect the project**\n\nRead existing fields first.')),
        event(3,'message.completed',dict(item_id='thought',channel='process',text='**Inspect the project**\n\nRead existing fields first.')),
        event(4,'item.updated',dict(id='tool',type='tool',title='Bash',status='running',rawInput=dict(command='python inspect.py',description='Inspect the project input'))),
        event(5,'output.delta',dict(item_id='tool',text='\nFound 3 fields.')),
        event(6,'item.updated',dict(id='tool',type='tool',title='Bash',status='completed',rawInput=dict(command='python inspect.py',description='Inspect the project input'))),
        event(7,'message.delta',dict(item_id='next-thought',channel='process',text='The input is ready. '+('Inspect the existing filtering conditions. '*20))),
        event(8,'message.completed',dict(item_id='next-thought',channel='process',text='The input is ready. '+('Inspect the existing filtering conditions. '*20))),
        event(9,'item.updated',dict(id='tool-2',type='commandExecution',command='python test.py',title='Validate the filter',status='completed')),
        event(10,'message.delta',dict(item_id='answer',text=final[:20])),
        event(11,'message.completed',dict(item_id='answer',text=final)),
        event(12,'message.completed',dict(item_id='answer',text=final)),
        event(13,'turn.state',dict(state='completed',cleanup='complete'),time=start+31),
        event(14,'development.state',dict(state='review_unavailable',reason='Waiting for independent review')),
    ]
    with presentation() as (page,expect,errors):
        requests=[];page.on('request',lambda request:requests.append(request.url))
        push(page,events)
        expect(page.locator('.dfc-turn-process>summary')).to_contain_text('Completed in 31s')
        assert page.locator('.dfc-turn-process>summary>.dfc-label').evaluate('(node)=>node.scrollWidth<=node.clientWidth')
        expect(page.locator('.dfc-step-process>summary')).to_contain_text('Ran commands')
        assert page.locator('.dfc-turn-process').evaluate('(node)=>node.open')
        assert page.locator('.dfc-step-process').evaluate('(node)=>node.open')
        expect(page.locator('.dfc-think')).to_have_count(2)
        row_positions=page.locator('.dfc-row').evaluate_all('(nodes)=>nodes.map(node=>node.getBoundingClientRect().top)')
        assert all(abs(row_positions[i+1]-row_positions[i]-28)<1 for i in range(len(row_positions)-1))
        assert page.locator('.dfc-think>summary').first.evaluate('(node)=>getComputedStyle(node).color')=='rgb(129, 133, 140)'
        assert not page.locator('.dfc-think').first.evaluate('(node)=>node.open')
        assert page.locator('.dfc-think').last.locator('.dfc-summary').evaluate('(node)=>node.scrollWidth>node.clientWidth')
        expect(page.locator('.dfc-answers>.dfc-answer')).to_have_count(1)
        expect(page.locator('.dfc-answer strong')).to_have_text('过滤算子')
        expect(page.locator('.dfc-answer ol li')).to_have_count(2)
        expect(page.locator('.dfc-answer pre code')).to_contain_text('filtered = rows')
        assert page.locator('.reading-toolbar,.reading-status,.reading-source-controls,.development-reader-options').count()==0
        assert page.locator('#flow script,#flow img').count()==0
        assert not page.evaluate('window.injected||false')
        assert not any('not-requested.invalid' in value for value in requests)
        user=page.locator('.dfc-user');expect(user.locator('time')).not_to_be_empty()
        user.get_by_role('button',name='Copy message',exact=True).click();assert page.evaluate('copied')=='开发一个过滤流程'
        tool=page.locator('.dfc-tool').first;tool.locator(':scope>summary').click()
        expect(tool.locator('.dfc-tool-output')).to_contain_text('Found 3 fields.')
        tool.locator(':scope>summary').click()
        shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(shots/'deepseek-chat-standard.png'),full_page=True)
        page.locator('.dfc-answer').get_by_role('button',name='View original').click()
        assert page.evaluate('raw.at(-1)')==dict(item_id='answer',turn_id='first',provider_epoch='epoch-1',from_sequence=12,through_sequence=12)
        assert not errors,errors


def test_streaming_legacy_ids_epochs_and_disclosure_choices():
    with presentation() as (page,expect,errors):
        original=[event(1,'message.user',dict(text='First requirement')),
            event(2,'message.delta',dict(item_id='acp-interval:client-3:answer',text='First progress.')),
            event(3,'item.updated',dict(id='same-tool',type='tool',title='Bash',rawInput=dict(command='check first'))),
            event(4,'message.delta',dict(item_id='acp-interval:client-3:answer',text='First answer.')),
            event(5,'turn.state',dict(state='completed',cleanup='complete')),
            event(6,'message.user',dict(text='Second requirement'),turn='second',epoch='epoch-2'),
            event(7,'message.delta',dict(item_id='same-thought',channel='process',text='Read the next input.\n'),turn='second',epoch='epoch-2')]
        before=json.dumps(original,sort_keys=True);push(page,original);assert json.dumps(original,sort_keys=True)==before
        think=page.locator('.dfc-think');think.locator(':scope>summary').click()
        push(page,[event(8,'message.delta',dict(item_id='same-thought',channel='process',text='Keep the reader’s expanded view.'),turn='second',epoch='epoch-2')])
        expect(think.locator('.dfc-row-content')).to_contain_text('expanded view')
        assert think.evaluate('(node)=>node.open')
        push(page,[event(9,'message.completed',dict(item_id='same-thought',channel='process',text='Read the next input.\nKeep the reader’s expanded view.',interrupted=True,status='incomplete'),turn='second',epoch='epoch-2'),
            event(10,'message.delta',dict(item_id='acp-interval:client-3:answer',text='Second progress.'),turn='second',epoch='epoch-2'),
            event(11,'item.updated',dict(id='same-tool',type='tool',title='Bash',rawInput=dict(command='check second')) ,turn='second',epoch='epoch-2'),
            event(12,'message.delta',dict(item_id='acp-interval:client-3:answer',text='Second answer.'),turn='second',epoch='epoch-2')])
        expect(page.locator('.dfc-answers>.dfc-answer').last).to_contain_text('Second answer.')
        assert think.evaluate('(node)=>node.open')
        expect(think.locator('.dfc-incomplete')).to_have_text('Incomplete')
        expect(page.locator('.dfc-tool')).to_have_count(2)
        expect(page.locator('.dfc-progress')).to_have_count(2)
        assert not page.locator('.dfc-turn-process').first.evaluate('(node)=>node.open')
        push(page,[event(13,'message.completed',dict(item_id='acp-interval:client-3:answer',text='Second answer.'),turn='second',epoch='epoch-2'),
            event(14,'turn.state',dict(state='completed',cleanup='complete'),turn='second',epoch='epoch-2')])
        assert page.locator('.dfc-turn-process').last.evaluate('(node)=>node.open')
        assert think.evaluate('(node)=>node.open')
        # Manual disclosure remains available for each step.
        page.locator('.dfc-step-process').last.locator(':scope>summary').click()
        assert not page.locator('.dfc-step-process').last.evaluate('(node)=>node.open')
        page.locator('.dfc-answers>.dfc-answer').last.get_by_role('button',name='View original').click()
        assert page.evaluate('raw.at(-1).turn_id')=='second'
        assert page.evaluate('raw.at(-1).from_sequence')==13
        # Reconstruct exactly the saved history, including older reused IDs.
        replay=original+[event(8,'message.delta',dict(item_id='same-thought',channel='process',text='Continuation'),turn='second',epoch='epoch-2')]
        page.evaluate('chat.reset()');push(page,replay)
        expect(page.locator('.dfc-user')).to_have_count(2)
        expect(page.locator('.dfc-answers>.dfc-answer').first).to_contain_text('First answer.')
        assert not errors,errors


def test_paged_completion_role_preview_budgets_and_narrow_screen(tmp_path):
    with presentation() as (page,expect,errors):
        push(page,[event(1,'message.user',dict(text='Long response')),
            event(2,'message.delta',dict(item_id='summary',channel='summary',text='Old prefix')),
            event(3,'message.completed',dict(item_id='summary',channel='summary',text='',paged=True)),
            event(4,'output.delta',dict(item_id='summary',channel='summary',text='Canonical public summary.\n'+('Detail. '*5000))),
            event(5,'message.completed',dict(item_id='answer',text='',paged=True)),
            event(6,'output.delta',dict(item_id='answer',text='Long final answer.\n'+('Result. '*9000))),
            event(7,'turn.state',dict(state='completed',cleanup='complete'))])
        expect(page.locator('.dfc-tool')).to_have_count(0)
        expect(page.locator('.dfc-think')).to_have_count(1)
        expect(page.locator('.dfc-answers>.dfc-answer')).to_have_count(1)
        expect(page.locator('.dfc-answer .dfc-limit')).to_have_text('Preview limit reached')
        page.locator('.dfc-answer').get_by_role('button',name='View original').click()
        assert page.evaluate('raw.at(-1).from_sequence')==5
        assert page.evaluate('raw.at(-1).through_sequence')==6
        page.set_viewport_size({'width':320,'height':850})
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(shots/'deepseek-chat-320.png'),full_page=False)
        page.evaluate('chat.reset()')
        flood=[event(i+1,'message.completed',dict(item_id='item-'+str(i),text='x'*16000),turn='budget') for i in range(110)]
        push(page,flood)
        state=page.evaluate('chat.stats')
        assert state['items']<=100 and state['textBytes']<=1048576 and state['trimmed']>=10
        assert page.locator('.dfc-turn').count()<=100
        page.evaluate('chat.reset()')
        # Control-only windows must also have bounded retained turn wrappers.
        push(page,[event(i+1,'turn.state',dict(state='completed',cleanup='complete'),turn='turn-'+str(i)) for i in range(130)])
        assert page.evaluate('chat.stats.turns')<=100
        assert not errors,errors
