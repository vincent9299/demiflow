"""Safe chat Markdown must survive many history messages and streaming updates."""
from pathlib import Path
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest

@pytest.fixture
def chat_page(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    executable = Path('/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome')
    if not executable.is_file():
        pytest.skip('Installed Chromium unavailable')
    class Static(SimpleHTTPRequestHandler):
        def log_message(self,*args): pass
        def end_headers(self):
            self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self'; img-src 'self' blob:; connect-src 'self'")
            super().end_headers()
    assets=Path(__file__).parents[1]/'demiflow/workbench/web'
    server=ThreadingHTTPServer(('127.0.0.1',0),partial(Static,directory=str(assets)))
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
      with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page()
        page.goto('http://127.0.0.1:'+str(server.server_port)+'/')
        page.wait_for_function('() => !!globalThis.ForgeReading?.createMarkdownView')
        page.evaluate('''() => {
          const Original = Worker; window.workerPeak=0; window.workerActive=0;
          window.Worker = class extends Original {
            constructor(...args) { super(...args); this.alive=true;
              workerActive++; workerPeak=Math.max(workerPeak,workerActive); }
            terminate() { if(this.alive) {this.alive=false; workerActive--;} return super.terminate(); }
          };
          window.views=[]; window.changes=[];
          window.chatHost=document.createElement('div'); document.body.append(chatHost);
          window.addMarkdown=(text)=>{
            const element=document.createElement('article'); chatHost.append(element);
            const view=ForgeReading.createMarkdownView({element,text,onChange:value=>changes.push(value)});
            views.push(view); return view;
          };
        }''')
        yield page
        page.evaluate('() => views.forEach(view=>view.dispose())')
        browser.close()
    finally:
        server.shutdown();thread.join(timeout=5);server.server_close()


def test_history_batch_and_latest_stream_text_all_render_without_file_reader_controls(chat_page):
    page=chat_page
    page.evaluate('''() => {
      for(let i=0;i<18;i++) addMarkdown('**Message '+i+'**\\n\\nNormal conversation.');
      for(let i=0;i<80;i++) views[0].update('**Newest '+i+'**\\n\\nStreaming paragraph.');
    }''')
    page.wait_for_function("() => chatHost.querySelectorAll('.development-markdown-body strong').length===18")
    page.wait_for_function("() => views[0].element.querySelector('strong')?.textContent==='Newest 79'")
    assert page.locator('.development-markdown .reading-source-nav').count()==0
    assert page.locator('.development-markdown .reading-toolbar').count()==0
    assert page.locator('.development-markdown [data-markdown-fallback]').count()==0
    assert page.evaluate('workerPeak')==1
    page.evaluate('() => views.forEach(view=>view.dispose())')
    assert page.evaluate('workerActive')==0


def test_chat_markdown_keeps_html_and_remote_images_inert_and_formats_code(chat_page):
    page=chat_page;external=[]
    page.on('request',lambda request: external.append(request.url) if 'untrusted.invalid' in request.url else None)
    page.evaluate('''() => addMarkdown('<script>window.chatInjected=true</script>\\n\\n![image](https://untrusted.invalid/image.png)\\n\\n[unsafe](javascript:alert(1))\\n\\n```python\\nprint("hello")\\n```')''')
    page.locator('.development-markdown-body pre').wait_for()
    assert page.locator('.development-markdown-body pre').inner_text()=='print("hello")\n'
    assert page.locator('.development-markdown script,.development-markdown img').count()==0
    assert page.evaluate('window.chatInjected===undefined')
    assert not external


def test_chat_markdown_limits_disposal_and_worker_start_failure_are_bounded(chat_page):
    page=chat_page
    page.evaluate("() => {const v=addMarkdown('**bounded**');v.update('x'.repeat(65537));}")
    page.locator('.development-markdown-body strong').wait_for()
    assert page.evaluate("changes.some(c=>c.reason==='bytes')")
    assert page.locator('.development-markdown-body').inner_text()=='bounded'
    page.evaluate('''() => {
      views.forEach(v=>v.dispose()); views=[];
      window.attempts=0;
      window.Worker=class {constructor(){attempts++;setTimeout(()=>this.onerror?.({preventDefault(){}}),0);}terminate(){}};
      for(let i=0;i<15;i++) addMarkdown('Readable '+i);
    }''')
    page.wait_for_function("() => chatHost.querySelectorAll('[data-markdown-fallback=worker]').length===15")
    page.wait_for_timeout(100)
    assert page.evaluate('attempts')==1
    assert page.locator('.development-markdown').last.inner_text()=='Readable 14'


def test_latest_plain_text_survives_line_limit_and_retired_worker_errors(chat_page):
    page=chat_page
    page.evaluate('''() => {
      const Original=Worker;window.chatWorkers=[];
      window.Worker=class extends Original {constructor(...args){super(...args);chatWorkers.push(this);}};
      addMarkdown('**Before line limit**');
    }''')
    page.locator('.development-markdown-body strong').wait_for()
    page.evaluate("() => {window.oldWorkerError=chatWorkers[0].onerror;window.oldMessageError=chatWorkers[0].onmessageerror;views[0].update('line\\n'.repeat(5000)+'LATEST END');}")
    source=page.locator('[data-markdown-fallback=lines]')
    assert source.inner_text().endswith('LATEST END')
    page.evaluate("() => {views[0].dispose();addMarkdown('**New view**');oldWorkerError({preventDefault(){}});oldMessageError();}")
    page.locator('.development-markdown-body strong').wait_for()
    assert page.locator('.development-markdown-body strong').inner_text()=='New view'
    assert page.evaluate('chatWorkers.length')==2
    assert page.evaluate('workerActive')==1
