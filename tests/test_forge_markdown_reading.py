"""Isolated real-browser/worker checks; no Forge app, project, model or service.

All served implementation files are read from this checkout. Worker delay and
held-message fixtures inject scheduling faults around the real module; they do
not substitute a parser or change production budgets. No shared installation.
"""
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import threading
import time

import pytest


WEB = Path(__file__).resolve().parents[1] / 'demiflow/workbench/web'
ASSETS = {'/' + name: WEB / name for name in (
    'reading.js', 'reading.css', 'markdown-worker.js', 'markdown-it.min.js')}
HTML = b'''<!doctype html><meta charset="utf-8"><title>Reading fixture</title>
<link rel="stylesheet" href="/reading.css">
<script src="/markdown-it.min.js" defer></script>
<script src="/reading.js" defer></script><main id="mount"></main>'''
TRACK_WORKERS = r'''(() => {
  const NativeWorker = globalThis.Worker;
  const stats = globalThis.workerStats = {created:0, terminated:0, alive:0, maxAlive:0, posts:[], results:[], held:[]};
  globalThis.Worker = function(url, options) {
    const worker = new NativeWorker(url, options);
    const started = performance.now(); let sent;
    stats.created++; stats.alive++; stats.maxAlive = Math.max(stats.maxAlive, stats.alive);
    const terminate = worker.terminate.bind(worker), post = worker.postMessage.bind(worker);
    let alive = true, handler;
    worker.terminate = () => { if (alive) {alive=false; stats.terminated++; stats.alive--;} terminate(); };
    worker.postMessage = data => {sent=performance.now(); stats.posts.push({id:data.id, keys:Object.keys(data), length:data.text.length, startupMs:sent-started}); post(data);};
    Object.defineProperty(worker, 'onmessage', {get:()=>handler, set:value=>{handler=value;}});
    worker.addEventListener('message', event => {
      const saved = handler;
      if (!saved) return;
      const transferred=event.data instanceof ArrayBuffer;
      const data=transferred ? JSON.parse(new TextDecoder().decode(event.data)) : event.data;
      if (data.type !== 'ready') stats.results.push({type:data.type,metrics:data.metrics,transferred,
        actualBytes:transferred ? event.data.byteLength : new TextEncoder().encode(JSON.stringify(data)).length,
        parseMs:performance.now()-sent});
      if (globalThis.holdFirstResult && data.type === 'result' && !globalThis.didHold) {
        globalThis.didHold = true;
        stats.held.push(() => saved(event));
      } else saved(event);
    });
    return worker;
  };
  globalThis.Worker.prototype = NativeWorker.prototype;
  globalThis.flushHeld = () => {for (const callback of stats.held.splice(0)) callback();};
})();'''


@pytest.fixture(scope='module')
def browser():
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runtime:
        executable = Path(os.environ.get('FORGE_CHROMIUM', runtime.chromium.executable_path))
        if not executable.is_file():
            pytest.skip('FORGE_CHROMIUM must point to an existing Chromium')
        instance = runtime.chromium.launch(executable_path=str(executable), headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'])
        yield instance
        instance.close()


@contextmanager
def static_fixture(*, parse_delay_ms=0, startup_delay=0):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            assert len(requests) < 256, 'fixture request budget exceeded'
            requests.append({'path': self.path, 'headers': dict(self.headers)})
            path = self.path.split('?', 1)[0]
            if path == '/':
                data, kind = HTML, 'text/html; charset=utf-8'
            elif path in ASSETS:
                data = ASSETS[path].read_bytes()
                kind = 'text/css' if path.endswith('.css') else 'text/javascript'
                if path == '/markdown-worker.js':
                    if startup_delay:
                        time.sleep(startup_delay)
                    if parse_delay_ms:
                        # The actual bootstrap imports the actual parser/module;
                        # delay its registered parse handler to exercise timeout.
                        data += (f'\nconst actual = self.onmessage; self.onmessage = event => {{'
                            f'const end = performance.now()+{parse_delay_ms};'
                            'while(performance.now()<end){} actual(event);};').encode()
            elif path == '/clicked':
                data, kind = b'Explicit link navigation', 'text/plain'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', kind)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Content-Security-Policy', "default-src 'none'; script-src 'self'; "
                "worker-src 'self'; style-src 'self'; img-src 'none'; connect-src 'none'; base-uri 'none'")
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass  # an intentionally terminated worker can close its startup request

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@contextmanager
def opened(browser, **fixture_options):
    with static_fixture(**fixture_options) as (origin, requests):
        context = browser.new_context(viewport={'width': 390, 'height': 900})
        context.set_default_timeout(5000)
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script(TRACK_WORKERS)
        page.goto(origin + '/#view=keep-this-protocol')
        page.wait_for_function('() => !!globalThis.ForgeReading')
        try:
            yield page, origin, requests
            assert not errors, errors
        finally:
            context.close()


def mount(page, text, *, default_mode='preview', offline=False, compact_source=False, continuous_source=False):
    page.evaluate('''({text, defaultMode, offline, compactSource, continuousSource}) => {
      if (globalThis.reader) globalThis.reader.dispose();
      globalThis.reader = ForgeReading.createTextReader({text, defaultMode, offline, compactSource, continuousSource,
        identity: {object:'fixture', generation:1}});
      document.getElementById('mount').replaceChildren(reader.element);
    }''', {'text': text, 'defaultMode': default_mode, 'offline': offline, 'compactSource':compact_source, 'continuousSource':continuous_source})


def wait_mode(page, mode):
    page.locator(f'.forge-reader[data-mode="{mode}"]').wait_for()
    if mode == 'preview':
        page.locator('.reading-preview').wait_for()


def test_upstream_assets_and_public_contract(browser):
    expected = {
        'markdown-it.min.js': '38c70a1e7ca91ab40e2d9e6e60129851a717ed1c7d4acbbdd41bf9503791cf68',
        'markdown-it.LICENSE.txt': '792c48c5a849a15fdf9e37e8bcf9e6d1dd13b32b46c642a748a0a46a9919d473',
        'markdown-it.NOTICES.txt': '822151a3210d4accfb3bc9653275aa399e33ff7d8582cdaeecbbd9c231c626a2',
    }
    provenance = json.loads((WEB / 'markdown-it.provenance.json').read_text())
    for name, digest in expected.items():
        assert hashlib.sha256((WEB / name).read_bytes()).hexdigest() == digest
        assert provenance['files'][name]['sha256'] == digest
    assert provenance['version'] == '14.1.0' and 'not a verified complete' in provenance['provenance_scope']
    with opened(browser) as (page, _, _):
        # The registered reader also publishes its existing readiness contract.
        assert page.evaluate('Object.keys(ForgeReading).sort()') == ['createTextReader', 'initialReady', 'limits']
        assert page.evaluate('ForgeReading.initialReady === true')
        assert page.evaluate('Object.isFrozen(ForgeReading.limits)')
        assert page.evaluate("(() => {try {ForgeReading.createTextReader({text:3}); return false;} catch {return true;}})()")


def test_real_worker_markdown_source_and_explicit_links(browser):
    with opened(browser) as (page, origin, requests):
        text = ('# 阅读标题\r\n\r\n段落 **强调**、*斜体*、~~删除~~ 和 `a<b`。\n\n'
            '> 引用\n\n3. 三\n4. 四\n   - 嵌套\n\n---\n\n'
            '| A | B |\n| - | - |\n| 一 | 二 |\n\n```python\nprint("read only")\n```\n\n'
            f'[external]({origin}/clicked?ok=yes) [anchor](#阅读标题) [missing](#missing)\n'
            '[source](operators/read.py) [unknown](unknown.md) [escape](../hidden.md)\n')
        page.evaluate('''() => {
          globalThis.resolved=[]; globalThis.opened=[];
          globalThis.resolver = href => {resolved.push(href); return href==='operators/read.py' ?
            {label:'读取已识别源码', open:()=>opened.push(href)} : null;};
          sessionStorage.setItem('forge-fixture-token','never-forward-this');
        }''')
        page.evaluate('''text => {
          globalThis.reader=ForgeReading.createTextReader({text, identity:{epoch:7}, resolveLink:resolver});
          document.getElementById('mount').append(reader.element);
        }''', text)
        wait_mode(page, 'preview')
        for selector in ['h1', 'strong', 'em', 's', 'blockquote', 'ol', 'ul', 'hr', 'table', 'pre code']:
            assert page.locator('.reading-preview ' + selector).count()
        assert page.locator('.reading-preview ol').get_attribute('start') == '3'
        assert page.locator('.reading-preview pre code').text_content() == 'print("read only")\n'
        assert page.evaluate('resolved') == ['operators/read.py', 'unknown.md']
        page.get_by_role('button', name='source', exact=True).click()
        assert page.evaluate('opened') == ['operators/read.py']
        page.get_by_role('button', name='anchor', exact=True).click()
        assert page.evaluate('location.hash') == '#view=keep-this-protocol'
        page.get_by_role('button', name='missing', exact=True).click()
        assert '文内引用未找到' in page.locator('.reading-status').text_content()
        external = page.get_by_role('link', name='external', exact=True)
        assert external.get_attribute('rel') == 'noopener noreferrer'
        assert external.get_attribute('referrerpolicy') == 'no-referrer'
        assert not any(request['path'].startswith('/clicked') for request in requests)
        with page.expect_popup() as popup:
            external.click()
        popup.value.wait_for_load_state()
        assert popup.value.evaluate('window.opener === null')
        clicked = next(request for request in requests if request['path'].startswith('/clicked'))
        assert not any(key.lower() in ['referer', 'authorization', 'x-forge-token'] for key in clicked['headers'])
        popup.value.close()
        page.get_by_role('button', name='原文', exact=True).click()
        assert page.locator('.reading-source-chunk').all_text_contents() == [text]
        assert page.evaluate('workerStats.alive') == 0
        assert page.evaluate('workerStats.posts[0].keys.sort()') == ['id', 'text', 'type']
        assert not page.evaluate('document.documentElement.scrollWidth > innerWidth')


def test_malicious_content_has_no_script_image_or_automatic_navigation(browser):
    with opened(browser) as (page, origin, requests):
        calls = []
        page.on('request', lambda request: calls.append(request.url))
        attack = ('# Safe\n<script>globalThis.pwned=1</script>\n'
            '<svg onload="globalThis.pwned=2"><script>globalThis.pwned=3</script></svg>\n'
            '<img src="/unexpected-html.png" onerror="globalThis.pwned=4">\n\n'
            f'![remote]({origin}/unexpected.png) ![file](file:///etc/passwd) '
            '![data](data:image/svg+xml,evil) ![blob](blob:pretend) ![local](./secret.png)\n\n'
            '[js](javascript:alert(1)) [data](data:text/html,evil) '
            '[creds](https://user:pass@example.invalid/) [token](https://example.invalid/?TOKEN=secret) '
            '[encoded](https://example.invalid/?%2574oken=secret) [hash](https://example.invalid/#access_token=secret) '
            '[slash](//example.invalid/x) [hidden](.secret/file) [encodedpath](%2e%2e/file) '
            '[filelink](file:///etc/passwd) [control](https://example.invalid/%0aevil) '
            '[backslash](https://example.invalid/\\evil)')
        mount(page, attack)
        wait_mode(page, 'preview')
        page.wait_for_timeout(150)
        assert page.evaluate('globalThis.pwned') is None
        assert page.locator('.forge-reader img,.forge-reader svg,.forge-reader script,.forge-reader iframe').count() == 0
        assert page.locator('.forge-reader a').count() == 0
        assert page.locator('.reading-image-placeholder').count() >= 2
        assert '<script>' in page.locator('.reading-preview').text_content()
        assert '引用未支持' in page.locator('.reading-preview').text_content()
        assert calls and all(url.startswith(origin + '/') for url in calls)
        allowed = {'/markdown-worker.js', '/markdown-it.min.js', '/reading.js'}
        assert all(url[len(origin):].split('?', 1)[0] in allowed for url in calls)
        assert not any('unexpected' in request['path'] for request in requests)


@pytest.mark.parametrize('text,reason,starts_worker', [
    ('x' * 65537, '64 KiB', False),
    ('界' * 22000, '64 KiB', False),
    ('\r\n' * 4096, '4096 行', False),
    ('> ' * 25 + 'too deep', '嵌套', True),
    ('*x* ' * 1600, '6000 token', True),
    ('![](/no-image) ' * 2700, '8000', True),
    ('[x][ref] ' * 400 + '\n\n[ref]: https://example.invalid/' + 'a' * 1800, '512 KiB', True),
    ('[long](https://example.invalid/' + 'a' * 2048 + ')', '2048 字节', True),
], ids=['ascii-bytes', 'utf8-bytes', 'crlf-lines', 'parser-nesting', 'tokens',
    'projected-nodes', 'expanded-reference-message', 'single-link'])
def test_real_limits_fall_back_to_complete_source(browser, text, reason, starts_worker):
    with opened(browser) as (page, _, _):
        mount(page, text)
        wait_mode(page, 'source')
        assert reason in page.locator('.reading-status').text_content()
        assert not page.locator('.reading-preview').count()
        assert bool(page.evaluate('workerStats.created')) == starts_worker
        assert page.evaluate('workerStats.alive') == 0
        assert page.evaluate("Array.from(document.querySelectorAll('.reading-source-chunk')).map(n=>n.textContent).join('')") == text[:16384]


def test_admission_exact_boundary_and_generated_tree_depth(browser):
    with opened(browser) as (page, _, _):
        mount(page, 'x' * 65536)
        wait_mode(page, 'preview')
        assert len(page.locator('.reading-preview').text_content()) == 65536
        markers = ['*', '**', '~~'] * 4
        deep = '> ' * 12 + ''.join(marker + 'a ' for marker in markers) + 'center' + ''.join(' z' + marker for marker in reversed(markers))
        mount(page, deep)
        wait_mode(page, 'source')
        assert '24 层' in page.locator('.reading-status').text_content()


def test_dom_budget_includes_generated_link_notes_and_text_nodes(browser):
    with opened(browser) as (page, _, _):
        mount(page, '[![](/x)](unknown)' * 1900)
        wait_mode(page, 'source')
        assert '8000' in page.locator('.reading-status').text_content()
        result = page.evaluate('workerStats.results[0]')
        assert result['type'] == 'result' and result['metrics']['nodes'] < 8000
        assert result['metrics']['tokens'] <= 6000


def test_bounded_large_message_is_measured_before_clone_and_no_partial_tree(browser):
    with opened(browser) as (page, _, _):
        text = '[x][ref] ' * 240 + '\n\n[ref]: https://example.invalid/' + 'a' * 1800
        mount(page, text)
        wait_mode(page, 'preview')
        measured = page.evaluate('workerStats.results[0]')
        assert measured['transferred']
        assert 400000 < measured['actualBytes'] <= measured['metrics']['messageBytes'] <= 524288
        assert measured['parseMs'] < 500
        assert page.evaluate('workerStats.posts[0].startupMs < 2000')
        assert page.evaluate('''() => {
          let nodes=1; const scan=document.createTreeWalker(reader.element,NodeFilter.SHOW_ALL);
          while(scan.nextNode()) nodes++;
          return nodes<=8000;
        }''')


@pytest.mark.parametrize('compact_source,continuous_source',[
    pytest.param(False,False,id='False'),pytest.param(True,False,id='True'),
    pytest.param(True,True,id='continuous')])
def test_source_paging_preserves_all_characters_and_bounded_copy(browser,compact_source,continuous_source):
    # CRLF, tabs, NUL, combining marks, astral characters and repeated boundaries.
    text = ('空白\r\n\t😀e\u0301\x00 <>&' + '界' * 31) * 3100
    with opened(browser) as (page, _, _):
        page.evaluate("Object.defineProperty(navigator, 'clipboard', {value:{writeText:async text=>{globalThis.copied=text;}}})")
        mount(page, text, default_mode='source',compact_source=compact_source,continuous_source=continuous_source)
        portions = []
        while True:
            info = page.evaluate('''() => {
              const blocks=Array.from(document.querySelectorAll('.reading-source-chunk'));
              return {text:blocks.map(n=>n.textContent).join(''), bytes:blocks.map(n=>new TextEncoder().encode(n.textContent).length),
                from:+blocks[0].dataset.from,to:+blocks.at(-1).dataset.to,
                split:blocks.some(n=>{const s=n.textContent;return /[\uD800-\uDBFF]$/.test(s)||/^[\uDC00-\uDFFF]/.test(s);})};
            }''')
            assert len(info['bytes']) <= 4 and max(info['bytes']) <= 16384 and sum(info['bytes']) <= 65536
            assert not info['split']
            if continuous_source:
                assert page.locator('.reading-source-page').text_content()==info['text']
            page.get_by_role('button', name='复制代码' if compact_source else '复制本页原文', exact=True).click()
            assert page.evaluate('copied') == info['text']
            portions.append(info['text'])
            if page.get_by_role('button', name='下一页原文', exact=True).is_disabled():
                break
            page.get_by_role('button', name='下一页原文', exact=True).click()
        assert ''.join(portions) == text
        page.get_by_role('button', name='上一页原文', exact=True).click()
        assert ''.join(page.locator('.reading-source-chunk').all_text_contents()) == portions[-2]
        assert page.evaluate('workerStats.created') == 0
        page.evaluate('reader.dispose(); reader.dispose()')
        assert page.locator('.forge-reader').text_content() == ''


def test_online_parse_timeout_and_separate_startup_budget(browser):
    with opened(browser, parse_delay_ms=850) as (page, _, _):
        began = time.monotonic()
        mount(page, '# delayed actual worker')
        wait_mode(page, 'source')
        elapsed = time.monotonic() - began
        assert '解析预算 500 ms' in page.locator('.reading-status').text_content()
        assert .45 <= elapsed < 3
        assert page.evaluate('workerStats.created === 1 && workerStats.alive === 0')
    with opened(browser, startup_delay=2.4) as (page, _, _):
        mount(page, '# slow bootstrap')
        wait_mode(page, 'source')
        assert '启动超时' in page.locator('.reading-status').text_content()
        assert page.evaluate('workerStats.created === 1 && workerStats.posts.length === 0 && workerStats.alive === 0')


def test_one_worker_latest_waiter_cancel_and_late_real_result(browser):
    with opened(browser) as (page, _, _):
        page.evaluate('holdFirstResult=true')
        mount(page, '# Old actual result')
        page.wait_for_function('() => workerStats.held.length === 1')
        page.evaluate('''() => {
          globalThis.waiter = ForgeReading.createTextReader({text:'# Replaced waiter',identity:{epoch:2}});
          globalThis.latest = ForgeReading.createTextReader({text:'# Latest',identity:{epoch:3}});
          document.getElementById('mount').append(waiter.element,latest.element);
        }''')
        assert '更新的选择替代' in page.locator('.forge-reader').nth(1).text_content()
        assert page.evaluate('workerStats.created === 1 && workerStats.maxAlive === 1')
        # Mode switch cancels the still-held job, letting only the latest waiter run.
        page.locator('.forge-reader').first.get_by_role('button', name='原文', exact=True).click()
        page.locator('.forge-reader').nth(2).get_by_role('heading', name='Latest', exact=True).wait_for()
        page.evaluate('flushHeld()')
        assert page.locator('.forge-reader').first.get_attribute('data-mode') == 'source'
        assert page.locator('.forge-reader').nth(2).get_by_role('heading', name='Latest').count() == 1
        assert page.evaluate('workerStats.created === 2 && workerStats.maxAlive === 1 && workerStats.alive === 0')
        page.evaluate('reader.dispose(); waiter.dispose(); latest.dispose()')
        assert page.evaluate('workerStats.created === workerStats.terminated')


def test_dispose_cancels_active_and_pending_and_drops_late_callbacks(browser):
    with opened(browser) as (page, _, _):
        page.evaluate('holdFirstResult=true')
        mount(page, '# Dispose me')
        page.wait_for_function('() => workerStats.held.length === 1')
        page.evaluate('''() => {
          const queued=ForgeReading.createTextReader({text:'# Never scheduled',identity:{epoch:5}});
          queued.dispose(); reader.dispose(); flushHeld();
        }''')
        assert page.locator('.forge-reader').text_content() == ''
        assert page.evaluate('workerStats.created === 1 && workerStats.terminated === 1 && workerStats.alive === 0')
        mount(page, '# New selection')
        wait_mode(page, 'preview')
        assert page.get_by_role('heading', name='New selection').count() == 1
        assert page.get_by_role('heading', name='Dispose me').count() == 0


def test_offline_inline_file_uses_no_worker_storage_or_resource_requests(browser, tmp_path):
    def inline(data):
        return re.sub(r'</script', r'<\\/script', data, flags=re.IGNORECASE)

    target = tmp_path / 'offline.html'
    target.write_text('<!doctype html><meta charset="utf-8"><main id="mount"></main>' +
        '<script>' + inline((WEB / 'markdown-it.min.js').read_text()) + '</script>' +
        '<script>' + inline((WEB / 'reading.js').read_text()) + '</script>')
    context = browser.new_context()
    page = context.new_page()
    requests, errors = [], []
    page.on('request', lambda request: requests.append(request.url))
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.add_init_script(TRACK_WORKERS + "Object.defineProperty(window,'sessionStorage',{get(){throw Error('forbidden storage');}});")
    try:
        page.goto(target.as_uri())
        text = '# Offline\n\n**readable** ![no request](https://example.invalid/no.png)\n\n</script><script>globalThis.pwned=1</script>'
        mount(page, text, offline=True)
        wait_mode(page, 'preview')
        assert page.get_by_role('heading', name='Offline').count() == 1
        assert '没有在线 worker' in page.locator('.reading-status').text_content()
        assert page.evaluate('workerStats.created') == 0
        assert page.evaluate('globalThis.pwned') is None
        page.get_by_role('button', name='原文', exact=True).click()
        assert page.locator('.reading-source-chunk').all_text_contents() == [text]
        mount(page, '界' * 22000, offline=True)
        wait_mode(page, 'source')
        assert requests == [target.as_uri()] and not errors
    finally:
        context.close()
