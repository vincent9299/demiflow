"""Optional, worker-owned Chromium rendering with bounded HTTP admission.

One page and one admitted HTTP exchange per worker. CDP pauses *each* request,
including redirects, before it is sent. The existing parent owns global/host
permits. Images/fonts/media, service workers, downloads and off-policy hosts
are excluded. Byte/RSS guards observe delivery and are not hard socket/heap
limits; request/page/result bounds and the parent deadline remain independent.
"""
from __future__ import annotations

import asyncio
import math
import os
import re
import time
from urllib.parse import urlsplit, unquote


DEFAULTS = dict(max_requests=48, max_total_bytes=8*1024*1024,
                max_rss_bytes=1536*1024*1024, max_context_queries=24,
                proxy_connections_per_worker=8, ready_timeout_s=12.)


def browser_options(value):
    if not isinstance(value, dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid browser options')
    result = {**DEFAULTS, **value}
    for key in DEFAULTS:
        v = result[key]
        if key == 'ready_timeout_s':
            if type(v) not in (int, float) or not math.isfinite(v) or not 0 < v <= 60:
                raise ValueError('Invalid browser ready_timeout_s')
        elif type(v) is not int or v < 1:
            raise ValueError('Invalid browser option: '+key)
    if (result['max_requests'] > 256 or result['max_context_queries'] > 1000
            or result['proxy_connections_per_worker'] > 32):
        raise ValueError('Browser request/context window exceeds supported bound')
    return result


class BrowserFailure(Exception):
    def __init__(self, status, reason, metrics=None):
        self.status, self.reason = status, reason
        self.metrics = metrics
        super().__init__(reason)


class BrowserTransport:
    def __init__(self, config, send, receive):
        self.config, self.send, self.receive = config, send, receive
        self.options = browser_options(config.get('browser', {}))
        self.loop = asyncio.new_event_loop()
        self.pw = self.browser = self.context = None
        self.queries = 0
        self.context_key = None

    async def start(self, proxy, language):
        key = (proxy, language)
        if self.context is not None and (key != self.context_key or self.queries >= self.options['max_context_queries']):
            await self.context.close()
            self.context = None
        if self.pw is None:
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                raise BrowserFailure('dependency_error', 'Install demiflow[search-browser] and its Chromium binary') from None
            # The driver creates Chromium profiles via its TMPDIR. Keep them
            # under the parent's owned directory, including on forced cleanup.
            old_tmp = os.environ.get('TMPDIR')
            os.environ['TMPDIR'] = self.config['worker_directory']
            try:
                self.pw = await async_playwright().start()
            finally:
                if old_tmp is None: os.environ.pop('TMPDIR', None)
                else: os.environ['TMPDIR'] = old_tmp
            try:
                self.browser = await self.pw.chromium.launch(channel='chromium', headless=True,
                    downloads_path=self.config['worker_directory'],
                    ignore_default_args=['--disable-popup-blocking'],
                    args=['--disable-background-networking', '--disable-quic', '--disable-component-update',
                          '--disk-cache-size=16777216', '--media-cache-size=1048576'])
            except Exception:
                await self.pw.stop()
                self.pw = None
                raise BrowserFailure('dependency_error', 'Chromium could not start; verify browser installation and OS dependencies') from None
        if self.context is None:
            options = dict(service_workers='block', accept_downloads=False,
                           locale='en-US' if language == 'all' else language)
            if proxy:
                p = urlsplit(proxy)
                if p.scheme not in ('http', 'https') or not p.hostname:
                    raise BrowserFailure('configuration_error', 'Browser requires an HTTP(S) proxy or CONNECT relay')
                options['proxy'] = {'server':f'{p.scheme}://{p.hostname}:{p.port or 80}'}
                # Auth is handled on our CDP session below, scoped to a Proxy
                # challenge. Playwright's generic HTTP credentials can also
                # answer origin challenges, which must not receive this secret.
            self.context = await self.browser.new_context(**options)
            # Dedicated workers/websockets use targets outside this page's CDP
            # network session. Explicitly disable them rather than silently
            # bypassing parent admission and byte accounting.
            await self.context.add_init_script("""for (const name of ['Worker','SharedWorker','WebSocket']) {
                Object.defineProperty(globalThis,name,{value:class {constructor(){throw new DOMException('Disabled by search transport policy','NotSupportedError')}},configurable:false});
            }""")
            self.context_key, self.queries = key, 0

    def fetch(self, url, *, proxy, language, allowed_hosts, ready_script):
        return self.loop.run_until_complete(self._fetch(url, proxy, language, allowed_hosts, ready_script))

    def close(self):
        async def shutdown():
            async with asyncio.timeout(5):
                try:
                    if self.browser is not None: await self.browser.close()
                finally:
                    if self.pw is not None: await self.pw.stop()
        try:
            self.loop.run_until_complete(shutdown())
        finally:
            self.loop.close()

    async def _fetch(self, url, proxy, language, allowed_hosts, ready_script):
        from .worker_network import http_receipts
        await self.start(proxy, language)
        self.queries += 1
        page = await self.context.new_page()
        async def popup(p):
            await p.close()
        page.on('popup', popup)
        async def dialog(d):
            await d.dismiss()
        page.on('dialog', dialog)
        cdp = await self.context.new_cdp_session(page)
        main_frame = (await cdp.send('Page.getFrameTree'))['frameTree']['frame']['id']
        lock = asyncio.Semaphore(1)
        tasks = set()
        active = {}
        failure = None
        metrics = {'requests':0, 'blocked':0, 'decoded_bytes':0, 'rss_peak_bytes':0,
                   'context_query':self.queries, 'browser_version':self.browser.version}
        main_status = None
        redirects = 0
        auth_attempts = set()
        proxy_parts = urlsplit(proxy) if proxy else None

        def fail(status, reason):
            nonlocal failure
            if failure is None:
                failure = BrowserFailure(status, reason)

        async def command(name, value):
            try:
                return await cdp.send(name, value)
            except Exception:
                return None

        def close(network_id, *, code=None):
            item = active.pop(network_id, None)
            if item is None:
                return
            receipt = {'host':item['host'], 'method':item['method'],
                       'http_status':code if code is not None else item['status'],
                       'bytes':item['bytes'], 'elapsed_s':time.monotonic()-item['started'],
                       'retry_after':item.get('retry_after')}
            http_receipts.append(receipt)
            self.send({'event':'http_close', 'receipt':receipt})
            lock.release()

        async def paused(event):
            nonlocal main_status, redirects
            request_id = event['requestId']
            network_id = event.get('networkId', request_id)
            request = event['request']
            p = urlsplit(request['url'])
            if 'responseStatusCode' in event or 'responseErrorReason' in event:
                item = active.get(network_id)
                if item:
                    item['status'] = event.get('responseStatusCode')
                    headers = {h['name'].lower():h['value'] for h in event.get('responseHeaders', [])}
                    item['retry_after'] = headers.get('retry-after')
                    if event.get('resourceType') == 'Document': main_status = item['status']
                    length = headers.get('content-length', '')
                    if length.isdigit() and int(length) > self.options['max_total_bytes']-metrics['decoded_bytes']:
                        fail('response_too_large', 'Browser response Content-Length exceeds remaining byte budget')
                    if item['status'] in (301,302,303,307,308):
                        redirects += 1
                        if redirects > self.config['max_redirects']:
                            fail('redirect_limit', 'Browser redirect budget exceeded')
                        close(network_id)
                if failure:
                    await command('Fetch.failRequest', {'requestId':request_id, 'errorReason':'Aborted'})
                else:
                    await command('Fetch.continueResponse', {'requestId':request_id})
                return
            if (failure or p.scheme not in ('http','https') or p.username or
                not any(p.hostname == h or (p.hostname or '').endswith('.'+h) for h in allowed_hosts) or
                event.get('resourceType') not in {'Document','Script','XHR','Fetch','Stylesheet'} or
                (event.get('resourceType') == 'Document' and event.get('frameId') != main_frame)):
                metrics['blocked'] += 1
                await command('Fetch.failRequest', {'requestId':request_id, 'errorReason':'BlockedByClient'})
                return
            # Chromium emits another Request-stage event with a NEW Fetch ID
            # and the SAME Network ID after CONNECT proxy authentication. It
            # is still the admitted exchange, not a second request waiting for
            # its own semaphore. Re-acquiring here deadlocks HTTPS navigation.
            if network_id in active:
                item = active[network_id]
                item['auth_restarts'] = item.get('auth_restarts', 0) + 1
                if item['auth_restarts'] > 1:
                    fail('authentication_error', 'Repeated proxy authentication restart')
                    await command('Fetch.failRequest', {'requestId':request_id, 'errorReason':'Aborted'})
                else:
                    metrics['auth_restarts'] = metrics.get('auth_restarts', 0) + 1
                    await command('Fetch.continueRequest', {'requestId':request_id})
                return
            if metrics['requests'] >= self.options['max_requests']:
                fail('request_budget', 'Browser subrequest budget exceeded')
                await command('Fetch.failRequest', {'requestId':request_id, 'errorReason':'Aborted'})
                return
            metrics['requests'] += 1  # reserve before waiting; pending tasks are bounded
            await lock.acquire()
            if failure:
                lock.release()
                await command('Fetch.failRequest', {'requestId':request_id, 'errorReason':'Aborted'})
                return
            self.send({'event':'http_open', 'host':p.hostname, 'method':request['method']})
            grant = await asyncio.to_thread(self.receive)
            if grant.get('event') != 'http_grant':
                raise RuntimeError('Invalid browser HTTP admission')
            active[network_id] = {'host':p.hostname, 'method':request['method'], 'status':None,
                                  'bytes':0, 'started':time.monotonic()}
            await command('Fetch.continueRequest', {'requestId':request_id})

        def schedule(event):
            # Avoid an unbounded task for rejected requests: terminate page load
            # once the finite pending window is consumed.
            if len(tasks) >= self.options['max_requests']*2+8:
                fail('request_budget', 'Browser pending-event budget exceeded')
                return
            task = asyncio.create_task(paused(event))
            tasks.add(task)
            def done(t):
                tasks.discard(t)
                if not t.cancelled() and t.exception():
                    fail('browser_error', 'Browser admission callback failed')
            task.add_done_callback(done)

        async def authenticate(event):
            # Enabling Fetch on our CDP session also makes it responsible for
            # authentication. Never send proxy credentials to an origin server.
            challenge = event['authChallenge']
            origin = urlsplit(challenge.get('origin', ''))
            key = event['requestId']
            match = (proxy_parts is not None and proxy_parts.username is not None
                     and challenge.get('source') == 'Proxy'
                     and origin.hostname == proxy_parts.hostname
                     and (origin.port or 80) == (proxy_parts.port or 80))
            if match and key not in auth_attempts and len(auth_attempts) < self.options['max_requests']:
                auth_attempts.add(key)
                response = {'response':'ProvideCredentials', 'username':unquote(proxy_parts.username),
                            'password':unquote(proxy_parts.password or '')}
            else:
                response = {'response':'CancelAuth'}
                fail('authentication_error', 'Proxy authentication failed' if challenge.get('source') == 'Proxy'
                     else 'Origin authentication is not configured')
            await command('Fetch.continueWithAuth', {'requestId':key, 'authChallengeResponse':response})

        def data(event):
            size = event.get('dataLength', 0)
            metrics['decoded_bytes'] += size
            if event['requestId'] in active:
                active[event['requestId']]['bytes'] += size
            if metrics['decoded_bytes'] > self.options['max_total_bytes']:
                fail('response_too_large', 'Browser observed decoded bytes exceeded budget')

        async def guard():
            import psutil
            parent = psutil.Process(os.getpid())
            while True:
                try:
                    rss = sum(p.memory_info().rss for p in [parent, *parent.children(recursive=True)] if p.is_running())
                    metrics['rss_peak_bytes'] = max(metrics['rss_peak_bytes'], rss)
                    if rss > self.options['max_rss_bytes']:
                        fail('resource_limit', 'Browser worker process-tree RSS exceeded budget')
                except psutil.NoSuchProcess:
                    pass
                if failure:
                    await page.close()
                    return
                await asyncio.sleep(.1)

        cdp.on('Fetch.requestPaused', schedule)
        cdp.on('Fetch.authRequired', authenticate)
        cdp.on('Network.dataReceived', data)
        cdp.on('Network.loadingFinished', lambda e: close(e['requestId']))
        cdp.on('Network.loadingFailed', lambda e: close(e['requestId']))
        await cdp.send('Network.enable', {'maxTotalBufferSize':1048576, 'maxResourceBufferSize':262144})
        await cdp.send('Network.setCacheDisabled', {'cacheDisabled':True})
        await cdp.send('Fetch.enable', {'handleAuthRequests':True, 'patterns':[{'urlPattern':'*','requestStage':'Request'},
                                                   {'urlPattern':'*','requestStage':'Response'}]})
        watcher = asyncio.create_task(guard())
        value = None
        try:
            navigation = await page.goto(url, wait_until='domcontentloaded', timeout=self.config['timeout_s']*1000)
            if main_status is None and navigation is not None: main_status = navigation.status
            try:
                if main_status is not None and main_status < 400:
                    await page.wait_for_function(ready_script, timeout=self.options['ready_timeout_s']*1000)
            except Exception:
                if failure: raise failure
            content = await page.evaluate("max => {const s=document.documentElement.outerHTML; return new TextEncoder().encode(s).length<=max ? s : null}", self.config['max_bytes'])
            if content is None:
                raise BrowserFailure('response_too_large', 'Rendered DOM exceeds byte budget')
            value = {'html':content, 'url':page.url, 'status_code':main_status, 'metrics':metrics}
        except BrowserFailure as exc:
            exc.metrics = metrics
            raise
        except Exception as exc:
            if failure:
                failure.metrics = metrics
                raise failure
            code = re.search(r'net::[A-Z_]+', str(exc))
            reason = 'Browser navigation did not complete'+(': '+code.group(0) if code else '')
            raise BrowserFailure('network_error', reason, metrics) from None
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            await page.close()
            for task in list(tasks): task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for key in list(active): close(key)
        if failure: raise failure
        return value
