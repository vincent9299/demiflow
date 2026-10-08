"""Explicit Google web/image HTTP/browser backends; legacy adapters remain available.

Parsing never accepts HTTP 200, a JS gate, consent, or an unrecognized empty
layout as a successful zero-result search. Provider policy belongs here;
browser lifetime/admission/resource accounting belongs to BrowserTransport.
"""
from __future__ import annotations

import hashlib
import json
from urllib.parse import urlencode, urlsplit, parse_qs, urljoin

from .browser import BrowserTransport, BrowserFailure

PARSER_VERSION = 'google-web-dom-1'
_browser = None
READY = """() => Boolean(document.querySelector('a h3') ||
    document.querySelector('#captcha-form, #recaptcha') ||
    location.pathname.startsWith('/sorry') || location.hostname.startsWith('consent.') ||
    document.querySelector('#topstuff')?.innerText.match(/did not match any documents|找不到和|没有找到|未找到与/))"""


def request_url(query, parameters, source):
    page = parameters['pageno']
    if page > 50:
        raise BrowserFailure('unsupported_parameters', 'Google web supports at most 50 declared pages')
    base = source.get('base_url', 'https://www.google.com/search')
    parsed = urlsplit(base)
    if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
        raise BrowserFailure('configuration_error', 'Google endpoint must be an uncredentialed HTTP(S) URL without query')
    options = {'q':query, 'num':10, 'start':(page-1)*10,
               'hl':'en' if parameters['language']=='all' else parameters['language'],
               'safe':{0:'off', 1:'medium', 2:'active'}[parameters['safesearch']]}
    if source.get('engine')=='google_images':
        options.pop('num')
        options['udm']='2'
    if parameters['time_range']:
        options['tbs'] = 'qdr:'+{'day':'d','week':'w','month':'m','year':'y'}[parameters['time_range']]
    return base+'?'+urlencode(options)


def parse_page(content, url, code, max_results):
    from lxml import html
    p = urlsplit(url)
    if p.path.startswith('/sorry'):
        return 'captcha', 'Google unusual-traffic verification', []
    if p.hostname and p.hostname.startswith('consent.'):
        return 'consent_required', 'Google consent page requires an explicit policy', []
    if code == 429: return 'rate_limited', 'Google HTTP 429', []
    if code in (401,403): return ('authentication_error' if code==401 else 'access_denied'), f'Google HTTP {code}', []
    if code is None or code >= 400: return 'http_error', 'Google did not return a successful page', []
    try:
        tree = html.fromstring(content)
    except (ValueError, html.etree.ParserError):
        return 'parse_error', 'Invalid Google HTML', []
    if tree.xpath('//*[@id="captcha-form" or @id="recaptcha"] | //form[contains(@action,"/sorry/")]'):
        return 'captcha', 'Google verification form', []
    if tree.xpath('//form[contains(@action,"consent.google")]'):
        return 'consent_required', 'Google consent form', []
    results, seen = [], set()
    for anchor in tree.xpath('//a[.//h3]'):
        href = urljoin(url, anchor.get('href',''))
        target = urlsplit(href)
        if target.path == '/url' and (target.hostname or '').endswith('google.com'):
            params = parse_qs(target.query)
            href = (params.get('q') or params.get('url') or [''])[0]
            target = urlsplit(href)
        if target.scheme not in ('http','https') or not target.hostname or target.username or href in seen:
            continue
        if target.hostname == 'accounts.google.com' or (target.hostname in {'google.com','www.google.com'} and target.path in {'/search','/url','/preferences'}):
            continue
        title = ' '.join(anchor.xpath('.//h3')[0].text_content().split())[:1024]
        if not title: continue
        snippet = ''
        parent = anchor
        for _ in range(5):
            parent = parent.getparent()
            if parent is None or len(parent.xpath('.//h3')) > 1: break
            parts = parent.xpath('.//*[contains(concat(" ",normalize-space(@class)," ")," VwiC3b ") or contains(concat(" ",normalize-space(@class)," ")," yXK7lf ")]')
            if parts:
                snippet = ' '.join(parts[0].text_content().split())[:2048]
                break
        results.append({'url':href, 'title':title, 'content':snippet})
        seen.add(href)
        if len(results) >= max_results: break
    if results: return 'ok', '', results
    top = ' '.join(' '.join(tree.xpath('//*[@id="topstuff"]//text()')).split()).lower()
    if any(s in top for s in ('did not match any documents','找不到和','没有找到与','未找到与')):
        return 'no_results', 'Explicit Google no-results message', []
    if tree.xpath('//a[contains(@href,"/httpservice/retry/enablejs")]') or 'enablejs' in content[:100000].lower():
        return 'render_required', 'Google page requires JavaScript execution', []
    return 'parse_error', 'Google page has neither recognized results nor an explicit no-results message', []


def run(query, parameters, source, config, send, receive):
    global _browser
    from .worker_network import http_receipts
    backend = source['backend']
    image_search=source.get('engine')=='google_images'
    if image_search:
        from .google_images import parse_image_page, READY as IMAGE_READY, PARSER_VERSION as IMAGE_PARSER_VERSION
    result = {'backend':backend, 'parser_version':IMAGE_PARSER_VERSION if image_search else PARSER_VERSION, 'results':[],
              'capabilities':{'paging':True,'max_page':50,'time_range_support':True,
                              'safesearch':True,'language_support':True,'engine_type':'online'},
              'ignored_parameters':[]}
    try:
        url = request_url(query, parameters, source)
        if config.get('offline_audit'):
            raise BrowserFailure('configuration_error', 'Network disabled during source audit')
        if backend == 'http':
            from searx.network import get
            response = get(url, headers={'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'},
                           impersonate='chrome', raise_for_httperror=False)
            page = {'html':response.text, 'url':str(response.url), 'status_code':response.status_code}
        else:
            if _browser is None: _browser = BrowserTransport(config, send, receive)
            outgoing = config['outgoing'].get('proxies', {})
            proxy = outgoing.get('all://') or outgoing.get('https://')
            allowed = [urlsplit(url).hostname, 'google.com', 'gstatic.com', 'googleusercontent.com']
            page = _browser.fetch(url, proxy=proxy, language=parameters['language'],
                                  allowed_hosts=allowed, ready_script=IMAGE_READY if image_search else READY)
            result['browser_metrics_json'] = json.dumps(page['metrics'], sort_keys=True)
        result['response_sha256'] = hashlib.sha256(page['html'].encode()).hexdigest()
        parser=parse_image_page if image_search else parse_page
        result['status'], result['reason'], result['results'] = parser(
            page['html'], page['url'], page['status_code'], config['max_results'])
    except BrowserFailure as exc:
        result.update(status=exc.status, reason=exc.reason)
        if exc.metrics is not None:
            result['browser_metrics_json'] = json.dumps(exc.metrics, sort_keys=True)
    finally:
        result['http'] = list(http_receipts)
    return result
