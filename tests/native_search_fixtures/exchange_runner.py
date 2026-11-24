"""Run exchange scenarios through this runtime's search worker and print what really happened as JSON:
receipt status, merged results, HTTP receipts, and every request the loopback servers received.

Usage: python exchange_runner.py <suite>
Suites: transport, outgoing-proxy, outgoing-ips, network-names, network-names-default,
        network-names-outgoing, sqlite, loader, named, options, request-options, answers

``essentials`` reduces the output to the comparable facts stored in ``exchange_golden.json``; those were
recorded by running the same scenarios on the previous runtime (main 5673d101; the network-names, options,
request-options and answers suites on main 41f232fd, the same runtime).
"""
import hashlib
import json
import re
import socket
import sqlite3
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
DEFAULT_QUERY = [('oak', {})]
THREE = [('one', {}), ('two', {}), ('three', {})]
CONFIG = {'timeout_s': 10, 'max_bytes': 65536, 'max_results': 8, 'max_redirects': 3, 'outgoing': {}}
# Exception names of the previous runtime -> their names here (declared rename).
RENAMED = {'SearxEngineCaptchaException': 'SourceCaptcha', 'SearxEngineTooManyRequestsException': 'SourceRateLimited',
           'SearxEngineAccessDeniedException': 'SourceAccessDenied', 'SearxEngineAPIException': 'SourceError',
           'SearxEngineException': 'SourceError', 'SearxEngineResponseException': 'SourceError'}
KEPT_HEADERS = ('cookie', 'authorization', 'proxy-authorization', 'x-probe', 'x-keep', 'x-helper', 'accept',
                'accept-language', 'content-type', 'referer', 'origin', 'secret-key', 'sec-gpc',
                'x-pinterest-appstate', 'x-pinterest-source-url', 'x-pinterest-pws-handler')
RANDOM_BROWSER = re.compile(r'Mozilla/5\.0 \((Windows NT 10\.0; Win64; x64|X11; Linux x86_64); rv:\d+\.0\) '
                            r'Gecko/20100101 Firefox/\d+\.0')


# ---------------------------------------------------------------------------- comparable facts

def _agent(value):
    """User-Agent values that are random or version-bound by design compare by family."""
    if value is None:
        return None
    if RANDOM_BROWSER.fullmatch(value):
        return 'random desktop browser'
    if value.startswith('Nokia'):
        return 'random feature phone'
    if value.startswith(('SearXNG/', 'DemiFlow/')):
        return 'names the client'
    if value.startswith('Mozilla/5.0') and 'Chrome/' in value:
        return 'impersonated browser'
    return value


def _request(record):
    headers = {k.lower(): v for k, v in record['headers']}
    kept = {k: headers[k] for k in KEPT_HEADERS if k in headers}
    if kept.get('accept', '').startswith('text/html,application/xhtml+xml'):
        kept['accept'] = 'browser default'
    return {'to': record.get('server') or record.get('host'), 'method': record['method'],
            'path': re.sub(r'tgp=\d', 'tgp=N', record['path']), 'body': record['body'],
            'client': record.get('client'), 'headers': kept, 'browser_headers': 'sec-ch-ua' in headers,
            'http2_upgrade': 'upgrade' in headers, 'agent': _agent(headers.get('user-agent'))}


def _digest(value):
    text = json.dumps(value, sort_keys=True, ensure_ascii=False)
    for old, new in RENAMED.items():
        text = text.replace(old, new)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def essentials(output):
    """``{run key: comparable facts}`` of a suite output; repeated identical queries are numbered."""
    found, counts = {}, {}
    for run in output['runs']:
        base = f"{run['source']}|{run.get('query')}|{json.dumps(run.get('parameters') or {}, sort_keys=True)}"
        counts[base] = counts.get(base, -1) + 1
        if 'declare_error' in run:
            facts = {'declare_error': True}
        else:
            facts = {'status': run['status'], 'reason': RENAMED.get(run['reason'], run['reason']),
                     'ignored': run['ignored'], 'results': run['result_count'],
                     'http': [[r.get('method'), r.get('http_status'), r.get('bytes')] for r in run['http']],
                     'requests': [_request(r) for r in run['server']],
                     'merged': None if run['merged'] is None else _digest(run['merged'])}
        found[f'{base}|{counts[base]}'] = facts
    return found


# ---------------------------------------------------------------------------- scenarios

def resolves_to_loopback(name):
    try:
        return socket.gethostbyname(name) == '127.0.0.1'
    except OSError:
        return False


def can_bind(address):
    try:
        with socket.socket() as probe:
            probe.bind((address, 0))
        return True
    except OSError:
        return False


def transport(urls):
    A, B, P1, P2 = urls
    base = {'module': 'exchange_probe', 'revision': 'x1', 'base_url': A, 'other_url': B, 'enable_http': True}

    def probe(name, mode, queries=None, **settings):
        return {'name': name, **base, 'mode': mode, **settings}, queries or DEFAULT_QUERY

    def net(name, mode, network, queries=None):
        # A ``network`` mapping replaces the transport options given as source attributes.
        declared = {'name': name, **{k: v for k, v in base.items() if k != 'enable_http'}, 'mode': mode,
                    'network': network}
        return declared, queries or DEFAULT_QUERY

    modes = ['plain', 'none', 'empty', 'android', 'safari', 'badimp', 'curlopt', 'cookies', 'twocookies', 'headers',
             'follow', 'nofollow', 'limit1', 'softmax', 'post301', 'post302', 'post303', 'post307', 'post308', 'cross',
             'same', 'cookieredirect', 'auth', 'userinfo', 'postdata', 'poststr', 'postjson', 'postcontent',
             'postempty', 'put', 'getdata', 's403', 's402', 's429', 's401', 's404', 's500', 's503re', 's503cf',
             's429cf', 's403cf', 's403fw', 'noraise403', 'noraise500', 'big', 'json', 'verifyfalse', 'decline',
             'helpers', 'helpersredirect', 'multi']
    items = [probe('x' + mode, mode) for mode in modes]
    items += [
        probe('xsetcookie', 'setcookie', [('first', {}), ('second', {}), ('third', {})]),
        probe('xparams', 'plain', [('oak', {'pageno': 2, 'safesearch': 1, 'time_range': 'day', 'language': 'zh-CN',
                                            'engine_data': {'xparams': {'k': 'v'}}}),
                                   ('oak', {'language': 'all'}), ('oak', {'language': 'zh-tw'})]),
        probe('xnoal', 'plain', send_accept_language_header=False),
        probe('xfixedlang', 'plain', language='zh'),
        probe('xhttpoff', 'plain', enable_http=False),
        probe('xattrproxy', 'plain', proxies={'all://': P1}),
        probe('xattrlocal', 'plain', local_addresses='127.0.0.2'),
        probe('xattrhttp1', 'plain', enable_http2=False),
        probe('xattrmaxredir', 'follow', max_redirects=1),
        net('xnetnohttp', 'plain', {}),
        net('xnetplain', 'plain', {'enable_http': True}),
        net('xnetproxyall', 'plain', {'proxies': {'all://': P1}, 'enable_http': True}),
        net('xnetproxyhttp', 'plain', {'proxies': {'http://': P1}, 'enable_http': True}),
        net('xnetproxykey', 'plain', {'proxies': {'http': P1}, 'enable_http': True}),
        net('xnetproxystr', 'plain', {'proxies': P1, 'enable_http': True}),
        net('xnetproxylist', 'plain', {'proxies': {'all://': [P1, P2]}, 'enable_http': True}, THREE),
        net('xnetproxyboth', 'plain', {'proxies': {'all://': P1, 'http://': P2}, 'enable_http': True}),
        net('xnetproxyhttps', 'plain', {'proxies': {'https://': P1}, 'enable_http': True}),
        net('xnetproxynohttp', 'plain', {'proxies': {'http://': P1}}),
        net('xnetlocal', 'plain', {'local_addresses': '127.0.0.2', 'enable_http': True}),
        net('xnetlocallist', 'plain', {'local_addresses': ['127.0.0.2', '127.0.0.3'], 'enable_http': True}, THREE),
        net('xnetlocalcidr', 'plain', {'local_addresses': '127.0.0.8/30', 'enable_http': True}, THREE),
        net('xnethttp1', 'plain', {'enable_http2': False, 'enable_http': True}),
        net('xnethttp2', 'plain', {'enable_http2': True, 'enable_http': True}),
        net('xnetredirects', 'follow', {'max_redirects': 1, 'enable_http': True}),
        net('xnetretries', 'plain', {'retries': 2, 'enable_http': True}),
        net('xnetshared', 'plain', 'xnetlocal'),
        probe('xparamscross', 'paramscross'),
        # The redirect answers with the same host and port, the host name in capitals: the same origin.
        probe('xparamscase', 'paramscase', base_url=A.replace('127.0.0.1', 'localhost')),
        probe('xcredcase', 'credcase', base_url=A.replace('127.0.0.1', 'localhost')),
    ]
    return items, dict(CONFIG)


def outgoing_proxy(urls):
    A, B, P1, P2 = urls
    base = {'module': 'exchange_probe', 'revision': 'x1', 'base_url': A, 'other_url': B}
    items = [({'name': 'xplain', **base, 'mode': 'plain', 'enable_http': True}, DEFAULT_QUERY),
             ({'name': 'xfollow', **base, 'mode': 'follow', 'enable_http': True}, DEFAULT_QUERY),
             ({'name': 'xnetplain', **base, 'mode': 'plain', 'network': {'enable_http': True}}, DEFAULT_QUERY),
             ({'name': 'xnetown', **base, 'mode': 'plain',
               'network': {'proxies': {'all://': P2}, 'enable_http': True}}, DEFAULT_QUERY)]
    return items, {**CONFIG, 'outgoing': {'proxies': {'all://': P1}, 'max_redirects': 1}}


def outgoing_ips(urls):
    A, B, P1, P2 = urls
    item = {'name': 'xplain', 'module': 'exchange_probe', 'revision': 'x1', 'base_url': A, 'other_url': B,
            'mode': 'plain', 'enable_http': True}
    return [(item, DEFAULT_QUERY)], {**CONFIG, 'outgoing': {'source_ips': '127.0.0.2'}}


OTHER_HOST = 'https://probe.example'  # reaches server A through route_clients()


def network_names(urls):
    """Sources that name the network they send through, in each of the three suites below."""
    A, B, P1, P2 = urls
    base = {'module': 'exchange_probe', 'revision': 'x1', 'base_url': A, 'other_url': B, 'mode': 'plain'}

    def probe(name, **settings):
        return {'name': name, **base, 'enable_http': True, **settings}, DEFAULT_QUERY

    def named(name, network, **settings):
        return {'name': name, **base, 'network': network, **settings}, DEFAULT_QUERY

    # A declared source, a catalog source that is not declared, ipv4, an unknown name; yandex images next
    # to a yandex that sends through a proxy. A named network replaces the source's own transport options.
    names = [
        named('xholder', {'proxies': {'all://': P1}, 'enable_http': True}),
        named('xshared', 'xholder'),
        named('xsharedattr', 'xholder', proxies={'all://': P2}, enable_http=True),
        probe('xholderattr', proxies={'all://': P2}),
        named('xsharedattr2', 'xholderattr'),
        named('xchain', 'xshared'),
        named('xipv4', 'ipv4', base_url=OTHER_HOST),
        named('xipv4http', 'ipv4'),
        named('xunknown', 'nosuchnetwork'),
        named('xviagoogle', 'google', base_url=OTHER_HOST),
        named('xviabing', 'bing', base_url=OTHER_HOST, proxies={'all://': P2}, enable_http=True),
        ({'name': 'mwmbl', 'network': {'proxies': {'all://': P1}, 'enable_http': True}}, []),
        named('xviamwmbl', 'mwmbl'),
        ({'name': 'yandex', 'network': {'proxies': {'all://': P1}}}, DEFAULT_QUERY),
        ({'name': 'yandex images'}, DEFAULT_QUERY),
        ({'name': 'qwant images', 'network': {'proxies': {'all://': P2}}}, DEFAULT_QUERY),
    ]
    # The catalog sources with a default network name when the named source is not declared.
    default = [
        ({'name': 'yandex images', 'proxies': {'all://': P2}}, DEFAULT_QUERY),
        ({'name': 'brave.images', 'proxies': {'all://': P2}}, DEFAULT_QUERY),
        ({'name': 'qwant images', 'proxies': {'all://': P2}}, DEFAULT_QUERY),
        probe('xplain'),
    ]
    # Session-wide proxies and a session-wide named network.
    outgoing = [
        named('xnamednet', 'viap2'),
        named('xipv4', 'ipv4', base_url=OTHER_HOST),
        ({'name': 'yandex images'}, DEFAULT_QUERY),
        ({'name': 'brave.images'}, DEFAULT_QUERY),
        probe('xplain'),
    ]
    session = {'proxies': {'all://': P1}, 'networks': {'viap2': {'proxies': {'all://': P2}, 'enable_http': True}}}
    return {'network-names': (names, dict(CONFIG)), 'network-names-default': (default, dict(CONFIG)),
            'network-names-outgoing': (outgoing, {**CONFIG, 'outgoing': session})}


RESULT = "[{'url': 'https://example.org/' + NAME, 'title': NAME}]"
ERRORS = '''
def errors():
    from demiflow.collect.native_search import adapter as m
    return m.SourceCaptcha, m.SourceRateLimited, m.SourceAccessDenied, m.SourceError
'''
OFFLINE = "engine_type = 'offline'\ncategories = ['general']\nNAME = '@NAME@'\n"
SEARCH = "def search(query, params):\n    return " + RESULT + "\n"
LOADER = {
    'setupnone': OFFLINE + "def setup(settings):\n    pass\n" + SEARCH,
    'setuptrue': OFFLINE + "def setup(settings):\n    return True\n" + SEARCH,
    'setupfalse': OFFLINE + "def setup(settings):\n    return False\n" + SEARCH,
    'setupzero': OFFLINE + "def setup(settings):\n    return 0\n" + SEARCH,
    'setupraise': OFFLINE + "def setup(settings):\n    raise ValueError('declined')\n" + SEARCH,
    'setupattr': OFFLINE + "setup = 'not callable'\n" + SEARCH,
    'setupsees': OFFLINE + "seen = {}\ndef setup(settings):\n    seen.update(keys=sorted(settings))\n"
                 "def search(query, params):\n    import json\n    return [{'url': 'https://example.org/s', "
                 "'title': json.dumps(seen)}]\n",
    'initnone': OFFLINE + "def init(settings):\n    pass\n" + SEARCH,
    'initfalse': OFFLINE + "def init(settings):\n    return False\n" + SEARCH,
    'inittrue': OFFLINE + "def init(settings):\n    return True\n" + SEARCH,
    'initraise': OFFLINE + "def init(settings):\n    raise RuntimeError('boom')\n" + SEARCH,
    'required': OFFLINE + "needed = None\n" + SEARCH,
    'requiredset': OFFLINE + "needed = None\n" + SEARCH,
    'inactive': OFFLINE + "inactive = True\n" + SEARCH,
    'disabled': OFFLINE + "disabled = True\n" + SEARCH,
    'catstr': OFFLINE.replace("categories = ['general']", "categories = 'images, web'") + SEARCH,
    'cattab': OFFLINE.replace("categories = ['general']", "categories = ['custom']") + SEARCH,
    'catempty': OFFLINE.replace("categories = ['general']", "categories = []") + SEARCH,
    'offline': OFFLINE + SEARCH,
    'dicttype': OFFLINE.replace("'offline'", "'online_dictionary'") + SEARCH,
    'oddtype': OFFLINE.replace("'offline'", "'nonsense'") + SEARCH,
    'norequest': "categories = ['general']\ndef response(resp):\n    return []\n",
    'noresponse': "categories = ['general']\nbase_url = ''\ndef request(query, params):\n"
                  "    params['url'] = base_url + '/echo'\n",
    'importerror': "import no_such_module_for_xparity\n" + OFFLINE + SEARCH,
    'syntaxerror': "def (\n",
    'valueerror': "raise ValueError('at import')\n",
    'systemexit': "raise SystemExit(3)\n",
    'netmodule': OFFLINE + "from urllib import request as network\n" + SEARCH,
    'aboutbad': OFFLINE + "about = {'bogus_key': 1}\n" + SEARCH,
    'aboutok': OFFLINE + "about = {'website': 'https://example.org', 'results': 'JSON'}\n" + SEARCH,
    'paging0': OFFLINE + "paging = False\n" + SEARCH,
    'maxpage': OFFLINE + "paging = True\nmax_page = 2\n" + SEARCH,
    'notime': OFFLINE + "time_range_support = False\n" + SEARCH,
    'withtime': OFFLINE + "time_range_support = True\npaging = True\n" + SEARCH,
    'nosafe': OFFLINE + "safesearch = False\nlanguage_support = False\n" + SEARCH,
    'yessafe': OFFLINE + "safesearch = True\nlanguage_support = True\n" + SEARCH,
    'seesparams': OFFLINE + "paging = True\ntime_range_support = True\nlanguage = 'zh'\n"
                  "def search(query, params):\n    import json\n    view = {k: v for k, v in params.items() "
                  "if k != 'locale'}\n    view['locale'] = params.get('locale')\n"
                  "    return [{'url': 'https://example.org/p', "
                  "'title': json.dumps(view, sort_keys=True, default=str)}]\n",
    'empty': OFFLINE + "def search(query, params):\n    return []\n",
    'none': OFFLINE + "def search(query, params):\n    return None\n",
    'nourl': OFFLINE + "def search(query, params):\n    return [{'title': 'no url'}]\n",
    'badurl': OFFLINE + "def search(query, params):\n    return [{'url': 12, 'title': 'bad url'}]\n",
    'notlist': OFFLINE + "def search(query, params):\n    return 'text'\n",
    'families': OFFLINE + "def search(query, params):\n    return [\n"
                "        {'url': 'https://example.org/a', 'title': 'A', 'content': 'first'},\n"
                "        {'suggestion': 'try this'}, {'correction': 'fixed'}, {'answer': '42'},\n"
                "        {'infobox': 'Box', 'id': 'https://example.org/box', 'content': 'about', "
                "'urls': [{'title': 'site', 'url': 'https://example.org/site'}]},\n"
                "        {'engine_data': 'token', 'key': 'next'},\n"
                "        {'url': 'https://example.org/a', 'title': 'A longer title', 'content': 'x'},\n"
                "        {'url': 'https://example.org/img', 'title': 'I', 'template': 'images.html', "
                "'img_src': 'https://example.org/i.png', 'thumbnail_src': 'https://example.org/t.png'}]\n",
    'toomany': OFFLINE + "def search(query, params):\n    return [{'url': 'https://example.org/%d' % i, "
               "'title': str(i)} for i in range(20)]\n",
    'raisecaptcha': OFFLINE + ERRORS + "def search(query, params):\n    raise errors()[0]()\n",
    'raiserate': OFFLINE + ERRORS + "def search(query, params):\n    raise errors()[1]()\n",
    'raisedenied': OFFLINE + ERRORS + "def search(query, params):\n    raise errors()[2]()\n",
    'raiseapi': OFFLINE + ERRORS + "def search(query, params):\n    raise errors()[3]('api said no')\n",
    'raisevalue': OFFLINE + "def search(query, params):\n    raise ValueError('bad page')\n",
    'raisekey': OFFLINE + "def search(query, params):\n    return [{}['missing']]\n",
    'raiseimport': OFFLINE + "def search(query, params):\n    import no_such_module_for_xparity\n",
}
LOADER_QUERIES = {
    'paging0': [('oak', {}), ('oak', {'pageno': 2})],
    'maxpage': [('oak', {'pageno': 2}), ('oak', {'pageno': 3})],
    'notime': [('oak', {}), ('oak', {'time_range': 'day'})],
    'withtime': [('oak', {'time_range': 'week', 'pageno': 2})],
    'nosafe': [('oak', {'safesearch': 1}), ('oak', {'language': 'all'}), ('oak', {'safesearch': 2, 'language': 'de'})],
    'yessafe': [('oak', {'safesearch': 1, 'language': 'de'})],
    'seesparams': [('oak', {'pageno': 2, 'safesearch': 2, 'time_range': 'year', 'language': 'fr-CA',
                            'engine_data': {'xlseesparams': {'next': 'tok'}, 'other': {'a': 1}}})],
    'initnone': [('oak', {}), ('pine', {})],
    'initfalse': [('oak', {}), ('pine', {})],
    'initraise': [('oak', {}), ('pine', {})],
}
LOADER_SETTINGS = {'requiredset': {'needed': 'value'}, 'aboutok': {'about': {'wikidata_id': 'Q1'}},
                   'setupsees': {'extra_option': 5}}


def loader(urls, work):
    directory = work / 'adapters'
    directory.mkdir()
    sys.path.insert(0, str(directory))
    items = []
    for name, source in LOADER.items():
        (directory / f'xl_{name}.py').write_text(source.replace('@NAME@', name))
        declared = {'name': 'xl' + name, 'module': 'xl_' + name, 'revision': 'x1', 'base_url': urls[0],
                    'enable_http': True, **LOADER_SETTINGS.get(name, {})}
        items.append((declared, LOADER_QUERIES.get(name, DEFAULT_QUERY)))
    return items, dict(CONFIG)


def sqlite(work):
    database = work / 'source.db'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text, n int, link text, body text)')
        db.executemany('insert into docs values (?, ?, ?, ?)', [
            ('oak', 2, 'https://example.org/oak', 'An oak tree'), ('oak leaf', 3, 'https://example.org/leaf', 'A leaf'),
            ('pine', 5, 'https://example.org/pine', None), ('oak', 2, 'https://example.org/oak', 'An oak tree'),
            ('oak bark', 7, 'https://example.org/bark', 'Bark')])
    base = {'engine': 'sqlite', 'database': str(database)}
    main = "select link as url, label as title, body as content from docs where label like :wildcard order by n"
    cases = {
        'default': ({'query_str': 'select label, n from docs where label = :query'}, DEFAULT_QUERY),
        'keyvalue': ({'query_str': 'select label, n from docs where label = :query', 'result_type': 'KeyValue'},
                     DEFAULT_QUERY),
        'kvwild': ({'query_str': 'select label, n, body from docs where label like :wildcard order by n'},
                   [('oak', {}), ('oak bark', {}), ('none', {})]),
        'main': ({'query_str': main, 'result_type': 'MainResult'}, [('oak', {}), ('pine', {}), ('none', {})]),
        'mainextra': ({'query_str': "select link as url, label as title, body as content, 'https://example.org/i.png' "
                                    "as img_src, 'https://example.org/t.png' as thumbnail, 'An Author' as author, "
                                    "'12' as views, 'meta' as metadata from docs where label = :query",
                       'result_type': 'MainResult'}, DEFAULT_QUERY),
        'mainnourl': ({'query_str': 'select label as title from docs where label = :query',
                       'result_type': 'MainResult'}, DEFAULT_QUERY),
        'mainunknown': ({'query_str': 'select link as url, label from docs where label = :query',
                         'result_type': 'MainResult'}, DEFAULT_QUERY),
        'othertype': ({'query_str': 'select label, n from docs where label = :query', 'result_type': 'Other'},
                      DEFAULT_QUERY),
        'limit2': ({'query_str': main, 'result_type': 'MainResult', 'limit': 2},
                   [('oak', {}), ('oak', {'pageno': 2}), ('oak', {'pageno': 3})]),
        'kvlimit2': ({'query_str': 'select label, n from docs where label like :wildcard order by n', 'limit': 2},
                     [('oak', {}), ('oak', {'pageno': 2})]),
        'limitstr': ({'query_str': main, 'result_type': 'MainResult', 'limit': '2'}, DEFAULT_QUERY),
        'limitzero': ({'query_str': main, 'result_type': 'MainResult', 'limit': 0}, DEFAULT_QUERY),
        'limitbig': ({'query_str': main, 'result_type': 'MainResult', 'limit': 5000}, DEFAULT_QUERY),
        'nodb': ({'query_str': main, 'result_type': 'MainResult', 'database': str(work / 'missing.db')},
                 DEFAULT_QUERY),
        'noquery': ({}, DEFAULT_QUERY),
        'notselect': ({'query_str': "delete from docs where label = :query"}, DEFAULT_QUERY),
        'upperselect': ({'query_str': 'SELECT label, n FROM docs WHERE label = :query'}, DEFAULT_QUERY),
        'categories': ({'query_str': main, 'result_type': 'MainResult', 'categories': ['images']}, DEFAULT_QUERY),
        'nopaging': ({'query_str': main, 'result_type': 'MainResult', 'paging': False}, [('oak', {'pageno': 2})]),
        'badsql': ({'query_str': 'select nothing from nowhere where x = :query'}, DEFAULT_QUERY),
        # the same rows whatever the columns of the query are called and however its text ends
        'samename': ({'query_str': 'select label as a, n as a, link as b from docs where label like :wildcard '
                                   'order by n'}, DEFAULT_QUERY),
        'mainsamename': ({'query_str': 'select link as url, body as title, label as title from docs '
                                       'where label = :query', 'result_type': 'MainResult'}, DEFAULT_QUERY),
        'truename': ({'query_str': 'select label, true, n > 2 from docs where label like :wildcard order by n'},
                     DEFAULT_QUERY),
        'exprnames': ({'query_str': "select label || '!', n + 1, upper(link) from docs where label like :wildcard "
                                    "order by n"}, DEFAULT_QUERY),
        'quotednames': ({'query_str': 'select label as "x y", n as "a`b", link as "q""r", body as "" from docs '
                                      'where label like :wildcard order by n'}, DEFAULT_QUERY),
        'takennames': ({'query_str': 'select label as demiflow_n0, n as DEMIFLOW_N1, link as demiflow_taken from docs '
                                     'where label like :wildcard order by n'}, DEFAULT_QUERY),
        'star': ({'query_str': 'select * from docs where label like :wildcard order by n'}, DEFAULT_QUERY),
        'types': ({'query_str': 'select label, cast(body as blob) as raw, n * 1.5 as real, null as missing, n '
                                'from docs where label like :wildcard order by n'}, DEFAULT_QUERY),
        'linecomment': ({'query_str': 'select label, n from docs where label like :wildcard order by n -- by n',
                         'limit': 2}, [('oak', {}), ('oak', {'pageno': 2})]),
        'blockcomment': ({'query_str': 'select label, n from docs where label like :wildcard order by n /* by n',
                          'limit': 2}, [('oak', {}), ('oak', {'pageno': 2})]),
        'compound': ({'query_str': 'select label, n from docs where n < 3 union all select label, n from docs '
                                   'where n > 4 order by n', 'limit': 2}, [('oak', {}), ('oak', {'pageno': 2})]),
        'aggregate': ({'query_str': 'select label, count(*) as c, group_concat(n) as ns from docs '
                                    'where label like :wildcard group by label order by label'}, DEFAULT_QUERY),
        'joined': ({'query_str': 'select d.label, s.c from docs d join (select label, count(*) as c from docs '
                                 'group by label) s on s.label = d.label where d.label like :wildcard order by d.n',
                    'limit': 2}, [('oak', {}), ('oak', {'pageno': 2})]),
        'distinct': ({'query_str': 'select distinct label, n from docs where label like :wildcard order by n desc'},
                     DEFAULT_QUERY),
    }
    items = [({'name': 'xq' + name, **base, **settings}, queries) for name, (settings, queries) in cases.items()]
    return items, dict(CONFIG)


BUILT_IN = ['google', 'bing', 'yandex', 'mwmbl', 'wikipedia', 'wikisearch', 'google images', 'bing images',
            'yandex images', 'baidu images', 'brave.images', 'sogou images', 'quark images', 'duckduckgo images',
            'qwant images', 'pinterest', 'wikicommons.images', 'public domain image archive', 'pexels', 'openverse',
            'unsplash']
PLATFORM = ['artic_images', 'cleveland_images', 'flickr_images', 'fws_images', 'gbif_images', 'getty_images',
            'inaturalist_images', 'loc_images', 'metmuseum_images', 'nasa_images', 'openverse_images',
            'rijksmuseum_images', 'unsplash_images', 'usda_images', 'wellcome_images']
NAMED_QUERIES = [('oak', {}), ('oak', {}), ('oak', {'language': 'zh-CN', 'safesearch': 1})]


def named():
    items = [(name, NAMED_QUERIES) for name in BUILT_IN]
    items += [({'name': 'google http', 'engine': 'google', 'backend': 'http'}, NAMED_QUERIES),
              ({'name': 'google images http', 'engine': 'google_images', 'backend': 'http', 'categories': ['images']},
               NAMED_QUERIES)]
    items += [({'name': name.replace('_images', ' module'), 'module': 'demiflow.services.engines.' + name,
                'revision': 'r1', 'categories': ['images']}, NAMED_QUERIES) for name in PLATFORM]
    return items, {**CONFIG, 'max_bytes': 1 << 20, 'max_results': 100}


# One option of a built-in source changed in its declaration: (source, option, value, host the option
# pointed to). Endpoint options get the host override.example, numbers and switches another value.
OPTION_VARIANTS = [
    ('bing', 'base_url', 'https://override.example', 'www.bing.com'),
    ('yandex', 'base_url_images', 'https://override.example/images/search', 'yandex.com'),
    ('yandex', 'base_url_web', 'https://override.example/search/site/', 'yandex.com'),
    ('mwmbl', 'api_url', 'https://override.example/api/v1', 'api.mwmbl.org'),
    ('wikipedia', 'rest_v1_summary_url', 'https://override.example/api/rest_v1/page/summary/{title}',
     'en.wikipedia.org'),
    ('bing images', 'base_url', 'https://override.example', 'www.bing.com'),
    ('yandex images', 'base_url_images', 'https://override.example/images/search', 'yandex.com'),
    ('yandex images', 'base_url_web', 'https://override.example/search/site/', 'yandex.com'),
    ('baidu images', 'image_base_url', 'https://override.example/', 'image.baidu.com'),
    ('baidu images', 'results_per_page', 11, None),
    ('brave.images', 'base_url', 'https://override.example/', 'search.brave.com'),
    ('brave.images', 'brave_spellcheck', True, None),
    ('sogou images', 'base_url', 'https://override.example', 'pic.sogou.com'),
    ('quark images', 'results_per_page', 11, None),
    ('qwant images', 'api_url', 'https://override.example/v3/search/', 'api.qwant.com'),
    ('qwant images', 'base_url', 'https://override.example', 'www.qwant.com'),
    ('pinterest', 'base_url', 'https://override.example', 'www.pinterest.com'),
    ('wikicommons.images', 'page_size', 11, None),
    ('wikicommons.images', 'wc_api_url', 'https://override.example/w/api.php', 'commons.wikimedia.org'),
    ('public domain image archive', 'page_size', 21, None),
    ('public domain image archive', 'pdia_base_url', 'https://override.example', 'pdimagearchive.org'),
    ('pexels', 'base_url', 'https://override.example', 'www.pexels.com'),
    ('pexels', 'enable_http2', True, None),
    ('pexels', 'results_per_page', 21, None),
    ('openverse', 'base_url', 'https://override.example/v1/images/', 'api.openverse.org'),
    ('openverse', 'page_size', 21, None),
    ('unsplash', 'base_url', 'https://override.example/', 'unsplash.com'),
    ('unsplash', 'page_size', 21, None),
    ('unsplash', 'search_url', 'https://override.example/napi/search/photos?', 'unsplash.com'),
]
OPTION_QUERIES = [('oak', {}), ('oak', {'pageno': 2})]
# Options that shape the search request itself: the query template, time-range tables, the language list
# and the User-Agent pool. The values are JSON as a stored source declaration carries them.
REQUEST_OPTION_VARIANTS = [
    ('openverse', 'search_string', '?page={page}&page_size={page_size}&format=json&license=cc0&{query}', None),
    ('bing images', 'time_map', {'day': 1, 'week': 2, 'month': 3, 'year': 4}, None),
    ('google', 'time_range_dict', {'day': 'h', 'week': 'h', 'month': 'h', 'year': 'h'}, None),
    ('google', 'nokia_useragents', ['Probe-UA/1.0'], None),
    ('pexels', 'time_range_map', {'day': 'last_week', 'week': 'last_week', 'month': 'last_week',
                                  'year': 'last_week'}, None),
    ('yandex', 'yandex_supported_langs', [], None),
]
REQUEST_OPTION_QUERIES = [('oak', {}), ('oak', {'time_range': 'day'}), ('oak', {'time_range': 'year'})]
UNEXPECTED_KINDS = ('html', 'emptyobject', 's403', 's429', 's500')


def options(catalog, variants=OPTION_VARIANTS, queries=OPTION_QUERIES):
    """One source per option variant: the catalog entry with that option changed. The first variant of a
    source keeps its name; further ones are declared under another name with the same settings."""
    items, seen = [], set()
    for index, (source, option, value, host) in enumerate(variants):
        declared = {**catalog[source], option: value}
        if source in seen:
            declared.update(name=f'variant {index:02d}', shortcut=f'variant {index:02d}')
        seen.add(source)
        state = {'label': f'{source} [{option}]', 'alias': {'override.example': host} if host else {}}
        items.append((declared, [(query, parameters, state) for query, parameters in queries]))
    return items, {**CONFIG, 'max_bytes': 1 << 20, 'max_results': 100}


def request_options(catalog):
    return options(catalog, REQUEST_OPTION_VARIANTS, REQUEST_OPTION_QUERIES)


def answers():
    """Every source asked once per unexpected answer; its preparation requests are answered normally."""
    items, config = named()
    found = []
    for source, _ in items:
        name = source if isinstance(source, str) else source['name']
        found.append((source, [('oak', {}, {'label': f'{name} [{kind}]', 'unexpected': kind})
                               for kind in UNEXPECTED_KINDS]))
    return found, config


# ---------------------------------------------------------------------------- loopback destination for named sources

class ProviderRecords:
    """One loopback server that stands in for every provider host; requests keep their original host."""

    def __init__(self):
        from provider_pages import answer
        self.records, self.lock = [], threading.Lock()
        # Per run: hosts that stand for another host, and the unexpected answer to give (None: fixed pages).
        self.state = {'alias': {}, 'unexpected': None}
        records, lock, state = self.records, self.lock, self.state

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def serve(self):
                length = int(self.headers.get('Content-Length') or 0)
                body = self.rfile.read(length) if length else b''
                _, marker, host, rest = self.path.split('/', 3)
                rest = '/' + rest
                with lock:
                    records.append({'host': host, 'method': self.command, 'path': rest, 'body': body.decode('latin-1'),
                                    'headers': [[k, v] for k, v in self.headers.items() if k.lower() != 'host']})
                status, extra, payload = answer(state['alias'].get(host, host), urlsplit(rest).path,
                                                state['unexpected'])
                self.send_response(status)
                for name, value in extra:
                    self.send_header(name, value)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(payload)

            do_GET = do_POST = do_PUT = do_HEAD = serve

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.address = '127.0.0.1:' + str(self.server.server_port)

    def route_clients(self):
        """Send every request of the HTTP clients to this server instead of its real destination."""
        route_clients('http://' + self.address, loopback=True)

    def prepare(self, alias=None, unexpected=None, **_):
        self.state.update(alias=alias or {}, unexpected=unexpected)

    def take(self):
        with self.lock:
            found = list(self.records)
            self.records.clear()
        return found

    def normalize(self, text):
        return text.replace(self.address, 'LOCAL')


def route_clients(destination, loopback=False):
    """Send the requests of the HTTP clients to ``destination`` (path ``/__h/<host>/<path>``) instead of
    their real host; requests for 127.0.0.1 are left alone unless ``loopback``."""
    from curl_cffi import requests
    send = requests.Session.request

    def routed(client, method, url, **kwargs):
        parts = urlsplit(str(url))
        if parts.hostname == '127.0.0.1' and not loopback:
            return send(client, method, url, **kwargs)
        local = f"{destination}/__h/{parts.netloc}{parts.path or '/'}" + ('?' + parts.query if parts.query else '')
        reply = send(client, method, local, **kwargs)
        reply.url = str(url)
        return reply

    requests.Session.request = routed


# ---------------------------------------------------------------------------- runner

def main(suite):
    sys.path.insert(0, str(HERE))  # exchange_probe, exchange_server and provider_pages are modules here
    work = Path(tempfile.mkdtemp(prefix='exchange-'))
    skipped = []
    if suite in ('named', 'options', 'request-options', 'answers'):
        destination = ProviderRecords()
        destination.route_clients()
        if suite in ('options', 'request-options'):
            from demiflow.collect.native_search.config import catalog
            entries = {entry['name'].lower(): entry for entry in catalog()['engines']}
            items, config = options(entries) if suite == 'options' else request_options(entries)
        else:
            items, config = named() if suite == 'named' else answers()
    else:
        from exchange_server import Servers
        destination = Servers()
        urls = tuple(destination.url(tag) for tag in ('A', 'B', 'P1', 'P2'))
        if suite == 'sqlite':
            items, config = sqlite(work)
        elif suite == 'loader':
            items, config = loader(urls, work)
        elif suite.startswith('network-names'):
            route_clients(urls[0])
            items, config = network_names(urls)[suite]
        else:
            items, config = {'transport': transport, 'outgoing-proxy': outgoing_proxy,
                             'outgoing-ips': outgoing_ips}[suite](urls)
        if not all(can_bind(a) for a in ('127.0.0.2', '127.0.0.3', '127.0.0.9', '127.0.0.10')):
            # Hosts without extra loopback addresses cannot run the local-address scenarios.
            local = {s['name'] for s, _ in items if 'local' in json.dumps(s)}
            if 'source_ips' in config['outgoing']:
                local = {s['name'] for s, _ in items}
            skipped = sorted(local)
            items = [(s, q) for s, q in items if s['name'] not in local]
        if not resolves_to_loopback('localhost'):
            # The scenario that writes the host as ``localhost`` needs the name to reach this machine.
            skipped = sorted({*skipped, 'xparamscase', 'xcredcase'})
            items = [(s, q) for s, q in items if s['name'] not in ('xparamscase', 'xcredcase')]
    from demiflow.collect.native_search import results, worker
    from demiflow.collect.native_search.config import SearchConfig
    events = []
    worker.send = events.append
    worker.receive = lambda: {'event': 'http_grant'}
    declared, declare_errors = [], {}
    for source, _ in items:
        try:
            SearchConfig(engines=(source,), language='en').source_configs()
            declared.append(source)
        except Exception as exc:
            declare_errors[source if isinstance(source, str) else source['name']] = type(exc).__name__
    config = {**config, 'sources': SearchConfig(engines=tuple(declared), language='en').source_configs(),
              'worker_directory': str(work)}
    config = json.loads(json.dumps(config))  # the parent sends the configuration to the worker as JSON
    loaded, errors = worker.bootstrap(config)
    if isinstance(destination, ProviderRecords):
        from provider_pages import BRAVE_DATA
        for module in loaded.values():
            if hasattr(module, 'brave_category'):
                module.extract_data = lambda *args, **kwargs: BRAVE_DATA
    by_name = {source['name']: source for source in config['sources']}
    initialized, runs = set(), []
    for source, queries in items:
        name = source if isinstance(source, str) else source['name']
        if name in declare_errors:
            runs.append({'source': name, 'declare_error': declare_errors[name]})
            continue
        for query, parameters, *state in queries:
            state = state[0] if state else {}
            if isinstance(destination, ProviderRecords):
                destination.prepare(**state)
            events.clear()
            destination.take()
            message = {'engine': name, 'query': query,
                       'parameters': {'language': 'en', 'pageno': 1, 'safesearch': 0, 'time_range': None,
                                      'engine_data': {}, **parameters}}
            try:
                result = worker.run_search(message, loaded, errors, initialized, config)
            except BaseException as exc:
                result = {'status': 'RAISED', 'reason': type(exc).__name__, 'results': []}
            merged = None
            if result.get('results'):
                categories = by_name[name].get('categories') or ['general']
                if isinstance(categories, str):
                    categories = [c.strip() for c in categories.split(',')]
                try:
                    merged = worker.merge({
                        'sources': [{'name': name, 'weight': 1.0, 'categories': categories,
                                     'paging': bool((result.get('capabilities') or {}).get('paging'))}],
                        'groups': [{'engine': name, 'results': result['results']}]})
                except BaseException as exc:
                    merged = {'merge_error': type(exc).__name__}
            runs.append({
                'source': state.get('label', name), 'query': query, 'parameters': parameters,
                'status': result.get('status'),
                'reason': result.get('reason'), 'ignored': result.get('ignored_parameters'),
                'capabilities': results.plain(result.get('capabilities')),
                'result_count': len(result.get('results') or []),
                'http': [{k: v for k, v in receipt.items() if k != 'elapsed_s'} for receipt in result.get('http') or []],
                'merged': merged, 'server': destination.take(),
                'events': [[e.get('event'), e.get('host'), e.get('method')] for e in events]})
    text = json.dumps({'suite': suite, 'bootstrap_errors': errors, 'skipped': skipped, 'runs': runs},
                      sort_keys=True, default=str, ensure_ascii=False)
    print(destination.normalize(text), file=sys.__stdout__, flush=True)


if __name__ == '__main__':
    import os
    main(sys.argv[1])
    os._exit(0)  # server threads may outlive the suite
