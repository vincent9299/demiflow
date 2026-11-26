"""Real exchanges of the search worker against loopback servers, compared with the previous runtime.

Each suite runs ``native_search_fixtures/exchange_runner.py`` in its own process. Sources really send
their requests: a probe adapter exercising the HTTP layer (fingerprint on/off, client options, cookies,
redirects, bodies, error statuses, proxies and local addresses in rotation), sources that name the
network they send through (another source, a catalog source that is not declared, ``ipv4``, a
session-wide network; yandex images, brave.images and qwant images name one by default), adapter loading
variants, the SQLite source, and every built-in source and platform image module with only the final
destination replaced by a loopback server that answers with fixed pages; the built-in sources again with
one option changed in the declaration (endpoints, page sizes, switches; then the query template,
time-range tables, language list and User-Agent pool), and every source against answers a provider may
give instead of results (HTML, an empty object, 403, 429, 500).

``exchange_golden.json`` holds what the previous runtime (main 5673d101; main 41f232fd for the
network-names, options, request-options and answers suites) did with the same scenarios: receipt status
and reason, ignored parameters, HTTP receipts, each request as the server received it (including which
server: the destination or a proxy) and a digest of the merged results. Values that are random or bound
to the HTTP library version by design (browser User-Agent strings, Qwant's test group) compare by
family; see ``exchange_runner.essentials``.
"""
import functools
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'native_search_fixtures'
# Adapter variants whose outcome differs from the previous runtime on purpose (docs/web-search.md).
DIFFERENT_BY_DESIGN = {
    'xldicttype': 'only online and offline adapters are supported',
    'xlaboutbad': 'unknown keys in about are accepted',
    'xlnotlist': 'a search that returns text fails under another exception name',
    'xlyessafe': 'language_support declared by a custom adapter is recorded as declared',
    'xparamscross': 'a redirect to another origin does not get the query parameters given with the request',
    'xcredcase': 'credentials go to the same origin in another spelling (the host in capitals)',
}
PROXY_VARIABLES = ('no_proxy', 'http_proxy', 'https_proxy', 'all_proxy')


def essentials(output):
    spec = importlib.util.spec_from_file_location('exchange_runner', FIXTURES / 'exchange_runner.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.essentials(output)


SUITES = ['transport', 'outgoing-proxy', 'outgoing-ips', 'network-names', 'network-names-default',
          'network-names-outgoing', 'sqlite', 'loader', 'named', 'options', 'request-options', 'answers']


@functools.lru_cache(maxsize=None)
def exchanges(suite):
    """Output of one suite run (each suite runs once per test session)."""
    # Loopback requests must reach the fixture proxies, so inherited proxy settings are dropped.
    env = {k: v for k, v in os.environ.items() if k.lower() not in PROXY_VARIABLES}
    env['PYTHONPATH'] = str(ROOT)
    result = subprocess.run([sys.executable, str(FIXTURES / 'exchange_runner.py'), suite], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout)


@pytest.mark.parametrize('suite', SUITES)
def test_exchanges_match_recorded_reference(suite):
    output = exchanges(suite)
    actual = essentials(output)
    golden = json.loads((FIXTURES / 'exchange_golden.json').read_text())[suite]
    source = lambda key: key.split('|')[0]
    expected = {key: facts for key, facts in golden.items() if source(key) not in output['skipped']}
    if not expected:
        pytest.skip('this host has no extra loopback addresses for the local-address scenarios')
    unexpected = {source(key) for key in actual} - {source(key) for key in golden}
    assert unexpected <= set(DIFFERENT_BY_DESIGN), unexpected
    differences = {key: {'expected': expected[key], 'actual': actual.get(key)}
                   for key in expected if actual.get(key) != expected[key]}
    assert not differences, json.dumps(dict(list(differences.items())[:3]), indent=1, ensure_ascii=False)[:4000]


def test_redirect_to_the_same_origin_in_other_spelling_keeps_the_request_parameters():
    """The same host and port with the host name in capitals is the same origin: the parameters go along."""
    run = next((run for run in exchanges('transport')['runs'] if run['source'] == 'xparamscase'), None)
    if run is None:
        pytest.skip('localhost does not reach this machine')
    assert run['status'] == 'ok'
    received = [(record['server'], record['path']) for record in run['server']][1:]  # after the search request
    assert received == [('A', '/case-redirect?p=one'), ('A', '/echo?after=case&p=one')]


def test_redirect_to_the_same_origin_in_other_spelling_keeps_the_credentials():
    """The previous runtime compared the host:port text and dropped credentials when only the case differed."""
    run = next((run for run in exchanges('transport')['runs'] if run['source'] == 'xcredcase'), None)
    if run is None:
        pytest.skip('localhost does not reach this machine')
    assert run['status'] == 'ok'
    first, second = (dict((k.lower(), v) for k, v in record['headers']) for record in run['server'][1:])
    for headers in (first, second):  # the request itself, then the redirect target spelled with capitals
        assert headers.get('authorization') == 'Bearer t' and headers.get('cookie') == 'session=abc'
        assert headers.get('x-keep') == '1'


def test_redirect_to_another_origin_gets_no_request_parameters():
    """Query parameters given with a helper call stay on its origin.

    The previous runtime sent them along to the origin a redirect points to, next to the credentials it
    did drop; here the new origin gets only the query string of the redirect target itself.
    """
    run = next(run for run in exchanges('transport')['runs'] if run['source'] == 'xparamscross')
    assert run['status'] == 'ok'
    received = [(record['server'], record['path']) for record in run['server']][1:]  # after the search request
    assert received == [
        ('A', '/redirect/302/1?p=one'), ('A', '/redirect/302/0?p=one'),
        ('A', '/cross?to=http%3A%2F%2FHOST_B%2Fecho%3Ffrom%3Dcross&p=one'), ('B', '/echo?from=cross'),
        ('A', '/cross?to=http%3A%2F%2FHOST_B%2Fecho%3Ffrom%3Dcross%26kept%3D1&p=one'), ('B', '/echo?from=cross&kept=1'),
    ]
