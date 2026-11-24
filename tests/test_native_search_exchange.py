"""Real exchanges of the search worker against loopback servers, compared with the previous runtime.

Each suite runs ``native_search_fixtures/exchange_runner.py`` in its own process. Sources really send
their requests: a probe adapter exercising the HTTP layer (fingerprint on/off, client options, cookies,
redirects, bodies, error statuses, proxies and local addresses in rotation), adapter loading variants,
the SQLite source, and every built-in source and platform image module with only the final destination
replaced by a loopback server that answers with fixed pages.

``exchange_golden.json`` holds what the previous runtime (main 5673d101) did with the same scenarios:
receipt status and reason, ignored parameters, HTTP receipts, each request as the server received it and
a digest of the merged results. Values that are random or bound to the HTTP library version by design
(browser User-Agent strings, Qwant's test group) compare by family; see ``exchange_runner.essentials``.
"""
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'native_search_fixtures'
# Adapter variants whose outcome differs from the previous runtime on purpose (docs/native-search.md).
DIFFERENT_BY_DESIGN = {
    'xldicttype': 'only online and offline adapters are supported',
    'xlaboutbad': 'unknown keys in about are accepted',
    'xlnotlist': 'a search that returns text fails under another exception name',
    'xlyessafe': 'language_support declared by a custom adapter is recorded as declared',
}
PROXY_VARIABLES = ('no_proxy', 'http_proxy', 'https_proxy', 'all_proxy')


def essentials(output):
    spec = importlib.util.spec_from_file_location('exchange_runner', FIXTURES / 'exchange_runner.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.essentials(output)


@pytest.mark.parametrize('suite', ['transport', 'outgoing-proxy', 'outgoing-ips', 'sqlite', 'loader', 'named'])
def test_exchanges_match_recorded_reference(suite):
    # Loopback requests must reach the fixture proxies, so inherited proxy settings are dropped.
    env = {k: v for k, v in os.environ.items() if k.lower() not in PROXY_VARIABLES}
    env['PYTHONPATH'] = str(ROOT)
    result = subprocess.run([sys.executable, str(FIXTURES / 'exchange_runner.py'), suite], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-3000:]
    output = json.loads(result.stdout)
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
