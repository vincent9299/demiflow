"""Native search against recorded behaviour of the previous (vendored SearXNG) runtime.

The fixtures hold inputs and the outputs the previous runtime produced (main
9bad3137): result merging, offline request building of every built-in source
across languages (including other letter cases such as ``zh-tw``), paging, safe
search and time ranges (helper lookups such as cookie or token pages are stubbed
identically on both sides), and the source capabilities receipts record.
"""
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'native_search_fixtures'


def revive(value):
    if isinstance(value, dict):
        if set(value) == {'$dt'}:
            return datetime.datetime.fromisoformat(value['$dt'])
        return {k: revive(v) for k, v in value.items()}
    if isinstance(value, list):
        return [revive(v) for v in value]
    return value


def test_merge_matches_recorded_reference():
    from demiflow.collect.native_search import results
    golden = json.loads((FIXTURES / 'merge_golden.json').read_text())
    assert len(golden['cases']) >= 20
    for case, expected in zip(golden['cases'], golden['expected']):
        try:
            actual = {'ok': results.merge(case['sources'], revive(case['groups']))}
        except Exception as exc:
            actual = {'error': type(exc).__name__}
        assert json.loads(json.dumps(actual, sort_keys=True)) == expected, case


PROGRAM = r'''
import json, sys
from demiflow.collect.native_search import worker as w
from demiflow.collect.native_search.config import SearchConfig
golden = json.load(open(sys.argv[1]))
names = sorted({c['source'] for c in golden['cases']})
config = SearchConfig(engines=tuple(names), language='en')
settings = {'sources': config.source_configs(), 'outgoing': {}, 'timeout_s': 10, 'max_bytes': 1 << 20,
            'max_results': 100, 'max_redirects': 3, 'offline_audit': True}
loaded, errors = w.bootstrap(settings)
keys = ('method', 'url', 'cookies', 'data', 'json', 'allow_redirects', 'max_redirects', 'raise_for_httperror',
        'impersonate', 'verify', 'auth')
out = []
for case in golden['cases']:
    engine = loaded[case['source']]
    for attribute, value in (case.get('stubs') or {}).get('candidate', {}).items():
        setattr(engine, attribute, (lambda constant: (lambda *a, **k: constant))(value))
    spec = {'language': case.get('language', 'en'), 'pageno': case.get('pageno', 1),
            'safesearch': case.get('safesearch', 0), 'time_range': case.get('time_range'), 'engine_data': {}}
    try:
        params = w.request_params(engine, case['query'], spec)
        if params is not None:
            params = w.online_params(engine, case['query'], params)
        if params is None or not params.get('url'):
            out.append({'declined': True})
            continue
        summary = {k: params.get(k) for k in keys}
        if isinstance(summary.get('data'), bytes):
            summary['data'] = summary['data'].decode('latin-1')
        headers = params.get('headers') or {}
        summary['headers'] = {k: v for k, v in sorted(headers.items()) if k.lower() != 'user-agent'}
        summary['user_agent'] = bool(headers.get('User-Agent') or headers.get('user-agent'))
        out.append(json.loads(json.dumps(summary, default=str)))
    except Exception as exc:
        out.append({'error': type(exc).__name__ + ': ' + str(exc)[:120]})
w.send(out)
'''


def test_request_building_matches_recorded_reference():
    path = FIXTURES / 'request_golden.json'
    golden = json.loads(path.read_text())
    result = subprocess.run([sys.executable, '-c', PROGRAM, str(path)], cwd=ROOT, capture_output=True, text=True,
                            timeout=120)
    assert result.returncode == 0, result.stderr
    actual = json.loads(result.stdout)
    def normalize(value):
        # Error entries compare by message: exception classes were renamed (see docs/web-search.md).
        if isinstance(value, dict) and set(value) == {'error'}:
            return {'error': value['error'].split(': ', 1)[-1]}
        return json.loads(re.sub(r'tgp=\d', 'tgp=N', json.dumps(value, sort_keys=True)))
    sources = {case['source'] for case in golden['cases']}
    assert len(sources) == 21, sorted(sources)
    differences = [(case, expected, normalize(got)) for case, expected, got
                   in zip(golden['cases'], golden['expected'], actual) if normalize(got) != normalize(expected)]
    assert not differences, differences[:3]


def test_language_tags_babel_cannot_parse_never_reach_sources():
    # Locale tables cover babel-parsable tags only; anything else is refused up front.
    import pytest
    from demiflow.collect.native_search.config import SearchConfig, validate_language
    for tag in ('az-TR', 'no-NO', 'en_US', 'xx-YY'):
        with pytest.raises(ValueError):
            validate_language(tag)
    with pytest.raises(ValueError):
        SearchConfig(engines=('google',), language='az-TR')
    assert validate_language('zh-tw') == 'zh-tw'


CAPABILITY_PROGRAM = r'''
import json, sys
from demiflow.collect.native_search import worker as w
from demiflow.collect.native_search.config import SearchConfig
golden = json.load(open(sys.argv[1]))['capabilities']
config = SearchConfig(engines=tuple(golden), language='en')
settings = {'sources': config.source_configs(), 'outgoing': {}, 'timeout_s': 10, 'max_bytes': 1 << 20,
            'max_results': 100, 'max_redirects': 3, 'offline_audit': True}
loaded, errors = w.bootstrap(settings)
w.send({'errors': errors, 'capabilities': {name: {key: getattr(loaded[name], key, '<absent>') for key in fields}
                                           for name, fields in golden.items() if name in loaded}})
'''


def test_source_capabilities_match_recorded_reference():
    # Receipts store these capabilities and derive ignored_parameters from safesearch/language_support.
    path = FIXTURES / 'capabilities_golden.json'
    golden = json.loads(path.read_text())['capabilities']
    assert len(golden) == 21
    result = subprocess.run([sys.executable, '-c', CAPABILITY_PROGRAM, str(path)], cwd=ROOT, capture_output=True,
                            text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    actual = json.loads(result.stdout)
    assert actual['errors'] == {}
    assert actual['capabilities'] == golden
