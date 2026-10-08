"""Private pipe worker. Never imports SearXNG into the pipeline interpreter."""
from __future__ import annotations

import asyncio
import datetime
import hashlib
import hmac
import importlib
import json
import logging
import os
from pathlib import Path
import sys
import time
import types
import warnings

from .config import VENDOR

# stdout is protocol only, including while untrusted third-party code prints.
WIRE = sys.stdout
sys.stdout = sys.stderr
logging.disable(logging.CRITICAL)
warnings.filterwarnings('ignore')


def send(value):
    WIRE.write(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')
    WIRE.flush()


def receive():
    line = sys.stdin.readline()
    if not line:
        raise EOFError()
    return json.loads(line)


def plain(value):
    if hasattr(value, '__struct_fields__'):
        return {k: plain(getattr(value, k)) for k in value.__struct_fields__}
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if hasattr(value, 'as_dict'):
        return plain(value.as_dict())
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, set):
        return sorted((plain(v) for v in value), key=str)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def encode(value):
    # Preserve dates, sets and typed/nested result structs across private IPC.
    import msgspec
    if isinstance(value, msgspec.Struct):
        return {'__native_type__': type(value).__module__ + ':' + type(value).__qualname__,
                'value': {f: encode(getattr(value, f)) for f in value.__struct_fields__}}
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {'__native_type__': 'datetime:' + type(value).__name__, 'value': value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {'__native_type__': 'datetime:timedelta', 'value': value.total_seconds()}
    if isinstance(value, set):
        return {'__native_type__': 'set', 'value': [encode(v) for v in value]}
    if isinstance(value, dict):
        return {'__native_type__': 'mapping', 'value': [[encode(k), encode(v)] for k, v in value.items()]}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    return value


def decode(value):
    if isinstance(value, list):
        return [decode(v) for v in value]
    if not isinstance(value, dict):
        return value
    if '__native_type__' not in value:
        return {k: decode(v) for k, v in value.items()}
    name, body = value['__native_type__'], value['value']
    if name == 'mapping':
        return {decode(k): decode(v) for k, v in body}
    if name == 'set':
        return set(decode(body))
    if name == 'datetime:timedelta':
        return datetime.timedelta(seconds=body)
    if name in ('datetime:date', 'datetime:datetime', 'datetime:time'):
        return getattr(datetime, name.split(':')[1]).fromisoformat(body)
    mod, cls = name.split(':', 1)
    if not mod.startswith('searx.result_types'):
        raise ValueError('Unsupported result class')
    obj = importlib.import_module(mod)
    for part in cls.split('.'):
        if part.startswith('_'):
            raise ValueError('Private result class')
        obj = getattr(obj, part)
    fields = decode(body)
    fields.pop('parsed_url', None)
    return obj(**fields)


def pack(result):
    return encode(result)


def unpack(result):
    decoded = decode(result)
    if isinstance(decoded, dict):
        decoded.pop('parsed_url', None)
    return decoded


class MemoryEngineCache:
    """Worker-private expiring adapter tokens; query receipts live in the parent."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.values = {}
        self.salt = os.urandom(32)

    def get(self, key, default=None, ctx=''):
        until, value = self.values.get((ctx, key), (0, default))
        return value if until > time.monotonic() else default

    def set(self, key, value, expire=None, ctx=''):
        import pickle
        if len(pickle.dumps(value)) > 256 * 1024:
            return False
        self.values[(ctx, key)] = (time.monotonic() + (expire or self.cfg.MAXHOLD_TIME), value)
        if len(self.values) > 256:
            now = time.monotonic()
            self.values = {k: v for k, v in self.values.items() if v[0] > now}
            while len(self.values) > 256:
                self.values.pop(next(iter(self.values)))
        return True

    def secret_hash(self, name):
        return hmac.new(self.salt, name.encode() if isinstance(name, str) else name, hashlib.sha256).hexdigest()


def bootstrap(config):
    # All absolute imports in the vendored source resolve only inside this child.
    sys.path.insert(0, str(VENDOR))
    os.environ.pop('SEARXNG_SETTINGS_PATH', None)
    os.environ.pop('SEARXNG_DEBUG', None)
    import yaml
    import searx
    from searx.settings_defaults import SCHEMA, apply_schema
    settings = yaml.safe_load((VENDOR / 'searx/settings.yml').read_text())
    # Some sources share a named network with another catalog source (e.g.
    # google images -> google). Load that source's transport declaration without
    # selecting it for search or running its initialization.
    defaults = {s['name'].lower(): s for s in settings['engines']}
    source_settings = [{**s, 'name': s['name'].lower()} for s in config['sources']]
    names = {s['name'] for s in source_settings}
    for source in source_settings:
        reference = source.get('network')
        if isinstance(reference, str) and reference not in names and reference in defaults:
            source_settings.append({**defaults[reference], 'name': reference.lower()})
            names.add(reference)
    settings['engines'] = source_settings
    settings['server']['secret_key'] = os.urandom(32).hex()
    settings['outgoing'].update(config['outgoing'])
    settings['outgoing'].update(retries=0, retry_on_http_error=False,
                                request_timeout=config['timeout_s'], pool_connections=1)
    apply_schema(settings, SCHEMA, [])
    searx.settings.update(settings)
    # Adapter caches must not open /etc, ~/.cache or a cross-pipeline token DB.
    import searx.cache
    original_cache = searx.cache.ExpireCacheSQLite.build_cache
    def private_cache(cfg):
        if cfg.name == 'ENGINES_CACHE':
            return MemoryEngineCache(cfg)
        # Shared lookup helpers (e.g. currencies) need the full SQL cache ABI.
        # Their derived indices are worker-private and parent-cleaned on kill.
        directory = config.get('worker_directory')
        if not directory:
            import tempfile
            directory = tempfile.mkdtemp(prefix='demiflow-search-probe-')
            import atexit, shutil
            atexit.register(shutil.rmtree, directory, ignore_errors=True)
        cfg.db_url = str(Path(directory) / (searx.cache.ExpireCache.normalize_name(cfg.name) + '.sqlite'))
        return original_cache(cfg)
    searx.cache.ExpireCacheSQLite.build_cache = staticmethod(private_cache)
    from .worker_network import install
    install(config, send, receive)
    import searx.engines
    errors = {}
    for source in source_settings:
        try:
            obj = searx.engines.load_engine(dict(source))
            if obj is None:
                errors[source['name']] = 'configuration_error'
            else:
                searx.engines.engines[source['name']] = obj
        except (Exception, SystemExit) as exc:
            errors[source['name']] = 'dependency_error' if isinstance(exc, (ImportError, SystemExit)) else 'configuration_error'
    import searx.network
    valid_networks = {'ipv4', 'ipv6'} | set(settings['outgoing']['networks']) | set(searx.engines.engines)
    for name, obj in list(searx.engines.engines.items()):
        reference = getattr(obj, 'network', None)
        if isinstance(reference, str) and reference not in valid_networks:
            errors[name] = 'configuration_error'
            del searx.engines.engines[name]
    searx.network.initialize()
    from searx.metrics import initialize
    initialize(enabled=False)
    # Numeric formatting has a useful locale without a Flask application.
    import flask_babel
    from babel.numbers import format_decimal
    flask_babel.format_decimal = lambda n, **kw: format_decimal(n, locale='en', **kw)
    from searx.search.processors import ProcessorMap
    processors = {}
    for name, engine in searx.engines.engines.items():
        cls = ProcessorMap.processor_types.get(engine.engine_type)
        if cls is None:
            errors[name] = 'unsupported_processor'
        else:
            processors[name] = cls(engine)
    return processors, errors


def classify(exc):
    from searx.exceptions import (SearxEngineCaptchaException, SearxEngineTooManyRequestsException,
                                 SearxEngineAccessDeniedException)
    from curl_cffi.requests.exceptions import Timeout, RequestException, HTTPError
    if isinstance(exc, SearxEngineCaptchaException):
        return 'captcha'
    if isinstance(exc, SearxEngineTooManyRequestsException):
        return 'rate_limited'
    if isinstance(exc, SearxEngineAccessDeniedException):
        return 'access_denied'
    if isinstance(exc, (Timeout, TimeoutError)):
        return 'timeout'
    if isinstance(exc, HTTPError):
        code = getattr(getattr(exc, 'response', None), 'status_code', None)
        return 'authentication_error' if code == 401 else 'http_error'
    if isinstance(exc, RequestException):
        return 'network_error'
    if isinstance(exc, ImportError):
        return 'dependency_error'
    if type(exc).__name__ == 'ResponseTooLarge':
        return 'response_too_large'
    return 'parse_error'


class ResponseTooLarge(Exception):
    pass


def run_search(message, processors, errors, initialized, config):
    name = message['engine']
    if name in errors:
        return {'status': errors[name], 'reason': errors[name], 'results': []}
    proc = processors[name]
    engine = proc.engine
    params_spec = message['parameters']
    from searx.search.models import SearchQuery, EngineRef
    from searx.network import set_timeout_for_thread, set_context_network_name, reset_time_for_thread
    from . import worker_network
    worker_network.http_receipts.clear()
    started = time.monotonic()
    set_timeout_for_thread(config['timeout_s'], start_time=started)
    set_context_network_name(name)
    reset_time_for_thread()
    stage = 'initialize'
    try:
        source = next(c for c in config['sources'] if c['name'].lower() == name)
        if source.get('backend') in ('http', 'browser'):
            from .google_search import run
            value = run(message['query'], params_spec, source, config, send, receive)
            value['results'] = [pack(r) for r in value['results']]
            return value
        if name not in initialized:
            init = getattr(engine, 'init', None)
            if init and init(next(c for c in config['sources'] if c['name'].lower() == name)) is False:
                raise ValueError('Engine initialization declined')
            initialized.add(name)
        stage = 'search'
        sq = SearchQuery(message['query'], [EngineRef(name, engine.categories[0])],
                         lang=params_spec['language'], pageno=params_spec['pageno'],
                         safesearch=params_spec['safesearch'], time_range=params_spec['time_range'],
                         engine_data=params_spec.get('engine_data', {}))
        params = proc.get_params(sq, engine.categories[0])
        capability = {k: plain(getattr(engine, k, None)) for k in
                      ('paging', 'max_page', 'time_range_support', 'safesearch', 'language_support', 'language', 'engine_type')}
        if params is None:
            return {'status': 'unsupported_parameters', 'reason': 'Source does not support this query/page/time range',
                    'capabilities': capability, 'results': [], 'http': list(worker_network.http_receipts)}
        if Path(getattr(engine, '__file__', '')).stem == 'wikisearch' and params_spec['language'] == 'all':
            return {'status': 'unsupported_parameters', 'reason': 'wikisearch requires an explicit language', 'results': []}
        if engine.engine_type == 'offline':
            results = engine.search(message['query'], params)
        else:
            results = proc._search_basic(params['query'], params)
        if results is None:
            return {'status': 'unsupported_parameters', 'reason': 'Adapter declined the request', 'results': []}
        results = list(results)
        if len(results) > config['max_results']:
            raise ResponseTooLarge()
        # Validate using the real result types before persisting a success.
        from searx.results import ResultContainer
        check = ResultContainer()
        encoded = [pack(r) for r in results]
        check.extend(name, [unpack(r) for r in encoded])
        check.get_ordered_results()
        ignored = []
        if params_spec['safesearch'] and not engine.safesearch:
            ignored.append('safesearch')
        if params_spec['language'] != 'all' and not engine.language_support:
            ignored.append('language')
        return {'status': 'ok' if results else 'no_results', 'reason': '', 'results': encoded,
                'ignored_parameters': ignored,
                'capabilities': capability, 'http': list(worker_network.http_receipts)}
    except Exception as exc:
        status = classify(exc)
        if stage == 'initialize' and status == 'parse_error':
            status = 'initialization_error'
        return {'status': status, 'reason': type(exc).__name__, 'results': [],
                **({'backend':source['backend']} if source.get('backend') else {}),
                'http': list(worker_network.http_receipts)}


def merge(message):
    from searx.results import ResultContainer
    import searx.engines
    rc = ResultContainer()
    for source in message['sources']:
        # Result scoring needs metadata, not network initialization or live engines.
        name = source['name']
        searx.engines.engines[name] = types.SimpleNamespace(name=name, weight=source.get('weight', 1.0),
            categories=source.get('categories', ['general']), paging=source.get('paging', False))
    for group in message['groups']:
        rc.extend(group['engine'], [unpack(r) for r in group['results']])
    return {'results': plain(rc.get_ordered_results()), 'infoboxes': plain(rc.infoboxes),
            'answers': plain(list(rc.answers)), 'suggestions': sorted(rc.suggestions),
            'corrections': sorted(rc.corrections), 'engine_data': plain(rc.engine_data), 'paging': rc.paging}


def main():
    config = receive()
    processors, errors = bootstrap(config)
    initialized = set()
    send({'event': 'ready'})
    while True:
        message = receive()
        if message['op'] == 'search':
            result = run_search(message, processors, errors, initialized, config)
        elif message['op'] == 'merge':
            # Restore metadata after merge, since the worker is reusable for search.
            import searx.engines
            original = dict(searx.engines.engines)
            try:
                result = merge(message)
            finally:
                searx.engines.engines.clear()
                searx.engines.engines.update(original)
        elif message['op'] == 'inspect':
            result = {'sources': {n: {k: plain(getattr(p.engine, k, None)) for k in
                       ('engine_type', 'paging', 'max_page', 'safesearch', 'language_support', 'time_range_support', 'about')}
                       for n, p in processors.items()}, 'errors': errors}
        else:
            raise ValueError('Unknown worker operation')
        encoded = json.dumps(result, ensure_ascii=False)
        if len(encoded.encode()) > config['max_bytes'] * 8:
            result = {'status': 'response_too_large', 'reason': 'Worker output limit', 'results': []}
        send({'event': 'result', 'value': result})


if __name__ == '__main__':
    try:
        main()
    except EOFError:
        pass
    except BaseException as exc:
        # Exception strings and adapter logs may contain authentication URLs.
        try:
            send({'event': 'fatal', 'reason': type(exc).__name__})
        except (BrokenPipeError, OSError):
            pass
    finally:
        # Parent pipe closure must also stop a browser started by this worker.
        google = sys.modules.get('demiflow.collect.native_search.google_search')
        browser = getattr(google, '_browser', None)
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
