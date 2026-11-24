"""Private pipe worker that runs search source adapters for one native session."""
from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
import sys
import warnings

# stdout is protocol only, including while untrusted third-party code prints.
WIRE = sys.stdout
sys.stdout = sys.stderr
logging.disable(logging.CRITICAL)
warnings.filterwarnings('ignore')

ONLINE_DEFAULTS = {'method': 'GET', 'headers': {}, 'data': {}, 'json': {}, 'content': b'', 'url': '',
                   'cookies': {}, 'allow_redirects': False, 'max_redirects': 0, 'soft_max_redirects': 0,
                   'auth': None, 'verify': None, 'raise_for_httperror': True}
# Older receipts carry result classes of the previously vendored search engine.
LEGACY_RESULT_MODULES = ('searx.result_types',)


def send(value):
    WIRE.write(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')
    WIRE.flush()


def receive():
    line = sys.stdin.readline()
    if not line:
        raise EOFError()
    return json.loads(line)


def encode(value):
    # Preserve dates, sets and nested mappings across private IPC.
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {'__native_type__': 'datetime:' + type(value).__name__, 'value': value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {'__native_type__': 'datetime:timedelta', 'value': value.total_seconds()}
    if isinstance(value, (set, frozenset)):
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
    module, _, cls = name.partition(':')
    if not module.startswith(LEGACY_RESULT_MODULES) or not cls or cls.startswith('_'):
        raise ValueError('Unsupported result class')
    fields = decode(body)
    if not isinstance(fields, dict):
        raise ValueError('Invalid result record')
    fields.pop('parsed_url', None)
    from .results import TYPED
    fields[TYPED] = cls
    return fields


def pack(result):
    return encode(result)


def unpack(result):
    decoded = decode(result)
    if isinstance(decoded, dict):
        decoded.pop('parsed_url', None)
    return decoded


def bootstrap(config):
    """Load every source and its network; a source that cannot be used gets a receipt status in ``errors``."""
    from . import worker_network
    from .sources import SourceLoadError, load
    loaded, errors, networks, shared = {}, {}, {}, {}
    for settings in config['sources']:
        name = settings['name'].lower()
        try:
            # Sources with an explicit backend search through google_search, with their adapter's network.
            module = load(settings, config['timeout_s'])
            declared = getattr(module, 'network', None)
            if isinstance(declared, str):
                shared[name] = declared
            else:
                networks[name] = worker_network.Network(worker_network.network_options(
                    config['outgoing'], declared, vars(module)))
        except SourceLoadError as exc:
            errors[name] = exc.status
        except (ImportError, SystemExit):
            errors[name] = 'dependency_error'
        except Exception:
            errors[name] = 'configuration_error'
        else:
            loaded[name] = module
    for name, other in shared.items():
        # Sources may share the network (options, rotation and clients) of another declared source.
        if other in networks:
            networks[name] = networks[other]
        else:
            errors[name] = 'configuration_error'
            loaded.pop(name, None)
    worker_network.install(config, send, receive, networks)
    return loaded, errors


def accept_language(locale):
    if locale and locale != 'all':
        try:
            import babel
            parsed = babel.Locale.parse(locale, sep='-')
        except Exception:
            parsed = None
        if parsed is not None:
            territory = parsed.territory or parsed.language
            return f'{parsed.language},{parsed.language}-{territory};q=0.7,en;q=0.3'
    return 'en-US,en;q=0.9'


def request_params(engine, query, spec):
    """Search parameters for one source, or None when it cannot serve this request."""
    pageno, time_range = spec['pageno'], spec['time_range']
    if pageno > 1 and not engine.paging:
        return None
    if engine.max_page and engine.max_page < pageno:
        return None
    if time_range and not engine.time_range_support:
        return None
    return {'query': query, 'category': engine.categories[0] if engine.categories else '',
            'pageno': pageno, 'safesearch': spec['safesearch'], 'time_range': time_range,
            'engine_data': (spec.get('engine_data') or {}).get(engine.name, {}),
            'locale': spec['language'], 'language': engine.language or spec['language']}


def online_params(engine, query, params):
    """HTTP request parameters after the adapter's ``request``; ``url`` is empty when it declined."""
    params = {**{k: (v.copy() if isinstance(v, dict) else v) for k, v in ONLINE_DEFAULTS.items()}, **params}
    if engine.send_accept_language_header:
        params['headers']['Accept-Language'] = accept_language(params['locale'])
    engine.request(query, params)
    return params


def online_search(engine, query, params):
    from . import worker_network
    params = online_params(engine, query, params)
    if not params['url']:
        return None
    options = {'headers': params['headers'], 'cookies': params['cookies'], 'auth': params['auth'],
               'allow_redirects': params['allow_redirects'], 'raise_for_httperror': params['raise_for_httperror']}
    for key in ('curl_options', 'impersonate'):
        if params.get(key):
            options[key] = params[key]
    if params.get('verify') is not None:
        options['verify'] = params['verify']
    if params.get('max_redirects'):
        options['max_redirects'] = params['max_redirects']
    # A request is a GET or, for any other declared method, a POST with its body.
    method = 'GET' if params['method'] == 'GET' else 'POST'
    if method == 'POST':
        for key in ('data', 'json', 'content'):
            if params.get(key):
                options[key] = params[key]
    response = worker_network.request(method, params['url'], **options)
    response.search_params = params
    return engine.response(response)


def run_search(message, loaded, errors, initialized, config):
    from . import results as merging, worker_network
    from .errors import classify
    from .sources import capabilities
    name = message['engine']
    if name in errors:
        return {'status': errors[name], 'reason': errors[name], 'results': []}
    spec = message['parameters']
    worker_network.begin(name, config['timeout_s'])
    source = next(c for c in config['sources'] if c['name'].lower() == name)
    stage = 'initialize'
    try:
        if source.get('backend') in ('http', 'browser'):
            from .google_search import run
            value = run(message['query'], spec, source, config, send, receive)
            value['results'] = [pack(r) for r in value['results']]
            return value
        engine = loaded[name]
        if name not in initialized:
            init = getattr(engine, 'init', None)
            if init and init(source) is False:
                raise ValueError('Engine initialization declined')
            initialized.add(name)
        stage = 'search'
        capability = merging.plain(capabilities(engine))
        params = request_params(engine, message['query'], spec)
        if params is None:
            return {'status': 'unsupported_parameters', 'reason': 'Source does not support this query/page/time range',
                    'capabilities': capability, 'results': [], 'http': list(worker_network.http_receipts)}
        if getattr(engine, 'requires_language', False) and spec['language'] == 'all':
            adapter = Path(getattr(engine, '__file__', None) or name).stem
            return {'status': 'unsupported_parameters', 'reason': adapter + ' requires an explicit language', 'results': []}
        if engine.engine_type == 'offline':
            results = engine.search(message['query'], params)
        else:
            results = online_search(engine, message['query'], params)
        if results is None:
            return {'status': 'unsupported_parameters', 'reason': 'Adapter declined the request', 'results': []}
        results = list(results)
        if len(results) > config['max_results']:
            raise worker_network.ResponseTooLarge()
        encoded = [pack(r) for r in results]
        # Validate with the merge rules before persisting a success.
        merging.validate(name, [unpack(r) for r in encoded])
        ignored = []
        if spec['safesearch'] and not engine.safesearch:
            ignored.append('safesearch')
        if spec['language'] != 'all' and not engine.language_support:
            ignored.append('language')
        return {'status': 'ok' if results else 'no_results', 'reason': '', 'results': encoded,
                'ignored_parameters': ignored,
                'capabilities': capability, 'http': list(worker_network.http_receipts)}
    except Exception as exc:
        status = classify(exc)
        if stage == 'initialize' and status == 'parse_error':
            status = 'initialization_error'
        return {'status': status, 'reason': type(exc).__name__, 'results': [],
                **({'backend': source['backend']} if source.get('backend') else {}),
                'http': list(worker_network.http_receipts)}


def merge(message):
    from . import results as merging
    groups = [{'engine': g['engine'], 'results': [unpack(r) for r in g['results']]} for g in message['groups']]
    return merging.merge(message['sources'], groups)


def inspect(loaded, errors):
    from . import results as merging
    keys = ('engine_type', 'paging', 'max_page', 'safesearch', 'language_support', 'time_range_support', 'about')
    return {'sources': {n: {k: merging.plain(getattr(e, k, None)) for k in keys} for n, e in loaded.items()},
            'errors': errors}


def main():
    config = receive()
    loaded, errors = bootstrap(config)
    initialized = set()
    send({'event': 'ready'})
    while True:
        message = receive()
        if message['op'] == 'search':
            result = run_search(message, loaded, errors, initialized, config)
        elif message['op'] == 'merge':
            result = merge(message)
        elif message['op'] == 'inspect':
            result = inspect(loaded, errors)
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
