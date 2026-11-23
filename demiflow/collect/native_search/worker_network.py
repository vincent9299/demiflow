"""HTTP for search source adapters inside the private worker process.

Every exchange is admitted by the parent session (``http_open`` ->
``http_grant``) and reported back (``http_close``) with a receipt, so host
pacing, global concurrency and attempt records stay with the parent.

Rules:

* one exchange at a time per worker and no hidden retries;
* redirects are followed hop by hop within the session limit; a cross-origin
  hop drops credentials, cookies and host headers; 303 (and 301/302 after a
  POST) continue as GET without a body;
* the response body is capped at ``max_bytes`` across all hops;
* userinfo in a URL becomes basic authentication and is removed from the URL;
* only http(s) URLs, plain http only when the source network enables it;
* requests share the remaining deadline of the current source search.

Transport is curl_cffi with browser TLS impersonation (``chrome`` unless a
network or request sets ``impersonate``; ``none`` disables it).
"""
import time
from urllib.parse import unquote, urljoin, urlsplit

from .errors import check_status

http_receipts = []
DEFAULT_IMPERSONATE = 'chrome'
REDIRECT_CODES = (301, 302, 303, 307, 308)
NETWORK_DEFAULTS = {'proxies': {}, 'verify': True, 'enable_http': False, 'enable_http2': True,
                    'enable_http3': False, 'local_addresses': None, 'max_redirects': 30,
                    'impersonate': DEFAULT_IMPERSONATE}
NETWORK_KEYS = tuple(NETWORK_DEFAULTS)


class ResponseTooLarge(Exception):
    """The response exceeded the session byte limit."""


class _State:
    config = None
    send = None
    receive = None
    networks = {}
    sessions = {}
    source = None
    deadline = None


def install(config, send, receive, networks):
    """Bind worker configuration, the parent pipe and resolved per-source networks."""
    _State.config, _State.send, _State.receive = config, send, receive
    _State.networks = networks
    _State.sessions = {}


def begin(source, timeout_s):
    """Start one source search: fresh receipts, its network and its deadline."""
    http_receipts.clear()
    _State.source = source
    _State.deadline = time.monotonic() + timeout_s


def network_settings(outgoing, source=None, sources=()):
    """Resolve the network of ``source`` from worker defaults and source overrides.

    ``source['network']`` may name another source (shared network) or hold a
    mapping of network options. Source-level network options (for example
    ``enable_http``) apply as well.
    """
    settings = {**NETWORK_DEFAULTS, **{k: v for k, v in outgoing.items() if k in NETWORK_KEYS}}
    if source is None:
        return settings
    declared = source.get('network')
    if isinstance(declared, str):
        shared = next((s for s in sources if s.get('name') == declared), None)
        if shared is None:
            raise ValueError('Unknown shared search network: ' + declared)
        return network_settings(outgoing, shared, ())
    settings.update({k: source[k] for k in NETWORK_KEYS if k in source})
    if isinstance(declared, dict):
        unknown = set(declared) - set(NETWORK_KEYS) - {'retries', 'retry_on_http_error', 'request_timeout'}
        if unknown:
            raise ValueError('Unsupported search network options: ' + ', '.join(sorted(unknown)))
        settings.update({k: declared[k] for k in NETWORK_KEYS if k in declared})
    elif declared is not None:
        raise ValueError('Search network must be a source name or a mapping')
    return settings


def _proxies(declared):
    if not declared:
        return None
    if isinstance(declared, str):
        return {'http': declared, 'https': declared}
    mapped = {}
    for pattern, url in declared.items():
        if pattern in ('all://', 'all'):
            mapped.setdefault('http', url)
            mapped.setdefault('https', url)
        elif pattern in ('http://', 'http'):
            mapped['http'] = url
        elif pattern in ('https://', 'https'):
            mapped['https'] = url
        else:
            raise ValueError('Unsupported proxy pattern: ' + str(pattern))
    return mapped


def _session(settings):
    from curl_cffi import requests as curl_requests
    from curl_cffi.const import CurlHttpVersion
    proxies = _proxies(settings['proxies'])
    local = settings['local_addresses']
    if isinstance(local, (list, tuple)):
        local = local[0] if local else None
    key = (repr(sorted((proxies or {}).items())), repr(settings['verify']), local,
           settings['enable_http2'], settings['enable_http3'], settings['impersonate'])
    session = _State.sessions.get(key)
    if session is None:
        options = {'verify': settings['verify'], 'proxies': proxies}
        if settings['impersonate'] not in ('', 'none', None):
            options['impersonate'] = settings['impersonate']
        if local:
            options['interface'] = local
        if not settings['enable_http2']:
            options['http_version'] = CurlHttpVersion.V1_1
        elif settings['enable_http3'] and not proxies and hasattr(CurlHttpVersion, 'V3'):
            options['http_version'] = CurlHttpVersion.V3
        session = _State.sessions[key] = curl_requests.Session(**options)
    return session


def _remaining(requested):
    limit = _State.config['timeout_s']
    timeout = min(float(requested or limit), limit)
    if _State.deadline is not None:
        left = _State.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError('Source search deadline exceeded')
        timeout = min(timeout, left)
    return timeout


def request(method, url, *, params=None, headers=None, cookies=None, auth=None, data=None, json=None,
            content=None, allow_redirects=True, max_redirects=None, timeout=None, verify=None,
            impersonate=None, curl_options=None, raise_for_httperror=True, **extra):
    """Admitted HTTP exchange; returns a curl_cffi response with ``history``."""
    config = _State.config
    if config is None:
        raise RuntimeError('Worker HTTP is not installed')
    if config.get('offline_audit'):
        raise RuntimeError('Network disabled during source audit')
    if extra.pop('stream', False):
        raise ValueError('Raw object streaming belongs to Dataset.fetch')
    from curl_cffi.requests.exceptions import InvalidSchema
    settings = _State.networks.get(_State.source) or network_settings(config['outgoing'])
    session = _session(settings)
    limit = min(settings['max_redirects'] if max_redirects is None else max_redirects, config['max_redirects'])
    method = str(method).upper()
    headers = dict(headers or {})
    history, total = [], 0
    for hop in range(limit + 1):
        if str(url).startswith('http://') and not settings['enable_http']:
            raise InvalidSchema('HTTP protocol is disabled')
        parts = urlsplit(str(url))
        if parts.scheme not in ('http', 'https') or not parts.hostname:
            raise ValueError('Invalid adapter HTTP URL')
        if parts.username is not None:
            if not auth:
                auth = (unquote(parts.username), unquote(parts.password or ''))
            parts = parts._replace(netloc=parts.netloc.rsplit('@', 1)[-1])
            url = parts.geturl()
        chunks, hop_bytes, too_large = [], 0, False

        def body(chunk):
            nonlocal total, hop_bytes, too_large
            total += len(chunk)
            hop_bytes += len(chunk)
            if total > config['max_bytes']:
                too_large = True
                return 0
            chunks.append(chunk)
            return len(chunk)

        hop_timeout = _remaining(timeout)
        _State.send({'event': 'http_open', 'host': parts.hostname, 'method': method})
        if _State.receive().get('event') != 'http_grant':
            raise RuntimeError('Invalid admission protocol')
        started = time.monotonic()
        code = None
        options = {'headers': headers, 'cookies': cookies, 'auth': auth, 'allow_redirects': False,
                   'timeout': hop_timeout, 'content_callback': body, **extra}
        for name, value in (('params', params if hop == 0 else None), ('data', data), ('json', json),
                            ('data', content), ('verify', verify), ('impersonate', impersonate),
                            ('curl_options', curl_options)):
            if value is not None:
                options[name] = value
        try:
            response = session.request(method, url, **options)
            code = response.status_code
            if too_large:
                raise ResponseTooLarge()
            response.content = b''.join(chunks)
            response.history = list(history)
            http_receipts.append({'host': parts.hostname, 'method': method, 'http_status': code,
                                  'retry_after': response.headers.get('retry-after'),
                                  'bytes': len(response.content), 'elapsed_s': time.monotonic() - started})
        except BaseException:
            http_receipts.append({'host': parts.hostname, 'method': method, 'http_status': code,
                                  'bytes': hop_bytes, 'elapsed_s': time.monotonic() - started})
            if too_large:
                raise ResponseTooLarge() from None
            raise
        finally:
            _State.send({'event': 'http_close', 'receipt': http_receipts[-1]})
        location = response.headers.get('location')
        if not allow_redirects or code not in REDIRECT_CODES or not location:
            if raise_for_httperror:
                check_status(response)
            return response
        if hop == limit:
            raise ValueError('Adapter redirect limit exceeded')
        target = urljoin(str(url), location)
        nxt = urlsplit(target)
        if (nxt.scheme, nxt.netloc) != (parts.scheme, parts.netloc):
            auth = cookies = None
            headers = {k: v for k, v in headers.items()
                       if k.lower() not in ('authorization', 'proxy-authorization', 'cookie', 'host')}
        if code == 303 or (code in (301, 302) and method == 'POST'):
            method, data, json, content = 'GET', None, None, None
        history.append(response)
        url = target
    raise AssertionError('unreachable')


def get(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    return request('GET', url, **kwargs)


def options(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    return request('OPTIONS', url, **kwargs)


def head(url, **kwargs):
    kwargs.setdefault('allow_redirects', False)
    return request('HEAD', url, **kwargs)


def post(url, data=None, **kwargs):
    return request('POST', url, data=data, **kwargs)


class Request:
    """A deferred request for :func:`multi_requests`."""

    def __init__(self, method, url, **kwargs):
        self.method, self.url, self.kwargs = method, url, kwargs

    @classmethod
    def get(cls, url, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return cls('GET', url, **kwargs)

    @classmethod
    def post(cls, url, **kwargs):
        return cls('POST', url, **kwargs)


def multi_requests(requests):
    """Run requests in order; each item is a response or the exception it raised."""
    replies = []
    for item in requests:
        try:
            replies.append(request(item.method, item.url, **item.kwargs))
        except Exception as exc:
            replies.append(exc)
    return replies
