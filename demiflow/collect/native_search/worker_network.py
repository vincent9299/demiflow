"""HTTP for search source adapters inside the private worker process.

Every exchange is admitted by the parent session (``http_open`` ->
``http_grant``) and reported back (``http_close``) with a receipt, so host
pacing, global concurrency and attempt records stay with the parent.

Rules:

* one exchange at a time per worker and no hidden retries;
* redirects are followed hop by hop within the session limit; a hop to
  another origin drops credentials, cookies, host headers and the query
  parameters given with the request (the target's own query string is all
  the new origin gets); 303 (and 301/302 after a POST) continue as GET
  without a body. Origin means scheme, host and port: the host is compared
  without regard to case, a default port written out equals one left out, and
  a target that carries credentials of its own is another origin;
* the response body is capped at ``max_bytes`` across all hops;
* userinfo in a URL becomes basic authentication and is removed from the URL;
* only http(s) URLs, plain http only when the source network enables it;
* requests share the remaining deadline of the current source search;
* cookies a server sets are visible on that response only: clients keep no
  cookie store, so one query or source never changes the next one's request.

Transport is curl_cffi. Each source has a network (``Network``): its options,
its rotation over several proxies or local addresses, and its HTTP clients. A
request may choose a client by ``verify``, ``impersonate`` (a browser name;
``none`` disables the browser fingerprint; default ``chrome``),
``curl_options`` and ``enable_http3``; these are client properties and are
never sent as per-request arguments.

Bounds: local addresses are taken from their declaration one request at a
time, so a network of any size (an IPv6 /64) costs nothing to declare; a
network keeps at most ``CLIENT_LIMIT`` HTTP clients for reuse and closes the
least recently used one beyond that. The encoded reply of one search may have
``output_limit()`` bytes: ``worker.within_budget`` stops reading results at
that size, and adapters that read from a store bound what they take by it.
"""
from collections import OrderedDict
import ipaddress
import itertools
import os
import time
from urllib.parse import unquote, urljoin, urlsplit

from .errors import check_status

http_receipts = []
DEFAULT_IMPERSONATE = 'chrome'
NO_IMPERSONATE = 'none'
REDIRECT_CODES = (301, 302, 303, 307, 308)
# Session-wide outgoing option -> network option it sets for every source.
OUTGOING_OPTIONS = {'proxies': 'proxies', 'verify': 'verify', 'enable_http2': 'enable_http2',
                    'source_ips': 'local_addresses', 'max_redirects': 'max_redirects'}
NETWORK_DEFAULTS = {'enable_http': False, 'verify': True, 'enable_http2': True, 'enable_http3': False,
                    'proxies': None, 'local_addresses': None, 'max_redirects': 30}
NETWORK_KEYS = tuple(NETWORK_DEFAULTS)
# Accepted in a source network for older configurations; the parent owns retries, timeouts and pooling.
IGNORED_OPTIONS = ('retries', 'retry_on_http_error', 'request_timeout', 'max_connections', 'using_tor_proxy')
PROXY_SCHEMES = ('http', 'https', 'socks4', 'socks5', 'socks5h')
CLIENT_OPTIONS = ('verify', 'impersonate', 'curl_options', 'enable_http3')
# HTTP clients one network keeps for reuse (one per combination of local address, proxies and client options).
CLIENT_LIMIT = 32
DEFAULT_PORTS = {'http': 80, 'https': 443}
# The encoded reply of one search (results, receipts and the rest) may be this many times ``max_bytes``.
OUTPUT_FACTOR = 8


class ResponseTooLarge(Exception):
    """The response or the results exceeded the session limits."""


class _State:
    config = None
    send = None
    receive = None
    networks = {}
    default = None
    source = None
    deadline = None
    response_class = None


def output_limit():
    """Bytes the encoded reply of one search may have."""
    if _State.config is None:
        raise RuntimeError('Worker budgets are not installed')
    return _State.config['max_bytes'] * OUTPUT_FACTOR


def install(config, send, receive, networks):
    """Bind worker configuration, the parent pipe and the network of every source."""
    _State.config, _State.send, _State.receive = config, send, receive
    _State.networks = networks
    _State.default = None


def begin(source, timeout_s):
    """Start one source search: fresh receipts, its network and its deadline."""
    http_receipts.clear()
    _State.source = source
    _State.deadline = time.monotonic() + timeout_s


def network_options(outgoing, declared=None, attributes=None):
    """Network options of one source.

    Session-wide ``outgoing`` options are the base. ``declared`` is the source's
    ``network`` mapping: it is laid over the base and replaces what the source says
    elsewhere. Without it, ``attributes`` (the adapter module's namespace after the
    source settings were applied) supply the options they define.
    """
    options = dict(NETWORK_DEFAULTS)
    for name, target in OUTGOING_OPTIONS.items():
        if name in outgoing:
            options[target] = outgoing[name]
    if isinstance(declared, dict):
        unknown = set(declared) - set(NETWORK_KEYS) - set(IGNORED_OPTIONS)
        if unknown:
            raise ValueError('Unsupported search network options: ' + ', '.join(sorted(unknown)))
        options.update({k: declared[k] for k in NETWORK_KEYS if k in declared})
    elif declared is not None:
        raise ValueError('Search network must be a source name or a mapping')
    elif attributes is not None:
        options.update({k: attributes[k] for k in NETWORK_KEYS if k in attributes})
    return options


def _origin(parts):
    """Scheme, host and port of a split URL (``urlsplit`` result), or None when it names credentials of its own
    or a port that is no number: such a URL is no origin that anything given for another one may go to."""
    if parts.username is not None or parts.password is not None:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    return parts.scheme, parts.hostname, port or DEFAULT_PORTS.get(parts.scheme)


def same_origin(first, second):
    """True when two split URLs have the same scheme, host (any case) and effective port."""
    origin = _origin(first)
    return origin is not None and origin == _origin(second)


def _local_addresses(declared):
    """Declared local addresses as address and network objects; a value that is neither raises ValueError."""
    items = [declared] if isinstance(declared, str) else list(declared or ())
    return [ipaddress.ip_network(item, False) if '/' in item else ipaddress.ip_address(item) for item in items]


def _address_rotation(items):
    """The local address of each request: declared order, a network's hosts one by one, round after round.

    Hosts are produced when their turn comes and nothing is kept between rounds, so the size of a declared
    network does not matter. Without addresses every step is None.
    """
    networks = (ipaddress.IPv4Network, ipaddress.IPv6Network)
    while True:
        empty = True
        for item in items:
            for address in item.hosts() if isinstance(item, networks) else (item,):
                empty = False
                yield str(address)
        if empty:
            yield None


def _proxy_patterns(declared):
    """``{pattern: [proxy URL, ...]}`` from one URL or a mapping of patterns to a URL or a list of URLs."""
    if not declared:
        return {}
    if isinstance(declared, str):
        return {'all://': [declared]}
    if not isinstance(declared, dict):
        raise ValueError('proxies must be a URL or a mapping')
    patterns = {}
    for pattern, urls in declared.items():
        scheme = pattern[:-1] if pattern.endswith(':') else pattern
        if scheme in PROXY_SCHEMES:
            pattern = scheme + '://'
        patterns[pattern] = [urls] if isinstance(urls, str) else list(urls)
    return patterns


def _proxy_arguments(step, enable_http):
    """curl_cffi proxy arguments for one rotation step; a catch-all pattern wins over scheme patterns."""
    by_scheme, catch_all = {}, None
    for pattern, url in step:
        if pattern.startswith('http://') and not enable_http:
            continue
        if pattern.startswith('https'):
            by_scheme['https'] = url
        elif pattern.startswith('http'):
            by_scheme['http'] = url
        else:
            catch_all = url
    if catch_all:
        return {'proxy': catch_all}
    return {'proxies': by_scheme} if by_scheme else {}


def _response_class():
    if _State.response_class is None:
        from curl_cffi.requests import Response

        class SourceURL(str):
            """Response URL as text, with its ``host`` and ``path``."""

            @property
            def host(self):
                return urlsplit(self).hostname

            @property
            def path(self):
                return urlsplit(self).path

        class SourceResponse(Response):
            _url = ''

            @property
            def url(self):
                return SourceURL(self._url)

            @url.setter
            def url(self, value):
                self._url = str(value or '')

        _State.response_class = SourceResponse
    return _State.response_class


class Network:
    """Transport of one source, or of the sources that share it by name."""

    def __init__(self, options):
        self.options = options
        self._addresses = _address_rotation(_local_addresses(options['local_addresses']))
        patterns = _proxy_patterns(options['proxies'])
        self._proxies = {pattern: itertools.cycle(urls) for pattern, urls in patterns.items()}
        self._clients = OrderedDict()  # least recently used first

    def client(self, verify=None, impersonate=None, curl_options=None, enable_http3=None):
        """The HTTP client for the next request; advances proxy and local-address rotation one step."""
        verify = self.options['verify'] if verify is None else verify
        impersonate = impersonate or DEFAULT_IMPERSONATE
        enable_http3 = self.options['enable_http3'] if enable_http3 is None else enable_http3
        address = next(self._addresses)
        step = tuple((pattern, next(urls)) for pattern, urls in self._proxies.items())
        key = (repr(verify), address, step, impersonate, enable_http3,
               tuple(sorted((int(k), v) for k, v in (curl_options or {}).items())))
        client = self._clients.pop(key, None)
        if client is None:
            client = self._new_client(verify, address, step, impersonate, curl_options, enable_http3)
            while len(self._clients) >= CLIENT_LIMIT:
                self._clients.popitem(last=False)[1].close()
        self._clients[key] = client
        return client

    def _new_client(self, verify, address, step, impersonate, curl_options, enable_http3):
        from curl_cffi import CurlOpt, requests
        from curl_cffi.const import CurlHttpVersion
        low_level = dict(curl_options or {})
        for variable, option in (('SSL_CERT_FILE', CurlOpt.CAINFO), ('SSL_CERT_DIR', CurlOpt.CAPATH)):
            if os.environ.get(variable):
                low_level.setdefault(option, os.environ[variable])
        arguments = {'verify': verify, 'discard_cookies': True, 'response_class': _response_class(),
                     **_proxy_arguments(step, self.options['enable_http'])}
        if impersonate != NO_IMPERSONATE:
            arguments.update(impersonate=impersonate, default_headers=True)
        if address:
            arguments['interface'] = address
        if not self.options['enable_http2']:
            arguments['http_version'] = CurlHttpVersion.V1_1
        elif enable_http3 and not step:
            arguments['http_version'] = CurlHttpVersion.V3
        else:
            arguments['http_version'] = CurlHttpVersion.V2_0
        if low_level:
            arguments['curl_options'] = low_level
        return requests.Session(**arguments)


def _network():
    network = _State.networks.get(_State.source)
    if network is None:
        if _State.default is None:
            _State.default = Network(network_options(_State.config['outgoing']))
        network = _State.default
    return network


def _remaining(requested):
    limit = _State.config['timeout_s']
    timeout = min(float(requested or limit), limit)
    if _State.deadline is not None:
        left = _State.deadline - time.monotonic()
        if left <= 0:
            from curl_cffi.requests.exceptions import Timeout
            raise Timeout('Source search deadline exceeded')
        timeout = min(timeout, left)
    return timeout


def request(method, url, *, raise_for_httperror=True, allow_redirects=True, max_redirects=None, timeout=None,
            **arguments):
    """Admitted HTTP exchange; returns the response with ``history``.

    ``verify``, ``impersonate``, ``curl_options`` and ``enable_http3`` choose the HTTP client; the other
    arguments (``headers``, ``cookies``, ``auth``, ``params``, ``data``, ``json``, ``content`` ...) go to
    curl_cffi with every hop on the same origin (scheme, host and port; see the module docstring). A hop to
    another origin gets neither credentials nor ``params``: only the query string of the redirect target itself.
    """
    config = _State.config
    if config is None:
        raise RuntimeError('Worker HTTP is not installed')
    if config.get('offline_audit'):
        raise RuntimeError('Network disabled during source audit')
    if arguments.pop('stream', False):
        raise ValueError('Raw object streaming belongs to Dataset.fetch')
    from curl_cffi.requests.exceptions import InvalidSchema
    network = _network()
    client = network.client(**{name: arguments.pop(name) for name in CLIENT_OPTIONS if name in arguments})
    limit = min(network.options['max_redirects'] if max_redirects is None else max_redirects,
                config['max_redirects'])
    method = str(method).upper()
    history, total = [], 0
    for hop in range(limit + 1):
        if str(url).startswith('http://') and not network.options['enable_http']:
            raise InvalidSchema('HTTP protocol is disabled')
        parts = urlsplit(str(url))
        if parts.scheme not in ('http', 'https') or not parts.hostname:
            raise ValueError('Invalid adapter HTTP URL')
        if parts.username is not None:
            if not arguments.get('auth'):
                arguments['auth'] = (unquote(parts.username), unquote(parts.password or ''))
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
        try:
            response = client.request(method, url, **{**arguments, 'allow_redirects': False,
                                                      'content_callback': body, 'timeout': hop_timeout})
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
        if not same_origin(parts, nxt):
            arguments.pop('auth', None)
            arguments.pop('cookies', None)
            arguments.pop('params', None)
            arguments['headers'] = {k: v for k, v in (arguments.get('headers') or {}).items()
                                    if k.lower() not in ('authorization', 'proxy-authorization', 'cookie', 'host')}
        if code == 303 or (code in (301, 302) and method == 'POST'):
            method = 'GET'
            for name in ('data', 'json', 'content'):
                arguments.pop(name, None)
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
