"""Probe adapter for the exchange tests: ``mode`` selects what one request does and what its response
handler reports. The expected values in ``exchange_golden.json`` were recorded with the same modes on
the previous runtime (main 5673d101)."""
import json
from urllib.parse import urlencode

base_url = ''
other_url = ''
mode = 'plain'
categories = ['general']
paging = True
time_range_support = True
safesearch = True


def helpers():
    from demiflow.collect.native_search import adapter
    return adapter


def view(resp):
    """What an adapter can read from a response."""
    info = {'status': resp.status_code, 'text': resp.text[:300], 'url': str(resp.url),
            'url_host': str(getattr(resp.url, 'host', 'ABSENT')), 'url_path': str(getattr(resp.url, 'path', 'ABSENT')),
            'history': [h.status_code for h in (getattr(resp, 'history', None) or [])],
            'ok': str(getattr(resp, 'ok', 'ABSENT')), 'reason': str(getattr(resp, 'reason', 'ABSENT')),
            'content_type': resp.headers.get('content-type'), 'retry_after': resp.headers.get('retry-after'),
            'has_search_params': hasattr(resp, 'search_params')}
    try:
        info['cookies'] = sorted([str(k), str(v)] for k, v in resp.cookies.items())
    except Exception as exc:
        info['cookies'] = 'ERR ' + type(exc).__name__
    return info


def record(name, value):
    return {'url': 'https://example.org/' + name, 'title': json.dumps(value, sort_keys=True)}


def request(query, params):
    params['url'] = base_url + '/echo?' + urlencode({'q': query, 'm': mode})
    seen = {k: params[k] for k in ('query', 'category', 'pageno', 'safesearch', 'time_range', 'engine_data',
                                    'language', 'method', 'allow_redirects', 'max_redirects', 'raise_for_httperror',
                                    'auth', 'verify', 'soft_max_redirects') if k in params}
    seen['locale'] = params.get('locale', params.get('searxng_locale'))
    seen['headers'] = dict(params.get('headers') or {})
    seen['keys'] = sorted(k for k in params if k not in ('locale', 'searxng_locale'))
    request.seen = seen
    handler = REQUESTS.get(mode)
    if handler:
        handler(query, params)


def response(resp):
    handler = RESPONSES.get(mode)
    if handler:
        return handler(resp)
    return [record(mode, {'response': view(resp), 'params': request.seen})]


def _impersonate(value):
    def apply(query, params):
        params['impersonate'] = value
    return apply


def _curl_options(query, params):
    from curl_cffi import CurlOpt
    params['curl_options'] = {CurlOpt.TCP_NODELAY: 1}


def _cookies(query, params):
    params['cookies'] = {'a': '1', 'b': 'two words'}


def _setcookie(query, params):
    params['url'] = base_url + ('/setcookie' if query == 'first' else '/echo?q=' + query)


def _twocookies(query, params):
    params['url'] = base_url + '/twocookies'


def _headers(query, params):
    params['headers'].update({'X-Probe': 'yes', 'User-Agent': 'Probe/1.0', 'Accept': 'text/plain'})


def _redirect(code, hops, follow=True, limit=None, method='GET', body=None, soft=None):
    def apply(query, params):
        params['url'] = base_url + f'/redirect/{code}/{hops}'
        if follow is not None:
            params['allow_redirects'] = follow
        if limit is not None:
            params['max_redirects'] = limit
        if soft is not None:
            params['soft_max_redirects'] = soft
        params['method'] = method
        if body is not None:
            params['data'] = body
    return apply


def _cross(query, params):
    params['url'] = base_url + '/cross?' + urlencode({'to': other_url + '/echo?from=cross'})
    params['allow_redirects'] = True
    params['auth'] = ('user', 'secret')
    params['cookies'] = {'session': 'abc'}
    params['headers'].update({'Authorization': 'Bearer t', 'X-Keep': '1', 'Proxy-Authorization': 'Basic x'})


def _same(query, params):
    params['url'] = base_url + '/redirect/302/1'
    params['allow_redirects'] = True
    params['auth'] = ('user', 'secret')
    params['cookies'] = {'session': 'abc'}
    params['headers'].update({'Authorization': 'Bearer t', 'X-Keep': '1'})


def _cookie_redirect(query, params):
    params['url'] = base_url + '/cookie-redirect'
    params['allow_redirects'] = True


def _auth(query, params):
    params['auth'] = ('user', 'pass word')


def _userinfo(query, params):
    params['url'] = base_url.replace('http://', 'http://user:p%40ss@') + '/echo?q=userinfo'


def _body(method, **fields):
    def apply(query, params):
        params['method'] = method
        params.update(fields)
    return apply


def _status(code, kind='', raise_error=True):
    def apply(query, params):
        params['url'] = base_url + f'/status/{code}' + ('?kind=' + kind if kind else '')
        params['raise_for_httperror'] = raise_error
    return apply


def _path(path, **fields):
    def apply(query, params):
        params['url'] = base_url + path
        params.update(fields)
    return apply


def _decline(query, params):
    params['url'] = None


def _helper_response(calls):
    """Response handler that performs helper calls and reports each outcome."""
    def handle(resp):
        net = helpers()
        out = []
        for label, call in calls:
            try:
                reply = call(net)
                if isinstance(reply, list):
                    out.append([label, [view(r) if hasattr(r, 'status_code') else 'EXC ' + type(r).__name__
                                        for r in reply]])
                else:
                    out.append([label, view(reply)])
            except Exception as exc:
                out.append([label, 'EXC ' + type(exc).__name__])
        return [record(mode, {'calls': out})]
    return handle


def _requests_list(net):
    return net.multi_requests([net.Request.get(base_url + '/echo?n=1'),
                               net.Request.post(base_url + '/echo?n=2', data={'k': 'v'}),
                               net.Request.get(base_url + '/status/403'),
                               net.Request.get(base_url + '/redirect/302/1')])


REQUESTS = {
    'none': _impersonate('none'), 'empty': _impersonate(''), 'android': _impersonate('chrome99_android'),
    'safari': _impersonate('safari17_0'), 'badimp': _impersonate('no_such_browser'),
    'curlopt': _curl_options, 'cookies': _cookies, 'setcookie': _setcookie, 'twocookies': _twocookies,
    'headers': _headers,
    'follow': _redirect(302, 2), 'nofollow': _redirect(302, 2, follow=None), 'limit1': _redirect(302, 3, limit=1),
    'softmax': _redirect(302, 1, soft=0),
    'post301': _redirect(301, 1, method='POST', body={'k': 'v'}),
    'post302': _redirect(302, 1, method='POST', body={'k': 'v'}),
    'post303': _redirect(303, 1, method='POST', body={'k': 'v'}),
    'post307': _redirect(307, 1, method='POST', body={'k': 'v'}),
    'post308': _redirect(308, 1, method='POST', body={'k': 'v'}),
    'cross': _cross, 'same': _same, 'cookieredirect': _cookie_redirect,
    'auth': _auth, 'userinfo': _userinfo,
    'postdata': _body('POST', data={'a': '1', 'b': 'two words'}), 'poststr': _body('POST', data='raw=body&x=1'),
    'postjson': _body('POST', json={'a': [1, 2], 'b': 'x'}), 'postcontent': _body('POST', content=b'\x00\x01binary'),
    'postempty': _body('POST'), 'put': _body('PUT', data={'a': '1'}), 'getdata': _body('GET', data={'a': '1'}),
    's403': _status(403), 's402': _status(402), 's429': _status(429, 'retry'), 's401': _status(401),
    's404': _status(404), 's500': _status(500), 's503re': _status(503, 'recaptcha'),
    's503cf': _status(503, 'cfchallenge'), 's429cf': _status(429, 'cfchallenge'), 's403cf': _status(403, 'cfcaptcha'),
    's403fw': _status(403, 'cf1020'), 'noraise403': _status(403, raise_error=False),
    'noraise500': _status(500, raise_error=False),
    'big': _path('/big/200000'), 'slow': _path('/slow/4'), 'json': _path('/json'),
    'verifyfalse': _path('/echo?q=verify', verify=False),
    'decline': _decline,
    'helpers': None, 'helpersredirect': None, 'multi': None,
}
RESPONSES = {
    'helpers': _helper_response([
        ('get', lambda net: net.get(base_url + '/echo?h=get')),
        ('get-headers-cookies', lambda net: net.get(base_url + '/echo?h=hc', headers={'X-Helper': '1'},
                                                    cookies={'c': 'd'})),
        ('post', lambda net: net.post(base_url + '/echo?h=post', data={'k': 'v'})),
        ('post-json', lambda net: net.post(base_url + '/echo?h=json', json={'k': [1]})),
        ('head', lambda net: net.head(base_url + '/echo?h=head')),
        ('request-put', lambda net: net.request('PUT', base_url + '/echo?h=put', data='body')),
        ('get-403', lambda net: net.get(base_url + '/status/403')),
        ('get-403-noraise', lambda net: net.get(base_url + '/status/403', raise_for_httperror=False)),
        ('get-timeout', lambda net: net.get(base_url + '/echo?h=timeout', timeout=5)),
        ('get-none', lambda net: net.get(base_url + '/echo?h=none', impersonate='none')),
        ('get-setcookie', lambda net: net.get(base_url + '/setcookie')),
        ('get-after-setcookie', lambda net: net.get(base_url + '/echo?h=after')),
    ]),
    'helpersredirect': _helper_response([
        ('get-follows', lambda net: net.get(base_url + '/redirect/302/1')),
        ('get-nofollow', lambda net: net.get(base_url + '/redirect/302/1', allow_redirects=False)),
        ('head-default', lambda net: net.head(base_url + '/redirect/302/1')),
        ('post-default', lambda net: net.post(base_url + '/redirect/302/1', data={'k': 'v'})),
        ('get-params', lambda net: net.get(base_url + '/redirect/302/1', params={'p': '1'})),
        ('get-limit', lambda net: net.get(base_url + '/redirect/302/3', max_redirects=1)),
    ]),
    'multi': _helper_response([('multi', _requests_list)]),
}
