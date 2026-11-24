"""Deterministic adapter exercising the public adapter API against local HTTP."""
import datetime
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import Request, SourceCaptcha, SourceRateLimited, multi_requests

base_url = ''
engine_type = 'online'
paging = True
time_range_support = True
safesearch = True
language_support = True
categories = ['general']
about = {'results': 'JSON'}


def request(query, params):
    if query == 'multi':
        replies = multi_requests([Request.get(base_url + '?q=helper' + str(i)) for i in range(3)])
        if any(isinstance(r, Exception) for r in replies):
            raise ValueError('helper request failed')
    params['url'] = base_url + '?' + urlencode({'q': query, 'language': params['locale'],
        'page': params['pageno'], 'safe': params['safesearch'], 'time': params['time_range']})
    params['allow_redirects'] = True
    params['max_redirects'] = 3
    params['headers']['X-Source'] = name
    if globals().get('api_key'):
        params['headers']['Authorization'] = 'Bearer ' + api_key


def response(resp):
    data = resp.json()
    if data.get('error') == 'captcha':
        raise SourceCaptcha()
    if data.get('error') == 'rate':
        raise SourceRateLimited()
    if data.get('typed'):
        return [{'url': 'https://example.org/typed', 'title': 'Typed',
                 'publishedDate': datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)}]
    results = data.get('results', [])
    if globals().get('api_key') and results and results[0].get('content') == api_key:
        results.append({'template': 'keyvalue.html',
                        'kvmap': {'status': api_key, 'runtime': api_key, '__native_type__': api_key}})
    return results
