"""Wikipedia keyword search (MediaWiki REST ``search/page``) in the edition of the request language.

The ``wikipedia`` source returns the summary of one matching article; this
source returns up to eight articles whose text matches the keywords. An
explicit request language is required.
"""
from urllib.parse import quote

from demiflow.collect.native_search.adapter import SourceError
from demiflow.collect.native_search.locales import wikipedia_host

about = {'website': 'https://www.wikipedia.org/', 'wikidata_id': 'Q52',
         'official_api_documentation': 'https://www.mediawiki.org/wiki/API:REST_API/Reference#Search_pages',
         'use_official_api': True, 'require_api_key': False, 'results': 'JSON'}
categories = ['general']
paging = False
# Receipts have always reported no language support for this source although the edition follows the
# request language; kept so its capabilities and ignored_parameters stay comparable with earlier runs.
language_support = False
requires_language = True
page_limit = 8
excerpt_chars = 300
browser_user_agent = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')


def request(query, params):
    if params['locale'] in (None, '', 'all'):
        raise SourceError('wikisearch requires an explicit language')
    host = wikipedia_host(params['locale'], default=None)
    if host is None:
        raise SourceError('Wikipedia does not declare the requested language')
    params['url'] = f'https://{host}/w/rest.php/v1/search/page?q={quote(query)}&limit={page_limit}'
    params['headers'] = {'User-Agent': browser_user_agent, 'Accept': 'application/json'}


def response(resp):
    try:
        answer = resp.json()
    except ValueError:
        raise SourceError('wikisearch answer is not JSON') from None
    site = str(resp.url).split('/w/rest.php')[0]
    found = []
    for page in (answer.get('pages') or [])[:page_limit]:
        key = page.get('key')
        if key:
            found.append({'url': site + '/wiki/' + key, 'title': page.get('title') or key,
                          'content': (page.get('excerpt') or '')[:excerpt_chars]})
    return found
