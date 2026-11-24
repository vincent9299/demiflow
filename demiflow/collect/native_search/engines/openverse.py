"""Openverse openly licensed images (official API, 20 per page)."""
import json
from urllib.parse import urlencode

about = {'website': 'https://openverse.org/', 'wikidata_id': 'Q38117183',
         'official_api_documentation': 'https://api.openverse.org/v1/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 20
base_url = 'https://api.openverse.org/v1/images/'
# Query string of a search; a source declaration may add API filters to it (such as ``license=cc0``).
search_string = '?page={page}&page_size={page_size}&format=json&{query}'


def request(query, params):
    params['url'] = base_url + search_string.format(query=urlencode({'q': query}), page_size=page_size,
                                                    page=params['pageno'])


def response(resp):
    return [{'url': item['foreign_landing_url'], 'title': item['title'], 'img_src': item['url'],
             'template': 'images.html'} for item in json.loads(resp.text)['results']]
