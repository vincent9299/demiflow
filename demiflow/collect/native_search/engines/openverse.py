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


def request(query, params):
    params['url'] = (f"{base_url}?page={params['pageno']}&page_size={page_size}&format=json&"
                     f"{urlencode({'q': query})}")


def response(resp):
    return [{'url': item['foreign_landing_url'], 'title': item['title'], 'img_src': item['url'],
             'template': 'images.html'} for item in json.loads(resp.text)['results']]
