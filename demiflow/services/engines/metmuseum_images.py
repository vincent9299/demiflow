"""Bounded Met Collection API search; original URLs come from object records."""
from urllib.parse import urlencode
from searx.network import get
from demiflow.services.engines.image_source_utils import result_list

about = {'website': 'https://www.metmuseum.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON',
         'official_api_documentation': 'https://metmuseum.github.io/'}
categories = ['images']
paging = True
max_objects = 4
base_url = 'https://collectionapi.metmuseum.org/public/collection/v1'


def request(query, params):
    params['url'] = base_url + '/search?' + urlencode({'q': query, 'hasImages': 'true'})
    return params


def response(resp):
    body = resp.json()
    # The Met explicitly returns null IDs for a valid zero-hit response.
    if isinstance(body, dict) and 'objectIDs' in body and body['objectIDs'] is None and body.get('total') == 0:
        return []
    ids = result_list(body, 'objectIDs')
    page = resp.search_params.get('pageno', 1)
    results = []
    for oid in ids[(page - 1) * max_objects:page * max_objects]:
        if type(oid) is not int or oid <= 0:
            continue
        item = get(base_url + '/objects/' + str(oid)).json()
        if not item.get('isPublicDomain') or not item.get('primaryImage'):
            continue
        for url in [item['primaryImage'], *(item.get('additionalImages') or [])[:2]]:
            results.append({'template': 'images.html', 'url': item.get('objectURL') or
                'https://www.metmuseum.org/art/collection/search/' + str(oid),
                'img_src': url, 'thumbnail_src': item.get('primaryImageSmall'),
                'title': item.get('title', ''), 'content': item.get('objectName', ''),
                'author': item.get('artistDisplayName', ''), 'license': 'CC0'})
    return results
