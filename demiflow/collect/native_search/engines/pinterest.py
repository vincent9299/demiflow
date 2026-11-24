"""Pinterest pin search (``BaseSearchResource``); the next-page bookmark travels in ``engine_data``."""
from json import dumps

about = {'website': 'https://www.pinterest.com/', 'wikidata_id': 'Q255381', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
base_url = 'https://www.pinterest.com'


def request(query, params):
    args = {'options': {'query': query, 'bookmarks': [params['engine_data'].get('bookmark', '')]}, 'context': {}}
    params['url'] = f'{base_url}/resource/BaseSearchResource/get/?data={dumps(args)}'
    params['headers'] = {'X-Pinterest-AppState': 'active', 'X-Pinterest-Source-Url': '/ideas/',
                         'X-Pinterest-PWS-Handler': 'www/ideas.js'}


def response(resp):
    payload = resp.json()['resource_response']
    found = [{'engine_data': payload['bookmark'], 'key': 'bookmark'}]
    for pin in payload['data']['results']:
        if pin['type'] == 'story':
            continue
        original, summary = pin['images']['orig'], pin.get('rich_summary') or {}
        found.append({'template': 'images.html', 'url': pin.get('link') or f"{base_url}/pin/{pin['id']}/",
                      'title': pin.get('title') or pin.get('grid_title'),
                      'content': summary.get('display_description') or '', 'img_src': original['url'],
                      'thumbnail_src': pin['images']['236x']['url'], 'source': summary.get('site_name'),
                      'resolution': f"{original['width']}x{original['height']}",
                      'author': f"{pin['pinner'].get('full_name')} ({pin['pinner']['username']})"})
    return found
