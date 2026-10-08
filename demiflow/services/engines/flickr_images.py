"""Flickr public search and explicit all-sizes links, with bounded detail reads.

No API key, image-path rewriting, or assumptions about access to original files.
Commons is the same adapter with commons_only=True. The source page chooses
which renditions are available; business image filters still verify pixels.
"""
import json
import re
from urllib.parse import urlencode, urljoin, urlsplit
from lxml import html
from searx.network import get
from searx.utils import ecma_unescape, html_to_text

about = {'website': 'https://www.flickr.com/', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
categories = ['images']
paging = True
commons_only = False
min_short_side = 0
max_details = 3
max_results = 25
_MODEL = re.compile(r'^\s*modelExport:\s*({.*}),$', re.M)
_SIZE = re.compile(r'(\d{1,7})\s*[x×]\s*(\d{1,7})')


def request(query, params):
    args = {'text': query, 'page': params['pageno']}
    if commons_only:args['is_commons'] = 1
    if min_short_side:args.update(dimension_search_mode='min', width=min_short_side, height=min_short_side)
    params['url'] = 'https://www.flickr.com/search/?' + urlencode(args)
    return params


def size_options(text, page_url):
    doc = html.fromstring(text)
    options = []
    for node in doc.xpath('//ol[contains(@class,"sizes-list")]//li[small]'):
        match = _SIZE.search(' '.join(node.xpath('./small//text()')))
        if not match:continue
        w, h = map(int, match.groups())
        links = node.xpath('./a/@href')
        url = urljoin(page_url, links[0]) if links else page_url
        if urlsplit(url).hostname in {'www.flickr.com', 'flickr.com'}:
            options.append((w * h, w, h, url))
    return sorted(options, reverse=True), doc


def response(resp):
    match = _MODEL.search(resp.text)
    if not match:
        raise ValueError('Flickr response is missing its photo model')
    model = json.loads(match[1]);results = [];details = 0
    if not isinstance(model, dict) or not isinstance(model.get('legend'), list) or 'main' not in model:
        raise ValueError('Flickr response has an invalid photo model')
    for path in model['legend'][:max_results]:
        photo = model['main']
        for k in path:photo = photo[int(k)] if isinstance(photo, list) else photo[k]
        sizes = [x['data'] for x in (photo.get('sizes', {}).get('data') or {}).values()
                 if x.get('data', {}).get('url') and x['data'].get('width') and x['data'].get('height')]
        if not sizes or not photo.get('ownerNsid'):continue
        chosen = max(sizes, key=lambda x: int(x['width']) * int(x['height']))
        page_url = f"https://www.flickr.com/photos/{photo['ownerNsid']}/{photo['id']}/"
        result = {'template': 'images.html', 'url': page_url,
            'img_src': urljoin(page_url, chosen['url']),
            'resolution': f"{chosen['width']} x {chosen['height']}",
            'title': ecma_unescape(photo.get('title', '')),
            'content': html_to_text(ecma_unescape(photo.get('description', ''))),
            'author': ecma_unescape(photo.get('realname', ''))}
        thumb = min(sizes, key=lambda x: abs(int(x['width']) - 320))
        result['thumbnail_src'] = urljoin(page_url, thumb['url'])
        # Search pages usually declare only screen renditions. Inspect a bounded
        # number of public all-sizes pages before rejecting these as too small.
        if details < max_details and min(int(chosen['width']), int(chosen['height'])) < min_short_side:
            details += 1
            size_page = get(page_url + 'sizes/')
            options, doc = size_options(size_page.text, str(size_page.url))
            if options:
                _, w, h, best = options[0]
                if w * h > int(chosen['width']) * int(chosen['height']):
                    if best != str(size_page.url):doc = html.fromstring(get(best).text)
                    original = doc.xpath('//*[@id="allsizes-photo"]/img/@src')
                    if original:
                        # A size link may redirect to a smaller accessible size.
                        # Bind dimensions to the page's current (unlinked) size,
                        # not the larger size originally requested.
                        current = doc.xpath('//ol[contains(@class,"sizes-list")]//li[small and not(a)]/small//text()')
                        actual = _SIZE.search(' '.join(current))
                        result.update(img_src=urljoin(best, original[0]))
                        if actual:result['resolution'] = f'{actual[1]} x {actual[2]}'
                        else:result.pop('resolution', None)
        results.append(result)
    return results
