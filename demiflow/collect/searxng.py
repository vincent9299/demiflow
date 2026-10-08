"""SearXNG wire adapter, including result rows and entity infobox links."""
import json
from html import unescape
import re
from urllib.parse import urljoin

ADAPTER_VERSION = 'searxng-results-5'


def normalize_response(payload, normalize_url):
    if not isinstance(payload,dict): raise ValueError('Response is not an object')
    items = payload.get('results',[])
    boxes = payload.get('infoboxes',[])
    if not isinstance(items,list) or not isinstance(boxes,list): raise ValueError('Invalid result collections')
    found, by_url, malformed = [], {}, 0
    def add(item, kind, parent=None):
        nonlocal malformed
        if not isinstance(item,dict): malformed += 1; return
        url = normalize_url(item.get('url'))
        if not url: malformed += 1; return
        parent = parent or {}
        engines = item.get('engines') or parent.get('engines') or [item.get('engine') or parent.get('engine')]
        engines = [e for e in engines if isinstance(e,str) and e] if isinstance(engines,list) else []
        # Resolve only scheme-relative media URLs against their source page.
        # This preserves the provider's exact path/size, without guessing a
        # larger rendition or turning arbitrary text into a download URL.
        def media_url(value):
            if isinstance(value, str) and value.startswith('//'):
                value = urljoin(url, value)
            return normalize_url(value)
        image_url = media_url(item.get('img_src'))
        # One source page can contain several different image results.
        key = (url, image_url)
        if key in by_url:
            old = by_url[key]
            old['engines'] = list(dict.fromkeys(old['engines']+engines))
            return
        snippet = re.sub('<[^>]*>', '', str(item.get('content') or parent.get('content') or ''))
        value = {'url':url, 'title':str(item.get('title') or parent.get('infobox') or item.get('name') or ''),
                 'snippet':unescape(snippet), 'engines':engines, 'result_kind':kind}
        if image_url is not None or item.get('category') == 'images':
            value.update(img_src=image_url,
                         thumbnail_src=media_url(item.get('thumbnail_src')))
            resolution = item.get('resolution')
            size_bytes = item.get('declared_file_bytes')
            if isinstance(size_bytes, str) and len(size_bytes) <= 19 and size_bytes.isascii() and size_bytes.isdigit():
                size_bytes = int(size_bytes)
            if type(size_bytes) is int and 0 < size_bytes < 2**63:
                value['declared_file_bytes'] = size_bytes
            mime = item.get('mime_type')
            if isinstance(mime, str) and len(mime) <= 255:
                mime = mime.split(';', 1)[0].strip().lower()
                if re.fullmatch(r'[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+', mime):
                    value['mime_type'] = mime
            if isinstance(resolution, str) and len(resolution) <= 128:
                size = re.fullmatch(r'\s*(\d{1,7})\s*[xX×]\s*(\d{1,7})\s*', resolution)
                if size and all(int(n) > 0 for n in size.groups()):
                    value.update(resolution=resolution, declared_width=int(size[1]), declared_height=int(size[2]))
        found.append(value); by_url[key] = value
    for item in items: add(item, 'result')
    for box in boxes:
        if not isinstance(box,dict): malformed += 1; continue
        links = box.get('urls',[])
        if not isinstance(links,list): malformed += 1; continue
        for link in links: add(link, 'infobox_link', box)
    warnings = payload.get('unresponsive_engines') or payload.get('errors') or payload.get('error')
    reasons = []
    if warnings: reasons.append('Provider reported retrieval problems: '+json.dumps(warnings,ensure_ascii=False))
    if malformed: reasons.append(f'{malformed} malformed candidate records ignored')
    status = 'ok' if found else ('invalid_search_response' if malformed else ('search_incomplete' if warnings else 'no_results'))
    return {'status':status,'reason':'; '.join(reasons),'candidates':found}
