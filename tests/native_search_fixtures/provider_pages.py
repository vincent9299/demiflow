"""Fixed provider pages for the exchange tests: synthetic answers in each provider's response shape
(no provider content), the loopback routes that serve them and the helper pages of sources that
look up a token, cookie, key or proxy address first."""

PAGES = {
    'google': (
        '<html><body><div class="zMzFAb"><a class="fuLhoc" '
        'href="/url?q=https://example.org/oak%3Fa%3D1&sa=U&ved=x"><span class="CVA68e">Oak  '
        'tree</span></a><div class="taTFJ"><span class="FrIlee">An oak\ntree.</span></div><img '
        'src="https://encrypted-tbn0.gstatic.com/images?q=1"></div><div class="zMzFAb"><a class="fuLhoc" '
        'href="https://example.org/pine"><span class="CVA68e">Pine</span></a></div><div '
        'class="zMzFAb"><span>no link</span></div><table class="HExoMb"><tr><td><a class="ZWRArf">oak species'
        '</a></td></tr></table>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxx</body></html>'
    ),
    'bing': (
        '<html><body><ol id="b_results"><li class="b_algo"><h2><a '
        'href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9leGFtcGxlLm9yZy9iaW5nLXBhZ2U_eD0x&ntb=1">Oak '
        '<b>tree</b></a></h2><p><span class="algoSlug_icon">icon</span>About oaks.</p></li><li '
        'class="b_algo"><h2><a href="https://example.org/pine">Pine</a></h2><p>Pines.</p><p>More.</p></li><li'
        ' class="b_algo"><h2><a href="">Empty</a></h2></li><li class="b_algo"><p>no '
        'title</p></li></ol></body></html>'
    ),
    'yandex': (
        '<html><body><li class="serp-item"><h3 class="b-serp-item__title"><a class="b-serp-item__title-link" '
        'href="https://example.org/y1"><span>Oak</span></a></h3><div class="b-serp-item__content"><div '
        'class="b-serp-item__text">About oak.</div></div></li></body></html>'
    ),
    'mwmbl': (
        '[{"url": "https://example.org/m1", "title": [{"value": "Oak "}, {"value": "tree"}], "extract": '
        '[{"value": "A"}]}, {"url": "https://example.org/m2", "title": [{"value": "Pine"}]}]'
    ),
    'wikipedia': (
        '{"type": "standard", "title": "Oak", "titles": {"display": "<span>Oak</span>"}, "extract": "A '
        'tree.", "description": "tree genus", "thumbnail": {"source": "https://upload.example/oak.jpg"}, '
        '"content_urls": {"desktop": {"page": "https://en.wikipedia.org/wiki/Oak"}}}'
    ),
    'wikisearch': (
        '{"pages": [{"key": "Oak", "title": "Oak", "excerpt": "<span class=\\"searchmatch\\">Oak</span> xxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}, '
        '{"key": null, "title": "skip"}, {"key": "Quercus", "title": null, "excerpt": null}]}'
    ),
    'google images': (
        '<html><body><a href="/imgres?imgurl=https://i.example/oak%20tree.jpg&imgrefurl=https://example.org/o'
        'ak&w=640&h=480&tbnid=T1">x</a><a '
        'href="/imgres?imgurl=https://i.example/&imgrefurl=https://example.org/root&tbnid=T2">y</a><a href="/'
        'imgres?imgurl=&imgrefurl=https://example.org/none">z</a>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</body></html>'
    ),
    'bing images': (
        '<html><body><ul class="dgControl_list"><li><a class="iusc" m=\'{"purl": "https://example.org/p1", '
        '"turl": "https://t.example/1.jpg", "murl": "https://i.example/1.jpg", "desc": "Oak '
        'picture"}\'>x</a><div class="infnmpt"><a>Oak</a> <a>tree</a></div><div class="imgpt"><div><span>1024 '
        'x 768 · jpeg</span></div><div class="lnkw"><a>example.org</a></div></div></li><li><a class="iusc" '
        'm=\'{"purl": "https://example.org/p2", "turl": "https://t.example/2.jpg", "murl": '
        '"https://i.example/2.jpg"}\'>y</a><div class="imgpt"><div><span>800 x '
        '600</span></div></div></li><li>no metadata</li></ul></body></html>'
    ),
    'yandex images': (
        '<html><body><div data-state=\'{"location": "/images/search/", "initialState": {"serpList": {"items": '
        '{"entities": {"a": {"snippet": {"title": "Oak", "url": "https://example.org/oak"}, "image": '
        '"https://t.example/a.jpg", "viewerData": {"thumb": {"url": "https://t.example/a.jpg", "w": 100, "h":'
        ' 80}, "dups": [{"url": "https://i.example/a-big.jpg", "w": 1600, "h": 1200, "fileSizeInBytes": '
        '2048000}], "preview": [{"url": "https://i.example/a-mid.jpg", "w": 800, "h": 600}]}}, "b": '
        '{"snippet": {"title": "Pine", "url": "https://example.org/pine"}, "image": '
        '"https://t.example/b.jpg", "viewerData": {"thumb": {"url": "https://t.example/b.jpg", "w": 120, "h":'
        ' 90}, "dups": [], "preview": []}}}}}}, "advRsyaSearchColumn": null}\'></div></body></html>'
    ),
    'baidu images': (
        '{"data":[{"replaceUrl":[{"FromURL":"https:\\/\\/example.org\\/b1","ObjURL":"https:\\/\\/i.example\\/b1.jpg'
        '"}],"thumbURL":"https:\\/\\/t.example\\/b1.jpg","fromPageTitle":"<strong>Oak<\\/strong> tree","fromURLHo'
        'st":"example.org","width":640,"height":480,"type":"jpg","filesize":"","bdImgnewsDate":"2026-01-02 '
        '03:04:05"},{}]}'
    ),
    'brave.images': (
        '<html><body>data</body></html>'
    ),
    'sogou images': (
        '<html><body><script>window.__INITIAL_STATE__ = {"searchList": {"searchList": [{"url": '
        '"https://example.org/s1", "picUrl": "https://i.example/s1.jpg", "content_major": "Oak", "title": '
        '"Oak tree", "ch_site_name": "Example"}]}};</script></body></html>'
    ),
    'quark images': (
        '{"data": {"hit": {"imgInfo": {"item": [{"imgUrl": "https://example.org/q1", "img": '
        '"https://t.example/q1.jpg", "bigPicUrl": "https://i.example/q1.jpg", "title": "Oak", "site": '
        '"example.org", "width": 640, "height": 480, "publish_time": "1767225600"}, {"imgUrl": '
        '"https://example.org/q2", "width": 1, "height": 2, "publish_time": "bad"}]}}}}'
    ),
    'duckduckgo images': (
        '{"results": [{"url": "https://example.org/d1", "title": "Oak", "thumbnail": '
        '"https://t.example/d1.jpg", "image": "https://i.example/d1.jpg", "width": 640, "height": 480, '
        '"source": "Bing"}]}'
    ),
    'qwant images': (
        '{"status": "success", "data": {"result": {"items": [{"title": "Oak", "url": '
        '"https://example.org/w1", "thumbnail": "https://t.example/w1.jpg", "media": '
        '"https://i.example/w1.jpg", "width": 640, "height": 480, "thumb_type": "jpeg"}, {"title": "Pine", '
        '"url": "https://example.org/w2", "thumbnail": null, "media": null, "width": 1, "height": 1}]}}}'
    ),
    'pinterest': (
        '{"resource_response": {"bookmark": "NEXT", "data": {"results": [{"type": "pin", "id": "42", "link": '
        'null, "title": "", "grid_title": "Oak", "rich_summary": null, "images": {"orig": {"url": '
        '"https://i.example/p.jpg", "width": 640, "height": 480}, "236x": {"url": '
        '"https://t.example/p.jpg"}}, "pinner": {"full_name": "Ann", "username": "ann"}}, {"type": "story", '
        '"id": "43"}]}}}'
    ),
    'wikicommons.images': (
        '{"query": {"pages": {"1": {"title": "File:Oak tree.jpg", "snippet": "An <b>oak</b>", "imageinfo": '
        '[{"descriptionurl": "https://commons.wikimedia.org/wiki/File:Oak_tree.jpg", "url": '
        '"https://upload.example/oak.jpg", "thumburl": "https://upload.example/oak-180.jpg", "width": 4000, '
        '"height": 3000, "mime": "image/jpeg", "size": 2500000, "extmetadata": {"LicenseShortName": {"value":'
        ' "CC BY-SA 4.0"}, "Artist": {"value": "<a href=\\"x\\">Ann</a>"}}}]}, "2": {"title": "File:Pine.png", '
        '"snippet": "", "imageinfo": [{"descriptionurl": "https://commons.wikimedia.org/wiki/File:Pine.png", '
        '"url": "https://upload.example/pine.png", "thumburl": "https://upload.example/pine-180.png", '
        '"width": 10, "height": 10, "mime": "image/png"}]}, "3": {"title": "File:None.jpg", "snippet": ""}}}}'
    ),
    'public domain image archive': (
        '{"results": [{"hits": [{"objectID": "abc", "thumbnail": "https://img.example/x.jpg?ixid=1&s=2&w=10",'
        ' "title": " Oak ", "artist": "Ann", "displayYear": "1850", "themes": "Trees", "encompassingWork": '
        '"Book"}]}]}'
    ),
    'pexels': (
        '{"data": [{"attributes": {"slug": "oak", "id": 7, "title": "Oak", "description": "Tree", "image": '
        '{"small": "https://t.example/7.jpg", "download_link": "https://i.example/7.jpg"}, "width": 640, '
        '"height": 480, "user": {"username": "ann"}}}]}'
    ),
    'openverse': (
        '{"results": [{"foreign_landing_url": "https://example.org/o1", "title": "Oak", "url": '
        '"https://i.example/o1.jpg"}]}'
    ),
    'unsplash': (
        '{"results": [{"links": {"html": "https://unsplash.com/photos/x?ixid=AA&utm=1"}, "urls": {"thumb": '
        '"https://images.example/x?ixid=AA&w=200", "regular": "https://images.example/x?ixid=AA&w=1080"}, '
        '"alt_description": null, "description": null}]}'
    ),
}

# What the Brave page-data extractor returns for the fixed Brave page (its input format is not reproduced).
BRAVE_DATA = {'data': [{},
          {'data': {'body': {'response': {'results': [{'properties': {'url': 'https://i.example/b1.jpg'},
                                                       'source': 'example.org',
                                                       'thumbnail': {'src': 'https://t.example/b1.jpg'},
                                                       'title': 'B1',
                                                       'url': 'https://example.org/b1'}]}}}}]}

JSON = [('Content-Type', 'application/json')]


def page(name):
    return 200, [], PAGES[name].encode()


# (host, path prefix) -> (status, headers, body); the longest matching prefix wins.
ROUTES = {
    ('duckduckgo.com', '/i.js'): page('duckduckgo images'),
    ('duckduckgo.com', '/'): (200, [], b'<html><script>vqd="4-1234567890";</script></html>'),
    ('image.baidu.com', '/search/acjson'): page('baidu images'),
    ('image.baidu.com', '/'): (200, [('Set-Cookie', 'BAIDUID=FIXTURE; Path=/')], b'<html>baidu</html>'),
    ('www.pexels.com', '/en-us/api/'): page('pexels'),
    ('www.pexels.com', '/assets/app.js'): (200, [], b'window.x={"secret-key":"KEY-FROM-SCRIPT"};'),
    ('www.pexels.com', '/'): (200, [], b'<html><head><script src="https://www.pexels.com/assets/app.js"></script>'
                                     b'</head><body>home</body></html>'),
    ('pdimagearchive.org', '/_astro/InfiniteSearch.'): (200, [], b'const u="https://proxy.example/search-proxy";'),
    ('pdimagearchive.org', '/search/'): (200, [], b'<script src="/_astro/InfiniteSearch.abc.js"></script>'),
    ('proxy.example', '/search-proxy'): page('public domain image archive'),
    ('api.qwant.com', '/'): (200, JSON + [('Set-Cookie', 'datadome=FIXTURE-DOME; Path=/')],
                             PAGES['qwant images'].encode()),
    ('www.google.com', '/'): page('google'),
    ('www.bing.com', '/images/'): page('bing images'),
    ('www.bing.com', '/'): page('bing'),
    ('yandex.com', '/images/'): page('yandex images'),
    ('yandex.com', '/'): page('yandex'),
    ('pic.sogou.com', '/'): page('sogou images'),
    ('vt.sm.cn', '/'): page('quark images'),
    ('www.pinterest.com', '/'): page('pinterest'),
    ('commons.wikimedia.org', '/'): page('wikicommons.images'),
    ('api.openverse.org', '/'): page('openverse'),
    ('unsplash.com', '/'): page('unsplash'),
    ('search.brave.com', '/'): page('brave.images'),
    ('api.mwmbl.org', '/'): page('mwmbl'),
}
WIKIPEDIA = {'/api/rest_v1/page/summary/': page('wikipedia'), '/w/rest.php/v1/search/page': page('wikisearch')}


def answer(host, path):
    """The fixed answer for a request to ``host`` and ``path``; unknown destinations get an empty JSON object."""
    if host.endswith('.wikipedia.org'):
        for prefix, reply in WIKIPEDIA.items():
            if path.startswith(prefix):
                return reply
    best = None
    for (route_host, prefix), reply in ROUTES.items():
        if route_host == host and path.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, reply)
    return best[1] if best else (200, JSON, b'{}')
