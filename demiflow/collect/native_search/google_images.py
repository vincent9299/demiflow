"""Parse explicit image/page bindings from HTTP or rendered Google image DOM.

No image bytes are fetched here. Unknown markup is a parse error, never an
empty success; a thumbnail alone is not promoted to an original-image URL.
"""
from urllib.parse import parse_qs, urlsplit, urljoin, unquote

PARSER_VERSION='google-images-dom-1'
READY="""() => Boolean(document.querySelector('a[href*="/imgres?"], [data-ou][data-ru], [data-iurl][data-lpage]') ||
    document.querySelector('#captcha-form, #recaptcha') ||
    location.pathname.startsWith('/sorry') || location.hostname.startsWith('consent.') ||
    document.querySelector('#topstuff')?.innerText.match(/did not match any|找不到和|没有找到|未找到与/))"""


def _url(value):
    if not isinstance(value,str) or len(value)>16384:return None
    try:parsed=urlsplit(value)
    except ValueError:return None
    return value if parsed.scheme in {'http','https'} and parsed.hostname and not parsed.username else None


def parse_image_page(content,url,code,max_results):
    from lxml import html
    from .google_search import parse_page
    status,reason,_=parse_page(content,url,code,1)
    if status in {'captcha','consent_required','rate_limited','authentication_error','access_denied','http_error','no_results'}:
        return status,reason,[]
    try:tree=html.fromstring(content)
    except (ValueError,html.etree.ParserError):return 'parse_error','Invalid Google image HTML',[]
    results=[];seen=set()
    # Iterate the bounded DOM instead of allocating a list of every card.
    for node in tree.iter():
        original=page=thumbnail=None
        if node.tag=='a':
            try:link=urlsplit(urljoin(url,node.get('href','')))
            except ValueError:continue
            if link.path!='/imgres' or link.hostname!=urlsplit(url).hostname:continue
            try:qs=parse_qs(link.query,max_num_fields=64)
            except ValueError:continue
            original=(qs.get('imgurl') or [None])[0]
            page=(qs.get('imgrefurl') or [None])[0]
            thumbnail=(qs.get('tbnurl') or [None])[0]
        elif node.get('data-ou') and node.get('data-ru'):
            original,page=node.get('data-ou'),node.get('data-ru')
        elif node.get('data-iurl') and node.get('data-lpage'):
            original,page=node.get('data-iurl'),node.get('data-lpage')
        original,page=_url(original),_url(page)
        if not original or not page or (page,original) in seen:continue
        image_host=urlsplit(original).hostname or ''
        if image_host.startswith('encrypted-tbn') and image_host.endswith(('.gstatic.com','.google.com')):
            continue
        image=next(node.iter('img'),None)
        title=node.get('aria-label') or (image.get('alt') if image is not None else None)
        if image is not None:
            thumbnail=thumbnail or image.get('data-src') or image.get('src')
        if original==thumbnail:continue
        title=title or unquote(urlsplit(original).path.rsplit('/',1)[-1]) or urlsplit(page).hostname
        results.append({'url':page,'img_src':original,'thumbnail_src':_url(thumbnail),
            'title':' '.join(title.split())[:1024],'content':'','category':'images','template':'images.html'})
        seen.add((page,original))
        if len(results)>=max_results:break
    if results:return 'ok','',results
    if status=='render_required':return status,reason,[]
    return 'parse_error','Google image page has no recognized original-image/page bindings',[]
