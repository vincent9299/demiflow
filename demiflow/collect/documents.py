"""Immutable, table-independent text documents and verified block reads."""
from __future__ import annotations
import hashlib
import json
import re
from datetime import datetime, timezone
from demiflow.objects import LocalObjectStore, ObjectRef, open_object

SCHEMA_VERSION = 'demiflow.document.v1'
PARSER_VERSION = 'structured-html-3'


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class DocumentError(ValueError):
    pass


def read_document(ref, *, max_bytes=8 * 1024 * 1024):
    reference = ObjectRef(**ref)
    with open_object(reference.uri) as stream:
        payload = stream.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise DocumentError('normalized_document_too_large')
    if hashlib.sha256(payload).hexdigest() != reference.sha256:
        raise DocumentError('document_sha256_mismatch')
    try:
        doc = json.loads(payload)
        if doc.get('schema_version') != SCHEMA_VERSION:
            raise DocumentError('unsupported_document_schema')
        ids = [b['block_id'] for b in doc['blocks']]
        valid = ids and len(ids)==len(set(ids)) and all(
            isinstance(b['block_id'],str) and b['block_id'] and b['position']==i and
            isinstance(b['kind'],str) and isinstance(b['text'],str) and b['text'].strip() and
            isinstance(b['headings'],list) and all(isinstance(h,str) for h in b['headings'])
            for i,b in enumerate(doc['blocks']))
        if not valid: raise DocumentError('invalid_document_blocks')
        if not all(isinstance(doc['source'][k],str) for k in ('url','final_url','title','retrieved_at','content_type')):
            raise DocumentError('invalid_document_source')
        if not doc['parser']['version']: raise DocumentError('missing_parser_version')
        ObjectRef(**doc['raw_ref'])
    except (KeyError,TypeError,AttributeError) as exc:
        raise DocumentError('invalid_document_shape') from exc

    return doc


def parse_document(body, *, url, final_url, content_type, retrieved_at=None, pdf_parser=None):
    """Structural extraction only. Source reliability belongs to consumers."""
    media = content_type.partition(';')[0].strip().lower()
    if media=='application/pdf' and pdf_parser is not None:
        from .pdf_text import extract_pdf,pdf_identity
        result=extract_pdf(body,pdf_parser);blocks=[]
        for page,text in enumerate(result['pages'],1):
            if text.strip():
                blocks.append({'block_id':f'b{len(blocks):06d}','position':len(blocks),
                    'kind':'pdf_page_text','text':text.strip(),'headings':[f'Page {page}'],
                    'page_number':page})
        profile=pdf_identity(pdf_parser)
        return {'schema_version':SCHEMA_VERSION,
            'source':{'url':url,'final_url':final_url,'content_type':content_type,
                'title':result['title'],'author':result['author'],'published_at':'',
                'retrieved_at':retrieved_at or datetime.now(timezone.utc).isoformat()},
            'parser':{'name':'demiflow.pdf_text','version':profile['version'],
                'config_sha256':hashlib.sha256(canonical(profile).encode()).hexdigest(),
                'page_count':len(result['pages']),'empty_text_pages':result['empty_text_pages'],
                'limitations':'Text layer only; no OCR or visual/figure interpretation. Text order and table geometry require source review.'},
            'blocks':blocks}
    if media not in {'text/html', 'application/xhtml+xml', 'text/plain'}:
        raise DocumentError('unsupported_media_type:' + media)
    match = re.search(r'charset\s*=\s*["\']?([^\s;"\']+)', content_type, re.I)
    html_charset = re.search(br'<meta[^>]+charset\s*=\s*[\"\']?([a-zA-Z0-9_-]+)',body[:8192],re.I) if media!='text/plain' else None
    encoding = match.group(1) if match else (html_charset.group(1).decode('ascii') if html_charset else 'utf-8-sig')
    try:
        text = body.decode(encoding, errors='replace')
    except LookupError as exc:
        raise DocumentError('unsupported_charset') from exc
    if text.count('\ufffd') > max(3,len(text)*.01):
        raise DocumentError('damaged_text_encoding')
    title, author, date, blocks = '', '', '', []
    if media != 'text/plain':
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(text, 'lxml')
        title = soup.title.get_text(' ', strip=True) if soup.title else ''
        for key, target in [('author', 'author'), ('article:published_time', 'date')]:
            tag = soup.find('meta', attrs={'name':key}) or soup.find('meta', attrs={'property':key})
            if tag:
                if target == 'author': author = tag.get('content', '')
                else: date = tag.get('content', '')
        visible = soup.find('main') or soup.body or soup
        # Sidebar widgets are not access barriers for a public main document.
        password = [tag for tag in visible.select('input[type="password"]') if not tag.find_parent(['aside','nav','footer'])]
        captcha = [tag for tag in visible.select('[id*="captcha"], [class*="captcha"]') if not tag.find_parent(['aside','nav','footer'])]
        if password and not visible.find(['article','p']) and visible.name!='article':
            raise DocumentError('login_required')
        if captcha and not visible.find(['article','p']) and visible.name!='article':
            raise DocumentError('captcha')
        # Malformed HTML may put real content inside <head>; remove metadata
        # elements, not their entire ancestor and any recovered article in it.
        for tag in soup.select('title,meta,base,link,script,style,noscript,nav,footer,aside,form,iframe,[role="navigation"],[role="banner"],[role="contentinfo"],[aria-hidden="true"],.mw-portlet,.vector-header-container,.vector-page-toolbar,.mw-jump-link'):
            tag.decompose()
        # Layout classes such as has-sidebar, sidebar-right and ast-no-sidebar
        # describe the whole article/body. Only explicit widget classes are
        # removable; never erase a content container because of its layout.
        widgets = {'sidebar', 'sidebar-content', 'sidebar_content', 'widget-area',
                   'advert', 'advertisement', 'cookie-banner', 'cookie-notice',
                   'cookie-consent', 'cookies-banner', 'related-posts'}
        for tag in list(soup.find_all(attrs={'class':True})):
            if (tag.attrs and tag.name not in {'html','body','main','article'}
                    and not tag.find(['main','article'])
                    and widgets.intersection(c.lower() for c in tag.get('class',[]))):
                tag.decompose()
        # Explicit body markup is stronger than the first <article>, which can
        # be a navigation card or product review. Selection is structural only.
        bodies = soup.select('[itemprop="articleBody"], .entry-content, .post-content, .article-body')
        prose_size = lambda tag: sum(len(p.get_text(' ',strip=True)) for p in tag.find_all(['p','table','pre','blockquote']))
        substantive = [tag for tag in bodies if prose_size(tag)]
        if substantive:
            main = max(substantive, key=prose_size)
        elif soup.find('main') or soup.find(attrs={'role':'main'}):
            main = soup.find('main') or soup.find(attrs={'role':'main'})
        else:
            articles = [tag for tag in soup.find_all('article') if prose_size(tag)]
            main = max(articles, key=prose_size) if articles else (soup.body or soup)
        headings = []; heading_stack=[]
        for tag in main.find_all(['h1','h2','h3','h4','h5','h6','p','li','table','figcaption','pre','blockquote','dt','dd']):
            # A table/list item/quote is a semantic block, not duplicated descendants.
            if any(p.name in {'table','li','blockquote','pre'} for p in tag.parents if p is not main):
                continue
            if tag.name == 'table':
                value = '\n'.join(' | '.join(cell.get_text(' ',strip=True) for cell in row.find_all(['th','td'],recursive=False))
                                  for row in tag.find_all('tr'))
            else:
                value = tag.get_text('\n' if tag.name == 'pre' else ' ', strip=True)
            if not value: continue
            kind = 'paragraph'
            if re.fullmatch('h[1-6]', tag.name):
                level = int(tag.name[1])
                while heading_stack and heading_stack[-1][0]>=level: heading_stack.pop()
                heading_stack.append((level,value));headings=[text for _,text in heading_stack];kind='heading'
            elif tag.name == 'table': kind = 'table'
            elif tag.name == 'figcaption': kind = 'caption'
            elif tag.name == 'li': kind = 'list_item'
            blocks.append({'kind':kind,'text':value,'headings':list(headings),
                           'heading_level':int(tag.name[1]) if kind=='heading' else None})
        if not blocks:
            value = main.get_text('\n', strip=True)
            blocks = [{'kind':'paragraph','text':p.strip(),'headings':[]} for p in value.split('\n') if p.strip()]
    else:
        blocks = [{'kind':'paragraph','text':p.strip(),'headings':[]} for p in re.split(r'\n\s*\n',text) if p.strip()]
    if not blocks: raise DocumentError('empty_body')
    for i, block in enumerate(blocks):
        block.update(block_id=f'b{i:06d}', position=i)
    return {'schema_version':SCHEMA_VERSION,
            'source':{'url':url,'final_url':final_url,'content_type':content_type,'title':title,
                      'author':author,'published_at':date,'retrieved_at':retrieved_at or datetime.now(timezone.utc).isoformat()},
            'parser':{'name':'demiflow.structured_html','version':PARSER_VERSION,
                      'config_sha256':hashlib.sha256(PARSER_VERSION.encode()).hexdigest()},
            'blocks':blocks}


def store_document(directory, body, *, max_normalized_bytes=8*1024*1024, **source):
    doc = parse_document(body, **source)
    store = LocalObjectStore(directory)
    doc['raw_ref'] = store.put(body).to_dict()
    payload = canonical(doc).encode('utf-8')
    if len(payload) > max_normalized_bytes: raise DocumentError('normalized_document_too_large')
    ref = store.put(payload)
    ref.verify()
    # Reject invalid parser output inside fetch's parse-error boundary, before
    # shared-library publication can turn one bad page into a fatal run error.
    # Valid bytes, block IDs and parser/cache identities remain unchanged.
    read_document(ref.to_dict(), max_bytes=max_normalized_bytes)
    return ref.to_dict()
