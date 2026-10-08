"""Source-format adapters behind Dataset.register_documents.

Wikitext templates need a MediaWiki installation to expand faithfully. Keep
their literal source instead of dropping measurements, names or conditions.
"""
import hashlib
import re
from .documents import SCHEMA_VERSION, DocumentError, canonical

WIKI_PARSER_VERSION = 'structured-wikitext-1'


def _visible(code):
    from mwparserfromhell.nodes import Text, Wikilink, ExternalLink, HTMLEntity, Comment, Tag
    out=[]
    for node in code.nodes:
        if isinstance(node, Text):
            out.append(str(node))
        elif isinstance(node, Comment):
            out.append(' ')
        elif isinstance(node, HTMLEntity):
            out.append(node.normalize())
        elif isinstance(node, Wikilink):
            title=str(node.title)
            namespace=title.partition(':')[0].casefold()
            if namespace in {'category','分类','分類'}:
                continue
            if namespace in {'file','image','文件','檔案','图像','圖像'}:
                out.append(str(node))  # retain captions and media qualifiers
            else:
                out.append(_visible(node.text if node.text is not None else node.title))
        elif isinstance(node, ExternalLink):
            out.append(_visible(node.title) if node.title is not None else str(node.url))
        elif isinstance(node, Tag) and str(node.tag).casefold() in {'ref','references'}:
            out.append(' ')
        elif isinstance(node, Tag) and str(node.tag).casefold() in {'b','i','em','strong','small','span','br'}:
            out.append('\n' if str(node.tag).casefold()=='br' else _visible(node.contents))
        else:
            # Includes templates, parser functions, math, tables and unknown
            # extensions. Preserving source is safer than inventing rendering.
            out.append(str(node))
    return ''.join(out)


def _paragraphs(code):
    """Break prose at blank lines without cutting a template/table in half."""
    from mwparserfromhell.nodes import Text
    from mwparserfromhell.wikicode import Wikicode
    atoms=[]; pieces=[]
    for node in code.nodes:
        if isinstance(node,Text):pieces.append(str(node))
        else:
            atoms.append(_visible(Wikicode([node])))
            pieces.append('\x00'+str(len(atoms)-1)+'\x00')
    for paragraph in re.split(r'\n\s*\n',''.join(pieces)):
        value=re.sub(r'\x00(\d+)\x00',lambda m:atoms[int(m[1])],paragraph).strip()
        if value:yield value


def normalize_material(request, *, max_bytes):
    """Return normalized metadata/blocks and an honest raw source snapshot."""
    if request.get('format') == 'text':
        from .documents import parse_document
        source, text = request.get('source'), request.get('text')
        if not isinstance(text, str) or len(text) > max_bytes or not isinstance(source, dict):
            raise DocumentError('text_import_requires_bounded_text_and_source')
        if not all(isinstance(source.get(k), str) for k in ('url', 'final_url', 'title', 'retrieved_at')):
            raise DocumentError('invalid_import_source')
        raw = text.encode('utf-8')
        if len(raw) > max_bytes:
            raise DocumentError('raw_snapshot_too_large')
        doc = parse_document(raw, url=source['url'], final_url=source['final_url'],
                             content_type='text/plain', retrieved_at=source['retrieved_at'])
        # An import with unknown acquisition time must remain undated. The raw
        # object is exactly the supplied extracted text, not a claimed HTTP body.
        doc['source'].update(source)
        doc['source']['content_type'] = 'text/plain'
        doc['parser']['raw_format'] = 'supplied extracted UTF-8 text; not original HTTP response'
        doc['parser']['config_sha256'] = hashlib.sha256(canonical(doc['parser']).encode()).hexdigest()
        return doc, raw
    if request.get('format') != 'wikitext_sections':
        raise DocumentError('unsupported_import_format')
    from importlib.metadata import version
    import mwparserfromhell
    sections=request.get('sections')
    source=request.get('source')
    if not isinstance(sections,list) or not isinstance(source,dict):
        raise DocumentError('wikitext_sections_requires_source_and_sections')
    if not all(isinstance(source.get(k),str) for k in ('url','final_url','title','retrieved_at')):
        raise DocumentError('invalid_import_source')
    raw=canonical({'format':'wikitext_sections','source':source,'sections':sections}).encode()
    if len(raw)>max_bytes:
        raise DocumentError('raw_snapshot_too_large')
    blocks=[]; stack=[]
    for section in sections:
        if not isinstance(section,dict) or not all(isinstance(section.get(k),str) for k in ('title','text')):
            raise DocumentError('invalid_wikitext_section')
        if '\x00' in section['text'] or '\x00' in section['title']:
            raise DocumentError('invalid_wikitext_control_character')
        level=section.get('level',2)
        if type(level) is not int or not 1<=level<=6:
            raise DocumentError('invalid_heading_level')
        title=_visible(mwparserfromhell.parse(section['title'])).strip()
        if title:
            while stack and stack[-1][0]>=level:stack.pop()
            stack.append((level,title))
            blocks.append({'kind':'heading','text':title,'headings':[x[1] for x in stack],'heading_level':level})
        for paragraph in _paragraphs(mwparserfromhell.parse(section['text'])):
            blocks.append({'kind':'wikitext_source','text':paragraph,'headings':[x[1] for x in stack]})
    if not any(b['kind']!='heading' for b in blocks):
        raise DocumentError('empty_body')
    for i,block in enumerate(blocks):block.update(block_id=f'b{i:06d}',position=i)
    parser={'name':'demiflow.wikitext_sections','version':WIKI_PARSER_VERSION,
            'library_version':version('mwparserfromhell'),
            'template_expansion':'not_performed; template source retained',
            'raw_format':'JSON snapshot of supplied sections; not original dump XML'}
    parser['config_sha256']=hashlib.sha256(canonical(parser).encode()).hexdigest()
    doc={'schema_version':SCHEMA_VERSION,
         'source':{**source,'content_type':'text/x-wiki','author':source.get('author',''),
                   'published_at':source.get('published_at','')},
         'parser':parser,'blocks':blocks}
    return doc,raw
