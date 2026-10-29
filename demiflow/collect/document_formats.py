"""Generic extracted-text import; source-specific formats belong to consumers."""
import hashlib
from .documents import DocumentError, canonical, parse_document


def normalize_material(request, *, max_bytes):
    if request.get('format') != 'text':
        raise DocumentError('unsupported_import_format; supply a normalized document_ref')
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
    doc['source'].update(source)
    doc['source']['content_type'] = 'text/plain'
    doc['parser']['raw_format'] = 'supplied extracted UTF-8 text; not original HTTP response'
    doc['parser']['config_sha256'] = hashlib.sha256(canonical(doc['parser']).encode()).hexdigest()
    return doc, raw
