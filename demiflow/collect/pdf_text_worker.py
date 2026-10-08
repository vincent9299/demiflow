"""Private standalone PDF worker; deliberately imports no demiflow/Arrow stack."""
import json
from pathlib import Path
import resource
import sys


def main():
    source,config,output=map(Path,sys.argv[1:])
    with config.open('rb') as stream:encoded=stream.read(4097)
    if len(encoded)>4096:raise ValueError('oversized_worker_configuration')
    cfg=json.loads(encoded)
    memory=cfg['memory_mb']*1024*1024
    resource.setrlimit(resource.RLIMIT_AS,(memory,memory))
    resource.setrlimit(resource.RLIMIT_CPU,(cfg['cpu_s'],cfg['cpu_s']))
    resource.setrlimit(resource.RLIMIT_FSIZE,(cfg['max_text_bytes']+256*1024,)*2)
    resource.setrlimit(resource.RLIMIT_NOFILE,(32,32))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    try:
        import pypdf
        if pypdf.__version__!=cfg['pypdf_version']:
            raise ValueError('pdf_dependency_version_mismatch')
        if source.stat().st_size>cfg['max_input_bytes']:raise ValueError('pdf_input_limit')
        with source.open('rb') as stream:
            reader=pypdf.PdfReader(stream,strict=True)
            # Public PDFs can have an empty user password and owner-only
            # editing restrictions. Never guess or accept a supplied password.
            if reader.is_encrypted and not reader.decrypt(''):
                raise ValueError('pdf_password_required')
            count=len(reader.pages)
            if not 1<=count<=cfg['max_pages']:raise ValueError('pdf_page_limit')
            texts=[];size=0
            def validate_mapping(text,user_matrix,text_matrix,font,size):
                if not text.strip():return
                if font is None:
                    raise ValueError('pdf_missing_font_mapping')
                if (font.get('/Subtype')=='/Type0' and font.get('/Encoding') in ('/Identity-H','/Identity-V')
                        and not font.get('/ToUnicode')):
                    # CID numbers are glyph identities, not Unicode points.
                    # Guessing here can produce fluent-looking wrong letters.
                    raise ValueError('pdf_missing_unicode_map')
            for page in reader.pages:
                text=page.extract_text(visitor_text=validate_mapping)
                if not isinstance(text,str):raise ValueError('pdf_invalid_page_text')
                size+=len(text.encode())
                if size>cfg['max_text_bytes']:raise ValueError('pdf_text_limit')
                if '\x00' in text or text.count('\ufffd')>max(3,len(text)*.01):
                    raise ValueError('pdf_damaged_text_encoding')
                texts.append(text)
            if not any(t.strip() for t in texts):raise ValueError('pdf_no_extractable_text')
            metadata=reader.metadata
            # Metadata is optional but cannot grow the result without bounds.
            title=str(metadata.title or '') if metadata else ''
            author=str(metadata.author or '') if metadata else ''
            if len(title.encode())+len(author.encode())>16384:
                raise ValueError('pdf_metadata_limit')
            result={'ok':True,'pages':texts,'title':title,'author':author,
                    'empty_text_pages':[i+1 for i,t in enumerate(texts) if not t.strip()]}
    except MemoryError:
        result={'ok':False,'error':'pdf_memory_limit'}
    except Exception as exc:
        # Keep exceptions bounded; avoid dumping document text or arbitrary
        # file paths from parser diagnostics into durable provider receipts.
        known={'pdf_dependency_version_mismatch','pdf_input_limit','pdf_password_required','pdf_page_limit',
               'pdf_invalid_page_text','pdf_text_limit','pdf_damaged_text_encoding',
               'pdf_no_extractable_text','pdf_metadata_limit','pdf_missing_unicode_map',
               'pdf_missing_font_mapping'}
        message=str(exc)
        result={'ok':False,'error':message if message in known else 'pdf_parse_error:'+type(exc).__name__}
    with output.open('w') as stream:
        for chunk in json.JSONEncoder(ensure_ascii=False,separators=(',',':')).iterencode(result):
            stream.write(chunk)


if __name__=='__main__':main()
