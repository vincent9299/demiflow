"""Opt-in PDF text extraction with an isolated, bounded parser process.

No OCR or visual interpretation. Input/output, pages, CPU, address space and
wall time are bounded. A rejected page rejects the document; never truncate.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile


PDF_VERSION='pdf-text-1'
PYPDF_VERSION='6.19.0'
DEFAULTS=dict(max_input_bytes=8*1024*1024,max_text_bytes=2*1024*1024,
              max_pages=200,memory_mb=512,cpu_s=20,timeout_s=25)
BOUNDS=dict(max_input_bytes=(1024,32*1024*1024),max_text_bytes=(1024,8*1024*1024),
            max_pages=(1,1000),memory_mb=(64,2048),cpu_s=(1,120),timeout_s=(1,180))


def pdf_policy(value):
    if value is None:return None
    if not isinstance(value,dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid PDF parser declaration')
    result={**DEFAULTS,**value}
    for key,(low,high) in BOUNDS.items():
        if type(result[key]) is not int or not low<=result[key]<=high:
            raise ValueError('Invalid PDF parser bound: '+key)
    return result


def pdf_identity(policy):
    return {'version':PDF_VERSION,'pypdf':PYPDF_VERSION,'policy':pdf_policy(policy)}


def extract_pdf(body,policy):
    from .documents import DocumentError
    policy=pdf_policy(policy)
    if policy is None:raise DocumentError('pdf_parser_not_enabled')
    if not isinstance(body,bytes) or len(body)>policy['max_input_bytes']:
        raise DocumentError('pdf_input_limit')
    if not body.startswith(b'%PDF-'):raise DocumentError('invalid_pdf_signature')
    if not sys.platform.startswith('linux'):
        raise DocumentError('pdf_resource_limits_require_linux')
    # Upstream fetch concurrency bounds the number of these workers. Temporary
    # input is bounded above; output has a hard child file-size limit below.
    with tempfile.TemporaryDirectory(prefix='demiflow-pdf-') as folder:
        root=Path(folder);source=root/'input.pdf';config=root/'policy.json';output=root/'result.json'
        source.write_bytes(body);config.write_text(json.dumps({**policy,'pypdf_version':PYPDF_VERSION}))
        process=subprocess.Popen([sys.executable,'-I',str(Path(__file__).with_name('pdf_text_worker.py')),
            str(source),str(config),str(output)],stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            code=process.wait(timeout=policy['timeout_s'])
        except subprocess.TimeoutExpired:
            process.kill();process.wait()
            raise DocumentError('pdf_wall_time_limit') from None
        except BaseException:
            process.kill();process.wait();raise
        if code or not output.exists():
            raise DocumentError('pdf_worker_resource_or_process_failure:'+str(code))
        limit=policy['max_text_bytes']+256*1024
        with output.open('rb') as stream:encoded=stream.read(limit+1)
        if len(encoded)>limit:raise DocumentError('pdf_output_limit')
        result=json.loads(encoded)
        if not result.get('ok'):raise DocumentError(result.get('error','pdf_parse_error'))
        pages=result.get('pages')
        if (not isinstance(pages,list) or not 1<=len(pages)<=policy['max_pages']
                or any(not isinstance(t,str) for t in pages)
                or sum(len(t.encode()) for t in pages)>policy['max_text_bytes']):
            raise DocumentError('invalid_pdf_worker_result')
        return result
