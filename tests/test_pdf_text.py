import io
import subprocess
import sys
import zlib

import httpx
import pytest
pypdf=pytest.importorskip('pypdf')
PdfWriter=pypdf.PdfWriter
from pypdf.generic import DictionaryObject,NameObject,DecodedStreamObject,EncodedStreamObject,ArrayObject,NumberObject,TextStringObject

from demiflow.collect.documents import DocumentError,parse_document,read_document
from demiflow.collect.pdf_text import extract_pdf,pdf_policy
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient


def pdf(pages):
    writer=PdfWriter()
    for text in pages:
        page=writer.add_blank_page(width=200,height=200)
        font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),
            NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
        page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):
            DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
        stream=DecodedStreamObject();stream.set_data(b'BT /F1 12 Tf 10 10 Td ('+text.encode()+b') Tj ET')
        page[NameObject('/Contents')]=writer._add_object(stream)
    output=io.BytesIO();writer.write(output);return output.getvalue()


def test_pdf_is_explicit_and_page_addresses_are_preserved():
    body=pdf(['paired opposite leaves','', 'side embouchure hole'])
    options=dict(url='https://example.test/a.pdf',final_url='https://example.test/a.pdf',
                 content_type='application/pdf',retrieved_at='2026-10-02T00:00:00Z')
    with pytest.raises(DocumentError,match='unsupported_media_type'):
        parse_document(body,**options)
    doc=parse_document(body,**options,pdf_parser={})
    assert [b['page_number'] for b in doc['blocks']]==[1,3]
    assert doc['parser']['empty_text_pages']==[2]
    assert doc['parser']['page_count']==3
    assert doc['blocks'][1]['text']=='side embouchure hole'
    assert 'no OCR' in doc['parser']['limitations']


@pytest.mark.parametrize('pages,policy,reason',[
    (['first','second'],{'max_pages':1},'pdf_page_limit'),
    (['a'*800,'b'*800],{'max_text_bytes':1024},'pdf_text_limit'),
    (['',''],{},'pdf_no_extractable_text')])
def test_limits_reject_whole_document_without_partial_text(pages,policy,reason):
    with pytest.raises(DocumentError,match=reason):extract_pdf(pdf(pages),policy)


def test_corrupt_encrypted_and_oversized_input_are_distinct():
    with pytest.raises(DocumentError,match='invalid_pdf_signature'):extract_pdf(b'not PDF',{})
    with pytest.raises(DocumentError,match='pdf_parse_error'):extract_pdf(b'%PDF-1.7\n corrupt',{})
    writer=PdfWriter();writer.add_blank_page(width=200,height=200);writer.encrypt('private')
    output=io.BytesIO();writer.write(output)
    with pytest.raises(DocumentError,match='pdf_password_required'):extract_pdf(output.getvalue(),{})
    public=PdfWriter(io.BytesIO(pdf(['publicly readable text'])))
    public.encrypt(user_password='',owner_password='owner');encoded=io.BytesIO();public.write(encoded)
    assert extract_pdf(encoded.getvalue(),{})['pages'][0].strip()=='publicly readable text'
    with pytest.raises(DocumentError,match='pdf_input_limit'):
        extract_pdf(b'%PDF-'+b'x'*1024,{'max_input_bytes':1024})


def test_decompression_pressure_is_confined_to_child():
    # Produce 128 MiB decoded stream using a constant 64 KiB input chunk.
    # Parent retains only a small compressed fixture, not its decoded payload.
    encoder=zlib.compressobj();pieces=[];chunk=b' '*65536
    for _ in range(2048):pieces.append(encoder.compress(chunk))
    pieces.append(encoder.flush());compressed=b''.join(pieces)
    writer=PdfWriter();page=writer.add_blank_page(width=200,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),
        NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):
        DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
    stream=EncodedStreamObject();stream._data=compressed;stream[NameObject('/Filter')]=NameObject('/FlateDecode')
    page[NameObject('/Contents')]=writer._add_object(stream)
    output=io.BytesIO();writer.write(output)
    with pytest.raises(DocumentError,match='pdf_(memory_limit|parse_error|worker_resource)'):
        extract_pdf(output.getvalue(),{'memory_mb':64,'timeout_s':10})


def test_declaration_is_lazy_and_bounded(tmp_path):
    assert pdf_policy(None) is None
    for bad in ({'max_pages':0},{'memory_mb':8192},{'extra':1},{'cpu_s':True}):
        with pytest.raises(ValueError):
            WebSession(cache_path=tmp_path/'missing'/'cache.sqlite',object_directory=tmp_path/'objects',pdf_parser=bad)
    assert not list(tmp_path.iterdir())


def test_identity_cid_without_unicode_map_is_not_accepted_as_evidence():
    writer=PdfWriter();page=writer.add_blank_page(width=200,height=200)
    cid=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/CIDFontType2'),
        NameObject('/BaseFont'):NameObject('/TestCID'),NameObject('/CIDSystemInfo'):DictionaryObject({
            NameObject('/Registry'):TextStringObject('Adobe'),NameObject('/Ordering'):TextStringObject('Identity'),
            NameObject('/Supplement'):NumberObject(0)})})
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type0'),
        NameObject('/BaseFont'):NameObject('/TestCID'),NameObject('/Encoding'):NameObject('/Identity-H'),
        NameObject('/DescendantFonts'):ArrayObject([writer._add_object(cid)])})
    page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):
        DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
    stream=DecodedStreamObject();stream.set_data(b'BT /F1 12 Tf 10 10 Td <0031> Tj ET')
    page[NameObject('/Contents')]=writer._add_object(stream)
    output=io.BytesIO();writer.write(output)
    with pytest.raises(DocumentError,match='pdf_missing_unicode_map'):
        extract_pdf(output.getvalue(),{})


def test_stuck_parser_is_killed_and_reaped(monkeypatch):
    from demiflow.collect import pdf_text
    original=subprocess.Popen;children=[]
    def stuck(*args,**kwargs):
        child=original([sys.executable,'-c','import time; time.sleep(60)'])
        children.append(child);return child
    monkeypatch.setattr(pdf_text.subprocess,'Popen',stuck)
    with pytest.raises(DocumentError,match='pdf_wall_time_limit'):
        extract_pdf(pdf(['text']),{'timeout_s':1})
    assert children[0].poll() is not None


async def test_opt_in_reparses_saved_pdf_without_downloading_or_mutating_html(tmp_path):
    calls=[];body=pdf(['knowledge from a retained PDF'])
    class Stream(httpx.AsyncByteStream):
        def __init__(self,value):self.value=value
        async def __aiter__(self):yield self.value
    async def handle(request):
        calls.append(str(request.url))
        is_pdf=request.url.path.endswith('.pdf')
        return httpx.Response(200,headers={'content-type':'application/pdf' if is_pdf else 'text/html'},
                              stream=Stream(body if is_pdf else b'<p>unchanged HTML evidence</p>'))
    options=dict(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',host_interval_s=0,retries=0)
    first=WebClient(**options);first.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
    old_pdf=await first.fetch('https://example.test/a.pdf');old_html=await first.fetch('https://example.test/a.html')
    assert old_pdf['reason']=='unsupported_media_type:application/pdf' and old_pdf['raw_ref']
    await first.aclose()
    second=WebClient(**options,pdf_parser={});second.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
    new_pdf=await second.fetch('https://example.test/a.pdf');new_html=await second.fetch('https://example.test/a.html')
    assert new_pdf['status']=='ok' and new_pdf['raw_ref']==old_pdf['raw_ref']
    assert new_html['document_ref']==old_html['document_ref'] and len(calls)==2
    assert read_document(new_pdf['document_ref'])['blocks'][0]['text']=='knowledge from a retained PDF'
    await second.aclose()
    legacy=WebClient(**options)
    assert (await legacy.fetch('https://example.test/a.pdf'))==old_pdf
    await legacy.aclose()
