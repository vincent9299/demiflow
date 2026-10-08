"""Request/parser parity and all result families, independent of live providers."""
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, parse_qs

ROOT = Path(__file__).resolve().parents[1]


def probe(body):
    program = '''
import json,sys,traceback
from demiflow.collect.native_search import worker as w
from demiflow.collect.native_search.config import SearchConfig
config=SearchConfig(engines=('google','wikisearch','wikipedia','mwmbl'),language='en')
settings={'sources':config.source_configs(),'outgoing':{},'timeout_s':10,'max_bytes':1048576,'max_results':100,'max_redirects':3,'offline_audit':True}
try:
 processors,errors=w.bootstrap(settings)
 if errors: raise RuntimeError(errors)
 from searx.search.models import SearchQuery,EngineRef
 from searx.extended_types import SXNG_Response
''' + body + '''
except BaseException:
 traceback.print_exc(file=sys.stderr)
 raise
'''
    result = subprocess.run([sys.executable, '-c', program], cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_wikipedia_languages_keyword_and_summary_fixtures():
    result = probe('''
 values={}
 for lang in ('zh-CN','en','ja','nb'):
  proc=processors['wikisearch']
  params=proc.get_params(SearchQuery('天牛',[EngineRef('wikisearch','general')],lang=lang),'general')
  proc.engine.request('天牛',params)
  resp=SXNG_Response();resp.url=params['url'];resp.status_code=200
  resp.content=json.dumps({'pages':[{'key':'Cerambycidae','title':'天牛科','excerpt':'<b>天牛</b>科'}]}).encode()
  values[lang]={'url':params['url'],'result':w.plain(proc.engine.response(resp))}
 proc=processors['wikipedia']
 params=proc.get_params(SearchQuery('Crassula ovata',[EngineRef('wikipedia','general')],lang='en'),'general')
 proc.engine.request('Crassula ovata',params)
 resp=SXNG_Response();resp.url=params['url'];resp.status_code=200
 resp.content=json.dumps({'type':'standard','title':'Crassula ovata','extract':'A succulent plant.',
   'content_urls':{'desktop':{'page':'https://en.wikipedia.org/wiki/Crassula_ovata'}}}).encode()
 values['summary']={'url':params['url'],'result':w.plain(proc.engine.response(resp))}
 w.send(values)
''')
    for lang, host in [('zh-CN','zh.wikipedia.org'), ('en','en.wikipedia.org'), ('ja','ja.wikipedia.org'), ('nb','no.wikipedia.org')]:
        assert urlsplit(result[lang]['url']).hostname == host
        assert result[lang]['result'][0]['url'] == f'https://{host}/wiki/Cerambycidae'
    assert '/api/rest_v1/page/summary/Crassula%20ovata' in result['summary']['url']
    assert result['summary']['result'][0]['infobox'] == 'Crassula ovata'


def test_google_and_mwmbl_request_parser_fixtures():
    result = probe('''
 values={}
 for name in ('google','mwmbl'):
  proc=processors[name]
  sq=SearchQuery('oak tree',[EngineRef(name,'general')],lang='zh-CN',pageno=2 if name=='google' else 1,safesearch=2)
  params=proc.get_params(sq,'general');proc.engine.request(sq.query,params)
  resp=SXNG_Response();resp.url=params['url'];resp.status_code=200;resp.search_params=params
  if name=='google':
   resp.content=b'<html><div class="zMzFAb"><a class="fuLhoc" href="https://example.org/oak"><span class="CVA68e">Oak</span></a><div class="taTFJ"><span class="FrIlee">An oak tree.</span></div></div></html>'
  else:
   resp.content=json.dumps([{'url':'https://example.org/oak','title':[{'value':'Oak'}],'extract':[{'value':'An oak tree.'}]}]).encode()
  values[name]={'url':params['url'],'results':w.plain(proc.engine.response(resp)),'impersonate':params.get('impersonate')}
 w.send(values)
''')
    for name in ('google', 'mwmbl'):
        assert result[name]['results'][0]['url'] == 'https://example.org/oak'
        assert result[name]['results'][0]['title'] == 'Oak'
    query = parse_qs(urlsplit(result['google']['url']).query)
    assert query['q'] == ['oak tree'] and query['start'] == ['10']
    assert result['google']['impersonate'] == 'chrome99_android'
    assert parse_qs(urlsplit(result['mwmbl']['url']).query)['s'] == ['oak tree']


def test_all_result_families_roundtrip_and_ranking():
    result = probe('''
 import datetime
 from searx.result_types import MainResult, KeyValue
 from searx.result_types.answer import Answer
 from searx.result_types.image import Image
 from searx.result_types.file import File
 from searx.result_types.code import Code
 from searx.result_types.paper import Paper
 results=[MainResult(url='https://example.org/shared',title='Main',publishedDate=datetime.datetime(2026,1,2)),
  Image(url='https://example.org/image',title='Image',img_src='https://example.org/pixel.png'),
  File(url='https://example.org/file',title='File'),Code(url='https://example.org/code',title='Code'),
  Paper(url='https://example.org/paper',title='Paper'),Answer(answer='42'),
  KeyValue(kvmap={'__native_type__':'literal','value':'preserved','parsed_url':'data column'}),
  {'infobox':'Oak','id':'https://example.org/oak','urls':[{'url':'https://example.org/oak','title':'Oak'}]},
  {'suggestion':'oak trees'},{'correction':'oak'},{'engine_data':'cursor-value','key':'cursor'}]
 packed=json.loads(json.dumps([w.pack(r) for r in results]))
 value=w.merge({'sources':[{'name':'google','weight':1},{'name':'mwmbl','weight':2}],
  'groups':[{'engine':'google','results':packed},{'engine':'mwmbl','results':[w.pack({'url':'https://example.org/shared','title':'Shared again'})]}]})
 w.send(value)
''')
    assert result['results'][0]['url'] == 'https://example.org/shared'
    assert result['results'][0]['engines'] == ['google', 'mwmbl']
    assert result['results'][0]['score'] == 8
    assert len(result['results']) == 6
    keyvalue = next(r for r in result['results'] if r.get('kvmap'))
    assert keyvalue['kvmap']['__native_type__'] == 'literal'
    assert keyvalue['kvmap']['parsed_url'] == 'data column'
    assert result['answers'][0]['answer'] == '42'
    assert result['suggestions'] == ['oak trees'] and result['corrections'] == ['oak']
    assert result['engine_data']['google']['cursor'] == 'cursor-value'
    assert len(result['infoboxes']) == 1


def test_all_five_processor_families_available_without_flask_context():
    result = probe('''
 from searx.search.processors import ProcessorMap
 import flask
 engine=processors['google'].engine
 values={'families':sorted(ProcessorMap.processor_types),'flask_context':flask.has_request_context()}
 for family,query in [('online_dictionary','en-de hello'),('online_currency','10 USD to EUR'),('online_url_search','lookup https://example.org/page')]:
  p=ProcessorMap.processor_types[family](engine)
  sq=SearchQuery(query,[EngineRef('google','general')],lang='en')
  values[family]=w.plain(p.get_params(sq,'general'))
 w.send(values)
''')
    assert result['families'] == ['offline','online','online_currency','online_dictionary','online_url_search']
    assert result['flask_context'] is False
    assert result['online_dictionary']['query'] == 'hello'
    assert result['online_dictionary']['from_lang'][1] == 'en'
    assert result['online_currency']['amount'] == 10
    assert result['online_currency']['from_iso4217'] == 'USD'
    assert result['online_url_search']['search_urls']['http'] == 'https://example.org/page'
