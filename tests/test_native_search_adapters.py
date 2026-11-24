"""Request/parser parity and all result families, independent of live providers."""
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, parse_qs

ROOT = Path(__file__).resolve().parents[1]


def probe(body, engines=('google', 'wikisearch', 'wikipedia', 'mwmbl')):
    program = '''
import json,sys,traceback
from demiflow.collect.native_search import worker as w
from demiflow.collect.native_search.config import SearchConfig
config=SearchConfig(engines=%r,language='en')
settings={'sources':config.source_configs(),'outgoing':{},'timeout_s':10,'max_bytes':1048576,'max_results':100,'max_redirects':3,'offline_audit':True}

class Reply:
 def __init__(self,url,body,status=200,params=None):
  self.url,self.status_code,self.headers,self.search_params=url,status,{},params
  self.content=body if isinstance(body,bytes) else json.dumps(body).encode()
 @property
 def text(self): return self.content.decode()
 def json(self): return json.loads(self.content)

def prepare(name,query,**spec):
 engine=loaded[name]
 spec={'language':'en','pageno':1,'safesearch':0,'time_range':None,'engine_data':{},**spec}
 params=w.request_params(engine,query,spec)
 params={**{k:(v.copy() if isinstance(v,dict) else v) for k,v in w.ONLINE_DEFAULTS.items()},**params}
 engine.request(query,params)
 return engine,params
try:
 loaded,errors=w.bootstrap(settings)
 if errors: raise RuntimeError(errors)
''' % (tuple(engines),) + body + '''
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
  engine,params=prepare('wikisearch','天牛',language=lang)
  reply=Reply(params['url'],{'pages':[{'key':'Cerambycidae','title':'天牛科','excerpt':'<b>天牛</b>科'}]})
  values[lang]={'url':params['url'],'result':engine.response(reply)}
 engine,params=prepare('wikipedia','Crassula ovata')
 reply=Reply(params['url'],{'type':'standard','title':'Crassula ovata','extract':'A succulent plant.',
   'content_urls':{'desktop':{'page':'https://en.wikipedia.org/wiki/Crassula_ovata'}}})
 values['summary']={'url':params['url'],'result':engine.response(reply)}
 w.send(values)
''', engines=('wikisearch', 'wikipedia'))
    for lang, host in [('zh-CN','zh.wikipedia.org'), ('en','en.wikipedia.org'), ('ja','ja.wikipedia.org'), ('nb','no.wikipedia.org')]:
        assert urlsplit(result[lang]['url']).hostname == host
        assert result[lang]['result'][0]['url'] == f'https://{host}/wiki/Cerambycidae'
    assert '/api/rest_v1/page/summary/Crassula%20ovata' in result['summary']['url']
    assert result['summary']['result'][0]['infobox'] == 'Crassula ovata'


def test_wikisearch_requires_an_explicit_language():
    result = probe('''
 value=w.run_search({'engine':'wikisearch','query':'oak','parameters':{'language':'all','pageno':1,'safesearch':0,
   'time_range':None,'engine_data':{},'network':'default'}},loaded,errors,set(),settings)
 w.send(value)
''', engines=('wikisearch',))
    assert result['status'] == 'unsupported_parameters'
    assert result['reason'] == 'wikisearch requires an explicit language'


def test_google_and_mwmbl_request_parser_fixtures():
    result = probe('''
 values={}
 for name in ('google','mwmbl'):
  engine,params=prepare(name,'oak tree',language='zh-CN',pageno=2 if name=='google' else 1,safesearch=2)
  if name=='google':
   body=b'<html><div class="zMzFAb"><a class="fuLhoc" href="https://example.org/oak"><span class="CVA68e">Oak</span></a><div class="taTFJ"><span class="FrIlee">An oak tree.</span></div></div></html>'
  else:
   body=[{'url':'https://example.org/oak','title':[{'value':'Oak'}],'extract':[{'value':'An oak tree.'}]}]
  reply=Reply(params['url'],body,params=params)
  values[name]={'url':params['url'],'results':engine.response(reply),'impersonate':params.get('impersonate')}
 w.send(values)
''', engines=('google', 'mwmbl'))
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
 results=[{'url':'https://example.org/shared','title':'Main','publishedDate':datetime.datetime(2026,1,2)},
  {'url':'https://example.org/image','title':'Image','img_src':'https://example.org/pixel.png','template':'images.html'},
  {'url':'https://example.org/file','title':'File','template':'files.html'},
  {'url':'https://example.org/code','title':'Code','template':'code.html'},
  {'url':'https://example.org/paper','title':'Paper','template':'paper.html'},{'answer':'42'},
  {'template':'keyvalue.html','kvmap':{'__native_type__':'literal','value':'preserved','parsed_url':'data column'}},
  {'infobox':'Oak','id':'https://example.org/oak','urls':[{'url':'https://example.org/oak','title':'Oak'}]},
  {'suggestion':'oak trees'},{'correction':'oak'},{'engine_data':'cursor-value','key':'cursor'}]
 packed=json.loads(json.dumps([w.pack(r) for r in results]))
 value=w.merge({'sources':[{'name':'google','weight':1},{'name':'mwmbl','weight':2}],
  'groups':[{'engine':'google','results':packed},{'engine':'mwmbl','results':[w.pack({'url':'https://example.org/shared','title':'Shared again'})]}]})
 w.send(value)
''', engines=('mwmbl',))
    assert result['results'][0]['url'] == 'https://example.org/shared'
    assert result['results'][0]['engines'] == ['google', 'mwmbl']
    assert result['results'][0]['score'] == 8
    assert result['results'][0]['publishedDate'] == '2026-01-02T00:00:00'
    assert len(result['results']) == 6
    keyvalue = next(r for r in result['results'] if r.get('kvmap'))
    assert keyvalue['kvmap']['__native_type__'] == 'literal'
    assert keyvalue['kvmap']['parsed_url'] == 'data column'
    assert result['answers'][0]['answer'] == '42'
    assert result['suggestions'] == ['oak trees'] and result['corrections'] == ['oak']
    assert result['engine_data']['google']['cursor'] == 'cursor-value'
    assert len(result['infoboxes']) == 1


def test_receipts_from_the_previous_runtime_still_decode():
    """Stored source receipts carry result classes of the formerly vendored engine."""
    result = probe('''
 old=[{'__native_type__':'searx.result_types._base:MainResult','value':{'__native_type__':'mapping','value':[
   ['url','https://example.org/old'],['title','Old'],['parsed_url',['https','example.org','/old','','','']],
   ['engines',{'__native_type__':'set','value':['google']}]]}},
  {'__native_type__':'searx.result_types.answer:Answer','value':{'__native_type__':'mapping','value':[['answer','7']]}}]
 value=w.merge({'sources':[{'name':'google','weight':1}],'groups':[{'engine':'google','results':old}]})
 try:
  w.unpack({'__native_type__':'os:system','value':{}})
  rejected=False
 except ValueError:
  rejected=True
 w.send({'merged':value,'rejected':rejected})
''', engines=('mwmbl',))
    assert result['merged']['results'][0]['url'] == 'https://example.org/old'
    assert result['merged']['answers'][0]['answer'] == '7'
    assert result['rejected'] is True


def test_only_online_and_offline_adapters_are_supported(tmp_path):
    adapter = tmp_path / 'dictionary_adapter.py'
    adapter.write_text('\n'.join(["engine_type = 'online_dictionary'", "categories = ['general']",
                                   'def request(query, params):', "    params['url'] = 'https://example.org'",
                                   'def response(resp):', '    return []', '']))
    result = probe(f'''
 from demiflow.collect.native_search.sources import load, SourceLoadError
 try:
  load({{'name':'dictionary','engine':{str(adapter)[:-3]!r}}},10)
  status='loaded'
 except SourceLoadError as exc:
  status=exc.status
 w.send({{'status':status,'sqlite':loaded['local'].engine_type}})
''', engines=({'name': 'local', 'engine': 'sqlite', 'database': str(tmp_path / 'db.sqlite'),
                'query_str': 'select 1'},))
    assert result == {'status': 'unsupported_processor', 'sqlite': 'offline'}


def test_helper_lookups_are_cached_bounded_and_refreshed():
    # Token, cookie and key pages of the built-in adapters, answered by fixed pages instead of the network.
    result = probe('''
 import types
 clock=[1000.0]
 calls=[]
 class Page:
  def __init__(self,text='',status=200,cookies=None): self.text,self.status_code,self.cookies=text,status,cookies or {}
 def serve(pages):
  def get(url,**kw):
   calls.append(url)
   return pages(url)
  return get
 out={}
 ddg=loaded['duckduckgo images']
 ddg.time=types.SimpleNamespace(monotonic=lambda: clock[0])
 token=['4-111']
 ddg.get=serve(lambda url: Page('<script>vqd="%s"</script>' % token[0]))
 spec={'headers':{'User-Agent':'UA'}}
 out['ddg']=[ddg.vqd('oak',spec),ddg.vqd('oak',spec),len(calls)]
 clock[0]+=ddg.TOKEN_SECONDS+1; token[0]='4-222'
 out['ddg']+=[ddg.vqd('oak',spec),len(calls)]
 for i in range(300): ddg.vqd('q%d'%i,spec)
 token[0]='x'*300
 out['ddg']+=[len(ddg._tokens),ddg.vqd('long',spec),any(k[0]=='long' for k in ddg._tokens)]
 calls.clear()
 baidu=loaded['baidu images']
 baidu.time=types.SimpleNamespace(monotonic=lambda: clock[0])
 jar=[{'BAIDUID':'A'}]
 baidu.get=serve(lambda url: Page(cookies=dict(jar[0])))
 out['baidu']=[baidu.image_cookies({}),baidu.image_cookies({}),len(calls)]
 clock[0]+=baidu.COOKIE_SECONDS+1; jar[0]={'BAIDUID':'B'}
 out['baidu']+=[baidu.image_cookies({}),len(calls)]
 calls.clear()
 pexels=loaded['pexels']
 secret=['KEY1']
 pexels.get=serve(lambda url: Page('<html><head><script src="https://cdn.example/a.js"></script></head></html>')
                  if url.rstrip('/')==pexels.base_url else Page('x={"secret-key":"%s"}' % secret[0]))
 params=lambda: {'pageno':1,'time_range':None,'headers':{}}
 first,second=params(),params()
 pexels.request('oak',first); pexels.request('oak',second)
 out['pexels']=[first['headers']['secret-key'],second['headers']['secret-key'],len(calls)]
 pexels._secret['value']=None; secret[0]='k'*300
 try:
  pexels.request('oak',params()); out['pexels'].append('no error')
 except Exception as exc:
  out['pexels'].append(type(exc).__name__)
 pexels._secret['value']=None; pexels.fallback_secret_key='FALLBACK'
 fallback=params(); pexels.request('oak',fallback); out['pexels'].append(fallback['headers']['secret-key'])
 calls.clear()
 pdia=loaded['public domain image archive']
 proxy=['https://proxy.example/search-proxy']
 pdia.get=serve(lambda url: Page('<script src="/_astro/InfiniteSearch.abc.js"></script>') if '/search/' in url
                else Page('const u="%s";' % proxy[0]))
 out['pdia']=[pdia.proxy_url(),pdia.proxy_url(),len(calls)]
 try:
  pdia.response(Reply(proxy[0],{'error':'refused'},status=403)); out['pdia'].append('no error')
 except Exception as exc:
  out['pdia'].append(type(exc).__name__)
 out['pdia'].append(pdia._proxy['url'])
 proxy[0]='https://'+'p'*3000+'/search-proxy'
 try:
  pdia.proxy_url(); out['pdia'].append('no error')
 except LookupError as exc:
  out['pdia'].append(str(exc))
 w.send(out)
''', engines=('duckduckgo images', 'baidu images', 'pexels', 'public domain image archive'))
    assert result['ddg'] == ['4-111', '4-111', 1, '4-222', 2, 256, '', False]
    assert result['baidu'] == [{'BAIDUID': 'A'}, {'BAIDUID': 'A'}, 1, {'BAIDUID': 'B'}, 2]
    assert result['pexels'] == ['KEY1', 'KEY1', 2, 'SourceError', 'FALLBACK']
    assert result['pdia'] == ['https://proxy.example/search-proxy', 'https://proxy.example/search-proxy', 2,
                              'SourceAccessDenied', None, 'Search proxy URL not found']
