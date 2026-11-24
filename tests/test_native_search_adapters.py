"""Request/parser parity and all result families, independent of live providers."""
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit, parse_qs

import pytest

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
 pexels._secret['value']=None; pexels.api_key='CONFIGURED'; before=len(calls)
 fallback=params(); pexels.request('oak',fallback); out['pexels'].append(fallback['headers']['secret-key'])
 again=params(); pexels.request('oak',again)
 out['pexels']+=[again['headers']['secret-key'],pexels._secret['value'],len(calls)-before]
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
    # The configured api_key is sent when the page lookup fails; it is not kept, so every search looks again.
    assert result['pexels'] == ['KEY1', 'KEY1', 2, 'SourceError', 'CONFIGURED', 'CONFIGURED', None, 4]
    assert result['pdia'] == ['https://proxy.example/search-proxy', 'https://proxy.example/search-proxy', 2,
                              'SourceAccessDenied', None, 'Search proxy URL not found']


def test_local_address_networks_are_used_one_request_at_a_time(monkeypatch):
    """A network among the local addresses is walked as requests are made, never expanded ahead of use."""
    import ipaddress
    from demiflow.collect.native_search import worker_network as wn
    taken, hosts = [], ipaddress._BaseNetwork.hosts

    def counted(self):
        for host in hosts(self):
            taken.append(str(host))
            yield host

    monkeypatch.setattr(ipaddress._BaseNetwork, 'hosts', counted)
    network = wn.Network(wn.network_options({}, {'local_addresses': ['192.0.2.0/24', '198.51.100.7']}))
    assert taken == []  # nothing before the first request
    assert [next(network._addresses) for _ in range(3)] == ['192.0.2.1', '192.0.2.2', '192.0.2.3'] == taken
    # an IPv6 /64 is declared and walked like any other network
    six = wn._address_rotation(wn._local_addresses('2001:db8::/64'))
    assert [next(six), next(six)] == ['2001:db8::1', '2001:db8::2']
    # declared order, a network's hosts one by one, round after round
    small = wn._address_rotation(wn._local_addresses(['192.0.2.0/30', '198.51.100.7']))
    assert [next(small) for _ in range(7)] == ['192.0.2.1', '192.0.2.2', '198.51.100.7'] * 2 + ['192.0.2.1']
    assert next(wn._address_rotation(wn._local_addresses(None))) is None
    with pytest.raises(ValueError):
        wn._local_addresses('not an address')


def test_a_network_keeps_a_bounded_number_of_http_clients(monkeypatch):
    """Clients are reused per local address; beyond the limit the least recently used one is closed."""
    from demiflow.collect.native_search import worker_network as wn

    class Client:
        closed = False

        def close(self):
            self.closed = True

    made = []
    monkeypatch.setattr(wn.Network, '_new_client', lambda self, *args: made.append(Client()) or made[-1])
    few = wn.Network(wn.network_options({}, {'local_addresses': ['192.0.2.1', '192.0.2.2']}))
    clients = [few.client() for _ in range(6)]
    assert len(made) == 2 and clients[0] is clients[2] is clients[4] and clients[1] is clients[3] is clients[5]
    made.clear()
    many = wn.Network(wn.network_options({}, {'local_addresses': '192.0.2.0/24'}))
    for _ in range(wn.CLIENT_LIMIT + 10):
        many.client()
    assert len(many._clients) == wn.CLIENT_LIMIT
    assert [client.closed for client in made] == [True] * 10 + [False] * wn.CLIENT_LIMIT


def test_sqlite_source_reads_rows_up_to_the_result_budget(tmp_path):
    """One row more than the session allows is read to see the overflow, whatever the page size."""
    import sqlite3
    database = tmp_path / 'rows.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text, n int)')
        db.executemany('insert into docs values (?, ?)', [(f'row {i}', i) for i in range(24)])
    source = {'engine': 'sqlite', 'database': str(database), 'query_str': 'select label, n from docs order by n'}
    result = probe('''
 values={}
 for name in ('page','unlimited'):
  engine=loaded[name]
  made=[]
  engine.key_value=lambda row,made=made,build=engine.key_value: made.append(1) or build(row)
  spec={'language':'en','pageno':1,'safesearch':0,'time_range':None,'engine_data':{}}
  message={'engine':name,'query':'row','parameters':spec}
  tight=w.run_search(message,loaded,errors,set(),{**settings,'max_results':3})
  taken=len(made); made.clear()
  roomy=w.run_search(message,loaded,errors,set(),settings)
  values[name]=[tight['status'],taken,roomy['status'],len(roomy['results']),len(made)]
 w.send(values)
''', engines=({'name': 'page', **source, 'limit': 2000}, {'name': 'unlimited', **source, 'limit': -1}))
    # a negative limit means no page size to SQLite; the session budget still bounds what is read
    assert result == {'page': ['response_too_large', 4, 'ok', 24, 24],
                      'unlimited': ['response_too_large', 4, 'ok', 24, 24]}


def test_options_an_adapter_no_longer_has_are_rejected_when_declared():
    """A declaration that sets a withdrawn option fails with its name instead of being accepted without effect."""
    from demiflow.collect.native_search.catalog import WITHDRAWN_OPTIONS, entries
    from demiflow.collect.native_search.config import SearchConfig
    source_of = {}
    for name, entry in entries().items():
        source_of.setdefault(entry['engine'], name)
    for adapter, options in WITHDRAWN_OPTIONS.items():
        for option in options:
            message = f'{source_of[adapter]} sets options the {adapter} adapter no longer has: {option}'
            with pytest.raises(ValueError, match=re.escape(message)):
                SearchConfig(engines=({'name': source_of[adapter], option: {}},)).source_configs()
    # the adapter under another source name; a name that never had an effect is still accepted
    with pytest.raises(ValueError, match='results_xpath'):
        SearchConfig(engines=({'name': 'other', 'engine': 'yandex', 'search_type': 'web',
                               'results_xpath': '//li'},)).source_configs()
    assert SearchConfig(engines=({'name': 'wikipedia', 'lang_map': {}},)).source_configs()[0]['lang_map'] == {}


@pytest.mark.parametrize('first, second, same', [
    ('http://localhost:8080/a', 'http://LOCALHOST:8080/b?x=1', True),
    ('http://example.org/a', 'http://example.org:80/b', True),
    ('https://example.org:443/a', 'https://EXAMPLE.org/b', True),
    ('http://example.org:8080/a', 'http://Example.org:8080/a', True),
    ('http://example.org/a', 'https://example.org/a', False),  # an upgrade to https is another origin
    ('http://example.org:8080/a', 'http://example.org:8081/a', False),
    ('http://example.org/a', 'http://example.org:8080/a', False),
    ('http://example.org/a', 'http://example.org:0/a', False),  # port 0 is a port of its own, not the default
    ('http://example.org/a', 'http://other.example.org/a', False),
    ('http://example.org/a', 'http://example.org.evil.test/a', False),
    ('http://example.org/a', 'http://user:secret@example.org/a', False),  # credentials of its own
    ('http://example.org/a', 'http://user@example.org/a', False),
    ('http://example.org/a', 'http://example.org:abc/a', False),  # a port that is no number
])
def test_same_origin_is_scheme_host_and_effective_port(first, second, same):
    from demiflow.collect.native_search import worker_network as wn
    assert wn.same_origin(urlsplit(first), urlsplit(second)) is same


def test_results_are_read_up_to_the_count_and_byte_limits(monkeypatch):
    """Records are taken one at a time; the first past a limit stops the source, which is then closed."""
    import json as jsonlib
    import sys
    from demiflow.collect.native_search import worker, worker_network as wn
    monkeypatch.setattr(wn._State, 'config', {'max_bytes': 100})  # a reply may have 800 bytes
    assert wn.output_limit() == 800
    produced, closed = [], []

    def produce(count, size):
        try:
            for index in range(count):
                produced.append(index)
                yield {'url': f'https://example.org/{index}', 'title': 'x' * size}
        finally:
            closed.append(True)

    def run(count, size, limit):
        produced.clear(), closed.clear()
        try:
            return worker.within_budget(produce(count, size), limit)
        finally:
            assert closed == [True]

    assert len(run(3, 10, 5)) == 3  # within both limits
    assert len(worker.within_budget([{'url': 'https://example.org/', 'title': 'list'}], 5)) == 1
    with pytest.raises(wn.ResponseTooLarge):
        run(10, 10, 3)  # the count limit
    assert produced == [0, 1, 2, 3]  # one past the limit, no further
    one = worker.pack({'url': 'https://example.org/0', 'title': 'x' * 100})
    size = len(jsonlib.dumps(one, ensure_ascii=False).encode())
    with pytest.raises(wn.ResponseTooLarge):
        run(50, 100, 1000)  # the byte limit
    assert len(produced) == 800 // size + 1
    for limit in (sys.maxsize, 10 ** 30):  # no count a list could reach is a limit
        assert len(run(2, 10, limit)) == 2


def test_sqlite_source_reads_no_more_bytes_than_a_reply_may_have(tmp_path):
    """A wide value is refused before it is read, a page over the byte limit stops early, a normal page passes."""
    import sqlite3
    database = tmp_path / 'bytes.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text, body text)')
        db.execute("insert into docs values ('wide', ?)", ('w' * 24576,))
        db.executemany('insert into docs values (?, ?)', [(f'row {i:02d}', 'g' * 1024) for i in range(24)])
        db.execute("insert into docs values ('small', 'tiny')")

    def source(name, pattern):
        return {'name': name, 'engine': 'sqlite', 'database': str(database), 'limit': 100,
                'query_str': f"select label, body from docs where label like '{pattern}' order by label"}

    result = probe('''
 settings['max_bytes']=1024; settings['max_results']=1000  # a reply may have 8 KiB
 spec={'language':'en','pageno':1,'safesearch':0,'time_range':None,'engine_data':{}}
 values={}
 for name in ('wide','group','small'):
  engine=loaded[name]
  made=[]
  engine.key_value=lambda row,made=made,build=engine.key_value: made.append(len(row['body'])) or build(row)
  reply=w.run_search({'engine':name,'query':'x','parameters':spec},loaded,errors,set(),settings)
  values[name]=[reply['status'],reply.get('reason'),len(made),sum(made),len(reply['results'])]
 one=w.pack(engine.key_value({'label':'row 00','body':'g'*1024}))
 values['row_bytes']=len(json.dumps(one,ensure_ascii=False).encode())
 w.send(values)
''', engines=(source('wide', 'wide'), source('group', 'row %'), source('small', 'small')))
    rows = 8192 // result['row_bytes'] + 1  # the row that crosses the limit is the last one read
    assert result['wide'] == ['response_too_large', 'ResponseTooLarge', 0, 0, 0]  # refused before it was read
    assert result['group'] == ['response_too_large', 'ResponseTooLarge', rows, rows * 1024, 0]
    assert result['small'] == ['ok', '', 1, 4, 1]
    assert rows < 24


def test_max_results_may_be_any_integer(tmp_path):
    import sqlite3
    import sys
    database = tmp_path / 'one.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text)')
        db.execute("insert into docs values ('one')")
    result = probe(f'''
 spec={{'language':'en','pageno':1,'safesearch':0,'time_range':None,'engine_data':{{}}}}
 values=[]
 for limit in ({sys.maxsize}, {10 ** 30}):
  settings['max_results']=limit
  reply=w.run_search({{'engine':'one','query':'x','parameters':spec}},loaded,errors,set(),settings)
  values.append([reply['status'],len(reply['results'])])
 w.send(values)
''', engines=({'name': 'one', 'engine': 'sqlite', 'database': str(database), 'query_str': 'select label from docs'},))
    assert result == [['ok', 1], ['ok', 1]]


# Counts what the SQLite source reads into Python: the bytes of every text value as it is made (``made``) and the
# rows a cursor hands over (``handed``); the database's own encoding name, which every search reads, is left out.
COUNTING = '''
 import sqlite3
 made=[]; handed=[]; reading=['']
 def text(raw):
  if not reading[0].startswith('PRAGMA'): made.append(len(raw))
  return raw.decode()
 class Rows(sqlite3.Cursor):
  def execute(self,sql,*args):
   reading[0]=sql
   return super().execute(sql,*args)
  def __next__(self):
   row=super().__next__()
   handed.append(row)
   return row
 class Counted(sqlite3.Connection):
  def __init__(self,*args,**kwargs):
   super().__init__(*args,**kwargs)
   self.text_factory=text
  def cursor(self,factory=Rows): return super().cursor(factory)
  def execute(self,*args): return self.cursor().execute(*args)
 connect=sqlite3.connect
 sqlite3.connect=lambda *args,**kwargs: connect(*args,**{**kwargs,'factory':Counted})
 spec={'language':'en','pageno':1,'safesearch':0,'time_range':None,'engine_data':{}}
 def run(name,query='x'):
  made.clear(); handed.clear()
  reply=w.run_search({'engine':name,'query':query,'parameters':spec},loaded,errors,set(),settings)
  blobs=sum(len(value) for row in handed for value in row if isinstance(value,bytes))
  return [reply['status'],reply.get('reason'),sum(made),blobs,len(reply['results'])]
'''


def test_sqlite_source_hands_over_no_more_than_a_reply_however_wide_the_row(tmp_path):
    """SQLite adds up a row's text and blob bytes itself: what would pass the reply limit never enters Python."""
    import sqlite3
    database = tmp_path / 'wide.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text, body text, raw blob)')
        db.execute("insert into docs values ('wide', ?, ?)", ('b' * 5000, b'\x01' * 5000))
        db.executemany('insert into docs values (?, ?, ?)',
                       [(f'row {i}', 'g' * 1000, b'\x02' * 1000) for i in range(8)])
        db.execute("insert into docs values ('edge', ?, x'')", ('e' * 8192,))
        db.execute("insert into docs values ('over', ?, x'')", ('o' * 8193,))
        db.execute("insert into docs values ('many', ?, x'')", ('m' * 300,))
        db.execute("insert into docs values ('few', ?, x'')", ('f' * 100,))

    def source(name, query):
        return {'name': name, 'engine': 'sqlite', 'database': str(database), 'limit': 100, 'query_str': query}

    def columns(column, count):
        return ', '.join(f'{column} as c{i}' for i in range(count))

    result = probe(COUNTING + '''
 settings['max_bytes']=1024; settings['max_results']=1000  # a reply may have 8 KiB
 values={name: run(name) for name in loaded}
 reply=w.run_search({'engine':'fortyfit','query':'x','parameters':spec},loaded,errors,set(),settings)
 values['fields']=len(w.unpack(reply['results'][0])['kvmap'])
 w.send(values)
''', engines=(
        source('text', f"select {columns('body', 8)} from docs where label = 'wide'"),  # 40,000 bytes in one row
        source('blob', f"select {columns('raw', 8)} from docs where label = 'wide'"),
        source('rows', f"select {columns('body', 3)} from docs where label like 'row %' order by label"),
        source('blobrows', f"select {columns('raw', 3)} from docs where label like 'row %' order by label"),
        source('edge', "select body from docs where label = 'edge'"),
        source('over', "select body from docs where label = 'over'"),
        source('forty', f"select {columns('body', 40)} from docs where label = 'many'"),  # 12,000 bytes in 40 columns
        source('fortyfit', f"select {columns('body', 40)} from docs where label = 'few'"),
        source('fits', "select label, body from docs where label = 'wide'")))
    too_large = ['response_too_large', 'ResponseTooLarge']
    assert result['text'] == [*too_large, 0, 0, 0] and result['blob'] == [*too_large, 0, 0, 0]
    # rows of 3,000 bytes: two fit in 8,192, the third is not handed over
    assert result['rows'] == [*too_large, 6000, 0, 0]
    assert result['blobrows'][:2] == too_large and result['blobrows'][3] <= 6000
    assert result['edge'] == [*too_large, 8192, 0, 0]  # exactly a reply: handed over, too large once encoded
    assert result['over'] == [*too_large, 0, 0, 0]  # one byte more: not handed over
    assert result['forty'] == [*too_large, 0, 0, 0]
    assert result['fortyfit'] == ['ok', '', 4000, 0, 1] and result['fields'] == 40
    assert result['fits'] == ['ok', '', 5004, 0, 1]


def test_sqlite_reply_limit_does_not_bound_what_a_query_reads(tmp_path):
    """Sort keys, measured values, the schema and the search text may be longer than a reply; so may UTF-16 text."""
    import sqlite3
    database, wide = tmp_path / 'long.sqlite', tmp_path / 'utf16.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (id integer primary key, url text, title text, body text)')
        db.executemany('insert into docs values (?, ?, ?, ?)', [
            (1, 'https://example.org/a', 'oak a', 'k' * 12000), (2, 'https://example.org/b', 'oak b', 'j' * 12000)])
        db.execute("create view unrelated as select '%s' as x" % ('v' * 12000))
    with sqlite3.connect(wide) as db:
        db.execute("pragma encoding = 'UTF-16'")
        db.execute('create table docs (title text, body text)')
        db.executemany('insert into docs values (?, ?)', [
            ('fits', 'u' * 6000), ('over', 'u' * 9000), ('wide', '橡' * 3000)])

    def source(name, query, path=database):
        return {'name': name, 'engine': 'sqlite', 'database': str(path), 'limit': 100, 'query_str': query}

    result = probe(COUNTING + '''
 settings['max_bytes']=1024; settings['max_results']=1000  # a reply may have 8 KiB
 values={name: run(name) for name in loaded if name!='searched'}
 values['searched']=run('searched','q'*9000)
 w.send(values)
''', engines=(
        source('schema', 'select url, title from docs'),  # the view of 12,000 characters is none of its business
        source('sorted', 'select id from docs order by body'),
        source('measured', 'select length(body) as n from docs'),
        source('head', 'select substr(body, 1, 10) as head from docs'),
        source('searched', 'select url, title from docs where title = :query or id = 1'),
        source('fits', "select title, body from docs where title = 'fits'", wide),
        source('over', "select title, body from docs where title = 'over'", wide),
        source('wide', "select body from docs where title = 'wide'", wide)))
    assert result['schema'] == ['ok', '', 52, 0, 2]
    assert result['sorted'] == ['ok', '', 0, 0, 2] and result['measured'] == ['ok', '', 0, 0, 2]
    assert result['head'] == ['ok', '', 20, 0, 2]
    assert result['searched'] == ['ok', '', 26, 0, 1]
    # UTF-16 text counts at half its stored size: 6,000 characters fit, 9,000 do not and are not handed over
    assert result['fits'] == ['ok', '', 6004, 0, 1]
    assert result['over'] == ['response_too_large', 'ResponseTooLarge', 0, 0, 0]
    # 3,000 CJK characters are 6,000 bytes there and 9,000 in UTF-8: read (within three times a reply), then too large
    assert result['wide'] == ['response_too_large', 'ResponseTooLarge', 9000, 0, 0]


def test_sqlite_value_limit_is_separate_from_the_reply_limit(tmp_path):
    """What SQLite may handle while it runs the query has a limit of its own and a status of its own."""
    import sqlite3
    from demiflow.collect.native_search.engines import sqlite as engine
    assert engine.VALUE_LIMIT == 32 * 1024 * 1024 and engine.ValueTooLarge.status == 'resource_limit'
    database = tmp_path / 'values.sqlite'
    with sqlite3.connect(database) as db:
        db.execute('create table docs (label text, body text)')
        db.executemany('insert into docs values (?, ?)', [
            ('small', 's' * 6000), ('medium', 'm' * 12000), ('large', 'l' * 20000)])

    def source(name, query):
        return {'name': name, 'engine': 'sqlite', 'database': str(database), 'limit': 100, 'query_str': query}

    result = probe(COUNTING + '''
 settings['max_bytes']=1024; settings['max_results']=1000  # a reply may have 8 KiB
 for module in loaded.values(): module.VALUE_LIMIT=16384
 values={name: run(name) for name in loaded}
 for module in loaded.values(): module.VALUE_LIMIT=1  # never under one reply, nor under the search text
 values['floor']=[run('returnedsmall'), run('sortedsmall','q'*9000)]
 w.send(values)
''', engines=(
        source('sortedmedium', "select label from docs where label != 'large' order by body"),
        source('sortedlarge', 'select label from docs order by body'),
        source('measuredlarge', "select length(body) as n from docs where label = 'large'"),
        source('returnedlarge', "select body from docs where label = 'large'"),
        source('returnedmedium', "select body from docs where label = 'medium'"),
        source('returnedsmall', "select body from docs where label = 'small'"),
        source('sortedsmall', "select label from docs where label = 'small' or label = :query order by body")))
    assert result['sortedmedium'] == ['ok', '', 11, 0, 2]  # a sort key of 12,000 bytes, more than a reply
    over = ['resource_limit', 'ValueTooLarge', 0, 0, 0]  # 20,000 bytes: more than SQLite may handle here
    assert result['sortedlarge'] == over and result['measuredlarge'] == over and result['returnedlarge'] == over
    # inside that limit, a value longer than a reply is still not handed over
    assert result['returnedmedium'] == ['response_too_large', 'ResponseTooLarge', 0, 0, 0]
    assert result['returnedsmall'] == ['ok', '', 6000, 0, 1]
    assert result['floor'] == [['ok', '', 6000, 0, 1], ['ok', '', 5, 0, 1]]


def test_what_a_source_returns_is_closed_once(monkeypatch):
    """The object an adapter returned is closed however the search ends, and so is the iterator made from it."""
    from demiflow.collect.native_search import worker, worker_network as wn
    monkeypatch.setattr(wn._State, 'config', {'max_bytes': 100})  # a reply may have 800 bytes
    rows = [{'url': f'https://example.org/{index}', 'title': 'x' * 10} for index in range(3)]
    closed = []

    class Owner:
        """Owns something to release; iterating it goes through another object."""

        def __init__(self, iterate):
            self.iterate = iterate

        def __iter__(self):
            return self.iterate()

        def close(self):
            closed.append('owner')

    class Reader:
        """Its own iterator, with a ``close`` of its own."""

        def __init__(self):
            self.rows = iter(rows)

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.rows)

        def close(self):
            closed.append('reader')

    def produced():
        try:
            yield from rows
        finally:
            closed.append('generator')

    def failing():
        yield rows[0]
        raise RuntimeError('iteration failed')

    def run(results, limit=5, error=None):
        closed.clear()
        if error is None:
            assert len(worker.within_budget(results, limit)) == 3
        else:
            with pytest.raises(error):
                worker.within_budget(results, limit)
        return list(closed)

    assert run(Owner(lambda: iter(rows))) == ['owner']  # the list iterator has nothing to close
    assert run(Owner(lambda: iter(rows)), limit=2, error=wn.ResponseTooLarge) == ['owner']
    assert run(Owner(failing), error=RuntimeError) == ['owner']
    assert run(Owner(produced)) == ['generator', 'owner']  # the iterator first, then what it came from
    assert run(Owner(produced), limit=2, error=wn.ResponseTooLarge) == ['generator', 'owner']
    assert run(Owner(lambda: None), error=TypeError) == ['owner']  # it cannot even be iterated
    assert run(Reader()) == ['reader'] and run(Reader(), limit=2, error=wn.ResponseTooLarge) == ['reader']
    assert run(produced()) == ['generator'] and run(produced(), limit=2, error=wn.ResponseTooLarge) == ['generator']
    assert run(list(rows)) == []
    wide = [{'url': 'https://example.org/wide', 'title': 'x' * 1000}]
    assert run(Owner(lambda: iter(wide)), error=wn.ResponseTooLarge) == ['owner']  # past the bytes of a reply
