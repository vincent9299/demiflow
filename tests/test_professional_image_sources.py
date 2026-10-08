"""Offline boundary checks for the public image source adapters."""
from test_native_search_adapters import probe


def test_source_errors_are_not_recorded_as_successful_empty_searches():
    value = probe('''
 from types import SimpleNamespace
 from importlib import import_module
 states={}
 for name,empty in [('openverse',{'results':[]}),('unsplash',{'results':[]}),
     ('inaturalist',{'results':[]}),('nasa',{'collection':{'items':[]}}),
     ('metmuseum',{'total':0,'objectIDs':None})]:
  module=import_module('demiflow.services.engines.'+name+'_images')
  try:module.response(SimpleNamespace(json=lambda:{'error':'temporary failure'},search_params={'pageno':1}))
  except ValueError:state='failed'
  else:state='accepted'
  states[name]=[state,module.response(SimpleNamespace(json=lambda:empty,search_params={'pageno':1}))]
 import demiflow.services.engines.flickr_images as f
 states['flickr_empty']=f.response(SimpleNamespace(text='modelExport: '+json.dumps({'main':{},'legend':[]})+',\\n'))
 w.send(states)
''')
    assert value.pop('flickr_empty') == []
    assert all(state == ['failed', []] for state in value.values())


def test_professional_api_sources_keep_explicit_urls_and_bound_detail_reads():
    value = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.nasa_images as nasa
 import demiflow.services.engines.metmuseum_images as met
 import demiflow.services.engines.inaturalist_images as nat
 calls=[]
 def response(data):return SimpleNamespace(json=lambda:data,search_params={'pageno':1})
 nasa.get=lambda url: (_ for _ in ()).throw(AssertionError('canonical needs no manifest'))
 nr=nasa.response(response({'collection':{'items':[{'data':[{'media_type':'image','nasa_id':'moon'}],
   'links':[{'render':'image','rel':'preview','href':'https://nasa.gov/small.jpg','width':640,'height':400},
            {'render':'image','rel':'canonical','href':'https://nasa.gov/original.jpg','width':2000,'height':1400}]}]}}))
 met.max_objects=2
 def detail(url):
  calls.append(url)
  return response({'isPublicDomain':True,'primaryImage':'https://metmuseum.org/original.jpg',
                   'primaryImageSmall':'https://metmuseum.org/small.jpg','additionalImages':['https://metmuseum.org/a.jpg']*10})
 met.get=detail
 mr=met.response(response({'objectIDs':list(range(1,101))}))
 nat.min_short_side=1024
 ncalls=[]
 def ndetail(url):
  ncalls.append(url)
  return response({'observation_photos':[{'photo':{'id':2,'large_url':'https://inat.org/large.jpg','medium_url':'https://inat.org/medium.jpg'}}]})
 nat.get=ndetail
 ir=nat.response(response({'results':[
  {'id':1,'photos':[{'id':1,'original_dimensions':{'width':1023,'height':4000}}]},
  {'id':2,'photos':[{'id':2,'original_dimensions':{'width':3000,'height':4000}}]}]}))
 w.send({'nasa':nr,'met':mr,'met_calls':len(calls),'inat':ir,'inat_calls':len(ncalls)})
''')
    assert value['nasa'][0]['img_src'].endswith('/original.jpg')
    assert value['nasa'][0]['resolution'] == '2000 x 1400'
    assert value['met_calls'] == 2 and len(value['met']) == 6
    assert value['inat_calls'] == 1 and len(value['inat']) == 1
    assert value['inat'][0]['img_src'].endswith('/large.jpg')
    assert 'resolution' not in value['inat'][0]


def test_flickr_largest_actual_size_and_commons_query():
    value = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.flickr_images as f
 f.max_details=0; f.min_short_side=1024; f.commons_only=True
 params={'pageno':1};f.request('tree',params)
 photo={'id':'1','ownerNsid':'owner','title':'tree','sizes':{'data':{
   'l':{'data':{'url':'//live.staticflickr.com/exact_l.jpg','width':1024,'height':700}},
   'o':{'data':{'url':'//live.staticflickr.com/exact_original.png','width':4000,'height':3000}}}}}
 body='modelExport: '+json.dumps({'main':{'photo':photo},'legend':[['photo']]})+',\\n'
 results=f.response(SimpleNamespace(text=body))
 sizes,doc=f.size_options('<ol class="sizes-list"><li><a href="/photos/x/1/sizes/o/">Original</a><small>(4000 × 3000)</small></li></ol>','https://www.flickr.com/photos/x/1/sizes/')
 w.send({'url':params['url'],'results':results,'sizes':sizes})
''')
    assert 'is_commons=1' in value['url'] and 'width=1024' in value['url'] and 'height=1024' in value['url']
    assert value['results'][0]['img_src'] == 'https://live.staticflickr.com/exact_original.png'
    assert value['results'][0]['resolution'] == '4000 x 3000'
    assert value['sizes'][0][1:3] == [4000, 3000]


def test_source_resolution_never_uses_inaccessible_larger_flickr_size():
    value = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.flickr_images as f
 f.max_details=1;f.min_short_side=1024
 photo={'id':'1','ownerNsid':'owner','sizes':{'data':{'l':{'data':{'url':'https://cdn.example/1024.jpg','width':1024,'height':768}}}}}
 listing='<ol class="sizes-list"><li><a href="/photos/owner/1/sizes/o/">Original</a><small>(4000 × 3000)</small></li><li>Large<small>(1024 × 768)</small></li></ol><div id="allsizes-photo"><img src="https://cdn.example/1024.jpg"></div>'
 f.get=lambda url:SimpleNamespace(text=listing,url=url)
 body='modelExport: '+json.dumps({'main':{'p':photo},'legend':[['p']]})+',\\n'
 w.send(f.response(SimpleNamespace(text=body)))
''')
    assert value[0]['img_src'] == 'https://cdn.example/1024.jpg'
    assert value[0]['resolution'] == '1024 x 768'


def test_inaturalist_open_data_original_uses_only_documented_bucket_and_matching_id():
    value = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.inaturalist_images as nat
 nat.min_short_side=1024
 nat.get=lambda url: (_ for _ in ()).throw(AssertionError('public bucket needs no detail read'))
 payload={'results':[{'id':5,'photos':[{'id':12,'url':'https://inaturalist-open-data.s3.amazonaws.com/photos/12/square.jpg',
   'original_dimensions':{'width':1536,'height':2048}}]}]}
 w.send(nat.response(SimpleNamespace(json=lambda:payload)))
''')
    assert value[0]['img_src'] == 'https://inaturalist-open-data.s3.amazonaws.com/photos/12/original.jpg'
    assert value[0]['resolution'] == '1536 x 2048'
