"""Source metadata must describe the selected file, with finite detail fan-out."""
from test_native_search_adapters import probe


def test_variants_avoid_oversized_tiffs_and_do_not_use_object_dimensions():
    result = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.cleveland_images as c
 import demiflow.services.engines.gbif_images as g
 c.page_size=1;c.min_short_side=1024;c.max_pixels=20000000;c.max_image_bytes=20971520
 item={'title':'vase','url':'https://clevelandart.org/art/1','dimensions':{'width':10,'height':20},'images':{
   'full':{'url':'https://cdn.example/full.tif','width':9000,'height':8000},
   'web':{'url':'https://cdn.example/web.jpg','width':'600','height':'900'},
   'print':{'url':'https://cdn.example/print.jpg','width':'2200','height':'3400','filesize':'300000'}},
   'alternate_images':[{'print':{'url':'https://cdn.example/alt.jpg','width':2000,'height':3000}}]*100}
 rows=c.response(SimpleNamespace(json=lambda:{'data':[item]*100}))
 gr=g.response(SimpleNamespace(json=lambda:{'results':[{'key':1,'license':'record-license','media':[
  {'type':'Sound','identifier':'https://cdn.example/a.mp3'},
  {'type':'StillImage','identifier':'https://publisher.example/original.jpg','license':'image-license'}]}]}))
 w.send({'cleveland':rows,'gbif':gr})
''')
    assert len(result['cleveland']) == 3
    assert result['cleveland'][0]['img_src'].endswith('/print.jpg')
    assert result['cleveland'][0]['resolution'] == '2200 x 3400'
    assert len(result['gbif']) == 1
    assert result['gbif'][0]['license'] == 'image-license'
    assert 'resolution' not in result['gbif'][0]


def test_artic_uses_native_dimensions_only_for_unscaled_files_and_never_upscales():
    result = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.artic_images as a
 a.page_size=2;calls=[]
 def get(url):
  calls.append(url)
  return SimpleNamespace(json=lambda:{'data':{'width':800 if '/small?' in url else 2400,'height':3000}})
 a.get=get
 data={'config':{'iiif_url':'https://www.artic.edu/iiif/2'},'data':[
  {'id':1,'image_id':'large','is_public_domain':True},
  {'id':2,'image_id':'small','is_public_domain':True}]*100}
 w.send({'rows':a.response(SimpleNamespace(json=lambda:data)),'calls':calls})
''')
    assert len(result['calls']) == 2
    assert all(x.startswith('https://api.artic.edu/api/v1/images/') for x in result['calls'])
    assert '/1686,/' in result['rows'][0]['img_src']
    assert 'resolution' not in result['rows'][0]
    assert '/800,/' in result['rows'][1]['img_src']
    assert result['rows'][1]['resolution'] == '800 x 3000'


def test_wellcome_open_image_and_loc_file_metadata_are_bounded():
    result = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.wellcome_images as wc
 import demiflow.services.engines.loc_images as lc
 wc.page_size=2;calls=[]
 def wget(url):
  calls.append(url)
  return SimpleNamespace(json=lambda:{'@id':'https://iiif.wellcomecollection.org/image/A','protocol':'http://iiif.io/api/image','width':2000,'height':3000})
 wc.get=wget
 location={'url':'https://iiif.wellcomecollection.org/image/A/info.json','locationType':{'id':'iiif-image'}}
 data={'results':[{'source':{'id':'a'},'locations':[location]},
  {'source':{'id':'b'},'locations':[{**location,'accessConditions':[{'status':{'id':'restricted'}}]}]}]}
 wr=wc.response(SimpleNamespace(json=lambda:data))
 lc.page_size=1;lc.min_short_side=1024
 def lget(url):
  calls.append(url)
  return SimpleNamespace(json=lambda:{'resources':[{'files':[[
   {'url':'https://tile.loc.gov/preview.jpg','mimetype':'image/jpeg','width':640,'height':400},
   {'url':'https://tile.loc.gov/download?id=original','mimetype':'image/jpeg','width':3000,'height':2000}]]}]})
 lc.get=lget
 lr=lc.response(SimpleNamespace(json=lambda:{'results':[{'id':'https://www.loc.gov/item/1/'}]*100}))
 w.send({'wc':wr,'lc':lr,'calls':calls})
''')
    assert len(result['calls']) == 2
    assert result['wc'][0]['resolution'] == '2000 x 3000'
    assert result['wc'][0]['img_src'].endswith('/full/full/0/default.jpg')
    assert len(result['lc']) == 1
    assert result['lc'][0]['resolution'] == '3000 x 2000'
    assert result['lc'][0]['img_src'] == 'https://tile.loc.gov/download?id=original'


def test_variant_mime_can_identify_extensionless_images_but_not_nonimages():
    result = probe('''
 from demiflow.services.engines.image_source_utils import raster_variant
 cases=[{'url':'https://cdn.example/download?id=1','mimetype':'image/jpeg'},
        {'url':'https://cdn.example/download?id=2','mimetype':'image/png'},
        {'url':'https://cdn.example/preview.jpg','mimetype':'text/html'},
        {'url':'https://cdn.example/download'},
        {'url':'https://cdn.example/normal.JPG?token=1'}]
 w.send([raster_variant([v]) is not None for v in cases])
''')
    assert result == [True, True, False, False, True]


def test_missing_result_arrays_are_failures_not_empty_searches():
    result = probe('''
 from types import SimpleNamespace
 from importlib import import_module
 out={}
 for name,key in [('gbif','results'),('cleveland','data'),('artic','data'),('wellcome','results'),('loc','results')]:
  module=import_module('demiflow.services.engines.'+name+'_images')
  base={'config':{'iiif_url':'https://www.artic.edu/iiif/2'}} if name=='artic' else {}
  module.get=lambda *a: (_ for _ in ()).throw(AssertionError('no detail request expected'))
  states=[]
  for body in [{**base,'error':'upstream unavailable'},{**base,key:None},{**base,key:{}}]:
   try:module.response(SimpleNamespace(json=lambda:body))
   except ValueError:states.append('failed')
   else:states.append('accepted')
  states.append(module.response(SimpleNamespace(json=lambda:{**base,key:[]})))
  out[name]=states
 import demiflow.services.engines.flickr_images as f
 try:f.response(SimpleNamespace(text='<html>challenge or layout changed</html>'))
 except ValueError:out['flickr']='failed'
 else:out['flickr']='accepted'
 w.send(out)
''')
    assert result.pop('flickr') == 'failed'
    assert all(states == ['failed', 'failed', 'failed', []] for states in result.values())


def test_fws_original_links_and_ars_caption_links_do_not_rewrite_thumbnails():
    result = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.fws_images as f
 import demiflow.services.engines.usda_images as u
 f.page_size=1;f.min_short_side=1024
 calls=[]
 def get(url):
  calls.append(url)
  return SimpleNamespace(text='<a href="/sites/default/files/bird.jpg">Original (3000 x 2000) 2M</a><a href="/sites/default/files/thumb.jpg">Medium (650 x 433)</a>')
 f.get=get
 snippet='<div class="teaser media-image"><section class="teaser-title"><a href="/media/bird">Bird</a></section><img src="/thumb.jpg" width="480" height="480"></div>'
 fr=f.response(SimpleNamespace(json=lambda:{'list':[snippet]*100,'_meta':{'total':100}}))
 ur=u.caption_image('<title>Fruit</title><a href="/full.jpg"><img alt="Download a high-resolution (300dpi) digital image"></a><img width="640" height="400" src="/preview.jpg">','https://www.ars.usda.gov/oc/images/photos/fruit')
 try:u.response(SimpleNamespace(status_code=202,text=''))
 except Exception as exc:failure=type(exc).__name__
 else:failure=None
 upscaled=f.media_image('<a href="/original.jpg">Original (800 x 800)</a><a href="/large.jpg">Large (1300 x 1300)</a>',
   'https://www.fws.gov/media/small','small')
 w.send({'fws':fr,'ars':ur,'calls':calls,'failure':failure,'upscaled':upscaled})
''')
    assert len(result['calls']) == 1
    assert result['fws'][0]['resolution'] == '3000 x 2000'
    assert result['ars']['img_src'] == 'https://www.ars.usda.gov/full.jpg'
    assert 'resolution' not in result['ars']
    assert result['failure'] == 'SearxEngineAccessDeniedException'
    assert result['upscaled']['img_src'].endswith('/original.jpg')
    assert result['upscaled']['resolution'] == '800 x 800'
