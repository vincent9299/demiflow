"""Bounded provider fan-out and metadata survive native merge and Arrow."""
from test_native_search_adapters import probe


def test_iiif_sizes_do_not_upscale_or_label_resized_files_as_originals():
    from demiflow.services.engines.image_source_utils import iiif3_variant
    base = {'id': 'https://iiif.example/a', 'type': 'ImageService3',
            'protocol': 'http://iiif.io/api/image', 'width': 4160, 'height': 5916,
            'maxArea': 17550000, 'extraFeatures': ['sizeByConfinedWh']}
    limits = dict(min_short_side=1024, max_pixels=20000000)
    resized = iiif3_variant(base, **limits)
    assert '/!2400,2400/' in resized['url'] and resized['width'] is None
    small = iiif3_variant({**base, 'width': 800, 'height': 1200}, **limits)
    assert '/max/' in small['url'] and small['width'] == 800
    listed = iiif3_variant({**base, 'sizes': [{'width': 811, 'height': 1074},
        {'width': 1623, 'height': 2148}, {'width': 9999, 'height': 9999}]}, **limits)
    assert '/1623,2148/' in listed['url'] and listed['width'] == 1623
    tight = iiif3_variant({**base, 'maxArea': 2000000}, **limits)
    assert '/!1457,1457/' in tight['url']


def test_native_merge_arrow_preserves_exact_rendition_metadata():
    result = probe('''
 import pyarrow as pa
 from demiflow.collect.contracts import CANDIDATE
 from demiflow.collect.searxng import normalize_response
 from demiflow.collect.web import normalized_url
 packed=w.pack({'url':'https://example.org/item','img_src':'https://example.org/download',
  'resolution':'1623 x 2148','declared_file_bytes':'123456','mime_type':'IMAGE/JPEG; charset=binary'})
 merged=w.merge({'sources':[{'name':'google','weight':1}],
  'groups':[{'engine':'google','results':[packed]}]})
 candidates=normalize_response(merged,normalized_url)['candidates']
 w.send(pa.array(candidates,type=CANDIDATE).to_pylist())
''')
    assert result[0]['declared_file_bytes'] == 123456
    assert result[0]['mime_type'] == 'image/jpeg'
    assert (result[0]['declared_width'], result[0]['declared_height']) == (1623, 2148)


def test_rijks_and_getty_bound_detail_requests_and_skip_nonimage_bodies():
    result = probe('''
 from types import SimpleNamespace
 import demiflow.services.engines.rijksmuseum_images as r
 import demiflow.services.engines.getty_images as g
 calls=[]
 def get(url):
  calls.append(url)
  if 'info.json' in url:
   value={'id':url[:-10],'type':'ImageService3','protocol':'http://iiif.io/api/image',
    'width':4160,'height':5916,'maxArea':17550000,'extraFeatures':['sizeByConfinedWh']}
  elif '/100?' in url:value={'identified_by':[{'type':'Name','content':'Vase'}],'shows':[{'id':'https://id.rijksmuseum.nl/200','type':'VisualItem'}]}
  elif '/200?' in url:value={'digitally_shown_by':[{'id':'https://id.rijksmuseum.nl/300','type':'DigitalObject'}]}
  elif '/300?' in url:value={'access_point':[{'id':'https://iiif.micr.io/A/full/max/0/default.jpg'}]}
  else:value={'items':[{'items':[{'items':[{'body':{'type':'Video','id':'https://media.getty.edu/movie.mp4'}}]}]}]}
  return SimpleNamespace(json=lambda:value)
 r.get=g.get=get;r.page_size=g.page_size=1;r.min_short_side=1024;r.max_pixels=20000000
 rr=r.response(SimpleNamespace(json=lambda:{'orderedItems':[{'id':'https://id.rijksmuseum.nl/100'}]*100}))
 gr=g.response(SimpleNamespace(json=lambda:{'data':[{'manifest':{'url':'https://media.getty.edu/manifest/one'}}]*100}))
 w.send({'rijks':rr,'getty':gr,'calls':calls})
''')
    assert len(result['calls']) == 5 and len(result['rijks']) == 1
    assert 'resolution' not in result['rijks'][0] and result['getty'] == []
