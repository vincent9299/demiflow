"""Typed output/media boundaries. Private fixtures, no business or executor."""
from contextlib import contextmanager
import base64
import copy
import hashlib
import io
import json
import os
import sqlite3
import wave

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import outputs
from demiflow.workbench.analysis import media
from demiflow.workbench.analysis import output_reader as reader
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.database import _SCHEMA
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.forge.schema import ForgeError

PROJECT = 'w'+'0'*32
OBJECT = 'o'+'1'*32


def image_bytes(width=8, height=6):
    from PIL import Image
    output = io.BytesIO()
    with Image.new('RGB', (width, height), 'navy') as image:
        image.save(output, format='PNG')
    return output.getvalue()


def wav_bytes(seconds=.05):
    target = io.BytesIO()
    with wave.open(target, 'wb') as stream:
        stream.setnchannels(1); stream.setsampwidth(2); stream.setframerate(8000)
        stream.writeframes(b'\0\0'*int(8000*seconds))
    return target.getvalue()


def witness_and_raw(items=None):
    provenance = dict(project=PROJECT, notebook_id='book', document_revision=1, document_sha256='c'*64,
        cell_id='cell', source_sha256=hashlib.sha256(b'print(1)').hexdigest(), language='python',
        execution_id='execution', session_id='session', kernel_generation='generation', output_generation=1,
        source_snapshots={}, status='succeeded')
    value = dict(generation='generation', parent_msg_id='parent', status='succeeded', complete=True,
        outputs=items if items is not None else [dict(output_type='stream', name='stdout', text='1\n')],
        output_bytes=2, messages=2, ignored_messages=0, stdin_supported=False, active_widgets_supported=False,
        execution_id='execution', cell_id='cell', source_sha256=provenance['source_sha256'], language='python',
        session_id='session', final_status='succeeded', raw_fd_output_collected=False)
    raw = c.encoded(value, maximum=16*1024*1024, nodes=32768)
    witness = dict(artifact=dict(id=OBJECT, generation=1, digest='d'*64, sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)),
        provenance=provenance, sources={})
    return witness, raw


def registered_database(raw, witness):
    """Real SQLite, actual platform schema and read-only witness queries."""
    class DB:
        def __init__(self):
            self.connection = sqlite3.connect(':memory:'); self.connection.row_factory = sqlite3.Row
            self.connection.executescript(_SCHEMA)
        @contextmanager
        def locked(self):
            yield self.connection
    db = DB(); conn = db.connection
    seed = dict(project=PROJECT, object_id=OBJECT, generation=1, retention='ephemeral', execution_id='execution', parents=[], source_binding=None)
    payload = dict(format='document', mime=outputs.OUTPUT_MIME, bytes=len(raw), sha256=witness['artifact']['sha256'], complete=True)
    manifest = candidate_manifest(seed, payload, {'document.json': dict(bytes=len(raw), sha256=payload['sha256'])})
    digest = c.digest(manifest)
    source = dict(title='source', cells=[dict(id='cell', language='python', source='print(1)')])
    conn.execute('INSERT INTO documents VALUES (?,?,?,?)', (PROJECT,'notebook','book',1))
    conn.execute('INSERT INTO document_revisions VALUES (?,?,?,?,?,?,?,?)', (PROJECT,'notebook','book',1,c.digest(source),c.encoded(source),'owner','now'))
    conn.execute('INSERT INTO analysis_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', (PROJECT,'session','owner','book',1,'new-live-generation','idle',None,b'{}',b'{}','now','now',None))
    conn.execute('INSERT INTO analysis_executions(project,id,session_id,generation,cell_id,notebook_revision,source_sha256,language,state,output_id,status,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (PROJECT,'execution','session','generation','cell',1,witness['provenance']['source_sha256'],'python','finished',OBJECT,'succeeded','now','now'))
    conn.execute('INSERT INTO analysis_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (PROJECT,OBJECT,'p'+'2'*32,1,'execution','ready',c.encoded(seed),len(raw),4,digest,c.encoded(manifest),None,None))
    conn.commit()
    return Records(db), digest


def test_registered_witness_keeps_old_execution_generation_and_rejects_foreign_or_changed_binding():
    witness, raw = witness_and_raw(); records, digest = registered_database(raw, witness)
    checks=[]
    access=Access(PROJECT,'owner',lambda *args:checks.append(args) or True)
    actual=reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,expected_binding=digest)
    assert actual['provenance']['kernel_generation']=='generation'
    assert len(checks)>=2 and actual['provenance']['source_snapshots']=={}
    assert reader.project_document(raw,actual)['items'][0]['representations'][0]['value']=='1\n'
    for kwargs in ({'expected_binding':'f'*64},{'expected_output_id':'o'+'9'*32}):
        with pytest.raises(ForgeError):reader.registered_witness(records,access,'execution',**kwargs)
    with pytest.raises(ForgeError):reader.registered_witness(records,Access('w'+'9'*32,'owner',lambda *args:True),'execution')
    with pytest.raises(ForgeError):reader.registered_witness(records,Access(PROJECT,'owner',lambda *args:False),'execution')
    records.database.connection.close()


def test_output_provenance_mismatch_never_relabels_original_and_pages_are_bounded():
    items=[dict(output_type='stream',name='stdout',text=('宽'*18000)) for _ in range(20)]
    witness, raw=witness_and_raw(items)
    page=reader.project_document(raw,witness,limit=16)
    assert len(c.encoded(page)) <= reader.MAX_PAGE and page['next_offset']
    assert all(item['representations'][0]['truncated'] for item in page['items'])
    other=copy.deepcopy(witness);other['provenance']['kernel_generation']='different'
    with pytest.raises(ForgeError):reader.project_document(raw,other)
    other=copy.deepcopy(witness);other['provenance']['source_sha256']='a'*64
    with pytest.raises(ForgeError):reader.project_document(raw,other)
    with pytest.raises(ForgeError):reader.project_document(raw+b' ',witness)


def test_mime_alternatives_and_embedded_image_do_not_echo_base64_or_execute_unknown():
    data=image_bytes()
    bundle={'application/javascript':'globalThis.pwned=true','image/png':base64.b64encode(data).decode(),
            'text/plain':'a chart','application/json':{'null':None,'boolean':False,'zero':0},
            outputs.CASE_MIME: {'source_snapshot':'a'*64}}
    witness,raw=witness_and_raw([dict(output_type='display_data',data=bundle,metadata={})])
    page=reader.project_document(raw,witness)
    reps={item['mime']:item for item in page['items'][0]['representations']}
    ref=reps['image/png']['value']
    assert ref['width']==8 and ref['height']==6 and ref['output_index']==0
    assert not reps['application/javascript']['available'] and not reps[outputs.CASE_MIME]['available']
    assert reps['application/json']['value']=={'null':None,'boolean':False,'zero':0}
    assert base64.b64encode(data).decode() not in json.dumps(page)
    descriptor,actual=reader.embedded_media(raw,witness,output_index=0,mime='image/png',expected_sha256=ref['sha256'])
    assert descriptor==ref and actual==data
    with pytest.raises(ForgeError):reader.embedded_media(raw,witness,output_index=0,mime='image/png',expected_sha256='f'*64)
    bundle['image/png']='not base64!'
    witness,raw=witness_and_raw([dict(output_type='display_data',data=bundle,metadata={})])
    assert not next(rep for rep in reader.project_document(raw,witness)['items'][0]['representations'] if rep['mime']=='image/png')['available']


def test_fixed_output_path_integrity_and_no_symlink_following(tmp_path):
    witness,raw=witness_and_raw()
    path=tmp_path/'objects'/OBJECT/'payload';path.mkdir(parents=True)
    target=path/'document.json';target.write_bytes(raw);target.chmod(0o400)
    assert reader.open_document(tmp_path,witness)==raw
    target.chmod(0o600);target.write_bytes(b'X'*len(raw));target.chmod(0o400)
    with pytest.raises(ForgeError):reader.open_document(tmp_path,witness)
    target.unlink();(tmp_path/'other').write_bytes(raw);target.symlink_to(tmp_path/'other')
    with pytest.raises((ForgeError,OSError)):reader.open_document(tmp_path,witness)


@pytest.mark.parametrize('header,expected', [('bytes=0-0',(0,0,206)),('bytes=-10',(90,99,206)),('bytes=90-',(90,99,206)),('bytes=90-999',(90,99,206)),(None,(0,99,200))])
def test_range_semantics(header,expected):assert media.byte_range(header,100)==expected


@pytest.mark.parametrize('header',['bytes=0-1,4-8','bytes=2097152-','bytes=1-0','bytes=-0','bytes=0-1048576','bytes=+1-2','bytes=0-²','items=0-1'])
def test_ranges_reject_multi_oversized_bad_and_unsatisfiable(header):
    with pytest.raises(ForgeError):media.byte_range(header,2*media.MIB)


def test_native_open_range_is_served_as_bounded_declared_chunk():
    assert media.byte_range('bytes=0-',2*media.MIB)==(0,media.MIB-1,206)
    with pytest.raises(ForgeError):media.byte_range(None,2*media.MIB)


def test_real_image_and_wav_decoders_check_metadata_and_explicit_failures():
    image=media.probe_image(image_bytes(),artifact_id=OBJECT,mime='image/png')
    assert image['width']==8 and image['height']==6
    audio=media.probe_wav(wav_bytes(),artifact_id=OBJECT)
    assert audio['duration_s']==.05 and audio['codec']=='pcm_s16le'
    for raw,mime in [(b'broken','image/png'),(image_bytes(),'image/jpeg')]:
        with pytest.raises(ForgeError):media.probe_image(raw,artifact_id=OBJECT,mime=mime)
    with pytest.raises(ForgeError) as error:media.probe_image(image_bytes(2049,2049),artifact_id=OBJECT,mime='image/png')
    assert error.value.code=='resource_limit'
    with pytest.raises(ForgeError):media.probe_wav(wav_bytes()[:-5],artifact_id=OBJECT)
    with pytest.raises(ForgeError) as error:media.probe_av_fd(-1,artifact_id=OBJECT,mime='video/mp4',kind='video')
    assert error.value.code=='operation_rejected' and error.value.reason=='media_unavailable'


def test_authorized_media_rechecks_project_binding_bytes_and_revocation(tmp_path):
    raw=wav_bytes();path=tmp_path/'asset.wav';path.write_bytes(raw)
    reference=media.probe_wav(raw,artifact_id=OBJECT); binding='c'*64
    permissions=[True];calls=[]
    access=Access(PROJECT,'owner',lambda *args:permissions[0])
    def resolve(access,reference,expected,kind):
        calls.append((access.project,expected,kind))
        return media.MediaRecord(PROJECT,binding,reference,lambda:os.open(path,os.O_RDONLY|os.O_NOFOLLOW))
    service=media.AuthorizedMedia(resolve)
    assert service.inspect(access,reference,expected_binding=binding,kind='audio')['available']
    part=service.read(access,reference,expected_binding=binding,kind='audio',range_header='bytes=4-11')
    assert part['data']==raw[4:12] and part['status']==206 and part['headers']['Content-Range']==f'bytes 4-11/{len(raw)}'
    assert len(calls)==3 and service.budget.used==0
    permissions[0]=False
    with pytest.raises(ForgeError):service.read(access,reference,expected_binding=binding,kind='audio',range_header='bytes=4-11')
    permissions[0]=True
    with pytest.raises(ForgeError):service.inspect(access,reference,expected_binding='f'*64,kind='audio')
    with service.budget.admit(100):
        with pytest.raises(ForgeError) as error:service.read(access,reference,expected_binding=binding,kind='audio',range_header='bytes=0-10')
        assert error.value.code=='busy'
    assert service.budget.used==0


def test_null_and_media_metadata_are_not_guessed():
    for kind in c.FORMATS:assert outputs.field_preview(kind,None)['null'] is True
    assert outputs.field_preview('audio','https://bad.test/a.wav')['compatible'] is False
    with pytest.raises(ForgeError):media.verified_metadata(dict(artifact_id=OBJECT,sha256='a'*64,mime='video/mp4',bytes=100),'video')


def test_fixed_output_worker_returns_small_page_and_exact_original_chunk(tmp_path):
    from demiflow.workbench.analysis import output_worker
    witness,raw=witness_and_raw();folder=tmp_path/'objects'/OBJECT/'payload';folder.mkdir(parents=True)
    path=folder/'document.json';path.write_bytes(raw);path.chmod(0o400)
    config=dict(version=1,operation='output-page',witness=witness,offset=0,limit=8)
    result=output_worker.execute(config,state_root=tmp_path)
    assert result['kind']=='json' and result['value']['provenance']['execution_id']=='execution'
    config=dict(version=1,operation='output-original',witness=witness,range='bytes=0-31')
    result=output_worker.execute(config,state_root=tmp_path)
    assert result['kind']=='blob' and result['data']==raw[:32] and result['metadata']['attachment']
    with pytest.raises(ForgeError):output_worker.execute({**config,'path':'/etc/passwd'},state_root=tmp_path)


def test_existing_client_table_mime_and_case_definition_witness_remain_separate_from_saved_views():
    from demiflow.workbench.analysis.client import TableHandle
    from demiflow.workbench.analysis.client import CaseView
    source=c.source_snapshot(dict(kind='pinned_object',binding='b'*64,schema=[dict(name='text',type='utf8',nullable=True)],rows=1,case_key=dict(kind='row_id',field=None),retention='pinned'))
    table=dict(id='o'+'3'*32,generation=1,digest='b'*64,rows=1,schema=source['schema'],retention='pinned')
    descriptor=dict(version=1,source_snapshot=source['digest'],case_key=source['case_key'],fields=[dict(id='f1',field='text',label='Text',role='input',format='text')],grouping=[],filters=None,sort=[],layout='card',questions=[dict(id='q',title='Quality',type='text',required=False)])
    # The actual client now records each displayed handle in its current
    # cell. This renderer-shape fixture uses an explicit broker double and
    # verifies that the registration handshake remains mandatory.
    calls=[]
    class Client:
        def _call(self,operation,payload):
            calls.append((operation,payload))
            assert operation=='handle-info' and payload=={'handle':{key:table[key] for key in ('id','generation','digest')}}
            return table
    bundle={**TableHandle(Client(),table)._repr_mimebundle_(),**CaseView(descriptor)._repr_mimebundle_()}
    assert len(calls)==1
    witness,raw=witness_and_raw([dict(output_type='display_data',data=bundle,metadata={})])
    witness['sources']={source['digest']:source};witness['table_handles']={table['id']:table}
    page=reader.project_document(raw,witness)
    reps={rep['format']:rep for rep in page['items'][0]['representations']}
    assert reps['table']['value']==table and reps['table']['available']
    definition=reps['case']['definition_witness']
    assert not definition['registered'] and definition['origin']['kind']=='notebook_output'
    assert definition['binding']['question_sha256']==[c.digest(descriptor['questions'][0])]
    assert 'view_ref' not in definition and 'answers' not in definition
    assert reader.case_definition_witness(witness,1,descriptor)['origin']!=definition['origin']
    witness['table_handles']={}
    page=reader.project_document(raw,witness)
    assert not next(rep for rep in page['items'][0]['representations'] if rep['format']=='table')['available']


def test_json_int64_source_is_exact_instead_of_rounding_through_browser_number():
    value={'safe':2**53-1,'large':2**63-1,'false':False,'null':None}
    reps=outputs.mime_representations({'application/json':value})
    assert reps[0]['available'] and 'value' not in reps[0]
    assert json.loads(reps[0]['raw_json'])==value
    assert str(2**63-1) in reps[0]['raw_json'] and reps[0]['format']=='json'


def test_embedded_wav_mime_uses_real_decoder_and_missing_video_probe_is_explicit():
    data=wav_bytes()
    bundle={'audio/wav':base64.b64encode(data).decode(),'video/webm':base64.b64encode(b'not video').decode(),'text/plain':'media'}
    witness,raw=witness_and_raw([dict(output_type='display_data',data=bundle,metadata={})])
    page=reader.project_document(raw,witness)
    reps={rep['mime']:rep for rep in page['items'][0]['representations']}
    assert reps['audio/wav']['value']['codec']=='pcm_s16le' and reps['audio/wav']['available']
    assert not reps['video/webm']['available'] and reps['video/webm']['reason']=='media_unavailable'
    ref=reps['audio/wav']['value']
    descriptor,actual=reader.embedded_media(raw,witness,output_index=0,mime='audio/wav',expected_sha256=ref['sha256'])
    assert actual==data and descriptor==ref


@pytest.mark.parametrize('mime,format_name',[('image/jpeg','JPEG'),('image/webp','WEBP'),('image/gif','GIF')])
def test_real_static_image_decoders_for_closed_non_png_mimes(mime,format_name):
    from PIL import Image
    stream=io.BytesIO()
    with Image.new('RGB',(8,6),'navy') as image:image.save(stream,format=format_name)
    descriptor=media.probe_image(stream.getvalue(),artifact_id=OBJECT,mime=mime)
    assert descriptor['width']==8 and descriptor['height']==6 and descriptor['mime']==mime


def test_animated_image_is_explicitly_unavailable_instead_of_unbounded_decoding():
    from PIL import Image
    stream=io.BytesIO()
    with Image.new('RGB',(8,6),'navy') as first,Image.new('RGB',(8,6),'red') as second:
        first.save(stream,format='GIF',save_all=True,append_images=[second],duration=100,loop=0)
    with pytest.raises(media.MediaError) as error:media.probe_image(stream.getvalue(),artifact_id=OBJECT,mime='image/gif')
    assert error.value.reason=='media_unavailable'
