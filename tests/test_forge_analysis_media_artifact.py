"""Closed pending probes and bounded file ranges; no real OCI/ffprobe claim.

Rich-output primitives come from the hash-bound overlay runner. The synthetic
64MiB video descriptor tests only ready range IO, never decoder success.
"""
import copy
import hashlib
import io
import os
from pathlib import Path
from unittest.mock import Mock
import wave

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import media_artifact_worker as worker
from demiflow.workbench.analysis import media_worker
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError
from test_forge_analysis_read_control import authority
from test_forge_analysis_read_retirement import harness,cleanup
from test_forge_analysis_case_read import finish

REAL_AUTHORIZATION=read_control.Authorization


def pending(raw=b'private media',mime='image/png',project='w'+'2'*32):
    sha=hashlib.sha256(raw).hexdigest()
    seed=dict(project=project,object_id='o'+'f'*32,generation=1,retention='pinned',execution_id='capture-one',parents=[],source_binding='a'*64)
    return dict(version=1,operation='probe-media',reservation=dict(publish_id='p'+'e'*32,seed=seed,seed_digest=c.digest(seed),reserved_bytes=128*1024**2,reserved_entries=4),
        content=dict(sha256=sha,bytes=len(raw),mime=mime),source=dict(version=1,source_snapshot='a'*64,case_key='0',field='clip',declared_sha256=sha,resource_binding='b'*64))


def frozen(path,raw):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw);path.chmod(0o400)


@pytest.mark.parametrize('denied',['read','analysis_write'])
def test_probe_publication_denial_never_creates_control_owner(tmp_path,denied):
    a=authority()
    def check(project,principal,permission):
        if permission==denied:raise ForgeError('grant revoked',code='operation_rejected')
        return True
    a.check.side_effect=check
    with pytest.raises(ForgeError,match='revoked'):read_control.publish_probe(tmp_path,a,'capture-one',pending())
    assert not (tmp_path/read_control.NAME).exists()


@pytest.mark.parametrize('kind',['image','audio'])
def test_hash_before_real_bounded_png_wav_probe_and_no_publication(tmp_path,monkeypatch,kind):
    stream=io.BytesIO()
    if kind=='image':
        from PIL import Image
        Image.new('RGB',(2,3),(5,6,7)).save(stream,format='PNG');mime='image/png'
    else:
        with wave.open(stream,'wb') as output:
            output.setnchannels(1);output.setsampwidth(2);output.setframerate(8000);output.writeframes(b'\x00\x00'*80)
        mime='audio/wav'
    raw=stream.getvalue();value=pending(raw,mime);path=tmp_path/'pending/media.bin';frozen(path,raw)
    result=worker.probe(value,pending_root=path.parent)
    assert result['operation']=='media-probe' and result['sha256']==hashlib.sha256(raw).hexdigest()
    assert result['descriptor']['bytes']==len(raw) and result['chunk_sha256']==[result['sha256']]
    assert 'ready' not in result and not (tmp_path/'objects').exists() and not (path.parent/'manifest.json').exists()
    bad=copy.deepcopy(value);bad['content']['sha256']=bad['source']['declared_sha256']='c'*64
    target='probe_image' if kind=='image' else 'probe_wav'
    monkeypatch.setattr(worker.primitives(),target,lambda *a,**k:pytest.fail('decode before hash'))
    with pytest.raises(ForgeError,match='hash differs'):worker.probe(bad,pending_root=path.parent)


def test_pending_probe_refuses_links_mutability_sizes_and_untrusted_config(tmp_path):
    value=pending();path=tmp_path/'pending/media.bin';frozen(path,b'private media')
    assert media_worker.configuration(value)==value
    variants=[]
    for target,key,data in [('reservation','publish_id','../other'),('content','bytes',64*1024**2+1),('source','declared_sha256','e'*64)]:
        bad=copy.deepcopy(value);bad[target][key]=data;variants.append(bad)
    bad=copy.deepcopy(value);bad['reservation']['seed']['generation']=True;variants.append(bad)
    bad=copy.deepcopy(value);bad['path']='/etc/passwd';variants.append(bad)
    for bad in variants:
        with pytest.raises(ForgeError):worker.configuration(bad)
    path.chmod(0o600)
    with pytest.raises(ForgeError,match='metadata'):worker.probe(value,pending_root=path.parent)
    path.unlink();path.symlink_to(tmp_path/'unknown')
    with pytest.raises(OSError):worker.probe(value,pending_root=path.parent)


def test_full_source_witness_rejects_loss_bad_types_and_noncanonical_case_keys():
    value=pending();original=worker.probe_binding(value)
    changed=copy.deepcopy(value);changed['source']['resource_binding']='d'*64
    rebound=worker.probe_binding(changed)
    assert rebound['source_digest']!=original['source_digest'] and rebound['config_digest']!=original['config_digest']
    variants=[]
    for field,replacement in [('version',True),('resource_binding',None),('resource_binding','g'*64),
            ('case_key','01'),('case_key','-1'),('case_key',str(2**64)),('case_key',1),('field','')]:
        bad=copy.deepcopy(value);bad['source'][field]=replacement;variants.append(bad)
    bad=copy.deepcopy(value);del bad['source']['resource_binding'];variants.append(bad)
    bad=copy.deepcopy(value);bad['source']['path']='/private/secret';variants.append(bad)
    for bad in variants:
        with pytest.raises(ForgeError):worker.configuration(bad)


@pytest.mark.parametrize('denied',[None,'read','analysis_write'])
def test_pending_owner_requires_both_grants_and_fresh_watchdog_authorization(tmp_path,monkeypatch,denied):
    a=authority();value=pending()
    read_control.publish_probe(tmp_path,a,'capture-one',value)
    assert [call.args[-1] for call in a.check.call_args_list]==['read','analysis_write']
    owner=read_control.load(tmp_path);observed=[]
    class Exchange:
        def __init__(self,authority,operation,payload):
            observed.append((operation,payload))
            self.value=dict(principal=a.principal) if operation=='whoami' else dict(allowed=payload['permission']!=denied)
        def advance(self):return True
        def close(self):pass
    monkeypatch.setattr(read_control,'Exchange',Exchange)
    task=REAL_AUTHORIZATION(None,owner,peer=a.peer)
    try:
        assert not task.advance()
        if denied=='read':
            with pytest.raises(ForgeError):task.advance()
            assert len(observed)==2
        else:
            assert not task.advance()
            if denied:
                with pytest.raises(ForgeError,match='capture'):task.advance()
            else:assert task.advance()
            assert observed[-1]==('permission',{'permission':'analysis_write'})
    finally:task.close()
    original=read_control.load(tmp_path)
    assert read_control.publish_probe(tmp_path,a,'capture-one',value)==c.digest(original)
    assert read_control.load(tmp_path)['deadline']==original['deadline']


def test_probe_runtime_exact_pending_mount_serial_owner_and_revocation(tmp_path,monkeypatch):
    runtime,make,service,a,old=harness(tmp_path,monkeypatch)
    try:
        service._read_owner_control(a.peer,'clear-read-owner',c.digest(old));(tmp_path/read_control.NAME).unlink()
        value=pending(project=a.project);read_control.publish_probe(tmp_path,a,'capture-one',value);owner=read_control.load(tmp_path)
        service._read_owner_control(a.peer,'bind-read-owner',c.digest(owner),record=owner)
        runtime._config=Runtime._config
        def create(config=value):
            return runtime.create(project=a.project,execution_id=owner['read_request_id'],role='media',config=config,
                web_parent=owner['web_parent'],owner_digest=c.digest(owner),read_owner=owner,authorize=lambda:True)['job_id']
        wrong=copy.deepcopy(value);wrong['reservation']['publish_id']='p'+'d'*32
        with pytest.raises(ForgeError):create(wrong)
        wrong=copy.deepcopy(value);wrong['source']['resource_binding']='d'*64
        with pytest.raises(ForgeError,match='reservation'):create(wrong)
        identifier=create();spec=c.decode((tmp_path/identifier/'bundle/config.json').read_bytes())
        mounts={item['destination']:item for item in spec['mounts']}
        assert mounts['/pending']['source']==str(runtime.deployment.paths.state/'.staging'/value['reservation']['publish_id']/'payload')
        assert 'ro' in mounts['/pending']['options'] and not any(p.startswith('/objects/') for p in mounts)
        assert spec['linux']['resources']['memory']['limit']==256*1024**2
        with pytest.raises(ForgeError):read_control.publish(tmp_path,a,'other',dict(id='o'+'f'*32,generation=1,digest='b'*64,sha256='c'*64,bytes=20))
        runtime.start(identifier,authorize=lambda:True)
        service.read_monitor=Mock();service.read_monitor.advance.side_effect=ForgeError('capture revoked')
        service._tick_watches()
        assert service.read_stop_requested and not service.stop_requested and runtime.groups.kills==[identifier]
        runtime.stop(identifier);cleanup(runtime,identifier);assert runtime.retire_read(identifier)['retired']
        service._read_owner_control(a.peer,'clear-read-owner',c.digest(owner))
        assert service.owner['generation']=='newer-kernel-generation'
    finally:finish(runtime,service)


def video_fixture(tmp_path):
    """Known synthetic encoded bytes, NOT a successfully decoded video."""
    root=tmp_path/('o'+'f'*32);root.mkdir()
    raw=bytes(range(256))*4096;path=root/'payload/media.bin';path.parent.mkdir()
    digest=hashlib.sha256();parts=[]
    with path.open('wb') as output:
        for _ in range(64):output.write(raw);digest.update(raw);parts.append(hashlib.sha256(raw).hexdigest())
    path.chmod(0o400)
    descriptor=dict(artifact_id='o'+'f'*32,sha256=digest.hexdigest(),mime='video/mp4',bytes=64*1024**2,width=2,height=2,codec='h264',duration_s=1.,fps=1.)
    artifact=dict(project='w'+'2'*32,id=descriptor['artifact_id'],generation=1,digest='b'*64,source_digest='a'*64,
        sha256=descriptor['sha256'],bytes=descriptor['bytes'],mime=descriptor['mime'],descriptor=descriptor,chunk_sha256=parts)
    from demiflow.workbench.analysis.object_files import candidate_manifest
    witness=dict(version=1,source_snapshot='a'*64,case_key='0',field='video',declared_sha256=digest.hexdigest(),resource_binding='c'*64)
    seed=dict(project=artifact['project'],object_id=artifact['id'],generation=1,retention='pinned',execution_id='range-fixture',parents=[],source_binding=witness['source_snapshot'])
    payload=dict(format='media',complete=True,source=witness,**{key:artifact[key] for key in ('sha256','bytes','mime','descriptor','chunk_sha256')})
    manifest=candidate_manifest(seed,payload,{'media.bin':dict(bytes=artifact['bytes'],sha256=artifact['sha256'])})
    frozen(root/'manifest.json',c.encoded(manifest));artifact.update(digest=c.digest(manifest),source_digest=c.digest(witness))
    return dict(version=1,operation='artifact-media',artifact=artifact,range='bytes=0-1048575'),path,raw


def test_64mib_file_ranges_read_at_most_two_chunks_and_detect_changed_chunk(tmp_path,monkeypatch,record_property):
    value,path,chunk=video_fixture(tmp_path);worker.configuration(value)
    real=os.pread;reads=[]
    def pread(fd,size,offset):reads.append((size,offset));return real(fd,size,offset)
    monkeypatch.setattr(os,'pread',pread)
    # 64 aligned1MiB reads plus a cross-boundary seek: never full-file buffering.
    for index in range(64):
        reads.clear();value['range']=f'bytes={index*len(chunk)}-{(index+1)*len(chunk)-1}'
        result=worker.ready_range(value,objects_root=tmp_path)
        assert result['data']==chunk and sum(size for size,_ in reads)==len(chunk)
    reads.clear();value['range']='bytes=1048575-1048577'
    assert worker.ready_range(value,objects_root=tmp_path)['data']==bytes([255,0,1])
    assert sum(size for size,_ in reads)==2*1024**2
    value['range']=None
    with pytest.raises(ForgeError):worker.configuration(value)
    value['range']='bytes=1048576-1048578';path.chmod(0o600)
    with path.open('r+b') as target:target.seek(1048576);target.write(b'changed')
    path.chmod(0o400)
    with pytest.raises(ForgeError,match='chunk proof'):worker.ready_range(value,objects_root=tmp_path)
    record_property('range_io_cycles',65);record_property('fixture_bytes',64*1024**2)
    record_property('scope','actual canonical manifest and bounded chunk IO with proposed shared chunk patch; synthetic video metadata; no DB-ready/OCI/decoder claim')


def test_media_main_sandbox_guard_precedes_pending_probe(monkeypatch):
    monkeypatch.setattr(media_worker,'require_job',lambda _:(_ for _ in ()).throw(ForgeError('not admitted')))
    monkeypatch.setattr(worker,'probe',lambda *a,**k:pytest.fail('probe before guard'))
    with pytest.raises(ForgeError,match='not admitted'):media_worker.main()


def test_pending_error_reply_has_owned_identity_no_data_or_host_path(monkeypatch):
    value=pending()
    monkeypatch.setattr(worker,'probe',lambda *a,**k:(_ for _ in ()).throw(OSError('/private/secret unreadable')))
    result,data=worker.response(value)
    assert result['publish_id']==value['reservation']['publish_id'] and result['object_id']==value['reservation']['seed']['object_id']
    assert result['error']['code']=='operation_failed' and data is None and '/private' not in str(result)


def test_real_png_probe_then_canonical_sqlite_ready_then_exact_range(tmp_path):
    """Actual Artifacts/Ingress/SQLite with the explicit shared chunk patch.

    Pending bytes are a private fixture, not an implemented export/ingress
    capture coordinator. Probe happens before the final canonical Ingress.
    """
    from contextlib import contextmanager
    import json
    import sqlite3
    from PIL import Image
    from demiflow.workbench.analysis.database import _SCHEMA
    from demiflow.workbench.analysis.records import Access
    from demiflow.workbench.analysis.records import Records
    from demiflow.workbench.analysis.artifacts import Artifacts
    from demiflow.workbench.analysis.object_files import Ingress
    from demiflow.workbench.analysis.object_files import verify_object
    class Database:
        def __init__(self):
            self.connection=sqlite3.connect(':memory:');self.connection.row_factory=sqlite3.Row
            self.connection.executescript(_SCHEMA)
        @contextmanager
        def locked(self):yield self.connection
        @contextmanager
        def transaction(self,**kwargs):
            with self.connection:yield self.connection
    db=Database();grants={'read','analysis_write'}
    access=Access('w'+'2'*32,'reader',lambda p,u,grant:grant in grants)
    artifacts=Artifacts(Records(db));stream=io.BytesIO();Image.new('RGB',(2,3)).save(stream,format='PNG');raw=stream.getvalue()
    value=pending(raw)
    try:
        allocated=artifacts.reserve(access,request_id='capture-request',execution_id='capture-request',generation=1,retention='pinned',parents=[],source_binding=value['source']['source_snapshot'])
        ticket=artifacts.receiving(access,allocated['id']);value['reservation']={**ticket,'seed_digest':c.digest(ticket['seed'])}
        frozen(tmp_path/'probe-fixture/media.bin',raw)
        result=worker.probe(value,pending_root=tmp_path/'probe-fixture')
        assert not artifacts.status(access,allocated['id'])['ready']
        payload=dict(format='media',complete=True,source=value['source'],**{key:result[key] for key in ('mime','bytes','sha256','descriptor','chunk_sha256')})
        receiver=Ingress(tmp_path/'state',publish_id=ticket['publish_id'],seed=ticket['seed'],description=payload,files={'media.bin':dict(bytes=len(raw),sha256=result['sha256'])})
        receiver.start_file('media.bin');receiver.write(raw);receiver.finish_file();receipt=receiver.seal()
        artifacts.prepared(access,allocated['id'],manifest=receiver.manifest,receipt=receipt)
        artifacts.begin_publish(access,allocated['id'],expected_digest=receiver.digest);receiver.rename(authorize=lambda *_:True)
        assert not artifacts.status(access,allocated['id'])['ready']
        grants.remove('analysis_write')
        with pytest.raises(ForgeError):artifacts.commit_ready(access,allocated['id'],expected_digest=receiver.digest,verify_renamed=lambda i,d:verify_object(tmp_path/'state',i,d))
        assert not artifacts.status(access,allocated['id'])['ready']
        grants.add('analysis_write')
        committed=artifacts.commit_ready(access,allocated['id'],expected_digest=receiver.digest,verify_renamed=lambda i,d:verify_object(tmp_path/'state',i,d))
        assert committed['ready']
        # Read facts from the actual ready SQLite row; no fabricated notebook.
        row=db.connection.execute('SELECT * FROM analysis_objects WHERE id=?',(allocated['id'],)).fetchone()
        manifest=json.loads(row['manifest']);payload=manifest['payload']
        assert payload['source']==value['source'] and payload['source']['resource_binding']=='b'*64
        artifact=dict(project=access.project,id=row['id'],generation=row['generation'],digest=row['manifest_digest'],source_digest=c.digest(payload['source']),
            **{key:payload[key] for key in ('sha256','bytes','mime','descriptor','chunk_sha256')})
        ready=dict(version=1,operation='artifact-media',artifact=artifact,range=None)
        control=tmp_path/'control';control.mkdir();a=authority()
        read_control.publish_artifact(control,a,'ready-range',ready)
        assert [call.args[-1] for call in a.check.call_args_list]==['read']
        assert worker.ready_range(ready,objects_root=tmp_path/'state/objects')['data']==raw
        for table in ('analysis_sessions','analysis_executions','documents'):
            assert db.connection.execute('SELECT count(*) FROM '+table).fetchone()[0]==0
        altered=copy.deepcopy(ready);altered['artifact']['source_digest']='e'*64
        with pytest.raises(ForgeError,match='sealed facts'):worker.ready_range(altered,objects_root=tmp_path/'state/objects')
    finally:db.connection.close()
