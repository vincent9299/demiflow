"""Canonical private media object metadata/seal; no capture/OCI/B1 claim."""
import copy
import hashlib

import pytest

from demiflow.workbench.analysis import media
from demiflow.workbench.analysis.object_files import Ingress
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.object_files import verify_object
from demiflow.forge.schema import ForgeError
from test_forge_outputs_media import image_bytes


def fixture():
    raw=image_bytes();identifier='o'+'a'*32
    descriptor=media.probe_image(raw,artifact_id=identifier,mime='image/png')
    source=dict(version=1,source_snapshot='b'*64,case_key='18446744073709551615',field='image',declared_sha256=hashlib.sha256(raw).hexdigest(),resource_binding='c'*64)
    payload=dict(format='media',mime='image/png',bytes=len(raw),sha256=descriptor['sha256'],complete=True,descriptor=descriptor,source=source,
        chunk_sha256=[hashlib.sha256(raw).hexdigest()])
    seed=dict(project='w'+'1'*32,object_id=identifier,generation=1,retention='pinned',execution_id='explicit-capture-request',parents=[],source_binding=source['source_snapshot'])
    return raw,seed,payload,{'media.bin':dict(bytes=len(raw),sha256=descriptor['sha256'])}


def test_real_small_png_uses_same_atomic_immutable_object_layout(tmp_path):
    raw,seed,payload,files=fixture()
    receiver=Ingress(tmp_path/'state',publish_id='capture',seed=seed,description=payload,files=files)
    receiver.start_file('media.bin');receiver.write(raw);receiver.finish_file()
    receipt=receiver.seal();assert receipt['prepared'] and not receipt['database_ready']
    receiver.rename(authorize=lambda *_:True)
    assert verify_object(tmp_path/'state',seed['object_id'],receiver.digest)
    assert (tmp_path/'state/objects'/seed['object_id']/'payload/media.bin').read_bytes()==raw


def test_missing_probe_changed_declaration_wrong_retention_or_malformed_identity_refuses_publication():
    raw,seed,payload,files=fixture()
    invalid=[]
    for key,value in [('complete',False),('bytes',True),('mime','application/json')]:invalid.append({**payload,key:value})
    changed=copy.deepcopy(payload);changed['descriptor'].pop('width');invalid.append(changed)
    changed=copy.deepcopy(payload);changed['source']['declared_sha256']='d'*64;invalid.append(changed)
    changed=copy.deepcopy(payload);changed['descriptor']['artifact_id']='o'+'b'*32;invalid.append(changed)
    changed=copy.deepcopy(payload);changed['source']['case_key']='01';invalid.append(changed)
    changed=copy.deepcopy(payload);changed['descriptor']['output_index']=0;invalid.append(changed)
    changed=copy.deepcopy(payload);changed['chunk_sha256']=[];invalid.append(changed)
    changed=copy.deepcopy(payload);changed['chunk_sha256']*=2;invalid.append(changed)
    changed=copy.deepcopy(payload);changed['chunk_sha256']=['invalid'];invalid.append(changed)
    for item in invalid:
        with pytest.raises(ForgeError):candidate_manifest(seed,item,files)
    with pytest.raises(ForgeError):candidate_manifest({**seed,'retention':'ephemeral'},payload,files)
    with pytest.raises(ForgeError):candidate_manifest(seed,payload,{'other.bin':files['media.bin']})
