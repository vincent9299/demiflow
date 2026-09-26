import hashlib
from pathlib import Path
import pytest
from demiflow.lance.artifacts import ARTIFACTS, ArtifactSet, logical_path
from demiflow.lance.blobs import LanceBlobStore
from demiflow.lance.registry import write_registered_table


def evidence(tmp_path, entries):
    ref,_,_=write_registered_table(tmp_path,'run/artifacts.lance',schema=ARTIFACTS,
        schema_name='named_artifacts',schema_version='v1',rows_factory=lambda:iter(entries),fingerprint='test')
    return ArtifactSet(tmp_path,ref)


def test_fixed_bytes_survive_source_removal_and_new_blob_versions(tmp_path):
    source=tmp_path/'input.txt';source.write_bytes(b'original')
    store=LanceBlobStore(tmp_path,'run/blobs.lance');blob=store.put(source.read_bytes())
    entry={'path':'batch/input.txt','sha256':blob.sha256,'byte_size':8,'media_type':'text/plain','role':'input',
           'blob_uri':blob.relative_uri,'blob_version':blob.version,'blob_column':blob.column}
    saved=evidence(tmp_path,[entry,{**entry,'path':'review/same.txt','role':'review_input'}])
    source.unlink();store.put(b'new bytes')
    assert saved.read_bytes('batch/input.txt')==b'original'
    assert saved.read_bytes('review/same.txt')==b'original'
    assert saved.verify()=={'artifacts':2,'verified_blobs':1}
    with pytest.raises(KeyError):saved.read_bytes('input.txt')
    assert saved.reference('batch/input.txt')['artifact_set']['lance_version']==1


def test_size_and_logical_names_are_validated(tmp_path):
    blob=LanceBlobStore(tmp_path,'run/blobs.lance').put(b'abc')
    entry={'path':'x','sha256':blob.sha256,'byte_size':4,'media_type':None,'role':None,
           'blob_uri':blob.relative_uri,'blob_version':blob.version,'blob_column':blob.column}
    saved=evidence(tmp_path,[entry])
    with pytest.raises(ValueError,match='size'):saved.read_bytes('x')
    for value in ['/etc/passwd','../x','a/../x','a//x','']:
        with pytest.raises(ValueError):logical_path(value)


def test_streamed_blob_checkpoint_replays_without_reopening_source(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.lance.blobs import BlobRef
    raw=b'full original binary evidence'
    schema=pa.schema([('sha256',pa.string()),lance.blob_field('data')])
    key=hashlib.sha256(raw).hexdigest()
    args=dict(schema=schema,schema_name='evidence_blobs',schema_version='v1',fingerprint='blob-test',max_rows_per_batch=1)
    ref,_,_=write_registered_table(tmp_path,'blobs.lance',rows_factory=lambda:iter([{'sha256':key,'data':raw}]),**args)
    assert BlobRef(ref.relative_uri,ref.lance_version,key).read(tmp_path)==raw
    def missing():raise AssertionError('replay reopened source')
    again,_,replayed=write_registered_table(tmp_path,'blobs.lance',rows_factory=missing,**args)
    assert replayed and again==ref
