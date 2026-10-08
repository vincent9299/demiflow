import hashlib
from pathlib import Path
import pytest
from demiflow.lance.artifacts import ARTIFACTS, ArtifactSet, logical_path
from demiflow.objects import LocalObjectStore
from demiflow.lance.registry import write_registered_table


def evidence(tmp_path, entries):
    ref,_,_=write_registered_table(tmp_path,'run/artifacts.lance',schema=ARTIFACTS,
        schema_name='named_artifacts',schema_version='v1',rows_factory=lambda:iter(entries),fingerprint='test')
    return ArtifactSet(tmp_path,ref)


def test_fixed_bytes_survive_source_removal_and_new_blob_versions(tmp_path):
    source=tmp_path/'input.txt';source.write_bytes(b'original')
    store=LocalObjectStore(tmp_path / 'objects');blob=store.put(source.read_bytes())
    entry={'path':'batch/input.txt','sha256':blob.sha256,'byte_size':8,'media_type':'text/plain','role':'input',
           'object_uri':blob.uri}
    saved=evidence(tmp_path,[entry,{**entry,'path':'review/same.txt','role':'review_input'}])
    source.unlink();store.put(b'new bytes')
    assert saved.read_bytes('batch/input.txt')==b'original'
    assert saved.read_bytes('review/same.txt')==b'original'
    assert saved.verify()=={'artifacts':2,'verified_objects':1}
    with pytest.raises(KeyError):saved.read_bytes('input.txt')
    assert saved.reference('batch/input.txt')['artifact_set']['lance_version']==1


def test_size_and_logical_names_are_validated(tmp_path):
    blob=LocalObjectStore(tmp_path / 'objects').put(b'abc')
    entry={'path':'x','sha256':blob.sha256,'byte_size':4,'media_type':None,'role':None,
           'object_uri':blob.uri}
    saved=evidence(tmp_path,[entry])
    with pytest.raises(ValueError,match='size'):saved.read_bytes('x')
    for value in ['/etc/passwd','../x','a/../x','a//x','']:
        with pytest.raises(ValueError):logical_path(value)


def test_streamed_blob_checkpoint_replays_without_reopening_source(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.objects import ObjectRef
    raw=b'full original binary evidence'
    schema=pa.schema([('sha256',pa.string()),lance.blob_field('data')])
    key=hashlib.sha256(raw).hexdigest()
    args=dict(schema=schema,schema_name='evidence_blobs',schema_version='v1',fingerprint='blob-test',max_rows_per_batch=1)
    ref,_,_=write_registered_table(tmp_path,'blobs.lance',rows_factory=lambda:iter([{'sha256':key,'data':raw}]),**args)
    assert ref.open(tmp_path).take_blobs('data', indices=[0])[0].read() == raw
    def missing():raise AssertionError('replay reopened source')
    again,_,replayed=write_registered_table(tmp_path,'blobs.lance',rows_factory=missing,**args)
    assert replayed and again==ref


def test_legacy_artifacts_allow_read_only_audit_but_no_new_reference(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.lance.artifacts import _LEGACY_ARTIFACTS
    raw = b'old evidence'
    sha = hashlib.sha256(raw).hexdigest()
    lance.write_dataset(pa.Table.from_arrays([pa.array([sha]), lance.blob_array([raw])],
        names=['sha256', 'data']), str(tmp_path / 'legacy.lance'))
    entry = {'path': 'old.txt', 'sha256': sha, 'byte_size': len(raw),
             'blob_uri': 'legacy.lance', 'blob_version': 1, 'blob_column': 'data'}
    ref, _, _ = write_registered_table(tmp_path, 'old_artifacts.lance', schema=_LEGACY_ARTIFACTS,
        schema_name='named_artifacts', schema_version='v1', rows_factory=lambda: iter([entry]), fingerprint='old')
    saved = ArtifactSet(tmp_path, ref)
    assert saved.read_bytes('old.txt') == raw
    assert saved.verify() == {'artifacts': 1, 'verified_blobs': 1}
    with pytest.raises(ValueError, match='Migrate legacy artifacts'):
        saved.object_ref('old.txt')
