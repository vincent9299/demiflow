"""Actual bounded workspace worker + fixed Lance row/public media declaration.

Only private fixtures; no persistent capture, isolated codec, Runtime or B1.
"""
import hashlib
import os
from pathlib import Path
import tempfile

import lance
import pyarrow as pa
import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench import catalog as table_view
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.filesystem_identity import identity
from demiflow.workbench.analysis.filesystem_identity import parse
from demiflow.forge.schema import ForgeError
from demiflow.forge.platform import content_digest
from demiflow.workbench.worker import operation
from test_forge_outputs_media import image_bytes


def declared(tmp_path,row_path='picture.png'):
    root=tmp_path/'project';ws.init_workspace(root,name='owned-media-export');(root/'data').mkdir();(root/'tables').mkdir()
    control=ws.read_control(root/'workspace.json');expected=content_digest(control)
    control['locations']['local']=dict(root='data',writable=False)
    ws.write_control(root/'workspace.json',control,expected=expected)
    raw=image_bytes();(root/'data/picture.png').write_bytes(raw)
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    lance.write_dataset(pa.table(dict(image_path=[row_path],image_sha=[hashlib.sha256(raw).hexdigest()])),str(root/'data/rows.lance'))
    attached=project_ops.attach_table(root,'rows',relative_uri='rows.lance',version=1,dataset_id='rows',schema_name='media',schema_version='1')
    field=dict(path=['image_path'],kind='image',location_id='local',sha256_path=['image_sha'],max_bytes=8*1024*1024,mime='image/png')
    project_ops.set_table_view(root,'rows',expected=attached['digest'],display=dict(fields=[field]),provenance={})
    return root,raw


def fixed(root):
    info=project_ops.describe_table(root,'rows')
    locator=dict(kind='workspace_table',alias='rows',binding=info['binding']['digest'],columns=['image_path'])
    return locator,web_sources.resolve(root,locator)['source']


def exported(root,tmp_path,locator,source,name='out'):
    path=tmp_path/(name+'.bin');descriptor=os.open(path,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
    try:
        return operation('analysis_media_export',root,source_locator=locator,source_snapshot=source['digest'],
            case_key='0',field='image_path',output_fd=descriptor,_pass_fds=(descriptor,)),path
    finally:os.close(descriptor)


def test_exact_declared_fixed_row_streams_only_to_exclusive_inherited_file(tmp_path):
    root,raw=declared(tmp_path);locator,source=fixed(root)
    value,path=exported(root,tmp_path,locator,source)
    assert path.read_bytes()==raw and value['sha256']==hashlib.sha256(raw).hexdigest()
    assert value['mime']=='image/png' and value['bytes']==len(raw)
    assert value['source']['source_snapshot']==source['digest'] and value['source']['case_key']=='0'
    assert len(value['source']['resource_binding'])==64 and 'path' not in value
    # Altered external bytes do not inherit validity from the unchanged table.
    (root/'data/picture.png').write_bytes(raw+b'changed')
    with pytest.raises(ForgeError) as error:exported(root,tmp_path,locator,source,'changed')
    assert error.value.code=='conflict'


def test_missing_declaration_or_symlink_never_uses_a_project_root_fallback(tmp_path):
    root,raw=declared(tmp_path);locator,source=fixed(root)
    image=root/'data/picture.png';image.unlink();outside=tmp_path/'outside.png';outside.write_bytes(raw);image.symlink_to(outside)
    with pytest.raises(ForgeError):exported(root,tmp_path,locator,source,'link')
    image.unlink();image.write_bytes(raw)
    control=project_ops.describe_table(root,'rows');project_ops.set_table_view(root,'rows',expected=control['digest'],display={},provenance={})
    locator,source=fixed(root)
    with pytest.raises(ForgeError) as error:exported(root,tmp_path,locator,source,'undeclared')
    assert error.value.code=='operation_rejected'


def test_audio_video_declarations_have_real_resource_root_hash_mime_and_finite_budget():
    for kind,mime,maximum in [('audio','audio/wav',16),('video','video/webm',64)]:
        field=dict(path=['path'],kind=kind,location_id='public',sha256_path=['sha'],max_bytes=maximum*1024*1024,mime=mime)
        table_view.validate(dict(fields=[field]),{},locations={'public':{}})
        for changed in ({**field,'mime':'text/plain'},{**field,'max_bytes':maximum*1024*1024+1},{key:value for key,value in field.items() if key!='sha256_path'}):
            with pytest.raises(ForgeError):table_view.validate(dict(fields=[changed]),{},locations={'public':{}})


@pytest.mark.parametrize('base_name',['.forge','.forge/assets','public'])
def test_control_root_itself_ancestor_and_child_refuse_before_copy(base_name,tmp_path):
    root,raw=declared(tmp_path,row_path='.forge/picture.png' if base_name=='public' else 'picture.png')
    base=root/base_name;base.mkdir(parents=True,exist_ok=True)
    if base_name=='public':
        (base/'.forge').mkdir();target=base/'.forge/picture.png';relative='.forge/picture.png'
    else:target=base/'picture.png';relative='picture.png'
    target.write_bytes(raw)
    workspace=ws.read_control(root/'workspace.json');previous=content_digest(workspace)
    workspace['locations']['media']=dict(root=base_name,writable=False);ws.write_control(root/'workspace.json',workspace,expected=previous)
    before=project_ops.describe_table(root,'rows')
    field=dict(path=['image_path'],kind='image',location_id='media',sha256_path=['image_sha'],max_bytes=8*1024*1024,mime='image/png')
    project_ops.set_table_view(root,'rows',expected=before['digest'],display=dict(fields=[field]),provenance={})
    locator,source=fixed(root);output=tmp_path/'refused.bin';fd=os.open(output,os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600)
    try:
        with pytest.raises(ForgeError):operation('analysis_media_export',root,source_locator=locator,source_snapshot=source['digest'],case_key='0',field='image_path',output_fd=fd,_pass_fds=(fd,))
        assert output.stat().st_size==0
    finally:os.close(fd)


def test_real_workspace_resource_inode_uses_exact_unsigned_identity_without_global_json_relaxation():
    parent=Path(__file__).resolve().parents[2]/'P2'
    if not parent.is_dir():
        pytest.skip('not_run: requires the sibling P2 lane worktree and a filesystem yielding '
                    'inode > 2**63-1; absent in this integration tree (env, not a pass)')
    with tempfile.TemporaryDirectory(prefix='owned-media-inode-',dir=parent) as directory:
        root,raw=declared(Path(directory));locator,source=fixed(root)
        facts=(root/'data').stat()
        assert facts.st_ino>2**63-1,'workspace fixture did not exercise the required real high inode'
        encoded=identity(facts);assert parse(encoded)==dict(device=facts.st_dev,inode=facts.st_ino)
        assert encoded['inode']==str(facts.st_ino)
        with pytest.raises(ForgeError):c.encoded(dict(inode=facts.st_ino))
        value,path=exported(root,Path(directory),locator,source)
        assert path.read_bytes()==raw and len(value['source']['resource_binding'])==64


def test_filesystem_identity_is_bounded_exact_and_keeps_legacy_small_metadata_readable():
    from types import SimpleNamespace
    info=SimpleNamespace(st_dev=2**64-1,st_ino=2**64-2)
    assert parse(identity(info))==dict(device=2**64-1,inode=2**64-2)
    assert parse(dict(device=1,inode=2))==dict(device=1,inode=2)
    for bad in (True,-1,2**64,1.5):
        with pytest.raises(ForgeError):identity(SimpleNamespace(st_dev=1,st_ino=bad))
    for bad in ('01','-1','1.0',str(2**64)):
        with pytest.raises(ForgeError):parse(dict(device='1',inode=bad))
