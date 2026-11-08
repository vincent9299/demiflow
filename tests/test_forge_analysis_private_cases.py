"""Actual private native Lance + sealed Ingress; no OCI/cgroup/B1 claim."""
import copy
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import case_index
from demiflow.workbench.analysis import case_query
from demiflow.workbench.analysis.arrow_bridge import schema_description
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.workbench.analysis.case_pages import native_config
from demiflow.workbench.analysis.object_files import Ingress
from demiflow.workbench.analysis.result_transfer import files_manifest
from demiflow.forge.schema import ForgeError


def sealed(tmp_path):
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    data=tmp_path/'producer'/'rows.lance';data.parent.mkdir()
    table=pa.table(dict(input=['one','two','three'],score=pa.array([3.,1.,2.],type=pa.float64())))
    snapshot=lance.write_dataset(table,str(data));case_index.build_index(snapshot,data)
    description=dict(format='lance',version=1,rows=3,schema=schema_description(snapshot.schema),
        schema_sha256=schema_digest(snapshot.schema),logical_bytes=table.nbytes,complete=True)
    identifier='o'+'1'*32;project='w'+'2'*32
    ingress=Ingress(tmp_path/'private',publish_id='publish',seed=dict(project=project,object_id=identifier,generation=1,
        retention='pinned',execution_id='owned-fixture',parents=[],source_binding=None),description=description,files=files_manifest(data))
    for name in ingress.manifest['files']:
        ingress.start_file(name)
        with (data/name).open('rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):ingress.write(chunk)
        ingress.finish_file()
    ingress.seal();ingress.rename(authorize=lambda *_:True)
    source=c.source_snapshot(dict(kind='pinned_object',binding=ingress.digest,rows=3,schema=description['schema'],
        case_key=dict(kind='row_id',field=None),retention='pinned'))
    return source,ingress.objects_root/identifier,project


def test_actual_sealed_private_lance_keeps_fixed_source_projection_and_case_ids(tmp_path):
    source,path,project=sealed(tmp_path)
    query=dict(kind='cases',columns=['input','score'],filters=None,grouping=[],sort=[dict(field='score',direction='asc')],scope='full',offset=0,limit=2)
    config=dict(version=1,operation='case-page',witness=dict(project=project,artifact=dict(id=path.name,generation=1,digest=source['binding']),source=source),query=query)
    assert native_config(config)==config
    result=case_query.read_private(source,path,query)
    assert result['kind']=='cases' and result['source_snapshot']==source['digest']
    assert [row['input'] for row in result['rows']]==['two','three']
    assert result['coverage']['complete'] and result['next_offset']==2
    last=case_query.read_private(source,path,{**query,'offset':2})
    assert last['rows'][0]['input']=='one' and last['rows'][0]['_rowid']=='0'
    assert last['scope_digest']==result['scope_digest']
    # Same registered source cannot redirect to a different path/object or
    # accept changed source metadata under an unchanged object binding.
    wrong=copy.deepcopy(source);wrong['rows']=4
    wrong=c.source_snapshot({key:value for key,value in wrong.items() if key!='digest'})
    with pytest.raises(ForgeError):case_query.read_private(wrong,path,query)
    with pytest.raises(ForgeError):case_query.read_private(source,tmp_path/'outside',query)
    for bad in (True,'1',0):
        changed=copy.deepcopy(config);changed['witness']['artifact']['generation']=bad
        with pytest.raises(ForgeError):native_config(changed)


def test_tampered_sealed_native_case_index_never_becomes_a_successful_page(tmp_path):
    source,path,project=sealed(tmp_path)
    index=path/'data.lance'/case_index.NAME
    index.chmod(0o600);raw=bytearray(index.read_bytes());raw[-1]^=1;index.write_bytes(raw);index.chmod(0o400)
    with pytest.raises(ForgeError):case_query.read_private(source,path,dict(kind='cases',columns=['input'],scope='full'))
