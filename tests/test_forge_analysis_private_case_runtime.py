"""Real private Lance/Ingress/Case reader/native admission plus closed Runtime.

Test-only fixed mount/resource paths redirect into tmp_path. OCI and cgroups
are lifecycle doubles; no external isolation claim.
"""
import copy
import os
from pathlib import Path

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import case_worker
from demiflow.workbench.analysis import case_query
from demiflow.workbench.analysis import case_index
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.arrow_bridge import schema_description
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.workbench.analysis.object_files import Ingress
from demiflow.workbench.analysis.result_transfer import files_manifest
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError
from test_forge_analysis_read_retirement import harness,cleanup
from test_forge_analysis_case_read import finish


def sealed(tmp_path,*,v2):
    import lance
    import pyarrow as pa
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    producer=tmp_path/'producer/rows.lance';producer.parent.mkdir()
    table=pa.table(dict(input=['one','two','three'],score=pa.array([3.,1.,2.],type=pa.float64())))
    snapshot=lance.write_dataset(table,str(producer),enable_v2_manifest_paths=v2);case_index.build_index(snapshot,producer)
    description=dict(format='lance',version=1,rows=3,schema=schema_description(snapshot.schema),schema_sha256=schema_digest(snapshot.schema),logical_bytes=table.nbytes,complete=True)
    seed=dict(project='w'+'2'*32,object_id='o'+'f'*32,generation=1,retention='pinned',execution_id='private-fixture',parents=[],source_binding=None)
    receiver=Ingress(tmp_path/'state',publish_id='p'+'e'*32,seed=seed,description=description,files=files_manifest(producer))
    for name in receiver.manifest['files']:
        receiver.start_file(name)
        with (producer/name).open('rb') as source:
            for chunk in iter(lambda:source.read(1024**2),b''):receiver.write(chunk)
        receiver.finish_file()
    receiver.seal();receiver.rename(authorize=lambda *_:True)
    source=c.source_snapshot(dict(kind='pinned_object',binding=receiver.digest,rows=3,schema=description['schema'],case_key=dict(kind='row_id',field=None),retention='pinned'))
    return dict(version=1,operation='case-page',witness=dict(project=seed['project'],artifact=dict(id=seed['object_id'],generation=1,digest=receiver.digest),source=source),
        query=dict(kind='cases',columns=['input','score'],scope='full',sort=[dict(field='score',direction='asc')],offset=0,limit=2)),receiver.objects_root/seed['object_id']


@pytest.mark.parametrize('v2',[False,True])
def test_registered_case_configuration_to_native_reader_and_runtime_without_kernel(tmp_path,monkeypatch,v2):
    config,object_root=sealed(tmp_path,v2=v2);assert case_worker.configuration(config)==config
    from demiflow.execution.hosts import datafusion
    real_session=datafusion.DataFusionSession;real_reader=case_query.read_private
    (tmp_path/'scratch').mkdir();(tmp_path/'diagnostics').mkdir()
    def session(**kwargs):
        assert kwargs['resource_directory']=='/var/lib/demiflow-analysis/native-slots' and kwargs['max_concurrent']==1
        kwargs.update(resource_directory=tmp_path/'native-slots',temp_directory=str(tmp_path/'scratch'),diagnostics_directory=tmp_path/'diagnostics')
        return real_session(**kwargs)
    def reader(source,path,query):
        assert path=='/objects/'+object_root.name
        return real_reader(source,object_root,query)
    monkeypatch.setattr(datafusion,'DataFusionSession',session);monkeypatch.setattr(case_query,'read_private',reader)
    result=case_worker.execute(config)
    assert [row['input'] for row in result['rows']]==['two','three'] and result['next_offset']==2
    assert result['source_snapshot']==config['witness']['source']['digest'] and result['coverage']['complete']
    control=tmp_path/'runtime';control.mkdir()
    runtime,make,service,a,old=harness(control,monkeypatch)
    try:
        service._read_owner_control(a.peer,'clear-read-owner',c.digest(old));(control/read_control.NAME).unlink()
        read_control.publish_case(control,a,'private-case',config);owner=read_control.load(control)
        service._read_owner_control(a.peer,'bind-read-owner',c.digest(owner),record=owner)
        service.owner=None;runtime._config=Runtime._config
        job=runtime.create(project=a.project,execution_id=owner['read_request_id'],role='native',config=config,web_parent=owner['web_parent'],
            owner_digest=c.digest(owner),read_owner=owner,authorize=lambda:True)['job_id']
        assert c.decode((control/job/'job/job.json').read_bytes())['config']==config
        runtime.start(job,authorize=lambda:True);runtime.stop(job);cleanup(runtime,job);runtime.retire_read(job)
        assert service.owner is None
    finally:finish(runtime,service)
    # A changed native Case index cannot retain the registered object binding.
    path=object_root/'data.lance'/case_index.NAME;path.chmod(0o600)
    raw=bytearray(path.read_bytes());raw[-1]^=1;path.write_bytes(raw);path.chmod(0o400)
    with pytest.raises(ForgeError):case_worker.execute(config)
