"""Real SQLite/output-reader -> read owner -> Runtime configuration/watch.

Requires merged rich_output modules (or the explicitly hashed evidence overlay
runner). No mocked configuration validation. Only OCI/cgroups are doubles.
"""
from contextlib import contextmanager
import copy
import hashlib
import os
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import media_worker
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis import output_worker
from demiflow.workbench.analysis import outputs
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.database import _SCHEMA
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.oci import FixedPaths
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.runtime import Runtime
from demiflow.forge.schema import ForgeError
from test_forge_analysis_read_control import Groups, authority, close, fixture

PROJECT, OBJECT = 'w'+'2'*32, 'o'+'a'*32


def registered(generation):
    class Database:
        def __init__(self):
            self.connection=sqlite3.connect(':memory:'); self.connection.row_factory=sqlite3.Row
            self.connection.executescript(_SCHEMA)
        @contextmanager
        def locked(self): yield self.connection
    db=Database(); conn=db.connection
    # Real candidate manifest and rows; generation INTEGER remains separate
    # from historical and currently live Notebook generation TEXT.
    raw=c.encoded(dict(generation='historical-kernel-generation',parent_msg_id='parent',status='succeeded',complete=True,
        outputs=[dict(output_type='stream',name='stdout',text='1\n')],output_bytes=2,messages=2,ignored_messages=0,
        stdin_supported=False,active_widgets_supported=False,execution_id='execution',cell_id='cell',
        source_sha256=hashlib.sha256(b'print(1)').hexdigest(),language='python',session_id='session',
        final_status='succeeded',raw_fd_output_collected=False))
    sha=hashlib.sha256(raw).hexdigest()
    seed=dict(project=PROJECT,object_id=OBJECT,generation=generation,retention='ephemeral',
        execution_id='execution',parents=[],source_binding=None)
    manifest=candidate_manifest(seed,dict(format='document',mime=outputs.OUTPUT_MIME,bytes=len(raw),sha256=sha,complete=True),
        {'document.json':dict(bytes=len(raw),sha256=sha)})
    document=dict(title='source',cells=[dict(id='cell',language='python',source='print(1)')])
    conn.execute('INSERT INTO documents VALUES (?,?,?,?)',(PROJECT,'notebook','book',1))
    conn.execute('INSERT INTO document_revisions VALUES (?,?,?,?,?,?,?,?)',
        (PROJECT,'notebook','book',1,c.digest(document),c.encoded(document),'reader','now'))
    conn.execute('INSERT INTO analysis_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (PROJECT,'session','reader','book',1,'new-live-generation','idle',None,b'{}',b'{}','now','now',None))
    conn.execute('INSERT INTO analysis_executions(project,id,session_id,generation,cell_id,notebook_revision,source_sha256,language,state,output_id,status,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (PROJECT,'execution','session','historical-kernel-generation','cell',1,hashlib.sha256(b'print(1)').hexdigest(),'python','finished',OBJECT,'succeeded','now','now'))
    conn.execute('INSERT INTO analysis_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (PROJECT,OBJECT,'p'+'2'*32,generation,'execution','ready',c.encoded(seed),len(raw),4,c.digest(manifest),c.encoded(manifest),None,None))
    conn.commit()
    try:
        witness=output_reader.registered_witness(Records(db),Access(PROJECT,'reader',lambda *args:True),
            'execution',expected_output_id=OBJECT,expected_binding=c.digest(manifest))
        return witness,raw
    finally:conn.close()


@pytest.mark.parametrize('generation',[1,2**31-1])
@pytest.mark.parametrize('live_kernel',[False,True])
def test_registered_sqlite_artifact_to_real_configuration_owner_and_runtime(tmp_path,monkeypatch,generation,live_kernel):
    witness,raw=registered(generation)
    assert type(witness['artifact']['generation']) is int
    assert witness['provenance']['kernel_generation']=='historical-kernel-generation'
    assert witness['provenance']['output_generation']==generation
    assert 'table_handles' in witness
    config=dict(version=1,operation='output-page',witness=witness,offset=0,limit=8)
    assert media_worker.configuration(config) is config
    # Actual bounded worker reader on a private fixture, before any OCI double.
    payload=tmp_path/'state/objects'/OBJECT/'payload';payload.mkdir(parents=True)
    document=payload/'document.json';document.write_bytes(raw);document.chmod(0o400)
    page=output_worker.execute(config,state_root=tmp_path/'state')
    assert page['value']['items'][0]['representations'][0]['value']=='1\n'
    # Runtime index accepts only its bounded control directory, not state.
    control=tmp_path/'runtime';control.mkdir();tmp_path=control
    service,a,owner,_=fixture(tmp_path,monkeypatch,generation=live_kernel,registered_artifact=witness['artifact'])
    runtime=Runtime.__new__(Runtime)
    runtime.root=tmp_path;runtime.leases={};runtime.pidfds={};runtime.stop_fences=set();runtime.groups=service.groups
    runtime.deployment=SimpleNamespace(paths=FixedPaths(tmp_path/'image',tmp_path/'state',tmp_path/'cg',23456,23456),check_site=lambda:None)
    runtime.groups._root=lambda:None
    runtime.groups.verify=lambda *args:None
    runtime.groups.create=lambda identifier,role:dict(job_id=identifier,role=role,cgroup_identity=dict(device=1,inode=6),
        delegation_identity=Groups.root_identity,jobs_identity=Groups.jobs_identity)
    def watch(value):service._watch(value);return True
    runtime.register_watch=watch
    def command(value,operation):
        assert operation=='create'
        (tmp_path/value['job_id']/'init.pid').write_text(str(os.getpid()))
    runtime._command=command
    try:
        result=runtime.create(project=PROJECT,execution_id=owner['read_request_id'],role='media',config=config,
            web_parent=owner['web_parent'],owner_digest=c.digest(owner),read_owner=owner,authorize=lambda:True)
        identifier=result['job_id'];saved=leases.load(tmp_path,identifier)
        assert saved['owner_kind']=='media-read' and saved['owner_digest']==c.digest(owner)
        assert saved['deadline']==owner['deadline'] and identifier in service.watches
        actual=c.decode((tmp_path/identifier/'job/output-request.json').read_bytes())
        assert actual==config and actual['witness']['artifact']['generation']==generation
        assert read_control.load(tmp_path)['artifact']==witness['artifact']
        assert service.owner is None if not live_kernel else service.owner['generation']=='newer-kernel-generation'
    finally:
        for fd in runtime.pidfds.values():os.close(fd)
        close(service)


@pytest.mark.parametrize('bad',[True,False,1.0,0,-1,2**31,'1','output-generation'])
def test_registered_artifact_generation_cannot_be_coerced_at_either_boundary(tmp_path,bad):
    witness=copy.deepcopy(registered(1)[0]);witness['artifact']['generation']=bad
    with pytest.raises(ForgeError):
        media_worker.configuration(dict(version=1,operation='output-page',witness=witness,offset=0,limit=8))
    with pytest.raises(ForgeError):read_control.publish(tmp_path,authority(),'read-current',witness['artifact'])
    assert not (tmp_path/read_control.NAME).exists()
