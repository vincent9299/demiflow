"""Owned Case UI metadata fixture, with real sources and native read workers.

The authority transport is an explicit test adapter. No admitted analysis
runtime, kernel, OCI container, production project or paid model is enabled.
"""
import json
from pathlib import Path
import sys
import os
import hashlib
import uuid

# Optional read-only integrator backend for additive registration tests.
# UI/assets and this helper remain in the Case lane; no shared file is edited.
if os.environ.get('FORGE_CASE_ORIGIN_BACKEND_ROOT'):
    sys.path.insert(0,os.environ['FORGE_CASE_ORIGIN_BACKEND_ROOT'])

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.analysis.database import Database
from demiflow.workbench.analysis.database import FileSizeGuard
from demiflow.workbench.analysis.metadata import Metadata
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.sources import Sources
from demiflow.forge.schema import ForgeError
from demiflow.workbench.worker import operation as native


def register_output_fixture(records,authority,payload):
    """Sealed output metadata fixture, not a kernel/admitted output job.

    The real reader derives the witness, parses the bounded immutable output
    file, and presentations.register performs its real SQLite validation. Only
    execution/object producer facts are seeded here; registration is not faked.
    """
    from demiflow.workbench.analysis import output_reader
    from demiflow.workbench.analysis import outputs
    from demiflow.workbench.analysis import presentations
    from demiflow.workbench.analysis.object_files import candidate_manifest
    access=authority.access();descriptor=payload['descriptor']
    source=records.source(access,descriptor['source_snapshot']);c.case_view(descriptor,source)
    execution='e'+uuid.uuid4().hex;session='s'+uuid.uuid4().hex;artifact='o'+uuid.uuid4().hex
    generation='origin-fixture-generation';code='# sealed output metadata fixture; no kernel executed'
    document=dict(title='Case origin fixture',cells=[dict(id='cell',language='python',source=code)])
    saved=records.create_notebook(access,request_id='book'+uuid.uuid4().hex,document=document)
    output=dict(output_type='display_data',data={outputs.CASE_MIME:descriptor},metadata={})
    raw=c.encoded(dict(generation=generation,parent_msg_id='fixture-parent',status='succeeded',complete=True,
        outputs=[output],output_bytes=len(c.encoded(output)),messages=1,ignored_messages=0,stdin_supported=False,
        active_widgets_supported=False,execution_id=execution,cell_id='cell',source_sha256=hashlib.sha256(code.encode()).hexdigest(),
        language='python',session_id=session,final_status='succeeded',raw_fd_output_collected=False),maximum=48*1024)
    seed=dict(project=access.project,object_id=artifact,generation=1,retention='ephemeral',execution_id=execution,parents=[],source_binding=None)
    info=dict(format='document',mime=outputs.OUTPUT_MIME,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),complete=True)
    manifest=candidate_manifest(seed,info,{'document.json':dict(bytes=len(raw),sha256=info['sha256'])})
    state_root=records.database.directory.parent;path=state_root/'objects'/artifact/'payload'/'document.json';path.parent.mkdir(parents=True)
    path.write_bytes(raw);path.chmod(0o400)
    with records.database.transaction(payload_bytes=128*1024) as db:
        db.execute('INSERT INTO analysis_sessions(project,id,principal,notebook_id,notebook_revision,generation,state,job_id,sources,web_parent,created,updated,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (access.project,session,access.principal,saved['id'],1,generation,'idle',None,c.encoded({'cases':source['digest']}),b'{}','fixture','fixture',None))
        db.execute('INSERT INTO analysis_executions(project,id,session_id,generation,cell_id,notebook_revision,source_sha256,language,state,output_id,status,created,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (access.project,execution,session,generation,'cell',1,hashlib.sha256(code.encode()).hexdigest(),'python','finished',artifact,'succeeded','fixture','fixture'))
        db.execute('INSERT INTO analysis_objects(project,id,publish_id,generation,execution_id,state,seed,held_bytes,held_entries,manifest_digest,manifest,prepared,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (access.project,artifact,'p'+uuid.uuid4().hex,1,execution,'ready',c.encoded(seed),len(raw),4,c.digest(manifest),c.encoded(manifest),None,None))
    witness=output_reader.registered_witness(records,access,execution,expected_output_id=artifact,expected_binding=c.digest(manifest))
    actual=output_reader.open_document(state_root,witness)
    projection=output_reader.project_document(actual,witness)
    unregistered=projection['items'][0]['representations'][0]['definition_witness']
    if payload.get('register',True):projection=presentations.register(records,access,witness=witness,projection=projection)
    representation=projection['items'][0]['representations'][0];proof=representation['definition_witness']
    # Keep an exact hash inventory of loaded backend Python inputs for review.
    package_root=Path(c.__file__).resolve().parents[2]
    inputs={}
    for module in tuple(sys.modules.values()):
        filename=getattr(module,'__file__',None)
        if filename and filename.endswith('.py'):
            candidate=Path(filename).resolve()
            if candidate.is_relative_to(package_root) and (getattr(module,'__name__','').startswith('demiflow.workbench.analysis.') or getattr(module,'__name__','') in {'demiflow.forge.schema','demiflow.workbench.projects.workspace','demiflow.workbench.worker'}):inputs[str(candidate)]=hashlib.sha256(candidate.read_bytes()).hexdigest()
    return dict(descriptor=representation['value'],origin=proof['origin'],annotation_origin_registered=proof['registered'],
        provenance=projection['provenance'],output_index=projection['items'][0]['index'],
        fixture=dict(kind='sealed_metadata_not_kernel_or_B1',raw_sha256=info['sha256'],unregistered_witness=unregistered,
            registered=proof['registered'],backend_sources=inputs))

def main():
    guard=FileSizeGuard(database_bytes=16*1024**2,wal_bytes=8*1024**2).install()
    database=Database(Path(sys.argv[1]),guard=guard)
    records=Records(database)
    metadata=Metadata(records,Sources(records,database.directory.parent))
    print(json.dumps(dict(ready=True)),flush=True)
    try:
        while True:
            line=sys.stdin.buffer.readline(65537)
            if not line:break
            if len(line)>65536:raise RuntimeError('fixture request exceeded 64KiB')
            command=json.loads(line)
            root=Path(command['root']);project=command['project'];grants=command['permissions']
            class Authority:
                principal='case-ui-fixture-owner'
                def access(self):
                    return Access(project,self.principal,lambda p,u,g:p==project and u==self.principal and bool(grants.get(g)))
                def resolve_source(self,locator):
                    self.access().check();return web_sources.resolve(root,locator)
                def verify_source(self,source,target=None,*,locator):
                    self.access().check();return web_sources.verify(root,source=source,target=target,locator=locator)==dict(verified=True)
                def call(self,name,payload):
                    assert name=='source-query' and payload['purpose']=='catalog'
                    self.access().check()
                    value=native('analysis_source_read',root,**{key:payload[key] for key in ('source_locator','source_snapshot','query')})
                    self.access().check();return value
            try:
                result=register_output_fixture(records,Authority(),command['payload']) if command['name']=='fixture-output-register' else metadata.route(Authority(),command['name'],command['payload'])
                print(c.encoded(dict(result=result)).decode(),flush=True)
            except ForgeError as error:
                print(json.dumps(dict(code=error.code,error=str(error))),flush=True)
    finally:database.close()


if __name__=='__main__':main()
