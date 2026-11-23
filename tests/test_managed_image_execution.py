"""Checked native image execution with real loopback HTTP and synthetic pixels."""
from demiflow.forge.project import initialize_project
import importlib.util
import json
import os
from pathlib import Path
import sys
import hashlib
import io
import subprocess
import sqlite3

import lance
import pytest

from demiflow import data
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules,LIMITS
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from demiflow.forge.project_bindings import resolve_flow_resources
from demiflow.workbench.projects import workspace as ws
from demiflow.objects import ObjectRef
from demiflow.operator_llm.parser import parse_prompt_pack
from PIL import Image
from dataclasses import asdict
from demiflow.data.datasink import Datasink,WriteResult
from demiflow.execution.pipeline.run_context import current_action
from demiflow.execution.outputs.lance_sink import LanceSink
from demiflow.lance.model import LanceWriteSpec

from image_http_fixture import fixture_http,definition,json_definition

import demiflow
SOURCE=Path(demiflow.__file__).resolve().parent.parent


def setup(root,server,api='images_json',*,direct=False,vision=False,edit=False):
    initialize_project(root)
    for name in ('flows','operators','data'):(root/name).mkdir()
    control=ws.read_control(root/'workspace.json');expected=ws.content_digest(control)
    control['locations']['objects']={'root':'data','writable':True}
    ws.write_control(root/'workspace.json',control,expected=expected)
    pack=definition(server['base'],api,template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s']=10
    if vision:
        score=json_definition(server['base'])['prompts']['answer']
        score['template']='Score {{ pixels | image }}'
        pack['prompts']['score']=score
    if edit:
        edited=definition(server['base'],api)['prompts']['generate']
        edited['model']['timeout_s']=10
        pack['prompts']['edit']=edited
        (root/'operators/bundle.py').write_text('''from typing import TypedDict
class Ref(TypedDict):
    uri: str
    sha256: str
class Input(TypedDict):
    instruction: str
    image: Ref | None
    pixels: bytes | None
class Output(TypedDict):
    instruction: str
    original: Ref
    refs: list[bytes]
def bundle(row: Input) -> Output:
    image = row['image']
    pixels = row['pixels']
    if image is None or pixels is None:
        raise ValueError('Missing generated fixture image')
    return {'instruction': row['instruction'], 'original': image, 'refs': [pixels]}
''')
    (root/'flows/image.json').write_text(json.dumps(pack))
    settings={'object_store':str(root/'data'),'image_encoding':'preserve',
        'limits':{'max_images':2,'max_image_bytes':4096,'max_text_bytes':1024,
            'max_pixels':64,'max_total_pixels':128,'max_request_bytes':8192,'max_response_bytes':8192},
        'io_workers':1,'prepare_workers':1,'response_workers':1}
    code="from demiflow import data\n"
    if edit:code+='from operators.bundle import bundle\n'
    code+='def run(*, target):\n'
    code+="    result = data.from_items([{'instruction':'paint'}]).map_prompt_async('generate', config='flows/image.json', inputs={'instruction':'instruction'}, output='image', call_output='call', error_output='error', max_requests=2, options="+repr(settings)+")"
    code+=".read_object(reference='image', output='pixels', root="+repr(str(root/'data'))+", layout='sha256_prefix2', max_bytes=4096)"
    columns=['image','pixels']
    if edit:
        code+=".map(bundle).map_prompt_async('edit', config='flows/image.json', inputs={'instruction':'instruction','refs':'refs'}, output='edited', call_output='edit_call', error_output='edit_error', max_requests=2, options="+repr(settings)+")"
        code+=".read_object(reference='edited', output='edited_pixels', root="+repr(str(root/'data'))+", layout='sha256_prefix2', max_bytes=4096)"
        columns=['original','edited','edited_pixels']
    if vision:
        score_options={'trust_env':False,'timeout_s':10,'max_request_bytes':8192,'max_response_bytes':8192,
            'max_event_bytes':8192,'model_revision':'fixture-r1',
            'input_limits':{'max_images':1,'max_image_bytes':4096,'max_total_image_bytes':4096}}
        code+=".map_prompt_async('score', config='flows/image.json', inputs={'pixels':'pixels'}, output='score', max_requests=2, options="+repr(score_options)+")"
        columns.append('score')
    if not direct:code+='.materialize()'
    code+='\n'
    code+="    return result.select_columns("+repr(columns)+").write_lance(target, mode='create', return_receipt=True)\n"
    (root/'flows/image.py').write_text(code)
    target=str(root/'out.lance')
    catalog=inspect_modules(['demiflow.data'],SOURCE/'demiflow',LIMITS)
    report=FlowChecker(root,catalog,{},[target],resource_resolver=lambda tree,refs,bindings:
        resolve_flow_resources(root,tree,refs,bindings)).inspect('flows/image.py:run',{'target':target})
    return report,target


def load(path):
    name='image_flow_'+str(abs(hash(str(path))))
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module)
    return module


def test_checked_chain_http_object_ref_same_stream_read_and_lance(tmp_path,fixture_http):
    root=tmp_path/'project'
    report,target=setup(root,fixture_http)
    assert report['status']=='checked',report['diagnostics']
    context=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=root,resource_root=tmp_path/'control',limits={**DEFAULT_LIMITS,'memory_bytes':160*1024**2,
            'max_row_bytes':65536,'max_batch_bytes':262144},record=lambda *a:None,check_stop=lambda:None)
    flow=load(root/'flows/image.py')
    with RunContext.start(tmp_path/'run',limits={'memory_bytes':160*1024**2,'max_row_bytes':65536,
            'max_batch_bytes':262144,'max_output_bytes':1024**2,'max_scratch_bytes':2*1024**2}):
        with data.local_execution(workers=1,worker_mode='thread',partitions=1,memory_bytes=128*1024**2) as session:
            session._executor.resource_root=root
            with context.bind_context():receipt=flow.run(target=target)
    row,=lance.dataset(target).to_table().to_pylist()
    assert row['pixels']==fixture_http['raw']
    assert ObjectRef(**row['image']).read(max_bytes=4096)==row['pixels']
    assert '.demiflow-objects/' in row['image']['uri']
    assert len(fixture_http['requests'])==1 and receipt.status=='committed'


@pytest.mark.parametrize('api',['images','chat','chat_interleaved','openrouter_images'])
def test_declared_http_protocols_without_business_materialize(tmp_path,fixture_http,api):
    root=tmp_path/'project'
    report,target=setup(root,fixture_http,api,direct=True)
    assert report['status']=='checked',report['diagnostics']
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=root,resource_root=tmp_path/'control',limits={**DEFAULT_LIMITS,'memory_bytes':160*1024**2,
            'max_row_bytes':65536,'max_batch_bytes':262144},record=lambda *a:None,check_stop=lambda:None)
    flow=load(root/'flows/image.py')
    with RunContext.start(tmp_path/'run',limits={'memory_bytes':160*1024**2,'max_row_bytes':65536,
            'max_batch_bytes':262144,'max_output_bytes':1024**2,'max_scratch_bytes':2*1024**2}):
        with data.local_execution(workers=1,worker_mode='thread',partitions=1,memory_bytes=128*1024**2) as session:
            session._executor.resource_root=root
            with checked.bind_context():flow.run(target=target)
    row,=lance.dataset(target).to_table().to_pylist()
    assert row['pixels']==fixture_http['raw'] and len(fixture_http['requests'])==1


def test_same_graph_generation_read_object_and_vision_scoring(tmp_path,fixture_http,monkeypatch):
    monkeypatch.setenv('SYNTHETIC_JSON_KEY','fixture-only')
    root=tmp_path/'project'
    report,target=setup(root,fixture_http,direct=True,vision=True)
    assert report['status']=='checked',report['diagnostics']
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=root,resource_root=tmp_path/'control',limits={**DEFAULT_LIMITS,'memory_bytes':160*1024**2,
            'max_row_bytes':65536,'max_batch_bytes':262144},record=lambda *a:None,check_stop=lambda:None)
    flow=load(root/'flows/image.py')
    with RunContext.start(tmp_path/'run',limits={'memory_bytes':160*1024**2,'max_row_bytes':65536,
            'max_batch_bytes':262144,'max_output_bytes':1024**2,'max_scratch_bytes':2*1024**2}):
        with data.local_execution(workers=1,worker_mode='thread',partitions=1,memory_bytes=128*1024**2) as session:
            session._executor.resource_root=root
            with checked.bind_context():flow.run(target=target)
    row,=lance.dataset(target).to_table().to_pylist()
    assert row['pixels']==fixture_http['raw'] and row['score']=='ok'
    assert len(fixture_http['requests'])==2


def native_dataset(root,server,count=8,*,concurrency=1):
    pack=definition(server['base'],template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s']=10
    result=data.from_items([{'id':i,'instruction':'paint '+str(i)} for i in range(count)]).map_prompt_async('generate',
        config=parse_prompt_pack(json.dumps(pack)),inputs={'instruction':'instruction'},
        output='image',call_output='call',error_output='error',max_requests=count,concurrency=concurrency,
        options={'object_store':str(root),'image_encoding':'preserve','io_workers':1,'prepare_workers':1,'response_workers':1,
            'limits':{'max_images':2,'max_image_bytes':4096,'max_text_bytes':1024,'max_pixels':64,
                'max_total_pixels':128,'max_request_bytes':8192,'max_response_bytes':8192}})
    return result.read_object(reference='image',output='pixels',root=str(root),layout='sha256_prefix2',max_bytes=4096)


def run_limits():
    return {'memory_bytes':160*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144,
            'max_output_bytes':2*1024**2,'max_scratch_bytes':8*1024**2}


def test_native_bounded_take_commits_image_before_return(tmp_path,fixture_http):
    logical=tmp_path/'assets'
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        rows=native_dataset(logical,fixture_http).take(1)
    assert not rows[0].get('error'), rows[0]
    assert len(rows)==1 and ObjectRef(**rows[0]['image']).read(max_bytes=4096)==rows[0]['pixels']
    assert 1<=len(fixture_http['requests'])<=4


def test_take_zero_has_no_model_request_or_object_namespace(tmp_path,fixture_http):
    logical=tmp_path/'assets'
    assert native_dataset(logical,fixture_http).take(0)==[]
    assert not fixture_http['requests'] and not logical.exists()


def test_iterator_early_close_keeps_handed_images_and_cleans_unprepared(tmp_path,fixture_http):
    logical=tmp_path/'assets';logical.mkdir()
    (logical/'historical.bin').write_bytes(b'foreign-history')
    class UniqueRequests(list):
        def append(self,value):
            super().append(value)
            with Image.new('RGB',(4,4),(len(self)%255,0,0)) as picture,io.BytesIO() as out:
                picture.save(out,format='PNG');fixture_http['raw']=out.getvalue()
    fixture_http['requests']=UniqueRequests()
    with RunContext.start(tmp_path/'run',limits=run_limits()) as run:
        iterator=native_dataset(logical,fixture_http,64).iter_rows()
        first=next(iterator)
        iterator.close()
    assert ObjectRef(**first['image']).read(max_bytes=4096)==first['pixels']
    assert len(fixture_http['requests'])<64
    assert (logical/'historical.bin').read_bytes()==b'foreign-history'
    import sqlite3
    with sqlite3.connect(tmp_path/'run/run.sqlite') as db:
        prepared=[json.loads(r[0]) for r in db.execute('SELECT prepared FROM runtime_commits WHERE prepared IS NOT NULL')]
    hashes=set()
    def scan(value):
        if isinstance(value,dict):
            if 'sha256' in value:hashes.add(value['sha256'])
            for child in value.values():scan(child)
        elif isinstance(value,list):
            for child in value:scan(child)
    for value in prepared:scan(value)
    blobs=[p for p in logical.rglob('*') if p.is_file() and len(p.name)==64]
    assert all(p.name in hashes for p in blobs)


def test_stable_logical_destination_supports_independent_incremental_runs(tmp_path,fixture_http):
    logical=tmp_path/'assets';logical.mkdir()
    (logical/'historical.bin').write_bytes(b'preserve')
    references=[]
    for index in range(2):
        with RunContext.start(tmp_path/('run'+str(index)),limits=run_limits()):
            row,=native_dataset(logical,fixture_http,1).take(1)
            references.append(row['image'])
    assert references[0]['uri']!=references[1]['uri']
    assert references[0]['sha256']==references[1]['sha256']
    assert all(ObjectRef(**ref).read(max_bytes=4096)==fixture_http['raw'] for ref in references)
    assert (logical/'historical.bin').read_bytes()==b'preserve' and len(fixture_http['requests'])==2


def test_checked_same_graph_generation_read_and_edit(tmp_path,fixture_http,monkeypatch):
    root=tmp_path/'project'
    report,target=setup(root,fixture_http,direct=True,edit=True)
    assert report['status']=='checked',report['diagnostics']
    spec=importlib.util.spec_from_file_location('operators.bundle',root/'operators/bundle.py')
    helper=importlib.util.module_from_spec(spec);monkeypatch.setitem(sys.modules,'operators.bundle',helper)
    spec.loader.exec_module(helper)
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=root,resource_root=tmp_path/'control',limits={**DEFAULT_LIMITS,'memory_bytes':160*1024**2,
            'max_row_bytes':65536,'max_batch_bytes':262144},record=lambda *a:None,check_stop=lambda:None)
    flow=load(root/'flows/image.py')
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        with data.local_execution(workers=1,worker_mode='thread',partitions=1,memory_bytes=128*1024**2) as session:
            session._executor.resource_root=root
            with checked.bind_context():flow.run(target=target)
    row,=lance.dataset(target).to_table().to_pylist()
    assert ObjectRef(**row['original']).read(max_bytes=4096)==fixture_http['raw']
    assert ObjectRef(**row['edited']).read(max_bytes=4096)==row['edited_pixels']
    assert [r['path'] for r in fixture_http['requests']]==['/v1/images/generations','/v1/images/edits']
    encoded=fixture_http['requests'][1]['body']['image'][0].split(',',1)[1]
    import base64
    assert base64.b64decode(encoded)==fixture_http['raw']


def test_failed_downstream_preserves_known_effect_and_resumes_without_regeneration(tmp_path,fixture_http,monkeypatch):
    root=tmp_path/'assets'
    with RunContext.start(tmp_path/'first',limits=run_limits()):
        old,=native_dataset(root,fixture_http,1).take(1)
    def fail(row):
        if os.environ.get('NATIVE_IMAGE_DOWNSTREAM_FAILURE'):
            raise RuntimeError('controlled downstream failure')
        return row
    monkeypatch.setenv('NATIVE_IMAGE_DOWNSTREAM_FAILURE', '1')
    with pytest.raises(RuntimeError,match='controlled downstream'):
        with RunContext.start(tmp_path/'second',limits=run_limits()) as run:
            failed_namespace=root/'.demiflow-objects'/run.ref.run_id
            reference=run.ref
            native_dataset(root,fixture_http,1).map(fail).write_lance(str(tmp_path/'failed.lance'),mode='create')
    assert ObjectRef(**old['image']).read(max_bytes=4096)==old['pixels']
    assert [p for p in failed_namespace.rglob('*') if p.is_file() and len(p.name)==64]
    count=len(fixture_http['requests'])
    monkeypatch.delenv('NATIVE_IMAGE_DOWNSTREAM_FAILURE')
    with RunContext.resume(reference):
        native_dataset(root,fixture_http,1).map(fail).write_lance(str(tmp_path/'failed.lance'),mode='create')
    assert len(fixture_http['requests'])==count
    assert lance.dataset(str(tmp_path/'failed.lance')).count_rows()==1


@pytest.mark.parametrize('window,damage',[('after_provider_before_artifact',False),
    ('after_staging_before_prepared',False),('objects_committed_before_rows',False),
    ('object_publication_before_return',False),('after_acked',True)])
def test_http_process_fault_recovery_never_regenerates_prepared_images(tmp_path,fixture_http,window,damage):
    pack=definition(fixture_http['base'],template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s']=10
    (tmp_path/'pack.json').write_text(json.dumps(pack))
    script=Path(__file__).with_name('managed_image_fault_child.py')
    env={**os.environ,'PYTHONPATH':str(SOURCE),'PYTHONDONTWRITEBYTECODE':'1'}
    first=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),window],cwd=tmp_path,env=env,
        capture_output=True,text=True,timeout=40)
    assert first.returncode==73,first.stdout+first.stderr
    if damage:
        sha=hashlib.sha256(fixture_http['raw']).hexdigest()
        next((tmp_path/'assets').rglob(sha)).write_bytes(b'x'*len(fixture_http['raw']))
    second=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'resume'],cwd=tmp_path,env=env,
        capture_output=True,text=True,timeout=40)
    assert len(fixture_http['requests'])==1
    if window == 'after_provider_before_artifact':
        assert second.returncode!=0 and 'automatic regeneration is forbidden' in second.stderr
        assert not (tmp_path/'out.lance').exists()
    elif damage:
        assert second.returncode!=0 and 'checksum differs' in second.stderr
    else:
        assert second.returncode==0,second.stdout+second.stderr
        row,=lance.dataset(str(tmp_path/'out.lance')).to_table().to_pylist()
        assert ObjectRef(**row['image']).read(max_bytes=4096)==fixture_http['raw']


class FixtureDatasink(Datasink):
    sink_id='fixture'
    def __init__(self,target):self.target=target
    def recovery_identity(self):return {'target':self.target,'mode':'create'}
    def native(self):return LanceSink(current_action(),LanceWriteSpec(uri=self.target,mode='create'))
    def prepare(self,blocks,context):return self.native().prepare(blocks,context)
    def inspect(self,prepared,context):
        native=self.native().inspect(prepared,context)
        if native is None:return None
        return {'commit_id':context.commit_id,'sink_id':self.sink_id,'status':'committed',
            'data':asdict(WriteResult(written_rows=prepared['rows'],blocks_written=1,target=self.target))}
    def commit(self,prepared,context):
        self.native().commit(prepared,context)
        return self.inspect(prepared,context)
    def abort(self,prepared,context):return self.native().abort(prepared,context)
    def write(self,blocks,context):raise AssertionError('Durable native prepare must be used')


def test_native_formal_datasink_accepts_same_stream_image_rows(tmp_path,fixture_http):
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        native_dataset(tmp_path/'assets',fixture_http,1).select_columns(['image','pixels']).write_datasink(
            FixtureDatasink(str(tmp_path/'sink.lance')))
    row,=lance.dataset(str(tmp_path/'sink.lance')).to_table().to_pylist()
    assert ObjectRef(**row['image']).read(max_bytes=4096)==row['pixels']==fixture_http['raw']


@pytest.mark.parametrize('failure',['gzip','invalid_pixels','too_many_pixels','multiple','bad_base64','illegal_uri'])
def test_response_boundaries_do_not_publish_invalid_images(tmp_path,fixture_http,failure):
    if failure=='gzip':fixture_http['mode']='gzip_large'
    elif failure=='invalid_pixels':fixture_http['raw']=b'invalid PNG pixels'
    elif failure=='too_many_pixels':
        with Image.new('RGB',(16,16)) as image,io.BytesIO() as output:
            image.save(output,format='PNG');fixture_http['raw']=output.getvalue()
    elif failure=='multiple':fixture_http['response_override']={'data':[{'url':'x'},{'url':'y'}]}
    elif failure=='bad_base64':fixture_http['response_override']={'data':[{'b64_json':'!!!'}]}
    else:fixture_http['response_override']={'data':[{'url':'file:///etc/hosts'}]}
    root=tmp_path/'assets'
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        row,=native_dataset(root,fixture_http,1).take(1)
    assert row['image'] is None and row['error'] and row['pixels'] is None
    assert not [p for p in root.rglob('*') if p.is_file() and len(p.name)==64]


def test_native_run_stream_keeps_generated_images_without_request_journal(tmp_path,fixture_http):
    result_file = tmp_path / 'worker-rows.jsonl'
    def collect(row):
        # Observe real worker output without assuming shared driver memory.
        with result_file.open('a') as stream:
            stream.write(json.dumps({'image': row['image'], 'pixels': row['pixels'].hex()}) + '\n')
        return row
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        native_dataset(tmp_path/'assets',fixture_http,1).map(collect).run_stream()
    rows = [json.loads(line) for line in result_file.read_text().splitlines()]
    assert len(rows)==1 and ObjectRef(**rows[0]['image']).read(max_bytes=4096)==bytes.fromhex(rows[0]['pixels'])


def test_native_iterator_batches_keep_requested_grouping_and_readable_images(tmp_path,fixture_http):
    with RunContext.start(tmp_path/'run',limits=run_limits()):
        iterator=native_dataset(tmp_path/'assets',fixture_http,3).iter_batches(batch_size=2,batch_format='pyarrow')
        batches=list(iterator)
    assert [b.num_rows for b in batches]==[2,1]
    assert len(fixture_http['requests'])==3
    for batch in batches:
        for row in batch.to_pylist():
            assert ObjectRef(**row['image']).read(max_bytes=4096)==row['pixels']


def test_terminal_batch_byte_budget_checked_before_arrow_allocation(tmp_path,monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    import pyarrow as pa

    calls=[]
    monkeypatch.setattr(pa,'Table',type('Table',(),{'from_pylist':staticmethod(lambda rows:calls.append(rows))}))
    with pytest.raises(DataflowError, match='batch byte budget'):
        with RunContext.start(tmp_path/'run', limits={'max_row_bytes':1024, 'max_batch_bytes':1024}):
            items=data.from_items([{'payload':'x'*650}, {'payload':'y'*650}])
            list(items.iter_batches(batch_size=1000000, batch_format='pyarrow'))
    assert calls==[]


from demiflow.execution.operator_bindings.dataflow_model_factory import PromptFactory as OriginalPromptFactory


class DelayedNativePromptFactory:
    """A sealed worker fault: delay completion after the real native response."""
    def __new__(cls, operation, settings):
        import asyncio
        import os
        from pathlib import Path
        # Retain the real constructor before replacing the declaration factory.
        # Thread workers share the same module as the compiler.
        actor = OriginalPromptFactory(operation, settings)
        original = actor._runtime.call_with_trace
        root = Path(operation.options['object_store'])
        async def delayed(name, values):
            result = await original(name, values)
            if os.getenv('DEMIFLOW_TEST_PENDING_NATIVE') == '1':
                root.mkdir(parents=True, exist_ok=True)
                if values['instruction'] == 'paint 7':
                    (root / 'fast-known').write_text('response confirmed')
                if values['instruction'] == 'paint 0':
                    (root / 'slow-known').write_text('response confirmed')
                    for _ in range(2000):
                        if (root / 'release-slow').exists():
                            break
                        await asyncio.sleep(.01)
                    else:
                        raise RuntimeError('fast native slot made no progress across checkpoints')
            return result
        actor._runtime.call_with_trace = delayed
        return actor


def test_known_inflight_model_response_crosses_barrier_and_recovers_once(tmp_path, fixture_http, monkeypatch):
    import demiflow.execution.commit as protocol
    import demiflow.execution.operator_bindings.dataflow_model_factory as factories
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    from demiflow.execution.seal.sealed_plans import sealed_read_bounds
    from demiflow.execution.storage.objects import readonly_run_store
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.contracts import state_codec
    from demiflow.execution.kernels.operator import IMAGE_MEDIA_TYPE
    monkeypatch.setattr(factories, 'PromptFactory', DelayedNativePromptFactory)
    monkeypatch.setenv('DEMIFLOW_TEST_PENDING_NATIVE', '1')
    release = tmp_path / 'assets' / 'release-slow'
    cuts = []
    def fail(name, commit_id):
        fact = RunJournal(tmp_path/'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['intent']['sinks'] == ['materialize']:
            cuts.append(fact)
    monkeypatch.setattr(protocol, 'fault_point', fail)
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    pump = DataflowExecution.pump
    def stop_after_known_response(execution, **kwargs):
        status = pump(execution, **kwargs)
        if (tmp_path/'assets'/'fast-known').exists() and len(cuts) >= 2:
            with RunJournal(tmp_path/'run')._connection() as db:
                known, unknown = db.execute(
                    'SELECT count(response_json), count(*) - count(response_json) FROM calls').fetchone()
            if known >= 8 and unknown == 0:
                # Stop while the worker still awaits a confirmed RPC receipt.
                # A received HTTP body alone must never authorize regeneration.
                assert (tmp_path/'assets'/'slow-known').exists()
                release.write_text('release')
                raise RuntimeError('controlled published checkpoint failure')
        return status
    monkeypatch.setattr(DataflowExecution, 'pump', stop_after_known_response)
    limits = {**run_limits(), 'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}
    try:
        with pytest.raises(RuntimeError, match='controlled published checkpoint failure'):
            with RunContext.start(tmp_path/'run', limits=limits) as run:
                reference = run.ref
                native_dataset(tmp_path/'assets', fixture_http, 12, concurrency=2).materialize()
    finally:
        release.parent.mkdir(parents=True, exist_ok=True)
        release.write_text('release')
    assert len(cuts) >= 2, 'the fast slot must progress through multiple real checkpoints'
    first = cuts[0]
    carrier = StateArtifacts(run).read(first['prepared']['state']['dataflow-cut-v3'])
    assert not carrier['cut']['final']
    marker = run.journal.source_binding('action-1-dataflow-sealed')
    bounds = sealed_read_bounds()
    store = readonly_run_store(run.directory, store_id=marker['physical_ref']['store_id'], bounds=bounds)
    pending = []
    for contribution in carrier['contributions']:
        for value in contribution['operator_states'].values():
            ref = ArtifactRef(**value)
            if ref.media_type == IMAGE_MEDIA_TYPE:
                image = state_codec.loads(b''.join(store.read(ref, max_bytes=bounds.max_state_bytes)),
                                          max_bytes=bounds.max_state_bytes)
                pending.extend(image['inflight_calls'])
    assert len(pending) >= 1
    assert all(item['input'] is not None and len(item['scope']) == 64 for item in pending)
    monkeypatch.delenv('DEMIFLOW_TEST_PENDING_NATIVE')
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    monkeypatch.setattr(DataflowExecution, 'pump', pump)
    with RunContext.resume(reference):
        rows = native_dataset(tmp_path/'assets', fixture_http, 12, concurrency=2).materialize().take_all()
    assert sorted(row['id'] for row in rows) == list(range(12))
    assert all(row.get('error') is None for row in rows), [(r['id'], r.get('error')) for r in rows]
    assert len(fixture_http['requests']) == 12
    import pyarrow as pa
    schemas = [pa.Table.from_pylist([row]).schema for row in rows]
    assert all(schema == schemas[0] for schema in schemas)
