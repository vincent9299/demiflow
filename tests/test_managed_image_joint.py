"""Latest P0–P2/resource + image joint acceptance, using actual Forge lifecycle.

All HTTP is bounded loopback synthetic PNG/JSON. No provider or GPU is used.
"""
from demiflow.forge.project import initialize_project
from _forge_project_fixtures import declare_existing_flow
import base64
import contextlib
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

import lance
import pyarrow as pa
from PIL import Image
import pytest

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.objects import ObjectRef
from image_http_fixture import definition, json_definition


@pytest.fixture
def joint_http():
    state = dict(requests=[], active=0, peak=0, lock=threading.Lock())
    class Server(ThreadingHTTPServer):
        daemon_threads = True
        block_on_close = False
        request_queue_size = 8
        slots = threading.BoundedSemaphore(8)
        def process_request(self,request,address):
            if not self.slots.acquire(timeout=1):
                self.shutdown_request(request);return
            try:super().process_request(request,address)
            except BaseException:
                self.slots.release();raise
        def process_request_thread(self,request,address):
            try:super().process_request_thread(request,address)
            finally:self.slots.release()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def setup(self):
            super().setup();self.connection.settimeout(5)
        def log_message(self, *args): pass
        def do_POST(self):
            length = int(self.headers.get('Content-Length', 0))
            if not 0 < length <= 32768:
                self.send_error(413);return
            body = json.loads(self.rfile.read(length))
            with state['lock']:
                if len(state['requests']) >= 512 or state['active'] >= 4:
                    self.send_error(429);return
                state['requests'].append(dict(path=self.path, body=body))
                state['active'] += 1
                state['peak'] = max(state['peak'], state['active'])
            try:
                time.sleep(.02)
                if self.path.startswith('/json/'):
                    result = {'choices':[{'message':{'content':'{"answer":"ok"}'}}]}
                else:
                    # Each prompt has stable distinct bytes. Retries cannot be
                    # hidden by a same-hash object or a request cache.
                    prompt = body.get('prompt', '')
                    color = tuple(hashlib.sha256(prompt.encode()).digest()[:3])
                    with Image.new('RGB', (4, 4), color) as image, io.BytesIO() as out:
                        image.save(out, format='PNG');raw = out.getvalue()
                    result = {'data':[{'b64_json':base64.b64encode(raw).decode()}]}
                raw = json.dumps(result).encode()
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(raw)));self.end_headers()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):self.wfile.write(raw)
            finally:
                with state['lock']:state['active'] -= 1
    server = Server(('127.0.0.1', 0), Handler)
    state['base'] = f'http://127.0.0.1:{server.server_port}/v1'
    thread = threading.Thread(target=lambda:server.serve_forever(poll_interval=.02), daemon=True)
    thread.start()
    try:yield state
    finally:
        server.shutdown();server.server_close();thread.join(2)


@pytest.fixture(scope='module')
def image_workspace_base(tmp_path_factory):
    root = tmp_path_factory.mktemp('joint-base')/'workspace'
    initialize_project(root)
    for name in ('flows','operators','tables','data'):(root/name).mkdir()
    control = ws.read_control(root/'workspace.json');expected = ws.content_digest(control)
    control['locations']['local'] = dict(root='data',writable=True)
    ws.write_control(root/'workspace.json',control,expected=expected)
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root,expected=workspace['platform']['digest'],extras=['llm','images'])
    return root


def budgets(root, run_id):
    with sqlite3.connect(runs.journal(root,run_id).path) as db:
        return dict(db.execute('SELECT name,consumed FROM runtime_budgets'))


def make_run(root, server, *, count=2, chain=False, materialize=False, limits=None, max_requests=None):
    pack = definition(server['base'],template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s'] = 5
    options = dict(object_store=str(root/'data'),image_encoding='preserve',io_workers=2,prepare_workers=2,response_workers=2,
        limits=dict(max_images=2,max_image_bytes=4096,max_text_bytes=1024,max_pixels=64,max_total_pixels=128,max_request_bytes=8192,max_response_bytes=8192))
    items = [dict(instruction='paint-'+str(i)) for i in range(count)]
    concurrency = 1 if max_requests is not None else 2
    fixed_input = count>128
    if fixed_input:
        schema=pa.schema([pa.field('instruction',pa.string(),nullable=False)])
        lance.write_dataset(pa.Table.from_pylist(items,schema=schema),str(root/'data/input.lance'))
        project_ops.attach_table(root,'input',relative_uri='input.lance',version=1,dataset_id='input',schema_name='input',schema_version='1')
    code = 'from demiflow import data\n'
    if chain:
        score = json_definition(server['base'])['prompts']['answer'];score['template'] = 'Score {{ pixels | image }}'
        pack['prompts']['score'] = score
        pack['prompts']['edit'] = definition(server['base'])['prompts']['generate']
        pack['prompts']['edit']['model']['timeout_s'] = 5
        (root/'operators/bundle.py').write_text('''from typing import TypedDict
class Ref(TypedDict):
    uri: str
    sha256: str
class Input(TypedDict):
    instruction: str
    image: Ref | None
    pixels: bytes | None
    score: str | None
class Output(TypedDict):
    instruction: str
    original: Ref
    refs: list[bytes]
    score: str
def bundle(row: Input) -> Output:
    image = row['image']
    pixels = row['pixels']
    score = row['score']
    if image is None or pixels is None or score is None:
        raise ValueError('Missing real generated/read/scored fixture result')
    return {'instruction':row['instruction'],'original':image,'refs':[pixels],'score':score}
''')
        code += 'from operators.bundle import bundle\n'
    code += 'def run(*, '+('source, ' if fixed_input else '')+'target):\n'
    source = "data.read_lance(source['uri'],version=source['version'],columns=['instruction'])" if fixed_input else 'data.from_items('+repr(items)+')'
    code += '    result = '+source+".map_prompt_async('generate',config='flows/image.json',inputs={'instruction':'instruction'},output='image',call_output='call',error_output='error',max_requests="+repr(max_requests or count)+',concurrency='+str(concurrency)+',queue_depth=2,options='+repr(options)+')'
    code += ".read_object(reference='image',output='pixels',root="+repr(str(root/'data'))+",layout='sha256_prefix2',max_bytes=4096)"
    columns = ['instruction','image','pixels','error']
    if chain:
        score_options = dict(trust_env=False,timeout_s=2,max_request_bytes=8192,max_response_bytes=8192,max_event_bytes=8192,model_revision='fixture-r1',input_limits=dict(max_images=1,max_image_bytes=4096,max_total_image_bytes=4096))
        code += ".map_prompt_async('score',config='flows/image.json',inputs={'pixels':'pixels'},output='score',max_requests="+str(count)+',concurrency=2,queue_depth=2,options='+repr(score_options)+')'
        code += ".map(bundle).map_prompt_async('edit',config='flows/image.json',inputs={'instruction':'instruction','refs':'refs'},output='edited',call_output='edit_call',error_output='edit_error',max_requests="+str(count)+',concurrency=2,queue_depth=2,options='+repr(options)+')'
        code += ".read_object(reference='edited',output='edited_pixels',root="+repr(str(root/'data'))+",layout='sha256_prefix2',max_bytes=4096)"
        columns = ['instruction','original','score','edited','edited_pixels']
    if materialize:code += '.materialize()'
    code += '\n    return result.select_columns('+repr(columns)+").write_lance(target['uri'],mode='create',return_receipt=True)\n"
    (root/'flows/image.py').write_text(code)
    (root/'flows/image.json').write_text(json.dumps(pack))
    params = dict(format='demiflow_forge_flow_parameters_v1',arguments=dict(target=dict(kind='output',location_id='local',relative_uri='joint.lance')))
    if fixed_input:params['arguments']['source']=dict(kind='table',alias='input')
    (root/'flows/params.json').write_text(json.dumps(params))
    declare_existing_flow(root,'flows/image.py:run',params='flows/params.json')
    report = project_ops.inspect_flow(root,'flows/image.py:run',params='flows/params.json')
    assert report['status'] == 'checked',report['diagnostics']
    limit = dict(timeout_s=480,memory_bytes=160*1024**2,max_scratch_bytes=32*1024**2,max_output_bytes=2*1024**2,
        max_evidence_bytes=8*1024**2,max_row_bytes=65536,max_batch_bytes=262144,batch_rows=16,log_bytes=1024**2)
    limit.update(limits or {})
    return runs.prepare(root,'flows/image.py:run',params='flows/params.json',limits=limit)


def stop(root, run_id, timeout=240):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        value = runs.show(root,run_id)
        if value['status'] in runs.TERMINAL and not any(runs.process_matches(value['state'].get(name)) for name in ('worker','supervisor')):
            return value
        time.sleep(.1)
    runs.cancel(root,run_id)
    raise AssertionError(('Joint Forge run did not stop',runs.show(root,run_id),runs.logs(root,run_id)))


def output_rows(value):
    return lance.dataset(value['outputs'][0]['uri']).to_table().to_pylist()


def save_facts(root, prepared, result, server, **more):
    facts = dict(run_id=prepared['run_id'],prepared_digest=prepared['digest'],status=result['status'],attempt=result['attempt'],
        requests=server['requests'],peak_http_concurrency=server['peak'],budgets=budgets(root,prepared['run_id']),limits=result['limits'],state=result['state'],**more)
    (root/'joint_acceptance_facts.json').write_text(json.dumps(facts,indent=2)+'\n')


def launch_observed(root, value, window, monkeypatch, *, resume=False):
    (root/'joint_fault.json').write_text(json.dumps(dict(window=window,
        image_checkpoint_fixture=(root/'flows/image.py').is_file())))
    driver = str(Path(__file__).with_name('joint_forge_fault_driver.py'))
    original = subprocess.Popen
    target = str(runs.directory(root,value['run_id'])/'runtime/demiflow/workbench/runs/worker.py')
    def launch(command,*args,**kwargs):
        if isinstance(command,list) and len(command)>4 and command[3]==target and command[4]=='supervise':
            command = command[:3]+[driver,target]+command[4:]
        return original(command,*args,**kwargs)
    with monkeypatch.context() as scope:
        scope.setattr(subprocess,'Popen',launch)
        if resume:
            runs.resume(root,value['run_id'],expected=value['digest'],expected_attempt=1)
        else:
            runs.start(root,value['run_id'],expected=value['digest'])


def start_fault(root, value, window, monkeypatch):
    launch_observed(root,value,window,monkeypatch)
    deadline = time.monotonic()+220
    marker = root/'joint_fault_ready.json'
    while time.monotonic()<deadline:
        if marker.exists():return json.loads(marker.read_text())
        current = runs.show(root,value['run_id'])
        assert current['status'] not in runs.TERMINAL,(current,runs.logs(root,value['run_id']))
        time.sleep(.05)
    runs.cancel(root,value['run_id'])
    raise AssertionError(('Joint fault window not reached',runs.show(root,value['run_id']),runs.logs(root,value['run_id'])))


def test_actual_forge_generate_read_score_edit_direct_sink(image_workspace_base,tmp_path,joint_http,monkeypatch):
    root = tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    monkeypatch.setenv('SYNTHETIC_JSON_KEY','fixture-only')
    value = make_run(root,joint_http,chain=True)
    assert not joint_http['requests']
    launch_observed(root,value,'observe_only',monkeypatch)
    result = stop(root,value['run_id'])
    assert result['status']=='succeeded',(result,runs.logs(root,value['run_id']))
    rows = output_rows(result)
    assert len(rows)==2 and len(joint_http['requests'])==6
    assert all(row['score']=='ok' and ObjectRef(**row['edited']).read(max_bytes=4096)==row['edited_pixels'] for row in rows)
    assert all(ObjectRef(**row['original']).read(max_bytes=4096) for row in rows)
    assert lance.dataset(result['outputs'][0]['uri']).version==1
    consumed = budgets(root,value['run_id'])
    assert consumed['requests']==6 and consumed['output_bytes']==consumed['object_bytes']+consumed['row_bytes']
    assert 1<=joint_http['peak']<=2
    before = len(joint_http['requests'])
    assert runs.start(root,value['run_id'],expected=value['digest'])['status']=='succeeded'
    assert len(joint_http['requests'])==before
    save_facts(root,value,result,joint_http)


@pytest.mark.parametrize('window',['after_prepared','after_acked'])
def test_actual_forge_multibatch_cancel_resume_prefix(image_workspace_base,tmp_path,joint_http,monkeypatch,window):
    root = tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    logical = root/'data';(logical/'foreign.bin').write_bytes(b'foreign-history')
    value = make_run(root,joint_http,count=130,materialize=True)
    ready = start_fault(root,value,window,monkeypatch)
    assert ready['fact']['status']==('prepared' if window=='after_prepared' else 'acked')
    assert 128 <= len(joint_http['requests']) <= 130
    assert sum(source['next_sequence'] for source in ready['cut']['dataflow.sources.v3']) == 128
    assert not ready['cut']['cut']['final']
    runs.cancel(root,value['run_id']);previous = stop(root,value['run_id'])
    assert previous['status']=='cancelled' and previous['state']['usage_complete'] is True
    original_limits = runs.journal(root,value['run_id']).recovery_identity()['limits']
    before_budgets = budgets(root,value['run_id'])
    launch_observed(root,value,'observe_only',monkeypatch,resume=True)
    result = stop(root,value['run_id'])
    assert result['status']=='succeeded',(result,runs.logs(root,value['run_id']))
    rows = output_rows(result)
    assert len(rows)==130 and len(joint_http['requests'])==130
    assert len({r['instruction'] for r in rows})==130
    assert all(ObjectRef(**r['image']).read(max_bytes=4096)==r['pixels'] for r in rows)
    assert lance.dataset(result['outputs'][0]['uri']).version==1
    assert (logical/'foreign.bin').read_bytes()==b'foreign-history'
    assert runs.show(root,value['run_id'],attempt=1)==previous
    consumed = budgets(root,value['run_id'])
    assert consumed['requests']==130 and consumed['output_bytes']==consumed['object_bytes']+consumed['row_bytes']
    assert consumed['output_bytes']>=before_budgets['output_bytes'] and consumed['output_bytes']<=value['limits']['max_output_bytes']
    assert runs.journal(root,value['run_id']).recovery_identity()['limits']==original_limits
    commits = runs.journal(root,value['run_id']).execution_commits(ready['fact']['action_id'])
    outputs = [f for f in commits if f['intent']['sinks']==['materialize']]
    from types import SimpleNamespace
    from demiflow.execution.state_artifacts import StateArtifacts
    states = StateArtifacts(SimpleNamespace(directory=runs.directory(root, value['run_id']), limits=original_limits))
    cuts = [states.read(f['prepared']['state']['dataflow-cut-v3']) for f in outputs]
    assert [sum(source['next_sequence'] for source in cut['dataflow.sources.v3']) for cut in cuts] == [128,130]
    assert [cut['cut']['final'] for cut in cuts] == [False, True]
    assert all(f['status']=='acked' for f in outputs) and joint_http['peak']<=2
    assert len({r['body']['prompt'] for r in joint_http['requests']})==130
    save_facts(root,value,result,joint_http,prefix_window=window,prefix_fact=ready['fact'],before_budgets=before_budgets)


@pytest.mark.parametrize('window,damage',[('after_prepared',False),('after_acked',False),('after_acked',True),('after_prepared',True)])
def test_actual_forge_final_image_sink_resume_never_republishes(image_workspace_base,tmp_path,joint_http,monkeypatch,window,damage):
    root=tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    value=make_run(root,joint_http,count=2)
    ready=start_fault(root,value,'final_'+window,monkeypatch)
    assert ready['fact']['status']==('prepared' if window=='after_prepared' else 'acked')
    uri=value['outputs'][0]['uri']
    assert len(joint_http['requests'])==2
    if window=='after_prepared':assert not Path(uri).exists()
    else:assert lance.dataset(uri).version==1
    runs.cancel(root,value['run_id']);previous=stop(root,value['run_id'])
    assert previous['status']=='cancelled' and previous['state']['usage_complete'] is True
    consumed=budgets(root,value['run_id'])
    published_before_resume = Path(uri).exists()
    if window == 'after_acked': assert published_before_resume
    if published_before_resume: assert lance.dataset(uri).version == 1
    if damage:
        item=ready['fact']['prepared']['sinks']['write_lance']['objects']['objects'][0]
        path=next((root/'data/.demiflow-objects').rglob(item['sha256']))
        path.write_bytes(b'x'*path.stat().st_size)
        with pytest.raises(ValueError,match='checksum differs'):
            launch_observed(root,value,'observe_only',monkeypatch,resume=True)
        result=runs.show(root,value['run_id'])
        assert result['attempt']==1 and len(joint_http['requests'])==2
        assert Path(uri).exists() == published_before_resume
        if published_before_resume: assert lance.dataset(uri).version == 1
        save_facts(root,value,result,joint_http,final_sink_window=window,expected_blocked=True,publication_before_cancel=window=='after_acked',publication_before_resume=published_before_resume)
        return
    launch_observed(root,value,'observe_only',monkeypatch,resume=True)
    result=stop(root,value['run_id'])
    assert result['status']=='succeeded',(result,runs.logs(root,value['run_id']))
    rows=output_rows(result)
    assert len(rows)==2 and len(joint_http['requests'])==2 and lance.dataset(uri).version==1
    assert all(ObjectRef(**r['image']).read(max_bytes=4096)==r['pixels'] for r in rows)
    final=budgets(root,value['run_id'])
    assert all(final[k]==consumed[k] for k in ['requests','object_bytes','row_bytes','output_bytes','output_rows'])
    save_facts(root,value,result,joint_http,final_sink_window=window,publication_before_cancel=window=='after_acked',before_budgets=consumed)


@pytest.mark.parametrize('window',['after_provider_before_artifact','corrupt_committed'])
def test_actual_forge_unknown_or_corrupt_images_block_regeneration(image_workspace_base,tmp_path,joint_http,monkeypatch,window):
    root = tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    corrupt = window=='corrupt_committed'
    value = make_run(root,joint_http,count=130 if corrupt else 2,materialize=True)
    ready = start_fault(root,value,'after_acked' if corrupt else window,monkeypatch)
    runs.cancel(root,value['run_id']);previous = stop(root,value['run_id'])
    assert previous['status']=='cancelled' and previous['state']['usage_complete'] is True
    before = len(joint_http['requests'])
    if corrupt:
        fact = ready['fact']['prepared']['sinks']['materialize']['objects']['objects'][0]
        path = next((root/'data/.demiflow-objects').rglob(fact['sha256']))
        path.write_bytes(b'x'*path.stat().st_size)
    launch_observed(root,value,'observe_only',monkeypatch,resume=True)
    result = stop(root,value['run_id'])
    assert result['status']=='failed',(result,runs.logs(root,value['run_id']))
    text = runs.logs(root,value['run_id'])['text']
    assert ('checksum differs' if corrupt else 'automatic regeneration is forbidden') in text,text
    assert len(joint_http['requests'])==before and not result['outputs'][0]['result']
    assert not Path(result['outputs'][0]['uri']).exists()
    save_facts(root,value,result,joint_http,expected_blocked=True,blocking_window=window)


@pytest.mark.parametrize('kind',['output','scratch'])
def test_actual_forge_small_shared_budgets_fail_without_publication(image_workspace_base,tmp_path,joint_http,monkeypatch,kind):
    root = tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    limits = {'max_output_bytes':16} if kind=='output' else {'max_scratch_bytes':65536}
    value = make_run(root,joint_http,limits=limits)
    launch_observed(root,value,'observe_only',monkeypatch)
    result = stop(root,value['run_id'])
    assert result['status']=='failed',(result,runs.logs(root,value['run_id']))
    assert not result['outputs'][0]['result'] and not Path(result['outputs'][0]['uri']).exists()
    assert len(joint_http['requests'])==0
    text = runs.logs(root,value['run_id'])['text']
    assert ('budget' in text.lower() or 'exhausted' in text.lower()),text
    save_facts(root,value,result,joint_http,expected_budget_rejection=kind)


def test_actual_forge_node_request_cap_preserves_declared_error_rows(image_workspace_base,tmp_path,joint_http,monkeypatch):
    root=tmp_path/'workspace';shutil.copytree(image_workspace_base,root)
    value=make_run(root,joint_http,max_requests=1)
    launch_observed(root,value,'observe_only',monkeypatch)
    result=stop(root,value['run_id'])
    assert result['status']=='succeeded',(result,runs.logs(root,value['run_id']))
    rows=sorted(output_rows(result),key=lambda r:r['instruction'])
    assert len(rows)==2 and len(joint_http['requests'])==budgets(root,value['run_id'])['requests']==1
    assert rows[0]['error'] is None and ObjectRef(**rows[0]['image']).read(max_bytes=4096)==rows[0]['pixels']
    assert rows[1]['image'] is rows[1]['pixels'] is None and 'PromptBudgetExceededError' in rows[1]['error']
    assert lance.dataset(result['outputs'][0]['uri']).version==1
    save_facts(root,value,result,joint_http,expected_native_node_request_error='PromptBudgetExceededError; error_output retains the failed row')


def test_native_run_request_ceiling_fails_before_second_http_and_cannot_reset(tmp_path,joint_http):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.operator_llm.parser import parse_prompt_pack
    pack=definition(joint_http['base'],template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s']=5
    def dataset():
        return data.from_items([{'instruction':'global-0'},{'instruction':'global-1'}]).map_prompt_async('generate',
            config=parse_prompt_pack(json.dumps(pack)),inputs={'instruction':'instruction'},output='image',call_output='call',error_output='error',max_requests=2,
            concurrency=1,queue_depth=1,options={'object_store':str(tmp_path/'assets'),'image_encoding':'preserve','io_workers':1,'prepare_workers':1,'response_workers':1,
            'limits':{'max_images':2,'max_image_bytes':4096,'max_text_bytes':1024,'max_pixels':64,'max_total_pixels':128,'max_request_bytes':8192,'max_response_bytes':8192}})
    with pytest.raises(RecoveryContractError,match='budget.*exhausted'):
        with RunContext.start(tmp_path/'run',limits={'max_requests':1,'memory_bytes':160*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144}) as run:
            reference=run.ref
            dataset().write_lance(str(tmp_path/'out.lance'),mode='create')
    assert len(joint_http['requests'])==1 and not (tmp_path/'out.lance').exists()
    with sqlite3.connect(tmp_path/'run/run.sqlite') as db:
        assert db.execute("SELECT consumed,ceiling FROM runtime_budgets WHERE name='requests'").fetchone()==(1,1)
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
    # The first response is now durable even before the output cut. Resume may
    # reuse it, but cannot replenish the exhausted budget for the second row.
    with pytest.raises(RecoveryContractError,match='budget.*exhausted'):
        with RunContext.resume(reference):dataset().write_lance(str(tmp_path/'out.lance'),mode='create')
    assert len(joint_http['requests'])==1 and not (tmp_path/'out.lance').exists()


def test_native_image_delivery_remains_explicitly_nonresumable(tmp_path,joint_http):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.operator_llm.parser import parse_prompt_pack
    pack = definition(joint_http['base'],template='{{ instruction }}')
    pack['prompts']['generate']['model']['timeout_s']=2
    def dataset():
        return data.from_items([{'instruction':'delivery'}]).map_prompt_async('generate',config=parse_prompt_pack(json.dumps(pack)),
            inputs={'instruction':'instruction'},output='image',call_output='call',error_output='error',max_requests=1,
            options={'object_store':str(tmp_path/'assets'),'image_encoding':'preserve','io_workers':1,'prepare_workers':1,'response_workers':1,
                'limits':{'max_images':2,'max_image_bytes':4096,'max_text_bytes':1024,'max_pixels':64,'max_total_pixels':128,'max_request_bytes':8192,'max_response_bytes':8192}})
    with pytest.raises(RuntimeError,match='controlled interruption'):
        with RunContext.start(tmp_path/'run',limits={'memory_bytes':160*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144}) as run:
            reference=run.ref
            rows=dataset().take(1)
            raise RuntimeError('controlled interruption after delivery')
    assert len(joint_http['requests'])==1
    # Historical selector: unified Dataset take now seals a final ACK.
    # Reuse that exact committed result; no replay of the paid operation.
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(reference.directory)
    before = journal.execution_commits('action-1')
    assert before and all(fact['status'] == 'acked' for fact in before)
    with RunContext.resume(reference):
        assert dataset().take(1) == rows
    assert journal.execution_commits('action-1') == before
    assert len(joint_http['requests'])==1 and ObjectRef(**rows[0]['image']).read(max_bytes=4096)
