"""V027 actor-owner loss; original ArtifactRefs remain recovery authority.

The business probe deliberately creates an unconfirmed Ray cache entry. This
is not a claim that v3's data channel stores records in Ray ObjectRefs: it does
not. The real HostedDataflowWorker owns this transient test cache, while the
unmodified native commit owns only durable artifact references.
"""
from __future__ import annotations
import json
import hashlib
import os
from pathlib import Path
import shutil
import time
from unittest.mock import patch
from contextlib import contextmanager


def _ephemeral_producer(row, *, directory):
    import ray
    import ray.cloudpickle
    import os
    from pathlib import Path
    context = ray.get_runtime_context()
    # Borrowed by the driver solely to verify owner-death semantics. No ref is
    # returned as a Record or submitted to the original commit protocol.
    ref = ray.put({'id':row['id'], 'unconfirmed':b'x'*(256*1024)})
    name = str(row['id']) + '-' + context.get_actor_id()
    path = Path(directory)/(name+'.pickle')
    raw = ray.cloudpickle.dumps(ref)
    with path.open('wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return {'id':row['id'], 'actor_id':context.get_actor_id(), 'pid':os.getpid()}


def _dead_actor(ray, actor, *, timeout=10):
    end = time.monotonic()+timeout
    while time.monotonic()<end:
        state = ray._private.state.actors(actor._actor_id.hex())
        if state['State'] == 'DEAD':
            return {'actor_id':actor._actor_id.hex(), 'state':state['State'],
                'pid':state.get('Pid'), 'node_id':state['Address']['NodeID'],
                'num_restarts':state.get('NumRestarts')}
        time.sleep(.02)
    raise AssertionError('owned real worker actor did not die')


def producer_loss(fx):
    import ray
    import ray.cloudpickle
    from demiflow import data
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_residency_scenarios import _provider
    root = fx.journal_dir/'producer-owner-loss'
    cache_refs = root/'transient-business-cache'
    cache_refs.mkdir(parents=True)
    lost, attempts, owned_actors = {}, [], []
    original = DataflowExecution._advance_cut
    def advance(execution):
        result = original(execution)
        image = execution._restore_image
        if image is None or image.cut.final or lost:
            return result
        fact = execution._action.context.journal.execution_commit(image.commit_id)
        assert fact['status'] == 'acked'
        transform = {n.occurrence_id for n in execution._graph.nodes if n.kind == 'transform'}
        matches = [(allocation,endpoint) for allocation,endpoint in execution._endpoints.items()
            if any(transform.intersection(s.occurrences) for s in endpoint._assignment.stages)]
        assert len(matches) == 1
        allocation, endpoint = matches[0]
        actor = endpoint._connector._actor(allocation)
        owned_actors.append(actor)
        actor_id = actor._actor_id.hex()
        before = ray._private.state.actors(actor_id)
        assert before['State'] == 'ALIVE'
        files = sorted(cache_refs.glob('*-'+actor_id+'.pickle'))
        assert files, 'actual producer made no transient object'
        locator = ray.cloudpickle.loads(files[0].read_bytes())
        assert isinstance(locator, ray.ObjectRef)
        lost.update({'actor_id':actor_id,'allocation_id':allocation,
            'borrowed_ref':locator.hex(),'borrowed_before_kill':True,
            'ack_before_kill':{'commit_id':image.commit_id,'cut_id':image.cut.cut_id,
                               'status':fact['status'],'final':image.cut.final},
            'plan_id':execution._physical.plan_id,'first_attempt':execution._attempt(),
            'state_before':before['State'], 'cache_ref_count_before':len(files)})
        from demiflow.execution.contracts.dataflow_codec import encode_wire
        raw = encode_wire(image,type_name='RecoveryImage',bounds=execution._bounds)
        assert locator.hex().encode() not in raw
        lost['durable_image_sha256'] = hashlib.sha256(raw).hexdigest()
        lost['committed_rows'] = execution._publication(image)['manifest']['total_rows']
        ray.kill(actor,no_restart=True)
        lost['death'] = _dead_actor(ray, actor)
        assert lost['death']['num_restarts'] == 0
        # A completed put can remain readable from a borrower's cached copy.
        # Neither outcome creates recovery authority. The separate real model
        # case kills an actually pending original actor RPC and locks its loss.
        try:
            cached = ray.get(locator, timeout=10)
        except (ray.exceptions.OwnerDiedError, ray.exceptions.ObjectLostError) as error:
            lost['completed_cache'] = {'status':'lost','type':type(error).__name__, 'message':str(error)}
        else:
            assert cached['id']==int(files[0].name.split('-')[0]) and len(cached['unconfirmed'])==256*1024
            lost['completed_cache'] = {'status':'readable','bytes':len(cached['unconfirmed']),
                'payload_sha256':hashlib.sha256(cached['unconfirmed']).hexdigest()}
        return result  # original runtime must discover actor loss; no test exception
    def pipeline():
        return data.range(12).map(_ephemeral_producer, fn_kwargs={'directory':str(cache_refs)})
    limits = {'checkpoint_rows':2,'checkpoint_interval_ms':60000,'timeout_s':60,
        'max_row_bytes':65536,'max_batch_bytes':262144,'max_state_bytes':2*1024**2,
        'memory_bytes':512*1024**2,'max_scratch_bytes':512*1024**2}
    with _provider(fx, workers=3, fusion=False):
        failure = None
        try:
            with RunContext.start(root/'run',limits=limits) as run:
                reference, first_fence = run.ref, run.fence
                with patch.object(DataflowExecution, '_advance_cut', advance):
                    pipeline().take_all()
        except DataflowError as error:
            failure = {'type':type(error).__name__,'code':error.code,'message':str(error)}
        assert lost and failure and failure['code'] == 'UNKNOWN_EFFECT', (lost,failure)
        before = run.journal.execution_commits('action-1')
        assert before and before[-1]['status'] == 'acked'
        # Only the dead attempt's test-owned code cache is removed. Original
        # store/journal/artifact roots are never removed or reconstructed.
        worker_cache = root/'run'/'worker-cache'
        cached_files = list(worker_cache.rglob('*')) if worker_cache.exists() else []
        if worker_cache.exists():
            shutil.rmtree(worker_cache)
        lost['dead_worker_cache_removed'] = not worker_cache.exists()
        lost['dead_worker_cache_entries'] = len(cached_files)
        assert cached_files, 'must remove an actual dead-worker code cache'
        with RunContext.resume(reference) as resumed:
            assert resumed.fence != first_fence and resumed.attempt == lost['first_attempt']+1
            output = pipeline().take_all()
            after = resumed.journal.execution_commits('action-1')
            assert after[:len(before)] == before and after[-1]['status'] == 'acked'
            assert sorted(row['id'] for row in output) == list(range(12))
            assert len(output) == 12
            for row_id in range(lost['committed_rows']):
                assert len(list(cache_refs.glob(str(row_id)+'-*.pickle'))) == 1, 'committed producer row was recomputed'
            sealed = resumed.journal.source_binding('action-1-dataflow-sealed')
            assert sealed['plan_id']==lost['plan_id']
            attempts.append({'attempt':resumed.attempt,'fence':resumed.fence,
                             'plan_id':sealed['plan_id'],'final_status':after[-1]['status'],
                             'final_commit_id':after[-1]['commit_id']})
        lost['after_resume_death'] = _dead_actor(ray, actor=owned_actors[0])
        assert lost['after_resume_death']['state']=='DEAD' and lost['after_resume_death']['num_restarts']==0
    return {'loss':lost,'initial_error':failure,'resumed':attempts,
        'output':output,'original_commits_preserved':len(before),
        'original_durable_store_present':(root/'run'/'.dataflow-store').is_dir(),
        'scope':'real producer HostedDataflowWorker actor loss; no physical host outage'}


@contextmanager
def _accepted_http(root, on_request):
    """The only external model transport substitute; it never confirms a reply."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    calls, errors = [], []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                size = int(self.headers['Content-Length'])
                assert 0<size<=128*1024
                value = {'path':self.path,'request':json.loads(self.rfile.read(size))}
                calls.append(value)
                with (root/'accepted-http.jsonl').open('a') as stream:
                    stream.write(json.dumps(value)+'\n');stream.flush();os.fsync(stream.fileno())
                on_request(value)
            except BaseException as error:
                errors.append(error)
            finally:
                self.close_connection = True
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        yield calls, 'http://127.0.0.1:'+str(server.server_port), errors
    finally:
        server.shutdown();server.server_close();thread.join(5)
        assert not thread.is_alive()


def unknown_model_owner_loss(fx, kind):
    import ray
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.run_context import RunContext
    from runtime_v3_recovery_scenarios import _model_pipeline, _native_calls
    from runtime_v3_residency_scenarios import _provider
    root = fx.journal_dir/('unknown-owner-'+kind)
    root.mkdir(parents=True)
    original = DataflowExecution._launch_phase
    current, deaths, first_errors = [], [], []
    def launched(execution):
        result = original(execution)
        if not current:
            current.append(execution)
        return result
    def accepted(request):
        assert len(deaths)==0, 'provider received a duplicated unknown paid request'
        execution = current[0]
        model_ids = {n.occurrence_id for n in execution._graph.nodes if n.kernel.kind in {'prompt','agent','embedding'}}
        matching = [(allocation,endpoint) for allocation,endpoint in execution._endpoints.items()
            if any(model_ids.intersection(stage.occurrences) for stage in endpoint._assignment.stages)]
        assert len(matching)==1
        allocation, endpoint = matching[0]
        actor = endpoint._connector._actor(allocation)
        state = ray._private.state.actors(actor._actor_id.hex())
        assert state['State']=='ALIVE'
        before = _native_calls(execution._action.context.journal)
        assert before['requests_consumed']==1 and before['unresolved']==1 and before['confirmed']==0
        death = {'actor_id':actor._actor_id.hex(),'allocation_id':allocation,'native_at_accept':before,
                 'original_actor':actor, 'state_before':state['State']}
        deaths.append(death)
        # Existing production lifecycle method, not an injected actor method.
        # It must drain the accepted HTTP call whose external fixture has not
        # returned any response, so this real actor task stays unconfirmed.
        pending = actor.close.remote()
        ready, _ = ray.wait([pending],timeout=.05)
        assert not ready, 'original close RPC completed before the owned fault boundary'
        death['pending_rpc'] = {'method':'HostedDataflowWorker.close', 'ref':pending.hex(),
                                'completed_before_kill':False}
        ray.kill(actor,no_restart=True)
        death['confirmed_dead'] = _dead_actor(ray,actor)
        assert death['confirmed_dead']['num_restarts']==0
        try:
            ray.get(pending,timeout=10)
        except ray.exceptions.RayActorError as error:
            death['pending_rpc']['error'] = {'type':type(error).__name__,'message':str(error)}
        else:
            raise AssertionError('pending actor RPC did not fail when its worker died')
    with _accepted_http(root,accepted) as (http,url,errors), \
            patch.dict(os.environ,{'DF_V3_NATIVE_TEST_URL':url+'/'+kind+'/v1','DF_V3_NATIVE_TEST_KEY':'owned-test-only'}), \
            _provider(fx,workers=3,fusion=False):
        try:
            with RunContext.start(root/'run',limits={'timeout_s':60,'max_requests':1,
                'checkpoint_rows':1,'checkpoint_interval_ms':60000}) as run:
                reference, original_fence, original_attempt = run.ref,run.fence,run.attempt
                with patch.object(DataflowExecution,'_launch_phase',launched):
                    _model_pipeline(kind).take_all()
        except Exception as error:
            first_errors.append({'type':type(error).__name__,'code':getattr(error,'code',None),'message':str(error)})
        assert not errors, errors
        assert len(http)==1 and len(deaths)==1 and first_errors
        assert first_errors[0]['type']=='DataflowError' and first_errors[0]['code']=='UNKNOWN_EFFECT', first_errors
        before = _native_calls(run.journal)
        assert before['unresolved']==1 and before['confirmed']==0 and before['requests_consumed']==1
        assert not run.journal.execution_commits('action-1')
        resumed_error = None
        try:
            with RunContext.resume(reference) as resumed:
                assert resumed.attempt==original_attempt+1 and resumed.fence!=original_fence
                _model_pipeline(kind).take_all()
        except Exception as error:
            resumed_error = {'type':type(error).__name__,'message':str(error)}
        assert resumed_error and resumed_error['type']=='UncertainPromptCall', resumed_error
        assert 'automatic regeneration is forbidden' in resumed_error['message']
        assert len(http)==1 and not errors
        after = _native_calls(resumed.journal)
        assert after['keys']==before['keys'] and after['requests_consumed']==1
        assert after['unresolved']==1 and after['confirmed']==0
        assert not resumed.journal.execution_commits('action-1')
        death = deaths[0]
        death['after_resume_dead'] = _dead_actor(ray,death.pop('original_actor'))
        assert death['after_resume_dead']['num_restarts']==0
        return {'kind':kind,'death':death,'initial_error':first_errors[0],
            'resume_error':resumed_error,'before_resume':before,'after_resume':after,
            'http':http,'attempt_before':original_attempt,'attempt_after':resumed.attempt,
            'fence_before':original_fence,'fence_after':resumed.fence,'commits':[]}


def validate_evidence(value):
    assert set(value) == {'producer','models','fault_scope'}
    producer = value['producer']
    loss = producer['loss']
    assert loss['borrowed_before_kill'] and loss['borrowed_ref']
    assert loss['ack_before_kill']['status']=='acked' and loss['ack_before_kill']['final'] is False
    cache=loss['completed_cache']
    assert cache['status'] in {'lost','readable'}
    if cache['status']=='lost':assert cache['type'] in {'OwnerDiedError','ObjectLostError'}
    else:assert cache['bytes']==256*1024 and cache['payload_sha256']
    assert loss['death']['state']==loss['after_resume_death']['state']=='DEAD'
    assert loss['death']['num_restarts']==loss['after_resume_death']['num_restarts']==0
    assert producer['initial_error']['code']=='UNKNOWN_EFFECT'
    assert loss['dead_worker_cache_removed'] and loss['dead_worker_cache_entries']>0
    assert producer['original_durable_store_present'] and producer['original_commits_preserved']>0
    assert len(producer['output'])==12 and sorted(r['id'] for r in producer['output'])==list(range(12))
    resumed, = producer['resumed']
    assert resumed['attempt']==loss['first_attempt']+1 and resumed['final_status']=='acked'
    assert resumed['plan_id']==loss['plan_id']
    assert {case['kind'] for case in value['models']}=={'prompt','embedding','agent'} and len(value['models'])==3
    for case in value['models']:
        assert case['initial_error']['code']=='UNKNOWN_EFFECT' and case['resume_error']['type']=='UncertainPromptCall'
        assert 'automatic regeneration is forbidden' in case['resume_error']['message']
        assert len(case['http'])==1 and not case['commits']
        assert case['attempt_after']==case['attempt_before']+1 and case['fence_after']!=case['fence_before']
        before, after = case['before_resume'],case['after_resume']
        assert before['keys']==after['keys'] and len(before['keys'])==1
        assert before['requests_consumed']==after['requests_consumed']==1
        assert before['unresolved']==after['unresolved']==1 and before['confirmed']==after['confirmed']==0
        assert case['death']['confirmed_dead']['state']==case['death']['after_resume_dead']['state']=='DEAD'
        assert case['death']['confirmed_dead']['num_restarts']==case['death']['after_resume_dead']['num_restarts']==0
        rpc=case['death']['pending_rpc']
        assert rpc['method']=='HostedDataflowWorker.close' and rpc['ref'] and rpc['completed_before_kill'] is False
        assert rpc['error']['type'] in {'ActorDiedError','RayActorError'}
    assert value['fault_scope']=='actual actor owner loss; same-host Ray; no physical outage or network partition'


def node_loss(fx, fault=None):
    from runtime_v3_support import NotRunError
    assert fault is None, 'node-loss gate executes its whole finite failure matrix'
    if fx.provider != 'ray_core':
        detail={'missing_environment':'real Ray Core actor owners; a Local process is not a Ray actor'}
        fx._record_not_run('node_loss',detail)
        raise NotRunError('node_loss',detail)
    producer = producer_loss(fx)
    models = [unknown_model_owner_loss(fx,kind) for kind in ('prompt','embedding','agent')]
    value = {'producer':producer,'models':models,
        'fault_scope':'actual actor owner loss; same-host Ray; no physical outage or network partition'}
    validate_evidence(value)
    return value
