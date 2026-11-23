"""V017: public business routing and native exchange contracts stay distinct.

Only observers and the external HTTP transport belong to this harness. All
source reads, worker executions, channel snapshots, leases and commits belong
to the original engine. An actual worker is killed after a nonfinal native ACK.
"""
from __future__ import annotations
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import threading
import time
from unittest.mock import patch


def _shared(row, *, directory):
    import json
    import os
    from pathlib import Path
    path = Path(directory)/('source-'+str(os.getpid())+'.jsonl')
    with path.open('a') as stream:
        stream.write(json.dumps({'i':row['i'],'pid':os.getpid()})+'\n')
        stream.flush()
        os.fsync(stream.fileno())
    return {**row,'source_pid':os.getpid()}


async def _echo(row):
    import os
    return {**row,'branch':'echo','consumer_pid':os.getpid()}


def _model_result(row):
    import os
    return {**row,'branch':'model','consumer_pid':os.getpid()}


async def _identity(row):
    return row


@contextmanager
def _model_server():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    calls, lock = [], threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            size = int(self.headers['Content-Length'])
            assert 0 < size <= 65536
            body = json.loads(self.rfile.read(size))
            with lock:
                calls.append(body)
            time.sleep(.03 if body['model']=='route-a' else .01)
            raw = json.dumps({'choices':[{'message':{'content':json.dumps({'result':body['model']})}}]}).encode()
            self.send_response(200)
            self.send_header('Content-Length',str(len(raw)))
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(raw)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with patch.dict(os.environ,{'DF_V3_NATIVE_TEST_URL':f'http://127.0.0.1:{server.server_port}/v1',
                                   'DF_V3_NATIVE_TEST_KEY':'route-recovery-test'}):
            yield calls
    finally:
        server.shutdown();server.server_close();thread.join(5)
        assert not thread.is_alive()


def _channel_images(coordinator,image,*,parked=False):
    from demiflow.execution.contracts.dataflow import ArtifactRef
    values=[]
    for contribution in image.contributions:
        for name,document in contribution.channels.items():
            ref=ArtifactRef(**dict(document))
            raw=b''.join(coordinator._store.read(ref,max_bytes=coordinator._physical.bounds.max_control_bytes))
            state=json.loads(raw)
            origin = contribution.cut_id if parked else image.cut.cut_id
            assert state['cut_id']==origin and state['channel_id']==name
            values.append({'participant':contribution.participant_id,'ref':asdict(ref),'state':state})
    return values


def _kill_owned_worker(execution,fx):
    # The PID/actor is taken from an authenticated real ready event belonging
    # to this execution, not a process-name scan or a caller-supplied PID.
    candidates=[(key,endpoint,execution._current_worker_observations[key])
        for key,endpoint in execution._endpoints.items()
        if key in execution._current_worker_observations and key not in execution._done]
    assert candidates,'no active worker at the nonfinal cut'
    key,endpoint,observation=candidates[0]
    pid=observation['worker_process_id']
    assert pid!=os.getpid() and (Path('/proc')/str(pid)).exists()
    fact={'allocation_id':key,'boot_id':endpoint._boot_id,**observation}
    if fx.provider=='ray_core':
        import ray
        from runtime_v3_node_loss_scenarios import _dead_actor
        actor=endpoint._connector._actor(key)
        before=ray._private.state.actors(actor._actor_id.hex())
        assert before['State']=='ALIVE'
        ray.kill(actor,no_restart=True)
        fact.update(kind='actual Ray HostedDataflowWorker loss',actor_id=actor._actor_id.hex(),
                    death=_dead_actor(ray,actor))
    else:
        # Verify the exact worker owns this execution's control socket/launch.
        command=(Path('/proc')/str(pid)/'cmdline').read_bytes().decode(errors='replace')
        assert 'demiflow.execution.worker.bootstrap' in command,command
        os.kill(pid,signal.SIGKILL)
        fact.update(kind='actual Local worker SIGKILL',signal=signal.SIGKILL,command=command)
    return fact


def _recover(fx,name,build,*,eligible=None,workers=8,first_run=None,parked=False):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow import thaw_record_value
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from runtime_v3_residency_scenarios import _provider,_cut_facts
    from runtime_v3_support import _ExecuteActionRecorder,_RayDataBlocker
    root=fx.journal_dir/name
    cuts,events,duplicates,loss=[],[],[],{}
    commit=DataflowCheckpointCoordinator.commit
    handle=DataflowExecution._handle_event
    advance=DataflowExecution._advance_cut
    def committed(coordinator,*args,**kwargs):
        image=commit(coordinator,*args,**kwargs)
        facts=_cut_facts(coordinator,image)
        facts['channels']=_channel_images(coordinator,image,parked=parked)
        facts['attempt']=coordinator._action.context.attempt
        cuts.append(facts)
        return image
    def observed(execution,event):
        value=thaw_record_value(event.payload) if event.payload is not None else {}
        row=None
        if event.kind=='data':
            from demiflow.execution.contracts.record_codec import _decode_record_document
            from demiflow.execution.contracts.dataflow import ArtifactRef
            if 'ref' in value:
                raw=b''.join(execution._store.read(ArtifactRef(**value['ref']),max_bytes=execution._bounds.max_row_bytes))
            else:
                from demiflow.execution.exchange.dataflow_transport import read_peer
                raw=read_peer(value['peer'],bounds=execution._bounds)
            record=_decode_record_document(raw,record_id=value['record_id'],sequence=value['sequence'],
                                           max_bytes=execution._bounds.max_row_bytes)
            row=thaw_record_value(record.inline)
        result=handle(execution,event)
        if event.kind in ('data','ack','ready'):
            assert len(events)<4096,'finite route evidence budget exceeded'
            events.append({'kind':event.kind,'attempt':event.attempt,'allocation_id':event.allocation_id,
                           'boot_id':event.boot_id,'payload':value,'row':row})
        durable = execution._durable_residency if parked else None
        stored = (execution._exchange._parked.get(value['delivery_id'])
                  if event.kind=='data' and durable is not None else None)
        if event.kind=='data' and not duplicates and (stored is not None if parked else value['delivery_id'] in execution._exchange._inflight):
            from copy import deepcopy
            before=dict(execution._exchange._inflight)
            retained=deepcopy(execution._exchange._parked) if durable is not None else None
            counters=(execution._confirmed_rows,execution._terminal_seen)
            action=execution.request.action
            prefix=action.context.journal.execution_commits(action.action_id)
            if stored is not None:
                # Replay exactly the data envelope already owned by the native
                # durable ledger, never invent an absent duplicate sample.
                assert stored['payload']==value
            try:handle(execution,event)
            except DataflowError as error:
                assert error.code=='INVALID_CONTRACT' and 'duplicate' in str(error)
                assert execution._exchange._inflight==before
                assert durable is None or execution._exchange._parked==retained
                assert (execution._confirmed_rows,execution._terminal_seen)==counters
                assert action.context.journal.execution_commits(action.action_id)==prefix
                duplicates.append({'delivery_id':value['delivery_id'],'error':str(error),
                    'source':'native durable entries' if stored is not None else 'native in-flight deliveries',
                    'outstanding_unchanged':len(before),'durable_entries_unchanged':len(retained) if retained is not None else None,
                    'confirmed_rows_unchanged':counters[0],'terminal_seen_unchanged':counters[1],
                    'commit_prefix_unchanged':len(prefix)})
            else:raise AssertionError('duplicate real in-flight delivery admitted')
        return result
    def boundary(execution):
        result=advance(execution)
        image=execution._restore_image
        if image is None or image.cut.final or loss:return result
        facts=next(c for c in reversed(cuts) if c['commit_id']==image.commit_id)
        # A finite phase can keep an older restore image while a later live
        # phase emits uncommitted data. Kill only after that actual live route
        # owns a positive ordinal IN the current original committed image.
        from demiflow.execution.contracts.dataflow_codec import compute_participant_id
        owners={p:s.stage_id for s in execution._physical.stages for p in s.partitions}
        specs={e.edge_id:e for e in execution._physical.exchanges}
        saved={(c['participant'],c['state']['channel_id']):c['state'] for c in facts['channels']}
        qualified=False
        for event in events:
            if event['kind']!='data' or event['attempt']!=execution._attempt():continue
            payload=event['payload'];spec=specs[payload['edge_id']];producer=payload['producer_partition']
            state=saved.get((compute_participant_id(owners[producer],producer),spec.channel_id))
            if state and state['routing_ordinal']>0:qualified=True;break
        if not qualified:return result
        if eligible is not None and not eligible(facts):return result
        loss.update(boundary=facts,plan_id=execution._physical.plan_id,dag_id=execution._graph.dag_id,
                    attempt=execution._attempt(),fence=execution._fence(),
                    worker=_kill_owned_worker(execution,fx))
        return result
    limits={'checkpoint_rows':2,'checkpoint_interval_ms':60000,'timeout_s':120,
        'max_row_bytes':65536,'max_batch_bytes':262144,'max_state_bytes':2*1024**2,
        'memory_bytes':512*1024**2,'max_scratch_bytes':1024**3}
    with _provider(fx,workers=workers,fusion=False),_ExecuteActionRecorder() as recorder,_RayDataBlocker() as blocker, \
            patch.object(DataflowCheckpointCoordinator,'commit',committed), \
            patch.object(DataflowExecution,'_handle_event',observed):
        error=None
        try:
            with RunContext.start(root,limits=limits) as run:
                ref=run.ref
                with patch.object(DataflowExecution,'_advance_cut',boundary):
                    if first_run is None:build().take_all()
                    else:first_run(build(),run)
        except (DataflowError,BrokenPipeError,ConnectionResetError) as failure:
            error={'type':type(failure).__name__,'code':getattr(failure,'code',None),'message':str(failure)}
        except Exception as failure:
            # With concurrent journal replies, the very next RPC may observe
            # the actual actor killed above before the poller translates loss.
            # Only that already authenticated victim/death is admissible.
            if not parked or fx.provider!='ray_core' or not loss:
                raise
            import ray
            if (not isinstance(failure,ray.exceptions.ActorDiedError)
                    or failure.actor_id!=loss['worker']['actor_id']):
                raise
            assert loss['worker']['death']['state']=='DEAD'
            error={'type':type(failure).__name__,'code':getattr(failure,'code',None),'message':str(failure),
                   'actor_id':failure.actor_id,'authenticated_ray_death':True}
        assert loss and error,(loss,error)
        assert (error['code']=='UNKNOWN_EFFECT' or error['type'] in ('BrokenPipeError','ConnectionResetError')
                or error.get('authenticated_ray_death') is True),error
        before=run.journal.execution_commits('action-1')
        assert before[-1]['status']=='acked' and not loss['boundary']['final']
        with RunContext.resume(ref) as resumed:
            assert resumed.attempt==loss['attempt']+1 and resumed.fence!=loss['fence']
            rows=build().take_all()
            after=resumed.journal.execution_commits('action-1')
            assert after[:len(before)]==before and after[-1]['status']=='acked'
            attempts={'old':loss['attempt'],'new':resumed.attempt,'old_fence':loss['fence'],'new_fence':resumed.fence}
        bundle=load_sealed_plan(ref.directory,'action-1')
        assert bundle['physical'].plan_id==loss['plan_id'] and bundle['graph'].dag_id==loss['dag_id']
        physical=bundle['physical'];graph=bundle['graph']
        assert not blocker.attempts
    assert cuts[-1]['final'] and duplicates
    result={'rows':rows,'loss':loss,'error':error,'cuts':cuts,'events':events,'duplicate_delivery':duplicates,
        'attempts':attempts,'commits_before':before,'commits_after':after,
        'physical':{'plan_id':physical.plan_id,'dag_id':graph.dag_id,
            'exchanges':[asdict(e) for e in physical.exchanges],
            'stages':[{'stage_id':s.stage_id,'partitions':list(s.partitions),
                       'occurrences':list(s.occurrences)} for s in physical.stages],
            'nodes':[{'occurrence_id':n.occurrence_id,'kind':n.kernel.kind} for n in graph.nodes],
            'sources':list(graph.sources),
            'edges':[asdict(e) for e in graph.edges]},
        'child_execute_action_calls':recorder.calls,'child_ray_data_import_attempts':blocker.attempts,
        'self_managed_run_context':True}
    evidence=fx.root/(name+'-observed.json')
    evidence.write_text(json.dumps(result,sort_keys=True,default=str))
    _assert_actual_routes(result)
    return result


def _assert_actual_routes(result):
    """Read original cut carriers, then compare actual resumed data envelopes."""
    specs={s['edge_id']:s for s in result['physical']['exchanges']}
    state_by_owner={}
    stage_of={p:s['stage_id'] for s in result['physical']['stages'] for p in s['partitions']}
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    for entry in result['loss']['boundary']['channels']:
        state=entry['state'];spec=next(s for s in specs.values() if s['channel_id']==state['channel_id'])
        for reader,value in state['readers'].items():
            assert reader in spec['consumer_partitions']
            assert 0<=value['rows_left']<=spec['credit_rows']
            assert 0<=value['bytes_left']<=spec['credit_bytes']
        state_by_owner[(entry['participant'],state['channel_id'])]=state
    suffix=[];ordinals={};unique=set()
    for event in result['events']:
        if event['kind']!='data':continue
        payload=event['payload'];spec=specs[payload['edge_id']]
        producer=payload['producer_partition'];consumer=payload['target_partition']
        delivery=(event['attempt'],payload['delivery_id'])
        assert delivery not in unique,'duplicate actual delivery'
        unique.add(delivery)
        if spec['routing']=='target':assert consumer==event['row'][spec['keys'][0]]
        if spec['routing']=='hash':
            from demiflow.execution.exchange.dataflow_channels import _route_targets
            from demiflow.execution.contracts.dataflow import ExchangeSpec,Record
            from demiflow.execution.contracts.record_codec import _encode_payload
            row=event['row'];encoded=_encode_payload(row,max_bytes=65536)
            record=Record(payload['record_id'],{},None,row,len(encoded),payload['sequence'])
            assert (consumer,)==_route_targets(record,spec=ExchangeSpec(**spec),
                ordinal=payload['route_sequence'],window=65536)
        assert producer in spec['producer_partitions'] and consumer in spec['consumer_partitions']
        if spec['routing']=='rebalance':
            assert consumer==spec['consumer_partitions'][payload['route_sequence']%len(spec['consumer_partitions'])]
        if event['attempt']==result['attempts']['new']:
            owner=compute_participant_id(stage_of[producer],producer)
            saved=state_by_owner.get((owner,spec['channel_id']))
            if saved is not None and saved['routing_ordinal']>0:
                assert payload['route_sequence']>=saved['routing_ordinal'],(payload,saved)
                ordinals.setdefault((owner,spec['channel_id'],saved['routing_ordinal']),set()).add(payload['route_sequence'])
                suffix.append({'edge_id':spec['edge_id'],'reader':consumer,
                    'saved_ordinal':saved['routing_ordinal'],'actual_ordinal':payload['route_sequence'],
                    'record_sequence':payload['sequence'],'record_id':payload['record_id']})
    assert suffix,'no actual resumed routing suffix after a positive original ordinal'
    for (_,_,saved),actual in ordinals.items():
        assert sorted(actual)==list(range(saved,max(actual)+1)),(saved,sorted(actual))
    result['verified_suffix']=suffix


def public_shared_models(fx):
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_runtime_v3_native_execution import PACK
    directory=fx.root/'shared-observer';directory.mkdir()
    def build():
        source=data.from_items([{'i':i,'route':'a' if i%2==0 else 'b'} for i in range(12)]).map(
            _shared,fn_kwargs={'directory':str(directory)})
        routed=source.map_prompt_async('enrich',config=parse_prompt_pack(PACK),
            inputs={'payload':'i'},output='answer',concurrency=1,queue_depth=1,max_requests=12,
            routes={name:{'config':parse_prompt_pack(PACK.replace('local-fixture','route-'+name)),
                          'concurrency':1,'max_requests':6} for name in ('a','b')},route='route',
            options={'max_response_bytes':65536,'max_event_bytes':65536,'trust_env':False})
        return routed.map(_model_result).union(source.map_async(_echo,concurrency=1,queue_depth=1))
    with _model_server() as calls:
        result=_recover(fx,'public-shared-model-routes',build,eligible=lambda facts:0<len(calls)<12)
        assert len(calls)==12
        expected={(i,branch) for i in range(12) for branch in ('echo','model')}
        assert {(r['i'],r['branch']) for r in result['rows']}==expected and len(result['rows'])==24
        for row in result['rows']:
            if row['branch']=='model':assert row['answer']=='route-'+row['route']
            assert row['source_pid']!=row['consumer_pid']!=os.getpid()
        seen=[json.loads(line) for path in directory.glob('*.jsonl') for line in path.read_text().splitlines()]
        owner_attempt={e['payload']['worker_process_id']:e['attempt'] for e in result['events'] if e['kind']=='ready'}
        for row in seen:row['attempt']=owner_attempt[row['pid']]
        per_attempt=Counter((r['attempt'],r['i']) for r in seen)
        assert per_attempt and set(per_attempt.values())=={1},'shared upstream was repeated for a downstream branch'
        source_cut=result['loss']['boundary']['sources']
        assert len(source_cut)==1 and 0<source_cut[0]['offset']['index']<12
        prefix=source_cut[0]['offset']['index'];all_calls=Counter(row['i'] for row in seen)
        assert all(all_calls[i]==1 for i in range(prefix)), 'committed shared prefix was re-executed'
        result['committed_shared_prefix_rows']=prefix
        assert len(result['physical']['sources'])==1
        outgoing=Counter(e['producer'] for e in result['physical']['edges'])
        assert any(n>=2 for n in outgoing.values())
        assert Counter(c['model'] for c in calls)=={'route-a':6,'route-b':6}
        result.update(http_requests=calls,source_calls=seen,
            semantic_scope='public shared Dataset forks and business-field RoutedPromptActor; not literal target/broadcast ExchangeSpec')
    return result


def public_hash_rebalance(fx):
    from demiflow import data
    def build():
        left=data.from_items([{'k':i%2,'i':i} for i in range(12)])
        right=data.from_items([{'k':0},{'k':1}])
        return left.join(right,on='k').repartition(2,shuffle=True).map_async(_identity,concurrency=1)
    result=_recover(fx,'public-hash-rebalance',build)
    assert sorted(result['rows'],key=lambda r:r['i'])==[{'k':i%2,'i':i} for i in range(12)]
    assert {'hash','rebalance'} <= {s['routing'] for s in result['physical']['exchanges']}
    return result


def _native_consumer(row):
    import os
    import time
    time.sleep(.01)
    return {**row,'consumer_pid':os.getpid()}


def _first_native_action(fx,dataset,run,routing,evidence):
    """Initial immutable native submission; original engine owns all execution.

    The fixture declares physical reader IDs before the first seal. It never
    replaces the public compiler, changes a sealed plan, or runs a private
    scheduler. Resume must consume exactly this plan and an original ACK.
    """
    from dataclasses import replace
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.executors.dataflow_api import make_action_request
    from demiflow.execution.action_driver.services import make_driver_services
    from demiflow.execution.run_context import RunContext
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.planning.dataflow_place import (compile_physical_dataflow,validate_physical_dataflow,
        solve_residency)
    from demiflow.execution.seal.plan import (stage_dataflow_programs,resolve_execution_constants,seal_dataflow)
    from demiflow.execution.sources.snapshots import snapshot_source, plan_source_splits
    from demiflow.execution.dataflow_artifacts import stage_runtime_bundle
    from demiflow.execution.contracts.dataflow_codec import compute_physical_digest
    from runtime_v3_residency_scenarios import _provider
    from demiflow.execution.outputs.terminals import open_terminal, collect_rows
    request=make_action_request(dataset._executor,dataset._source,dataset._plan,kind='take_all',arguments={'limit':None})
    policy,bounds=resolve_runtime_policy({'worker_mode':'process'},run.limits,provider=fx.provider)
    policy=replace(policy,fusion=False,default_partitions=1,max_replicas=2)
    services=make_driver_services(request.action,policy=policy,bounds=bounds)
    execution=DataflowExecution(request,services=services)
    try:
        compiled=lower_action(request,bounds=bounds)
        graph=stage_dataflow_programs(compiled,action=request.action,store=services.store,
                                      bounds=bounds,worker_mode='process')
        snapshots=[];splits=[]
        for declaration in compiled.declarations:
            if declaration.source_spec is None:continue
            snap=snapshot_source(declaration.source_spec,action=request.action,store=services.store,
                                 bounds=bounds,source_id=declaration.occurrence_id)
            snapshots.append(snap)
            splits.extend(plan_source_splits(snap,declaration.source_spec,target_partitions=1,bounds=bounds))
        constants=resolve_execution_constants(graph,action=request.action,store=services.store,policy=policy)
        physical=compile_physical_dataflow(graph,tuple(snapshots),tuple(splits),policy=policy,
            hosts=services.hosts,bounds=bounds,constants_ref=constants)
        consumer=next(n.occurrence_id for n in graph.nodes if n.kind=='transform')
        stages=tuple(replace(s,partitions=('r0','r1'),replica_min=2,replica_target=2,replica_max=2)
            if consumer in s.occurrences else s for s in physical.stages)
        owner={o:s for s in stages for o in s.occurrences}
        edges={e.edge_id:e for e in graph.edges}
        exchanges=[]
        for spec in physical.exchanges:
            edge=edges[spec.edge_id]
            route=routing if edge.consumer==consumer else 'rebalance'
            exchanges.append(replace(spec,producer_partitions=owner[edge.producer].partitions,
                consumer_partitions=owner[edge.consumer].partitions,routing=route,
                keys=('target',) if route=='target' else ('k',) if route=='hash' else (),credit_rows=2,
                reason='explicit native contract fixture: '+route))
        exchanges=tuple(exchanges)
        residency=solve_residency(stages,exchanges,hosts=services.hosts,bounds=bounds,
            max_active_workers=policy.max_active_workers)
        physical=replace(physical,stages=stages,exchanges=exchanges,residency=residency)
        physical=replace(physical,plan_id=compute_physical_digest(physical))
        validate_physical_dataflow(graph,physical)
        sealed=seal_dataflow(request.action,graph,physical,store=services.store)
        bundle=stage_runtime_bundle(sealed,store=services.store,bounds=bounds)
        prepared={'attempt':run.attempt,'fence':run.fence,'plan_id':physical.plan_id,
            'dag_id':graph.dag_id,'sealed':asdict(sealed),'runtime_bundle':asdict(bundle),
            'exchanges':[asdict(e) for e in physical.exchanges],
            'scope':'explicit original native physical contract; no public Dataset lowering claim'}
        evidence.update(prepared)
        assert run.journal.execution_commits('action-1')==[] and not execution._started
        execution.prepare()
        assert execution._physical.plan_id==prepared['plan_id']
        execution.start()
        return collect_rows(open_terminal(request,execution=execution),limit=None,bounds=bounds)
    finally:execution.close()


def _assert_broadcast_recovery(result, incoming):
    """Check the committed prefix and complete replay of this 12-row fixture."""
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    producer, = incoming['producer_partitions']
    stage = next(s for s in result['physical']['stages'] if producer in s['partitions'])
    owner = compute_participant_id(stage['stage_id'], producer)
    saved, = [c['state'] for c in result['loss']['boundary']['channels']
              if c['participant'] == owner and c['state']['channel_id'] == incoming['channel_id']]
    frontier = saved['routing_ordinal']
    assert 0 < frontier < 12
    old, new = result['attempts']['old'], result['attempts']['new']
    readers = set(incoming['consumer_partitions'])
    assert readers == {'r0', 'r1'}
    groups = {old: {}, new: {}}
    for event in result['events']:
        if event['kind'] != 'data' or event['payload']['edge_id'] != incoming['edge_id']:
            continue
        p = event['payload']
        assert p['producer_partition'] == producer and p['target_partition'] in readers
        targets = groups[event['attempt']].setdefault(p['route_sequence'], {})
        assert p['target_partition'] not in targets, 'duplicate physical delivery appeared'
        targets[p['target_partition']] = p['record_id']
    assert set(range(frontier)) <= set(groups[old]) <= set(range(12))
    assert set(groups[new]) == set(range(frontier, 12)), 'incomplete recovered routing suffix'
    for sequence, targets in groups[old].items():
        assert len(set(targets.values())) == 1, 'broadcast readers received different records'
        if sequence < frontier:
            assert set(targets) == readers, 'incomplete committed broadcast prefix'
        else:
            # Failure may interrupt either reader of an uncommitted broadcast.
            # Every observed tail record must be replayed to both real readers.
            assert set(targets.values()) == set(groups[new][sequence].values()), 'replay record differs'
    for targets in groups[new].values():
        assert set(targets) == readers, 'incomplete recovered broadcast'
        assert len(set(targets.values())) == 1, 'broadcast readers received different records'


def native_exchange(fx,routing):
    assert routing in ('hash','rebalance','target','broadcast')
    from demiflow import data
    def build():
        return data.from_items([{'i':i,'k':i%3,'target':'r'+str(i%2)} for i in range(12)]).map(_native_consumer)
    name='native-'+routing
    prepared={}
    result=_recover(fx,name,build,workers=4,first_run=lambda ds,run:_first_native_action(fx,ds,run,routing,prepared))
    result['preparation']=prepared
    assert prepared['plan_id']==result['physical']['plan_id']
    assert result['attempts']['old']==prepared['attempt']
    incoming=next(s for s in result['physical']['exchanges'] if s['routing']==routing)
    route_events=[e for e in result['events'] if e['kind']=='data' and e['payload']['edge_id']==incoming['edge_id']]
    assert route_events
    delivered=Counter()
    for event in route_events:
        p=event['payload']
        delivered[(event['attempt'],p['route_sequence'],p['target_partition'])]+=1
    assert set(delivered.values())=={1},'duplicate physical delivery appeared'
    if routing!='broadcast':
        assert len(result['rows'])==12 and sorted(r['i'] for r in result['rows'])==list(range(12))
        if routing=='target':
            for event in route_events:
                p=event['payload'];assert p['target_partition']=='r'+str(p['sequence']%2)
    else:
        assert len(result['rows'])==24 and Counter(r['i'] for r in result['rows'])=={i:2 for i in range(12)}
        for i in range(12):assert len({r['consumer_pid'] for r in result['rows'] if r['i']==i})==2
        _assert_broadcast_recovery(result, incoming)
    result['scope']=prepared['scope']
    return result


def native_unknown_target(fx):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_residency_scenarios import _provider
    evidence={}
    try:
        # Same explicit row/batch windows as the routed scenarios above: the fixture seals two
        # reader replicas by hand, which the default 16-MiB windows no longer fit (7d4c84a8).
        with _provider(fx,workers=4,fusion=False),RunContext.start(fx.journal_dir/'native-unknown-target',
                limits={'max_row_bytes':65536,'max_batch_bytes':262144}) as run:
            dataset=data.from_items([{'i':0,'k':0,'target':'undeclared-reader'}]).map(_native_consumer)
            _first_native_action(fx,dataset,run,'target',evidence)
    except DataflowError as error:
        assert error.code=='INVALID_CONTRACT' and 'target value is not a reader' in str(error)
        evidence['error']={'code':error.code,'message':str(error)}
    else:raise AssertionError('native target silently selected another reader')
    evidence['commits']=run.journal.execution_commits('action-1')
    assert not evidence['commits']
    return evidence


def public_unknown_model(fx):
    from demiflow.execution.run_context import RunContext
    from runtime_v3_residency_scenarios import _provider
    from test_runtime_v3_routed_prompt import _pipeline
    with _model_server() as calls:
        try:
            with _provider(fx,workers=3,fusion=False),RunContext.start(fx.journal_dir/'public-unknown-model') as run:
                _pipeline([{'item':'a','route':'undeclared'}],total=1).take_all()
        except KeyError as error:
            assert 'undeclared' in str(error)
            result={'error':str(error),'http_requests':list(calls),
                    'commits':run.journal.execution_commits('action-1')}
            assert not result['commits'] and not calls
        else:raise AssertionError('public model target was silently substituted')
    return result


def route_recovery(fx,fault=None):
    if fault is not None:raise ValueError('route recovery owns actual worker-loss boundaries')
    # Ordinary Ray gate invocations keep SDK lifecycle in one disposable child.
    # Fixed external runners may already own a private cluster and its FD base.
    if fx.provider=='ray_core' and not getattr(fx,'_residency_child',False):
        import ray
        if not ray.is_initialized():
            from runtime_v3_residency_scenarios import _ray_child
            return _ray_child(fx,'route_recovery',fault)
    from runtime_v3_residency_scenarios import _provider,_channel_recovery
    from contextlib import nullcontext
    environment=_provider(fx,workers=8,fusion=False) if fx.provider=='ray_core' else nullcontext()
    with environment:
        public={'shared_models':public_shared_models(fx),'hash_rebalance':public_hash_rebalance(fx)}
        native={routing:native_exchange(fx,routing) for routing in ('hash','rebalance','target','broadcast')}
        negatives={'unknown_physical_target':native_unknown_target(fx),
            'unknown_business_model':public_unknown_model(fx),
            'credit_and_fence_components':_channel_recovery(fx)}
    cases=[*public.values(),*native.values()]
    return {'public_dataset_semantics':public,'native_physical_contracts':native,'negative_cases':negatives,
        'child_execute_action_calls':[c for case in cases for c in case['child_execute_action_calls']],
        'child_ray_data_import_attempts':[c for case in cases for c in case['child_ray_data_import_attempts']],
        'self_managed_run_context':True,
        'scope':{'public':'actual Dataset model-field routing and shared forks; original hash/rebalance lowering',
                 'native':'explicit ExchangeSpec contracts submitted before first seal; original HostedWorkers and ACK recovery',
                 'not_claimed':['new Dataset target/broadcast DSL','business-name to physical-reader compiler mapping',
                                'physical host outage or two-host execution']}}
