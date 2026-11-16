"""Native residency acceptance: original worker cuts, state and lifecycle facts.

Observers delegate to production methods. They neither build replacement
journals nor rewrite physical plans. Optional fusion is explicitly disabled
only where the scenario requires independent worker owners.
"""
from __future__ import annotations
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
import functools
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch


def _emit(path, event, **fields):
    import threading
    entry = {'event': event, 'pid': os.getpid(), 'tid': threading.get_native_id(), **fields}
    with open(path, 'a') as stream:
        stream.write(json.dumps(entry, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def _events(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()] if Path(path).exists() else []


def _identity(row):
    return row


def _plus(row, *, delta):
    return {**row, 'plus':row['i']+delta, 'worker_pid':os.getpid()}


def _empty(row):
    return False


def _expand(row):
    return [{'k': 0, 'i': n} for n in range(3)]


async def _batch(rows):
    return rows


@contextmanager
def _provider(fx, *, workers=4, fusion=True):
    from demiflow import data
    from demiflow.execution import dataflow_api
    resolve = dataflow_api.resolve_runtime_policy
    def policy(options, limits, **kwargs):
        if fx.provider == 'local':
            options = {**options, 'worker_mode':'process'}
        value, bounds = resolve(options, limits, **kwargs)
        return replace(value, fusion=fusion), bounds
    with patch.dict(os.environ, {'DEMIFLOW_RESOURCE_ROOT': str(fx.root/'capacity')}), \
            patch.object(dataflow_api, 'resolve_runtime_policy', policy):
        if fx.provider == 'local':
            from demiflow.data.api import _use_executor
            from demiflow.execution.executors.local import LocalDatasetExecutor
            executor = LocalDatasetExecutor(workers=workers)
            try:
                with _use_executor(executor):
                    yield
            finally:
                executor.close()
            return
        import ray
        from demiflow.data.api import _use_executor
        from demiflow.execution.executors.ray import RayDatasetExecutor
        assert Path(ray.__file__).is_file() and ray.ObjectRef is ray._raylet.ObjectRef
        owned = not ray.is_initialized()
        if owned:
            ray.init(address='local', num_cpus=workers, num_gpus=0, include_dashboard=False,
                     object_store_memory=100*1024**2,
                     _temp_dir=tempfile.mkdtemp(prefix='demiflow-residency-ray-', dir='/tmp'))
        executor = RayDatasetExecutor(workers=workers)
        try:
            with _use_executor(executor):
                yield
        finally:
            executor.close()
            if owned:
                ray.shutdown()


def _cut_facts(coordinator, image):
    from demiflow.execution.dataflow_checkpoint import _contribution_json, _cursor_json
    from demiflow.execution.kernels import state_codec
    states = []
    for owner in image.contributions:
        for occurrence, ref in owner.operator_states.items():
            if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                continue
            value = state_codec.loads(b''.join(coordinator._store.read(ref,
                max_bytes=coordinator._physical.bounds.max_state_bytes)),
                max_bytes=coordinator._physical.bounds.max_state_bytes)
            states.append({'participant': owner.participant_id, 'occurrence': occurrence,
                'kind': value['kind'], 'batch_rows': len(value.get('batch', ())),
                'state': value.get('state'), 'finish_cursor': value.get('finish_cursor'),
                'image_ref': dict(digest=ref.digest, size_bytes=ref.size_bytes, store_id=ref.store_id,
                                  key=ref.key, media_type=ref.media_type)})
    commit = coordinator._action.context.journal.execution_commit(image.commit_id)
    assert commit['status'] == 'acked'
    return {'commit_id': image.commit_id, 'cut_id': image.cut.cut_id,
        'sequence': image.cut.sequence, 'final': image.cut.final,
        'participants': list(image.cut.participants),
        'contributions': [_contribution_json(c) for c in image.contributions],
        'sources': [_cursor_json(c) for c in image.source_vector], 'kernel_images': states}


def _resume_after_ack(fx, name, build, *, eligible=None, workers=4, fusion=True, limits=None):
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.sealed_plans import load_sealed_plan
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker
    directory = fx.journal_dir/name
    cuts, boundary = [], {}
    commit = DataflowCheckpointCoordinator.commit
    class InterruptedAfterOriginalAck(Exception):
        pass
    def observe(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        facts = _cut_facts(coordinator, image)
        assert len(cuts) < 128, 'scenario cut count exceeds its finite evidence budget'
        cuts.append(facts)
        if not image.cut.final and not boundary and (eligible is None or eligible(facts)):
            boundary.update(facts)
            raise InterruptedAfterOriginalAck()
        return image
    scope_limits = {'checkpoint_rows': 1, 'checkpoint_interval_ms':60000, 'timeout_s':120,
        'memory_bytes':512*1024**2, 'max_row_bytes':65536, 'max_batch_bytes':262144,
        'max_state_bytes':2*1024**2, 'max_scratch_bytes':2*1024**3, **(limits or {})}
    with _provider(fx, workers=workers, fusion=fusion), _ExecuteActionRecorder() as recorder, _RayDataBlocker() as blocked:
        try:
            with RunContext.start(directory, limits=scope_limits) as run:
                reference, first_fence, first_attempt = run.ref, run.fence, run.attempt
                with patch.object(DataflowCheckpointCoordinator, 'commit', observe):
                    build().take_all()
        except InterruptedAfterOriginalAck:
            pass
        else:
            raise AssertionError('pipeline never reached the required actual nonfinal ACK')
        before = run.journal.execution_commits('action-1')
        assert boundary and before[-1]['status'] == 'acked'
        def final_observe(coordinator, *args, **kwargs):
            image = commit(coordinator, *args, **kwargs)
            cuts.append(_cut_facts(coordinator, image))
            return image
        with RunContext.resume(reference) as resumed:
            assert resumed.attempt == first_attempt+1 and resumed.fence != first_fence
            with patch.object(DataflowCheckpointCoordinator, 'commit', final_observe):
                rows = build().take_all()
            after = resumed.journal.execution_commits('action-1')
            assert after[:len(before)] == before and after[-1]['status'] == 'acked'
            assert cuts[-1]['final']
            attempts = {'before':first_attempt, 'after':resumed.attempt,
                        'old_fence':first_fence, 'new_fence':resumed.fence}
        bundle = load_sealed_plan(reference.directory, 'action-1')
        assert not blocked.attempts
    return {'rows':rows, 'boundary':boundary, 'cuts':cuts, 'attempts':attempts,
        'commits_before':before, 'commits_after':after,
        'physical':{'plan_id':bundle['physical'].plan_id, 'dag_id':bundle['graph'].dag_id,
            'sources':list(bundle['graph'].sources),
            'stages':[{'stage_id':s.stage_id, 'partitions':list(s.partitions),
                       'occurrences':list(s.occurrences)} for s in bundle['physical'].stages],
            'routes':sorted(set(e.routing for e in bundle['physical'].exchanges))},
        'child_execute_action_calls':recorder.calls, 'child_ray_data_import_attempts':blocked.attempts,
        'self_managed_run_context':True}


def multiworker_barrier(fx, fault=None):
    if fx.provider=='ray_core' and not getattr(fx,'_residency_child',False):
        return _ray_child(fx,'multiworker_barrier',fault)
    if fault is not None:
        raise ValueError('multiworker_barrier uses the original nonfinal ACK boundary')
    from demiflow import data
    def build():
        left = data.from_items([{'seed':0}]).flat_map(_expand).batch_map(_batch,max_batch=4)
        empty = data.from_items([{'k':0,'i':99}]).filter(_empty)
        right = data.from_items([{'k':0,'j':j} for j in range(3)])
        return left.union(empty).join(right,on='k')
    result = _resume_after_ack(fx,'multiworker-barrier',build,workers=12,fusion=False,
        eligible=lambda f:len(f['contributions'])>=3 and any(s['batch_rows'] for s in f['kernel_images']))
    expected = [(i,j) for i in range(3) for j in range(3)]
    assert sorted((r['i'],r['j']) for r in result['rows']) == expected
    cut = result['boundary']
    assert set(cut['participants']) == {c['participant_id'] for c in cut['contributions']}
    assert len({c['boot_id'] for c in cut['contributions']}) >= 3
    assert len(result['physical']['sources']) == 3
    assert all(c['cut_id']==cut['cut_id'] for c in cut['contributions'])
    assert all(not c['final'] for c in cut['contributions'])
    final = result['cuts'][-1]
    assert all(s['end_of_input'] for s in final['sources'])
    result['joined'] = len(result['rows'])
    result['covered'] = ['original all-owner nonfinal cut','partial batch after flatmap expansion',
                         'empty filtered branch EOI','two join input ports','original ACK resume']
    return result


class Accumulator:
    """No snapshot/restore hooks: the actual class-state adapter owns total."""
    def __init__(self, path):
        import uuid
        self.path, self.owner, self.total = path, uuid.uuid4().hex, 0
        _emit(path,'created',instance=id(self),owner=self.owner)
    async def astart(self):
        _emit(self.path,'started',instance=id(self),owner=self.owner)
    def __call__(self,row):
        self.total += row['i']
        _emit(self.path,'call',instance=id(self),owner=self.owner,i=row['i'],total=self.total)
        return {**row,'total':self.total,'owner':self.owner}
    async def astop(self):
        _emit(self.path,'stopped',instance=id(self),owner=self.owner)
    async def aclose(self):
        _emit(self.path,'closed',instance=id(self),owner=self.owner)


class LifecycleActor:
    def __init__(self,path,phase):
        import threading
        self.path,self.phase,self.lock=path,phase,threading.Lock()
        _emit(path,'created',instance=id(self))
    async def astart(self):
        import asyncio
        async def background():
            try:
                await asyncio.Event().wait()
            finally:
                _emit(self.path,'background-drained',instance=id(self))
        self.task=asyncio.create_task(background())
        await asyncio.sleep(0)
        _emit(self.path,'started',instance=id(self))
    async def __call__(self,row):
        import asyncio
        with self.lock:
            _emit(self.path,'call',instance=id(self),i=row['i'])
        if self.phase=='failure':
            raise ValueError('actual lifecycle call failure')
        if self.phase=='cancel':
            raise asyncio.CancelledError('actual lifecycle cancellation')
        return row
    async def astop(self):
        _emit(self.path,'stopped',instance=id(self))
    async def aclose(self):
        assert self.task.done(), 'background task outlived owning actor'
        _emit(self.path,'closed',instance=id(self))
        if self.phase=='close_failure':
            raise LookupError('actual lifecycle close failure')


def actor_state_lifecycle(fx,fault=None):
    if fx.provider=='ray_core' and not getattr(fx,'_residency_child',False):
        return _ray_child(fx,'actor_state_lifecycle',fault)
    if fault is not None:
        raise ValueError('actor lifecycle covers success, failure and cancellation together')
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker
    path = str(fx.evidence_dir/'accumulator.jsonl')
    def build():
        return data.from_items([{'i':i} for i in range(5)]).map(Accumulator,
            fn_constructor_args=[path],execution='inline',concurrency=1)
    result = _resume_after_ack(fx,'actor-state',build)
    assert [(r['i'],r['total']) for r in result['rows']] == [(0,0),(1,1),(2,3),(3,6),(4,10)]
    calls = [e for e in _events(path) if e['event']=='call']
    assert [(e['i'],e['total']) for e in calls] == [(r['i'],r['total']) for r in result['rows']]
    assert len({r['owner'] for r in result['rows']}) == 1
    owners_path = str(fx.evidence_dir/'worker-owners.jsonl')
    def worker_build():
        return data.from_items([{'i':i} for i in range(9)]).map(Accumulator,
            fn_constructor_args=[owners_path],execution='thread',concurrency=3,callable_scope='worker')
    owners = _resume_after_ack(fx,'actor-worker-slots',worker_build,limits={'checkpoint_rows':4})
    assert sorted(r['i'] for r in owners['rows']) == list(range(9))
    identities = {r['owner'] for r in owners['rows']}
    assert len(identities) == 3
    owner_calls = [e for e in _events(owners_path) if e['event']=='call']
    assert len(owner_calls) == 9
    for owner in identities:
        values = [r for r in owners['rows'] if r['owner']==owner]
        total = 0
        for row in values:
            total += row['i']
            assert row['total']==total
        assert [(e['i'],e['total']) for e in owner_calls if e['owner']==owner] == [
            (r['i'],r['total']) for r in values]
    result['worker_owners'] = owners
    result['child_execute_action_calls'] += owners['child_execute_action_calls']
    result['child_ray_data_import_attempts'] += owners['child_ray_data_import_attempts']
    cases = []
    with _provider(fx), _ExecuteActionRecorder() as recorder, _RayDataBlocker() as blocked:
        for phase in ('success','failure','cancel','close_failure'):
            events_path = str(fx.evidence_dir/('lifecycle-'+phase+'.jsonl'))
            class Actor(LifecycleActor):
                def __init__(self):
                    super().__init__(events_path,phase)
            second_path = str(fx.evidence_dir/('lifecycle-'+phase+'-second.jsonl'))
            class SecondActor(LifecycleActor):
                def __init__(self):
                    super().__init__(second_path,'success')
            error = None
            try:
                with RunContext.start(fx.journal_dir/('lifecycle-'+phase),limits={'timeout_s':60}):
                    pipeline = data.from_items([{'i':0}]).map_async(Actor,concurrency=1)
                    if phase=='close_failure':
                        pipeline = pipeline.map_async(SecondActor,concurrency=1)
                    pipeline.take_all()
            except BaseException as exc:
                error = exc
            if phase=='success':
                assert error is None,error
            elif phase=='failure':
                assert isinstance(error,ValueError) and str(error)=='actual lifecycle call failure',repr(error)
            elif phase=='cancel':
                import asyncio
                assert isinstance(error,asyncio.CancelledError),repr(error)
            else:
                assert isinstance(error,ExceptionGroup),repr(error)
            events = _events(events_path)
            names = [e['event'] for e in events]
            assert Counter(names)==Counter({'created':1,'started':1,'call':1,'stopped':1,
                                           'background-drained':1,'closed':1}),events
            assert names.index('stopped')<names.index('background-drained')<names.index('closed')
            assert all(e['pid']!=os.getpid() for e in events)
            second_events = _events(second_path)
            if phase=='close_failure':
                assert Counter(e['event'] for e in second_events)==Counter(names)
                assert all(e['pid']!=os.getpid() for e in second_events)
            cases.append({'phase':phase,'events':events,'other_owner_events':second_events,'error':None if error is None else
                          {'type':type(error).__name__,'message':str(error)}})
        shared_path = str(fx.evidence_dir/'shared-rejected.jsonl')
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        try:
            with RunContext.start(fx.journal_dir/'shared-rejected'):
                data.from_items([{'i':0}]).map(Accumulator,fn_constructor_args=[shared_path],
                    execution='thread',concurrency=3,callable_scope='stage').take_all()
        except DataflowError as exc:
            assert 'auto_json state requires concurrency 1' in str(exc)
            result['shared_rejected'] = {'code':exc.code,'message':str(exc),'events':_events(shared_path)}
            assert all(e['event']!='call' for e in _events(shared_path))
        else:
            raise AssertionError('shared mutable state was run without an explicit concurrent adapter')
        with RunContext.start(fx.journal_dir/'pure-partial'):
            partial_rows = data.from_items([{'i':i} for i in range(3)]).map(
                functools.partial(_plus,delta=2)).take_all()
        assert [(r['i'],r['plus']) for r in partial_rows]==[(i,i+2) for i in range(3)]
        assert all(r['worker_pid']!=os.getpid() for r in partial_rows)
        result['partial_rows'] = partial_rows
        assert not blocked.attempts
    result['child_execute_action_calls'] += recorder.calls
    result['child_ray_data_import_attempts'] += blocked.attempts
    result.update(final_total=result['rows'][-1]['total'],actor_calls=calls,lifecycle_cases=cases)
    return result


def route_recovery(fx,fault=None):
    from runtime_v3_route_recovery_scenarios import route_recovery as execute
    return execute(fx,fault)


def _channel_recovery(fx):
    """Real native store/channel component proof, never public Dataset coverage."""
    from types import SimpleNamespace
    from demiflow.execution.contracts.dataflow import Record, ExchangeSpec, CutRequest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.exchange import dataflow_channels as channels
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    from demiflow.execution.recovery_contract import identity
    from test_runtime_v3_artifact_accounting import store_fixture
    cases=[]
    for routing in ('hash','rebalance','target','broadcast'):
        with store_fixture(fx.journal_dir/('channel-'+routing)) as (run,store,port):
            services=SimpleNamespace(store=store,bounds=port._bounds,
                assignment=SimpleNamespace(plan_revision=0),check_stop=run.check_stop)
            spec=ExchangeSpec(channel_id='route',edge_id=identity('route-edge'),
                producer_partitions=('producer',),consumer_partitions=('a','b','c'),
                mode='durable_incremental',routing=routing,
                keys=('target',) if routing=='target' else ('key',) if routing=='hash' else (),
                scope_id=None,route_epoch=0,credit_rows=16,credit_bytes=65536,
                storage_bytes=1024**2,read_domain='local',reason='explicit native channel contract test')
            records=[]
            for i in range(10):
                row={'key':i%4,'target':('a','b','c')[i%3],'value':i}
                raw=_encode_payload(row,max_bytes=65536)
                records.append(Record(identity(['route-record',i]),{'ordinal':i},None,row,len(raw),i))
            def routed(state,record):
                return [(r.reader_id,r.delivery_id,r.record.sequence) for r in
                    channels.route_record(record,spec=spec,state=state.routing)]
            expected_state=channels.open_channels((spec,),services=services)
            expected=[routed(expected_state,r) for r in records]
            state=channels.open_channels((spec,),services=services)
            prefix=[]
            for record in records[:4]:
                prefix.append(routed(state,record))
                state.enqueue(spec.channel_id,state.issue_sequence(spec.channel_id),
                              channels.encode_record(record,bounds=port._bounds))
            lease=state.reserve(spec.channel_id,'a',rows=1,size_bytes=17,operation_id=identity('credit'))
            slow=state.reserve(spec.channel_id,'a',rows=15,size_bytes=17,operation_id=identity('slow-reader'))
            try:state.reserve(spec.channel_id,'a',rows=1,size_bytes=1,operation_id=identity('overflow'))
            except DataflowError as exc:
                assert exc.code=='CAPACITY_PENDING'
            else:raise AssertionError('slow reader exceeded its original credit')
            assert state.reader_credit(spec.channel_id,'b')['rows_left']==spec.credit_rows
            assert state.reader_credit(spec.channel_id,'c')['rows_left']==spec.credit_rows
            cut=CutRequest(identity(['channel-cut',routing]),1,0,0,('owner',),False)
            refs=channels.snapshot_channels(state,cut=cut,store=store)
            restored=channels.restore_channels((spec,),refs,services=services)
            assert restored.routing.values()==state.routing.values()=={'route':4}
            assert restored.next_sequence('route')==4
            assert restored.buffer_items('route')==state.buffer_items('route')
            before=state.reader_credit('route','a')
            assert restored.reader_credit('route','a')==before
            assert restored.reserve('route','a',rows=1,size_bytes=17,operation_id=identity('credit'))==lease
            assert restored.reader_credit('route','a')==before
            suffix=[routed(restored,r) for r in records[4:]]
            assert prefix+suffix==expected
            restored.release(lease)
            released=restored.reader_credit('route','a')
            restored.release(lease)
            assert restored.reader_credit('route','a')==released
            fenced=[]
            for bad_spec,bad_services in ((replace(spec,route_epoch=1),services),
                (spec,SimpleNamespace(store=store,bounds=port._bounds,
                    assignment=SimpleNamespace(plan_revision=1),check_stop=run.check_stop))):
                try:channels.restore_channels((bad_spec,),refs,services=bad_services)
                except DataflowError as exc:
                    assert exc.code=='FENCED';fenced.append(exc.code)
                else:raise AssertionError('stale route/plan was accepted')
            cases.append({'routing':routing,'scope':'native channel contract, not public lowering',
                'prefix':prefix,'suffix':suffix,'expected':expected,
                'credit_before':before,'credit_after_release':released,'fenced':fenced,
                'slow_reader':'CAPACITY_PENDING','unaffected_siblings':['b','c'],
                'cut_id':cut.cut_id,'snapshot_digest':refs['route'].digest})
            restored.release(slow)
            assert restored.reader_credit('route','a')['rows_left']==spec.credit_rows
            store.close()
    return cases


def _ray_child(fx,name,fault):
    """Own Ray's process-scoped runtime in a disposable, tree-pinned child."""
    import subprocess
    import sys
    from runtime_v3_recovery_scenarios import _write, _read, WORKTREE
    directory=fx.root/('child-'+name)
    directory.mkdir()
    request=directory/'request.json'
    _write(request,{'root':str(fx.root),'task':name,'fault':fault,'output':str(directory/'result.json')})
    with (directory/'stdout').open('wb') as out,(directory/'stderr').open('wb') as err:
        child=subprocess.Popen([sys.executable,'-B','-m','runtime_v3_residency_scenarios',str(request)],
            cwd=WORKTREE,env={**os.environ,'PYTHONPATH':str(WORKTREE)+':'+str(WORKTREE/'tests'),
                'PYTHONDONTWRITEBYTECODE':'1','DEMIFLOW_RESOURCE_ROOT':str(fx.root/'capacity')},
            stdout=out,stderr=err,start_new_session=True)
        fx._children.append(child)
        try:child.wait(timeout=240)
        except subprocess.TimeoutExpired:
            child.kill();child.wait(timeout=10)
            raise AssertionError('private Ray acceptance child exceeded 240s aggregate deadline')
    assert child.returncode in (0,3),(directory/'stderr').read_text()[-16000:]
    result=_read(directory/'result.json')
    if child.returncode==3:
        from runtime_v3_support import NotRunError
        verified=Path(result['missing']['verified_evidence'])
        assert verified.resolve().is_relative_to(fx.root.resolve())
        fx._save_evidence(name+'-partial',_read(verified))
        fx._record_not_run(name,result['missing'])
        raise NotRunError(name,result['missing'])
    result['ray_child']={'pid':child.pid,'returncode':child.returncode,'scope':'same-host private Ray Core'}
    return result


def _main(path):
    import sys
    import demiflow
    from types import SimpleNamespace
    from runtime_v3_recovery_scenarios import _write, _read, WORKTREE
    from runtime_v3_support import NotRunError, _RayDataBlocker
    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)
    config=_read(Path(path));root=Path(config['root'])
    assert root.is_relative_to('/tmp') and any(p.startswith('demiflow-') for p in root.parts)
    fx=SimpleNamespace(root=root,journal_dir=root/'journal',evidence_dir=root/'evidence',
        provider='ray_core',bounds={'timeout_ms':120000},_children=[],_residency_child=True)
    fx._save_evidence=lambda name,value:_write(fx.evidence_dir/(name+'.json'),value)
    fx._record_not_run=lambda name,detail:_write(fx.evidence_dir/('not-run-'+name+'.json'),detail)
    code=0
    with _RayDataBlocker() as blocked:
        try:result=globals()[config['task']](fx,config['fault'])
        except NotRunError as exc:
            result={'missing':exc.missing};code=3
        assert not blocked.attempts
    _write(Path(config['output']),result)
    raise SystemExit(code)


if __name__=='__main__':
    import sys
    _main(sys.argv[1])
