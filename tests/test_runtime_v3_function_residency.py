"""Function-owned data survives actual managed residency and original ACK restore."""
import functools
import json
import os
from pathlib import Path
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.kernels.task_scope import (
    function_residency_kind, snapshot_resident_function, restore_resident_function,
)


def identity(row):
    return row


def counter(path, form='cell'):
    state = [0]
    if form == 'cell':
        def count(row):
            state[0] += 1
            with open(path, 'a') as stream:
                stream.write(json.dumps({'i': row['i'], 'count': state[0], 'pid': os.getpid(),
                                         'tid': threading.get_native_id()}) + '\n')
            return {**row, 'count': state[0]}
        return count
    if form == 'default':
        def count(row, dependency=json, owned=state):
            owned[0] += 1
            with open(path, 'a') as stream:
                stream.write(dependency.dumps({'i': row['i'], 'count': owned[0]}) + '\n')
            return {**row, 'count': owned[0]}
        return count
    if form == 'kwdefault':
        def count(row, *, dependency=json, owned=state):
            owned[0] += 1
            with open(path, 'a') as stream:
                stream.write(dependency.dumps({'i': row['i'], 'count': owned[0]}) + '\n')
            return {**row, 'count': owned[0]}
        return count
    def count(owned, row):
        owned[0] += 1
        with open(path, 'a') as stream:
            stream.write(json.dumps({'i': row['i'], 'count': owned[0]}) + '\n')
        return {**row, 'count': owned[0]}
    return functools.partial(count, state)


def recorded(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.fixture
def separate_stages(monkeypatch):
    """Keep a real inter-stage parking boundary in residency tests."""
    from dataclasses import replace
    from demiflow.execution import dataflow_api
    resolve = dataflow_api.resolve_runtime_policy

    def without_fusion(*args, **kwargs):
        policy, bounds = resolve(*args, **kwargs)
        return replace(policy, fusion=False), bounds

    monkeypatch.setattr(dataflow_api, 'resolve_runtime_policy', without_fusion)


@pytest.mark.parametrize('workers,form', [(1, 'cell'), (4, 'cell'), (1, 'default'),
                                         (1, 'kwdefault'), (1, 'partial')])
def test_actual_function_keeps_stage_state_across_worker_parking(tmp_path, workers, form, separate_stages):
    path = tmp_path / 'calls.jsonl'
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2,
            'checkpoint_interval_ms':60000}) as run, data.local_execution(
                workers=workers, worker_mode='thread', partitions=1):
        flow = data.from_items([{'i':i} for i in range(8)]).map_async(identity, concurrency=1).map_async(
            counter(path, form), concurrency=1)
        materialized = flow.materialize()
        assert materialized.take_all() == [{'i':i, 'count':i+1} for i in range(8)]
        from demiflow.execution.sealed_plans import load_sealed_plan
        bundle = load_sealed_plan(run.ref.directory, 'action-1')
        maps = [node for node in bundle['graph'].nodes if node.kernel.kind == 'AsyncMapOp']
        assert len(maps) == 2 and maps[0].kernel.state == 'none' and maps[1].kernel.state == 'adapter'
        assert all('task_batch_rows' not in node.kernel.configuration for node in maps)
        assert len(bundle['physical'].splits) == 1
        if workers == 1:
            assert any(edge.mode == 'durable_incremental' for edge in bundle['physical'].exchanges)
        commits = run.journal.execution_commits('action-1')
        assert commits and all(c['status'] == 'acked' for c in commits)
        materialized.release()
    calls = recorded(path)
    assert [(c['i'], c['count']) for c in calls] == [(i,i+1) for i in range(8)]


def test_residency_slots_preserve_readonly_dependencies_and_partial_state(tmp_path):
    for form in ('default', 'kwdefault', 'partial'):
        first = counter(tmp_path/(form+'-calls'), form)
        assert function_residency_kind(first) == 'adapter'
        assert first({'i':0})['count'] == 1
        state = snapshot_resident_function(first, max_bytes=4096)
        assert state['mode'] == 'function_slots_v1'
        assert not any('dependency' in key for key in state['value'])
        second = counter(tmp_path/(form+'-calls'), form)
        restore_resident_function(second, state, max_bytes=4096)
        assert second({'i':1})['count'] == 2
    def only_dependencies(row, module=json, path=tmp_path, typename=Path):
        return row
    assert function_residency_kind(only_dependencies) == 'none'
    assert function_residency_kind(len) == 'none'
    def with_keyword(row, *, owned):
        owned['count'] += 1
        return {**row, **owned}
    first = functools.partial(with_keyword, owned={'count':0})
    assert first({})['count'] == 1
    state = snapshot_resident_function(first, max_bytes=4096)
    second = functools.partial(with_keyword, owned={'count':0})
    restore_resident_function(second, state, max_bytes=4096)
    assert second({})['count'] == 2


def test_residency_unsupported_alias_global_cycle_and_resource_are_not_stateless():
    shared = []
    value = [shared,shared]
    def aliased(row):
        value[0].append(1)
        return row
    cycle = []; cycle.append(cycle)
    def cyclic(row):
        return row if cycle else row
    lock = threading.Lock()
    def locked(row):
        with lock:
            return row
    namespace = {'state':[]}
    exec('def external(row):\n state.append(1)\n return row', namespace)
    for callback in (aliased, cyclic, locked, namespace['external']):
        assert function_residency_kind(callback) == 'nonrecoverable'
        assert snapshot_resident_function(callback, max_bytes=4096)['mode'] == 'nonrecoverable'


def test_restore_validates_whole_state_before_mutating_any_slot(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    callback = counter(tmp_path/'calls')
    state = snapshot_resident_function(callback, max_bytes=4096)
    state['value']['cell:state'] = ['x'*5000]
    with pytest.raises(DataflowError, match='state'):
        restore_resident_function(callback, state, max_bytes=4096)
    assert callback({'i':0})['count'] == 1


@pytest.mark.parametrize('form',['cell','bound'])
def test_actual_function_restore_uses_committed_counter_and_no_prefix_reexecution(tmp_path, monkeypatch, form):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.kernels import state_codec
    class Interrupted(Exception):
        pass
    path = tmp_path/'calls'
    def bound(row, owned):
        owned[0] += 1
        with open(path,'a') as stream:
            stream.write(json.dumps({'i':row['i'],'count':owned[0]})+'\n')
        return {**row,'count':owned[0]}
    def build():
        source = data.from_items([{'i':i} for i in range(8)]).map_async(identity, concurrency=1)
        return (source.map_async(counter(path), concurrency=1) if form == 'cell' else
                source.map(bound,fn_kwargs={'owned':[0]},execution='inline',concurrency=1))
    original = DataflowCheckpointCoordinator.commit
    captured = {}
    def interrupt(coordinator, cut_id, *, sinks, **forwarded):
        # The driver also passes native_queue_state; forward whatever the real commit is given.
        image = original(coordinator, cut_id, sinks=sinks, **forwarded)
        if not image.cut.final and not captured:
            for contribution in image.contributions:
                for ref in contribution.operator_states.values():
                    if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                        continue
                    raw = b''.join(coordinator._store.read(ref,max_bytes=ref.size_bytes))
                    value = state_codec.loads(raw,max_bytes=coordinator._physical.bounds.max_state_bytes)
                    state = value.get('state') or {}
                    if state.get('mode') == 'function_slots_v1':
                        owned = state['value'].get('cell:state' if form == 'cell' else 'call:keyword:owned')
                        if owned and 0 < owned[0] < 8:
                            captured.update(count=owned[0],commit=image.commit_id)
                            raise Interrupted('after original ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowCheckpointCoordinator,'commit',interrupt)
        with pytest.raises(Interrupted,match='original ACK'):
            with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':2,
                    'checkpoint_interval_ms':60000}) as run, data.local_execution(
                        workers=1,worker_mode='process',partitions=1):
                reference = run.ref
                build().take_all()
    assert 0 < captured['count'] < 8
    prefix = [v for v in recorded(path) if v['i'] < captured['count']]
    assert [v['i'] for v in prefix] == list(range(captured['count']))
    with RunContext.resume(reference) as run, data.local_execution(workers=1,worker_mode='process',partitions=1):
        assert build().take_all() == [{'i':i,'count':i+1} for i in range(8)]
        assert captured['commit'] in {c['commit_id'] for c in run.journal.execution_commits('action-1')}
    assert [v for v in recorded(path) if v['i'] < captured['count']] == prefix


def declared_counter(row, state=[0]):
    state[0] += 1
    return {**row, 'counts':row.get('counts',[])+[state[0]]}


def attributed_counter(row):
    attributed_counter.count += 1
    return {**row, 'count':attributed_counter.count}


attributed_counter.count = 0


def test_instances_do_not_mutate_sealed_importable_function_or_other_occurrence(tmp_path):
    from demiflow.execution.kernels.task_scope import clone_resident_function
    a = clone_resident_function(attributed_counter,max_bytes=4096)
    b = clone_resident_function(attributed_counter,max_bytes=4096)
    assert a({})['count'] == 1 and a({})['count'] == 2
    assert b({})['count'] == 1 and attributed_counter.count == 0
    before = declared_counter.__defaults__[0][:]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=4, worker_mode='thread',partitions=1):
        flow = data.from_items([{'i':i} for i in range(8)]).map_async(
            declared_counter,concurrency=1).map_async(declared_counter,concurrency=1)
        assert flow.take_all() == [{'i':i,'counts':[i+1,i+1]} for i in range(8)]
    assert declared_counter.__defaults__[0] == before == [0]


@pytest.mark.parametrize('form', ['default','partial'])
def test_task_function_keeps_original_batch_boundary_through_parking(tmp_path, form, separate_stages):
    callback = counter(tmp_path/'calls',form)
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':1,'checkpoint_interval_ms':60000}), data.local_execution(
            workers=1,worker_mode='thread',partitions=1,batch_rows=3):
        assert [r['count'] for r in data.from_items([{'i':i} for i in range(8)]).map(callback).take_all()] == [1,2,3,1,2,3,1,2]


def test_nonrecoverable_alias_can_run_resident_but_never_silently_park(tmp_path, separate_stages):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    path = tmp_path/'calls'
    def build():
        shared = []; owned = [shared, shared]
        def callback(row):
            owned[0].append(row['i'])
            with open(path,'a') as stream:
                stream.write(str(row['i'])+'\n')
            return {**row,'count':len(owned[1])}
        return data.from_items([{'i':i} for i in range(3)]).map_async(identity,concurrency=1).map_async(callback,concurrency=1)
    with pytest.raises(DataflowError,match='cannot safely park') as failed:
        with RunContext.start(tmp_path/'small'), data.local_execution(workers=1,worker_mode='thread',partitions=1):
            build().take_all()
    assert failed.value.code == 'RESOURCE_INFEASIBLE'
    assert not path.exists()
    with RunContext.start(tmp_path/'resident'), data.local_execution(workers=4,worker_mode='thread',partitions=1):
        assert build().take_all() == [{'i':i,'count':i+1} for i in range(3)]
    assert path.read_text().splitlines() == ['0','1','2']


def test_nested_nonlocal_state_and_unused_empty_cell_are_preserved():
    from demiflow.execution.kernels.task_scope import clone_resident_function
    def make():
        total = 0
        def outer(row):
            def advance():
                nonlocal total
                total += 1
            advance()
            return {**row,'count':total}
        return outer
    first = make()
    assert function_residency_kind(first) == 'adapter'
    assert first({})['count'] == 1
    image = snapshot_resident_function(first,max_bytes=4096)
    second = clone_resident_function(make(),max_bytes=4096)
    restore_resident_function(second,image,max_bytes=4096)
    assert second({})['count'] == 2
    absent = None
    def empty(row, state=[0]):
        if row.get('read_absent'):
            return absent
        state[0] += 1
        return state[0]
    del absent
    cloned = clone_resident_function(empty,max_bytes=4096)
    assert cloned({}) == 1 and empty.__defaults__ == ([0],)
    with pytest.raises(NameError):
        cloned({'read_absent':True})


def test_nested_global_cannot_be_downgraded_by_ordinary_task_declaration(tmp_path, separate_stages):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    namespace = {'outside':[]}
    exec('def callback(row):\n def nested():\n  outside.append(row)\n nested()\n return row',namespace)
    callback = namespace['callback']
    assert function_residency_kind(callback) == 'nonrecoverable'
    with pytest.raises(DataflowError,match='cannot safely park'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1,worker_mode='thread',partitions=1):
            data.from_items([{'i':0}]).map(callback).take_all()
    assert namespace['outside'] == []


@pytest.mark.parametrize('form',['args','kwargs','field'])
@pytest.mark.parametrize('workers',[1,4])
def test_explicit_bound_function_data_is_owned_and_survives_park(tmp_path,form,workers):
    state=[0]
    def callback(row,state):
        state[0]+=1
        return {**row,'count':state[0]}
    def field(i, state):
        state[0]+=1
        return state[0]
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':1,'checkpoint_interval_ms':60000}), data.local_execution(
            workers=workers,worker_mode='thread',partitions=1):
        source=data.from_items([{'i':i} for i in range(3)]).map_async(identity,concurrency=1)
        options={'execution':'inline','concurrency':1}
        if form=='field':
            flow=source.map(field,inputs={'i':'i'},output='count',fn_kwargs={'state':state},**options)
        else:
            options.update({'fn_args':[state]} if form=='args' else {'fn_kwargs':{'state':state}})
            flow=source.map(callback,**options)
        assert flow.take_all()==[{'i':i,'count':i+1} for i in range(3)]
    assert state==[0]  # The driver declaration is not the active state owner.


def test_call_bindings_share_alias_detection_with_cells_defaults_and_partial():
    state=[]
    def closure(row,bound):
        return state is bound
    def default(row,bound,original=state):
        return original is bound
    def plain(owned,row,bound):
        return owned is bound
    for callback in (closure,default,functools.partial(plain,state)):
        assert function_residency_kind(callback,call_args=(state,))=='nonrecoverable'
        assert snapshot_resident_function(callback,call_kwargs={'bound':state},max_bytes=4096)['mode']=='nonrecoverable'


def test_readonly_task_dependencies_need_no_state_reservation(tmp_path, separate_stages):
    def readonly(row,module=json,path=tmp_path):
        assert module.loads('null') is None and path.name
        return row
    with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=1,worker_mode='thread',partitions=1,batch_rows=3):
        assert data.from_items([{'i':0}]).map(readonly).take_all()==[{'i':0}]
        from demiflow.execution.sealed_plans import load_sealed_plan
        bundle=load_sealed_plan(run.ref.directory,'action-1')
        node=next(n for n in bundle['graph'].nodes if n.kernel.kind=='MapOp')
        assert node.kernel.state=='none' and node.kernel.configuration['task_batch_rows']==3
        stage = next(stage for stage in bundle['physical'].stages if node.occurrence_id in stage.occurrences)
        assert stage.occurrences == (node.occurrence_id,)
        assert stage.state_bytes == 0
        assert stage.profile.requests['memory_bytes'] >= stage.resident_bytes
        assert stage.resident_bytes >= node.kernel.configuration.get('task_load_bytes', 0)


def test_nested_external_state_snapshot_cannot_claim_legacy_task_recovery():
    from test_runtime_v3_k import services,spec,program,InputPort
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.kernels import state_codec
    namespace={'outside':[]}
    exec('def callback(row):\n def nested():\n  outside.append(row)\n nested()\n return row',namespace)
    # Component boundary: fake store/journal ports, real kernel and image
    # DTO/codec. The actual driver/worker restore proofs above use native ports.
    restrictions=[]
    class JournalPort:
        def restrict_recovery(self,occurrence):
            restrictions.append(occurrence)
    svc=services(programs={'fn':program(CallableSpec.create(namespace['callback']))})
    svc.journal=JournalPort()
    kernel=OperatorKernel(spec('MapOp',{'program':'fn','task_batch_rows':3},
        inputs=[InputPort('input','incremental')],state='nonrecoverable'),services=svc,partition_id='p0')
    kernel.start()
    try:
        ref=kernel.snapshot()
        image=state_codec.loads(b''.join(svc.store.read(ref,max_bytes=ref.size_bytes)),max_bytes=svc.bounds.max_state_bytes)
        assert image['state']['mode']=='nonrecoverable' and restrictions
    finally:
        kernel.close()


def test_function_binding_classification_executes_no_container_hooks():
    touched=[]
    class Defaults(tuple):
        def __iter__(self):
            touched.append('iter')
            raise AssertionError('user hook executed')
    def callback(row,value=0):
        return row
    callback.__defaults__=Defaults((0,))
    assert function_residency_kind(callback)=='nonrecoverable'
    assert touched==[]
    append=[].append
    def bound_builtin(row):
        append(row)
        return row
    assert function_residency_kind(bound_builtin)=='nonrecoverable'
    assert function_residency_kind(len)=='none'
