"""Real named graph partitions retain one-worker admission and original ACKs."""
from copy import deepcopy
from pathlib import Path

import pytest

from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.dataflow_terminals import materialized_handle
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext
from demiflow.execution.state_artifacts import StateArtifacts
from test_runtime_v3_graph_adapter import _spec, graph_environment


@pytest.mark.parametrize('mode,routing', [
    ('durable_incremental', 'broadcast'), ('memory_incremental', 'broadcast'),
    ('finite_sealed', 'broadcast'), ('durable_incremental', 'rebalance'),
    ('durable_incremental', 'hash'), ('durable_incremental', 'target')])
def test_single_worker_partition_wave_resume_keeps_original_membership(graph_environment, monkeypatch, mode, routing):
    class Interrupted(Exception): pass
    root = graph_environment
    spec = _spec(root)
    from demiflow.data.sources import ItemsSource
    spec['source'] = ItemsSource(tuple({'x': i, 'group': 'a' if i % 2 == 0 else 'b'} for i in range(3)))
    spec.update(exchange_mode=mode, routing=routing, key_fields=['group'], target_field='group',
                target_classes={'a': ['left'], 'b': ['right']},
                credit_budget={'rows': 8, 'bytes': 8*1024**2})
    executor = LocalDatasetExecutor(workers=1)
    limits = {'checkpoint_rows': 8, 'checkpoint_unconfirmed_rows': 8,
              'checkpoint_interval_ms': 60000, 'timeout_s': 120}
    retire, finish = DataflowExecution._retire_phase, DataflowExecution.finish
    stops, handles, outputs = [], [], []
    def interrupt(execution, image):
        active = image.cut.participants
        router = next(s for s in execution._physical.stages if s.partitions == ('left', 'right'))
        from demiflow.execution.contracts.dataflow_codec import compute_participant_id
        if compute_participant_id(router.stage_id, 'right') in active:
            assert len(active) == 1
            stops.append(image)
            raise Interrupted('after one original router partition ACK')
        return retire(execution, image)
    def observe(execution):
        result = finish(execution)
        stored = materialized_handle(result, execution=execution)
        try:
            outputs.extend(stored.rows())
        finally:
            stored.release()
        handles.append(execution)
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(root/'run', limits=limits) as run:
            reference = run.ref
            with run.action({'kind': 'materialize', 'source': {'fixture': 'partition-waves'}}):
                with monkeypatch.context() as patch:
                    patch.setattr(DataflowExecution, '_retire_phase', interrupt)
                    executor.materialize_graph(spec)
    assert len(stops) == 1
    with RunContext.resume(reference, limits=limits) as run:
        before = run.journal.execution_commits('action-1')
        assert before and all(f['status'] == 'acked' for f in before)
        with run.action({'kind': 'materialize', 'source': {'fixture': 'partition-waves'}}) as action:
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'finish', observe)
                result = executor.materialize_graph(spec)
            assert result['egress_rows'] == 3
            assert result['target_rows'] == {reader: sum(row['reader_id'] == reader for row in outputs)
                                             for reader in ('left', 'right')}
            action.complete({'kind': 'graph-materialized'})
        after = run.journal.execution_commits('action-1')
        assert after[:len(before)] == before
        states = [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3']) for f in after]
        assert states[-1]['cut']['final']
        assert all(s['residency']['schema'] == 'demiflow.durable_residency.v2' for s in states)
        assert all(len(s['cut']['participants']) == 1 for s in states)
        assert {p for s in states for p in s['residency']['active_partitions']} >= {'left', 'right'}
        assert handles[0]._peak_active_workers == 1
        actual = sorted((r['reader_id'], r['row']['x']) for r in outputs)
        if routing == 'broadcast':
            assert actual == [('left', 110), ('left', 111), ('left', 112),
                              ('right', 210), ('right', 211), ('right', 212)]
        elif routing in ('rebalance', 'target'):
            assert actual == [('left', 110), ('left', 112), ('right', 211)]
        else:
            assert sorted(r['row']['x'] - (100 if r['reader_id'] == 'left' else 200)
                          for r in outputs) == [10, 11, 12]
            assert all(len({r['reader_id'] for r in outputs if r['row']['group'] == group}) == 1
                       for group in ('a', 'b'))
        # Every inherited participant must retain an actual earlier ACKed fact;
        # a forged "completed" partition may not be used to skip business work.
        from demiflow.execution.dataflow_durable_residency import validate_recovered
        from demiflow.execution.checkpoint.coordinator import _cut_from_json, _contribution_from_json
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        value = states[-1]
        kwargs = dict(facts=after, artifacts=StateArtifacts(run), current_commit=after[-1]['commit_id'],
                      store=handles[0]._store)
        contributions = tuple(_contribution_from_json(c) for c in value['contributions'])
        validate_recovered(handles[0]._physical, _cut_from_json(value['cut']), contributions,
                           value['residency'], **kwargs)
        forged = deepcopy(value['residency'])
        forged['visited_partitions'].remove('left')
        with pytest.raises(DataflowError):
            validate_recovered(handles[0]._physical, _cut_from_json(value['cut']), contributions, forged, **kwargs)
        with pytest.raises(DataflowError, match='ACKed completion'):
            validate_recovered(handles[0]._physical, _cut_from_json(value['cut']), contributions,
                value['residency'], **{**kwargs, 'facts': [after[-1]]})


@pytest.mark.parametrize('mode,workers', [('finite_sealed', 4), ('memory_incremental', 1)])
def test_explicit_graph_storage_is_sealed_and_not_silently_enlarged(graph_environment, mode, workers):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    spec = _spec(graph_environment)
    spec.update(exchange_mode=mode, storage_bytes=1)
    with pytest.raises(DataflowError, match='(?i)storage|spill'):
        with RunContext.start(graph_environment/'run') as run:
            with run.action({'kind': 'materialize', 'source': {'fixture': 'one-byte-storage'}}):
                LocalDatasetExecutor(workers=workers).materialize_graph(spec)
    assert not run.journal.execution_commits('action-1')
    assert not (graph_environment/'ancestor.jsonl').exists()


@pytest.mark.parametrize('mode', ['finite_sealed', 'memory_incremental'])
def test_graph_storage_remains_per_channel_under_original_aggregate_grant(graph_environment, monkeypatch, mode):
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution.sealed_plans import load_sealed_plan
    original, captures = dataflow.execute_action, []
    def capture(request, **kwargs):
        handle = original(request, **kwargs)
        captures.append(handle)
        return handle
    monkeypatch.setattr(dataflow, 'execute_action', capture)
    spec = _spec(graph_environment)
    amount = 1024**2
    spec.update(exchange_mode=mode, storage_bytes=amount)
    with RunContext.start(graph_environment/'run', limits={'timeout_s': 120}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'per-channel-storage'}}) as action:
            result = LocalDatasetExecutor(workers=1).materialize_graph(spec)
            assert result['target_rows'] == {'left': 3, 'right': 3}
            action.complete({'kind': 'graph-materialized'})
        sealed = load_sealed_plan(run.directory, action.action_id, version='v3')
        graph, physical = sealed['graph'], sealed['physical']
        router = next(n.occurrence_id for n in graph.nodes if 'graph_route' in n.kernel.configuration)
        edge_ids = {e.edge_id for e in graph.edges if router in (e.producer, e.consumer)}
        channels = [e for e in physical.exchanges if e.edge_id in edge_ids and e.mode != 'memory_incremental']
        assert len(channels) == 3
        assert all(e.storage_bytes == amount for e in channels), 'declared limit must not be divided by channel count'
        assert physical.residency.required_spill_bytes == sum(e.storage_bytes for e in physical.exchanges
                                                              if e.mode != 'memory_incremental')
        assert physical.residency.required_spill_bytes <= physical.bounds.max_spill_bytes
        assert len(captures) == 1 and captures[0]._execution._peak_active_workers == 1
