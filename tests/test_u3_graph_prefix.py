"""Literal graph prefixes keep original readers, grants and recovery images."""
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import IterableSource, RangeSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext
from demiflow.resources.contracts import ResourceProfile
from _native_graph_fixtures import HISTORICAL_MEMORY, HISTORICAL_WORKERS
from test_u3_graph_commit import _graph_spec, _stream_map


@pytest.mark.parametrize('variant', ['items', 'range', 'two_maps', 'async', 'actor',
    'concurrent', 'thread', 'native', 'opaque', 'source_native', 'resources',
    'partitions', 'host_small', 'host_cpu', 'fusion_off', 'no_hosts'])
def test_literal_prefix_only_colocates_proven_safe_source_and_callbacks(tmp_path, variant):
    from demiflow.execution.graph import _graph_action_request
    from demiflow.execution.dataflow_api import resolve_action_runtime
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_graph_adapter import specialize_graph_stages
    spec, readers, _ = _graph_spec(n=1)
    op = spec['ancestor']['plan'].operations[0]
    def never(row):
        raise AssertionError('compile cannot invoke business code')
    if variant == 'range':
        spec['source'] = RangeSource(1)
    elif variant == 'two_maps':
        spec['ancestor']['plan'] = LogicalPlan((op, MapOp(CallableSpec.create(never))))
    elif variant == 'async':
        async def callback(row):
            raise AssertionError('compile cannot invoke an async callback')
        spec['ancestor']['plan'] = LogicalPlan((AsyncMapOp(CallableSpec.create(callback)),))
    elif variant == 'actor':
        class Actor:
            def __init__(self):
                raise AssertionError('compile cannot construct a factory')
            def __call__(self, row):
                return row
        spec['ancestor']['plan'] = _stream_map(Actor)
    elif variant in {'concurrent', 'thread'}:
        spec['ancestor']['plan'] = LogicalPlan((replace(op,
            stream_options=MapStreamOptions(concurrency=2 if variant == 'concurrent' else 1,
                                            execution='thread' if variant == 'thread' else 'inline')),))
    elif variant == 'native':
        spec['ancestor']['plan'] = LogicalPlan((replace(op,
            native_options=NativeOptions('ray', worker_resources={'cpu': 2})),))
    elif variant == 'source_native':
        spec['source'] = RangeSource(1, native_options=NativeOptions('ray', worker_resources={'cpu': 2}))
    elif variant == 'opaque':
        spec['source'] = IterableSource(lambda: (_ for _ in () ))
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with RunContext.start(tmp_path/'compile', limits={'memory_bytes': HISTORICAL_MEMORY}) as run:
        with run.action({'kind': 'materialize', 'fixture': variant}) as action:
            request = _graph_action_request(spec, executor)
            policy, bounds = resolve_action_runtime(request)
            graph = lower_action(request, bounds=bounds).graph
            stages = partition_stages(graph, policy=policy, bounds=bounds)
            source = graph.sources[0]
            ancestor = next(n.kernel.configuration['graph_result']['ancestor'] for n in graph.nodes
                            if 'graph_result' in n.kernel.configuration)
            if variant == 'resources':
                stages = tuple(replace(s, profile=ResourceProfile(requests=dict(s.profile.requests),
                    requirements={'isolation': 'process'})) if source in s.occurrences else s for s in stages)
            if variant == 'partitions':
                stages = tuple(replace(s, partitions=(s.partitions[0], s.partitions[0]+'-other'),
                    replica_target=2, replica_max=2) if source in s.occurrences else s for s in stages)
            if variant == 'fusion_off':
                policy = replace(policy, fusion=False)
            hosts = None if variant == 'no_hosts' else (SimpleNamespace(logical_resources={
                'cpu_millis': 1000 if variant == 'host_cpu' else 16000,
                'memory_bytes': 1 if variant == 'host_small' else HISTORICAL_MEMORY}),)
            before = {o: s for s in stages for o in s.occurrences}
            result = specialize_graph_stages(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
            after = {o: s for s in result for o in s.occurrences}
            assert set(after) == set(before)
            expected = variant in {'items', 'range', 'two_maps'}
            assert (after[source] is after[ancestor]) is expected
            if expected:
                chain = tuple(next(n.kernel.configuration['graph_literal_prefix'] for n in graph.nodes
                                   if 'graph_literal_prefix' in n.kernel.configuration))
                originals = [before[o] for o in chain]
                assert after[source].occurrences == chain
                assert after[source].profile.requests['cpu_millis'] == sum(s.profile.requests['cpu_millis'] for s in originals)
                assert after[source].profile.requests['memory_bytes'] == sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in originals)
                assert after[source].resident_bytes == after[source].profile.requests['memory_bytes']
                assert after[source].output_bytes == sum(s.output_bytes for s in originals)
                assert after[source].state_bytes == sum(s.state_bytes for s in originals)
                assert after[source].kernel_refs == tuple(dict.fromkeys(ref for s in originals for ref in s.kernel_refs))
            else:
                assert after[source] == before[source]
                assert after[ancestor] == before[ancestor]
            router = next(n.occurrence_id for n in graph.nodes if 'graph_route' in n.kernel.configuration)
            assert after[router] is not after[source]
            assert tuple(after[router].partitions) == tuple(readers)
            targets = [n.occurrence_id for n in graph.nodes if 'graph_reader' in n.kernel.configuration]
            assert len({after[o].stage_id for o in targets}) == len(readers)
            assert all(after[o] is not after[source] for o in targets)
            action.complete({'kind': 'compile-only'})


def test_colocated_prefix_resumes_original_source_cursor_and_sealed_layout(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.action_driver import dataflow
    from _native_graph_fixtures import assert_native_graph
    spec, readers, count = _graph_spec(n=24, cut_rows=8, readers=('c0', 'c1'))
    log = str(tmp_path/'calls')
    def observed(row):
        with open(log, 'a') as stream:
            stream.write(str(row['x'])+'\n')
        return {**row, 'x': row['x']*2}
    spec['ancestor']['plan'] = _stream_map(observed)
    limits = {'memory_bytes': HISTORICAL_MEMORY, 'checkpoint_rows': 8,
              'checkpoint_unconfirmed_rows': 8, 'checkpoint_interval_ms': 60000}
    original = DataflowCheckpointCoordinator.commit
    class Interrupted(Exception):
        pass
    def interrupt(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        if not image.cut.final:
            raise Interrupted()
        return image
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with monkeypatch.context() as patch:
        patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupt)
        with pytest.raises(Interrupted):
            with RunContext.start(tmp_path/'run', limits=limits) as run:
                reference = run.ref
                with run.action({'kind': 'materialize', 'fixture': 'shared-prefix-restore'}) as action:
                    executor.materialize_graph(spec)
    journal = RunJournal(reference.directory)
    prior = journal.execution_commits('action-1')
    assert len(prior) == 1 and prior[0]['status'] == 'acked'
    bundle = load_sealed_plan(reference.directory, 'action-1', version='v3')
    graph, physical = bundle['graph'], bundle['physical']
    prefix = next(tuple(n.kernel.configuration['graph_literal_prefix']) for n in graph.nodes
                  if 'graph_literal_prefix' in n.kernel.configuration)
    assert any(s.occurrences == prefix for s in physical.stages)
    assert Counter(Path(log).read_text().splitlines()) == Counter(map(str, range(8)))
    def no_recompile(*args, **kwargs):
        raise AssertionError('resume must use its original sealed placement')
    monkeypatch.setattr(dataflow, 'compile_physical_dataflow', no_recompile)
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with RunContext.resume(reference) as resumed:
        with resumed.action({'kind': 'materialize', 'fixture': 'shared-prefix-restore'}) as action:
            result = executor.materialize_graph(spec)
            action.complete({'kind': 'graph-materialized', 'egress': result['egress_rows']})
        assert_native_graph(resumed, action.action_id, result, readers=readers, count=count)
        assert journal.execution_commits('action-1')[:len(prior)] == prior
    assert (result['dag_id'], result['plan_id']) == (graph.dag_id, physical.plan_id)
    assert Counter(Path(log).read_text().splitlines()) == Counter(map(str, range(count)))


def test_range_prefix_uses_original_reader_and_exact_committed_rows(tmp_path):
    from _native_receipt_fixtures import committed_rows
    from demiflow.execution.state_artifacts import StateArtifacts
    from demiflow.execution.sealed_plans import load_sealed_plan
    spec, readers, count = _graph_spec(n=8, cut_rows=4, readers=('c0', 'c1'))
    spec['source'] = RangeSource(count)
    spec['ancestor']['plan'] = _stream_map(lambda row: {'x': row['id'] * 2})
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with RunContext.start(tmp_path/'run', limits={'memory_bytes': HISTORICAL_MEMORY,
            'checkpoint_rows': 4, 'checkpoint_unconfirmed_rows': 4,
            'checkpoint_interval_ms': 60000}) as run:
        with run.action({'kind': 'materialize', 'fixture': 'range-prefix'}) as action:
            result = executor.materialize_graph(spec)
            action.complete({'kind': 'graph-materialized', 'egress': result['egress_rows']})
        facts = run.journal.execution_commits(action.action_id)
        assert facts and all(f['status'] == 'acked' for f in facts)
        carriers = [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3']) for f in facts]
        cursors = [c['dataflow.sources.v3'][0] for c in carriers]
        # Range's actual original cursor is `next`; Items uses `index`.
        assert sorted({c['next_sequence'] for c in cursors}) == [4, 8]
        assert all(c['offset']['next'] == c['next_sequence'] for c in cursors)
        assert cursors[-1]['end_of_input'] is True
        rows = committed_rows(run, action.action_id, facts[-1])
        assert len(rows) == count * len(readers)
        for reader in readers:
            assert [r['row'] for r in rows if r['reader_id'] == reader] == [{'x': i*2} for i in range(count)]
        bundle = load_sealed_plan(run.directory, action.action_id, version='v3')
        source = bundle['graph'].sources[0]
        owner = next(s for s in bundle['physical'].stages if source in s.occurrences)
        assert len(owner.occurrences) == 2 and owner.profile.requests['cpu_millis'] == 2000
    assert result['target_rows'] == {reader: count for reader in readers}
    assert result['egress_rows'] == count
