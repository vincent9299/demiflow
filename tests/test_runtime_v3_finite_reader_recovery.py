"""Actual finite consumer prefix, original ACK/GC, and Local/Ray recovery.

The same business case checks partial consumer ACK, original GC roots and
exact typed recovery. Component routing/cursor rejection and original U3/close
gates remain separate.
"""
import json
import math

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.storage.segments import tree_refs
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.pipeline.run_context import RunContext
from runtime_v3_recovery_scenarios import _provider
from runtime_v3_regression_inventory import assert_private


class InterruptedAfterFinitePrefix(Exception):
    pass


def _pipeline():
    left = [{'k': 0, 'i': i, 'left_typed': (b'\x00', -0.0, (i,))}
            for i in range(100)]
    right = [{'k': 0, 'j': j, 'right_typed': (b'\xff', (j, -0.0))}
             for j in range(10)]
    return data.from_items(left).join(data.from_items(right), on='k')


def _read_json(store, ref, bounds):
    assert ref.size_bytes <= bounds.max_control_bytes
    return json.loads(b''.join(store.read(ref, max_bytes=bounds.max_control_bytes)))


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_finite_reader_prefix_original_ack_gc_and_exact_recovery(tmp_path, monkeypatch, provider):
    assert_private()
    if provider == 'ray_core':
        import ray
        assert not ray.is_initialized(), 'fixture must own its private Ray Core cluster'
        assert ray.ObjectRef is ray._raylet.ObjectRef
    limits = {'memory_bytes': 128 * 1024**2, 'max_row_bytes': 16 * 1024**2,
              'max_batch_bytes': 16 * 1024**2, 'checkpoint_rows': 64,
              'checkpoint_interval_ms': 60000, 'timeout_s': 120}
    prepare, close = DataflowExecution.prepare, DataflowExecution.close
    handle_event, commit = DataflowExecution._handle_event, DataflowCheckpointCoordinator.commit
    launch = DataflowExecution._prepare_launch
    first, boundary, seen, resumed_stages = {}, {}, [], []

    def prepared(execution):
        value = prepare(execution)
        assert execution._policy.provider == provider
        bounds = execution._physical.bounds
        assert bounds.max_resident_bytes == limits['memory_bytes']
        assert bounds.max_row_bytes == bounds.max_batch_bytes == 16 * 1024**2
        if not first:
            graph, physical = execution._graph, execution._physical
            terminal, = (n for n in graph.nodes if n.kind == 'terminal')
            join, = (n for n in graph.nodes if n.kernel.kind == 'join')
            terminal_stage, = (s for s in physical.stages if terminal.occurrence_id in s.occurrences)
            incoming, = (e for e in graph.edges if e.consumer == terminal.occurrence_id)
            spec, = (e for e in physical.exchanges if e.edge_id == incoming.edge_id)
            # Explicit original R/B windows make simultaneous join+terminal
            # residency infeasible under M=128MiB, even after the compiler
            # shares optional codec slack; do not force another planner.
            assert incoming.producer == join.occurrence_id
            assert spec.mode == 'finite_sealed' and spec.routing in {'forward', 'rebalance'}
            assert len(spec.consumer_partitions) == len(terminal_stage.partitions) == 1
            assert tuple(spec.consumer_partitions) == tuple(terminal_stage.partitions)
            upstream = {n.occurrence_id for n in graph.nodes if n.kind == 'source'} | {join.occurrence_id}
            completed = {s.stage_id for s in physical.stages if set(s.occurrences) & upstream}
            assert completed and terminal_stage.stage_id not in completed
            # One owned, intentionally unrooted publication. The unchanged
            # DataflowExecution.close must collect it; no extra owner or GC stub.
            raw = b'finite-reader-private-unrooted-orphan:' + provider.encode()
            orphan = execution._store.put(iter((raw,)), max_bytes=len(raw), media_type='application/octet-stream')
            first.update(execution=execution, spec=spec, terminal_stage=terminal_stage.stage_id,
                         completed=completed, orphan=orphan)
        return value

    def closed(execution):
        try:
            return close(execution)
        finally:
            if execution is first.get('execution'):
                first['gc'] = getattr(execution, '_published_gc', None)

    def event(execution, message):
        value = handle_event(execution, message)
        if message.kind == 'terminal':
            seen.append(message.payload['record_id'])
            if len(seen) == 5:
                execution.request_checkpoint()
        return value

    def committed(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)  # Original journal ACK first.
        if len(seen) < 5 or image.cut.final:
            return image
        execution, spec = first['execution'], first['spec']
        assert coordinator is execution._checkpoint
        publication = execution._publication(image)
        prefix = publication['manifest']['total_rows']
        assert 0 < prefix < 1000, 'must ACK a strict finite-consumer prefix'
        residency = coordinator.residency
        assert residency and residency['schema'] == 'demiflow.finite_residency.v1'
        assert not residency['phase_complete']
        completed = set(residency['completed_stages'])
        assert first['completed'] <= completed
        assert first['terminal_stage'] in residency['active_stages']
        frontier_key = identity(['worker-frontier', spec.consumer_partitions[0]])
        frontier_ref, = (c.operator_states[frontier_key] for c in image.contributions
                        if c.participant_id in image.cut.participants and frontier_key in c.operator_states)
        frontier = _read_json(coordinator._store, frontier_ref, coordinator._physical.bounds)
        assert frontier['schema'] == 'demiflow.worker-frontier.v3'
        reads = frontier['finite_reads']
        assert set(reads) == {spec.edge_id}, 'this oracle requires exactly one finite input and one consumer'
        reader = reads[spec.edge_id]
        assert reader['seal_ref'] == residency['finite_seals'][spec.channel_id]
        assert set(reader['positions']) == set(spec.producer_partitions)
        assert sum(reader['positions'].values()) == prefix
        seal_ref = ArtifactRef(**reader['seal_ref'])
        seal = _read_json(coordinator._store, seal_ref, coordinator._physical.bounds)
        assert sum(member['last_sequence'] + 1 for member in seal['producers']) == 1000
        # Preserve original refs for inspection only. A Python set is not a
        # store pin; the real original journal must be sufficient for GC.
        protected = {frontier_ref, seal_ref, image.confirmed_sinks['take_all']}
        for member in seal['producers']:
            protected.update(tree_refs(coordinator._store, ArtifactRef(**member['manifest_ref']),
                                       coordinator._physical.bounds))
        fact = coordinator._action.context.journal.execution_commit(image.commit_id)
        assert fact['status'] == 'acked'
        boundary.update(commit_id=image.commit_id, prefix=prefix, completed=completed,
                        protected=protected, fact=fact, plan_id=coordinator._physical.plan_id)
        raise InterruptedAfterFinitePrefix('actual nonfinal finite-reader ACK')

    with _provider(provider):
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, 'prepare', prepared)
            patch.setattr(DataflowExecution, 'close', closed)
            patch.setattr(DataflowExecution, '_handle_event', event)
            patch.setattr(DataflowCheckpointCoordinator, 'commit', committed)
            with pytest.raises(InterruptedAfterFinitePrefix, match='finite-reader ACK'):
                with RunContext.start(tmp_path/'run', limits=limits) as run:
                    run_ref = run.ref
                    _pipeline().take_all()
        assert boundary and 0 < boundary['prefix'] < 1000
        gc = first['gc']
        assert gc is not None and gc['blocked'] is None and gc['removed'] >= 1
        assert gc['bytes'] >= first['orphan'].size_bytes
        assert not first['execution']._store.inspect(first['orphan'])
        assert all(first['execution']._store.inspect(ref) for ref in boundary['protected'])

        def launched(execution, lease, stage):
            assert execution._policy.provider == provider
            assert execution._physical.plan_id == boundary['plan_id']
            assert stage.stage_id not in boundary['completed'], 'completed source/join must not restart'
            resumed_stages.append(stage.stage_id)
            return launch(execution, lease, stage)

        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_prepare_launch', launched)
            with RunContext.resume(run_ref) as resumed:
                before = resumed.journal.execution_commits('action-1')
                assert resumed.journal.execution_commit(boundary['commit_id']) == boundary['fact']
                # Original completed GC fact proves actual removal/refund of
                # this orphan, not merely a successful optional-GC return.
                with resumed.journal._connection() as db:
                    reclaimed = db.execute('SELECT bytes,status FROM runtime_dataflow_gc '
                        'WHERE store_id=? AND digest=?',
                        (first['orphan'].store_id, first['orphan'].digest)).fetchall()
                assert reclaimed == [(first['orphan'].size_bytes, 'done')]
                rows = _pipeline().take_all()
                after = resumed.journal.execution_commits('action-1')
                assert after[:len(before)] == before and after[-1]['status'] == 'acked'
        assert resumed_stages and not set(resumed_stages) & boundary['completed']
        assert len(rows) == 1000
        assert sorted((r['i'], r['j']) for r in rows) == [(i, j) for i in range(100) for j in range(10)]
        for row in rows:
            left, right = row['left_typed'], row['right_typed']
            assert row['k'] == 0 and type(left) is type(right) is tuple
            assert type(left[0]) is bytes and left[0] == b'\x00'
            assert type(left[1]) is float and math.copysign(1, left[1]) == -1
            assert type(left[2]) is tuple and left[2] == (row['i'],)
            assert type(right[0]) is bytes and right[0] == b'\xff'
            assert type(right[1]) is tuple and right[1][0] == row['j']
            assert type(right[1][1]) is float and math.copysign(1, right[1][1]) == -1
