"""Historical graph guarantees read from the original native v3 commits.

One driver commits each actual cut; final ACK contributions cover every sealed
participant. Read exact cumulative values and source frontiers, preserve fixed
history and restore, and collect only unrooted owned artifacts after owner exit.
The retired K6/result-child implementation is not another runtime authority.
"""
import sys

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                                stream_options=MapStreamOptions(concurrency=1)),))


def _graph_spec(n=24, cut_rows=8, mode="durable_incremental", readers=("c0", "c1", "c2", "c3", "c4", "c5")):
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    targets = [{"plan": _stream_map(passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(shared_double),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": mode,
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    return spec, list(readers), n


class _AfterGraphAction(RuntimeError):
    def __init__(self, proof):
        super().__init__('interrupted after graph ACK, before run completion')
        self.proof = proof


def _execute(tmp_path, spec, readers, count, *, checkpoint_rows, interrupt_after_action=False):
    from _native_graph_fixtures import (HISTORICAL_MEMORY, HISTORICAL_WORKERS,
        assert_native_graph, assert_managed_graph)
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS, unified_graph=True)
    with RunContext.start(tmp_path / "run", limits={
            "memory_bytes": HISTORICAL_MEMORY, "checkpoint_rows": checkpoint_rows,
            "checkpoint_unconfirmed_rows": checkpoint_rows,
            "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-commit"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts, carriers, physical = assert_native_graph(run, action_id, result,
            readers=readers, count=count)
        assert_managed_graph(executor, run.directory, result)
        if interrupt_after_action:
            raise _AfterGraphAction((run, action_id, result, facts, carriers, physical))
    return run, action_id, result, facts, carriers, physical


def test_multi_cut_single_commit_full_k6(tmp_path):
    # The original K6 guarantee is full actual participation and one authority.
    # Native transfer cuts may repeat a source frontier, never skip 8/16/24.
    spec, readers, n = _graph_spec(n=24, cut_rows=8)
    _, _, _, _, carriers, _ = _execute(tmp_path, spec, readers, n, checkpoint_rows=8)
    assert sorted({c['dataflow.sources.v3'][0]['next_sequence'] for c in carriers}) == [8, 16, 24]


def test_empty_source_sealed_cut(tmp_path):
    spec, readers, n = _graph_spec(n=0, cut_rows=8)
    _, _, result, facts, carriers, _ = _execute(tmp_path, spec, readers, n, checkpoint_rows=8)
    # Empty phases still supply real EOI contributions. Exactly one final seal.
    assert all(c['dataflow.sources.v3'][0]['next_sequence'] == 0 for c in carriers)
    assert all(f['receipts']['materialize']['data']['rows'] == 0 for f in facts)
    assert result['egress_rows'] == 0


def test_finite_sealed_mode_commits(tmp_path):
    spec, readers, n = _graph_spec(n=12, cut_rows=12, mode="finite_sealed", readers=("c0", "c1"))
    with pytest.raises(_AfterGraphAction) as stopped:
        _execute(tmp_path, spec, readers, n, checkpoint_rows=64, interrupt_after_action=True)
    run, action_id, original, facts, carriers, physical = stopped.value.proof
    assert any(e.mode == 'finite_sealed' for e in physical.exchanges)
    assert carriers[-1]['cut']['final'] is True
    assert carriers[-1]['dataflow.sources.v3'][0]['next_sequence'] == 12
    # Recovery reads the original sealed plan and final ACK, starting no worker.
    from _native_graph_fixtures import HISTORICAL_WORKERS, assert_native_graph
    records = set(run.directory.with_name('run-driver-resources').glob('units/*/*/process.json'))
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS, unified_graph=True)
    with RunContext.resume(run.ref) as resumed:
        with resumed.action({"kind": "materialize", "source": {"fixture": "u3-graph-commit"}}) as execution:
            recovered = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", egress=recovered['egress_rows']))
        assert execution.action_id == action_id
        assert_native_graph(resumed, action_id, recovered, readers=readers, count=n)
        assert resumed.journal.execution_commits(action_id) == facts
        assert (recovered['dag_id'], recovered['plan_id'], recovered['result_ref']) == (
            original['dag_id'], original['plan_id'], original['result_ref'])
        assert recovered['workers'] == 0
    assert set(run.directory.with_name('run-driver-resources').glob('units/*/*/process.json')) == records


def test_durable_graph_reclaims_channel_segments(tmp_path):
    from _native_receipt_fixtures import committed_rows
    spec, readers, n = _graph_spec(n=24, cut_rows=8, mode="durable_incremental", readers=("c0", "c1"))
    # This unregistered private byte must never be collected as an owned object.
    foreign = tmp_path / 'run' / '.dataflow-store' / 'foreign-protected.bin'
    foreign.parent.mkdir(parents=True)
    foreign.write_bytes(b'not-a-runtime-object')
    run, action_id, _, facts, carriers, physical = _execute(tmp_path, spec, readers, n, checkpoint_rows=8)
    assert any(e.mode == 'durable_incremental' for e in physical.exchanges)
    # GC operates on the original object settlements after confirmed owner exit.
    # ACKed channel snapshots remain roots for historical recovery, so expecting
    # every old transport payload to disappear would itself destroy recovery.
    assert run._published_gc_result['blocked'] is None
    assert run._published_gc_result['removed'] > 0
    with run.journal._connection() as db:
        collected = db.execute('SELECT store_id,digest,status FROM runtime_dataflow_gc').fetchall()
        assert collected and all(status == 'done' for _, _, status in collected)
        assert not db.execute("SELECT 1 FROM runtime_reservations WHERE name='artifact_staging_bytes' AND amount>0").fetchone()
    for store_id, digest, _ in collected:
        assert not (run.directory / '.dataflow-store' / store_id / 'objects' / digest).exists()
    assert foreign.read_bytes() == b'not-a-runtime-object'
    assert run.journal.execution_commits(action_id) == facts
    rows = committed_rows(run, action_id, facts[-1])
    for reader in readers:
        assert [row['row'] for row in rows if row['reader_id'] == reader] == [{'x': i * 2} for i in range(n)]
    assert sorted({c['dataflow.sources.v3'][0]['next_sequence'] for c in carriers}) == [8, 16, 24]


@pytest.mark.parametrize('variant', ['plain', 'concurrent', 'async', 'actor', 'resources', 'fusion_off'])
def test_private_tag_colocation_keeps_reader_and_resource_boundaries(tmp_path, variant):
    from dataclasses import replace
    from _native_graph_fixtures import HISTORICAL_MEMORY, HISTORICAL_WORKERS
    from demiflow.execution.graph import _graph_action_request
    from demiflow.execution.dataflow_api import resolve_action_runtime
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_graph_adapter import specialize_graph_stages
    from demiflow.resources.contracts import ResourceProfile

    spec, readers, _ = _graph_spec(n=1)
    if variant == 'concurrent':
        spec['targets'][0]['plan'] = LogicalPlan((MapOp(CallableSpec.create(lambda row: row),
            stream_options=MapStreamOptions(concurrency=2)),))
    elif variant == 'async':
        from demiflow.data.plan import AsyncMapOp
        async def callback(row):
            raise AssertionError('capture must not invoke async business code')
        spec['targets'][0]['plan'] = LogicalPlan((AsyncMapOp(CallableSpec.create(callback)),))
    elif variant == 'actor':
        class Actor:
            def __call__(self, row):
                raise AssertionError('compile must not execute or construct this actor')
        spec['targets'][0]['plan'] = _stream_map(Actor)
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with RunContext.start(tmp_path/'compile', limits={'memory_bytes': HISTORICAL_MEMORY}) as run:
        with run.action({'kind': 'materialize', 'fixture': variant}) as execution:
            request = _graph_action_request(spec, executor)
            policy, bounds = resolve_action_runtime(request)
            graph = lower_action(request, bounds=bounds).graph
            stages = partition_stages(graph, policy=policy, bounds=bounds)
            tags = {n.occurrence_id: n.kernel.configuration['graph_private_tag'] for n in graph.nodes
                    if 'graph_private_tag' in n.kernel.configuration}
            target = next(v['producer'] for v in tags.values() if v['reader'] == 'c0')
            if variant == 'resources':
                stages = tuple(replace(stage, profile=ResourceProfile(
                    requests=dict(stage.profile.requests), requirements={'isolation': 'process'}))
                    if target in stage.occurrences else stage for stage in stages)
            if variant == 'fusion_off':
                policy = replace(policy, fusion=False)
            original = {occ: stage for stage in stages for occ in stage.occurrences}
            placed = specialize_graph_stages(graph, stages, policy=policy, bounds=bounds)
            owners = {occ: stage for stage in placed for occ in stage.occurrences}
            assert set(owners) == set(original)
            router = next(n.occurrence_id for n in graph.nodes if 'graph_route' in n.kernel.configuration)
            assert set(owners[router].partitions) == set(readers)
            for tag, marker in tags.items():
                target = marker['producer']
                expected = variant != 'fusion_off' and (variant == 'plain' or marker['reader'] != 'c0')
                assert (owners[tag] is owners[target]) is expected
                if expected:
                    before = (original[tag], original[target])
                    merged = owners[tag]
                    assert merged.occurrences == (target, tag)
                    assert merged.resident_bytes >= sum(s.resident_bytes for s in before)
                    assert merged.profile.requests['memory_bytes'] >= sum(s.profile.requests['memory_bytes'] for s in before)
                    assert merged.output_bytes == sum(s.output_bytes for s in before)
                    assert merged.state_bytes == sum(s.state_bytes for s in before)
                    assert merged.profile.requests['cpu_millis'] == max(s.profile.requests['cpu_millis'] for s in before)
                else:
                    assert owners[tag] == original[tag]
                    assert owners[target] == original[target]
            assert len({owners[v['producer']].stage_id for v in tags.values()}) == len(readers)
            execution.complete({'kind': 'compile-only'})
