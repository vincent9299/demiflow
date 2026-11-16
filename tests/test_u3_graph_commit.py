"""U3 graph single-commit + full-coverage K6 acceptance (S4b, exit gates 2/3 Local).

The driver schedules global cuts; every worker contributes its K6 fragment and the
driver commits the whole group ONCE through a single CommitCoordinator on a graph
result child action, then acks. These tests prove: one durable acked commit per cut
(no worker owns commit authority), a full-coverage CheckpointManifest per cut
(source/channel/operator/sink participants all covered), committed staging blocks
read back verbatim with the shared expansion applied once, the final cut sealed,
and the empty-source / finite_sealed edges.
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


def _execute(tmp_path, spec, readers, count, *, checkpoint_rows):
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
    _, _, _, _, carriers, physical = _execute(tmp_path, spec, readers, n, checkpoint_rows=64)
    assert any(e.mode == 'finite_sealed' for e in physical.exchanges)
    assert carriers[-1]['cut']['final'] is True
    assert carriers[-1]['dataflow.sources.v3'][0]['next_sequence'] == 12


def test_durable_graph_reclaims_channel_segments(tmp_path):
    from _native_receipt_fixtures import committed_rows
    spec, readers, n = _graph_spec(n=24, cut_rows=8, mode="durable_incremental", readers=("c0", "c1"))
    # This unregistered private byte must never be collected as an owned object.
    foreign = tmp_path / 'run' / '.dataflow-store' / 'foreign-protected.bin'
    foreign.parent.mkdir(parents=True)
    foreign.write_bytes(b'not-a-runtime-object')
    run, action_id, _, facts, carriers, physical = _execute(tmp_path, spec, readers, n, checkpoint_rows=8)
    assert sorted({c['dataflow.sources.v3'][0]['next_sequence'] for c in carriers}) == [8, 16, 24]
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
