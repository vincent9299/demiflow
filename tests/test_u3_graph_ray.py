"""U3 Ray Core graph carriage + SEM + fault matrix (S5, exit gate 3 Ray).

Single-host Ray Core is semantic-adaptation evidence only (design §8): no
multi-host/GPU/performance claim. Graph actions run through the unified worker
runtime with Ray actor hosts, original source capture, RunJournal and the sole
driver commit authority. The separate large-payload carriage test retains its
original Ray object read and checksum oracle.

SEM proves the same graph/source yields an identical output multiset, record
identity, contiguous original commit history, final source cursors and runtime_owner
across Local and Ray; legal physical stage/cut counts can differ. The fault matrix covers producer/consumer actor death
(whole-graph restore from the acked cut, exactly once) and sealed storage early
failure with no unauthorized publication.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")
from ray.exceptions import RayActorError

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.contracts.checkpoint import validate_checkpoint_manifest
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunAction, RunContext

pytestmark = [
    pytest.mark.ray,
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="managed units require Linux"),
]


@pytest.fixture(scope="module")
def ray_cluster():
    # Keep the original single-host scheduling tokens and all business oracles.
    import tempfile
    assert not ray.is_initialized(), 'test must own a new private Ray cluster'
    with tempfile.TemporaryDirectory(prefix='demiflow-ray-u3-', dir='/tmp') as root:
        try:
            ray.init(address='local', num_cpus=8, include_dashboard=False,
                     logging_level='ERROR', _temp_dir=root)
            yield
        finally:
            ray.shutdown()


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                                stream_options=MapStreamOptions(concurrency=1)),))


def _graph_spec(n=12, cut_rows=4, mode="memory_incremental", readers=("c0", "c1"),
                ancestor_fn=None, target_fns=None):
    # Nested closures: cloudpickle serializes by value so neither the Local worker
    # process nor the Ray actor need import this test module.
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    targets = []
    for rid in readers:
        fn = (target_fns or {}).get(rid, passthrough)
        targets.append({"plan": _stream_map(fn), "logical_node_id": "node-" + rid,
                        "occurrence_id": rid + "-p0", "reader_id": rid})
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(ancestor_fn or shared_double),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": mode,
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    return spec, list(readers), n


def _committed_rows(run, action_id, rid, facts):
    from runtime_v3_graph_history import committed_rows
    return committed_rows(run, action_id, rid, facts)


def _commit_shape(run, action_id):
    facts = run.journal.execution_commits(action_id)
    return {"sequences": [f["sequence"] for f in facts],
            "positions": [f["intent"]["position"]["next_ordinal"] for f in facts],
            "statuses": [f["status"] for f in facts],
            "commit_ids": [f["commit_id"] for f in facts]}


def _k6_shape(run, action_id):
    from demiflow.execution.state_artifacts import StateArtifacts
    shapes = []
    for fact in run.journal.execution_commits(action_id):
        value = StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
        shapes.append({'sequence': fact['sequence'], 'sealed': value['cut']['final'],
            'participants': list(value['cut']['participants']),
            'sources': sorted((c['next_sequence'], c['end_of_input']) for c in value['dataflow.sources.v3'])})
    assert sum(s['sealed'] for s in shapes) == 1 and shapes[-1]['sealed']
    return shapes


def _exit_owned_actor(marker, driver_pid):
    import psutil
    pid = os.getpid()
    assert pid != driver_pid, 'fatal fixture must execute in its owned Ray actor'
    with Path(marker).open('x') as stream:
        json.dump({'pid': pid, 'driver_pid': driver_pid, 'created': psutil.Process().create_time(), 'exit_code': 17}, stream)
        stream.flush(); os.fsync(stream.fileno())
    os._exit(17)


def _assert_actor_exited(marker, run):
    import psutil
    fact = json.loads(Path(marker).read_text())
    assert fact['driver_pid'] == os.getpid() and fact['pid'] != os.getpid() and fact['exit_code'] == 17
    try:
        process = psutil.Process(fact['pid'])
        assert process.create_time() != fact['created'] or process.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        pass
    with run.journal._connection() as db:
        allocations = [json.loads(row[0]) for row in db.execute('SELECT document FROM resource_allocations')]
    assert allocations and all(a['cleanup'] == 'CONFIRMED' and a['observation'] == 'FRESH'
        and a['evidence_ref'].startswith('ray-actor-dead:') for a in allocations)


def _owner(run):
    with run.journal._connection() as db:
        return db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()


# ---------------- basic Ray graph: real actor hosts, single commit ----------------

def test_ray_graph_two_targets_shared_egress_once(tmp_path, ray_cluster):
    spec, readers, n = _graph_spec(n=12, cut_rows=4)
    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_unconfirmed_rows": 4,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", workers=result["workers"],
                                    egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id)
        assert [f["sequence"] for f in facts] == list(range(1, len(facts) + 1))
        assert all(f["status"] == "acked" for f in facts)
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run) == (run.attempt, run.fence)
    with run.journal._connection() as db:
        allocations = db.execute('SELECT count(*) FROM resource_allocations').fetchone()[0]
    assert result['workers'] == allocations and allocations > 0
    assert 'ray.data' not in sys.modules
    assert result["egress_rows"] == n  # shared expansion once, not 2n
    for rid in readers:
        assert result["target_rows"][rid] == n
    assert all(result["final_arrival"][rid] is not None for rid in ["ancestor", *readers])


# ---------------- SEM: same graph/source, Local vs Ray, identical semantics ----------------

def test_sem_local_ray_graph_equivalence(tmp_path, ray_cluster):
    spec, readers, n = _graph_spec(n=12, cut_rows=4)
    limits = {"checkpoint_rows": 4, "checkpoint_unconfirmed_rows": 4, "checkpoint_interval_ms": 60000}

    local_executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    with RunContext.start(tmp_path / "local-run", limits=limits) as local_run:
        with local_run.action({"kind": "materialize", "source": {"fixture": "u3-sem"}}) as execution:
            local_result = local_executor.materialize_graph(spec)
            local_action = execution.action_id
            execution.complete(dict(kind="graph-materialized"))
        local_facts = local_run.journal.execution_commits(local_action)
        local_rows = {rid: _committed_rows(local_run, local_action, rid, local_facts) for rid in readers}
        local_commits = _commit_shape(local_run, local_action)
        local_owner = _owner(local_run)
        assert local_owner == (local_run.attempt, local_run.fence)
    local_k6 = _k6_shape(local_run, local_action)

    ray_executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.start(tmp_path / "ray-run", limits=limits) as ray_run:
        with ray_run.action({"kind": "materialize", "source": {"fixture": "u3-sem"}}) as execution:
            ray_result = ray_executor.materialize_graph(spec)
            ray_action = execution.action_id
            execution.complete(dict(kind="graph-materialized"))
        ray_facts = ray_run.journal.execution_commits(ray_action)
        ray_rows = {rid: _committed_rows(ray_run, ray_action, rid, ray_facts) for rid in readers}
        ray_commits = _commit_shape(ray_run, ray_action)
        ray_owner = _owner(ray_run)
        assert ray_owner == (ray_run.attempt, ray_run.fence)
    ray_k6 = _k6_shape(ray_run, ray_action)

    expected = [{"x": i * 2} for i in range(n)]
    for rid in readers:
        assert local_rows[rid] == expected
        assert ray_rows[rid] == expected
        # Output multiset + record identity (x value) identical across providers.
        assert sorted(local_rows[rid], key=lambda r: r["x"]) == sorted(ray_rows[rid], key=lambda r: r["x"])
    # Physical cuts and worker waves may differ. Each side still has one
    # contiguous original journal, immutable ordered payload prefixes, one final
    # seal and the same complete source frontier and target values.
    for commits in (local_commits, ray_commits):
        assert commits['sequences'] == list(range(1, len(commits['sequences']) + 1))
        assert commits['positions'] == commits['sequences']
        assert commits['statuses'] == ['acked'] * len(commits['sequences'])
        assert len(set(commits['commit_ids'])) == len(commits['commit_ids'])
    assert local_k6[-1]['sources'] == ray_k6[-1]['sources'] == [(n, True)]
    assert local_result['egress_rows'] == ray_result['egress_rows'] == n
    assert local_result['target_rows'] == ray_result['target_rows'] == {reader: n for reader in readers}
    assert 'ray.data' not in sys.modules


# ---------------- ray_object large payload: ref on the wire, consumer ray.get direct ----------------


# ---------------- Ray fault matrix: actor death -> whole-graph restore exactly once ----------------

def test_ray_graph_producer_death_recovers_acked_prefix_exactly_once(tmp_path, ray_cluster):
    from runtime_v3_graph_history import capture_acks, assert_acks_retained, acknowledged_ancestor_rows
    from runtime_v3_owned_crash import assert_worker_loss
    driver_pid = os.getpid()
    from collections import Counter
    from demiflow.execution.graph import run_ray_graph
    crash_marker = tmp_path / "ancestor-crashed-once"
    trace = tmp_path / "ancestor-trace"

    def crashing_ancestor(row):
        # Trace each processed row, then kill the actor process once on the first
        # row x==8. The authenticated frontier below proves the committed prefix;
        # a physical pressure cut need not contain exactly checkpoint_rows rows.
        with open(trace, "a") as stream:
            stream.write(str(row["x"]) + "\n")
        if row["x"] == 8 and not crash_marker.exists():
            _exit_owned_actor(crash_marker, driver_pid)
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(n=24, cut_rows=8, ancestor_fn=crashing_ancestor)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8, "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, RayActorError, OSError, DataflowError)) as failure:
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-producer-fault"}}):
                run_ray_graph(spec)
    assert_worker_loss(failure.value)
    _assert_actor_exited(crash_marker, run)
    original_acks = capture_acks(run, 'action-1')
    acknowledged_prefix = acknowledged_ancestor_rows(run, 'action-1', original_acks)
    assert 0 < acknowledged_prefix <= 8

    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.resume(reference, limits=limits) as run2:
        assert run2.attempt == 2
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-ray-producer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id)
        assert [f["sequence"] for f in facts] == list(range(1, len(facts) + 1))  # contiguous across the fault
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        assert_acks_retained(run2, action_id, original_acks)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run2) == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n
    # Original committed business calls are never replayed; the unacknowledged
    # suffix through the fatal call is replayed once, and later inputs run once.
    counts = Counter(int(line) for line in trace.read_text().splitlines())
    assert set(counts) == set(range(n))
    assert all(counts[i] == 1 for i in range(acknowledged_prefix))
    assert all(counts[i] == 2 for i in range(acknowledged_prefix, 9))
    assert all(counts[i] == 1 for i in range(9, n))


def test_ray_graph_consumer_death_recovers_whole_graph_exactly_once(tmp_path, ray_cluster):
    from runtime_v3_graph_history import capture_acks, assert_acks_retained
    from runtime_v3_owned_crash import assert_worker_loss
    driver_pid = os.getpid()
    from demiflow.execution.graph import run_ray_graph
    crash_marker = tmp_path / "target-crashed-once"

    def crashing_target(row):
        # Kill exactly one target actor once, on the first POST-cut row (x==16 doubled
        # == input 8). O_EXCL makes the marker atomic so racing broadcast targets kill
        # only one; cut 8 is already durable when the death lands.
        if row["x"] == 16:
            try:
                _exit_owned_actor(crash_marker, driver_pid)
            except FileExistsError:
                return row
        return row

    spec, readers, n = _graph_spec(n=24, cut_rows=8, target_fns={"c0": crashing_target})
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8, "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, RayActorError, OSError, DataflowError)) as failure:
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-consumer-fault"}}):
                run_ray_graph(spec)
    assert_worker_loss(failure.value)
    _assert_actor_exited(crash_marker, run)
    original_acks = capture_acks(run, 'action-1')

    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-ray-consumer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id)
        assert [f["sequence"] for f in facts] == list(range(1, len(facts) + 1))
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        assert_acks_retained(run2, action_id, original_acks)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run2) == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n


# ---------------- Ray fault matrix: sealed storage insufficiency fails early ----------------

def test_ray_graph_sealed_storage_exhaustion_fails_early_without_publication(tmp_path, ray_cluster):
    from demiflow.execution.graph import run_ray_graph
    from runtime_v3_graph_history import assert_rejected_before_workers
    business_marker = tmp_path / 'business-started'
    def double(row):
        business_marker.write_text('called')
        return {**row, 'x': row['x'] * 2}

    # Reject the original insufficient declared pool before provisioning an actor;
    # an unrelated planning failure is not proof of the storage boundary.
    spec, readers, n = _graph_spec(n=24, cut_rows=8, mode="finite_sealed", readers=("c0",),
                                   ancestor_fn=double)
    spec["storage_bytes"] = 1
    limits = {"checkpoint_unconfirmed_rows": 64, "checkpoint_interval_ms": 60000}
    start = time.monotonic()
    with pytest.raises(DataflowError, match='^BUDGET_EXCEEDED: graph storage budget cannot admit every declared channel$') as failure:
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-sealed-exhaust"}}):
                run_ray_graph(spec, control_timeout_s=10)
    assert time.monotonic() - start < 40, "sealed exhaustion deadlocked instead of failing early"
    assert failure.value.code == 'BUDGET_EXCEEDED'
    assert_rejected_before_workers(run, 'action-1', business_marker)
