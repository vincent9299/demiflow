"""U2 Ray Core full-chain + SEM two-provider equivalence (S5, D1/D3/D5).

Single-host Ray Core (env-ray, ray 2.60.0 Core only, no ray[data]) is semantic-
adaptation evidence only (design §8): no multi-host/GPU/performance claim. The
operator chain runs in a named Ray actor hosting the same sealed DataflowWorkerExecutor
as the Local process worker; the
driver owns the control plane, the fixed source capture and the single commit
authority. Control is actor methods (driver->worker) plus a bounded outbox pull
(worker->driver); result blocks hand off as staging descriptors on the shared
same-host run directory (D3, no new physical data transport).

SEM proves the same plan/source yields an identical output multiset, record
identity, commit sequence/position progression and K6 manifest shape across Local
and Ray. The Ray fault matrix covers actor death (new allocation recovers the acked
prefix exactly once), single-host node-loss simulation (actor unreachable ->
inspect LOST, confirmed cleanup) and scheduling with no capacity (PENDING, never a
false RUNNING). ray.kill having been called is not itself cleanup confirmation.
"""
import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")
from ray.exceptions import RayActorError

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, FilterOp, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
    ResourceScope, WorkerLaunchSpec, WorkerRequest,
)
from demiflow.resources.manager import ResourceManager
from demiflow.resources.ray import RayResourceProvider
from demiflow.execution.hosts.ray_worker_host import RayControlConnector

pytestmark = [
    pytest.mark.ray,
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="Local managed units require Linux"),
]


@pytest.fixture(scope="module")
def ray_cluster():
    import tempfile
    assert not ray.is_initialized(), 'test must own a new private Ray cluster'
    with tempfile.TemporaryDirectory(prefix='demiflow-ray-u2-', dir='/tmp') as root:
        try:
            ray.init(address='local', num_cpus=2, include_dashboard=False, _temp_dir=root)
            yield
        finally:
            ray.shutdown()


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _plan(double, items_count=10):
    # Local closures: cloudpickle serializes them by value so neither the Local
    # worker process nor the Ray actor need import this test module.
    def keep_even_doubled(row):
        return row["x"] % 4 == 0

    items = tuple({"x": i} for i in range(items_count))
    plan = LogicalPlan((
        MapOp(CallableSpec.create(double), stream_options=MapStreamOptions(concurrency=1)),
        FilterOp(CallableSpec.create(keep_even_doubled)),
    ))
    expected = [{"x": i * 2} for i in range(items_count) if (i * 2) % 4 == 0]
    return items, plan, expected


def _chain_child_id(run):
    from runtime_v3_owned_crash import materialize_action_id
    return materialize_action_id(run)


def _commit_shape(run):
    facts = run.journal.execution_commits(_chain_child_id(run))
    return {
        "sequences": [f["sequence"] for f in facts],
        "positions": [f["intent"]["position"]["next_ordinal"] for f in facts],
        "statuses": [f["status"] for f in facts],
        "commit_ids": [f["commit_id"] for f in facts],
    }


def _k6_shapes(run, execution, expected):
    """Verify native cuts/retained ACK proofs and every ordered published prefix."""
    import hashlib
    from demiflow.execution.outputs.replay import PublishedRecordReader
    from demiflow.execution.contracts.record_codec import document_limit
    from demiflow.execution.contracts.record_codec import _decode_record_document
    shapes, inherited, prior_ids, growth = [], {}, [], []
    for fact in run.journal.execution_commits(_chain_child_id(run)):
        assert fact['status'] == 'acked'
        ref = fact['prepared']['state']['dataflow-cut-v3']
        raw = (run.directory / ref['path']).read_bytes()
        assert len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha256']
        value = json.loads(raw)
        assert value['schema'] == 'demiflow.dataflow_cut.v3'
        cut, contributions = value['cut'], value['contributions']
        assert cut['sequence'] == fact['sequence']
        by_owner = {c['participant_id']: c for c in contributions}
        assert len(by_owner) == len(contributions)
        assert set(cut['participants']) <= by_owner.keys()
        for key, contribution in by_owner.items():
            if key in cut['participants']:
                assert contribution['cut_id'] == cut['cut_id']
            else:
                assert contribution == inherited[key], 'retained state lacks an earlier original ACK'
        inherited = by_owner
        publication = fact['prepared']['sinks']['materialize']
        reader = PublishedRecordReader(publication, store=execution._store, bounds=execution._bounds)
        rows, ids = [], []
        try:
            while True:
                try:
                    document, record_id, sequence, _ = reader.pop()
                except StopIteration:
                    break
                record = _decode_record_document(document, record_id=record_id, sequence=sequence,
                    max_bytes=document_limit(execution._bounds),
                    payload_max_bytes=execution._bounds.max_row_bytes,
                    metadata_max_bytes=execution._bounds.max_control_bytes)
                rows.append(dict(record.inline)); ids.append(record_id)
                assert len(rows) <= len(expected)
        finally:
            reader.close()
        assert len(rows) == publication['rows'] == publication['manifest']['total_rows']
        assert rows == expected[:len(rows)]
        assert ids[:len(prior_ids)] == prior_ids
        assert len(rows) >= len(prior_ids)
        if len(rows) > len(prior_ids):
            growth.append(len(rows))
        prior_ids = ids
        sources = [(c['next_sequence'], c['end_of_input']) for c in value['dataflow.sources.v3']]
        if cut['final']:
            assert rows == expected and all(eoi for _, eoi in sources)
            assert publication['manifest']['final']
        shapes.append({'sequence': fact['sequence'], 'participants': len(cut['participants']),
            'retained_contributions': len(contributions), 'source_progress': sorted(sources),
            'channel_cuts': sum(len(c['channels']) for c in contributions),
            'operator_states': sum(len(c['operator_states']) for c in contributions),
            'seal_keys': sorted(cut), 'sealed': cut['final'], 'published_rows': len(rows)})
    assert growth == sorted(set(growth)) and growth[-1] == len(expected)
    assert sum(v['sealed'] for v in shapes) == 1 and shapes[-1]['sealed']
    (run.directory/'provider-cut-evidence.json').write_text(json.dumps(shapes, indent=2))
    return shapes


# ---------------- SEM: same plan/source, two providers, identical semantics ----------------

def test_sem_local_ray_equivalence(tmp_path, ray_cluster, monkeypatch):
    from demiflow.execution.executors import dataflow_api
    original = dataflow_api.execute_action
    executions = []
    def observe(request, **kwargs):
        handle = original(request, **kwargs)
        executions.append(handle._execution)
        return handle
    monkeypatch.setattr(dataflow_api, "execute_action", observe)
    def double(row):
        return {**row, "x": row["x"] * 2}

    items, plan, expected = _plan(double)
    limits = {"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}

    local_dir = tmp_path / "local-run"
    local_executor = LocalDatasetExecutor(workers=1)
    local_dataset = Dataset(ItemsSource(items), plan, local_executor)
    with RunContext.start(local_dir, limits=limits) as local_run:
        local_rows = local_dataset.materialize().take_all()
        local_commits = _commit_shape(local_run)
        with local_run.journal._connection() as db:
            local_owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert local_owner == (local_run.attempt, local_run.fence)
        # K6 manifests are per-cut checkpoint records, GC'd on run exit (durable
        # recovery uses the journal commit's prepared.state); capture while open.
        local_k6 = _k6_shapes(local_run, next(e for e in executions if e._action.context is local_run), expected)

    ray_dir = tmp_path / "ray-run"
    ray_executor = RayDatasetExecutor(workers=1)
    ray_dataset = Dataset(ItemsSource(items), plan, ray_executor)
    with RunContext.start(ray_dir, limits=limits) as ray_run:
        ray_rows = ray_dataset.materialize().take_all()
        ray_commits = _commit_shape(ray_run)
        with ray_run.journal._connection() as db:
            ray_owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert ray_owner == (ray_run.attempt, ray_run.fence)
        ray_k6 = _k6_shapes(ray_run, next(e for e in executions if e._action.context is ray_run), expected)

    # Output multiset + record identity (x value) identical across providers.
    assert local_rows == expected
    assert ray_rows == expected
    assert sorted(local_rows, key=lambda r: r["x"]) == sorted(ray_rows, key=lambda r: r["x"])
    # Physical phase/park cuts may differ between providers. Every actual ACK
    # remains contiguous and immutable; each published business prefix above is
    # checked against the same exact ordered records, including retained IDs.
    for commits in (local_commits, ray_commits):
        assert commits['sequences'] == list(range(1, len(commits['sequences']) + 1))
        assert commits['positions'] == commits['sequences']
        assert commits['statuses'] == ['acked'] * len(commits['sequences'])
        assert len(set(commits['commit_ids'])) == len(commits['commit_ids'])
    assert local_k6[-1]['source_progress'] == ray_k6[-1]['source_progress'] == [(10, True)]
    assert local_k6[-1]['published_rows'] == ray_k6[-1]['published_rows'] == len(expected)
    assert local_k6[-1]['seal_keys'] == ray_k6[-1]['seal_keys']
    assert 'ray.data' not in sys.modules


# ---------------- Ray fault: actor death, new allocation recovers exactly once ----------------

def test_ray_actor_death_recovers_acked_prefix_exactly_once(tmp_path, ray_cluster):
    from collections import Counter
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import kill_owned_worker
    driver_pid = os.getpid()
    crash_marker = tmp_path / "crashed-once"
    trace = tmp_path / 'business-calls'

    def double(row):
        with trace.open('a') as stream:
            stream.write(str(row['x']) + '\n')
        if row["x"] == 4 and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        return {**row, "x": row["x"] * 2}

    items, plan, expected = _plan(double)
    executor = RayDatasetExecutor(workers=1)
    dataset = Dataset(ItemsSource(items), plan, executor)
    with pytest.raises(DataflowError, match='UNKNOWN_EFFECT') as failure:
        with RunContext.start(tmp_path / "run",
                              limits={"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}) as run:
            reference = run.ref
            dataset.materialize().take_all()
    assert failure.value.code == 'UNKNOWN_EFFECT'
    assert isinstance(failure.value.__cause__, RayActorError)
    death = json.loads(crash_marker.read_text())
    assert death['pid'] != driver_pid and death['signal'] == 'SIGKILL'
    with run.journal._connection() as db:
        allocations = [json.loads(row[0]) for row in db.execute('SELECT document FROM resource_allocations')]
    assert allocations and all(a['cleanup'] == 'CONFIRMED' and a['observation'] == 'FRESH'
        and a['evidence_ref'].startswith('ray-actor-dead:') for a in allocations)
    first_calls = [int(x) for x in trace.read_text().splitlines()]
    with RunContext.resume(reference) as run2:
        before = {f['commit_id']:f for f in run2.journal.execution_commits(_chain_child_id(run2))
                  if f['status'] == 'acked'}
        assert before, 'no original ACK prefix survived the actor death'
        rows = dataset.materialize().take_all()
        assert rows == expected
        facts = run2.journal.execution_commits(_chain_child_id(run2))
        sequences = [f["sequence"] for f in facts]
        assert sequences == list(range(1, len(facts) + 1))
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        after = {f['commit_id']:f for f in facts}
        assert before.keys() < after.keys()
        assert all(after[key]['receipts'] == fact['receipts'] for key,fact in before.items())
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)
    all_calls = [int(x) for x in trace.read_text().splitlines()]
    counts = Counter(all_calls)
    assert first_calls == list(range(5))
    assert [counts[i] for i in range(4)] == [1]*4
    assert counts[4] == 2 and all(counts[i] == 1 for i in range(5,10))
    assert 'ray.data' not in sys.modules


# ---------------- Ray fault: single-host node-loss simulation + confirmed cleanup ----------------

def _launch_spec(run, scope_id, allocation_id, boot_id):
    return {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": ["cpu-map"], "grant": None,
    }


def _immutable_spec(provider, scope_id, allocation_id, cpu_millis=1000):
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": "0" * 64, "location": "file:///tmp/plan.pkl"}},
        entrypoint="demiflow-worker-actor", argv=("ray-actor",),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(
        "worker-" + allocation_id, allocation_id,
        ResourceProfile(requests={"cpu_millis": cpu_millis, "memory_bytes": 1024 ** 3}),
        launch, provision_deadline_ms=60000).to_dict()
    normalized = provider.describe_and_normalize(None, None, request)
    return {"scope_id": scope_id, "allocation_id": allocation_id, "request": request,
            "spec_hash": normalized["spec_hash"], "effective": normalized["effective"],
            "requirements": None, "binding": {}}


def test_ray_node_loss_simulation_inspect_lost_and_confirmed_cleanup(tmp_path, ray_cluster):
    connector = RayControlConnector()
    provider = RayResourceProvider(connector, provision_timeout_s=15)
    scope_id, allocation_id, boot_id = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
    with RunContext.start(tmp_path / "run") as run:
        connector.stage(allocation_id, _launch_spec(run, scope_id, allocation_id, boot_id))
        immutable = _immutable_spec(provider, scope_id, allocation_id)
        observed = provider.ensure_present(allocation_id, immutable)
        assert observed["phase"] == "RUNNING"
        assert observed["boot_id"] == boot_id
        actor = provider._actors[allocation_id][0]
        # Single-host node-loss simulation: the actor becomes unreachable.
        ray.kill(actor, no_restart=True)
        # ray.kill is asynchronous: a task submitted in the kill window can still
        # succeed, so poll until Ray reports the actor unreachable. This makes the
        # LOST inventory observation below deterministic.
        deadline = time.monotonic() + 15
        while True:
            try:
                ray.get(actor.identity.remote(), timeout=2)
            except RayActorError:
                break
            if time.monotonic() > deadline:
                pytest.fail("actor did not become unreachable after ray.kill")
            time.sleep(0.05)
        inventory = provider.inspect(scope_id, None)
        assert inventory["allocations"][0]["phase"] == "LOST"
        # ray.kill called is not confirmation; ensure_absent observes death evidence.
        result = provider.ensure_absent(
            {"scope_id": scope_id, "allocation_id": allocation_id}, 5000)
        assert result["cleanup"] == "CONFIRMED"
        assert result["evidence_ref"].startswith("ray-actor-dead:")
        with pytest.raises(RayActorError):
            ray.get(actor.identity.remote(), timeout=5)


# ---------------- Ray fault: scheduling with no capacity stays PENDING ----------------

def test_ray_scheduling_no_capacity_pending_not_running(tmp_path, ray_cluster):
    connector = RayControlConnector()
    provider = RayResourceProvider(connector, provision_timeout_s=3)
    scope_id, allocation_id, boot_id = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
    # num_cpus = 100 exceeds the 2-CPU single-host cluster: Ray Core cannot place
    # the actor, so it never initializes and must not be reported RUNNING.
    connector.stage(allocation_id, {"boot_id": boot_id})
    immutable = _immutable_spec(provider, scope_id, allocation_id, cpu_millis=100000)
    observed = provider.ensure_present(allocation_id, immutable)
    assert observed["phase"] == "PENDING"
    assert "capacity_pending" in observed["conditions"]
    # The pending actor was killed by the provider; cleanup is confirmed, no leak.
    result = provider.ensure_absent(
        {"scope_id": scope_id, "allocation_id": allocation_id}, 5000)
    assert result["cleanup"] == "CONFIRMED"
