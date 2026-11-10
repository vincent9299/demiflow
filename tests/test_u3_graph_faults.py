"""U3 fault matrix + exit gates 2/3 Local (S4e, design §7 CUT/FLT / §9 S4).

Composes the frozen recovery contract through the real Local graph: a consumer
(target) crash and a stage failure never publish an unauthorized cut, the whole
graph restores from the latest acked cut with an equal output multiset, sealed
storage insufficiency fails early (non-retriable QUOTA_EXCEEDED) rather than
deadlocking, and a multi-input reader aligns a cut across channels including an
EOI channel that contributes an empty barrier. Producer-crash restore is the S4c
group; segment-corruption detection (verify-before-deserialize) is the S2 carriage
group; old-fence rejection and effect-unknown fail-closed are inherited frozen
ResourceManager/K6 contracts (U2/E2), not re-proven here.
"""
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.exchange.carriage_local import LocalSegmentStore, channel_pair, inline_records
from demiflow.execution.exchange.graph_router import GraphInputReader
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.graph import run_local_graph
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.process_unit import ManagedProcessUnit

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


def _spec(target_fns, *, ancestor_fn=None, n=24, cut_rows=8, mode="durable_incremental",
          storage_bytes=None):
    # Plan functions must be nested closures so cloudpickle carries them by value;
    # a module-level fn would pickle by reference and the worker cannot import the
    # test module. ``None`` selects the local passthrough.
    def double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    ancestor_fn = ancestor_fn or double
    items = tuple({"x": i} for i in range(n))
    readers = list(target_fns)
    targets = [{"plan": _stream_map(target_fns[rid] or passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(ancestor_fn),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": mode,
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    if storage_bytes is not None:
        spec["storage_bytes"] = storage_bytes
    return spec, readers, n


def _committed_rows(run, action_id, rid, facts):
    sink = BlockSink(RunAction(run, action_id + "-graph-" + rid, {"kind": "graph-node"}), rid)
    rows = []
    for fact in facts:
        rows.extend(sink.iter_rows(fact["receipts"][rid]["data"]))
    return rows


def _release_crash_orphans(root):
    root = Path(root)
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    for record in pool.snapshot()["active"]:
        allocation_id = record["allocation_id"]
        process_json = next((root / "units").glob("*/" + allocation_id + "/process.json"))
        scope_id = process_json.parent.parent.name
        evidence = unit.stop(scope_id, allocation_id, terminate_deadline_ms=2000)
        assert evidence["confirmed"] is True
        pool.release(allocation_id, evidence)
        try:
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


# ---------------- exit gate 3: consumer crash, whole-graph restore ----------------

def test_consumer_crash_recovers_whole_graph_exactly_once(tmp_path):
    crash_marker = tmp_path / "target-crashed-once"

    def crashing_target(row):
        # Crash exactly one target once, on the first POST-cut row (x==16 doubled).
        # O_EXCL makes the marker atomic so racing broadcast targets crash only one.
        if row["x"] == 16:
            try:
                fd = os.open(crash_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            except FileExistsError:
                return row
            os.kill(os.getpid(), signal.SIGKILL)
        return row

    spec, readers, n = _spec({"c0": crashing_target, "c1": None},
                             n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-consumer-fault"}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized"))
    assert crash_marker.exists(), "the consumer crash window did not fire"

    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-graph-consumer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- exit gate 2: stage failure publishes nothing ----------------

def test_stage_failure_seals_nothing(tmp_path):
    def failing_target(row):
        if row["x"] == 4:  # within the first cut, before any durable ack
            raise RuntimeError("target stage failed")
        return row

    spec, readers, n = _spec({"c0": failing_target, "c1": None},
                             n=24, cut_rows=8)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    # A failed stage leaves the worker host alive but never contributes; the driver's
    # bounded cut collection fails rather than hanging, and publishes nothing.
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-stage-fail"}}) as execution:
                run_local_graph(spec, control_timeout_s=10)
                execution.complete(dict(kind="graph-materialized"))
    # The failing cut never aligned/acked: no authorized commit, no final seal.
    journal_facts = run.journal.execution_commits("action-1-graph-result")
    assert all(f["status"] != "acked" for f in journal_facts)
    assert not any(f["intent"].get("seal", {}).get("sealed") for f in journal_facts)
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- exit gate 2: sealed storage insufficiency fails early ----------------

def test_sealed_storage_exhaustion_fails_early_without_publication(tmp_path):
    # finite_sealed charges a bounded storage reservation; a 1-byte budget cannot
    # hold even one segment, so the producer raises non-retriable QUOTA_EXCEEDED
    # immediately (no credit deadlock). The graph run fails fast and publishes nothing.
    spec, readers, n = _spec({"c0": None}, n=24, cut_rows=8,
                             mode="finite_sealed", storage_bytes=1)
    limits = {"checkpoint_unconfirmed_rows": 64, "checkpoint_interval_ms": 60000}
    start = time.monotonic()
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-sealed-exhaust"}}) as execution:
                run_local_graph(spec, control_timeout_s=8)
                execution.complete(dict(kind="graph-materialized"))
    assert time.monotonic() - start < 30, "sealed exhaustion deadlocked instead of failing early"
    facts = run.journal.execution_commits("action-1-graph-result")
    assert all(f["status"] != "acked" for f in facts)
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- CUT: multi-input fan-in alignment + EOI empty barrier ----------------

def test_cut_multi_input_aligns_and_eoi_channel_empty_barrier(tmp_path):
    from demiflow.execution.exchange.graph_channel import GraphChannelProducer
    # One reader over two input channels (fan-in). Each producer sends pre-cut data
    # then a barrier for the same checkpoint; the reader aligns only when BOTH hold
    # it and pre-cut data is drained. An EOI producer contributes an empty barrier.
    store = LocalSegmentStore(tmp_path, host_id="h")
    cids = ["ch-a", "ch-b"]
    plinks, clinks = {}, {}
    for cid in cids:
        plinks[cid], clinks[cid] = channel_pair()
    producers = {cid: GraphChannelProducer(cid, reader_id="reader", exchange_mode="memory_incremental")
                 for cid in cids}
    specs = [{"channel_id": cid, "reader_id": "reader", "exchange_mode": "memory_incremental",
              "credit_budget": {"rows": 32, "bytes": 1 << 20}, "route_epoch": 0} for cid in cids]
    reader = GraphInputReader(specs, clinks, reader_id="reader", reader_domain=store.domain, store=store)
    reader.start()
    # The reader grants initial credit back to each producer endpoint; consume those
    # frames so the bare producers (no credit thread here) can charge data.
    for cid in cids:
        credit_frame = plinks[cid].recv()
        assert credit_frame["kind"] == "credit"
        producers[cid].grant_credit(credit_frame["credit"])

    def records(cid, ordinal):
        return inline_records([{"v": ordinal}], source_id="src", occurrence_id="shared",
                              start_ordinal=ordinal)

    # Pre-cut data on both channels, then a barrier naming the same checkpoint.
    for cid in cids:
        for ordinal in range(3):
            envelope = producers[cid].data_envelope(records(cid, ordinal),
                                                    producer={"logical_node_id": "n", "occurrence_id": "n-p0"})
            plinks[cid].send({"kind": "data", "sequence": envelope["sequence"], "envelope": envelope})
    # Both producers finish (EOI) before the cut; each still participates with an
    # empty barrier (frozen InputGate: finished channels stay participants until the
    # same cut), and the aligned cut is final because every input has EOI'd.
    for cid in cids:
        plinks[cid].send({"kind": "eoi", **producers[cid].end_of_input()})
    for cid in cids:
        barrier = producers[cid].barrier("cut-1", epoch=1, topology_revision=0,
                                         producer_sequence_bound=None)
        plinks[cid].send({"kind": "barrier", **barrier})

    # Drain pre-cut data; the reader aligns the multi-input cut and waits on the hook.
    done = threading.Event()
    release = threading.Event()
    aligned = {}

    def hook(checkpoint, consumed, final):
        aligned["checkpoint"] = checkpoint
        aligned["consumed"] = consumed
        aligned["final"] = final
        done.set()
        return release  # held until the test asserts alignment, then released

    reader.set_cut_hook(hook)
    consumed = []
    pump = threading.Thread(target=lambda: consumed.extend(reader.rows()), daemon=True)
    pump.start()
    assert done.wait(timeout=5), "multi-input cut did not align"
    assert aligned["checkpoint"] == "cut-1"
    assert aligned["consumed"] == 6  # 3 pre-cut rows from each of the two channels
    assert aligned["final"] is True  # ch-b EOI + ch-a barrier => all inputs finished
    assert sorted(row["v"] for row in consumed) == [0, 0, 1, 1, 2, 2]
    # The barrier never overtook pre-cut data: every pre-cut row dequeued first.
    release.set()
    pump.join(timeout=5)
    reader.close()
    for link in plinks.values():
        link.close()