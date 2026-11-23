"""F5 node-identity independent counterexamples (approved Option A).

The defect: compile_logical_dag derived node_id from the operator content hash
alone, so two occurrences of the same operator (map(f).map(f)) collapsed to one
logical_node_id, the chain zip produced a self-loop, and the physical plan keyed
partitions_by_node on the collided id. The fix distinguishes operator SEMANTIC
identity (content hash, shared) from graph LOGICAL NODE identity (content +
chain position, unique per occurrence). These six categories bind the fix and
its boundaries; they fail against the pre-fix dag.py and pass against Option A.

compile_declared_dag is intentionally unchanged: a shared bridge legitimately
reuses one logical_node_id across reuse/fan-out occurrences, so the validator
rejects duplicate occurrence_id / self-loop / cycle / dangling edge, but never
blanket-rejects a repeated logical_node_id. K1-K6 contracts are byte-frozen;
the wire-stability category proves the recomputed identities flow through them.
"""
import copy
import json
import os
import sys

import pytest

import demiflow.data as data
from demiflow.data import plan as ops
from demiflow.data.plan import CallableSpec, LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts import exchange, operator
from demiflow.execution.plan_facts import operation_facts
from demiflow.execution.recovery_contract import identifier, identity
from demiflow.execution.run_context import RunContext
from demiflow.planning.dag import (
    compile_declared_dag, compile_logical_dag, compile_physical_execution_plan,
    dag_id_for, validate_logical_dag,
)


def _passthrough(row):
    return row


def _map_op():
    return ops.MapOp(callable=CallableSpec.create(_passthrough))


def _repeated_map_dag(action_id="action-f5"):
    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    plan = LogicalPlan((_map_op(), _map_op()))
    return compile_logical_dag(action_id=action_id, source=source,
                               operations=plan.operations,
                               terminal_kind="materialize", sink_ids=("output",))


def _physical(dag, **overrides):
    values = dict(
        logical_dag=dag, plan_revision=0,
        artifact_manifest_ref={"manifest_id": "m" * 64, "sha256": "a" * 64},
        worker_groups=[],
        recovery={"checkpoint_interval_ms": 1000, "unconfirmed_rows": 100,
                  "unconfirmed_bytes": 1024, "terminals": []},
        feasibility_snapshot_ref="snap-1", read_domain="local",
    )
    values.update(overrides)
    return compile_physical_execution_plan(**values)


# --- (1) repeated map: distinct positional nodes, no self-loop, faithful plan --

def test_repeated_map_yields_distinct_positional_nodes():
    dag = _repeated_map_dag()
    op_nodes = dag["nodes"][1:-1]
    assert len(op_nodes) == 2
    ids = [node["logical_node_id"] for node in op_nodes]
    assert ids[0] != ids[1], "repeated map collapsed to one logical node (F5 defect)"
    assert len({node["occurrence_id"] for node in op_nodes}) == 2
    # Both occurrences share ONE operator semantic content hash; only the chain
    # index distinguishes them. This is the semantic-vs-node identity split.
    semantic = {"operation": operation_facts(_map_op())}
    assert ids[0] == identity([semantic, 0])
    assert ids[1] == identity([semantic, 1])
    # No self-loop, and the chain is faithfully source -> map0 -> map1 -> terminal.
    for edge in dag["edges"]:
        assert edge["from"]["logical_node_id"] != edge["to"]["logical_node_id"]
    chain = [dag["nodes"][0]["logical_node_id"], ids[0], ids[1], dag["nodes"][-1]["logical_node_id"]]
    assert [(e["from"]["logical_node_id"], e["to"]["logical_node_id"])
            for e in dag["edges"]] == list(zip(chain, chain[1:]))
    validate_logical_dag(dag)
    # Physical plan keeps a distinct partition family per occurrence (no key collision).
    physical = _physical(dag, partition_count=2)
    assert len({stage["logical_node_id"] for stage in physical["stages"]}) == 4
    by_node = {stage["logical_node_id"]: stage for stage in physical["stages"]}
    assert by_node[ids[0]]["partitions"] != by_node[ids[1]]["partitions"]


# --- (2) declared shared ancestor: legitimate reuse preserved (Option A) -------

def test_declared_shared_ancestor_still_shared_and_stable():
    nodes = [{"id": "src", "kind": "from_items", "role": "source"},
             {"id": "shared", "kind": "materialize", "role": "bridge"},
             {"id": "left", "kind": "map", "role": "transform"},
             {"id": "right", "kind": "map", "role": "transform"},
             {"id": "act", "kind": "count", "role": "action"}]
    edges = [{"from": "src", "to": "shared", "port": 0},
             {"from": "shared", "to": "left", "port": 0},
             {"from": "shared", "to": "right", "port": 0},
             {"from": "left", "to": "act", "port": 0},
             {"from": "right", "to": "act", "port": 1}]
    descriptors = {node["id"]: {"kind": node["kind"], "declaration": node["id"]}
                   for node in nodes}
    dag = compile_declared_dag(action_id="act", nodes=nodes, edges=edges,
                               descriptors=descriptors)
    assert dag["shared_ancestors"]
    # The bridge (kind=materialize) is reused once per consumer path: one shared
    # logical_node_id across two occurrences, each with a distinct occurrence_id.
    bridge = [n for n in dag["nodes"] if n["op_kind"] == "materialize"]
    assert len(bridge) == 2
    assert len({n["logical_node_id"] for n in bridge}) == 1
    assert len({n["occurrence_id"] for n in bridge}) == 2
    shared_id = bridge[0]["logical_node_id"]
    assert shared_id in {s["logical_node_id"] for s in dag["shared_ancestors"]}
    # The strengthened validator must accept this legitimate logical_node_id reuse.
    assert len({n["occurrence_id"] for n in dag["nodes"]}) == len(dag["nodes"])
    validate_logical_dag(dag)
    again = compile_declared_dag(action_id="act", nodes=nodes, edges=edges,
                                 descriptors=descriptors)
    assert dag["dag_id"] == again["dag_id"]


# --- (3) validator: reject duplicate occurrence / self-loop / cycle / dangling -

def test_validator_rejects_duplicate_occurrence_self_loop_cycle_and_dangling():
    dag = _repeated_map_dag()
    validate_logical_dag(dag)                       # legitimate input passes

    bad = copy.deepcopy(dag)                        # duplicate occurrence_id
    bad["nodes"][2]["occurrence_id"] = bad["nodes"][1]["occurrence_id"]
    bad["dag_id"] = dag_id_for(bad)
    with pytest.raises(ValueError, match="duplicate occurrence"):
        validate_logical_dag(bad)

    bad = copy.deepcopy(dag)                        # self-loop edge
    node_id = bad["nodes"][1]["logical_node_id"]
    bad["edges"].append({"edge_id": identity(["self", node_id]),
                         "from": {"logical_node_id": node_id, "port": "out"},
                         "to": {"logical_node_id": node_id, "port": "in"}})
    bad["dag_id"] = dag_id_for(bad)
    with pytest.raises(ValueError, match="self-loop"):
        validate_logical_dag(bad)

    bad = copy.deepcopy(dag)                        # cycle: terminal -> first op
    producer = bad["nodes"][-1]["logical_node_id"]
    consumer = bad["nodes"][1]["logical_node_id"]
    bad["edges"].append({"edge_id": identity(["back", producer, consumer]),
                         "from": {"logical_node_id": producer, "port": "out"},
                         "to": {"logical_node_id": consumer, "port": "in"}})
    bad["dag_id"] = dag_id_for(bad)
    with pytest.raises(ValueError, match="cycle"):
        validate_logical_dag(bad)

    bad = copy.deepcopy(dag)                        # edge references unknown node
    bad["edges"][0]["to"]["logical_node_id"] = identity(["ghost"])
    bad["dag_id"] = dag_id_for(bad)
    with pytest.raises(ValueError, match="unknown logical node"):
        validate_logical_dag(bad)


# --- (4) chain sealing: repeated stream operators seal distinct identities -----

def test_chain_sealing_input_distinguishes_repeated_async_map():
    # Mirrors compile_chain_topology's logical compilation (terminal=materialize,
    # sink=output); operators are nodes[1:-1]. Two identical async_map occurrences
    # must seal as two distinct operator identities, not one collided node.
    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    async_op = ops.AsyncMapOp(callable=CallableSpec.create(_passthrough), concurrency=1)
    plan = LogicalPlan((async_op, async_op))
    dag = compile_logical_dag(action_id="chain-f5", source=source,
                              operations=plan.operations,
                              terminal_kind="materialize", sink_ids=("output",))
    operators = [{"logical_node_id": n["logical_node_id"], "occurrence_id": n["occurrence_id"]}
                 for n in dag["nodes"][1:-1]]
    assert len(operators) == 2
    assert operators[0]["logical_node_id"] != operators[1]["logical_node_id"]
    assert operators[0]["occurrence_id"] != operators[1]["occurrence_id"]
    validate_logical_dag(dag)


@pytest.mark.skipif(not sys.platform.startswith("linux"),
                    reason="Local managed units require Linux")
@pytest.mark.parametrize("mode", ["process", "thread"])
def test_unified_chain_repeated_map_async_runs_end_to_end(tmp_path, mode):
    from _worker_event_fixtures import emit, events
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))
    evidence = tmp_path / 'operator-calls.jsonl'
    def inc(row):
        emit(evidence, 'call', x=row['x'])
        return {**row, "x": row["x"] + 1}

    limits = {"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        with data.local_execution(workers=1, worker_mode=mode, partitions=1):
            handle = (data.from_items([{"x": i} for i in range(8)])
                      .map_async(inc, concurrency=1)
                      .map_async(inc, concurrency=1)
                      .materialize())
            sealed = load_sealed_plan(run.directory, 'action-1', version='v3')
            graph = sealed['graph']
            nodes = [node for node in graph.nodes if node.kernel.kind == 'AsyncMapOp']
            assert len(nodes) == 2
            assert nodes[0].semantic_id == nodes[1].semantic_id
            assert nodes[0].occurrence_id != nodes[1].occurrence_id
            assert all(edge.producer != edge.consumer for edge in graph.edges)
            assert any(edge.producer == nodes[0].occurrence_id and
                       edge.consumer == nodes[1].occurrence_id for edge in graph.edges)
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            rows = handle.take_all()
            handle.release()
    assert sorted(rows, key=lambda r: r["x"]) == [{"x": i + 2} for i in range(8)]
    calls = events(evidence)
    assert len(calls) == 16
    assert sorted(event['x'] for event in calls) == sorted([*range(8), *range(1, 9)])
    pids = {event['pid'] for event in calls}
    assert pids
    if mode == 'process':
        assert os.getpid() not in pids
    else:
        assert pids == {os.getpid()}


# --- (5) dag_id stability and worker-count independence ------------------------

def test_dag_id_stable_and_worker_count_independent():
    assert _repeated_map_dag()["dag_id"] == _repeated_map_dag()["dag_id"]
    assert _repeated_map_dag(action_id="other")["dag_id"] != _repeated_map_dag()["dag_id"]
    dag = _repeated_map_dag()
    small = _physical(dag, partition_count=1)
    large = _physical(dag, partition_count=4, worker_groups=[
        {"group_id": "g1", "role": "cpu_prepare", "profile_ref": "prof",
         "stage_refs": [stage["stage_id"] for stage in small["stages"]],
         "min_workers": 1, "initial_workers": 4, "max_workers": 8}])
    assert small["plan_id"] != large["plan_id"]     # physical plans differ
    assert [s["logical_node_id"] for s in small["stages"]] == [
        s["logical_node_id"] for s in large["stages"]]
    assert [s["occurrence_id"] for s in small["stages"]] == [
        s["occurrence_id"] for s in large["stages"]]


# --- (6) wire stability: recomputed identities flow through frozen K2/K4 -------

def test_recomputed_identities_pass_frozen_operator_and_exchange_wire():
    dag = _repeated_map_dag()
    op_nodes = dag["nodes"][1:-1]
    physical = _physical(dag)
    ref = {"manifest_id": "m" * 64, "sha256": "a" * 64}
    source_id = dag["sources"][0]["source_id"]
    for index, node in enumerate(op_nodes):
        identifier(node["logical_node_id"])         # format-valid sha256 identifier
        identifier(node["occurrence_id"])
        # K2 OperatorSpec accepts the positional identity (shape unchanged).
        spec = operator.compile_operator_spec(
            CallableSpec.create(_passthrough), logical_node_id=node["logical_node_id"],
            occurrence_id=node["occurrence_id"], artifact_manifest_ref=ref,
            module_key="p_" + "b" * 24)
        operator.validate_operator_spec(spec)
        assert spec["logical_node_id"] == node["logical_node_id"]
        # K4 DataEnvelope producer carries exactly {logical_node_id, occurrence_id}.
        channel_id = physical["channels"][index + 1]["channel_id"]
        stage = next(s for s in physical["stages"]
                     if s["logical_node_id"] == node["logical_node_id"])
        envelope = {
            "schema_version": "demiflow.data_envelope.v1",
            "channel_id": channel_id, "sequence": 0,
            "partition_id": stage["partitions"][0], "route_epoch": 0,
            "producer": {"logical_node_id": node["logical_node_id"],
                         "occurrence_id": node["occurrence_id"]},
            "records": [{"record_id": exchange.record_identity(source_id, index),
                         "parent_record_ids": [], "occurrence_id": node["occurrence_id"],
                         "size_bytes": 0, "payload": {"x": 1}}],
            "bounds": {"rows": 1, "bytes": 0},
        }
        assert set(envelope["producer"]) == {"logical_node_id", "occurrence_id"}
        exchange.validate_data_envelope(envelope)