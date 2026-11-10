"""U3 RTG conformance: physical 1:1 channel derivation and local routing (S1).

Pure planning model over the frozen logical channel plan (dag.py untouched).
Physical channel ids are deterministic (D1); every physical channel is one
producer partition -> one consumer partition (D2); distribution is stateless
(ordinal-derived) so replay routes identically; business target classes are
declared and never changed by balancing; broadcast multiplicity is explicit.
"""
import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.recovery_contract import identity
from demiflow.planning.dag import (
    build_channel_plan, compile_declared_dag, compile_physical_execution_plan,
)
from demiflow.planning.graph_channels import (
    RouteTable, materialize_channels, physical_channel_id, physical_pairs,
    validate_physical_channel,
)


def _plan(routing, producers, consumers, mode="memory_incremental"):
    return build_channel_plan(
        edge_id="edge-1", scheme_version=1,
        producer_partitions=producers, consumer_partitions=consumers,
        exchange_mode=mode, routing=routing, read_domain="file://h:/r",
        durability_declared="durable" if mode == "durable_incremental" else "ephemeral",
        seal_scope=({"scope_id": "scope-1", "membership": producers, "generation": 1}
                    if mode == "finite_sealed" else None))


def _code(exc):
    return exc.value.code


def test_rtg_physical_channel_id_deterministic_and_binding():
    first = physical_channel_id("logical-1", "p0", "c0")
    assert first == physical_channel_id("logical-1", "p0", "c0")
    assert first == identity(["logical-1", "p0", "c0"])
    assert first != physical_channel_id("logical-1", "p0", "c1")
    assert first != physical_channel_id("logical-1", "p1", "c0")
    assert first != physical_channel_id("logical-2", "p0", "c0")


def test_rtg_forward_pairs_positional():
    channel = _plan("forward", ["p0", "p1"], ["c0", "c1"])
    assert physical_pairs(channel) == [("p0", "c0"), ("p1", "c1")]
    table = RouteTable(channel)
    assert table.targets("p0", 0) == [physical_channel_id(channel["channel_id"], "p0", "c0")]
    assert table.targets("p1", 7) == [physical_channel_id(channel["channel_id"], "p1", "c1")]
    assert table.multiplicity == 1
    with pytest.raises(ValueError):
        physical_pairs(_plan("forward", ["p0", "p1"], ["c0"]))


def test_rtg_fan_in_gives_each_producer_its_own_channel():
    channel = _plan("fan_in", ["p0", "p1", "p2"], ["c0"])
    table = RouteTable(channel)
    assert len(table.channel_ids) == 3 and len(set(table.channel_ids)) == 3
    for producer in ("p0", "p1", "p2"):
        assert table.targets(producer, 0) == [physical_channel_id(channel["channel_id"], producer, "c0")]
    with pytest.raises(ValueError):
        physical_pairs(_plan("fan_in", ["p0"], ["c0", "c1"]))


def test_rtg_fan_out_multiplicity_one_and_ordinal_distribution():
    channel = _plan("fan_out", ["p0"], ["c0", "c1", "c2"])
    table = RouteTable(channel)
    assert len(table.channel_ids) == 3 and table.multiplicity == 1
    chosen = [table.targets("p0", ordinal)[0] for ordinal in range(4)]
    assert all(len(table.targets("p0", ordinal)) == 1 for ordinal in range(4))
    assert chosen[0] == physical_channel_id(channel["channel_id"], "p0", "c0")
    assert chosen[1] == physical_channel_id(channel["channel_id"], "p0", "c1")
    assert chosen[2] == physical_channel_id(channel["channel_id"], "p0", "c2")
    assert chosen[3] == chosen[0]
    with pytest.raises(ValueError):
        physical_pairs(_plan("fan_out", ["p0", "p1"], ["c0"]))


def test_rtg_broadcast_replicates_to_every_consumer():
    channel = _plan("broadcast", ["p0", "p1"], ["c0", "c1"])
    table = RouteTable(channel)
    assert len(table.channel_ids) == 4 and table.multiplicity == 2
    targets = table.targets("p0", 3)
    assert targets == [physical_channel_id(channel["channel_id"], "p0", "c0"),
                       physical_channel_id(channel["channel_id"], "p0", "c1")]


def _consumer_of(table, channel_id):
    return next(item["consumer_partition"] for item in table.physical_channels
                if item["channel_id"] == channel_id)


def test_rtg_hash_requires_declared_keys_and_is_stable():
    channel = _plan("hash", ["p0", "p1"], ["c0", "c1", "c2"])
    with pytest.raises(ValueError):
        RouteTable(channel)
    table = RouteTable(channel, key_fields=["k"])
    # Same key lands on the same consumer regardless of producer partition/ordinal
    # (the physical channel id still differs: it binds the producer endpoint too).
    assert _consumer_of(table, table.targets("p0", 0, route_value=("x",))[0]) == \
        _consumer_of(table, table.targets("p1", 9, route_value=("x",))[0])
    assert table.targets("p0", 0, route_value=("x",)) == table.targets("p0", 0, route_value=("x",))
    assert table.targets("p0", 0, route_value=("x",))[0] in table.channel_ids
    with pytest.raises(ProtocolError) as exc:
        table.targets("p0", 0, route_value=("x", "y"))
    assert _code(exc) == "INVALID_SPEC"
    with pytest.raises(ProtocolError):
        table.targets("p0", 0)


def test_rtg_target_class_is_never_changed_by_balancing():
    channel = _plan("target", ["p0"], ["c0", "c1", "c2"])
    classes = {"model-a": ["c0", "c1"], "model-b": ["c2"]}
    table = RouteTable(channel, target_classes=classes)
    allowed = {physical_channel_id(channel["channel_id"], "p0", replica)
               for replica in classes["model-a"]}
    for ordinal in range(6):
        assert table.targets("p0", ordinal, route_value="model-a")[0] in allowed
    assert table.targets("p0", 0, route_value="model-b") == [
        physical_channel_id(channel["channel_id"], "p0", "c2")]
    with pytest.raises(ProtocolError) as exc:
        table.targets("p0", 0, route_value="model-unknown")
    assert _code(exc) == "INVALID_SPEC"
    with pytest.raises(ValueError):  # a consumer in no class would silently black-hole records
        RouteTable(channel, target_classes={"model-a": ["c0", "c1"]})
    with pytest.raises(ValueError):  # a partition serves exactly one class
        RouteTable(channel, target_classes={"model-a": ["c0", "c1"], "model-b": ["c1", "c2"]})


def test_rtg_rebalance_is_stateless_and_replay_identical():
    channel = _plan("rebalance", ["p0", "p1"], ["c0", "c1"])
    table = RouteTable(channel)
    assert len(table.channel_ids) == 4
    restored = RouteTable(channel)  # a fresh table after restore holds no hidden counters
    for ordinal in range(8):
        assert table.targets("p0", ordinal) == restored.targets("p0", ordinal)
    assert table.targets("p0", 0) != table.targets("p0", 1)


def _diamond_dag(shared_role="transform"):
    nodes = [{"id": "src", "kind": "from_items", "role": "source"},
             {"id": "shared", "kind": "materialize", "role": shared_role},
             {"id": "left", "kind": "map", "role": "transform"},
             {"id": "right", "kind": "map", "role": "transform"},
             {"id": "act", "kind": "count", "role": "action"}]
    edges = [{"from": "src", "to": "shared", "port": 0},
             {"from": "shared", "to": "left", "port": 0},
             {"from": "shared", "to": "right", "port": 0},
             {"from": "left", "to": "act", "port": 0},
             {"from": "right", "to": "act", "port": 1}]
    descriptors = {node["id"]: {"kind": node["kind"], "declaration": node["id"]} for node in nodes}
    return compile_declared_dag(action_id="act", nodes=nodes, edges=edges, descriptors=descriptors)


def _physical_plan(dag, partition_count=2):
    stage_refs = [identity([dag["dag_id"], node["occurrence_id"]]) for node in dag["nodes"]]
    return compile_physical_execution_plan(
        logical_dag=dag, plan_revision=0,
        artifact_manifest_ref={"manifest_id": "m" * 64, "sha256": "a" * 64},
        worker_groups=[{"group_id": "g1", "role": "cpu_prepare", "profile_ref": {"profile_id": "cpu"},
                        "stage_refs": stage_refs, "min_workers": 1, "initial_workers": 1,
                        "max_workers": 1}],
        recovery={"checkpoint_interval_ms": 60000, "unconfirmed_rows": 1024,
                  "unconfirmed_bytes": 8 * 1024**2, "terminals": []},
        feasibility_snapshot_ref={"sha256": "b" * 64},
        partition_count=partition_count, read_domain="file://h:/r")


def test_rtg_physical_plan_channels_fully_covered_and_deterministic():
    dag = _diamond_dag()
    plan = _physical_plan(dag)
    ids = []
    for channel in plan["channels"]:
        for physical in materialize_channels(channel):
            validate_physical_channel(physical)
            assert physical["channel_id"] == physical_channel_id(
                channel["channel_id"], physical["producer_partition"], physical["consumer_partition"])
            ids.append(physical["channel_id"])
    assert len(ids) == 10 and len(set(ids)) == 10  # 5 edges x 2 forward pairs; K6 participants coverage
    again = _physical_plan(_diamond_dag())
    again_ids = [physical["channel_id"] for channel in again["channels"]
                 for physical in materialize_channels(channel)]
    assert ids == again_ids


def test_rtg_shared_ancestor_executes_once_at_logical_layer():
    # Bridge role = reuse boundary (frozen plan_facts semantics): the shared
    # upstream is never re-expanded into this action's instances, and fan-out
    # to consumers happens at the bridge's egress over distinct channels.
    dag = _diamond_dag(shared_role="bridge")
    shared_id = next(node["logical_node_id"] for node in dag["nodes"]
                     if node["op_kind"] == "materialize")
    assert shared_id in {item["logical_node_id"] for item in dag["shared_ancestors"]}
    assert all(node["op_kind"] != "from_items" for node in dag["nodes"])  # upstream not re-executed
    outgoing = [edge for edge in dag["edges"] if edge["from"]["logical_node_id"] == shared_id]
    assert len(outgoing) == 2
    left_channel = build_channel_plan(
        edge_id=outgoing[0]["edge_id"], scheme_version=1,
        producer_partitions=["p0"], consumer_partitions=["c0"],
        exchange_mode="memory_incremental", routing="forward", read_domain="file://h:/r")
    right_channel = build_channel_plan(
        edge_id=outgoing[1]["edge_id"], scheme_version=1,
        producer_partitions=["p0"], consumer_partitions=["c0"],
        exchange_mode="memory_incremental", routing="forward", read_domain="file://h:/r")
    assert materialize_channels(left_channel)[0]["channel_id"] != \
        materialize_channels(right_channel)[0]["channel_id"]


def test_rtg_sealed_scope_carries_into_physical_channels():
    channel = _plan("forward", ["p0"], ["c0"], mode="finite_sealed")
    physical = materialize_channels(channel)[0]
    assert physical["seal_scope"] == channel["seal_scope"]
    broken = dict(physical)
    broken.pop("seal_scope")
    with pytest.raises(ValueError):
        validate_physical_channel(broken)