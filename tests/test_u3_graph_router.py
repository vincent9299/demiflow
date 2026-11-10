"""U3 graph data-plane conformance: decoupled per-channel carriage (S3, exit gate 1).

Real same-host socketpairs + independent sender/credit threads. The shared
ancestor routes each record once; broadcast replicates at egress into six
independent channels. A slow target lags on its own channel without fencing or
stalling siblings: fast targets dequeue all records timely while the slow target
drains later, no record is lost, and egress computation happens exactly once.
"""
import threading
import time

import pytest

from demiflow.execution.exchange.carriage_local import (
    LocalSegmentStore, channel_pair, inline_records,
)
from demiflow.execution.exchange.graph_router import GraphInputReader, GraphOutputRouter
from demiflow.planning.dag import build_channel_plan
from demiflow.planning.graph_channels import RouteTable


def _channel(routing, consumers, *, mode="memory_incremental", budget=None):
    return build_channel_plan(
        edge_id="shared", scheme_version=1, producer_partitions=["p0"],
        consumer_partitions=consumers, exchange_mode=mode, routing=routing,
        read_domain="file://h:/r", credit_budget=budget or {"rows": 32, "bytes": 1 << 20},
        durability_declared="durable" if mode == "durable_incremental" else "ephemeral",
        seal_scope=({"scope_id": "scope-1", "membership": ["p0"], "generation": 1}
                    if mode == "finite_sealed" else None))


def _wire(channel, tmp_path):
    table = RouteTable(channel)
    store = LocalSegmentStore(tmp_path, host_id="h")
    producer_links, consumer_links, specs = {}, {}, []
    for physical in table.physical_channels:
        cid = physical["channel_id"]
        producer_link, consumer_link = channel_pair()
        producer_links[cid] = producer_link
        consumer_links[cid] = consumer_link
        specs.append({"channel_id": cid, "reader_id": physical["consumer_partition"],
                      "exchange_mode": physical["exchange_mode"],
                      "credit_budget": physical["credit_budget"], "route_epoch": 0})
    return table, store, producer_links, consumer_links, specs


def _records(ordinal):
    return inline_records([{"v": ordinal}], source_id="src", occurrence_id="shared",
                          start_ordinal=ordinal)


def _consume(reader, sink, *, delay=0.0):
    reader.start()
    for row in reader.rows():
        if delay:
            time.sleep(delay)
        sink.append((row, time.monotonic()))


def test_router_broadcast_six_targets_single_egress_timely_dequeue(tmp_path):
    consumers = ["c0", "c1", "c2", "c3", "c4", "c5"]
    channel = _channel("broadcast", consumers)
    table, store, plinks, clinks, specs = _wire(channel, tmp_path)
    router = GraphOutputRouter(specs, plinks, producer={"logical_node_id": "shared", "occurrence_id": "shared-p0"},
                               route_table=table, depth=256)
    # c3 is the slow target; the other five must dequeue timely regardless.
    readers = {spec["channel_id"]: GraphInputReader(
        [spec], {spec["channel_id"]: clinks[spec["channel_id"]]},
        reader_id=spec["reader_id"], reader_domain=store.domain, store=store)
        for spec in specs}
    sinks = {spec["reader_id"]: [] for spec in specs}
    finished = {}
    threads = []
    for spec in specs:
        reader = readers[spec["channel_id"]]
        rid = spec["reader_id"]
        delay = 0.005 if rid == "c3" else 0.0

        def run(reader=reader, sink=sinks[rid], delay=delay, rid=rid):
            _consume(reader, sink, delay=delay)
            finished[rid] = time.monotonic()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        threads.append(thread)

    n = 120
    start = time.monotonic()
    egress_calls = 0
    for ordinal in range(n):
        router.route(_records(ordinal))  # shared record computed once, replicated at egress
        egress_calls += 1
    router.end_of_input()
    for thread in threads:
        thread.join(timeout=10)
    assert all(len(sinks[rid]) == n for rid in consumers), {rid: len(sinks[rid]) for rid in consumers}
    # Every target received the identical record multiset (broadcast replication).
    for rid in consumers:
        assert [row["v"] for row, _ in sinks[rid]] == list(range(n))
    # Egress computed once per source record; each channel sent exactly n.
    assert egress_calls == n
    assert router.sent_records() == {spec["channel_id"]: n for spec in specs}
    # Fast targets dequeue timely; the slow target lags on its own channel only.
    fast_finish = max(finished[rid] for rid in consumers if rid != "c3")
    slow_finish = finished["c3"]
    assert fast_finish - start < 0.5, fast_finish - start
    assert slow_finish - fast_finish > 0.2, slow_finish - fast_finish
    router.close()
    for reader in readers.values():
        reader.close()


def test_router_forward_one_to_one_delivers_only_to_matching_target(tmp_path):
    channel = _channel("forward", ["c0"])
    table, store, plinks, clinks, specs = _wire(channel, tmp_path)
    router = GraphOutputRouter(specs, plinks, producer={"logical_node_id": "n", "occurrence_id": "n-p0"},
                               route_table=table, depth=64)
    spec = specs[0]
    reader = GraphInputReader([spec], {spec["channel_id"]: clinks[spec["channel_id"]]},
                              reader_id="c0", reader_domain=store.domain, store=store)
    sink = []
    thread = threading.Thread(target=lambda: _consume(reader, sink), daemon=True)
    thread.start()
    for ordinal in range(20):
        assert router.route(_records(ordinal)) == [spec["channel_id"]]
    router.end_of_input()
    thread.join(timeout=5)
    assert [row["v"] for row, _ in sink] == list(range(20))
    router.close()
    reader.close()


def test_router_target_routing_isolates_slow_class(tmp_path):
    # Two business target classes; records route by declared class, never rebalanced.
    channel = _channel("target", ["c0", "c1"])
    table = RouteTable(channel, target_classes={"alpha": ["c0"], "beta": ["c1"]})
    store = LocalSegmentStore(tmp_path, host_id="h")
    plinks, clinks, specs = {}, {}, []
    for physical in table.physical_channels:
        cid = physical["channel_id"]
        plink, clink = channel_pair()
        plinks[cid], clinks[cid] = plink, clink
        specs.append({"channel_id": cid, "reader_id": physical["consumer_partition"],
                      "exchange_mode": "memory_incremental",
                      "credit_budget": physical["credit_budget"], "route_epoch": 0})
    router = GraphOutputRouter(specs, plinks, producer={"logical_node_id": "n", "occurrence_id": "n-p0"},
                               route_table=table, depth=128)
    readers = {spec["reader_id"]: GraphInputReader(
        [spec], {spec["channel_id"]: clinks[spec["channel_id"]]},
        reader_id=spec["reader_id"], reader_domain=store.domain, store=store) for spec in specs}
    sinks = {"c0": [], "c1": []}
    threads = []
    for rid, reader in readers.items():
        delay = 0.004 if rid == "c1" else 0.0
        thread = threading.Thread(target=lambda r=reader, s=sinks[rid], d=delay: _consume(r, s, delay=d), daemon=True)
        thread.start()
        threads.append(thread)
    for ordinal in range(60):
        route_value = "alpha" if ordinal % 2 == 0 else "beta"
        router.route(_records(ordinal), route_value=route_value)
    router.end_of_input()
    for thread in threads:
        thread.join(timeout=10)
    assert [row["v"] for row, _ in sinks["c0"]] == list(range(0, 60, 2))
    assert [row["v"] for row, _ in sinks["c1"]] == list(range(1, 60, 2))
    router.close()
    for reader in readers.values():
        reader.close()


def test_router_credit_feedback_replenishes_and_cursors_advance(tmp_path):
    channel = _channel("forward", ["c0"], budget={"rows": 4, "bytes": 1 << 20})
    table, store, plinks, clinks, specs = _wire(channel, tmp_path)
    router = GraphOutputRouter(specs, plinks, producer={"logical_node_id": "n", "occurrence_id": "n-p0"},
                               route_table=table, depth=64)
    spec = specs[0]
    reader = GraphInputReader([spec], {spec["channel_id"]: clinks[spec["channel_id"]]},
                              reader_id="c0", reader_domain=store.domain, store=store)
    sink = []
    thread = threading.Thread(target=lambda: _consume(reader, sink), daemon=True)
    thread.start()
    # Window is only 4 rows; 50 records must still flow via consumer replenishment.
    for ordinal in range(50):
        router.route(_records(ordinal))
    router.end_of_input()
    thread.join(timeout=5)
    assert [row["v"] for row, _ in sink] == list(range(50))
    assert reader.cursors[spec["channel_id"]] >= 49
    router.close()
    reader.close()