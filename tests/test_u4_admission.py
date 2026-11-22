"""U4/S1 ADM conformance: engine-side fair admission (resource protocol v1 §07).

plan_activation is a pure plan: downstream (consume/model) roles are admitted
before producers, a downstream shortfall blocks every producer (no hold-and-wait
deadlock), control progress capacity is reserved off the top, and peers interleave
round-robin across fairness groups. Atomicity comes from the journal accept_plan
CAS plus the provider reserve — never from probing free capacity and then starting
as if that were atomic. FairTurns rotates the GraphInputReader pass order so a fast
channel is never systematically first and an empty/slow channel never blocks peers.
"""
import sys
import time

import pytest

from demiflow.execution.admission import FairTurns, RoleDemand, plan_activation
from demiflow.execution.exchange.carriage_local import (
    LocalSegmentStore, channel_pair, inline_records,
)
from demiflow.planning.dag import build_channel_plan
from demiflow.planning.graph_channels import RouteTable
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.device_pool import DevicePool
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.manager import ResourceManager
from demiflow.resources.process_unit import ManagedProcessUnit

GIB = 1024 ** 3


def _demand(allocation_id, role, cpu=1000, memory=GIB, fairness_key=""):
    return RoleDemand(allocation_id, role, cpu, memory, fairness_key=fairness_key)


def _capacity(cpu=4000, memory=4 * GIB):
    return {"cpu_millis": cpu, "memory_bytes": memory}


# ---------------- role ordering and deadlock prevention ----------------

def test_downstream_admitted_before_producers():
    demands = [_demand("p", "produce"), _demand("c1", "consume"),
                _demand("m1", "model"), _demand("c2", "consume")]
    plan = plan_activation(demands, capacity=_capacity(8000, 8 * GIB))
    assert plan["admitted"] == ["c1", "c2", "m1", "p"]
    assert plan["shortfall"] == []
    assert plan["reserved"] == {"cpu_millis": 4000, "memory_bytes": 4 * GIB}


def test_downstream_shortfall_blocks_all_producers():
    demands = [_demand("p", "produce", cpu=500), _demand("c", "consume", cpu=2000),
                _demand("m", "model", cpu=1000)]
    plan = plan_activation(demands, capacity=_capacity(2500))
    assert plan["admitted"] == ["c"]
    assert plan["shortfall"] == ["m", "p"]  # p would fit alone but must not hold downstream capacity
    assert plan["reserved"] == {"cpu_millis": 2000, "memory_bytes": GIB}


def test_oversized_downstream_does_not_block_peers_but_blocks_producers():
    demands = [_demand("c1", "consume", cpu=8000), _demand("c2", "consume"),
                _demand("p", "produce")]
    plan = plan_activation(demands, capacity=_capacity())
    assert plan["admitted"] == ["c2"]
    assert set(plan["shortfall"]) == {"c1", "p"}


def test_control_reserve_comes_off_the_top():
    demands = [_demand("c%d" % i, "consume") for i in range(4)]
    reserve = {"cpu_millis": 1000, "memory_bytes": GIB}
    plan = plan_activation(demands, capacity=_capacity(), control_reserve=reserve)
    assert plan["admitted"] == ["c0", "c1", "c2"]
    assert plan["shortfall"] == ["c3"]
    assert plan["reserved"] == {"cpu_millis": 3000, "memory_bytes": 3 * GIB}
    assert plan["control_reserve"] == reserve
    with pytest.raises(ValueError):
        plan_activation(demands, capacity=_capacity(500), control_reserve=reserve)


# ---------------- fairness ----------------

def test_model_groups_interleave_round_robin():
    demands = [_demand("a%d" % i, "model", fairness_key="model-a") for i in (1, 2, 3)]
    demands += [_demand("b%d" % i, "model", fairness_key="model-b") for i in (1, 2, 3)]
    plan = plan_activation(demands, capacity=_capacity())
    assert plan["admitted"] == ["a1", "b1", "a2", "b2"]  # neither model exhausts the tier first
    assert plan["shortfall"] == ["a3", "b3"]


def test_fair_turns_rotates_pass_start():
    turns = FairTurns(["a", "b", "c"])
    assert turns.pass_order() == ["a", "b", "c"]
    assert turns.pass_order() == ["b", "c", "a"]
    assert turns.pass_order() == ["c", "a", "b"]
    assert turns.pass_order() == ["a", "b", "c"]
    assert FairTurns(["only"]).pass_order() == ["only"]
    with pytest.raises(ValueError):
        FairTurns([])


def test_role_demand_validation():
    with pytest.raises(ValueError):
        _demand("x", "unknown")
    with pytest.raises(ValueError):
        _demand("x", "consume", cpu=0)
    with pytest.raises(ValueError):
        plan_activation([_demand("a", "consume"), _demand("a", "model")], capacity=_capacity())


# ---------------- purity: admission never probes-and-holds ----------------

@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local managed units require Linux")
def test_plan_and_describe_are_pure_no_reservation(tmp_path):
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))
    pool = LocalCapacityPool(tmp_path / "pool", capacity=_capacity())
    devices = DevicePool(tmp_path / "devices", inventory={"devices": [
        {"device_id": "gpu-0", "type": "gpu", "memory_bytes": 16 * GIB}]})
    before_pool, before_devices = pool.snapshot(), devices.snapshot()
    plan_activation([_demand("c", "consume"), _demand("p", "produce")], capacity=_capacity())
    assert pool.snapshot() == before_pool
    assert devices.snapshot() == before_devices
    provider = LocalResourceProvider(pool, ManagedProcessUnit(tmp_path / "units"),
                                     device_pool=devices)
    manager = ResourceManager(provider, state=None)
    described = manager.describe({"provider": "local"}, None)  # preflight probe, no reservation
    assert described["probe"]["effective"] == {"cpu_millis": 1, "memory_bytes": 1}
    assert pool.snapshot() == before_pool
    assert devices.snapshot() == before_devices


def test_fanout_roles_have_deterministic_target_order_and_report_shortfall():
    demands = [_demand("ancestor", "produce"), _demand("alloc-c1", "consume", fairness_key="c1"),
               _demand("alloc-c0", "consume", fairness_key="c0")]
    assert plan_activation(demands, capacity=_capacity())["admitted"] == [
        "alloc-c0", "alloc-c1", "ancestor"]
    assert plan_activation(demands, capacity=_capacity(2000, 2 * GIB))["shortfall"] == ["ancestor"]


# ---------------- reader-side fairness over real socketpairs ----------------

def _channel(edge_id, consumer="c0"):
    return build_channel_plan(
        edge_id=edge_id, scheme_version=1, producer_partitions=["p0"],
        consumer_partitions=[consumer], exchange_mode="memory_incremental",
        routing="forward", read_domain="file://h:/r",
        credit_budget={"rows": 32, "bytes": 1 << 20}, durability_declared="ephemeral",
        seal_scope=None)


def _records(tag, ordinal):
    return inline_records([{"v": "%s%d" % (tag, ordinal)}], source_id="src",
                          occurrence_id="shared", start_ordinal=ordinal)


def _wait_eoi(reader, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if reader._all_eoi():
            return
        time.sleep(0.01)
    raise AssertionError("reader did not observe end-of-input on every channel")


