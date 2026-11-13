"""B-package pure interpretation: sealed inputs -> read-only ExecutionLayout.

Interface unit tests use test-domain fixed document doubles (program_ref /
manifest_ref / profile) plus the real frozen planning compilers for the chain
and a hand-built single-occurrence fanout for the static graph. No run, no
business call, no file I/O: interpretation is pure. All scratch stays in memory.

Positive: legal chain / static graph item-by-item correspondence, repeated
interpretation fully equal, no side effects, deep read-only detachment.
Negative: missing stage / multi-ownership / unknown profile / wrong program
scope / wrong manifest-dag-plan / unsupported slice / multi-worker / multi-
partition / channel coverage / assignment out-of-bounds all fail closed on
.reason (never on the message string).
"""
import copy
import re
from dataclasses import FrozenInstanceError
from types import MappingProxyType, SimpleNamespace

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts.worker import ASSIGNMENT_SCHEMA
from demiflow.execution.plan_interpreter import (
    EXECUTION_BINDINGS_SCHEMA, SUPPORTED_SLICES, ChannelBinding, CheckpointPolicy,
    ExecutionLayout, PlanContractError, WorkerPlacement, _freeze,
    interpret_physical_plan, to_wire, validate_worker_assignment,
)
from demiflow.execution.recovery_contract import RecoveryContractError, document, identity
from demiflow.planning.dag import (
    LOGICAL_DAG_SCHEMA, _DEFAULT_TRAITS, build_channel_plan, channel_id_for,
    compile_logical_dag, compile_physical_execution_plan, dag_id_for, edge_id_for,
    occurrence_id_for, partition_id_for, validate_logical_dag,
)
from demiflow.planning.graph_channels import materialize_channels

_HEX = re.compile(r"[0-9a-f]{64}\Z")

# Test-domain doubles: the interpreter never reads the pickle or the manifest.
PROFILE = {"requests": {"cpu_millis": 1000, "memory_bytes": 1024 ** 3,
                        "accelerators": [{"type": "gpu", "count": 1}],
                        "custom": {"slots": {"amount": 2, "unit": "core"}}}}
PROFILE_REF = identity(PROFILE)
MANIFEST_REF = {"manifest_id": "test-manifest", "sha256": "ab" * 32}
PROGRAM_REF = {"path": "scratch/chain-plans/" + "cd" * 32 + ".pkl", "sha256": "cd" * 32,
               "bytes": 128, "code_pin_digest": "ef" * 32, "code_sources": []}
RECOVERY = {"checkpoint_interval_ms": 60000, "unconfirmed_rows": 1024,
            "unconfirmed_bytes": 8 * 1024 ** 2, "terminals": []}


def stage_id(ld, node):
    return identity([ld["dag_id"], node["occurrence_id"]])


def bundle_for(doc):
    """The {ref, document} bundle; ref mirrors the real content-addressed carrier."""
    raw = document(doc)
    digest = identity(doc)
    ref = {"path": "scratch/chain-execution-bindings/" + digest + ".json",
           "sha256": digest, "bytes": len(raw)}
    return {"ref": ref, "document": doc}


def _assemble(ld, grouping, *, slice_name, run_id="run-test", checkpoint_rows=2,
              profiles=None, program_ref=None, source_kind=None):
    """Compile the physical plan and build the matching execution-bindings doc.

    grouping is [(group_id, [operation nodes])]; the driver owns exactly the
    source/terminal stages, each worker group owns operation stages only.
    """
    nodes_by_id = {n["logical_node_id"]: n for n in ld["nodes"]}
    source_nodes = [n for n in ld["nodes"] if not n["input_ports"]]
    terminal_nodes = [n for n in ld["nodes"] if not n["output_ports"]]
    operation_nodes = [n for n in ld["nodes"] if n["input_ports"] and n["output_ports"]]
    scheme_version = ld["sources"][0]["partition_scheme"]["scheme_version"]
    profiles = {PROFILE_REF: copy.deepcopy(PROFILE)} if profiles is None else profiles
    profile_ref = next(iter(profiles))
    program_ref = PROGRAM_REF if program_ref is None else program_ref
    groups = [{"group_id": gid, "role": "cpu_prepare", "profile_ref": profile_ref,
               "stage_refs": [stage_id(ld, n) for n in members],
               "min_workers": 1, "initial_workers": 1, "max_workers": 1}
              for gid, members in grouping]
    pp = compile_physical_execution_plan(
        logical_dag=ld, plan_revision=0, artifact_manifest_ref=MANIFEST_REF,
        worker_groups=groups, recovery=dict(RECOVERY),
        feasibility_snapshot_ref=identity(["test-feasibility"]),
        read_domain="test-domain", durability_declared="durable",
        partition_count=1, scheme_kind="single")
    programs = []
    for gid, members in grouping:
        stages = [stage_id(ld, n) for n in members]
        programs.append({
            "group_id": gid, "stage_ids": stages, "plan_ref": copy.deepcopy(program_ref),
            "sealed_operators": [{"logical_node_id": n["logical_node_id"],
                                  "occurrence_id": n["occurrence_id"]} for n in members],
            "operator_bindings": [{"program_index": i, "stage_id": s}
                                  for i, s in enumerate(stages)]})
    if source_kind is None:
        source_kind = ("queue_declaration" if slice_name == "queue_transform_v1"
                       else "fixed_source")
    source_bindings = []
    for node in source_nodes:
        if source_kind == "queue_declaration":
            ref = {"source_id": "queue-src", "source_identity": identity(["queue-decl"])}
        else:
            ref = {"source_id": ld["sources"][0]["source_id"],
                   "source_identity_ref": ld["sources"][0]["source_identity_ref"]}
        source_bindings.append({"stage_id": stage_id(ld, node), "kind": source_kind, "ref": ref})
    channel_options = [{"channel_id": channel_id_for(e["edge_id"], scheme_version),
                        "logical_edge_ids": [e["edge_id"]]} for e in ld["edges"]]
    doc = {"schema": EXECUTION_BINDINGS_SCHEMA, "run_id": run_id, "action_id": ld["action_id"],
           "dag_id": ld["dag_id"], "plan_id": pp["plan_id"], "artifact_manifest_ref": MANIFEST_REF,
           "supported_slice": slice_name, "profiles": profiles, "programs": programs,
           "driver_stage_ids": [stage_id(ld, n) for n in source_nodes + terminal_nodes],
           "source_bindings": source_bindings, "channel_options": channel_options,
           "checkpoint_rows": checkpoint_rows}
    return SimpleNamespace(
        ld=ld, pp=pp, mref=MANIFEST_REF, doc=doc, bundle=bundle_for(doc), grouping=grouping,
        source_nodes=source_nodes, terminal_nodes=terminal_nodes, operation_nodes=operation_nodes,
        nodes_by_id=nodes_by_id, action_id=ld["action_id"], slice_name=slice_name)


def chain_fixture(action_id="main-action", *, slice_name="chain_v1"):
    def first(row):
        return {**row, "a": row["x"] + 1}

    def second(row):
        return {**row, "b": row["a"] * 2}
    plan = LogicalPlan(tuple(MapOp(CallableSpec.create(fn)) for fn in (first, second)))
    ld = compile_logical_dag(action_id=action_id, source=ItemsSource(items=({"x": 1}, {"x": 2})),
                             operations=plan.operations, terminal_kind="materialize",
                             sink_ids=("output",))
    return _assemble(ld, [("g0", ld["nodes"][1:-1])], slice_name=slice_name)


def _fanout_dag(action_id):
    """Hand-built single-occurrence fanout: src -> shared -> {left, right} -> act.

    compile_declared_dag re-expands a shared ancestor per consumer path (multiple
    occurrence stages per logical node), which the single-occurrence slices reject;
    this fixture keeps exactly one occurrence per node using the frozen id helpers.
    """
    def traits(category, worker_model="ephemeral"):
        value = dict(_DEFAULT_TRAITS)
        value["category"], value["worker_model"] = category, worker_model
        return value

    def node(name, kind, inputs, outputs, category, worker_model="ephemeral"):
        logical = identity({"fanout_node": name})
        return {"logical_node_id": logical,
                "occurrence_id": occurrence_id_for(action_id, logical, 0),
                "op_kind": kind, "operator_spec_ref": None,
                "input_ports": inputs, "output_ports": outputs,
                "traits": traits(category, worker_model)}

    named = {"src": node("src", "from_items", [], ["out"], "source"),
             "shared": node("shared", "map", ["in"], ["out"], "transform"),
             "left": node("left", "map", ["in"], ["out"], "transform"),
             "right": node("right", "map", ["in"], ["out"], "transform"),
             "act": node("act", "count", ["in"], [], "action", "driver")}

    def edge(a, b):
        return {"edge_id": edge_id_for(a["logical_node_id"], "out", b["logical_node_id"], "in"),
                "from": {"logical_node_id": a["logical_node_id"], "port": "out"},
                "to": {"logical_node_id": b["logical_node_id"], "port": "in"}}
    edges = [edge(named["src"], named["shared"]), edge(named["shared"], named["left"]),
             edge(named["shared"], named["right"]), edge(named["left"], named["act"]),
             edge(named["right"], named["act"])]
    ld = {"schema_version": LOGICAL_DAG_SCHEMA, "action_id": action_id,
          "nodes": list(named.values()), "edges": edges,
          "sources": [{"source_id": named["src"]["logical_node_id"],
                       "source_identity_ref": {"fanout_source": "src"},
                       "partition_scheme": {"kind": "single", "partition_count": 1,
                                            "scheme_version": 1}}],
          "terminals": [{"logical_node_id": named["act"]["logical_node_id"],
                         "kind": "count", "sink_ids": ["output"]}],
          "shared_ancestors": []}
    ld["dag_id"] = dag_id_for(ld)
    validate_logical_dag(ld)
    return ld, named


def fanout_fixture(action_id="fanout-action"):
    ld, named = _fanout_dag(action_id)
    grouping = [("gA", [named["shared"]]), ("gB", [named["left"]]), ("gC", [named["right"]])]
    return _assemble(ld, grouping, slice_name="static_fanout_v1")


def fanout_joint_fixture(action_id="fanout-action"):
    """Static fanout whose shared egress is ONE joint fan_out channel over two edges.

    Exercises the contract's joint-routing branch (logical_edge_ids > 1): one
    physical channel plan covers shared->left and shared->right, materializing to
    two physical pair ids. Single-partition compile emits per-edge forward channels,
    so the joint plan is assembled here from the frozen build_channel_plan helper.
    """
    fx = fanout_fixture(action_id)
    ld, pp, doc = copy.deepcopy(fx.ld), copy.deepcopy(fx.pp), copy.deepcopy(fx.doc)
    shared, left, right = (fx.grouping[i][1][0] for i in (0, 1, 2))
    p_shared = partition_id_for(shared["logical_node_id"], 1, 0)
    p_left = partition_id_for(left["logical_node_id"], 1, 0)
    p_right = partition_id_for(right["logical_node_id"], 1, 0)
    e_left = edge_id_for(shared["logical_node_id"], "out", left["logical_node_id"], "in")
    e_right = edge_id_for(shared["logical_node_id"], "out", right["logical_node_id"], "in")
    template = pp["channels"][0]
    joint = build_channel_plan(
        edge_id=e_left, scheme_version=1, producer_partitions=[p_shared],
        consumer_partitions=[p_left, p_right], exchange_mode=template["exchange_mode"],
        routing="fan_out", credit_budget=template["credit_budget"],
        durability_declared="durable", read_domain="test-domain")
    dropped = {channel_id_for(e_left, 1), channel_id_for(e_right, 1)}
    pp["channels"] = [c for c in pp["channels"] if c["channel_id"] not in dropped] + [joint]
    pp["plan_id"] = identity({k: v for k, v in pp.items() if k != "plan_id"})
    doc["channel_options"] = [o for o in doc["channel_options"]
                              if o["logical_edge_ids"][0] not in (e_left, e_right)]
    doc["channel_options"].append({"channel_id": joint["channel_id"],
                                   "logical_edge_ids": [e_left, e_right]})
    doc["plan_id"] = pp["plan_id"]
    return SimpleNamespace(
        ld=ld, pp=pp, mref=MANIFEST_REF, doc=doc, bundle=bundle_for(doc), grouping=fx.grouping,
        source_nodes=fx.source_nodes, terminal_nodes=fx.terminal_nodes,
        operation_nodes=fx.operation_nodes, nodes_by_id=fx.nodes_by_id,
        action_id=action_id, slice_name="static_fanout_v1", joint_edges=(e_left, e_right))


def expected_channel_ids(layout, placement):
    """Independent oracle: physical pair ids for cross-worker, non-driver edges."""
    partitions = set(placement.partition_ids)
    inputs, outputs = set(), set()
    for channel in layout.channels:
        if channel.in_process:
            continue
        producer_kinds = {layout.stage_owners[s]["kind"] for s in channel.producer_stage_ids}
        consumer_kinds = {layout.stage_owners[s]["kind"] for s in channel.consumer_stage_ids}
        if "driver" in producer_kinds or "driver" in consumer_kinds:
            continue
        for physical in materialize_channels(to_wire(channel.channel_plan)):
            cid = physical["channel_id"]
            producer_here = physical["producer_partition"] in partitions
            consumer_here = physical["consumer_partition"] in partitions
            if consumer_here and not producer_here:
                inputs.add(cid)
            if producer_here and not consumer_here:
                outputs.add(cid)
    return inputs, outputs


def assignment_for(layout, placement, inputs, outputs):
    return {"schema_version": ASSIGNMENT_SCHEMA, "assignment_id": "a1", "operation_id": "op1",
            "scope_id": "sc1", "allocation_id": "al1", "boot_id": "b1", "worker_id": "w1",
            "controller_epoch": 0, "grant_ref": "gr1", "plan_revision": layout.plan_revision,
            "expected_plan_revision": layout.plan_revision,
            "artifact_manifest_ref": to_wire(layout.artifact_manifest_ref),
            "stages": list(placement.stage_ids), "partitions": list(placement.partition_ids),
            "channels": {"inputs": [{"channel_id": c} for c in inputs], "outputs": list(outputs)},
            "restore_point": None, "route_epoch": 0, "topology_revision": 0,
            "ready_deadline_ms": 1000}


def _interpret(fx, *, ld=None, pp=None, bundle=None, mref=None):
    return interpret_physical_plan(
        fx.ld if ld is None else ld, fx.pp if pp is None else pp,
        bindings=fx.bundle if bundle is None else bundle,
        manifest_ref=fx.mref if mref is None else mref)


def _reseal(pp):
    pp = copy.deepcopy(pp)
    pp["plan_id"] = identity({k: v for k, v in pp.items() if k != "plan_id"})
    return pp


def _mutated(fx, *, doc=None, pp=None):
    """Deep-copy fixture inputs, apply mutations, keep plan_id bindings consistent."""
    ld = copy.deepcopy(fx.ld)
    new_pp = copy.deepcopy(fx.pp) if pp is None else pp
    new_doc = copy.deepcopy(fx.doc) if doc is None else doc
    if pp is not None:
        new_pp = _reseal(new_pp)
        new_doc["plan_id"] = new_pp["plan_id"]
    return ld, new_pp, bundle_for(new_doc)


def _reason(exc):
    assert isinstance(exc.value, PlanContractError)
    assert isinstance(exc.value, RecoveryContractError)
    return exc.value.reason


# --- positive: chain item-by-item correspondence ---

def test_chain_layout_corresponds_item_by_item():
    fx = chain_fixture()
    layout = _interpret(fx)
    assert isinstance(layout, ExecutionLayout)
    assert layout.dag_id == fx.ld["dag_id"]
    assert layout.plan_id == fx.pp["plan_id"]
    assert layout.plan_revision == fx.pp["plan_revision"] == 0
    assert to_wire(layout.artifact_manifest_ref) == MANIFEST_REF
    assert to_wire(layout.bindings_ref) == fx.bundle["ref"]
    assert layout.supported_slice == "chain_v1"
    # stages: every physical stage, frozen, in order
    assert [to_wire(s) for s in layout.stages] == fx.pp["stages"]
    # ownership: driver owns source+terminal, g0 owns the two operations
    src_sid = stage_id(fx.ld, fx.source_nodes[0])
    term_sid = stage_id(fx.ld, fx.terminal_nodes[0])
    op_sids = [stage_id(fx.ld, n) for n in fx.operation_nodes]
    assert to_wire(layout.stage_owners[src_sid]) == {"kind": "driver"}
    assert to_wire(layout.stage_owners[term_sid]) == {"kind": "driver"}
    for sid in op_sids:
        assert to_wire(layout.stage_owners[sid]) == {"kind": "worker", "group_id": "g0"}
    assert set(layout.stage_owners) == {s["stage_id"] for s in fx.pp["stages"]}
    # one worker placement
    assert len(layout.workers) == 1
    worker = layout.workers[0]
    assert isinstance(worker, WorkerPlacement)
    assert worker.group_id == "g0"
    assert worker.stage_ids == tuple(op_sids)
    assert worker.partition_ids == tuple(fx.pp["stages"][i]["partitions"][0]
                                         for i in (1, 2))
    assert to_wire(worker.profile) == PROFILE
    assert to_wire(worker.program_ref) == PROGRAM_REF
    assert [to_wire(o) for o in worker.sealed_operators] == fx.doc["programs"][0]["sealed_operators"]
    assert [to_wire(b) for b in worker.operator_bindings] == fx.doc["programs"][0]["operator_bindings"]
    # channels: one per edge, endpoints + in_process derived from ownership
    assert len(layout.channels) == len(fx.pp["channels"])
    for binding, channel in zip(layout.channels, fx.pp["channels"]):
        assert isinstance(binding, ChannelBinding)
        assert binding.channel_id == channel["channel_id"]
        assert to_wire(binding.channel_plan) == channel
    # explicit endpoint correspondence per edge
    for binding, edge in zip(layout.channels, fx.ld["edges"]):
        producer = fx.nodes_by_id[edge["from"]["logical_node_id"]]
        consumer = fx.nodes_by_id[edge["to"]["logical_node_id"]]
        assert binding.producer_stage_ids == (stage_id(fx.ld, producer),)
        assert binding.consumer_stage_ids == (stage_id(fx.ld, consumer),)
        opts = next(o for o in fx.doc["channel_options"]
                    if o["channel_id"] == binding.channel_id)
        assert to_wire(binding.options) == opts
    # the middle operation->operation channel is in-process; driver edges are not
    assert [c.in_process for c in layout.channels] == [False, True, False]
    # source bindings + checkpoint
    assert [to_wire(s) for s in layout.source_bindings] == fx.doc["source_bindings"]
    assert isinstance(layout.checkpoint, CheckpointPolicy)
    assert layout.checkpoint == CheckpointPolicy(
        checkpoint_rows=2, interval_ms=RECOVERY["checkpoint_interval_ms"],
        unconfirmed_rows=RECOVERY["unconfirmed_rows"], unconfirmed_bytes=RECOVERY["unconfirmed_bytes"])


def test_fanout_layout_corresponds_item_by_item():
    fx = fanout_fixture()
    layout = _interpret(fx)
    assert layout.supported_slice == "static_fanout_v1"
    assert len(layout.workers) == 3
    assert {w.group_id for w in layout.workers} == {"gA", "gB", "gC"}
    by_group = {w.group_id: w for w in layout.workers}
    for gid, members in fx.grouping:
        worker = by_group[gid]
        assert worker.stage_ids == tuple(stage_id(fx.ld, n) for n in members)
        assert worker.partition_ids == tuple(
            partition_id_for(n["logical_node_id"], 1, 0) for n in members)
        assert to_wire(worker.profile) == PROFILE
    # five channels, none in-process (every operation is its own worker)
    assert len(layout.channels) == 5
    assert all(not c.in_process for c in layout.channels)
    # driver owns exactly src + act
    driver = {sid for sid, o in layout.stage_owners.items() if o["kind"] == "driver"}
    assert driver == {stage_id(fx.ld, fx.source_nodes[0]), stage_id(fx.ld, fx.terminal_nodes[0])}
    # cross-worker channel ownership: gA produces two, gB/gC each consume one
    ga_in, ga_out = expected_channel_ids(layout, by_group["gA"])
    gb_in, gb_out = expected_channel_ids(layout, by_group["gB"])
    gc_in, gc_out = expected_channel_ids(layout, by_group["gC"])
    assert ga_in == set() and len(ga_out) == 2
    assert len(gb_in) == 1 and gb_out == set()
    assert len(gc_in) == 1 and gc_out == set()
    assert ga_out == gb_in | gc_in


def test_partition_and_stage_ids_are_hex_strings():
    for fx in (chain_fixture(), fanout_fixture()):
        layout = _interpret(fx)
        for worker in layout.workers:
            assert isinstance(worker.partition_ids, tuple)
            for pid in worker.partition_ids:
                assert type(pid) is str and _HEX.fullmatch(pid)
            for sid in worker.stage_ids:
                assert type(sid) is str and _HEX.fullmatch(sid)


# --- positive: determinism, purity, read-only detachment ---

def test_repeated_interpretation_fully_equal():
    fx = fanout_fixture()
    first = _interpret(fx)
    second = _interpret(fx)
    assert first == second
    assert to_wire(first) == to_wire(second)
    # re-interpreting deep-copied inputs is also equal (no hidden aliasing/state)
    ld, pp, bundle = _mutated(fx)
    assert _interpret(fx, ld=ld, pp=pp, bundle=bundle) == first


def test_interpretation_has_no_side_effects():
    fx = chain_fixture()
    ld_before = copy.deepcopy(fx.ld)
    pp_before = copy.deepcopy(fx.pp)
    bundle_before = copy.deepcopy(fx.bundle)
    _interpret(fx)
    assert fx.ld == ld_before
    assert fx.pp == pp_before
    assert fx.bundle == bundle_before


def test_layout_views_are_frozen_and_detached():
    fx = chain_fixture()
    layout = _interpret(fx)
    assert isinstance(layout.stages[0], MappingProxyType)
    assert isinstance(layout.stage_owners, MappingProxyType)
    profile = layout.workers[0].profile
    assert isinstance(profile, MappingProxyType)
    accelerators = profile["requests"]["accelerators"]
    assert isinstance(accelerators, tuple)
    assert isinstance(accelerators[0], MappingProxyType)
    assert isinstance(profile["requests"]["custom"]["slots"], MappingProxyType)
    # mutating the source document after interpretation cannot reach the layout
    fx.doc["profiles"][PROFILE_REF]["requests"]["cpu_millis"] = 999999
    fx.doc["profiles"][PROFILE_REF]["requests"]["accelerators"][0]["count"] = 99
    assert layout.workers[0].profile["requests"]["cpu_millis"] == 1000
    assert layout.workers[0].profile["requests"]["accelerators"][0]["count"] == 1
    # frozen data classes reject attribute assignment
    with pytest.raises(FrozenInstanceError):
        layout.workers[0].group_id = "other"
    with pytest.raises(FrozenInstanceError):
        layout.checkpoint.checkpoint_rows = 5


def test_to_wire_is_deep_independent_json():
    fx = fanout_fixture()
    layout = _interpret(fx)
    wire = to_wire(layout)
    assert type(wire) is dict
    assert wire["workers"][0]["profile"] == PROFILE
    # mutating the wire output never touches the layout
    wire["workers"][0]["profile"]["requests"]["cpu_millis"] = 1
    wire["workers"][0]["stage_ids"].append("bogus")
    assert layout.workers[0].profile["requests"]["cpu_millis"] == 1000
    assert "bogus" not in layout.workers[0].stage_ids
    assert to_wire(layout.workers[0].profile) == PROFILE


def test_freeze_rejects_non_json_value():
    with pytest.raises(PlanContractError) as exc:
        _freeze({"bad": {1, 2}})
    assert _reason(exc) == "invalid_contract"


# --- positive: assignment validation ---

def test_chain_worker_assignment_accepts_empty_channels():
    fx = chain_fixture()
    layout = _interpret(fx)
    worker = layout.workers[0]
    inputs, outputs = expected_channel_ids(layout, worker)
    assert inputs == set() and outputs == set()
    placement = validate_worker_assignment(
        layout, assignment_for(layout, worker, sorted(inputs), sorted(outputs)), group_id="g0")
    assert placement is worker


def test_fanout_worker_assignments_accept_cross_worker_channels():
    fx = fanout_fixture()
    layout = _interpret(fx)
    for worker in layout.workers:
        inputs, outputs = expected_channel_ids(layout, worker)
        placement = validate_worker_assignment(
            layout, assignment_for(layout, worker, sorted(inputs), sorted(outputs)),
            group_id=worker.group_id)
        assert placement is worker


def test_queue_transform_slice_accepted():
    fx = chain_fixture(slice_name="queue_transform_v1")
    layout = _interpret(fx)
    assert layout.supported_slice == "queue_transform_v1"
    assert all(sb["kind"] == "queue_declaration" for sb in layout.source_bindings)


def test_fanout_joint_routing_layout_and_assignment():
    fx = fanout_joint_fixture()
    layout = _interpret(fx)
    assert layout.supported_slice == "static_fanout_v1"
    # src->shared, the joint fan_out channel, left->act, right->act
    assert len(layout.channels) == 4
    joint = next(c for c in layout.channels if len(c.consumer_stage_ids) == 2)
    assert joint.channel_plan["routing"] == "fan_out"
    assert to_wire(joint.options)["logical_edge_ids"] == list(fx.joint_edges)
    assert not joint.in_process
    by_group = {w.group_id: w for w in layout.workers}
    ga_in, ga_out = expected_channel_ids(layout, by_group["gA"])
    gb_in, gb_out = expected_channel_ids(layout, by_group["gB"])
    gc_in, gc_out = expected_channel_ids(layout, by_group["gC"])
    assert ga_in == set() and len(ga_out) == 2  # one joint plan -> two physical pair ids
    assert len(gb_in) == 1 and gb_out == set()
    assert len(gc_in) == 1 and gc_out == set()
    assert ga_out == gb_in | gc_in
    for worker in layout.workers:
        inputs, outputs = expected_channel_ids(layout, worker)
        validate_worker_assignment(
            layout, assignment_for(layout, worker, sorted(inputs), sorted(outputs)),
            group_id=worker.group_id)


def test_joint_routing_consumer_coverage_rejected():
    fx = fanout_joint_fixture()
    doc = copy.deepcopy(fx.doc)
    joint = next(o for o in doc["channel_options"] if len(o["logical_edge_ids"]) == 2)
    joint["logical_edge_ids"] = joint["logical_edge_ids"][:1]  # leave a consumer uncovered
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


# --- negative: interpretation relational gates ---

def test_missing_stage_rejected():
    fx = chain_fixture()
    pp = copy.deepcopy(fx.pp)
    pp["stages"] = pp["stages"][1:]  # drop the source stage
    ld, pp, bundle = _mutated(fx, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_stage_multi_ownership_rejected():
    fx = fanout_fixture()
    pp = copy.deepcopy(fx.pp)
    # gB also claims gA's stage -> a stage owned by two groups
    pp["worker_groups"][1]["stage_refs"].append(pp["worker_groups"][0]["stage_refs"][0])
    ld, pp, bundle = _mutated(fx, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_unknown_profile_rejected():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    doc["profiles"] = {}  # group profile_ref no longer resolves
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_profile_ref_must_be_content_identity():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    doc["profiles"] = {"profile-local": PROFILE}  # baseline-style local ref, not identity
    pp = copy.deepcopy(fx.pp)
    pp["worker_groups"][0]["profile_ref"] = "profile-local"
    ld, pp, bundle = _mutated(fx, doc=doc, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "identity_mismatch"


def test_wrong_program_scope_rejected():
    fx = fanout_fixture()
    doc = copy.deepcopy(fx.doc)
    # gB's program claims gA's stage instead of its own
    doc["programs"][1]["stage_ids"] = list(doc["programs"][0]["stage_ids"])
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_program_owning_driver_stage_rejected():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    src_sid = stage_id(fx.ld, fx.source_nodes[0])
    doc["programs"][0]["stage_ids"].append(src_sid)  # source is not an operation
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_operator_binding_wrong_node_rejected():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    # bind program_index 0 to the second operation's stage (node mismatch)
    doc["programs"][0]["operator_bindings"][0]["stage_id"] = doc["programs"][0]["stage_ids"][1]
    doc["programs"][0]["operator_bindings"][1]["stage_id"] = doc["programs"][0]["stage_ids"][0]
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "identity_mismatch"


@pytest.mark.parametrize("field,value,reason", [
    ("dag_id", "f" * 64, "identity_mismatch"),
    ("plan_id", "e" * 64, "identity_mismatch"),
    ("artifact_manifest_ref", {"manifest_id": "other", "sha256": "ab" * 32}, "identity_mismatch"),
    ("action_id", "other-action", "identity_mismatch"),
])
def test_wrong_manifest_dag_plan_action_rejected(field, value, reason):
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    doc[field] = value
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == reason


def test_unsupported_slice_rejected():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    doc["supported_slice"] = "dynamic_elastic_v9"
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "unsupported_layout"


def test_multi_worker_group_rejected():
    fx = chain_fixture()
    pp = copy.deepcopy(fx.pp)
    pp["worker_groups"][0]["max_workers"] = 2
    ld, pp, bundle = _mutated(fx, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "unsupported_layout"


def test_multi_partition_stage_rejected():
    fx = chain_fixture()
    pp = copy.deepcopy(fx.pp)
    pp["stages"][1]["partitions"].append(partition_id_for(
        fx.operation_nodes[0]["logical_node_id"], 1, 1))
    ld, pp, bundle = _mutated(fx, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "unsupported_layout"


def test_driver_owns_operation_rejected():
    fx = chain_fixture()
    doc = copy.deepcopy(fx.doc)
    doc["driver_stage_ids"].append(doc["programs"][0]["stage_ids"][0])
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_channel_edge_double_coverage_rejected():
    fx = fanout_fixture()
    doc = copy.deepcopy(fx.doc)
    # cover the first edge twice (once in its own channel, once appended to another)
    doc["channel_options"][1]["logical_edge_ids"].append(
        doc["channel_options"][0]["logical_edge_ids"][0])
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_channel_edge_uncovered_rejected():
    fx = fanout_fixture()
    doc = copy.deepcopy(fx.doc)
    doc["channel_options"][0]["logical_edge_ids"] = []  # leave an edge uncovered
    ld, pp, bundle = _mutated(fx, doc=doc)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_channel_endpoint_partition_outside_rejected():
    fx = fanout_fixture()
    pp = copy.deepcopy(fx.pp)
    # shrink a channel's consumer partitions so the edge endpoint falls outside
    pp["channels"][1]["consumer_partitions"] = [pp["channels"][0]["consumer_partitions"][0]]
    ld, pp, bundle = _mutated(fx, pp=pp)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld, pp=pp, bundle=bundle)
    assert _reason(exc) == "invalid_contract"


def test_bad_bindings_bundle_shape_rejected():
    fx = chain_fixture()
    with pytest.raises(PlanContractError) as exc:
        interpret_physical_plan(fx.ld, fx.pp, bindings={"document": fx.doc}, manifest_ref=fx.mref)
    assert _reason(exc) == "invalid_contract"


def test_invalid_dag_shape_rejected():
    fx = chain_fixture()
    ld = copy.deepcopy(fx.ld)
    ld["nodes"] = ld["nodes"][:-1]  # break the v1 validator (terminal missing)
    with pytest.raises(PlanContractError) as exc:
        _interpret(fx, ld=ld)
    assert _reason(exc) == "invalid_contract"


# --- negative: assignment out-of-bounds ---

def _fanout_assignment_case():
    fx = fanout_fixture()
    layout = _interpret(fx)
    worker = next(w for w in layout.workers if w.group_id == "gB")
    inputs, outputs = expected_channel_ids(layout, worker)
    good = assignment_for(layout, worker, sorted(inputs), sorted(outputs))
    return layout, worker, good


def test_assignment_unknown_group_rejected():
    layout, worker, good = _fanout_assignment_case()
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, good, group_id="ghost")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_wrong_stages_rejected():
    layout, worker, good = _fanout_assignment_case()
    other = next(w for w in layout.workers if w.group_id == "gA")
    bad = {**good, "stages": list(other.stage_ids)}  # gA's stage, not gB's
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_wrong_partitions_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = {**good, "partitions": ["0" * 64]}
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_wrong_manifest_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = {**good, "artifact_manifest_ref": {"manifest_id": "other", "sha256": "ab" * 32}}
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_wrong_revision_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = {**good, "plan_revision": 7, "expected_plan_revision": 7}
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_extra_channel_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = copy.deepcopy(good)
    bad["channels"]["outputs"].append("0" * 64)
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_missing_channel_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = copy.deepcopy(good)
    bad["channels"]["inputs"] = []
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_logical_channel_id_rejected():
    layout, worker, good = _fanout_assignment_case()
    physical_input = good["channels"]["inputs"][0]["channel_id"]
    # the logical channel_id is not the physical pair id the layout expects
    logical = next(c.channel_id for c in layout.channels
                   if any(p["channel_id"] == physical_input
                          for p in materialize_channels(to_wire(c.channel_plan))))
    assert logical != physical_input
    bad = copy.deepcopy(good)
    bad["channels"]["inputs"] = [{"channel_id": logical}]
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_malformed_rejected():
    layout, worker, good = _fanout_assignment_case()
    bad = {**good, "ready_deadline_ms": 0}  # fails the frozen K3 validator
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, bad, group_id="gB")
    assert _reason(exc) == "invalid_assignment"


def test_assignment_non_layout_rejected():
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment({"not": "layout"}, {}, group_id="g0")
    assert _reason(exc) == "invalid_assignment"


# --- error contract ---

def test_plan_contract_error_reason_and_detail():
    err = PlanContractError("invalid_contract", "x" * 1000)
    assert err.reason == "invalid_contract"
    assert len(err.detail) == 512
    assert isinstance(err, RecoveryContractError)
    assert str(err) == err.detail


def test_plan_contract_error_unknown_reason_raises():
    with pytest.raises(ValueError):
        PlanContractError("not_a_reason", "detail")


def test_all_reasons_constructible_and_slices_fixed():
    reasons = {"missing_plan", "missing_bindings", "unsafe_artifact", "artifact_too_large",
               "corrupt_artifact", "identity_mismatch", "invalid_contract",
               "unsupported_layout", "invalid_assignment"}
    for reason in reasons:
        assert PlanContractError(reason, "d").reason == reason
    assert SUPPORTED_SLICES == {"chain_v1", "queue_transform_v1", "static_fanout_v1"}