"""Static multi-worker graph orchestration (U3/S3-S4, Local; exit gates 1-3).

The driver compiles a fixed stage->partition->worker placement, provisions one
managed Local worker per node partition through the real ResourceManager, wires
1:1 physical data channels between them (socketpair fds inherited via the existing
process-unit hook), runs the frozen K3 lifecycle per worker, then schedules global
cuts: each worker contributes its K6 fragment, the driver commits the whole group
once through GraphCommitTerminal (full-coverage CheckpointManifest) and acks with a
receipt that releases the workers. Business payloads flow worker->worker over the
data sockets and never transit the driver; the shared ancestor computes each record
once and replicates at egress (broadcast/fan_out).

S3 proves the multi-worker data plane + lifecycle (exit gate 1); S4 adds the single
CommitCoordinator commit, full K6 assembly, whole-graph restore and segment GC
(exit gates 2/3). chain.py (U2) and the dataset API stay untouched; this is selected
only by the opt-in ``unified_graph`` executor seam (D6).
"""
import logging
import select
import socket
import time
import uuid

from .block_sink import BlockSink
from .chain import ChainResultSink, stage_plan_artifact
from .commit import CommitCoordinator
from .contracts.checkpoint import replayable_effects, validate_checkpoint_manifest
from .contracts.envelope import WORKER_PROTOCOL_VERSION
from .contracts.errors import ProtocolError
from .contracts.worker import WorkerProtocolModel
from .effect_keys import merge_inflight, replayable_effect_refs
from .graph_wire import (
    CUT_CONTRIBUTION_SCHEMA, build_graph_checkpoint, validate_cut_contribution,
)
from .recovery_contract import RecoveryContractError, SourcePosition, identity
from .reservation import derive_prefetch
from .run_context import RunAction, current_action
from .source_snapshot import capture_source, freeze_source
from .state_artifacts import StateArtifacts
from .worker.bootstrap import recv_message, send_message
from ..planning.dag import build_channel_plan, _operation_semantics
from ..planning.graph_channels import RouteTable

logger = logging.getLogger(__name__)

_WORKER_CPU_MILLIS = 1000
_WORKER_MEMORY_BYTES = 1024 ** 3


def _sealed_operators(plan):
    # Must mirror verify_loaded_plan's recompute exactly (frozen dag semantics); each
    # graph worker runs a single-node LogicalPlan, so enumerate its operations directly.
    return [{"logical_node_id": identity([_operation_semantics(op)[0], index])}
            for index, op in enumerate(plan.operations)]


def _activation_order(ancestor_alloc, target_allocs, *, capacity):
    """Engine-side admission: downstream-first, control progress reserved.

    The static graph's minimum viable role combination is every worker, so a
    shortfall fails before provisioning instead of spawning a partial graph that
    can never drain. Parallelism reduction/elasticity is the scaling step's job.
    """
    from .admission import RoleDemand, plan_activation
    demands = [RoleDemand(alloc, "consume", _WORKER_CPU_MILLIS, _WORKER_MEMORY_BYTES,
                          fairness_key=rid) for rid, alloc in target_allocs.items()]
    demands.append(RoleDemand(ancestor_alloc, "produce", _WORKER_CPU_MILLIS, _WORKER_MEMORY_BYTES))
    # Driver commit/lease/cancel progress runs outside the worker pool, so the
    # control reserve here is zero; the admission module enforces nonzero reserves
    # for callers whose control role shares the domain.
    plan = plan_activation(demands, capacity=capacity)
    if plan["shortfall"]:
        raise RecoveryContractError(
            "Graph role minimum exceeds worker capacity; shortfall: " + ",".join(plan["shortfall"]))
    return plan["admitted"]


class GraphLocalConnector:
    """Local control connector that also inherits data-channel socket fds.

    Wraps the U2 LocalControlConnector control socketpair and adds the child-end
    data descriptors the orchestrator pre-wired per allocation, so the managed
    process unit passes them through its existing extra_pass_fds hook.
    """

    def __init__(self):
        from .worker.bootstrap import LocalControlConnector
        self._control = LocalControlConnector()
        self._data_fds = {}
        self.driver_socks = self._control.driver_socks

    def stage(self, allocation_id, launch_spec, data_fds):
        self._control.stage(allocation_id, launch_spec)
        self._data_fds[allocation_id] = tuple(data_fds)

    def spawn_payload(self, allocation_id):
        payload = self._control.spawn_payload(allocation_id)
        data_fds = self._data_fds.get(allocation_id, ())
        payload["extra_pass_fds"] = (*payload["extra_pass_fds"], *data_fds)
        return payload

    def spawned(self, allocation_id):
        return self._control.spawned(allocation_id)

    def abort(self, allocation_id):
        self._data_fds.pop(allocation_id, None)
        self._control.abort(allocation_id)

    def close(self, allocation_id):
        self._data_fds.pop(allocation_id, None)
        self._control.close(allocation_id)


def _build_manifest(run, plan_refs):
    from .contracts.artifact import build_manifest, save_manifest, seal_manifest
    config = run.journal.recovery_identity()
    bundle = identity([ref["sha256"] for ref in plan_refs])
    manifest = build_manifest(
        run_id=run.ref.run_id,
        platform={"source_digest": config["platform_revision"], "distribution": "demiflow",
                  "version": "u3-graph", "runtime_identity_ref": identity([run.ref.run_id, "runtime"])},
        business={"bundle_digest": "sha256:" + bundle,
                  "entrypoints": [{"name": "graph", "module_key": "p_" + bundle[:24],
                                   "definition_ref": {"sha256": bundle}}]},
        config={"resolved_config_digest": identity([run.ref.run_id, "config"]), "model_config_refs": []},
        environment={"requirements_sha256": identity(["requirements"]), "lock_digest": identity(["lock"]),
                     "resolution_digest": identity(["resolution"])},
        protocol_versions={"worker": {"major": 1, "minor": 0}, "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1, "resource_protocol": "1.0"},
        secret_refs=[])
    manifest = seal_manifest(manifest, int(config["started_at"] * 1000))
    ref = save_manifest(run.directory, manifest)
    return {"manifest_id": ref["manifest_id"], "sha256": ref["sha256"]}


def compile_graph_topology(action, source, ancestor, targets, *,
                           manifest_ref, host_id="local"):
    """Compile the full-graph DAG/physical plan and seal F1 + bindings (C).

    Uses ``graph_dag.compile_graph_logical_dag`` to compose per-program linear
    fragments into one DAG, then ``compile_physical_execution_plan`` for the
    worker-group placement.  Seals F1 facts, execution bindings (one program per
    worker group) through B's single-authority binder, and the identity bridge.
    """
    from ..planning.dag import channel_id_for, compile_physical_execution_plan
    from ..planning.graph_dag import compile_graph_logical_dag
    from .chain import seal_plan_facts, stage_plan_artifact
    from .exchange import read_domain
    from .sealed_plans import seal_execution_bindings

    run = action.context
    logical_dag, programs = compile_graph_logical_dag(
        action_id=action.action_id, source=source, ancestor=ancestor, targets=targets)
    domain = read_domain(run.directory, host_id)
    profile_doc = {"requests": {"cpu_millis": _WORKER_CPU_MILLIS,
                                "memory_bytes": _WORKER_MEMORY_BYTES}}
    profile_ref = identity(profile_doc)
    dag_id = logical_dag["dag_id"]
    worker_groups = []
    stage_budgets = {}
    for prog in programs:
        stage_refs = [identity([dag_id, op["occurrence_id"]])
                      for op in prog["operators"]]
        worker_groups.append({
            "group_id": prog["group_id"], "role": "cpu_prepare",
            "profile_ref": profile_ref, "stage_refs": stage_refs,
            "min_workers": 1, "initial_workers": 1, "max_workers": 1})
        for op in prog["operators"]:
            stage_budgets[op["occurrence_id"]] = {
                "inflight_rows": 1024, "inflight_bytes": 8 * 1024 ** 2,
                "output_bytes": 1 * 1024 ** 2}
    physical = compile_physical_execution_plan(
        logical_dag=logical_dag, plan_revision=0, artifact_manifest_ref=manifest_ref,
        worker_groups=worker_groups,
        recovery={"checkpoint_interval_ms": run.limits["checkpoint_interval_ms"],
                  "unconfirmed_rows": run.limits["checkpoint_unconfirmed_rows"],
                  "unconfirmed_bytes": run.limits["checkpoint_unconfirmed_bytes"],
                  "terminals": []},
        feasibility_snapshot_ref=identity([run.ref.run_id, "feasibility"]),
        read_domain=domain, durability_declared="durable",
        partition_count=1, scheme_kind="single", stage_budgets=stage_budgets)
    seal_plan_facts(run, action, logical_dag, physical, manifest_ref,
                    int(run.journal.recovery_identity()["started_at"] * 1000))

    plans_by_group = {"ancestor": ancestor["plan"]}
    for target in targets:
        plans_by_group["target-" + target["reader_id"]] = target["plan"]
    binding_programs = []
    for prog in programs:
        plan = plans_by_group[prog["group_id"]]
        plan_ref = stage_plan_artifact(action, plan)
        op_stage_ids = [identity([dag_id, op["occurrence_id"]])
                        for op in prog["operators"]]
        binding_programs.append({
            "group_id": prog["group_id"], "stage_ids": op_stage_ids,
            "plan_ref": plan_ref,
            "sealed_operators": [{"logical_node_id": op["logical_node_id"],
                                  "occurrence_id": op["occurrence_id"]}
                                 for op in prog["operators"]],
            "operator_bindings": [{"program_index": i, "stage_id": sid}
                                  for i, sid in enumerate(op_stage_ids)]})
    nodes = logical_dag["nodes"]
    source_nodes = [n for n in nodes if not n["input_ports"]]
    terminal_nodes = [n for n in nodes if not n["output_ports"]]
    driver_stage_ids = [identity([dag_id, n["occurrence_id"]])
                        for n in source_nodes + terminal_nodes]
    src = logical_dag["sources"][0]
    source_bindings = [{"stage_id": identity([dag_id, n["occurrence_id"]]),
                        "kind": "fixed_source",
                        "ref": {"source_id": src["source_id"],
                                "source_identity_ref": src["source_identity_ref"]}}
                       for n in source_nodes]
    scheme_version = logical_dag["sources"][0]["partition_scheme"]["scheme_version"]
    channel_options = [{"channel_id": channel_id_for(e["edge_id"], scheme_version),
                        "logical_edge_ids": [e["edge_id"]]} for e in logical_dag["edges"]]
    bindings_ref = seal_execution_bindings(
        run, action, logical_dag=logical_dag, physical_plan=physical,
        profiles={profile_ref: profile_doc}, programs=binding_programs,
        driver_stage_ids=driver_stage_ids, source_bindings=source_bindings,
        channel_options=channel_options,
        checkpoint_rows=run.limits["checkpoint_rows"], supported_slice="static_fanout_v1")

    from .checked import current
    checked = current()
    bridge_ref = None
    if checked is not None and checked.report_digest:
        from .identity_bridge import build_identity_bridge, seal_identity_bridge
        bridge_bindings = []
        seen_report_keys = set()
        for prog in programs:
            plan = plans_by_group[prog["group_id"]]
            resolved = checked.execution_bindings(source, plan, action_id=action.action_id)
            for item in resolved:
                report_key = (item["report_node_id"], item["report_occurrence"])
                if report_key in seen_report_keys:
                    continue
                if item["role"] == "source":
                    node = nodes[0]
                elif item["role"] == "operation":
                    node = prog["operators"][item["program_index"]]
                else:
                    continue
                seen_report_keys.add(report_key)
                bridge_bindings.append({
                    "report_node_id": item["report_node_id"],
                    "report_occurrence": item["report_occurrence"],
                    "logical_node_id": node["logical_node_id"],
                    "occurrence_id": node["occurrence_id"],
                    "mode": item["mode"]})
        if bridge_bindings:
            bridge = build_identity_bridge(logical_dag, action_id=action.action_id,
                                           report_digest=checked.report_digest,
                                           bindings=bridge_bindings)
            bridge_ref = seal_identity_bridge(run, action, bridge)

    return {"logical_dag": logical_dag, "physical": physical, "programs": programs,
            "bindings_ref": bindings_ref, "identity_bridge_ref": bridge_ref}


def _worker_assignment_channels(layout, placement):
    """Cross-worker physical channel ids for a K3 assignment (mirrors B's ownership)."""
    from ..planning.graph_channels import materialize_channels
    from .plan_interpreter import to_wire
    group_partitions = set(placement.partition_ids)
    inputs, outputs = [], []
    for channel in layout.channels:
        if channel.in_process:
            continue
        producer_kinds = {layout.stage_owners[sid]["kind"] for sid in channel.producer_stage_ids}
        consumer_kinds = {layout.stage_owners[sid]["kind"] for sid in channel.consumer_stage_ids}
        if "driver" in producer_kinds or "driver" in consumer_kinds:
            continue
        for physical in materialize_channels(to_wire(channel.channel_plan)):
            cid = physical["channel_id"]
            producer_here = physical["producer_partition"] in group_partitions
            consumer_here = physical["consumer_partition"] in group_partitions
            if consumer_here and not producer_here:
                inputs.append({"channel_id": cid})
            if producer_here and not consumer_here:
                outputs.append(cid)
    return {"inputs": inputs, "outputs": outputs}


class _WorkerLink:
    """Driver side of one worker control socket: lifecycle + contribution intake."""

    def __init__(self, sock, model, allocation_id, now_ms):
        self.sock = sock
        self.model = model
        self.allocation_id = allocation_id
        self._now_ms = now_ms
        self.contributions = {}
        self.pending_ops = {}
        self.final_seen = False
        self.final_at = None
        self.command_ops = {}

    def command(self, kind, payload):
        op = uuid.uuid4().hex
        send_message(self.sock, {
            "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
            "scope_id": self.model.scope_id, "run_id": self.model.run_id, "operation_id": op,
            "controller_epoch": self.model.controller_epoch, "authority_ref": self.model.grant["grant_id"],
            "allocation_id": self.allocation_id, "boot_id": self.model.boot_id, "payload": payload})
        return op

    def _handle_event(self, message):
        if message.get("schema_version") == CUT_CONTRIBUTION_SCHEMA or message.get("kind") == "CutContribution":
            contribution = validate_cut_contribution(message["contribution"])
            checkpoint_id = contribution["checkpoint_id"]
            # Hold the contribution until the driver's single durable commit for the
            # whole cut group; the receipt (send_receipt) is the ack that releases the
            # worker. Acking here would publish before the group commit (S4).
            self.contributions[checkpoint_id] = contribution
            self.pending_ops[checkpoint_id] = message["operation_id"]
            if contribution["final"] and not self.final_seen:
                self.final_seen = True
                self.final_at = time.monotonic()
            return True
        if message.get("kind") in ("Register", "Ready", "Heartbeat"):
            self.model.handle(message, now_ms=self._now_ms())
            return True
        return False

    def send_graph_checkpoint(self, command):
        send_message(self.sock, command)

    def send_receipt(self, checkpoint_id, receipt):
        operation_id = self.pending_ops.pop(checkpoint_id)
        send_message(self.sock, {"response_to": operation_id, "receipt": receipt})

    def read_response(self, op):
        while True:
            message = recv_message(self.sock)
            if message is None:
                raise RecoveryContractError("Worker closed before responding")
            if message.get("response_to") == op:
                if "error" in message:
                    raise RecoveryContractError("Worker command failed: " + str(message["error"]))
                return message["receipt"]
            self._handle_event(message)

    def pump(self):
        message = recv_message(self.sock)
        if message is None:
            return False
        if not self._handle_event(message):
            response_to = message.get("response_to")
            if response_to is not None:
                self.command_ops[response_to] = message
        return True


class GraphCommitTerminal:
    """The driver's single result-commit authority for the whole graph (S4).

    One ``CommitCoordinator.commit`` per cut group publishes every target's staging
    descriptor verbatim (``ChainResultSink`` per target child root, no re-encode),
    then records a full-coverage K6 CheckpointManifest: the ancestor's confirmed
    source ordinal, every physical channel cut (consumer offsets are authoritative
    for 1:1 channels), all operator participants and every target sink receipt. The
    workers never own commit authority; the receipt is their ack.
    """

    def __init__(self, action, *, source_id, ancestor, targets, target_children,
                 physical, manifest_ref, result_child_id, target_allocs, ancestor_alloc):
        self.run = action.context
        self.source_id = source_id
        self.reader_ids = [target["reader_id"] for target in targets]
        self.target_allocs = dict(target_allocs)
        self.ancestor_alloc = ancestor_alloc
        self.channel_by_reader = {rid: physical[rid]["channel_id"] for rid in self.reader_ids}
        self.channel_ids = [self.channel_by_reader[rid] for rid in self.reader_ids]
        self.manifest_ref = manifest_ref
        self.operators = [dict(logical_node_id=ancestor["logical_node_id"],
                               occurrence_id=ancestor["occurrence_id"])]
        self.operators += [dict(logical_node_id=target["logical_node_id"],
                                occurrence_id=target["occurrence_id"]) for target in targets]
        declaration = dict(kind="graph-result", source=action.declaration["source"],
                           parent=action.semantic_id, protocol="graph-cut-v1")
        self.run.journal.bind_execution_action(self.run.fence, result_child_id, declaration)
        self.child = RunAction(self.run, result_child_id, declaration)
        self.coordinator = CommitCoordinator(self.child)
        self.sinks = {}
        for target in targets:
            rid = target["reader_id"]
            child_decl = dict(kind="graph-node", source=action.declaration["source"],
                              parent=action.semantic_id, protocol="graph-cut-v1")
            child_action = RunAction(self.run, target_children[rid], child_decl)
            self.sinks[rid] = ChainResultSink(child_action, rid)
        self.sequence = self.inputs = 0
        self.rows = {rid: 0 for rid in self.reader_ids}
        self.checkpoints = {}
        facts = self.coordinator.recover(self.sinks)
        latest = facts[-1] if facts else None
        if latest is not None and latest["status"] != "pending":
            position = latest["intent"]["position"]
            if position["source_id"] != self.source_id:
                raise RecoveryContractError("Graph source progress differs")
            self.inputs = position["next_ordinal"]
            self.sequence = latest["sequence"]
            summary = StateArtifacts(self.run).read(latest["prepared"]["state"]["graph-progress"])
            self.rows = dict(summary["target_rows"])
        self.complete = self.child.record["result"] is not None

    def restore_point(self):
        latest = self.run.journal.latest_execution_commit(self.child.action_id, confirmed=True)
        if latest is None or latest["status"] == "pending":
            return None
        summary = StateArtifacts(self.run).read(latest["prepared"]["state"]["graph-progress"])
        return {"position": latest["intent"]["position"], "sequence": latest["sequence"],
                "checkpoint_id": latest["commit_id"], "summary": summary,
                "replayable_effects": replayable_effect_refs(summary["inflight"]),
                "state": latest["prepared"]["state"]}

    def commit_group(self, checkpoint_id, group, *, ordinal, final, epoch):
        outputs = {}
        channel_cuts = []
        for rid in self.reader_ids:
            contribution = group[self.target_allocs[rid]]
            descriptor = contribution["output_descriptor"]
            if descriptor is None:
                raise RecoveryContractError("Graph target cut lacks a staging descriptor")
            outputs[rid] = (self.sinks[rid], descriptor)
            channel_id = self.channel_by_reader[rid]
            cut = next((c for c in contribution["channel_cuts"] if c["channel_id"] == channel_id), None)
            if cut is None:
                raise RecoveryContractError("Graph target cut lacks its channel cut")
            channel_cuts.append(cut)
        # Full K6 effect coverage: union every participant's in-flight external
        # effects (the worker journals are the one durable authority). The merged
        # description rides the recoverable graph-progress summary AND the manifest
        # so Restore can reuse known_completed refs and fail-close unknown ones.
        merged_inflight = merge_inflight(group.values())
        # Each worker persisted its stage snapshots under its occurrence key and
        # rode them in the contribution's operator_states (K6 state_ref). Read the
        # values back and commit them through the one authority so prepared.state
        # durably holds them for Restore; the manifest refs bind to that commit.
        # The ancestor's egress ordinal rides the artifact VALUE under a reserved
        # key: extract it into the driver-side summary (not the K6 contribution) and
        # strip it from the committed stage-state so OperatorStates local-key restore
        # never sees it (separation hard condition).
        operator_states = []
        actor_states = {}
        egress_ordinal = 0
        for contribution in group.values():
            for entry in contribution["operator_states"]:
                occ = entry["occurrence_id"]
                value = StateArtifacts(self.run).read(entry["state_ref"])
                if "__egress_ordinal__" in value:
                    egress_ordinal = value.pop("__egress_ordinal__")
                actor_states[occ] = value
                operator_states.append(entry)
        summary = {"source_ordinal": ordinal, "sequence": self.sequence + 1,
                   "target_rows": {rid: self.rows[rid] + outputs[rid][1]["rows"] for rid in self.reader_ids},
                   "final": bool(final), "inflight": merged_inflight,
                   "egress_ordinal": egress_ordinal}
        fact = self.coordinator.commit(self.sequence + 1, outputs=outputs,
            position=SourcePosition(self.source_id, ordinal),
            states={"graph-progress": summary, **actor_states})
        manifest_states = [dict(entry, state_ref=fact["prepared"]["state"][entry["occurrence_id"]])
                           for entry in operator_states]
        sink_receipts = [{"sink_id": rid, "commit_id": fact["commit_id"], "receipt_ref": fact["commit_id"]}
                         for rid in self.reader_ids]
        manifest = {
            "schema_version": "demiflow.checkpoint_manifest.v1",
            "checkpoint_id": checkpoint_id, "run_id": self.run.ref.run_id,
            "action_id": self.child.action_id, "epoch": epoch,
            "topology_revision": 0, "route_epoch": 0, "plan_revision": 0,
            "artifact_manifest_ref": self.manifest_ref,
            "participants": {"sources": [{"source_id": self.source_id, "partition_id": "p0"}],
                             "channels": self.channel_ids, "operators": self.operators,
                             "sinks": self.reader_ids},
            "source_progress": [{"source_id": self.source_id, "partition_id": "p0",
                                 "position": {"next_ordinal": ordinal}}],
            "channel_cuts": channel_cuts, "operator_states": manifest_states, "inflight": merged_inflight,
            "sink_receipts": sink_receipts, "seal": {"sealed": bool(final)},
        }
        if final:
            manifest["seal"]["digest"] = identity({k: v for k, v in manifest.items() if k != "seal"})
        manifest = validate_checkpoint_manifest(manifest)
        # Production wiring of the frozen description-level rule: only
        # known_completed effects carry a reusable ref; unknown never auto-replays.
        replayable = replayable_effects(manifest)
        ref = StateArtifacts(self.run).put(checkpoint_id, manifest)
        self.checkpoints[checkpoint_id] = {"manifest": manifest, "ref": ref,
                                           "replayable_effects": replayable}
        self.sequence = fact["sequence"]
        self.inputs = ordinal
        self.rows = dict(summary["target_rows"])
        return fact, manifest

    def finish(self, source_rows):
        if self.inputs != source_rows:
            raise RecoveryContractError("Unconfirmed source at graph completion")
        self.child.complete(dict(kind="graph-materialized", inputs=self.inputs,
                                 target_rows=self.rows, commits=self.sequence))
        self.complete = True


def run_local_graph(graph_spec, *, host_id="local", resource_root=None, control_timeout_s=120):
    """Run a static ancestor->targets graph in independent Local worker hosts.

    graph_spec: {source, source_id, ancestor{plan,logical_node_id,occurrence_id},
    targets[{plan,logical_node_id,occurrence_id,reader_id}], routing, exchange_mode,
    credit_budget, cut_rows}. Returns {target_rows, contributions, egress_rows,
    workers}. The driver schedules global cuts, collects every worker's K6 fragment,
    commits the group once through GraphCommitTerminal and acks with a receipt.
    """
    from ..data.plan import LogicalPlan
    from ..resources.capacity import LocalCapacityPool
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.local import LocalResourceProvider
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.process_unit import ManagedProcessUnit
    from .resource_state import RunResourceState
    from .exchange import read_domain
    from pathlib import Path

    action = current_action()
    if action is None:
        raise RuntimeError("Graph materialization requires the native action context")
    run = action.context
    source = graph_spec["source"]
    source_id = graph_spec["source_id"]
    ancestor = graph_spec["ancestor"]
    targets = graph_spec["targets"]
    from .operator_precheck import precheck_plan_state
    precheck_plan_state(ancestor["plan"])
    for target in targets:
        precheck_plan_state(target["plan"])
    routing = graph_spec.get("routing", "broadcast")
    key_fields = graph_spec.get("key_fields")
    target_classes = graph_spec.get("target_classes")
    target_field = graph_spec.get("target_field")
    exchange_mode = graph_spec.get("exchange_mode", "memory_incremental")
    budget = graph_spec.get("credit_budget", {"rows": 64, "bytes": 8 * 1024 ** 2})
    # Prefetch bounds derive from the admitted inflight budget only when one is
    # configured; the default path injects no key and stays byte-identical (U3).
    prefetch_slots = graph_spec.get("prefetch_slots")
    prefetch = (derive_prefetch(prefetch_slots, max_pending_floor=budget["rows"])
                if prefetch_slots is not None else None)
    queue_depth = graph_spec.get("queue_depth", 4096)
    if prefetch is not None:
        queue_depth = min(queue_depth, prefetch["queue_depth"])

    from .executors.local import LocalDatasetExecutor
    capture_executor = LocalDatasetExecutor(workers=1)
    selected = freeze_source(action, source)
    source_sink, source_reference = capture_source(
        action, lambda: capture_executor.iter_rows(selected, LogicalPlan()))
    ancestor_plan_ref = stage_plan_artifact(action, ancestor["plan"])
    target_plan_refs = [stage_plan_artifact(action, target["plan"]) for target in targets]
    ancestor_sealed = _sealed_operators(ancestor["plan"])
    target_sealed = [_sealed_operators(target["plan"]) for target in targets]
    manifest_ref = _build_manifest(run, [ancestor_plan_ref, *target_plan_refs])
    graph_topology = compile_graph_topology(
        action, selected, ancestor, targets, manifest_ref=manifest_ref, host_id=host_id)
    domain = read_domain(run.directory, host_id)

    reader_ids = [target["reader_id"] for target in targets]
    channel_plan = build_channel_plan(
        edge_id=identity([action.action_id, "graph-egress"]), scheme_version=1,
        producer_partitions=["p0"], consumer_partitions=reader_ids,
        exchange_mode=exchange_mode, routing=routing, read_domain=domain,
        credit_budget=budget,
        durability_declared="durable" if exchange_mode == "durable_incremental" else "ephemeral",
        seal_scope=({"scope_id": identity([action.action_id, "seal"]), "membership": ["p0"], "generation": 1}
                    if exchange_mode == "finite_sealed" else None))
    table = RouteTable(channel_plan, key_fields=key_fields, target_classes=target_classes)
    physical = {item["consumer_partition"]: item for item in table.physical_channels}

    # Pre-wire one socketpair per physical channel; ancestor owns the producer end,
    # each target owns its consumer end. The driver keeps neither data end.
    data_pairs = {}
    for reader_id in reader_ids:
        producer_sock, consumer_sock = socket.socketpair()
        data_pairs[reader_id] = (producer_sock, consumer_sock, physical[reader_id]["channel_id"])

    ids = {k: uuid.uuid4().hex for k in ("scope_id", "boot_id", "grant_id")}
    scope_id = ids["scope_id"]
    ancestor_alloc = uuid.uuid4().hex
    target_allocs = {target["reader_id"]: uuid.uuid4().hex for target in targets}
    all_allocs = [ancestor_alloc, *target_allocs.values()]

    def grant_for(allocation_id, boot_id):
        return {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
                "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
                "boot_id": boot_id, "controller_epoch": run.attempt,
                "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}

    boot_ids = {alloc: uuid.uuid4().hex for alloc in all_allocs}
    capabilities = ["cpu-map", "stream", "graph"]

    def base_spec(allocation_id, output_child_id):
        return {"directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
                "allocation_id": allocation_id, "boot_id": boot_ids[allocation_id],
                "controller_epoch": run.attempt, "capabilities": capabilities,
                "grant": grant_for(allocation_id, boot_ids[allocation_id]),
                "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"]}

    # Bind every worker child action read-only before spawn (WorkerAuthority requires it).
    ancestor_output_child = action.action_id + "-graph-ancestor"
    # capture_source already bound and completed the fixed-source child; the
    # ancestor reads it read-only, so it reuses that exact id rather than re-binding.
    ancestor_source_child = action.action_id + "-source"
    decl = dict(kind="graph-node", source=action.declaration["source"], parent=action.semantic_id,
                protocol="graph-cut-v1")
    run.journal.bind_execution_action(run.fence, ancestor_output_child, decl)
    target_children = {}
    for target in targets:
        child_id = action.action_id + "-graph-" + target["reader_id"]
        decl = dict(kind="graph-node", source=action.declaration["source"], parent=action.semantic_id,
                    protocol="graph-cut-v1")
        run.journal.bind_execution_action(run.fence, child_id, decl)
        target_children[target["reader_id"]] = child_id

    # The driver commit authority reconciles any prepared cut before re-provisioning
    # (D5 whole-graph restore). A completed graph short-circuits re-entry; otherwise
    # every worker resumes from the latest acked cut (ancestor skips the acked source
    # prefix and re-sends post-cut data; targets stage only post-cut output).
    result_child_id = action.action_id + "-graph-result"
    terminal = GraphCommitTerminal(
        action, source_id=source_id, ancestor=ancestor, targets=targets,
        target_children=target_children, physical=physical, manifest_ref=manifest_ref,
        result_child_id=result_child_id, target_allocs=target_allocs,
        ancestor_alloc=ancestor_alloc)
    source_rows = source_reference["data"]["rows"]
    if terminal.complete:
        for pair in data_pairs.values():
            pair[0].close()
            pair[1].close()
        return {"target_rows": dict(terminal.rows), "contributions": {rid: [] for rid in reader_ids},
                "egress_rows": terminal.inputs, "workers": 0, "ancestor_contributions": [],
                "final_arrival": {}, "checkpoints": dict(terminal.checkpoints)}
    from .plan_interpreter import interpret_physical_plan, to_wire
    from .sealed_plans import load_execution_bindings, load_sealed_plan
    _sealed_plan = load_sealed_plan(run.directory, action.action_id)
    _bindings = load_execution_bindings(run.directory, action.action_id,
                                        sealed_plan=_sealed_plan)
    _layout = interpret_physical_plan(
        _sealed_plan["logical_dag"], _sealed_plan["physical_plan"],
        bindings=_bindings, manifest_ref=_sealed_plan["facts"]["artifact_manifest_ref"])
    _placements = {w.group_id: w for w in _layout.workers}
    _alloc_group = {ancestor_alloc: "ancestor"}
    _alloc_group.update({target_allocs[t["reader_id"]]: "target-" + t["reader_id"] for t in targets})
    _sealed_profiles = {alloc: ResourceProfile(**to_wire(_placements[gid].profile))
                        for alloc, gid in _alloc_group.items()}
    _bindings_ref = _bindings["ref"]
    restore = terminal.restore_point()
    restore_ordinal = restore["position"]["next_ordinal"] if restore else 0

    def worker_state(occurrence_id):
        # Each worker's combined stage-state artifact is keyed by its occurrence;
        # hand back only that ref so the worker restore never sees sibling keys
        # or the graph-progress summary. Pure-function stages have no artifact.
        if restore is None:
            return {}
        ref = restore["state"].get(occurrence_id)
        return {occurrence_id: ref} if ref is not None else {}

    ancestor_restore = ({"inputs": restore_ordinal, "sequence": terminal.sequence, "rows": 0,
                         "state": worker_state(ancestor["occurrence_id"]),
                         "egress_ordinal": restore["summary"].get("egress_ordinal", 0)}
                        if restore else None)

    ancestor_spec = base_spec(ancestor_alloc, ancestor_output_child)
    ancestor_spec["graph"] = {
        "allocation_id": ancestor_alloc, "output_child_id": ancestor_output_child,
        "cut_namespace": action.action_id,
        "source_child_id": ancestor_source_child, "plan_ref": ancestor_plan_ref,
        "sealed_operators": ancestor_sealed,
        "source_reference": source_reference, "source_id": source_id, "host_id": host_id,
        "logical_node_id": ancestor["logical_node_id"], "occurrence_id": ancestor["occurrence_id"],
        "route_plan": channel_plan, "queue_depth": queue_depth,
        "key_fields": key_fields, "target_classes": target_classes, "target_field": target_field,
        "restore": ancestor_restore,
        "output_channels": [{"channel_id": data_pairs[rid][2], "fd": data_pairs[rid][0].fileno(),
                             "reader_id": rid, "exchange_mode": exchange_mode, "credit_budget": budget,
                             "storage_bytes": graph_spec.get("storage_bytes", budget["bytes"]),
                             "route_epoch": 0} for rid in reader_ids]}
    ancestor_spec["execution_plan"] = {"action_id": action.action_id, "group_id": "ancestor",
                                       "bindings_ref": _bindings_ref}
    ancestor_data_fds = [data_pairs[rid][0].fileno() for rid in reader_ids]

    target_specs = {}
    target_data_fds = {}
    for target in targets:
        rid = target["reader_id"]
        alloc = target_allocs[rid]
        spec = base_spec(alloc, target_children[rid])
        spec["graph"] = {
            "allocation_id": alloc, "output_child_id": target_children[rid],
            "plan_ref": target_plan_refs[targets.index(target)], "source_id": source_id,
            "sealed_operators": target_sealed[targets.index(target)],
            "host_id": host_id, "logical_node_id": target["logical_node_id"],
            "occurrence_id": target["occurrence_id"], "sink_id": rid,
            "restore": ({"inputs": 0, "sequence": terminal.sequence, "rows": 0,
                         "state": worker_state(target["occurrence_id"])} if restore else None),
            "input_channels": [{"channel_id": data_pairs[rid][2], "fd": data_pairs[rid][1].fileno(),
                                "reader_id": rid, "exchange_mode": exchange_mode, "credit_budget": budget,
                                "route_epoch": 0}]}
        spec["execution_plan"] = {"action_id": action.action_id, "group_id": "target-" + rid,
                                  "bindings_ref": _bindings_ref}
        target_specs[rid] = spec
        target_data_fds[rid] = [data_pairs[rid][1].fileno()]

    if prefetch is not None:
        ancestor_spec["graph"]["prefetch"] = prefetch
        for spec in target_specs.values():
            spec["graph"]["prefetch"] = prefetch

    root = run.directory.with_name(run.directory.name + "-resources") if resource_root is None \
        else Path(resource_root)
    pool = LocalCapacityPool(root / "capacity", capacity={"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3})
    all_allocs = _activation_order(ancestor_alloc, target_allocs, capacity=pool.capacity)
    unit = ManagedProcessUnit(root / "units")
    connector = GraphLocalConnector()
    provider = LocalResourceProvider(pool, unit, control=connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(ancestor_alloc, ancestor_spec, ancestor_data_fds)
    for rid in reader_ids:
        connector.stage(target_allocs[rid], target_specs[rid], target_data_fds[rid])
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "local"}, run.fence,
        {"cpu_millis": 32000, "memory_bytes": 32 * 1024 ** 3, "max_workers": 16}))

    def request_for(allocation_id):
        launch = WorkerLaunchSpec(
            artifact_refs={"graph": {"digest": manifest_ref["sha256"], "location": "file://" + str(run.directory)}},
            entrypoint="demiflow-graph-worker",
            argv=(__import__("sys").executable, "-B", "-m", "demiflow.execution.worker.graph_host"),
            bootstrap_ref="bootstrap-ref", access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
        return WorkerRequest(allocation_id, allocation_id,
                             _sealed_profiles[allocation_id],
                             launch, provision_deadline_ms=60000)

    requests = tuple(request_for(alloc) for alloc in all_allocs)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence, uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, requests, ()))

    links = {}
    socks = []
    drained = False
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 60, 32)
        snapshot = manager.get(scope_id)
        phases = {a["allocation_id"]: a["phase"] for a in snapshot["allocations"]}
        for alloc in all_allocs:
            if phases.get(alloc) != "RUNNING":
                raise RecoveryContractError("Graph worker did not reach RUNNING: " + str(phases.get(alloc)))
        # Workers inherited the data fds; the driver releases its copies now.
        for rid in reader_ids:
            data_pairs[rid][0].close()
            data_pairs[rid][1].close()

        def now_ms():
            return int(time.time() * 1000)

        for alloc in all_allocs:
            sock = connector.driver_socks[alloc]
            sock.settimeout(control_timeout_s)
            socks.append(sock)
            model = WorkerProtocolModel(
                scope_id=scope_id, run_id=run.ref.run_id, allocation_id=alloc,
                boot_id=boot_ids[alloc], controller_epoch=run.attempt,
                grant=grant_for(alloc, boot_ids[alloc]), plan_revision=0, capabilities=capabilities)
            links[alloc] = _WorkerLink(sock, model, alloc, now_ms)

        # Lifecycle per worker: Register -> Verify -> Restore -> Activate -> Assign.
        for alloc in all_allocs:
            link = links[alloc]
            register = recv_message(link.sock)
            if register is None or register.get("kind") != "Register":
                raise RecoveryContractError("Graph worker did not register")
            link.model.handle(register, now_ms=now_ms())
            link.read_response(link.command("VerifyArtifactAndCapabilities",
                                            {"artifact_manifest_ref": manifest_ref,
                                             "required_capabilities": capabilities}))
            assignment_id = uuid.uuid4().hex
            link.read_response(link.command("Restore", {"assignment_ref": assignment_id,
                                                        "restore_point": None, "state_refs": []}))
            link.read_response(link.command("Activate", {"grant_ref": ids["grant_id"],
                                                         "controller_epoch": run.attempt,
                                                         "operation_id": uuid.uuid4().hex}))
            occurrence = (ancestor["occurrence_id"] if alloc == ancestor_alloc
                          else next(t["occurrence_id"] for t in targets if target_allocs[t["reader_id"]] == alloc))
            assignment = {
                "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
                "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": alloc,
                "boot_id": boot_ids[alloc], "worker_id": alloc, "controller_epoch": run.attempt,
                "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
                "artifact_manifest_ref": manifest_ref,
                "stages": list(_placements[_alloc_group[alloc]].stage_ids),
                "partitions": list(_placements[_alloc_group[alloc]].partition_ids),
                "channels": _worker_assignment_channels(_layout, _placements[_alloc_group[alloc]]),
                "restore_point": None,
                "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
            link.read_response(link.command("Assign", {"assignment": assignment}))

        # Driver cut schedule: one global cut per interval, the last at source EOF.
        # Each cut names a checkpoint, every worker contributes its K6 fragment, the
        # driver commits the whole group once and only then acks (releases the workers).
        # On restore the schedule continues strictly after the latest acked ordinal.
        cut_rows = max(1, int(graph_spec.get("cut_rows", source_rows or 1)))
        bounds = [b for b in range(cut_rows, source_rows, cut_rows) if b > restore_ordinal]
        # The final cut seals the graph and must run even for an empty source (bound
        # 0); skip it only when a prior attempt already acked up to source_rows.
        if restore is None or restore_ordinal < source_rows:
            bounds.append(source_rows)
        contributions = {rid: [] for rid in reader_ids}
        ancestor_contributions = []
        epoch = run.attempt
        # EOF-driven finality: the driver commits final only when the ancestor's actual
        # confirmed ordinal reaches source_rows. A budget-forced early cut (or full)
        # undershoots the requested bound, so the driver retries that bound until the
        # ancestor reaches EOF; every committed cut advances the ordinal (no zero-progress
        # loop). command["final"] is only a provisional hint, never the seal authority.
        done = False
        for bound in bounds:
            if done:
                break
            while True:
                hint_final = bound == source_rows
                checkpoint_id = identity([action.action_id, "graph-cut", bound])
                command = build_graph_checkpoint(checkpoint_id, epoch=epoch,
                                                 producer_sequence_bound=bound, final=hint_final)
                for alloc in all_allocs:
                    links[alloc].send_graph_checkpoint(command)
                group = {}
                deadline = time.monotonic() + control_timeout_s
                while len(group) < len(all_allocs):
                    if time.monotonic() > deadline:
                        raise RecoveryContractError("Graph cut collection deadline exceeded")
                    readable, _, _ = select.select(socks, [], [], 0.5)
                    for sock in readable:
                        link = next(l for l in links.values() if l.sock is sock)
                        if not link.pump():
                            # A worker host died (producer/consumer fault): fail fast so the
                            # run exits and whole-graph restore re-enters from the acked cut.
                            raise RecoveryContractError("Graph worker closed during cut collection")
                    for alloc, link in links.items():
                        if alloc not in group and checkpoint_id in link.contributions:
                            group[alloc] = link.contributions[checkpoint_id]
                # Use the ancestor's actual confirmed ordinal (the chunked feed may
                # overshoot the requested bound), never the requested bound itself.
                ancestor_progress = group[ancestor_alloc]["source_progress"]
                ordinal = ancestor_progress[0]["position"]["next_ordinal"] if ancestor_progress else 0
                final = ordinal == source_rows
                fact, manifest = terminal.commit_group(checkpoint_id, group, ordinal=ordinal,
                                                       final=final, epoch=epoch)
                receipt = {"status": "acked", "checkpoint_id": checkpoint_id,
                           "sequence": fact["sequence"], "commit_id": fact["commit_id"],
                           "channel_cuts": manifest["channel_cuts"]}
                for alloc in all_allocs:
                    links[alloc].send_receipt(checkpoint_id, receipt)
                    # Drop the just-committed fragment so a retry at this bound collects the
                    # next cut's contribution, never the stale one (same checkpoint_id).
                    links[alloc].contributions.pop(checkpoint_id, None)
                ancestor_contributions.append(group[ancestor_alloc])
                for rid in reader_ids:
                    contributions[rid].append(group[target_allocs[rid]])
                if final:
                    done = True
                    break
                if ordinal >= bound:
                    break
                # ordinal < bound: the budget forced an early cut; retry to reach EOF.

        target_rows = dict(terminal.rows)
        egress_rows = terminal.inputs
        run.journal.complete_execution_action(run.fence, ancestor_output_child,
            dict(kind="graph-ancestor", egress=egress_rows, cuts=len(ancestor_contributions)))
        for target in targets:
            rid = target["reader_id"]
            run.journal.complete_execution_action(run.fence, target_children[rid],
                dict(kind="graph-target", rows=target_rows[rid], cuts=len(contributions[rid])))
        terminal.finish(source_rows)

        for alloc in all_allocs:
            link = links[alloc]
            link.read_response(link.command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 10000,
                                                      "reason": "graph drained"}))
        drained = True
    finally:
        # Unified resource terminal (carrier N3): success/failure/cancel all convert
        # confirmed exit to capacity release through the same ResourceManager
        # close+reconcile. Only an unconfirmed exit keeps its charge; the kill below
        # is a backstop for survivors the protocol could not confirm.
        try:
            manager.close(
                MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                                uuid.uuid4().hex, accepted["desired_revision"]),
                {"mode": "DRAINED" if drained else "ABANDON",
                 "engine_receipt_ref": scope_id + ("-drained" if drained else "-abandoned"),
                 "terminate_deadline_ms": 5000,
                 "reason": "graph drained" if drained else "graph terminal cleanup"})
            manager.reconcile_once(run.fence, time.monotonic() + 20, 32)
        except Exception:
            logger.exception("Graph resource terminal failed; falling back to process kill")
        for sock in socks:
            sock.close()
        for alloc in all_allocs:
            connector.close(alloc)
        for rid in reader_ids:
            for sock in data_pairs[rid][:2]:
                sock.close()
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    final_arrival = {"ancestor": links[ancestor_alloc].final_at}
    for rid, alloc in target_allocs.items():
        final_arrival[rid] = links[alloc].final_at
    return {"target_rows": target_rows, "contributions": contributions,
            "egress_rows": egress_rows, "workers": len(all_allocs),
            "ancestor_contributions": ancestor_contributions,
            "final_arrival": final_arrival, "checkpoints": terminal.checkpoints}


def run_ray_graph(graph_spec, *, host_id="local", resource_root=None, control_timeout_s=120):
    """Run a static ancestor->targets graph in independent Ray Core actor hosts (S5).

    Same driver orchestration as ``run_local_graph`` (fixed source capture, K1
    manifest, frozen K3 lifecycle, single GraphCommitTerminal commit authority,
    whole-graph restore, cut schedule). Only the worker host and transports differ:
    named Ray actors (max_restarts=0) instead of managed processes; actor methods +
    a bounded outbox pull instead of an inherited control socketpair; the data plane
    is the Ray carriage (one async channel actor per physical channel, ray_object
    plasma for large payloads) instead of inherited socket fds. Business rows flow
    worker->worker and never transit the driver. Single-host Ray is semantic-
    adaptation evidence only (no multi-host/performance claim); selected by the
    opt-in ``unified_graph`` executor seam (D6).
    """
    import sys
    import ray
    from ray.exceptions import RayActorError

    from ..data.plan import LogicalPlan
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.ray import RayResourceProvider
    from .resource_state import RunResourceState
    from .exchange import read_domain
    from .exchange.carriage_ray import ray_channel_pair
    from .worker.graph_ray_host import RayGraphControlConnector

    action = current_action()
    if action is None:
        raise RuntimeError("Graph materialization requires the native action context")
    run = action.context
    source = graph_spec["source"]
    source_id = graph_spec["source_id"]
    ancestor = graph_spec["ancestor"]
    targets = graph_spec["targets"]
    from .operator_precheck import precheck_plan_state
    precheck_plan_state(ancestor["plan"])
    for target in targets:
        precheck_plan_state(target["plan"])
    routing = graph_spec.get("routing", "broadcast")
    key_fields = graph_spec.get("key_fields")
    target_classes = graph_spec.get("target_classes")
    target_field = graph_spec.get("target_field")
    exchange_mode = graph_spec.get("exchange_mode", "memory_incremental")
    budget = graph_spec.get("credit_budget", {"rows": 64, "bytes": 8 * 1024 ** 2})
    # Prefetch bounds derive from the admitted inflight budget only when one is
    # configured; the default path injects no key and stays byte-identical (U3).
    prefetch_slots = graph_spec.get("prefetch_slots")
    prefetch = (derive_prefetch(prefetch_slots, max_pending_floor=budget["rows"])
                if prefetch_slots is not None else None)
    queue_depth = graph_spec.get("queue_depth", 4096)
    if prefetch is not None:
        queue_depth = min(queue_depth, prefetch["queue_depth"])

    from .executors.local import LocalDatasetExecutor
    capture_executor = LocalDatasetExecutor(workers=1)
    selected = freeze_source(action, source)
    source_sink, source_reference = capture_source(
        action, lambda: capture_executor.iter_rows(selected, LogicalPlan()))
    ancestor_plan_ref = stage_plan_artifact(action, ancestor["plan"])
    target_plan_refs = [stage_plan_artifact(action, target["plan"]) for target in targets]
    ancestor_sealed = _sealed_operators(ancestor["plan"])
    target_sealed = [_sealed_operators(target["plan"]) for target in targets]
    manifest_ref = _build_manifest(run, [ancestor_plan_ref, *target_plan_refs])
    graph_topology = compile_graph_topology(
        action, selected, ancestor, targets, manifest_ref=manifest_ref, host_id=host_id)
    domain = read_domain(run.directory, host_id)

    reader_ids = [target["reader_id"] for target in targets]
    channel_plan = build_channel_plan(
        edge_id=identity([action.action_id, "graph-egress"]), scheme_version=1,
        producer_partitions=["p0"], consumer_partitions=reader_ids,
        exchange_mode=exchange_mode, routing=routing, read_domain=domain,
        credit_budget=budget,
        durability_declared="durable" if exchange_mode == "durable_incremental" else "ephemeral",
        seal_scope=({"scope_id": identity([action.action_id, "seal"]), "membership": ["p0"], "generation": 1}
                    if exchange_mode == "finite_sealed" else None))
    table = RouteTable(channel_plan, key_fields=key_fields, target_classes=target_classes)
    physical = {item["consumer_partition"]: item for item in table.physical_channels}

    ids = {k: uuid.uuid4().hex for k in ("scope_id", "boot_id", "grant_id")}
    scope_id = ids["scope_id"]
    ancestor_alloc = uuid.uuid4().hex
    target_allocs = {target["reader_id"]: uuid.uuid4().hex for target in targets}
    all_allocs = [ancestor_alloc, *target_allocs.values()]

    def grant_for(allocation_id, boot_id):
        return {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
                "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
                "boot_id": boot_id, "controller_epoch": run.attempt,
                "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}

    boot_ids = {alloc: uuid.uuid4().hex for alloc in all_allocs}
    capabilities = ["cpu-map", "stream", "graph"]

    def base_spec(allocation_id):
        return {"directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
                "allocation_id": allocation_id, "boot_id": boot_ids[allocation_id],
                "controller_epoch": run.attempt, "capabilities": capabilities,
                "grant": grant_for(allocation_id, boot_ids[allocation_id]),
                "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"]}

    ancestor_output_child = action.action_id + "-graph-ancestor"
    ancestor_source_child = action.action_id + "-source"
    decl = dict(kind="graph-node", source=action.declaration["source"], parent=action.semantic_id,
                protocol="graph-cut-v1")
    run.journal.bind_execution_action(run.fence, ancestor_output_child, decl)
    target_children = {}
    for target in targets:
        child_id = action.action_id + "-graph-" + target["reader_id"]
        run.journal.bind_execution_action(run.fence, child_id, decl)
        target_children[target["reader_id"]] = child_id

    result_child_id = action.action_id + "-graph-result"
    terminal = GraphCommitTerminal(
        action, source_id=source_id, ancestor=ancestor, targets=targets,
        target_children=target_children, physical=physical, manifest_ref=manifest_ref,
        result_child_id=result_child_id, target_allocs=target_allocs,
        ancestor_alloc=ancestor_alloc)
    source_rows = source_reference["data"]["rows"]
    if terminal.complete:
        return {"target_rows": dict(terminal.rows), "contributions": {rid: [] for rid in reader_ids},
                "egress_rows": terminal.inputs, "workers": 0, "ancestor_contributions": [],
                "final_arrival": {}, "checkpoints": dict(terminal.checkpoints)}
    from .plan_interpreter import interpret_physical_plan, to_wire
    from .sealed_plans import load_execution_bindings, load_sealed_plan
    _sealed_plan = load_sealed_plan(run.directory, action.action_id)
    _bindings = load_execution_bindings(run.directory, action.action_id,
                                        sealed_plan=_sealed_plan)
    _layout = interpret_physical_plan(
        _sealed_plan["logical_dag"], _sealed_plan["physical_plan"],
        bindings=_bindings, manifest_ref=_sealed_plan["facts"]["artifact_manifest_ref"])
    _placements = {w.group_id: w for w in _layout.workers}
    _alloc_group = {ancestor_alloc: "ancestor"}
    _alloc_group.update({target_allocs[t["reader_id"]]: "target-" + t["reader_id"] for t in targets})
    _sealed_profiles = {alloc: ResourceProfile(**to_wire(_placements[gid].profile))
                        for alloc, gid in _alloc_group.items()}
    _bindings_ref = _bindings["ref"]
    restore = terminal.restore_point()
    restore_ordinal = restore["position"]["next_ordinal"] if restore else 0

    def worker_state(occurrence_id):
        # Each worker's combined stage-state artifact is keyed by its occurrence;
        # hand back only that ref so the worker restore never sees sibling keys
        # or the graph-progress summary. Pure-function stages have no artifact.
        if restore is None:
            return {}
        ref = restore["state"].get(occurrence_id)
        return {occurrence_id: ref} if ref is not None else {}

    ancestor_restore = ({"inputs": restore_ordinal, "sequence": terminal.sequence, "rows": 0,
                         "state": worker_state(ancestor["occurrence_id"]),
                         "egress_ordinal": restore["summary"].get("egress_ordinal", 0)}
                        if restore else None)

    # Ray data plane: one named channel actor per physical channel. The driver keeps
    # the link handles alive for the whole run so the actors are not GC-killed before
    # the workers look them up by name; each worker reconstructs its endpoint on Assign.
    channel_names = {}
    channel_links = {}
    for reader_id in reader_ids:
        cid = physical[reader_id]["channel_id"]
        producer_link, consumer_link, name = ray_channel_pair(cid)
        channel_names[reader_id] = name
        channel_links[reader_id] = (producer_link, consumer_link)

    ancestor_spec = base_spec(ancestor_alloc)
    ancestor_spec["graph"] = {
        "allocation_id": ancestor_alloc, "output_child_id": ancestor_output_child,
        "cut_namespace": action.action_id,
        "source_child_id": ancestor_source_child, "plan_ref": ancestor_plan_ref,
        "sealed_operators": ancestor_sealed,
        "source_reference": source_reference, "source_id": source_id, "host_id": host_id,
        "logical_node_id": ancestor["logical_node_id"], "occurrence_id": ancestor["occurrence_id"],
        "route_plan": channel_plan, "queue_depth": queue_depth,
        "key_fields": key_fields, "target_classes": target_classes, "target_field": target_field,
        "restore": ancestor_restore,
        "output_channels": [{"channel_id": physical[rid]["channel_id"], "ray_queue": channel_names[rid],
                             "carriage": "ray", "reader_id": rid, "exchange_mode": exchange_mode,
                             "credit_budget": budget,
                             "storage_bytes": graph_spec.get("storage_bytes", budget["bytes"]),
                             "route_epoch": 0} for rid in reader_ids]}
    ancestor_spec["execution_plan"] = {"action_id": action.action_id, "group_id": "ancestor",
                                       "bindings_ref": _bindings_ref}

    target_specs = {}
    for target in targets:
        rid = target["reader_id"]
        alloc = target_allocs[rid]
        spec = base_spec(alloc)
        spec["graph"] = {
            "allocation_id": alloc, "output_child_id": target_children[rid],
            "plan_ref": target_plan_refs[targets.index(target)], "source_id": source_id,
            "sealed_operators": target_sealed[targets.index(target)],
            "host_id": host_id, "logical_node_id": target["logical_node_id"],
            "occurrence_id": target["occurrence_id"], "sink_id": rid,
            "restore": ({"inputs": 0, "sequence": terminal.sequence, "rows": 0,
                         "state": worker_state(target["occurrence_id"])} if restore else None),
            "input_channels": [{"channel_id": physical[rid]["channel_id"], "ray_queue": channel_names[rid],
                                "carriage": "ray", "reader_id": rid, "exchange_mode": exchange_mode,
                                "credit_budget": budget, "route_epoch": 0}]}
        spec["execution_plan"] = {"action_id": action.action_id, "group_id": "target-" + rid,
                                  "bindings_ref": _bindings_ref}
        target_specs[rid] = spec

    if prefetch is not None:
        ancestor_spec["graph"]["prefetch"] = prefetch
        for spec in target_specs.values():
            spec["graph"]["prefetch"] = prefetch

    # Ray admission checks the authorized scope ceiling; physical placement stays
    # with Ray Core (a busy cluster reports PENDING, never a fake RUNNING).
    scope_budget = {"cpu_millis": 32000, "memory_bytes": 32 * 1024 ** 3, "max_workers": 16}
    # plan_activation plans CPU/memory role minimums only; max_workers is enforced
    # by the scope ceiling in resource_state, not by the admission planner.
    all_allocs = _activation_order(ancestor_alloc, target_allocs,
                                   capacity={k: scope_budget[k] for k in ("cpu_millis", "memory_bytes")})
    connector = RayGraphControlConnector(command_timeout_s=control_timeout_s)
    provider = RayResourceProvider(connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(ancestor_alloc, ancestor_spec)
    for rid in reader_ids:
        connector.stage(target_allocs[rid], target_specs[rid])
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "ray"}, run.fence, scope_budget))

    def request_for(allocation_id):
        launch = WorkerLaunchSpec(
            artifact_refs={"graph": {"digest": manifest_ref["sha256"], "location": "file://" + str(run.directory)}},
            entrypoint="demiflow-graph-worker-actor",
            argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.graph_ray_host"),
            bootstrap_ref="bootstrap-ref", access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
        return WorkerRequest(allocation_id, allocation_id,
                             _sealed_profiles[allocation_id],
                             launch, provision_deadline_ms=60000)

    requests = tuple(request_for(alloc) for alloc in all_allocs)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence, uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, requests, ()))

    def now_ms():
        return int(time.time() * 1000)

    models = {}
    final_at = {}
    drained = False
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 60, 32)
        snapshot = manager.get(scope_id)
        phases = {a["allocation_id"]: a["phase"] for a in snapshot["allocations"]}
        for alloc in all_allocs:
            if phases.get(alloc) != "RUNNING":
                raise RecoveryContractError("Ray graph worker did not reach RUNNING: " + str(phases.get(alloc)))

        def command(alloc, kind, payload):
            op = uuid.uuid4().hex
            result = connector.handle(alloc, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": alloc, "boot_id": boot_ids[alloc], "payload": payload})
            if "error" in result:
                raise RecoveryContractError("Worker command failed: " + str(result["error"]))
            return result["receipt"]

        def await_event(alloc, kind, deadline):
            while True:
                events = connector.pull(alloc)
                for message in events:
                    if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                        models[alloc].handle(message, now_ms=now_ms())
                    if message.get("kind") == kind:
                        return message
                if time.monotonic() > deadline:
                    raise RecoveryContractError("Ray graph worker did not emit " + kind)
                if not events:
                    time.sleep(0.01)

        for alloc in all_allocs:
            models[alloc] = WorkerProtocolModel(
                scope_id=scope_id, run_id=run.ref.run_id, allocation_id=alloc,
                boot_id=boot_ids[alloc], controller_epoch=run.attempt,
                grant=grant_for(alloc, boot_ids[alloc]), plan_revision=0, capabilities=capabilities)

        lifecycle_deadline = time.monotonic() + control_timeout_s
        for alloc in all_allocs:
            await_event(alloc, "Register", lifecycle_deadline)
            command(alloc, "VerifyArtifactAndCapabilities",
                    {"artifact_manifest_ref": manifest_ref, "required_capabilities": capabilities})
            assignment_id = uuid.uuid4().hex
            command(alloc, "Restore", {"assignment_ref": assignment_id,
                                       "restore_point": None, "state_refs": []})
            await_event(alloc, "Ready", lifecycle_deadline)
            command(alloc, "Activate", {"grant_ref": ids["grant_id"],
                                        "controller_epoch": run.attempt,
                                        "operation_id": uuid.uuid4().hex})
            occurrence = (ancestor["occurrence_id"] if alloc == ancestor_alloc
                          else next(t["occurrence_id"] for t in targets if target_allocs[t["reader_id"]] == alloc))
            assignment = {
                "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
                "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": alloc,
                "boot_id": boot_ids[alloc], "worker_id": alloc, "controller_epoch": run.attempt,
                "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
                "artifact_manifest_ref": manifest_ref,
                "stages": list(_placements[_alloc_group[alloc]].stage_ids),
                "partitions": list(_placements[_alloc_group[alloc]].partition_ids),
                "channels": _worker_assignment_channels(_layout, _placements[_alloc_group[alloc]]),
                "restore_point": None,
                "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
            command(alloc, "Assign", {"assignment": assignment})

        cut_rows = max(1, int(graph_spec.get("cut_rows", source_rows or 1)))
        bounds = [b for b in range(cut_rows, source_rows, cut_rows) if b > restore_ordinal]
        if restore is None or restore_ordinal < source_rows:
            bounds.append(source_rows)
        contributions = {rid: [] for rid in reader_ids}
        ancestor_contributions = []
        epoch = run.attempt
        # EOF-driven finality mirrors the Local driver: commit final only at the ancestor's
        # actual source_rows, retrying a bound the budget forced early until EOF. Ray pull
        # drains each cut's contributions, so a retry at the same checkpoint_id naturally
        # collects fresh fragments (no stale carry-over). Ray runtime verification deferred.
        done = False
        for bound in bounds:
            if done:
                break
            while True:
                hint_final = bound == source_rows
                checkpoint_id = identity([action.action_id, "graph-cut", bound])
                cut_command = build_graph_checkpoint(checkpoint_id, epoch=epoch,
                                                     producer_sequence_bound=bound, final=hint_final)
                for alloc in all_allocs:
                    connector.send_graph_checkpoint(alloc, cut_command)
                group = {}
                pending_ops = {}
                deadline = time.monotonic() + control_timeout_s
                while len(group) < len(all_allocs):
                    if time.monotonic() > deadline:
                        raise RecoveryContractError("Graph cut collection deadline exceeded")
                    for alloc in all_allocs:
                        if alloc in group:
                            continue
                        try:
                            events = connector.pull(alloc)
                        except RayActorError:
                            raise RecoveryContractError("Ray graph worker died during cut collection")
                        for message in events:
                            if message.get("schema_version") == CUT_CONTRIBUTION_SCHEMA or message.get("kind") == "CutContribution":
                                contribution = validate_cut_contribution(message["contribution"])
                                if contribution["checkpoint_id"] == checkpoint_id:
                                    group[alloc] = contribution
                                    pending_ops[alloc] = message["operation_id"]
                                    if contribution["final"] and alloc not in final_at:
                                        final_at[alloc] = time.monotonic()
                            elif message.get("kind") in ("Register", "Ready", "Heartbeat"):
                                models[alloc].handle(message, now_ms=now_ms())
                    if len(group) < len(all_allocs):
                        time.sleep(0.01)
                ancestor_progress = group[ancestor_alloc]["source_progress"]
                ordinal = ancestor_progress[0]["position"]["next_ordinal"] if ancestor_progress else 0
                final = ordinal == source_rows
                fact, manifest = terminal.commit_group(checkpoint_id, group, ordinal=ordinal,
                                                       final=final, epoch=epoch)
                receipt = {"status": "acked", "checkpoint_id": checkpoint_id,
                           "sequence": fact["sequence"], "commit_id": fact["commit_id"],
                           "channel_cuts": manifest["channel_cuts"]}
                for alloc in all_allocs:
                    connector.send_receipt(alloc, pending_ops[alloc], receipt)
                ancestor_contributions.append(group[ancestor_alloc])
                for rid in reader_ids:
                    contributions[rid].append(group[target_allocs[rid]])
                if final:
                    done = True
                    break
                if ordinal >= bound:
                    break
                # ordinal < bound: the budget forced an early cut; retry to reach EOF.

        target_rows = dict(terminal.rows)
        egress_rows = terminal.inputs
        run.journal.complete_execution_action(run.fence, ancestor_output_child,
            dict(kind="graph-ancestor", egress=egress_rows, cuts=len(ancestor_contributions)))
        for target in targets:
            rid = target["reader_id"]
            run.journal.complete_execution_action(run.fence, target_children[rid],
                dict(kind="graph-target", rows=target_rows[rid], cuts=len(contributions[rid])))
        terminal.finish(source_rows)

        for alloc in all_allocs:
            command(alloc, "Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 10000,
                                     "reason": "graph drained"})
        drained = True
    finally:
        # Unified resource terminal (carrier N3), mirrors Local: success/failure/cancel
        # all release capacity through the same ResourceManager close+reconcile; the
        # actor kill below is a backstop for survivors the protocol could not confirm.
        try:
            manager.close(
                MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                                uuid.uuid4().hex, accepted["desired_revision"]),
                {"mode": "DRAINED" if drained else "ABANDON",
                 "engine_receipt_ref": scope_id + ("-drained" if drained else "-abandoned"),
                 "terminate_deadline_ms": 5000,
                 "reason": "graph drained" if drained else "graph terminal cleanup"})
            manager.reconcile_once(run.fence, time.monotonic() + 20, 32)
        except Exception:
            logger.exception("Ray graph resource terminal failed; falling back to actor kill")
        for alloc in all_allocs:
            connector.terminate(alloc)
            connector.close(alloc)
        for name in channel_names.values():
            try:
                ray.kill(ray.get_actor(name), no_restart=True)
            except Exception:
                pass
    final_arrival = {"ancestor": final_at.get(ancestor_alloc)}
    for rid, alloc in target_allocs.items():
        final_arrival[rid] = final_at.get(alloc)
    return {"target_rows": target_rows, "contributions": contributions,
            "egress_rows": egress_rows, "workers": len(all_allocs),
            "ancestor_contributions": ancestor_contributions,
            "final_arrival": final_arrival, "checkpoints": terminal.checkpoints}