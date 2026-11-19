"""Compatibility graph declarations translated into the unified v3 engine.

Shared ancestors, target programs and routing are sealed as actual v3 nodes and
channels. The original dataflow runtime owns workers, source snapshots, credits,
checkpointing and commits. Legacy classes below remain import-compatible for
historical readers; the public graph entry points do not run their old driver.
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
    row_bytes = run.limits["max_row_bytes"] + 256
    queue_bytes = max(row_bytes, run.limits["max_batch_bytes"])
    for prog in programs:
        stage_refs = [identity([dag_id, op["occurrence_id"]])
                      for op in prog["operators"]]
        worker_groups.append({
            "group_id": prog["group_id"], "role": "cpu_prepare",
            "profile_ref": profile_ref, "stage_refs": stage_refs,
            "min_workers": 1, "initial_workers": 1, "max_workers": 1})
        for op in prog["operators"]:
            stage_budgets[op["occurrence_id"]] = {
                "inflight_rows": run.limits["batch_rows"], "inflight_bytes": queue_bytes,
                "output_bytes": row_bytes}
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


def _graph_action_request(graph_spec, executor):
    from .dataflow_api import make_action_request
    from ..data.plan import LogicalPlan
    from ..planning.dataflow_graph_adapter import translate_graph
    source = translate_graph(graph_spec)
    return make_action_request(executor, source, LogicalPlan(), kind='materialize',
                               arguments={'graph': source.declaration})


def _run_graph(graph_spec, executor, *, control_timeout_s):
    from dataclasses import replace
    from .dataflow import execute_action
    from .dataflow_api import resolve_action_runtime, record_execution_metadata
    from ..planning.dataflow_graph_adapter import graph_credit, graph_result, validate_graph_replay
    if current_action() is None:
        raise RuntimeError('Graph materialization requires the native action context')
    request = _graph_action_request(graph_spec, executor)
    policy, bounds = resolve_action_runtime(request)
    declaration = request.source.declaration
    if 'queue_depth' in declaration:
        depth = declaration['queue_depth']
        if type(depth) is not int or depth < 1:
            raise ValueError('graph queue_depth must be a positive integer')
        bounds = replace(bounds, max_pending_events=min(bounds.max_pending_events, depth))
    if 'cut_rows' in declaration:
        rows = declaration['cut_rows']
        if type(rows) is not int or rows < 1:
            raise ValueError('graph cut_rows must be a positive integer')
        policy = replace(policy, checkpoint_rows=min(policy.checkpoint_rows, rows))
    if type(control_timeout_s) not in (int, float) or control_timeout_s <= 0:
        raise ValueError('graph control timeout must be positive')
    policy = replace(policy, control_timeout_ms=min(policy.control_timeout_ms, int(control_timeout_s * 1000)))
    # Credit rejection precedes source snapshot/read, program construction and
    # worker provisioning. The same solver seals the actual exchange windows.
    graph_credit(declaration, bounds=bounds)
    validate_graph_replay(request, bounds=bounds)
    handle = execute_action(request, policy=policy, bounds=bounds)
    primary_failure = None
    try:
        handle.result()
        return graph_result(handle._execution)
    except BaseException as exc:
        primary_failure = exc
        raise
    finally:
        # Close drains final observations. A failure while projecting them
        # must not replace the business failure or prevent remaining cleanup.
        failure = primary_failure
        for label, cleanup in (
                ('Graph cleanup also failed: ', handle.close),
                ('Execution metadata also failed: ', lambda: record_execution_metadata(request, handle._execution))):
            try:
                cleanup()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                elif exc is not failure:
                    failure.add_note(label + str(exc))
        if primary_failure is None and failure is not None:
            raise failure


def _validate_graph_location(executor, *, host_id, resource_root):
    """Legacy location keywords cannot create a second placement authority."""
    from pathlib import Path
    from .contracts.dataflow_errors import DataflowError
    if host_id != 'local':
        raise DataflowError('UNSUPPORTED_CAPABILITY',
                            'graph host placement must use the current execution provider')
    if resource_root is not None and Path(resource_root).resolve() != executor.resource_root:
        raise DataflowError('INVALID_CONTRACT',
                            'graph resource_root differs from the current executor domain')


def run_local_graph(graph_spec, *, host_id='local', resource_root=None,
                    control_timeout_s=120, _executor=None):
    """Run the complete graph through the original Local v3 action authority."""
    from .executors.local import LocalDatasetExecutor
    executor = _executor if _executor is not None else LocalDatasetExecutor()
    _validate_graph_location(executor, host_id=host_id, resource_root=resource_root)
    return _run_graph(graph_spec, executor, control_timeout_s=control_timeout_s)


def run_ray_graph(graph_spec, *, host_id='local', resource_root=None,
                  control_timeout_s=120, _executor=None):
    """Run the same graph through Ray Core, retaining its caller's envelope."""
    from .executors.ray import RayDatasetExecutor
    executor = _executor if _executor is not None else RayDatasetExecutor()
    _validate_graph_location(executor, host_id=host_id, resource_root=resource_root)
    return _run_graph(graph_spec, executor, control_timeout_s=control_timeout_s)
