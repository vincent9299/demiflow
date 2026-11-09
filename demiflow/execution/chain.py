"""E-side vertical chain orchestration and the driver commit terminal (U2/S4).

The operator chain runs whole inside one independent worker host (D3); the
driver owns only the control plane plus the existing source capture and the
single result-commit authority (D). The worker stages result blocks under the
granted fence and requests a durable commit over the control channel with a
``CommitCut``; the driver re-reads that staging descriptor (no re-write), builds
the K6 checkpoint manifest, commits through the existing ``CommitCoordinator``
and replies ``CommitReceipt``. The worker then advances its confirmed ledger.

This is a new wire message family, not a change to the frozen K3 kind set. The
first batch is single-source/single-partition: the K6 source-progress vector
degenerates to one component (``legacy_checkpoint_fragment`` shape); it never
pretends one ordinal covers a multi-source DAG (U3+). The ordered-channel
protocol itself is proven by the CHN group; the single-worker real chain commits
in lock-step, so a barrier can never overtake its pre-cut output descriptor.
"""
import hashlib

from .artifact_manifest import entries
from .block_sink import BlockSink
from .commit import CommitCoordinator
from .contracts.checkpoint import (
    legacy_checkpoint_fragment, validate_checkpoint_manifest,
)
from .recovery_contract import (
    RecoveryContractError, SourcePosition, document, identifier, identity,
)
from .run_context import RunAction
from .state_artifacts import StateArtifacts

COMMIT_CUT_SCHEMA = "demiflow.commit_cut.v1"
COMMIT_RECEIPT_SCHEMA = "demiflow.commit_receipt.v1"
_MAX_CONTROL_BYTES = 1 << 20


def build_commit_cut(operation_id, checkpoint_id, *, source_id, next_ordinal,
                     sequence, channel_id, channel_cut_sequence,
                     output_descriptor, states, inflight=(), final=False):
    identifier(operation_id)
    identifier(checkpoint_id)
    identifier(source_id)
    identifier(channel_id)
    if type(next_ordinal) is not int or not 0 <= next_ordinal < 2**63:
        raise RecoveryContractError("CommitCut source ordinal is invalid")
    if type(sequence) is not int or sequence < 1:
        raise RecoveryContractError("CommitCut sequence must be positive")
    if type(channel_cut_sequence) is not int or channel_cut_sequence < 0:
        raise RecoveryContractError("CommitCut channel sequence is invalid")
    if type(final) is not bool:
        raise RecoveryContractError("CommitCut final flag must be a bool")
    value = {
        "schema_version": COMMIT_CUT_SCHEMA, "operation_id": operation_id,
        "checkpoint_id": checkpoint_id, "source_id": source_id,
        "next_ordinal": next_ordinal, "sequence": sequence,
        "channel_id": channel_id, "channel_cut_sequence": channel_cut_sequence,
        "output_descriptor": output_descriptor, "states": states,
        "inflight": list(inflight), "final": final,
    }
    validate_commit_cut(value)
    return value


def validate_commit_cut(value):
    if type(value) is not dict or value.get("schema_version") != COMMIT_CUT_SCHEMA:
        raise RecoveryContractError("Invalid CommitCut schema")
    required = {"schema_version", "operation_id", "checkpoint_id", "source_id",
                "next_ordinal", "sequence", "channel_id", "channel_cut_sequence",
                "output_descriptor", "states", "inflight", "final"}
    if set(value) != required:
        raise RecoveryContractError("CommitCut key set is unsupported")
    for name in ("operation_id", "checkpoint_id", "source_id", "channel_id"):
        identifier(value[name])
    descriptor = value["output_descriptor"]
    if type(descriptor) is not dict or set(descriptor) != {"blocks", "rows"}:
        raise RecoveryContractError("CommitCut output descriptor is invalid")
    if type(descriptor["rows"]) is not int or descriptor["rows"] < 0:
        raise RecoveryContractError("CommitCut row count is invalid")
    if type(value["states"]) is not dict:
        raise RecoveryContractError("CommitCut states must be a mapping")
    if type(value["inflight"]) is not list:
        raise RecoveryContractError("CommitCut inflight must be a list")
    for entry in value["inflight"]:
        if type(entry) is not dict or not {"request_key", "status"} <= set(entry) \
                or set(entry) - {"request_key", "status", "effect_ref"}:
            raise RecoveryContractError("CommitCut inflight entry is invalid")
        identifier(entry["request_key"])
        if entry["status"] not in ("known_completed", "unknown"):
            raise RecoveryContractError("CommitCut inflight status is unsupported")
        if entry["status"] == "known_completed" and "effect_ref" not in entry:
            raise RecoveryContractError("Known completion needs an effect reference")
        if "effect_ref" in entry:
            identifier(entry["effect_ref"])
    if type(value["final"]) is not bool:
        raise RecoveryContractError("CommitCut final flag must be a bool")
    document(value, _MAX_CONTROL_BYTES)
    return value


def build_commit_receipt(operation_id, commit_id, sequence, status, receipt_descriptor):
    identifier(operation_id)
    identifier(commit_id)
    if status not in ("acked", "data_committed", "checkpoint_committed"):
        raise RecoveryContractError("CommitReceipt status is unsupported")
    value = {"schema_version": COMMIT_RECEIPT_SCHEMA, "operation_id": operation_id,
             "commit_id": commit_id, "sequence": sequence, "status": status,
             "receipt_descriptor": receipt_descriptor}
    validate_commit_receipt(value)
    return value


def validate_commit_receipt(value):
    if type(value) is not dict or value.get("schema_version") != COMMIT_RECEIPT_SCHEMA:
        raise RecoveryContractError("Invalid CommitReceipt schema")
    if set(value) != {"schema_version", "operation_id", "commit_id", "sequence",
                      "status", "receipt_descriptor"}:
        raise RecoveryContractError("CommitReceipt key set is unsupported")
    identifier(value["operation_id"])
    identifier(value["commit_id"])
    if type(value["sequence"]) is not int or value["sequence"] < 1:
        raise RecoveryContractError("CommitReceipt sequence must be positive")
    document(value, _MAX_CONTROL_BYTES)
    return value


class ChainResultSink(BlockSink):
    """Driver-side adapter that commits a worker staging descriptor verbatim.

    The worker already wrote and registered the blocks under the granted fence
    (staging_only), so the driver never re-encodes rows: ``prepare`` validates the
    descriptor against the shared child-action root, charges the driver output
    budget exactly once, and returns the descriptor as the prepared evidence.
    commit/inspect/abort/iter_rows keep the native BlockSink receipt semantics.
    """

    def prepare(self, descriptor, context):
        if type(descriptor) is not dict or set(descriptor) != {"blocks", "rows"}:
            raise RecoveryContractError("Chain result preparation needs a descriptor")
        self._validate(descriptor)
        if not self.input_capture and not self.staging_only:
            for block in self.block_entries(descriptor):
                reservation = 'rows-' + hashlib.sha256(block['path'].encode()).hexdigest()
                self.run.reserve_output('row_bytes', block['bytes'], reservation_id=reservation)
            self.run.reserve('output_rows', descriptor['rows'],
                             ceiling=self.run.limits['max_output_rows'])
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return descriptor


def chain_checkpoint_manifest(topology, cut, *, state_refs, sink_receipts, sealed):
    """Build the K6 manifest for a single-source chain cut (degenerate vector).

    Coverage is the manifest's own participant set: one source component, the
    result-out channel cut, the chain operator participants and the output sink.
    Keyed operator-state indexing beyond pure transforms is U3/U4; operator_states
    only carries entries that have a durable state ref in this cut.
    """
    source_id = cut["source_id"]
    position = {"source_id": source_id, "next_ordinal": cut["next_ordinal"]}
    fragment = legacy_checkpoint_fragment(position, source_ids=[source_id],
                                          channel_ids=(), partition_count=1)
    participants = {
        "sources": fragment["participants"]["sources"],
        "channels": [topology["channel_id"]],
        "operators": [dict(logical_node_id=node["logical_node_id"],
                           occurrence_id=node["occurrence_id"])
                      for node in topology["operators"]],
        "sinks": [topology["sink_id"]],
    }
    channel_cuts = [{"channel_id": topology["channel_id"],
                     "producer_cut_sequence": cut["channel_cut_sequence"],
                     "consumer_offsets": {"driver": cut["channel_cut_sequence"]}}]
    operator_states = []
    for index, node in enumerate(topology["operators"]):
        ref = state_refs.get("node-" + str(index))
        if ref is not None:
            operator_states.append({"logical_node_id": node["logical_node_id"],
                                    "occurrence_id": node["occurrence_id"],
                                    "state_ref": ref})
    value = {
        "schema_version": "demiflow.checkpoint_manifest.v1",
        "checkpoint_id": cut["checkpoint_id"], "run_id": topology["run_id"],
        "action_id": topology["action_id"], "epoch": topology["epoch"],
        "topology_revision": topology["topology_revision"],
        "route_epoch": topology["route_epoch"], "plan_revision": topology["plan_revision"],
        "artifact_manifest_ref": topology["artifact_manifest_ref"],
        "participants": participants, "source_progress": fragment["source_progress"],
        "channel_cuts": channel_cuts, "operator_states": operator_states,
        "inflight": cut["inflight"], "sink_receipts": sink_receipts,
        "seal": {"sealed": bool(sealed)},
    }
    if sealed:
        value["seal"]["digest"] = identity({k: v for k, v in value.items() if k != "seal"})
    return validate_checkpoint_manifest(value)


class DriverStreamTerminal:
    """The driver's single result-commit authority for one worker chain.

    Reconciles any prepared/partial commit before resume (existing
    ``CommitCoordinator.recover``), then serves ``CommitCut`` requests: re-read
    the worker staging descriptor, commit through the one native protocol, record
    the K6 checkpoint manifest and reply ``CommitReceipt``. Idempotency is the
    journal's: commit_id derives from (run, action, sequence); begin_commit
    dedups an identical intent and rejects a differing one.
    """

    def __init__(self, action, source_reference, *, output_child_id, topology,
                 sink_id="output"):
        self.run = action.context
        self.source_id = source_reference["source_id"]
        self.topology = dict(topology)
        self.sink_id = identifier(sink_id)
        if self.topology["sink_id"] != self.sink_id:
            raise RecoveryContractError("Topology sink differs from the terminal sink")
        declaration = dict(kind="chain-result", source=action.declaration["source"],
                           parent=action.semantic_id, protocol="commit-cut-v1")
        previous = self.run.journal.execution_action(output_child_id)
        self.run.journal.bind_execution_action(self.run.fence, output_child_id, declaration)
        self.child = RunAction(self.run, output_child_id, declaration)
        self.child.replaying = previous is not None
        self.sink = ChainResultSink(self.child, sink_id)
        self.coordinator = CommitCoordinator(self.child)
        self.checkpoints = {}
        self.inputs = self.rows = self.sequence = 0
        facts = self.coordinator.recover({sink_id: self.sink})
        latest = facts[-1] if facts else None
        if latest is not None and latest["status"] != "pending":
            position = latest["intent"]["position"]
            if position["source_id"] != self.source_id:
                raise RecoveryContractError("Chain source progress differs")
            self.inputs = position["next_ordinal"]
            self.sequence = latest["sequence"]
            summary = StateArtifacts(self.run).read(
                latest["prepared"]["state"]["stream-progress"])
            self.rows = summary["rows"]
        self.complete = self.child.record["result"] is not None

    def restore_point(self):
        """The latest acked cut the driver hands the worker on Restore."""
        latest = self.run.journal.latest_execution_commit(self.child.action_id, confirmed=True)
        if latest is None or latest["status"] == "pending":
            return None
        return {"position": latest["intent"]["position"], "sequence": latest["sequence"],
                "rows": self.rows, "state": latest["prepared"]["state"],
                "checkpoint_id": latest["commit_id"]}

    def replay_descriptors(self):
        """Forward already committed output descriptors without actor replay."""
        for fact in self.run.journal.iter_execution_commits(
                self.child.action_id, through_sequence=self.sequence):
            if fact["status"] != "pending":
                yield fact["receipts"][self.sink_id]["data"]

    def commit_cut(self, cut):
        validate_commit_cut(cut)
        if cut["source_id"] != self.source_id:
            raise RecoveryContractError("CommitCut belongs to another source")
        if cut["channel_id"] != self.topology["channel_id"]:
            raise RecoveryContractError("CommitCut belongs to another channel")
        # sequence == self.sequence is an idempotent retry of an accepted cut:
        # begin_commit dedups an identical intent and rejects a differing one.
        if cut["sequence"] not in (self.sequence, self.sequence + 1):
            raise RecoveryContractError("CommitCut sequence is not the next cut")
        descriptor = cut["output_descriptor"]
        fact = self.coordinator.commit(cut["sequence"],
            outputs={self.sink_id: (self.sink, descriptor)},
            position=SourcePosition(self.source_id, cut["next_ordinal"]),
            states=cut["states"])
        data = fact["receipts"][self.sink_id]["data"]
        manifest = chain_checkpoint_manifest(self.topology, cut,
            state_refs=fact["prepared"]["state"],
            sink_receipts=[{"sink_id": self.sink_id, "commit_id": fact["commit_id"],
                            "receipt_ref": fact["commit_id"]}],
            sealed=cut["final"])
        ref = StateArtifacts(self.run).put(cut["checkpoint_id"], manifest)
        self.checkpoints[cut["checkpoint_id"]] = {"manifest": manifest, "ref": ref}
        self.inputs = cut["next_ordinal"]
        self.sequence = fact["sequence"]
        self.rows = self._summary_rows(fact)
        return build_commit_receipt(cut["operation_id"], fact["commit_id"],
                                    fact["sequence"], fact["status"], data)

    def _summary_rows(self, fact):
        summary = StateArtifacts(self.run).read(fact["prepared"]["state"]["stream-progress"])
        return summary["rows"]

    def finish(self, source_rows):
        if self.inputs != source_rows:
            raise RecoveryContractError("Unconfirmed source at chain completion")
        self.child.complete(dict(kind="materialized", rows=self.rows,
                                 inputs=self.inputs, commits=self.sequence))
        self.complete = True


def stage_plan_artifact(action, plan):
    """Content-address the in-run pickled plan; the worker verifies before load.

    Persistent protocol never carries an arbitrary cloudpickle document (E2):
    this is the in-run pickle delivery path, sha256-bound into the K1 manifest.
    """
    import cloudpickle
    from .state_artifacts import atomic_bytes
    run = action.context
    raw = cloudpickle.dumps(plan)
    digest = hashlib.sha256(raw).hexdigest()
    path = run.directory / "scratch" / "chain-plans" / (digest + ".pkl")
    run.allocate_artifact(path, size=len(raw))
    with run.journal.publication_guard(run.fence):
        atomic_bytes(path, raw)
    run.release_temporaries([path.with_name(path.name + ".staging")])
    return {"path": str(path.relative_to(run.directory)), "sha256": digest, "bytes": len(raw)}


def compile_chain_topology(action, source, plan, source_reference, *,
                           output_child_id, host_id, plan_ref):
    """Compile the E2 linear DAG/physical plan and seal the K1 manifest (D3).

    The whole operator chain runs in one worker host, so the source-progress
    vector is a single component and the result-out channel is the terminal edge.
    Worker counts live only in the physical worker group, never in logical ids.
    """
    from ..planning.dag import compile_logical_dag, compile_physical_execution_plan
    from .contracts.artifact import build_manifest, save_manifest, seal_manifest
    from .exchange import read_domain
    run = action.context
    config = run.journal.recovery_identity()
    domain = read_domain(run.directory, host_id)
    logical_dag = compile_logical_dag(action_id=action.action_id, source=source,
                                      operations=plan.operations,
                                      terminal_kind="materialize", sink_ids=("output",))
    manifest = build_manifest(
        run_id=run.ref.run_id,
        platform={"source_digest": config["platform_revision"], "distribution": "demiflow",
                  "version": "u2", "runtime_identity_ref": identity([run.ref.run_id, "runtime"])},
        business={"bundle_digest": "sha256:" + plan_ref["sha256"],
                  "entrypoints": [{"name": "chain", "module_key": "p_" + plan_ref["sha256"][:24],
                                   "definition_ref": {"sha256": plan_ref["sha256"]}}]},
        config={"resolved_config_digest": identity([run.ref.run_id, "config"]),
                "model_config_refs": []},
        environment={"requirements_sha256": identity(["requirements"]),
                     "lock_digest": identity(["lock"]), "resolution_digest": identity(["resolution"])},
        protocol_versions={"worker": {"major": 1, "minor": 0},
                           "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1,
                           "resource_protocol": "1.0"},
        secret_refs=[])
    # Seal with the run's persisted birth epoch so the ref is stable across
    # driver restarts (manifest_id excludes sealed_at; the storage digest binds it).
    manifest = seal_manifest(manifest, int(config["started_at"] * 1000))
    ref = save_manifest(run.directory, manifest)
    mref = {"manifest_id": ref["manifest_id"], "sha256": ref["sha256"]}
    stage_refs = [identity([logical_dag["dag_id"], node["occurrence_id"]])
                  for node in logical_dag["nodes"]]
    physical = compile_physical_execution_plan(
        logical_dag=logical_dag, plan_revision=0, artifact_manifest_ref=mref,
        worker_groups=[{"group_id": "chain-group", "role": "cpu_prepare",
                        "profile_ref": "profile-local", "stage_refs": stage_refs,
                        "min_workers": 1, "initial_workers": 1, "max_workers": 1}],
        recovery={"checkpoint_interval_ms": run.limits["checkpoint_interval_ms"],
                  "unconfirmed_rows": run.limits["checkpoint_unconfirmed_rows"],
                  "unconfirmed_bytes": run.limits["checkpoint_unconfirmed_bytes"],
                  "terminals": []},
        feasibility_snapshot_ref=identity([run.ref.run_id, "feasibility"]),
        read_domain=domain, durability_declared="durable",
        partition_count=1, scheme_kind="single")
    operators = [{"logical_node_id": node["logical_node_id"],
                  "occurrence_id": node["occurrence_id"]}
                 for node in logical_dag["nodes"][1:-1]]
    topology = {"run_id": run.ref.run_id, "action_id": output_child_id,
                "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                "plan_revision": 0, "artifact_manifest_ref": mref,
                "source_id": source_reference["source_id"],
                "channel_id": physical["channels"][-1]["channel_id"],
                "operators": operators, "sink_id": "output"}
    return topology, logical_dag, physical


class _ChainBlocks:
    """Repeatable lazy block paths over the driver's committed chain receipts."""

    def __init__(self, terminal):
        self.terminal = terminal

    def __iter__(self):
        from .executors.local import _SpilledBlock
        run = self.terminal.run
        for descriptor in self.terminal.replay_descriptors():
            for block in self.terminal.sink.block_entries(descriptor):
                yield _SpilledBlock(str(run.directory / block["path"]), durable=True)

    def __len__(self):
        return sum(1 for _ in self)


def run_local_chain(executor, dataset, *, source=None, plan=None, host_id="local",
                    resource_root=None, control_timeout_s=120):
    """Drive one whole operator chain in an independent Local worker host (D3).

    The driver captures the fixed source, compiles the E2 DAG/physical plan and
    K1 manifest, provisions one managed worker through the real ResourceManager,
    runs the frozen K3 lifecycle over an inherited socketpair, then serves the
    worker's ``CommitCut`` requests through the single ``DriverStreamTerminal``
    commit authority and replies ``CommitReceipt``. The worker stages result
    blocks under the granted fence; the driver never re-encodes rows. After the
    final cut is acked the driver drains, finishes the child action and retires
    the worker through confirmed exit evidence. Default in-driver path is
    untouched: this is selected only by the platform-side executor seam (D5).
    """
    import sys
    import time
    import uuid

    from ..data.plan import LogicalPlan
    from ..resources.capacity import LocalCapacityPool
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.local import LocalResourceProvider
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.process_unit import ManagedProcessUnit
    from .contracts.envelope import WORKER_PROTOCOL_VERSION
    from .contracts.worker import WorkerProtocolModel
    from .executors.local import _LocalMaterializedHandle
    from .resource_state import RunResourceState
    from .run_context import admit_stream, current_action
    from .source_snapshot import capture_source, freeze_source
    from .worker.bootstrap import (
        LocalControlConnector, recv_message, send_message,
    )

    action = current_action()
    if action is None:
        raise RuntimeError("Chain materialization requires the native action context")
    run = action.context
    plan = dataset._plan if plan is None else plan
    source = dataset._source if source is None else source
    admit_stream(plan)
    selected = freeze_source(action, source)
    source_sink, source_reference = capture_source(
        action, lambda: executor.iter_rows(selected, LogicalPlan()))
    source_rows = source_reference["data"]["rows"]
    output_child_id = action.action_id + "-chain"
    source_child_id = action.action_id + "-source"
    plan_ref = stage_plan_artifact(action, plan)
    topology, logical_dag, physical = compile_chain_topology(
        action, selected, plan, source_reference, output_child_id=output_child_id,
        host_id=host_id, plan_ref=plan_ref)
    terminal = DriverStreamTerminal(action, source_reference,
                                    output_child_id=output_child_id, topology=topology)
    executor._run_ref = run.ref
    if terminal.complete:
        return _LocalMaterializedHandle(
            _ChainBlocks(terminal), terminal.rows,
            recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                    semantic_id=action.semantic_id))

    ids = {k: uuid.uuid4().hex for k in
           ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
    scope_id, allocation_id, boot_id = ids["scope_id"], ids["allocation_id"], ids["boot_id"]
    grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
             "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
             "boot_id": boot_id, "controller_epoch": run.attempt,
             "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
    capabilities = ["cpu-map", "stream"]
    restore = terminal.restore_point()
    launch_spec = {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": capabilities, "grant": grant,
        "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"],
        "execution": {"source_child_id": source_child_id, "output_child_id": output_child_id,
                      "plan_ref": plan_ref, "source_reference": source_reference,
                      "channel_id": topology["channel_id"], "restore": restore},
    }

    root = run.directory.with_name(run.directory.name + "-resources") if resource_root is None \
        else __import__("pathlib").Path(resource_root)
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    connector = LocalControlConnector()
    provider = LocalResourceProvider(pool, unit, control=connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(allocation_id, launch_spec)
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "local"}, run.fence,
        {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": plan_ref["sha256"],
                                "location": "file://" + str(run.directory / plan_ref["path"])}},
        entrypoint="demiflow-worker",
        argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap"),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(ids["worker_id"], allocation_id,
                            ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3}),
                            launch, provision_deadline_ms=60000)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                        uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, (request,), ()))
    sock = None
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise RecoveryContractError("Local chain worker did not reach RUNNING: " + str(phase))
        sock = connector.driver_socks[allocation_id]
        sock.settimeout(control_timeout_s)
        model = WorkerProtocolModel(
            scope_id=scope_id, run_id=run.ref.run_id, allocation_id=allocation_id,
            boot_id=boot_id, controller_epoch=run.attempt, grant=grant,
            plan_revision=0, capabilities=capabilities)

        def now_ms():
            return int(time.time() * 1000)

        def command(kind, payload):
            op = uuid.uuid4().hex
            send_message(sock, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": allocation_id, "boot_id": boot_id, "payload": payload})
            return op

        def read_response(op):
            while True:
                message = recv_message(sock)
                if message is None:
                    raise RecoveryContractError("Worker closed before responding")
                if message.get("response_to") == op:
                    if "error" in message:
                        raise RecoveryContractError("Worker command failed: " + str(message["error"]))
                    return message["receipt"]
                if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    model.handle(message, now_ms=now_ms())

        register = recv_message(sock)
        if register is None or register.get("kind") != "Register":
            raise RecoveryContractError("Worker did not register")
        model.handle(register, now_ms=now_ms())
        read_response(command("VerifyArtifactAndCapabilities",
                              {"artifact_manifest_ref": topology["artifact_manifest_ref"],
                               "required_capabilities": capabilities}))
        assignment_id = uuid.uuid4().hex
        read_response(command("Restore", {"assignment_ref": assignment_id,
                                          "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
                                          "state_refs": []}))
        read_response(command("Activate", {"grant_ref": ids["grant_id"],
                                           "controller_epoch": run.attempt,
                                           "operation_id": uuid.uuid4().hex}))
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
            "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "worker_id": ids["worker_id"], "controller_epoch": run.attempt,
            "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": topology["artifact_manifest_ref"],
            "stages": [node["occurrence_id"] for node in logical_dag["nodes"]],
            "partitions": ["p0"], "channels": {"inputs": [], "outputs": [topology["channel_id"]]},
            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
            "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
        assign_op = command("Assign", {"assignment": assignment})
        final_done = False
        while not final_done:
            message = recv_message(sock)
            if message is None:
                raise RecoveryContractError("Worker closed before the final commit cut")
            if message.get("response_to") == assign_op:
                if "error" in message:
                    raise RecoveryContractError("Worker assign failed: " + str(message["error"]))
                continue
            if message.get("schema_version") == COMMIT_CUT_SCHEMA:
                receipt = terminal.commit_cut(message)
                send_message(sock, receipt)
                final_done = message["final"]
                continue
            if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                model.handle(message, now_ms=now_ms())
        read_response(command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 5000,
                                        "reason": "chain drained"}))
        terminal.finish(source_rows)
        manager.close(
            MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                            uuid.uuid4().hex, accepted["desired_revision"]),
            {"mode": "DRAINED", "engine_receipt_ref": allocation_id + "-drained",
             "terminate_deadline_ms": 5000, "reason": "chain drained"})
        manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
    finally:
        if sock is not None:
            sock.close()
        connector.close(allocation_id)
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    return _LocalMaterializedHandle(
        _ChainBlocks(terminal), terminal.rows,
        recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                semantic_id=action.semantic_id))


def run_ray_chain(executor, dataset, *, source=None, plan=None, host_id="local",
                  resource_root=None, control_timeout_s=120):
    """Drive one whole operator chain in an independent Ray Core actor host (D3).

    Same driver orchestration as ``run_local_chain`` (fixed source capture, E2
    DAG/physical plan, K1 manifest, single DriverStreamTerminal commit authority,
    frozen K3 lifecycle, CommitCut/CommitReceipt handshake, final drain). Only the
    worker host and control transport differ: a named Ray actor (max_restarts=0)
    instead of a managed process, actor method calls plus a bounded outbox pull
    instead of an inherited socketpair (design §8). Ray Core places/reserves the
    actor; there is no LocalCapacityPool double-charge. Result blocks still hand
    off as staging descriptors on the shared same-host run directory (D3). The
    default Ray Data path is untouched: this is selected only by the platform-side
    executor seam (D5). Single-host Ray is semantic-adaptation evidence only.
    """
    import sys
    import time
    import uuid

    from ..data.plan import LogicalPlan
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.ray import RayResourceProvider
    from .contracts.envelope import WORKER_PROTOCOL_VERSION
    from .contracts.worker import WorkerProtocolModel
    from .executors.local import _LocalMaterializedHandle
    from .resource_state import RunResourceState
    from .run_context import admit_stream, current_action
    from .source_snapshot import capture_source, freeze_source
    from .worker.ray_host import RayControlConnector

    action = current_action()
    if action is None:
        raise RuntimeError("Chain materialization requires the native action context")
    run = action.context
    plan = dataset._plan if plan is None else plan
    source = dataset._source if source is None else source
    admit_stream(plan)
    selected = freeze_source(action, source)
    # Driver-side source capture is backend-neutral (D3): read the fixed source
    # with the local reader, never Ray Data (D1 env is Ray Core only).
    from .executors.local import LocalDatasetExecutor
    capture_executor = LocalDatasetExecutor(workers=1)
    source_sink, source_reference = capture_source(
        action, lambda: capture_executor.iter_rows(selected, LogicalPlan()))
    source_rows = source_reference["data"]["rows"]
    output_child_id = action.action_id + "-chain"
    source_child_id = action.action_id + "-source"
    plan_ref = stage_plan_artifact(action, plan)
    topology, logical_dag, physical = compile_chain_topology(
        action, selected, plan, source_reference, output_child_id=output_child_id,
        host_id=host_id, plan_ref=plan_ref)
    terminal = DriverStreamTerminal(action, source_reference,
                                    output_child_id=output_child_id, topology=topology)
    executor._run_ref = run.ref
    if terminal.complete:
        return _LocalMaterializedHandle(
            _ChainBlocks(terminal), terminal.rows,
            recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                    semantic_id=action.semantic_id))

    ids = {k: uuid.uuid4().hex for k in
           ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
    scope_id, allocation_id, boot_id = ids["scope_id"], ids["allocation_id"], ids["boot_id"]
    grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
             "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
             "boot_id": boot_id, "controller_epoch": run.attempt,
             "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
    capabilities = ["cpu-map", "stream"]
    restore = terminal.restore_point()
    launch_spec = {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": capabilities, "grant": grant,
        "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"],
        "execution": {"source_child_id": source_child_id, "output_child_id": output_child_id,
                      "plan_ref": plan_ref, "source_reference": source_reference,
                      "channel_id": topology["channel_id"], "restore": restore},
    }

    connector = RayControlConnector(command_timeout_s=control_timeout_s)
    provider = RayResourceProvider(connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(allocation_id, launch_spec)
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "ray"}, run.fence,
        {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": plan_ref["sha256"],
                                "location": "file://" + str(run.directory / plan_ref["path"])}},
        entrypoint="demiflow-worker-actor",
        argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.ray_host"),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(ids["worker_id"], allocation_id,
                            ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3}),
                            launch, provision_deadline_ms=60000)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                        uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, (request,), ()))
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise RecoveryContractError("Ray chain worker did not reach RUNNING: " + str(phase))
        model = WorkerProtocolModel(
            scope_id=scope_id, run_id=run.ref.run_id, allocation_id=allocation_id,
            boot_id=boot_id, controller_epoch=run.attempt, grant=grant,
            plan_revision=0, capabilities=capabilities)

        def now_ms():
            return int(time.time() * 1000)

        def command(kind, payload):
            op = uuid.uuid4().hex
            result = connector.handle(allocation_id, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": allocation_id, "boot_id": boot_id, "payload": payload})
            if "error" in result:
                raise RecoveryContractError("Worker command failed: " + str(result["error"]))
            return result["receipt"]

        def await_event(kind, deadline):
            while True:
                events = connector.pull(allocation_id)
                for message in events:
                    if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                        model.handle(message, now_ms=now_ms())
                    if message.get("kind") == kind:
                        return message
                if time.monotonic() > deadline:
                    raise RecoveryContractError("Ray worker did not emit " + kind)
                if not events:
                    time.sleep(0.01)

        lifecycle_deadline = time.monotonic() + control_timeout_s
        await_event("Register", lifecycle_deadline)
        command("VerifyArtifactAndCapabilities",
                {"artifact_manifest_ref": topology["artifact_manifest_ref"],
                 "required_capabilities": capabilities})
        assignment_id = uuid.uuid4().hex
        command("Restore", {"assignment_ref": assignment_id,
                            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
                            "state_refs": []})
        await_event("Ready", lifecycle_deadline)
        command("Activate", {"grant_ref": ids["grant_id"],
                             "controller_epoch": run.attempt,
                             "operation_id": uuid.uuid4().hex})
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
            "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "worker_id": ids["worker_id"], "controller_epoch": run.attempt,
            "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": topology["artifact_manifest_ref"],
            "stages": [node["occurrence_id"] for node in logical_dag["nodes"]],
            "partitions": ["p0"], "channels": {"inputs": [], "outputs": [topology["channel_id"]]},
            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
            "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
        command("Assign", {"assignment": assignment})
        final_done = False
        deadline = time.monotonic() + control_timeout_s
        while not final_done:
            events = connector.pull(allocation_id)
            for message in events:
                if message.get("schema_version") == COMMIT_CUT_SCHEMA:
                    receipt = terminal.commit_cut(message)
                    connector.deliver_receipt(allocation_id, receipt)
                    final_done = message["final"]
                elif message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    model.handle(message, now_ms=now_ms())
            if not events:
                if time.monotonic() > deadline:
                    raise RecoveryContractError("Ray worker closed before the final commit cut")
                time.sleep(0.01)
        command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 5000,
                          "reason": "chain drained"})
        terminal.finish(source_rows)
        manager.close(
            MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                            uuid.uuid4().hex, accepted["desired_revision"]),
            {"mode": "DRAINED", "engine_receipt_ref": allocation_id + "-drained",
             "terminate_deadline_ms": 5000, "reason": "chain drained"})
        manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
    finally:
        connector.terminate(allocation_id)
        connector.close(allocation_id)
    return _LocalMaterializedHandle(
        _ChainBlocks(terminal), terminal.rows,
        recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                semantic_id=action.semantic_id))