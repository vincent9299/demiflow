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
import logging

from .seal.business_sources import business_dependency_modules, plan_code_pin, source_callable_roots

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
WORKER_FAILURE_SCHEMA = "demiflow.worker_failure.v1"
_MAX_CONTROL_BYTES = 1 << 20
SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2

logger = logging.getLogger(__name__)


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
            # Commit-keyed + ordinal survives a window-3 re-prepare via the journal's idempotent charge.
            for ordinal, block in enumerate(self.block_entries(descriptor)):
                self.run.reserve_output('row_bytes', block['bytes'],
                    reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                             'component': 'row_bytes', 'block': ordinal}))
            self.run.reserve('output_rows', descriptor['rows'],
                             ceiling=self.run.limits['max_output_rows'],
                             reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                                      'component': 'output_rows'}))
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


def snapshot_code_sources(action, plan, *, source=None):
    """Seal the business UDF source files as immutable run artifacts for audit.

    The code-pin digest already binds each UDF's source bytes (``describe`` carries
    ``source_hash``) and its transitive business dependencies; this preserves the
    actual sealed source so a refused resume can be audited against the code that
    was pinned. Best-effort for plain module functions; native actors are engine
    code identified by their native declarations. Dependency modules archive through the
    same content-addressed path — no second artifact authority. ``source`` adds the
    native source's own callable files and walks the identical dependency set the
    pin used, so the archive can never cover less than the pin claims.
    """
    import inspect
    from pathlib import Path
    from .seal.business_sources import SourceFile
    from .state_artifacts import atomic_chunks
    run = action.context
    seen, refs = set(), []

    def archive(source, captured=None):
        source = Path(source)
        if not source.is_file() or source in seen:
            return
        seen.add(source)
        captured = captured or SourceFile.capture(source.resolve())
        digest = captured.digest
        path = run.directory / "scratch" / "chain-code-pin" / (digest + ".py")
        # Unregistered, mirroring seal_plan_facts: clean_orphans unlinks registered
        # protected=0 artifacts at run exit, but this source archive is audited
        # post-run (the manifest definition_ref points at it), so it must survive.
        with run.journal.publication_guard(run.fence):
            atomic_chunks(path, captured.chunks())
        refs.append({"path": str(path.relative_to(run.directory)), "sha256": digest,
                     "bytes": captured.size, "source": str(source)})

    for operation in plan.operations:
        target = getattr(getattr(operation, "callable", None), "target", None)
        function = target.__self__ if inspect.ismethod(target) else target
        if not inspect.isfunction(function):
            continue
        filename = inspect.getsourcefile(function)
        if filename:
            archive(filename)
    for function in source_callable_roots(source) if source is not None else ():
        import functools
        while type(function) is functools.partial:
            function = function.func
        filename = inspect.getsourcefile(function)
        if filename:
            archive(filename)
    for captured in business_dependency_modules(plan, source=source).values():
        archive(captured.path, captured)
    return refs


def _seal_plan_code(action, plan, pin_key, *, source=None):
    """Seal the code-pin binding + source archive shared by every plan-seal path.

    One implementation for the chain/graph pickle delivery and the in-process write
    seam. ``pin_key`` must be stable across attempts so a resume whose current
    business code differs from the sealed pin fail-closes (history immutable, D4):
    delivery keys it on the pickle digest (stable for UDFs carried by reference, and
    distinct per plan for graph's multiple plans per action); the write seam keys it
    on a fixed per-action key (one write plan per action), never on the code-pin
    identity itself, which would change with the code and defeat the resume refusal.
    The source archive is the manifest definition_ref.location. ``source`` is passed
    to the pin and the archive alike so both cover the identical dependency set.
    """
    run = action.context
    code_pin = plan_code_pin(plan, source=source)
    sealed = run.journal.source_binding(pin_key)
    if sealed is not None and sealed != code_pin:
        raise RecoveryContractError(
            "Business code changed since the plan was sealed; resume refused (fail-closed)")
    run.journal.bind_source(run.fence, pin_key, code_pin)
    sources = snapshot_code_sources(action, plan, source=source)
    return {"code_pin_digest": identity(code_pin), "code_sources": sources}


def stage_plan_artifact(action, plan):
    """Content-address the in-run pickled plan; the worker verifies before load.

    Persistent protocol never carries an arbitrary cloudpickle document (E2):
    this is the in-run pickle delivery path, sha256-bound into the K1 manifest.
    The pickle alone does not pin importable business UDFs (carried by reference),
    so a code-sensitive code-pin is sealed alongside it through the shared
    ``_seal_plan_code`` helper (also used by the in-process write seam).
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
    code_facts = _seal_plan_code(action, plan, action.action_id + "-code-pin-" + digest)
    return {"path": str(path.relative_to(run.directory)), "sha256": digest, "bytes": len(raw),
            **code_facts}


def seal_write_plan_ref(action, plan, *, source=None, program_id=None):
    """Bundle identity for the in-process write seam: the code-pin, not a pickle.

    The streamed/relational write_lance path runs the live plan via
    prepare_stream_blocks and never loads a pickled payload, so its bundle identity
    must not be the digest of a live-actor pickle: materialized PromptActor instances
    carry runtime locks (unpicklable) and those bytes drift across attempts, weakening
    gap④. Reuse the sole code-pin authority — identity(plan_code_pin) is the bundle
    digest, sealed as a real unregistered JSON carrier that survives run exit (bindings
    carry plan_ref; the workbench audits it). The code-pin binding and source archive
    come from the same ``_seal_plan_code`` helper the delivery path uses: one sealing
    implementation, no second serialization authority.

    ``program_id=None`` keeps the historical single-program key byte-for-byte. A
    multi-slice native program must pass its stable ProgramSlice id, keyed as
    ``identity([action_id, "code-pin-write", program_id])`` — never an attempt number
    or pickle bytes, so a resume finds the original binding. ``source`` reaches the
    pin and the archive as the same set; a sealed record that predates source
    coverage therefore recomputes to a different digest and recovery refuses rather
    than guessing that the old record protected the source's code.
    """
    from .state_artifacts import atomic_bytes
    run = action.context
    if program_id is None:
        pin_key = action.action_id + "-code-pin-write"
    else:
        if type(program_id) is not str or not program_id:
            raise RecoveryContractError("Program slice id must be a nonempty string")
        pin_key = identity([action.action_id, "code-pin-write", program_id])
    code_pin = plan_code_pin(plan, source=source)
    digest = identity(code_pin)
    raw = document(code_pin, MAX_SEALED_PLAN_BYTES)
    path = run.directory / "scratch" / "chain-write-pin" / (digest + ".json")
    # Unregistered, mirroring seal_plan_facts: clean_orphans prunes registered
    # protected=0 artifacts at run exit, but this post-run audited side-car must survive.
    with run.journal.publication_guard(run.fence):
        if not path.exists():
            atomic_bytes(path, raw)
    run.release_temporaries([path.with_name(path.name + ".staging")])
    # Stable per-program pin key: a resume with drifted business code finds the
    # sealed pin and fail-closes. Keying on identity(code_pin) would change with the
    # code and silently re-bind, defeating gap④.
    code_facts = _seal_plan_code(action, plan, pin_key, source=source)
    return {"path": str(path.relative_to(run.directory)), "sha256": digest, "bytes": len(raw),
            **code_facts}


def verify_loaded_plan(plan, plan_ref, sealed_operators):
    """Worker load-time gate: the loaded plan must equal the sealed plan contract.

    Two complementary bindings, both recomputed from the loaded plan alone: the
    code-pin (``describe`` identities, code-sensitive) fail-closes if business code
    changed since seal; the operator ``logical_node_id``s (frozen ``dag.py``
    semantics) bind the loaded plan structure to the sealed logical_dag. The worker
    holds no commit authority — this is a pure load check, not a second authority.
    """
    from ..planning.dag import _operation_semantics
    if identity(plan_code_pin(plan)) != plan_ref["code_pin_digest"]:
        raise RecoveryContractError(
            "Worker plan code-pin differs; business code edited since seal (fail-closed)")
    if sealed_operators is not None:
        recomputed = [identity([_operation_semantics(op)[0], index])
                      for index, op in enumerate(plan.operations)]
        sealed = [node["logical_node_id"] for node in sealed_operators]
        if recomputed != sealed:
            raise RecoveryContractError(
                "Loaded plan operator identity differs from the sealed logical_dag")


def _worker_failure(message):
    """Turn a worker-level failure frame into an identifiable driver error (F8).

    A load/init failure (or a row error outside an outstanding cut) happens before
    any CommitCut, so it cannot ride a cut response; the worker sends this frame
    instead and the driver ends immediately rather than blocking to control timeout.
    """
    detail = message.get("traceback")
    if detail:
        logger.error("Worker failed:\n%s", detail)
    error = message.get("error") or {}
    return RecoveryContractError("Worker failed: %s: %s"
                                 % (error.get("type"), error.get("message")))


def seal_plan_facts(run, action, logical_dag, physical, manifest_ref, sealed_at):
    """Persist the executed sealed plan as durable content-addressed facts (D1a).

    gap⑤: the compiled logical_dag/physical were returned but never persisted, so
    the workbench display graph had no authoritative execution plan to read. Both
    documents are sealed under ``scratch/chain-sealed-plans/<sha256>.json`` (atomic,
    bounded, idempotent) and an attempt-independent F1 reference is bound through the
    existing ``runtime_sources`` slot (zero journal schema change). The single compile
    seam covers the local/ray/queue callers; the return signature is unchanged.

    F1 omits ``attempt`` (bind_source is immutable; an attempt field would make resume
    re-compilation differ and falsely refuse recovery) and omits ``identity_bridge_ref``
    (the D1b bridge is a separate side-car binding key, so it never mutates this F1).
    """
    from .state_artifacts import atomic_bytes

    def archive(value):
        raw = document(value, MAX_SEALED_PLAN_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        path = run.directory / "scratch" / "chain-sealed-plans" / (digest + ".json")
        # Unregistered, mirroring the K1 manifest: clean_orphans deletes registered
        # protected=0 artifacts at run exit, but the workbench audits these post-run.
        with run.journal.publication_guard(run.fence):
            if not path.exists():
                atomic_bytes(path, raw)
            elif path.read_bytes() != raw:
                raise RecoveryContractError("Existing sealed-plan artifact is corrupt")
        return {"sha256": digest, "path": str(path.relative_to(run.directory)),
                "bytes": len(raw)}

    facts = {"schema": SEALED_PLAN_FACTS_SCHEMA, "run_id": run.ref.run_id,
             "action_id": action.action_id, "dag_id": logical_dag["dag_id"],
             "plan_id": physical["plan_id"], "artifact_manifest_ref": manifest_ref,
             "logical_dag_ref": archive(logical_dag),
             "physical_plan_ref": archive(physical), "sealed_at": sealed_at}
    run.journal.bind_source(run.fence, action.action_id + "-sealed-plan", facts)
    return facts


def _seal_chain_bindings(run, action, logical_dag, physical, source_reference,
                         plan, plan_ref, mref, *, supported_slice="chain_v1"):
    """Assemble §3 inputs and seal through B's single-authority binder."""
    from .sealed_plans import seal_execution_bindings
    nodes = logical_dag["nodes"]
    dag_id = logical_dag["dag_id"]
    operation_nodes = nodes[1:-1]
    op_stage_ids = [identity([dag_id, n["occurrence_id"]]) for n in operation_nodes]
    profile_doc = {"requests": {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}}
    profile_ref = identity(profile_doc)
    programs = [{
        "group_id": "chain-group", "stage_ids": op_stage_ids,
        "plan_ref": plan_ref,
        "sealed_operators": [{"logical_node_id": n["logical_node_id"],
                              "occurrence_id": n["occurrence_id"]} for n in operation_nodes],
        "operator_bindings": [{"program_index": i, "stage_id": sid}
                              for i, sid in enumerate(op_stage_ids)],
    }]
    driver_stage_ids = [identity([dag_id, nodes[0]["occurrence_id"]]),
                        identity([dag_id, nodes[-1]["occurrence_id"]])]
    source_kind = ("queue_declaration" if supported_slice == "queue_transform_v1"
                   else "fixed_source")
    if source_kind == "queue_declaration":
        src_ref = {"source_id": source_reference["source_id"],
                   "source_identity": source_reference["source_id"]}
    else:
        src = logical_dag["sources"][0]
        src_ref = {"source_id": src["source_id"],
                   "source_identity_ref": src["source_identity_ref"]}
    source_bindings = [{"stage_id": driver_stage_ids[0], "kind": source_kind, "ref": src_ref}]
    scheme_version = logical_dag["sources"][0]["partition_scheme"]["scheme_version"]
    from ..planning.dag import channel_id_for
    channel_options = [{"channel_id": channel_id_for(e["edge_id"], scheme_version),
                        "logical_edge_ids": [e["edge_id"]]} for e in logical_dag["edges"]]
    return seal_execution_bindings(
        run, action, logical_dag=logical_dag, physical_plan=physical,
        profiles={profile_ref: profile_doc}, programs=programs,
        driver_stage_ids=driver_stage_ids, source_bindings=source_bindings,
        channel_options=channel_options,
        checkpoint_rows=run.limits["checkpoint_rows"], supported_slice=supported_slice)


def _seal_chain_identity_bridge(run, action, logical_dag, source, plan):
    """Map report observation keys to sealed DAG nodes and seal the bridge (C).

    Returns the bridge ref or None when no checked report context is active.
    """
    from .checked import current
    from .identity_bridge import build_identity_bridge, seal_identity_bridge
    checked = current()
    if checked is None:
        return None
    resolved = checked.execution_bindings(source, plan, action_id=action.action_id)
    if not resolved:
        return None
    nodes = logical_dag["nodes"]
    bridge_bindings = []
    for item in resolved:
        role = item["role"]
        if role == "source":
            node = nodes[0]
        elif role == "terminal":
            node = nodes[-1]
        else:
            node = nodes[item["program_index"] + 1]
        bridge_bindings.append({
            "report_node_id": item["report_node_id"],
            "report_occurrence": item["report_occurrence"],
            "logical_node_id": node["logical_node_id"],
            "occurrence_id": node["occurrence_id"],
            "mode": item["mode"],
        })
    if not checked.report_digest:
        return None
    bridge = build_identity_bridge(logical_dag, action_id=action.action_id,
                                   report_digest=checked.report_digest, bindings=bridge_bindings)
    return seal_identity_bridge(run, action, bridge)


def _build_sealed_manifest(run, plan_ref, *, entrypoint="chain"):
    """Build/seal/save the K1 manifest; return (manifest, manifest_ref).

    The entrypoint definition_ref binds the code-pin (the actually-executed
    business code), not the mutable pickle reference; location points at the
    sealed source artifact. bundle_digest is plan_ref's sha256: the pickle digest
    for chain/graph delivery, the code-pin identity for the in-process write seam.
    Sealed with the run's persisted birth epoch so the ref is stable across
    driver restarts (manifest_id excludes sealed_at; the storage digest binds it).
    """
    from .contracts.artifact import build_manifest, save_manifest, seal_manifest
    config = run.journal.recovery_identity()
    code_pin_digest = plan_ref["code_pin_digest"]
    code_location = (plan_ref["code_sources"][0]["path"] if plan_ref["code_sources"]
                     else plan_ref["path"])
    manifest = build_manifest(
        run_id=run.ref.run_id,
        platform={"source_digest": config["platform_revision"], "distribution": "demiflow",
                  "version": "u2", "runtime_identity_ref": identity([run.ref.run_id, "runtime"])},
        business={"bundle_digest": "sha256:" + plan_ref["sha256"],
                  "entrypoints": [{"name": entrypoint, "module_key": "p_" + plan_ref["sha256"][:24],
                                   "definition_ref": {"sha256": code_pin_digest,
                                                      "location": code_location}}]},
        config={"resolved_config_digest": identity([run.ref.run_id, "config"]),
                "model_config_refs": []},
        environment={"requirements_sha256": identity(["requirements"]),
                     "lock_digest": identity(["lock"]), "resolution_digest": identity(["resolution"])},
        protocol_versions={"worker": {"major": 1, "minor": 0},
                           "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1,
                           "resource_protocol": "1.0"},
        secret_refs=[])
    manifest = seal_manifest(manifest, int(config["started_at"] * 1000))
    ref = save_manifest(run.directory, manifest)
    return manifest, {"manifest_id": ref["manifest_id"], "sha256": ref["sha256"]}


def compile_chain_topology(action, source, plan, source_reference, *,
                           output_child_id, host_id, plan_ref,
                           supported_slice="chain_v1",
                           terminal_kind="materialize", sink_ids=("output",),
                           entrypoint="chain"):
    """Compile the E2 linear DAG/physical plan and seal the K1 manifest (D3).

    The whole operator chain runs in one worker host, so the source-progress
    vector is a single component and the result-out channel is the terminal edge.
    Worker counts live only in the physical worker group, never in logical ids.
    """
    from ..planning.dag import compile_logical_dag, compile_physical_execution_plan
    from .exchange import read_domain
    run = action.context
    domain = read_domain(run.directory, host_id)
    logical_dag = compile_logical_dag(action_id=action.action_id, source=source,
                                      operations=plan.operations,
                                      terminal_kind=terminal_kind, sink_ids=sink_ids)
    manifest, mref = _build_sealed_manifest(run, plan_ref, entrypoint=entrypoint)
    op_nodes = logical_dag["nodes"][1:-1]
    stage_refs = [identity([logical_dag["dag_id"], node["occurrence_id"]])
                  for node in op_nodes]
    profile_doc = {"requests": {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}}
    row_bytes = run.limits["max_row_bytes"] + 256
    queue_bytes = max(row_bytes, run.limits["max_batch_bytes"])
    stage_budgets = {node["occurrence_id"]: {
        "inflight_rows": run.limits["batch_rows"], "inflight_bytes": queue_bytes,
        "output_bytes": row_bytes} for node in op_nodes}
    physical = compile_physical_execution_plan(
        logical_dag=logical_dag, plan_revision=0, artifact_manifest_ref=mref,
        worker_groups=[{"group_id": "chain-group", "role": "cpu_prepare",
                        "profile_ref": identity(profile_doc), "stage_refs": stage_refs,
                        "min_workers": 1, "initial_workers": 1, "max_workers": 1}],
        recovery={"checkpoint_interval_ms": run.limits["checkpoint_interval_ms"],
                  "unconfirmed_rows": run.limits["checkpoint_unconfirmed_rows"],
                  "unconfirmed_bytes": run.limits["checkpoint_unconfirmed_bytes"],
                  "terminals": []},
        feasibility_snapshot_ref=identity([run.ref.run_id, "feasibility"]),
        read_domain=domain, durability_declared="durable",
        partition_count=1, scheme_kind="single", stage_budgets=stage_budgets)
    operators = [{"logical_node_id": node["logical_node_id"],
                  "occurrence_id": node["occurrence_id"]}
                 for node in op_nodes]
    topology = {"run_id": run.ref.run_id, "action_id": output_child_id,
                "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                "plan_revision": 0, "artifact_manifest_ref": mref,
                "source_id": (source_reference["source_id"] if source_reference is not None
                              else logical_dag["sources"][0]["source_id"]),
                "channel_id": physical["channels"][-1]["channel_id"],
                "operators": operators, "sink_id": sink_ids[0]}
    seal_plan_facts(run, action, logical_dag, physical, mref, manifest["sealed_at"])
    bindings_ref = _seal_chain_bindings(
        run, action, logical_dag, physical, source_reference, plan, plan_ref, mref,
        supported_slice=supported_slice)
    topology["bindings_ref"] = bindings_ref
    bridge_ref = _seal_chain_identity_bridge(run, action, logical_dag, source, plan)
    if bridge_ref is not None:
        topology["identity_bridge_ref"] = bridge_ref
    return topology, logical_dag, physical


class _WriteSeal:
    """Pre-publication binding gate for the native write_lance seam (route b).

    Holds the sealed plan contract. ``verify`` re-derives the code-pin and
    operator identities from the actually-executed plan and fail-closes on any
    drift before publication; ``layout`` reuses the worker layout reader (import
    only, no copied extraction) to recover the checkpoint policy and stage limits
    that the streamed path consumes. A v2 native seal binds the driver execution
    instead: there is no worker group to load, so both gates read the sealed
    placement rather than inventing one.
    """

    def __init__(self, action, plan_ref, topology, *, source=None,
                 native_layout=None, native_execution_id=None):
        self._action = action
        self._plan_ref = plan_ref
        self.topology = topology
        self.bindings_ref = topology["bindings_ref"]
        self._source = source
        self._native_layout = native_layout
        self._native_execution_id = native_execution_id
        self._native_placement = None
        if native_layout is None:
            if native_execution_id is not None:
                raise RecoveryContractError("A native execution id requires its sealed layout")
            return
        if native_execution_id is None:
            raise RecoveryContractError("A native layout requires its sealed execution id")
        if source is None:
            raise RecoveryContractError("Native write verification requires the fixed source")
        matches = [entry for entry in native_layout.executions
                   if entry.execution_id == native_execution_id]
        if len(matches) != 1:
            raise RecoveryContractError("Sealed native execution id is absent or ambiguous")
        if matches[0].placement != "driver":
            raise RecoveryContractError("Native write binds driver executions only")
        self._native_placement = matches[0]

    def verify(self, plan):
        if self._native_placement is not None:
            from .native_plan import verify_native_programs
            verify_native_programs(self._source, plan,
                                   programs=self._native_placement.programs,
                                   action_id=self._action.action_id)
            return
        verify_loaded_plan(plan, self._plan_ref, self.topology["operators"])

    def layout(self):
        if self._native_placement is not None:
            placement = self._native_placement
            stage_limits = {}
            for program in placement.programs:
                # The streamed stage machine indexes root-plan operations only;
                # sub-program budgets are consumed by the native backend itself.
                if list(program["input_path"]) != []:
                    continue
                for binding in program["operator_bindings"]:
                    stage_id = binding["stage_id"]
                    if stage_id not in placement.stage_limits:
                        raise RecoveryContractError("Sealed native stage budget is missing")
                    stage_limits[binding["program_index"]] = dict(placement.stage_limits[stage_id])
            return self._native_layout.checkpoint, stage_limits
        from .worker.layout import load_worker_layout
        launch_spec = {"execution_plan": {"action_id": self._action.action_id,
                       "group_id": "chain-group", "bindings_ref": self.bindings_ref}}
        _, checkpoint_policy, stage_limits = load_worker_layout(
            launch_spec, self._action.context, None)
        return checkpoint_policy, stage_limits


def seal_write_topology(action, source, plan, *, host_id="local", decision=None):
    """Compile + seal the executed write_lance plan under the parent action (D1a).

    Route (b): native execution and the single LanceSink publication are unchanged;
    this adds the F1 plan facts, execution bindings and identity bridge above them,
    plus a pre-publication binding gate. The seal binds the frozen source + the
    LogicalPlan that actually execute — not the discarded native ``physical`` of the
    relational branch. Deferred branches (pure Lance copy, arrow_batch_sink) never
    call this, so the D reader honestly reports no sealed plan for them.

    ``decision`` is additive: a native driver route seals execution_bindings.v2 and
    every other route keeps the v1 chain seal. On recovery the sealed bindings stay
    the authority — a new decision only has to agree with them and can never
    replace the historical backend.
    """
    run = action.context

    def root_program(programs):
        for program in programs:
            if list(program["input_path"]) == []:
                return program
        return None

    if run.journal.source_binding(action.action_id + "-sealed-plan") is not None:
        from ..planning.dag import compile_logical_dag
        from .plan_interpreter import interpret_physical_plan, to_wire
        from .sealed_plans import load_execution_bindings, load_sealed_plan
        sealed = load_sealed_plan(run.directory, action.action_id)
        bindings = load_execution_bindings(run.directory, action.action_id, sealed_plan=sealed)
        logical_dag, physical = sealed["logical_dag"], sealed["physical_plan"]
        if bindings["document"]["schema"] == "demiflow.execution_bindings.v2":
            from ..planning.dataset_dag import compile_dataset_logical_dag
            from .native_plan import verify_native_programs
            from .seal.plan import load_native_execution
            drivers = [entry for entry in bindings["document"]["executions"]
                       if entry["placement"] == "driver"]
            if len(drivers) != 1:
                raise RecoveryContractError("Sealed native write requires one driver execution")
            execution_id = drivers[0]["execution_id"]
            if decision is not None:
                if decision.backend != drivers[0]["backend"]:
                    raise RecoveryContractError(
                        "Native write decision backend differs from the sealed backend")
                if identity([action.action_id, "driver", decision.route]) != execution_id:
                    raise RecoveryContractError(
                        "Native write decision route differs from the sealed execution")
            layout, placement = load_native_execution(
                run, action.action_id, execution_id=execution_id)
            verify_native_programs(source, plan, programs=placement.programs,
                                   action_id=action.action_id)
            actual, _ = compile_dataset_logical_dag(
                action_id=action.action_id, source=source, plan=plan,
                terminal_kind="write_lance", sink_ids=("lance",))
            if actual != logical_dag:
                raise RecoveryContractError("Native write logical plan differs from the sealed plan")
            program = root_program(placement.programs)
            plan_ref = None if program is None else to_wire(program["plan_ref"])
            operators = [] if program is None else to_wire(program["sealed_operators"])
            topology = {"run_id": run.ref.run_id, "action_id": action.action_id,
                        "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                        "plan_revision": physical["plan_revision"],
                        "artifact_manifest_ref": sealed["facts"]["artifact_manifest_ref"],
                        "source_id": logical_dag["sources"][0]["source_id"],
                        "channel_id": physical["channels"][-1]["channel_id"],
                        "operators": operators, "sink_id": "lance", "bindings_ref": bindings["ref"]}
            bridge_ref = _seal_chain_identity_bridge(run, action, logical_dag, source, plan)
            if bridge_ref is not None:
                topology["identity_bridge_ref"] = bridge_ref
            return _WriteSeal(action, plan_ref, topology, source=source,
                              native_layout=layout, native_execution_id=execution_id)
        layout = interpret_physical_plan(logical_dag, physical, bindings=bindings,
                                         manifest_ref=sealed["facts"]["artifact_manifest_ref"])
        if (layout.supported_slice != "chain_v1" or len(layout.workers) != 1
                or layout.workers[0].group_id != "chain-group"):
            raise RecoveryContractError("Sealed native write layout differs")
        placement = layout.workers[0]
        plan_ref = to_wire(placement.program_ref)
        operators = to_wire(placement.sealed_operators)
        # Native writes execute this in-memory plan, not the transient pickle.
        # Its bytes may vary across processes despite identical business code.
        # Verify that code and the full logical identity against the original
        # seal; never replace its manifest, bindings or program reference.
        verify_loaded_plan(plan, plan_ref, operators)
        actual = compile_logical_dag(action_id=action.action_id, source=source,
            operations=plan.operations, terminal_kind="write_lance", sink_ids=("lance",))
        if actual != logical_dag:
            raise RecoveryContractError("Native write logical plan differs from the sealed plan")
        topology = {"run_id": run.ref.run_id, "action_id": action.action_id,
                    "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                    "plan_revision": physical["plan_revision"],
                    "artifact_manifest_ref": sealed["facts"]["artifact_manifest_ref"],
                    "source_id": logical_dag["sources"][0]["source_id"],
                    "channel_id": physical["channels"][-1]["channel_id"],
                    "operators": operators, "sink_id": "lance", "bindings_ref": bindings["ref"]}
        bridge_ref = _seal_chain_identity_bridge(run, action, logical_dag, source, plan)
        if bridge_ref is not None:
            topology["identity_bridge_ref"] = bridge_ref
        return _WriteSeal(action, plan_ref, topology)
    if decision is not None and decision.route in ("native_relational", "arrow_batch", "lance_scan"):
        from .native_plan import compile_native_topology
        from .seal.plan import load_native_execution
        from .plan_interpreter import to_wire
        logical_dag, physical, bindings_ref = compile_native_topology(
            action, source, plan, decision=decision)
        execution_id = identity([action.action_id, "driver", decision.route])
        layout, placement = load_native_execution(
            run, action.action_id, execution_id=execution_id)
        program = root_program(placement.programs)
        plan_ref = None if program is None else to_wire(program["plan_ref"])
        operators = [] if program is None else to_wire(program["sealed_operators"])
        topology = {"run_id": run.ref.run_id, "action_id": action.action_id,
                    "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                    "plan_revision": physical["plan_revision"],
                    "artifact_manifest_ref": to_wire(layout.artifact_manifest_ref),
                    "source_id": logical_dag["sources"][0]["source_id"],
                    "channel_id": physical["channels"][-1]["channel_id"],
                    "operators": operators, "sink_id": "lance", "bindings_ref": bindings_ref}
        bridge_ref = _seal_chain_identity_bridge(run, action, logical_dag, source, plan)
        if bridge_ref is not None:
            topology["identity_bridge_ref"] = bridge_ref
        return _WriteSeal(action, plan_ref, topology, source=source,
                          native_layout=layout, native_execution_id=execution_id)
    plan_ref = seal_write_plan_ref(action, plan)
    # The write bindings are a linear chain (single group, fixed source, single
    # partition, forward channels), so they reuse B's chain_v1 slice unchanged;
    # the write_lance terminal kind is carried by the logical_dag, not the slice.
    topology, _, _ = compile_chain_topology(
        action, source, plan, None, output_child_id=action.action_id,
        host_id=host_id, plan_ref=plan_ref, supported_slice="chain_v1",
        terminal_kind="write_lance", sink_ids=("lance",), entrypoint="write_lance")
    return _WriteSeal(action, plan_ref, topology)


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
