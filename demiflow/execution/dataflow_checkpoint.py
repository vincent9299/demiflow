"""Driver-side global cut orchestration for the unified dataflow runtime (D).

``DataflowCheckpointCoordinator`` is the single driver owner of cut sequencing,
the fixed participant table and the prepared/unknown/confirmed image. It is an
orchestration *consumer* of the native :class:`CommitCoordinator`: every durable
prepare/publish/receipt/ACK still flows through the one native RunJournal. This
module keeps no second persistent ledger and opens no database of its own.

The frozen v3 source vector lives in the committed states document under the
reserved key ``dataflow.sources.v3``. Because the native state-artifact identifier
alphabet excludes ``.``, that reserved key is carried inside the native-safe
``dataflow-cut-v3`` states carrier; the reserved name itself is preserved verbatim.
The scalar :class:`SourcePosition` handed to the native coordinator is a fixed
virtual source (``identity(['dataflow-vector', action_id])``) whose ordinal is the
cut sequence — never a business row number. The real per-source cursors are the
vector, and recovery must read them rather than skip sources by the scalar.
"""
from __future__ import annotations

import threading
import weakref

from .commit import CommitCoordinator
from .contracts.dataflow import (
    ArtifactRef, CutContribution, CutRequest, PhysicalDataflow, RecoveryImage,
    SourceCursor,
)
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import (
    MAX_SINKS, MAX_STATES, RecoveryContractError, SourcePosition, document,
    identifier, identity,
)
from .run_context import RunAction
from .state_artifacts import StateArtifacts

# Reserved vector slot named by ARCHITECTURE §6. It is nested inside the
# native-safe carrier below because native state identifiers forbid '.'.
SOURCE_VECTOR_KEY = "dataflow.sources.v3"
_CUT_STATE_KEY = "dataflow-cut-v3"
_CUT_STATE_SCHEMA = "demiflow.dataflow_cut.v3"
_MAX_PENDING_CUTS = 8
_MAX_CONTRIBUTION_REFS = 4096

# One live coordinator per action in this process; the native run lease already
# excludes a second driver process. Weak values so a closed owner is not pinned.
_OWNERS: "weakref.WeakValueDictionary[str, DataflowCheckpointCoordinator]" = (
    weakref.WeakValueDictionary())
_OWNERS_LOCK = threading.Lock()


def _artifact_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes,
            "store_id": ref.store_id, "key": ref.key, "media_type": ref.media_type}


def _artifact_from_json(value) -> ArtifactRef:
    if type(value) is not dict or set(value) != {
            "digest", "size_bytes", "store_id", "key", "media_type"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid artifact reference in recovery image")
    return ArtifactRef(digest=value["digest"], size_bytes=value["size_bytes"],
                       store_id=value["store_id"], key=value["key"],
                       media_type=value["media_type"])


def _cursor_json(cursor: SourceCursor) -> dict:
    from .contracts.dataflow import thaw_record_value
    return {"split_id": cursor.split_id, "offset": thaw_record_value(cursor.offset),
            "next_sequence": cursor.next_sequence, "end_of_input": cursor.end_of_input,
            "lease_ref": _artifact_json(cursor.lease_ref) if cursor.lease_ref is not None else None,
            "pending_refs": [_artifact_json(r) for r in cursor.pending_refs]}


def _cursor_from_json(value) -> SourceCursor:
    if type(value) is not dict or set(value) != {
            "split_id", "offset", "next_sequence", "end_of_input", "lease_ref", "pending_refs"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid source cursor in recovery image")
    return SourceCursor(
        split_id=value["split_id"], offset=value["offset"],
        next_sequence=value["next_sequence"], end_of_input=value["end_of_input"],
        lease_ref=_artifact_from_json(value["lease_ref"]) if value["lease_ref"] is not None else None,
        pending_refs=tuple(_artifact_from_json(r) for r in value["pending_refs"]))


def _cut_json(cut: CutRequest) -> dict:
    return {"cut_id": cut.cut_id, "sequence": cut.sequence,
            "plan_revision": cut.plan_revision, "route_epoch": cut.route_epoch,
            "participants": list(cut.participants), "final": cut.final}


def _cut_from_json(value) -> CutRequest:
    if type(value) is not dict or set(value) != {
            "cut_id", "sequence", "plan_revision", "route_epoch", "participants", "final"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid cut request in recovery image")
    return CutRequest(cut_id=value["cut_id"], sequence=value["sequence"],
                      plan_revision=value["plan_revision"], route_epoch=value["route_epoch"],
                      participants=tuple(value["participants"]), final=value["final"])


def _contribution_json(contribution: CutContribution) -> dict:
    from .contracts.dataflow import thaw_record_value
    return {"cut_id": contribution.cut_id, "participant_id": contribution.participant_id,
            "boot_id": contribution.boot_id,
            "sources": [_cursor_json(c) for c in contribution.sources],
            "operator_states": {k: _artifact_json(v) for k, v in contribution.operator_states.items()},
            "channels": thaw_record_value(contribution.channels),
            "buffers": [_artifact_json(r) for r in contribution.buffers],
            "effects": [_artifact_json(r) for r in contribution.effects],
            "sinks": {k: _artifact_json(v) for k, v in contribution.sinks.items()},
            "final": contribution.final}


def _contribution_from_json(value) -> CutContribution:
    if type(value) is not dict or set(value) != {
            "cut_id", "participant_id", "boot_id", "sources", "operator_states",
            "channels", "buffers", "effects", "sinks", "final"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid cut contribution in recovery image")
    return CutContribution(
        cut_id=value["cut_id"], participant_id=value["participant_id"],
        boot_id=value["boot_id"],
        sources=tuple(_cursor_from_json(c) for c in value["sources"]),
        operator_states={k: _artifact_from_json(v) for k, v in value["operator_states"].items()},
        channels={k: dict(v) for k, v in value["channels"].items()},
        buffers=tuple(_artifact_from_json(r) for r in value["buffers"]),
        effects=tuple(_artifact_from_json(r) for r in value["effects"]),
        sinks={k: _artifact_from_json(v) for k, v in value["sinks"].items()},
        final=value["final"])


def _contribution_digest(contribution: CutContribution) -> str:
    return identity(_contribution_json(contribution))


def _validate_ref(ref, *, what: str) -> None:
    if not isinstance(ref, ArtifactRef):
        raise DataflowError("INVALID_CONTRACT", what + " must be a durable ArtifactRef")
    if (type(ref.digest) is not str or len(ref.digest) != 64
            or any(c not in "0123456789abcdef" for c in ref.digest)):
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid digest")
    if type(ref.key) is not str or not ref.key or ".." in ref.key.split("/"):
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid store key")
    if type(ref.size_bytes) is not int or ref.size_bytes < 0:
        raise DataflowError("INVALID_CONTRACT", what + " has an invalid size")


class _PendingCut:
    __slots__ = ("request", "contributions", "digests", "boots", "aborted", "residency", "inherited")

    def __init__(self, request: CutRequest):
        self.request = request
        self.contributions: dict[str, CutContribution] = {}
        self.digests: dict[str, str] = {}
        self.boots: dict[str, str] = {}
        self.aborted = False
        self.residency = None
        self.inherited = ()


class DataflowCheckpointCoordinator:
    """The driver's single global cut/commit orchestrator (C08).

    Persistence is delegated to the native ``CommitCoordinator``; this class only
    sequences cuts, admits contributions from the current boot, and assembles the
    recovery image. Workers cannot construct it: it requires a live driver
    ``RunAction`` and only one instance may own an action at a time.
    """

    __slots__ = ("_action", "_physical", "_store", "_commit", "_artifacts",
                 "_participants", "_route_epoch", "_lock", "_sequence",
                 "_pending", "_last_acked_sequence", "_source_eoi", "_operator_complete", "_closed",
                 "_fence", "_owner_key", "residency", "_last_image", "__weakref__")

    def __init__(self, action: RunAction, physical: PhysicalDataflow, *, store) -> None:
        if not isinstance(action, RunAction):
            raise DataflowError("INVALID_CONTRACT",
                                "Checkpoint coordinator requires a driver RunAction")
        context = action.context
        if not getattr(context, "active", False) or getattr(context, "journal", None) is None:
            raise DataflowError("FENCED", "Checkpoint coordinator requires the live driver context")
        if not isinstance(physical, PhysicalDataflow):
            raise DataflowError("INVALID_CONTRACT", "Checkpoint coordinator requires a PhysicalDataflow")
        from .contracts.dataflow_codec import compute_participant_id
        participants = []
        for stage in physical.stages:
            for partition in stage.partitions:
                participants.append(compute_participant_id(stage.stage_id, partition))
        if not participants:
            raise DataflowError("INVALID_CONTRACT", "Physical plan has no cut participants")
        if len(participants) != len(set(participants)):
            raise DataflowError("INVALID_CONTRACT", "Duplicate cut participant identity")
        if len(participants) > MAX_STATES:
            raise DataflowError("BUDGET_EXCEEDED", "Cut participant count exceeds state budget")
        self._owner_key = (context.ref.run_id, action.action_id)
        with _OWNERS_LOCK:
            existing = _OWNERS.get(self._owner_key)
            if existing is not None and not existing._closed:
                raise DataflowError("INVALID_CONTRACT",
                                    "Action already has a live checkpoint coordinator")
            _OWNERS[self._owner_key] = self
        self._action = action
        self._physical = physical
        self._store = store
        self._commit = CommitCoordinator(action)
        self._artifacts = StateArtifacts(context)
        self._participants = tuple(participants)
        self._route_epoch = max((e.route_epoch for e in physical.exchanges), default=0)
        self._lock = threading.RLock()
        self._sequence = 0
        self._pending: dict[str, _PendingCut] = {}
        self._last_acked_sequence = 0
        self._source_eoi: dict[str, bool] = {}
        self._operator_complete = set()
        self._closed = False
        self._fence = context.fence
        self.residency = None
        self._last_image = None

    # -- internal helpers -------------------------------------------------
    def _check_open(self) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "Checkpoint coordinator is closed")
        if self._action.context.fence != self._fence:
            raise DataflowError("FENCED", "Checkpoint coordinator fence is stale")

    def _merge_source_vector(self, contributions) -> tuple[SourceCursor, ...]:
        by_split: dict[str, SourceCursor] = {}
        for contribution in contributions:
            for cursor in contribution.sources:
                prior = by_split.get(cursor.split_id)
                if prior is not None and prior != cursor:
                    raise DataflowError("SOURCE_CHANGED",
                                        "Conflicting source cursors for one split in a cut")
                by_split[cursor.split_id] = cursor
        return tuple(by_split[k] for k in sorted(by_split))

    def _record_eoi(self, contributions) -> None:
        for contribution in contributions:
            for cursor in contribution.sources:
                if cursor.end_of_input:
                    self._source_eoi[cursor.split_id] = True

    def _sources_exhausted(self) -> bool:
        # A zero-source (literal_empty) plan is trivially exhausted. Otherwise every
        # declared source split must have reported end_of_input.
        splits = {s.split_id for s in self._physical.splits}
        if not splits:
            return True
        return all(self._source_eoi.get(split, False) for split in splits)

    def observe_source_eoi(self, cursors):
        """Driver has authenticated the worker and exact split ownership first."""
        declared = {s.split_id for s in self._physical.splits}
        with self._lock:
            self._check_open()
            for cursor in cursors:
                if cursor.split_id not in declared:
                    raise DataflowError("FENCED", "EOI names an undeclared source split")
                if cursor.end_of_input:
                    self._source_eoi[cursor.split_id] = True

    def observe_input_satisfied(self, split_ids):
        """Driver proved every outgoing path was closed by a sealed limit."""
        declared = {s.split_id for s in self._physical.splits}
        if not set(split_ids) <= declared:
            raise DataflowError("FENCED", "input completion names undeclared splits")
        self._operator_complete = getattr(self, "_operator_complete", set()) | set(split_ids)

    def inputs_complete(self):
        closed = getattr(self, "_operator_complete", set())
        return all(self._source_eoi.get(s.split_id, False) or s.split_id in closed
                   for s in self._physical.splits)

    # -- D002 -------------------------------------------------------------
    def begin(self, *, final: bool, participants=None, residency=None) -> CutRequest:
        with self._lock:
            self._check_open()
            if type(final) is not bool:
                raise DataflowError("INVALID_CONTRACT", "final must be a bool")
            if final and not self.inputs_complete():
                raise DataflowError("INVALID_CONTRACT",
                                    "final cut requires end-of-input evidence for every source")
            if len(self._pending) >= _MAX_PENDING_CUTS:
                raise DataflowError("BUDGET_EXCEEDED", "Too many pending cuts")
            selected = self._participants if participants is None else tuple(participants)
            if not selected or len(set(selected)) != len(selected) or not set(selected) <= set(self._participants):
                raise DataflowError("INVALID_CONTRACT", "invalid active cut participant set")
            if selected != self._participants and residency is None:
                raise DataflowError("INVALID_CONTRACT", "partial participants require a residency proof")
            sequence = self._sequence + 1
            cut_id = identity(["dataflow-cut-v3", self._action.action_id,
                               self._physical.plan_revision, self._route_epoch, sequence])
            request = CutRequest(cut_id=cut_id, sequence=sequence,
                                 plan_revision=self._physical.plan_revision,
                                 route_epoch=self._route_epoch,
                                 participants=selected, final=final)
            pending = _PendingCut(request)
            if residency is not None:
                document(residency, self._physical.bounds.max_state_bytes)
                import copy
                pending.residency = copy.deepcopy(residency)
                pending.inherited = tuple(c for c in self._last_image.contributions
                    if c.participant_id not in selected) if self._last_image is not None else ()
            self._pending[cut_id] = pending
            self._sequence = sequence
            return request

    # -- D003 -------------------------------------------------------------
    def contribute(self, contribution: CutContribution) -> None:
        with self._lock:
            self._check_open()
            if not isinstance(contribution, CutContribution):
                raise DataflowError("INVALID_CONTRACT", "contribute requires a CutContribution")
            pending = self._pending.get(contribution.cut_id)
            if pending is None or pending.aborted:
                raise DataflowError("INVALID_CONTRACT", "Contribution for an unknown or aborted cut")
            if contribution.participant_id not in pending.request.participants:
                raise DataflowError("INVALID_CONTRACT", "Contribution from a non-participant")
            if type(contribution.boot_id) is not str or not contribution.boot_id:
                raise DataflowError("FENCED", "Contribution must carry the current boot identity")
            prior_boot = pending.boots.get(contribution.participant_id)
            if prior_boot is not None and prior_boot != contribution.boot_id:
                raise DataflowError("FENCED",
                                    "Participant contributed from a different boot in one cut")
            digest = _contribution_digest(contribution)
            prior_digest = pending.digests.get(contribution.participant_id)
            if prior_digest is not None:
                if prior_digest == digest:
                    return  # same cut, same content: idempotent
                raise DataflowError("INVALID_CONTRACT",
                                    "Conflicting contribution content for one participant")
            self._validate_contribution_bounds(contribution)
            pending.contributions[contribution.participant_id] = contribution
            pending.digests[contribution.participant_id] = digest
            pending.boots[contribution.participant_id] = contribution.boot_id

    def _validate_contribution_bounds(self, contribution: CutContribution) -> None:
        refs = 0
        for ref in contribution.operator_states.values():
            _validate_ref(ref, what="operator state"); refs += 1
        for ref in contribution.buffers:
            _validate_ref(ref, what="buffer"); refs += 1
        for ref in contribution.effects:
            _validate_ref(ref, what="effect"); refs += 1
        for ref in contribution.sinks.values():
            _validate_ref(ref, what="prepared sink"); refs += 1
        for cursor in contribution.sources:
            if cursor.lease_ref is not None:
                _validate_ref(cursor.lease_ref, what="source lease"); refs += 1
            for ref in cursor.pending_refs:
                _validate_ref(ref, what="pending source"); refs += 1
        if refs > _MAX_CONTRIBUTION_REFS:
            raise DataflowError("BUDGET_EXCEEDED", "Contribution reference count exceeds budget")
        if len(contribution.sinks) > MAX_SINKS:
            raise DataflowError("BUDGET_EXCEEDED", "Contribution sink count exceeds budget")
        # Channel snapshots must be bounded JSON, never live handles.
        document({k: dict(v) for k, v in contribution.channels.items()},
                 self._physical.bounds.max_state_bytes)

    # -- D004 -------------------------------------------------------------
    def commit(self, cut_id: str, *, sinks) -> RecoveryImage:
        from .dataflow_sinks import ack_source_claims, combine_prepared_sinks
        with self._lock:
            self._check_open()
            pending = self._pending.get(cut_id)
            if pending is None or pending.aborted:
                raise DataflowError("INVALID_CONTRACT", "commit of an unknown or aborted cut")
            request = pending.request
            missing = [p for p in request.participants if p not in pending.contributions]
            if missing:
                raise DataflowError("INVALID_CONTRACT",
                                    "cut is incomplete; missing participants: " + ",".join(sorted(missing)))
            fresh = tuple(pending.contributions[p] for p in request.participants)
            contributions = pending.inherited + fresh
            if request.final and {c.participant_id for c in contributions} != set(self._participants):
                raise DataflowError("INVALID_CONTRACT", "final cut lacks actual contributions from every stage")
            if request.final:
                for contribution in contributions:
                    for cursor in contribution.sources:
                        if (not cursor.end_of_input and cursor.split_id not in self._operator_complete
                                and cursor.split_id in {s.split_id for s in self._physical.splits}):
                            raise DataflowError("INVALID_CONTRACT",
                                                "final cut committed without full source EOI")
            source_vector = self._merge_source_vector(contributions)
            image = RecoveryImage(commit_id="0" * 64, cut=request, contributions=contributions,
                                  confirmed_sinks={}, source_vector=source_vector)
            position = source_vector_position(image, action_id=self._action.action_id)
            states = {_CUT_STATE_KEY: {
                "schema": _CUT_STATE_SCHEMA,
                SOURCE_VECTOR_KEY: [_cursor_json(c) for c in source_vector],
                "cut": _cut_json(request),
                "contributions": [_contribution_json(c) for c in contributions],
            }}
            if pending.residency is not None:
                states[_CUT_STATE_KEY]["residency"] = pending.residency
            outputs = {}
            for sink_id, sink in sinks.items():
                sink_contributions = (contributions if pending.residency is not None and
                    pending.residency.get('schema') == 'demiflow.durable_residency.v1' else fresh)
                refs = tuple(c.sinks[sink_id] for c in sink_contributions if sink_id in c.sinks)
                manifest = combine_prepared_sinks(refs, sink_id=sink_id, services=self._driver_services())
                outputs[sink_id] = (sink, {**manifest, "final": request.final})
            if not outputs:
                raise DataflowError("INVALID_CONTRACT", "A commit requires at least one sink")
            try:
                fact = self._commit.commit(request.sequence, outputs=outputs,
                                           position=position, states=states, acknowledge=None)
            except RecoveryContractError as exc:
                raise DataflowError("INVALID_CONTRACT", str(exc)) from exc
            confirmed = {}
            for sink_id in sinks:
                from .dataflow_images import row_preparation
                prepared_sink = row_preparation(sinks[sink_id], fact["prepared"]["sinks"].get(sink_id))
                if type(prepared_sink) is dict and "ref" in prepared_sink:
                    confirmed[sink_id] = _artifact_from_json(prepared_sink["ref"])
            image = RecoveryImage(commit_id=fact["commit_id"], cut=request,
                                  contributions=contributions, confirmed_sinks=confirmed,
                                  source_vector=source_vector)
            # Queue settlement is the native finalize fact; only verify it, never re-ACK.
            ack_source_claims(image, action=self._action)
            self._record_eoi(contributions)
            del self._pending[cut_id]
            self._last_acked_sequence = request.sequence
            self.residency = pending.residency
            self._last_image = image
            return image

    def _driver_services(self):
        # combine_prepared_sinks only needs the driver store; build the minimal
        # frozen service view without re-deriving any authority.
        from .contracts.dataflow import DriverServices
        return DriverServices(action=self._action, journal_service=None, store=self._store,
                              resources=None, policy=self._physical.policy,
                              bounds=self._physical.bounds, hosts=(),
                              clock_ms=lambda: 0)

    # -- D005 -------------------------------------------------------------
    def recover(self, *, sinks) -> RecoveryImage | None:
        from .dataflow_sinks import ack_source_claims
        with self._lock:
            self._check_open()
            journal = self._action.context.journal
            facts = journal.execution_commits(self._action.action_id)
            # The native v3 cut proof is the committed state carrier, not the
            # program seal: an unsealed v3 cut still reconciles through the journal.
            v3_facts = [f for f in facts
                        if _CUT_STATE_KEY in ((f.get("prepared") or {}).get("state") or {})]
            sealed = journal.source_binding(self._action.action_id + "-dataflow-sealed")
            if not v3_facts:
                if sealed is not None:
                    pending_only = bool(facts) and all(f['status'] == 'pending'
                        and _CUT_STATE_KEY in f['intent']['states'] for f in facts)
                    native = journal.source_binding(self._action.action_id + '-native-model-journal')
                    admitted_create = (not facts and self._action.context._create_target_was_absent(
                        self._action.action_id, self._action.declaration))
                    if pending_only or admitted_create or (not facts and native == {
                            'schema': 'demiflow.native_model_journal.v1',
                            'action_id': self._action.action_id}):
                        # No row publication was permitted. Recompute the initial
                        # cut from sealed source positions, reusing confirmed
                        # native responses; unknown requests are checked first.
                        return None
                    # A v3 action with no journal proof: files alone are not authority.
                    raise DataflowError("INCOMPATIBLE_HISTORY",
                                        "No committed cut with journal proof; cannot recover from files alone")
                return self._recover_historical(sinks)
            try:
                self._commit.recover(sinks, acknowledge=None)
            except RecoveryContractError as exc:
                from .contracts.dataflow_errors import DataflowRecoveryError
                raise DataflowRecoveryError("UNKNOWN_EFFECT", str(exc)) from exc
            facts = journal.execution_commits(self._action.action_id)
            acked = [f for f in facts if f["status"] == "acked"
                     and _CUT_STATE_KEY in ((f.get("prepared") or {}).get("state") or {})]
            if not acked:
                # Files without a journal proof are never a recovery authority.
                raise DataflowError("INCOMPATIBLE_HISTORY",
                                    "No committed cut with journal proof; cannot recover from files alone")
            fact = max(acked, key=lambda f: f["sequence"])
            state_refs = fact["prepared"]["state"]
            if _CUT_STATE_KEY not in state_refs:
                raise DataflowError("CORRUPT_ARTIFACT", "Committed cut has no v3 state carrier")
            carrier = self._artifacts.read(state_refs[_CUT_STATE_KEY])
            if carrier.get("schema") != _CUT_STATE_SCHEMA:
                raise DataflowError("INCOMPATIBLE_HISTORY", "Unsupported cut state schema")
            cut = _cut_from_json(carrier["cut"])
            contributions = tuple(_contribution_from_json(c) for c in carrier["contributions"])
            vector = tuple(_cursor_from_json(c) for c in carrier[SOURCE_VECTOR_KEY])
            residency = carrier.get("residency")
            if residency is None:
                if set(c.participant_id for c in contributions) != set(self._participants):
                    raise DataflowError("CORRUPT_ARTIFACT", "Recovered cut participant set differs")
            elif residency.get('schema') == 'demiflow.durable_residency.v1':
                from .dataflow_durable_residency import validate_recovered
                validate_recovered(self._physical, cut, contributions, residency,
                    facts=acked, artifacts=self._artifacts, current_commit=fact["commit_id"], store=self._store)
            else:
                from .dataflow_residency import validate_recovered_residency
                validate_recovered_residency(self._physical, cut, contributions, residency,
                    facts=acked, artifacts=self._artifacts, current_commit=fact["commit_id"])
            self.residency = residency
            if residency is not None:
                self._operator_complete.update(residency.get('satisfied_source_splits', ()))
            merged = self._merge_source_vector(contributions)
            if merged != vector:
                raise DataflowError("SOURCE_CHANGED", "Recovered source vector disagrees with contributions")
            confirmed = {}
            for sink_id in sinks:
                from .dataflow_images import row_preparation
                prepared_sink = row_preparation(sinks[sink_id], fact["prepared"]["sinks"].get(sink_id))
                if type(prepared_sink) is dict and "ref" in prepared_sink:
                    confirmed[sink_id] = _artifact_from_json(prepared_sink["ref"])
            image = RecoveryImage(commit_id=fact["commit_id"], cut=cut,
                                  contributions=contributions, confirmed_sinks=confirmed,
                                  source_vector=vector)
            source_vector_position(image, action_id=self._action.action_id)
            ack_source_claims(image, action=self._action)
            self._record_eoi(contributions)
            self._sequence = max(self._sequence, cut.sequence)
            self._last_acked_sequence = cut.sequence
            self._last_image = image
            return image

    def _recover_historical(self, sinks) -> RecoveryImage:
        from .dataflow_history import (
            load_execution_version, translate_native_checkpoint,
            validate_resume_compatibility,
        )
        from .recovery_identity import platform_identity
        directory = self._action.context.directory
        binding = load_execution_version(directory, self._action.action_id,
                                         max_bytes=self._physical.bounds.max_state_bytes)
        report = validate_resume_compatibility(binding, current_runtime=platform_identity(),
                                               store=self._store)
        if not report.supported:
            raise DataflowError("INCOMPATIBLE_HISTORY",
                                "Historical run is not recoverable: " + ";".join(report.reasons))
        return translate_native_checkpoint(binding, store=self._store, bounds=self._physical.bounds)

    # -- D006 -------------------------------------------------------------
    def abort(self, cut_id: str, *, reason: str) -> None:
        with self._lock:
            self._check_open()
            if type(reason) is not str or not reason:
                raise DataflowError("INVALID_CONTRACT", "abort requires a reason")
            pending = self._pending.get(cut_id)
            if pending is None:
                # A stale cut from another attempt is not owned here; it must not
                # disturb the current attempt's work.
                raise DataflowError("INVALID_CONTRACT", "abort of a cut not owned by this attempt")
            if pending.aborted:
                return
            pending.aborted = True
            # Release only this cut's unpublished staging. Published effects and
            # committed references are owned by the native journal and stay.
            for contribution in pending.contributions.values():
                for ref in contribution.buffers:
                    try:
                        self._store.release(ref, owner=cut_id)
                    except Exception:
                        pass
            del self._pending[cut_id]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._pending.clear()
            with _OWNERS_LOCK:
                if _OWNERS.get(self._owner_key) is self:
                    del _OWNERS[self._owner_key]


# -- D007 -----------------------------------------------------------------
def source_vector_position(image: RecoveryImage, *, action_id: str) -> SourcePosition:
    """Project the fixed virtual v3 source position for the native coordinator.

    The scalar ordinal is the cut sequence, not a business row number. The real
    cursors are ``image.source_vector`` and are cross-checked here; recovery must
    read that vector and never skip all sources by this scalar.
    """
    if not isinstance(image, RecoveryImage):
        raise DataflowError("INVALID_CONTRACT", "source_vector_position requires a RecoveryImage")
    identifier(action_id)
    if image.cut.sequence < 1:
        raise DataflowError("INVALID_CONTRACT", "Recovery image has no committed cut sequence")
    seen = set()
    for cursor in image.source_vector:
        if cursor.split_id in seen:
            raise DataflowError("SOURCE_CHANGED", "Duplicate split in recovery source vector")
        seen.add(cursor.split_id)
    source_id = identity(["dataflow-vector", action_id])
    return SourcePosition(source_id=source_id, next_ordinal=image.cut.sequence)
