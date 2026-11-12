"""Unified K6 effect request_key generation and in-flight coverage (U4/S4).

The frozen K6 ``inflight`` description (contracts/checkpoint.py) carries
``{request_key, status, effect_ref?}`` with status ``known_completed`` or
``unknown``; ``replayable_effects`` returns only the known_completed refs so an
unknown paid effect is never auto-replayed. This module maps each producer's
durable journal key onto that description without changing any journal's own key
semantics and without building a second effect authority: the journal request_key
(already a sha256 identifier) is reused verbatim as the K6 request_key/effect_ref
so the checkpoint points at the one durable effect fact.

The default lineage is occurrence_lineage — identity([action, node, occurrence,
kind]) — matching the native prompt journal's ``native_occurrence`` precedent
(operator_llm/stream_recovery.py). Producers that already key their journal by a
request_key (LLM/embedding) keep that key; the generator only names the lineage
for producers that need a stable per-occurrence effect identifier.
"""
from .recovery_contract import identity


def effect_request_key(action_id, logical_node_id, occurrence_id, kind):
    return identity([action_id, logical_node_id, occurrence_id, kind])


def _confirmed_unsent(error):
    call = (error or {}).get("call") or {}
    return (call.get("request_sent") is False
            and call.get("batch_cancelled_before_dispatch") is True)


def collect_inflight(journal):
    """Describe a native effect journal's in-flight calls as K6 inflight entries.

    Bounded to the current unresolved calls (pending_only): inflight carries the
    in-flight unknown effects, not the whole completed history, so a long run's
    cut metadata stays within the document byte budget instead of growing with
    every completed call. Completed effects are durable journal facts reused by
    the journal's own check_recovery (the one effect authority), so they need not
    be re-listed each cut; the frozen replayable_effects rule still holds (a
    manifest with no in-flight completed entry yields [], which is contract-valid).

    unknown: a request was reserved without a complete response -> never
    auto-replayed; the journal's own check_recovery fail-closes it before any new
    dispatch. A confirmed-unsent dispatch wrote no effect and is omitted (pure
    compute may recompute), matching the journal's unresolved-call rule.
    """
    if journal is None:
        return []
    entries = []
    for call in journal.calls(pending_only=True):
        key = call["request_id"]
        if call["response"] is not None:
            entries.append({"request_key": key, "status": "known_completed", "effect_ref": key})
        elif not _confirmed_unsent(call["error"]):
            entries.append({"request_key": key, "status": "unknown"})
    return entries


def merge_inflight(contributions):
    """Union worker cut fragments' inflight by request_key (one durable fact each)."""
    merged = {}
    for contribution in contributions:
        for entry in contribution.get("inflight", ()):
            merged[entry["request_key"]] = entry
    return [merged[key] for key in sorted(merged)]


def replayable_effect_refs(inflight):
    """Recovery-side rule over the durable graph-progress summary inflight.

    Identical to contracts.checkpoint.replayable_effects: only known_completed
    entries carry a reusable effect_ref; unknown entries are never auto-replayed.
    The commit path calls the canonical replayable_effects on the full manifest;
    the summary inflight is the recoverable primitive because the manifest is a
    post-commit derived artifact (its sink receipts/operator state refs bind to the
    commit) and so cannot be referenced from its own commit's prepared state. The
    summary entries are already K6-validated by the cut_contribution wire, so the
    description-level filter applies directly without re-validating a manifest.
    """
    return [entry["effect_ref"] for entry in inflight
            if entry["status"] == "known_completed"]