"""U4/S4 effect (EFF) conformance: one K6 inflight coverage + replayable rule.

Proves the S4 effect mainline rather than re-proving frozen units: a worker's
native effect journal is the one durable authority; ``collect_inflight`` maps its
UNRESOLVED calls (pending_only) onto the frozen K6 inflight description — a BOUNDED
description carrying the in-flight unknown effects (unknown never carries
effect_ref), omitting completed effects (durable journal facts reused by the
journal's own check_recovery, not re-listed each cut) and confirmed-unsent
dispatches; the driver unions every participant's fragment into the
CheckpointManifest inflight AND the recoverable graph-progress summary;
``replayable_effects`` (frozen) fires on every committed manifest and returns only
known_completed refs (an all-completed run yields [], which is contract-valid). An
uncertain request fail-closes the journal's own recovery check before any new
dispatch (no blind retry). The end-to-end graph tests use a dynamic effect actor
that writes the worker journal through the same SQLite mechanism an LLM actor uses
— no network, no paid model.
"""
import sys

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts.checkpoint import replayable_effects
from demiflow.execution.effect_keys import (
    collect_inflight, effect_request_key, merge_inflight, replayable_effect_refs,
)
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _journal(tmp_path):
    # No throughput batching: reserve/response commit synchronously so the cut-time
    # description is deterministic (the production RunPromptJournal batches async).
    return _SQLiteJournalShard(tmp_path / "effects.sqlite", max_requests=1024)


def _recovery_check(path):
    # check_recovery is the RunPromptJournal startup gate; build one over the same
    # DB a synchronous shard populated (the check is a read-only indexed query).
    from types import SimpleNamespace
    from demiflow.operator_llm.stream_recovery import RunPromptJournal
    run = SimpleNamespace(journal=SimpleNamespace(path=path),
                          limits={"max_requests": 1024, "max_control_bytes": 16 * 1024 ** 2})
    return RunPromptJournal(run)


class _UnsentError(Exception):
    call = {"request_sent": False, "batch_cancelled_before_dispatch": True}


# --- collect_inflight maps the durable journal onto the frozen K6 description ---

def test_collect_inflight_known_unknown_and_confirmed_unsent(tmp_path):
    journal = _journal(tmp_path)
    done = {"kind": "effect", "id": "done"}
    uncertain = {"kind": "effect", "id": "uncertain"}
    unsent = {"kind": "effect", "id": "unsent"}
    assert journal.reserve(done) is True
    journal.response(done, {"ok": True})
    assert journal.reserve(uncertain) is True            # reserved, no response
    assert journal.reserve(unsent) is True
    journal.failed(unsent, _UnsentError("cancelled before dispatch"), 0.0)
    entries = {e["request_key"]: e for e in collect_inflight(journal)}
    # Bounded inflight (pending_only): only the unresolved UNKNOWN call is described.
    # The completed effect is a durable journal fact reused by the journal's own
    # check_recovery (the one authority), not re-listed each cut; a confirmed-unsent
    # dispatch wrote no effect and is omitted (pure compute may recompute).
    assert len(entries) == 1
    assert request_key(done) not in entries               # completed omitted (bounded)
    assert request_key(unsent) not in entries             # confirmed-unsent omitted
    unknown = entries[request_key(uncertain)]
    assert unknown["status"] == "unknown"
    assert "effect_ref" not in unknown                    # unknown never carries a ref
    journal.close()


def test_collect_inflight_no_journal_is_empty():
    assert collect_inflight(None) == []


def test_collect_inflight_empty_journal_is_empty(tmp_path):
    journal = _journal(tmp_path)
    assert collect_inflight(journal) == []
    journal.close()


def test_merge_inflight_unions_by_request_key():
    a = {"request_key": "a" * 64, "status": "known_completed", "effect_ref": "a" * 64}
    b = {"request_key": "b" * 64, "status": "unknown"}
    merged = merge_inflight([{"inflight": [a, b]}, {"inflight": [a]}, {}])
    assert merged == [a, b]                               # dedup, sorted by key
    assert merge_inflight([]) == []


def test_replayable_effect_refs_returns_only_known_completed():
    inflight = [{"request_key": "a" * 64, "status": "known_completed", "effect_ref": "a" * 64},
                {"request_key": "b" * 64, "status": "unknown"}]
    assert replayable_effect_refs(inflight) == ["a" * 64]
    assert replayable_effect_refs([]) == []


def test_effect_request_key_is_stable_occurrence_lineage():
    key = effect_request_key("action", "node", "occ", "llm")
    assert key == effect_request_key("action", "node", "occ", "llm")
    assert key != effect_request_key("action", "node", "occ2", "llm")
    assert len(key) == 64                                 # sha256 identifier


# --- the journal's own recovery check fail-closes unknown (no blind retry) -------

def test_journal_check_recovery_fail_closes_uncertain(tmp_path):
    journal = _journal(tmp_path)
    journal.reserve({"kind": "effect", "id": "uncertain"})
    journal.close()
    recovery = _recovery_check(tmp_path / "effects.sqlite")
    with pytest.raises(UncertainPromptCall, match="automatic regeneration is forbidden"):
        recovery.check_recovery()
    recovery.close()


def test_journal_confirmed_unsent_is_the_only_recovery_exception(tmp_path):
    journal = _journal(tmp_path)
    request = {"kind": "effect", "id": "unsent"}
    journal.reserve(request)
    journal.failed(request, _UnsentError("cancelled before dispatch"), 0.0)
    journal.close()
    recovery = _recovery_check(tmp_path / "effects.sqlite")
    recovery.check_recovery()                             # no raise: wrote no effect
    recovery.close()


def test_journal_completed_response_reuses_without_new_reservation(tmp_path):
    journal = _journal(tmp_path)
    request = {"kind": "effect", "id": "done"}
    assert journal.reserve(request) is True
    journal.response(request, {"ok": True})
    # The operative reuse authority is the journal itself: a completed request is
    # never re-reserved (reserve -> False), independent of the manifest description.
    assert journal.reserve(request) is False              # known_completed reuse
    # Bounded inflight: a completed-only journal yields an empty description
    # (contract-valid) — completed effects are durable journal facts, not re-listed.
    assert collect_inflight(journal) == []
    journal.close()
    recovery = _recovery_check(tmp_path / "effects.sqlite")
    recovery.check_recovery()                             # complete response is provable
    recovery.close()


# --- end-to-end graph: worker journal -> contribution -> manifest coverage -------

def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                              stream_options=MapStreamOptions(concurrency=1)),))


def _make_effect_actor(mode):
    # Dynamic class: cloudpickle stages it BY VALUE into the independent worker
    # host. Like a native PromptActor it binds its effect journal once at prepare
    # time (astart runs in the prepared run context) and declares empty semantic
    # state so the K2 actor precheck admits it; __call__ only writes the journal.
    # mode is a closed-over string, also serialized by value. No network/paid model.
    class EffectActor:
        def astart(self):
            from demiflow.execution.run_context import current_run
            from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
            run = current_run()
            journal = getattr(run, "_native_prompt_journal", None)
            if journal is None:
                # An isolated file namespaced by allocation: production batches
                # async on the shared run DB; this controlled actor writes
                # synchronously, so a separate file keeps the cut description
                # deterministic and avoids lock contention with the runtime.
                journal = run._native_prompt_journal = _SQLiteJournalShard(
                    run.directory / ("eff-" + run.allocation_id + ".sqlite"),
                    max_requests=run.limits["max_requests"])
            self._journal = journal

        def __call__(self, row):
            request = {"kind": "conformance-effect", "x": row["x"]}
            if self._journal.reserve(request) and mode == "completed":
                self._journal.response(request, {"ok": True, "x": row["x"]})
            return {**row, "x": row["x"] * 2}

        def snapshot_state(self):
            return {}                       # journal is a resource, not row state

        def restore_state(self, state):
            if state != {}:
                raise ValueError("Effect actor has no mutable semantic state")

    return EffectActor()


def _graph_spec(ancestor_actor, n=24, cut_rows=8, readers=("c0", "c1")):
    def passthrough(row):                                 # local closure: by value
        return row

    items = tuple({"x": i} for i in range(n))
    targets = [{"plan": _stream_map(passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "eff-graph-source",
        "ancestor": {"plan": _stream_map(ancestor_actor),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": "durable_incremental",
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    return spec, list(readers), n


def _run_graph(tmp_path, spec):
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "eff-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
    return result


def test_graph_manifest_bounded_inflight_completed_journal_reused(tmp_path):
    from demiflow.execution.state_artifacts import StateArtifacts
    spec, readers, n = _graph_spec(_make_effect_actor("completed"), n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "eff-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        # The recoverable graph-progress summary carries the identical inflight
        # description (the manifest is a post-commit derived artifact; the summary
        # is the durable primitive Restore reads).
        summaries = [StateArtifacts(run).read(f["prepared"]["state"]["graph-progress"])
                     for f in facts]
    assert result["egress_rows"] == n
    manifests = [cp["manifest"] for cp in result["checkpoints"].values()]
    final = next(m for m in manifests if m["seal"]["sealed"])
    # BOUNDED coverage: every ancestor effect completed in the worker journal, so the
    # cut description omits them (pending_only) — an all-completed run carries an empty
    # inflight, which is contract-valid. Completed effects stay durable journal facts.
    assert final["inflight"] == []
    # The frozen production rule still fires on the committed manifest: no in-flight
    # completed entry -> no replayable ref ([]), never a blind replay of paid effects.
    assert replayable_effects(final) == []
    final_summary = next(s for s in summaries if s["final"])
    assert final_summary["inflight"] == final["inflight"]
    assert replayable_effect_refs(final_summary["inflight"]) == []
    for cp in result["checkpoints"].values():
        assert cp["manifest"]["inflight"] == []
        assert cp["replayable_effects"] == replayable_effects(cp["manifest"])
    # The reuse authority is the journal, NOT the manifest: a completed request is
    # never re-reserved (reserve -> False) even though it is omitted from inflight.
    # Same SQLite mechanism the effect actor writes through the worker journal.
    reuse = _SQLiteJournalShard(tmp_path / "reuse.sqlite", max_requests=1024)
    request = {"kind": "conformance-effect", "x": 0}
    assert reuse.reserve(request) is True
    reuse.response(request, {"ok": True, "x": 0})
    assert reuse.reserve(request) is False                # completed reuse, journal-authoritative
    assert collect_inflight(reuse) == []                  # bounded: omitted from the description
    reuse.close()
    # SAFETY companion: an unknown (reserved, no response) effect IS retained in inflight
    # and is never replayable — bounded-inflight does not weaken the no-blind-retry rule.
    uspec, _, un = _graph_spec(_make_effect_actor("unknown"), n=12, cut_rows=6)
    uresult = _run_graph(tmp_path / "unknown", uspec)
    ufinal = next(cp["manifest"] for cp in uresult["checkpoints"].values()
                  if cp["manifest"]["seal"]["sealed"])
    assert len(ufinal["inflight"]) == un
    assert all(e["status"] == "unknown" for e in ufinal["inflight"])
    assert all("effect_ref" not in e for e in ufinal["inflight"])
    assert replayable_effects(ufinal) == []               # unknown never auto-replayed


def test_graph_unknown_effects_are_never_replayable(tmp_path):
    spec, readers, n = _graph_spec(_make_effect_actor("unknown"), n=12, cut_rows=6)
    result = _run_graph(tmp_path, spec)
    final = next(cp["manifest"] for cp in result["checkpoints"].values()
                 if cp["manifest"]["seal"]["sealed"])
    assert len(final["inflight"]) == n
    assert all(e["status"] == "unknown" for e in final["inflight"])
    assert all("effect_ref" not in e for e in final["inflight"])
    assert replayable_effects(final) == []                # unknown never auto-replayed
    for cp in result["checkpoints"].values():
        assert cp["replayable_effects"] == []


def test_graph_pure_compute_inflight_is_byte_neutral(tmp_path):
    def pure(row):                                        # local closure: by value
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(pure, n=16, cut_rows=8)
    result = _run_graph(tmp_path, spec)
    assert result["egress_rows"] == n
    for cp in result["checkpoints"].values():
        assert cp["manifest"]["inflight"] == []           # no journal -> no effects
        assert cp["replayable_effects"] == []