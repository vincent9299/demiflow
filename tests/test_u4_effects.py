"""Real effect journals and native graph commits preserve no-blind-retry semantics."""
import sys
import pytest
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts.checkpoint import replayable_effects
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local managed units require Linux")

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
    from demiflow.execution.journal.prompt import RunPromptJournal
    run = SimpleNamespace(journal=SimpleNamespace(path=path),
                          limits={"max_requests": 1024, "max_control_bytes": 16 * 1024 ** 2})
    return RunPromptJournal(run)


class _UnsentError(Exception):
    call = {"request_sent": False, "batch_cancelled_before_dispatch": True}


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
    journal.close()
    recovery = _recovery_check(tmp_path / "effects.sqlite")
    recovery.check_recovery()                             # complete response is provable
    recovery.close()


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                              stream_options=MapStreamOptions(concurrency=1)),))


def _make_effect_actor(mode, directory):
    # A by-value test actor owns a real SQLite effect journal at an explicit
    # path. This exercises the worker lifecycle without granting the actor
    # access to the driver's RunContext or fabricating engine effect receipts.
    class EffectActor:
        def astart(self):
            from pathlib import Path
            from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
            # This controlled business actor owns its own external-call journal.
            # It does not reach through the worker's source-only run view.
            self._journal = _SQLiteJournalShard(Path(directory) / 'effects.sqlite',
                                                max_requests=1024)

        def aclose(self):
            self._journal.close()

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


def _run_graph(tmp_path, spec, readers, count):
    from runtime_v3_graph_history import committed_rows
    executor = LocalDatasetExecutor(workers=1)
    try:
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows':8,
                'checkpoint_unconfirmed_rows':8, 'checkpoint_interval_ms':60000}) as run:
            with run.action({'kind':'materialize','source':{'fixture':'eff-graph'}}) as action:
                result = executor.materialize_graph(spec)
                action.complete(dict(kind='graph-materialized', egress=result['egress_rows']))
            facts = run.journal.execution_commits(action.action_id)
            assert result['egress_rows'] == count
            assert result['target_rows'] == dict.fromkeys(readers, count)
            assert all(cp['commit']['status'] == 'acked' for cp in result['checkpoints'].values())
            for reader in readers:
                assert committed_rows(run, action.action_id, reader, facts) == [
                    {'x':i*2} for i in range(count)]
        return result
    finally:
        executor.close()


@pytest.mark.parametrize('mode', ['completed', 'unknown'])
def test_real_graph_results_do_not_invent_external_effect_receipts(tmp_path, mode):
    spec, readers, count = _graph_spec(_make_effect_actor(mode, str(tmp_path / 'effect-journal')), n=12, cut_rows=6)
    _run_graph(tmp_path, spec, readers, count)
    journals = list((tmp_path / 'effect-journal').glob('effects.sqlite'))
    assert journals
    entries = []
    for path in journals:
        journal = _SQLiteJournalShard(path, max_requests=1024)
        try:
            entries.extend(journal.calls())
        finally:
            journal.close()
        recovery = _recovery_check(path)
        try:
            if mode == 'unknown':
                with pytest.raises(UncertainPromptCall):
                    recovery.check_recovery()
            else:
                recovery.check_recovery()
        finally:
            recovery.close()
    by_key = {row['request_id']:row for row in entries}
    assert len(by_key) == count
    assert all((row['response'] is not None) == (mode == 'completed') for row in by_key.values())


def test_graph_pure_compute_inflight_is_byte_neutral(tmp_path):
    def pure(row):
        return {**row, 'x': row['x'] * 2}
    spec, readers, count = _graph_spec(pure, n=16, cut_rows=8)
    _run_graph(tmp_path, spec, readers, count)
    assert not list((tmp_path / 'effect-journal').glob('effects.sqlite'))


def test_historical_manifest_only_replays_known_completed_effects():
    # Real supported historical reader, independent of the retired live writer.
    from test_u4_source_progress import _manifest
    manifest = _manifest(inflight=[
        {'request_key':'unknown-call','status':'unknown'},
        {'request_key':'done-call','status':'known_completed','effect_ref':'done-ref'}])
    assert replayable_effects(manifest) == ['done-ref']
