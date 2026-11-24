"""Native run protocol: actual process death, durable effects and fenced owners."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from demiflow.execution.outputs.block_sink import BlockSink
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, RunRef, SourcePosition, UnresolvedCommit
from demiflow.execution.pipeline.run_context import RunContext, current_run
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.errors import ExecutionConflict


WINDOWS = [
    "after_intent", "after_prepare_before_record", "after_prepared",
    "before_sink_commit", "after_sink_commit_before_receipt", "after_sink_receipt",
    "after_data_committed", "after_checkpoint_committed", "after_ack_before_record",
    "after_acked",
]


def acknowledge(root, position, context):
    with sqlite3.connect(root / "source.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS acks (commit_id TEXT PRIMARY KEY)")
        db.execute("INSERT OR IGNORE INTO acks VALUES (?)", (context.commit_id,))


def protocol_worker(root, window=None):
    root = Path(root)
    directory = root / "run"
    identity = RunJournal(directory).recovery_identity()
    scope = (RunContext.resume(RunRef(str(directory), identity["run_id"]))
             if identity else RunContext.start(directory))
    if window is not None:
        import demiflow.execution.checkpoint.commit as protocol
        def interrupt(name, commit_id):
            if name == window:
                os._exit(37)
        protocol.fault_point = interrupt
    with scope as run:
        with run.action(dict(kind="fixture", source={"fixed": 1})) as action:
            sinks = {key: BlockSink(action, key) for key in ("left", "right")}
            coordinator = CommitCoordinator(action)
            ack = lambda position, context: acknowledge(root, position, context)
            facts = coordinator.recover(sinks, acknowledge=ack)
            if not facts or facts[-1]["status"] == "pending":
                for key, sink in sinks.items():
                    sink.append(dict(side=key, value=7))
                coordinator.commit(1, outputs={k: (s, None) for k, s in sinks.items()},
                    position=SourcePosition("fixed-source", 1), states={"counter": {"n": 1}},
                    acknowledge=ack)
            action.complete({"kind": "fixture", "rows": 1})
        return run.ref


@pytest.mark.parametrize("window", WINDOWS)
def test_process_loss_between_every_durable_transition(tmp_path, window):
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(repo), str(repo / "tests")]),
           "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}
    child = subprocess.run([sys.executable, "-B", "-c",
        "from test_execution_recovery_protocol import protocol_worker; import sys; protocol_worker(sys.argv[1], sys.argv[2])",
        str(tmp_path), window], env=env, capture_output=True, text=True, timeout=30)
    assert child.returncode == 37, child.stdout + child.stderr
    identity = RunJournal(tmp_path / "run").recovery_identity()
    before = RunJournal(tmp_path / "run").execution_commits("action-1")
    if window in {"after_intent", "after_prepare_before_record", "after_prepared",
                  "before_sink_commit", "after_sink_commit_before_receipt", "after_sink_receipt"}:
        assert before[0]["status"] not in {"checkpoint_committed", "acked"}
    reference = protocol_worker(tmp_path)
    assert reference.run_id == identity["run_id"]
    journal = RunJournal(tmp_path / "run")
    assert journal.execution_status()["attempt"] == 2
    facts = journal.execution_commits("action-1")
    assert len(facts) == 1 and facts[0]["status"] == "acked"
    assert set(facts[0]["receipts"]) == {"left", "right"}
    assert facts[0]["prepared"]["state"]["counter"]["path"].startswith("state/")
    with sqlite3.connect(tmp_path / "source.sqlite") as db:
        assert db.execute("SELECT count(*) FROM acks").fetchone()[0] == 1
    assert not list((tmp_path / "run").rglob("checkpoint.json"))
    assert len(list((tmp_path / "run").rglob("*.sqlite"))) == 1


def test_distinct_runs_have_distinct_publication_identities(tmp_path):
    identifiers = []
    for name in ("first", "second"):
        root = tmp_path / name
        root.mkdir()
        protocol_worker(root)
        identifiers.append(RunJournal(root / "run").execution_commits("action-1")[0]["commit_id"])
    assert identifiers[0] != identifiers[1]


def test_cumulative_budget_survives_failure_and_attempt_change(tmp_path):
    with pytest.raises(RuntimeError, match="interrupt"):
        with RunContext.start(tmp_path / "run", limits={"max_requests": 3}) as run:
            reference = run.ref
            run.reserve("requests", 2, reservation_id="admitted-before-loss")
            raise RuntimeError("interrupt")
    with RunContext.resume(reference) as run:
        assert run.reserve("requests", 2, reservation_id="admitted-before-loss") == 2
        assert run.reserve("requests", 1, reservation_id="after-loss") == 3
        with pytest.raises(RecoveryContractError, match="exhausted"):
            run.reserve("requests")


def test_changed_identity_and_concurrent_owner_rejected(tmp_path):
    with RunContext.start(tmp_path / "run", entry_identity={"revision": 1}) as run:
        with pytest.raises(RecoveryContractError, match="nest"):
            with RunContext.resume(run.ref):
                pass
        from concurrent.futures import ThreadPoolExecutor
        def other_owner():
            with RunContext.resume(run.ref):
                pytest.fail("concurrent owner acquired the same run")
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(BlockingIOError):
                pool.submit(other_owner).result()
        configuration = run.journal.recovery_identity()
        run.journal.claim_execution(configuration, "replacement")
        with pytest.raises(ExecutionConflict, match="Stale"):
            with run.journal.execution_guard(run.fence):
                pytest.fail("stale owner reached publication")
        # Restore ownership only to allow the test scope to close normally.
        run.journal.claim_execution(configuration, run.fence)


def test_resume_requires_original_reference_and_frozen_limits(tmp_path):
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / "run", entry_identity={"revision": 1}) as run:
            reference = run.ref
            raise RuntimeError()
    with pytest.raises(RecoveryContractError, match="identity changed"):
        with RunContext.resume(reference, entry_identity={"revision": 2}):
            pass
    with pytest.raises(RecoveryContractError, match="limits changed"):
        with RunContext.resume(reference, limits={"max_requests": 2}):
            pass
    with pytest.raises(RecoveryContractError, match="RunRef"):
        with RunContext.resume(RunRef(reference.directory, "other")):
            pass


def test_state_is_durable_before_any_sink_publication(tmp_path, monkeypatch):
    import demiflow.execution.checkpoint.commit as protocol
    reference = None
    with pytest.raises(RuntimeError, match="prepared"):
        with RunContext.start(tmp_path / "run") as run:
            reference = run.ref
            with run.action({"source": "fixed"}) as action:
                sink = BlockSink(action)
                sink.append({"x": 1})
                def fail(name, _):
                    if name == "after_prepared":
                        raise RuntimeError("prepared")
                monkeypatch.setattr(protocol, "fault_point", fail)
                CommitCoordinator(action).commit(1, outputs={"output": (sink, None)},
                    position=SourcePosition("source", 1), states={"counter": {"n": 1}})
    fact = RunJournal(tmp_path / "run").execution_commits("action-1")[0]
    state = fact["prepared"]["state"]["counter"]
    (tmp_path / "run" / state["path"]).write_bytes(b"corrupt")
    monkeypatch.setattr(protocol, "fault_point", lambda *_: None)
    with pytest.raises(RecoveryContractError, match="unconfirmed"), RunContext.resume(reference) as run:
        with run.action({"source": "fixed"}) as action:
            sink = BlockSink(action)
            with pytest.raises(RecoveryContractError, match="State artifact"):
                CommitCoordinator(action).recover({"output": sink})
            assert not list(sink.root.glob("*.json"))


def test_empty_outputs_still_confirm_input_occurrences(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        with run.action({"source": "fixed"}) as action:
            sink = BlockSink(action)
            fact = CommitCoordinator(action).commit(1, outputs={"output": (sink, None)},
                position=SourcePosition("source", 4))
            assert fact["receipts"]["output"]["data"]["rows"] == 0
            assert fact["intent"]["position"]["next_ordinal"] == 4
            action.complete({"empty": True})


def test_oversized_state_does_not_publish_or_advance(tmp_path):
    with pytest.raises(RecoveryContractError, match="unconfirmed"), RunContext.start(tmp_path / "run", limits={"max_state_bytes": 16}) as run:
        with run.action({"source": "fixed"}) as action:
            sink = BlockSink(action)
            with pytest.raises(RecoveryContractError, match="byte budget"):
                CommitCoordinator(action).commit(1, outputs={"output": (sink, None)},
                    position=SourcePosition("source", 1), states={"counter": {"payload": "x" * 100}})
            assert run.journal.execution_commits(action.action_id) == []
            assert not sink.root.exists()


def test_orphan_cleanup_preserves_prepared_and_foreign_files(tmp_path, monkeypatch):
    import demiflow.execution.checkpoint.commit as protocol
    def stop(name, commit_id):
        if name == 'after_prepared':
            raise RuntimeError('prepared')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='prepared'):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            foreign = run.directory / 'scratch' / 'foreign'
            foreign.parent.mkdir()
            foreign.write_bytes(b'not ours')
            orphan = run.directory / 'scratch' / 'owned'
            run.allocate_artifact(orphan)
            orphan.write_bytes(b'unpublished')
            with run.action({'kind': 'fixture', 'source': {'fixed': 1}}) as action:
                sink = BlockSink(action)
                sink.append({'id': 1})
                CommitCoordinator(action).commit(1, outputs={'output': (sink, None)},
                    position=SourcePosition('fixed', 1), states={'counter': {'n': 1}})
    assert not orphan.exists()
    assert foreign.read_bytes() == b'not ours'
    assert list((tmp_path / 'run/materializations').rglob('*.pkl'))
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference) as run:
        with run.action({'kind': 'fixture', 'source': {'fixed': 1}}) as action:
            facts = CommitCoordinator(action).recover({'output': BlockSink(action)})
            assert facts[0]['status'] == 'acked'
            action.complete({'rows': 1})
    assert foreign.read_bytes() == b'not ours'


def test_cancel_releases_owner_and_next_attempt_keeps_run_identity(tmp_path):
    with pytest.raises(RecoveryContractError, match='cancellation'):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            assert RunContext.cancel(reference)
            run.check_stop()
    assert RunJournal(reference.directory).execution_status()['status'] == 'cancelled'
    with RunContext.resume(reference) as run:
        assert run.ref == reference and run.attempt == 2


def test_lost_owner_cannot_mark_run_successful(tmp_path):
    with pytest.raises(ExecutionConflict, match='Stale'):
        with RunContext.start(tmp_path / 'run') as run:
            run.journal.claim_execution(run.journal.recovery_identity(), 'replacement')
    assert RunJournal(tmp_path / 'run').execution_status()['status'] == 'running'


def test_lineage_and_position_cannot_regress(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        with run.action({'kind': 'fixture'}) as action:
            sink = BlockSink(action)
            CommitCoordinator(action).commit(1, outputs={'output': (sink, [])},
                                            position=SourcePosition('fixed', 2))
            with pytest.raises(RecoveryContractError, match='regress'):
                CommitCoordinator(action).commit(2, outputs={'output': (sink, [])},
                                                position=SourcePosition('fixed', 1))
            with pytest.raises(RecoveryContractError, match='lineage'):
                CommitCoordinator(action).commit(2, outputs={'output': (sink, [])},
                                                position=SourcePosition('different', 3))
            action.complete({'rows': 0})


def test_owner_transfer_cannot_race_between_check_and_publication(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    ready, transferred = Event(), Event()
    with RunContext.start(tmp_path / 'run') as run:
        configuration = run.journal.recovery_identity()
        def transfer():
            ready.set()
            run.journal.claim_execution(configuration, 'replacement')
            transferred.set()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with run.journal.execution_guard(run.fence):
                future = pool.submit(transfer)
                assert ready.wait(2)
                assert not transferred.wait(.1)
                # Publication is still authorized under the same locked fence.
            future.result(timeout=3)
        with pytest.raises(ExecutionConflict, match='Stale'):
            with run.journal.execution_guard(run.fence):
                pytest.fail('stale publisher admitted')
        run.journal.claim_execution(configuration, run.fence)


def test_cancel_after_prepare_prevents_publication(tmp_path, monkeypatch):
    import demiflow.execution.checkpoint.commit as protocol
    def stop(name, commit_id):
        if name == 'after_prepared':
            RunContext.cancel(current_run().ref)
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RecoveryContractError, match='cancellation'):
        with RunContext.start(tmp_path / 'run') as run:
            with run.action({'kind': 'fixture'}) as action:
                CommitCoordinator(action).commit(1, outputs={'output': (BlockSink(action), [{'id': 1}])},
                    position=SourcePosition('fixed', 1))
    fact = RunJournal(tmp_path / 'run').execution_commits('action-1')[0]
    assert fact['status'] == 'prepared' and fact['receipts'] == {}


def test_aggregate_state_budget_rejects_before_any_artifact(tmp_path):
    with pytest.raises(RecoveryContractError, match='Aggregate'):
        with RunContext.start(tmp_path / 'run', limits={'max_state_bytes': 40}) as run:
            with run.action({'kind': 'fixture'}) as action:
                CommitCoordinator(action).commit(1, outputs={'output': (BlockSink(action), [])},
                    position=SourcePosition('fixed', 0), states={'a': 'x'*24, 'b': 'x'*24})
    assert not (tmp_path / 'run/state').exists()
    assert not RunJournal(tmp_path / 'run').execution_commits('action-1')


def test_deadline_is_not_reset_by_another_attempt(tmp_path, monkeypatch):
    import time
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 10}) as run:
            ref = run.ref
            deadline = run.deadline
            raise RuntimeError('lost worker')
    monkeypatch.setattr(time, 'time', lambda: deadline + 1)
    with pytest.raises(RecoveryContractError, match='time budget'):
        with RunContext.resume(ref) as run:
            run.check_stop()


def test_explicit_checkpoint_is_not_an_alternate_execution_mode(tmp_path):
    from demiflow import data
    with pytest.raises(RecoveryContractError, match='retired'):
        data.from_items([]).run_stream(checkpoint={'path': str(tmp_path / 'legacy')})
    assert not (tmp_path / 'legacy').exists()


def test_source_code_change_and_late_timeout_cannot_be_marked_success(tmp_path, monkeypatch):
    import demiflow.execution.seal.program_identity as identities
    import time
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / 'run') as run:
            ref = run.ref
            raise RuntimeError('failure')
    monkeypatch.setattr(identities, 'platform_identity', lambda: 'different')
    with pytest.raises(RecoveryContractError, match='implementation changed'):
        with RunContext.resume(ref):
            pass
    with pytest.raises(RecoveryContractError, match='time budget'):
        with RunContext.start(tmp_path / 'late', limits={'timeout_s': 2}) as run:
            monkeypatch.setattr(time, 'time', lambda: run.deadline + 1)
    assert RunJournal(tmp_path / 'late').execution_status()['status'] == 'failed'
