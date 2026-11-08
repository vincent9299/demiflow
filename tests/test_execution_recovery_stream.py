"""Recovery through the ordinary Dataset executor, without Forge or a model."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef, RecoveryContractError


class Counter:
    def __init__(self):
        self.n = 0

    async def __call__(self, row):
        self.n += 1
        trace = os.environ.get("FOUNDATION_TRACE")
        if trace:
            with open(trace, "a") as stream:
                stream.write(str(self.n) + "\n")
        if os.environ.get("FOUNDATION_CRASH") == "during_batch" and self.n == 603:
            os._exit(37)
        if row["id"] % 5 == 0:
            return None
        return [{"id": row["id"], "n": self.n, "child": child} for child in range(2)]

    def snapshot_state(self):
        return {"n": self.n}

    def restore_state(self, state):
        self.n = state["n"]


def run_graph(root, window=None):
    root = Path(root)
    directory = root / "run"
    prior = RunJournal(directory).recovery_identity()
    scope = (RunContext.resume(RunRef(str(directory), prior["run_id"]))
             if prior else RunContext.start(directory))
    if window and window != "during_batch":
        import demiflow.execution.commit as protocol
        def interrupt(name, commit_id):
            fact = RunJournal(directory).execution_commit(commit_id)
            if name == window and fact["intent"]["sinks"] == ["output"] and fact["sequence"] == 2:
                os._exit(37)
        protocol.fault_point = interrupt
    with scope as run:
        rows = [{"id": i} for i in range(1093)] + [{"id": 1}] * 7
        materialized = data.from_items(rows).map_async(Counter(), concurrency=1).materialize()
        result = materialized.take_all()
        return result, run.ref


@pytest.mark.parametrize("window,expected_calls", [
    ("during_batch", 588), ("after_prepared", 76),
    ("after_sink_commit_before_receipt", 76), ("after_checkpoint_committed", 76),
])
def test_real_actor_state_outputs_and_occurrences_resume_together(tmp_path, monkeypatch, window, expected_calls):
    repo = Path(__file__).resolve().parents[1]
    trace = tmp_path / "calls"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(repo), str(repo / "tests")]),
        "FOUNDATION_TRACE": str(trace), "FOUNDATION_CRASH": window,
        "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}
    child = subprocess.run([sys.executable, "-B", "-c",
        "import sys; from test_execution_recovery_stream import run_graph; run_graph(sys.argv[1], sys.argv[2])",
        str(tmp_path), window], env=env, capture_output=True, text=True, timeout=45)
    assert child.returncode == 37, child.stdout + child.stderr
    previous = len(trace.read_text().splitlines())
    monkeypatch.setenv("FOUNDATION_TRACE", str(trace))
    result, reference = run_graph(tmp_path)
    calls = trace.read_text().splitlines()
    assert len(calls) - previous == expected_calls
    assert int(calls[previous]) == 1101 - expected_calls
    expected = [dict(id=i, n=i+1, child=c) for i in range(1093) if i % 5 for c in range(2)]
    expected += [dict(id=1, n=n, child=c) for n in range(1094, 1101) for c in range(2)]
    assert result == expected
    journal = RunJournal(reference.directory)
    assert journal.execution_status()["attempt"] == 2
    import sqlite3
    with sqlite3.connect(journal.path) as db:
        actions = [key for key, raw in db.execute("SELECT action_id,declaration FROM runtime_actions")
                   if json.loads(raw)["kind"] == "materialize"]
    assert len(actions) == 1
    facts = journal.execution_commits(actions[0])
    assert len(facts) == 3 and all(c["status"] == "acked" for c in facts)
    assert not list(Path(reference.directory).rglob("checkpoint.json"))


def test_actor_without_state_contract_fails_before_first_call(tmp_path):
    calls = []
    class UnsafeCounter:
        def __init__(self):
            self.n = 0
        async def __call__(self, row):
            calls.append(row)
            self.n += 1
            return row
    with pytest.raises(RecoveryContractError, match="snapshot_state"):
        with RunContext.start(tmp_path / "run"):
            data.range(10).map_async(UnsafeCounter).materialize()
    assert not calls


def test_fixed_source_is_reused_after_source_capture_commit(tmp_path):
    def source():
        path = tmp_path / "mutable.json"
        yield from json.loads(path.read_text())
    path = tmp_path / "mutable.json"
    path.write_text('[{"id":1},{"id":2}]')
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / "run") as run:
            reference = run.ref
            result = data.from_iter(source).map_async(Counter()).materialize().take_all()
            raise RuntimeError("later action failed")
    path.write_text('[{"id":999}]')
    with RunContext.resume(reference):
        assert data.from_iter(source).map_async(Counter()).materialize().take_all() == result
