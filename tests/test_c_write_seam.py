"""C package write_lance seal seam: relational counterexamples + branch coverage.

Route (b): native execution and the single LanceSink publication are unchanged;
compile + seal + a forced pre-publication binding gate sit above them. These
tests prove, on the relational branch, that the gate fail-closes on structure
drift (R1), code-pin drift (R2) and end-to-end before any publication (R3); that
the three covered branches seal a plan the workbench reader can read back; and
that the streamed branch really consumes the sealed checkpoint policy and stage
limits. Deferred branches (pure Lance copy, arrow_batch_sink) honestly seal
nothing. All scratch output lives under the pytest tmp_path (/tmp/demiflow-*).
"""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.chain import seal_write_topology
from demiflow.execution.local_kernel import keyed_dataset
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.sealed_plans import load_execution_bindings, load_sealed_plan
from demiflow.workbench.runs.facts import read_sealed_plan


def enrich(id):
    return id * 2


def enrich_drift(id):
    return id * 3


def keep(row):
    return row["v"] % 4 == 0


def relational_dataset():
    left = data.from_items([{"id": i, "a": i} for i in range(8)])
    right = data.from_items([{"id": i, "b": i * 10} for i in range(8)])
    keyed = keyed_dataset(left, "join", on=("id",), right=right)
    return keyed.map(enrich, inputs={"id": "id"}, output="v").filter(keep)


def drifted_relational_dataset():
    left = data.from_items([{"id": i, "a": i} for i in range(8)])
    right = data.from_items([{"id": i, "b": i * 10} for i in range(8)])
    keyed = keyed_dataset(left, "join", on=("id",), right=right)
    return keyed.map(enrich_drift, inputs={"id": "id"}, output="v").filter(keep)


def sealed_action_id(journal):
    with journal._connection() as db:
        actions = [r[0] for r in db.execute("SELECT action_id FROM runtime_actions")]
    sealed = [a for a in actions
              if journal.source_binding(a + "-sealed-plan") is not None]
    assert sealed, "no action carries a sealed-plan binding"
    return sealed[0]


def assert_sealed_readable(run_dir, run, expected_kinds):
    action_id = sealed_action_id(run.journal)
    facts = read_sealed_plan(run_dir, action_id)
    assert facts["available"] is True, facts
    sealed = load_sealed_plan(run_dir, action_id)
    nodes = sealed["logical_dag"]["nodes"]
    assert [n["op_kind"] for n in nodes] == expected_kinds
    assert sealed["logical_dag"]["action_id"] == action_id
    # Four-way identity: F1 facts and the bindings bind the same run/action/dag/plan.
    f1 = sealed["facts"]
    bindings = load_execution_bindings(run_dir, action_id, sealed_plan=sealed)
    doc = bindings["document"]
    assert doc["run_id"] == f1["run_id"] == run.ref.run_id
    assert doc["action_id"] == f1["action_id"] == action_id
    assert doc["dag_id"] == f1["dag_id"]
    assert doc["plan_id"] == f1["plan_id"]
    return action_id


# --- Golden paths: the three covered branches seal the executed plan ----------

def test_relational_write_seals_executed_plan(tmp_path):
    ds = relational_dataset()
    assert type(ds._source).__name__ == "KeyedSource"
    target = tmp_path / "out.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["KeyedSource", "map", "filter", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    expected = [{"id": i, "a": i, "b": i * 10, "v": i * 2}
                for i in range(8) if (i * 2) % 4 == 0]
    assert sorted(rows, key=lambda r: r["id"]) == sorted(expected, key=lambda r: r["id"])


def test_fallback_write_seals_executed_plan(tmp_path):
    def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map(double).filter(keep)
    target = tmp_path / "fallback.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["MaterializedSource", "map", "filter", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 4, 8]


def test_streamed_write_seals_executed_plan(tmp_path):
    async def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map_async(double)
    target = tmp_path / "stream.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        assert_sealed_readable(tmp_path / "run", run,
                               ["MaterializedSource", "async_map", "write_lance"])
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 2, 4, 6, 8, 10]


# --- Relational counterexamples: the binding gate fail-closes -----------------

def test_relational_structure_drift_fail_closed(tmp_path):
    """R1 (layer 2): identical executed code-pin, divergent sealed operator order.

    The executed plan is unchanged so its code-pin still matches the seal; only
    the sealed logical_dag operator sequence is perturbed. The structure recheck
    must fail-close independently of the code-pin gate.
    """
    ds = relational_dataset()
    with RunContext.start(tmp_path / "run") as run:
        action = RunAction(run, "rel-action", {"kind": "write_table"})
        handle = seal_write_topology(action, ds._source, ds._plan)
        handle.verify(ds._plan)  # the honest plan passes both bindings
        handle.topology["operators"] = list(reversed(handle.topology["operators"]))
        with pytest.raises(RecoveryContractError, match="operator identity differs"):
            handle.verify(ds._plan)


def test_relational_code_pin_drift_fail_closed(tmp_path):
    """R2 (layer 1): business code edited since seal changes the code-pin."""
    ds = relational_dataset()
    drifted = drifted_relational_dataset()
    with RunContext.start(tmp_path / "run") as run:
        action = RunAction(run, "rel-action", {"kind": "write_table"})
        handle = seal_write_topology(action, ds._source, ds._plan)
        handle.verify(ds._plan)
        with pytest.raises(RecoveryContractError, match="code-pin differs"):
            handle.verify(drifted._plan)


def test_relational_drift_fail_closed_before_publication(tmp_path, monkeypatch):
    """R3: on the real relational write path, drift fail-closes before commit.

    The seal is perturbed to bind a different business UDF than the plan that
    actually executes. The pre-publication gate must raise before the LanceSink
    commits: no target dataset is created and no commit is acked.
    """
    import demiflow.execution.chain as chain_mod

    real_seal = chain_mod.seal_write_topology

    def drift_seal(action, source, plan, *, host_id="local"):
        return real_seal(action, source, drifted_relational_dataset()._plan,
                         host_id=host_id)

    monkeypatch.setattr(chain_mod, "seal_write_topology", drift_seal)
    ds = relational_dataset()
    assert type(ds._source).__name__ == "KeyedSource"
    target = tmp_path / "out.lance"
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        with RunContext.start(tmp_path / "run") as run:
            ds.write_lance(str(target), mode="create", return_receipt=True)
    assert not target.exists(), "publication happened despite a failed binding gate"
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(tmp_path / "run")
    action_id = sealed_action_id(journal)
    assert not journal.execution_commits(action_id), "a commit was acked before the gate"


# --- Streamed layer-3: the sealed layout is really consumed -------------------

def test_streamed_consumes_sealed_checkpoint_and_stage_limits(tmp_path, monkeypatch):
    import demiflow.execution.chain as chain_mod
    import demiflow.execution.stream_resources as stream_resources
    import demiflow.execution.stream_materialize as stream_materialize

    captured = {"terminals": [], "queues": []}
    real_seal = chain_mod.seal_write_topology

    def capture_seal(action, source, plan, *, host_id="local"):
        handle = real_seal(action, source, plan, host_id=host_id)
        captured["handle"] = handle
        return handle

    real_init = stream_materialize.StreamMaterialization.__init__

    def spy_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        captured["terminals"].append(self)

    real_queue = stream_resources.ObservedQueue

    def spy_queue(*args, **kwargs):
        captured["queues"].append(kwargs)
        return real_queue(*args, **kwargs)

    monkeypatch.setattr(chain_mod, "seal_write_topology", capture_seal)
    monkeypatch.setattr(stream_materialize.StreamMaterialization, "__init__", spy_init)
    monkeypatch.setattr(stream_resources, "ObservedQueue", spy_queue)

    async def double(row):
        return {**row, "v": row["i"] * 2}

    ds = data.from_items([{"i": i} for i in range(6)]).map_async(double)
    target = tmp_path / "stream.lance"
    limits = {"max_row_bytes": 1024 ** 2, "max_batch_bytes": 4 * 1024 ** 2,
              "batch_rows": 16, "checkpoint_rows": 4, "checkpoint_interval_ms": 60000,
              "checkpoint_unconfirmed_rows": 1024,
              "checkpoint_unconfirmed_bytes": 8 * 1024 ** 2}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        policy, stage_limits = captured["handle"].layout()
        # The layout reader round-trips the run.limits-derived sealed budgets.
        assert policy.interval_ms == run.limits["checkpoint_interval_ms"]
        assert policy.unconfirmed_rows == run.limits["checkpoint_unconfirmed_rows"]
        assert policy.unconfirmed_bytes == run.limits["checkpoint_unconfirmed_bytes"]
        output_bytes = run.limits["max_row_bytes"] + 256
        inflight_bytes = max(output_bytes, run.limits["max_batch_bytes"])
        assert stage_limits[0] == {"inflight_rows": run.limits["batch_rows"],
                                   "inflight_bytes": inflight_bytes,
                                   "output_bytes": output_bytes}
        # checkpoint_policy really lands on the terminal progress object.
        terminal = captured["terminals"][-1]
        assert terminal.checkpoint_policy == policy
        # The sealed stage budget really reaches a bounded queue constructor.
        assert any(q.get("max_item_bytes") == output_bytes
                   and q.get("max_bytes") == inflight_bytes
                   for q in captured["queues"]), captured["queues"]
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(r["v"] for r in rows) == [0, 2, 4, 6, 8, 10]


# --- Deferred branch: honest absence of a sealed plan -------------------------

def test_deferred_pure_copy_seals_nothing(tmp_path):
    source = tmp_path / "src.lance"
    lance.write_dataset(pa.table({"id": [1, 2, 3]}), str(source))
    ds = data.read_lance(str(source))
    target = tmp_path / "copy.lance"
    with RunContext.start(tmp_path / "run") as run:
        ds.write_lance(str(target), mode="create", return_receipt=True)
        with run.journal._connection() as db:
            actions = [r[0] for r in db.execute("SELECT action_id FROM runtime_actions")]
        sealed = [a for a in actions
                  if run.journal.source_binding(a + "-sealed-plan") is not None]
        assert sealed == [], "a deferred branch must not seal a plan it does not bind"
    assert lance.dataset(str(target)).count_rows() == 3