"""L004 unit: seal_execution_bindings v2 branch (additive, v1 preserved).

Real RunContext/RunJournal lifecycle. v2 seals a content-addressed carrier with
the v2 schema and is idempotent; passing a non-empty programs sequence with
executions is refused; the executions=None branch still produces a v1 carrier.
All scratch is confined to /tmp/demiflow-lcontract-*.
"""
import json
import shutil

import pytest

from demiflow.execution.plan_interpreter import (
    EXECUTION_BINDINGS_SCHEMA, EXECUTION_BINDINGS_SCHEMA_V2, PlanContractError,
)
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _carrier_doc(root, ref):
    return json.loads((root / ref["path"]).read_bytes())


def test_v2_seal_returns_content_addressed_carrier(workdir, legacy_seal_writer):
    n = fx.native_v2_doc()
    root = workdir / "run"
    sealed = fx.seal_existing_format(legacy_seal_writer, root, n, native=True)
    ref = sealed.ref
    assert set(ref) == {"path", "sha256", "bytes"}
    assert (root / ref["path"]).is_file()
    doc = _carrier_doc(root, ref)
    assert doc["schema"] == EXECUTION_BINDINGS_SCHEMA_V2
    assert doc["supported_slice"] == "native_driver_v1"
    assert "executions" in doc and "programs" not in doc


def test_v2_reseal_is_idempotent(workdir):
    n = fx.native_v2_doc()
    with fx.active_native_run(workdir / "run", n) as (run, action):
        first = fx.seal_bindings(run, action, n)
        second = fx.seal_bindings(run, action, n)
        assert first == second


def test_v2_rejects_nonempty_programs(workdir):
    n = fx.native_v2_doc()
    with fx.active_native_run(workdir / "run", n) as (run, action):
        with pytest.raises(PlanContractError) as exc:
            fx.seal_bindings(run, action, n, programs=[{"group_id": "g0"}])
        assert exc.value.reason == "invalid_contract"


def test_v1_branch_preserved_when_executions_none(workdir):
    v1 = chain_fixture()
    root = workdir / "run"
    with _v1_active(root, v1) as (run, action):
        ref = _seal_v1(run, action, v1)
        doc = _carrier_doc(root, ref)
        assert doc["schema"] == EXECUTION_BINDINGS_SCHEMA
        assert "programs" in doc and "executions" not in doc


def _v1_active(root, v1):
    import contextlib
    from demiflow.execution import chain
    from demiflow.execution.run_context import RunAction, RunContext

    @contextlib.contextmanager
    def _cm():
        with RunContext.start(root) as run:
            declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
            run.journal.bind_execution_action(run.fence, v1.action_id, declaration)
            action = RunAction(run, v1.action_id, declaration)
            sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
            chain.seal_plan_facts(run, action, v1.ld, v1.pp, v1.mref, sealed_at)
            yield run, action
            run.journal.complete_execution_action(run.fence, v1.action_id, {"kind": "test-complete"})
    return _cm()


def _seal_v1(run, action, v1):
    from demiflow.execution.sealed_plans import seal_execution_bindings
    doc = v1.doc
    return seal_execution_bindings(
        run, action, logical_dag=v1.ld, physical_plan=v1.pp,
        profiles=doc["profiles"], programs=doc["programs"],
        driver_stage_ids=doc["driver_stage_ids"], source_bindings=doc["source_bindings"],
        channel_options=doc["channel_options"], checkpoint_rows=doc["checkpoint_rows"],
        supported_slice=doc["supported_slice"])