"""A-lane code-pin convergence: F9 (by-value registration) + F7c (carried state).

Two real-person priorities are bound here with the ACTUAL public surfaces, not a
pickle roundtrip alone:

  F9 「第一次要正确」 — the workbench execute driver registers every sealed-source
  module ``cloudpickle.register_pickle_by_value`` (runs/worker.py:78-81), so an
  importable business UDF actually travels BY VALUE with a minimal reconstructed
  ``__globals__``. The pre-fix by-reference judgement (module+qualname identity
  only) made the driver walk the full module namespace (pinning the UDF's own
  module via its siblings) while the worker reached the minimal carried subset ->
  ``code-pin differs`` on an UNCHANGED first run. ``_module_registered_by_value``
  (public registry, parent-package aware) + the opcode-exact ``_code_global_names``
  (mirrors cloudpickle ``_extract_code_globals``) unify both sides. Bound by a REAL
  workbench prepare/start/show run (fixed-version Lance -> normalize_question
  map(concurrency=1) -> materialize -> write_lance) through an independent managed
  worker, plus a by-value registration roundtrip whose driver/worker pins are equal
  and whose externally by-reference helper edit is still detected.

  F7c 「同一次运行和恢复不能混用业务代码」 — ``_by_value_reachable`` now mirrors the
  full cloudpickle ``_function_getstate`` carried state: referenced globals subset +
  closure cells + ``__defaults__`` + ``__kwdefaults__`` + ``__dict__`` +
  ``__annotations__``. A business module captured ONLY as a (keyword-)default arg
  travels by name and must be pinned; editing ONLY that helper after a real worker
  SIGKILL (acked prefix) must fail-closed on the SAME RunRef resume with an
  identifiable RecoveryContractError (never TimeoutError, never mixed rows).

Honest scope: F9/F7c code-pin only. The run/compile assembly region
(compile_chain_topology/run_local_chain/run_ray_chain) is C's; this file does not
edit it. F10/workbench.js is D's. No paid model, no network. Real-workbench tests
use the self-contained forge demo workspace; recovery tests write only under
pytest tmp_path and kill only their own managed worker.
"""
import importlib
import os
import sys
import uuid
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext, RunAction

from test_stage2_unified_plan import (
    LIMITS, _crash_hook, _load_modules, _map_plan, _topology,
)
from test_forge_runs import base, root, custom, execute, rows

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        try:
            yield SimpleNamespace(run=run, action=action)
        finally:
            record = run.journal.execution_action("main-action")
            if record is not None and record["result"] is None:
                run.journal.complete_execution_action(
                    run.fence, "main-action", {"kind": "test-complete"})


def _helper_graph(tmp_path, monkeypatch, prefix):
    """One importable business helper module: ``delta() -> 1`` (by name on worker)."""
    helper = prefix + "_helper_" + uuid.uuid4().hex[:8]
    sources = {helper: "def delta():\n    return 1\n"}
    module, paths = _load_modules(tmp_path, monkeypatch, sources, helper)
    return module, paths, helper


# --- F7c factories: a by-value closure capturing the helper ONLY as a default ---

def _make_default_cell(helper):
    # <locals> closure -> by value; the module is carried in __defaults__ (by name).
    def transform(row, dep=helper):
        return {"input": row["x"], "value": row["x"] + dep.delta()}
    return transform


def _make_kwdefault_cell(helper):
    # keyword-only default -> the module is carried in __kwdefaults__ (by name).
    def transform(row, *, dep=helper):
        return {"input": row["x"], "value": row["x"] + dep.delta()}
    return transform


FACTORIES = {"default": _make_default_cell, "kwdefault": _make_kwdefault_cell}
FORMS = ["default", "kwdefault"]


# === F9 acceptance 1: REAL workbench unchanged-code first run is NOT rejected ===

def test_f9_workbench_unchanged_first_run_succeeds(root, tmp_path, monkeypatch):
    from demiflow.workbench.runs import manager as runs
    # Isolate from any real deployment host-budget.json: a fresh domain has none,
    # so the fixed-host-partition guard does not apply to this owned workspace.
    domain = tmp_path / "domain"
    domain.mkdir()
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(domain))
    value = custom(root, '''from operators.question_rules import normalize_question

def run(*, source, target):
    d = data.read_lance(source['uri'], version=source['version'], columns=['case_id', 'question'])
    m = d.map(normalize_question, inputs={'text': 'question'}, output='normalized_question',
              fn_kwargs={'max_chars': 4096}, concurrency=1).materialize()
    return m.write_lance(target['uri'], mode='create', return_receipt=True)
''')
    rules = root / 'operators/question_rules.py'
    before = rules.read_bytes()
    result = execute(root, value)
    assert result['status'] == 'succeeded', (result, runs.logs(root, result['run_id']))
    # Business bytes are completely unchanged (no edit, no re-seal, no stand-in).
    assert rules.read_bytes() == before
    out = rows(root, result)
    assert {r['case_id'] for r in out} == {'c01', 'c02', 'c03'}
    assert {r['normalized_question'] for r in out} == {
        '三角形的面积是多少？', '增长率是多少？', '这个结论一定成立吗？'}
    # The v3 worker loads the original code-pin/program bundle before emitting
    # any cut. Read that bundle and its original ACK, not retired chain keys.
    import json
    from demiflow.execution.sealed_plans import load_sealed_plan, _v3_read_store
    from demiflow.execution.dataflow_seal import _read_json, CODE_PIN_MEDIA, PROGRAM_MEDIA
    journal = runs.journal(root, result['run_id'])
    with journal._connection() as db:
        seals = [json.loads(row[0]) for row in db.execute(
            "SELECT document FROM runtime_sources WHERE action_id LIKE '%-dataflow-sealed'")]
    assert seals, "no unified execution seal; original worker gate was not exercised"
    witnessed_map = False
    for seal in seals:
        directory = runs.directory(root, result['run_id'])
        bundle = load_sealed_plan(directory, seal['action_id'])
        store, bounds = _v3_read_store(directory, store_id=bundle['sealed'].code_pin_ref.store_id)
        pin = _read_json(store,bundle['sealed'].code_pin_ref,max_bytes=bounds.max_control_bytes,
                         media_type=CODE_PIN_MEDIA)
        assert pin['action_id'] == seal['action_id']
        for node in bundle['graph'].nodes:
            if node.program_ref is None:
                continue
            manifest = _read_json(store,node.program_ref,max_bytes=bounds.max_control_bytes,
                                  media_type=PROGRAM_MEDIA)
            assert pin['programs'][node.occurrence_id] == manifest['code_identity']
            witnessed_map |= node.kernel.kind == 'BoundMapOp'
        commits = journal.execution_commits(seal['action_id'])
        assert commits and commits[-1]['status'] == 'acked'
        assert commits[-1]['prepared']['state']['dataflow-cut-v3']
        assert journal.execution_action(seal['action_id'])['result'] is not None
    assert witnessed_map


# === F9 acceptance 4: by-value registration roundtrip, driver pin == worker pin ===

def test_f9_by_value_registration_roundtrip(tmp_path, monkeypatch):
    import cloudpickle
    from demiflow.execution.chain import plan_code_pin, _is_by_reference_udf
    suffix = uuid.uuid4().hex[:8]
    helper_name, rules_name = "f9rt_helper_" + suffix, "f9rt_rules_" + suffix
    sources = {
        helper_name: "def delta():\n    return 1\n",
        rules_name: ("import %s\n\n\ndef sibling():\n    return 0\n\n\n"
                     "def transform(row):\n    return {'v': row['x'] + %s.delta()}\n"
                     % (helper_name, helper_name)),
    }
    rules, paths = _load_modules(tmp_path, monkeypatch, sources, rules_name)
    helper = sys.modules[helper_name]
    cloudpickle.register_pickle_by_value(rules)
    try:
        # Registered module -> the importable UDF actually travels BY VALUE.
        assert not _is_by_reference_udf(rules.transform)
        plan = _map_plan(rules.transform)
        driver_pin = plan_code_pin(plan)
        # The by-name helper is pinned; the by-value defining module is not (R4).
        assert helper_name in driver_pin["dependencies"]
        assert rules_name not in driver_pin["dependencies"]
        # A real cloudpickle roundtrip reconstructs the worker-side plan; its pin
        # must equal the driver's for UNCHANGED code (the F9 first-run guarantee).
        loaded = cloudpickle.loads(cloudpickle.dumps(plan))
        assert plan_code_pin(loaded) == driver_pin
        # An externally by-reference helper edit is still detected.
        paths[helper_name].write_text("def delta():\n    return 9999\n")
        importlib.reload(helper)
        assert plan_code_pin(_map_plan(rules.transform)) != driver_pin
    finally:
        cloudpickle.unregister_pickle_by_value(rules)


# === F7c acceptance 2: default-arg helper edit -> SAME RunRef resume refused ===

@pytest.mark.parametrize("form", FORMS)
def test_f7c_default_edit_same_run_resume_refuses(tmp_path, monkeypatch, form):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.run_journal import RunJournal
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7cg1_" + form)
    transform = FACTORIES[form](helper_mod)
    hook, marker = _crash_hook(tmp_path)
    rows_in = [{"x": i} for i in range(8)]

    def pipeline():
        return (data.from_items(rows_in).map_async(transform, concurrency=1)
                .map_async(hook, concurrency=1))

    root = tmp_path / "run"
    with pytest.raises(DataflowError) as crashed:
        with RunContext.start(root, limits=LIMITS) as run:
            reference = run.ref
            with data.local_execution(workers=1, worker_mode="process", partitions=1,
                                      unified_chain=True):
                pipeline().materialize().take_all()
    # SIGKILL belongs to an actual managed child, never pytest's thread host.
    assert crashed.value.code == 'UNKNOWN_EFFECT'
    assert 'worker connection closed' in str(crashed.value)
    # Unedited code runs to the crash point: driver/worker manifests agree.
    assert marker.exists(), "first run must crash after acked cuts, not fail at load"
    journal = RunJournal(reference.directory)
    commits = journal.execution_commits('action-1')
    assert commits and all(item['status'] == 'acked' for item in commits)
    assert any(item['receipts']['materialize']['data']['rows'] > 0 for item in commits)
    original_seal = journal.source_binding('action-1-dataflow-sealed')
    # Edit ONLY the default-arg-referenced helper; the closure's pickle bytes unchanged.
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(DataflowError, match="Loaded program code differs from the sealed pin") as excinfo:
        with RunContext.resume(reference):
            with data.local_execution(workers=1, worker_mode="process", partitions=1,
                                      unified_chain=True):
                pipeline().materialize().take_all()
    assert not isinstance(excinfo.value, TimeoutError)
    assert excinfo.value.code == 'INVALID_CONTRACT'  # existing v3 code-pin rejection vocabulary
    assert journal.execution_commits('action-1') == commits
    assert journal.source_binding('action-1-dataflow-sealed') == original_seal


# === F7c: default-arg helper edit -> worker LOAD gate refuses ===

@pytest.mark.parametrize("form", FORMS)
def test_f7c_default_edit_worker_load_gate_refuses(sealed, tmp_path, monkeypatch, form):
    from demiflow.execution.chain import (
        _business_dependency_manifest, stage_plan_artifact, verify_loaded_plan)
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7cg3_" + form)
    transform = FACTORIES[form](helper_mod)
    plan = _map_plan(transform)
    assert helper in _business_dependency_manifest(plan), "default-arg helper must be pinned"
    plan_ref = stage_plan_artifact(sealed.action, plan)
    topology, _, _ = _topology(sealed, plan, plan_ref)
    verify_loaded_plan(plan, plan_ref, topology["operators"])      # unedited passes
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_map_plan(transform), plan_ref, topology["operators"])


# === F7c: unedited manifest stable + a clean capturing run succeeds (no false refuse) ===

@pytest.mark.parametrize("form", FORMS)
def test_f7c_default_unedited_manifest_stable(tmp_path, monkeypatch, form):
    from demiflow.execution.chain import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7cg2_" + form)
    plan = _map_plan(FACTORIES[form](helper_mod))
    first = _business_dependency_manifest(plan)
    again = _business_dependency_manifest(plan)
    assert first == again, "two walks of unedited code must be byte-identical"
    assert helper in first, "default-arg business helper must be pinned"


@pytest.mark.parametrize("form", FORMS)
def test_f7c_default_clean_run_succeeds(tmp_path, monkeypatch, form):
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7cclean_" + form)
    transform = FACTORIES[form](helper_mod)
    rows_in = [{"x": i} for i in range(8)]
    root = tmp_path / "run"
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
            out = (data.from_items(rows_in).map_async(transform, concurrency=1)
                   .materialize().take_all())
    assert {r["input"]: r["value"] for r in out} == {i: i + 1 for i in range(8)}


# === Regression guards: F7b cell capture still pinned; no-capture stays empty ===

def test_regression_cell_capture_still_pinned(tmp_path, monkeypatch):
    from demiflow.execution.chain import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7creg_cell")

    def transform(row):
        return {"input": row["x"], "value": row["x"] + helper_mod.delta()}

    assert helper in _business_dependency_manifest(_map_plan(transform))


def test_regression_no_capture_by_value_dependencies_empty(sealed):
    from demiflow.execution.chain import (
        _business_dependency_manifest, stage_plan_artifact, verify_loaded_plan)

    def v1(row):
        return {**row, "score": row["x"] + 1}

    plan = _map_plan(v1)
    assert _business_dependency_manifest(plan) == {}
    plan_ref = stage_plan_artifact(sealed.action, plan)
    topology, _, _ = _topology(sealed, plan, plan_ref)
    verify_loaded_plan(plan, plan_ref, topology["operators"])