"""F7b closure-dependency code-pin (gap④, R2 composite capture forms).

A by-value UDF (lambda / ``<locals>`` closure / ``__main__``) is excluded from the
by-reference dependency walk, so a business module it CAPTURES is never pinned:
``describe`` records a captured module name-only, a callable instance as
actor + ``__dict__`` (the module inside is name-only), and a ``functools.partial``
as actor + empty ``__dict__`` (its ``args`` are invisible). Editing ONLY the
captured helper therefore escapes the code-pin and a same-Run resume mixes
business code versions (real-person priority #2「同一次运行和恢复不能混用业务代码」).

These tests pin the THREE composite capture forms the coordinator's independent
cloudpickle probe proved are carried BY NAME (driver == worker) yet missed by a
module/function/class-only cell walk:
  (a) module_cell    -- closure cell is the business module directly;
  (b) instance_dict  -- closure cell is a callable model instance whose __dict__
                        holds the module (the common ML model-capture shape);
  (c) partial_args   -- closure cell is a functools.partial whose args hold the
                        module.
Plus containers, cycles, budgets, the unedited-equality and no-capture controls.

Real public Local API + managed worker SIGKILL after acked cuts; only the captured
helper's ``delta()`` is edited (1 -> 9999) and reloaded; resume must fail-closed
with an identifiable RecoveryContractError (match "code"), never a TimeoutError and
never mixed rows. The by-value factories use nested classes/functions so they cross
the worker BY VALUE; the captured business module crosses BY NAME (re-imported from
PYTHONPATH). No paid model, no network. Tests write only under pytest tmp_path.
"""
import functools
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
    LIMITS, _crash_hook, _load_modules, _map_plan,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")

FORMS = ["module_cell", "instance_dict", "partial_args"]


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        try:
            from runtime_v3_program_probe import program_store
            with program_store(action) as (store, bounds):
                yield SimpleNamespace(run=run, action=action, store=store, bounds=bounds)
        finally:
            record = run.journal.execution_action("main-action")
            if record is not None and record["result"] is None:
                run.journal.complete_execution_action(
                    run.fence, "main-action", {"kind": "test-complete"})


def _helper_graph(tmp_path, monkeypatch, prefix):
    """One importable business helper module: ``delta() -> 1`` (by name on the worker)."""
    helper = prefix + "_helper_" + uuid.uuid4().hex[:8]
    sources = {helper: "def delta():\n    return 1\n"}
    module, paths = _load_modules(tmp_path, monkeypatch, sources, helper)
    return module, paths, helper


def _make_module_cell(helper):
    # (a) the closure cell IS the business module (freevar, not a co_name global).
    def transform(row):
        return {"input": row["x"], "value": row["x"] + helper.delta()}
    return transform


def _make_instance_cell(helper):
    # (b) the closure cell is a callable model instance; its __dict__ holds the
    # module. Nested class -> by-value (worker reconstructs); describe encodes the
    # instance as actor + initial_state, where the module is name-only.
    class Model:
        def __init__(self, m):
            self.m = m

        def __call__(self, x):
            return x + self.m.delta()

    model = Model(helper)

    def transform(row):
        return {"input": row["x"], "value": model(row["x"])}
    return transform


def _make_partial_cell(helper):
    # (c) the closure cell is a functools.partial; its args hold the module.
    # describe encodes partial as actor + empty __dict__ -> args invisible.
    def use_module(m, x):
        return x + m.delta()

    part = functools.partial(use_module, helper)

    def transform(row):
        return {"input": row["x"], "value": part(row["x"])}
    return transform


FACTORIES = {"module_cell": _make_module_cell,
             "instance_dict": _make_instance_cell,
             "partial_args": _make_partial_cell}


# --- G1: closure-captured helper edit -> SAME RunRef resume refused (three forms) ---

def _checkpointable_capture(transform, trace):
    # These three fixtures have declaration-only captures: a module, a Model.m
    # module, or partial.args[0] module. Preserve that exact inner reference path;
    # the explicit owner declares that none of them is mutable business state.
    class CapturedTransform:
        def __init__(self):
            self.transform = transform

        def __call__(self, row):
            with trace.open("a") as stream:
                stream.write(str(row["x"]) + "\n")
            return self.transform(row)

        def snapshot_state(self):
            return {}

        def restore_state(self, state):
            assert state == {}

    return CapturedTransform()


def _assert_capture_resume_code_pin(tmp_path, monkeypatch, form, owner_factory):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import (assert_worker_killed, assert_released,
                                        materialize_action_id)
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bg1_" + form)
    transform = FACTORIES[form](helper_mod)
    captures = dict(zip(transform.__code__.co_freevars,
                        (cell.cell_contents for cell in transform.__closure__)))
    if form == "module_cell":
        assert captures["helper"] is helper_mod
    elif form == "instance_dict":
        assert vars(captures["model"])["m"] is helper_mod
    else:
        assert isinstance(captures["part"], functools.partial)
        assert captures["part"].args[0] is helper_mod
    rows_in = [{"x": i} for i in range(8)]

    def crashed_case(name):
        case = tmp_path / name
        case.mkdir()
        hook, marker = _crash_hook(case)
        trace = case / "business-calls"
        owner = owner_factory(transform, trace)
        root = case / "run"
        def pipeline():
            return (data.from_items(rows_in).map_async(owner, concurrency=1)
                    .map_async(hook, concurrency=1))
        with pytest.raises(DataflowError, match="UNKNOWN_EFFECT"):
            with RunContext.start(root, limits=LIMITS) as run:
                reference = run.ref
                with data.local_execution(workers=1, worker_mode="process", partitions=1):
                    pipeline().materialize().take_all()
        assert_worker_killed(marker, root)
        assert_released(root)
        return root, reference, pipeline, trace

    # Same original dependency shape is genuinely resumable before its code is
    # edited. This rules out counting an unrelated attempt-only refusal as pinning.
    control_root, control_ref, control_pipeline, _ = crashed_case("unchanged")
    with RunContext.resume(control_ref) as run:
        pre = [f for f in run.journal.execution_commits(materialize_action_id(run))
               if f["status"] == "acked"]
        assert pre
        with data.local_execution(workers=1, worker_mode="process", partitions=1):
            rows = control_pipeline().materialize().take_all()
        assert rows == [{"input": i, "value": i + 1} for i in range(8)]
        after = {f["commit_id"]: f for f in run.journal.execution_commits(materialize_action_id(run))}
        assert all(after[f["commit_id"]]["receipts"] == f["receipts"] for f in pre)
    assert_released(control_root)

    root, reference, pipeline, trace = crashed_case("edited")
    before_calls = trace.read_bytes()
    # Edit ONLY the captured helper; original callable and reference path survive.
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises((RecoveryContractError, DataflowError), match="code") as excinfo:
        with RunContext.resume(reference):
            with data.local_execution(workers=1, worker_mode="process", partitions=1):
                pipeline().materialize().take_all()
    if isinstance(excinfo.value, DataflowError):
        assert excinfo.value.code == "INVALID_CONTRACT"
        assert "Loaded program code differs from the sealed pin" in str(excinfo.value)
    assert trace.read_bytes() == before_calls, "edited code ran before pin verification"
    assert_released(root)


@pytest.mark.parametrize("form", FORMS)
def test_g1_closure_helper_edit_same_run_resume_refuses(tmp_path, monkeypatch, form):
    _assert_capture_resume_code_pin(tmp_path, monkeypatch, form, _checkpointable_capture)


# --- G3: closure-captured helper edit -> worker LOAD gate refuses (three forms) ---

@pytest.mark.parametrize("form", FORMS)
def test_g3_closure_helper_edit_worker_load_gate_refuses(sealed, tmp_path, monkeypatch, form):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_program_probe import worker_loader
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bg3_" + form)
    transform = FACTORIES[form](helper_mod)
    plan = _map_plan(transform)
    assert helper in _business_dependency_manifest(plan), "captured helper must be pinned"
    load = worker_loader(sealed, plan.operations[0].callable)
    load()      # unedited passes
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(DataflowError, match="Loaded program code differs"):
        load()


# --- G2: unedited manifest stable + captured helper pinned (three forms) ---

@pytest.mark.parametrize("form", FORMS)
def test_g2_closure_unedited_manifest_stable(tmp_path, monkeypatch, form):
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bg2_" + form)
    plan = _map_plan(FACTORIES[form](helper_mod))
    first = _business_dependency_manifest(plan)
    again = _business_dependency_manifest(plan)
    assert first == again, "two walks of unedited code must be byte-identical"
    assert helper in first, "captured business helper must be pinned"


# --- G2 control: a by-value closure with NO capture pins nothing (no regression) ---

def test_g2_no_capture_by_value_dependencies_empty(sealed):
    from runtime_v3_program_probe import worker_loader
    from demiflow.execution.seal.business_sources import _business_dependency_manifest

    def v1(row):
        return {**row, "score": row["x"] + 1}

    plan = _map_plan(v1)
    assert _business_dependency_manifest(plan) == {}
    load = worker_loader(sealed, plan.operations[0].callable)
    load()


# --- G2: a clean full run with a capturing closure succeeds (no false refuse) ---

@pytest.mark.parametrize("form", FORMS)
def test_g2_closure_clean_run_succeeds(tmp_path, monkeypatch, form):
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bclean_" + form)
    transform = FACTORIES[form](helper_mod)
    rows_in = [{"x": i} for i in range(8)]
    root = tmp_path / "run"
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            rows = (data.from_items(rows_in).map_async(transform, concurrency=1)
                    .materialize().take_all())
    assert {r["input"]: r["value"] for r in rows} == {i: i + 1 for i in range(8)}


# --- R2 containers: a closure capturing a list/dict holding the module is pinned ---

@pytest.mark.parametrize("container", ["list", "dict"])
def test_g2_container_capture_pinned_and_edit_refuses(sealed, tmp_path, monkeypatch, container):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_program_probe import worker_loader
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bcont_" + container)
    if container == "list":
        captured = [helper_mod]

        def transform(row):
            return {"input": row["x"], "value": row["x"] + captured[0].delta()}
    else:
        captured = {"m": helper_mod}

        def transform(row):
            return {"input": row["x"], "value": row["x"] + captured["m"].delta()}
    plan = _map_plan(transform)
    assert helper in _business_dependency_manifest(plan)
    load = worker_loader(sealed, plan.operations[0].callable)
    load()
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(DataflowError, match="Loaded program code differs"):
        load()


# --- R2 by-reference business class instance: type module AND __dict__ module pinned ---

def test_g2_by_reference_class_instance_type_module_pinned(tmp_path, monkeypatch):
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    suffix = uuid.uuid4().hex[:8]
    modelname, helper = "f7bmodel_" + suffix, "f7bmhelper_" + suffix
    sources = {
        helper: "def delta():\n    return 1\n",
        modelname: ("import %s\n\n\nclass Model:\n    def __init__(self):\n"
                    "        self.m = %s\n\n    def __call__(self, x):\n"
                    "        return x + self.m.delta()\n" % (helper, helper)),
    }
    model_module, paths = _load_modules(tmp_path, monkeypatch, sources, modelname)
    model = model_module.Model()          # by-reference business class instance

    def transform(row):
        return {"input": row["x"], "value": model(row["x"])}

    manifest = _business_dependency_manifest(_map_plan(transform))
    assert modelname in manifest, "by-reference business class type module must be pinned"
    assert helper in manifest, "instance __dict__ captured module must be pinned"


# --- G6: instance<->function mutual capture cycle terminates, helper still pinned ---

def test_g6_instance_function_cycle_terminates(tmp_path, monkeypatch):
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bcyc")

    class Node:
        pass

    node = Node()

    def fn(x):
        return x + helper_mod.delta() + node.extra

    node.fn = fn          # instance __dict__ holds fn ...
    node.extra = 0        # ... and fn's cell holds node -> a cycle the walk must break

    def transform(row):
        return {"input": row["x"], "value": fn(row["x"])}

    manifest = _business_dependency_manifest(_map_plan(transform))
    assert helper in manifest


# --- G6: a captured dynamic (no-source) business module fail-closes at seal ---

def test_g6_dynamic_captured_module_refuses(tmp_path):
    import types
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    dynamic = types.ModuleType("f7bdyn_" + uuid.uuid4().hex[:8])
    dynamic.delta = lambda: 1

    def transform(row):
        return {"input": row["x"], "value": row["x"] + dynamic.delta()}

    with pytest.raises(RecoveryContractError, match="no resolvable source"):
        _business_dependency_manifest(_map_plan(transform))


# --- G6: function / value / byte walk budgets fail-closed for by-value captures ---

def test_g6_function_budget_refuses(tmp_path, monkeypatch):
    import demiflow.execution.seal.business_sources as chain_mod
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bfb")
    monkeypatch.setattr(chain_mod, "_DEP_MAX_FUNCTIONS", 1)

    def inner(row):
        return row["x"]

    def outer(row):
        return helper_mod.delta() + inner(row)   # captures inner + helper -> 2 functions

    with pytest.raises(RecoveryContractError, match="function budget"):
        _business_dependency_manifest(_map_plan(outer))


def test_g6_value_budget_refuses(tmp_path, monkeypatch):
    import demiflow.execution.seal.business_sources as chain_mod
    from demiflow.execution.seal.business_sources import _business_dependency_manifest
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bvb")
    monkeypatch.setattr(chain_mod, "_DEP_MAX_NODES", 1)
    with pytest.raises(RecoveryContractError, match="value budget"):
        _business_dependency_manifest(_map_plan(_make_module_cell(helper_mod)))


# --- G4 queue: a capturing closure is eligible, seals the helper end-to-end on a
#     managed worker through queue_chain's shared stage_plan_artifact seam ---

def test_g4_queue_closure_end_to_end_seals_helper(tmp_path, monkeypatch):
    from test_mvp2_queue_unified import _config, _make_builder, _seed_tasks
    from runtime_v3_owned_crash import process_records
    from test_raw_fetch import origin_server
    from demiflow.execution.queue_sinks import QueueResultSink

    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bg4")

    def label_fn(row):
        helper_mod.delta()    # captured + used; discarded so the sink schema is unchanged
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    cfg = _config(tmp_path, 'g4')
    root = tmp_path / 'run'
    build = _make_builder(1, label_fn=label_fn)
    with origin_server() as (url, calls):
        channel, _ = _seed_tasks(cfg, tmp_path / 'seed', url, 1)
        with RunContext.start(root, limits=LIMITS) as run:
            with data.local_execution(workers=1, worker_mode="process", partitions=1):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
            _assert_sealed_helper(run, helper, paths[helper])
    assert channel.state('p')['counts'] == {'done_ok': 1}
    records = process_records(root)
    assert records and all(record['identity']['pid'] != os.getpid() for _, record in records.values())


def _assert_sealed_helper(run, helper, source):
    import hashlib
    import json
    from demiflow.execution.seal.plan import CODE_PIN_MEDIA, _read_json
    from demiflow.execution.sealed_plans import _v3_read_store, load_sealed_plan

    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    with run.journal._connection() as db:
        seals = [json.loads(row[0]) for row in db.execute(
            "SELECT document FROM runtime_sources WHERE action_id LIKE '%-dataflow-sealed'")]
    matches = []
    for seal in seals:
        bundle = load_sealed_plan(run.directory, seal['action_id'])
        ref = bundle['sealed'].code_pin_ref
        store, bounds = _v3_read_store(run.directory, store_id=ref.store_id)
        pins = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=CODE_PIN_MEDIA)
        matches.extend(pin['imports'][helper] for pin in pins['programs'].values()
                       if helper in pin.get('imports', {}))
    assert matches and all(record['sha256'] == digest for record in matches)


# --- G4 queue: edit the captured helper -> the shared resume seam refuses ---

def test_g4_queue_seam_edit_refuses(sealed, tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_program_probe import worker_loader
    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, "f7bg4n")

    def label_fn(row):
        helper_mod.delta()
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    load = worker_loader(sealed, _map_plan(label_fn).operations[0].callable)
    load()
    paths[helper].write_text("def delta():\n    return 9999\n")
    importlib.reload(sys.modules[helper])
    with pytest.raises(DataflowError, match="Loaded program code differs"):
        load()



@pytest.mark.ray
def test_g4_ray_worker_uses_sealed_captured_dependency(tmp_path, monkeypatch):
    ray = pytest.importorskip('ray')
    from demiflow.data.dataset import Dataset
    from demiflow.data.sources import ItemsSource
    from demiflow.execution.executors.ray import RayDatasetExecutor

    helper_mod, paths, helper = _helper_graph(tmp_path, monkeypatch, 'g4_ray')
    transform = _make_module_cell(helper_mod)
    assert not ray.is_initialized()
    ray.init(address='local', num_cpus=2, include_dashboard=False)
    try:
        with RunContext.start(tmp_path / 'run', limits=LIMITS) as run:
            dataset = Dataset(ItemsSource(({'x': 7},)), _map_plan(transform), RayDatasetExecutor())
            assert dataset.take_all() == [{'input': 7, 'value': 8}]
            _assert_sealed_helper(run, helper, paths[helper])
    finally:
        ray.shutdown()
