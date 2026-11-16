"""Lane F regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

TREE = Path(__file__).resolve().parents[1]
TEST_DOUBLES: list[str] = []


# ---------------------------------------------------------------------------
# Spec-bound fakes (test domain only)
# ---------------------------------------------------------------------------
_ERROR_CODES = frozenset({
    "INVALID_CONTRACT", "BUDGET_EXCEEDED", "FENCED", "CANCELLED",
    "UNKNOWN_EFFECT", "INCOMPATIBLE_HISTORY",
})


class _FakeDataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        if code not in _ERROR_CODES:
            raise ValueError("unknown dataflow error code: " + repr(code))
        if code == "UNKNOWN_EFFECT" and retryable:
            raise ValueError("UNKNOWN_EFFECT must not be retryable")
        self.code = code
        self.retryable = retryable
        self.context = dict(context or {})
        super().__init__(code + ": " + message)


@dataclass(frozen=True)
class _FakeBounds:
    max_nodes: int = 4096
    max_edges: int = 8192
    max_partitions: int = 256
    max_depth: int = 64
    max_control_bytes: int = 1 << 20
    max_row_bytes: int = 1 << 20
    max_batch_bytes: int = 8 << 20
    max_resident_bytes: int = 256 << 20
    max_spill_bytes: int = 1 << 30
    max_state_bytes: int = 64 << 20
    max_pending_events: int = 4096
    max_open_files: int = 256
    max_output_rows: int = 1 << 20
    timeout_ms: int = 120000


@dataclass(frozen=True)
class _FakeRuntimePolicy:
    provider: str = "local"
    control_timeout_ms: int = 120000


@dataclass(frozen=True)
class _FakePlanInputBinding:
    port: str
    source: Any
    plan: Any
    path: tuple = ()


@dataclass(frozen=True)
class _FakeActionRequest:
    kind: str
    source: Any
    plan: Any
    arguments: Any
    sink: Any
    programs: tuple
    provider: str
    action: Any


@dataclass(frozen=True)
class _FakeTerminalResult:
    kind: str = "materialize"
    rows: Any = None
    scalar: Any = None
    result_ref: Any = None
    write_result: Any = None
    complete: bool = True
    completion: str = "source_exhausted"


class _FakeActionHandle:
    def __init__(self, result_value=None, rows=None):
        self._result = result_value
        self._rows = list(rows or [])
        self.closed = False

    def result(self):
        return self._result

    def rows(self):
        return iter(self._rows)

    def close(self):
        self.closed = True


class _FakeState:
    """Controllable fake behaviour shared across the wiring tests."""
    def __init__(self):
        self.execute_calls = []
        self.request_calls = []
        self.run_calls = []
        self.policy_calls = []
        self.relational_calls = []
        self.operation_calls = []
        self.handle = _FakeActionHandle(result_value={"target_rows": {}})
        self.run_result = iter([{"id": 0}])
        self.relational_bindings = None

    def reset(self):
        self.__init__()


STATE = _FakeState()


def _fake_execute_action(request, *, policy, bounds):
    STATE.execute_calls.append({"request": request, "policy": policy, "bounds": bounds})
    return STATE.handle


def _fake_make_action_request(executor, source, plan, *, kind, arguments, sink=None, programs=()):
    provider = {"local": "local", "ray": "ray_core"}.get(getattr(executor, "NAME", ""), "local")
    request = _FakeActionRequest(kind, source, plan, dict(arguments), sink, tuple(programs),
                                 provider, getattr(executor, "_fake_action", None))
    STATE.request_calls.append(request)
    return request


def _fake_run_dataset_action(request):
    STATE.run_calls.append(request)
    return STATE.run_result


def _fake_resolve_runtime_policy(options, limits, *, provider):
    STATE.policy_calls.append({"options": dict(options), "limits": dict(limits), "provider": provider})
    return _FakeRuntimePolicy(provider=provider), _FakeBounds()


def _fake_expand_relational_inputs(source, *, path, bounds):
    from demiflow.execution.local_kernel import KeyedSource, UnionSource
    STATE.relational_calls.append({"source": source, "path": path, "bounds": bounds})
    if STATE.relational_bindings is not None:
        return STATE.relational_bindings
    if isinstance(source, KeyedSource):
        if source.kind == "join":
            return (_FakePlanInputBinding("left", source.left.source, source.left.plan, path + ("left",)),
                    _FakePlanInputBinding("right", source.right.source, source.right.plan, path + ("right",)))
        return (_FakePlanInputBinding("left", source.left.source, source.left.plan, path + ("left",)),)
    if isinstance(source, UnionSource):
        return tuple(_FakePlanInputBinding(str(i), item.source, item.plan, path + (str(i),))
                     for i, item in enumerate(source.inputs))
    return ()


def _fake_expand_operation_inputs(operation, *, path, bounds):
    STATE.operation_calls.append({"operation": operation, "path": path, "bounds": bounds})
    return ()


def _real_module_available(name):
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _make_module(name):
    mod = types.ModuleType(name)
    mod.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
    if not _real_module_available(name):
        sys.modules[name] = mod
        TEST_DOUBLES.append(name)
    return mod


def _install_fakes():
    err = _make_module("demiflow.execution.contracts.dataflow_errors")
    err.DataflowError = _FakeDataflowError
    err.DATAFLOW_ERROR_CODES = _ERROR_CODES

    dto = _make_module("demiflow.execution.contracts.dataflow")
    dto.Bounds = _FakeBounds
    dto.RuntimePolicy = _FakeRuntimePolicy
    dto.ActionRequest = _FakeActionRequest
    dto.PlanInputBinding = _FakePlanInputBinding
    dto.TerminalResult = _FakeTerminalResult
    dto.MaterializedHandle = object
    dto.ActionProgram = object

    df = _make_module("demiflow.execution.dataflow")
    df.execute_action = _fake_execute_action

    api = _make_module("demiflow.execution.dataflow_api")
    api.make_action_request = _fake_make_action_request
    api.run_dataset_action = _fake_run_dataset_action

    pol = _make_module("demiflow.planning.dataflow_policy")
    pol.resolve_runtime_policy = _fake_resolve_runtime_policy

    comp = _make_module("demiflow.planning.dataflow_compile")
    comp.expand_relational_inputs = _fake_expand_relational_inputs
    comp.expand_operation_inputs = _fake_expand_operation_inputs


from demiflow.execution.contracts.dataflow_errors import DataflowError as _FakeDataflowError

# Production modules under test (real, in-tree).
from demiflow.execution import dataflow_inputs  # noqa: E402
from demiflow.execution.dataflow_inputs import build_input_handle, InputMaterializedHandle  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """These are adapter unit tests; fake only the called cross-module seam."""
    import demiflow.execution.dataflow as engine
    import demiflow.execution.dataflow_api as api
    import demiflow.planning.dataflow_policy as policy
    import demiflow.planning.dataflow_compile as compiler
    import demiflow.execution.driver as driver
    monkeypatch.setattr(engine, "execute_action", _fake_execute_action)
    monkeypatch.setattr(api, "make_action_request", _fake_make_action_request)
    monkeypatch.setattr(api, "run_dataset_action", _fake_run_dataset_action)
    monkeypatch.setattr(policy, "resolve_runtime_policy", _fake_resolve_runtime_policy)
    if hasattr(driver, "resolve_runtime_policy"):
        monkeypatch.setattr(driver, "resolve_runtime_policy", _fake_resolve_runtime_policy)
    monkeypatch.setattr(compiler, "expand_relational_inputs", _fake_expand_relational_inputs)
    monkeypatch.setattr(compiler, "expand_operation_inputs", _fake_expand_operation_inputs)
    STATE.reset()
    yield


@pytest.fixture(scope="session", autouse=True)
def _record_doubles(request):
    request.config._f_test_doubles = TEST_DOUBLES
    yield


@pytest.fixture
def fake_action(monkeypatch):
    """Provide a non-None current_action() for the wiring entries."""
    import demiflow.execution.run_context as rc

    class _Action:
        action_id = "fake-action"
        semantic_id = "fake-semantic"
        context = types.SimpleNamespace(ref=types.SimpleNamespace(run_id="fake-run"))

    monkeypatch.setattr(rc, "current_action", lambda: _Action())
    return _Action()


class _FakeExecutor:
    NAME = "local"

    def __init__(self, name="local", **kwargs):
        self.NAME = name
        self._fake_action = None


def _binding_assertion():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE), demiflow.__file__
    for name, mod in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            f = getattr(mod, "__file__", None)
            if f and name not in TEST_DOUBLES:
                assert Path(f).resolve().is_relative_to(TREE), (name, f)


# ---------------------------------------------------------------------------
# F001-F005 input handles
# ---------------------------------------------------------------------------
def test_f001():
    _binding_assertion()
    handle = build_input_handle([{"id": 1}, {"id": 2}], format="items")
    assert handle.num_blocks() == 1
    assert list(handle.rows()) == [{"id": 1}, {"id": 2}]
    with pytest.raises(_FakeDataflowError) as exc:
        build_input_handle([{"id": 1}], format="bogus")
    assert exc.value.code == "INVALID_CONTRACT"
    with pytest.raises(_FakeDataflowError):
        build_input_handle((x for x in []), format="items")


def test_f002():
    _binding_assertion()
    rows = [{"id": 1, "payload": b"\x00", "neg": -0.0, "t": (1, 2)}]
    handle = InputMaterializedHandle(rows, format="items", max_bytes=1 << 20)
    rows[0]["id"] = 999  # external mutation must not change sealed content
    out = list(handle.rows())
    assert out[0]["id"] == 1
    assert out[0]["payload"] == b"\x00"
    assert out[0]["t"] == (1, 2)
    import math
    assert math.copysign(1.0, out[0]["neg"]) == -1.0
    with pytest.raises(_FakeDataflowError) as exc:
        InputMaterializedHandle([{"id": 1}], format="items", max_bytes=1)
    assert exc.value.code == "BUDGET_EXCEEDED"


def test_f003():
    _binding_assertion()
    handle = build_input_handle([{"id": i} for i in range(3)], format="items")
    assert list(handle.rows()) == [{"id": 0}, {"id": 1}, {"id": 2}]
    empty = build_input_handle([], format="items")
    assert list(empty.rows()) == []


def test_f004():
    _binding_assertion()
    assert build_input_handle([], format="items").num_blocks() == 0
    assert build_input_handle([{"id": 1}], format="items").num_blocks() == 1


def test_f005():
    _binding_assertion()
    handle = build_input_handle([{"id": 1}], format="items")
    handle.release()
    handle.release()  # idempotent
    with pytest.raises(_FakeDataflowError) as exc:
        list(handle.rows())
    assert exc.value.code == "FENCED"


# ---------------------------------------------------------------------------
# F006 keyed_dataset
# ---------------------------------------------------------------------------
def test_f006(fake_action):
    _binding_assertion()
    from demiflow.execution.local_kernel import keyed_dataset
    from demiflow.data.api import DataAPI
    from demiflow.data.plan import LogicalPlan

    api = DataAPI(_FakeExecutor())
    left = api.from_items([{"k": 1, "v": "a"}])
    right = api.from_items([{"k": 1, "w": "b"}])
    ds = keyed_dataset(left, "join", ["k"], right=right, right_on=["k"])
    assert ds is not None
    assert len(STATE.relational_calls) == 1
    # executor-specific rejection removed: a non-local right executor is accepted
    right_ray = DataAPI(_FakeExecutor("ray")).from_items([{"k": 1, "w": "b"}])
    ds2 = keyed_dataset(left, "join", ["k"], right=right_ray, right_on=["k"])
    assert ds2 is not None


# ---------------------------------------------------------------------------
# F007-F011 DataAPI
# ---------------------------------------------------------------------------
def test_f007():
    _binding_assertion()
    from demiflow.data.api import DataAPI
    api = DataAPI(_FakeExecutor())
    ds = api.from_items([{"id": 1}])
    handle = ds._source.handle
    assert isinstance(handle, InputMaterializedHandle)
    assert ds._source.row_count == 1
    with pytest.raises(TypeError):
        api.from_items((x for x in [{"id": 1}]))


def test_f008():
    _binding_assertion()
    pa = pytest.importorskip("pyarrow")
    from demiflow.data.api import DataAPI
    api = DataAPI(_FakeExecutor())
    table = pa.table({"id": [1, 2]})
    ds = api.from_arrow(table)
    assert isinstance(ds._source.handle, InputMaterializedHandle)
    assert list(ds._source.handle.rows()) == [{"id": 1}, {"id": 2}]


def test_f009():
    _binding_assertion()
    np = pytest.importorskip("numpy")
    from demiflow.data.api import DataAPI
    api = DataAPI(_FakeExecutor())
    ds = api.from_numpy(np.array([1, 2, 3]))
    assert isinstance(ds._source.handle, InputMaterializedHandle)
    assert list(ds._source.handle.rows()) == [{"data": 1}, {"data": 2}, {"data": 3}]


def test_f010():
    _binding_assertion()
    pd = pytest.importorskip("pandas")
    from demiflow.data.api import DataAPI
    api = DataAPI(_FakeExecutor())
    ds = api.from_pandas(pd.DataFrame({"id": [1, 2]}))
    assert isinstance(ds._source.handle, InputMaterializedHandle)
    assert list(ds._source.handle.rows()) == [{"id": 1}, {"id": 2}]


def test_f011(tmp_path):
    _binding_assertion()
    from demiflow.data.api import DataAPI
    p = tmp_path / "rows.jsonl"
    p.write_text('{"value": 1}\n{"value": 2}\n')
    # non-local executor: the old class-name guard must no longer reject
    api = DataAPI(_FakeExecutor("ray"))
    ds = api.read_records(str(p))
    assert ds is not None


# ---------------------------------------------------------------------------
# F012 driver
# ---------------------------------------------------------------------------
def test_f012(monkeypatch, tmp_path):
    _binding_assertion()
    import demiflow.execution.driver as driver

    captured = {}

    class _Exec:
        NAME = "local"
        def __init__(self, **kw):
            captured["init"] = kw
        def close(self):
            captured["closed"] = True

    monkeypatch.setattr(driver, "LocalDatasetExecutor", _Exec)

    class _Pipeline:
        def run(self, *, dataset_executor):
            captured["executor"] = dataset_executor

    class _Def:
        class execution:
            backend_affinity = None
        entrypoint = "ep"

    monkeypatch.setattr(driver, "discover_pipeline_definition", lambda root: _Def())
    monkeypatch.setattr(driver, "Pipeline", types.SimpleNamespace(load=lambda root, pkg: _Pipeline()))
    monkeypatch.setattr(driver, "load_bundle_packages", lambda root, namespace=None: types.SimpleNamespace(pipeline_package="pkg"))
    monkeypatch.setattr(driver, "load_referenced_prompt_packs", lambda root: {})
    monkeypatch.setattr(driver, "required_environment", lambda names: None)
    monkeypatch.setattr(driver, "parse_platform_planning_policy", lambda policy: types.SimpleNamespace(digest="d", max_parallelism=2))
    monkeypatch.setattr(driver, "BUILT_IN_RULE_VERSION", "v1")

    class _Bundle:
        content_digest = "digest"
        runtime_abi = "abi"
        def __init__(self, **kw):
            pass
        def verify(self):
            captured["verified"] = True

    monkeypatch.setattr(driver, "PipelineBundleRef", _Bundle)

    import demiflow.execution.run_context as rc
    class _Ctx:
        @staticmethod
        def start(entry_identity):
            captured["identity"] = entry_identity
            import contextlib
            return contextlib.nullcontext()
    monkeypatch.setattr(rc, "RunContext", _Ctx)

    request = driver.PipelineDriverRequest(
        run_id="r", bundle=driver.PipelineBundleRef(root=str(tmp_path), content_digest="digest", runtime_abi="abi"),
        bundle_root=str(tmp_path), backend="local", platform_preflight_imports=("os",),
        planning_policy={}, planning_policy_digest="d", planning_rule_version="v1")
    driver.execute_driver(request)
    assert len(STATE.policy_calls) == 1
    assert STATE.policy_calls[0]["provider"] == "local"
    ex = captured["executor"]
    assert ex.run_dataset_action is not None
    assert ex.runtime_policy.provider == "local"


# ---------------------------------------------------------------------------
# F013-F017 chain/graph/queue wiring
# ---------------------------------------------------------------------------
def _dataset():
    from demiflow.data.api import DataAPI
    return DataAPI(_FakeExecutor()).from_items([{"id": 1}])


def test_f013(fake_action):
    _binding_assertion()
    from demiflow.execution.chain import run_local_chain
    from demiflow.execution.dataflow_api import declare_action_v3
    dataset = _dataset()
    fake_action.__class__.declaration = declare_action_v3(dataset._source, dataset._plan,
        kind='materialize', arguments={}, sink_declaration=None)
    STATE.run_result = "materialized-handle"
    out = run_local_chain(_FakeExecutor(), dataset)
    assert out == "materialized-handle"
    assert len(STATE.run_calls) == 1
    assert STATE.run_calls[0].kind == "materialize"
    assert STATE.run_calls[0].provider == "local"


def test_f014(fake_action):
    _binding_assertion()
    from demiflow.execution.chain import run_ray_chain
    from demiflow.execution.dataflow_api import declare_action_v3
    dataset = _dataset()
    fake_action.__class__.declaration = declare_action_v3(dataset._source, dataset._plan,
        kind='materialize', arguments={}, sink_declaration=None)
    STATE.run_result = "ray-handle"
    out = run_ray_chain(_FakeExecutor("ray"), dataset)
    assert out == "ray-handle"
    assert len(STATE.run_calls) == 1
    assert STATE.run_calls[0].provider == "ray_core"


def _graph_spec():
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource
    return {
        "source": ItemsSource([{"id": 1}]),
        "source_id": "src",
        "ancestor": {"plan": LogicalPlan(), "logical_node_id": "a", "occurrence_id": "ao"},
        "targets": [{"plan": LogicalPlan(), "logical_node_id": "t", "occurrence_id": "to", "reader_id": "r0"}],
    }


def test_f015(fake_action, monkeypatch):
    _binding_assertion()
    import demiflow.execution.graph as graph
    import demiflow.execution.executors.local as local_mod
    monkeypatch.setattr(local_mod, "LocalDatasetExecutor", _FakeExecutor)
    STATE.handle = _FakeActionHandle(result_value={"target_rows": {"r0": 1}})
    out = graph.run_local_graph(_graph_spec())
    assert out == {"target_rows": {"r0": 1}}
    assert STATE.execute_calls[0]["request"].kind == "materialize"


def test_f016(fake_action, monkeypatch):
    _binding_assertion()
    import demiflow.execution.graph as graph
    # ray is not installed in this environment; the graph entry imports the Ray
    # executor lazily, so bind a test-domain fake module (recorded as a double).
    if not _real_module_available("demiflow.execution.executors.ray"):
        fake_ray = types.ModuleType("demiflow.execution.executors.ray")
        fake_ray.RayDatasetExecutor = lambda: _FakeExecutor("ray")
        monkeypatch.setitem(sys.modules, "demiflow.execution.executors.ray", fake_ray)
        TEST_DOUBLES.append("demiflow.execution.executors.ray")
    STATE.handle = _FakeActionHandle(result_value={"target_rows": {"r0": 2}})
    out = graph.run_ray_graph(_graph_spec())
    assert out == {"target_rows": {"r0": 2}}
    assert STATE.execute_calls[0]["request"].provider == "ray_core"


def test_f017(fake_action):
    _binding_assertion()
    from demiflow.execution.queue_chain import run_local_queue_chain
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource
    STATE.handle = _FakeActionHandle(result_value="write-result")
    out = run_local_queue_chain(_FakeExecutor(), ItemsSource([{"id": 1}]), LogicalPlan(),
                                sink=None, action=fake_action)
    assert out == "write-result"
    assert STATE.execute_calls[0]["request"].kind == "write_datasink"


# ---------------------------------------------------------------------------
# F018-F019 dataset_native
# ---------------------------------------------------------------------------
def test_f018(fake_action, monkeypatch):
    _binding_assertion()
    from demiflow.execution.dataset_native import execute
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource
    from demiflow.execution.dataflow_api import declare_action_v3
    from demiflow.execution import run_context
    source, plan = ItemsSource([{"id": 1}]), LogicalPlan()
    fake_action.__class__.declaration = declare_action_v3(source, plan,
        kind='iter_rows', arguments={}, sink_declaration=None)
    monkeypatch.setattr(run_context, 'current_run', lambda: fake_action.context)
    STATE.run_result = (row for row in [{"id": 1}, {"id": 2}])
    out = list(execute(_FakeExecutor(), source, plan))
    assert out == [{"id": 1}, {"id": 2}]
    assert len(STATE.run_calls) == 1
    assert STATE.run_calls[0].kind == "iter_rows"
    import pyarrow as pa
    table = pa.table({'id': [1, 2]})
    STATE.run_result = (batch for batch in [table])
    out_arrow = list(execute(_FakeExecutor(), source, plan, arrow=True))
    assert out_arrow == table.to_batches()
    assert STATE.run_calls[1].kind == "iter_batches"


def test_f019(fake_action, monkeypatch):
    _binding_assertion()
    import demiflow.execution.dataset_native as native
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource
    import demiflow.data.dataset as dataset_mod

    calls = []
    def _fake_iter_rows(self):
        calls.append(self)
        return iter([{"id": 7}])
    monkeypatch.setattr(dataset_mod.Dataset, "iter_rows", _fake_iter_rows)

    engine = native.Engine.__new__(native.Engine)
    engine.stats = {}
    executor = _FakeExecutor()
    executor._local_kernel_stats = {}
    out = list(engine.python_rows(executor, ItemsSource([{"id": 1}]), LogicalPlan()))
    assert out == [{"id": 7}]
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# F020-F021 retire implicit prefix path
# ---------------------------------------------------------------------------
def test_f020():
    _binding_assertion()
    source = (TREE / "demiflow/execution/stream.py").read_text()
    assert "prefix_rows" not in source
    assert "stage.prefix is not None" not in source
    assert "_apply_plan" not in source


def test_f021():
    _binding_assertion()
    import demiflow.execution.source_snapshot as snap
    assert not hasattr(snap, "prefix_rows")
    assert hasattr(snap, "freeze_source")
    assert hasattr(snap, "capture_source")
    source = (TREE / "demiflow/execution/source_snapshot.py").read_text()
    assert "_apply_plan" not in source
