"""Lane Y regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

import importlib
import importlib.abc
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

WORKTREE = Path(__file__).resolve().parents[1]
TEST_FILE = Path(__file__).resolve()


class _RayImportBan(importlib.abc.MetaPathFinder):
    def __init__(self) -> None:
        self.attempts: list[str] = []

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "ray" or fullname.startswith("ray."):
            self.attempts.append(fullname)
            raise ImportError(f"G-Y local gate bans ray import: {fullname}")
        return None


_RAY_BAN = _RayImportBan()


@pytest.fixture(autouse=True)
def _ray_import_ban():
    sys.meta_path.insert(0, _RAY_BAN)
    try:
        yield
    finally:
        sys.meta_path.remove(_RAY_BAN)


# --- frozen-contract test-domain fakes (C14: spec-bound, test domain only) ---
_VALID_KINDS = {
    "take", "take_all", "take_batch", "iter_rows", "iter_batches", "count",
    "aggregate", "schema", "columns", "size_bytes", "materialize",
    "write_datasink", "write_lance", "write_file", "run_stream", "collect_summary",
}
_VALID_PROVIDERS = {"local", "ray_core"}
_VALID_FORMATS = {"items", "arrow", "numpy", "pandas"}


@dataclass(frozen=True)
class FakeActionProgram:
    name: str
    kind: str
    value: object

    def __post_init__(self) -> None:
        if self.kind not in {"aggregate", "sink"}:
            raise ValueError(f"ActionProgram.kind invalid: {self.kind!r}")


@dataclass(frozen=True)
class FakeBounds:
    max_nodes: int = 1024
    max_edges: int = 4096
    max_partitions: int = 64
    max_depth: int = 64
    max_control_bytes: int = 1 << 20
    max_row_bytes: int = 1 << 20
    max_batch_bytes: int = 8 << 20
    max_resident_bytes: int = 64 << 20
    max_spill_bytes: int = 256 << 20
    max_state_bytes: int = 8 << 20
    max_pending_events: int = 4096
    max_open_files: int = 256
    max_output_rows: int = 1 << 20
    timeout_ms: int = 120000


@dataclass(frozen=True)
class FakeRuntimePolicy:
    provider: str
    durability: str = "ephemeral"
    default_partitions: int = 4
    max_replicas: int = 4
    fusion: bool = True
    checkpoint_rows: int = 4096
    checkpoint_interval_ms: int = 1000
    fairness_quantum_bytes: int = 1 << 20
    control_timeout_ms: int = 120000
    store_id: str = "fake-store"
    required_recovery: str = "none"


@dataclass(frozen=True)
class FakeActionRequest:
    kind: str
    source: object
    plan: object
    arguments: dict
    sink: object
    programs: tuple
    provider: str
    action: object


class FakeInputHandle:
    def __init__(self, value: object, format: str) -> None:
        self.value = value
        self.format = format
        self.released = False

    def num_blocks(self) -> int:
        return 1

    def rows(self):
        return iter(self.value if isinstance(self.value, list) else [])

    def release(self) -> None:
        self.released = True


class FakeResultHandle:
    def __init__(self, kind: str = "materialize") -> None:
        self.kind = kind
        self.released = False
        self.closed = False
        self._usage = {"calls": 1}
        self._metadata = None

    def release(self) -> None:
        self.released = True

    def close(self) -> None:
        self.closed = True

    def stats(self) -> str:
        return f"stats:{self.kind}"

    def num_blocks(self) -> int:
        return 2

    def rows(self):
        return iter([{"id": 0}])

    def execution_metadata(self):
        return self._metadata

    def operator_llm_usage(self) -> dict:
        return dict(self._usage)


def _default_result(kind: str) -> Any:
    if kind in {"take", "take_all"}:
        return [{"id": 0}]
    if kind == "count":
        return 1
    if kind == "size_bytes":
        return 8
    if kind == "columns":
        return ["id"]
    if kind == "iter_rows":
        return iter([{"id": 0}])
    if kind == "iter_batches":
        return iter([[{"id": 0}]])
    return FakeResultHandle(kind)


class _FakeEngine:
    def __init__(self) -> None:
        self.requests: list[FakeActionRequest] = []
        self.run_calls: list[FakeActionRequest] = []
        self.policy_calls: list[dict] = []
        self.input_calls: list[dict] = []
        self.next_result: Any = None

    def reset(self) -> None:
        self.requests.clear()
        self.run_calls.clear()
        self.policy_calls.clear()
        self.input_calls.clear()
        self.next_result = None

    def resolve_runtime_policy(self, options, limits, *, provider):
        if provider not in _VALID_PROVIDERS:
            raise ValueError(f"provider invalid: {provider!r}")
        if not isinstance(options, dict) or not isinstance(limits, dict):
            raise TypeError("options/limits must be mappings")
        self.policy_calls.append(
            {"options": dict(options), "limits": dict(limits), "provider": provider}
        )
        return FakeRuntimePolicy(provider=provider), FakeBounds()

    def normalize_native_options(self, options, *, operation_kind):
        return dict(options or {})

    def build_input_handle(self, value, *, format, bounds=None, override_num_blocks=None):
        if format not in _VALID_FORMATS:
            raise ValueError(f"input format invalid: {format!r}")
        self.input_calls.append({"value": value, "format": format, "bounds": bounds})
        return FakeInputHandle(value=value, format=format)

    def make_action_request(self, executor, source, plan, *, kind, arguments,
                            sink=None, programs=()):
        if kind not in _VALID_KINDS:
            raise ValueError(f"ActionRequest.kind invalid: {kind!r}")
        if getattr(executor, "PROVIDER", None) not in _VALID_PROVIDERS:
            raise ValueError("executor provider invalid")
        if not isinstance(arguments, dict):
            raise TypeError("arguments must be a mapping")
        for program in programs:
            if not isinstance(program, FakeActionProgram):
                raise TypeError("programs must be ActionProgram declarations")
        request = FakeActionRequest(
            kind=kind, source=source, plan=plan, arguments=dict(arguments),
            sink=sink, programs=tuple(programs), provider=executor.PROVIDER,
            action=object(),
        )
        self.requests.append(request)
        return request

    def run_dataset_action(self, request):
        if not isinstance(request, FakeActionRequest):
            raise TypeError("run_dataset_action requires an ActionRequest")
        self.run_calls.append(request)
        if self.next_result is not None:
            return self.next_result
        return _default_result(request.kind)

    def explain_dataset(self, source, plan, *, provider, options):
        if provider not in _VALID_PROVIDERS:
            raise ValueError(f"provider invalid: {provider!r}")
        return {"supported": True, "provider": provider}


from demiflow.execution.contracts.dataflow import ActionProgram as FakeActionProgram

ENGINE = _FakeEngine()


def _real_module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_fake_module(name: str, **attrs) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__file__ = str(TEST_FILE)
    for key, value in attrs.items():
        setattr(module, key, value)
    if not _real_module_available(name):
        sys.modules[name] = module
        parent, _, child = name.rpartition(".")
        if parent and parent in sys.modules:
            setattr(sys.modules[parent], child, module)
    return module


# Import the real worktree packages, then inject the absent cross-lane facades.
import demiflow  # noqa: E402
import demiflow.execution  # noqa: E402
import demiflow.execution.contracts  # noqa: E402
import demiflow.planning  # noqa: E402

_install_fake_module(
    "demiflow.execution.contracts.dataflow", ActionProgram=FakeActionProgram,
)
_install_fake_module(
    "demiflow.planning.dataflow_policy",
    resolve_runtime_policy=ENGINE.resolve_runtime_policy,
    normalize_native_options=ENGINE.normalize_native_options,
)
_install_fake_module(
    "demiflow.execution.dataflow_inputs",
    build_input_handle=ENGINE.build_input_handle,
    InputMaterializedHandle=FakeInputHandle,
)
_install_fake_module(
    "demiflow.execution.dataflow_api",
    make_action_request=ENGINE.make_action_request,
    run_dataset_action=ENGINE.run_dataset_action,
    explain_dataset=ENGINE.explain_dataset,
)

ray_module = importlib.import_module("demiflow.execution.executors.ray")
RayDatasetExecutor = ray_module.RayDatasetExecutor

from demiflow.data.plan import LogicalPlan  # noqa: E402
from demiflow.data.sources import ItemsSource, MaterializedSource  # noqa: E402


def _assert_tree_binding() -> None:
    root = WORKTREE
    assert Path(demiflow.__file__).resolve().is_relative_to(root), demiflow.__file__
    for name, module in list(sys.modules.items()):
        if not name.startswith("demiflow"):
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            continue
        assert Path(path).resolve().is_relative_to(root), f"{name}: {path}"


_assert_tree_binding()


@pytest.fixture(autouse=True)
def _reset_engine(monkeypatch):
    """Unit-test the Ray adapter against explicit facade seams, never sys.modules."""
    import demiflow.execution.dataflow_api as api
    import demiflow.execution.dataflow_inputs as inputs
    import demiflow.planning.dataflow_policy as policy
    monkeypatch.setattr(api, "make_action_request", ENGINE.make_action_request)
    monkeypatch.setattr(api, "run_dataset_action", ENGINE.run_dataset_action)
    monkeypatch.setattr(api, "explain_dataset", ENGINE.explain_dataset)
    monkeypatch.setattr(inputs, "build_input_handle", ENGINE.build_input_handle)
    monkeypatch.setattr(policy, "resolve_runtime_policy", ENGINE.resolve_runtime_policy)
    monkeypatch.setattr(ray_module, "resolve_runtime_policy", ENGINE.resolve_runtime_policy)
    ENGINE.reset()
    _RAY_BAN.attempts.clear()
    _assert_tree_binding()
    yield
    assert _RAY_BAN.attempts == [], f"ray import attempted: {_RAY_BAN.attempts}"


@pytest.fixture
def executor():
    return RayDatasetExecutor()


@pytest.fixture
def source():
    return ItemsSource(items=({"id": 1}, {"id": 2}))


@pytest.fixture
def plan():
    return LogicalPlan()


class _Aggregate:
    def __init__(self, name: str) -> None:
        self.name = name


class _Datasink:
    pass


# --- Y001: __init__ maps provider to the unified policy; unified switches compat-only ---
def test_y001(executor):
    assert executor.NAME == "ray"
    assert executor.PROVIDER == "ray_core"
    assert ENGINE.policy_calls[-1]["provider"] == "ray_core"
    assert executor._unified_chain is False and executor._unified_graph is False
    legacy = RayDatasetExecutor(unified_chain=True, unified_graph=True)
    assert legacy._unified_chain is True and legacy._unified_graph is True
    assert ENGINE.policy_calls[-1]["provider"] == "ray_core"
    assert legacy.PROVIDER == executor.PROVIDER


def test_y002(executor):
    result = executor.from_items([{"id": 1}])
    assert isinstance(result, FakeInputHandle)
    assert ENGINE.input_calls[-1]["format"] == "items"
    assert result.value == [{"id": 1}]
    assert ENGINE.run_calls == []
    # override_num_blocks is accepted but unused; no bounds are imposed (parity L).
    hinted = executor.from_items([{"id": 1}], override_num_blocks=0)
    assert isinstance(hinted, FakeInputHandle)
    assert ENGINE.input_calls[-1]["bounds"] is None
    with pytest.raises(TypeError):
        executor.from_items("not-a-list")


def test_y003(executor):
    table = object()
    result = executor.from_arrow(table)
    assert isinstance(result, FakeInputHandle)
    assert ENGINE.input_calls[-1]["format"] == "arrow"
    assert result.value is table
    assert ENGINE.run_calls == []


def test_y004(executor):
    array = object()
    result = executor.from_numpy(array)
    assert isinstance(result, FakeInputHandle)
    assert ENGINE.input_calls[-1]["format"] == "numpy"
    assert ENGINE.run_calls == []


def test_y005(executor):
    frame = object()
    result = executor.from_pandas(frame, override_num_blocks=2)
    assert isinstance(result, FakeInputHandle)
    assert ENGINE.input_calls[-1]["format"] == "pandas"
    assert ENGINE.input_calls[-1]["bounds"] is None
    assert ENGINE.run_calls == []
    # Negative hint is accepted but unused; no bounds validation (parity L).
    executor.from_pandas(frame, override_num_blocks=-1)
    assert ENGINE.input_calls[-1]["bounds"] is None


def _assert_terminal(executor, source, plan, kind, call, expected_arguments=None):
    ENGINE.reset()
    result = call()
    assert len(ENGINE.requests) == 1
    request = ENGINE.requests[0]
    assert request.kind == kind
    assert request.provider == "ray_core"
    assert request.source is source and request.plan is plan
    assert ENGINE.run_calls == [request]
    if expected_arguments is not None:
        for key, value in expected_arguments.items():
            assert request.arguments[key] == value
    return result


def test_y006(executor, source, plan):
    _assert_terminal(executor, source, plan, "iter_rows",
                     lambda: executor.iter_rows(source, plan))


def test_y007(executor, source, plan):
    _assert_terminal(
        executor, source, plan, "iter_batches",
        lambda: executor.iter_batches(source, plan, prefetch_batches=3,
                                      batch_size=64, batch_format="pandas", drop_last=True),
        {"prefetch_batches": 3, "batch_size": 64, "batch_format": "pandas", "drop_last": True},
    )


def test_y008(executor, source, plan):
    result = _assert_terminal(executor, source, plan, "materialize",
                              lambda: executor.materialize(source, plan))
    assert isinstance(result, FakeResultHandle)


def test_y009(executor, source, plan):
    _assert_terminal(executor, source, plan, "take",
                     lambda: executor.take(source, plan, 5), {"limit": 5})


def test_y010(executor, source, plan):
    _assert_terminal(executor, source, plan, "take_batch",
                     lambda: executor.take_batch(source, plan, 32, batch_format="arrow"),
                     {"batch_size": 32, "batch_format": "arrow"})


def test_y011(executor, source, plan):
    result = _assert_terminal(executor, source, plan, "count",
                              lambda: executor.count(source, plan))
    assert result == 1


def test_y012(executor, source, plan):
    aggregates = (_Aggregate("sum"), _Aggregate("mean"))
    ENGINE.reset()
    executor.aggregate(source, plan, aggregates)
    request = ENGINE.requests[0]
    assert request.kind == "aggregate"
    assert request.arguments["names"] == ["sum", "mean"]
    assert len(request.programs) == 2
    assert all(isinstance(p, FakeActionProgram) and p.kind == "aggregate" for p in request.programs)
    assert request.programs[0].value is aggregates[0]
    assert request.programs[1].value is aggregates[1]
    assert ENGINE.run_calls == [request]


def test_y013(executor, source, plan):
    # fetch_if_missing=False short-circuits to None without any request (parity L).
    ENGINE.reset()
    assert executor.schema(source, plan, fetch_if_missing=False) is None
    assert ENGINE.requests == [] and ENGINE.run_calls == []
    _assert_terminal(executor, source, plan, "schema",
                     lambda: executor.schema(source, plan, fetch_if_missing=True),
                     {"fetch_if_missing": True})


def test_y014(executor, source, plan):
    ENGINE.reset()
    assert executor.columns(source, plan, fetch_if_missing=False) is None
    assert ENGINE.requests == [] and ENGINE.run_calls == []
    _assert_terminal(executor, source, plan, "columns",
                     lambda: executor.columns(source, plan, fetch_if_missing=True),
                     {"fetch_if_missing": True})


def test_y015(executor, source, plan):
    result = _assert_terminal(executor, source, plan, "size_bytes",
                              lambda: executor.size_bytes(source, plan))
    assert result == 8


def test_y016(executor, plan):
    handle = FakeInputHandle(value=[{"id": 1}], format="items")
    materialized = MaterializedSource(handle=handle, row_count=1)
    assert executor.num_blocks(materialized, plan) == 1
    with pytest.raises(TypeError):
        executor.num_blocks(ItemsSource(items=({"id": 1},)), plan)


def test_y017(executor, source, plan):
    assert "not executed" in executor.stats(source, plan)
    ENGINE.next_result = FakeResultHandle("materialize")
    executor.materialize(source, plan)
    # stats reflects the executor-held ExecutionMetadata, not the result handle.
    rendered = executor.stats(source, plan)
    assert "not executed" not in rendered
    assert "ItemsSource" in rendered


def test_y018(executor, source, plan):
    from demiflow.data.stats import ExecutionMetadata
    assert executor.execution_metadata(source, plan) is None
    ENGINE.next_result = FakeResultHandle("materialize")
    executor.materialize(source, plan)
    metadata = executor.execution_metadata(source, plan)
    assert isinstance(metadata, ExecutionMetadata)
    assert metadata.source_name == "ItemsSource"
    assert metadata.run_ref is None


def test_y019(executor, source, plan):
    datasink = _Datasink()
    ENGINE.reset()
    # Ray is a valid native target: native_options are forwarded to the unified
    # engine (P009 normalize_native_options owns the ActorPoolStrategy migration
    # diagnostic); the adapter never instantiates Ray Data itself.
    native = {"num_cpus": 2, "concurrency": 4}
    executor.write_datasink(source, plan, datasink, native_options=native)
    request = ENGINE.requests[0]
    assert request.kind == "write_datasink"
    assert request.sink is datasink
    assert request.arguments["native_options"] == native
    assert ENGINE.run_calls == [request]
    # Without native_options the argument is absent (no silent fabrication).
    ENGINE.reset()
    executor.write_datasink(source, plan, datasink)
    assert "native_options" not in ENGINE.requests[0].arguments


def test_y020(executor, source, plan):
    spec = object()
    ENGINE.reset()
    native = {"num_gpus": 1}
    executor.write_lance(source, plan, spec, native_options=native)
    request = ENGINE.requests[0]
    assert request.kind == "write_lance"
    assert len(request.programs) == 1
    assert request.programs[0].kind == "sink"
    assert request.programs[0].value is spec
    assert request.arguments["native_options"] == native
    assert ENGINE.run_calls == [request]


def test_y021(executor, source, plan):
    ENGINE.reset()
    options = {"compression": "snappy", "_demiflow_native_options": {"x": 1}}
    executor.write_file(source, plan, "parquet", "/tmp/demiflow-y/out", options)
    request = ENGINE.requests[0]
    assert request.kind == "write_file"
    assert request.arguments["format"] == "parquet"
    assert request.arguments["path"] == "/tmp/demiflow-y/out"
    # options forwarded verbatim as a copy; no native_options extraction (parity L).
    assert request.arguments["options"] == options
    assert "native_options" not in request.arguments
    assert request.arguments["options"] is not options
    assert ENGINE.run_calls == [request]


def test_y022(executor, source, plan):
    # Caller-owned result handles are never released by close (parity L).
    owned = FakeResultHandle("materialize")
    ENGINE.next_result = owned
    executor.materialize(source, plan)
    other = FakeResultHandle("materialize")
    # The executor's own hosted resource scope IS released on close.
    scope = FakeResultHandle("host")
    executor._hosted_scopes.append(scope)
    executor.close()
    assert scope.released is True
    assert owned.released is False
    assert other.released is False
    assert executor._hosted_scopes == []
    assert executor._closed is True
    executor.close()


def test_y023(executor, source, plan):
    ENGINE.reset()
    result = executor.plan(source, plan, "materialize")
    # plan() is a capability explanation via explain_dataset, not execution.
    assert result == {"supported": True, "provider": "ray_core"}
    assert ENGINE.requests == [] and ENGINE.run_calls == []
    assert _RAY_BAN.attempts == []


def test_y024(executor, source, plan):
    # No hosted coordinators -> empty usage; materialize does not fabricate usage.
    assert executor.operator_llm_usage() == {}
    ENGINE.next_result = FakeResultHandle("materialize")
    executor.materialize(source, plan)
    assert executor.operator_llm_usage() == {}

    class _Coordinator:
        def __init__(self, usage):
            self._usage = usage

        def usage(self):
            return types.SimpleNamespace(to_dict=lambda: dict(self._usage))

    executor._prompt_coordinators["a"] = _Coordinator({"calls": 2, "tokens": 10})
    executor._prompt_coordinators["b"] = _Coordinator({"calls": 1, "tokens": 5})
    assert executor.operator_llm_usage() == {"calls": 3, "tokens": 15}


def test_y025(executor, source, plan):
    dataset = types.SimpleNamespace(_source=source, _plan=plan)
    ENGINE.reset()
    executor.materialize_stream(dataset)
    request = ENGINE.requests[0]
    assert request.kind == "materialize"
    assert request.source is source and request.plan is plan
    assert ENGINE.run_calls == [request]


def test_y026(executor, source, plan):
    graph_spec = {
        "source": source, "source_id": "s0",
        "ancestor": {"plan": plan}, "targets": [],
        "routing": "hash", "key_fields": ["id"], "exchange_mode": "durable",
    }
    ENGINE.reset()
    executor.materialize_graph(graph_spec)
    request = ENGINE.requests[0]
    assert request.kind == "materialize"
    assert request.source is source and request.plan is plan
    assert request.arguments["routing"] == "hash"
    assert request.arguments["exchange_mode"] == "durable"
    assert request.arguments["key_fields"] == ["id"]
    assert ENGINE.run_calls == [request]


# --- Y027-Y037: retirement evidence (symbol absent + no Ray Data import) ---
def _assert_retired_module_symbol(name):
    assert not hasattr(ray_module, name), f"{name} still present"
    assert _RAY_BAN.attempts == []


def _assert_retired_method(name):
    assert not hasattr(RayDatasetExecutor, name), f"{name} still present"
    assert _RAY_BAN.attempts == []


def test_y027():
    _assert_retired_method("_compile")


def test_y028():
    _assert_retired_method("_build_source")


def test_y029():
    _assert_retired_method("_lower_stage_options")


def test_y030():
    _assert_retired_method("_chain_readback")


def test_y031():
    _assert_retired_module_symbol("_RayDatasourceAdapter")


def test_y032():
    _assert_retired_module_symbol("_RayLanceDatasource")


def test_y033():
    _assert_retired_module_symbol("_RayDatasinkAdapter")


def test_y034():
    _assert_retired_module_symbol("_RayLanceFragmentWriteSink")


def test_y035():
    assert not hasattr(ray_module, "_RayLanceFragmentWriteSink")
    assert not hasattr(ray_module, "ray_datasink")
    assert _RAY_BAN.attempts == []


def test_y036():
    _assert_retired_method("_write_lance_direct")


def test_y037():
    _assert_retired_module_symbol("_ray_aggregate")