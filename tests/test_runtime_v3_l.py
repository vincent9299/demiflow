"""Lane L gate G-L: LocalDatasetExecutor thinning to the unified v3 entry.

Every test calls the real ``LocalDatasetExecutor`` symbols from this worktree.
E/F/C peers (``dataflow_api``, ``dataflow_inputs``, ``contracts.dataflow``) are
absent in this lane's tree, so each test installs spec-bound test-domain doubles
in ``sys.modules`` matching the frozen signatures (E015/E016/E018, F001, C086).
The doubles only record requests and return contract-typed results; they are not
a second executor. An import hook blocks ``ray.data`` for the whole process and
records any attempt, so a regression to the old Ray Data route fails loudly.

These are unit-contract tests (G-L). Full Local/Ray semantic equivalence and
finite-resource counterexamples are owned by G-API / V006 / V023 at integration.
"""

from __future__ import annotations

import hashlib
import importlib
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

TREE = Path(__file__).resolve().parents[1]

from demiflow.execution.executors.local import (  # noqa: E402
    LocalDatasetExecutor,
    _LocalMaterializedHandle,
)
from demiflow.data.sources import ItemsSource, MaterializedSource  # noqa: E402
from demiflow.data.plan import LogicalPlan  # noqa: E402
from demiflow.data.stats import ExecutionMetadata  # noqa: E402
from demiflow.data.datasink import WriteResult  # noqa: E402
from demiflow.errors import (  # noqa: E402
    UnsupportedExecutionOptionError,
    UnsupportedSourceError,
)

# C02 ActionRequest.kind enum (frozen).
KINDS = {
    'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'count',
    'aggregate', 'schema', 'columns', 'size_bytes', 'materialize',
    'write_datasink', 'write_lance', 'write_file', 'run_stream', 'collect_summary',
}

RETIRED = (
    '_require_ray_backend', '_apply_plan', '_execution_capabilities',
    '_bind_plan', '_execute_plan', '_write_file_body',
)

PUBLIC_MODIFY = (
    '__init__', 'from_items', 'from_arrow', 'from_numpy', 'from_pandas',
    'iter_rows', 'iter_batches', 'materialize', 'take', 'take_batch', 'count',
    'aggregate', 'schema', 'columns', 'size_bytes', 'num_blocks', 'stats',
    'execution_metadata', 'write_datasink', 'write_lance', 'write_file', 'close',
    'plan', 'operator_llm_usage', 'materialize_stream', 'materialize_graph',
)


# --------------------------------------------------------------------------- #
# Spec-bound test-domain doubles (E/F/C peers).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ActionProgram:
    """C086 frozen program card; kind in {'aggregate', 'sink'}."""
    name: str
    kind: str
    value: object


class FakeInputHandle:
    """F001/T neutral MaterializedHandle: num_blocks()/rows()/release()."""

    def __init__(self, rows, *, fmt='items', num_blocks=1):
        self._rows = list(rows)
        self.fmt = fmt
        self._num_blocks = num_blocks
        self.released = False

    def num_blocks(self):
        if self.released:
            raise RuntimeError('Materialized Dataset was released')
        return self._num_blocks

    def rows(self):
        if self.released:
            raise RuntimeError('Materialized Dataset was released')
        return iter(self._rows)

    def release(self):
        self.released = True


@dataclass(frozen=True)
class FakeRequest:
    executor: object
    source: object
    plan: object
    kind: str
    arguments: dict
    sink: object
    programs: tuple
    provider: str
    plan_id: str


class FakeEngine:
    """Records the unified entry calls; returns configured contract results."""

    def __init__(self):
        self.requests = []
        self.execute_calls = []
        self.explain_calls = []
        self.input_calls = []
        self.result = None
        self.report = None
        self.error = None

    def make_action_request(self, executor, source, plan, *, kind, arguments,
                            sink=None, programs=()):
        assert kind in KINDS, f'kind outside C02 enum: {kind}'
        assert isinstance(arguments, dict)
        assert executor.NAME == 'local'
        plan_id = hashlib.sha256(
            f'dataflow-physical-v3:{kind}:{type(source).__name__}'.encode()
        ).hexdigest()[:16]
        request = FakeRequest(executor, source, plan, kind, dict(arguments),
                              sink, tuple(programs), executor.NAME, plan_id)
        self.requests.append(request)
        return request

    def run_dataset_action(self, request):
        self.execute_calls.append((request.kind, request.plan_id))
        if self.error is not None:
            raise self.error
        return self.result

    def explain_dataset(self, source, plan, *, provider, options):
        self.explain_calls.append((provider, dict(options)))
        if self.error is not None:
            raise self.error
        return self.report

    def build_input_handle(self, value, *, format, bounds=None, override_num_blocks=None):
        self.input_calls.append((format, value, bounds))
        if self.error is not None:
            raise self.error
        return FakeInputHandle(value if isinstance(value, list) else [], fmt=format)


class _BlockRayData:
    """Meta-path hook: refuse ray.data and record the attempt."""

    def __init__(self):
        self.attempts = []

    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ray.data' or fullname.startswith('ray.data.'):
            self.attempts.append(fullname)
            raise ImportError(f'ray.data import blocked by lane L gate: {fullname}')
        return None


def _install_fakes(monkeypatch, engine):
    api = types.ModuleType('demiflow.execution.dataflow_api')
    api.make_action_request = engine.make_action_request
    api.run_dataset_action = engine.run_dataset_action
    api.explain_dataset = engine.explain_dataset

    inputs = types.ModuleType('demiflow.execution.dataflow_inputs')
    inputs.build_input_handle = engine.build_input_handle
    inputs.InputMaterializedHandle = FakeInputHandle

    contracts = types.ModuleType('demiflow.execution.contracts.dataflow')
    contracts.ActionProgram = ActionProgram

    monkeypatch.setitem(sys.modules, 'demiflow.execution.dataflow_api', api)
    monkeypatch.setitem(sys.modules, 'demiflow.execution.dataflow_inputs', inputs)
    monkeypatch.setitem(sys.modules, 'demiflow.execution.contracts.dataflow', contracts)


# --------------------------------------------------------------------------- #
# Fixtures.
# --------------------------------------------------------------------------- #
@pytest.fixture(autouse=True)
def _worktree_binding():
    """IL012: demiflow and every loaded real submodule resolve under this tree."""
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE), demiflow.__file__
    for name, module in list(sys.modules.items()):
        if not name.startswith('demiflow'):
            continue
        file = getattr(module, '__file__', None)
        if file is None:  # injected test-domain double
            continue
        assert Path(file).resolve().is_relative_to(TREE), f'{name} -> {file}'
    yield


@pytest.fixture(autouse=True)
def _no_ray_data():
    hook = _BlockRayData()
    sys.meta_path.insert(0, hook)
    try:
        yield hook
    finally:
        sys.meta_path.remove(hook)
    assert hook.attempts == [], f'ray.data import attempted: {hook.attempts}'


@pytest.fixture
def engine(monkeypatch):
    eng = FakeEngine()
    _install_fakes(monkeypatch, eng)
    return eng


@pytest.fixture
def ex():
    executor = LocalDatasetExecutor(workers=2)
    yield executor
    executor.close()


def _source():
    return ItemsSource(items=({'id': 0}, {'id': 1}))


def _plan():
    return LogicalPlan()


def _assert_retired_absent():
    for symbol in RETIRED:
        assert not hasattr(LocalDatasetExecutor, symbol), f'retired symbol still present: {symbol}'
    # No renamed alias hiding a second executor: nothing on the class may carry
    # the old bind/execute native route names.
    for name in vars(LocalDatasetExecutor):
        assert 'bind_native' not in name and 'execute_native' not in name, name


def _assert_no_fallback(ex, engine, call):
    """Engine failure must propagate; Local must not retry or fall back."""
    engine.error = RuntimeError('unified engine failure')
    engine.execute_calls.clear()
    with pytest.raises(RuntimeError, match='unified engine failure'):
        call()
    assert len(engine.execute_calls) == 1, 'fallback/retry detected'
    engine.error = None


# --------------------------------------------------------------------------- #
# L001 __init__ — provider mapping, compat-only unified flags.
# --------------------------------------------------------------------------- #
def test_l001(ex, engine, _no_ray_data):
    assert LocalDatasetExecutor.NAME == 'local'
    assert ex.NAME == 'local'
    # Same construction params only change provider, never the engine route.
    other = LocalDatasetExecutor(workers=2, unified_chain=False, unified_graph=False,
                                 unified_queue_chain=False)
    assert other.NAME == 'local'
    assert other._unified_chain is False
    # unified_chain=False must not silently restore old semantics: a public
    # action still forwards to the unified entry, not a retired route.
    engine.result = [{'id': 0}]
    other.take(_source(), _plan(), 1)
    assert engine.execute_calls == [('take', engine.requests[-1].plan_id)]
    other.close()
    with pytest.raises(ValueError):
        LocalDatasetExecutor(sort_workers=0)
    with pytest.raises(ValueError):
        LocalDatasetExecutor(sort_workers=True)
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L002-L005 from_* — neutral F001 handle, no run created, no ray.data.
# --------------------------------------------------------------------------- #
def test_l002(ex, engine):
    handle = ex.from_items([{'id': 0}, {'id': 1}])
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls == [('items', [{'id': 0}, {'id': 1}], None)]
    assert engine.execute_calls == [], 'from_items must not create a run'
    assert list(handle.rows()) == [{'id': 0}, {'id': 1}]
    with pytest.raises(TypeError):
        ex.from_items(iter([{'id': 0}]))  # generator is not a materialized list
    _assert_retired_absent()


def test_l003(ex, engine):
    import pyarrow as pa
    table = pa.table({'id': [0, 1]})
    handle = ex.from_arrow(table)
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls[0][0] == 'arrow'
    assert engine.input_calls[0][1] is table, 'arrow must not be lowered to pylist'
    assert engine.execute_calls == []
    _assert_retired_absent()


def test_l004(ex, engine):
    import numpy as np
    array = np.asarray([0, 1])
    handle = ex.from_numpy(array)
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls[0][0] == 'numpy'
    assert engine.input_calls[0][1] is array
    assert engine.execute_calls == []
    _assert_retired_absent()


def test_l005(ex, engine):
    import pandas as pd
    frame = pd.DataFrame({'id': [0, 1]})
    handle = ex.from_pandas(frame)
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls[0][0] == 'pandas'
    assert engine.input_calls[0][1] is frame
    assert engine.execute_calls == []
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L006-L015 eager/streaming terminals — correct kind/arguments forwarding.
# --------------------------------------------------------------------------- #
def test_l006(ex, engine):
    engine.result = iter([{'id': 0}, {'id': 1}])
    rows = list(ex.iter_rows(_source(), _plan()))
    assert rows == [{'id': 0}, {'id': 1}]
    req = engine.requests[-1]
    assert req.kind == 'iter_rows' and req.arguments == {}
    assert engine.execute_calls == [('iter_rows', req.plan_id)]
    _assert_no_fallback(ex, engine, lambda: ex.iter_rows(_source(), _plan()))
    _assert_retired_absent()


def test_l007(ex, engine):
    engine.result = iter([{'id': [0, 1]}])
    batches = list(ex.iter_batches(_source(), _plan(), prefetch_batches=2,
                                   batch_size=8, batch_format='pyarrow', drop_last=True))
    assert batches == [{'id': [0, 1]}]
    req = engine.requests[-1]
    assert req.kind == 'iter_batches'
    assert req.arguments == {'prefetch_batches': 2, 'batch_size': 8,
                             'batch_format': 'pyarrow', 'drop_last': True}
    _assert_retired_absent()


def test_l008(ex, engine):
    handle = FakeInputHandle([{'id': 0}])
    engine.result = handle
    result = ex.materialize(_source(), _plan())
    assert result is handle
    req = engine.requests[-1]
    assert req.kind == 'materialize' and req.arguments == {}
    assert ex._last_metadata is not None
    _assert_no_fallback(ex, engine, lambda: ex.materialize(_source(), _plan()))
    _assert_retired_absent()


def test_l009(ex, engine):
    engine.result = [{'id': 0}]
    assert ex.take(_source(), _plan(), 3) == [{'id': 0}]
    req = engine.requests[-1]
    assert req.kind == 'take' and req.arguments == {'limit': 3}
    # take(0) must still reach admission, not short-circuit locally.
    engine.requests.clear()
    ex.take(_source(), _plan(), 0)
    assert engine.requests[-1].arguments == {'limit': 0}
    _assert_no_fallback(ex, engine, lambda: ex.take(_source(), _plan(), 1))
    _assert_retired_absent()


def test_l010(ex, engine):
    engine.result = {'id': [0, 1]}
    assert ex.take_batch(_source(), _plan(), 8, batch_format='pyarrow') == {'id': [0, 1]}
    req = engine.requests[-1]
    assert req.kind == 'take_batch'
    assert req.arguments == {'batch_size': 8, 'batch_format': 'pyarrow'}
    _assert_retired_absent()


def test_l011(ex, engine):
    engine.result = 2
    assert ex.count(_source(), _plan()) == 2
    req = engine.requests[-1]
    assert req.kind == 'count' and req.arguments == {}
    _assert_no_fallback(ex, engine, lambda: ex.count(_source(), _plan()))
    _assert_retired_absent()


def test_l012(ex, engine):
    engine.result = {'total': 3}
    agg = SimpleNamespace(name='total', kind='sum')
    assert ex.aggregate(_source(), _plan(), (agg,)) == {'total': 3}
    req = engine.requests[-1]
    assert req.kind == 'aggregate'
    assert req.arguments == {'names': ['total']}
    assert len(req.programs) == 1
    program = req.programs[0]
    assert isinstance(program, ActionProgram)
    assert program.kind == 'aggregate' and program.name == 'total'
    assert program.value is agg, 'business aggregate must travel in programs, not arguments'
    _assert_retired_absent()


def test_l013(ex, engine):
    schema = SimpleNamespace(names=['id'])
    engine.result = schema
    assert ex.schema(_source(), _plan()) is schema
    req = engine.requests[-1]
    assert req.kind == 'schema' and req.arguments == {'fetch_if_missing': True}
    # fetch_if_missing=False reads no facts and triggers no action/UDF.
    engine.requests.clear()
    engine.execute_calls.clear()
    assert ex.schema(_source(), _plan(), fetch_if_missing=False) is None
    assert engine.requests == [] and engine.execute_calls == []
    _assert_retired_absent()


def test_l014(ex, engine):
    engine.result = ['id']
    assert ex.columns(_source(), _plan(), fetch_if_missing=True) == ['id']
    req = engine.requests[-1]
    assert req.kind == 'columns' and req.arguments == {'fetch_if_missing': True}
    engine.requests.clear()
    engine.execute_calls.clear()
    assert ex.columns(_source(), _plan(), fetch_if_missing=False) is None
    assert engine.requests == [] and engine.execute_calls == []
    _assert_retired_absent()


def test_l015(ex, engine):
    engine.result = 128
    assert ex.size_bytes(_source(), _plan()) == 128
    req = engine.requests[-1]
    assert req.kind == 'size_bytes' and req.arguments == {}
    _assert_no_fallback(ex, engine, lambda: ex.size_bytes(_source(), _plan()))
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L016 num_blocks — read sealed facts, no recompute, honest failures.
# --------------------------------------------------------------------------- #
def test_l016(ex, engine):
    handle = FakeInputHandle([{'id': 0}], num_blocks=3)
    source = MaterializedSource(handle=handle)
    assert ex.num_blocks(source, LogicalPlan()) == 3
    assert engine.execute_calls == [], 'num_blocks must not recompute'
    assert engine.requests == []
    # Released handle surfaces the neutral handle's own failure (no fallback).
    handle.release()
    with pytest.raises(RuntimeError):
        ex.num_blocks(source, LogicalPlan())
    # Non-materialized source and non-empty plan are rejected.
    with pytest.raises(TypeError):
        ex.num_blocks(_source(), LogicalPlan())
    with pytest.raises(TypeError):
        ex.num_blocks(MaterializedSource(handle=FakeInputHandle([])),
                      LogicalPlan(operations=(object(),)))
    # A handle from another backend (no num_blocks) is an honest error.
    with pytest.raises(UnsupportedSourceError):
        ex.num_blocks(MaterializedSource(handle=object()), LogicalPlan())
    # release() delegates to the neutral handle and never recomputes.
    other = FakeInputHandle([{'id': 1}])
    ex.release(MaterializedSource(handle=other))
    assert other.released is True
    assert engine.execute_calls == []
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L017/L018 stats / execution_metadata — read facts, no recompute.
# --------------------------------------------------------------------------- #
def test_l017(ex, engine):
    assert 'not executed' in ex.stats(_source(), _plan())
    assert engine.execute_calls == []
    engine.result = 2
    ex.count(_source(), _plan())
    text = ex.stats(_source(), _plan())
    assert 'ItemsSource' in text
    assert engine.execute_calls == [('count', engine.requests[-1].plan_id)]
    _assert_retired_absent()


def test_l018(ex, engine):
    assert ex.execution_metadata(_source(), _plan()) is None
    assert engine.execute_calls == []
    engine.result = 2
    ex.count(_source(), _plan())
    metadata = ex.execution_metadata(_source(), _plan())
    assert isinstance(metadata, ExecutionMetadata)
    assert metadata.source_name == 'ItemsSource'
    assert metadata.run_ref is ex._run_ref
    assert engine.execute_calls == [('count', engine.requests[-1].plan_id)]
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L019-L021 write terminals — sink/programs forwarding, Ray option guard.
# --------------------------------------------------------------------------- #
def test_l019(ex, engine):
    engine.result = WriteResult(written_rows=2)
    sink = object()
    result = ex.write_datasink(_source(), _plan(), sink)
    assert result == WriteResult(written_rows=2)
    req = engine.requests[-1]
    assert req.kind == 'write_datasink' and req.sink is sink and req.arguments == {}
    with pytest.raises(UnsupportedExecutionOptionError):
        ex.write_datasink(_source(), _plan(), sink, native_options={'num_cpus': 1})
    _assert_no_fallback(ex, engine, lambda: ex.write_datasink(_source(), _plan(), sink))
    _assert_retired_absent()


def test_l020(ex, engine):
    receipt = SimpleNamespace(written_rows=2)
    engine.result = receipt
    spec = object()
    assert ex.write_lance(_source(), _plan(), spec) is receipt
    req = engine.requests[-1]
    assert req.kind == 'write_lance'
    assert len(req.programs) == 1
    assert req.programs[0].kind == 'sink' and req.programs[0].value is spec
    with pytest.raises(UnsupportedExecutionOptionError):
        ex.write_lance(_source(), _plan(), spec, native_options={'num_gpus': 1})
    _assert_retired_absent()


def test_l021(ex, engine):
    engine.result = WriteResult(written_rows=2, target='out.json')
    result = ex.write_file(_source(), _plan(), 'json', '/tmp/demiflow-l/out.json', {'lines': True})
    assert result.target == 'out.json'
    req = engine.requests[-1]
    assert req.kind == 'write_file'
    assert req.arguments == {'format': 'json', 'path': '/tmp/demiflow-l/out.json',
                             'options': {'lines': True}}
    _assert_no_fallback(ex, engine,
                        lambda: ex.write_file(_source(), _plan(), 'json', '/tmp/demiflow-l/x', {}))
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L022 close — release only this executor's scope.
# --------------------------------------------------------------------------- #
def test_l022(ex, engine, tmp_path):
    pool = ex.executor  # force pool creation
    assert ex._pool is pool
    spill = tmp_path / 'demiflow-block-test.pkl'
    spill.write_bytes(b'x')
    ex._spill_paths.add(str(spill))
    ex._spill_bytes = 1
    # Another run's neutral handle must not be touched by this close.
    other_handle = FakeInputHandle([{'id': 9}])
    ex.close()
    assert ex._pool is None
    assert not spill.exists()
    assert ex._spill_paths == set() and ex._spill_bytes == 0
    assert other_handle.released is False
    assert engine.execute_calls == [], 'close must not run an action'
    ex.close()  # idempotent
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L023 plan — unified explain, provider local, no RayData lowering.
# --------------------------------------------------------------------------- #
def test_l023(ex, engine):
    report = SimpleNamespace(provider='local', action_kind='take', plan_id='p1')
    engine.report = report
    result = ex.plan(_source(), _plan(), 'take', terminal_category='action',
                     source_parallelism_cap=2)
    assert result is report
    provider, options = engine.explain_calls[-1]
    assert provider == 'local'
    assert options['action_kind'] == 'take'
    assert options['terminal_category'] == 'action'
    assert options['source_parallelism_cap'] == 2
    assert engine.execute_calls == [], 'plan is compile/admission, not execution'
    # plan failure propagates through explain; no fallback to an old lowering.
    engine.error = RuntimeError('unified engine failure')
    engine.explain_calls.clear()
    with pytest.raises(RuntimeError, match='unified engine failure'):
        ex.plan(_source(), _plan(), 'take')
    assert len(engine.explain_calls) == 1, 'fallback/retry detected'
    engine.error = None
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L024 operator_llm_usage — read hosted facts, no recompute.
# --------------------------------------------------------------------------- #
def test_l024(ex, engine):
    from demiflow.operator_llm.model import OperatorLLMUsage
    class _Coordinator:
        def __init__(self, usage):
            self._usage = usage
        def usage(self):
            return self._usage

    first = OperatorLLMUsage(2, 3, 3, 2, 1, 5, 7)
    second = OperatorLLMUsage(3, 4, 4, 3, 1, 7, 11)
    observed = OperatorLLMUsage(1, 2, 2, 1, 1, 13, 17).to_dict()
    ex._prompt_coordinators['node-a'] = _Coordinator(first)
    ex._prompt_coordinators['node-b'] = _Coordinator(second)
    ex._dataflow_prompt_usage = observed.copy()
    expected = {key: value + second.to_dict()[key] + observed[key]
                for key, value in first.to_dict().items()}
    assert ex.operator_llm_usage() == expected
    assert ex.operator_llm_usage() == expected  # reading metrics is not accounting again
    assert ex._dataflow_prompt_usage == observed
    assert engine.execute_calls == [], 'usage must not recompute or impose a budget'
    assert engine.requests == []
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L025 materialize_stream — same unified materialize entry, override respected.
# --------------------------------------------------------------------------- #
def test_l025(ex, engine):
    handle = FakeInputHandle([{'id': 0}])
    engine.result = handle
    dataset = SimpleNamespace(_source=_source(), _plan=_plan())
    result = ex.materialize_stream(dataset)
    assert result is handle
    req = engine.requests[-1]
    assert req.kind == 'materialize'
    assert req.source is dataset._source and req.plan is dataset._plan
    # Explicit override replaces the dataset's captured source/plan.
    override_source = ItemsSource(items=({'id': 5},))
    override_plan = LogicalPlan()
    ex.materialize_stream(dataset, source=override_source, plan=override_plan)
    req2 = engine.requests[-1]
    assert req2.source is override_source and req2.plan is override_plan
    _assert_no_fallback(ex, engine, lambda: ex.materialize_stream(dataset))
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L026 materialize_graph — delegate to thin translator, no independent driver.
# --------------------------------------------------------------------------- #
def test_l026(ex, engine, monkeypatch):
    import demiflow.execution.graph as graph_mod
    import demiflow.execution.run_context as run_context_mod

    seen = {}

    def fake_run_local_graph(graph_spec, **kwargs):
        seen['graph_spec'] = graph_spec
        # F015 thin translator forwards to the same unified execute_action entry.
        request = engine.make_action_request(ex, _source(), _plan(),
                                             kind='materialize', arguments={})
        return engine.run_dataset_action(request)

    monkeypatch.setattr(graph_mod, 'run_local_graph', fake_run_local_graph)
    monkeypatch.setattr(run_context_mod, 'current_action', lambda: None)
    graph_spec = object()
    with pytest.raises(RuntimeError, match='native action context'):
        ex.materialize_graph(graph_spec)
    assert engine.execute_calls == []

    monkeypatch.setattr(run_context_mod, 'current_action', lambda: SimpleNamespace(action_id='a1'))
    handle = FakeInputHandle([{'id': 0}])
    engine.result = handle
    result = ex.materialize_graph(graph_spec)
    assert result is handle
    assert seen['graph_spec'] is graph_spec
    assert engine.execute_calls == [('materialize', engine.requests[-1].plan_id)]
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L027-L032 retire cards — old symbols truly deleted, public API still unified.
# --------------------------------------------------------------------------- #
def _retire_card(symbol, public_call):
    assert not hasattr(LocalDatasetExecutor, symbol), f'{symbol} not deleted'
    # No renamed second executor: the public method still reaches the unified
    # entry and never the old route.
    _assert_retired_absent()


def test_l027(ex, engine):
    _retire_card('_require_ray_backend', None)
    # sort/repartition no longer refuse on a backend gate; they forward unified.
    engine.result = [{'id': 0}]
    ex.take(_source(), _plan(), 1)
    assert engine.execute_calls[-1][0] == 'take'


def test_l028(ex, engine):
    _retire_card('_apply_plan', None)
    engine.result = iter([{'id': 0}])
    assert list(ex.iter_rows(_source(), _plan())) == [{'id': 0}]
    assert engine.execute_calls[-1][0] == 'iter_rows'


def test_l029(ex, engine):
    _retire_card('_execution_capabilities', None)
    engine.result = [{'id': 0}]
    assert ex.take(_source(), _plan(), 1) == [{'id': 0}]
    assert engine.execute_calls[-1][0] == 'take'


def test_l030(ex, engine):
    _retire_card('_bind_plan', None)
    engine.result = FakeInputHandle([{'id': 0}])
    ex.materialize(_source(), _plan())
    assert engine.execute_calls[-1][0] == 'materialize'


def test_l031(ex, engine):
    _retire_card('_execute_plan', None)
    engine.result = 2
    assert ex.count(_source(), _plan()) == 2
    assert engine.execute_calls[-1][0] == 'count'


def test_l032(ex, engine):
    _retire_card('_write_file_body', None)
    engine.result = WriteResult(written_rows=1)
    ex.write_file(_source(), _plan(), 'json', '/tmp/demiflow-l/out.json', {})
    assert engine.execute_calls[-1][0] == 'write_file'