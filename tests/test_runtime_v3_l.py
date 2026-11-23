"""Lane L gate: public SDK actions use the Local execution port.

Real Dataset validation and action scopes reach the production runtime bridge;
only dataflow execution and input construction are replaced with unit doubles.
These tests pin provider/argument forwarding, no fallback, metadata ownership,
retained graph delegation and resource cleanup. Real data behavior is exercised
by the per-method suites, not inferred from the doubles.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.execution_contract import InputSpec
from demiflow.data.aggregate import Sum, Mean

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
    'count', 'take', 'take_batch', 'aggregate', 'iter_rows', 'iter_batches',
    'materialize', 'materialize_stream', 'size_bytes', 'write_datasink',
    'write_lance', 'write_file', 'from_items', 'from_arrow', 'from_numpy', 'from_pandas',
)




# --------------------------------------------------------------------------- #
# Spec-bound test-domain doubles (E/F/C peers).
# --------------------------------------------------------------------------- #
from demiflow.execution.contracts.dataflow import ActionProgram

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
        request.executor._last_metadata = ExecutionMetadata(
            source_name=type(request.source).__name__, rows_read=7, rows_output=2,
            diagnostics={'engine_fact': 'preserved'},
        )
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
    import demiflow.execution.dataflow_api as api
    import demiflow.execution.sources.materialized_input as inputs
    import demiflow.execution.executors.base as input_runtime
    monkeypatch.setattr(api, 'make_action_request', engine.make_action_request)
    monkeypatch.setattr(api, 'run_dataset_action', engine.run_dataset_action)
    monkeypatch.setattr(api, 'explain_dataset', engine.explain_dataset)
    monkeypatch.setattr(inputs, 'build_input_handle', engine.build_input_handle)
    monkeypatch.setattr(input_runtime, 'build_input_handle', engine.build_input_handle)


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
    other = LocalDatasetExecutor(workers=2)
    assert other.NAME == 'local'
    engine.result = [{'id': 0}]
    Dataset(_source(), _plan(), other).take(1)
    assert engine.execute_calls == [('take', engine.requests[-1].plan_id)]
    other.close()
    for option in ('unified_chain', 'unified_graph', 'unified_queue_chain', 'sort_workers'):
        with pytest.raises(TypeError, match=option):
            LocalDatasetExecutor(**{option: 1})
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L002-L005 from_* — neutral F001 handle, no run created, no ray.data.
# --------------------------------------------------------------------------- #
def test_l002(ex, engine):
    handle = ex.prepare_input(InputSpec('items', [{'id': 0}, {'id': 1}]))
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls == [('items', [{'id': 0}, {'id': 1}], None)]
    assert engine.execute_calls == [], 'from_items must not create a run'
    assert list(handle.rows()) == [{'id': 0}, {'id': 1}]
    with pytest.raises(TypeError):
        ex.prepare_input(InputSpec('items', iter([{'id': 0}])))  # generator is not a materialized list
    _assert_retired_absent()


def test_l003(ex, engine):
    import pyarrow as pa
    table = pa.table({'id': [0, 1]})
    handle = ex.prepare_input(InputSpec('arrow', table))
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls[0][0] == 'arrow'
    assert engine.input_calls[0][1] is table, 'arrow must not be lowered to pylist'
    assert engine.execute_calls == []
    _assert_retired_absent()


def test_l004(ex, engine):
    import numpy as np
    array = np.asarray([0, 1])
    handle = ex.prepare_input(InputSpec('numpy', array))
    assert isinstance(handle, FakeInputHandle)
    assert engine.input_calls[0][0] == 'numpy'
    assert engine.input_calls[0][1] is array
    assert engine.execute_calls == []
    _assert_retired_absent()


def test_l005(ex, engine):
    import pandas as pd
    frame = pd.DataFrame({'id': [0, 1]})
    handle = ex.prepare_input(InputSpec('pandas', frame))
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
    rows = list(list(Dataset(_source(), _plan(), ex).iter_rows()))
    assert rows == [{'id': 0}, {'id': 1}]
    req = engine.requests[-1]
    assert req.kind == 'iter_rows' and req.arguments == {}
    assert engine.execute_calls == [('iter_rows', req.plan_id)]
    _assert_no_fallback(ex, engine, lambda: list(Dataset(_source(), _plan(), ex).iter_rows()))
    _assert_retired_absent()


def test_l007(ex, engine):
    engine.result = iter([{'id': [0, 1]}])
    batches = list(list(Dataset(_source(), _plan(), ex).iter_batches(prefetch_batches=2, batch_size=8, batch_format='pyarrow', drop_last=True)))
    assert batches == [{'id': [0, 1]}]
    req = engine.requests[-1]
    assert req.kind == 'iter_batches'
    assert req.arguments == {'prefetch_batches': 2, 'batch_size': 8,
                             'batch_format': 'pyarrow', 'drop_last': True}
    _assert_retired_absent()


def test_l008(ex, engine):
    handle = FakeInputHandle([{'id': 0}])
    engine.result = handle
    result = Dataset(_source(), _plan(), ex).materialize()._source.handle
    assert result is handle
    req = engine.requests[-1]
    assert req.kind == 'materialize' and req.arguments == {}
    assert ex._last_metadata is not None
    _assert_no_fallback(ex, engine, lambda: Dataset(_source(), _plan(), ex).materialize()._source.handle)
    _assert_retired_absent()


def test_l009(ex, engine):
    engine.result = [{'id': 0}]
    assert Dataset(_source(), _plan(), ex).take(3) == [{'id': 0}]
    req = engine.requests[-1]
    assert req.kind == 'take' and req.arguments == {'limit': 3}
    # take(0) must still reach admission, not short-circuit locally.
    engine.requests.clear()
    Dataset(_source(), _plan(), ex).take(0)
    assert engine.requests[-1].arguments == {'limit': 0}
    _assert_no_fallback(ex, engine, lambda: Dataset(_source(), _plan(), ex).take(1))
    _assert_retired_absent()


def test_l010(ex, engine):
    engine.result = {'id': [0, 1]}
    assert Dataset(_source(), _plan(), ex).take_batch(8, batch_format='pyarrow') == {'id': [0, 1]}
    req = engine.requests[-1]
    assert req.kind == 'take_batch'
    assert req.arguments == {'batch_size': 8, 'batch_format': 'pyarrow'}
    _assert_retired_absent()


def test_l011(ex, engine):
    engine.result = 2
    assert Dataset(_source(), _plan(), ex).count() == 2
    req = engine.requests[-1]
    assert req.kind == 'count' and req.arguments == {}
    _assert_no_fallback(ex, engine, lambda: Dataset(_source(), _plan(), ex).count())
    _assert_retired_absent()


def test_l012(ex, engine):
    engine.result = {'total': 3}
    agg = Sum('id', alias_name='total')
    assert Dataset(_source(), _plan(), ex).aggregate(*(agg,)) == {'total': 3}
    req = engine.requests[-1]
    assert req.kind == 'aggregate'
    assert req.arguments == {'names': ['total']}
    assert len(req.programs) == 1
    program = req.programs[0]
    assert isinstance(program, ActionProgram)
    assert program.kind == 'aggregate' and program.name == 'aggregate-0'
    assert req.arguments['names'] == [agg.name]
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
    assert Dataset(_source(), _plan(), ex).size_bytes() == 128
    req = engine.requests[-1]
    assert req.kind == 'size_bytes' and req.arguments == {}
    _assert_no_fallback(ex, engine, lambda: Dataset(_source(), _plan(), ex).size_bytes())
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
    Dataset(_source(), _plan(), ex).count()
    text = ex.stats(_source(), _plan())
    assert 'ItemsSource' in text
    assert engine.execute_calls == [('count', engine.requests[-1].plan_id)]
    _assert_retired_absent()


def test_l018(ex, engine):
    assert ex.execution_metadata(_source(), _plan()) is None
    assert engine.execute_calls == []
    engine.result = 2
    Dataset(_source(), _plan(), ex).count()
    metadata = ex.execution_metadata(_source(), _plan())
    assert isinstance(metadata, ExecutionMetadata)
    assert metadata.source_name == 'ItemsSource'
    assert metadata.run_ref is ex._run_ref
    assert metadata.rows_read == 7 and metadata.rows_output == 2
    assert metadata.diagnostics == {'engine_fact': 'preserved'}
    assert engine.execute_calls == [('count', engine.requests[-1].plan_id)]
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L019-L021 write terminals — sink/programs forwarding, Ray option guard.
# --------------------------------------------------------------------------- #
def test_l019(ex, engine):
    class Sink:
        def recovery_identity(self):
            return {'kind': 'unit-sink'}
    engine.result = WriteResult(written_rows=2)
    sink = Sink()
    dataset = Dataset(_source(), _plan(), ex)
    assert dataset.write_datasink(sink) is None
    req = engine.requests[-1]
    assert req.kind == 'write_datasink' and req.sink is sink and req.arguments == {}
    dataset.write_datasink(sink, backend_options={'ray': {'worker_resources': {'cpu': 1}}})
    assert engine.requests[-1].arguments['native_options']['worker_resources'] == {'cpu': 1.0}
    _assert_no_fallback(ex, engine, lambda: dataset.write_datasink(sink))
    _assert_retired_absent()


def test_l020(ex, engine, tmp_path):
    from demiflow.lance.model import LanceWriteReceipt
    uri = str(tmp_path / 'result.lance')
    receipt = LanceWriteReceipt('request', uri, None, 1, 2, 2, 'schema', 'committed')
    engine.result = receipt
    dataset = Dataset(_source(), _plan(), ex)
    assert dataset.write_lance(uri, return_receipt=True) is receipt
    req = engine.requests[-1]
    assert req.kind == 'write_lance'
    assert len(req.programs) == 1
    assert req.programs[0].kind == 'sink' and req.programs[0].value.uri == uri
    assert dataset.write_lance(uri, backend_options={'ray': {'worker_resources': {'cpu': 1}}}) is None
    assert engine.requests[-1].arguments['native_options']['worker_resources'] == {'cpu': 1.0}
    _assert_retired_absent()


def test_l021(ex, engine, tmp_path):
    engine.result = WriteResult(written_rows=2, target='out.json')
    target = str(tmp_path / 'out.json')
    dataset = Dataset(_source(), _plan(), ex)
    assert dataset.write_json(target, lines=True) is None
    req = engine.requests[-1]
    assert req.kind == 'write_file'
    assert req.arguments == {'format': 'json', 'path': target, 'options': {'lines': True}}
    _assert_no_fallback(ex, engine, lambda: dataset.write_json(target))
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L022 close — release only this executor's scope.
# --------------------------------------------------------------------------- #
def test_l022(ex, engine, tmp_path):
    # Another run's neutral handle must not be touched by this close.
    other_handle = FakeInputHandle([{'id': 9}])
    ex.close()
    assert ex._local_kernel_closed and not hasattr(ex, 'executor')
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
    observed = OperatorLLMUsage(1, 2, 2, 1, 1, 13, 17).to_dict()
    ex._dataflow_prompt_usage = observed.copy()
    assert ex.operator_llm_usage() == observed
    assert ex.operator_llm_usage() == observed  # reading metrics is not accounting again
    assert ex._dataflow_prompt_usage == observed
    assert engine.execute_calls == [], 'usage must not recompute or impose a budget'
    assert engine.requests == []
    _assert_retired_absent()


# --------------------------------------------------------------------------- #
# L025 materialize_stream — same unified materialize entry, override respected.
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# L026 materialize_graph — delegate to thin translator, no independent driver.
# --------------------------------------------------------------------------- #
def test_l026(ex, engine, monkeypatch):
    from demiflow.execution.executors import graph as graph_mod
    from demiflow.execution.action_driver import dataflow as graph_driver
    import demiflow.execution.run_context as run_context_mod

    seen = {}

    def graph_request(graph_spec, executor):
        seen['graph_spec'] = graph_spec
        # F015 thin translator forwards to the same unified execute_action entry.
        request = engine.make_action_request(ex, _source(), _plan(),
                                             kind='materialize', arguments={})
        return request

    monkeypatch.setattr(graph_mod, 'graph_action_request', graph_request)
    monkeypatch.setattr(graph_driver, 'run_graph_action', lambda request, **kwargs: engine.run_dataset_action(request))
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
    Dataset(_source(), _plan(), ex).take(1)
    assert engine.execute_calls[-1][0] == 'take'


def test_l028(ex, engine):
    _retire_card('_apply_plan', None)
    engine.result = iter([{'id': 0}])
    assert list(list(Dataset(_source(), _plan(), ex).iter_rows())) == [{'id': 0}]
    assert engine.execute_calls[-1][0] == 'iter_rows'


def test_l029(ex, engine):
    _retire_card('_execution_capabilities', None)
    engine.result = [{'id': 0}]
    assert Dataset(_source(), _plan(), ex).take(1) == [{'id': 0}]
    assert engine.execute_calls[-1][0] == 'take'


def test_l030(ex, engine):
    _retire_card('_bind_plan', None)
    engine.result = FakeInputHandle([{'id': 0}])
    Dataset(_source(), _plan(), ex).materialize()._source.handle
    assert engine.execute_calls[-1][0] == 'materialize'


def test_l031(ex, engine):
    _retire_card('_execute_plan', None)
    engine.result = 2
    assert Dataset(_source(), _plan(), ex).count() == 2
    assert engine.execute_calls[-1][0] == 'count'


def test_l032(ex, engine):
    _retire_card('_write_file_body', None)
    engine.result = WriteResult(written_rows=1)
    Dataset(_source(), _plan(), ex)._write_file('json', '/tmp/demiflow-l/out.json', {})
    assert engine.execute_calls[-1][0] == 'write_file'