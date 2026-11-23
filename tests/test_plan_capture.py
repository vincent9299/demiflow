"""Compile actual Dataset actions without granting execution or faking results."""
import json

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.plan_capture import capture_pipeline, current_capture, build_logical_plan_view


def forbidden(*args, **kwargs):
    raise AssertionError('execution escaped compile-only capture')


@pytest.fixture
def no_execution(monkeypatch):
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution.pipeline.run_context import RunContext
    from demiflow.observability import ActionObserver
    monkeypatch.setattr(dataflow, 'make_driver_services', forbidden)
    monkeypatch.setattr(dataflow, 'snapshot_source', forbidden)
    monkeypatch.setattr(dataflow.DataflowExecution, 'start', forbidden)
    monkeypatch.setattr(RunContext, '__enter__', forbidden)
    monkeypatch.setattr(ActionObserver, 'start', forbidden)


def kinds(captured):
    return [node.kernel.kind for node in captured.compiled.graph.nodes]


def test_helper_chain_and_parameter_branch_use_real_dataset_graph(no_execution):
    def transform(ds):
        return ds.map(forbidden).filter(forbidden)

    def entry(*, choice):
        rows = transform(data.range(5))
        if choice == 'sorted':
            rows = rows.sort('id')
        return rows.take_all()

    plain = capture_pipeline(entry, {'choice': 'plain'})
    sorted_plan = capture_pipeline(entry, {'choice': 'sorted'})
    assert kinds(plain) == ['range', 'MapOp', 'FilterOp', 'take_all']
    assert kinds(sorted_plan) == ['range', 'MapOp', 'FilterOp', 'SortOp', 'take_all']
    assert plain.compiled.graph.dag_id != sorted_plan.compiled.graph.dag_id
    assert not plain.entry_completed
    assert current_capture() is None


def test_shared_union_and_join_preserve_all_ports(no_execution):
    def entry():
        rows = data.range(4).map(forbidden)
        other = data.from_items([{'id': 1}])
        return rows.union(rows.filter(forbidden)).join(other, on='id').take_all()

    captured = capture_pipeline(entry, {})
    graph = captured.compiled.graph
    assert kinds(captured).count('MapOp') == 1
    assert {'union', 'join', 'take_all'} <= set(kinds(captured))
    view = build_logical_plan_view(graph)
    json.dumps(view, allow_nan=False)
    assert view['dag_id'] == graph.dag_id
    assert {n['occurrence_id'] for n in view['nodes']} == {n.occurrence_id for n in graph.nodes}
    assert {e['edge_id'] for e in view['edges']} == {e.edge_id for e in graph.edges}
    join = next(n for n in view['nodes'] if n['op_kind'] == 'join')
    assert set(join['input_ports']) == {'left', 'right'}
    assert 'physical' not in view and 'plan_id' not in view and 'status' not in view
    assert view['sources'] == list(graph.sources) and view['terminals'] == list(graph.terminals)


@pytest.mark.parametrize('source', ['iterator', 'lance', 'datasource'])
def test_lazy_inputs_are_never_acquired(no_execution, tmp_path, source):
    from demiflow.data.datasource import Datasource

    class Source(Datasource):
        def snapshot_identity(self):
            return {'kind': 'declared-only'}
        get_read_tasks = forbidden

    def entry():
        if source == 'iterator':
            ds = data.from_iter(forbidden)
        elif source == 'lance':
            ds = data.read_lance(str(tmp_path / 'does-not-exist.lance'), version=1)
        else:
            ds = data.read_datasource(Source())
        return ds.map(forbidden).take_all()

    captured = capture_pipeline(entry, {})
    assert captured.kind == 'take_all'
    assert not list(tmp_path.iterdir())


def test_source_labels_use_frozen_table_name_and_version_without_io(no_execution):
    captured = capture_pipeline(lambda: data.read_lance(
        's3://bucket/concepts.lance', version=7).take_all(), {})
    graph = captured.compiled.graph
    view = build_logical_plan_view(graph)
    source = next(node for node in view['nodes'] if node['op_kind'] == 'lance')
    assert source['label'] == '读取 concepts.lance'
    assert source['data_source'] == {'name': 'concepts.lance', 'version': 7}
    assert 's3://' not in json.dumps(view)
    assert view['dag_id'] == graph.dag_id
    assert {n['occurrence_id'] for n in view['nodes']} == {n.occurrence_id for n in graph.nodes}


def test_actor_factory_is_not_constructed(no_execution):
    class Actor:
        __init__ = forbidden
        __call__ = forbidden

    captured = capture_pipeline(lambda: data.range(3).map(Actor).take_all(), {})
    assert 'MapOp' in kinds(captured)


def test_datasink_lifecycle_is_not_called(no_execution):
    from demiflow.data.datasink import Datasink

    class Sink(Datasink):
        def recovery_identity(self):
            return {'sink': 'declared-only'}
        on_write_start = on_write_failed = on_write_complete = forbidden
        prepare = commit = abort = inspect = write = forbidden

    captured = capture_pipeline(lambda: data.range(2).write_datasink(Sink()), {})
    assert captured.kind == 'write_datasink'
    assert kinds(captured)[-1] == 'write_datasink'


def test_stream_callbacks_and_cancellation_are_not_entered(no_execution):
    class Cancellation:
        @property
        def requested(self):
            forbidden()

    async def transform(row):
        forbidden()

    captured = capture_pipeline(lambda: data.range(2).map_async(transform).run_stream(
        on_progress=forbidden, on_drain=forbidden, cancellation=Cancellation()), {})
    assert captured.kind == 'run_stream'


def test_prompt_configuration_is_compiled_without_model_lifecycle(no_execution, monkeypatch):
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.runtime import PromptActor, AsyncOperatorLLMRuntime
    from test_map_prompt_async import PACK
    # The public API creates a passive declaration object. Its execution
    # lifecycle and transport acquisition must never run during capture.
    for name in ('astart', 'aclose', '__call__'):
        monkeypatch.setattr(PromptActor, name, forbidden)
    from demiflow.execution.operator_bindings import dataflow_model_journal
    monkeypatch.setattr(dataflow_model_journal, 'bind_prompt_adapter', forbidden)
    monkeypatch.setattr(AsyncOperatorLLMRuntime, 'call_with_trace', forbidden)
    monkeypatch.setattr(AsyncOperatorLLMRuntime, 'transport_context', forbidden)
    pack = parse_prompt_pack(PACK)

    def entry():
        return data.from_items([{'item': 'declared'}]).map_prompt_async(
            'enrich', config=pack, inputs={'payload': 'item'}, output='answer',
            max_requests=1, concurrency=1).take_all()

    captured = capture_pipeline(entry, {})
    assert 'prompt' in kinds(captured)
    prompt = next(n for n in captured.compiled.graph.nodes if n.kernel.kind == 'prompt')
    assert prompt.kernel.effect == 'journaled'
    view = build_logical_plan_view(captured.compiled.graph, declarations=captured.compiled.declarations)
    assert next(n for n in view['nodes'] if n['op_kind'] == 'prompt')['label'] == 'enrich'


def test_both_providers_use_identical_logical_compiler_without_ray_initialization(no_execution):
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor

    def entry(executor):
        return DataAPI(executor).range(3).map(forbidden).sort('id').take_all()

    local = capture_pipeline(entry, {'executor': LocalDatasetExecutor(workers=2)})
    ray = capture_pipeline(entry, {'executor': RayDatasetExecutor(workers=2)})
    assert local.provider == 'local' and ray.provider == 'ray_core'
    assert build_logical_plan_view(local.compiled.graph) == build_logical_plan_view(ray.compiled.graph)


def test_program_labels_read_only_frozen_compiler_facts(no_execution):
    def named_transform(row):
        forbidden()

    captured = capture_pipeline(lambda: data.range(2).map(named_transform).take_all(), {})
    view = build_logical_plan_view(captured.compiled.graph, declarations=captured.compiled.declarations)
    transform = next(n for n in view['nodes'] if n['op_kind'] == 'MapOp')
    assert transform['label'].endswith('named_transform')
    assert transform['program']['module'] == __name__
    assert transform['program']['qualname'] == named_transform.__qualname__


def test_declared_business_labels_are_compiler_facts_not_topology(no_execution):
    captured = capture_pipeline(lambda: data.range(2).map(forbidden, label='筛选参考候选').take_all(), {})
    before = build_logical_plan_view(captured.compiled.graph, declarations=captured.compiled.declarations)
    after = build_logical_plan_view(captured.compiled.graph, declarations=captured.compiled.declarations,
                                   observation_labels=captured.compiled.observation_labels)
    transform = next(n for n in after['nodes'] if n['op_kind'] == 'MapOp')
    assert transform['label'] == '筛选参考候选'
    assert before['dag_id'] == after['dag_id'] and before['edges'] == after['edges']
    assert [n['occurrence_id'] for n in before['nodes']] == [n['occurrence_id'] for n in after['nodes']]
    with pytest.raises(ValueError, match='compiled graph'):
        build_logical_plan_view(captured.compiled.graph, observation_labels={'invented': '假节点'})


def test_lance_write_keeps_original_program_without_publication(no_execution, tmp_path):
    import pyarrow as pa
    from demiflow.execution.executors.dataset_runtime import bind_lance_action
    target = tmp_path / 'untouched.lance'

    def entry():
        with bind_lance_action(forbidden, max_rows_per_batch=8):
            return data.range(2).write_lance(str(target), mode='create',
                schema=pa.schema([('id', pa.int64())]), return_receipt=True)

    captured = capture_pipeline(entry, {})
    programs = [d.action_program for d in captured.compiled.declarations if d.action_program]
    assert captured.kind == 'write_lance'
    assert len(programs) == 1 and programs[0].value.uri == str(target)
    assert captured.policy.native_limits['batch_rows'] == 8
    assert not target.exists()


def test_local_configuration_resolves_native_limits_without_run(no_execution, tmp_path):
    def entry():
        with data.local_execution(workers=2, worker_mode='thread', memory_bytes=64 * 1024**2,
                                  native_resources={'memory_bytes': 32 * 1024**2, 'batch_rows': 7},
                                  temp_directory=str(tmp_path)):
            return data.range(4).take_all()

    captured = capture_pipeline(entry, {})
    assert captured.policy.max_active_workers == 2
    assert captured.policy.native_limits['batch_rows'] == 7
    assert captured.bounds.max_resident_bytes <= 32 * 1024**2
    assert not list(tmp_path.iterdir())


def test_stops_at_first_result_dependent_action(no_execution):
    reached = []

    def entry():
        result = data.range(3).take_all()
        reached.append(result)
        return data.from_items(result).count()

    captured = capture_pipeline(entry, {})
    assert captured.kind == 'take_all'
    assert reached == []
    assert not captured.entry_completed


def test_prerequisite_failure_and_no_action_are_not_success(no_execution):
    def entry():
        data.require(False, 'real prerequisite')
        return data.range(1).count()

    with pytest.raises(ValueError, match='real prerequisite'):
        capture_pipeline(entry, {})
    with pytest.raises(DataflowError, match='did not reach'):
        capture_pipeline(lambda: data.range(1).map(forbidden), {})
    assert current_capture() is None


def test_lance_location_alias_uses_authoritative_manifest_without_cache_writes(no_execution, tmp_path, monkeypatch):
    from demiflow.execution.storage.location_index import IndexedLocations
    root = tmp_path / 'storage'
    (root / '_demiflow').mkdir(parents=True)
    manifest = root / '_demiflow' / 'lance_locations.json'
    manifest.write_text(json.dumps({'version': 1, 'tables': {'logical.lance': 'physical/table.lance'}}))
    before = manifest.read_bytes()
    monkeypatch.setattr(IndexedLocations, '_ensure', forbidden)
    cache = tmp_path / 'absent-cache'
    monkeypatch.setenv('DEMIFLOW_LOCATION_CACHE', str(cache))
    captured = capture_pipeline(lambda: data.read_lance(str(root / 'logical.lance'), version=2).take_all(), {})
    source = next(d.source_spec for d in captured.compiled.declarations if d.source_spec is not None)
    assert source.query.uri == str(root / 'physical/table.lance')
    assert source.query.version == 2
    assert manifest.read_bytes() == before
    assert not cache.exists()


def test_readonly_location_lookup_keeps_duplicate_and_whole_document_validation(tmp_path):
    from demiflow.execution.storage.location_index import IndexedLocations, identity
    path = tmp_path / 'locations.json'
    path.write_text('{"version":1,"tables":{"x.lance":"old.lance"},'
                    '"tables":{"x.lance":"middle.lance","x.lance":"last.lance"}}')
    mapping = IndexedLocations(str(path), identity(path.stat()), field='tables')
    assert mapping.get_readonly('x.lance') == 'last.lance'
    assert mapping.get_readonly('absent.lance') is None
    path.write_text('{"version":1,"tables":{"x.lance":"found.lance"}} trailing')
    invalid = IndexedLocations(str(path), identity(path.stat()), field='tables')
    with pytest.raises(ValueError, match='Trailing'):
        invalid.get_readonly('x.lance')


def test_swallowed_boundary_and_nested_capture_fail_closed(no_execution):
    def swallowed():
        try:
            data.range(1).count()
        except BaseException:
            pass

    with pytest.raises(DataflowError, match='swallowed'):
        capture_pipeline(swallowed, {})
    with pytest.raises(DataflowError, match='cannot nest'):
        capture_pipeline(lambda: capture_pipeline(lambda: data.range(1).count(), {}), {})
    assert current_capture() is None


def test_explicit_run_start_is_rejected_before_creation(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    target = tmp_path / 'run'

    def entry():
        with RunContext.start(target):
            data.range(1).count()

    with pytest.raises(DataflowError, match='explicit run'):
        capture_pipeline(entry, {})
    assert not target.exists()


def test_capture_does_not_leak_into_ordinary_execution(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    captured = capture_pipeline(lambda: data.range(3).take_all(), {})
    with RunContext.start(tmp_path / 'run'):
        assert data.range(3).take_all() == [{'id': 0}, {'id': 1}, {'id': 2}]
    assert captured.kind == 'take_all'
    assert current_capture() is None
