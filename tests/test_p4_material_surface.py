"""Material projections preserve real compile facts and independent ownership."""
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread
import hashlib
import json

import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan
from demiflow.data.declaration_inspection import dataset_declaration_links
from demiflow.execution.contracts.dataflow_codec import encode_wire
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.managed_native import capture_pipeline
from demiflow.execution.journal.program_materials import compiled_node_materials
from demiflow.workbench.graph.plan_details import build_plan_details
from test_plan_capture import no_execution


def passive(row):
    raise AssertionError('material must not execute the UDF')


def test_sdk_links_preserve_identity_and_refuse_subclasses_or_overbudget():
    ds = data.range(3).map(passive)
    source, operations = dataset_declaration_links(ds)
    assert source is ds._source and operations is ds._plan.operations
    assert dataset_declaration_links(ds, max_operations=0) is None
    opaque = object()
    for count in (0, 1024, 1025):
        bounded = Dataset(source, LogicalPlan((opaque,) * count), ds._executor)
        value = dataset_declaration_links(bounded)
        assert (value is None) == (count > 1024)
        if value is not None:
            assert value[1] is bounded._plan.operations
    class Hostile(Dataset):
        def __getattribute__(self, name):
            raise AssertionError('subclass inspected')
    assert dataset_declaration_links(object.__new__(Hostile)) is None
    assert dataset_declaration_links(object.__new__(Dataset)) is None
    for limit in (True, -1, 1025):
        with pytest.raises(ValueError):
            dataset_declaration_links(ds, max_operations=limit)


def test_all_schema_nodes_share_one_native_pass_and_preserve_wire(no_execution, monkeypatch):
    from demiflow.execution.compiler_support import dataflow_schema
    captured = capture_pipeline(lambda: data.from_items([{'x': 1}]).select_columns(['x']).take_all(), {})
    compiled, bounds = captured.compiled, captured.bounds
    before = encode_wire(compiled.graph, type_name='DataflowGraph', bounds=bounds)
    original, calls = dataflow_schema.node_schemas, []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(dataflow_schema, 'node_schemas', counted)
    result = compiled_node_materials(compiled, bounds=bounds)
    assert len(calls) == 1 and set(result) == {n.occurrence_id for n in compiled.graph.nodes}
    for key, item in result.items():
        expected = original(compiled.graph, bounds=bounds)[key]
        assert dataflow_schema.terminal_schema(compiled.graph, None, terminal=key, bounds=bounds) == expected
        assert item.schema.output['status'] == ('unknown' if expected is None else 'declared')
    assert encode_wire(compiled.graph, type_name='DataflowGraph', bounds=bounds) == before


def test_schema_budget_is_unavailable_not_unknown_and_corruption_stays_error(no_execution, monkeypatch):
    from demiflow.execution.compiler_support import dataflow_schema
    captured = capture_pipeline(lambda: data.range(3).map(passive).take_all(), {})
    result = compiled_node_materials(captured.compiled, bounds=replace(captured.bounds, max_nodes=1))
    assert all(item.schema.availability == 'unavailable' and item.schema.output is None for item in result.values())
    assert all(item.configuration.availability == 'available' for item in result.values())
    result = compiled_node_materials(captured.compiled, bounds=captured.bounds)
    udf = next(item for item in result.values() if item.implementation.function is passive)
    assert udf.schema.output['status'] == 'unknown'
    assert udf.implementation.kind == 'function'
    def corrupt(*args, **kwargs):
        raise DataflowError('CORRUPT_ARTIFACT', 'bad source schema')
    monkeypatch.setattr(dataflow_schema, 'node_schemas', corrupt)
    with pytest.raises(DataflowError) as caught:
        compiled_node_materials(captured.compiled, bounds=captured.bounds)
    assert caught.value.code == 'CORRUPT_ARTIFACT'


def test_passive_function_source_and_opaque_configuration_remain_renderable(no_execution):
    captured = capture_pipeline(lambda: data.range(3).map(passive).take_all(), {})
    source = Path(__file__).resolve()
    details = build_plan_details(captured.compiled, root=source.parent,
        sources={source.name: hashlib.sha256(source.read_bytes()).hexdigest()})
    impl = next(v['implementation'] for v in details.values() if v['implementation']['availability'] == 'available')
    assert 'def passive' in impl['text'] and impl['referenced']
    assert all(v['schema']['availability'] == 'unavailable' for v in details.values())
    for item in compiled_node_materials(captured.compiled, bounds=captured.bounds).values():
        json.dumps(item.configuration.engine_configuration, allow_nan=False)
        assert not hasattr(item, 'kernel') and not hasattr(item, 'declaration')


def test_inventory_public_validation_is_pure_and_same_native_implementation(tmp_path, monkeypatch):
    from demiflow.resources import device_pool
    from demiflow.resources.contracts import ResourceError
    monkeypatch.setattr(device_pool.DevicePool, '__init__', lambda *a, **k: pytest.fail('pool allocated'))
    assert device_pool.validate_inventory is device_pool._validate_inventory
    value = {'devices': [{'device_id': 'gpu0', 'type': 'gpu', 'memory_bytes': 4}]}
    actual = device_pool.validate_inventory(value)
    assert actual == value['devices'] and actual is not value['devices'] and actual[0] is not value['devices'][0]
    assert 'max_residents' not in actual[0]
    for invalid in ({'devices': []}, {'devices': value['devices'] * 2},
                    {'devices': [{**actual[0], 'memory_bytes': True}]},
                    {'devices': [{**actual[0], 'extra': 1}]}):
        with pytest.raises(ResourceError):
            device_pool.validate_inventory(invalid)
    assert not list(tmp_path.iterdir())


def test_borrowed_native_slot_is_drained_before_session_close(tmp_path, monkeypatch):
    from demiflow.execution.hosts import datafusion as native
    from test_native_resources import session
    monkeypatch.setattr(native, '_cpu_capacity', lambda: 4)
    monkeypatch.setattr(native, '_cgroup_memory', lambda: (0, 32 * 2**30))
    borrowed, release, closed = Event(), Event(), Event()
    instance = session(tmp_path, 2 * 2**30)
    instance.__enter__()
    failures = []
    def work():
        try:
            with instance.borrow_resources() as options:
                assert options is instance.options
                borrowed.set()
                assert release.wait(5)
                raise ValueError('caller failed')
        except ValueError:
            pass
        except BaseException as exc:
            failures.append(exc)
    def close():
        instance.__exit__(None, None, None)
        closed.set()
    worker = Thread(target=work)
    worker.start()
    assert borrowed.wait(5)
    closer = Thread(target=close)
    closer.start()
    try:
        assert not closed.wait(.05)
    finally:
        release.set()
        worker.join(5)
        closer.join(5)
    assert closed.is_set() and not failures and not list(tmp_path.glob('slot-*.json'))
    with pytest.raises(RuntimeError):
        with instance.borrow_resources():
            pytest.fail('closed session borrowed')


def test_configuration_projection_limits_and_redaction_do_not_become_schema(no_execution):
    captured = capture_pipeline(lambda: data.range(3).take_all(), {})
    original = captured.compiled
    source = original.graph.nodes[0]
    def with_configuration(configuration):
        changed = replace(source, kernel=replace(source.kernel, configuration=configuration))
        return replace(original, graph=replace(original.graph, nodes=(changed, *original.graph.nodes[1:])))
    compiled = with_configuration({'query': {'fields': {'columns': ['id'], 'token': 'NOT_FOR_DISPLAY'}},
                                   'endpoint': 'https://user:NOT_FOR_DISPLAY@host/path?token=NOT_FOR_DISPLAY',
                                   'password': 'NOT_FOR_DISPLAY'})
    projected = compiled_node_materials(compiled, bounds=captured.bounds)[source.occurrence_id]
    assert projected.schema.output['status'] == 'fields_only'
    assert projected.schema.read_fields == ('id',)
    details = build_plan_details(compiled, root='/missing', sources={}, bounds=captured.bounds)
    assert 'NOT_FOR_DISPLAY' not in json.dumps(details)
    oversized = with_configuration({'text': 'x' * (128 * 1024 + 1)})
    result = compiled_node_materials(oversized, bounds=captured.bounds)
    assert result[source.occurrence_id].configuration.availability == 'unavailable'
    assert result[source.occurrence_id].schema.availability == 'available'
    assert oversized.graph.nodes[0].kernel.configuration['text'] == 'x' * (128 * 1024 + 1)


def test_schema_redaction_failure_only_disables_that_material(no_execution):
    from demiflow.workbench.graph.plan_schema import build_schema_details
    captured = capture_pipeline(lambda: data.range(3).take_all(), {})
    projected = compiled_node_materials(captured.compiled, bounds=captured.bounds)
    key = captured.compiled.graph.nodes[0].occurrence_id
    item = projected[key]
    projected[key] = replace(item, schema=replace(item.schema, bindings={'input': 'http://['}))
    details = build_schema_details(projected)
    assert details[key]['availability'] == 'unavailable'
    assert all(value['availability'] == 'available' for node, value in details.items() if node != key)
    assert len(captured.compiled.graph.nodes) == 2


def test_index_missing_run_directory_and_missing_native_storage_remain_absent(tmp_path):
    from demiflow.workbench.runs import manager as runs
    run_id = 'r' + '1' * 24
    assert runs.run_index_entry(tmp_path, run_id) is None
    target = tmp_path / '.forge/runs' / run_id
    target.mkdir(parents=True)
    assert runs.run_index_entry(tmp_path, run_id) is None
    assert not list(target.iterdir())
