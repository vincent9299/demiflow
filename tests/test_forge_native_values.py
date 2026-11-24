"""Native query facts and guards use actual Dataset actions, not an IR runner."""
import importlib.util
from pathlib import Path
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from test_forge_native_semantics import native


@pytest.fixture(scope='module')
def catalog():
    result = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    assert result['inspection_complete'], result['diagnostics']
    return result


def fixture(tmp_path, catalog, *, minimum=1, body=None):
    source, target = str(tmp_path / 'input.lance'), str(tmp_path / 'output.lance')
    table = pa.Table.from_pylist([{'x': 1}, {'x': 2}])
    lance.write_dataset(table, source)
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'operators').mkdir()
    path = tmp_path / 'flows' / 'values.py'
    path.write_text('from demiflow import data\ndef run(*, source, target, minimum):\n'
                    '    rows = data.read_lance(source, version=1)\n' + (body or
                    '    total = rows.count()\n'
                    '    data.require(total >= minimum, "insufficient rows")\n'
                    '    return rows.write_lance(target, mode="create", return_receipt=True)\n'))
    arguments = dict(source=source, target=target, minimum=minimum)
    report = FlowChecker(tmp_path, catalog, {'input': {'uri': source, 'version': 1, 'fields': arrow_fields(table.schema)}}, [target]).inspect('flows/values.py:run', arguments)
    return path, report, arguments


@pytest.mark.parametrize('minimum', [1, 3])
def test_query_guard_checks_actual_values_before_commit(tmp_path, catalog, minimum):
    path, report, arguments = fixture(tmp_path, catalog, minimum=minimum)
    assert report['status'] == 'checked', report['diagnostics']
    assert len(report['value_edges']) == 1 and len(report['action_edges']) == 2
    spec = importlib.util.spec_from_file_location('flows.values', path)
    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)
    events = []
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *args: events.append(args), check_stop=lambda: None)
    with native(tmp_path):
        if minimum == 3:
            with pytest.raises(ValueError, match='insufficient rows'):
                with checked.bind_context():
                    flow.run(**arguments)
            assert not Path(arguments['target']).exists()
            assert [event[1] for event in events] == ['intent', 'value', 'intent', 'failed']
        else:
            with checked.bind_context():
                receipt = flow.run(**arguments)
            assert receipt.written_rows == 2
            assert events[1][2] == {'value': 2}


@pytest.mark.parametrize('columns,exact,valid', [(['x'], True, True), (['absent'], True, False), (['x', 'extra'], False, False)])
def test_column_prerequisite_is_checked_before_source_projection(tmp_path, catalog, columns, exact, valid):
    body = f'    rows.require_columns({columns!r}, exact={exact!r})\n' + '    return rows.write_lance(target, mode="create", return_receipt=True)\n'
    path, report, arguments = fixture(tmp_path, catalog, body=body)
    assert (report['status'] == 'checked') == valid, report['diagnostics']
    if valid:
        spec = importlib.util.spec_from_file_location('flows.values', path)
        flow = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(flow)
        checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                                   limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None)
        with native(tmp_path), checked.bind_context():
            assert flow.run(**arguments).written_rows == 2
    else:
        with native(tmp_path), pytest.raises(ValueError, match='columns differ'):
            data.read_lance(arguments['source'], version=1).require_columns(columns, exact=exact)


def test_empty_fixed_table_keeps_columns_and_declared_types(tmp_path, monkeypatch):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    schema = pa.schema([pa.field('key', pa.string(), nullable=False), pa.field('optional', pa.float64())])
    uri = str(tmp_path / 'empty.lance')
    lance.write_dataset(pa.Table.from_pylist([], schema=schema), uri)
    def no_sampling(*args, **kwargs):
        raise AssertionError('schema lookup must not execute a row plan')
    monkeypatch.setattr(LocalDatasetExecutor, '_apply_plan', no_sampling)
    with native(tmp_path):
        rows = data.read_lance(uri, version=1)
        assert rows.schema().equals(schema)
        rows.require_columns(['key', 'optional'])
        projected = data.read_lance(uri, version=1, columns=['key'])
        assert projected.schema().equals(pa.schema([schema.field('key')]))
        with pytest.raises(ValueError, match='columns differ'):
            rows.require_columns(['key'])


@pytest.mark.parametrize('body', [
    '    if rows.count() > 0:\n        return rows.write_lance(target, mode="create", return_receipt=True)\n',
    '    data.require({"runtime_value": {"node": "forged"}}, "forged")\n',
    '    total = rows.count()\n    return rows.limit(total).count()\n',
])
def test_query_values_cannot_choose_topology_or_forge_provenance(tmp_path, catalog, body):
    _, report, _ = fixture(tmp_path, catalog, body=body)
    assert report['status'] == 'invalid', report['diagnostics']
    assert report['diagnostics'][0]['rule'] in {'FL-06', 'FL-07'}


def test_runtime_guard_argument_drift_is_rejected(tmp_path, catalog):
    _, report, arguments = fixture(tmp_path, catalog)
    assert report['status'] == 'checked', report['diagnostics']
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None)
    with native(tmp_path), pytest.raises(RuntimeContractError, match='arguments'):
        with checked.bind_context():
            data.read_lance(arguments['source'], version=1).count()
            data.require(False, 'insufficient rows')
    assert not Path(arguments['target']).exists()


@pytest.mark.parametrize('corrupt', [False, True])
def test_resume_reuses_only_a_verified_native_write(tmp_path, catalog, corrupt):
    path, report, arguments = fixture(tmp_path, catalog)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.values', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    facts = []
    setup = {**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
             'source_root': tmp_path, 'limits': DEFAULT_LIMITS, 'check_stop': lambda: None}
    with native(tmp_path):
        with CheckedExecution(**setup, record=lambda *args: facts.append(args)).bind_context():
            first = module.run(**arguments)
        identity = report['actions'][-1]
        previous = {kind: value for node, kind, value in facts if node == identity and kind in {'intent', 'receipt'}}
        previous['attempt'] = 1
        if corrupt:
            previous['intent'] = {**previous['intent'], 'operation_id': 'forged'}
        following = []
        checked = CheckedExecution(**setup, prior_writes={identity: previous}, record=lambda *args: following.append(args))
        if corrupt:
            with pytest.raises(RuntimeContractError, match='commit evidence'):
                with checked.bind_context():
                    module.run(**arguments)
        else:
            with checked.bind_context():
                second = module.run(**arguments)
            assert first == second
            assert any(kind == 'intent' and value.get('reused_from_attempt') == 1 for _, kind, value in following)
        assert lance.dataset(arguments['target']).version == 1


def test_summary_rejects_non_json_floats_before_emitting_a_value(tmp_path):
    with native(tmp_path), pytest.raises(ValueError, match='JSON'):
        data.from_items([{'score': float('nan')}]).collect_summary(max_rows=1)


@pytest.mark.parametrize('drift', [False, True])
def test_explicit_schema_preserves_metadata_and_rejects_runtime_drift(tmp_path, catalog, monkeypatch, drift):
    body = ('    declared = data.schema({"fields": [{"name": "x", "type": "int64", "nullable": True, "metadata": {"unit": "items"}}], "metadata": {"revision": "1"}})\n'
            '    return rows.write_lance(target, mode="create", schema=declared, return_receipt=True)\n')
    path, report, arguments = fixture(tmp_path, catalog, body=body)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.values', path)
    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None)
    if drift:
        original = data.schema
        monkeypatch.setattr(data, 'schema', lambda declaration: original({**declaration, 'metadata': {'revision': 'changed'}}))
    with native(tmp_path):
        if drift:
            with pytest.raises(RuntimeContractError, match='write'):
                with checked.bind_context():
                    flow.run(**arguments)
            assert not Path(arguments['target']).exists()
        else:
            with checked.bind_context():
                flow.run(**arguments)
            schema = lance.dataset(arguments['target']).schema
            assert schema.metadata == {b'revision': b'1'}
            assert schema.field('x').metadata == {b'unit': b'items'}


@pytest.mark.parametrize('failure', [None, 'return_type', 'state_size'])
def test_reducer_actual_state_is_checked_on_each_step(tmp_path, catalog, monkeypatch, failure):
    path, _, arguments = fixture(tmp_path, catalog)
    op_path = tmp_path / 'operators' / 'fold.py'
    op_path.write_text('from typing import TypedDict\n'
                      'class Row(TypedDict):\n    x: int | None\n'
                      'def unique(acc: Row | None, row: Row) -> Row:\n'
                      '    if acc is not None:\n        raise ValueError("duplicate key")\n'
                      '    return row\n')
    path.write_text('from demiflow import data\nfrom operators.fold import unique\n'
                    'def run(*, source, target, minimum):\n'
                    '    rows = data.read_lance(source, version=1).reduce_by_key("x", unique)\n'
                    '    return rows.write_lance(target, mode="create", return_receipt=True)\n')
    report = FlowChecker(tmp_path, catalog, {'input': {'uri': arguments['source'], 'version': 1,
                         'fields': arrow_fields(lance.dataset(arguments['source']).schema)}}, [arguments['target']]).inspect('flows/values.py:run', arguments)
    assert report['status'] == 'checked', report['diagnostics']
    modules = []
    for name, location in [('operators.fold', op_path), ('flows.values', path)]:
        spec = importlib.util.spec_from_file_location(name, location)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules.append(module)
    if failure:
        # Keep the checked native callback identity and source location, while
        # injecting a runtime return violation that annotations cannot prevent.
        replacement = 'def changed(acc, row):\n    return {"x": "wrong"}\n' if failure == 'return_type' else 'def changed(acc, row):\n    return {"x": row["x"], "payload": "x" * 8192}\n'
        namespace = {}
        exec(compile(replacement, str(op_path), 'exec'), namespace)
        modules[0].unique.__code__ = namespace['changed'].__code__
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits={**DEFAULT_LIMITS, 'max_row_bytes': 4096}, record=lambda *args: None, check_stop=lambda: None)
    with native(tmp_path):
        if failure:
            with pytest.raises(RuntimeContractError):
                with checked.bind_context():
                    modules[-1].run(**arguments)
            assert not Path(arguments['target']).exists()
        else:
            with checked.bind_context():
                receipt = modules[-1].run(**arguments)
            assert receipt.written_rows == 2


@pytest.mark.parametrize('drift', [False, True])
def test_read_after_commit_uses_receipt_version(tmp_path, catalog, monkeypatch, drift):
    body = ('    receipt = rows.write_lance(target, mode="create", return_receipt=True)\n'
            '    committed = data.read_lance(receipt.uri, version=receipt.committed_version)\n'
            '    total = committed.count()\n'
            '    data.require(total == receipt.written_rows, "receipt mismatch")\n')
    path, report, arguments = fixture(tmp_path, catalog, body=body)
    assert report['status'] == 'checked', report['diagnostics']
    assert any(n['kind'] == 'read_table' and isinstance(n['parameters']['version'], dict) for n in report['nodes'])
    spec = importlib.util.spec_from_file_location('flows.values', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if drift:
        original = data.read_lance
        def wrong(uri, **kwargs):
            if uri == arguments['target']:
                kwargs['version'] += 1
            return original(uri, **kwargs)
        monkeypatch.setattr(data, 'read_lance', wrong)
    events = []
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *event: events.append(event), check_stop=lambda: None)
    with native(tmp_path):
        if drift:
            with pytest.raises(RuntimeContractError, match='version'):
                with checked.bind_context():
                    module.run(**arguments)
            # A later read failure cannot erase the earlier actual commit.
            assert lance.dataset(arguments['target']).version == 1
            assert any(e[1] == 'receipt' for e in events)
        else:
            with checked.bind_context():
                module.run(**arguments)
            assert [event[1] for event in events] == ['intent', 'receipt', 'intent', 'value', 'intent', 'value']


def test_count_cannot_forge_a_table_version(tmp_path, catalog):
    _, report, _ = fixture(tmp_path, catalog, body='    return data.read_lance(source, version=rows.count()).count()\n')
    assert report['status'] == 'invalid' and report['diagnostics'][0]['rule'] == 'FL-05'


@pytest.mark.parametrize('configured', ['None', "{'limit': 1}", '[1, 2]'])
def test_optional_plain_configuration_has_static_presence(tmp_path, catalog, configured):
    _, report, _ = fixture(tmp_path, catalog, body=(
        '    option = ' + configured + '\n'
        '    if option is not None:\n'
        '        rows = rows.limit(1)\n'
        '    return rows.write_lance(target, mode="create", return_receipt=True)\n'))
    assert report['status'] == 'checked', report['diagnostics']


def test_native_query_is_not_an_optional_configuration(tmp_path, catalog):
    _, report, _ = fixture(tmp_path, catalog, body=(
        '    option = rows.count()\n'
        '    if option is not None:\n'
        '        rows = rows.limit(1)\n'
        '    return rows.write_lance(target, mode="create", return_receipt=True)\n'))
    assert report['status'] == 'invalid', report['diagnostics']


@pytest.mark.parametrize('race', [False, True])
def test_fixed_read_and_cas_overwrite_preserve_historical_version(tmp_path, catalog, race):
    path, _, arguments = fixture(tmp_path, catalog)
    arguments['target'] = arguments['source']
    path.write_text('from demiflow import data\ndef run(*, source, target, minimum):\n'
                    '    rows = data.read_lance(source, version=1).limit(1)\n'
                    '    return rows.write_lance(target, mode="overwrite", expected_version=1, return_receipt=True)\n')
    table = lance.dataset(arguments['source'])
    report = FlowChecker(tmp_path, catalog, {'input': {'uri': arguments['source'], 'version': 1, 'fields': arrow_fields(table.schema)}},
                         {arguments['target']: {'mode': 'overwrite', 'expected_version': 1}}).inspect('flows/values.py:run', arguments)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.values', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events = []
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *event: events.append(event), check_stop=lambda: None)
    if race:
        lance.write_dataset(pa.Table.from_pylist([{'x': 3}]), arguments['source'], mode='append')
    with native(tmp_path):
        if race:
            with pytest.raises(RuntimeContractError, match='CAS baseline'):
                with checked.bind_context():
                    module.run(**arguments)
            assert lance.dataset(arguments['source']).count_rows() == 3
        else:
            with checked.bind_context():
                receipt = module.run(**arguments)
            assert receipt.committed_version == 2 and receipt.written_rows == 1
            import json
            import os
            import subprocess
            script = ('import json,resource,sys\n'
                      'from demiflow.lance.reconcile import reconcile_lance_overwrite\n'
                      'resource.setrlimit(resource.RLIMIT_AS, (4*1024**3, 4*1024**3))\n'
                      'print(json.dumps(reconcile_lance_overwrite(**json.load(sys.stdin)).to_dict()))\n')
            recovered = subprocess.run([sys.executable, '-B', '-c', script], text=True, capture_output=True, timeout=30,
                input=json.dumps(dict(uri=arguments['target'], request_hash=receipt.request_hash,
                                      operation_id=events[0][2]['operation_id'], expected_version=1)),
                env={**os.environ, 'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'RAYON_NUM_THREADS': '2', 'TOKIO_WORKER_THREADS': '2'})
            assert recovered.returncode == 0, recovered.stderr
            assert json.loads(recovered.stdout) == receipt.to_dict()
    assert lance.dataset(arguments['source'], version=1).count_rows() == 2


@pytest.mark.parametrize('bound', [1, 2])
def test_bounded_summary_never_truncates_and_can_be_written(tmp_path, catalog, bound):
    body = ('    summary = rows.collect_summary(max_rows=BOUND, max_bytes=4096)\n'
            '    output = data.from_items(summary)\n'
            '    return output.write_lance(target, mode="create", return_receipt=True)\n').replace('BOUND', str(bound))
    path, report, arguments = fixture(tmp_path, catalog, body=body)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.values', path)
    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *event: None, check_stop=lambda: None)
    with native(tmp_path):
        if bound == 1:
            with pytest.raises(ValueError, match='Summary row budget'):
                with checked.bind_context():
                    flow.run(**arguments)
            assert not Path(arguments['target']).exists()
        else:
            with checked.bind_context():
                receipt = flow.run(**arguments)
            assert receipt.written_rows == 2


def test_summary_row_can_combine_runtime_counts_and_config(tmp_path, catalog):
    body = ('    total = rows.count()\n'
            '    summary = data.from_items([{"total": total, "remaining": total - minimum, "label": "summary"}])\n'
            '    return summary.write_lance(target, mode="create", return_receipt=True)\n')
    path, report, arguments = fixture(tmp_path, catalog, body=body)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.values', path)
    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')}, source_root=tmp_path,
                               limits=DEFAULT_LIMITS, record=lambda *event: None, check_stop=lambda: None)
    with native(tmp_path), checked.bind_context():
        flow.run(**arguments)
    assert lance.dataset(arguments['target']).to_table().to_pylist() == [{'total': 2, 'remaining': 1, 'label': 'summary'}]


@pytest.mark.parametrize('mode', ['valid', 'static_type', 'runtime_drift'])
def test_reducer_fixed_parameters_are_checked_and_used(tmp_path, catalog, monkeypatch, mode):
    path, _, arguments = fixture(tmp_path, catalog)
    op_path = tmp_path / 'operators' / 'configured.py'
    op_path.write_text('from typing import TypedDict\n'
        'class Row(TypedDict):\n    x: int | None\n'
        'class Total(TypedDict):\n    x: int\n    total: int\n'
        'def fold(acc: Total | None, row: Row, scale: int) -> Total:\n'
        '    x = row["x"]\n'
        '    if x is None:\n        raise ValueError("missing x")\n'
        '    return {"x": x, "total": (acc["total"] if acc is not None else 0) + x * scale}\n')
    scale = 'wrong' if mode == 'static_type' else 3
    path.write_text('from demiflow import data\nfrom operators.configured import fold\n'
        'def run(*, source, target, minimum):\n'
        f'    rows = data.read_lance(source, version=1).reduce_by_key("x", fold, fn_kwargs={{"scale": {scale!r}}})\n'
        '    return rows.write_lance(target, mode="create", return_receipt=True)\n')
    report = FlowChecker(tmp_path, catalog, {'input': {'uri': arguments['source'], 'version': 1,
        'fields': arrow_fields(lance.dataset(arguments['source']).schema)}}, [arguments['target']]).inspect('flows/values.py:run', arguments)
    if mode == 'static_type':
        assert report['status'] == 'invalid'
        assert 'fold.scale' in report['diagnostics'][0]['detail']
        return
    assert report['status'] == 'checked', report['diagnostics']
    for name, location in [('operators.configured', op_path), ('flows.values', path)]:
        spec = importlib.util.spec_from_file_location(name, location)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
    if mode == 'runtime_drift':
        original = data.Dataset.reduce_by_key
        def changed(self, *args, **kwargs):
            result = original(self, *args, **kwargs)
            result._source.reducer_kwargs['scale'] = 9
            return result
        monkeypatch.setattr(data.Dataset, 'reduce_by_key', changed)
    checked = CheckedExecution(**{key: report[key] for key in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None)
    with native(tmp_path):
        if mode == 'runtime_drift':
            with pytest.raises(RuntimeContractError, match='scale|fn_kwargs'):
                with checked.bind_context():
                    module.run(**arguments)
            assert not Path(arguments['target']).exists()
        else:
            with checked.bind_context():
                receipt = module.run(**arguments)
            assert sorted(lance.dataset(receipt.uri).to_table().to_pylist(), key=lambda r: r['x']) == [
                {'x': 1, 'total': 3}, {'x': 2, 'total': 6}]
