"""Pure preflight keeps the original admission contract and export identities."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


REPO = Path(__file__).resolve().parents[1]


def _cold(script):
    environment = {**os.environ, 'PYTHONPATH': str(REPO), 'PYTHONDONTWRITEBYTECODE': '1'}
    return subprocess.run([sys.executable, '-c', script], cwd=REPO, env=environment,
                          check=True, capture_output=True, text=True, timeout=30)


def test_pure_types_and_actual_preflight_do_not_load_runtime_owners():
    result = _cold('''
import json
import sys
from demiflow.data.execution_contract import InputSpec
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import ItemsSource, KeyedSource, PlanInput, UnionSource
from demiflow.planning.dataflow_declaration import validate_native_operation, validate_relational_inputs
assert validate_native_operation('fetch', {'uri': 'synthetic'}, bindings={}) is None
left = PlanInput(ItemsSource([{'id': 1}]), LogicalPlan(), None)
right = PlanInput(ItemsSource([{'id': 1}]), LogicalPlan(), None)
source = KeyedSource('join', left, ('id',), right, ('id',))
assert validate_relational_inputs(source, expected_inputs=2) is None
forbidden = (
    'demiflow.execution.local_kernel', 'demiflow.execution.run_context', 'demiflow.execution.checked',
    'demiflow.execution.action_driver.dataflow', 'demiflow.execution.executors',
    'demiflow.execution.package_loader', 'demiflow.execution.dataflow_api',
    'demiflow.execution.dataflow_models',
)
loaded = [name for name in sys.modules
          if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden)]
assert not loaded, loaded
print(json.dumps({'loaded_runtime_owners': loaded}))
''')
    assert json.loads(result.stdout) == {'loaded_runtime_owners': []}


def test_lazy_execution_exports_are_the_original_objects():
    _cold('''
import importlib
import demiflow.execution as execution
import demiflow.execution.contracts as contracts
for name, (module, attribute) in execution._EXPORTS.items():
    original = getattr(importlib.import_module(module, execution.__name__), attribute)
    assert getattr(execution, name) is original, name
    assert getattr(execution, name) is original, name
pipeline = importlib.import_module('demiflow.execution.contracts.pipeline')
for name in contracts.__all__:
    assert getattr(contracts, name) is getattr(pipeline, name), name
namespace = {}
exec('from demiflow.execution import *', namespace)
assert {name for name in namespace if name != '__builtins__'} == set(execution.__all__)
assert all(namespace[name] is getattr(execution, name) for name in execution.__all__)
for package in (execution, contracts):
    assert set(package.__all__) <= set(dir(package))
    try:
        package.not_a_public_export
    except AttributeError:
        pass
    else:
        raise AssertionError('unknown export accepted')
''')


def _depth_limit():
    value = 0
    for _ in range(33):
        value = [value]
    return {'value': value}


# All payloads are finite test data. The largest is one 1 MiB string; the node
# case is 65,536 nulls, below the encoded mapping budget but above strict Json's
# node ceiling. These distinguish the two original admission stages.
INVALID_DECLARATIONS = [
    (None, {}, {}, 'DataflowError', 'INVALID_CONTRACT', 'native operation kind must be a non-empty str'),
    ('', {}, {}, 'DataflowError', 'INVALID_CONTRACT', 'native operation kind must be a non-empty str'),
    ('x' * 129, {}, {}, 'ValueError', None, 'native operation kind must be a non-empty bounded string'),
    ('x' * 129, [], {}, 'DataflowError', 'INVALID_CONTRACT', 'configuration must be a mapping'),
    ('fetch', [], {}, 'DataflowError', 'INVALID_CONTRACT', 'configuration must be a mapping'),
    ('fetch', {}, [], 'DataflowError', 'INVALID_CONTRACT', 'bindings must be a mapping'),
    ('fetch', {'value': object()}, {}, 'DataflowError', 'INVALID_CONTRACT', 'configuration must be bounded Json'),
    ('fetch', {'value': float('nan')}, {}, 'DataflowError', 'INVALID_CONTRACT', 'configuration must be bounded Json'),
    ('fetch', {'value': 'x' * (1024 * 1024)}, {}, 'DataflowError', 'BUDGET_EXCEEDED', 'configuration exceeds declaration budget'),
    ('fetch', {'value': (1, 2)}, {}, 'ValueError', None, 'native operation declaration must be strict Json, got tuple'),
    ('fetch', {}, {'value': (1, 2)}, 'ValueError', None, 'native operation declaration must be strict Json, got tuple'),
    ('fetch', {True: 1}, {}, 'ValueError', None, 'native operation declaration keys must be strings'),
    ('fetch', _depth_limit(), {}, 'ValueError', None, 'native operation declaration exceeds depth budget'),
    ('fetch', {'value': [None] * 65536}, {}, 'ValueError', None, 'native operation declaration exceeds node budget'),
    ('unknown-kind', {'value': (1, 2)}, {}, 'ValueError', None, 'native operation declaration must be strict Json, got tuple'),
    ('unknown-kind', {}, {}, 'DataflowError', 'UNSUPPORTED_CAPABILITY', 'unknown native operation: unknown-kind'),
]


@pytest.mark.parametrize('kind,configuration,bindings,error_type,code,message', INVALID_DECLARATIONS)
def test_preflight_and_real_factory_preserve_full_errors_and_order(
        kind, configuration, bindings, error_type, code, message):
    from demiflow.planning.dataflow_declaration import validate_native_operation
    from demiflow.execution.dataflow_api import make_native_operation

    for function in (validate_native_operation, make_native_operation):
        with pytest.raises(Exception) as raised:
            function(kind, configuration, bindings=bindings)
        error = raised.value
        assert type(error).__name__ == error_type
        assert getattr(error, 'code', None) == code
        assert str(error) == (f'{code}: {message}' if code else message)


@pytest.mark.parametrize('kind', [
    'embeddings', 'fetch', 'enqueue', 'ack_queue', 'lookup', 'prepend',
    'read_object', 'admit', 'cache', 'run_stream', 'MapOp', 'prompt',
])
def test_preflight_returns_none_and_real_factory_keeps_its_original_target(kind):
    from demiflow.data.dataflow_operation import NativeOperation
    from demiflow.execution.dataflow_api import make_native_operation, _native_operation_factory
    from demiflow.planning.dataflow_declaration import validate_native_operation

    configuration, bindings = {'values': [1, True, None]}, {'key': 'value'}
    assert validate_native_operation(kind, configuration, bindings=bindings) is None
    operation = make_native_operation(kind, configuration, bindings=bindings)
    assert type(operation) is NativeOperation
    assert operation.kind == kind
    assert operation.factory.target is _native_operation_factory
    assert operation.configuration['values'] == (1, True, None)
    assert dict(operation.bindings) == bindings
    assert configuration == {'values': [1, True, None]}


def test_relational_preflight_retains_original_port_multiplicity_refusal():
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource, KeyedSource, PlanInput
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.planning.dataflow_declaration import validate_relational_inputs

    left = PlanInput(ItemsSource([{'id': 1}]), LogicalPlan(), None)
    source = KeyedSource('groups', left, ('id',))
    assert validate_relational_inputs(source, expected_inputs=1) is None
    with pytest.raises(DataflowError, match='relational input port multiplicity was not preserved'):
        validate_relational_inputs(source, expected_inputs=2)
